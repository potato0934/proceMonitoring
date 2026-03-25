package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"
)

type ProductConfig struct {
	Name        string `json:"name"`
	StoreName   string `json:"store_name"`
	URL         string `json:"url"`
	PriceRegex  string `json:"price_regex"`
	UpdateRegex string `json:"update_regex"`
	Currency    string `json:"currency"`
	Active      bool   `json:"active"`
}

type Product struct {
	ID          int64
	Name        string
	StoreName   string
	URL         string
	PriceRegex  string
	UpdateRegex string
	Currency    string
	Active      bool
}

type PriceRecord struct {
	Date           string   `json:"date"`
	Price          float64  `json:"price"`
	CNYPrice       *float64 `json:"cny_price,omitempty"`
	FXRate         *float64 `json:"fx_rate,omitempty"`
	RecordedAt     string   `json:"recorded_at,omitempty"`
	MerchantUpdate string   `json:"merchant_update,omitempty"`
}

type FetchResult struct {
	Price          float64
	RawPrice       string
	MerchantUpdate string
}

var chinaLoc = mustLoadChinaLocation()
var defaultFlareSolverrURL = "http://172.25.0.102:8191/v1"
var defaultBasePath = ""
var defaultWeComWebhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b109bcf8-2316-4015-9a8f-7c3770799f0c"
var flareSolverrCB flaresolverrCircuitBreaker
var flareSolverrReqMu sync.Mutex
var flareSolverrSessionCache struct {
	mu      sync.Mutex
	name    string
	url     string
	ensured time.Time
}
var collectRunning atomic.Bool
var verboseLogEnabled atomic.Bool
var weComAlertState struct {
	mu   sync.Mutex
	last map[string]time.Time
}

type App struct {
	db        *sql.DB
	templates *template.Template
	fx        fxCache
	basePath  string
}

type fxCache struct {
	mu      sync.Mutex
	date    string
	rate    float64
	source  string
	updated time.Time
}

type flareSolverrResp struct {
	Status   string `json:"status"`
	Message  string `json:"message"`
	Solution struct {
		Status   int    `json:"status"`
		Response string `json:"response"`
	} `json:"solution"`
}

type flaresolverrCircuitBreaker struct {
	mu           sync.Mutex
	fails        int
	disabledTill time.Time
}

type httpStatusError struct {
	Code int
	Body string
}

type outOfStockError struct {
	Detail string
}

type dailyLogWriter struct {
	mu              sync.Mutex
	dir             string
	currentDate     string
	lastCleanupDate string
	retentionDays   int
	file            *os.File
	stopCh          chan struct{}
	doneCh          chan struct{}
}

var httpRequestSeq uint64
var runtimeMon = newRuntimeMonitor(1200, 20)

type runtimeLogEntry struct {
	ID   int64  `json:"id"`
	TS   string `json:"ts"`
	Line string `json:"line"`
}

type runtimeTask struct {
	Worker    string `json:"worker"`
	ProductID int64  `json:"product_id"`
	Product   string `json:"product"`
	Store     string `json:"store"`
	Phase     string `json:"phase"`
	UpdatedAt string `json:"updated_at"`
}

type runtimeCollectRunSummary struct {
	RunID           int64  `json:"run_id"`
	RunLabel        string `json:"run_label"`
	StartedAt       string `json:"started_at"`
	EndedAt         string `json:"ended_at"`
	DurationMS      int64  `json:"duration_ms"`
	TotalItems      int    `json:"total_items"`
	SuccessCount    int    `json:"success_count"`
	OutOfStockCount int    `json:"out_of_stock_count"`
	FailCount       int    `json:"fail_count"`
	Result          string `json:"result"`
	Error           string `json:"error,omitempty"`
}

type runtimeMonitor struct {
	mu sync.RWMutex

	logs      []runtimeLogEntry
	logCap    int
	nextLogID int64
	logCarry  string

	runSeq int64

	running          bool
	runID            int64
	runLabel         string
	runStartedAt     time.Time
	runUpdatedAt     time.Time
	runEndedAt       time.Time
	totalGroups      int
	doneGroups       int
	totalItems       int
	doneItems        int
	successCount     int
	outOfStockCount  int
	failCount        int
	pendingRetry     int
	currentGroupURL  string
	currentGroupIdx  int
	currentGroupSize int
	currentTask      runtimeTask
	lastResult       string
	lastError        string
	failureBuckets   map[string]int
	recentRuns       []runtimeCollectRunSummary
	recentRunCap     int
}

func newRuntimeMonitor(logCap, recentRunCap int) *runtimeMonitor {
	if logCap < 200 {
		logCap = 200
	}
	if recentRunCap < 5 {
		recentRunCap = 5
	}
	return &runtimeMonitor{
		logCap:       logCap,
		recentRunCap: recentRunCap,
		logs:         make([]runtimeLogEntry, 0, logCap),
		failureBuckets: map[string]int{
			"connectivity": 0,
			"waf403":       0,
			"regex":        0,
			"other":        0,
		},
	}
}

func (m *runtimeMonitor) appendLogBytes(p []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logCarry += string(p)
	for {
		idx := strings.IndexByte(m.logCarry, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(m.logCarry[:idx], "\r")
		m.logCarry = m.logCarry[idx+1:]
		m.appendLogLineLocked(line)
	}
}

func (m *runtimeMonitor) appendLogLineLocked(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	m.nextLogID++
	m.logs = append(m.logs, runtimeLogEntry{
		ID:   m.nextLogID,
		TS:   chinaNow().Format("2006-01-02 15:04:05"),
		Line: line,
	})
	if len(m.logs) > m.logCap {
		m.logs = m.logs[len(m.logs)-m.logCap:]
	}
}

func (m *runtimeMonitor) beginCollectRun(runLabel string, totalGroups, totalItems int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runSeq++
	m.running = true
	m.runID = m.runSeq
	m.runLabel = runLabel
	m.runStartedAt = chinaNow()
	m.runUpdatedAt = m.runStartedAt
	m.runEndedAt = time.Time{}
	m.totalGroups = totalGroups
	m.doneGroups = 0
	m.totalItems = totalItems
	m.doneItems = 0
	m.successCount = 0
	m.outOfStockCount = 0
	m.failCount = 0
	m.pendingRetry = 0
	m.currentGroupURL = ""
	m.currentGroupIdx = 0
	m.currentGroupSize = 0
	m.currentTask = runtimeTask{}
	m.lastResult = ""
	m.lastError = ""
	m.failureBuckets = map[string]int{
		"connectivity": 0,
		"waf403":       0,
		"regex":        0,
		"other":        0,
	}
}

func (m *runtimeMonitor) setCollectGroup(idx, total, groupSize int, groupURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentGroupIdx = idx
	m.totalGroups = total
	m.currentGroupSize = groupSize
	m.currentGroupURL = groupURL
	m.runUpdatedAt = chinaNow()
}

func (m *runtimeMonitor) finishCollectGroup() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doneGroups++
	m.runUpdatedAt = chinaNow()
}

func (m *runtimeMonitor) setCurrentTask(p Product, phase string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTask = runtimeTask{
		Worker:    "collector-1",
		ProductID: p.ID,
		Product:   p.Name,
		Store:     p.StoreName,
		Phase:     phase,
		UpdatedAt: chinaNow().Format("2006-01-02 15:04:05"),
	}
	m.runUpdatedAt = chinaNow()
}

func (m *runtimeMonitor) clearCurrentTask() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTask = runtimeTask{}
	m.runUpdatedAt = chinaNow()
}

func (m *runtimeMonitor) setPendingRetry(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingRetry = n
	m.runUpdatedAt = chinaNow()
}

func classifyRuntimeFailure(errText string) string {
	s := strings.ToLower(strings.TrimSpace(errText))
	switch {
	case strings.Contains(s, "connection refused"),
		strings.Contains(s, "i/o timeout"),
		strings.Contains(s, "context deadline exceeded"),
		strings.Contains(s, "client.timeout exceeded"),
		strings.Contains(s, "no such host"),
		strings.Contains(s, "network is unreachable"),
		strings.Contains(s, "flaresolverr 不可用("):
		return "connectivity"
	case strings.Contains(s, "http 状态码异常: 403"),
		strings.Contains(s, "just a moment"),
		strings.Contains(s, "cloudflare"):
		return "waf403"
	case strings.Contains(s, "未匹配到字段"),
		strings.Contains(s, "正则无效"),
		strings.Contains(s, "解析价格失败"):
		return "regex"
	default:
		return "other"
	}
}

func (m *runtimeMonitor) markItemResult(status string, errText string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doneItems++
	switch status {
	case "success":
		m.successCount++
	case "out_of_stock":
		m.outOfStockCount++
	default:
		m.failCount++
		b := classifyRuntimeFailure(errText)
		m.failureBuckets[b] = m.failureBuckets[b] + 1
		m.lastError = strings.TrimSpace(errText)
	}
	m.runUpdatedAt = chinaNow()
}

func (m *runtimeMonitor) finishCollectRun(result string, runErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	m.runEndedAt = chinaNow()
	m.runUpdatedAt = m.runEndedAt
	m.lastResult = result
	if runErr != nil {
		m.lastError = strings.TrimSpace(runErr.Error())
	}
	duration := m.runEndedAt.Sub(m.runStartedAt)
	sum := runtimeCollectRunSummary{
		RunID:           m.runID,
		RunLabel:        m.runLabel,
		StartedAt:       m.runStartedAt.Format("2006-01-02 15:04:05"),
		EndedAt:         m.runEndedAt.Format("2006-01-02 15:04:05"),
		DurationMS:      duration.Milliseconds(),
		TotalItems:      m.totalItems,
		SuccessCount:    m.successCount,
		OutOfStockCount: m.outOfStockCount,
		FailCount:       m.failCount,
		Result:          result,
	}
	if runErr != nil {
		sum.Error = runErr.Error()
	}
	m.recentRuns = append([]runtimeCollectRunSummary{sum}, m.recentRuns...)
	if len(m.recentRuns) > m.recentRunCap {
		m.recentRuns = m.recentRuns[:m.recentRunCap]
	}
	m.currentTask = runtimeTask{}
	m.pendingRetry = 0
}

func (m *runtimeMonitor) snapshot() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cbDisabledTill := ""
	cbFails := 0
	flareSolverrCB.mu.Lock()
	cbFails = flareSolverrCB.fails
	disabledTill := flareSolverrCB.disabledTill
	flareSolverrCB.mu.Unlock()
	if !disabledTill.IsZero() {
		cbDisabledTill = disabledTill.In(chinaLoc).Format("2006-01-02 15:04:05")
	}
	now := chinaNow()
	runForMS := int64(0)
	if m.running && !m.runStartedAt.IsZero() {
		runForMS = now.Sub(m.runStartedAt).Milliseconds()
	}
	failBuckets := map[string]int{
		"connectivity": m.failureBuckets["connectivity"],
		"waf403":       m.failureBuckets["waf403"],
		"regex":        m.failureBuckets["regex"],
		"other":        m.failureBuckets["other"],
	}
	recentRuns := make([]runtimeCollectRunSummary, len(m.recentRuns))
	copy(recentRuns, m.recentRuns)
	task := m.currentTask
	activeTasks := []runtimeTask{}
	if m.running && task.ProductID > 0 {
		activeTasks = append(activeTasks, task)
	}
	return map[string]any{
		"collect_running":      m.running,
		"collect_lock_running": collectRunning.Load(),
		"run_id":               m.runID,
		"run_label":            m.runLabel,
		"run_started_at":       formatRuntimeTime(m.runStartedAt),
		"run_updated_at":       formatRuntimeTime(m.runUpdatedAt),
		"run_ended_at":         formatRuntimeTime(m.runEndedAt),
		"run_for_ms":           runForMS,
		"total_groups":         m.totalGroups,
		"done_groups":          m.doneGroups,
		"total_items":          m.totalItems,
		"done_items":           m.doneItems,
		"success_count":        m.successCount,
		"out_of_stock_count":   m.outOfStockCount,
		"fail_count":           m.failCount,
		"pending_retry":        m.pendingRetry,
		"current_group_url":    m.currentGroupURL,
		"current_group_index":  m.currentGroupIdx,
		"current_group_size":   m.currentGroupSize,
		"active_workers":       len(activeTasks),
		"active_tasks":         activeTasks,
		"last_result":          m.lastResult,
		"last_error":           m.lastError,
		"fail_buckets":         failBuckets,
		"recent_runs":          recentRuns,
		"goroutines":           runtime.NumGoroutine(),
		"flaresolverr_enabled": useFlareSolverrFallback(),
		"flaresolverr_url":     flareSolverrURL(),
		"flaresolverr_breaker": map[string]any{
			"fails":         cbFails,
			"disabled_till": cbDisabledTill,
		},
		"verbose_log": verboseLogEnabled.Load(),
	}
}

func formatRuntimeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(chinaLoc).Format("2006-01-02 15:04:05")
}

func (m *runtimeMonitor) logsSince(sinceID int64, limit int) ([]runtimeLogEntry, int64) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 1000 {
		limit = 1000
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]runtimeLogEntry, 0, limit)
	for i := len(m.logs) - 1; i >= 0; i-- {
		e := m.logs[i]
		if e.ID <= sinceID {
			break
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, m.nextLogID
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP 状态码异常: %d, body=%q", e.Code, e.Body)
}

func (e *outOfStockError) Error() string {
	if strings.TrimSpace(e.Detail) == "" {
		return "无货"
	}
	return "无货: " + strings.TrimSpace(e.Detail)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: go run ./cmd/price-monitor [collect|ingest|serve|schedule|notify]")
		os.Exit(1)
	}

	dbPath := envOrDefault("DB_PATH", "./data.db")
	cfgPath := envOrDefault("PRODUCTS_CONFIG", "./config/products.json")
	logDir := envOrDefault("LOG_DIR", "./logs")
	verboseLogEnabled.Store(isTruthyEnv(os.Getenv("VERBOSE_LOG")))
	log.Printf("[log] VERBOSE_LOG=%t", verboseLogEnabled.Load())

	logWriter, err := setupLogging(logDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化日志失败: %v\n", err)
		os.Exit(1)
	}
	defer logWriter.Close()

	db, err := sql.Open("sqlite", dbPath)
	must(err)
	defer db.Close()

	must(applySQLiteRuntimePragmas(db))
	must(withSQLiteBusyRetry("migrate", func() error { return migrate(db) }))
	must(withSQLiteBusyRetry("sync-products-config-if-empty", func() error { return syncProductsFromConfigIfEmpty(db, cfgPath) }))

	switch os.Args[1] {
	case "collect":
		must(collectToday(db))
		fmt.Println("采集完成")
	case "serve":
		addr := envOrDefault("ADDR", ":8080")
		app := &App{
			db:       db,
			basePath: normalizeBasePath(envOrDefault("BASE_PATH", defaultBasePath)),
		}
		must(app.loadTemplates())
		must(app.serve(addr))
	case "schedule":
		must(runSchedule(db))
	case "notify":
		must(runNotify(db))
		fmt.Println("推送完成")
	default:
		fmt.Println("未知命令，仅支持 collect、serve、schedule 或 notify")
		os.Exit(1)
	}
}

func runNotify(db *sql.DB) error {
	log.Printf("[notify] 手动触发企业微信推送开始")
	if err := sendPricePushToWeCom(db); err != nil {
		return err
	}
	log.Printf("[notify] 手动触发企业微信推送成功")
	return nil
}

func setupLogging(dir string) (*dailyLogWriter, error) {
	retentionDays := 7
	if v := strings.TrimSpace(os.Getenv("LOG_RETENTION_DAYS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			retentionDays = n
		}
	}
	w := &dailyLogWriter{
		dir:           dir,
		retentionDays: retentionDays,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	if err := w.rotateIfNeeded(chinaNow()); err != nil {
		return nil, err
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(io.MultiWriter(os.Stdout, w))
	w.startMaintenance()
	log.Printf("[log] 日志初始化完成: dir=%s retention_days=%d", dir, retentionDays)
	return w, nil
}

func (w *dailyLogWriter) Write(p []byte) (int, error) {
	runtimeMon.appendLogBytes(p)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateIfNeeded(chinaNow()); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}

func (w *dailyLogWriter) Close() error {
	close(w.stopCh)
	<-w.doneCh

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

func (w *dailyLogWriter) rotateIfNeeded(now time.Time) error {
	date := now.Format("2006-01-02")
	if w.file == nil || w.currentDate != date {
		if err := os.MkdirAll(w.dir, 0o755); err != nil {
			return err
		}
		if w.file != nil {
			_ = w.file.Close()
			w.file = nil
		}
		path := filepath.Join(w.dir, date+".log")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		w.file = f
		w.currentDate = date
	}
	return w.cleanupOldLogs(now)
}

func (w *dailyLogWriter) startMaintenance() {
	go func() {
		defer close(w.doneCh)
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-w.stopCh:
				return
			case <-ticker.C:
				w.mu.Lock()
				err := w.rotateIfNeeded(chinaNow())
				w.mu.Unlock()
				if err != nil {
					log.Printf("[log] 定时维护失败: %v", err)
				}
			}
		}
	}()
}

func (w *dailyLogWriter) cleanupOldLogs(now time.Time) error {
	if w.retentionDays <= 0 {
		return nil
	}
	today := now.Format("2006-01-02")
	if w.lastCleanupDate == today {
		return nil
	}
	cutoff := now.AddDate(0, 0, -w.retentionDays)
	pattern := filepath.Join(w.dir, "*.log")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, p := range paths {
		base := filepath.Base(p)
		if !strings.HasSuffix(base, ".log") {
			continue
		}
		day := strings.TrimSuffix(base, ".log")
		d, err := time.ParseInLocation("2006-01-02", day, chinaLoc)
		if err != nil {
			continue
		}
		if d.Before(cutoff) {
			_ = os.Remove(p)
		}
	}
	w.lastCleanupDate = today
	return nil
}

type dailySlot struct {
	Hour   int
	Minute int
}

type scheduleKind string

const (
	scheduleCollect scheduleKind = "collect"
	schedulePush    scheduleKind = "push"
)

func runSchedule(db *sql.DB) error {
	timesRaw := envOrDefault("COLLECT_TIMES", "10:00,11:00,15:00")
	pushTimesRaw := envOrDefault("PUSH_TIMES", "12:00")
	locName := envOrDefault("SCHEDULE_TZ", "Asia/Shanghai")
	failedRetryInterval := collectFailedRetryInterval()
	log.Printf("[schedule] 启动参数原文: COLLECT_TIMES=%s PUSH_TIMES=%s SCHEDULE_TZ=%s", timesRaw, pushTimesRaw, locName)

	loc, err := time.LoadLocation(locName)
	if err != nil {
		log.Printf("[schedule] 时区加载失败: %s err=%v，回退为 Asia/Shanghai(UTC+8)", locName, err)
		loc = chinaLoc
	}
	collectSlots, err := parseDailySlots(timesRaw)
	if err != nil {
		return err
	}
	pushSlots, err := parseDailySlots(pushTimesRaw)
	if err != nil {
		return err
	}
	log.Printf("[schedule] 调度启动参数: 时区=%s", locName)
	log.Printf("[schedule] 定时采集时间表: %s", formatDailySlots(collectSlots))
	log.Printf("[schedule] 定时推送时间表: %s", formatDailySlots(pushSlots))
	if failedRetryInterval > 0 {
		log.Printf("[schedule] 失败补采检查间隔: %s", failedRetryInterval.String())
	} else {
		log.Printf("[schedule] 失败补采检查已禁用")
	}

	if useFlareSolverrFallback() {
		if err := preflightFlareSolverr(); err != nil {
			log.Printf("[schedule] 启动预检 FlareSolverr 失败，不影响推送任务: %v", err)
			maybeNotifyFlareSolverrConnectivityAlert("schedule 启动预检", err)
		}
	} else {
		log.Printf("[schedule] 已禁用 FlareSolverr 后备通道（ENABLE_FLARESOLVERR_FALLBACK=false）")
	}
	log.Printf("[schedule] 调度已启动")

	if failedRetryInterval > 0 {
		go func() {
			for {
				now := time.Now().In(loc)
				next := nextIntervalBoundary(now, failedRetryInterval)
				wait := time.Until(next)
				if wait < 0 {
					wait = 0
				}
				timer := time.NewTimer(wait)
				<-timer.C
				timer.Stop()
				log.Printf("[schedule] 到达失败补采检查时间，开始检查失败商品")
				if err := collectFailedProducts(db); err != nil {
					log.Printf("[schedule] 失败补采执行结果: %v", err)
				} else {
					log.Printf("[schedule] 失败补采执行完成")
				}
			}
		}()
	}

	for {
		now := time.Now().In(loc)
		nextCollect := nextRunTime(now, collectSlots, loc)
		nextPush := nextRunTime(now, pushSlots, loc)
		next := nextCollect
		kind := scheduleCollect
		runBoth := false
		if nextPush.Before(nextCollect) {
			next = nextPush
			kind = schedulePush
		} else if nextPush.Equal(nextCollect) {
			next = nextCollect
			kind = scheduleCollect
			runBoth = true
		}
		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		debugLogf("[schedule] 当前时间=%s", now.Format("2006-01-02 15:04:05 -0700 MST"))
		debugLogf("[schedule] 下次任务=%s 时间=%s (等待=%s)", kind, next.Format("2006-01-02 15:04:05 -0700 MST"), wait.String())

		timer := time.NewTimer(wait)
		<-timer.C
		timer.Stop()

		if kind == scheduleCollect {
			log.Printf("[schedule] 开始执行定时采集")
			if err := collectToday(db); err != nil {
				log.Printf("[schedule] 定时采集失败: %v", err)
			} else {
				log.Printf("[schedule] 定时采集完成")
			}
			if !runBoth {
				continue
			}
			log.Printf("[schedule] 同时命中推送时间，继续执行企业微信推送")
		}
		if kind == schedulePush || runBoth {
			log.Printf("[schedule] 到达推送触发时间，开始执行企业微信推送")
			if err := sendPricePushToWeCom(db); err != nil {
				log.Printf("[schedule] 企业微信推送失败: %v", err)
			} else {
				log.Printf("[schedule] 企业微信推送完成")
			}
		}
	}
}

func parseDailySlots(raw string) ([]dailySlot, error) {
	items := strings.Split(raw, ",")
	var out []dailySlot
	for _, it := range items {
		s := strings.TrimSpace(it)
		parts := strings.Split(s, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("时间格式错误: %s，要求 HH:MM", s)
		}
		h, err := strconv.Atoi(parts[0])
		if err != nil || h < 0 || h > 23 {
			return nil, fmt.Errorf("小时格式错误: %s", s)
		}
		m, err := strconv.Atoi(parts[1])
		if err != nil || m < 0 || m > 59 {
			return nil, fmt.Errorf("分钟格式错误: %s", s)
		}
		out = append(out, dailySlot{Hour: h, Minute: m})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("COLLECT_TIMES 不能为空")
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hour == out[j].Hour {
			return out[i].Minute < out[j].Minute
		}
		return out[i].Hour < out[j].Hour
	})
	return out, nil
}

func nextRunTime(now time.Time, slots []dailySlot, loc *time.Location) time.Time {
	y, mo, d := now.Date()
	for _, s := range slots {
		candidate := time.Date(y, mo, d, s.Hour, s.Minute, 0, 0, loc)
		if candidate.After(now) {
			return candidate
		}
	}
	first := slots[0]
	return time.Date(y, mo, d+1, first.Hour, first.Minute, 0, 0, loc)
}

func formatDailySlots(slots []dailySlot) string {
	if len(slots) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(slots))
	for _, s := range slots {
		parts = append(parts, fmt.Sprintf("%02d:%02d", s.Hour, s.Minute))
	}
	return strings.Join(parts, ",")
}

func collectFailedRetryInterval() time.Duration {
	const defaultMinutes = 15
	v := strings.TrimSpace(envOrDefault("COLLECT_FAILED_RETRY_MINUTES", strconv.Itoa(defaultMinutes)))
	if v == "" {
		return defaultMinutes * time.Minute
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultMinutes * time.Minute
	}
	if n <= 0 {
		return 0
	}
	if n > 180 {
		n = 180
	}
	return time.Duration(n) * time.Minute
}

// nextIntervalBoundary 将当前时间对齐到下一个固定周期边界，避免 ticker 漂移。
func nextIntervalBoundary(now time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		return now
	}
	step := int64(interval / time.Second)
	if step <= 0 {
		step = 1
	}
	sec := now.Unix()
	nextSec := ((sec / step) + 1) * step
	return time.Unix(nextSec, 0).In(now.Location())
}

func isTruthyEnv(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func debugLogf(format string, args ...any) {
	if !verboseLogEnabled.Load() {
		return
	}
	log.Printf(format, args...)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func applySQLiteRuntimePragmas(db *sql.DB) error {
	// 避免并发初始化时立即报 SQLITE_BUSY。
	_, err := db.Exec(`PRAGMA busy_timeout=10000;`)
	return err
}

func withSQLiteBusyRetry(name string, fn func() error) error {
	const maxAttempts = 12
	var lastErr error
	for i := 1; i <= maxAttempts; i++ {
		err := fn()
		if err == nil {
			if i > 1 {
				log.Printf("[db-init] %s 重试成功: attempt=%d", name, i)
			}
			return nil
		}
		lastErr = err
		if !isSQLiteBusyErr(err) {
			return err
		}
		wait := time.Duration(i*300) * time.Millisecond
		log.Printf("[db-init] %s 遇到数据库锁，准备重试: attempt=%d/%d wait=%s err=%v", name, i, maxAttempts, wait.String(), err)
		time.Sleep(wait)
	}
	return fmt.Errorf("%s 多次重试后仍失败: %w", name, lastErr)
}

func isSQLiteBusyErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "database is locked") ||
		strings.Contains(s, "sqlite_busy") ||
		strings.Contains(s, "database table is locked")
}

func normalizeBasePath(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" || s == "/" {
		return ""
	}
	if !strings.HasPrefix(s, "/") {
		s = "/" + s
	}
	s = strings.TrimRight(s, "/")
	if s == "/" {
		return ""
	}
	return s
}

func migrate(db *sql.DB) error {
	stmts := []string{
		`PRAGMA journal_mode=WAL;`,
		`CREATE TABLE IF NOT EXISTS products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			store_name TEXT NOT NULL DEFAULT 'Galaxy 銀河攝影器材',
			url TEXT NOT NULL,
			price_regex TEXT NOT NULL,
			update_regex TEXT NOT NULL DEFAULT '',
			currency TEXT NOT NULL DEFAULT 'CNY',
			active INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours'))
		);`,
		`CREATE TABLE IF NOT EXISTS price_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			product_id INTEGER NOT NULL,
			record_date TEXT NOT NULL,
			price REAL NOT NULL,
			store_name TEXT NOT NULL DEFAULT '',
			merchant_update TEXT NOT NULL DEFAULT '',
			raw_text TEXT,
			source_url TEXT,
			created_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
			UNIQUE(product_id, record_date),
			FOREIGN KEY(product_id) REFERENCES products(id)
		);`,
		`CREATE TABLE IF NOT EXISTS exchange_rates (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			base_currency TEXT NOT NULL,
			quote_currency TEXT NOT NULL,
			rate_date TEXT NOT NULL,
			rate REAL NOT NULL,
			source TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
			UNIQUE(base_currency, quote_currency, rate_date)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_price_records_pid_date ON price_records(product_id, record_date);`,
		`CREATE INDEX IF NOT EXISTS idx_exchange_rates_pair_date ON exchange_rates(base_currency, quote_currency, rate_date);`,
		`CREATE TABLE IF NOT EXISTS fetch_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			product_id INTEGER NOT NULL,
			success INTEGER NOT NULL,
			error_text TEXT NOT NULL DEFAULT '',
			fetched_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
			FOREIGN KEY(product_id) REFERENCES products(id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_fetch_logs_pid_time ON fetch_logs(product_id, fetched_at DESC, id DESC);`,
		`CREATE TABLE IF NOT EXISTS store_options (
			name TEXT PRIMARY KEY,
			created_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours'))
		);`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	if err := ensureColumnExists(db, "products", "store_name", "TEXT NOT NULL DEFAULT 'Galaxy 銀河攝影器材'"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, "products", "update_regex", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, "products", "last_fetch_ok", "INTEGER"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, "products", "last_fetch_at", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, "products", "last_fetch_error", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, "price_records", "store_name", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, "price_records", "merchant_update", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, "price_records", "updated_at", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE price_records
		SET updated_at = CASE
			WHEN IFNULL(TRIM(created_at), '') = '' THEN datetime('now', '+8 hours')
			ELSE created_at
		END
		WHERE IFNULL(TRIM(updated_at), '') = ''`); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE price_records SET merchant_update = record_date WHERE IFNULL(TRIM(merchant_update), '') = ''`); err != nil {
		return err
	}
	if err := ensureTimezoneMigration(db); err != nil {
		return err
	}
	if err := ensureMerchantUpdateNormalizationMigration(db); err != nil {
		return err
	}
	if err := ensureStoreOptionsInit(db); err != nil {
		return err
	}
	return nil
}

func ensureStoreOptionsInit(db *sql.DB) error {
	var cnt int
	if err := db.QueryRow(`SELECT COUNT(*) FROM store_options`).Scan(&cnt); err != nil {
		return err
	}
	// 只在空表时初始化，避免用户删除后的店铺选项在重启时被自动加回。
	if cnt > 0 {
		return nil
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO store_options(name) VALUES
		('Galaxy 銀河攝影器材'),
		('順星數碼')`); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO store_options(name)
		SELECT DISTINCT TRIM(store_name)
		FROM products
		WHERE IFNULL(TRIM(store_name), '') <> ''`); err != nil {
		return err
	}
	return nil
}

func ensureTimezoneMigration(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS migration_meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours'))
	)`); err != nil {
		return err
	}
	var v string
	err := db.QueryRow(`SELECT value FROM migration_meta WHERE key='tz_fix_v1'`).Scan(&v)
	if err == nil && v == "done" {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := db.Exec(`UPDATE products SET created_at=datetime(created_at, '+8 hours'), updated_at=datetime(updated_at, '+8 hours')`); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE price_records SET created_at=datetime(created_at, '+8 hours')`); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE price_records SET updated_at=datetime(updated_at, '+8 hours')`); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE exchange_rates SET created_at=datetime(created_at, '+8 hours'), updated_at=datetime(updated_at, '+8 hours')
		WHERE EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='exchange_rates')`); err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO migration_meta(key, value) VALUES('tz_fix_v1', 'done')
	ON CONFLICT(key) DO UPDATE SET value='done'`)
	return err
}

func ensureMerchantUpdateNormalizationMigration(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS migration_meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours'))
	)`); err != nil {
		return err
	}

	var v string
	err := db.QueryRow(`SELECT value FROM migration_meta WHERE key='merchant_update_norm_v1'`).Scan(&v)
	if err == nil && v == "done" {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	rows, err := db.Query(`SELECT id, merchant_update FROM price_records WHERE IFNULL(TRIM(merchant_update), '') <> ''`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE price_records SET merchant_update = ? WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	updated := 0
	for rows.Next() {
		var id int64
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return err
		}
		normalized := formatMerchantUpdate(raw)
		if normalized == "" || normalized == raw {
			continue
		}
		if _, err := stmt.Exec(normalized, id); err != nil {
			return err
		}
		updated++
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if _, err := tx.Exec(`INSERT INTO migration_meta(key, value) VALUES('merchant_update_norm_v1', 'done')
		ON CONFLICT(key) DO UPDATE SET value='done'`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Printf("[migrate] 商户更新时间文案规范化完成: updated=%d", updated)
	return nil
}

func ensureColumnExists(db *sql.DB, tableName, columnName, columnDDL string) error {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(name, columnName) {
			return nil
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	_, err = db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, tableName, columnName, columnDDL))
	return err
}

func syncProductsFromConfig(db *sql.DB, cfgPath string) error {
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("缺少配置文件: %s（可复制 config/products.json.example）", cfgPath)
		}
		return err
	}

	var list []ProductConfig
	if err := json.Unmarshal(b, &list); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt := `INSERT INTO products(name, store_name, url, price_regex, update_regex, currency, active, created_at, updated_at)
	VALUES(?, ?, ?, ?, ?, ?, ?, datetime('now', '+8 hours'), datetime('now', '+8 hours'))
	ON CONFLICT(name) DO UPDATE SET
	store_name=excluded.store_name,
	url=excluded.url,
	price_regex=excluded.price_regex,
	update_regex=excluded.update_regex,
	currency=excluded.currency,
	active=excluded.active,
	updated_at=datetime('now', '+8 hours')`

	for _, p := range list {
		name := strings.TrimSpace(p.Name)
		storeName := strings.TrimSpace(p.StoreName)
		if storeName == "" {
			storeName = "Galaxy 銀河攝影器材"
		}
		url := strings.TrimSpace(p.URL)
		re := strings.TrimSpace(p.PriceRegex)
		updateRe := strings.TrimSpace(p.UpdateRegex)
		currency := strings.ToUpper(strings.TrimSpace(p.Currency))
		if currency == "" {
			currency = "CNY"
		}
		if name == "" || url == "" || re == "" {
			return fmt.Errorf("配置项不完整: %+v", p)
		}

		active := 0
		if p.Active {
			active = 1
		}
		if _, err := tx.Exec(stmt, name, storeName, url, re, updateRe, currency, active); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func syncProductsFromConfigIfEmpty(db *sql.DB, cfgPath string) error {
	var cnt int
	if err := db.QueryRow(`SELECT COUNT(1) FROM products`).Scan(&cnt); err != nil {
		return err
	}
	if cnt > 0 {
		log.Printf("[sync-products-config] products 表已有数据（count=%d），跳过从配置文件导入", cnt)
		return nil
	}
	log.Printf("[sync-products-config] products 表为空，开始从配置文件导入: %s", cfgPath)
	return syncProductsFromConfig(db, cfgPath)
}

type collectFailureDetail struct {
	ProductName string
	StoreName   string
	Err         string
}

type pendingCollectFailure struct {
	product  Product
	firstErr error
}

// collectRunState 聚合一轮采集的统计与失败样本，避免在主流程中散落大量计数和写库细节。
type collectRunState struct {
	runLabel        string
	successCount    int
	outOfStockCount int
	failCount       int
	blockedByWAF    bool
	failures        []collectFailureDetail
}

func newCollectRunState(runLabel string) *collectRunState {
	return &collectRunState{
		runLabel: runLabel,
		failures: make([]collectFailureDetail, 0, 8),
	}
}

func (s *collectRunState) markOutOfStock(db *sql.DB, p Product, err error) {
	debugLogf("[collect][%s][%s] 店铺报价不可用，标记无货: %v", s.runLabel, p.Name, err)
	s.outOfStockCount++
	_ = insertFetchLog(db, p.ID, false, err.Error())
	_ = updateLastFetchStatus(db, p.ID, false, err.Error())
	runtimeMon.markItemResult("out_of_stock", err.Error())
}

func (s *collectRunState) markFailure(db *sql.DB, p Product, errText string) {
	errText = strings.TrimSpace(errText)
	s.failCount++
	s.failures = append(s.failures, collectFailureDetail{
		ProductName: p.Name,
		StoreName:   p.StoreName,
		Err:         errText,
	})
	_ = insertFetchLog(db, p.ID, false, errText)
	_ = updateLastFetchStatus(db, p.ID, false, errText)
	runtimeMon.markItemResult("failed", errText)
}

func (s *collectRunState) markSuccess(db *sql.DB, p Product, got FetchResult, phase string) {
	if phase == "补抓" {
		debugLogf("[collect][%s][%s][%s] 单店失败补抓成功: price=%.2f raw=%s merchant_update=%s", s.runLabel, p.Name, p.StoreName, got.Price, got.RawPrice, got.MerchantUpdate)
	} else {
		debugLogf("[collect][%s][%s][%s] 采集成功: price=%.2f raw=%s merchant_update=%s", s.runLabel, p.Name, p.StoreName, got.Price, got.RawPrice, got.MerchantUpdate)
	}
	_ = insertFetchLog(db, p.ID, true, "")
	_ = updateLastFetchStatus(db, p.ID, true, "")
	s.successCount++
	runtimeMon.markItemResult("success", "")
}

func (s *collectRunState) inspectFetchErr(err error) {
	lowered := strings.ToLower(err.Error())
	if strings.Contains(lowered, "just a moment") || strings.Contains(lowered, "cloudflare") {
		s.blockedByWAF = true
	}
}

func collectToday(db *sql.DB) error {
	if !collectRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("采集任务正在执行，跳过重复触发")
	}
	defer collectRunning.Store(false)

	if shouldCheckFlareSolverrBeforeCollect() {
		log.Printf("[collect] 采集前检查 FlareSolverr 连通性: %s", flareSolverrURL())
		if err := ensureFlareSolverrReachableWithRetry(); err != nil {
			log.Printf("[collect] 采集前检查失败，已跳过本轮采集: %v", err)
			maybeNotifyFlareSolverrConnectivityAlert("collect 采集前检查", err)
			return fmt.Errorf("采集前检查 0.102 服务不通，已跳过采集: %w", err)
		}
	}

	products, err := getActiveProducts(db)
	if err != nil {
		return err
	}
	return runCollectForProducts(db, products, "collect")
}

func collectFailedProducts(db *sql.DB) error {
	if !collectRunning.CompareAndSwap(false, true) {
		log.Printf("[retry-collect] 已有采集任务在执行，跳过本次失败补采")
		return nil
	}
	defer collectRunning.Store(false)

	products, err := getFailedProductsForRetry(db)
	if err != nil {
		return err
	}
	if len(products) == 0 {
		log.Printf("[retry-collect] 当前无失败商品，跳过补采")
		return nil
	}
	log.Printf("[retry-collect] 检测到失败商品数量=%d，开始补采", len(products))
	return runCollectForProducts(db, products, "retry")
}

// runCollectForProducts 统一处理常规采集与失败补采，保证两条路径逻辑一致。
func runCollectForProducts(db *sql.DB, products []Product, runLabel string) error {
	if len(products) == 0 {
		return fmt.Errorf("没有可采集的 active 商品")
	}

	today := chinaNow().Format("2006-01-02")
	log.Printf("[collect][%s] 本轮采集开始，日期=%s，商品数量=%d", runLabel, today, len(products))
	urlGroups := groupProductsByURL(products)
	log.Printf("[collect][%s] 本轮按URL分组后需访问页面数=%d", runLabel, len(urlGroups))
	runtimeMon.beginCollectRun(runLabel, len(urlGroups), len(products))
	defer runtimeMon.clearCurrentTask()
	state := newCollectRunState(runLabel)
	for i, g := range urlGroups {
		runtimeMon.setCollectGroup(i+1, len(urlGroups), len(g.Items), g.URL)
		if i > 0 {
			delay := collectItemDelay()
			if delay > 0 {
				debugLogf("[collect][%s] 商品间隔等待: %s", runLabel, delay.String())
				time.Sleep(delay)
			}
		}
		sample := g.Items[0]
		debugLogf("[collect][%s][%d/%d][url=%s] 开始采集，分组商品数=%d", runLabel, i+1, len(urlGroups), g.URL, len(g.Items))
		runtimeMon.setCurrentTask(sample, "抓取页面")
		content, err := fetchProductPageContent(sample)
		if err != nil {
			log.Printf("[collect][%s][url=%s] 页面抓取失败: %v", runLabel, g.URL, err)
			for _, p := range g.Items {
				runtimeMon.setCurrentTask(p, "处理抓取失败")
				if isOutOfStockError(err) {
					state.markOutOfStock(db, p, err)
					continue
				}
				state.markFailure(db, p, err.Error())
			}
			state.inspectFetchErr(err)
			runtimeMon.finishCollectGroup()
			continue
		}

		groupSuccess := 0
		pendingFailures := make([]pendingCollectFailure, 0, 2)
		for _, p := range g.Items {
			runtimeMon.setCurrentTask(p, "解析页面")
			got, perr := parseFetchResultFromContent(content, p)
			if perr != nil {
				if isOutOfStockError(perr) {
					state.markOutOfStock(db, p, perr)
					continue
				}
				log.Printf("[collect][%s][%s] 页面已抓取但解析失败: %v", runLabel, p.Name, perr)
				pendingFailures = append(pendingFailures, pendingCollectFailure{
					product:  p,
					firstErr: perr,
				})
				continue
			}
			debugLogf("[collect][%s][%s] 抓取完成，准备写入数据库", runLabel, p.Name)
			runtimeMon.setCurrentTask(p, "写入数据库")
			err = upsertDailyPrice(db, p.ID, today, p.StoreName, got.Price, got.RawPrice, got.MerchantUpdate, p.URL)
			if err != nil {
				log.Printf("[collect][%s][%s] 写入失败: %v", runLabel, p.Name, err)
				state.markFailure(db, p, err.Error())
				continue
			}
			debugLogf("[collect][%s][%s] 写入成功，记录抓取日志与状态", runLabel, p.Name)
			state.markSuccess(db, p, got, "首抓")
			groupSuccess++
		}

		// 同一URL下若出现“部分店铺成功 + 部分店铺失败”，对失败店铺做一次延迟补抓。
		if len(pendingFailures) > 0 && groupSuccess > 0 {
			retryDelay := collectPartialRetryDelay()
			log.Printf("[collect][%s][url=%s] 检测到单店失败，%s 后补抓失败店铺，数量=%d", runLabel, g.URL, retryDelay.String(), len(pendingFailures))
			runtimeMon.setPendingRetry(len(pendingFailures))
			time.Sleep(retryDelay)

			runtimeMon.setCurrentTask(sample, "补抓页面")
			retryContent, retryErr := fetchProductPageContent(sample)
			if retryErr != nil {
				log.Printf("[collect][%s][url=%s] 单店失败补抓页面失败: %v", runLabel, g.URL, retryErr)
				for _, pf := range pendingFailures {
					finalErr := fmt.Sprintf("首次解析失败: %v; 补抓失败: %v", pf.firstErr, retryErr)
					state.markFailure(db, pf.product, finalErr)
				}
			} else {
				for _, pf := range pendingFailures {
					runtimeMon.setCurrentTask(pf.product, "补抓解析")
					got, perr := parseFetchResultFromContent(retryContent, pf.product)
					if perr != nil {
						finalErr := fmt.Sprintf("首次解析失败: %v; 补抓解析失败: %v", pf.firstErr, perr)
						if isOutOfStockError(perr) {
							debugLogf("[collect][%s][%s] 补抓后判定无货: %v", runLabel, pf.product.Name, perr)
							state.markOutOfStock(db, pf.product, perr)
							continue
						}
						state.markFailure(db, pf.product, finalErr)
						continue
					}
					runtimeMon.setCurrentTask(pf.product, "补抓写入")
					if werr := upsertDailyPrice(db, pf.product.ID, today, pf.product.StoreName, got.Price, got.RawPrice, got.MerchantUpdate, pf.product.URL); werr != nil {
						finalErr := fmt.Sprintf("首次解析失败: %v; 补抓写入失败: %v", pf.firstErr, werr)
						state.markFailure(db, pf.product, finalErr)
						continue
					}
					state.markSuccess(db, pf.product, got, "补抓")
				}
			}
			runtimeMon.setPendingRetry(0)
		} else if len(pendingFailures) > 0 {
			for _, pf := range pendingFailures {
				state.markFailure(db, pf.product, pf.firstErr.Error())
			}
		}
		runtimeMon.finishCollectGroup()
	}
	log.Printf("[collect][%s] 本轮采集结束: success=%d out_of_stock=%d failed=%d", runLabel, state.successCount, state.outOfStockCount, state.failCount)
	if state.successCount == 0 && state.blockedByWAF {
		err := fmt.Errorf("当前被 Cloudflare 挑战拦截（Just a moment），请启用 FlareSolverr 或手动验证会话")
		runtimeMon.finishCollectRun("failed", err)
		return err
	}
	if state.successCount == 0 && state.failCount > 0 {
		err := fmt.Errorf("本轮采集无成功记录")
		runtimeMon.finishCollectRun("failed", err)
		return err
	}
	runtimeMon.finishCollectRun("success", nil)
	return nil
}

func collectPartialRetryDelay() time.Duration {
	const defaultSeconds = 3
	v := strings.TrimSpace(envOrDefault("COLLECT_PARTIAL_RETRY_DELAY_SECONDS", strconv.Itoa(defaultSeconds)))
	if v == "" {
		return defaultSeconds * time.Second
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultSeconds * time.Second
	}
	if n < 1 {
		n = 1
	}
	if n > 60 {
		n = 60
	}
	return time.Duration(n) * time.Second
}

type urlProductGroup struct {
	URL   string
	Items []Product
}

func groupProductsByURL(products []Product) []urlProductGroup {
	out := make([]urlProductGroup, 0, len(products))
	idxByURL := make(map[string]int, len(products))
	for _, p := range products {
		key := normalizeFetchURL(p.URL)
		if key == "" {
			key = strings.TrimSpace(p.URL)
		}
		if idx, ok := idxByURL[key]; ok {
			out[idx].Items = append(out[idx].Items, p)
			continue
		}
		idxByURL[key] = len(out)
		out = append(out, urlProductGroup{
			URL:   key,
			Items: []Product{p},
		})
	}
	return out
}

func shouldCheckFlareSolverrBeforeCollect() bool {
	if !useFlareSolverrFallback() {
		return false
	}
	return isFlareSolverrTargetHost(flareSolverrURL(), "172.25.0.102")
}

func ensureFlareSolverrReachableWithRetry() error {
	const defaultRetryCount = 5
	const defaultRetryInterval = 30 * time.Minute

	retryCount := defaultRetryCount
	if v := strings.TrimSpace(envOrDefault("COLLECT_PREFLIGHT_RETRY_COUNT", strconv.Itoa(defaultRetryCount))); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 24 {
			retryCount = n
		}
	}

	retryInterval := defaultRetryInterval
	if v := strings.TrimSpace(envOrDefault("COLLECT_PREFLIGHT_RETRY_INTERVAL_MINUTES", "30")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 720 {
			retryInterval = time.Duration(n) * time.Minute
		}
	}

	err := preflightFlareSolverr()
	if err == nil {
		return nil
	}
	if !isFlareSolverrConnectivityError(err) {
		log.Printf("[collect] 0.102 预检未通过，但不属于网络断连，继续采集: %v", err)
		return nil
	}
	firstErr := err
	for i := 1; i <= retryCount; i++ {
		log.Printf("[collect] 0.102 连通性检查失败，等待后重试: %d/%d interval=%s err=%v", i, retryCount, retryInterval.String(), err)
		time.Sleep(retryInterval)
		err = preflightFlareSolverr()
		if err == nil {
			log.Printf("[collect] 0.102 连通性恢复，继续采集")
			return nil
		}
		if !isFlareSolverrConnectivityError(err) {
			log.Printf("[collect] 0.102 重试后可连通，继续采集（当前为非网络错误）: %v", err)
			return nil
		}
	}
	maybeNotifyFlareSolverrConnectivityAlert(fmt.Sprintf("collect 采集前检查重试已达上限 %d 次", retryCount), err)
	return fmt.Errorf("0.102 连通性检查失败（首次错误: %v）", firstErr)
}

func getActiveProducts(db *sql.DB) ([]Product, error) {
	rows, err := db.Query(`SELECT id, name, store_name, url, price_regex, update_regex, currency, active FROM products WHERE active = 1 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Product
	for rows.Next() {
		var p Product
		var active int
		if err := rows.Scan(&p.ID, &p.Name, &p.StoreName, &p.URL, &p.PriceRegex, &p.UpdateRegex, &p.Currency, &active); err != nil {
			return nil, err
		}
		p.Active = active == 1
		out = append(out, p)
	}
	return out, rows.Err()
}

func getFailedProductsForRetry(db *sql.DB) ([]Product, error) {
	rows, err := db.Query(`SELECT id, name, store_name, url, price_regex, update_regex, currency, active
		FROM products
		WHERE active = 1
		  AND COALESCE(last_fetch_ok, 1) = 0
		  AND LOWER(COALESCE(last_fetch_error, '')) NOT LIKE '无货%'
		  AND LOWER(COALESCE(last_fetch_error, '')) NOT LIKE '無貨%'
		ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Product
	for rows.Next() {
		var p Product
		var active int
		if err := rows.Scan(&p.ID, &p.Name, &p.StoreName, &p.URL, &p.PriceRegex, &p.UpdateRegex, &p.Currency, &active); err != nil {
			return nil, err
		}
		p.Active = active == 1
		out = append(out, p)
	}
	return out, rows.Err()
}

func fetchPrice(p Product) (FetchResult, error) {
	content, err := fetchProductPageContent(p)
	if err != nil {
		return FetchResult{}, err
	}
	return parseFetchResultFromContent(content, p)
}

func fetchProductPageContent(p Product) (string, error) {
	debugLogf("[fetch][%s] 开始抓取链路", p.Name)
	var fsErr error
	if useFlareSolverrFallback() && shouldUseFlareSolverrNow() {
		debugLogf("[fetch][%s] 步骤1: 尝试 FlareSolverr", p.Name)
		content, err := fetchPriceViaFlareSolverr(p)
		if err == nil {
			debugLogf("[fetch][%s] FlareSolverr 抓取成功", p.Name)
			return content, nil
		}
		fsErr = err
		log.Printf("[fetch][%s] FlareSolverr 失败，回退 HTTP: %v", p.Name, err)
	} else if useFlareSolverrFallback() {
		debugLogf("[fetch][%s] 步骤1: 跳过 FlareSolverr（熔断窗口中）", p.Name)
	}

	debugLogf("[fetch][%s] 步骤2: 尝试 HTTP 抓取", p.Name)
	client := newBrowserLikeClient()
	uaList := []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}

	var lastErr error
	for i, ua := range uaList {
		debugLogf("[fetch][%s] HTTP 尝试 #%d", p.Name, i+1)
		content, err := fetchPriceOnce(client, p, ua)
		if err == nil {
			debugLogf("[fetch][%s] HTTP 抓取成功 (尝试 #%d)", p.Name, i+1)
			return content, nil
		}
		lastErr = err
		debugLogf("[fetch][%s] HTTP 失败 (尝试 #%d): %v", p.Name, i+1, err)

		var hs *httpStatusError
		if errors.As(err, &hs) && hs.Code == http.StatusForbidden {
			time.Sleep(time.Duration(i+1) * 1200 * time.Millisecond)
			continue
		}
		if errors.As(err, &hs) && hs.Code == http.StatusTooManyRequests {
			time.Sleep(time.Duration(i+2) * 1500 * time.Millisecond)
			continue
		}
		break
	}

	// 典型故障链路：FlareSolverr 短暂抖动(EOF/超时) + HTTP 403 挑战。
	// 这里做一次“强制重建会话后再试 FlareSolverr”，避免瞬时故障导致整轮失败。
	var hs *httpStatusError
	if fsErr != nil && errors.As(lastErr, &hs) && hs.Code == http.StatusForbidden && isFlareSolverrConnectivityError(fsErr) {
		debugLogf("[fetch][%s] 检测到 FlareSolverr 抖动 + HTTP 403，执行强制重建会话后重试", p.Name)
		fsURL := flareSolverrURL()
		sessionName := flareSolverrSessionName()
		invalidateFlareSolverrSessionCache(fsURL, sessionName)
		_ = destroyFlareSolverrSession(fsURL, sessionName)
		time.Sleep(2 * time.Second)
		content, retryErr := fetchPriceViaFlareSolverr(p)
		if retryErr == nil {
			debugLogf("[fetch][%s] 强制重试 FlareSolverr 成功", p.Name)
			return content, nil
		}
		debugLogf("[fetch][%s] 强制重试 FlareSolverr 失败: %v", p.Name, retryErr)
		fsErr = fmt.Errorf("%v; forced_retry=%v", fsErr, retryErr)
	}
	return "", fmt.Errorf("抓取失败: flaresolverrErr=%v httpErr=%w", fsErr, lastErr)
}

func parseFetchResultFromContent(content string, p Product) (FetchResult, error) {
	raw, err := extractByRegex(content, p.PriceRegex)
	if err != nil {
		if hint := detectOutOfStockHint(content); hint != "" {
			return FetchResult{}, &outOfStockError{Detail: hint}
		}
		return FetchResult{}, err
	}
	price, err := parsePrice(raw)
	if err != nil {
		return FetchResult{}, fmt.Errorf("解析价格失败: %w", err)
	}
	merchantUpdate := extractMerchantUpdate(content, p)
	if merchantUpdate == "" {
		merchantUpdate = chinaNow().Format("2006-01-02")
	}
	return FetchResult{
		Price:          price,
		RawPrice:       raw,
		MerchantUpdate: merchantUpdate,
	}, nil
}

func useFlareSolverrFallback() bool {
	v := strings.ToLower(strings.TrimSpace(envOrDefault("ENABLE_FLARESOLVERR_FALLBACK", "true")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func flareSolverrSessionName() string {
	s := strings.TrimSpace(os.Getenv("FLARESOLVERR_SESSION"))
	if s != "" {
		return s
	}
	// 默认按实例隔离会话，避免多实例互相 destroy/create 同一个 session。
	base := "pricemonitor"
	if host, err := os.Hostname(); err == nil && strings.TrimSpace(host) != "" {
		re := regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
		host = re.ReplaceAllString(strings.TrimSpace(host), "-")
		host = strings.Trim(host, "-")
		if host != "" {
			return base + "-" + host
		}
	}
	return base
}

func flareSolverrURL() string {
	fsURL := strings.TrimSpace(os.Getenv("FLARESOLVERR_URL"))
	if fsURL == "" {
		return strings.TrimSpace(defaultFlareSolverrURL)
	}
	return fsURL
}

func preflightFlareSolverr() error {
	fsURL := flareSolverrURL()
	timeout := flareSolverrPreflightTimeout()
	attempts := flareSolverrPreflightAttempts()
	interval := flareSolverrPreflightRetryInterval()
	var lastErr error

	for i := 1; i <= attempts; i++ {
		log.Printf("[flaresolverr] 预检开始: %s (attempt=%d/%d timeout=%s)", fsURL, i, attempts, timeout.String())
		_, err := callFlareSolverr(fsURL, map[string]any{"cmd": "sessions.list"}, timeout)
		if err == nil {
			if _, serr := ensureFlareSolverrSession(fsURL, flareSolverrSessionName()); serr != nil {
				if isFlareSolverrConnectivityError(serr) {
					lastErr = serr
				} else {
					log.Printf("[flaresolverr] 预检通过（会话检查失败但可连通）: %v", serr)
					return nil
				}
			} else {
				log.Printf("[flaresolverr] 预检通过，会话=%s", flareSolverrSessionName())
				return nil
			}
		} else {
			lastErr = err
			if !isFlareSolverrConnectivityError(err) {
				log.Printf("[flaresolverr] 预检返回非网络错误，视为可连通: %v", err)
				return nil
			}
		}
		if i < attempts {
			time.Sleep(interval)
		}
	}
	return fmt.Errorf("FlareSolverr 预检失败: %w", lastErr)
}

func flareSolverrPreflightTimeout() time.Duration {
	sec := 30
	if v := strings.TrimSpace(envOrDefault("FLARESOLVERR_PREFLIGHT_TIMEOUT_SECONDS", "30")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 5 && n <= 180 {
			sec = n
		}
	}
	return time.Duration(sec) * time.Second
}

func flareSolverrPreflightAttempts() int {
	attempts := 3
	if v := strings.TrimSpace(envOrDefault("FLARESOLVERR_PREFLIGHT_ATTEMPTS", "3")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 10 {
			attempts = n
		}
	}
	return attempts
}

func flareSolverrPreflightRetryInterval() time.Duration {
	sec := 5
	if v := strings.TrimSpace(envOrDefault("FLARESOLVERR_PREFLIGHT_RETRY_INTERVAL_SECONDS", "5")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 60 {
			sec = n
		}
	}
	return time.Duration(sec) * time.Second
}

func maybeNotifyFlareSolverrConnectivityAlert(scene string, err error) {
	fsURL := flareSolverrURL()
	if !isFlareSolverrTargetHost(fsURL, "172.25.0.102") {
		return
	}
	if !isFlareSolverrConnectivityError(err) {
		return
	}
	if !shouldSendWeComAlert("flaresolverr-connectivity-172.25.0.102", 10*time.Minute) {
		log.Printf("[alert] FlareSolverr连接告警抑制（冷却中）: scene=%s", scene)
		return
	}
	if nerr := sendFlareSolverrConnectivityAlertToWeCom(scene, fsURL, err); nerr != nil {
		log.Printf("[alert] FlareSolverr连接告警发送失败: %v", nerr)
		return
	}
	log.Printf("[alert] FlareSolverr连接告警已发送: scene=%s", scene)
}

func isFlareSolverrTargetHost(fsURL, targetHost string) bool {
	if strings.TrimSpace(fsURL) == "" || strings.TrimSpace(targetHost) == "" {
		return false
	}
	u, err := url.Parse(fsURL)
	if err == nil && strings.TrimSpace(u.Hostname()) != "" {
		return strings.EqualFold(u.Hostname(), targetHost)
	}
	return strings.Contains(strings.ToLower(fsURL), strings.ToLower(targetHost))
}

func isFlareSolverrConnectivityError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	nonConnectivitySigns := []string{
		"http 状态异常",
		"响应解析失败",
		"flaresolverr 失败: status=",
		"flaresolverr 失败: ",
	}
	for _, sign := range nonConnectivitySigns {
		if strings.Contains(s, sign) {
			return false
		}
	}
	signatures := []string{
		"dial tcp",
		"connection refused",
		"i/o timeout",
		"context deadline exceeded",
		"client.timeout exceeded",
		"no such host",
		"no route to host",
		"network is unreachable",
		"host is down",
	}
	for _, sign := range signatures {
		if strings.Contains(s, sign) {
			return true
		}
	}
	return false
}

func shouldSendWeComAlert(key string, cooldown time.Duration) bool {
	if cooldown <= 0 {
		return true
	}
	now := chinaNow()
	weComAlertState.mu.Lock()
	defer weComAlertState.mu.Unlock()
	if weComAlertState.last == nil {
		weComAlertState.last = make(map[string]time.Time)
	}
	last, ok := weComAlertState.last[key]
	if ok && now.Sub(last) < cooldown {
		return false
	}
	weComAlertState.last[key] = now
	return true
}

func newBrowserLikeClient() *http.Client {
	jar, _ := cookiejar.New(nil)
	return &http.Client{
		Timeout: 25 * time.Second,
		Jar:     jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("重定向过多")
			}
			return nil
		},
	}
}

func fetchPriceOnce(client *http.Client, p Product, ua string) (string, error) {
	debugLogf("[http][%s] 预热首页", p.Name)
	_ = warmUpHome(client, p.URL, ua)
	debugLogf("[http][%s] 请求商品页", p.Name)
	req, err := http.NewRequest(http.MethodGet, p.URL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("sec-ch-ua", "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"")
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", "\"macOS\"")
	req.Header.Set("Referer", "https://www.price.com.hk/")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snip := strings.TrimSpace(string(body))
		if len(snip) > 200 {
			snip = snip[:200]
		}
		return "", &httpStatusError{Code: resp.StatusCode, Body: snip}
	}
	content := string(body)
	debugLogf("[http][%s] 页面响应成功", p.Name)
	return content, nil
}

func warmUpHome(client *http.Client, pageURL, ua string) error {
	u, err := url.Parse(pageURL)
	if err != nil {
		return err
	}
	home := u.Scheme + "://" + u.Host + "/"
	req, err := http.NewRequest(http.MethodGet, home, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Referer", home)
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512*1024))
	return nil
}

func collectItemDelay() time.Duration {
	ms := 2500
	if v := strings.TrimSpace(envOrDefault("COLLECT_ITEM_DELAY_MS", "2500")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 10000 {
			ms = n
		}
	}
	return time.Duration(ms) * time.Millisecond
}

func looksLikeCloudflareChallenge(html string) bool {
	s := strings.ToLower(html)
	return strings.Contains(s, "cf-challenge") ||
		strings.Contains(s, "cloudflare") && strings.Contains(s, "checking your browser") ||
		strings.Contains(s, "cf-mitigated") ||
		strings.Contains(s, "just a moment")
}

func fetchPriceViaFlareSolverr(p Product) (string, error) {
	fsURL := flareSolverrURL()
	sessionName := flareSolverrSessionName()
	debugLogf("[flaresolverr][%s] 开始抓取，url=%s session=%s", p.Name, fsURL, sessionName)
	if _, err := ensureFlareSolverrSessionCached(fsURL, sessionName); err != nil {
		return "", err
	}

	var out flareSolverrResp
	var lastErr error
	httpTimeout := flareSolverrRequestHTTPTimeout()
	maxTimeoutMS := flareSolverrMaxTimeoutMS()
	retries := flareSolverrMaxRetries()
	for i := 0; i < retries; i++ {
		debugLogf("[flaresolverr][%s] 请求尝试 #%d", p.Name, i+1)
		if i > 0 || shouldWarmupFlareSolverr() {
			_ = warmupFlareSolverrHome(fsURL, sessionName, maxTimeoutMS, httpTimeout)
		}
		payload := newFlareSolverrRequestGetPayload(p.URL, sessionName, maxTimeoutMS)
		resp, err := callFlareSolverr(fsURL, payload, httpTimeout)
		if err != nil {
			debugLogf("[flaresolverr][%s] 尝试 #%d 失败: %v", p.Name, i+1, err)
			lastErr = err
			markFlareSolverrFailure(err)
			if shouldRecycleFlareSolverrSession() {
				_ = destroyFlareSolverrSession(fsURL, sessionName)
				invalidateFlareSolverrSessionCache(fsURL, sessionName)
			}
			_, _ = ensureFlareSolverrSessionCached(fsURL, sessionName)
			if i+1 < retries {
				time.Sleep(time.Duration(i+1) * 2 * time.Second)
			}
			continue
		}
		out = resp
		if looksLikeCloudflareChallenge(out.Solution.Response) {
			debugLogf("[flaresolverr][%s] 尝试 #%d 返回挑战页，重建会话", p.Name, i+1)
			lastErr = fmt.Errorf("FlareSolverr 返回挑战页")
			markFlareSolverrFailure(lastErr)
			if shouldRecycleFlareSolverrSession() {
				_ = destroyFlareSolverrSession(fsURL, sessionName)
				invalidateFlareSolverrSessionCache(fsURL, sessionName)
			}
			_, _ = ensureFlareSolverrSessionCached(fsURL, sessionName)
			if i+1 < retries {
				time.Sleep(time.Duration(i+1) * 2 * time.Second)
			}
			continue
		}
		lastErr = nil
		markFlareSolverrSuccess()
		debugLogf("[flaresolverr][%s] 尝试 #%d 成功", p.Name, i+1)
		break
	}
	if lastErr != nil {
		return "", fmt.Errorf("FlareSolverr 重试后仍失败: %w", lastErr)
	}
	return out.Solution.Response, nil
}

func shouldWarmupFlareSolverr() bool {
	// 默认开启，先访问首页让会话拿到更稳定的挑战态与 cookie。
	v := strings.ToLower(strings.TrimSpace(envOrDefault("FLARESOLVERR_WARMUP_HOME", "true")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func warmupFlareSolverrHome(fsURL, sessionName string, maxTimeoutMS int, httpTimeout time.Duration) error {
	homePayload := newFlareSolverrRequestGetPayload("https://www.price.com.hk/", sessionName, minInt(maxTimeoutMS, 45000))
	_, err := callFlareSolverr(fsURL, homePayload, httpTimeout)
	if err != nil {
		log.Printf("[flaresolverr] 首页预热失败(可忽略): %v", err)
		return err
	}
	log.Printf("[flaresolverr] 首页预热成功")
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func shouldRecycleFlareSolverrSession() bool {
	// 默认关闭，优先复用稳定会话，避免频繁 destroy/create 抖动。
	v := strings.ToLower(strings.TrimSpace(envOrDefault("FLARESOLVERR_RECYCLE_SESSION", "false")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func flareSolverrRequestHTTPTimeout() time.Duration {
	sec := 150
	if v := strings.TrimSpace(os.Getenv("FLARESOLVERR_HTTP_TIMEOUT_SECONDS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 30 && n <= 600 {
			sec = n
		}
	}
	return time.Duration(sec) * time.Second
}

func flareSolverrMaxTimeoutMS() int {
	ms := 120000
	if v := strings.TrimSpace(os.Getenv("FLARESOLVERR_MAX_TIMEOUT_MS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 30000 && n <= 600000 {
			ms = n
		}
	}
	return ms
}

func flareSolverrMaxRetries() int {
	retries := 2
	if v := strings.TrimSpace(os.Getenv("FLARESOLVERR_MAX_RETRIES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 5 {
			retries = n
		}
	}
	return retries
}

func shouldUseFlareSolverrNow() bool {
	flareSolverrCB.mu.Lock()
	defer flareSolverrCB.mu.Unlock()
	if flareSolverrCB.disabledTill.IsZero() {
		return true
	}
	if chinaNow().After(flareSolverrCB.disabledTill) {
		flareSolverrCB.disabledTill = time.Time{}
		flareSolverrCB.fails = 0
		return true
	}
	return false
}

func markFlareSolverrSuccess() {
	flareSolverrCB.mu.Lock()
	defer flareSolverrCB.mu.Unlock()
	flareSolverrCB.fails = 0
	flareSolverrCB.disabledTill = time.Time{}
}

func markFlareSolverrFailure(err error) {
	flareSolverrCB.mu.Lock()
	defer flareSolverrCB.mu.Unlock()
	flareSolverrCB.fails++
	threshold := 4
	if v := strings.TrimSpace(os.Getenv("FLARESOLVERR_BREAKER_FAILS")); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n >= 1 && n <= 10 {
			threshold = n
		}
	}
	minutes := 10
	if v := strings.TrimSpace(os.Getenv("FLARESOLVERR_BREAKER_MINUTES")); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n >= 1 && n <= 120 {
			minutes = n
		}
	}
	msg := strings.ToLower(err.Error())
	severe := strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "tls handshake timeout")
	if flareSolverrCB.fails >= threshold || severe {
		flareSolverrCB.disabledTill = chinaNow().Add(time.Duration(minutes) * time.Minute)
		log.Printf("[flaresolverr] 熔断触发: fails=%d disabled_until=%s", flareSolverrCB.fails, flareSolverrCB.disabledTill.Format("2006-01-02 15:04:05"))
	}
}

func newFlareSolverrRequestGetPayload(urlText, sessionName string, maxTimeoutMS int) map[string]any {
	urlText = normalizeFetchURL(urlText)
	payload := map[string]any{
		"cmd":        "request.get",
		"url":        urlText,
		"session":    sessionName,
		"maxTimeout": maxTimeoutMS,
	}
	if proxyURL := strings.TrimSpace(os.Getenv("FLARESOLVERR_PROXY_URL")); proxyURL != "" {
		payload["proxy"] = map[string]string{"url": proxyURL}
	}
	return payload
}

func normalizeFetchURL(raw string) string {
	s := normalizeProductURL(strings.TrimSpace(raw))
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	u.Fragment = ""
	if u.Host == "" {
		return s
	}
	return u.String()
}

func shouldSerializeFlareSolverrRequests() bool {
	v := strings.ToLower(strings.TrimSpace(envOrDefault("FLARESOLVERR_SERIAL", "true")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func callFlareSolverr(fsURL string, payload map[string]any, timeout time.Duration) (flareSolverrResp, error) {
	if shouldSerializeFlareSolverrRequests() {
		flareSolverrReqMu.Lock()
		defer flareSolverrReqMu.Unlock()
	}
	cmd := ""
	if v, ok := payload["cmd"].(string); ok {
		cmd = v
	}
	debugLogf("[flaresolverr] 调用接口: cmd=%s timeout=%s", cmd, timeout.String())
	b, _ := json.Marshal(payload)
	apiRetries := flareSolverrAPIRetries()
	var lastErr error
	for i := 0; i <= apiRetries; i++ {
		req, err := http.NewRequest(http.MethodPost, fsURL, bytes.NewReader(b))
		if err != nil {
			return flareSolverrResp{}, err
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("FlareSolverr 不可用(%s): %w", fsURL, err)
			if i < apiRetries && shouldRetryFlareSolverrTransportError(lastErr) {
				wait := time.Duration(i+1) * 900 * time.Millisecond
				debugLogf("[flaresolverr] 接口调用重试: cmd=%s attempt=%d/%d wait=%s err=%v", cmd, i+1, apiRetries+1, wait.String(), err)
				time.Sleep(wait)
				continue
			}
			return flareSolverrResp{}, lastErr
		}

		body, rerr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
		resp.Body.Close()
		if rerr != nil {
			lastErr = rerr
			if i < apiRetries {
				wait := time.Duration(i+1) * 900 * time.Millisecond
				debugLogf("[flaresolverr] 接口读取失败重试: cmd=%s attempt=%d/%d wait=%s err=%v", cmd, i+1, apiRetries+1, wait.String(), rerr)
				time.Sleep(wait)
				continue
			}
			return flareSolverrResp{}, rerr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("[flaresolverr] 调用失败: cmd=%s status=%d", cmd, resp.StatusCode)
			return flareSolverrResp{}, fmt.Errorf("FlareSolverr HTTP 状态异常: %d", resp.StatusCode)
		}

		var out flareSolverrResp
		if err := json.Unmarshal(body, &out); err != nil {
			return flareSolverrResp{}, fmt.Errorf("FlareSolverr 响应解析失败: %w", err)
		}
		if strings.ToLower(out.Status) != "ok" {
			if out.Message != "" {
				return flareSolverrResp{}, fmt.Errorf("FlareSolverr 失败: %s", out.Message)
			}
			return flareSolverrResp{}, fmt.Errorf("FlareSolverr 失败: status=%s", out.Status)
		}
		debugLogf("[flaresolverr] 调用成功: cmd=%s", cmd)
		return out, nil
	}
	return flareSolverrResp{}, lastErr
}

func flareSolverrAPIRetries() int {
	n := 2
	if v := strings.TrimSpace(envOrDefault("FLARESOLVERR_API_RETRIES", "2")); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 && i <= 5 {
			n = i
		}
	}
	return n
}

func shouldRetryFlareSolverrTransportError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	retrySigns := []string{
		"eof",
		"context deadline exceeded",
		"client.timeout exceeded",
		"connection reset by peer",
		"i/o timeout",
	}
	for _, sign := range retrySigns {
		if strings.Contains(s, sign) {
			return true
		}
	}
	return false
}

func ensureFlareSolverrSessionCached(fsURL, sessionName string) (string, error) {
	flareSolverrSessionCache.mu.Lock()
	if flareSolverrSessionCache.name == sessionName &&
		flareSolverrSessionCache.url == fsURL &&
		!flareSolverrSessionCache.ensured.IsZero() &&
		time.Since(flareSolverrSessionCache.ensured) < 10*time.Minute {
		flareSolverrSessionCache.mu.Unlock()
		return sessionName, nil
	}
	flareSolverrSessionCache.mu.Unlock()

	name, err := ensureFlareSolverrSession(fsURL, sessionName)
	if err != nil {
		return "", err
	}
	flareSolverrSessionCache.mu.Lock()
	flareSolverrSessionCache.name = sessionName
	flareSolverrSessionCache.url = fsURL
	flareSolverrSessionCache.ensured = chinaNow()
	flareSolverrSessionCache.mu.Unlock()
	return name, nil
}

func invalidateFlareSolverrSessionCache(fsURL, sessionName string) {
	flareSolverrSessionCache.mu.Lock()
	defer flareSolverrSessionCache.mu.Unlock()
	if flareSolverrSessionCache.name == sessionName && flareSolverrSessionCache.url == fsURL {
		flareSolverrSessionCache.ensured = time.Time{}
	}
}

func ensureFlareSolverrSession(fsURL, sessionName string) (string, error) {
	_, err := callFlareSolverr(fsURL, map[string]any{
		"cmd":     "sessions.create",
		"session": sessionName,
	}, 12*time.Second)
	if err == nil {
		return sessionName, nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return sessionName, nil
	}
	_, err2 := callFlareSolverr(fsURL, map[string]any{"cmd": "sessions.list"}, 12*time.Second)
	if err2 == nil {
		return sessionName, nil
	}
	return "", fmt.Errorf("FlareSolverr 会话不可用(%s): %v", sessionName, err)
}

func destroyFlareSolverrSession(fsURL, sessionName string) error {
	_, err := callFlareSolverr(fsURL, map[string]any{
		"cmd":     "sessions.destroy",
		"session": sessionName,
	}, 12*time.Second)
	return err
}

func parsePrice(s string) (float64, error) {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "¥", "")
	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, "元", "")
	s = strings.TrimSpace(s)
	return strconv.ParseFloat(s, 64)
}

func extractByRegex(content, regexText string) (string, error) {
	re, err := regexp.Compile(strings.TrimSpace(regexText))
	if err != nil {
		return "", fmt.Errorf("正则无效: %w", err)
	}
	m := re.FindStringSubmatch(content)
	if len(m) < 2 {
		return "", fmt.Errorf("未匹配到字段，建议调整 regex")
	}
	return strings.TrimSpace(m[1]), nil
}

func detectOutOfStockHint(content string) string {
	content = normalizeText(content)
	if content == "" {
		return ""
	}
	stockPhrases := []string{
		"无货",
		"無貨",
		"缺货",
		"缺貨",
		"售罄",
		"請先查詢",
		"请先查询",
		"請查詢",
		"请查询",
		"查詢中",
		"查询中",
		"待查詢",
		"待查询",
		"預訂",
		"预订",
		"離線",
		"离线",
	}
	for _, phrase := range stockPhrases {
		if strings.Contains(content, phrase) {
			return normalizeStockStatus(phrase)
		}
	}
	return ""
}

func isOutOfStockError(err error) bool {
	if err == nil {
		return false
	}
	var oos *outOfStockError
	if errors.As(err, &oos) {
		return true
	}
	return false
}

func extractOptionalByRegex(content, regexText string) string {
	regexText = strings.TrimSpace(regexText)
	if regexText == "" {
		return ""
	}
	re, err := regexp.Compile(regexText)
	if err != nil {
		return ""
	}
	m := re.FindStringSubmatch(content)
	if len(m) < 2 {
		return ""
	}
	return normalizeText(m[1])
}

func upsertDailyPrice(db *sql.DB, productID int64, day, storeName string, price float64, rawText, merchantUpdate, sourceURL string) error {
	_, err := db.Exec(`INSERT INTO price_records(product_id, record_date, price, store_name, merchant_update, raw_text, source_url, created_at, updated_at)
	VALUES(?, ?, ?, ?, ?, ?, ?, datetime('now', '+8 hours'), datetime('now', '+8 hours'))
	ON CONFLICT(product_id, record_date) DO UPDATE SET
	price=excluded.price,
	store_name=excluded.store_name,
	merchant_update=excluded.merchant_update,
	raw_text=excluded.raw_text,
	source_url=excluded.source_url,
	updated_at=datetime('now', '+8 hours')`, productID, day, price, storeName, merchantUpdate, rawText, sourceURL)
	return err
}

func insertFetchLog(db *sql.DB, productID int64, ok bool, errText string) error {
	success := 0
	if ok {
		success = 1
	}
	errText = strings.TrimSpace(errText)
	if len(errText) > 240 {
		errText = errText[:240]
	}
	_, err := db.Exec(`INSERT INTO fetch_logs(product_id, success, error_text, fetched_at)
	VALUES(?, ?, ?, datetime('now', '+8 hours'))`, productID, success, errText)
	return err
}

func updateLastFetchStatus(db *sql.DB, productID int64, ok bool, errMsg string) error {
	okInt := 0
	if ok {
		okInt = 1
	}
	errMsg = strings.TrimSpace(errMsg)
	if len(errMsg) > 240 {
		errMsg = errMsg[:240]
	}
	_, err := db.Exec(`UPDATE products
		SET last_fetch_ok = ?,
		    last_fetch_at = datetime('now', '+8 hours'),
		    last_fetch_error = ?,
		    updated_at = datetime('now', '+8 hours')
		WHERE id = ?`, okInt, errMsg, productID)
	return err
}

func normalizeText(s string) string {
	if s == "" {
		return ""
	}
	stripTags := regexp.MustCompile(`(?is)<[^>]+>`)
	s = stripTags.ReplaceAllString(s, " ")
	s = strings.ReplaceAll(s, "&nbsp;", " ")
	s = strings.ReplaceAll(s, "&#160;", " ")
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.Join(strings.Fields(s), " ")
	return strings.TrimSpace(s)
}

func extractMerchantUpdate(content string, p Product) string {
	if v := extractOptionalByRegex(content, p.UpdateRegex); v != "" {
		return formatMerchantUpdate(v)
	}
	// 兜底时只尝试按店铺模式提取，避免从整页文本误抓日期导致“商户更新”不准确。
	if v := extractOptionalByRegex(content, buildUpdateRegex(p.StoreName)); v != "" {
		return formatMerchantUpdate(v)
	}
	return ""
}

func formatMerchantUpdate(raw string) string {
	raw = normalizeText(raw)
	if raw == "" {
		return ""
	}
	reDate := regexp.MustCompile(`([0-9]{4}-[0-9]{2}-[0-9]{2})\s*更新`)
	reStock := regexp.MustCompile(`(請先查詢|请先查询|請查詢|请查询|查詢中|查询中|待查詢|待查询|少量存貨|少量存货|有現貨|有现货|現貨|现货|缺貨|缺货|預訂|预订|待定|離線|离线)`)
	dateMatch := reDate.FindStringSubmatch(raw)
	if len(dateMatch) < 2 {
		stockMatch := reStock.FindStringSubmatch(raw)
		if len(stockMatch) >= 2 {
			return normalizeStockStatus(stockMatch[1])
		}
		return ""
	}
	datePart := dateMatch[1] + " 更新"
	stockMatch := reStock.FindStringSubmatch(raw)
	if len(stockMatch) >= 2 {
		return datePart + " • " + normalizeStockStatus(stockMatch[1])
	}
	return datePart
}

func normalizeStockStatus(v string) string {
	s := strings.TrimSpace(v)
	switch s {
	case "請先查詢", "请先查询", "請查詢", "请查询", "查詢中", "查询中", "待查詢", "待查询":
		return "請先查詢"
	default:
		return s
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (a *App) loadTemplates() error {
	root := envOrDefault("TEMPLATE_DIR", "./templates")
	t, err := template.ParseFiles(
		filepath.Join(root, "index.html"),
		filepath.Join(root, "product.html"),
		filepath.Join(root, "runtime.html"),
		filepath.Join(root, "fx.html"),
	)
	if err != nil {
		return err
	}
	a.templates = t
	return nil
}

func (a *App) serve(addr string) error {
	mux := http.NewServeMux()
	appMux := http.NewServeMux()
	appMux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))
	appMux.HandleFunc("/", a.handleIndex)
	appMux.HandleFunc("/product/", a.handleProductPage)
	appMux.HandleFunc("/runtime", a.handleRuntimePage)
	appMux.HandleFunc("/fx", a.handleFXPage)
	appMux.HandleFunc("/api/products", a.handleProducts)
	appMux.HandleFunc("/api/products/", a.handleProductHistory)
	appMux.HandleFunc("/api/collect", a.handleCollect)
	appMux.HandleFunc("/api/fx/hkd-cny/today", a.handleTodayFX)
	appMux.HandleFunc("/api/fx/hkd-cny/history", a.handleFXHistory)
	appMux.HandleFunc("/api/fx/hkd-cny/chart", a.handleFXChart)
	appMux.HandleFunc("/api/store-options", a.handleStoreOptions)
	appMux.HandleFunc("/api/runtime/status", a.handleRuntimeStatus)
	appMux.HandleFunc("/api/runtime/logs", a.handleRuntimeLogs)

	if a.basePath == "" {
		mux.Handle("/", appMux)
	} else {
		prefix := a.basePath
		mux.Handle(prefix+"/", http.StripPrefix(prefix, appMux))
		mux.HandleFunc(prefix, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, prefix+"/", http.StatusPermanentRedirect)
		})
	}

	srv := &http.Server{
		Addr:         addr,
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	log.Printf("服务已启动: %s", addr)
	return srv.ListenAndServe()
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := atomic.AddUint64(&httpRequestSeq, 1)
		start := time.Now()
		ip := clientIP(r)
		path := r.URL.Path
		if r.URL.RawQuery != "" {
			path += "?" + r.URL.RawQuery
		}
		ua := strings.TrimSpace(r.UserAgent())
		if len(ua) > 180 {
			ua = ua[:180] + "..."
		}
		debugLogf("[http][rid=%d] -> %s %s ip=%s ua=%q", reqID, r.Method, path, ip, ua)

		rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		panicked := false
		defer func() {
			debugLogf("[http][rid=%d] <- %s %s status=%d bytes=%d cost=%s panic=%t", reqID, r.Method, path, rec.statusCode, rec.bytes, time.Since(start), panicked)
		}()
		defer func() {
			if rv := recover(); rv != nil {
				panicked = true
				log.Printf("[http][rid=%d] !! panic: %v method=%s path=%s ip=%s", reqID, rv, r.Method, path, ip)
				http.Error(rec, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	bytes      int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func clientIP(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	if xrip := strings.TrimSpace(r.Header.Get("X-Real-IP")); xrip != "" {
		return xrip
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data := map[string]any{"BasePath": a.basePath}
	if err := a.templates.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *App) handleProductPage(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/product/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	var name string
	err = a.db.QueryRow(`SELECT name FROM products WHERE id = ?`, id).Scan(&name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	name = normalizeProductDisplayName(name)

	data := map[string]any{
		"ProductID":   id,
		"ProductName": name,
		"BasePath":    a.basePath,
	}
	if err := a.templates.ExecuteTemplate(w, "product.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *App) handleRuntimePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/runtime" && r.URL.Path != "/runtime/" {
		http.NotFound(w, r)
		return
	}
	data := map[string]any{"BasePath": a.basePath}
	if err := a.templates.ExecuteTemplate(w, "runtime.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *App) handleFXPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/fx" && r.URL.Path != "/fx/" {
		http.NotFound(w, r)
		return
	}
	data := map[string]any{"BasePath": a.basePath}
	if err := a.templates.ExecuteTemplate(w, "fx.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *App) handleProducts(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		a.handleAddProduct(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rows, err := a.db.Query(`
	SELECT p.id, p.name, p.store_name, p.url, p.currency,
		COALESCE(p.last_fetch_ok, fl.success) AS last_fetch_ok,
		COALESCE(p.last_fetch_at, fl.fetched_at) AS last_fetch_at,
		COALESCE(NULLIF(TRIM(p.last_fetch_error), ''), NULLIF(TRIM(fl.error_text), '')) AS last_fetch_error,
		r.price,
		(
			SELECT r3.price
			FROM price_records r3
			WHERE r3.product_id = p.id
				AND r3.record_date < (
					SELECT MAX(r4.record_date)
					FROM price_records r4
					WHERE r4.product_id = p.id
				)
			ORDER BY r3.record_date DESC
			LIMIT 1
		) AS prev_price,
		r.record_date, r.store_name, COALESCE(r.updated_at, r.created_at), r.merchant_update
	FROM products p
	LEFT JOIN fetch_logs fl ON fl.id = (
		SELECT f2.id
		FROM fetch_logs f2
		WHERE f2.product_id = p.id
		ORDER BY f2.fetched_at DESC, f2.id DESC
		LIMIT 1
	)
	LEFT JOIN price_records r ON r.product_id = p.id
		AND r.record_date = (
			SELECT MAX(record_date) FROM price_records r2 WHERE r2.product_id = p.id
		)
	ORDER BY p.id`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type row struct {
		ID             int64    `json:"id"`
		Name           string   `json:"name"`
		StoreName      string   `json:"store_name"`
		URL            string   `json:"url"`
		Currency       string   `json:"currency"`
		LastFetchOK    *int     `json:"last_fetch_ok"`
		LastFetchAt    *string  `json:"last_fetch_at"`
		LastFetchError *string  `json:"last_fetch_error"`
		Latest         *float64 `json:"latest_price"`
		PrevPrice      *float64 `json:"prev_price"`
		LatestDate     *string  `json:"latest_date"`
		LatestFrom     *string  `json:"latest_store_name"`
		UpdatedAt      *string  `json:"latest_updated_at"`
		MerchantUpdate *string  `json:"merchant_update"`
	}
	var out []row
	for rows.Next() {
		var rr row
		if err := rows.Scan(&rr.ID, &rr.Name, &rr.StoreName, &rr.URL, &rr.Currency, &rr.LastFetchOK, &rr.LastFetchAt, &rr.LastFetchError, &rr.Latest, &rr.PrevPrice, &rr.LatestDate, &rr.LatestFrom, &rr.UpdatedAt, &rr.MerchantUpdate); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rr.Name = normalizeProductDisplayName(rr.Name)
		out = append(out, rr)
	}
	writeJSON(w, out)
}

func (a *App) handleProductHistory(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/products/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if len(parts) == 1 && r.Method == http.MethodDelete {
		a.handleDeleteProduct(w, r, parts[0])
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(parts) != 2 || parts[1] != "history" {
		http.NotFound(w, r)
		return
	}
	pid, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	days := 180
	if s := strings.TrimSpace(r.URL.Query().Get("days")); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 2000 {
			days = n
		}
	}
	from := chinaNow().AddDate(0, 0, -days).Format("2006-01-02")

	rows, err := a.db.Query(`
	SELECT pr.record_date, pr.price,
		COALESCE(pr.updated_at, pr.created_at) AS recorded_at,
		(
			SELECT er.rate
			FROM exchange_rates er
			WHERE er.base_currency = 'HKD'
				AND er.quote_currency = 'CNY'
				AND er.rate_date <= pr.record_date
			ORDER BY er.rate_date DESC
			LIMIT 1
		) AS fx_rate
	FROM price_records pr
	WHERE pr.product_id = ? AND pr.record_date >= ?
	ORDER BY pr.record_date`, pid, from)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var list []PriceRecord
	for rows.Next() {
		var p PriceRecord
		var fxRate sql.NullFloat64
		if err := rows.Scan(&p.Date, &p.Price, &p.RecordedAt, &fxRate); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		p.Price = math.Round(p.Price*100) / 100
		if fxRate.Valid && fxRate.Float64 > 0 {
			r := math.Round(fxRate.Float64*10000) / 10000
			cny := math.Round(p.Price*r*100) / 100
			p.FXRate = &r
			p.CNYPrice = &cny
		}
		list = append(list, p)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Date < list[j].Date })
	writeJSON(w, list)
}

func (a *App) handleAddProduct(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	log.Printf("[add-product] 收到新增请求: remote=%s", r.RemoteAddr)
	in, ierr := parseAddProductInput(r)
	if ierr != nil {
		log.Printf("[add-product] 输入校验失败: %v", ierr)
		http.Error(w, ierr.Message, ierr.Status)
		return
	}
	log.Printf("[add-product] 参数: url=%s store=%s", in.URL, in.StoreName)

	prepared, ierr := buildAddProductPrepared(in)
	if ierr != nil {
		log.Printf("[add-product] 准备新增参数失败: %v", ierr)
		http.Error(w, ierr.Message, ierr.Status)
		return
	}
	if err := addStoreOption(a.db, prepared.StoreName); err != nil {
		log.Printf("[add-product] 写入店铺选项失败(忽略): store=%s err=%v", prepared.StoreName, err)
	}

	if handled := a.tryDedupAddProductAndCollect(w, start, prepared); handled {
		return
	}

	pid, dedupByName, err := a.upsertProductByNameForAdd(prepared)
	if err != nil {
		log.Printf("[add-product] upsert 数据库记录失败: name=%s err=%v", prepared.Name, err)
		http.Error(w, "新增失败: "+err.Error(), http.StatusBadRequest)
		return
	}
	if dedupByName {
		log.Printf("[add-product] 商品已存在，已更新配置: id=%d name=%s", pid, prepared.Name)
	} else {
		log.Printf("[add-product] 新增数据库记录成功: id=%d name=%s", pid, prepared.Name)
	}
	a.collectAndRespondForAdd(w, start, pid, prepared.Name, prepared.StoreName, prepared.URL, prepared.PriceRegex, prepared.UpdateRegex, dedupByName)
}

type addProductReq struct {
	URL       string `json:"url"`
	StoreName string `json:"store_name"`
}

type addProductPrepared struct {
	URL         string
	StoreName   string
	ProductID   string
	Name        string
	PriceRegex  string
	UpdateRegex string
}

type addProductInputErr struct {
	Status  int
	Message string
	Cause   error
}

func (e *addProductInputErr) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause == nil {
		return e.Message
	}
	return e.Message + ": " + e.Cause.Error()
}

func parseAddProductInput(r *http.Request) (addProductReq, *addProductInputErr) {
	var in addProductReq
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&in); err != nil {
		return addProductReq{}, &addProductInputErr{
			Status:  http.StatusBadRequest,
			Message: "请求体格式错误",
			Cause:   err,
		}
	}

	urlText := normalizeProductURL(strings.TrimSpace(in.URL))
	if urlText == "" {
		return addProductReq{}, &addProductInputErr{
			Status:  http.StatusBadRequest,
			Message: "url 不能为空",
		}
	}
	if !strings.Contains(urlText, "price.com.hk/product.php") {
		return addProductReq{}, &addProductInputErr{
			Status:  http.StatusBadRequest,
			Message: "仅支持 price.com.hk 商品链接",
		}
	}
	store := normalizeStoreName(in.StoreName)
	if store == "" {
		store = "Galaxy 銀河攝影器材"
	}
	return addProductReq{
		URL:       urlText,
		StoreName: store,
	}, nil
}

func buildAddProductPrepared(in addProductReq) (addProductPrepared, *addProductInputErr) {
	productID, err := parseProductIDFromURL(in.URL)
	if err != nil {
		return addProductPrepared{}, &addProductInputErr{
			Status:  http.StatusBadRequest,
			Message: "链接中缺少有效商品ID(p)",
			Cause:   err,
		}
	}
	log.Printf("[add-product] 解析商品ID成功: product_id=%s", productID)

	autoName, err := autoDetectProductName(in.URL)
	if err != nil {
		return addProductPrepared{}, &addProductInputErr{
			Status:  http.StatusBadGateway,
			Message: "自动抓取商品名称失败: " + err.Error(),
			Cause:   err,
		}
	}
	name := buildAutoDisplayName(autoName, in.StoreName)
	priceRegex := buildPriceRegex(in.StoreName, productID)
	updateRegex := buildUpdateRegex(in.StoreName)
	log.Printf("[add-product] 自动抓取商品名成功: %s", name)
	log.Printf("[add-product] 生成抓取规则: price_regex=%s update_regex=%s", priceRegex, updateRegex)

	return addProductPrepared{
		URL:         in.URL,
		StoreName:   in.StoreName,
		ProductID:   productID,
		Name:        name,
		PriceRegex:  priceRegex,
		UpdateRegex: updateRegex,
	}, nil
}

// tryDedupAddProductAndCollect 优先按“店铺+链接 / 店铺+商品ID”去重，命中后直接更新并触发首次采集响应。
func (a *App) tryDedupAddProductAndCollect(w http.ResponseWriter, start time.Time, p addProductPrepared) bool {
	if existedByURL, existedName, ok := findProductByStoreAndURL(a.db, p.StoreName, p.URL); ok {
		log.Printf("[add-product] 命中去重(店铺+链接): id=%d name=%s", existedByURL, existedName)
		if err := a.updateProductForAddDedup(existedByURL, p); err != nil {
			log.Printf("[add-product] 链接去重更新失败: id=%d err=%v", existedByURL, err)
			http.Error(w, "新增失败: "+err.Error(), http.StatusInternalServerError)
			return true
		}
		log.Printf("[add-product] 链接去重更新成功: id=%d", existedByURL)
		nameForUse := p.Name
		if existedName != "" {
			nameForUse = existedName
		}
		a.collectAndRespondForAdd(w, start, existedByURL, nameForUse, p.StoreName, p.URL, p.PriceRegex, p.UpdateRegex, true)
		return true
	}

	if existedByPID, existedName, ok := findProductByStoreAndPID(a.db, p.StoreName, p.ProductID); ok {
		log.Printf("[add-product] 命中去重(店铺+商品ID): id=%d name=%s", existedByPID, existedName)
		if err := a.updateProductForAddDedup(existedByPID, p); err != nil {
			log.Printf("[add-product] 去重更新失败: id=%d err=%v", existedByPID, err)
			http.Error(w, "新增失败: "+err.Error(), http.StatusInternalServerError)
			return true
		}
		log.Printf("[add-product] 去重更新成功: id=%d", existedByPID)
		nameForUse := p.Name
		if existedName != "" {
			nameForUse = existedName
		}
		a.collectAndRespondForAdd(w, start, existedByPID, nameForUse, p.StoreName, p.URL, p.PriceRegex, p.UpdateRegex, true)
		return true
	}
	return false
}

func (a *App) updateProductForAddDedup(pid int64, p addProductPrepared) error {
	_, err := a.db.Exec(`UPDATE products
		SET url = ?,
		    price_regex = ?,
		    update_regex = ?,
		    currency = 'HKD',
		    active = 1,
		    updated_at = datetime('now', '+8 hours')
		WHERE id = ?`, p.URL, p.PriceRegex, p.UpdateRegex, pid)
	return err
}

func (a *App) upsertProductByNameForAdd(p addProductPrepared) (int64, bool, error) {
	var existedID sql.NullInt64
	_ = a.db.QueryRow(`SELECT id FROM products WHERE name = ?`, p.Name).Scan(&existedID)

	_, err := a.db.Exec(`INSERT INTO products(name, store_name, url, price_regex, update_regex, currency, active, created_at, updated_at)
	VALUES(?, ?, ?, ?, ?, 'HKD', 1, datetime('now', '+8 hours'), datetime('now', '+8 hours'))
	ON CONFLICT(name) DO UPDATE SET
	store_name=excluded.store_name,
	url=excluded.url,
	price_regex=excluded.price_regex,
	update_regex=excluded.update_regex,
	currency='HKD',
	active=1,
	updated_at=datetime('now', '+8 hours')`,
		p.Name, p.StoreName, p.URL, p.PriceRegex, p.UpdateRegex)
	if err != nil {
		return 0, false, err
	}

	var pid int64
	if err := a.db.QueryRow(`SELECT id FROM products WHERE name = ?`, p.Name).Scan(&pid); err != nil || pid <= 0 {
		return 0, false, fmt.Errorf("新增成功但无法获取商品ID: err=%v pid=%d", err, pid)
	}
	return pid, existedID.Valid, nil
}

func (a *App) collectAndRespondForAdd(
	w http.ResponseWriter,
	start time.Time,
	pid int64,
	name, store, urlText, priceRegex, updateRegex string,
	dedup bool,
) {
	newProduct := buildProductForAddCollect(pid, name, store, urlText, priceRegex, updateRegex)
	log.Printf("[add-product][%s] 开始首次抓取: product_id=%d", name, pid)
	got, ferr := fetchPrice(newProduct)
	if ferr != nil {
		a.respondAddProductCollectFailure(w, pid, name, dedup, "首次抓取失败", ferr)
		return
	}
	day := chinaNow().Format("2006-01-02")
	log.Printf("[add-product][%s] 首次抓取成功，开始写入台账: day=%s", name, day)
	if err := upsertDailyPrice(a.db, pid, day, store, got.Price, got.RawPrice, got.MerchantUpdate, urlText); err != nil {
		a.respondAddProductCollectFailure(w, pid, name, dedup, "首次抓取入库失败", err)
		return
	}
	_ = insertFetchLog(a.db, pid, true, "")
	_ = updateLastFetchStatus(a.db, pid, true, "")
	log.Printf("[add-product][%s] 首次抓取与入库完成: price=%.2f merchant_update=%s", name, got.Price, got.MerchantUpdate)
	log.Printf("[add-product] 新增流程完成: id=%d name=%s dedup=%t duration=%s", pid, name, dedup, time.Since(start).String())
	writeAddProductCollectResp(w, name, dedup, true, "")
}

func buildProductForAddCollect(pid int64, name, store, urlText, priceRegex, updateRegex string) Product {
	return Product{
		ID:          pid,
		Name:        name,
		StoreName:   store,
		URL:         urlText,
		PriceRegex:  priceRegex,
		UpdateRegex: updateRegex,
		Currency:    "HKD",
		Active:      true,
	}
}

// respondAddProductCollectFailure 统一处理首次抓取链路失败时的状态写入和响应格式。
func (a *App) respondAddProductCollectFailure(w http.ResponseWriter, pid int64, name string, dedup bool, stage string, cause error) {
	log.Printf("[add-product][%s] %s: %v", name, stage, cause)
	_ = insertFetchLog(a.db, pid, false, cause.Error())
	_ = updateLastFetchStatus(a.db, pid, false, cause.Error())
	writeAddProductCollectResp(w, name, dedup, false, cause.Error())
}

func writeAddProductCollectResp(w http.ResponseWriter, name string, dedup bool, ok bool, errText string) {
	initialCollect := "failed"
	if ok {
		initialCollect = "success"
	}
	resp := map[string]any{
		"status":          "ok",
		"name":            normalizeProductDisplayName(name),
		"dedup":           dedup,
		"initial_collect": initialCollect,
	}
	if strings.TrimSpace(errText) != "" {
		resp["error"] = errText
	}
	writeJSON(w, resp)
}

func findProductByStoreAndPID(db *sql.DB, storeName, productID string) (int64, string, bool) {
	rows, err := db.Query(`SELECT id, name, store_name, url FROM products ORDER BY id`)
	if err != nil {
		return 0, "", false
	}
	defer rows.Close()

	targetStore := strings.TrimSpace(storeName)
	targetPID := strings.TrimSpace(productID)
	for rows.Next() {
		var id int64
		var name string
		var store string
		var urlText string
		if err := rows.Scan(&id, &name, &store, &urlText); err != nil {
			continue
		}
		if strings.TrimSpace(store) != targetStore {
			continue
		}
		pid, err := parseProductIDFromURL(normalizeProductURL(urlText))
		if err != nil {
			continue
		}
		if pid == targetPID {
			return id, name, true
		}
	}
	return 0, "", false
}

func findProductByStoreAndURL(db *sql.DB, storeName, rawURL string) (int64, string, bool) {
	target, err := canonicalProductURL(rawURL)
	if err != nil {
		return 0, "", false
	}
	rows, err := db.Query(`SELECT id, name, store_name, url FROM products ORDER BY id`)
	if err != nil {
		return 0, "", false
	}
	defer rows.Close()

	targetStore := strings.TrimSpace(storeName)
	for rows.Next() {
		var id int64
		var name string
		var store string
		var urlText string
		if err := rows.Scan(&id, &name, &store, &urlText); err != nil {
			continue
		}
		if strings.TrimSpace(store) != targetStore {
			continue
		}
		u, err := canonicalProductURL(urlText)
		if err != nil {
			continue
		}
		if u == target {
			return id, name, true
		}
	}
	return 0, "", false
}

func canonicalProductURL(raw string) (string, error) {
	u, err := url.Parse(normalizeProductURL(raw))
	if err != nil {
		return "", err
	}
	u.Scheme = strings.ToLower(strings.TrimSpace(u.Scheme))
	u.Host = strings.ToLower(strings.TrimSpace(u.Host))
	u.Fragment = ""
	q := u.Query()
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func autoDetectProductName(urlText string) (string, error) {
	ua := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
	var content string

	if useFlareSolverrFallback() {
		fsURL := flareSolverrURL()
		sessionName := flareSolverrSessionName()
		httpTimeout := flareSolverrRequestHTTPTimeout()
		maxTimeoutMS := flareSolverrMaxTimeoutMS()
		log.Printf("[add-product][name-detect] 尝试 FlareSolverr: url=%s session=%s", fsURL, sessionName)
		_, _ = ensureFlareSolverrSessionCached(fsURL, sessionName)
		resp, err := callFlareSolverr(fsURL, newFlareSolverrRequestGetPayload(urlText, sessionName, maxTimeoutMS), httpTimeout)
		if err == nil && !looksLikeCloudflareChallenge(resp.Solution.Response) {
			content = resp.Solution.Response
			log.Printf("[add-product][name-detect] FlareSolverr 成功获取页面")
		} else if err != nil {
			log.Printf("[add-product][name-detect] FlareSolverr 失败: %v", err)
		} else {
			log.Printf("[add-product][name-detect] FlareSolverr 返回挑战页")
		}
	}

	if content == "" {
		log.Printf("[add-product][name-detect] 回退 HTTP 抓取商品页")
		client := newBrowserLikeClient()
		_ = warmUpHome(client, urlText, ua)
		req, err := http.NewRequest(http.MethodGet, urlText, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("User-Agent", ua)
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
		req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7")
		req.Header.Set("Referer", "https://www.price.com.hk/")
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[add-product][name-detect] HTTP 请求失败: %v", err)
			return "", err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("[add-product][name-detect] HTTP 状态异常: %d", resp.StatusCode)
			return "", fmt.Errorf("status=%d", resp.StatusCode)
		}
		content = string(b)
		log.Printf("[add-product][name-detect] HTTP 抓取成功")
	}

	name := extractPageProductName(content)
	if name == "" {
		log.Printf("[add-product][name-detect] 页面解析失败: 未提取到商品名")
		return "", fmt.Errorf("未从页面提取到商品名")
	}
	log.Printf("[add-product][name-detect] 页面解析成功: name=%s", name)
	return name, nil
}

func extractPageProductName(content string) string {
	patterns := []string{
		`(?is)<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']`,
		`(?is)<title>\s*([^<]+?)\s*(?:-\s*Price\.com\.hk|\|\s*Price\.com\.hk|Price\.com\.hk)?\s*</title>`,
		`(?is)<h1[^>]*>\s*([^<]+)\s*</h1>`,
	}
	for _, p := range patterns {
		re := regexp.MustCompile(p)
		m := re.FindStringSubmatch(content)
		if len(m) < 2 {
			continue
		}
		v := strings.TrimSpace(html.UnescapeString(m[1]))
		v = strings.Join(strings.Fields(v), " ")
		v = strings.Trim(v, "-| ")
		if v != "" && !strings.Contains(strings.ToLower(v), "price.com.hk") {
			return v
		}
	}
	return ""
}

func buildAutoDisplayName(base, store string) string {
	base = normalizeProductBaseName(base)
	if base == "" {
		base = "Product"
	}
	tag := "Galaxy 水貨"
	if strings.Contains(store, "順星") || strings.Contains(store, "顺星") {
		tag = "順星水貨"
	}
	if strings.Contains(base, tag) {
		return base
	}
	return fmt.Sprintf("%s (%s)", base, tag)
}

func normalizeProductBaseName(base string) string {
	base = strings.TrimSpace(html.UnescapeString(base))
	base = strings.Join(strings.Fields(base), " ")
	suffixPatterns := []string{
		`(?i)\s*價錢、規格及用家意見\s*-\s*香港格價網\s*$`,
		`(?i)\s*-\s*香港格價網\s*$`,
		`(?i)\s*-\s*price\.com\.hk\s*$`,
	}
	for _, p := range suffixPatterns {
		re := regexp.MustCompile(p)
		base = re.ReplaceAllString(base, "")
	}
	base = strings.TrimSpace(base)
	return base
}

func normalizeProductDisplayName(name string) string {
	s := strings.TrimSpace(name)
	re := regexp.MustCompile(`\s*\((?:Galaxy\s*水貨|Galaxy\s*水货|順星水貨|顺星水货|順星數碼\s*水貨|顺星数码\s*水货)\)\s*$`)
	s = re.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func normalizeStoreName(name string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(name)), " ")
}

func listStoreOptions(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM store_options ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 16)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		name = normalizeStoreName(name)
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func addStoreOption(db *sql.DB, name string) error {
	n := normalizeStoreName(name)
	if n == "" {
		return fmt.Errorf("店铺名不能为空")
	}
	if utf8.RuneCountInString(n) > 80 {
		return fmt.Errorf("店铺名过长")
	}
	_, err := db.Exec(`INSERT INTO store_options(name, created_at, updated_at)
		VALUES(?, datetime('now', '+8 hours'), datetime('now', '+8 hours'))
		ON CONFLICT(name) DO UPDATE SET updated_at=datetime('now', '+8 hours')`, n)
	return err
}

func deleteStoreOption(db *sql.DB, name string) error {
	n := normalizeStoreName(name)
	if n == "" {
		return fmt.Errorf("店铺名不能为空")
	}
	_, err := db.Exec(`DELETE FROM store_options WHERE name = ?`, n)
	return err
}

func (a *App) handleDeleteProduct(w http.ResponseWriter, r *http.Request, idStr string) {
	// 兼容旧客户端: 允许可选 JSON 请求体，但不再要求输入确认码。
	if r.Body != nil {
		var ignore map[string]any
		decErr := json.NewDecoder(io.LimitReader(r.Body, 8*1024)).Decode(&ignore)
		if decErr != nil && !errors.Is(decErr, io.EOF) {
			http.Error(w, "请求体格式错误", http.StatusBadRequest)
			return
		}
	}
	pid, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 64)
	if err != nil || pid <= 0 {
		http.NotFound(w, r)
		return
	}
	var pname, pstore, purl string
	if err := a.db.QueryRow(`SELECT name, store_name, url FROM products WHERE id = ?`, pid).Scan(&pname, &pstore, &purl); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cfgPath := envOrDefault("PRODUCTS_CONFIG", "./config/products.json")
	removed, err := removeProductFromConfig(cfgPath, pname, pstore, purl)
	if err != nil {
		http.Error(w, "更新配置文件失败: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !removed {
		log.Printf("[delete-product] 配置文件未匹配到目标项，继续删除数据库记录: id=%d name=%s store=%s", pid, pname, pstore)
	}

	tx, err := a.db.Begin()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM fetch_logs WHERE product_id = ?`, pid); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := tx.Exec(`DELETE FROM price_records WHERE product_id = ?`, pid); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res, err := tx.Exec(`DELETE FROM products WHERE id = ?`, pid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	aff, _ := res.RowsAffected()
	if aff == 0 {
		http.NotFound(w, r)
		return
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"status":         "ok",
		"config_removed": removed,
	})
}

func removeProductFromConfig(cfgPath, targetName, targetStore, targetURL string) (bool, error) {
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return false, err
	}
	var list []ProductConfig
	if err := json.Unmarshal(b, &list); err != nil {
		return false, fmt.Errorf("解析配置失败: %w", err)
	}

	targetName = strings.TrimSpace(targetName)
	targetStore = strings.TrimSpace(targetStore)
	targetCanonURL, _ := canonicalProductURL(targetURL)

	next := make([]ProductConfig, 0, len(list))
	removed := false
	for _, it := range list {
		if removed {
			next = append(next, it)
			continue
		}
		nameMatched := strings.TrimSpace(it.Name) == targetName
		storeMatched := strings.TrimSpace(it.StoreName) == targetStore
		itemCanonURL, _ := canonicalProductURL(it.URL)
		urlMatched := targetCanonURL != "" && itemCanonURL != "" && itemCanonURL == targetCanonURL
		if nameMatched && storeMatched && (urlMatched || strings.TrimSpace(it.URL) == strings.TrimSpace(targetURL)) {
			removed = true
			continue
		}
		next = append(next, it)
	}
	if !removed {
		return false, nil
	}

	out, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return false, fmt.Errorf("序列化配置失败: %w", err)
	}
	out = append(out, '\n')
	tmp := cfgPath + ".tmp"
	if err := os.WriteFile(tmp, out, 0644); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, cfgPath); err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	return true, nil
}

func parseProductIDFromURL(urlText string) (string, error) {
	u, err := url.Parse(urlText)
	if err != nil {
		return "", err
	}
	pid := strings.TrimSpace(u.Query().Get("p"))
	if pid == "" {
		return "", fmt.Errorf("missing p")
	}
	if _, err := strconv.Atoi(pid); err != nil {
		return "", err
	}
	return pid, nil
}

func normalizeProductURL(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return s
	}
	lower := strings.ToLower(s)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return s
	}
	return "https://" + s
}

func buildPriceRegex(storeName, productID string) string {
	if strings.Contains(storeName, "順星") || strings.Contains(strings.ToLower(storeName), "顺星") {
		return "(?:順星數碼|顺星数码)[\\s\\S]*?product_id=" + productID + "[\\s\\S]*?tr_pw=([0-9.]+)[\\s\\S]*?tr_so=w"
	}
	return "merchant_id=9146[\\s\\S]*?product_id=" + productID + "[\\s\\S]*?tr_pw=([0-9.]+)[\\s\\S]*?tr_so=w"
}

func buildUpdateRegex(storeName string) string {
	if strings.Contains(storeName, "順星") || strings.Contains(strings.ToLower(storeName), "顺星") {
		return "(?:順星數碼|顺星数码)[\\s\\S]*?([0-9]{4}-[0-9]{2}-[0-9]{2}\\s*更新(?:[\\s\\S]{0,120}?(?:請先查詢|请先查询|少量存貨|少量存货|有現貨|有现货|現貨|现货|缺貨|缺货|預訂|预订|待定|離線|离线))?)"
	}
	return "Galaxy\\s*(?:銀河|银河)攝影器材[\\s\\S]*?([0-9]{4}-[0-9]{2}-[0-9]{2}\\s*更新(?:[\\s\\S]{0,120}?(?:請先查詢|请先查询|少量存貨|少量存货|有現貨|有现货|現貨|现货|缺貨|缺货|預訂|预订|待定|離線|离线))?)"
}

func (a *App) handleCollect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	log.Printf("[api] 收到手动采集请求: %s", r.RemoteAddr)
	if err := collectToday(a.db); err != nil {
		log.Printf("[api] 手动采集失败: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("[api] 手动采集完成")
	writeJSON(w, map[string]string{"status": "ok"})
}

func (a *App) handleTodayFX(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rate, day, source, err := a.getTodayHKDCNYRate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]any{
		"pair":   "HKD/CNY",
		"rate":   rate,
		"date":   day,
		"source": source,
	})
}

func (a *App) handleStoreOptions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		list, err := listStoreOptions(a.db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"stores": list})
		return
	case http.MethodPost:
		var in struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&in); err != nil {
			http.Error(w, "请求体格式错误", http.StatusBadRequest)
			return
		}
		name := normalizeStoreName(in.Name)
		if name == "" {
			http.Error(w, "店铺名不能为空", http.StatusBadRequest)
			return
		}
		if err := addStoreOption(a.db, name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"status": "ok", "name": name})
		return
	case http.MethodDelete:
		name := normalizeStoreName(r.URL.Query().Get("name"))
		if name == "" {
			http.Error(w, "name 不能为空", http.StatusBadRequest)
			return
		}
		if err := deleteStoreOption(a.db, name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"status": "ok", "name": name})
		return
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *App) handleFXHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	page := 1
	if raw := strings.TrimSpace(r.URL.Query().Get("page")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			page = n
		}
	}
	pageSize := 20
	if raw := strings.TrimSpace(r.URL.Query().Get("page_size")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			pageSize = n
		}
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}

	var total int
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM exchange_rates WHERE base_currency = 'HKD' AND quote_currency = 'CNY'`).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	offset := (page - 1) * pageSize
	if offset < 0 {
		offset = 0
	}

	type fxRow struct {
		RateDate  string  `json:"rate_date"`
		Rate      float64 `json:"rate"`
		Source    string  `json:"source"`
		UpdatedAt string  `json:"updated_at"`
	}
	rows, err := a.db.Query(`SELECT rate_date, rate, source, updated_at
		FROM exchange_rates
		WHERE base_currency = 'HKD' AND quote_currency = 'CNY'
		ORDER BY rate_date DESC
		LIMIT ? OFFSET ?`, pageSize, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	out := make([]fxRow, 0, pageSize)
	for rows.Next() {
		var x fxRow
		if err := rows.Scan(&x.RateDate, &x.Rate, &x.Source, &x.UpdatedAt); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"pair":      "HKD/CNY",
		"page":      page,
		"page_size": pageSize,
		"total":     total,
		"rows":      out,
	})
}

func (a *App) handleFXChart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	days := 180
	if raw := strings.TrimSpace(r.URL.Query().Get("days")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			days = n
		}
	}
	if days < 7 {
		days = 7
	}
	if days > 1000 {
		days = 1000
	}

	type fxPoint struct {
		RateDate string  `json:"rate_date"`
		Rate     float64 `json:"rate"`
	}
	rows, err := a.db.Query(`SELECT rate_date, rate
		FROM exchange_rates
		WHERE base_currency = 'HKD' AND quote_currency = 'CNY'
		ORDER BY rate_date DESC
		LIMIT ?`, days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	tmp := make([]fxPoint, 0, days)
	for rows.Next() {
		var p fxPoint
		if err := rows.Scan(&p.RateDate, &p.Rate); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tmp = append(tmp, p)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Reverse to ascending date order for line chart rendering.
	points := make([]fxPoint, len(tmp))
	for i := range tmp {
		points[len(tmp)-1-i] = tmp[i]
	}
	writeJSON(w, map[string]any{
		"pair":   "HKD/CNY",
		"points": points,
	})
}

func (a *App) handleRuntimeStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, runtimeMon.snapshot())
}

func (a *App) handleRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	limit := 200
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	var sinceID int64
	if raw := strings.TrimSpace(r.URL.Query().Get("since_id")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n >= 0 {
			sinceID = n
		}
	}
	lines, latestID := runtimeMon.logsSince(sinceID, limit)
	writeJSON(w, map[string]any{
		"lines":     lines,
		"latest_id": latestID,
	})
}

func (a *App) getTodayHKDCNYRate() (float64, string, string, error) {
	if v := strings.TrimSpace(os.Getenv("HKD_CNY_RATE")); v != "" {
		rate, err := strconv.ParseFloat(v, 64)
		if err == nil && rate > 0 {
			return rate, chinaNow().Format("2006-01-02"), "env:HKD_CNY_RATE", nil
		}
	}

	today := chinaNow().Format("2006-01-02")
	a.fx.mu.Lock()
	if a.fx.rate > 0 && a.fx.date == today && time.Since(a.fx.updated) < 12*time.Hour {
		r, d, s := a.fx.rate, a.fx.date, a.fx.source
		a.fx.mu.Unlock()
		return r, d, s, nil
	}
	a.fx.mu.Unlock()

	if rate, source, ok := getStoredFXRate(a.db, "HKD", "CNY", today); ok {
		a.fx.mu.Lock()
		a.fx.rate = rate
		a.fx.date = today
		a.fx.source = source
		a.fx.updated = chinaNow()
		a.fx.mu.Unlock()
		return rate, today, source, nil
	}

	type provider struct {
		url   string
		parse func([]byte) (float64, string, error)
	}
	providers := []provider{
		{
			url: "https://api.frankfurter.app/latest?from=HKD&to=CNY",
			parse: func(b []byte) (float64, string, error) {
				var x struct {
					Date  string             `json:"date"`
					Rates map[string]float64 `json:"rates"`
				}
				if err := json.Unmarshal(b, &x); err != nil {
					return 0, "", err
				}
				rate := x.Rates["CNY"]
				if rate <= 0 {
					return 0, "", fmt.Errorf("frankfurter rate missing")
				}
				if strings.TrimSpace(x.Date) == "" {
					x.Date = today
				}
				return rate, x.Date, nil
			},
		},
		{
			url: "https://open.er-api.com/v6/latest/HKD",
			parse: func(b []byte) (float64, string, error) {
				var x struct {
					TimeLastUpdateUTC string             `json:"time_last_update_utc"`
					Rates             map[string]float64 `json:"rates"`
				}
				if err := json.Unmarshal(b, &x); err != nil {
					return 0, "", err
				}
				rate := x.Rates["CNY"]
				if rate <= 0 {
					return 0, "", fmt.Errorf("er-api rate missing")
				}
				return rate, today, nil
			},
		},
	}

	client := &http.Client{Timeout: 12 * time.Second}
	for _, p := range providers {
		req, err := http.NewRequest(http.MethodGet, p.url, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", "PriceMonitor/1.0")
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		rate, day, err := p.parse(body)
		if err != nil || rate <= 0 {
			continue
		}
		day = today
		if err := upsertFXRate(a.db, "HKD", "CNY", day, rate, p.url); err != nil {
			log.Printf("写入汇率失败 [%s]: %v", day, err)
		}
		a.fx.mu.Lock()
		a.fx.rate = rate
		a.fx.date = day
		a.fx.source = p.url
		a.fx.updated = chinaNow()
		a.fx.mu.Unlock()
		return rate, day, p.url, nil
	}

	a.fx.mu.Lock()
	if a.fx.rate > 0 {
		r, d, s := a.fx.rate, a.fx.date, a.fx.source
		a.fx.mu.Unlock()
		return r, d, s + " (stale)", nil
	}
	a.fx.mu.Unlock()

	if rate, day, source, ok := getLatestStoredFXRate(a.db, "HKD", "CNY"); ok {
		return rate, day, source + " (stale)", nil
	}

	return 0, "", "", fmt.Errorf("无法获取当日 HKD/CNY 汇率，请设置 HKD_CNY_RATE 环境变量")
}

func upsertFXRate(db *sql.DB, base, quote, day string, rate float64, source string) error {
	_, err := db.Exec(`INSERT INTO exchange_rates(base_currency, quote_currency, rate_date, rate, source, created_at, updated_at)
	VALUES(?, ?, ?, ?, ?, datetime('now', '+8 hours'), datetime('now', '+8 hours'))
	ON CONFLICT(base_currency, quote_currency, rate_date) DO UPDATE SET
	rate=excluded.rate,
	source=excluded.source,
	updated_at=datetime('now', '+8 hours')`, base, quote, day, rate, source)
	return err
}

func getStoredFXRate(db *sql.DB, base, quote, day string) (float64, string, bool) {
	var rate float64
	var source string
	err := db.QueryRow(`SELECT rate, source
		FROM exchange_rates
		WHERE base_currency = ? AND quote_currency = ? AND rate_date = ?`,
		base, quote, day).Scan(&rate, &source)
	if err != nil || rate <= 0 {
		return 0, "", false
	}
	return rate, source, true
}

func getLatestStoredFXRate(db *sql.DB, base, quote string) (float64, string, string, bool) {
	var rate float64
	var day string
	var source string
	err := db.QueryRow(`SELECT rate, rate_date, source
		FROM exchange_rates
		WHERE base_currency = ? AND quote_currency = ?
		ORDER BY rate_date DESC
		LIMIT 1`, base, quote).Scan(&rate, &day, &source)
	if err != nil || rate <= 0 {
		return 0, "", "", false
	}
	return rate, day, source, true
}

func weComWebhook() string {
	return strings.TrimSpace(envOrDefault("WECHAT_BOT_WEBHOOK", defaultWeComWebhook))
}

func trimAlertText(s string, maxRunes int) string {
	s = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "\r", " "), "\n", " "))
	if maxRunes <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return strings.TrimSpace(string(r[:maxRunes])) + "..."
}

func sendFlareSolverrConnectivityAlertToWeCom(scene, fsURL string, cause error) error {
	webhook := weComWebhook()
	if webhook == "" {
		return fmt.Errorf("未配置 WECHAT_BOT_WEBHOOK")
	}
	now := chinaNow().Format("2006-01-02 15:04:05")
	content := "### FlareSolverr 连接告警\n" +
		"> 时间: " + now + "\n" +
		"> 场景: " + trimAlertText(scene, 80) + "\n" +
		"> 目标: " + trimAlertText(fsURL, 120) + "\n" +
		"> 错误: " + trimAlertText(cause.Error(), 280) + "\n"
	return sendWeComMarkdown(webhook, content)
}

func sendPricePushToWeCom(db *sql.DB) error {
	webhook := weComWebhook()
	if webhook == "" {
		return fmt.Errorf("未配置 WECHAT_BOT_WEBHOOK")
	}
	fxRate, _ := getPushFXRate(db)
	lines, err := buildPricePushLines(db, fxRate)
	if err != nil {
		return err
	}
	if len(lines) == 0 {
		return fmt.Errorf("无当天更新价格数据")
	}

	now := chinaNow().Format("2006-01-02 15:04:05")
	content := "### 每日价格推送\n" +
		"> 时间: " + now + "\n\n" +
		strings.Join(lines, "\n\n")

	return sendWeComMarkdown(webhook, content)
}

func sendWeComMarkdown(webhook, content string) error {
	payload := map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"content": content,
		},
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, webhook, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("企业微信HTTP状态异常: %d", resp.StatusCode)
	}
	var out struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(body, &out); err == nil {
		if out.ErrCode != 0 {
			return fmt.Errorf("企业微信返回失败: errcode=%d errmsg=%s", out.ErrCode, out.ErrMsg)
		}
	}
	return nil
}

func getPushFXRate(db *sql.DB) (float64, string) {
	if v := strings.TrimSpace(os.Getenv("HKD_CNY_RATE")); v != "" {
		if rate, err := strconv.ParseFloat(v, 64); err == nil && rate > 0 {
			return rate, "env"
		}
	}
	if rate, day, source, ok := getLatestStoredFXRate(db, "HKD", "CNY"); ok && rate > 0 {
		return rate, day + " " + source
	}
	return 0.92, "fallback"
}

func buildPricePushLines(db *sql.DB, fxRate float64) ([]string, error) {
	today := chinaNow().Format("2006-01-02")
	rows, err := db.Query(`
	SELECT p.name, p.store_name, r.price, r.record_date, r.merchant_update,
		(
			SELECT r2.price
			FROM price_records r2
			WHERE r2.product_id = p.id
				AND r2.record_date < r.record_date
			ORDER BY r2.record_date DESC
			LIMIT 1
		) AS prev_price
	FROM products p
	LEFT JOIN price_records r ON r.product_id = p.id
		AND r.record_date = (
			SELECT MAX(record_date) FROM price_records r3 WHERE r3.product_id = p.id
		)
	WHERE p.active = 1
	ORDER BY p.name, p.store_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type storeLine struct {
		store string
		hkd   float64
		cny   float64
		arrow string
	}
	grouped := make(map[string][]storeLine)
	for rows.Next() {
		var name string
		var store string
		var latest sql.NullFloat64
		var recordDate sql.NullString
		var merchantUpdate sql.NullString
		var prev sql.NullFloat64
		if err := rows.Scan(&name, &store, &latest, &recordDate, &merchantUpdate, &prev); err != nil {
			return nil, err
		}
		name = normalizeProductBaseName(normalizeProductDisplayName(name))
		if !latest.Valid {
			continue
		}
		recordDay := strings.TrimSpace(recordDate.String)
		merchantDay := extractDateYMD(merchantUpdate.String)
		if merchantDay != "" {
			if merchantDay != today {
				continue
			}
		} else if recordDay != today {
			continue
		}
		hkd := latest.Float64
		cny := hkd * fxRate
		arrow := "→"
		if prev.Valid {
			diff := hkd - prev.Float64
			if diff > 0.004 {
				arrow = "↑"
			} else if diff < -0.004 {
				arrow = "↓"
			}
		}
		grouped[name] = append(grouped[name], storeLine{
			store: store,
			hkd:   hkd,
			cny:   cny,
			arrow: arrow,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var names []string
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)

	var lines []string
	for _, name := range names {
		items := grouped[name]
		sort.Slice(items, func(i, j int) bool {
			return items[i].store < items[j].store
		})
		var parts []string
		for _, item := range items {
			parts = append(parts, fmt.Sprintf("%s %.0f CNY %s", item.store, item.cny, item.arrow))
		}
		lines = append(lines, fmt.Sprintf("- %s：%s", name, strings.Join(parts, "；")))
	}
	return lines, nil
}

func extractDateYMD(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	re := regexp.MustCompile(`([0-9]{4}-[0-9]{2}-[0-9]{2})`)
	m := re.FindStringSubmatch(s)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

func mustLoadChinaLocation() *time.Location {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.FixedZone("CST", 8*3600)
	}
	return loc
}

func chinaNow() time.Time {
	return time.Now().In(chinaLoc)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
