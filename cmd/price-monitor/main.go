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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP 状态码异常: %d, body=%q", e.Code, e.Body)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: go run ./cmd/price-monitor [collect|ingest|serve|schedule|notify]")
		os.Exit(1)
	}

	dbPath := envOrDefault("DB_PATH", "./data.db")
	cfgPath := envOrDefault("PRODUCTS_CONFIG", "./config/products.json")
	logDir := envOrDefault("LOG_DIR", "./logs")

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
	must(withSQLiteBusyRetry("sync-products-config", func() error { return syncProductsFromConfig(db, cfgPath) }))

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

	if useFlareSolverrFallback() {
		if err := preflightFlareSolverr(); err != nil {
			log.Printf("[schedule] 启动预检 FlareSolverr 失败，不影响推送任务: %v", err)
			maybeNotifyFlareSolverrConnectivityAlert("schedule 启动预检", err)
		}
	} else {
		log.Printf("[schedule] 已禁用 FlareSolverr 后备通道（ENABLE_FLARESOLVERR_FALLBACK=false）")
	}
	log.Printf("[schedule] 调度已启动")

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
		log.Printf("[schedule] 当前时间=%s", now.Format("2006-01-02 15:04:05 -0700 MST"))
		log.Printf("[schedule] 下次任务=%s 时间=%s (等待=%s)", kind, next.Format("2006-01-02 15:04:05 -0700 MST"), wait.String())

		timer := time.NewTimer(wait)
		<-timer.C
		timer.Stop()

		if kind == scheduleCollect {
			if useFlareSolverrFallback() {
				log.Printf("[schedule] 到达采集触发时间，开始预检 FlareSolverr")
				if err := preflightFlareSolverr(); err != nil {
					log.Printf("[schedule] 预检失败，继续执行采集（将跳过/快速失败 FlareSolverr）: %v", err)
					maybeNotifyFlareSolverrConnectivityAlert("schedule 采集前预检", err)
				}
			}
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

type collectFailureDetail struct {
	ProductName string
	StoreName   string
	Err         string
}

func collectToday(db *sql.DB) error {
	if !collectRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("采集任务正在执行，跳过重复触发")
	}
	defer collectRunning.Store(false)

	products, err := getActiveProducts(db)
	if err != nil {
		return err
	}
	if len(products) == 0 {
		return fmt.Errorf("没有可采集的 active 商品")
	}

	today := chinaNow().Format("2006-01-02")
	log.Printf("[collect] 本轮采集开始，日期=%s，商品数量=%d", today, len(products))
	blockedByWAF := false
	flareConnIssue := false
	successCount := 0
	failCount := 0
	failures := make([]collectFailureDetail, 0, 8)
	for i, p := range products {
		if i > 0 {
			delay := collectItemDelay()
			if delay > 0 {
				log.Printf("[collect] 商品间隔等待: %s", delay.String())
				time.Sleep(delay)
			}
		}
		log.Printf("[collect][%d/%d][%s] 开始采集，店铺=%s，URL=%s", i+1, len(products), p.Name, p.StoreName, p.URL)
		got, err := fetchPrice(p)
		if err != nil {
			log.Printf("[collect][%s] 采集失败: %v", p.Name, err)
			failCount++
			failures = append(failures, collectFailureDetail{
				ProductName: p.Name,
				StoreName:   p.StoreName,
				Err:         err.Error(),
			})
			_ = insertFetchLog(db, p.ID, false, err.Error())
			_ = updateLastFetchStatus(db, p.ID, false, err.Error())
			if strings.Contains(strings.ToLower(err.Error()), "just a moment") ||
				strings.Contains(strings.ToLower(err.Error()), "cloudflare") {
				blockedByWAF = true
			}
			if isFlareSolverrTargetHost(flareSolverrURL(), "172.25.0.102") && isFlareSolverrConnectivityError(err) {
				flareConnIssue = true
			}
			continue
		}

		log.Printf("[collect][%s] 抓取完成，准备写入数据库", p.Name)
		err = upsertDailyPrice(db, p.ID, today, p.StoreName, got.Price, got.RawPrice, got.MerchantUpdate, p.URL)
		if err != nil {
			log.Printf("[collect][%s] 写入失败: %v", p.Name, err)
			failCount++
			failures = append(failures, collectFailureDetail{
				ProductName: p.Name,
				StoreName:   p.StoreName,
				Err:         err.Error(),
			})
			_ = insertFetchLog(db, p.ID, false, err.Error())
			_ = updateLastFetchStatus(db, p.ID, false, err.Error())
			continue
		}
		log.Printf("[collect][%s] 写入成功，记录抓取日志与状态", p.Name)
		_ = insertFetchLog(db, p.ID, true, "")
		_ = updateLastFetchStatus(db, p.ID, true, "")
		successCount++
		log.Printf("[collect][%s][%s] 采集成功: price=%.2f raw=%s merchant_update=%s", p.Name, p.StoreName, got.Price, got.RawPrice, got.MerchantUpdate)
	}
	log.Printf("[collect] 本轮采集结束: success=%d failed=%d", successCount, failCount)
	if failCount > 0 {
		if err := sendCollectFailureAlertToWeCom(today, successCount, failCount, failures, blockedByWAF, flareConnIssue); err != nil {
			log.Printf("[alert] 采集失败告警发送失败: %v", err)
		} else {
			log.Printf("[alert] 采集失败告警已发送")
		}
	}
	if successCount == 0 && blockedByWAF {
		return fmt.Errorf("当前被 Cloudflare 挑战拦截（Just a moment），请启用 FlareSolverr 或手动验证会话")
	}
	if successCount == 0 {
		return fmt.Errorf("本轮采集无成功记录")
	}
	return nil
}

type manualPriceInput struct {
	URL            string  `json:"url"`
	StoreName      string  `json:"store_name"`
	Price          float64 `json:"price"`
	MerchantUpdate string  `json:"merchant_update"`
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

func fetchPrice(p Product) (FetchResult, error) {
	log.Printf("[fetch][%s] 开始抓取链路", p.Name)
	var fsErr error
	if useFlareSolverrFallback() && shouldUseFlareSolverrNow() {
		log.Printf("[fetch][%s] 步骤1: 尝试 FlareSolverr", p.Name)
		got, err := fetchPriceViaFlareSolverr(p)
		if err == nil {
			log.Printf("[fetch][%s] FlareSolverr 抓取成功", p.Name)
			return got, nil
		}
		fsErr = err
		log.Printf("[fetch][%s] FlareSolverr 失败，回退 HTTP: %v", p.Name, err)
	} else if useFlareSolverrFallback() {
		log.Printf("[fetch][%s] 步骤1: 跳过 FlareSolverr（熔断窗口中）", p.Name)
	}

	log.Printf("[fetch][%s] 步骤2: 尝试 HTTP 抓取", p.Name)
	client := newBrowserLikeClient()
	uaList := []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	}

	var lastErr error
	for i, ua := range uaList {
		log.Printf("[fetch][%s] HTTP 尝试 #%d, UA=%s", p.Name, i+1, ua)
		got, err := fetchPriceOnce(client, p, ua)
		if err == nil {
			log.Printf("[fetch][%s] HTTP 抓取成功 (尝试 #%d)", p.Name, i+1)
			return got, nil
		}
		lastErr = err
		log.Printf("[fetch][%s] HTTP 失败 (尝试 #%d): %v", p.Name, i+1, err)

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
	return FetchResult{}, fmt.Errorf("抓取失败: flaresolverrErr=%v httpErr=%w", fsErr, lastErr)
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
	log.Printf("[flaresolverr] 预检开始: %s", fsURL)
	_, err := callFlareSolverr(fsURL, map[string]any{"cmd": "sessions.list"}, 10*time.Second)
	if err != nil {
		return fmt.Errorf("FlareSolverr 预检失败: %w", err)
	}
	_, err = ensureFlareSolverrSession(fsURL, flareSolverrSessionName())
	if err == nil {
		log.Printf("[flaresolverr] 预检通过，会话=%s", flareSolverrSessionName())
	}
	return err
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
	if strings.Contains(s, "flaresolverr 不可用(") {
		return true
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
		"connection reset by peer",
		"eof",
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

func fetchPriceOnce(client *http.Client, p Product, ua string) (FetchResult, error) {
	log.Printf("[http][%s] 预热首页", p.Name)
	_ = warmUpHome(client, p.URL, ua)
	log.Printf("[http][%s] 请求商品页", p.Name)
	req, err := http.NewRequest(http.MethodGet, p.URL, nil)
	if err != nil {
		return FetchResult{}, err
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
		return FetchResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return FetchResult{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snip := strings.TrimSpace(string(body))
		if len(snip) > 200 {
			snip = snip[:200]
		}
		return FetchResult{}, &httpStatusError{Code: resp.StatusCode, Body: snip}
	}
	content := string(body)
	log.Printf("[http][%s] 页面响应成功，开始解析价格字段", p.Name)

	raw, err := extractByRegex(content, p.PriceRegex)
	if err != nil {
		log.Printf("[http][%s] 价格正则未匹配", p.Name)
		return FetchResult{}, err
	}
	price, err := parsePrice(raw)
	if err != nil {
		return FetchResult{}, fmt.Errorf("解析价格失败: %w", err)
	}
	log.Printf("[http][%s] 价格解析成功: raw=%s price=%.2f", p.Name, raw, price)
	merchantUpdate := extractMerchantUpdate(content, p)
	if merchantUpdate == "" {
		merchantUpdate = chinaNow().Format("2006-01-02")
	}
	log.Printf("[http][%s] 商户更新字段=%s", p.Name, merchantUpdate)
	return FetchResult{Price: price, RawPrice: raw, MerchantUpdate: merchantUpdate}, nil
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

func fetchPriceViaFlareSolverr(p Product) (FetchResult, error) {
	fsURL := flareSolverrURL()
	sessionName := flareSolverrSessionName()
	log.Printf("[flaresolverr][%s] 开始抓取，url=%s session=%s", p.Name, fsURL, sessionName)
	if _, err := ensureFlareSolverrSessionCached(fsURL, sessionName); err != nil {
		return FetchResult{}, err
	}

	var out flareSolverrResp
	var lastErr error
	httpTimeout := flareSolverrRequestHTTPTimeout()
	maxTimeoutMS := flareSolverrMaxTimeoutMS()
	retries := flareSolverrMaxRetries()
	for i := 0; i < retries; i++ {
		log.Printf("[flaresolverr][%s] 请求尝试 #%d", p.Name, i+1)
		if i > 0 || shouldWarmupFlareSolverr() {
			_ = warmupFlareSolverrHome(fsURL, sessionName, maxTimeoutMS, httpTimeout)
		}
		payload := newFlareSolverrRequestGetPayload(p.URL, sessionName, maxTimeoutMS)
		resp, err := callFlareSolverr(fsURL, payload, httpTimeout)
		if err != nil {
			log.Printf("[flaresolverr][%s] 尝试 #%d 失败: %v", p.Name, i+1, err)
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
			log.Printf("[flaresolverr][%s] 尝试 #%d 返回挑战页，重建会话", p.Name, i+1)
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
		log.Printf("[flaresolverr][%s] 尝试 #%d 成功", p.Name, i+1)
		break
	}
	if lastErr != nil {
		return FetchResult{}, fmt.Errorf("FlareSolverr 重试后仍失败: %w", lastErr)
	}

	raw, err := extractByRegex(out.Solution.Response, p.PriceRegex)
	if err != nil {
		return FetchResult{}, err
	}
	log.Printf("[flaresolverr][%s] 价格匹配成功: raw=%s", p.Name, raw)
	price, err := parsePrice(raw)
	if err != nil {
		return FetchResult{}, fmt.Errorf("FlareSolverr 后备通道解析价格失败: %w", err)
	}
	merchantUpdate := extractMerchantUpdate(out.Solution.Response, p)
	if merchantUpdate == "" {
		merchantUpdate = chinaNow().Format("2006-01-02")
	}
	log.Printf("[flaresolverr][%s] 商户更新字段=%s", p.Name, merchantUpdate)
	return FetchResult{Price: price, RawPrice: raw, MerchantUpdate: merchantUpdate}, nil
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
	log.Printf("[flaresolverr] 调用接口: cmd=%s timeout=%s", cmd, timeout.String())
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, fsURL, bytes.NewReader(b))
	if err != nil {
		return flareSolverrResp{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return flareSolverrResp{}, fmt.Errorf("FlareSolverr 不可用(%s): %w", fsURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return flareSolverrResp{}, err
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
	log.Printf("[flaresolverr] 调用成功: cmd=%s", cmd)
	return out, nil
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
	return formatMerchantUpdate(normalizeText(content))
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
		return raw
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
	t, err := template.ParseFiles(filepath.Join(root, "index.html"), filepath.Join(root, "product.html"))
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
	appMux.HandleFunc("/api/products", a.handleProducts)
	appMux.HandleFunc("/api/products/", a.handleProductHistory)
	appMux.HandleFunc("/api/collect", a.handleCollect)
	appMux.HandleFunc("/api/fx/hkd-cny/today", a.handleTodayFX)

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
		log.Printf("[http][rid=%d] -> %s %s ip=%s ua=%q", reqID, r.Method, path, ip, ua)

		rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		panicked := false
		defer func() {
			log.Printf("[http][rid=%d] <- %s %s status=%d bytes=%d cost=%s panic=%t", reqID, r.Method, path, rec.statusCode, rec.bytes, time.Since(start), panicked)
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
		if err := rows.Scan(&rr.ID, &rr.Name, &rr.StoreName, &rr.URL, &rr.Currency, &rr.LastFetchOK, &rr.LastFetchAt, &rr.Latest, &rr.PrevPrice, &rr.LatestDate, &rr.LatestFrom, &rr.UpdatedAt, &rr.MerchantUpdate); err != nil {
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
	type req struct {
		URL       string `json:"url"`
		StoreName string `json:"store_name"`
	}
	start := time.Now()
	log.Printf("[add-product] 收到新增请求: remote=%s", r.RemoteAddr)
	var in req
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&in); err != nil {
		log.Printf("[add-product] 请求体解析失败: %v", err)
		http.Error(w, "请求体格式错误", http.StatusBadRequest)
		return
	}
	urlText := strings.TrimSpace(in.URL)
	store := strings.TrimSpace(in.StoreName)
	log.Printf("[add-product] 参数: url=%s store=%s", urlText, store)
	if urlText == "" {
		log.Printf("[add-product] 参数校验失败: url 为空")
		http.Error(w, "url 不能为空", http.StatusBadRequest)
		return
	}
	urlText = normalizeProductURL(urlText)
	log.Printf("[add-product] 标准化链接: %s", urlText)
	if store == "" {
		store = "Galaxy 銀河攝影器材"
		log.Printf("[add-product] 店铺为空，使用默认店铺=%s", store)
	}
	if !strings.Contains(urlText, "price.com.hk/product.php") {
		log.Printf("[add-product] 参数校验失败: 非 price.com.hk 商品链接")
		http.Error(w, "仅支持 price.com.hk 商品链接", http.StatusBadRequest)
		return
	}
	productID, err := parseProductIDFromURL(urlText)
	if err != nil {
		log.Printf("[add-product] 解析商品ID失败: url=%s err=%v", urlText, err)
		http.Error(w, "链接中缺少有效商品ID(p)", http.StatusBadRequest)
		return
	}
	log.Printf("[add-product] 解析商品ID成功: product_id=%s", productID)
	autoName, err := autoDetectProductName(urlText)
	if err != nil {
		log.Printf("[add-product] 自动抓取商品名称失败: url=%s err=%v", urlText, err)
		http.Error(w, "自动抓取商品名称失败: "+err.Error(), http.StatusBadGateway)
		return
	}
	name := buildAutoDisplayName(autoName, store)
	log.Printf("[add-product] 自动抓取商品名成功: %s", name)

	priceRegex := buildPriceRegex(store, productID)
	updateRegex := buildUpdateRegex(store)
	log.Printf("[add-product] 生成抓取规则: price_regex=%s update_regex=%s", priceRegex, updateRegex)
	if existedByURL, existedName, ok := findProductByStoreAndURL(a.db, store, urlText); ok {
		log.Printf("[add-product] 命中去重(店铺+链接): id=%d name=%s", existedByURL, existedName)
		if _, err := a.db.Exec(`UPDATE products
			SET url = ?,
			    price_regex = ?,
			    update_regex = ?,
			    currency = 'HKD',
			    active = 1,
			    updated_at = datetime('now', '+8 hours')
			WHERE id = ?`, urlText, priceRegex, updateRegex, existedByURL); err != nil {
			log.Printf("[add-product] 链接去重更新失败: id=%d err=%v", existedByURL, err)
			http.Error(w, "新增失败: "+err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("[add-product] 链接去重更新成功: id=%d", existedByURL)
		nameForUse := name
		if existedName != "" {
			nameForUse = existedName
		}
		a.collectAndRespondForAdd(w, start, existedByURL, nameForUse, store, urlText, priceRegex, updateRegex, true)
		return
	}
	if existedByPID, existedName, ok := findProductByStoreAndPID(a.db, store, productID); ok {
		log.Printf("[add-product] 命中去重(店铺+商品ID): id=%d name=%s", existedByPID, existedName)
		if _, err := a.db.Exec(`UPDATE products
			SET url = ?,
			    price_regex = ?,
			    update_regex = ?,
			    currency = 'HKD',
			    active = 1,
			    updated_at = datetime('now', '+8 hours')
			WHERE id = ?`, urlText, priceRegex, updateRegex, existedByPID); err != nil {
			log.Printf("[add-product] 去重更新失败: id=%d err=%v", existedByPID, err)
			http.Error(w, "新增失败: "+err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("[add-product] 去重更新成功: id=%d", existedByPID)
		nameForUse := name
		if existedName != "" {
			nameForUse = existedName
		}
		a.collectAndRespondForAdd(w, start, existedByPID, nameForUse, store, urlText, priceRegex, updateRegex, true)
		return
	}
	var existedID sql.NullInt64
	_ = a.db.QueryRow(`SELECT id FROM products WHERE name = ?`, name).Scan(&existedID)

	_, err = a.db.Exec(`INSERT INTO products(name, store_name, url, price_regex, update_regex, currency, active, created_at, updated_at)
	VALUES(?, ?, ?, ?, ?, 'HKD', 1, datetime('now', '+8 hours'), datetime('now', '+8 hours'))
	ON CONFLICT(name) DO UPDATE SET
	store_name=excluded.store_name,
	url=excluded.url,
	price_regex=excluded.price_regex,
	update_regex=excluded.update_regex,
	currency='HKD',
	active=1,
	updated_at=datetime('now', '+8 hours')`,
		name, store, urlText, priceRegex, updateRegex)
	if err != nil {
		log.Printf("[add-product] upsert 数据库记录失败: name=%s err=%v", name, err)
		http.Error(w, "新增失败: "+err.Error(), http.StatusBadRequest)
		return
	}

	var pid int64
	if err := a.db.QueryRow(`SELECT id FROM products WHERE name = ?`, name).Scan(&pid); err != nil || pid <= 0 {
		log.Printf("[add-product] upsert 成功但查询ID失败: err=%v pid=%d", err, pid)
		http.Error(w, "新增成功但无法获取商品ID", http.StatusInternalServerError)
		return
	}
	if existedID.Valid {
		log.Printf("[add-product] 商品已存在，已更新配置: id=%d name=%s", pid, name)
	} else {
		log.Printf("[add-product] 新增数据库记录成功: id=%d name=%s", pid, name)
	}
	a.collectAndRespondForAdd(w, start, pid, name, store, urlText, priceRegex, updateRegex, existedID.Valid)
}

func (a *App) collectAndRespondForAdd(
	w http.ResponseWriter,
	start time.Time,
	pid int64,
	name, store, urlText, priceRegex, updateRegex string,
	dedup bool,
) {
	newProduct := Product{
		ID:          pid,
		Name:        name,
		StoreName:   store,
		URL:         urlText,
		PriceRegex:  priceRegex,
		UpdateRegex: updateRegex,
		Currency:    "HKD",
		Active:      true,
	}
	log.Printf("[add-product][%s] 开始首次抓取: product_id=%d", name, pid)
	got, ferr := fetchPrice(newProduct)
	if ferr != nil {
		log.Printf("[add-product][%s] 首次抓取失败: %v", name, ferr)
		_ = insertFetchLog(a.db, pid, false, ferr.Error())
		_ = updateLastFetchStatus(a.db, pid, false, ferr.Error())
		writeJSON(w, map[string]any{
			"status":          "ok",
			"name":            normalizeProductDisplayName(name),
			"dedup":           dedup,
			"initial_collect": "failed",
			"error":           ferr.Error(),
		})
		return
	}
	day := chinaNow().Format("2006-01-02")
	log.Printf("[add-product][%s] 首次抓取成功，开始写入台账: day=%s", name, day)
	if err := upsertDailyPrice(a.db, pid, day, store, got.Price, got.RawPrice, got.MerchantUpdate, urlText); err != nil {
		log.Printf("[add-product][%s] 首次抓取入库失败: %v", name, err)
		_ = insertFetchLog(a.db, pid, false, err.Error())
		_ = updateLastFetchStatus(a.db, pid, false, err.Error())
		writeJSON(w, map[string]any{
			"status":          "ok",
			"name":            normalizeProductDisplayName(name),
			"dedup":           dedup,
			"initial_collect": "failed",
			"error":           err.Error(),
		})
		return
	}
	_ = insertFetchLog(a.db, pid, true, "")
	_ = updateLastFetchStatus(a.db, pid, true, "")
	log.Printf("[add-product][%s] 首次抓取与入库完成: price=%.2f merchant_update=%s", name, got.Price, got.MerchantUpdate)
	log.Printf("[add-product] 新增流程完成: id=%d name=%s dedup=%t duration=%s", pid, name, dedup, time.Since(start).String())

	writeJSON(w, map[string]any{
		"status":          "ok",
		"name":            normalizeProductDisplayName(name),
		"dedup":           dedup,
		"initial_collect": "success",
	})
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

func (a *App) handleDeleteProduct(w http.ResponseWriter, r *http.Request, idStr string) {
	type req struct {
		Confirm string `json:"confirm"`
	}
	var in req
	if err := json.NewDecoder(io.LimitReader(r.Body, 8*1024)).Decode(&in); err != nil {
		http.Error(w, "请求体格式错误", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(strings.ToUpper(in.Confirm)) != "DELETE" {
		http.Error(w, "确认码错误", http.StatusBadRequest)
		return
	}
	pid, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 64)
	if err != nil || pid <= 0 {
		http.NotFound(w, r)
		return
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
	writeJSON(w, map[string]any{"status": "ok"})
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
	return "Galaxy\\s*銀河攝影器材[\\s\\S]*?([0-9]{4}-[0-9]{2}-[0-9]{2}\\s*更新(?:[\\s\\S]{0,120}?(?:請先查詢|请先查询|少量存貨|少量存货|有現貨|有现货|現貨|现货|缺貨|缺货|預訂|预订|待定|離線|离线))?)"
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

func sendCollectFailureAlertToWeCom(day string, successCount, failCount int, failures []collectFailureDetail, blockedByWAF, flareConnIssue bool) error {
	webhook := weComWebhook()
	if webhook == "" {
		return fmt.Errorf("未配置 WECHAT_BOT_WEBHOOK")
	}
	now := chinaNow().Format("2006-01-02 15:04:05")
	builder := strings.Builder{}
	builder.WriteString("### 价格采集异常告警\n")
	builder.WriteString("> 时间: " + now + "\n")
	builder.WriteString("> 采集日期: " + trimAlertText(day, 32) + "\n")
	builder.WriteString(fmt.Sprintf("> 结果: success=%d failed=%d\n", successCount, failCount))
	if blockedByWAF {
		builder.WriteString("> 风险: 检测到 Cloudflare 挑战页\n")
	}
	if flareConnIssue {
		builder.WriteString("> 风险: 检测到 FlareSolverr 连接异常（172.25.0.102）\n")
		builder.WriteString("> 节点: " + trimAlertText(flareSolverrURL(), 120) + "\n")
	}
	if len(failures) > 0 {
		builder.WriteString("\n失败明细（最多 5 条）:\n")
		limit := len(failures)
		if limit > 5 {
			limit = 5
		}
		for i := 0; i < limit; i++ {
			f := failures[i]
			builder.WriteString(fmt.Sprintf("- %s（%s）：%s\n",
				trimAlertText(f.ProductName, 80),
				trimAlertText(f.StoreName, 60),
				trimAlertText(f.Err, 180),
			))
		}
	}
	return sendWeComMarkdown(webhook, builder.String())
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
		return fmt.Errorf("无可推送的价格数据")
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
	rows, err := db.Query(`
	SELECT p.name, p.store_name, r.price,
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
		var prev sql.NullFloat64
		if err := rows.Scan(&name, &store, &latest, &prev); err != nil {
			return nil, err
		}
		name = normalizeProductBaseName(normalizeProductDisplayName(name))
		if !latest.Valid {
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
