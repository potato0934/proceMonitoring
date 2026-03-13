# Price Monitoring (Go + SQLite)

一个用于监控商品价格的最小可用项目：
- 每日采集并写入 SQLite
- 网页查看价格走势与价格台账

## 1. 准备商品配置

编辑 `config/products.json`，每个商品需要：
- `name`: 商品名（唯一）
- `store_name`: 店铺标识（例如 `Galaxy 銀河攝影器材`、`順星數碼`）
- `url`: 商品页面 URL
- `price_regex`: 从页面源码中提取价格的正则（第 1 个捕获组必须是价格）
- `update_regex`: 提取商户价格更新时间（第 1 个捕获组，建议 `YYYY-MM-DD`）
- `currency`: 币种，默认 `CNY`
- `active`: 是否启用采集

### Price.com.hk + 指定商户水货价（Galaxy）

如果你要抓取 `Galaxy 銀河攝影器材` 的水货价，可用以下模式：

- `merchant_id=9146`（Galaxy）
- `tr_so=w`（水货）
- `tr_pw=...`（水货价格）

示例正则：

```json
"price_regex": "merchant_id=9146[\\\\s\\\\S]*?product_id=569595[\\\\s\\\\S]*?tr_pw=([0-9.]+)[\\\\s\\\\S]*?tr_so=w"
```

示例：

```json
[
  {
    "name": "示例商品A",
    "url": "https://example.com/product-a",
    "price_regex": "\\\"price\\\"\\s*:\\s*\\\"([0-9.,]+)\\\"",
    "currency": "CNY",
    "active": true
  }
]
```

## 2. 安装依赖并首次采集

```bash
go mod tidy
npm install
go run ./cmd/price-monitor collect
```

默认数据库文件是 `./data.db`。

## 3. 启动 Web

```bash
go run ./cmd/price-monitor serve
```

打开 `http://localhost:8080`：
- 首页：商品最新价格台账
- 商品详情页：价格走势图 + 近 365 天台账
- 首页展示店铺、商户更新时间（例如 `2026-03-06` 换行 `更新`）

## 4. 内置定时采集（推荐）

每天 `09:00`、`11:00`、`15:00` 自动采集：

```bash
go run ./cmd/price-monitor schedule
```

`schedule` 模式在启用 FlareSolverr 后备通道时，会在启动时以及每次采集前自动检查 FlareSolverr 可用性；
若不可用会报错或跳过本轮，避免无效采集。

可配置项：

- `COLLECT_TIMES`：逗号分隔的时间点，格式 `HH:MM`，默认 `09:00,11:00,15:00`
- `PUSH_TIMES`：逗号分隔的推送时间点，格式 `HH:MM`，默认 `12:00`（企业微信机器人价格推送）
- `SCHEDULE_TZ`：时区，默认 `Asia/Shanghai`

示例：

```bash
COLLECT_TIMES=09:00,11:00,15:00 PUSH_TIMES=12:00 SCHEDULE_TZ=Asia/Shanghai go run ./cmd/price-monitor schedule
```

## 4.1 企业微信通知配置

1. 在企业微信中进入：`群聊` -> `群机器人` -> `添加机器人`。
2. 复制机器人 Webhook（形如 `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxx`）。
3. 启动前设置环境变量：

```bash
export WECHAT_BOT_WEBHOOK="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxx"
```

4. 手动测试发送（立即推送一次当前数据库价格）：

```bash
go run ./cmd/price-monitor notify
```

5. 定时推送由 `schedule` 自动执行，时间通过 `PUSH_TIMES` 配置（默认 `12:00`）。

告警通知（已内置）：

- 当 FlareSolverr 目标为 `172.25.0.102` 且连接不可达时，会自动发送企业微信告警（含错误原因）。
- 当本轮采集出现失败项（`failed > 0`）时，会自动发送企业微信告警（含失败摘要）。

## 5. 定时每日采集（macOS/Linux cron，可选）

每天 09:00、11:00、15:00 自动执行：

```cron
0 9,11,15 * * * cd /Users/potato/Downloads/PriceMonitoring && /usr/local/bin/go run ./cmd/price-monitor collect >> cron.log 2>&1
```

## 6. 时区与历史数据

- 程序写入数据库的时间统一为中国时区（`Asia/Shanghai`）。
- 页面“最后更新时间”展示到秒（`YYYY-MM-DD HH:mm:ss`，中国时区）。
- 启动后会自动执行一次历史时间迁移，把旧的 UTC 记录批量转换为中国时区（仅执行一次）。

## 环境变量

- `DB_PATH`：SQLite 路径（默认 `./data.db`）
- `PRODUCTS_CONFIG`：配置文件路径（默认 `./config/products.json`）
- `ADDR`：Web 服务地址（默认 `:8080`）
- `TEMPLATE_DIR`：模板目录（默认 `./templates`）
- `LOG_DIR`：日志目录（默认 `./logs`，按天切割文件，例如 `logs/2026-03-06.log`）
- `LOG_RETENTION_DAYS`：日志保留天数（默认 `7`，程序每天自动清理更早日志）
- `BASE_PATH`：Web 路径前缀（默认空；例如设为 `/price` 后，页面地址为 `http://host:port/price/`）
- `WECHAT_BOT_WEBHOOK`：企业微信机器人 Webhook 地址（用于 `PUSH_TIMES` 推送）

## 命令说明

- `go run ./cmd/price-monitor collect`：执行一次采集
- `go run ./cmd/price-monitor serve`：启动 Web 页面
- `go run ./cmd/price-monitor schedule`：启动定时采集 + 定时企业微信推送
- `go run ./cmd/price-monitor notify`：手动推送一次企业微信通知（便于测试）

## 403 优化说明

程序已内置以下反 403 优化：

- 模拟桌面浏览器请求头（`Accept` / `Accept-Language` / `Sec-Fetch-*` 等）
- 先访问站点首页预热 Cookie，再访问商品页
- 多 User-Agent 重试
- 对 403/429 自动退避重试
- HTTP 失败后自动启用浏览器后备通道（Go + Playwright）

如果仍然 403，程序会使用代码内置的 `cf_clearance` Cookie 尝试请求。

浏览器后备通道开关：

- `ENABLE_BROWSER_FALLBACK=true`（默认启用）
- `ENABLE_BROWSER_FALLBACK=false`（禁用）
- `PLAYWRIGHT_HEADLESS=true`（默认）或 `false`（可见浏览器，便于手动过挑战）
- `PLAYWRIGHT_USER_DATA_DIR=/path/to/browser-profile`（可选，持久化 cookie）
- `PLAYWRIGHT_MANUAL_WAIT_SECONDS=120`（可选，仅在 `PLAYWRIGHT_HEADLESS=false` 时有意义，给人工过验证的等待秒数）
- `PLAYWRIGHT_BROWSER_PATH=/usr/bin/chromium`（可选，指定浏览器可执行文件）
- `PLAYWRIGHT_TIMEOUT_MS=60000`（可选，单次浏览器抓取超时）
- `PLAYWRIGHT_EXEC_TIMEOUT_SECONDS=90`（可选，Playwright 子进程最大执行时长）
- `PLAYWRIGHT_WAIT_AFTER_LOAD_MS=3000`（可选，页面加载后额外等待）
- `PLAYWRIGHT_CHALLENGE_WAIT_MS=20000`（可选，挑战页检测等待窗口）
- `PLAYWRIGHT_MAX_RELOADS=2`（可选，挑战时自动重载次数）
- `PLAYWRIGHT_SCRIPT_PATH=./scripts/playwright_fetch.js`（可选，脚本路径）
- `PLAYWRIGHT_NODE_BIN=node`（可选，Node 可执行路径）
- `ENABLE_FLARESOLVERR_FALLBACK=true`（默认启用 FlareSolverr）
- `COLLECT_ITEM_DELAY_MS=800`（可选，商品之间采集间隔，降低反爬触发概率）

注意：浏览器后备通道需要系统安装 Node.js + Chromium，并安装 `playwright-core`（容器镜像已内置）。
本地 macOS 若未设置 `PLAYWRIGHT_BROWSER_PATH`，程序会尝试自动探测 Chrome/Chromium；仍失败时请执行 `npx playwright install chromium`。

CentOS7 Docker 生产环境建议：

- `PLAYWRIGHT_DISABLE_SANDBOX=true`
- `PLAYWRIGHT_BROWSER_PATH=/usr/bin/chromium`
- `shm_size: 1gb`
- `security_opt: ["seccomp=unconfined"]`

以上配置已在仓库 `docker-compose.yml` 中给出默认值，避免旧内核环境下 Chromium 启动失败。

如果你遇到 Cloudflare 强挑战，建议使用“人工辅助模式”（最稳）：

```bash
PLAYWRIGHT_HEADLESS=false \
PLAYWRIGHT_USER_DATA_DIR=./browser-profile \
PLAYWRIGHT_MANUAL_WAIT_SECONDS=180 \
go run ./cmd/price-monitor collect
```

执行后会打开浏览器，你手动过一次验证，程序会在等待窗口内自动继续抓取；后续运行会复用 `./chrome-profile` 中的 cookie。

## Cloudflare 强挑战（推荐 FlareSolverr）

当站点返回 `cf-mitigated: challenge` 时，建议启用 FlareSolverr 后备通道。

1. 启动 FlareSolverr（示例）：

```bash
docker run -d --name flaresolverr -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest
```

2. 采集时启用：

```bash
FLARESOLVERR_URL='http://172.25.0.102:8191/v1' go run ./cmd/price-monitor collect
```

说明：如果未设置 `FLARESOLVERR_URL`，程序会使用构建时默认值（当前默认 `http://172.25.0.102:8191/v1`）。
程序会自动创建并复用会话 `pricemonitor`（可通过 `FLARESOLVERR_SESSION` 覆盖），并在挑战页时自动重建会话重试。

推荐在容器中固定以下参数（已写入仓库 `docker-compose.yml`）：

- `FLARESOLVERR_URL=http://172.25.0.102:8191/v1`
- `FLARESOLVERR_MAX_TIMEOUT_MS=180000`
- `FLARESOLVERR_HTTP_TIMEOUT_SECONDS=240`
- `FLARESOLVERR_MAX_RETRIES=3`
- `FLARESOLVERR_WARMUP_HOME=false`

可选：

- `FLARESOLVERR_SESSION`：指定会话名，复用挑战结果与 cookie。

## Docker 部署（构建时替换默认值）

项目提供 `Dockerfile`，可在构建阶段注入默认参数（不影响本地 `go run` 开发）：

- `DEFAULT_FLARESOLVERR_URL`：容器内默认 FlareSolverr 地址
- `DEFAULT_BASE_PATH`：容器内默认路径前缀

示例（符合你当前服务器需求）：

```bash
docker build -t price-monitor:latest \
  --build-arg DEFAULT_FLARESOLVERR_URL=http://172.25.0.102:8191/v1 \
  --build-arg DEFAULT_BASE_PATH=/price \
  .
```

运行：

```bash
docker run -d --name price-monitor \
  -p 33333:8080 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  price-monitor:latest
```

默认容器内会同时启动：

- `serve`（Web 服务）
- `schedule`（定时采集）

默认调度时间为中国时区：采集 `09:00,11:00,15:00`，推送 `12:00`。  
如需覆盖可在 `docker run` 时传入：

```bash
-e COLLECT_TIMES="09:00,11:00,15:00" -e PUSH_TIMES="12:00" -e SCHEDULE_TZ="Asia/Shanghai" -e WECHAT_BOT_WEBHOOK="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"
```

此时访问地址：

- `http://<服务器IP>:33333/price/`

### 宿主机持久化到 `/root/priceMonitoring`

如果你要固定保存到宿主机 `/root/priceMonitoring`，可直接使用仓库内 `docker-compose.yml`：

```bash
mkdir -p /root/priceMonitoring/data /root/priceMonitoring/logs
docker compose up -d --force-recreate
```

映射关系：

- `/root/priceMonitoring/data` -> `/app/data`（SQLite 数据库）
- `/root/priceMonitoring/logs` -> `/app/logs`（日志）

### 一键部署脚本

仓库已提供一键部署脚本（打包源码、上传服务器、重建镜像、重启容器）：

```bash
cd /Users/potato/Downloads/PriceMonitoring
./scripts/deploy_remote.sh
```

可选参数（环境变量）：

```bash
REMOTE_HOST=172.25.1.239 \
REMOTE_USER=root \
RUN_COLLECT_AFTER_DEPLOY=true \
./scripts/deploy_remote.sh
```

说明：默认部署到 `172.25.1.239`，默认镜像标签 `price-monitor:0.2`，默认 FlareSolverr 地址 `http://172.25.0.102:8191/v1`。
