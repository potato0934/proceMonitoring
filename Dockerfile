FROM golang:1.24-alpine AS builder

WORKDIR /src

ARG GOPROXY=https://goproxy.cn,direct
ARG GOSUMDB=sum.golang.google.cn
ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=${GOSUMDB}

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    sh -c 'for i in 1 2 3; do \
      echo "[build] go mod download attempt ${i}/3 (GOPROXY=${GOPROXY})"; \
      go mod download && exit 0; \
      sleep $((i*3)); \
    done; \
    exit 1'

COPY . .

ARG DEFAULT_FLARESOLVERR_URL=http://172.25.0.102:8191/v1

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "-s -w -X main.defaultFlareSolverrURL=${DEFAULT_FLARESOLVERR_URL}" \
    -o /out/price-monitor ./cmd/price-monitor

FROM alpine:3.21

WORKDIR /app

ARG INSTALL_BROWSER_DEPS=true

RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    && if [ "${INSTALL_BROWSER_DEPS}" = "true" ]; then \
      apk add --no-cache \
        chromium \
        harfbuzz \
        nss \
        nodejs \
        npm \
        ttf-freefont; \
    fi \
    && adduser -D -u 10001 appuser

COPY --from=builder /out/price-monitor /app/price-monitor
COPY package.json /app/package.json
COPY package-lock.json /app/package-lock.json
COPY templates /app/templates
COPY static /app/static
COPY config /app/config
COPY scripts /app/scripts
COPY docker-entrypoint.sh /app/docker-entrypoint.sh

RUN if [ "${INSTALL_BROWSER_DEPS}" = "true" ]; then \
      npm ci --omit=dev --no-audit --no-fund; \
    fi \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
    && chmod +x /app/docker-entrypoint.sh \
    && mkdir -p /app/data /app/logs \
    && chown -R appuser:appuser /app

USER appuser

ENV ADDR=:8080
ENV DB_PATH=/app/data/data.db
ENV PRODUCTS_CONFIG=/app/config/products.json
ENV TEMPLATE_DIR=/app/templates
ENV LOG_DIR=/app/logs
ENV COLLECT_TIMES=10:00,11:00,15:00
ENV PUSH_TIMES=12:00
ENV SCHEDULE_TZ=Asia/Shanghai
ENV TZ=Asia/Shanghai
ENV PLAYWRIGHT_BROWSER_PATH=/usr/bin/chromium
ENV PLAYWRIGHT_HEADLESS=true

EXPOSE 8080

CMD ["/app/docker-entrypoint.sh"]
