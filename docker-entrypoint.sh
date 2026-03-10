#!/bin/sh
set -eu

term_handler() {
  if [ -n "${SCHEDULE_PID:-}" ]; then
    kill "$SCHEDULE_PID" 2>/dev/null || true
  fi
  if [ -n "${SERVE_PID:-}" ]; then
    kill "$SERVE_PID" 2>/dev/null || true
  fi
  wait || true
  exit 0
}

trap term_handler INT TERM

echo "[entrypoint] start schedule"
/app/price-monitor schedule &
SCHEDULE_PID=$!

echo "[entrypoint] start serve on ADDR=${ADDR:-:8080}"
/app/price-monitor serve &
SERVE_PID=$!

# 监控两个子进程，任一退出就结束容器并输出原因。
while true; do
  if ! kill -0 "$SCHEDULE_PID" 2>/dev/null; then
    wait "$SCHEDULE_PID" || SCHEDULE_CODE=$?
    SCHEDULE_CODE=${SCHEDULE_CODE:-0}
    echo "[entrypoint] schedule exited with code=${SCHEDULE_CODE}"
    kill "$SERVE_PID" 2>/dev/null || true
    wait "$SERVE_PID" 2>/dev/null || true
    exit "$SCHEDULE_CODE"
  fi

  if ! kill -0 "$SERVE_PID" 2>/dev/null; then
    wait "$SERVE_PID" || SERVE_CODE=$?
    SERVE_CODE=${SERVE_CODE:-0}
    echo "[entrypoint] serve exited with code=${SERVE_CODE}"
    kill "$SCHEDULE_PID" 2>/dev/null || true
    wait "$SCHEDULE_PID" 2>/dev/null || true
    exit "$SERVE_CODE"
  fi

  sleep 1
done
