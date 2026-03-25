#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

REMOTE_HOST="${REMOTE_HOST:-172.25.1.239}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE="${REMOTE_USER}@${REMOTE_HOST}"

REMOTE_SRC_DIR="${REMOTE_SRC_DIR:-/root/priceMonitoring-src}"
REMOTE_RUNTIME_DIR="${REMOTE_RUNTIME_DIR:-/root/priceMonitoring}"
IMAGE_TAG="${IMAGE_TAG:-price-monitor:0.2}"
DEFAULT_FLARESOLVERR_URL="${DEFAULT_FLARESOLVERR_URL:-http://172.25.0.102:8191/v1}"
RUN_COLLECT_AFTER_DEPLOY="${RUN_COLLECT_AFTER_DEPLOY:-false}"
INSTALL_BROWSER_DEPS="${INSTALL_BROWSER_DEPS:-false}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  ./scripts/deploy_remote.sh

Optional env vars:
  REMOTE_HOST=172.25.1.239
  REMOTE_USER=root
  REMOTE_SRC_DIR=/root/priceMonitoring-src
  REMOTE_RUNTIME_DIR=/root/priceMonitoring
  IMAGE_TAG=price-monitor:0.2
  DEFAULT_FLARESOLVERR_URL=http://172.25.0.102:8191/v1
  RUN_COLLECT_AFTER_DEPLOY=true
  INSTALL_BROWSER_DEPS=false
EOF
  exit 0
fi

PACKAGE_NAME="pricemonitor-deploy-$(date +%Y%m%d-%H%M%S).tgz"
LOCAL_PACKAGE="/tmp/${PACKAGE_NAME}"
REMOTE_PACKAGE="/tmp/${PACKAGE_NAME}"

echo "[deploy] 1/4 package source -> ${LOCAL_PACKAGE}"
(
  cd "${ROOT_DIR}"
  export COPYFILE_DISABLE=1
  export COPY_EXTENDED_ATTRIBUTES_DISABLE=1
  tar czf "${LOCAL_PACKAGE}" \
    Dockerfile \
    docker-entrypoint.sh \
    go.mod \
    go.sum \
    package.json \
    package-lock.json \
    cmd \
    config \
    scripts \
    static \
    templates \
    docker-compose.yml \
    README.md
)

echo "[deploy] 2/4 upload package -> ${REMOTE}:${REMOTE_PACKAGE}"
scp -o StrictHostKeyChecking=no "${LOCAL_PACKAGE}" "${REMOTE}:${REMOTE_PACKAGE}"

echo "[deploy] 3/4 remote build + restart container"
ssh -o StrictHostKeyChecking=no "${REMOTE}" \
  "REMOTE_SRC_DIR='${REMOTE_SRC_DIR}' REMOTE_RUNTIME_DIR='${REMOTE_RUNTIME_DIR}' IMAGE_TAG='${IMAGE_TAG}' DEFAULT_FLARESOLVERR_URL='${DEFAULT_FLARESOLVERR_URL}' RUN_COLLECT_AFTER_DEPLOY='${RUN_COLLECT_AFTER_DEPLOY}' INSTALL_BROWSER_DEPS='${INSTALL_BROWSER_DEPS}' REMOTE_PACKAGE='${REMOTE_PACKAGE}' bash -s" <<'REMOTE_EOF'
set -euo pipefail

mkdir -p "${REMOTE_SRC_DIR}" "${REMOTE_RUNTIME_DIR}/data" "${REMOTE_RUNTIME_DIR}/logs"
rm -rf "${REMOTE_SRC_DIR:?}/"*
tar xzf "${REMOTE_PACKAGE}" -C "${REMOTE_SRC_DIR}"
find "${REMOTE_SRC_DIR}" -name '._*' -type f -delete || true

docker build \
  -t "${IMAGE_TAG}" \
  --build-arg DEFAULT_FLARESOLVERR_URL="${DEFAULT_FLARESOLVERR_URL}" \
  --build-arg INSTALL_BROWSER_DEPS="${INSTALL_BROWSER_DEPS}" \
  "${REMOTE_SRC_DIR}"

docker rm -f price-monitor >/dev/null 2>&1 || true
docker run -d \
  --name price-monitor \
  --restart always \
  --network bridge \
  -p 8080:8080 \
  --user root \
  --env-file "${REMOTE_RUNTIME_DIR}/price-monitor.env" \
  -v "${REMOTE_RUNTIME_DIR}/data:/app/data" \
  -v "${REMOTE_RUNTIME_DIR}/logs:/app/logs" \
  "${IMAGE_TAG}" >/dev/null

docker ps --filter name=price-monitor --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}'
docker logs --tail 60 price-monitor

if [[ "${RUN_COLLECT_AFTER_DEPLOY}" == "true" ]]; then
  echo "[deploy] run collect once"
  docker exec price-monitor /app/price-monitor collect
fi

rm -f "${REMOTE_PACKAGE}"
REMOTE_EOF

echo "[deploy] 4/4 cleanup local package"
rm -f "${LOCAL_PACKAGE}"

echo "[deploy] done"
