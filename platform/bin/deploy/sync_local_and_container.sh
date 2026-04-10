#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OPENCLAW_DIR="$HOME/.openclaw"
BUILD_FRONTEND=0
SYNC_LOCAL=1
SYNC_CONTAINER=1
WAIT_HEALTH=1
SERVICE_NAME="mission-control"
COMPOSE_FILE=""
HEALTH_URL=""

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/sync_local_and_container.sh [选项]

选项:
  --dir PATH
  --project-dir PATH
  --compose-file PATH
  --service NAME
  --build-frontend
  --skip-local
  --skip-container
  --skip-health-wait
  --health-url URL
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      OPENCLAW_DIR="$2"
      shift 2
      ;;
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --compose-file)
      COMPOSE_FILE="$2"
      shift 2
      ;;
    --service)
      SERVICE_NAME="$2"
      shift 2
      ;;
    --build-frontend)
      BUILD_FRONTEND=1
      shift
      ;;
    --skip-local)
      SYNC_LOCAL=0
      shift
      ;;
    --skip-container)
      SYNC_CONTAINER=0
      shift
      ;;
    --skip-health-wait)
      WAIT_HEALTH=0
      shift
      ;;
    --health-url)
      HEALTH_URL="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "[✗] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

OPENCLAW_DIR="$(python3 - "$OPENCLAW_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
PROJECT_DIR="$(python3 - "$PROJECT_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

if [[ -z "$COMPOSE_FILE" ]]; then
  COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
fi
COMPOSE_FILE="$(python3 - "$COMPOSE_FILE" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

if [[ -z "$HEALTH_URL" ]]; then
  HEALTH_URL="http://127.0.0.1:${MISSION_CONTROL_PORT:-18890}/api/auth/session"
fi

if [[ "$SYNC_LOCAL" -eq 1 ]]; then
  echo "[*] 同步本机 OpenClaw runtime: $OPENCLAW_DIR"
  SYNC_ARGS=(--dir "$OPENCLAW_DIR" --project-dir "$PROJECT_DIR")
  if [[ "$BUILD_FRONTEND" -eq 1 ]]; then
    SYNC_ARGS+=(--build-frontend)
  fi
  bash "$PROJECT_DIR/platform/bin/runtime/sync_runtime_assets.sh" "${SYNC_ARGS[@]}"
  bash "$PROJECT_DIR/platform/bin/verify/validate.sh" --dir "$OPENCLAW_DIR"
fi

if [[ "$SYNC_CONTAINER" -eq 1 ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "[✗] 未检测到 docker，无法同步容器版本。" >&2
    exit 1
  fi
  if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "[✗] 缺少 compose 文件: $COMPOSE_FILE" >&2
    exit 1
  fi
  echo "[*] 重建并重启容器服务: $SERVICE_NAME"
  docker compose -f "$COMPOSE_FILE" up -d --build "$SERVICE_NAME"
  if [[ "$WAIT_HEALTH" -eq 1 ]]; then
    container_id="$(docker compose -f "$COMPOSE_FILE" ps -q "$SERVICE_NAME")"
    if [[ -z "$container_id" ]]; then
      echo "[✗] 未找到容器服务: $SERVICE_NAME" >&2
      exit 1
    fi
    echo "[*] 等待容器健康检查通过..."
    for _ in $(seq 1 90); do
      health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
      if [[ "$health" == "healthy" || "$health" == "running" ]]; then
        if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
          echo "[✓] 容器版本已同步并可访问: $HEALTH_URL"
          break
        fi
      fi
      sleep 2
    done
    if ! curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
      echo "[✗] 容器已启动，但健康检查未通过: $HEALTH_URL" >&2
      exit 1
    fi
  fi
fi

echo "[✓] 本机与容器同步完成"
