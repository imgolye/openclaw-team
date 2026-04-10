#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUNTIME_PROFILE="${MISSION_CONTROL_RUNTIME_PROFILE:-host}"
PROFILE_RESOLVER="$PROJECT_DIR/platform/bin/runtime/resolve_runtime_profile.py"
HOST="${MISSION_CONTROL_HOST_BIND:-}"
PORT="${MISSION_CONTROL_HOST_PORT:-}"
JSON_OUTPUT=0

resolve_runtime_profile_value() {
  local field="$1"
  local fallback="${2:-}"
  local value=""
  if [[ -f "$PROFILE_RESOLVER" ]]; then
    value="$(python3 "$PROFILE_RESOLVER" --project-dir "$PROJECT_DIR" --profile "$RUNTIME_PROFILE" --field "$field" 2>/dev/null || true)"
  fi
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf '%s' "$fallback"
  fi
}

emit_json() {
  local ok="$1"
  local state="$2"
  local pid="${3:-}"
  local message="${4:-}"
  python3 - "$ok" "$state" "$pid" "$HOST" "$PORT" "$PID_FILE" "$LOG_FILE" "$message" <<'PY'
import json, sys
ok, state, pid, host, port, pid_file, log_file, message = sys.argv[1:]
payload = {
    "ok": ok == "1",
    "state": state,
    "pid": int(pid) if pid.isdigit() else None,
    "url": f"http://{host}:{port}",
    "pidFile": pid_file,
    "logFile": log_file,
    "message": message or None,
}
print(json.dumps(payload, ensure_ascii=False))
PY
}

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/status_host_product.sh [选项]

选项:
  --project-dir PATH
  --runtime-profile NAME
  --host HOST
  --port PORT
  --json
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --runtime-profile)
      RUNTIME_PROFILE="$2"
      shift 2
      ;;
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --json)
      JSON_OUTPUT=1
      shift
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

if [[ -z "$HOST" ]]; then
  HOST="$(resolve_runtime_profile_value product.host "127.0.0.1")"
fi
if [[ -z "$PORT" ]]; then
  PORT="$(resolve_runtime_profile_value product.port "18891")"
fi
export MISSION_CONTROL_RUNTIME_PROFILE="$RUNTIME_PROFILE"

PROJECT_DIR="$(python3 - "$PROJECT_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
PID_FILE="$PROJECT_DIR/local/logs/host-product-${PORT}.pid"
LOG_FILE="$PROJECT_DIR/local/logs/host-product-${PORT}.log"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    if curl -fsS "http://$HOST:$PORT/api/auth/session" >/dev/null 2>&1; then
      if [[ "$JSON_OUTPUT" == "1" ]]; then
        emit_json 1 "running" "$pid" "host 产品运行中"
      else
        echo "[✓] host 产品运行中: pid=$pid url=http://$HOST:$PORT"
      fi
      exit 0
    fi
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 0 "degraded" "$pid" "host 产品进程存在但接口未就绪"
    else
      echo "[!] host 产品进程存在但接口未就绪: pid=$pid url=http://$HOST:$PORT"
    fi
    exit 1
  fi
fi

listener_pid="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
if [[ -n "$listener_pid" ]]; then
  if curl -fsS "http://$HOST:$PORT/api/auth/session" >/dev/null 2>&1; then
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 1 "running" "$listener_pid" "host 产品运行中（未托管 PID）"
    else
      echo "[!] 端口 $PORT 有监听，服务可用，但不在 PID 文件管理内。"
    fi
    exit 0
  fi
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "port_conflict" "$listener_pid" "端口有监听，但服务未就绪"
  else
    echo "[!] 端口 $PORT 有监听，但不在 PID 文件管理内。"
  fi
  exit 1
fi

if [[ "$JSON_OUTPUT" == "1" ]]; then
  emit_json 1 "stopped" "" "host 产品未运行"
else
  echo "[*] host 产品未运行"
fi
