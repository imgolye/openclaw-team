#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUNTIME_PROFILE="${MISSION_CONTROL_RUNTIME_PROFILE:-host}"
PROFILE_RESOLVER="$PROJECT_DIR/platform/bin/runtime/resolve_runtime_profile.py"
PORT="${MISSION_CONTROL_HOST_PORT:-}"
LOG_DIR="$PROJECT_DIR/local/logs"
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
  python3 - "$ok" "$state" "$pid" "$message" <<'PY'
import json, sys
ok, state, pid, message = sys.argv[1:]
payload = {
    "ok": ok == "1",
    "state": state,
    "pid": int(pid) if pid.isdigit() else None,
    "message": message or None,
}
print(json.dumps(payload, ensure_ascii=False))
PY
}

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/stop_host_product.sh [选项]

选项:
  --project-dir PATH
  --runtime-profile NAME
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

if [[ -z "$PORT" ]]; then
  PORT="$(resolve_runtime_profile_value product.port "18891")"
fi
export MISSION_CONTROL_RUNTIME_PROFILE="$RUNTIME_PROFILE"

PID_FILE="$LOG_DIR/host-product-${PORT}.pid"

PROJECT_DIR="$(python3 - "$PROJECT_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
PID_FILE="$PROJECT_DIR/local/logs/host-product-${PORT}.pid"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid"
    for _ in $(seq 1 15); do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        rm -f "$PID_FILE"
        if [[ "$JSON_OUTPUT" == "1" ]]; then
          emit_json 1 "stopped" "$pid" "host 产品已停止"
        else
          echo "[✓] host 产品已停止: pid=$pid"
        fi
        exit 0
      fi
      sleep 1
    done
    kill -9 "$pid" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 1 "stopped" "$pid" "host 产品已强制停止"
    else
      echo "[✓] host 产品已强制停止: pid=$pid"
    fi
    exit 0
  fi
  rm -f "$PID_FILE"
fi

listener="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN || true)"
if [[ -n "$listener" ]]; then
  kill $listener >/dev/null 2>&1 || true
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 1 "stopped" "$(echo "$listener" | head -n 1)" "已释放端口监听"
  else
    echo "[✓] 已释放端口监听: $PORT"
  fi
  exit 0
fi

if [[ "$JSON_OUTPUT" == "1" ]]; then
  emit_json 1 "stopped" "" "host 产品未运行"
else
  echo "[*] host 产品未运行"
fi
