#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUNTIME_PROFILE="${MISSION_CONTROL_RUNTIME_PROFILE:-host}"
COMPOSE_FILE="$PROJECT_DIR/platform/infra/docker/compose.voice-runtime.yml"
PROFILE_RESOLVER="$PROJECT_DIR/platform/bin/runtime/resolve_runtime_profile.py"
PORT="${MISSION_CONTROL_SPEECH_RUNTIME_PORT:-}"
JSON_OUTPUT=0
PID_FILE=""

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
  local message="${3:-}"
  python3 - "$ok" "$state" "$message" <<'PY'
import json, sys
ok, state, message = sys.argv[1:]
payload = {
    "ok": ok == "1",
    "state": state,
    "message": message or None,
}
print(json.dumps(payload, ensure_ascii=False))
PY
}

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/stop_voice_runtime.sh [选项]

选项:
  --project-dir PATH
  --json
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      COMPOSE_FILE="$PROJECT_DIR/platform/infra/docker/compose.voice-runtime.yml"
      shift 2
      ;;
    --runtime-profile)
      RUNTIME_PROFILE="$2"
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

PID_FILE="$PROJECT_DIR/local/logs/voice-runtime-host.pid"
if [[ -z "$PORT" ]]; then
  PORT="$(resolve_runtime_profile_value speechRuntime.port "8090")"
fi

if [[ "$RUNTIME_PROFILE" == "host" ]]; then
  host_pid=""
  if [[ -f "$PID_FILE" ]]; then
    host_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  fi
  if [[ -z "$host_pid" ]]; then
    host_pid="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
  fi
  if [[ -n "$host_pid" ]] && kill -0 "$host_pid" >/dev/null 2>&1; then
    kill "$host_pid" >/dev/null 2>&1 || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$host_pid" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if kill -0 "$host_pid" >/dev/null 2>&1; then
      kill -9 "$host_pid" >/dev/null 2>&1 || true
    fi
  fi
  rm -f "$PID_FILE"
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 1 "stopped" "语音服务已停止（host 本机进程）"
  else
    echo "[✓] 语音服务已停止（host 本机进程）"
  fi
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "unavailable" "docker CLI 不可用"
  else
    echo "[✗] docker CLI 不可用"
  fi
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "unavailable" "Docker daemon 未运行"
  else
    echo "[✗] Docker daemon 未运行"
  fi
  exit 1
fi

docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null

if [[ "$JSON_OUTPUT" == "1" ]]; then
  emit_json 1 "stopped" "语音服务已停止"
else
  echo "[✓] 语音服务已停止"
fi
