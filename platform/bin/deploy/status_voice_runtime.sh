#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUNTIME_PROFILE="${MISSION_CONTROL_RUNTIME_PROFILE:-host}"
PROFILE_RESOLVER="$PROJECT_DIR/platform/bin/runtime/resolve_runtime_profile.py"
COMPOSE_FILE="$PROJECT_DIR/platform/infra/docker/compose.voice-runtime.yml"
SERVICE_NAME="sherpa-onnx-tts"
PORT="${MISSION_CONTROL_SPEECH_RUNTIME_PORT:-}"
HEALTH_URL="${MISSION_CONTROL_SPEECH_RUNTIME_HEALTH_URL:-}"
JSON_OUTPUT=0
LOG_DIR=""
PID_FILE=""

voice_runtime_ready() {
  if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1; then
    return 0
  fi
  local models_url="${HEALTH_URL%/healthz}/v1/models"
  curl -fsS --max-time 5 "$models_url" >/dev/null 2>&1
}

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
  local container_id="${3:-}"
  local message="${4:-}"
  python3 - "$ok" "$state" "$container_id" "$PORT" "$HEALTH_URL" "$message" <<'PY'
import json, sys
ok, state, container_id, port, health_url, message = sys.argv[1:]
payload = {
    "ok": ok == "1",
    "state": state,
    "containerId": container_id or None,
    "port": int(port) if str(port).isdigit() else None,
    "healthUrl": health_url or None,
    "message": message or None,
}
print(json.dumps(payload, ensure_ascii=False))
PY
}

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/status_voice_runtime.sh [选项]

选项:
  --project-dir PATH
  --runtime-profile NAME
  --port PORT
  --health-url URL
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
    --health-url)
      HEALTH_URL="$2"
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
  PORT="$(resolve_runtime_profile_value speechRuntime.port "8090")"
fi
if [[ -z "$HEALTH_URL" ]]; then
  HEALTH_URL="$(resolve_runtime_profile_value speechRuntime.healthUrl "http://127.0.0.1:${PORT}/healthz")"
fi
export MISSION_CONTROL_RUNTIME_PROFILE="$RUNTIME_PROFILE"
LOG_DIR="$PROJECT_DIR/local/logs"
PID_FILE="$LOG_DIR/voice-runtime-host.pid"

if [[ "$RUNTIME_PROFILE" == "host" ]]; then
  host_pid=""
  if [[ -f "$PID_FILE" ]]; then
    host_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  fi
  if [[ -z "$host_pid" ]]; then
    host_pid="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
    if [[ -n "$host_pid" ]]; then
      echo "$host_pid" > "$PID_FILE"
    fi
  fi
  if [[ -n "$host_pid" ]] && kill -0 "$host_pid" >/dev/null 2>&1; then
    if voice_runtime_ready; then
      if [[ "$JSON_OUTPUT" == "1" ]]; then
        emit_json 1 "running" "$host_pid" "语音服务运行中（host 本机进程）"
      else
        echo "[✓] 语音服务运行中（host 本机进程）: $HEALTH_URL"
      fi
      exit 0
    fi
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 0 "degraded" "$host_pid" "host 语音服务进程存在，但探活未通过"
    else
      echo "[!] host 语音服务进程存在，但探活未通过: $HEALTH_URL"
    fi
    exit 1
  fi
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 1 "stopped" "" "语音服务未运行（host 本机进程）"
  else
    echo "[*] 语音服务未运行（host 本机进程）"
  fi
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "unavailable" "" "docker CLI 不可用"
  else
    echo "[✗] docker CLI 不可用"
  fi
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "unavailable" "" "Docker daemon 未运行"
  else
    echo "[✗] Docker daemon 未运行"
  fi
  exit 1
fi

container_id="$(docker compose -f "$COMPOSE_FILE" ps -q "$SERVICE_NAME" 2>/dev/null || true)"
if [[ -z "$container_id" ]]; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 1 "stopped" "" "语音服务未运行"
  else
    echo "[*] 语音服务未运行"
  fi
  exit 0
fi

container_state="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
container_health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
if curl -fsS --max-time 3 "$HEALTH_URL" >/dev/null 2>&1; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 1 "running" "$container_id" "语音服务运行中"
  else
    echo "[✓] 语音服务运行中: $HEALTH_URL"
  fi
  exit 0
fi

if [[ "$container_state" == "running" ]]; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "degraded" "$container_id" "${container_health:-starting}"
  else
    echo "[!] 语音服务容器在运行，但探活未通过: ${container_health:-starting}"
  fi
  exit 1
fi

if [[ "$JSON_OUTPUT" == "1" ]]; then
  emit_json 0 "${container_state:-unknown}" "$container_id" "语音服务容器未就绪"
else
  echo "[!] 语音服务容器未就绪: ${container_state:-unknown}"
fi
exit 1
