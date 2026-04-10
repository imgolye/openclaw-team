#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUNTIME_PROFILE="${MISSION_CONTROL_RUNTIME_PROFILE:-host}"
PROFILE_RESOLVER="$PROJECT_DIR/platform/bin/runtime/resolve_runtime_profile.py"
COMPOSE_FILE="$PROJECT_DIR/platform/infra/docker/compose.voice-runtime.yml"
PORT="${MISSION_CONTROL_SPEECH_RUNTIME_PORT:-}"
HEALTH_URL="${MISSION_CONTROL_SPEECH_RUNTIME_HEALTH_URL:-}"
HOST_BIND="${MISSION_CONTROL_SPEECH_RUNTIME_HOST:-127.0.0.1}"
PYTHON_BIN="${MISSION_CONTROL_PYTHON_BIN:-}"
JSON_OUTPUT=0
LOG_DIR=""
PID_FILE=""
HOST_RUNTIME_ROOT=""
MAX_WAIT_SECONDS="${MISSION_CONTROL_SPEECH_RUNTIME_START_TIMEOUT_SECONDS:-240}"

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
  local health_url="${3:-}"
  local message="${4:-}"
  python3 - "$ok" "$state" "$PORT" "$health_url" "$message" <<'PY'
import json, sys
ok, state, port, health_url, message = sys.argv[1:]
payload = {
    "ok": ok == "1",
    "state": state,
    "port": int(port) if str(port).isdigit() else None,
    "healthUrl": health_url or None,
    "message": message or None,
}
print(json.dumps(payload, ensure_ascii=False))
PY
}

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/start_voice_runtime.sh [选项]

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
if [[ -z "$PYTHON_BIN" ]]; then
  for candidate in \
    /opt/homebrew/bin/python3.13 \
    /opt/homebrew/bin/python3.12 \
    /opt/homebrew/bin/python3.11 \
    /opt/homebrew/bin/python3 \
    /opt/homebrew/bin/python3.14 \
    "$(command -v python3 2>/dev/null || true)"; do
    if [[ -n "$candidate" ]] && [[ -x "$candidate" ]]; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
fi
LOG_DIR="$PROJECT_DIR/local/logs"
PID_FILE="$LOG_DIR/voice-runtime-host.pid"
HOST_RUNTIME_ROOT="$PROJECT_DIR/.mission-control/voice-runtime-host"

if [[ "$RUNTIME_PROFILE" == "host" ]]; then
  mkdir -p "$LOG_DIR" "$HOST_RUNTIME_ROOT"
  if [[ -f "$PID_FILE" ]]; then
    existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
      if voice_runtime_ready; then
        if [[ "$JSON_OUTPUT" == "1" ]]; then
          emit_json 1 "running" "$HEALTH_URL" "语音服务已在运行（host 本机进程）"
        else
          echo "[*] 语音服务已在运行（host 本机进程）: $HEALTH_URL"
        fi
        exit 0
      fi
    fi
    rm -f "$PID_FILE"
  fi

  host_pid="$(
    "$PYTHON_BIN" - "$PROJECT_DIR" "$HOST_RUNTIME_ROOT" "$HOST_BIND" "$PORT" "$LOG_DIR/voice-runtime-host.log" "$PYTHON_BIN" <<'PY'
import os
import shutil
import subprocess
import sys
from pathlib import Path

project_dir = Path(sys.argv[1]).resolve()
runtime_root = Path(sys.argv[2]).resolve()
host = sys.argv[3]
port = sys.argv[4]
log_path = Path(sys.argv[5]).resolve()
python_seed = Path(sys.argv[6]).resolve()
venv_dir = runtime_root / ".venv"
python_bin = venv_dir / "bin" / "python"
pip_bin = venv_dir / "bin" / "pip"
model_dir = runtime_root / "models" / "kokoro-int8-multi-lang-v1_1"
cache_dir = runtime_root / ".cache"
requirements_marker = runtime_root / ".venv-requirements.txt"

runtime_root.mkdir(parents=True, exist_ok=True)
model_dir.mkdir(parents=True, exist_ok=True)
cache_dir.mkdir(parents=True, exist_ok=True)
log_path.parent.mkdir(parents=True, exist_ok=True)

seed_version = subprocess.run(
    [str(python_seed), "-c", "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"],
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()

if python_bin.exists():
    current_version = subprocess.run(
        [str(python_bin), "-c", "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if current_version != seed_version:
        shutil.rmtree(venv_dir, ignore_errors=True)
        requirements_marker.unlink(missing_ok=True)

if not python_bin.exists():
    subprocess.run([str(python_seed), "-m", "venv", str(venv_dir)], check=True)

requirements = [
    "fastapi==0.115.6",
    "huggingface_hub==0.31.1",
    "pydantic==2.12.5",
    "sherpa-onnx==1.12.34",
    "soundfile==0.12.1",
    "uvicorn==0.30.0",
]
if sys.version_info >= (3, 13):
    requirements.append("numpy>=2.2,<2.3")
else:
    requirements.append("numpy==1.26.4")
requirements_signature = "\n".join(requirements)
needs_install = True

if requirements_marker.exists():
    try:
        needs_install = requirements_marker.read_text(encoding="utf-8") != requirements_signature
    except OSError:
        needs_install = True
    if not needs_install:
        probe = subprocess.run(
            [
                str(python_bin),
                "-c",
                "import fastapi, huggingface_hub, numpy, pydantic, sherpa_onnx, soundfile, uvicorn",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        needs_install = probe.returncode != 0

if needs_install:
    subprocess.run([str(pip_bin), "install", "--upgrade", "pip", "setuptools<81", "wheel"], check=True)
    subprocess.run([str(pip_bin), "install", *requirements], check=True)
    requirements_marker.write_text(requirements_signature, encoding="utf-8")

env = os.environ.copy()
env.update(
    {
        "PYTHONPATH": str(project_dir),
        "HF_HOME": str(cache_dir),
        "SHERPA_ONNX_TTS_MODEL_DIR": str(model_dir),
        "SHERPA_ONNX_TTS_PORT": str(port),
    }
)
with open(log_path, "ab", buffering=0) as stream:
    proc = subprocess.Popen(
        [
            str(python_bin),
            "-m",
            "uvicorn",
            "sherpa_onnx_kokoro_openai_server:APP",
            "--app-dir",
            str(project_dir / "platform" / "infra" / "docker"),
            "--host",
            host,
            "--port",
            str(port),
        ],
        cwd=str(project_dir),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=stream,
        stderr=stream,
        start_new_session=True,
        close_fds=True,
    )
    print(proc.pid)
PY
  )"
  echo "$host_pid" > "$PID_FILE"

  max_attempts=$(( (MAX_WAIT_SECONDS + 1) / 2 ))
  if (( max_attempts < 1 )); then
    max_attempts=1
  fi
  for _ in $(seq 1 "$max_attempts"); do
    if voice_runtime_ready; then
      listener_pid="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
      if [[ -n "$listener_pid" ]]; then
        host_pid="$listener_pid"
        echo "$host_pid" > "$PID_FILE"
      fi
      if [[ "$JSON_OUTPUT" == "1" ]]; then
        emit_json 1 "running" "$HEALTH_URL" "语音服务已启动（host 本机进程）"
      else
        echo "[✓] 语音服务已启动（host 本机进程）: $HEALTH_URL"
      fi
      exit 0
    fi
    if ! kill -0 "$host_pid" >/dev/null 2>&1; then
      if [[ "$JSON_OUTPUT" == "1" ]]; then
        emit_json 0 "failed" "$HEALTH_URL" "host 语音服务启动失败，请检查日志"
      else
        echo "[✗] host 语音服务启动失败，请检查日志: $LOG_DIR/voice-runtime-host.log"
      fi
      rm -f "$PID_FILE"
      exit 1
    fi
    sleep 2
  done

  listener_pid="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
  if [[ -n "$listener_pid" ]]; then
    echo "$listener_pid" > "$PID_FILE"
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 0 "degraded" "$HEALTH_URL" "host 语音服务已监听端口，但探活仍未稳定"
    else
      echo "[!] host 语音服务已监听端口，但探活仍未稳定: $HEALTH_URL"
    fi
    exit 1
  fi

  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "degraded" "$HEALTH_URL" "host 语音服务进程已启动，但探活未通过"
  else
    echo "[!] host 语音服务进程已启动，但探活未通过: $HEALTH_URL"
  fi
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "unavailable" "$HEALTH_URL" "docker CLI 不可用"
  else
    echo "[✗] docker CLI 不可用"
  fi
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "unavailable" "$HEALTH_URL" "Docker daemon 未运行"
  else
    echo "[✗] Docker daemon 未运行"
  fi
  exit 1
fi

export MISSION_CONTROL_RUNTIME_PROFILE="$RUNTIME_PROFILE"
export MISSION_CONTROL_SPEECH_RUNTIME_PORT="$PORT"

docker compose -f "$COMPOSE_FILE" up -d >/dev/null

for _ in $(seq 1 20); do
  if curl -fsS --max-time 3 "$HEALTH_URL" >/dev/null 2>&1; then
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 1 "running" "$HEALTH_URL" "语音服务已启动"
    else
      echo "[✓] 语音服务已启动: $HEALTH_URL"
    fi
    exit 0
  fi
  sleep 2
done

if [[ "$JSON_OUTPUT" == "1" ]]; then
  emit_json 0 "degraded" "$HEALTH_URL" "容器已启动，但探活未通过"
else
  echo "[!] 语音服务容器已启动，但探活未通过: $HEALTH_URL"
fi
exit 1
