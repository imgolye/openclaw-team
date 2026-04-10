#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
RUNTIME_PROFILE="${MISSION_CONTROL_RUNTIME_PROFILE:-container}"
PROFILE_RESOLVER="$PROJECT_DIR/platform/bin/runtime/resolve_runtime_profile.py"
OPENCLAW_DIR="${OPENCLAW_DIR:-}"
BOOTSTRAP_THEME="${BOOTSTRAP_THEME:-corporate}"
PORT="${PORT:-}"
CORS_ORIGINS="${CORS_ORIGINS:-http://127.0.0.1:5173,http://localhost:5173}"
MISSION_CONTROL_DATABASE_URL="${MISSION_CONTROL_DATABASE_URL:-${DATABASE_URL:-}}"
AUTO_START_GATEWAY="${AUTO_START_GATEWAY:-1}"
AUTOMATION_INTERVAL="${AUTOMATION_INTERVAL:-60}"
MISSION_CONTROL_DEPLOYMENT_MODE="${MISSION_CONTROL_DEPLOYMENT_MODE:-}"
MISSION_CONTROL_DEPLOYMENT_PROFILE="${MISSION_CONTROL_DEPLOYMENT_PROFILE:-}"
MISSION_CONTROL_ENVIRONMENT="${MISSION_CONTROL_ENV:-}"
MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL="${MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL:-}"
MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL="${MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL:-}"

export MISSION_CONTROL_DATABASE_URL
export MISSION_CONTROL_RUNTIME_PROFILE

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

if [[ -z "$OPENCLAW_DIR" ]]; then
  OPENCLAW_DIR="$(resolve_runtime_profile_value openclaw.stateDir "/data/openclaw")"
fi
if [[ -z "$PORT" ]]; then
  PORT="$(resolve_runtime_profile_value product.port "18890")"
fi
if [[ -z "$MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL" ]]; then
  MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL="$(resolve_runtime_profile_value speechRuntime.baseUrl "http://sherpa-onnx-tts:8080/v1")"
fi
if [[ -z "$MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL" ]]; then
  MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL="$(resolve_runtime_profile_value localRuntime.baseUrl "http://host.docker.internal:11434/v1")"
fi
if [[ -z "$MISSION_CONTROL_DEPLOYMENT_MODE" ]]; then
  MISSION_CONTROL_DEPLOYMENT_MODE="$(resolve_runtime_profile_value deployment.mode "single_tenant")"
fi
if [[ -z "$MISSION_CONTROL_DEPLOYMENT_PROFILE" ]]; then
  MISSION_CONTROL_DEPLOYMENT_PROFILE="$(resolve_runtime_profile_value deployment.profile "standard")"
fi

export OPENCLAW_DIR
export PORT
export MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL
export MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL
export MISSION_CONTROL_DEPLOYMENT_MODE
export MISSION_CONTROL_DEPLOYMENT_PROFILE

has_agent_auth_seed() {
  local source_file="${OPENCLAW_AUTH_SOURCE_FILE:-}"
  if [[ -n "$source_file" && -f "$source_file" ]]; then
    return 0
  fi
  if [[ -n "${OPENCLAW_AUTH_PROFILES_JSON:-}" || -n "${OPENCLAW_AUTH_PROFILES_B64:-}" ]]; then
    return 0
  fi
  local keys=(
    ZAI_API_KEY BIGMODEL_API_KEY ZHIPUAI_API_KEY
    OPENAI_API_KEY ANTHROPIC_API_KEY
    GOOGLE_API_KEY GEMINI_API_KEY
    DEEPSEEK_API_KEY QWEN_API_KEY DASHSCOPE_API_KEY
    OPENROUTER_API_KEY XAI_API_KEY
    MINIMAX_API_KEY MINIMAX_CN_API_KEY
  )
  local key=""
  for key in "${keys[@]}"; do
    if [[ -n "${!key:-}" ]]; then
      return 0
    fi
  done
  if discover_agent_auth_source_file >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

discover_agent_auth_source_file() {
  local candidate_roots=(
    "$OPENCLAW_DIR"
    "$HOME/.openclaw"
    "/data/openclaw"
    "/bootstrap/openclaw-home"
  )
  local root=""
  local preferred=""
  for root in "${candidate_roots[@]}"; do
    [[ -n "$root" && -d "$root/agents" ]] || continue
    for preferred in \
      "$root/agents/assistant/agent/auth-profiles.json" \
      "$root/agents/main/agent/auth-profiles.json"; do
      if [[ -f "$preferred" ]]; then
        printf '%s\n' "$preferred"
        return 0
      fi
    done
  done
  local discovered=""
  for root in "${candidate_roots[@]}"; do
    [[ -n "$root" && -d "$root/agents" ]] || continue
    discovered="$(find "$root/agents" -maxdepth 3 -path '*/agent/auth-profiles.json' -type f 2>/dev/null | head -n 1 || true)"
    if [[ -n "$discovered" ]]; then
      printf '%s\n' "$discovered"
      return 0
    fi
  done
  return 1
}

mkdir -p "$OPENCLAW_DIR"
mkdir -p "$OPENCLAW_DIR/.chub/annotations" "$OPENCLAW_DIR/.chub/sources"
mkdir -p "$OPENCLAW_DIR/deliverables"
mkdir -p "$HOME/.chub"

DELIVERABLES_SOURCE_DIR="$PROJECT_DIR/platform/deliverables"
if [[ ! -d "$DELIVERABLES_SOURCE_DIR" && -d "$PROJECT_DIR/deliverables" ]]; then
  echo "[!] 检测到旧根目录 deliverables，临时回退复用：$PROJECT_DIR/deliverables" >&2
  DELIVERABLES_SOURCE_DIR="$PROJECT_DIR/deliverables"
fi
if [[ -d "$DELIVERABLES_SOURCE_DIR" ]]; then
  cp -R "$DELIVERABLES_SOURCE_DIR/." "$OPENCLAW_DIR/deliverables/" 2>/dev/null || true
fi

if [[ -z "${OPENCLAW_AUTH_SOURCE_FILE:-}" ]]; then
  OPENCLAW_AUTH_SOURCE_FILE="$(discover_agent_auth_source_file || true)"
  if [[ -n "$OPENCLAW_AUTH_SOURCE_FILE" ]]; then
    export OPENCLAW_AUTH_SOURCE_FILE
    echo "[*] 复用主机已有 Agent 鉴权种子: $OPENCLAW_AUTH_SOURCE_FILE"
  fi
fi

python3 - "$OPENCLAW_DIR/.env" <<'PY'
import os
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
database_url = str(os.environ.get("MISSION_CONTROL_DATABASE_URL") or "").strip()
if not database_url:
    raise SystemExit(0)
lines = []
if env_path.exists():
    lines = env_path.read_text(encoding="utf-8").splitlines()
updated = False
next_lines = []
for line in lines:
    if line.startswith("MISSION_CONTROL_DATABASE_URL="):
        next_lines.append(f"MISSION_CONTROL_DATABASE_URL={database_url}")
        updated = True
        continue
    next_lines.append(line)
if not updated:
    next_lines.append(f"MISSION_CONTROL_DATABASE_URL={database_url}")
env_path.write_text("\n".join(next_lines).rstrip() + "\n", encoding="utf-8")
try:
    env_path.chmod(0o600)
except Exception:
    pass
PY

if [[ -f "$OPENCLAW_DIR/.chub/config.yaml" ]]; then
  ln -sfn "$OPENCLAW_DIR/.chub/config.yaml" "$HOME/.chub/config.yaml"
fi
ln -sfn "$OPENCLAW_DIR/.chub/annotations" "$HOME/.chub/annotations"
ln -sfn "$OPENCLAW_DIR/.chub/sources" "$HOME/.chub/sources"

if [[ ! -f "$OPENCLAW_DIR/openclaw.json" ]]; then
  echo "[*] No OpenClaw state found, bootstrapping theme: $BOOTSTRAP_THEME"
  printf 'n\nn\nn\n\n\n\n' | bash "$PROJECT_DIR/platform/bin/install/setup.sh" \
    --dir "$OPENCLAW_DIR" \
    --theme "$BOOTSTRAP_THEME" \
    --deployment-mode "$MISSION_CONTROL_DEPLOYMENT_MODE" \
    --deployment-profile "$MISSION_CONTROL_DEPLOYMENT_PROFILE"
fi

MISSION_CONTROL_SKIP_DASHBOARD_PREWARM=1 \
  bash "$PROJECT_DIR/platform/bin/runtime/sync_runtime_assets.sh" --dir "$OPENCLAW_DIR" --project-dir "$PROJECT_DIR"

python3 - "$OPENCLAW_DIR" "$PROJECT_DIR" <<'PY'
import json
import sys
from pathlib import Path

openclaw_dir = Path(sys.argv[1]).expanduser().resolve()
project_dir = Path(sys.argv[2]).expanduser().resolve()
sys.path.insert(0, str(project_dir / "platform" / "bin" / "install" / "lib"))

import project_metadata

metadata = project_metadata.load_project_metadata(openclaw_dir)
speech_runtime = metadata.get("speechRuntime") if isinstance(metadata.get("speechRuntime"), dict) else {}
provider = str(speech_runtime.get("provider") or "").strip().lower()
base_url = str(speech_runtime.get("baseUrl") or "").strip()
model = str(speech_runtime.get("model") or "").strip()
api_key_env = str(speech_runtime.get("apiKeyEnv") or "").strip()

stale_openai_default = (
    provider in {"", "openai"}
    and base_url in {"", project_metadata.DEFAULT_SPEECH_RUNTIME_BASE_URL}
    and model in {"", project_metadata.DEFAULT_SPEECH_RUNTIME_MODEL}
    and api_key_env in {"", project_metadata.DEFAULT_SPEECH_RUNTIME_API_KEY_ENV}
)

if not speech_runtime or stale_openai_default:
    metadata["speechRuntime"] = {
        "provider": "sherpa_onnx",
        "baseUrl": project_metadata.DEFAULT_SHERPA_ONNX_RUNTIME_DOCKER_BASE_URL,
        "model": project_metadata.DEFAULT_SHERPA_ONNX_RUNTIME_MODEL,
        "apiKeyEnv": project_metadata.DEFAULT_SHERPA_ONNX_RUNTIME_API_KEY_ENV,
    }
    project_metadata.write_project_metadata(openclaw_dir, metadata)
    print("[*] 默认语音运行时已切换到容器内 sherpa-onnx + kokoro 预设")

config_path = openclaw_dir / "openclaw.json"
if config_path.exists():
    try:
        original = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        original = {}
    sanitized = project_metadata.sanitize_openclaw_config(original)
    sanitized = project_metadata.apply_local_runtime_model_provider_config(sanitized, metadata)
    sessions = (
        sanitized.get("tools", {}).get("sessions")
        if isinstance(sanitized.get("tools"), dict)
        else {}
    )
    if sessions.get("visibility") != project_metadata.DEFAULT_OPENCLAW_SESSION_VISIBILITY or sanitized != original:
        config_path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("[*] OpenClaw tools.sessions.visibility 已统一为 all")
PY

echo "[*] 跳过 localRuntime 自动启动：模型服务需要独立于 product 容器运行"

if ! command -v chub >/dev/null 2>&1; then
  echo "[*] Context Hub CLI 未就绪，准备在容器内安装..."
  if npm install -g @aisuite/chub >/dev/null 2>&1; then
    echo "[✓] Context Hub CLI 已安装"
  else
    echo "[!] Context Hub CLI 安装失败，Context Hub 将保持不可用" >&2
  fi
fi

if command -v chub >/dev/null 2>&1 && [[ -f "$HOME/.chub/config.yaml" ]]; then
  if ! find "$HOME/.chub/sources" -mindepth 1 -print -quit 2>/dev/null | grep -q .; then
    echo "[*] Context Hub registry 尚未初始化，准备在容器内刷新..."
    if chub update --json >/dev/null 2>&1; then
      echo "[✓] Context Hub registry 已初始化"
    else
      echo "[!] Context Hub registry 初始化失败，首次检索可能为空" >&2
    fi
  fi
fi

export OPENCLAW_STATE_DIR="$OPENCLAW_DIR"
export OPENCLAW_CONFIG_PATH="$OPENCLAW_DIR/openclaw.json"

python3 - <<'PY'
import os
import subprocess
from pathlib import Path

allow_rfc2544 = str(os.environ.get("OPENCLAW_ALLOW_RFC2544_BENCHMARK_RANGE") or "").strip().lower()
if allow_rfc2544 not in {"1", "true", "yes", "on"}:
    raise SystemExit(0)

try:
    npm_root = subprocess.run(
        ["npm", "root", "-g"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
except Exception:
    raise SystemExit(0)

package_dir = Path(npm_root) / "openclaw"
if not package_dir.exists():
    raise SystemExit(0)

needle = 'return { allowRfc2544BenchmarkRange: policy?.allowRfc2544BenchmarkRange === true };'
replacement = (
    'return { allowRfc2544BenchmarkRange: policy?.allowRfc2544BenchmarkRange === true || '
    'process.env.OPENCLAW_ALLOW_RFC2544_BENCHMARK_RANGE === "1" || '
    'process.env.OPENCLAW_ALLOW_RFC2544_BENCHMARK_RANGE === "true" };'
)
patched = 0
for path in package_dir.rglob("*.js"):
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        continue
    if replacement in text or needle not in text:
        continue
    path.write_text(text.replace(needle, replacement), encoding="utf-8")
    patched += 1

if patched:
    print(f"[*] Applied RFC2544 SSRF compatibility patch to {patched} OpenClaw runtime files")
PY

if [[ -f "$OPENCLAW_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$OPENCLAW_DIR/.env"
  set +a
fi

if [[ -z "${GATEWAY_AUTH_TOKEN:-}" ]]; then
  if [[ "$MISSION_CONTROL_DEPLOYMENT_PROFILE" == "single_tenant_prod" || "$MISSION_CONTROL_ENVIRONMENT" == "prod" || "$MISSION_CONTROL_ENVIRONMENT" == "production" ]]; then
    echo "[✗] 单租户生产部署要求预先注入 GATEWAY_AUTH_TOKEN，当前不会自动生成。" >&2
    exit 1
  fi
  GATEWAY_AUTH_TOKEN="$(openssl rand -hex 24)"
  export GATEWAY_AUTH_TOKEN
  python3 - "$OPENCLAW_DIR/.env" "$GATEWAY_AUTH_TOKEN" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1]).expanduser().resolve()
token = str(sys.argv[2] or "").strip()
if not token:
    raise SystemExit(0)
lines = []
if env_path.exists():
    lines = env_path.read_text(encoding="utf-8").splitlines()
updated = []
replaced = False
for line in lines:
    if line.startswith("GATEWAY_AUTH_TOKEN="):
        updated.append(f"GATEWAY_AUTH_TOKEN={token}")
        replaced = True
    else:
        updated.append(line)
if not replaced:
    updated.append(f"GATEWAY_AUTH_TOKEN={token}")
env_path.write_text("\n".join(updated).rstrip() + "\n", encoding="utf-8")
PY
fi

if [[ "$MISSION_CONTROL_DEPLOYMENT_PROFILE" == "single_tenant_prod" || "$MISSION_CONTROL_ENVIRONMENT" == "prod" || "$MISSION_CONTROL_ENVIRONMENT" == "production" ]]; then
  if ! has_agent_auth_seed; then
    echo "[✗] 单租户生产部署缺少 Agent 鉴权配置。请注入至少一种 provider API key，或提供 OPENCLAW_AUTH_SOURCE_FILE / OPENCLAW_AUTH_PROFILES_JSON / OPENCLAW_AUTH_PROFILES_B64。" >&2
    exit 1
  fi
fi

python3 - "$OPENCLAW_DIR/openclaw.json" <<'PY'
import json
import os
import sys
from pathlib import Path

config_path = Path(sys.argv[1]).expanduser().resolve()
if not config_path.exists():
    raise SystemExit(0)

config = json.loads(config_path.read_text(encoding="utf-8"))
browser = config.get("browser") if isinstance(config.get("browser"), dict) else {}
plugins = config.get("plugins") if isinstance(config.get("plugins"), dict) else {}
entries = plugins.get("entries") if isinstance(plugins.get("entries"), dict) else {}
changed = False

# OpenClaw CLI rejects unknown root keys such as "sandbox".
# Keep the runtime config schema-clean here and expose compatibility
# projections from product APIs instead of persisting them in openclaw.json.
if "sandbox" in config:
    config.pop("sandbox", None)
    changed = True

defaults = {
    "enabled": True,
    "defaultProfile": "openclaw",
    "headless": True,
    "noSandbox": True,
    "executablePath": "/usr/bin/chromium",
}
for key, value in defaults.items():
    if browser.get(key) != value and (browser.get(key) in (None, "")):
        browser[key] = value
        changed = True

extra_args = browser.get("extraArgs")
if not isinstance(extra_args, list):
    extra_args = []
required_args = ["--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check"]
for arg in required_args:
    if arg not in extra_args:
        extra_args.append(arg)
        changed = True
if extra_args:
    browser["extraArgs"] = extra_args

profiles = browser.get("profiles") if isinstance(browser.get("profiles"), dict) else {}
openclaw_profile = profiles.get("openclaw") if isinstance(profiles.get("openclaw"), dict) else {}
if "cdpPort" not in openclaw_profile:
    openclaw_profile["cdpPort"] = 18800
    changed = True
if "color" not in openclaw_profile:
    openclaw_profile["color"] = "#FF4500"
    changed = True
profiles["openclaw"] = openclaw_profile
browser["profiles"] = profiles

if browser.get("enabled", True) is not False:
    browser_entry = entries.get("browser") if isinstance(entries.get("browser"), dict) else {}
    if browser_entry.get("enabled") is not True:
        browser_entry["enabled"] = True
        entries["browser"] = browser_entry
        plugins["entries"] = entries
        config["plugins"] = plugins
        changed = True

channels = config.get("channels") if isinstance(config.get("channels"), dict) else {}

def is_placeholder(value, env_key):
    return str(value or "").strip() == f"${{{env_key}}}"

def sanitize_channel_secret(channel_name, secret_key, env_key, clear_keys=()):
    global changed
    channel = channels.get(channel_name) if isinstance(channels.get(channel_name), dict) else {}
    if not channel:
        return
    current = str(channel.get(secret_key) or "").strip()
    resolved = str(os.environ.get(env_key) or "").strip()
    if is_placeholder(current, env_key) or (not current and resolved):
        if resolved:
            if current != resolved:
                channel[secret_key] = resolved
                changed = True
            if channel.get("enabled") is not True:
                channel["enabled"] = True
                changed = True
        else:
            if secret_key in channel:
                channel.pop(secret_key, None)
                changed = True
            if channel.get("enabled") is not False:
                channel["enabled"] = False
                changed = True
            for extra_key in clear_keys:
                if extra_key in channel:
                    channel.pop(extra_key, None)
                    changed = True
        channels[channel_name] = channel

sanitize_channel_secret("feishu", "appSecret", "FEISHU_APP_SECRET")
sanitize_channel_secret("telegram", "botToken", "TELEGRAM_BOT_TOKEN", clear_keys=("proxy",))
sanitize_channel_secret("qqbot", "clientSecret", "QQBOT_CLIENT_SECRET")
sanitize_channel_secret("qq", "clientSecret", "QQBOT_CLIENT_SECRET")

gateway = config.get("gateway") if isinstance(config.get("gateway"), dict) else {}
auth = gateway.get("auth") if isinstance(gateway.get("auth"), dict) else {}
gateway_token = str(auth.get("token") or "").strip()
resolved_gateway_token = str(os.environ.get("GATEWAY_AUTH_TOKEN") or "").strip()
if auth and (is_placeholder(gateway_token, "GATEWAY_AUTH_TOKEN") or (not gateway_token and resolved_gateway_token)):
    if resolved_gateway_token:
        if gateway_token != resolved_gateway_token:
            auth["token"] = resolved_gateway_token
            changed = True
    elif "token" in auth:
        auth.pop("token", None)
        changed = True
    gateway["auth"] = auth
    config["gateway"] = gateway

if channels:
    config["channels"] = channels

if changed:
    config["browser"] = browser
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY

if [[ "$AUTO_START_GATEWAY" == "1" ]]; then
  mkdir -p "$OPENCLAW_DIR/logs"
  if ! openclaw gateway health --json >/dev/null 2>&1; then
    echo "[*] Gateway 未就绪，准备在容器内后台启动..."
    nohup openclaw gateway run >"$OPENCLAW_DIR/logs/openclaw-gateway.log" 2>&1 &
    for _ in $(seq 1 20); do
      if openclaw gateway health --json >/dev/null 2>&1; then
        echo "[✓] Gateway 已启动"
        break
      fi
      sleep 1
    done
    if ! openclaw gateway health --json >/dev/null 2>&1; then
      echo "[!] Gateway 仍未就绪，当前将退回 embedded 通道" >&2
    fi
  fi
  PENDING_CLI_DEVICE_ID="$(
    python3 - <<'PY'
import json
import subprocess

command = ["openclaw", "devices", "list", "--json"]
process = subprocess.run(command, capture_output=True, text=True, check=False)
payload = {}
stdout = (process.stdout or "").strip()
if stdout:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        payload = {}
pending = payload.get("pending") if isinstance(payload, dict) else []
request_id = ""
for item in pending or []:
    if not isinstance(item, dict):
        continue
    if str(item.get("clientId") or "").strip() != "cli":
        continue
    if str(item.get("clientMode") or "").strip() != "cli":
        continue
    if str(item.get("platform") or "").strip() != "linux":
        continue
    request_id = str(item.get("requestId") or "").strip()
print(request_id)
PY
  )"
  if [[ -n "$PENDING_CLI_DEVICE_ID" ]]; then
    if openclaw devices approve "$PENDING_CLI_DEVICE_ID" --json >/dev/null 2>&1; then
      echo "[✓] 本地 CLI 设备配对已批准: $PENDING_CLI_DEVICE_ID"
    else
      echo "[!] 本地 CLI 设备配对审批失败: $PENDING_CLI_DEVICE_ID" >&2
    fi
  fi

  BROWSER_ENABLED="$(
    python3 - "$OPENCLAW_DIR/openclaw.json" <<'PY'
import json
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
browser = config.get("browser") if isinstance(config.get("browser"), dict) else {}
print("1" if browser.get("enabled", True) else "0")
PY
  )"
  if [[ "$BROWSER_ENABLED" == "1" ]]; then
    if ! openclaw browser --json status >/tmp/mission-control-browser-status.json 2>/dev/null; then
      true
    fi
    ENABLE_BROWSER_AUTOSTART="${MISSION_CONTROL_ENABLE_BROWSER_AUTOSTART:-1}"
    if [[ ! "${ENABLE_BROWSER_AUTOSTART,,}" =~ ^(1|true|yes|on)$ ]]; then
      echo "[*] 已跳过无头浏览器自动启动（如需开启请设置 MISSION_CONTROL_ENABLE_BROWSER_AUTOSTART=1）"
    elif ! python3 - <<'PY'
import json
from pathlib import Path

path = Path('/tmp/mission-control-browser-status.json')
running = False
if path.exists():
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        running = bool(payload.get('running'))
    except Exception:
        running = False
raise SystemExit(0 if running else 1)
PY
    then
      python3 - "$OPENCLAW_DIR/openclaw.json" <<'PY'
import json
import os
import subprocess
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
browser = config.get("browser") if isinstance(config.get("browser"), dict) else {}
profile = str(browser.get("defaultProfile") or "openclaw").strip() or "openclaw"
user_data_dir = Path("/data/openclaw/browser") / profile / "user-data"
lock_names = ("SingletonLock", "SingletonSocket", "SingletonCookie")

has_live_chromium = False
process = subprocess.run(["ps", "-ef"], capture_output=True, text=True, check=False)
for line in (process.stdout or "").splitlines():
    lowered = line.lower()
    if "chromium" in lowered and "<defunct>" not in lowered:
        has_live_chromium = True
        break

if not has_live_chromium and user_data_dir.exists():
    for name in lock_names:
        candidate = user_data_dir / name
        try:
            if candidate.exists() or candidate.is_symlink():
                candidate.unlink()
        except FileNotFoundError:
            pass
PY
      echo "[*] Browser 未运行，已清理 stale profile locks 并尝试拉起无头浏览器..."
      nohup bash -lc 'openclaw browser --json start >/tmp/mission-control-browser-start.json 2>/tmp/mission-control-browser-start.err' >/dev/null 2>&1 &
      for _ in $(seq 1 20); do
        if openclaw browser --json status >/tmp/mission-control-browser-status.json 2>/dev/null; then
          if python3 - <<'PY'
import json
from pathlib import Path

path = Path('/tmp/mission-control-browser-status.json')
running = False
if path.exists():
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        running = bool(payload.get('running'))
    except Exception:
        running = False
raise SystemExit(0 if running else 1)
PY
          then
            echo "[✓] 无头浏览器已启动"
            break
          fi
        fi
        sleep 1
      done
      if ! python3 - <<'PY'
import json
from pathlib import Path

path = Path('/tmp/mission-control-browser-status.json')
running = False
if path.exists():
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        running = bool(payload.get('running'))
    except Exception:
        running = False
raise SystemExit(0 if running else 1)
PY
      then
        echo "[!] 无头浏览器启动失败，稍后仍可按需重试" >&2
        cat /tmp/mission-control-browser-start.err >&2 || true
      fi
    fi
  fi
else
  echo "[*] 已跳过 OpenClaw gateway 自动启动（如需开启请设置 AUTO_START_GATEWAY=1）"
fi

LOCAL_MEMORY_ENABLED="$(
  python3 - "$OPENCLAW_DIR/openclaw.json" <<'PY'
import json
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
agents = config.get("agents") if isinstance(config.get("agents"), dict) else {}
defaults = agents.get("defaults") if isinstance(agents.get("defaults"), dict) else {}
memory = defaults.get("memorySearch") if isinstance(defaults.get("memorySearch"), dict) else {}
provider = str(memory.get("provider") or "").strip().lower()
enabled = memory.get("enabled")
print("1" if provider == "local" and enabled is not False else "0")
PY
)"
ENABLE_MEMORY_PREWARM="${MISSION_CONTROL_ENABLE_MEMORY_PREWARM:-0}"
if [[ "$LOCAL_MEMORY_ENABLED" == "1" && "${ENABLE_MEMORY_PREWARM,,}" =~ ^(1|true|yes|on)$ ]]; then
  if ! ps -ef | grep -F "openclaw memory status --deep" | grep -v grep >/dev/null 2>&1; then
    echo "[*] 本地记忆 embeddings 预热中..."
    nohup bash -lc 'openclaw memory status --deep >/data/openclaw/logs/openclaw-memory-prewarm.log 2>&1' >/dev/null 2>&1 &
  fi
elif [[ "$LOCAL_MEMORY_ENABLED" == "1" ]]; then
  echo "[*] 已跳过本地记忆 embeddings 自动预热（如需开启请设置 MISSION_CONTROL_ENABLE_MEMORY_PREWARM=1）"
fi

ROUTER_ID="$(
  python3 - "$OPENCLAW_DIR/openclaw.json" <<'PY'
import json
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
agents = config.get("agents", {}).get("list", [])
router = next((item.get("id", "") for item in agents if item.get("default")), "")
if not router and agents:
    router = agents[0].get("id", "")
print(router or "assistant")
PY
)"

exec python3 "$OPENCLAW_DIR/workspace-$ROUTER_ID/scripts/collaboration_dashboard.py" \
  --dir "$OPENCLAW_DIR" \
  --serve \
  --host "0.0.0.0" \
  --port "$PORT" \
  --frontend-dist "$PROJECT_DIR/apps/frontend/dist" \
  --cors-origins "$CORS_ORIGINS" \
  --automation-interval "$AUTOMATION_INTERVAL"
