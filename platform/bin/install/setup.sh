#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#  OpenClaw Team · OpenClaw 多 Agent 初始化工具
#  用法: bash setup.sh [--theme corporate]
# ============================================================

VERSION="1.18.0"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
INSTALL_LIB_DIR="$SCRIPT_DIR/lib"
BACKEND_DIR="$PROJECT_DIR/backend"
THEMES_DIR="$PROJECT_DIR/platform/config/themes"

# ---------- 默认值 ----------
THEME="corporate"
OPENCLAW_DIR="$HOME/.openclaw"
TASK_PREFIX="TASK"
PREFIX_SET=0
ENV_FILE=""
OPENCLAW_INSTALL_METHOD="auto"
SKIP_OPENCLAW_INSTALL=0
SKIP_CONTEXT_HUB_INSTALL=0
OPENCLAW_AUTH_SOURCE_FILE="${OPENCLAW_AUTH_SOURCE_FILE:-}"
DEPLOYMENT_MODE="${MISSION_CONTROL_DEPLOYMENT_MODE:-single_tenant}"
DEPLOYMENT_PROFILE="${MISSION_CONTROL_DEPLOYMENT_PROFILE:-standard}"
MISSION_CONTROL_ENVIRONMENT="${MISSION_CONTROL_ENV:-}"
SYNC_CONTAINER_MODE="${MISSION_CONTROL_SYNC_CONTAINER:-auto}"

# ---------- 颜色 ----------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*"; exit 1; }
ask()   { echo -en "${CYAN}[?]${NC} $1 "; }

sync_tree() {
  local source_dir="$1"
  local target_dir="$2"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete --exclude '__pycache__' --exclude '.DS_Store' "$source_dir/" "$target_dir/"
    return 0
  fi
  python3 - "$source_dir" "$target_dir" <<'PY'
import shutil
import sys
from pathlib import Path

source = Path(sys.argv[1]).resolve()
target = Path(sys.argv[2]).resolve()
target.mkdir(parents=True, exist_ok=True)

for child in list(target.iterdir()):
    if child.name in {"__pycache__", ".DS_Store"}:
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)
        continue
    if not (source / child.name).exists():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)

for child in source.iterdir():
  if child.name in {"__pycache__", ".DS_Store"}:
    continue
  destination = target / child.name
  if child.is_dir():
    if destination.exists():
      shutil.rmtree(destination)
    shutil.copytree(child, destination)
  else:
    target.mkdir(parents=True, exist_ok=True)
    shutil.copy2(child, destination)
PY
}

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

sync_agent_auth_profiles() {
  local auth_script="$PROJECT_DIR/platform/bin/runtime/sync_agent_auth.py"
  [[ -f "$auth_script" ]] || return 0

  if [[ -z "$OPENCLAW_AUTH_SOURCE_FILE" && -z "${OPENCLAW_AUTH_PROFILES_JSON:-}" && -z "${OPENCLAW_AUTH_PROFILES_B64:-}" && -z "${ZAI_API_KEY:-}" && -z "${BIGMODEL_API_KEY:-}" && -z "${ZHIPUAI_API_KEY:-}" && -z "${OPENAI_API_KEY:-}" && -z "${ANTHROPIC_API_KEY:-}" && -z "${GOOGLE_API_KEY:-}" && -z "${GEMINI_API_KEY:-}" && -z "${DEEPSEEK_API_KEY:-}" && -z "${QWEN_API_KEY:-}" && -z "${DASHSCOPE_API_KEY:-}" && -z "${OPENROUTER_API_KEY:-}" && -z "${XAI_API_KEY:-}" && -z "${MINIMAX_API_KEY:-}" && -z "${MINIMAX_CN_API_KEY:-}" ]]; then
    OPENCLAW_AUTH_SOURCE_FILE="$(discover_agent_auth_source_file || true)"
    if [[ -n "$OPENCLAW_AUTH_SOURCE_FILE" ]]; then
      info "复用已有 Agent 鉴权种子: $OPENCLAW_AUTH_SOURCE_FILE"
    fi
  fi

  local auth_args=(--dir "$OPENCLAW_DIR" --json)
  local overwrite=0
  if [[ -n "$OPENCLAW_AUTH_SOURCE_FILE" ]]; then
    auth_args+=(--source-file "$OPENCLAW_AUTH_SOURCE_FILE")
    overwrite=1
  fi
  if [[ -n "${OPENCLAW_AUTH_PROFILES_JSON:-}" || -n "${OPENCLAW_AUTH_PROFILES_B64:-}" || -n "${ZAI_API_KEY:-}" || -n "${BIGMODEL_API_KEY:-}" || -n "${ZHIPUAI_API_KEY:-}" || -n "${OPENAI_API_KEY:-}" || -n "${ANTHROPIC_API_KEY:-}" || -n "${GOOGLE_API_KEY:-}" || -n "${GEMINI_API_KEY:-}" || -n "${DEEPSEEK_API_KEY:-}" || -n "${QWEN_API_KEY:-}" || -n "${DASHSCOPE_API_KEY:-}" || -n "${OPENROUTER_API_KEY:-}" || -n "${XAI_API_KEY:-}" || -n "${MINIMAX_API_KEY:-}" || -n "${MINIMAX_CN_API_KEY:-}" ]]; then
    overwrite=1
  fi
  if [[ "$overwrite" -eq 1 ]]; then
    auth_args+=(--overwrite)
  fi

  local auth_json=""
  if ! auth_json="$(python3 "$auth_script" "${auth_args[@]}")"; then
    warn "Agent 鉴权同步失败，团队通信可能不可用。"
    return 0
  fi

  local auth_summary=""
  auth_summary="$(python3 - "$auth_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print("\t".join([
    str(payload.get("source") or "missing"),
    str(payload.get("writtenCount") or 0),
    str(payload.get("targetCount") or 0),
    ",".join(payload.get("providers") or []),
]))
PY
)"
  local source="" written="" total="" providers=""
  IFS=$'\t' read -r source written total providers <<< "$auth_summary"
  if [[ "$source" == "missing" ]]; then
    if [[ "$DEPLOYMENT_PROFILE" == "single_tenant_prod" || "$MISSION_CONTROL_ENVIRONMENT" == "prod" || "$MISSION_CONTROL_ENVIRONMENT" == "production" ]]; then
      error "单租户生产部署缺少 Agent 鉴权配置。请提供 OPENCLAW_AUTH_SOURCE_FILE、OPENCLAW_AUTH_PROFILES_JSON/B64，或至少一种 provider API key。"
    fi
    warn "未检测到 Agent 鉴权配置；团队通信和任务执行可能失败。可通过 OPENCLAW_AUTH_SOURCE_FILE 或 provider API key 重新运行 setup。"
  elif [[ "${written:-0}" -gt 0 ]]; then
    info "Agent 鉴权已同步 (${written}/${total}${providers:+ · ${providers}})"
  else
    info "Agent 鉴权已校验 (${total} 个 agent 已具备 auth-profiles)"
  fi
}

normalize_agent_permissions() {
  local permission_script="$PROJECT_DIR/platform/bin/runtime/max_agent_permissions.py"
  [[ -f "$permission_script" ]] || return 0
  local permission_preset="max"
  if [[ "$DEPLOYMENT_PROFILE" == "single_tenant_prod" || "$MISSION_CONTROL_ENVIRONMENT" == "prod" || "$MISSION_CONTROL_ENVIRONMENT" == "production" ]]; then
    permission_preset="single_tenant_prod"
  fi
  local permission_json=""
  if ! permission_json="$(python3 "$permission_script" --dir "$OPENCLAW_DIR" --preset "$permission_preset" --json)"; then
    warn "Agent 权限提升失败，当前将保留原配置。"
    return 0
  fi
  local agent_count=""
  local applied_preset=""
  read -r agent_count applied_preset <<<"$(python3 - "$permission_json" <<'PY'
import json
import sys
payload = json.loads(sys.argv[1])
print(f"{payload.get('agentCount') or 0} {payload.get('preset') or 'max'}")
PY
)"
  info "Agent 权限已按 ${applied_preset} preset 处理 (${agent_count} 个 agent)"
}

ensure_context_hub_cli() {
  if command -v chub &>/dev/null; then
    CHUB_BIN="$(command -v chub)"
    return 0
  fi

  if [[ "$SKIP_CONTEXT_HUB_INSTALL" -eq 1 ]]; then
    warn "已跳过 Context Hub CLI 安装。"
    return 0
  fi

  if ! command -v npm &>/dev/null; then
    warn "未检测到 npm，暂时无法自动安装 Context Hub CLI（chub）。"
    return 0
  fi

  warn "未检测到 chub CLI，准备自动安装..."
  if npm install -g @aisuite/chub >/dev/null 2>&1; then
    CHUB_BIN="$(command -v chub || true)"
    info "Context Hub CLI 已自动安装"
    return 0
  fi

  warn "Context Hub CLI 自动安装失败，后续可手动执行: npm install -g @aisuite/chub"
}

# ---------- 参数解析 ----------
while [[ $# -gt 0 ]]; do
  case $1 in
    --theme)  THEME="$2"; shift 2 ;;
    --dir)    OPENCLAW_DIR="$2"; shift 2 ;;
    --prefix) TASK_PREFIX="$2"; PREFIX_SET=1; shift 2 ;;
    --openclaw-install-method) OPENCLAW_INSTALL_METHOD="$2"; shift 2 ;;
    --deployment-mode) DEPLOYMENT_MODE="$2"; shift 2 ;;
    --deployment-profile) DEPLOYMENT_PROFILE="$2"; shift 2 ;;
    --sync-container) SYNC_CONTAINER_MODE="always"; shift ;;
    --skip-container-sync) SYNC_CONTAINER_MODE="never"; shift ;;
    --skip-openclaw-install) SKIP_OPENCLAW_INSTALL=1; shift ;;
    --skip-context-hub-install) SKIP_CONTEXT_HUB_INSTALL=1; shift ;;
    --help)   echo "用法: setup.sh [--theme corporate] [--dir ~/.openclaw] [--prefix TASK] [--deployment-mode single_tenant|shared_platform] [--deployment-profile standard|single_tenant_prod] [--openclaw-install-method auto|managed-local|npm-global|pnpm-global|bun-global] [--sync-container|--skip-container-sync] [--skip-openclaw-install] [--skip-context-hub-install]"; exit 0 ;;
    *)        warn "未知参数: $1"; shift ;;
  esac
done

# ---------- 前置检查 ----------
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║  OpenClaw Team · 多 Agent 系统 v${VERSION} ║"
echo "║  Multi-Agent Orchestration for OpenClaw  ║"
echo "╚══════════════════════════════════════════╝"
echo ""

ensure_openclaw_cli() {
  if command -v openclaw &>/dev/null; then
    OPENCLAW_BIN="$(command -v openclaw)"
    return 0
  fi

  if [[ "$SKIP_OPENCLAW_INSTALL" -eq 1 ]]; then
    error "未检测到 openclaw CLI，且已指定 --skip-openclaw-install。请先安装 OpenClaw。"
  fi

  local installer_script="$PROJECT_DIR/platform/bin/install/openclaw_installer.py"
  [[ -f "$installer_script" ]] || error "缺少 OpenClaw 安装器: $installer_script"

  warn "未检测到 openclaw CLI，准备自动安装..."
  local install_json=""
  if ! install_json="$(python3 "$installer_script" install --dir "$OPENCLAW_DIR" --method "$OPENCLAW_INSTALL_METHOD" --json)"; then
    error "OpenClaw 自动安装失败: $install_json"
  fi

  OPENCLAW_BIN="$(
    python3 -c 'import json,sys; data=json.loads(sys.argv[1]); print(((data.get("installation") or {}).get("cliPath") or "").strip())' \
      "$install_json"
  )"
  [[ -n "$OPENCLAW_BIN" ]] || error "OpenClaw 安装完成，但没有拿到 CLI 路径。"
  export PATH="$(dirname "$OPENCLAW_BIN"):$PATH"
  info "OpenClaw 已自动安装 (${OPENCLAW_INSTALL_METHOD})"
}

ensure_openclaw_cli
ensure_context_hub_cli

should_sync_container() {
  case "${SYNC_CONTAINER_MODE:-auto}" in
    always)
      return 0
      ;;
    never)
      return 1
      ;;
  esac
  command -v docker >/dev/null 2>&1 || return 1
  [[ -f "$PROJECT_DIR/docker-compose.yml" ]] || return 1
  docker compose -f "$PROJECT_DIR/docker-compose.yml" ps -q mission-control >/dev/null 2>&1 || return 1
  local container_id
  container_id="$(docker compose -f "$PROJECT_DIR/docker-compose.yml" ps -q mission-control 2>/dev/null || true)"
  [[ -n "$container_id" ]]
}

if ! python3 - <<'PY' >/dev/null 2>&1
import psycopg
PY
then
  error "未检测到 psycopg。OpenClaw Team 现在仅支持 PostgreSQL，请先安装: python3 -m pip install 'psycopg[binary]'"
fi

OPENCLAW_VERSION=$("$OPENCLAW_BIN" --version 2>/dev/null | head -1 || echo "unknown")
info "OpenClaw 版本: $OPENCLAW_VERSION"
if command -v chub &>/dev/null; then
  info "Context Hub CLI: $(command -v chub)"
fi
info "主题: $THEME"
info "安装目录: $OPENCLAW_DIR"
echo ""

ENV_FILE="$OPENCLAW_DIR/.env"
read_env_value() {
  local env_path="$1"
  local key="$2"
  [[ -f "$env_path" ]] || return 0
  python3 - "$env_path" "$key" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
key = sys.argv[2]
for line in env_path.read_text(encoding="utf-8").splitlines():
    if not line or line.lstrip().startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    if k == key:
        print(v)
        break
PY
}

EXISTING_FEISHU_SECRET="${FEISHU_APP_SECRET:-$(read_env_value "$ENV_FILE" "FEISHU_APP_SECRET")}"
EXISTING_TG_TOKEN="${TELEGRAM_BOT_TOKEN:-$(read_env_value "$ENV_FILE" "TELEGRAM_BOT_TOKEN")}"
EXISTING_QQ_SECRET="${QQBOT_CLIENT_SECRET:-$(read_env_value "$ENV_FILE" "QQBOT_CLIENT_SECRET")}"
EXISTING_GATEWAY_TOKEN="${GATEWAY_AUTH_TOKEN:-$(read_env_value "$ENV_FILE" "GATEWAY_AUTH_TOKEN")}"
EXISTING_DATABASE_URL="$(
  printf '%s' "${MISSION_CONTROL_DATABASE_URL:-${DATABASE_URL:-}}"
)"
if [[ -z "$EXISTING_DATABASE_URL" ]]; then
  EXISTING_DATABASE_URL="$(read_env_value "$ENV_FILE" "MISSION_CONTROL_DATABASE_URL")"
fi
if [[ -z "$EXISTING_DATABASE_URL" ]]; then
  EXISTING_DATABASE_URL="$(read_env_value "$ENV_FILE" "DATABASE_URL")"
fi

# ---------- 加载主题 ----------
THEME_FILE="$THEMES_DIR/$THEME/theme.json"
if [[ ! -f "$THEME_FILE" ]]; then
  error "主题文件不存在: $THEME_FILE (可选: corporate)"
fi

THEME_TASK_PREFIX="$(PYTHONPATH="$INSTALL_LIB_DIR${PYTHONPATH:+:$PYTHONPATH}" python3 - "$THEME_FILE" <<'PY'
import sys
from theme_utils import load_theme

theme = load_theme(sys.argv[1])
print(theme.get("task_prefix", "TASK"))
PY
)"

ALL_AGENTS=()
while IFS= read -r agent_id; do
  [[ -n "$agent_id" ]] && ALL_AGENTS+=("$agent_id")
done < <(PYTHONPATH="$INSTALL_LIB_DIR${PYTHONPATH:+:$PYTHONPATH}" python3 - "$THEME_FILE" <<'PY'
import sys
from theme_utils import get_all_agent_ids, load_theme

theme = load_theme(sys.argv[1])
for agent_id in get_all_agent_ids(theme):
    print(agent_id)
PY
)

if [[ "$PREFIX_SET" -eq 0 ]]; then
  TASK_PREFIX="$THEME_TASK_PREFIX"
fi

# ---------- 交互式配置 ----------
echo "=== 频道配置 ==="

ask "启用飞书? (y/n) [y]:"
read -r ENABLE_FEISHU; ENABLE_FEISHU="${ENABLE_FEISHU:-y}"

ask "启用 Telegram? (y/n) [n]:"
read -r ENABLE_TG; ENABLE_TG="${ENABLE_TG:-n}"

ask "启用 QQ 机器人? (y/n) [n]:"
read -r ENABLE_QQ; ENABLE_QQ="${ENABLE_QQ:-n}"

FEISHU_APP_ID="" FEISHU_APP_SECRET="$EXISTING_FEISHU_SECRET"
TG_BOT_TOKEN="$EXISTING_TG_TOKEN"
QQ_APP_ID="" QQ_CLIENT_SECRET="$EXISTING_QQ_SECRET"

if [[ "$ENABLE_FEISHU" == "y" ]]; then
  ask "飞书 App ID:"; read -r FEISHU_APP_ID
  ask "飞书 App Secret (留空保留现有值):"; read -rs INPUT_FEISHU_SECRET; echo ""
  FEISHU_APP_SECRET="${INPUT_FEISHU_SECRET:-$FEISHU_APP_SECRET}"
fi

if [[ "$ENABLE_TG" == "y" ]]; then
  ask "Telegram Bot Token (留空保留现有值):"; read -rs INPUT_TG_BOT_TOKEN; echo ""
  TG_BOT_TOKEN="${INPUT_TG_BOT_TOKEN:-$TG_BOT_TOKEN}"
  ask "Telegram 代理 (留空跳过):"; read -r TG_PROXY
fi

if [[ "$ENABLE_QQ" == "y" ]]; then
  ask "QQ Bot App ID:"; read -r QQ_APP_ID
  ask "QQ Bot Client Secret (留空保留现有值):"; read -rs INPUT_QQ_CLIENT_SECRET; echo ""
  QQ_CLIENT_SECRET="${INPUT_QQ_CLIENT_SECRET:-$QQ_CLIENT_SECRET}"
fi

echo ""
echo "=== 模型配置 ==="
ask "主模型 (如 gemma-4-e2b-edge) [gemma-4-e2b-edge]:"
read -r PRIMARY_MODEL; PRIMARY_MODEL="${PRIMARY_MODEL:-gemma-4-e2b-edge}"

ask "轻量模型 (用于审议/简报) [gemma-4-e2b-edge]:"
read -r LIGHT_MODEL; LIGHT_MODEL="${LIGHT_MODEL:-gemma-4-e2b-edge}"

DEFAULT_DATABASE_URL="${EXISTING_DATABASE_URL:-dbname=mission_control user=$USER}"
echo ""
echo "=== PostgreSQL 配置 ==="
ask "OpenClaw Team PostgreSQL URL [${DEFAULT_DATABASE_URL}]:"
read -r INPUT_DATABASE_URL
MISSION_CONTROL_DATABASE_URL="${INPUT_DATABASE_URL:-$DEFAULT_DATABASE_URL}"

echo ""
info "开始安装..."

DASHBOARD_SCRIPT_SOURCE="$BACKEND_DIR/collaboration_dashboard.py"
if [[ ! -f "$DASHBOARD_SCRIPT_SOURCE" ]]; then
  error "缺少 dashboard 主脚本: $BACKEND_DIR/collaboration_dashboard.py"
fi

# ---------- 创建目录结构 ----------
mkdir -p "$OPENCLAW_DIR"/{skills,logs,credentials}
chmod 700 "$OPENCLAW_DIR" "$OPENCLAW_DIR/credentials"

for agent in "${ALL_AGENTS[@]}"; do
  mkdir -p "$OPENCLAW_DIR/workspace-$agent"/{scripts,data,memory,shared-context/knowledge-base,skills}
  mkdir -p "$OPENCLAW_DIR/agents/$agent/agent"
done

info "目录结构已创建 (${#ALL_AGENTS[@]} 个 agent)"

# ---------- 复制核心脚本 ----------
for agent in "${ALL_AGENTS[@]}"; do
  rm -rf \
    "$OPENCLAW_DIR/workspace-$agent/scripts/backend" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/application" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/adapters" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/domain" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/presentation" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/services" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/dispatchers" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/stores" \
    "$OPENCLAW_DIR/workspace-$agent/scripts/integrations"
  rm -f "$OPENCLAW_DIR/workspace-$agent/scripts/"{dashboard_store.py,admin_service.py,chat_data_service.py,customer_access_service.py,orchestration_service.py,dashboard_data_service.py,desktop_service.py,management_service.py,runtime_service.py,route_aliases.py,query_route_dispatcher.py,task_command_dispatcher.py,agent_command_dispatcher.py,management_command_dispatcher.py,chat_command_dispatcher.py,platform_command_dispatcher.py,command_route_dispatcher.py,rest_route_dispatcher.py,http_route_dispatcher.py,env_utils.py}
  cp "$BACKEND_DIR/kanban_update.py" "$OPENCLAW_DIR/workspace-$agent/scripts/"
  cp "$BACKEND_DIR/file_lock.py" "$OPENCLAW_DIR/workspace-$agent/scripts/"
  cp "$BACKEND_DIR/model_decision_adapter.py" "$OPENCLAW_DIR/workspace-$agent/scripts/"
  cp "$BACKEND_DIR/refresh_live_data.py" "$OPENCLAW_DIR/workspace-$agent/scripts/"
  cp "$BACKEND_DIR/health_dashboard.py" "$OPENCLAW_DIR/workspace-$agent/scripts/"
  cp "$BACKEND_DIR/env_utils.py" "$OPENCLAW_DIR/workspace-$agent/scripts/"
  cp "$DASHBOARD_SCRIPT_SOURCE" "$OPENCLAW_DIR/workspace-$agent/scripts/collaboration_dashboard.py"
  sync_tree "$BACKEND_DIR" "$OPENCLAW_DIR/workspace-$agent/scripts/backend"
done
info "看板脚本已部署到所有 workspace"

# ---------- 生成 SOUL.md + 共享上下文 + 看板配置 ----------
PYTHONPATH="$INSTALL_LIB_DIR${PYTHONPATH:+:$PYTHONPATH}" python3 "$PROJECT_DIR/platform/bin/install/lib/render_templates.py" \
  --theme "$THEME_FILE" \
  --openclaw-dir "$OPENCLAW_DIR" \
  --primary-model "$PRIMARY_MODEL" \
  --light-model "$LIGHT_MODEL" \
  --task-prefix "$TASK_PREFIX"
info "SOUL.md / HEARTBEAT.md / MEMORY.md / memory/ / shared-context / kanban_config.json 已生成"

# ---------- 生成 openclaw.json ----------
GEN_CONFIG_ARGS=(
  --theme "$THEME_FILE"
  --openclaw-dir "$OPENCLAW_DIR"
  --primary-model "$PRIMARY_MODEL"
  --light-model "$LIGHT_MODEL"
  --feishu-app-id "$FEISHU_APP_ID"
  --feishu-app-secret "$FEISHU_APP_SECRET"
  --tg-bot-token "$TG_BOT_TOKEN"
  --tg-proxy "${TG_PROXY:-}"
  --qq-app-id "$QQ_APP_ID"
  --qq-client-secret "$QQ_CLIENT_SECRET"
  --task-prefix "$TASK_PREFIX"
  --project-dir "$PROJECT_DIR"
  --deployment-mode "$DEPLOYMENT_MODE"
  --deployment-profile "$DEPLOYMENT_PROFILE"
)
if [[ -f "$OPENCLAW_DIR/openclaw.json" ]]; then
  GEN_CONFIG_ARGS+=(--base-config "$OPENCLAW_DIR/openclaw.json")
fi
if [[ -z "$EXISTING_GATEWAY_TOKEN" ]]; then
  if [[ "$DEPLOYMENT_PROFILE" == "single_tenant_prod" || "$MISSION_CONTROL_ENVIRONMENT" == "prod" || "$MISSION_CONTROL_ENVIRONMENT" == "production" ]]; then
    error "单租户生产部署要求预先注入 GATEWAY_AUTH_TOKEN，setup 不会再自动生成。"
  fi
  EXISTING_GATEWAY_TOKEN="$(openssl rand -hex 24)"
fi
export FEISHU_APP_SECRET="$FEISHU_APP_SECRET"
export TELEGRAM_BOT_TOKEN="$TG_BOT_TOKEN"
export QQBOT_CLIENT_SECRET="$QQ_CLIENT_SECRET"
export GATEWAY_AUTH_TOKEN="$EXISTING_GATEWAY_TOKEN"
if [[ "$DEPLOYMENT_PROFILE" == "single_tenant_prod" || "$MISSION_CONTROL_ENVIRONMENT" == "prod" || "$MISSION_CONTROL_ENVIRONMENT" == "production" ]]; then
  has_agent_auth_seed || error "单租户生产部署要求预先注入 Agent 鉴权配置（provider API key 或 OPENCLAW_AUTH_SOURCE_FILE / OPENCLAW_AUTH_PROFILES_*）。"
fi
PYTHONPATH="$INSTALL_LIB_DIR${PYTHONPATH:+:$PYTHONPATH}" python3 "$PROJECT_DIR/platform/bin/install/lib/generate_config.py" "${GEN_CONFIG_ARGS[@]}"
info "openclaw.json 已生成"
normalize_agent_permissions

# ---------- 写入 .env ----------
cat > "$OPENCLAW_DIR/.env" << ENVEOF
# OpenClaw Team Secrets — 请勿提交版本控制
FEISHU_APP_SECRET=${FEISHU_APP_SECRET}
TELEGRAM_BOT_TOKEN=${TG_BOT_TOKEN}
QQBOT_CLIENT_SECRET=${QQ_CLIENT_SECRET}
GATEWAY_AUTH_TOKEN=${EXISTING_GATEWAY_TOKEN}
MISSION_CONTROL_DATABASE_URL=${MISSION_CONTROL_DATABASE_URL}
ENVEOF
chmod 600 "$OPENCLAW_DIR/.env"
info "Secrets 与 PostgreSQL 配置已写入 .env (权限 600)"

sync_agent_auth_profiles

# ---------- 初始化看板数据 ----------
info "任务主存储已切换到 PostgreSQL task_records"

MISSION_CONTROL_SKIP_AUTOMATION_CYCLE=1 \
  python3 "$OPENCLAW_DIR/workspace-${ALL_AGENTS[0]}/scripts/collaboration_dashboard.py" --quiet || true
info "协同态势看板已生成"

if [[ -f "$PROJECT_DIR/platform/bin/runtime/build_frontend.sh" ]]; then
  frontend_json=""
  if frontend_json="$(bash "$PROJECT_DIR/platform/bin/runtime/build_frontend.sh" --project-dir "$PROJECT_DIR" --json)"; then
    frontend_ok=""
    frontend_status=""
    frontend_message=""
    IFS=$'\t' read -r frontend_ok frontend_status frontend_message <<<"$(python3 - "$frontend_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
ok = "1" if payload.get("ok") else "0"
status = str(payload.get("status") or "").strip()
message = str(payload.get("message") or "").strip().replace("\n", " ")
print("\t".join([ok, status, message]))
PY
)"
    if [[ "$frontend_ok" == "1" ]]; then
      info "$frontend_message"
    else
      warn "$frontend_message"
      warn "前端未就绪时，当前 UI 路由会返回 503。"
    fi
  else
    warn "前端构建脚本执行失败，当前 UI 路由可能返回 503。"
  fi
fi

# ---------- 权限加固 ----------
chmod 600 "$OPENCLAW_DIR/openclaw.json"
info "文件权限已加固"

if should_sync_container; then
  info "检测到 mission-control 容器环境，准备同步容器版本..."
  bash "$PROJECT_DIR/platform/bin/deploy/sync_local_and_container.sh" \
    --dir "$OPENCLAW_DIR" \
    --project-dir "$PROJECT_DIR" \
    --skip-local \
    --build-frontend
elif [[ "${SYNC_CONTAINER_MODE:-auto}" == "always" ]]; then
  info "按要求同步容器版本..."
  bash "$PROJECT_DIR/platform/bin/deploy/sync_local_and_container.sh" \
    --dir "$OPENCLAW_DIR" \
    --project-dir "$PROJECT_DIR" \
    --skip-local \
    --build-frontend
fi

# ---------- 验证 ----------
echo ""
echo "=== 安装完成 ==="
info "Agent 数量: ${#ALL_AGENTS[@]}"
info "主题: $THEME"
info "配置文件: $OPENCLAW_DIR/openclaw.json"
echo ""
echo "下一步:"
echo "  1. 启动网关:  $OPENCLAW_BIN gateway run"
echo "  2. 健康检查:  $OPENCLAW_BIN gateway health"
echo "  3. 安全审计:  $OPENCLAW_BIN security audit"
echo "  4. Product UI: http://127.0.0.1:18890/"
echo ""
echo "发送消息给机器人即可开始使用！"
