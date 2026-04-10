from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import sys
import threading
import time
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import timedelta
from pathlib import Path


def _svc():
    module = sys.modules.get("backend.collaboration_dashboard")
    if module is not None:
        return module
    module = sys.modules.get("collaboration_dashboard")
    if module is not None:
        return module
    main = sys.modules.get("__main__")
    if main is not None and str(getattr(main, "__file__", "")).endswith("collaboration_dashboard.py"):
        return main
    import importlib

    try:
        return importlib.import_module("backend.collaboration_dashboard")
    except ModuleNotFoundError:
        return importlib.import_module("collaboration_dashboard")


def normalize_username(value):
    return str(value or "").strip().lower()


def role_meta(role):
    svc = _svc()
    return svc.USER_ROLES.get(role, svc.USER_ROLES["viewer"])


def permissions_for_role(role):
    meta = role_meta(role)
    return {
        "read": "read" in meta["permissions"],
        "taskWrite": "task_write" in meta["permissions"],
        "conversationWrite": "conversation_write" in meta["permissions"],
        "themeWrite": "theme_write" in meta["permissions"],
        "adminWrite": "admin_write" in meta["permissions"],
        "auditView": "audit_view" in meta["permissions"],
    }


def encode_base64url(raw):
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def decode_base64url(raw):
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def hash_password(password, salt=None, iterations=None):
    svc = _svc()
    if not password:
        raise ValueError("password required")
    if iterations is None:
        iterations = svc.PASSWORD_HASH_ITERATIONS
    salt_bytes = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_bytes, iterations)
    return "pbkdf2_sha256${}${}${}".format(
        iterations,
        encode_base64url(salt_bytes),
        encode_base64url(digest),
    )


def verify_password(password, encoded):
    try:
        algorithm, iterations_text, salt_text, _digest_text = str(encoded).split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        expected = hash_password(
            password,
            salt=decode_base64url(salt_text),
            iterations=int(iterations_text),
        )
        return hmac.compare_digest(expected, encoded)
    except (ValueError, TypeError):
        return False


def load_product_users(openclaw_dir):
    svc = _svc()
    return svc.store_load_product_users(openclaw_dir)


def save_product_users(openclaw_dir, users):
    svc = _svc()
    return svc.store_save_product_users(openclaw_dir, users)


def upsert_product_user(openclaw_dir, user):
    svc = _svc()
    return svc.store_upsert_product_user(openclaw_dir, user)


def touch_product_user_login(openclaw_dir, username, logged_in_at=""):
    svc = _svc()
    return svc.store_touch_product_user_login(openclaw_dir, username, logged_in_at=logged_in_at)


def safe_user_record(user):
    role = user.get("role", "viewer")
    meta = role_meta(role)
    return {
        "id": user.get("id"),
        "username": user.get("username"),
        "displayName": user.get("displayName") or user.get("username"),
        "role": role,
        "roleLabel": meta["label"],
        "roleDescription": meta["description"],
        "status": user.get("status", "active"),
        "createdAt": user.get("createdAt", ""),
        "lastLoginAt": user.get("lastLoginAt", ""),
    }


DEFAULT_PRODUCT_USER_BOOTSTRAP_VERSION = 1
DEFAULT_OPENCLAW_CONFIG_BOOTSTRAP_VERSION = 1
DEFAULT_BOOTSTRAP_THEME_NAME = "corporate"
def _generate_fallback_password():
    """Generate a random password when env var is not set."""
    return secrets.token_urlsafe(16)

DEFAULT_PRODUCT_USER_SEEDS = (
    ("owner", "Owner", "owner", "MISSION_CONTROL_OWNER_PASSWORD", ""),
    ("operator", "Operator", "operator", "MISSION_CONTROL_OPERATOR_PASSWORD", ""),
    ("viewer", "Viewer", "viewer", "MISSION_CONTROL_VIEWER_PASSWORD", ""),
)
DEFAULT_OPENCLAW_AGENT_SEEDS = (
    {"id": "assistant", "name": "小智", "emoji": "✨", "model": "glm-5-turbo", "params": {"profile": "user", "tier": "primary"}, "default": True},
    {"id": "vp_strategy", "name": "策略官", "emoji": "🧭", "model": "claude-sonnet-4-20250514"},
    {"id": "vp_compliance", "name": "合规官", "emoji": "🛡️", "model": "claude-sonnet-4-20250514"},
    {"id": "engineering", "name": "工程师", "emoji": "🛠️", "model": "glm-5-turbo"},
    {"id": "qa", "name": "质保官", "emoji": "✅", "model": "claude-sonnet-4-20250514"},
    {"id": "coo", "name": "运营官", "emoji": "📦", "model": "glm-5-turbo"},
    {"id": "devops", "name": "平台官", "emoji": "⚙️", "model": "glm-5-turbo"},
    {"id": "marketing", "name": "市场官", "emoji": "📣", "model": "glm-5-turbo"},
    {"id": "briefing", "name": "情报官", "emoji": "📰", "model": "glm-5-turbo"},
    {"id": "hr", "name": "人才官", "emoji": "🤝", "model": "glm-5-turbo"},
    {"id": "data_team", "name": "数据官", "emoji": "📊", "model": "glm-5-turbo"},
)


def _deployment_seed_password(openclaw_dir, env_key, fallback):
    svc = _svc()
    value = str(os.environ.get(env_key) or svc.read_env_value(openclaw_dir, env_key) or "").strip()
    if value:
        return value
    # Generate random password if no env var and no fallback
    generated = _generate_fallback_password()
    print(f"[bootstrap] No {env_key} set — generated random password for this user. Set {env_key} in .env for a stable password.")
    return generated


def default_product_user_blueprints(openclaw_dir):
    svc = _svc()
    created_at = svc.now_iso()
    items = []
    for username, display_name, role, env_key, fallback_password in DEFAULT_PRODUCT_USER_SEEDS:
        items.append(
            {
                "id": secrets.token_hex(8),
                "username": username,
                "displayName": display_name,
                "role": role,
                "passwordHash": hash_password(_deployment_seed_password(openclaw_dir, env_key, fallback_password)),
                "status": "active",
                "createdAt": created_at,
                "lastLoginAt": "",
            }
        )
    return items


def default_openclaw_agent_blueprints(openclaw_dir):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    items = []
    for seed in DEFAULT_OPENCLAW_AGENT_SEEDS:
        agent_id = str(seed.get("id") or "").strip()
        if not agent_id:
            continue
        workspace_path = openclaw_dir / f"workspace-{agent_id}"
        payload = {
            "id": agent_id,
            "default": bool(seed.get("default")),
            "workspace": str(workspace_path),
            "model": str(seed.get("model") or "").strip(),
            "identity": {
                "name": str(seed.get("name") or agent_id).strip(),
                "emoji": str(seed.get("emoji") or "").strip(),
            },
        }
        params = seed.get("params") if isinstance(seed.get("params"), dict) else {}
        if params:
            payload["params"] = deepcopy(params)
        items.append(payload)
    return items


def _ensure_default_agent_workspace_scaffolds(openclaw_dir, agents):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    for agent in agents or []:
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        workspace = Path(agent.get("workspace") or (openclaw_dir / f"workspace-{agent_id}")).expanduser()
        workspace.mkdir(parents=True, exist_ok=True)
        (workspace / "memory").mkdir(parents=True, exist_ok=True)
        (workspace / "data").mkdir(parents=True, exist_ok=True)
        memory_path = workspace / "MEMORY.md"
        if not memory_path.exists():
            memory_path.write_text(f"# {agent_id} Memory\n\n记录这个 Agent 的长期偏好、关键决策和项目经验。\n", encoding="utf-8")
        for relative_path, title, body in (
            ("memory/preferences.md", "Preferences", f"- {agent_id} prefers concise operating updates.\n"),
            ("memory/decisions.md", "Decisions", f"- Keep {agent_id} aligned to the shared rollout plan.\n"),
            ("memory/project-knowledge.md", "Project Knowledge", f"OpenClaw Team keeps local delivery signals for {agent_id}.\n"),
        ):
            path = workspace / relative_path
            if not path.exists():
                path.write_text(f"# {title}\n\n{body}", encoding="utf-8")
        agent_dir = openclaw_dir / "agents" / agent_id / "agent"
        agent_dir.mkdir(parents=True, exist_ok=True)


def ensure_default_openclaw_config_bootstrap(openclaw_dir, metadata=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    current_config = svc.load_config(openclaw_dir)
    current_config = deepcopy(current_config) if isinstance(current_config, dict) else {}
    metadata = deepcopy(metadata if isinstance(metadata, dict) else svc.load_project_metadata(openclaw_dir, config=current_config))
    current_agents = ((current_config.get("agents", {}) if isinstance(current_config.get("agents"), dict) else {}) or {}).get("list")
    current_agents = [item for item in current_agents if isinstance(item, dict) and str(item.get("id") or "").strip()] if isinstance(current_agents, list) else []
    changed_config = False
    changed_metadata = False
    try:
        bootstrap_version = int(metadata.get("openclawConfigBootstrapVersion") or 0)
    except (TypeError, ValueError):
        bootstrap_version = 0

    if not current_agents:
        next_config = deepcopy(current_config)
        existing_agents_payload = next_config.get("agents") if isinstance(next_config.get("agents"), dict) else {}
        defaults_payload = deepcopy(existing_agents_payload.get("defaults") if isinstance(existing_agents_payload.get("defaults"), dict) else {})
        memory_search = deepcopy(defaults_payload.get("memorySearch") if isinstance(defaults_payload.get("memorySearch"), dict) else {})
        memory_search["enabled"] = True
        memory_search["provider"] = str(memory_search.get("provider") or "local").strip() or "local"
        defaults_payload["memorySearch"] = memory_search
        bootstrapped_agents = default_openclaw_agent_blueprints(openclaw_dir)
        next_config["agents"] = {
            **existing_agents_payload,
            "defaults": defaults_payload,
            "list": bootstrapped_agents,
        }
        _ensure_default_agent_workspace_scaffolds(openclaw_dir, bootstrapped_agents)
        svc.save_config(openclaw_dir, next_config)
        current_config = next_config
        current_agents = bootstrapped_agents
        changed_config = True

    if not str(metadata.get("theme") or "").strip():
        metadata["theme"] = DEFAULT_BOOTSTRAP_THEME_NAME
        changed_metadata = True
    current_display_name = str(metadata.get("displayName") or "").strip()
    if not current_display_name:
        metadata["displayName"] = str(svc.THEME_CATALOG.get(DEFAULT_BOOTSTRAP_THEME_NAME, {}).get("displayName") or "现代企业").strip()
        changed_metadata = True

    if changed_config or bootstrap_version < DEFAULT_OPENCLAW_CONFIG_BOOTSTRAP_VERSION or changed_metadata:
        metadata["openclawConfigBootstrapVersion"] = int(DEFAULT_OPENCLAW_CONFIG_BOOTSTRAP_VERSION)
        metadata["openclawConfigBootstrapAt"] = svc.now_iso()
        metadata["openclawConfigBootstrapSource"] = (
            "product-default-self-heal"
            if changed_config and bootstrap_version >= int(DEFAULT_OPENCLAW_CONFIG_BOOTSTRAP_VERSION)
            else "product-default"
        )
        metadata["openclawConfigBootstrapAgentCount"] = len(current_agents)
        svc.save_project_metadata(openclaw_dir, metadata)
    return current_config, metadata


def ensure_default_product_users_bootstrap(openclaw_dir, metadata=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    current_metadata = deepcopy(metadata if isinstance(metadata, dict) else svc.load_project_metadata(openclaw_dir))
    try:
        bootstrap_version = int(current_metadata.get("productUserBootstrapVersion") or 0)
    except (TypeError, ValueError):
        bootstrap_version = 0
    users = load_product_users(openclaw_dir)
    created = 0
    if not users:
        for payload in default_product_user_blueprints(openclaw_dir):
            upsert_product_user(openclaw_dir, payload)
            created += 1
        users = load_product_users(openclaw_dir)
    if created or bootstrap_version < DEFAULT_PRODUCT_USER_BOOTSTRAP_VERSION:
        current_metadata["productUserBootstrapVersion"] = int(DEFAULT_PRODUCT_USER_BOOTSTRAP_VERSION)
        current_metadata["productUserBootstrapAt"] = svc.now_iso()
        current_metadata["productUserBootstrapSource"] = (
            "product-default-self-heal"
            if created and bootstrap_version >= int(DEFAULT_PRODUCT_USER_BOOTSTRAP_VERSION)
            else ("existing-users" if users and not created else "product-default")
        )
        current_metadata["productUserBootstrapCreated"] = int(created)
        svc.save_project_metadata(openclaw_dir, current_metadata)
    return current_metadata, users


def ensure_default_install_bootstrap(openclaw_dir):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    config, metadata = ensure_default_openclaw_config_bootstrap(openclaw_dir)
    metadata, users = ensure_default_product_users_bootstrap(openclaw_dir, metadata=metadata)
    try:
        sync_current_installation_registry(openclaw_dir, config)
    except Exception:
        pass
    # Backfill model provider configs from auth-profiles.json → DB.
    # If auth-profiles.json has provider keys that the DB doesn't have yet,
    # insert them so that provider_config_env_map() returns correct env vars.
    backfill_result = {}
    try:
        from backend.application.services.runtime_core import backfill_model_provider_configs_from_auth
        backfill_result = backfill_model_provider_configs_from_auth(openclaw_dir)
        if backfill_result.get("backfilled"):
            print(
                f"[bootstrap] Backfilled {backfill_result['backfilled']} model provider(s) "
                f"from {backfill_result.get('source', 'auth-profiles.json')}: "
                f"{', '.join(backfill_result.get('providers', []))}"
            )
    except Exception:
        pass
    return {
        "config": config,
        "metadata": metadata,
        "users": [safe_user_record(user) for user in users],
        "providerBackfill": backfill_result,
    }


def append_audit_event(openclaw_dir, action, actor, outcome="success", detail="", meta=None):
    svc = _svc()
    return svc.store_append_audit_event(openclaw_dir, action, actor, outcome=outcome, detail=detail, meta=meta)


def load_audit_events(openclaw_dir, limit=80):
    svc = _svc()
    return svc.store_load_audit_events(openclaw_dir, limit=limit)


def create_product_user(openclaw_dir, username, display_name, role, password):
    svc = _svc()
    username = normalize_username(username)
    if not username:
        raise RuntimeError("用户名不能为空。")
    if role not in svc.USER_ROLES:
        raise RuntimeError(f"未知角色：{role}")
    if len(password or "") < 8:
        raise RuntimeError("密码至少需要 8 位。")
    users = load_product_users(openclaw_dir)
    if any(user["username"] == username for user in users):
        raise RuntimeError(f"账号 {username} 已存在。")
    user = {
        "id": secrets.token_hex(8),
        "username": username,
        "displayName": (display_name or username).strip(),
        "role": role,
        "passwordHash": hash_password(password),
        "status": "active",
        "createdAt": svc.now_iso(),
        "lastLoginAt": "",
    }
    stored_user = upsert_product_user(openclaw_dir, user) or user
    return safe_user_record(stored_user)


def find_product_user_entry(users, username):
    normalized = normalize_username(username)
    for index, user in enumerate(users):
        if user["username"] == normalized:
            return index, user
    return -1, None


def ensure_active_owner_guard(users, target_user, next_role=None, next_status=None):
    if not target_user:
        return
    current_role = target_user.get("role", "viewer")
    current_status = target_user.get("status", "active")
    role_after = next_role or current_role
    status_after = next_status or current_status
    active_owners = [
        user for user in users if user.get("role") == "owner" and user.get("status", "active") == "active"
    ]
    target_is_last_active_owner = current_role == "owner" and current_status == "active" and len(active_owners) <= 1
    if target_is_last_active_owner and (role_after != "owner" or status_after != "active"):
        raise RuntimeError("至少要保留一个激活状态的 Owner，不能把最后一个 Owner 降级或停用。")


def update_product_user_access(openclaw_dir, username, role, status):
    svc = _svc()
    username = normalize_username(username)
    if not username:
        raise RuntimeError("请先选择一个团队账号。")
    if role not in svc.USER_ROLES:
        raise RuntimeError(f"未知角色：{role}")
    if status not in {"active", "suspended"}:
        raise RuntimeError(f"未知账号状态：{status}")
    users = load_product_users(openclaw_dir)
    index, user = find_product_user_entry(users, username)
    if not user:
        raise RuntimeError(f"账号 {username} 不存在。")
    ensure_active_owner_guard(users, user, next_role=role, next_status=status)
    users[index]["role"] = role
    users[index]["status"] = status
    stored_user = upsert_product_user(openclaw_dir, users[index]) or users[index]
    return safe_user_record(stored_user)


def reset_product_user_password(openclaw_dir, username, password):
    username = normalize_username(username)
    if not username:
        raise RuntimeError("请先选择一个团队账号。")
    if len(password or "") < 8:
        raise RuntimeError("重置后的密码至少需要 8 位。")
    users = load_product_users(openclaw_dir)
    index, user = find_product_user_entry(users, username)
    if not user:
        raise RuntimeError(f"账号 {username} 不存在。")
    users[index]["passwordHash"] = hash_password(password)
    stored_user = upsert_product_user(openclaw_dir, users[index]) or users[index]
    return safe_user_record(stored_user)


def update_product_user_login(openclaw_dir, username):
    svc = _svc()
    touch_product_user_login(openclaw_dir, username, logged_in_at=svc.now_iso())


def default_installation_label(config, openclaw_dir):
    svc = _svc()
    metadata = svc.load_project_metadata(openclaw_dir, config=config)
    theme_name = metadata.get("theme", svc.DEFAULT_THEME_NAME)
    return (
        metadata.get("displayName")
        or svc.THEME_CATALOG.get(theme_name, {}).get("displayName")
        or Path(openclaw_dir).expanduser().name
        or str(openclaw_dir)
    )


def sync_current_installation_registry(openclaw_dir, config):
    svc = _svc()
    resolved_dir = str(Path(openclaw_dir).expanduser().resolve())
    metadata = svc.load_project_metadata(openclaw_dir, config=config)
    return svc.store_upsert_product_installation(
        openclaw_dir,
        {
            "openclawDir": resolved_dir,
            "label": default_installation_label(config, resolved_dir),
            "projectDir": str(metadata.get("projectDir", "")).strip(),
            "theme": str(metadata.get("theme", svc.DEFAULT_THEME_NAME)).strip(),
            "routerAgentId": svc.get_router_agent_id(config),
        },
    )


def summarize_installation_record(current_openclaw_dir, installation, now):
    svc = _svc()
    openclaw_path = Path(str(installation.get("openclawDir", "") or "")).expanduser()
    current_path = Path(current_openclaw_dir).expanduser().resolve()
    resolved_target = openclaw_path.resolve() if openclaw_path.exists() else openclaw_path
    summary = {
        "id": installation.get("id", ""),
        "label": installation.get("label") or openclaw_path.name or str(openclaw_path),
        "openclawDir": str(openclaw_path),
        "projectDir": installation.get("projectDir", ""),
        "theme": installation.get("theme", ""),
        "themeLabel": svc.THEME_CATALOG.get(installation.get("theme", ""), {}).get("displayName", installation.get("theme", "") or "未知主题"),
        "routerAgentId": installation.get("routerAgentId", ""),
        "agentCount": 0,
        "activeTasks": 0,
        "blockedTasks": 0,
        "status": "missing",
        "statusLabel": "目录缺失",
        "statusNote": "登记的 OpenClaw 目录当前不存在，建议检查路径或移除旧实例。",
        "updatedAt": installation.get("updatedAt", ""),
        "updatedAgo": svc.format_age(svc.parse_iso(installation.get("updatedAt")), now),
        "current": resolved_target == current_path,
    }
    config_path = openclaw_path / "openclaw.json"
    if not openclaw_path.exists():
        return summary
    if not config_path.exists():
        summary["status"] = "broken"
        summary["statusLabel"] = "缺少配置"
        summary["statusNote"] = "目录存在，但没有找到 openclaw.json。"
        return summary
    try:
        config = svc.load_config(openclaw_path)
        metadata = svc.load_project_metadata(openclaw_path, config=config)
        theme_name = metadata.get("theme", "") or summary["theme"] or svc.DEFAULT_THEME_NAME
        tasks = svc.merge_tasks(openclaw_path, config)
        active_tasks = 0
        blocked_tasks = 0
        for task in tasks:
            state = str(task.get("state", task.get("status", ""))).lower()
            if state not in svc.TERMINAL_STATES:
                active_tasks += 1
            if state == "blocked":
                blocked_tasks += 1
        generated_at = ""
        dashboard_snapshot = svc.load_json(openclaw_path / "dashboard" / "collaboration-dashboard.json", {})
        if isinstance(dashboard_snapshot, dict):
            generated_at = dashboard_snapshot.get("generatedAt", "") or ""
        updated_at = generated_at or installation.get("updatedAt", "")
        summary.update(
            {
                "label": metadata.get("displayName") or summary["label"],
                "projectDir": metadata.get("projectDir", "") or summary["projectDir"],
                "theme": theme_name,
                "themeLabel": svc.THEME_CATALOG.get(theme_name, {}).get("displayName", theme_name),
                "routerAgentId": svc.get_router_agent_id(config),
                "agentCount": len(svc.load_agents(config)),
                "activeTasks": active_tasks,
                "blockedTasks": blocked_tasks,
                "status": "current" if summary["current"] else "ready",
                "statusLabel": "当前实例" if summary["current"] else "可管理",
                "statusNote": "本地路径可达，配置完整，可以纳入产品控制平面。" if not summary["current"] else "这就是你当前打开的OpenClaw Team 所属实例。",
                "updatedAt": updated_at,
                "updatedAgo": svc.format_age(svc.parse_iso(updated_at), now) if updated_at else summary["updatedAgo"],
            }
        )
        return summary
    except Exception as error:
        summary["status"] = "broken"
        summary["statusLabel"] = "读取失败"
        summary["statusNote"] = f"读取安装实例时发生异常：{error}"
        return summary


def register_installation(openclaw_dir, target_dir, label=""):
    svc = _svc()
    raw_target = str(target_dir or "").strip()
    if not raw_target:
        raise RuntimeError("请先输入 OpenClaw 安装目录。")
    candidate = Path(raw_target).expanduser()
    if not candidate.exists():
        raise RuntimeError(f"目录不存在：{candidate}")
    resolved = candidate.resolve()
    config_path = resolved / "openclaw.json"
    if not config_path.exists():
        raise RuntimeError(f"目录 {resolved} 中没有找到 openclaw.json。")
    config = svc.load_config(resolved)
    if not svc.load_agents(config):
        raise RuntimeError("该安装目录的 openclaw.json 没有可识别的 agents。")
    metadata = svc.load_project_metadata(resolved, config=config)
    theme_name = metadata.get("theme", svc.DEFAULT_THEME_NAME)
    return svc.store_upsert_product_installation(
        openclaw_dir,
        {
            "openclawDir": str(resolved),
            "label": str(label or "").strip() or default_installation_label(config, resolved),
            "projectDir": str(metadata.get("projectDir", "")).strip(),
            "theme": theme_name,
            "routerAgentId": svc.get_router_agent_id(config),
        },
    )


def remove_installation(openclaw_dir, target_dir):
    raw_target = str(target_dir or "").strip()
    if not raw_target:
        raise RuntimeError("请先选择要移除的安装实例。")
    current_dir = str(Path(openclaw_dir).expanduser().resolve())
    candidate = str(Path(raw_target).expanduser().resolve())
    if candidate == current_dir:
        raise RuntimeError("不能移除当前正在运行的OpenClaw Team 实例。")
    svc = _svc()
    if not svc.store_delete_product_installation(openclaw_dir, candidate):
        raise RuntimeError("指定的安装实例不存在。")
    return candidate


def find_tenant_record(openclaw_dir, tenant_ref):
    tenant_ref = str(tenant_ref or "").strip()
    if not tenant_ref:
        return None
    svc = _svc()
    for tenant in svc.store_list_tenants(openclaw_dir):
        if tenant.get("id") == tenant_ref or tenant.get("slug") == tenant_ref:
            return tenant
    return None


def tenant_primary_openclaw_dir(openclaw_dir, tenant):
    if not tenant:
        return None
    candidate = str(tenant.get("primaryOpenclawDir", "")).strip()
    if candidate:
        return Path(candidate).expanduser().resolve()
    svc = _svc()
    installations = svc.store_list_tenant_installations(openclaw_dir, tenant.get("id", ""))
    primary = next((item for item in installations if item.get("role") == "primary"), None)
    if primary and primary.get("openclawDir"):
        return Path(primary["openclawDir"]).expanduser().resolve()
    if installations and installations[0].get("openclawDir"):
        return Path(installations[0]["openclawDir"]).expanduser().resolve()
    return None


def build_tenant_admin_data(openclaw_dir, now):
    svc = _svc()
    tenants = svc.store_list_tenants(openclaw_dir)
    tenant_installations = svc.store_list_tenant_installations(openclaw_dir)
    installation_registry = {
        item.get("openclawDir"): item
        for item in svc.store_load_product_installations(openclaw_dir)
    }
    api_keys_by_tenant = defaultdict(list)
    for item in svc.store_list_tenant_api_keys(openclaw_dir):
        api_keys_by_tenant[item.get("tenantId", "")].append(item)

    installation_groups = defaultdict(list)
    for item in tenant_installations:
        installation_groups[item.get("tenantId", "")].append(item)

    items = []
    for tenant in tenants:
        primary_dir = tenant_primary_openclaw_dir(openclaw_dir, tenant)
        tenant_summary = None
        if primary_dir and primary_dir.exists():
            try:
                tenant_config = svc.load_config(primary_dir)
                tenant_tasks = svc.merge_tasks(primary_dir, tenant_config)
                tenant_summary = {
                    "taskIndex": tenant_tasks,
                    "agents": svc.load_agents(tenant_config),
                    "generatedAt": max(
                        [item.get("updatedAt", "") for item in tenant_tasks if item.get("updatedAt")] or [svc.now_iso()]
                    ),
                }
            except (OSError, ValueError, RuntimeError):
                tenant_summary = None
        installations = installation_groups.get(tenant.get("id", ""), [])
        task_index = (tenant_summary or {}).get("taskIndex", [])
        agents = (tenant_summary or {}).get("agents", [])
        items.append(
            {
                **tenant,
                "statusLabel": "Active" if tenant.get("status") == "active" else "Suspended",
                "primaryOpenclawDir": str(primary_dir) if primary_dir else tenant.get("primaryOpenclawDir", ""),
                "installationCount": len(installations),
                "activeTasks": sum(
                    1 for task in task_index if str(task.get("state", "")).strip().lower() not in svc.TERMINAL_STATES
                ),
                "blockedTasks": sum(1 for task in task_index if task.get("blocked")),
                "agentCount": len(agents),
                "apiKeyCount": len(api_keys_by_tenant.get(tenant.get("id", ""), [])),
                "lastUpdatedAt": (tenant_summary or {}).get("generatedAt", ""),
                "lastUpdatedAgo": svc.format_age(svc.parse_iso((tenant_summary or {}).get("generatedAt", "")), now)
                if (tenant_summary or {}).get("generatedAt")
                else "未同步",
                "installations": [
                    {
                        **item,
                        "theme": installation_registry.get(item.get("openclawDir", ""), {}).get("theme", ""),
                        "registeredLabel": installation_registry.get(item.get("openclawDir", ""), {}).get("label", item.get("label", "")),
                    }
                    for item in installations
                ],
            }
        )

    return {
        "items": items,
        "installations": tenant_installations,
        "apiKeys": [
            {
                **item,
                "tenantName": next(
                    (tenant.get("name") for tenant in tenants if tenant.get("id") == item.get("tenantId")),
                    item.get("tenantId", ""),
                ),
            }
            for item in svc.store_list_tenant_api_keys(openclaw_dir)
        ],
        "summary": {
            "total": len(items),
            "active": sum(1 for item in items if item.get("status") == "active"),
            "installations": len(tenant_installations),
            "apiKeys": sum(len(values) for values in api_keys_by_tenant.values()),
        },
    }


def api_scope_allows(granted_scopes, required_scope):
    required_scope = str(required_scope or "").strip()
    scopes = {str(item).strip() for item in granted_scopes or [] if str(item).strip()}
    if not required_scope:
        return True
    if "*" in scopes or required_scope in scopes:
        return True
    resource, _, action = required_scope.partition(":")
    if resource and f"{resource}:*" in scopes:
        return True
    if action and f"*:{action}" in scopes:
        return True
    if "tenant:read" in scopes and action == "read":
        return True
    return False


def build_external_api_reference():
    resources = [
        {
            "key": "catalog",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/catalog",
            "scope": "tenant:read",
            "summary": "返回租户可用 REST 资源目录与 curl 示例。",
        },
        {
            "key": "dashboard",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/dashboard",
            "scope": "dashboard:read",
            "summary": "读取租户完整控制台快照。",
        },
        {
            "key": "tasks_list",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/tasks",
            "scope": "tasks:read",
            "summary": "读取任务列表。",
        },
        {
            "key": "tasks_create",
            "method": "POST",
            "path": "/api/v1/tenants/{tenantId}/tasks",
            "scope": "tasks:write",
            "summary": "由外部系统创建任务并进入协同链路。",
        },
        {
            "key": "agents",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/agents",
            "scope": "agents:read",
            "summary": "读取租户 Agent 编组与运行态。",
        },
        {
            "key": "management",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/management",
            "scope": "tenant:read",
            "summary": "读取治理、告警、自动化与健康态。",
        },
        {
            "key": "runs",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/runs",
            "scope": "tasks:read",
            "summary": "读取运行批次与阶段看板。",
        },
        {
            "key": "deliverables",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/deliverables",
            "scope": "tasks:read",
            "summary": "读取交付产物列表。",
        },
        {
            "key": "deliverable_download",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/deliverables/{deliverableId}/download",
            "scope": "tasks:read",
            "summary": "下载指定交付产物的 zip 包。",
        },
        {
            "key": "conversations",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/conversations",
            "scope": "tenant:read",
            "summary": "读取会话目录与摘要。",
        },
        {
            "key": "communications",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/communications",
            "scope": "tenant:read",
            "summary": "读取沟通渠道、投递状态与审计轨迹。",
        },
        {
            "key": "context",
            "method": "GET",
            "path": "/api/v1/tenants/{tenantId}/context",
            "scope": "tenant:read",
            "summary": "读取 Context Hub、Agent Memory 和 Shared Context。",
        },
    ]
    return {
        "version": "v1",
        "basePath": "/api/v1/tenants/{tenantId}",
        "auth": [
            {"header": "Authorization", "value": "Bearer <tenant_api_key>"},
            {"header": "X-API-Key", "value": "<tenant_api_key>"},
        ],
        "notes": [
            "所有接口都返回 JSON；下载接口返回 application/zip。",
            "租户 API Key 只能访问所属 tenant，不能跨租户读取数据。",
            "浏览器跨域接入时，需要把来源域名加入允许的 CORS origins。",
        ],
        "resources": resources,
        "samples": [
            {
                "title": "读取租户 dashboard",
                "command": "curl -H 'Authorization: Bearer <tenant_api_key>' https://<host>/api/v1/tenants/<tenantId>/dashboard",
            },
            {
                "title": "创建任务",
                "command": "curl -X POST -H 'Content-Type: application/json' -H 'X-API-Key: <tenant_api_key>' -d '{\"title\":\"外部系统触发任务\",\"remark\":\"From CI/CD\"}' https://<host>/api/v1/tenants/<tenantId>/tasks",
            },
            {
                "title": "下载交付物",
                "command": "curl -L -H 'Authorization: Bearer <tenant_api_key>' https://<host>/api/v1/tenants/<tenantId>/deliverables/<deliverableId>/download --output deliverable.zip",
            },
        ],
    }


def tenant_rest_catalog_payload(tenant, tenant_dir):
    reference = deepcopy(build_external_api_reference())
    tenant_id = tenant.get("slug") or tenant.get("id") or "{tenantId}"
    for resource in reference.get("resources", []):
        resource["resolvedPath"] = str(resource.get("path", "")).replace("{tenantId}", tenant_id)
    for sample in reference.get("samples", []):
        command = str(sample.get("command", ""))
        command = command.replace("<tenantId>", tenant_id)
        command = command.replace("https://<host>", "http://127.0.0.1:18890")
        sample["command"] = command
    reference["tenant"] = {
        "id": tenant.get("id", ""),
        "name": tenant.get("name", ""),
        "slug": tenant.get("slug", ""),
        "primaryOpenclawDir": str(tenant_dir),
    }
    return reference


def build_admin_data(openclaw_dir, config, now, include_sensitive=True):
    svc = _svc()
    sync_current_installation_registry(openclaw_dir, config)
    users = [safe_user_record(user) for user in load_product_users(openclaw_dir)]
    audit_events = load_audit_events(openclaw_dir, limit=60)
    installations = [
        summarize_installation_record(openclaw_dir, item, now)
        for item in svc.store_load_product_installations(openclaw_dir)
    ]
    counts = Counter(user["role"] for user in users)
    status_counts = Counter(user.get("status", "active") for user in users)
    actions_24h = 0
    failed_logins_24h = 0
    recent_events = []
    for event in audit_events:
        at = svc.parse_iso(event.get("at"))
        if at and at >= now - timedelta(hours=24):
            actions_24h += 1
            if event.get("action") == "login" and event.get("outcome") != "success":
                failed_logins_24h += 1
        actor = event.get("actor", {})
        recent_events.append(
            {
                "id": event.get("id"),
                "action": event.get("action", "event"),
                "outcome": event.get("outcome", "success"),
                "headline": event.get("detail") or event.get("action", "event"),
                "detail": event.get("meta", {}),
                "actor": actor.get("displayName") or actor.get("username") or "system",
                "role": actor.get("role", ""),
                "at": event.get("at", ""),
                "atAgo": svc.format_age(at, now) if at else "未知时间",
            }
        )
    role_matrix = [
        {
            "role": role,
            "label": meta["label"],
            "description": meta["description"],
            "permissions": permissions_for_role(role),
        }
        for role, meta in svc.USER_ROLES.items()
    ]
    metadata = svc.load_project_metadata(openclaw_dir, config=config)
    tenant_admin = build_tenant_admin_data(openclaw_dir, now)
    return {
        "workspace": {
            "displayName": metadata.get("displayName") or metadata.get("theme", "OpenClaw Team"),
            "projectDir": metadata.get("projectDir", ""),
            "openclawDir": str(openclaw_dir),
            "storagePath": str(svc.dashboard_store_path(openclaw_dir)),
        },
        "instanceSummary": {
            "total": len(installations),
            "reachable": sum(1 for item in installations if item.get("status") in {"ready", "current"}),
            "broken": sum(1 for item in installations if item.get("status") == "broken"),
            "missing": sum(1 for item in installations if item.get("status") == "missing"),
            "activeTasks": sum(int(item.get("activeTasks") or 0) for item in installations),
        },
        "seatSummary": {
            "total": len(users),
            "owner": counts["owner"],
            "operator": counts["operator"],
            "viewer": counts["viewer"],
            "active": status_counts["active"],
            "suspended": status_counts["suspended"],
            "actions24h": actions_24h,
            "failedLogins24h": failed_logins_24h,
        },
        "instances": installations if include_sensitive else [item for item in installations if item.get("current")],
        "users": users if include_sensitive else [],
        "auditEvents": recent_events[:32] if include_sensitive else [],
        "roleMatrix": role_matrix,
        "tenants": tenant_admin["items"] if include_sensitive else [],
        "tenantInstallations": tenant_admin["installations"] if include_sensitive else [],
        "tenantApiKeys": tenant_admin["apiKeys"] if include_sensitive else [],
        "tenantSummary": tenant_admin["summary"],
        "hasUsers": bool(users),
    }


def build_admin_bootstrap_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()

    def build():
        sync_current_installation_registry(openclaw_dir, config)
        metadata = svc.load_project_metadata(openclaw_dir, config=config)
        users = [safe_user_record(user) for user in load_product_users(openclaw_dir)]
        counts = Counter(user["role"] for user in users)
        status_counts = Counter(user.get("status", "active") for user in users)
        installations = svc.store_load_product_installations(openclaw_dir)
        tenants = svc.store_list_tenants(openclaw_dir)
        tenant_installations = svc.store_list_tenant_installations(openclaw_dir)
        api_keys = svc.store_list_tenant_api_keys(openclaw_dir)
        return {
            "workspace": {
                "displayName": metadata.get("displayName") or metadata.get("theme", "OpenClaw Team"),
                "projectDir": metadata.get("projectDir", ""),
                "openclawDir": str(openclaw_dir),
                "storagePath": str(svc.dashboard_store_path(openclaw_dir)),
            },
            "instanceSummary": {
                "total": len(installations),
            },
            "seatSummary": {
                "total": len(users),
                "owner": counts["owner"],
                "operator": counts["operator"],
                "viewer": counts["viewer"],
                "active": status_counts["active"],
                "suspended": status_counts["suspended"],
            },
            "tenantSummary": {
                "total": len(tenants),
                "installations": len(tenant_installations),
                "apiKeys": len(api_keys),
            },
            "hasUsers": bool(users),
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        }

    return svc.cached_payload(
        ("admin-bootstrap-v1", str(openclaw_dir)),
        10.0,
        build,
    )


def build_admin_users_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()

    def build():
        users = [safe_user_record(user) for user in load_product_users(openclaw_dir)]
        counts = Counter(user["role"] for user in users)
        status_counts = Counter(user.get("status", "active") for user in users)
        role_matrix = [
            {
                "role": role,
                "label": meta["label"],
                "description": meta["description"],
                "permissions": permissions_for_role(role),
            }
            for role, meta in svc.USER_ROLES.items()
        ]
        return {
            "users": users,
            "seatSummary": {
                "total": len(users),
                "owner": counts["owner"],
                "operator": counts["operator"],
                "viewer": counts["viewer"],
                "active": status_counts["active"],
                "suspended": status_counts["suspended"],
            },
            "roleMatrix": role_matrix,
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        }

    return svc.cached_payload(
        ("admin-users-v1", str(openclaw_dir)),
        10.0,
        build,
    )


def build_admin_installations_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()

    def build():
        sync_current_installation_registry(openclaw_dir, config)
        instances = [
            summarize_installation_record(openclaw_dir, item, now)
            for item in svc.store_load_product_installations(openclaw_dir)
        ]
        return {
            "instances": instances,
            "instanceSummary": {
                "total": len(instances),
                "reachable": sum(1 for item in instances if item.get("status") in {"ready", "current"}),
                "broken": sum(1 for item in instances if item.get("status") == "broken"),
                "missing": sum(1 for item in instances if item.get("status") == "missing"),
                "activeTasks": sum(int(item.get("activeTasks") or 0) for item in instances),
            },
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        }

    return svc.cached_payload(
        ("admin-instances-v1", str(openclaw_dir)),
        10.0,
        build,
    )


def build_admin_tenants_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()

    def build():
        sync_current_installation_registry(openclaw_dir, config)
        installation_registry = {
            item.get("openclawDir"): summarize_installation_record(openclaw_dir, item, now)
            for item in svc.store_load_product_installations(openclaw_dir)
        }
        tenants = svc.store_list_tenants(openclaw_dir)
        tenant_installations = svc.store_list_tenant_installations(openclaw_dir)
        api_keys = svc.store_list_tenant_api_keys(openclaw_dir)
        api_keys_by_tenant = defaultdict(list)
        for item in api_keys:
            api_keys_by_tenant[item.get("tenantId", "")].append(item)
        installation_groups = defaultdict(list)
        for item in tenant_installations:
            installation_groups[item.get("tenantId", "")].append(item)
        items = []
        for tenant in tenants:
            installations = installation_groups.get(tenant.get("id", ""), [])
            resolved_installations = [
                installation_registry.get(item.get("openclawDir", ""), {"activeTasks": 0, "agentCount": 0, "updatedAt": ""})
                for item in installations
            ]
            latest_updated_at = max(
                [str(item.get("updatedAt") or "").strip() for item in resolved_installations if str(item.get("updatedAt") or "").strip()] or [""]
            )
            primary_dir = tenant_primary_openclaw_dir(openclaw_dir, tenant)
            items.append(
                {
                    **tenant,
                    "statusLabel": "Active" if tenant.get("status") == "active" else "Suspended",
                    "primaryOpenclawDir": str(primary_dir) if primary_dir else tenant.get("primaryOpenclawDir", ""),
                    "installationCount": len(installations),
                    "activeTasks": sum(int(item.get("activeTasks") or 0) for item in resolved_installations),
                    "blockedTasks": sum(int(item.get("blockedTasks") or 0) for item in resolved_installations),
                    "agentCount": sum(int(item.get("agentCount") or 0) for item in resolved_installations),
                    "apiKeyCount": len(api_keys_by_tenant.get(tenant.get("id", ""), [])),
                    "lastUpdatedAt": latest_updated_at,
                    "lastUpdatedAgo": svc.format_age(svc.parse_iso(latest_updated_at), now) if latest_updated_at else "未同步",
                }
            )
        return {
            "tenants": items,
            "tenantSummary": {
                "total": len(items),
                "active": sum(1 for item in items if item.get("status") == "active"),
                "installations": len(tenant_installations),
                "apiKeys": len(api_keys),
            },
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        }

    return svc.cached_payload(
        ("admin-tenants-v1", str(openclaw_dir)),
        10.0,
        build,
    )


def build_admin_api_keys_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()

    def build():
        tenants = {
            str(item.get("id") or "").strip(): item
            for item in svc.store_list_tenants(openclaw_dir)
            if str(item.get("id") or "").strip()
        }
        api_keys = []
        for item in svc.store_list_tenant_api_keys(openclaw_dir):
            tenant = tenants.get(str(item.get("tenantId") or "").strip(), {})
            api_keys.append(
                {
                    **item,
                    "tenantName": str(tenant.get("name") or item.get("tenantId") or "").strip(),
                }
            )
        return {
            "tenantApiKeys": api_keys,
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        }

    return svc.cached_payload(
        ("admin-api-keys-v1", str(openclaw_dir)),
        10.0,
        build,
    )


def build_admin_audit_logs_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()

    def build():
        audit_events = load_audit_events(openclaw_dir, limit=80)
        rows = []
        actions_24h = 0
        failed_logins_24h = 0
        for event in audit_events:
            event = event if isinstance(event, dict) else {}
            at = svc.parse_iso(event.get("at"))
            if at and at >= now - timedelta(hours=24):
                actions_24h += 1
                if str(event.get("action") or "").strip() == "login" and str(event.get("outcome") or "").strip() != "success":
                    failed_logins_24h += 1
            actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
            meta = event.get("meta") if isinstance(event.get("meta"), dict) else {}
            target = (
                str(meta.get("target") or "").strip()
                or str(meta.get("resource") or "").strip()
                or str(meta.get("tenantId") or "").strip()
                or str(meta.get("installationId") or "").strip()
            )
            rows.append(
                {
                    "id": str(event.get("id") or "").strip(),
                    "timestamp": str(event.get("at") or "").strip(),
                    "actor": str(actor.get("displayName") or actor.get("username") or "system").strip(),
                    "action": str(event.get("action") or "event").strip(),
                    "target": target,
                    "detail": str(event.get("detail") or "").strip(),
                    "outcome": str(event.get("outcome") or "success").strip(),
                    "atAgo": svc.format_age(at, now) if at else "未知时间",
                }
            )
        return {
            "auditLogs": rows,
            "auditSummary": {
                "total": len(rows),
                "actions24h": actions_24h,
                "failedLogins24h": failed_logins_24h,
            },
            "generatedAt": now.isoformat().replace("+00:00", "Z"),
        }

    return svc.cached_payload(
        ("admin-audit-logs-v1", str(openclaw_dir)),
        10.0,
        build,
    )


def admin_data_cache_key(openclaw_dir, include_sensitive=True):
    return ("admin-data", str(Path(openclaw_dir).expanduser().resolve()), bool(include_sensitive))


def build_admin_data_placeholder(openclaw_dir, config, now, include_sensitive=True):
    svc = _svc()
    metadata = svc.load_project_metadata(openclaw_dir, config=config)
    try:
        users = [safe_user_record(user) for user in load_product_users(openclaw_dir)]
    except RuntimeError as exc:
        if "requires PostgreSQL" not in str(exc):
            raise
        users = []
    counts = Counter(user["role"] for user in users)
    status_counts = Counter(user.get("status", "active") for user in users)
    role_matrix = [
        {
            "role": role,
            "label": meta["label"],
            "description": meta["description"],
            "permissions": permissions_for_role(role),
        }
        for role, meta in svc.USER_ROLES.items()
    ]
    return {
        "workspace": {
            "displayName": metadata.get("displayName") or metadata.get("theme", "OpenClaw Team"),
            "projectDir": metadata.get("projectDir", ""),
            "openclawDir": str(Path(openclaw_dir).expanduser().resolve()),
            "storagePath": str(svc.dashboard_store_path(openclaw_dir)),
        },
        "instanceSummary": {
            "total": 0,
            "reachable": 0,
            "broken": 0,
            "missing": 0,
            "activeTasks": 0,
        },
        "seatSummary": {
            "total": len(users),
            "owner": counts["owner"],
            "operator": counts["operator"],
            "viewer": counts["viewer"],
            "active": status_counts["active"],
            "suspended": status_counts["suspended"],
            "actions24h": 0,
            "failedLogins24h": 0,
        },
        "instances": [],
        "users": users if include_sensitive else [],
        "auditEvents": [],
        "roleMatrix": role_matrix,
        "tenants": [],
        "tenantInstallations": [],
        "tenantApiKeys": [],
        "tenantSummary": {
            "total": 0,
            "healthy": 0,
            "degraded": 0,
            "totalApiKeys": 0,
        },
        "hasUsers": bool(users),
    }


def build_admin_data_cached(openclaw_dir, config, now, include_sensitive=True, ttl_seconds=15.0):
    svc = _svc()
    cache_key = admin_data_cache_key(openclaw_dir, include_sensitive=include_sensitive)
    builder = lambda: build_admin_data(openclaw_dir, config, now, include_sensitive=include_sensitive)
    fallback = lambda: build_admin_data_placeholder(openclaw_dir, config, now, include_sensitive=include_sensitive)
    current = time.time()
    with svc.BUNDLE_CACHE_LOCK:
        cached = svc.PAYLOAD_CACHE.get(cache_key)
        if cached and current - cached["ts"] < ttl_seconds:
            return deepcopy(cached["value"])
        if cache_key not in svc.BACKGROUND_CACHE_KEYS:
            svc.BACKGROUND_CACHE_KEYS.add(cache_key)
            worker = threading.Thread(
                target=svc._refresh_cached_payload_async,
                args=(cache_key, builder),
                daemon=True,
            )
            worker.start()
    if cached:
        return deepcopy(cached["value"])
    return fallback()


def resolve_dashboard_auth_token(openclaw_dir):
    svc = _svc()
    for key in ("DASHBOARD_AUTH_TOKEN", "GATEWAY_AUTH_TOKEN"):
        value = os.environ.get(key) or svc.read_env_value(openclaw_dir, key)
        if value:
            return value
    return ""


def sign_session_payload(auth_token, payload_text):
    return hmac.new(auth_token.encode("utf-8"), payload_text.encode("utf-8"), hashlib.sha256).hexdigest()


def encode_session_cookie(auth_token, session_data):
    payload = encode_base64url(json.dumps(session_data, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    return f"{payload}.{sign_session_payload(auth_token, payload)}"


def decode_session_cookie(auth_token, cookie_value):
    svc = _svc()
    try:
        payload, signature = str(cookie_value or "").split(".", 1)
        if not hmac.compare_digest(signature, sign_session_payload(auth_token, payload)):
            return None
        data = json.loads(decode_base64url(payload).decode("utf-8"))
        expires_at = svc.parse_iso(data.get("expiresAt"))
        if expires_at and expires_at < svc.now_utc():
            return None
        return data
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def expected_action_value(auth_token):
    if not auth_token:
        return ""
    return hmac.new(auth_token.encode("utf-8"), b"mission-control-dashboard-actions", hashlib.sha256).hexdigest()
