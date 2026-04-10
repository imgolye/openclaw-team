#!/usr/bin/env python3
"""PostgreSQL-backed storage for OpenClaw Team product data."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

try:
    import psycopg
    from psycopg.rows import dict_row as psycopg_dict_row
except Exception:  # pragma: no cover - optional dependency
    psycopg = None
    psycopg_dict_row = None

from backend.adapters.storage.management import (
    list_model_provider_configs as _list_model_provider_configs,
    save_model_provider_config as _save_model_provider_config,
)

SCHEMA_VERSION = 16
MANAGEMENT_STAGE_ORDER = ("intake", "plan", "execute", "verify", "release")
MANAGEMENT_STAGE_LABELS = {
    "intake": "需求接入",
    "plan": "方案编排",
    "execute": "执行推进",
    "verify": "验证验收",
    "release": "发布收口",
}
AUTOMATION_RULE_TYPES = {
    "blocked_task_timeout",
    "task_incomplete_timeout",
    "critical_task_done",
    "agent_offline",
    "daily_review_push",
    "weekly_report_push",
}
NOTIFICATION_CHANNEL_TYPES = {"telegram", "feishu", "webhook"}
CUSTOMER_ACCESS_CHANNEL_TYPES = {
    "wechat_search",
    "wechat_official",
    "openclaw_weixin",
    "wechat_article",
    "website",
    "landing_page",
    "email",
    "phone",
    "qr",
}
ROUTING_STRATEGY_TYPES = {"keyword_department", "load_balance", "priority_queue"}
DATABASE_URL_KEYS = ("MISSION_CONTROL_DATABASE_URL", "DATABASE_URL")
CHAT_THREAD_STATUSES = {"open", "waiting_external", "waiting_internal", "blocked", "resolved", "archived"}
CHAT_MESSAGE_DIRECTIONS = {"inbound", "outbound", "agent", "draft", "system"}
CHAT_MESSAGE_MAX_BODY_LENGTH = 20_000
MEMORY_SCOPES = {"company", "team", "agent", "task"}
AGENT_TEAM_STATUSES = {"active", "draft", "paused", "archived"}
SKILL_ROLE_MODES = {"founder", "eng-review", "design-review", "execute", "qa", "release", "docs", "retro", "browser"}
SKILL_ROLE_STAGES = {"plan", "review", "implement", "verify", "ship", "document", "reflect"}
SKILL_RECOMMENDED_ENTRIES = {"skills", "studio", "chat", "run"}
WORKFLOW_PACK_STATUSES = {"draft", "active", "disabled"}
INITIALIZED_SCHEMA_DATABASE_URLS = set()
SCHEMA_INIT_LOCK = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def hash_api_key(raw_key):
    return hashlib.sha256(str(raw_key or "").encode("utf-8")).hexdigest()


def dashboard_dir(openclaw_dir):
    path = Path(openclaw_dir) / "dashboard"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_env_value(openclaw_dir, key):
    env_path = Path(openclaw_dir) / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key == key:
            return value.strip()
    return ""


def database_url(openclaw_dir):
    for key in DATABASE_URL_KEYS:
        value = str(os.environ.get(key) or _read_env_value(openclaw_dir, key) or "").strip()
        if value:
            return value
    return ""


def _redact_database_url(value):
    text = str(value or "").strip()
    if not text:
        return text
    if "://" not in text and "=" in text:
        fields = {}
        for token in text.split():
            if "=" not in token:
                continue
            key, raw = token.split("=", 1)
            fields[key.strip().lower()] = raw.strip()
        dbname = fields.get("dbname") or fields.get("database") or ""
        if dbname:
            username = fields.get("user", "")
            password = "***" if fields.get("password") else ""
            auth = username
            if password:
                auth = f"{auth}:{password}" if auth else f":{password}"
            host = fields.get("host", "localhost")
            netloc = f"{auth}@{host}" if auth else host
            if fields.get("port"):
                netloc = f"{netloc}:{fields['port']}"
            return urlunsplit(("postgresql", netloc, f"/{dbname}", "", ""))
        return text
    if "://" not in text:
        return text
    parts = urlsplit(text)
    if not parts.scheme:
        return text
    username = parts.username or ""
    password = "***" if parts.password else ""
    auth = username
    if password:
        auth = f"{auth}:{password}" if auth else f":{password}"
    host = parts.hostname or ""
    netloc = f"{auth}@{host}" if auth else host
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def storage_backend(openclaw_dir):
    url = database_url(openclaw_dir)
    if not url:
        env_hint = Path(openclaw_dir).expanduser().resolve() / ".env"
        raise RuntimeError(
            "OpenClaw Team now requires PostgreSQL. Set MISSION_CONTROL_DATABASE_URL "
            f"in the environment or in {env_hint}."
        )
    return {
        "kind": "postgres",
        "database_url": url,
        "display": _redact_database_url(url),
    }


def store_path(openclaw_dir):
    return storage_backend(openclaw_dir)["display"]


def check_storage_readiness(openclaw_dir):
    try:
        backend = storage_backend(openclaw_dir)
    except Exception as exc:
        return {
            "ok": False,
            "kind": "postgres",
            "display": "",
            "error": str(exc),
        }
    if psycopg is None:
        return {
            "ok": False,
            "kind": backend["kind"],
            "display": backend["display"],
            "error": "psycopg is not available",
        }
    raw = None
    try:
        raw = psycopg.connect(backend["database_url"], row_factory=psycopg_dict_row, connect_timeout=3)
        with raw.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
        return {
            "ok": True,
            "kind": backend["kind"],
            "display": backend["display"],
        }
    except Exception as exc:
        return {
            "ok": False,
            "kind": backend["kind"],
            "display": backend["display"],
            "error": str(exc),
        }
    finally:
        if raw is not None:
            try:
                raw.close()
            except Exception:
                pass


def legacy_users_path(openclaw_dir):
    return dashboard_dir(openclaw_dir) / "product_users.json"


def legacy_audit_path(openclaw_dir):
    return dashboard_dir(openclaw_dir) / "audit-log.jsonl"


def _load_json(path, default):
    file_path = Path(path)
    if not file_path.exists():
        return default
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _adapt_sql(sql, backend_kind):
    if backend_kind != "postgres":
        return sql
    adapted = str(sql or "").replace("?", "%s")
    if "INSERT OR IGNORE INTO" in adapted.upper():
        upper = adapted.upper()
        marker = upper.find("INSERT OR IGNORE INTO")
        if marker >= 0:
            adapted = adapted[:marker] + adapted[marker:].replace("INSERT OR IGNORE INTO", "INSERT INTO", 1)
        stripped = adapted.rstrip()
        suffix = ""
        if stripped.endswith(";"):
            stripped = stripped[:-1].rstrip()
            suffix = ";"
        adapted = f"{stripped}\nON CONFLICT DO NOTHING{suffix}"
    return adapted


class _StoreConnection:
    def __init__(self, raw, backend_kind):
        self.raw = raw
        self.backend_kind = backend_kind

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.rollback()
            else:
                self.commit()
        finally:
            self.close()
        return False

    def execute(self, sql, params=()):
        adapted = _adapt_sql(sql, self.backend_kind)
        return self.raw.execute(adapted, tuple(params or ()))

    def executemany(self, sql, seq_of_params):
        adapted = _adapt_sql(sql, self.backend_kind)
        payload = [tuple(item or ()) for item in seq_of_params]
        with self.raw.cursor() as cur:
            cur.executemany(adapted, payload)
        return None

    def executescript(self, script):
        adapted = _adapt_sql(script, self.backend_kind)
        return self.raw.execute(adapted)

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()

    def close(self):
        self.raw.close()


def _connect(openclaw_dir):
    backend = storage_backend(openclaw_dir)
    if psycopg is None:
        raise RuntimeError("OpenClaw Team requires psycopg for PostgreSQL storage")
    raw = psycopg.connect(backend["database_url"], row_factory=psycopg_dict_row)
    conn = _StoreConnection(raw, "postgres")
    database_key = backend["database_url"]
    try:
        with SCHEMA_INIT_LOCK:
            if database_key not in INITIALIZED_SCHEMA_DATABASE_URLS:
                _ensure_schema(conn)
                _ensure_legacy_migration(openclaw_dir, conn)
                INITIALIZED_SCHEMA_DATABASE_URLS.add(database_key)
            else:
                _ensure_legacy_migration(openclaw_dir, conn)
    except Exception:
        raw.close()
        raise
    return conn


def _ensure_schema(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS product_users (
            username TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_login_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS audit_events (
            id TEXT PRIMARY KEY,
            at TEXT NOT NULL,
            action TEXT NOT NULL,
            outcome TEXT NOT NULL,
            detail TEXT NOT NULL,
            actor_json TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS product_installations (
            openclaw_dir TEXT PRIMARY KEY,
            installation_id TEXT NOT NULL,
            label TEXT NOT NULL,
            project_dir TEXT NOT NULL,
            theme TEXT NOT NULL,
            router_agent_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS management_runs (
            run_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            goal TEXT NOT NULL,
            owner TEXT NOT NULL,
            status TEXT NOT NULL,
            stage_key TEXT NOT NULL,
            linked_task_id TEXT NOT NULL,
            linked_agent_id TEXT NOT NULL,
            linked_session_key TEXT NOT NULL,
            release_channel TEXT NOT NULL,
            risk_level TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            stages_json TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS automation_rules (
            rule_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            threshold_minutes INTEGER NOT NULL,
            cooldown_minutes INTEGER NOT NULL,
            severity TEXT NOT NULL,
            match_text TEXT NOT NULL,
            channel_ids_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS notification_channels (
            channel_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            channel_type TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            secret TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS customer_access_channels (
            channel_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            channel_type TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            entry_url TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS model_provider_configs (
            openclaw_dir TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            provider_label TEXT NOT NULL,
            key_value TEXT NOT NULL,
            status TEXT NOT NULL,
            env_keys_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL,
            PRIMARY KEY (openclaw_dir, provider_id)
        );

        CREATE TABLE IF NOT EXISTS automation_alerts (
            alert_id TEXT PRIMARY KEY,
            rule_id TEXT NOT NULL,
            event_key TEXT NOT NULL,
            title TEXT NOT NULL,
            detail TEXT NOT NULL,
            severity TEXT NOT NULL,
            status TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            triggered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS notification_deliveries (
            delivery_id TEXT PRIMARY KEY,
            alert_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            outcome TEXT NOT NULL,
            detail TEXT NOT NULL,
            delivered_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orchestration_workflows (
            workflow_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            lanes_json TEXT NOT NULL,
            nodes_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orchestration_workflow_versions (
            version_id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL,
            version_number INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            lanes_json TEXT NOT NULL,
            nodes_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS routing_policies (
            policy_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            keyword TEXT NOT NULL,
            target_agent_id TEXT NOT NULL,
            priority_level TEXT NOT NULL,
            queue_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS routing_decisions (
            decision_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            task_title TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            policy_name TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            workflow_version_id TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            matched_keyword TEXT NOT NULL,
            queue_name TEXT NOT NULL,
            priority_level TEXT NOT NULL,
            target_agent_id TEXT NOT NULL,
            source_text TEXT NOT NULL,
            decided_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tenants (
            tenant_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            primary_openclaw_dir TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tenant_installations (
            tenant_id TEXT NOT NULL,
            openclaw_dir TEXT NOT NULL,
            label TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL,
            PRIMARY KEY (tenant_id, openclaw_dir)
        );

        CREATE TABLE IF NOT EXISTS tenant_api_keys (
            key_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            prefix TEXT NOT NULL,
            status TEXT NOT NULL,
            scopes_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_threads (
            thread_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            channel TEXT NOT NULL,
            owner TEXT NOT NULL,
            primary_agent_id TEXT NOT NULL,
            current_target_agent_id TEXT NOT NULL,
            linked_task_id TEXT NOT NULL,
            linked_deliverable_id TEXT NOT NULL,
            linked_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_messages (
            message_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            sender_kind TEXT NOT NULL,
            sender_id TEXT NOT NULL,
            sender_label TEXT NOT NULL,
            direction TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS agent_teams (
            team_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            lead_agent_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS skill_role_profiles (
            skill_slug TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            stage TEXT NOT NULL,
            recommended_entry TEXT NOT NULL,
            output_contract_json TEXT NOT NULL,
            requires_runtime_json TEXT NOT NULL,
            handoff_artifacts_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workflow_packs (
            pack_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL,
            starter INTEGER NOT NULL,
            default_entry TEXT NOT NULL,
            recommended_team_id TEXT NOT NULL,
            stages_json TEXT NOT NULL,
            skills_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_snapshots (
            snapshot_key TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            label TEXT NOT NULL,
            summary TEXT NOT NULL,
            learning_json TEXT NOT NULL,
            notes_json TEXT NOT NULL,
            related_task_id TEXT NOT NULL,
            related_thread_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_events (
            event_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            summary TEXT NOT NULL,
            related_task_id TEXT NOT NULL,
            related_thread_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_records (
            task_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            workspace_path TEXT NOT NULL,
            position_index INTEGER NOT NULL,
            title TEXT NOT NULL,
            state TEXT NOT NULL,
            owner TEXT NOT NULL,
            org TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            task_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_audit_events_at ON audit_events(at DESC);
        CREATE INDEX IF NOT EXISTS idx_product_users_created_at ON product_users(created_at);
        CREATE INDEX IF NOT EXISTS idx_product_installations_updated_at ON product_installations(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_management_runs_updated_at ON management_runs(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_automation_rules_updated_at ON automation_rules(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_notification_channels_updated_at ON notification_channels(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_customer_access_channels_updated_at ON customer_access_channels(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_model_provider_configs_updated_at ON model_provider_configs(openclaw_dir, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_automation_alerts_updated_at ON automation_alerts(updated_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_automation_alerts_unique_event ON automation_alerts(rule_id, event_key);
        CREATE INDEX IF NOT EXISTS idx_notification_deliveries_alert_id ON notification_deliveries(alert_id, delivered_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_delivery_unique_channel ON notification_deliveries(alert_id, channel_id);
        CREATE INDEX IF NOT EXISTS idx_orchestration_workflows_updated_at ON orchestration_workflows(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_orchestration_workflow_versions_workflow ON orchestration_workflow_versions(workflow_id, version_number DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_policies_updated_at ON routing_policies(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_decisions_decided_at ON routing_decisions(decided_at DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_decisions_policy_id ON routing_decisions(policy_id, decided_at DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_decisions_task_id ON routing_decisions(task_id, decided_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tenants_updated_at ON tenants(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tenant_installations_tenant ON tenant_installations(tenant_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_tenant ON tenant_api_keys(tenant_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_threads_updated_at ON chat_threads(updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_messages_thread ON chat_messages(thread_id, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_agent_teams_updated_at ON agent_teams(updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_skill_role_profiles_mode_stage ON skill_role_profiles(mode, stage, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_skill_role_profiles_updated_at ON skill_role_profiles(updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_workflow_packs_status_updated_at ON workflow_packs(status, updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_workflow_packs_mode ON workflow_packs(mode, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_snapshots_scope_updated_at ON memory_snapshots(scope, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_snapshots_related_task ON memory_snapshots(related_task_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_events_scope_created_at ON memory_events(scope, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_events_owner_created_at ON memory_events(owner_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_events_related_task_created_at ON memory_events(related_task_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_task_records_workspace ON task_records(workspace_id, position_index ASC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_task_records_updated_at ON task_records(updated_at DESC, created_at DESC);
        """
    )
    from backend.adapters.storage.schema import _computer_use_schema_sql

    conn.executescript(_computer_use_schema_sql())
    _set_metadata(conn, "schema_version", str(SCHEMA_VERSION))
    conn.commit()


def _metadata(conn, key, default=""):
    row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def _set_metadata(conn, key, value):
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


THEME_WORKFORCE_METADATA_KEY = "theme_workforce_design"


def _normalize_theme_workforce_profile(record):
    if not isinstance(record, dict):
        return None
    agent_id = str(record.get("agentId") or record.get("agent_id") or "").strip()
    if not agent_id:
        return None
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "agentId": agent_id,
        "humanName": str(record.get("humanName") or record.get("human_name") or "").strip(),
        "role": str(record.get("role") or "").strip(),
        "jobTitle": str(record.get("jobTitle") or record.get("job_title") or "").strip(),
        "workingStyle": str(record.get("workingStyle") or record.get("working_style") or "").strip(),
        "department": str(record.get("department") or "").strip(),
        "voiceReplyVoice": str(record.get("voiceReplyVoice") or record.get("voice_reply_voice") or "").strip(),
        "voiceReplySpeed": record.get("voiceReplySpeed") if record.get("voiceReplySpeed") is not None else record.get("voice_reply_speed"),
        "skills": _clean_string_list(record.get("skills") or []),
        "capabilityTags": _clean_string_list(record.get("capabilityTags") or record.get("capability_tags") or []),
        "notes": str(record.get("notes") or "").strip(),
        "createdAt": created_at,
        "updatedAt": updated_at,
        "meta": meta,
    }


def list_theme_workforce_profiles(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        raw = _metadata(conn, THEME_WORKFORCE_METADATA_KEY, "")
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    items = payload.get("profiles") if isinstance(payload, dict) else []
    profiles = []
    for item in items if isinstance(items, list) else []:
        normalized = _normalize_theme_workforce_profile(item)
        if normalized:
            profiles.append(normalized)
    return profiles


def save_theme_workforce_profile(openclaw_dir, payload):
    normalized = _normalize_theme_workforce_profile(payload)
    if not normalized:
        raise RuntimeError("theme workforce profile payload is invalid")
    with _connect(openclaw_dir) as conn:
        raw = _metadata(conn, THEME_WORKFORCE_METADATA_KEY, "")
        try:
            current = json.loads(raw) if raw else {}
        except Exception:
            current = {}
        existing_profiles = current.get("profiles") if isinstance(current.get("profiles"), list) else []
        profiles = []
        created_at = normalized["createdAt"]
        for item in existing_profiles:
            existing = _normalize_theme_workforce_profile(item)
            if not existing:
                continue
            if existing["agentId"] == normalized["agentId"]:
                created_at = existing["createdAt"]
                continue
            profiles.append(existing)
        normalized["createdAt"] = created_at
        profiles.append(normalized)
        profiles.sort(key=lambda item: (str(item.get("updatedAt") or ""), str(item.get("agentId") or "")), reverse=True)
        next_payload = {
            "version": 1,
            "updatedAt": now_iso(),
            "profiles": profiles,
        }
        _set_metadata(
            conn,
            THEME_WORKFORCE_METADATA_KEY,
            json.dumps(next_payload, ensure_ascii=False, separators=(",", ":")),
        )
        conn.commit()
    return normalized


def _normalize_username(value):
    return str(value or "").strip().lower()


def _clean_string_list(values):
    cleaned = []
    if not isinstance(values, list):
        return cleaned
    for item in values:
        value = str(item or "").strip()
        if value and value not in cleaned:
            cleaned.append(value)
    return cleaned


def _normalize_memory_scope(value):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in MEMORY_SCOPES else "task"


def _memory_snapshot_key(scope, owner_id):
    return f"{_normalize_memory_scope(scope)}:{str(owner_id or '').strip()}"


def _normalize_memory_snapshot(record):
    if not isinstance(record, dict):
        return None
    scope = _normalize_memory_scope(record.get("scope"))
    owner_id = str(record.get("ownerId") or record.get("owner_id") or "").strip()
    if not owner_id:
        return None
    learning = _clean_string_list(record.get("learning") or record.get("learningHighlights") or [])
    notes = [
        item
        for item in (record.get("notes") or record.get("recentNotes") or [])
        if isinstance(item, dict)
    ][:8]
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    return {
        "snapshot_key": str(record.get("snapshotKey") or record.get("snapshot_key") or _memory_snapshot_key(scope, owner_id)).strip(),
        "scope": scope,
        "owner_id": owner_id,
        "label": str(record.get("label") or owner_id).strip(),
        "summary": str(record.get("summary") or "").strip(),
        "learning_json": json.dumps(learning, ensure_ascii=False, separators=(",", ":")),
        "notes_json": json.dumps(notes, ensure_ascii=False, separators=(",", ":")),
        "related_task_id": str(record.get("relatedTaskId") or record.get("related_task_id") or "").strip(),
        "related_thread_id": str(record.get("relatedThreadId") or record.get("related_thread_id") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _memory_snapshot_row_to_dict(row):
    try:
        learning = json.loads(row["learning_json"] or "[]")
    except Exception:
        learning = []
    try:
        notes = json.loads(row["notes_json"] or "[]")
    except Exception:
        notes = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "snapshotKey": row["snapshot_key"],
        "scope": row["scope"],
        "ownerId": row["owner_id"],
        "label": row["label"],
        "summary": row["summary"],
        "learning": learning,
        "notes": notes,
        "relatedTaskId": row["related_task_id"],
        "relatedThreadId": row["related_thread_id"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_memory_event(record):
    if not isinstance(record, dict):
        return None
    scope = _normalize_memory_scope(record.get("scope"))
    owner_id = str(record.get("ownerId") or record.get("owner_id") or "").strip()
    summary = str(record.get("summary") or "").strip()
    if not owner_id or not summary:
        return None
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    return {
        "event_id": str(record.get("eventId") or record.get("event_id") or secrets.token_hex(8)).strip(),
        "scope": scope,
        "owner_id": owner_id,
        "event_type": str(record.get("eventType") or record.get("event_type") or "update").strip() or "update",
        "summary": summary,
        "related_task_id": str(record.get("relatedTaskId") or record.get("related_task_id") or "").strip(),
        "related_thread_id": str(record.get("relatedThreadId") or record.get("related_thread_id") or "").strip(),
        "created_at": created_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _memory_event_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["event_id"],
        "scope": row["scope"],
        "ownerId": row["owner_id"],
        "eventType": row["event_type"],
        "summary": row["summary"],
        "relatedTaskId": row["related_task_id"],
        "relatedThreadId": row["related_thread_id"],
        "createdAt": row["created_at"],
        "meta": meta,
    }


def _task_workspace_id(workspace_path):
    name = Path(workspace_path).expanduser().resolve().name
    return name[len("workspace-"):] if name.startswith("workspace-") else name


def _normalize_task_record(record, workspace_id="", workspace_path="", position_index=0):
    if not isinstance(record, dict):
        return None
    task_id = str(record.get("id") or "").strip()
    if not task_id:
        return None
    resolved_workspace_path = str(workspace_path or "").strip()
    if not resolved_workspace_path:
        return None
    resolved_workspace_id = str(workspace_id or "").strip() or _task_workspace_id(resolved_workspace_path)
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    normalized = dict(record)
    normalized.setdefault("id", task_id)
    normalized.setdefault("createdAt", created_at)
    normalized["updatedAt"] = updated_at
    return {
        "task_id": task_id,
        "workspace_id": resolved_workspace_id,
        "workspace_path": resolved_workspace_path,
        "position_index": int(position_index or 0),
        "title": str(record.get("title") or task_id).strip(),
        "state": str(record.get("state") or record.get("status") or "").strip(),
        "owner": str(record.get("owner") or "").strip(),
        "org": str(record.get("org") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "task_json": json.dumps(normalized, ensure_ascii=False, separators=(",", ":")),
    }


def _task_record_row_to_dict(row):
    try:
        payload = json.loads(row["task_json"] or "{}")
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("id", row["task_id"])
    payload.setdefault("title", row["title"])
    payload.setdefault("state", row["state"])
    payload.setdefault("owner", row["owner"])
    payload.setdefault("org", row["org"])
    payload.setdefault("createdAt", row["created_at"])
    payload["updatedAt"] = payload.get("updatedAt") or row["updated_at"]
    payload.setdefault("workspaceId", row["workspace_id"])
    payload.setdefault("workspacePath", row["workspace_path"])
    payload.setdefault("_workspaceId", row["workspace_id"])
    payload.setdefault("_workspacePath", row["workspace_path"])
    return payload


def _normalize_user_record(user):
    if not isinstance(user, dict):
        return None
    username = _normalize_username(user.get("username"))
    if not username:
        return None
    return {
        "user_id": user.get("id") or user.get("user_id") or secrets.token_hex(8),
        "username": username,
        "display_name": (user.get("displayName") or user.get("display_name") or username).strip(),
        "role": user.get("role") if user.get("role") in {"owner", "operator", "viewer"} else "viewer",
        "password_hash": user.get("passwordHash") or user.get("password_hash") or "",
        "status": user.get("status") if user.get("status") in {"active", "suspended"} else "active",
        "created_at": user.get("createdAt") or user.get("created_at") or now_iso(),
        "last_login_at": user.get("lastLoginAt") or user.get("last_login_at") or "",
    }


def _normalize_audit_event(event):
    if not isinstance(event, dict):
        return None
    actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
    meta = event.get("meta") if isinstance(event.get("meta"), dict) else {}
    return {
        "id": event.get("id") or secrets.token_hex(8),
        "at": event.get("at") or now_iso(),
        "action": str(event.get("action") or "event"),
        "outcome": str(event.get("outcome") or "success"),
        "detail": str(event.get("detail") or ""),
        "actor_json": json.dumps(actor, ensure_ascii=False, separators=(",", ":")),
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _normalize_installation_record(record):
    if not isinstance(record, dict):
        return None
    openclaw_dir = str(record.get("openclawDir") or record.get("openclaw_dir") or "").strip()
    if not openclaw_dir:
        return None
    created_at = record.get("createdAt") or record.get("created_at") or now_iso()
    updated_at = record.get("updatedAt") or record.get("updated_at") or created_at
    return {
        "openclaw_dir": openclaw_dir,
        "installation_id": record.get("id") or record.get("installation_id") or secrets.token_hex(8),
        "label": str(record.get("label") or record.get("displayName") or Path(openclaw_dir).name or openclaw_dir).strip(),
        "project_dir": str(record.get("projectDir") or record.get("project_dir") or "").strip(),
        "theme": str(record.get("theme") or "").strip(),
        "router_agent_id": str(record.get("routerAgentId") or record.get("router_agent_id") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _default_management_stages():
    stages = []
    for key in MANAGEMENT_STAGE_ORDER:
        stages.append(
            {
                "key": key,
                "title": MANAGEMENT_STAGE_LABELS[key],
                "status": "pending",
                "note": "",
                "updatedAt": "",
            }
        )
    stages[0]["status"] = "active"
    stages[0]["updatedAt"] = now_iso()
    return stages


def _normalize_management_stage(stage, fallback_key):
    raw_key = str(stage.get("key") or fallback_key or "").strip().lower()
    key = raw_key if raw_key in MANAGEMENT_STAGE_LABELS else fallback_key
    status = str(stage.get("status") or "pending").strip().lower()
    if status not in {"pending", "active", "done", "blocked"}:
        status = "pending"
    return {
        "key": key,
        "title": str(stage.get("title") or MANAGEMENT_STAGE_LABELS.get(key, key)).strip(),
        "status": status,
        "note": str(stage.get("note") or "").strip(),
        "updatedAt": str(stage.get("updatedAt") or "").strip(),
    }


def _normalize_management_record(record):
    if not isinstance(record, dict):
        return None
    title = str(record.get("title") or "").strip()
    if not title:
        return None
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    run_id = str(record.get("id") or record.get("run_id") or secrets.token_hex(6)).strip()
    current_stage = str(record.get("stageKey") or record.get("stage_key") or "intake").strip().lower()
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "blocked", "complete"}:
        status = "active"
    risk_level = str(record.get("riskLevel") or record.get("risk_level") or "medium").strip().lower()
    if risk_level not in {"low", "medium", "high"}:
        risk_level = "medium"
    stages_input = record.get("stages") if isinstance(record.get("stages"), list) else []
    stages = []
    if stages_input:
        for index, stage in enumerate(stages_input):
            if not isinstance(stage, dict):
                continue
            fallback_key = _slugify(stage.get("key") or stage.get("title") or f"stage-{index + 1}") or f"stage-{index + 1}"
            stages.append(_normalize_management_stage(stage, fallback_key))
    else:
        stages = _default_management_stages()
    stage_keys = [stage["key"] for stage in stages]
    if current_stage not in stage_keys:
        current_stage = stage_keys[0] if stage_keys else "intake"
    active_found = False
    for stage in stages:
        if stage["status"] == "active":
            if active_found:
                stage["status"] = "pending"
            else:
                active_found = True
                current_stage = stage["key"]
    if not active_found and status != "complete":
        for stage in stages:
            if stage["key"] == current_stage:
                stage["status"] = "active" if status != "blocked" else "blocked"
                break
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    workflow_binding = record.get("workflowBinding") if isinstance(record.get("workflowBinding"), dict) else {}
    planning_binding = record.get("planningBinding") if isinstance(record.get("planningBinding"), dict) else {}
    pack_binding = record.get("packBinding") if isinstance(record.get("packBinding"), dict) else {}
    if workflow_binding:
        meta = {**meta, "workflowBinding": workflow_binding}
    if planning_binding:
        meta = {**meta, "planningBinding": planning_binding}
    if pack_binding:
        meta = {**meta, "packBinding": pack_binding}
    linked_team_id = str(record.get("linkedTeamId") or record.get("linked_team_id") or meta.get("linkedTeamId") or "").strip()
    if linked_team_id:
        meta = {**meta, "linkedTeamId": linked_team_id}
    return {
        "run_id": run_id,
        "title": title,
        "goal": str(record.get("goal") or "").strip(),
        "owner": str(record.get("owner") or "OpenClaw Team").strip(),
        "status": status,
        "stage_key": current_stage,
        "linked_task_id": str(record.get("linkedTaskId") or record.get("linked_task_id") or "").strip(),
        "linked_agent_id": str(record.get("linkedAgentId") or record.get("linked_agent_id") or "").strip(),
        "linked_session_key": str(record.get("linkedSessionKey") or record.get("linked_session_key") or "").strip(),
        "release_channel": str(record.get("releaseChannel") or record.get("release_channel") or "manual").strip(),
        "risk_level": risk_level,
        "created_at": created_at,
        "updated_at": updated_at,
        "started_at": str(record.get("startedAt") or record.get("started_at") or created_at).strip(),
        "completed_at": str(record.get("completedAt") or record.get("completed_at") or "").strip(),
        "stages_json": json.dumps(stages, ensure_ascii=False, separators=(",", ":")),
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _management_row_to_dict(row):
    try:
        stages = json.loads(row["stages_json"] or "[]")
    except Exception:
        stages = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["run_id"],
        "title": row["title"],
        "goal": row["goal"],
        "owner": row["owner"],
        "status": row["status"],
        "stageKey": row["stage_key"],
        "linkedTaskId": row["linked_task_id"],
        "linkedAgentId": row["linked_agent_id"],
        "linkedSessionKey": row["linked_session_key"],
        "releaseChannel": row["release_channel"],
        "riskLevel": row["risk_level"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "startedAt": row["started_at"],
        "completedAt": row["completed_at"],
        "stages": stages,
        "meta": meta,
        "workflowBinding": meta.get("workflowBinding") if isinstance(meta.get("workflowBinding"), dict) else {},
        "planningBinding": meta.get("planningBinding") if isinstance(meta.get("planningBinding"), dict) else {},
        "packBinding": meta.get("packBinding") if isinstance(meta.get("packBinding"), dict) else {},
        "linkedTeamId": str(meta.get("linkedTeamId") or "").strip(),
    }


def _normalize_automation_rule(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    if not name:
        return None
    trigger_type = str(record.get("triggerType") or record.get("trigger_type") or "").strip().lower()
    if trigger_type not in AUTOMATION_RULE_TYPES:
        raise RuntimeError(f"unsupported automation trigger type: {trigger_type or 'unknown'}")
    severity = str(record.get("severity") or "warning").strip().lower()
    if severity not in {"info", "warning", "critical"}:
        severity = "warning"
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    threshold_minutes = max(int(record.get("thresholdMinutes") or record.get("threshold_minutes") or 0), 0)
    cooldown_minutes = max(int(record.get("cooldownMinutes") or record.get("cooldown_minutes") or 60), 0)
    channel_ids = record.get("channelIds") or record.get("channel_ids") or []
    if not isinstance(channel_ids, list):
        channel_ids = []
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    return {
        "rule_id": str(record.get("id") or record.get("rule_id") or secrets.token_hex(6)).strip(),
        "name": name,
        "description": str(record.get("description") or "").strip(),
        "status": status,
        "trigger_type": trigger_type,
        "threshold_minutes": threshold_minutes,
        "cooldown_minutes": cooldown_minutes,
        "severity": severity,
        "match_text": str(record.get("matchText") or record.get("match_text") or "").strip(),
        "channel_ids_json": json.dumps([str(item).strip() for item in channel_ids if str(item).strip()], ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _automation_rule_row_to_dict(row):
    try:
        channel_ids = json.loads(row["channel_ids_json"] or "[]")
    except Exception:
        channel_ids = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["rule_id"],
        "name": row["name"],
        "description": row["description"],
        "status": row["status"],
        "triggerType": row["trigger_type"],
        "thresholdMinutes": row["threshold_minutes"],
        "cooldownMinutes": row["cooldown_minutes"],
        "severity": row["severity"],
        "matchText": row["match_text"],
        "channelIds": channel_ids if isinstance(channel_ids, list) else [],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_notification_channel(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    channel_type = str(record.get("type") or record.get("channelType") or record.get("channel_type") or "").strip().lower()
    if not name or channel_type not in NOTIFICATION_CHANNEL_TYPES:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "channel_id": str(record.get("id") or record.get("channel_id") or secrets.token_hex(6)).strip(),
        "name": name,
        "channel_type": channel_type,
        "status": status,
        "target": str(record.get("target") or "").strip(),
        "secret": str(record.get("secret") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _notification_channel_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["channel_id"],
        "name": row["name"],
        "type": row["channel_type"],
        "status": row["status"],
        "target": row["target"],
        "secret": row["secret"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_customer_access_channel(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    channel_type = str(record.get("type") or record.get("channelType") or record.get("channel_type") or "").strip().lower()
    if not name or channel_type not in CUSTOMER_ACCESS_CHANNEL_TYPES:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "channel_id": str(record.get("id") or record.get("channel_id") or secrets.token_hex(6)).strip(),
        "name": name,
        "channel_type": channel_type,
        "status": status,
        "target": str(record.get("target") or "").strip(),
        "entry_url": str(record.get("entryUrl") or record.get("entry_url") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _customer_access_channel_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["channel_id"],
        "name": row["name"],
        "type": row["channel_type"],
        "status": row["status"],
        "target": row["target"],
        "entryUrl": row["entry_url"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _customer_access_channel_dedupe_key(record):
    if not isinstance(record, dict):
        return None
    channel_type = str(record.get("type") or record.get("channel_type") or "").strip().lower()
    if not channel_type:
        return None
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    entry_url = str(record.get("entryUrl") or record.get("entry_url") or "").strip()
    target = str(record.get("target") or "").strip()
    name = str(record.get("name") or "").strip()
    if channel_type == "openclaw_weixin":
        identity = str(meta.get("accountId") or "").strip() or target or name
    elif channel_type == "wechat_official":
        identity = str(meta.get("appId") or "").strip() or str(meta.get("verificationToken") or "").strip() or target or name
    elif channel_type in {"wechat_article", "website", "landing_page"}:
        identity = entry_url or target or name
    else:
        identity = entry_url or target or name
    return (channel_type, identity) if identity else None


def _dedupe_customer_access_channels(items):
    deduped = []
    seen = set()
    for item in items:
        dedupe_key = _customer_access_channel_dedupe_key(item)
        if dedupe_key and dedupe_key in seen:
            continue
        if dedupe_key:
            seen.add(dedupe_key)
        deduped.append(item)
    return deduped


def _normalize_automation_alert(record):
    if not isinstance(record, dict):
        return None
    rule_id = str(record.get("ruleId") or record.get("rule_id") or "").strip()
    event_key = str(record.get("eventKey") or record.get("event_key") or "").strip()
    title = str(record.get("title") or "").strip()
    if not rule_id or not event_key or not title:
        return None
    severity = str(record.get("severity") or "warning").strip().lower()
    if severity not in {"info", "warning", "critical"}:
        severity = "warning"
    status = str(record.get("status") or "open").strip().lower()
    if status not in {"open", "notified", "resolved", "error"}:
        status = "open"
    triggered_at = str(record.get("triggeredAt") or record.get("triggered_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or triggered_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "alert_id": str(record.get("id") or record.get("alert_id") or secrets.token_hex(8)).strip(),
        "rule_id": rule_id,
        "event_key": event_key,
        "title": title,
        "detail": str(record.get("detail") or "").strip(),
        "severity": severity,
        "status": status,
        "source_type": str(record.get("sourceType") or record.get("source_type") or "").strip(),
        "source_id": str(record.get("sourceId") or record.get("source_id") or "").strip(),
        "triggered_at": triggered_at,
        "updated_at": updated_at,
        "resolved_at": str(record.get("resolvedAt") or record.get("resolved_at") or "").strip(),
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _automation_alert_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["alert_id"],
        "ruleId": row["rule_id"],
        "eventKey": row["event_key"],
        "title": row["title"],
        "detail": row["detail"],
        "severity": row["severity"],
        "status": row["status"],
        "sourceType": row["source_type"],
        "sourceId": row["source_id"],
        "triggeredAt": row["triggered_at"],
        "updatedAt": row["updated_at"],
        "resolvedAt": row["resolved_at"],
        "meta": meta,
    }


def _normalize_orchestration_workflow(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    if not name:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "draft", "disabled"}:
        status = "active"
    lanes = record.get("lanes") if isinstance(record.get("lanes"), list) else []
    nodes = record.get("nodes") if isinstance(record.get("nodes"), list) else []
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    return {
        "workflow_id": str(record.get("id") or record.get("workflow_id") or secrets.token_hex(6)).strip(),
        "name": name,
        "description": str(record.get("description") or "").strip(),
        "status": status,
        "lanes_json": json.dumps(lanes, ensure_ascii=False, separators=(",", ":")),
        "nodes_json": json.dumps(nodes, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _orchestration_workflow_row_to_dict(row):
    try:
        lanes = json.loads(row["lanes_json"] or "[]")
    except Exception:
        lanes = []
    try:
        nodes = json.loads(row["nodes_json"] or "[]")
    except Exception:
        nodes = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["workflow_id"],
        "name": row["name"],
        "description": row["description"],
        "status": row["status"],
        "lanes": lanes if isinstance(lanes, list) else [],
        "nodes": nodes if isinstance(nodes, list) else [],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_workflow_version(record):
    if not isinstance(record, dict):
        return None
    workflow_id = str(record.get("workflowId") or record.get("workflow_id") or "").strip()
    name = str(record.get("name") or "").strip()
    if not workflow_id or not name:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "draft", "disabled"}:
        status = "active"
    lanes = record.get("lanes") if isinstance(record.get("lanes"), list) else []
    nodes = record.get("nodes") if isinstance(record.get("nodes"), list) else []
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    version_number = int(record.get("versionNumber") or record.get("version_number") or 1)
    return {
        "version_id": str(record.get("id") or record.get("version_id") or secrets.token_hex(8)).strip(),
        "workflow_id": workflow_id,
        "version_number": max(version_number, 1),
        "name": name,
        "description": str(record.get("description") or "").strip(),
        "status": status,
        "lanes_json": json.dumps(lanes, ensure_ascii=False, separators=(",", ":")),
        "nodes_json": json.dumps(nodes, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _workflow_version_row_to_dict(row):
    try:
        lanes = json.loads(row["lanes_json"] or "[]")
    except Exception:
        lanes = []
    try:
        nodes = json.loads(row["nodes_json"] or "[]")
    except Exception:
        nodes = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["version_id"],
        "workflowId": row["workflow_id"],
        "versionNumber": row["version_number"],
        "name": row["name"],
        "description": row["description"],
        "status": row["status"],
        "lanes": lanes if isinstance(lanes, list) else [],
        "nodes": nodes if isinstance(nodes, list) else [],
        "createdAt": row["created_at"],
        "meta": meta,
    }


def _normalize_routing_policy(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    if not name:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    strategy_type = str(record.get("strategyType") or record.get("strategy_type") or "").strip().lower()
    if strategy_type not in ROUTING_STRATEGY_TYPES:
        raise RuntimeError(f"unsupported routing strategy type: {strategy_type or 'unknown'}")
    priority_level = str(record.get("priorityLevel") or record.get("priority_level") or "normal").strip().lower()
    if priority_level not in {"low", "normal", "high", "critical"}:
        priority_level = "normal"
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    return {
        "policy_id": str(record.get("id") or record.get("policy_id") or secrets.token_hex(6)).strip(),
        "name": name,
        "status": status,
        "strategy_type": strategy_type,
        "keyword": str(record.get("keyword") or "").strip(),
        "target_agent_id": str(record.get("targetAgentId") or record.get("target_agent_id") or "").strip(),
        "priority_level": priority_level,
        "queue_name": str(record.get("queueName") or record.get("queue_name") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _routing_policy_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["policy_id"],
        "name": row["name"],
        "status": row["status"],
        "strategyType": row["strategy_type"],
        "keyword": row["keyword"],
        "targetAgentId": row["target_agent_id"],
        "priorityLevel": row["priority_level"],
        "queueName": row["queue_name"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_routing_decision(record):
    if not isinstance(record, dict):
        return None
    task_id = str(record.get("taskId") or record.get("task_id") or "").strip()
    task_title = str(record.get("taskTitle") or record.get("task_title") or "").strip()
    target_agent_id = str(record.get("targetAgentId") or record.get("target_agent_id") or "").strip()
    if not task_id or not task_title or not target_agent_id:
        return None
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    decided_at = str(record.get("decidedAt") or record.get("decided_at") or now_iso()).strip()
    return {
        "decision_id": str(record.get("id") or record.get("decision_id") or secrets.token_hex(8)).strip(),
        "task_id": task_id,
        "task_title": task_title,
        "policy_id": str(record.get("policyId") or record.get("policy_id") or "").strip(),
        "policy_name": str(record.get("policyName") or record.get("policy_name") or "").strip(),
        "workflow_id": str(record.get("workflowId") or record.get("workflow_id") or "").strip(),
        "workflow_version_id": str(record.get("workflowVersionId") or record.get("workflow_version_id") or "").strip(),
        "strategy_type": str(record.get("strategyType") or record.get("strategy_type") or "").strip(),
        "matched_keyword": str(record.get("matchedKeyword") or record.get("matched_keyword") or "").strip(),
        "queue_name": str(record.get("queueName") or record.get("queue_name") or "").strip(),
        "priority_level": str(record.get("priorityLevel") or record.get("priority_level") or "normal").strip().lower(),
        "target_agent_id": target_agent_id,
        "source_text": str(record.get("sourceText") or record.get("source_text") or "").strip(),
        "decided_at": decided_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _routing_decision_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["decision_id"],
        "taskId": row["task_id"],
        "taskTitle": row["task_title"],
        "policyId": row["policy_id"],
        "policyName": row["policy_name"],
        "workflowId": row["workflow_id"],
        "workflowVersionId": row["workflow_version_id"],
        "strategyType": row["strategy_type"],
        "matchedKeyword": row["matched_keyword"],
        "queueName": row["queue_name"],
        "priorityLevel": row["priority_level"],
        "targetAgentId": row["target_agent_id"],
        "sourceText": row["source_text"],
        "decidedAt": row["decided_at"],
        "meta": meta,
    }


def _slugify(value):
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    while "--" in text:
        text = text.replace("--", "-")
    return text.strip("-")


def _normalize_tenant(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    if not name:
        return None
    slug = _slugify(record.get("slug") or name)
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "suspended"}:
        status = "active"
    primary_dir = str(record.get("primaryOpenclawDir") or record.get("primary_openclaw_dir") or "").strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    return {
        "tenant_id": str(record.get("id") or record.get("tenant_id") or secrets.token_hex(6)).strip(),
        "name": name,
        "slug": slug,
        "status": status,
        "primary_openclaw_dir": primary_dir,
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _tenant_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["tenant_id"],
        "name": row["name"],
        "slug": row["slug"],
        "status": row["status"],
        "primaryOpenclawDir": row["primary_openclaw_dir"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_tenant_installation(record):
    if not isinstance(record, dict):
        return None
    tenant_id = str(record.get("tenantId") or record.get("tenant_id") or "").strip()
    openclaw_dir = str(record.get("openclawDir") or record.get("openclaw_dir") or "").strip()
    if not tenant_id or not openclaw_dir:
        return None
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "tenant_id": tenant_id,
        "openclaw_dir": openclaw_dir,
        "label": str(record.get("label") or Path(openclaw_dir).name or openclaw_dir).strip(),
        "role": str(record.get("role") or "primary").strip().lower(),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _tenant_installation_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "tenantId": row["tenant_id"],
        "openclawDir": row["openclaw_dir"],
        "label": row["label"],
        "role": row["role"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_tenant_api_key(record):
    if not isinstance(record, dict):
        return None
    tenant_id = str(record.get("tenantId") or record.get("tenant_id") or "").strip()
    name = str(record.get("name") or "").strip()
    raw_key = str(record.get("rawKey") or record.get("raw_key") or "").strip()
    if not tenant_id or not name or not raw_key:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    scopes = record.get("scopes") if isinstance(record.get("scopes"), list) else []
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    return {
        "key_id": str(record.get("id") or record.get("key_id") or secrets.token_hex(8)).strip(),
        "tenant_id": tenant_id,
        "name": name,
        "key_hash": hash_api_key(raw_key),
        "prefix": raw_key[:10],
        "status": status,
        "scopes_json": json.dumps([str(item).strip() for item in scopes if str(item).strip()], ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "last_used_at": str(record.get("lastUsedAt") or record.get("last_used_at") or "").strip(),
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _tenant_api_key_row_to_dict(row):
    try:
        scopes = json.loads(row["scopes_json"] or "[]")
    except Exception:
        scopes = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["key_id"],
        "tenantId": row["tenant_id"],
        "name": row["name"],
        "prefix": row["prefix"],
        "status": row["status"],
        "scopes": scopes if isinstance(scopes, list) else [],
        "createdAt": row["created_at"],
        "lastUsedAt": row["last_used_at"],
        "meta": meta,
    }


_CHAT_THREAD_CHANNELS = {"internal", "feishu", "telegram", "email", "webhook", "customer_wechat"}
_CHAT_THREAD_ID_MAX = 128
_CHAT_THREAD_TITLE_MAX = 500
_CHAT_THREAD_OWNER_MAX = 255
_CHAT_LINKED_ID_MAX = 128
_CHAT_PARTICIPANT_AGENTS_MAX = 200
_CHAT_PARTICIPANT_HUMANS_MAX = 100
_CHAT_HUMAN_NAME_MAX = 255


def _normalize_chat_thread(record):
    if not isinstance(record, dict):
        return None
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    thread_id = str(record.get("id") or record.get("thread_id") or secrets.token_hex(8)).strip()
    if len(thread_id) > _CHAT_THREAD_ID_MAX:
        raise RuntimeError(f"thread_id 超过最大长度（{_CHAT_THREAD_ID_MAX} 字符）。")
    title = str(record.get("title") or "").strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    if not title:
        title = (
            str(meta.get("customerName") or "").strip()
            or str(record.get("linkedTaskId") or record.get("linked_task_id") or "").strip()
            or f"Chat {thread_id[:6]}"
        )
    title = title[:_CHAT_THREAD_TITLE_MAX]
    status = str(record.get("status") or "open").strip().lower()
    if status not in CHAT_THREAD_STATUSES:
        status = "open"
    channel = str(record.get("channel") or "internal").strip().lower()
    if channel not in _CHAT_THREAD_CHANNELS:
        channel = "internal"
    owner = str(record.get("owner") or "").strip()[:_CHAT_THREAD_OWNER_MAX]
    primary_agent_id = str(record.get("primaryAgentId") or record.get("primary_agent_id") or "").strip()
    current_target_agent_id = str(record.get("currentTargetAgentId") or record.get("current_target_agent_id") or primary_agent_id).strip()
    if not current_target_agent_id:
        current_target_agent_id = primary_agent_id
    linked_task_id = str(record.get("linkedTaskId") or record.get("linked_task_id") or "").strip()[:_CHAT_LINKED_ID_MAX]
    linked_deliverable_id = str(record.get("linkedDeliverableId") or record.get("linked_deliverable_id") or "").strip()[:_CHAT_LINKED_ID_MAX]
    linked_run_id = str(record.get("linkedRunId") or record.get("linked_run_id") or "").strip()[:_CHAT_LINKED_ID_MAX]
    linked_team_ids = record.get("linkedTeamIds") if isinstance(record.get("linkedTeamIds"), list) else meta.get("linkedTeamIds")
    collaborator_team_ids = record.get("collaboratorTeamIds") if isinstance(record.get("collaboratorTeamIds"), list) else []
    primary_team_id = str(record.get("teamId") or meta.get("teamId") or "").strip()
    if isinstance(linked_team_ids, list) or collaborator_team_ids or primary_team_id:
        cleaned_team_ids = []
        for item in [primary_team_id, *(linked_team_ids if isinstance(linked_team_ids, list) else []), *collaborator_team_ids]:
            value = str(item or "").strip()
            if value and value not in cleaned_team_ids:
                cleaned_team_ids.append(value)
        if cleaned_team_ids:
            meta = {**meta, "linkedTeamIds": cleaned_team_ids}
    participant_agents = record.get("participantAgentIds")
    if isinstance(participant_agents, list):
        cleaned = []
        for item in participant_agents:
            value = str(item or "").strip()
            if value and value not in cleaned:
                cleaned.append(value)
            if len(cleaned) >= _CHAT_PARTICIPANT_AGENTS_MAX:
                break
        if primary_agent_id and primary_agent_id not in cleaned:
            cleaned.insert(0, primary_agent_id)
        meta = {**meta, "participantAgentIds": cleaned}
    participant_humans = record.get("participantHumans")
    if isinstance(participant_humans, list):
        cleaned_humans = []
        for item in participant_humans:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("displayName") or item.get("username") or "").strip()
            if not name:
                continue
            cleaned_humans.append(
                {
                    "name": name[:_CHAT_HUMAN_NAME_MAX],
                    "username": str(item.get("username") or "").strip(),
                    "role": str(item.get("role") or "").strip(),
                }
            )
            if len(cleaned_humans) >= _CHAT_PARTICIPANT_HUMANS_MAX:
                break
        meta = {**meta, "participantHumans": cleaned_humans}
    sessions_by_agent = meta.get("sessionsByAgent") if isinstance(meta.get("sessionsByAgent"), dict) else {}
    pack_binding = record.get("packBinding") if isinstance(record.get("packBinding"), dict) else {}
    if pack_binding:
        meta = {**meta, "packBinding": pack_binding}
    mode = str(record.get("mode") or meta.get("mode") or "").strip().lower()
    if mode:
        meta = {**meta, "mode": mode}
    meta = {**meta, "sessionsByAgent": sessions_by_agent}
    return {
        "thread_id": thread_id,
        "title": title,
        "status": status,
        "channel": channel,
        "owner": owner,
        "primary_agent_id": primary_agent_id,
        "current_target_agent_id": current_target_agent_id,
        "linked_task_id": linked_task_id,
        "linked_deliverable_id": linked_deliverable_id,
        "linked_run_id": linked_run_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _chat_thread_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["thread_id"],
        "title": row["title"],
        "status": row["status"],
        "channel": row["channel"],
        "owner": row["owner"],
        "primaryAgentId": row["primary_agent_id"],
        "currentTargetAgentId": row["current_target_agent_id"],
        "linkedTaskId": row["linked_task_id"],
        "linkedDeliverableId": row["linked_deliverable_id"],
        "linkedRunId": row["linked_run_id"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
        "participantAgentIds": meta.get("participantAgentIds", []),
        "linkedTeamIds": meta.get("linkedTeamIds", []),
        "participantHumans": meta.get("participantHumans", []),
        "packBinding": meta.get("packBinding") if isinstance(meta.get("packBinding"), dict) else {},
        "mode": str(meta.get("mode") or "").strip(),
    }


def _normalize_agent_team(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    if not name:
        return None
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    team_id = str(record.get("id") or record.get("team_id") or secrets.token_hex(8)).strip()
    status = str(record.get("status") or "active").strip().lower()
    if status not in AGENT_TEAM_STATUSES:
        status = "active"
    lead_agent_id = str(record.get("leadAgentId") or record.get("lead_agent_id") or "").strip()
    member_agent_ids = record.get("memberAgentIds")
    cleaned_members = []
    if isinstance(member_agent_ids, list):
        for item in member_agent_ids:
            value = str(item or "").strip()
            if value and value not in cleaned_members:
                cleaned_members.append(value)
    if lead_agent_id and lead_agent_id not in cleaned_members:
        cleaned_members.insert(0, lead_agent_id)
    if not lead_agent_id and cleaned_members:
        lead_agent_id = cleaned_members[0]
    linked_task_ids = record.get("linkedTaskIds")
    cleaned_task_ids = []
    if isinstance(linked_task_ids, list):
        for item in linked_task_ids:
            value = str(item or "").strip()
            if value and value not in cleaned_task_ids:
                cleaned_task_ids.append(value)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    meta = {
        **meta,
        "description": str(record.get("description") or meta.get("description") or "").strip(),
        "focus": str(record.get("focus") or meta.get("focus") or "").strip(),
        "channel": str(record.get("channel") or meta.get("channel") or "internal").strip(),
        "defaultDispatchMode": str(record.get("defaultDispatchMode") or meta.get("defaultDispatchMode") or "").strip().lower(),
        "defaultWakeScope": str(record.get("defaultWakeScope") or meta.get("defaultWakeScope") or "").strip().lower(),
        "operatingBrief": str(record.get("operatingBrief") or meta.get("operatingBrief") or "").strip(),
        "teamMemory": str(record.get("teamMemory") or meta.get("teamMemory") or "").strip(),
        "decisionLog": str(record.get("decisionLog") or meta.get("decisionLog") or "").strip(),
        "memberAgentIds": cleaned_members,
        "linkedTaskIds": cleaned_task_ids,
    }
    return {
        "team_id": team_id,
        "name": name,
        "status": status,
        "lead_agent_id": lead_agent_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _agent_team_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["team_id"],
        "name": row["name"],
        "status": row["status"],
        "leadAgentId": row["lead_agent_id"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "description": str(meta.get("description") or "").strip(),
        "focus": str(meta.get("focus") or "").strip(),
        "channel": str(meta.get("channel") or "internal").strip(),
        "defaultDispatchMode": str(meta.get("defaultDispatchMode") or "").strip(),
        "defaultWakeScope": str(meta.get("defaultWakeScope") or "").strip(),
        "operatingBrief": str(meta.get("operatingBrief") or "").strip(),
        "teamMemory": str(meta.get("teamMemory") or "").strip(),
        "decisionLog": str(meta.get("decisionLog") or "").strip(),
        "memberAgentIds": meta.get("memberAgentIds", []),
        "linkedTaskIds": meta.get("linkedTaskIds", []),
        "meta": meta,
    }


def _normalize_skill_role_profile(record):
    if not isinstance(record, dict):
        return None
    skill_slug = str(record.get("skillSlug") or record.get("skill_slug") or record.get("slug") or "").strip()
    if not skill_slug:
        return None
    mode = str(record.get("mode") or "").strip().lower()
    if mode not in SKILL_ROLE_MODES:
        raise RuntimeError(f"unsupported skill role mode: {mode or 'unknown'}")
    stage = str(record.get("stage") or "").strip().lower()
    if stage not in SKILL_ROLE_STAGES:
        raise RuntimeError(f"unsupported skill role stage: {stage or 'unknown'}")
    recommended_entry = str(record.get("recommendedEntry") or record.get("recommended_entry") or "skills").strip().lower()
    if recommended_entry not in SKILL_RECOMMENDED_ENTRIES:
        raise RuntimeError(f"unsupported skill recommended entry: {recommended_entry or 'unknown'}")
    output_contract = _clean_string_list(record.get("outputContract") or record.get("output_contract") or [])
    requires_runtime = _clean_string_list(record.get("requiresRuntime") or record.get("requires_runtime") or [])
    handoff_artifacts = _clean_string_list(record.get("handoffArtifacts") or record.get("handoff_artifacts") or [])
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "skill_slug": skill_slug,
        "mode": mode,
        "stage": stage,
        "recommended_entry": recommended_entry,
        "output_contract_json": json.dumps(output_contract, ensure_ascii=False, separators=(",", ":")),
        "requires_runtime_json": json.dumps(requires_runtime, ensure_ascii=False, separators=(",", ":")),
        "handoff_artifacts_json": json.dumps(handoff_artifacts, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _skill_role_profile_row_to_dict(row):
    try:
        output_contract = json.loads(row["output_contract_json"] or "[]")
    except Exception:
        output_contract = []
    try:
        requires_runtime = json.loads(row["requires_runtime_json"] or "[]")
    except Exception:
        requires_runtime = []
    try:
        handoff_artifacts = json.loads(row["handoff_artifacts_json"] or "[]")
    except Exception:
        handoff_artifacts = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "skillSlug": row["skill_slug"],
        "mode": row["mode"],
        "stage": row["stage"],
        "recommendedEntry": row["recommended_entry"],
        "outputContract": output_contract if isinstance(output_contract, list) else [],
        "requiresRuntime": requires_runtime if isinstance(requires_runtime, list) else [],
        "handoffArtifacts": handoff_artifacts if isinstance(handoff_artifacts, list) else [],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_workflow_pack(record):
    if not isinstance(record, dict):
        return None
    name = str(record.get("name") or "").strip()
    if not name:
        return None
    pack_id = str(record.get("id") or record.get("packId") or record.get("pack_id") or _slugify(name)).strip()
    if not pack_id:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in WORKFLOW_PACK_STATUSES:
        raise RuntimeError(f"unsupported workflow pack status: {status or 'unknown'}")
    mode = str(record.get("mode") or "").strip().lower()
    if mode not in SKILL_ROLE_MODES:
        raise RuntimeError(f"unsupported workflow pack mode: {mode or 'unknown'}")
    default_entry = str(record.get("defaultEntry") or record.get("default_entry") or "skills").strip().lower()
    if default_entry not in SKILL_RECOMMENDED_ENTRIES:
        raise RuntimeError(f"unsupported workflow pack entry: {default_entry or 'unknown'}")
    stages = record.get("stages") if isinstance(record.get("stages"), list) else []
    skills_input = record.get("skills")
    skills = []
    if isinstance(skills_input, list):
        for item in skills_input:
            if isinstance(item, dict):
                value = str(item.get("slug") or item.get("skillSlug") or item.get("id") or "").strip()
            else:
                value = str(item or "").strip()
            if value and value not in skills:
                skills.append(value)
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "pack_id": pack_id,
        "name": name,
        "description": str(record.get("description") or "").strip(),
        "status": status,
        "mode": mode,
        "starter": 1 if bool(record.get("starter")) else 0,
        "default_entry": default_entry,
        "recommended_team_id": str(record.get("recommendedTeamId") or record.get("recommended_team_id") or "").strip(),
        "stages_json": json.dumps(stages, ensure_ascii=False, separators=(",", ":")),
        "skills_json": json.dumps(skills, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _workflow_pack_row_to_dict(row):
    try:
        stages = json.loads(row["stages_json"] or "[]")
    except Exception:
        stages = []
    try:
        skills = json.loads(row["skills_json"] or "[]")
    except Exception:
        skills = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["pack_id"],
        "name": row["name"],
        "description": row["description"],
        "status": row["status"],
        "mode": row["mode"],
        "starter": bool(row["starter"]),
        "defaultEntry": row["default_entry"],
        "recommendedTeamId": row["recommended_team_id"],
        "stages": stages if isinstance(stages, list) else [],
        "skills": skills if isinstance(skills, list) else [],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_chat_message(record):
    if not isinstance(record, dict):
        return None
    thread_id = str(record.get("threadId") or record.get("thread_id") or "").strip()
    body = str(record.get("body") or record.get("text") or "").strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    attachments = record.get("attachments") if isinstance(record.get("attachments"), list) else meta.get("attachments")
    has_attachments = any(isinstance(item, dict) and str(item.get("name") or "").strip() for item in attachments or [])
    if not thread_id or (not body and not has_attachments):
        return None
    if len(body) > CHAT_MESSAGE_MAX_BODY_LENGTH:
        raise RuntimeError(f"消息体超过最大长度限制（{CHAT_MESSAGE_MAX_BODY_LENGTH} 字符）。")
    direction = str(record.get("direction") or "outbound").strip().lower()
    if direction not in CHAT_MESSAGE_DIRECTIONS:
        raise RuntimeError(f"无效的消息方向：{direction!r}，必须是 {sorted(CHAT_MESSAGE_DIRECTIONS)} 之一。")
    if has_attachments and not isinstance(meta.get("attachments"), list):
        meta = {**meta, "attachments": [item for item in attachments if isinstance(item, dict)]}
    return {
        "message_id": str(record.get("id") or record.get("message_id") or secrets.token_hex(8)).strip(),
        "thread_id": thread_id,
        "sender_kind": str(record.get("senderKind") or record.get("sender_kind") or "user").strip(),
        "sender_id": str(record.get("senderId") or record.get("sender_id") or "").strip(),
        "sender_label": str(record.get("senderLabel") or record.get("sender_label") or "").strip(),
        "direction": direction,
        "body": body,
        "created_at": str(record.get("createdAt") or record.get("created_at") or now_iso()).strip(),
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _chat_message_row_to_dict(row):
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["message_id"],
        "threadId": row["thread_id"],
        "senderKind": row["sender_kind"],
        "senderId": row["sender_id"],
        "senderLabel": row["sender_label"],
        "direction": row["direction"],
        "body": row["body"],
        "createdAt": row["created_at"],
        "meta": meta,
    }


def _ensure_legacy_migration(openclaw_dir, conn):
    if _metadata(conn, "legacy_users_migrated") != "1":
        legacy_users = _load_json(legacy_users_path(openclaw_dir), {"users": []})
        for user in legacy_users.get("users", []) if isinstance(legacy_users, dict) else []:
            normalized = _normalize_user_record(user)
            if not normalized:
                continue
            conn.execute(
                """
                INSERT INTO product_users(
                    username, user_id, display_name, role, password_hash, status, created_at, last_login_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    user_id = excluded.user_id,
                    display_name = excluded.display_name,
                    role = excluded.role,
                    password_hash = excluded.password_hash,
                    status = excluded.status,
                    created_at = excluded.created_at,
                    last_login_at = excluded.last_login_at
                """,
                (
                    normalized["username"],
                    normalized["user_id"],
                    normalized["display_name"],
                    normalized["role"],
                    normalized["password_hash"],
                    normalized["status"],
                    normalized["created_at"],
                    normalized["last_login_at"],
                ),
            )
        _set_metadata(conn, "legacy_users_migrated", "1")

    if _metadata(conn, "legacy_audit_migrated") != "1":
        legacy_path = legacy_audit_path(openclaw_dir)
        if legacy_path.exists():
            for line in legacy_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                normalized = _normalize_audit_event(event)
                if not normalized:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO audit_events(
                        id, at, action, outcome, detail, actor_json, meta_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized["id"],
                        normalized["at"],
                        normalized["action"],
                        normalized["outcome"],
                        normalized["detail"],
                        normalized["actor_json"],
                        normalized["meta_json"],
                    ),
                )
        _set_metadata(conn, "legacy_audit_migrated", "1")

    if _metadata(conn, "legacy_task_records_migrated") != "1":
        _set_metadata(conn, "legacy_task_records_migrated", "1")

    conn.commit()


def load_product_users(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT user_id, username, display_name, role, password_hash, status, created_at, last_login_at
            FROM product_users
            ORDER BY created_at ASC, username ASC
            """
        ).fetchall()
    return [
        {
            "id": row["user_id"],
            "username": row["username"],
            "displayName": row["display_name"],
            "role": row["role"],
            "passwordHash": row["password_hash"],
            "status": row["status"],
            "createdAt": row["created_at"],
            "lastLoginAt": row["last_login_at"],
        }
        for row in rows
    ]


def upsert_product_user(openclaw_dir, user):
    normalized = _normalize_user_record(user)
    if not normalized:
        raise RuntimeError("user record is missing username")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO product_users(
                username, user_id, display_name, role, password_hash, status, created_at, last_login_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                user_id = COALESCE(NULLIF(product_users.user_id, ''), excluded.user_id),
                display_name = excluded.display_name,
                role = excluded.role,
                password_hash = excluded.password_hash,
                status = excluded.status,
                created_at = COALESCE(NULLIF(product_users.created_at, ''), excluded.created_at),
                last_login_at = excluded.last_login_at
            """,
            (
                normalized["username"],
                normalized["user_id"],
                normalized["display_name"],
                normalized["role"],
                normalized["password_hash"],
                normalized["status"],
                normalized["created_at"],
                normalized["last_login_at"],
            ),
        )
        conn.commit()
    users = load_product_users(openclaw_dir)
    return next((item for item in users if item["username"] == normalized["username"]), None)


def touch_product_user_login(openclaw_dir, username, logged_in_at=""):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return None
    login_at = str(logged_in_at or now_iso()).strip() or now_iso()
    with _connect(openclaw_dir) as conn:
        cursor = conn.execute(
            """
            UPDATE product_users
            SET last_login_at = ?
            WHERE username = ?
            """,
            (login_at, normalized_username),
        )
        conn.commit()
    if not getattr(cursor, "rowcount", 0):
        return None
    users = load_product_users(openclaw_dir)
    return next((item for item in users if item["username"] == normalized_username), None)


def load_product_installations(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT openclaw_dir, installation_id, label, project_dir, theme, router_agent_id, created_at, updated_at
            FROM product_installations
            ORDER BY updated_at DESC, label ASC
            """
        ).fetchall()
    return [
        {
            "id": row["installation_id"],
            "openclawDir": row["openclaw_dir"],
            "label": row["label"],
            "projectDir": row["project_dir"],
            "theme": row["theme"],
            "routerAgentId": row["router_agent_id"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }
        for row in rows
    ]


def upsert_product_installation(openclaw_dir, installation):
    normalized = _normalize_installation_record(installation)
    if not normalized:
        raise RuntimeError("installation record is missing openclawDir")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO product_installations(
                openclaw_dir, installation_id, label, project_dir, theme, router_agent_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(openclaw_dir) DO UPDATE SET
                label = excluded.label,
                project_dir = excluded.project_dir,
                theme = excluded.theme,
                router_agent_id = excluded.router_agent_id,
                updated_at = excluded.updated_at
            """,
            (
                normalized["openclaw_dir"],
                normalized["installation_id"],
                normalized["label"],
                normalized["project_dir"],
                normalized["theme"],
                normalized["router_agent_id"],
                normalized["created_at"],
                normalized["updated_at"],
            ),
        )
        conn.commit()
    return {
        "id": normalized["installation_id"],
        "openclawDir": normalized["openclaw_dir"],
        "label": normalized["label"],
        "projectDir": normalized["project_dir"],
        "theme": normalized["theme"],
        "routerAgentId": normalized["router_agent_id"],
        "createdAt": normalized["created_at"],
        "updatedAt": normalized["updated_at"],
    }


def delete_product_installation(openclaw_dir, target_openclaw_dir):
    normalized_dir = str(target_openclaw_dir or "").strip()
    if not normalized_dir:
        return False
    with _connect(openclaw_dir) as conn:
        cursor = conn.execute("DELETE FROM product_installations WHERE openclaw_dir = ?", (normalized_dir,))
        conn.commit()
    return cursor.rowcount > 0


def save_product_users(openclaw_dir, users):
    normalized = [record for record in (_normalize_user_record(user) for user in users) if record]
    with _connect(openclaw_dir) as conn:
        conn.execute("DELETE FROM product_users")
        conn.executemany(
            """
            INSERT INTO product_users(
                username, user_id, display_name, role, password_hash, status, created_at, last_login_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    record["username"],
                    record["user_id"],
                    record["display_name"],
                    record["role"],
                    record["password_hash"],
                    record["status"],
                    record["created_at"],
                    record["last_login_at"],
                )
                for record in normalized
            ],
        )
        conn.commit()


def append_audit_event(openclaw_dir, action, actor, outcome="success", detail="", meta=None):
    event = _normalize_audit_event(
        {
            "id": secrets.token_hex(8),
            "at": now_iso(),
            "action": action,
            "outcome": outcome,
            "detail": detail,
            "actor": actor or {"displayName": "system", "role": "owner", "kind": "system"},
            "meta": meta or {},
        }
    )
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO audit_events(
                id, at, action, outcome, detail, actor_json, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["id"],
                event["at"],
                event["action"],
                event["outcome"],
                event["detail"],
                event["actor_json"],
                event["meta_json"],
            ),
        )
        conn.commit()
    return {
        "id": event["id"],
        "at": event["at"],
        "action": event["action"],
        "outcome": event["outcome"],
        "detail": event["detail"],
        "actor": json.loads(event["actor_json"]),
        "meta": json.loads(event["meta_json"]),
    }


def load_audit_events(openclaw_dir, limit=80):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT id, at, action, outcome, detail, actor_json, meta_json
            FROM audit_events
            ORDER BY at DESC, id DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 0),),
        ).fetchall()
    events = []
    for row in rows:
        try:
            actor = json.loads(row["actor_json"] or "{}")
        except Exception:
            actor = {}
        try:
            meta = json.loads(row["meta_json"] or "{}")
        except Exception:
            meta = {}
        events.append(
            {
                "id": row["id"],
                "at": row["at"],
                "action": row["action"],
                "outcome": row["outcome"],
                "detail": row["detail"],
                "actor": actor,
                "meta": meta,
            }
        )
    return events


def list_management_runs(openclaw_dir, limit=32):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                run_id, title, goal, owner, status, stage_key, linked_task_id, linked_agent_id,
                linked_session_key, release_channel, risk_level, created_at, updated_at,
                started_at, completed_at, stages_json, meta_json
            FROM management_runs
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()
    return [_management_row_to_dict(row) for row in rows]


def get_management_run_record(openclaw_dir, run_id):
    run_id = str(run_id or "").strip()
    if not run_id:
        return None
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                run_id, title, goal, owner, status, stage_key, linked_task_id, linked_agent_id,
                linked_session_key, release_channel, risk_level, created_at, updated_at,
                started_at, completed_at, stages_json, meta_json
            FROM management_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
    return _management_row_to_dict(row) if row else None


def create_management_run(openclaw_dir, payload):
    normalized = _normalize_management_record(payload)
    if not normalized:
        raise RuntimeError("management run title is required")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO management_runs(
                run_id, title, goal, owner, status, stage_key, linked_task_id, linked_agent_id,
                linked_session_key, release_channel, risk_level, created_at, updated_at,
                started_at, completed_at, stages_json, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized["run_id"],
                normalized["title"],
                normalized["goal"],
                normalized["owner"],
                normalized["status"],
                normalized["stage_key"],
                normalized["linked_task_id"],
                normalized["linked_agent_id"],
                normalized["linked_session_key"],
                normalized["release_channel"],
                normalized["risk_level"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["started_at"],
                normalized["completed_at"],
                normalized["stages_json"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_management_runs(openclaw_dir, limit=64) if item["id"] == normalized["run_id"]), None)


def update_management_run(openclaw_dir, run_id, action, note="", risk_level="", linked_task_id=""):
    run_id = str(run_id or "").strip()
    if not run_id:
        raise RuntimeError("management run id is required")
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                run_id, title, goal, owner, status, stage_key, linked_task_id, linked_agent_id,
                linked_session_key, release_channel, risk_level, created_at, updated_at,
                started_at, completed_at, stages_json, meta_json
            FROM management_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"management run not found: {run_id}")
        record = _management_row_to_dict(row)
        stages = record["stages"] or _default_management_stages()
        stage_index = next((idx for idx, stage in enumerate(stages) if stage["key"] == record["stageKey"]), 0)
        current_stage = stages[stage_index]
        if note:
            current_stage["note"] = str(note).strip()
            current_stage["updatedAt"] = now_iso()
        if linked_task_id:
            record["linkedTaskId"] = str(linked_task_id).strip()
        if risk_level:
            level = str(risk_level).strip().lower()
            if level in {"low", "medium", "high"}:
                record["riskLevel"] = level
        action = str(action or "").strip().lower()
        now = now_iso()
        if action == "advance":
            current_stage["status"] = "done"
            current_stage["updatedAt"] = now
            if stage_index + 1 < len(stages):
                next_stage = stages[stage_index + 1]
                if next_stage["status"] != "done":
                    next_stage["status"] = "active"
                    next_stage["updatedAt"] = now
                record["stageKey"] = next_stage["key"]
                record["status"] = "active"
            else:
                record["stageKey"] = stages[-1]["key"]
                record["status"] = "complete"
                record["completedAt"] = now
        elif action == "block":
            current_stage["status"] = "blocked"
            current_stage["updatedAt"] = now
            record["status"] = "blocked"
        elif action == "resume":
            current_stage["status"] = "active"
            current_stage["updatedAt"] = now
            record["status"] = "active"
        elif action == "complete":
            for stage in stages:
                stage["status"] = "done"
                stage["updatedAt"] = now
            record["stageKey"] = stages[-1]["key"]
            record["status"] = "complete"
            record["completedAt"] = now
        elif action == "note":
            pass
        else:
            raise RuntimeError(f"unsupported management action: {action}")
        record["updatedAt"] = now
        record["stages"] = stages
        normalized = _normalize_management_record(record)
        conn.execute(
            """
            UPDATE management_runs
            SET
                title = ?, goal = ?, owner = ?, status = ?, stage_key = ?, linked_task_id = ?, linked_agent_id = ?,
                linked_session_key = ?, release_channel = ?, risk_level = ?, updated_at = ?, started_at = ?,
                completed_at = ?, stages_json = ?, meta_json = ?
            WHERE run_id = ?
            """,
            (
                normalized["title"],
                normalized["goal"],
                normalized["owner"],
                normalized["status"],
                normalized["stage_key"],
                normalized["linked_task_id"],
                normalized["linked_agent_id"],
                normalized["linked_session_key"],
                normalized["release_channel"],
                normalized["risk_level"],
                normalized["updated_at"],
                normalized["started_at"],
                normalized["completed_at"],
                normalized["stages_json"],
                normalized["meta_json"],
                run_id,
            ),
        )
        conn.commit()
    return next((item for item in list_management_runs(openclaw_dir, limit=64) if item["id"] == run_id), None)


def save_management_run_planning_binding(openclaw_dir, run_id, planning_binding):
    run_id = str(run_id or "").strip()
    if not run_id or not isinstance(planning_binding, dict) or not planning_binding:
        raise RuntimeError("management run planning binding requires run_id and planning_binding")
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                run_id, title, goal, owner, status, stage_key, linked_task_id, linked_agent_id,
                linked_session_key, release_channel, risk_level, created_at, updated_at,
                started_at, completed_at, stages_json, meta_json
            FROM management_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"management run not found: {run_id}")
        record = _management_row_to_dict(row)
        meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
        meta["planningBinding"] = planning_binding
        record["meta"] = meta
        record["planningBinding"] = planning_binding
        record["updatedAt"] = now_iso()
        normalized = _normalize_management_record(record)
        conn.execute(
            """
            UPDATE management_runs
            SET
                title = ?, goal = ?, owner = ?, status = ?, stage_key = ?, linked_task_id = ?, linked_agent_id = ?,
                linked_session_key = ?, release_channel = ?, risk_level = ?, updated_at = ?, started_at = ?,
                completed_at = ?, stages_json = ?, meta_json = ?
            WHERE run_id = ?
            """,
            (
                normalized["title"],
                normalized["goal"],
                normalized["owner"],
                normalized["status"],
                normalized["stage_key"],
                normalized["linked_task_id"],
                normalized["linked_agent_id"],
                normalized["linked_session_key"],
                normalized["release_channel"],
                normalized["risk_level"],
                normalized["updated_at"],
                normalized["started_at"],
                normalized["completed_at"],
                normalized["stages_json"],
                normalized["meta_json"],
                run_id,
            ),
        )
        conn.commit()
    return next((item for item in list_management_runs(openclaw_dir, limit=64) if item["id"] == run_id), None)


def save_management_run_pack_binding(openclaw_dir, run_id, pack_binding):
    run_id = str(run_id or "").strip()
    if not run_id or not isinstance(pack_binding, dict) or not pack_binding:
        raise RuntimeError("management run pack binding requires run_id and pack_binding")
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                run_id, title, goal, owner, status, stage_key, linked_task_id, linked_agent_id,
                linked_session_key, release_channel, risk_level, created_at, updated_at,
                started_at, completed_at, stages_json, meta_json
            FROM management_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"management run not found: {run_id}")
        record = _management_row_to_dict(row)
        meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
        meta["packBinding"] = pack_binding
        record["meta"] = meta
        record["packBinding"] = pack_binding
        record["updatedAt"] = now_iso()
        normalized = _normalize_management_record(record)
        conn.execute(
            """
            UPDATE management_runs
            SET
                title = ?, goal = ?, owner = ?, status = ?, stage_key = ?, linked_task_id = ?, linked_agent_id = ?,
                linked_session_key = ?, release_channel = ?, risk_level = ?, updated_at = ?, started_at = ?,
                completed_at = ?, stages_json = ?, meta_json = ?
            WHERE run_id = ?
            """,
            (
                normalized["title"],
                normalized["goal"],
                normalized["owner"],
                normalized["status"],
                normalized["stage_key"],
                normalized["linked_task_id"],
                normalized["linked_agent_id"],
                normalized["linked_session_key"],
                normalized["release_channel"],
                normalized["risk_level"],
                normalized["updated_at"],
                normalized["started_at"],
                normalized["completed_at"],
                normalized["stages_json"],
                normalized["meta_json"],
                run_id,
            ),
        )
        conn.commit()
    return next((item for item in list_management_runs(openclaw_dir, limit=64) if item["id"] == run_id), None)


def save_management_run_record(openclaw_dir, run_id, payload):
    run_id = str(run_id or "").strip()
    if not run_id:
        raise RuntimeError("management run id is required")
    payload = payload if isinstance(payload, dict) else {}
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                run_id, title, goal, owner, status, stage_key, linked_task_id, linked_agent_id,
                linked_session_key, release_channel, risk_level, created_at, updated_at,
                started_at, completed_at, stages_json, meta_json
            FROM management_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"management run not found: {run_id}")
        record = _management_row_to_dict(row)
        meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
        meta_patch = payload.get("metaPatch") if isinstance(payload.get("metaPatch"), dict) else {}
        replacement_meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else None
        if replacement_meta is not None:
            meta = replacement_meta
        elif meta_patch:
            meta = {**meta, **meta_patch}
        record["meta"] = meta

        for source_key, target_key in (
            ("title", "title"),
            ("goal", "goal"),
            ("owner", "owner"),
            ("status", "status"),
            ("stageKey", "stageKey"),
            ("linkedTaskId", "linkedTaskId"),
            ("linkedAgentId", "linkedAgentId"),
            ("linkedSessionKey", "linkedSessionKey"),
            ("releaseChannel", "releaseChannel"),
            ("riskLevel", "riskLevel"),
            ("startedAt", "startedAt"),
            ("completedAt", "completedAt"),
            ("stages", "stages"),
            ("workflowBinding", "workflowBinding"),
            ("planningBinding", "planningBinding"),
            ("packBinding", "packBinding"),
            ("linkedTeamId", "linkedTeamId"),
        ):
            if source_key in payload:
                record[target_key] = payload.get(source_key)
        record["updatedAt"] = str(payload.get("updatedAt") or now_iso()).strip()

        normalized = _normalize_management_record(record)
        conn.execute(
            """
            UPDATE management_runs
            SET
                title = ?, goal = ?, owner = ?, status = ?, stage_key = ?, linked_task_id = ?, linked_agent_id = ?,
                linked_session_key = ?, release_channel = ?, risk_level = ?, updated_at = ?, started_at = ?,
                completed_at = ?, stages_json = ?, meta_json = ?
            WHERE run_id = ?
            """,
            (
                normalized["title"],
                normalized["goal"],
                normalized["owner"],
                normalized["status"],
                normalized["stage_key"],
                normalized["linked_task_id"],
                normalized["linked_agent_id"],
                normalized["linked_session_key"],
                normalized["release_channel"],
                normalized["risk_level"],
                normalized["updated_at"],
                normalized["started_at"],
                normalized["completed_at"],
                normalized["stages_json"],
                normalized["meta_json"],
                run_id,
            ),
        )
        conn.commit()
    return next((item for item in list_management_runs(openclaw_dir, limit=64) if item["id"] == run_id), None)


def list_automation_rules(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                rule_id, name, description, status, trigger_type, threshold_minutes, cooldown_minutes,
                severity, match_text, channel_ids_json, created_at, updated_at, meta_json
            FROM automation_rules
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_automation_rule_row_to_dict(row) for row in rows]


def save_automation_rule(openclaw_dir, payload):
    normalized = _normalize_automation_rule(payload)
    if not normalized:
        raise RuntimeError("automation rule name is required")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO automation_rules(
                rule_id, name, description, status, trigger_type, threshold_minutes, cooldown_minutes,
                severity, match_text, channel_ids_json, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rule_id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                status = excluded.status,
                trigger_type = excluded.trigger_type,
                threshold_minutes = excluded.threshold_minutes,
                cooldown_minutes = excluded.cooldown_minutes,
                severity = excluded.severity,
                match_text = excluded.match_text,
                channel_ids_json = excluded.channel_ids_json,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["rule_id"],
                normalized["name"],
                normalized["description"],
                normalized["status"],
                normalized["trigger_type"],
                normalized["threshold_minutes"],
                normalized["cooldown_minutes"],
                normalized["severity"],
                normalized["match_text"],
                normalized["channel_ids_json"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_automation_rules(openclaw_dir) if item["id"] == normalized["rule_id"]), None)


def list_notification_channels(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                channel_id, name, channel_type, status, target, secret, created_at, updated_at, meta_json
            FROM notification_channels
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_notification_channel_row_to_dict(row) for row in rows]


def save_notification_channel(openclaw_dir, payload):
    normalized = _normalize_notification_channel(payload)
    if not normalized:
        raise RuntimeError("notification channel name and type are required")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO notification_channels(
                channel_id, name, channel_type, status, target, secret, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                name = excluded.name,
                channel_type = excluded.channel_type,
                status = excluded.status,
                target = excluded.target,
                secret = excluded.secret,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["channel_id"],
                normalized["name"],
                normalized["channel_type"],
                normalized["status"],
                normalized["target"],
                normalized["secret"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_notification_channels(openclaw_dir) if item["id"] == normalized["channel_id"]), None)


def list_customer_access_channels(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                channel_id, name, channel_type, status, target, entry_url, created_at, updated_at, meta_json
            FROM customer_access_channels
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    items = [_customer_access_channel_row_to_dict(row) for row in rows]
    return _dedupe_customer_access_channels(items)


def save_customer_access_channel(openclaw_dir, payload):
    normalized = _normalize_customer_access_channel(payload)
    if not normalized:
        raise RuntimeError("customer access channel name and type are required")
    explicit_id = str(payload.get("id") or payload.get("channel_id") or "").strip() if isinstance(payload, dict) else ""
    if not explicit_id:
        dedupe_probe = {
            "name": normalized["name"],
            "type": normalized["channel_type"],
            "target": normalized["target"],
            "entryUrl": normalized["entry_url"],
            "meta": json.loads(normalized["meta_json"] or "{}"),
        }
        dedupe_key = _customer_access_channel_dedupe_key(dedupe_probe)
        if dedupe_key:
            existing = next((item for item in list_customer_access_channels(openclaw_dir) if _customer_access_channel_dedupe_key(item) == dedupe_key), None)
            if existing:
                normalized["channel_id"] = str(existing.get("id") or normalized["channel_id"]).strip()
                normalized["created_at"] = str(existing.get("createdAt") or normalized["created_at"]).strip()
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO customer_access_channels(
                channel_id, name, channel_type, status, target, entry_url, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                name = excluded.name,
                channel_type = excluded.channel_type,
                status = excluded.status,
                target = excluded.target,
                entry_url = excluded.entry_url,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["channel_id"],
                normalized["name"],
                normalized["channel_type"],
                normalized["status"],
                normalized["target"],
                normalized["entry_url"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_customer_access_channels(openclaw_dir) if item["id"] == normalized["channel_id"]), None)


def list_model_provider_configs(openclaw_dir):
    return _list_model_provider_configs(openclaw_dir)


def save_model_provider_config(openclaw_dir, payload):
    return _save_model_provider_config(openclaw_dir, payload)


def list_automation_alerts(openclaw_dir, limit=60):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                alert_id, rule_id, event_key, title, detail, severity, status, source_type, source_id,
                triggered_at, updated_at, resolved_at, meta_json
            FROM automation_alerts
            ORDER BY updated_at DESC, triggered_at DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()
    return [_automation_alert_row_to_dict(row) for row in rows]


def upsert_automation_alert(openclaw_dir, payload):
    normalized = _normalize_automation_alert(payload)
    if not normalized:
        raise RuntimeError("automation alert ruleId, eventKey, and title are required")
    existing = next(
        (
            item
            for item in list_automation_alerts(openclaw_dir, limit=256)
            if item["ruleId"] == normalized["rule_id"] and item["eventKey"] == normalized["event_key"]
        ),
        None,
    )
    if existing:
        normalized["alert_id"] = existing["id"]
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO automation_alerts(
                alert_id, rule_id, event_key, title, detail, severity, status, source_type, source_id,
                triggered_at, updated_at, resolved_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rule_id, event_key) DO UPDATE SET
                title = excluded.title,
                detail = excluded.detail,
                severity = excluded.severity,
                status = excluded.status,
                source_type = excluded.source_type,
                source_id = excluded.source_id,
                updated_at = excluded.updated_at,
                resolved_at = excluded.resolved_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["alert_id"],
                normalized["rule_id"],
                normalized["event_key"],
                normalized["title"],
                normalized["detail"],
                normalized["severity"],
                normalized["status"],
                normalized["source_type"],
                normalized["source_id"],
                normalized["triggered_at"],
                normalized["updated_at"],
                normalized["resolved_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next(
        (
            item
            for item in list_automation_alerts(openclaw_dir, limit=256)
            if item["ruleId"] == normalized["rule_id"] and item["eventKey"] == normalized["event_key"]
        ),
        None,
    )


def resolve_automation_alerts(openclaw_dir, rule_id, active_event_keys):
    rule_id = str(rule_id or "").strip()
    if not rule_id:
        return 0
    keys = {str(item).strip() for item in (active_event_keys or []) if str(item).strip()}
    resolved = 0
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT alert_id, event_key
            FROM automation_alerts
            WHERE rule_id = ? AND status IN ('open', 'notified', 'error')
            """,
            (rule_id,),
        ).fetchall()
        for row in rows:
            if row["event_key"] in keys:
                continue
            conn.execute(
                """
                UPDATE automation_alerts
                SET status = 'resolved', updated_at = ?, resolved_at = ?
                WHERE alert_id = ?
                """,
                (now_iso(), now_iso(), row["alert_id"]),
            )
            resolved += 1
        conn.commit()
    return resolved


def list_notification_deliveries(openclaw_dir, limit=120):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT delivery_id, alert_id, channel_id, outcome, detail, delivered_at, meta_json
            FROM notification_deliveries
            ORDER BY delivered_at DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()
    deliveries = []
    for row in rows:
        try:
            meta = json.loads(row["meta_json"] or "{}")
        except Exception:
            meta = {}
        deliveries.append(
            {
                "id": row["delivery_id"],
                "alertId": row["alert_id"],
                "channelId": row["channel_id"],
                "outcome": row["outcome"],
                "detail": row["detail"],
                "deliveredAt": row["delivered_at"],
                "meta": meta,
            }
        )
    return deliveries


def save_notification_delivery(openclaw_dir, alert_id, channel_id, outcome, detail="", meta=None):
    alert_id = str(alert_id or "").strip()
    channel_id = str(channel_id or "").strip()
    if not alert_id or not channel_id:
        raise RuntimeError("notification delivery requires alert_id and channel_id")
    meta_payload = json.dumps(meta or {}, ensure_ascii=False, separators=(",", ":"))
    delivered_at = now_iso()
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            """
            SELECT delivery_id FROM notification_deliveries WHERE alert_id = ? AND channel_id = ?
            """,
            (alert_id, channel_id),
        ).fetchone()
        delivery_id = existing["delivery_id"] if existing else secrets.token_hex(8)
        conn.execute(
            """
            INSERT INTO notification_deliveries(
                delivery_id, alert_id, channel_id, outcome, detail, delivered_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(alert_id, channel_id) DO UPDATE SET
                outcome = excluded.outcome,
                detail = excluded.detail,
                delivered_at = excluded.delivered_at,
                meta_json = excluded.meta_json
            """,
            (delivery_id, alert_id, channel_id, str(outcome or "success"), str(detail or ""), delivered_at, meta_payload),
        )
        conn.commit()
    return next(
        (
            item
            for item in list_notification_deliveries(openclaw_dir, limit=256)
            if item["alertId"] == alert_id and item["channelId"] == channel_id
        ),
        None,
    )


def list_orchestration_workflows(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT workflow_id, name, description, status, lanes_json, nodes_json, created_at, updated_at, meta_json
            FROM orchestration_workflows
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_orchestration_workflow_row_to_dict(row) for row in rows]


def list_orchestration_workflow_versions(openclaw_dir, workflow_id="", limit=80):
    workflow_id = str(workflow_id or "").strip()
    with _connect(openclaw_dir) as conn:
        if workflow_id:
            rows = conn.execute(
                """
                SELECT
                    version_id, workflow_id, version_number, name, description, status,
                    lanes_json, nodes_json, created_at, meta_json
                FROM orchestration_workflow_versions
                WHERE workflow_id = ?
                ORDER BY version_number DESC, created_at DESC
                LIMIT ?
                """,
                (workflow_id, max(int(limit or 0), 1)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    version_id, workflow_id, version_number, name, description, status,
                    lanes_json, nodes_json, created_at, meta_json
                FROM orchestration_workflow_versions
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (max(int(limit or 0), 1),),
            ).fetchall()
    return [_workflow_version_row_to_dict(row) for row in rows]


def save_orchestration_workflow(openclaw_dir, payload):
    normalized = _normalize_orchestration_workflow(payload)
    if not normalized:
        raise RuntimeError("orchestration workflow name is required")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            """
            SELECT workflow_id, created_at, meta_json
            FROM orchestration_workflows
            WHERE workflow_id = ?
            """,
            (normalized["workflow_id"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        current_versions = conn.execute(
            """
            SELECT COALESCE(MAX(version_number), 0) AS version_number
            FROM orchestration_workflow_versions
            WHERE workflow_id = ?
            """,
            (normalized["workflow_id"],),
        ).fetchone()
        next_version_number = int(current_versions["version_number"] if current_versions else 0) + 1
        try:
            existing_meta = json.loads(existing["meta_json"] or "{}") if existing else {}
        except Exception:
            existing_meta = {}
        try:
            workflow_meta = json.loads(normalized["meta_json"] or "{}")
        except Exception:
            workflow_meta = {}
        workflow_meta = {
            **existing_meta,
            **workflow_meta,
            "latestVersionNumber": next_version_number,
        }
        version_id = secrets.token_hex(8)
        workflow_meta["latestVersionId"] = version_id
        normalized["meta_json"] = json.dumps(workflow_meta, ensure_ascii=False, separators=(",", ":"))
        conn.execute(
            """
            INSERT INTO orchestration_workflows(
                workflow_id, name, description, status, lanes_json, nodes_json, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workflow_id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                status = excluded.status,
                lanes_json = excluded.lanes_json,
                nodes_json = excluded.nodes_json,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["workflow_id"],
                normalized["name"],
                normalized["description"],
                normalized["status"],
                normalized["lanes_json"],
                normalized["nodes_json"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        version_record = _normalize_workflow_version(
            {
                "id": version_id,
                "workflowId": normalized["workflow_id"],
                "versionNumber": next_version_number,
                "name": normalized["name"],
                "description": normalized["description"],
                "status": normalized["status"],
                "lanes": json.loads(normalized["lanes_json"] or "[]"),
                "nodes": json.loads(normalized["nodes_json"] or "[]"),
                "createdAt": normalized["updated_at"],
                "meta": {"savedFrom": "workflow-save"},
            }
        )
        conn.execute(
            """
            INSERT INTO orchestration_workflow_versions(
                version_id, workflow_id, version_number, name, description, status, lanes_json, nodes_json, created_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_record["version_id"],
                version_record["workflow_id"],
                version_record["version_number"],
                version_record["name"],
                version_record["description"],
                version_record["status"],
                version_record["lanes_json"],
                version_record["nodes_json"],
                version_record["created_at"],
                version_record["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_orchestration_workflows(openclaw_dir) if item["id"] == normalized["workflow_id"]), None)


def restore_orchestration_workflow_version(openclaw_dir, workflow_id, version_id):
    workflow_id = str(workflow_id or "").strip()
    version_id = str(version_id or "").strip()
    if not workflow_id or not version_id:
        raise RuntimeError("workflow_id and version_id are required")
    version = next(
        (
            item
            for item in list_orchestration_workflow_versions(openclaw_dir, workflow_id=workflow_id, limit=200)
            if item["id"] == version_id
        ),
        None,
    )
    if not version:
        raise RuntimeError(f"workflow version not found: {version_id}")
    return save_orchestration_workflow(
        openclaw_dir,
        {
            "id": workflow_id,
            "name": version["name"],
            "description": version["description"],
            "status": version["status"],
            "lanes": version["lanes"],
            "nodes": version["nodes"],
            "meta": {
                "restoredFromVersionId": version["id"],
                "restoredFromVersionNumber": version["versionNumber"],
            },
        },
    )


def list_routing_policies(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                policy_id, name, status, strategy_type, keyword, target_agent_id, priority_level, queue_name,
                created_at, updated_at, meta_json
            FROM routing_policies
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_routing_policy_row_to_dict(row) for row in rows]


def save_routing_policy(openclaw_dir, payload):
    normalized = _normalize_routing_policy(payload)
    if not normalized:
        raise RuntimeError("routing policy name is required")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO routing_policies(
                policy_id, name, status, strategy_type, keyword, target_agent_id, priority_level, queue_name,
                created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(policy_id) DO UPDATE SET
                name = excluded.name,
                status = excluded.status,
                strategy_type = excluded.strategy_type,
                keyword = excluded.keyword,
                target_agent_id = excluded.target_agent_id,
                priority_level = excluded.priority_level,
                queue_name = excluded.queue_name,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["policy_id"],
                normalized["name"],
                normalized["status"],
                normalized["strategy_type"],
                normalized["keyword"],
                normalized["target_agent_id"],
                normalized["priority_level"],
                normalized["queue_name"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_routing_policies(openclaw_dir) if item["id"] == normalized["policy_id"]), None)


def list_routing_decisions(openclaw_dir, limit=120):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                decision_id, task_id, task_title, policy_id, policy_name, workflow_id, workflow_version_id,
                strategy_type, matched_keyword, queue_name, priority_level, target_agent_id, source_text,
                decided_at, meta_json
            FROM routing_decisions
            ORDER BY decided_at DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()
    return [_routing_decision_row_to_dict(row) for row in rows]


def save_routing_decision(openclaw_dir, payload):
    normalized = _normalize_routing_decision(payload)
    if not normalized:
        raise RuntimeError("routing decision taskId, taskTitle, and targetAgentId are required")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO routing_decisions(
                decision_id, task_id, task_title, policy_id, policy_name, workflow_id, workflow_version_id,
                strategy_type, matched_keyword, queue_name, priority_level, target_agent_id, source_text,
                decided_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(decision_id) DO UPDATE SET
                task_id = excluded.task_id,
                task_title = excluded.task_title,
                policy_id = excluded.policy_id,
                policy_name = excluded.policy_name,
                workflow_id = excluded.workflow_id,
                workflow_version_id = excluded.workflow_version_id,
                strategy_type = excluded.strategy_type,
                matched_keyword = excluded.matched_keyword,
                queue_name = excluded.queue_name,
                priority_level = excluded.priority_level,
                target_agent_id = excluded.target_agent_id,
                source_text = excluded.source_text,
                decided_at = excluded.decided_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["decision_id"],
                normalized["task_id"],
                normalized["task_title"],
                normalized["policy_id"],
                normalized["policy_name"],
                normalized["workflow_id"],
                normalized["workflow_version_id"],
                normalized["strategy_type"],
                normalized["matched_keyword"],
                normalized["queue_name"],
                normalized["priority_level"],
                normalized["target_agent_id"],
                normalized["source_text"],
                normalized["decided_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_routing_decisions(openclaw_dir, limit=256) if item["id"] == normalized["decision_id"]), None)


def list_tenants(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT tenant_id, name, slug, status, primary_openclaw_dir, created_at, updated_at, meta_json
            FROM tenants
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_tenant_row_to_dict(row) for row in rows]


def save_tenant(openclaw_dir, payload):
    normalized = _normalize_tenant(payload)
    if not normalized:
        raise RuntimeError("tenant name is required")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO tenants(
                tenant_id, name, slug, status, primary_openclaw_dir, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tenant_id) DO UPDATE SET
                name = excluded.name,
                slug = excluded.slug,
                status = excluded.status,
                primary_openclaw_dir = excluded.primary_openclaw_dir,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["tenant_id"],
                normalized["name"],
                normalized["slug"],
                normalized["status"],
                normalized["primary_openclaw_dir"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_tenants(openclaw_dir) if item["id"] == normalized["tenant_id"]), None)


def list_tenant_installations(openclaw_dir, tenant_id=""):
    with _connect(openclaw_dir) as conn:
        if tenant_id:
            rows = conn.execute(
                """
                SELECT tenant_id, openclaw_dir, label, role, created_at, updated_at, meta_json
                FROM tenant_installations
                WHERE tenant_id = ?
                ORDER BY updated_at DESC, created_at DESC
                """,
                (str(tenant_id).strip(),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT tenant_id, openclaw_dir, label, role, created_at, updated_at, meta_json
                FROM tenant_installations
                ORDER BY updated_at DESC, created_at DESC
                """
            ).fetchall()
    return [_tenant_installation_row_to_dict(row) for row in rows]


def save_tenant_installation(openclaw_dir, payload):
    normalized = _normalize_tenant_installation(payload)
    if not normalized:
        raise RuntimeError("tenant installation requires tenantId and openclawDir")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO tenant_installations(
                tenant_id, openclaw_dir, label, role, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tenant_id, openclaw_dir) DO UPDATE SET
                label = excluded.label,
                role = excluded.role,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["tenant_id"],
                normalized["openclaw_dir"],
                normalized["label"],
                normalized["role"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next(
        (
            item
            for item in list_tenant_installations(openclaw_dir, tenant_id=normalized["tenant_id"])
            if item["openclawDir"] == normalized["openclaw_dir"]
        ),
        None,
    )


def list_tenant_api_keys(openclaw_dir, tenant_id=""):
    with _connect(openclaw_dir) as conn:
        if tenant_id:
            rows = conn.execute(
                """
                SELECT key_id, tenant_id, name, prefix, status, scopes_json, created_at, last_used_at, meta_json
                FROM tenant_api_keys
                WHERE tenant_id = ?
                ORDER BY created_at DESC
                """,
                (str(tenant_id).strip(),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT key_id, tenant_id, name, prefix, status, scopes_json, created_at, last_used_at, meta_json
                FROM tenant_api_keys
                ORDER BY created_at DESC
                """
            ).fetchall()
    return [_tenant_api_key_row_to_dict(row) for row in rows]


def create_tenant_api_key(openclaw_dir, tenant_id, name, scopes=None):
    raw_key = f"slb_{secrets.token_urlsafe(24)}"
    record = _normalize_tenant_api_key(
        {
            "tenantId": tenant_id,
            "name": name,
            "rawKey": raw_key,
            "scopes": scopes or ["tenant:read", "dashboard:read", "agents:read", "tasks:read", "tasks:write"],
        }
    )
    if not record:
        raise RuntimeError("tenant API key requires tenantId and name")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO tenant_api_keys(
                key_id, tenant_id, name, key_hash, prefix, status, scopes_json, created_at, last_used_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["key_id"],
                record["tenant_id"],
                record["name"],
                record["key_hash"],
                record["prefix"],
                record["status"],
                record["scopes_json"],
                record["created_at"],
                record["last_used_at"],
                record["meta_json"],
            ),
        )
        conn.commit()
    saved = next((item for item in list_tenant_api_keys(openclaw_dir, tenant_id=tenant_id) if item["id"] == record["key_id"]), None)
    return {"rawKey": raw_key, "key": saved}


def touch_tenant_api_key(openclaw_dir, key_id):
    key_id = str(key_id or "").strip()
    if not key_id:
        return
    with _connect(openclaw_dir) as conn:
        conn.execute(
            "UPDATE tenant_api_keys SET last_used_at = ? WHERE key_id = ?",
            (now_iso(), key_id),
        )
        conn.commit()


def resolve_tenant_api_key(openclaw_dir, raw_key):
    digest = hash_api_key(raw_key)
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT key_id, tenant_id, name, prefix, status, scopes_json, created_at, last_used_at, meta_json
            FROM tenant_api_keys
            WHERE key_hash = ? AND status = 'active'
            """,
            (digest,),
        ).fetchone()
    if not row:
        return None
    return _tenant_api_key_row_to_dict(row)


def list_agent_teams(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT team_id, name, status, lead_agent_id, created_at, updated_at, meta_json
            FROM agent_teams
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_agent_team_row_to_dict(row) for row in rows]


def save_agent_team(openclaw_dir, payload):
    normalized = _normalize_agent_team(payload)
    if not normalized:
        raise RuntimeError("agent team name is required")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            "SELECT created_at FROM agent_teams WHERE team_id = ?",
            (normalized["team_id"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        conn.execute(
            """
            INSERT INTO agent_teams(
                team_id, name, status, lead_agent_id, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                name = excluded.name,
                status = excluded.status,
                lead_agent_id = excluded.lead_agent_id,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["team_id"],
                normalized["name"],
                normalized["status"],
                normalized["lead_agent_id"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_agent_teams(openclaw_dir) if item["id"] == normalized["team_id"]), None)


def list_skill_role_profiles(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                skill_slug, mode, stage, recommended_entry, output_contract_json, requires_runtime_json,
                handoff_artifacts_json, created_at, updated_at, meta_json
            FROM skill_role_profiles
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_skill_role_profile_row_to_dict(row) for row in rows]


def save_skill_role_profile(openclaw_dir, payload):
    normalized = _normalize_skill_role_profile(payload)
    if not normalized:
        raise RuntimeError("skill role profile payload is invalid")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            "SELECT created_at FROM skill_role_profiles WHERE skill_slug = ?",
            (normalized["skill_slug"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        conn.execute(
            """
            INSERT INTO skill_role_profiles(
                skill_slug, mode, stage, recommended_entry, output_contract_json, requires_runtime_json,
                handoff_artifacts_json, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(skill_slug) DO UPDATE SET
                mode = excluded.mode,
                stage = excluded.stage,
                recommended_entry = excluded.recommended_entry,
                output_contract_json = excluded.output_contract_json,
                requires_runtime_json = excluded.requires_runtime_json,
                handoff_artifacts_json = excluded.handoff_artifacts_json,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["skill_slug"],
                normalized["mode"],
                normalized["stage"],
                normalized["recommended_entry"],
                normalized["output_contract_json"],
                normalized["requires_runtime_json"],
                normalized["handoff_artifacts_json"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_skill_role_profiles(openclaw_dir) if item["skillSlug"] == normalized["skill_slug"]), None)


def list_workflow_packs(openclaw_dir):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                pack_id, name, description, status, mode, starter, default_entry, recommended_team_id,
                stages_json, skills_json, created_at, updated_at, meta_json
            FROM workflow_packs
            ORDER BY starter DESC, updated_at DESC, created_at DESC
            """
        ).fetchall()
    return [_workflow_pack_row_to_dict(row) for row in rows]


def save_workflow_pack(openclaw_dir, payload):
    normalized = _normalize_workflow_pack(payload)
    if not normalized:
        raise RuntimeError("workflow pack payload is invalid")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            "SELECT created_at FROM workflow_packs WHERE pack_id = ?",
            (normalized["pack_id"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        conn.execute(
            """
            INSERT INTO workflow_packs(
                pack_id, name, description, status, mode, starter, default_entry, recommended_team_id,
                stages_json, skills_json, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pack_id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                status = excluded.status,
                mode = excluded.mode,
                starter = excluded.starter,
                default_entry = excluded.default_entry,
                recommended_team_id = excluded.recommended_team_id,
                stages_json = excluded.stages_json,
                skills_json = excluded.skills_json,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["pack_id"],
                normalized["name"],
                normalized["description"],
                normalized["status"],
                normalized["mode"],
                normalized["starter"],
                normalized["default_entry"],
                normalized["recommended_team_id"],
                normalized["stages_json"],
                normalized["skills_json"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next((item for item in list_workflow_packs(openclaw_dir) if item["id"] == normalized["pack_id"]), None)


def list_memory_snapshots(openclaw_dir, scope="", owner_id="", limit=120):
    normalized_scope = _normalize_memory_scope(scope) if str(scope or "").strip() else ""
    normalized_owner_id = str(owner_id or "").strip()
    clauses = []
    params = []
    if normalized_scope:
        clauses.append("scope = ?")
        params.append(normalized_scope)
    if normalized_owner_id:
        clauses.append("owner_id = ?")
        params.append(normalized_owner_id)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                snapshot_key, scope, owner_id, label, summary, learning_json, notes_json,
                related_task_id, related_thread_id, created_at, updated_at, meta_json
            FROM memory_snapshots
            {where_clause}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (*params, max(int(limit or 0), 1)),
        ).fetchall()
    return [_memory_snapshot_row_to_dict(row) for row in rows]


def get_memory_snapshot(openclaw_dir, scope, owner_id):
    normalized_scope = _normalize_memory_scope(scope)
    normalized_owner_id = str(owner_id or "").strip()
    if not normalized_owner_id:
        return None
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                snapshot_key, scope, owner_id, label, summary, learning_json, notes_json,
                related_task_id, related_thread_id, created_at, updated_at, meta_json
            FROM memory_snapshots
            WHERE snapshot_key = ?
            """,
            (_memory_snapshot_key(normalized_scope, normalized_owner_id),),
        ).fetchone()
    return _memory_snapshot_row_to_dict(row) if row else None


def save_memory_snapshot(openclaw_dir, payload):
    normalized = _normalize_memory_snapshot(payload)
    if not normalized:
        raise RuntimeError("memory snapshot payload is invalid")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            "SELECT created_at FROM memory_snapshots WHERE snapshot_key = ?",
            (normalized["snapshot_key"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        conn.execute(
            """
            INSERT INTO memory_snapshots(
                snapshot_key, scope, owner_id, label, summary, learning_json, notes_json,
                related_task_id, related_thread_id, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_key) DO UPDATE SET
                label = excluded.label,
                summary = excluded.summary,
                learning_json = excluded.learning_json,
                notes_json = excluded.notes_json,
                related_task_id = excluded.related_task_id,
                related_thread_id = excluded.related_thread_id,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["snapshot_key"],
                normalized["scope"],
                normalized["owner_id"],
                normalized["label"],
                normalized["summary"],
                normalized["learning_json"],
                normalized["notes_json"],
                normalized["related_task_id"],
                normalized["related_thread_id"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return get_memory_snapshot(openclaw_dir, normalized["scope"], normalized["owner_id"])


def list_memory_events(openclaw_dir, scope="", owner_id="", related_task_id="", limit=200):
    normalized_scope = _normalize_memory_scope(scope) if str(scope or "").strip() else ""
    normalized_owner_id = str(owner_id or "").strip()
    normalized_related_task_id = str(related_task_id or "").strip()
    clauses = []
    params = []
    if normalized_scope:
        clauses.append("scope = ?")
        params.append(normalized_scope)
    if normalized_owner_id:
        clauses.append("owner_id = ?")
        params.append(normalized_owner_id)
    if normalized_related_task_id:
        clauses.append("related_task_id = ?")
        params.append(normalized_related_task_id)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                event_id, scope, owner_id, event_type, summary,
                related_task_id, related_thread_id, created_at, meta_json
            FROM memory_events
            {where_clause}
            ORDER BY created_at DESC, event_id DESC
            LIMIT ?
            """,
            (*params, max(int(limit or 0), 1)),
        ).fetchall()
    return [_memory_event_row_to_dict(row) for row in rows]


def append_memory_event(openclaw_dir, payload):
    normalized = _normalize_memory_event(payload)
    if not normalized:
        raise RuntimeError("memory event payload is invalid")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO memory_events(
                event_id, scope, owner_id, event_type, summary,
                related_task_id, related_thread_id, created_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                scope = excluded.scope,
                owner_id = excluded.owner_id,
                event_type = excluded.event_type,
                summary = excluded.summary,
                related_task_id = excluded.related_task_id,
                related_thread_id = excluded.related_thread_id,
                created_at = excluded.created_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["event_id"],
                normalized["scope"],
                normalized["owner_id"],
                normalized["event_type"],
                normalized["summary"],
                normalized["related_task_id"],
                normalized["related_thread_id"],
                normalized["created_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return normalized["event_id"]


def list_task_records(openclaw_dir, workspace_id="", workspace_path="", limit=2000):
    normalized_workspace_id = str(workspace_id or "").strip()
    normalized_workspace_path = str(workspace_path or "").strip()
    clauses = []
    params = []
    if normalized_workspace_id:
        clauses.append("workspace_id = ?")
        params.append(normalized_workspace_id)
    if normalized_workspace_path:
        clauses.append("workspace_path = ?")
        params.append(normalized_workspace_path)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                task_id, workspace_id, workspace_path, position_index, title, state, owner, org,
                created_at, updated_at, task_json
            FROM task_records
            {where_clause}
            ORDER BY workspace_id ASC, position_index ASC, updated_at DESC, created_at ASC
            LIMIT ?
            """,
            (*params, max(int(limit or 0), 1)),
        ).fetchall()
    return [_task_record_row_to_dict(row) for row in rows]


def get_task_record(openclaw_dir, task_id):
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return None
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                task_id, workspace_id, workspace_path, position_index, title, state, owner, org,
                created_at, updated_at, task_json
            FROM task_records
            WHERE task_id = ?
            """,
            (normalized_task_id,),
        ).fetchone()
    return _task_record_row_to_dict(row) if row else None


def replace_task_records_for_workspace(openclaw_dir, workspace_id, workspace_path, tasks):
    normalized_workspace_path = str(workspace_path or "").strip()
    normalized_workspace_id = str(workspace_id or "").strip() or _task_workspace_id(normalized_workspace_path)
    normalized_records = []
    task_ids = []
    for index, task in enumerate(tasks or []):
        normalized = _normalize_task_record(
            task,
            workspace_id=normalized_workspace_id,
            workspace_path=normalized_workspace_path,
            position_index=index,
        )
        if not normalized:
            continue
        normalized_records.append(normalized)
        task_ids.append(normalized["task_id"])
    with _connect(openclaw_dir) as conn:
        if task_ids:
            placeholders = ",".join("?" * len(task_ids))
            conn.execute(
                f"""
                DELETE FROM task_records
                WHERE workspace_id = ? AND task_id NOT IN ({placeholders})
                """,
                (normalized_workspace_id, *task_ids),
            )
        else:
            conn.execute(
                "DELETE FROM task_records WHERE workspace_id = ?",
                (normalized_workspace_id,),
            )
        for normalized in normalized_records:
            conn.execute(
                """
                INSERT INTO task_records(
                    task_id, workspace_id, workspace_path, position_index, title, state, owner, org,
                    created_at, updated_at, task_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    workspace_id = excluded.workspace_id,
                    workspace_path = excluded.workspace_path,
                    position_index = excluded.position_index,
                    title = excluded.title,
                    state = excluded.state,
                    owner = excluded.owner,
                    org = excluded.org,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    task_json = excluded.task_json
                """,
                (
                    normalized["task_id"],
                    normalized["workspace_id"],
                    normalized["workspace_path"],
                    normalized["position_index"],
                    normalized["title"],
                    normalized["state"],
                    normalized["owner"],
                    normalized["org"],
                    normalized["created_at"],
                    normalized["updated_at"],
                    normalized["task_json"],
                ),
            )
        conn.commit()
    return list_task_records(openclaw_dir, workspace_id=normalized_workspace_id, workspace_path=normalized_workspace_path)


def list_chat_threads(openclaw_dir, limit=120):
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                thread_id, title, status, channel, owner, primary_agent_id, current_target_agent_id,
                linked_task_id, linked_deliverable_id, linked_run_id, created_at, updated_at, meta_json
            FROM chat_threads
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()
    return [_chat_thread_row_to_dict(row) for row in rows]


def _chat_thread_filters(status="", query_text=""):
    clauses = []
    params = []
    normalized_status = str(status or "").strip().lower()
    normalized_query = str(query_text or "").strip().lower()
    if normalized_status:
        clauses.append("LOWER(status) = ?")
        params.append(normalized_status)
    if normalized_query:
        like = f"%{normalized_query}%"
        clauses.append(
            "("
            "LOWER(COALESCE(thread_id, '')) LIKE ? OR "
            "LOWER(COALESCE(title, '')) LIKE ? OR "
            "LOWER(COALESCE(status, '')) LIKE ? OR "
            "LOWER(COALESCE(channel, '')) LIKE ? OR "
            "LOWER(COALESCE(owner, '')) LIKE ? OR "
            "LOWER(COALESCE(primary_agent_id, '')) LIKE ? OR "
            "LOWER(COALESCE(current_target_agent_id, '')) LIKE ? OR "
            "LOWER(COALESCE(linked_task_id, '')) LIKE ? OR "
            "LOWER(COALESCE(linked_deliverable_id, '')) LIKE ? OR "
            "LOWER(COALESCE(linked_run_id, '')) LIKE ? OR "
            "LOWER(COALESCE(meta_json, '')) LIKE ?"
            ")"
        )
        params.extend([like] * 11)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_clause, params


def summarize_chat_threads(openclaw_dir, status="", query_text=""):
    where_clause, params = _chat_thread_filters(status=status, query_text=query_text)
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS thread_count,
                SUM(CASE WHEN LOWER(COALESCE(status, '')) = 'open' THEN 1 ELSE 0 END) AS open_count,
                SUM(CASE WHEN LOWER(COALESCE(status, '')) = 'waiting_internal' THEN 1 ELSE 0 END) AS waiting_internal_count,
                SUM(CASE WHEN LOWER(COALESCE(status, '')) = 'waiting_external' THEN 1 ELSE 0 END) AS waiting_external_count,
                SUM(CASE WHEN LOWER(COALESCE(status, '')) = 'resolved' THEN 1 ELSE 0 END) AS resolved_count
            FROM chat_threads
            {where_clause}
            """,
            tuple(params),
        ).fetchone()
    return {
        "threadCount": int(row["thread_count"] or 0) if row else 0,
        "openCount": int(row["open_count"] or 0) if row else 0,
        "waitingInternalCount": int(row["waiting_internal_count"] or 0) if row else 0,
        "waitingExternalCount": int(row["waiting_external_count"] or 0) if row else 0,
        "resolvedCount": int(row["resolved_count"] or 0) if row else 0,
    }


def list_chat_threads_page(openclaw_dir, page=1, page_size=24, status="", query_text=""):
    where_clause, params = _chat_thread_filters(status=status, query_text=query_text)
    normalized_page_size = max(int(page_size or 0), 1)
    normalized_page = max(int(page or 0), 1)
    offset = (normalized_page - 1) * normalized_page_size
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                thread_id, title, status, channel, owner, primary_agent_id, current_target_agent_id,
                linked_task_id, linked_deliverable_id, linked_run_id, created_at, updated_at, meta_json
            FROM chat_threads
            {where_clause}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ? OFFSET ?
            """,
            (*params, normalized_page_size, max(offset, 0)),
        ).fetchall()
    return [_chat_thread_row_to_dict(row) for row in rows]


def get_chat_thread(openclaw_dir, thread_id):
    """Direct O(1) lookup by thread_id — avoids loading the entire thread list."""
    thread_id = str(thread_id or "").strip()
    if not thread_id:
        return None
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                thread_id, title, status, channel, owner, primary_agent_id, current_target_agent_id,
                linked_task_id, linked_deliverable_id, linked_run_id, created_at, updated_at, meta_json
            FROM chat_threads
            WHERE thread_id = ?
            """,
            (thread_id,),
        ).fetchone()
    return _chat_thread_row_to_dict(row) if row else None


def list_chat_messages_for_threads(openclaw_dir, thread_ids, limit_per_thread=80):
    """Batch-load messages for multiple threads in a single query."""
    ids = [str(t or "").strip() for t in (thread_ids or []) if str(t or "").strip()]
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                message_id, thread_id, sender_kind, sender_id, sender_label, direction, body, created_at, meta_json
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY thread_id ORDER BY created_at ASC, message_id ASC) AS rn
                FROM chat_messages
                WHERE thread_id IN ({placeholders})
            )
            WHERE rn <= ?
            ORDER BY thread_id, created_at ASC, message_id ASC
            """,
            (*ids, max(int(limit_per_thread or 0), 1)),
        ).fetchall()
    return [_chat_message_row_to_dict(row) for row in rows]


def list_chat_thread_message_summaries(openclaw_dir, thread_ids):
    """Load one latest-message summary per thread plus the total message count."""
    ids = [str(t or "").strip() for t in (thread_ids or []) if str(t or "").strip()]
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    pending_preview_patterns = (
        "%暂未回包：%",
        "%暂未回应：%",
        "%等待回复超时%",
    )
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                thread_id,
                MAX(CASE WHEN latest_rn = 1 THEN message_id END) AS latest_message_id,
                MAX(CASE WHEN latest_rn = 1 THEN created_at END) AS latest_message_at,
                MAX(CASE WHEN preview_rn = 1 THEN body END) AS preview_body,
                MAX(message_count) AS message_count
            FROM (
                SELECT
                    thread_id,
                    message_id,
                    sender_kind,
                    body,
                    created_at,
                    COUNT(*) OVER (PARTITION BY thread_id) AS message_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY thread_id
                        ORDER BY created_at DESC, message_id DESC
                    ) AS latest_rn,
                    ROW_NUMBER() OVER (
                        PARTITION BY thread_id
                        ORDER BY
                            CASE
                                WHEN LOWER(COALESCE(sender_kind, '')) = 'system'
                                     AND (
                                        COALESCE(body, '') LIKE ?
                                        OR COALESCE(body, '') LIKE ?
                                        OR COALESCE(body, '') LIKE ?
                                     )
                                THEN 1
                                ELSE 0
                            END ASC,
                            created_at DESC,
                            message_id DESC
                    ) AS preview_rn
                FROM chat_messages
                WHERE thread_id IN ({placeholders})
            ) ranked
            GROUP BY thread_id
            ORDER BY latest_message_at DESC, latest_message_id DESC
            """,
            (*pending_preview_patterns, *ids),
        ).fetchall()
    return [
        {
            "threadId": row["thread_id"],
            "lastMessageId": row["latest_message_id"],
            "lastMessageBody": row["preview_body"],
            "lastMessageAt": row["latest_message_at"],
            "messageCount": int(row["message_count"] or 0),
        }
        for row in rows
    ]


def save_chat_thread(openclaw_dir, payload):
    normalized = _normalize_chat_thread(payload)
    if not normalized:
        raise RuntimeError("chat thread payload is invalid")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            "SELECT created_at FROM chat_threads WHERE thread_id = ?",
            (normalized["thread_id"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        conn.execute(
            """
            INSERT INTO chat_threads(
                thread_id, title, status, channel, owner, primary_agent_id, current_target_agent_id,
                linked_task_id, linked_deliverable_id, linked_run_id, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_id) DO UPDATE SET
                title = excluded.title,
                status = excluded.status,
                channel = excluded.channel,
                owner = excluded.owner,
                primary_agent_id = excluded.primary_agent_id,
                current_target_agent_id = excluded.current_target_agent_id,
                linked_task_id = excluded.linked_task_id,
                linked_deliverable_id = excluded.linked_deliverable_id,
                linked_run_id = excluded.linked_run_id,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["thread_id"],
                normalized["title"],
                normalized["status"],
                normalized["channel"],
                normalized["owner"],
                normalized["primary_agent_id"],
                normalized["current_target_agent_id"],
                normalized["linked_task_id"],
                normalized["linked_deliverable_id"],
                normalized["linked_run_id"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return get_chat_thread(openclaw_dir, normalized["thread_id"])


def delete_chat_thread(openclaw_dir, thread_id, *, delete_messages=True):
    thread_id = str(thread_id or "").strip()
    if not thread_id:
        return {"threadId": "", "deleted": False, "threadDeleted": 0, "messageDeleted": 0}
    with _connect(openclaw_dir) as conn:
        thread_row = conn.execute(
            """
            SELECT
                thread_id, title, status, channel, owner, primary_agent_id, current_target_agent_id,
                linked_task_id, linked_deliverable_id, linked_run_id, created_at, updated_at, meta_json
            FROM chat_threads
            WHERE thread_id = ?
            """,
            (thread_id,),
        ).fetchone()
        message_deleted = 0
        if delete_messages:
            message_deleted = int(
                conn.execute(
                    "DELETE FROM chat_messages WHERE thread_id = ?",
                    (thread_id,),
                ).rowcount
                or 0
            )
        thread_deleted = int(
            conn.execute(
                "DELETE FROM chat_threads WHERE thread_id = ?",
                (thread_id,),
            ).rowcount
            or 0
        )
        conn.commit()
    return {
        "threadId": thread_id,
        "deleted": bool(thread_deleted),
        "thread": _chat_thread_row_to_dict(thread_row) if thread_row else None,
        "threadDeleted": thread_deleted,
        "messageDeleted": message_deleted,
    }


def list_chat_messages(openclaw_dir, thread_id="", limit=200):
    thread_id = str(thread_id or "").strip()
    with _connect(openclaw_dir) as conn:
        if thread_id:
            rows = conn.execute(
                """
                SELECT
                    message_id, thread_id, sender_kind, sender_id, sender_label, direction, body, created_at, meta_json
                FROM chat_messages
                WHERE thread_id = ?
                ORDER BY created_at ASC, message_id ASC
                LIMIT ?
                """,
                (thread_id, max(int(limit or 0), 1)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    message_id, thread_id, sender_kind, sender_id, sender_label, direction, body, created_at, meta_json
                FROM chat_messages
                ORDER BY created_at ASC, message_id ASC
                LIMIT ?
                """,
                (max(int(limit or 0), 1),),
            ).fetchall()
    return [_chat_message_row_to_dict(row) for row in rows]


def list_recent_chat_messages(openclaw_dir, thread_id="", limit=120):
    thread_id = str(thread_id or "").strip()
    if not thread_id:
        return []
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                message_id, thread_id, sender_kind, sender_id, sender_label, direction, body, created_at, meta_json
            FROM chat_messages
            WHERE thread_id = ?
            ORDER BY created_at DESC, message_id DESC
            LIMIT ?
            """,
            (thread_id, max(int(limit or 0), 1)),
        ).fetchall()
    return [_chat_message_row_to_dict(row) for row in reversed(rows)]


def list_chat_messages_before(openclaw_dir, thread_id="", before_message_id="", limit=120):
    thread_id = str(thread_id or "").strip()
    before_message_id = str(before_message_id or "").strip()
    if not thread_id or not before_message_id:
        return []
    with _connect(openclaw_dir) as conn:
        reference = conn.execute(
            """
            SELECT created_at, message_id
            FROM chat_messages
            WHERE thread_id = ? AND message_id = ?
            """,
            (thread_id, before_message_id),
        ).fetchone()
        if not reference:
            return []
        rows = conn.execute(
            """
            SELECT
                message_id, thread_id, sender_kind, sender_id, sender_label, direction, body, created_at, meta_json
            FROM chat_messages
            WHERE thread_id = ?
              AND (
                created_at < ?
                OR (created_at = ? AND message_id < ?)
              )
            ORDER BY created_at DESC, message_id DESC
            LIMIT ?
            """,
            (
                thread_id,
                reference["created_at"],
                reference["created_at"],
                reference["message_id"],
                max(int(limit or 0), 1),
            ),
        ).fetchall()
    return [_chat_message_row_to_dict(row) for row in reversed(rows)]


def count_chat_messages_before(openclaw_dir, thread_id="", before_message_id=""):
    thread_id = str(thread_id or "").strip()
    before_message_id = str(before_message_id or "").strip()
    if not thread_id or not before_message_id:
        return 0
    with _connect(openclaw_dir) as conn:
        reference = conn.execute(
            """
            SELECT created_at, message_id
            FROM chat_messages
            WHERE thread_id = ? AND message_id = ?
            """,
            (thread_id, before_message_id),
        ).fetchone()
        if not reference:
            return 0
        row = conn.execute(
            """
            SELECT COUNT(*) AS message_count
            FROM chat_messages
            WHERE thread_id = ?
              AND (
                created_at < ?
                OR (created_at = ? AND message_id < ?)
              )
            """,
            (
                thread_id,
                reference["created_at"],
                reference["created_at"],
                reference["message_id"],
            ),
        ).fetchone()
    return int((row["message_count"] if row else 0) or 0)


def save_chat_message(openclaw_dir, payload):
    normalized = _normalize_chat_message(payload)
    if not normalized:
        raise RuntimeError("chat message requires threadId and body or attachments")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO chat_messages(
                message_id, thread_id, sender_kind, sender_id, sender_label, direction, body, created_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                meta_json = excluded.meta_json
            """,
            (
                normalized["message_id"],
                normalized["thread_id"],
                normalized["sender_kind"],
                normalized["sender_id"],
                normalized["sender_label"],
                normalized["direction"],
                normalized["body"],
                normalized["created_at"],
                normalized["meta_json"],
            ),
        )
        conn.execute(
            """
            UPDATE chat_threads
            SET updated_at = CASE
                WHEN updated_at IS NULL OR updated_at < ? THEN ?
                ELSE updated_at
            END
            WHERE thread_id = ?
            """,
            (normalized["created_at"], normalized["created_at"], normalized["thread_id"]),
        )
        conn.commit()
    return next((item for item in list_chat_messages(openclaw_dir, thread_id=normalized["thread_id"], limit=512) if item["id"] == normalized["message_id"]), None)
