#!/usr/bin/env python3
"""Database connection management for OpenClaw Team."""
from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

try:
    import psycopg
    from psycopg.rows import dict_row as psycopg_dict_row
except Exception:
    psycopg = None
    psycopg_dict_row = None

try:
    import psycopg_pool
except Exception:
    psycopg_pool = None

import sys
from pathlib import Path as _Path

_backend_dir = _Path(__file__).resolve().parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))
from backend.env_utils import read_env_value as _read_env_value  # noqa: E402


_CONNECTION_POOLS: dict[str, object] = {}  # database_url -> ConnectionPool

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


import sys
from pathlib import Path as _Path

_backend_dir = _Path(__file__).resolve().parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))
from backend.env_utils import read_env_value as _read_env_value  # noqa: E402

def database_url(openclaw_dir):
    for key in DATABASE_URL_KEYS:
        value = _read_env_value(openclaw_dir, key, env_keys=(key,)).strip()
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
    def __init__(self, raw, backend_kind, on_close=None):
        self.raw = raw
        self.backend_kind = backend_kind
        self._on_close = on_close
        self._closed = False

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
        if self._closed:
            return
        try:
            if self._on_close is not None:
                self._on_close(self.raw)
            else:
                self.raw.close()
        finally:
            self._closed = True

def _connect(openclaw_dir):
    return _raw_connect(openclaw_dir)


def _open_postgres_connection(database_url):
    return psycopg.connect(database_url, row_factory=psycopg_dict_row)


def _ensure_postgres_schema_initialized(openclaw_dir, database_key):
    with SCHEMA_INIT_LOCK:
        if database_key in INITIALIZED_SCHEMA_DATABASE_URLS:
            return
        raw = _open_postgres_connection(database_key)
        try:
            conn = _StoreConnection(raw, "postgres")
            from backend.adapters.storage.schema import _ensure_schema, _ensure_legacy_migration
            _ensure_schema(conn)
            _ensure_legacy_migration(openclaw_dir, conn)
            INITIALIZED_SCHEMA_DATABASE_URLS.add(database_key)
        finally:
            raw.close()


def _raw_connect(openclaw_dir):
    """Create a raw psycopg connection with schema initialization.

    Uses a psycopg_pool.ConnectionPool (min_size=2, max_size=20) to avoid
    creating a new TCP connection on every request.
    """
    backend = storage_backend(openclaw_dir)
    if psycopg is None:
        raise RuntimeError("OpenClaw Team requires psycopg for PostgreSQL storage")
    database_key = backend["database_url"]

    _ensure_postgres_schema_initialized(openclaw_dir, database_key)

    if psycopg_pool is None:
        raw = _open_postgres_connection(database_key)
        conn = _StoreConnection(raw, "postgres")
        from backend.adapters.storage.schema import _ensure_legacy_migration
        _ensure_legacy_migration(openclaw_dir, conn)
        return conn

    # ── connection pool (one per database_url) ──
    with SCHEMA_INIT_LOCK:
        if database_key not in _CONNECTION_POOLS:
            pool = psycopg_pool.ConnectionPool(
                database_key,
                min_size=2,
                max_size=20,
                kwargs={"row_factory": psycopg_dict_row},
            )
            _CONNECTION_POOLS[database_key] = pool

    pool = _CONNECTION_POOLS[database_key]
    raw = pool.getconn()
    conn = _StoreConnection(raw, "postgres", on_close=pool.putconn)
    # lightweight per-call legacy migration check
    from backend.adapters.storage.schema import _ensure_legacy_migration
    _ensure_legacy_migration(openclaw_dir, conn)
    return conn
