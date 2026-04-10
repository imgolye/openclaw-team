#!/usr/bin/env python3
"""User, tenant, installation, and API key management."""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from pathlib import Path

from backend.adapters.storage.connection import _connect, _adapt_sql, hash_api_key, now_iso


def _normalize_username(value):
    return str(value or "").strip().lower()

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
