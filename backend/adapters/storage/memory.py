#!/usr/bin/env python3
"""Memory snapshot and event storage."""
from __future__ import annotations

import json
import secrets

from backend.adapters.storage.connection import MEMORY_SCOPES, _connect, _adapt_sql, now_iso


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
