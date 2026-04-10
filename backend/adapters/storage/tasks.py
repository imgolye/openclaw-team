#!/usr/bin/env python3
"""Task record CRUD operations."""
from __future__ import annotations

import json
from pathlib import Path

from backend.adapters.storage.connection import _connect, _adapt_sql, now_iso


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
