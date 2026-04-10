#!/usr/bin/env python3
"""Chat thread and message CRUD operations."""
from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone

from backend.adapters.storage.auth import (
    _CHAT_HUMAN_NAME_MAX,
    _CHAT_LINKED_ID_MAX,
    _CHAT_PARTICIPANT_AGENTS_MAX,
    _CHAT_PARTICIPANT_HUMANS_MAX,
    _CHAT_THREAD_CHANNELS,
    _CHAT_THREAD_ID_MAX,
    _CHAT_THREAD_OWNER_MAX,
    _CHAT_THREAD_TITLE_MAX,
)
from backend.adapters.storage.connection import (
    CHAT_MESSAGE_DIRECTIONS,
    CHAT_MESSAGE_MAX_BODY_LENGTH,
    CHAT_THREAD_STATUSES,
    _connect,
    _adapt_sql,
    now_iso,
)


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
    workspace_path = str(record.get("workspacePath") or record.get("workspace_path") or meta.get("workspacePath") or "").strip()
    if workspace_path:
        meta = {**meta, "workspacePath": workspace_path}
    workspace_authorized = record.get("workspaceAuthorized") or meta.get("workspaceAuthorized")
    if workspace_authorized:
        meta = {**meta, "workspaceAuthorized": True}
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
    workspace_path = str(meta.get("workspacePath") or "").strip()
    workspace_authorized = bool(meta.get("workspaceAuthorized")) and bool(workspace_path)
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
        "workspacePath": workspace_path,
        "workspaceAuthorized": workspace_authorized,
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

def _load_chat_json(raw, default):
    if raw is None or raw == "":
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default

def _normalize_chat_context_kind(value, default="current"):
    normalized = str(value or "").strip().lower()
    return normalized or default

def _chat_payload_from_record(record, explicit_keys, excluded_keys):
    for key in explicit_keys:
        if key in record and record.get(key) is not None:
            return record.get(key)
    payload = {key: value for key, value in record.items() if key not in excluded_keys and value is not None}
    return payload or None

def _chat_summary_from_payload(payload, payload_keys, fallback):
    if isinstance(payload, dict):
        for key in payload_keys:
            value = str(payload.get(key) or "").strip()
            if value:
                return value
    elif isinstance(payload, str):
        value = payload.strip()
        if value:
            return value[:160]
    return fallback

def _normalize_chat_thread_context_snapshot(record):
    if not isinstance(record, dict):
        return None
    thread_id = str(record.get("threadId") or record.get("thread_id") or "").strip()
    if not thread_id or len(thread_id) > _CHAT_THREAD_ID_MAX:
        return None
    snapshot_kind = _normalize_chat_context_kind(record.get("snapshotKind") or record.get("snapshot_kind"), "current")
    context_payload = _chat_payload_from_record(
        record,
        ("context", "snapshot", "threadContext"),
        {
            "id",
            "threadId",
            "thread_id",
            "snapshotKey",
            "snapshot_key",
            "snapshotKind",
            "snapshot_kind",
            "summary",
            "meta",
            "createdAt",
            "created_at",
            "updatedAt",
            "updated_at",
        },
    )
    if context_payload is None:
        return None
    summary = str(record.get("summary") or record.get("label") or "").strip()
    if not summary:
        summary = _chat_summary_from_payload(
            context_payload,
            ("summary", "threadSummary", "currentFocus", "title", "label"),
            "",
        )
    if not summary:
        summary = snapshot_kind
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    return {
        "snapshot_key": f"{thread_id}:{snapshot_kind}",
        "thread_id": thread_id,
        "snapshot_kind": snapshot_kind,
        "summary": summary,
        "context_json": json.dumps(context_payload, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }

def _chat_thread_context_snapshot_row_to_dict(row):
    return {
        "snapshotKey": row["snapshot_key"],
        "threadId": row["thread_id"],
        "snapshotKind": row["snapshot_kind"],
        "summary": row["summary"],
        "context": _load_chat_json(row["context_json"], {}),
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": _load_chat_json(row["meta_json"], {}),
    }

def _normalize_chat_thread_context_segment(record):
    if not isinstance(record, dict):
        return None
    thread_id = str(record.get("threadId") or record.get("thread_id") or "").strip()
    if not thread_id or len(thread_id) > _CHAT_THREAD_ID_MAX:
        return None
    segment_kind = _normalize_chat_context_kind(record.get("segmentKind") or record.get("segment_kind"), "context")
    segment_order_raw = record.get("segmentOrder") if record.get("segmentOrder") is not None else record.get("segment_order")
    try:
        segment_order = int(segment_order_raw if segment_order_raw is not None else 0)
    except Exception:
        segment_order = 0
    segment_payload = _chat_payload_from_record(
        record,
        ("segment", "content", "payload", "data"),
        {
            "id",
            "segmentId",
            "segment_id",
            "threadId",
            "thread_id",
            "segmentKind",
            "segment_kind",
            "segmentOrder",
            "segment_order",
            "summary",
            "meta",
            "createdAt",
            "created_at",
            "updatedAt",
            "updated_at",
        },
    )
    if segment_payload is None:
        return None
    summary = str(record.get("summary") or record.get("label") or "").strip()
    if not summary:
        summary = _chat_summary_from_payload(segment_payload, ("summary", "title", "label"), "")
    if not summary:
        summary = segment_kind
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    segment_id = str(
        record.get("segmentId")
        or record.get("segment_id")
        or record.get("id")
        or secrets.token_hex(8)
    ).strip()
    return {
        "segment_id": segment_id,
        "thread_id": thread_id,
        "segment_kind": segment_kind,
        "segment_order": segment_order,
        "summary": summary,
        "segment_json": json.dumps(segment_payload, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }

def _chat_thread_context_segment_row_to_dict(row):
    return {
        "segmentId": row["segment_id"],
        "threadId": row["thread_id"],
        "segmentKind": row["segment_kind"],
        "segmentOrder": int(row["segment_order"] or 0),
        "summary": row["summary"],
        "segment": _load_chat_json(row["segment_json"], {}),
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": _load_chat_json(row["meta_json"], {}),
    }

def _normalize_chat_thread_event(record):
    if not isinstance(record, dict):
        return None
    thread_id = str(record.get("threadId") or record.get("thread_id") or "").strip()
    if not thread_id or len(thread_id) > _CHAT_THREAD_ID_MAX:
        return None
    event_type = _normalize_chat_context_kind(record.get("eventType") or record.get("event_type"), "update")
    event_payload = _chat_payload_from_record(
        record,
        ("event", "payload", "data", "details"),
        {
            "id",
            "eventId",
            "event_id",
            "threadId",
            "thread_id",
            "eventType",
            "event_type",
            "summary",
            "message",
            "label",
            "meta",
            "createdAt",
            "created_at",
        },
    )
    summary = str(record.get("summary") or record.get("message") or record.get("label") or "").strip()
    if not summary:
        summary = _chat_summary_from_payload(event_payload, ("summary", "message", "title", "label"), "")
    if not summary:
        summary = event_type
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    event_id = str(
        record.get("eventId")
        or record.get("event_id")
        or record.get("id")
        or secrets.token_hex(8)
    ).strip()
    return {
        "event_id": event_id,
        "thread_id": thread_id,
        "event_type": event_type,
        "summary": summary,
        "event_json": json.dumps(event_payload if event_payload is not None else {}, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }

def _chat_thread_event_row_to_dict(row):
    return {
        "eventId": row["event_id"],
        "threadId": row["thread_id"],
        "eventType": row["event_type"],
        "summary": row["summary"],
        "event": _load_chat_json(row["event_json"], {}),
        "createdAt": row["created_at"],
        "meta": _load_chat_json(row["meta_json"], {}),
    }

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


def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def update_chat_thread_last_read(openclaw_dir, thread_id, actor_id, message_id):
    """Persist last-read position in thread meta so other clients can query it."""
    thread_id = str(thread_id or "").strip()
    message_id = str(message_id or "").strip()
    actor_id = str(actor_id or "").strip()
    if not thread_id or not message_id:
        return False
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            "SELECT meta_json FROM chat_threads WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        if not row:
            return False
        meta = {}
        raw = row["meta_json"]
        if isinstance(raw, str) and raw.strip():
            try:
                meta = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                meta = {}
        if not isinstance(meta, dict):
            meta = {}
        reads = meta.setdefault("lastReadByUser", {})
        reads[actor_id or "__anonymous__"] = {"messageId": message_id, "ts": _utcnow_iso()}
        conn.execute(
            "UPDATE chat_threads SET meta_json = ?, updated_at = ? WHERE thread_id = ?",
            (json.dumps(meta, ensure_ascii=False), _utcnow_iso(), thread_id),
        )
        conn.commit()
    return True


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
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                thread_id,
                message_id,
                body,
                created_at,
                message_count
            FROM (
                SELECT
                    thread_id,
                    message_id,
                    body,
                    created_at,
                    COUNT(*) OVER (PARTITION BY thread_id) AS message_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY thread_id
                        ORDER BY created_at DESC, message_id DESC
                    ) AS rn
                FROM chat_messages
                WHERE thread_id IN ({placeholders})
            ) ranked
            WHERE rn = 1
            ORDER BY created_at DESC, message_id DESC
            """,
            ids,
        ).fetchall()
    return [
        {
            "threadId": row["thread_id"],
            "lastMessageId": row["message_id"],
            "lastMessageBody": row["body"],
            "lastMessageAt": row["created_at"],
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

def get_chat_thread_context_snapshot(openclaw_dir, thread_id, snapshot_kind="current"):
    thread_id = str(thread_id or "").strip()
    snapshot_kind = _normalize_chat_context_kind(snapshot_kind, "current")
    if not thread_id:
        return None
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                snapshot_key, thread_id, snapshot_kind, summary, context_json, created_at, updated_at, meta_json
            FROM chat_thread_context_snapshots
            WHERE thread_id = ? AND snapshot_kind = ?
            """,
            (thread_id, snapshot_kind),
        ).fetchone()
    return _chat_thread_context_snapshot_row_to_dict(row) if row else None

def save_chat_thread_context_snapshot(openclaw_dir, payload):
    normalized = _normalize_chat_thread_context_snapshot(payload)
    if not normalized:
        raise RuntimeError("chat thread context snapshot payload is invalid")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            "SELECT created_at FROM chat_thread_context_snapshots WHERE snapshot_key = ?",
            (normalized["snapshot_key"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        conn.execute(
            """
            INSERT INTO chat_thread_context_snapshots(
                snapshot_key, thread_id, snapshot_kind, summary, context_json, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_key) DO UPDATE SET
                summary = excluded.summary,
                context_json = excluded.context_json,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["snapshot_key"],
                normalized["thread_id"],
                normalized["snapshot_kind"],
                normalized["summary"],
                normalized["context_json"],
                normalized["created_at"],
                normalized["updated_at"],
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
            (normalized["updated_at"], normalized["updated_at"], normalized["thread_id"]),
        )
        conn.commit()
    return get_chat_thread_context_snapshot(openclaw_dir, normalized["thread_id"], normalized["snapshot_kind"])

def list_chat_thread_context_segments(openclaw_dir, thread_id="", segment_kind="", limit=200):
    thread_id = str(thread_id or "").strip()
    segment_kind = _normalize_chat_context_kind(segment_kind, "") if str(segment_kind or "").strip() else ""
    clauses = []
    params = []
    if thread_id:
        clauses.append("thread_id = ?")
        params.append(thread_id)
    if segment_kind:
        clauses.append("segment_kind = ?")
        params.append(segment_kind)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                segment_id, thread_id, segment_kind, segment_order, summary, segment_json, created_at, updated_at, meta_json
            FROM chat_thread_context_segments
            {where_clause}
            ORDER BY segment_order ASC, created_at ASC, segment_id ASC
            LIMIT ?
            """,
            (*params, max(int(limit or 0), 1)),
        ).fetchall()
    return [_chat_thread_context_segment_row_to_dict(row) for row in rows]

def save_chat_thread_context_segment(openclaw_dir, payload):
    normalized = _normalize_chat_thread_context_segment(payload)
    if not normalized:
        raise RuntimeError("chat thread context segment payload is invalid")
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            "SELECT created_at FROM chat_thread_context_segments WHERE segment_id = ?",
            (normalized["segment_id"],),
        ).fetchone()
        if existing:
            normalized["created_at"] = existing["created_at"]
        conn.execute(
            """
            INSERT INTO chat_thread_context_segments(
                segment_id, thread_id, segment_kind, segment_order, summary, segment_json, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(segment_id) DO UPDATE SET
                segment_kind = excluded.segment_kind,
                segment_order = excluded.segment_order,
                summary = excluded.summary,
                segment_json = excluded.segment_json,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["segment_id"],
                normalized["thread_id"],
                normalized["segment_kind"],
                normalized["segment_order"],
                normalized["summary"],
                normalized["segment_json"],
                normalized["created_at"],
                normalized["updated_at"],
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
            (normalized["updated_at"], normalized["updated_at"], normalized["thread_id"]),
        )
        conn.commit()
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            """
            SELECT
                segment_id, thread_id, segment_kind, segment_order, summary, segment_json, created_at, updated_at, meta_json
            FROM chat_thread_context_segments
            WHERE segment_id = ?
            """,
            (normalized["segment_id"],),
        ).fetchone()
    return _chat_thread_context_segment_row_to_dict(row) if row else None

def append_chat_thread_event(openclaw_dir, payload):
    normalized = _normalize_chat_thread_event(payload)
    if not normalized:
        raise RuntimeError("chat thread event payload is invalid")
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO chat_thread_events(
                event_id, thread_id, event_type, summary, event_json, created_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                thread_id = excluded.thread_id,
                event_type = excluded.event_type,
                summary = excluded.summary,
                event_json = excluded.event_json,
                created_at = excluded.created_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["event_id"],
                normalized["thread_id"],
                normalized["event_type"],
                normalized["summary"],
                normalized["event_json"],
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
    return normalized["event_id"]

def list_chat_thread_events(openclaw_dir, thread_id="", event_type="", limit=200):
    thread_id = str(thread_id or "").strip()
    event_type = _normalize_chat_context_kind(event_type, "") if str(event_type or "").strip() else ""
    clauses = []
    params = []
    if thread_id:
        clauses.append("thread_id = ?")
        params.append(thread_id)
    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT
                event_id, thread_id, event_type, summary, event_json, created_at, meta_json
            FROM chat_thread_events
            {where_clause}
            ORDER BY created_at DESC, event_id DESC
            LIMIT ?
            """,
            (*params, max(int(limit or 0), 1)),
        ).fetchall()
    return [_chat_thread_event_row_to_dict(row) for row in rows]
