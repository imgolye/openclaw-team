#!/usr/bin/env python3
"""Management runs, automation rules, notifications, and alerts."""
from __future__ import annotations

import json
import secrets
from pathlib import Path

from backend.adapters.storage.connection import (
    AUTOMATION_RULE_TYPES,
    CUSTOMER_ACCESS_CHANNEL_TYPES,
    MANAGEMENT_STAGE_LABELS,
    MANAGEMENT_STAGE_ORDER,
    NOTIFICATION_CHANNEL_TYPES,
    _connect,
    _adapt_sql,
    now_iso,
)


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

def _normalize_model_provider_config(openclaw_dir, record):
    if not isinstance(record, dict):
        return None
    provider_id = str(record.get("providerId") or record.get("provider_id") or "").strip().lower()
    key_value = str(record.get("keyValue") or record.get("key_value") or record.get("apiKey") or "").strip()
    if not provider_id or not key_value:
        return None
    env_keys_input = record.get("envKeys") or record.get("env_keys") or []
    if not isinstance(env_keys_input, (list, tuple)):
        env_keys_input = []
    env_keys = []
    for item in env_keys_input:
        value = str(item or "").strip()
        if value and value not in env_keys:
            env_keys.append(value)
    if not env_keys:
        return None
    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    created_at = str(record.get("createdAt") or record.get("created_at") or now_iso()).strip()
    updated_at = str(record.get("updatedAt") or record.get("updated_at") or created_at).strip()
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "openclaw_dir": str(Path(openclaw_dir).expanduser().resolve()),
        "provider_id": provider_id,
        "provider_label": str(record.get("providerLabel") or record.get("provider_label") or provider_id).strip(),
        "key_value": key_value,
        "status": status,
        "env_keys_json": json.dumps(env_keys, ensure_ascii=False, separators=(",", ":")),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }

def _model_provider_config_row_to_dict(row):
    try:
        env_keys = json.loads(row["env_keys_json"] or "[]")
    except Exception:
        env_keys = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "providerId": row["provider_id"],
        "providerLabel": row["provider_label"],
        "keyValue": row["key_value"],
        "status": row["status"],
        "envKeys": env_keys if isinstance(env_keys, list) else [],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }

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

def _slugify(value):
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    while "--" in text:
        text = text.replace("--", "-")
    return text.strip("-")

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
    resolved_dir = str(Path(openclaw_dir).expanduser().resolve())
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            """
            SELECT
                provider_id, provider_label, key_value, status, env_keys_json, created_at, updated_at, meta_json
            FROM model_provider_configs
            WHERE openclaw_dir = ?
            ORDER BY updated_at DESC, created_at DESC
            """,
            (resolved_dir,),
        ).fetchall()
    return [_model_provider_config_row_to_dict(row) for row in rows]

def save_model_provider_config(openclaw_dir, payload):
    normalized = _normalize_model_provider_config(openclaw_dir, payload)
    if not normalized:
        raise RuntimeError("model provider config requires providerId, envKeys, and keyValue")
    existing = next(
        (
            item
            for item in list_model_provider_configs(openclaw_dir)
            if str(item.get("providerId") or "").strip().lower() == normalized["provider_id"]
        ),
        None,
    )
    if existing:
        normalized["created_at"] = str(existing.get("createdAt") or normalized["created_at"]).strip()
    with _connect(openclaw_dir) as conn:
        conn.execute(
            """
            INSERT INTO model_provider_configs(
                openclaw_dir, provider_id, provider_label, key_value, status,
                env_keys_json, created_at, updated_at, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(openclaw_dir, provider_id) DO UPDATE SET
                provider_label = excluded.provider_label,
                key_value = excluded.key_value,
                status = excluded.status,
                env_keys_json = excluded.env_keys_json,
                updated_at = excluded.updated_at,
                meta_json = excluded.meta_json
            """,
            (
                normalized["openclaw_dir"],
                normalized["provider_id"],
                normalized["provider_label"],
                normalized["key_value"],
                normalized["status"],
                normalized["env_keys_json"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["meta_json"],
            ),
        )
        conn.commit()
    return next(
        (
            item
            for item in list_model_provider_configs(openclaw_dir)
            if str(item.get("providerId") or "").strip().lower() == normalized["provider_id"]
        ),
        None,
    )

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
