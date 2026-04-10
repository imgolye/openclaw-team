#!/usr/bin/env python3
"""Workflow packs, orchestration workflows, and routing."""
from __future__ import annotations

import json
import secrets

from backend.adapters.storage.connection import (
    AGENT_TEAM_STATUSES,
    ROUTING_STRATEGY_TYPES,
    SKILL_RECOMMENDED_ENTRIES,
    SKILL_ROLE_MODES,
    SKILL_ROLE_STAGES,
    WORKFLOW_PACK_STATUSES,
    _connect,
    _adapt_sql,
    now_iso,
)
from backend.adapters.storage.memory import _clean_string_list


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
