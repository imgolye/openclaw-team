#!/usr/bin/env python3
"""Computer use storage helpers."""
from __future__ import annotations

import json
import secrets

from backend.adapters.storage.connection import _connect, now_iso


def _prefixed_id(prefix):
    return f"{prefix}{secrets.token_hex(8)}"


def _json_dump(value, default):
    payload = value if value is not None else default
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_load(value, default):
    try:
        return json.loads(value or json.dumps(default, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        return default


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


def _normalized_status(value, allowed, default):
    text = _normalized_text(value, default)
    return text if text in allowed else default


def _normalized_int(value, default=0, minimum=0):
    try:
        normalized = int(value if value is not None else default)
    except (TypeError, ValueError):
        normalized = int(default)
    return max(minimum, normalized)


def _normalized_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalized_list(values):
    cleaned = []
    for item in values if isinstance(values, list) else []:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _upsert_row(openclaw_dir, table_name, key_field, row):
    row = row if isinstance(row, dict) else {}
    key_value = _normalized_text(row.get(key_field))
    if not key_value:
        raise RuntimeError(f"{table_name} payload is missing {key_field}")
    columns = list(row.keys())
    if key_field not in columns:
        columns.insert(0, key_field)
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(f"{column} = excluded.{column}" for column in columns if column != key_field)
    with _connect(openclaw_dir) as conn:
        existing = conn.execute(
            f"SELECT created_at FROM {table_name} WHERE {key_field} = ?",
            (key_value,),
        ).fetchone()
        if existing and "created_at" in row:
            row["created_at"] = existing["created_at"]
        conn.execute(
            f"""
            INSERT INTO {table_name}({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT({key_field}) DO UPDATE SET
                {updates}
            """,
            tuple(row[column] for column in columns),
        )
    return _get_row(openclaw_dir, table_name, key_field, key_value)


def _get_row(openclaw_dir, table_name, key_field, key_value):
    normalized = _normalized_text(key_value)
    if not normalized:
        return None
    with _connect(openclaw_dir) as conn:
        row = conn.execute(
            f"SELECT * FROM {table_name} WHERE {key_field} = ?",
            (normalized,),
        ).fetchone()
    return dict(row) if row else None


def _list_rows(openclaw_dir, table_name, where_clause="", params=(), order_by="updated_at DESC, created_at DESC", limit=200):
    limit_value = max(int(limit or 0), 1)
    with _connect(openclaw_dir) as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM {table_name}
            {where_clause}
            ORDER BY {order_by}
            LIMIT ?
            """,
            (*params, limit_value),
        ).fetchall()
    return [dict(row) for row in rows]


def _device_row_to_dict(row):
    if not row:
        return None
    try:
        capabilities = json.loads(row["capabilities_json"] or "[]")
    except Exception:
        capabilities = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["device_id"],
        "tenantId": row["tenant_id"],
        "name": row["name"],
        "kind": row["kind"],
        "osFamily": row["os_family"],
        "status": row["status"],
        "enrollmentStatus": row["enrollment_status"],
        "authMode": row["auth_mode"],
        "capabilities": capabilities if isinstance(capabilities, list) else [],
        "lastSeenAt": row["last_seen_at"],
        "lastHeartbeatAt": row["last_heartbeat_at"],
        "leaseOwner": row["lease_owner"],
        "leaseExpiresAt": row["lease_expires_at"],
        "policyId": row["policy_id"],
        "executorVersion": row["executor_version"],
        "protocolVersion": row["protocol_version"],
        "minControlPlaneVersion": row["min_control_plane_version"],
        "publicKeyFingerprint": row["public_key_fingerprint"],
        "revokedAt": row["revoked_at"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_device(record):
    if not isinstance(record, dict):
        return None
    device_id = _normalized_text(record.get("id") or record.get("deviceId") or record.get("device_id"))
    if not device_id:
        device_id = _prefixed_id("cdev_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "device_id": device_id,
        "tenant_id": _normalized_text(record.get("tenantId") or record.get("tenant_id")),
        "name": _normalized_text(record.get("name")),
        "kind": _normalized_status(record.get("kind"), {"browser_vm", "desktop_agent", "workstation"}, "browser_vm"),
        "os_family": _normalized_text(record.get("osFamily") or record.get("os_family"), "unknown"),
        "status": _normalized_status(record.get("status"), {"active", "offline", "draining", "disabled"}, "active"),
        "enrollment_status": _normalized_status(record.get("enrollmentStatus") or record.get("enrollment_status"), {"pending", "enrolled", "revoked"}, "pending"),
        "auth_mode": _normalized_status(record.get("authMode") or record.get("auth_mode"), {"token", "cert", "local"}, "token"),
        "capabilities_json": _json_dump(_normalized_list(record.get("capabilities") or record.get("capabilitiesJson") or []), []),
        "last_seen_at": _normalized_text(record.get("lastSeenAt") or record.get("last_seen_at"), ""),
        "last_heartbeat_at": _normalized_text(record.get("lastHeartbeatAt") or record.get("last_heartbeat_at"), ""),
        "lease_owner": _normalized_text(record.get("leaseOwner") or record.get("lease_owner"), ""),
        "lease_expires_at": _normalized_text(record.get("leaseExpiresAt") or record.get("lease_expires_at"), ""),
        "policy_id": _normalized_text(record.get("policyId") or record.get("policy_id"), ""),
        "executor_version": _normalized_text(record.get("executorVersion") or record.get("executor_version"), ""),
        "protocol_version": _normalized_text(record.get("protocolVersion") or record.get("protocol_version"), ""),
        "min_control_plane_version": _normalized_text(record.get("minControlPlaneVersion") or record.get("min_control_plane_version"), ""),
        "public_key_fingerprint": _normalized_text(record.get("publicKeyFingerprint") or record.get("public_key_fingerprint"), ""),
        "revoked_at": _normalized_text(record.get("revokedAt") or record.get("revoked_at"), ""),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_device(openclaw_dir, payload):
    normalized = _normalize_computer_device(payload)
    if not normalized:
        raise RuntimeError("computer device payload is invalid")
    _upsert_row(openclaw_dir, "computer_devices", "device_id", normalized)
    return get_computer_device(openclaw_dir, normalized["device_id"])


def create_computer_device(openclaw_dir, payload):
    return save_computer_device(openclaw_dir, payload)


def update_computer_device(openclaw_dir, payload):
    return save_computer_device(openclaw_dir, payload)


def get_computer_device(openclaw_dir, device_id):
    row = _get_row(openclaw_dir, "computer_devices", "device_id", device_id)
    return _device_row_to_dict(row)


def list_computer_devices(openclaw_dir, tenant_id="", status="", limit=200):
    clauses = []
    params = []
    tenant_value = _normalized_text(tenant_id)
    status_value = _normalized_text(status)
    if tenant_value:
        clauses.append("tenant_id = ?")
        params.append(tenant_value)
    if status_value:
        clauses.append("status = ?")
        params.append(status_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_device_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_devices", where_clause, params, limit=limit)]


def _credential_row_to_dict(row):
    if not row:
        return None
    try:
        allowed_domains = json.loads(row["allowed_domains_json"] or "[]")
    except Exception:
        allowed_domains = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["credential_id"],
        "tenantId": row["tenant_id"],
        "name": row["name"],
        "provider": row["provider"],
        "scope": row["scope"],
        "secretRef": row["secret_ref"],
        "status": row["status"],
        "allowedDomains": allowed_domains if isinstance(allowed_domains, list) else [],
        "rotationAt": row["rotation_at"],
        "lastRotatedAt": row["last_rotated_at"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_credential(record):
    if not isinstance(record, dict):
        return None
    credential_id = _normalized_text(record.get("id") or record.get("credentialId") or record.get("credential_id"))
    if not credential_id:
        credential_id = _prefixed_id("ccred_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "credential_id": credential_id,
        "tenant_id": _normalized_text(record.get("tenantId") or record.get("tenant_id")),
        "name": _normalized_text(record.get("name")),
        "provider": _normalized_text(record.get("provider"), "local"),
        "scope": _normalized_text(record.get("scope"), "browser"),
        "secret_ref": _normalized_text(record.get("secretRef") or record.get("secret_ref"), ""),
        "status": _normalized_status(record.get("status"), {"active", "disabled", "expired"}, "active"),
        "allowed_domains_json": _json_dump(_normalized_list(record.get("allowedDomains") or record.get("allowed_domains") or []), []),
        "rotation_at": _normalized_text(record.get("rotationAt") or record.get("rotation_at"), ""),
        "last_rotated_at": _normalized_text(record.get("lastRotatedAt") or record.get("last_rotated_at"), ""),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_credential(openclaw_dir, payload):
    normalized = _normalize_computer_credential(payload)
    if not normalized:
        raise RuntimeError("computer credential payload is invalid")
    _upsert_row(openclaw_dir, "computer_credentials", "credential_id", normalized)
    return get_computer_credential(openclaw_dir, normalized["credential_id"])


def create_computer_credential(openclaw_dir, payload):
    return save_computer_credential(openclaw_dir, payload)


def update_computer_credential(openclaw_dir, payload):
    return save_computer_credential(openclaw_dir, payload)


def get_computer_credential(openclaw_dir, credential_id):
    row = _get_row(openclaw_dir, "computer_credentials", "credential_id", credential_id)
    return _credential_row_to_dict(row)


def list_computer_credentials(openclaw_dir, tenant_id="", status="", limit=200):
    clauses = []
    params = []
    tenant_value = _normalized_text(tenant_id)
    status_value = _normalized_text(status)
    if tenant_value:
        clauses.append("tenant_id = ?")
        params.append(tenant_value)
    if status_value:
        clauses.append("status = ?")
        params.append(status_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_credential_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_credentials", where_clause, params, limit=limit)]


def _policy_row_to_dict(row):
    if not row:
        return None
    try:
        allowed_domains = json.loads(row["allowed_domains_json"] or "[]")
    except Exception:
        allowed_domains = []
    try:
        allowed_apps = json.loads(row["allowed_apps_json"] or "[]")
    except Exception:
        allowed_apps = []
    try:
        download_path_rules = json.loads(row["download_path_rules_json"] or "[]")
    except Exception:
        download_path_rules = []
    try:
        requires_confirmation = json.loads(row["requires_confirmation_json"] or "[]")
    except Exception:
        requires_confirmation = []
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["policy_id"],
        "tenantId": row["tenant_id"],
        "name": row["name"],
        "status": row["status"],
        "allowedDomains": allowed_domains if isinstance(allowed_domains, list) else [],
        "allowedApps": allowed_apps if isinstance(allowed_apps, list) else [],
        "downloadPathRules": download_path_rules if isinstance(download_path_rules, list) else [],
        "requiresConfirmation": requires_confirmation if isinstance(requires_confirmation, list) else [],
        "profileStrategy": row["profile_strategy"],
        "maxRuntimeSeconds": row["max_runtime_seconds"],
        "maxActions": row["max_actions"],
        "artifactRetentionDays": row["artifact_retention_days"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_policy(record):
    if not isinstance(record, dict):
        return None
    policy_id = _normalized_text(record.get("id") or record.get("policyId") or record.get("policy_id"))
    if not policy_id:
        policy_id = _prefixed_id("cpol_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "policy_id": policy_id,
        "tenant_id": _normalized_text(record.get("tenantId") or record.get("tenant_id")),
        "name": _normalized_text(record.get("name")),
        "status": _normalized_status(record.get("status"), {"active", "disabled", "draft"}, "active"),
        "allowed_domains_json": _json_dump(_normalized_list(record.get("allowedDomains") or record.get("allowed_domains") or []), []),
        "allowed_apps_json": _json_dump(_normalized_list(record.get("allowedApps") or record.get("allowed_apps") or []), []),
        "download_path_rules_json": _json_dump(record.get("downloadPathRules") or record.get("download_path_rules") or [], []),
        "requires_confirmation_json": _json_dump(_normalized_list(record.get("requiresConfirmation") or record.get("requires_confirmation") or []), []),
        "profile_strategy": _normalized_text(record.get("profileStrategy") or record.get("profile_strategy"), "ephemeral_per_run"),
        "max_runtime_seconds": _normalized_int(record.get("maxRuntimeSeconds") or record.get("max_runtime_seconds"), 1200, minimum=1),
        "max_actions": _normalized_int(record.get("maxActions") or record.get("max_actions"), 200, minimum=1),
        "artifact_retention_days": _normalized_int(record.get("artifactRetentionDays") or record.get("artifact_retention_days"), 30, minimum=0),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_policy(openclaw_dir, payload):
    normalized = _normalize_computer_policy(payload)
    if not normalized:
        raise RuntimeError("computer policy payload is invalid")
    _upsert_row(openclaw_dir, "computer_policies", "policy_id", normalized)
    return get_computer_policy(openclaw_dir, normalized["policy_id"])


def create_computer_policy(openclaw_dir, payload):
    return save_computer_policy(openclaw_dir, payload)


def update_computer_policy(openclaw_dir, payload):
    return save_computer_policy(openclaw_dir, payload)


def get_computer_policy(openclaw_dir, policy_id):
    row = _get_row(openclaw_dir, "computer_policies", "policy_id", policy_id)
    return _policy_row_to_dict(row)


def list_computer_policies(openclaw_dir, tenant_id="", status="", limit=200):
    clauses = []
    params = []
    tenant_value = _normalized_text(tenant_id)
    status_value = _normalized_text(status)
    if tenant_value:
        clauses.append("tenant_id = ?")
        params.append(tenant_value)
    if status_value:
        clauses.append("status = ?")
        params.append(status_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_policy_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_policies", where_clause, params, limit=limit)]


def _run_row_to_dict(row):
    if not row:
        return None
    try:
        plan = json.loads(row["plan_json"] or "{}")
    except Exception:
        plan = {}
    try:
        summary = json.loads(row["summary_json"] or "{}")
    except Exception:
        summary = {}
    try:
        recovery_state = json.loads(row["recovery_state_json"] or "{}")
    except Exception:
        recovery_state = {}
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["run_id"],
        "tenantId": row["tenant_id"],
        "deviceId": row["device_id"],
        "taskId": row["task_id"],
        "threadId": row["thread_id"],
        "planVersion": row["plan_version"],
        "objective": row["objective"],
        "mode": row["mode"],
        "status": row["status"],
        "riskLevel": row["risk_level"],
        "idempotencyKey": row["idempotency_key"],
        "currentStepId": row["current_step_id"],
        "needsHelpReason": row["needs_help_reason"],
        "clarificationReason": row["clarification_reason"],
        "leaseEpoch": row["lease_epoch"],
        "recoveryState": recovery_state,
        "plan": plan,
        "summary": summary,
        "startedAt": row["started_at"],
        "finishedAt": row["finished_at"],
        "createdBy": row["created_by"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_run(record):
    if not isinstance(record, dict):
        return None
    run_id = _normalized_text(record.get("id") or record.get("runId") or record.get("run_id"))
    if not run_id:
        run_id = _prefixed_id("crun_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    plan = record.get("plan") if isinstance(record.get("plan"), dict) else {}
    summary = record.get("summary") if isinstance(record.get("summary"), dict) else {}
    recovery_state = record.get("recoveryState") if isinstance(record.get("recoveryState"), dict) else {}
    return {
        "run_id": run_id,
        "tenant_id": _normalized_text(record.get("tenantId") or record.get("tenant_id")),
        "device_id": _normalized_text(record.get("deviceId") or record.get("device_id")),
        "task_id": _normalized_text(record.get("taskId") or record.get("task_id")),
        "thread_id": _normalized_text(record.get("threadId") or record.get("thread_id")),
        "plan_version": _normalized_text(record.get("planVersion") or record.get("plan_version"), "v1"),
        "objective": _normalized_text(record.get("objective")),
        "mode": _normalized_status(record.get("mode"), {"auto", "semi_auto", "manual"}, "semi_auto"),
        "status": _normalized_status(record.get("status"), {"queued", "planning", "claiming_device", "clarifying", "running", "paused", "needs_help", "takeover", "resuming", "succeeded", "failed", "canceled"}, "queued"),
        "risk_level": _normalized_status(record.get("riskLevel") or record.get("risk_level"), {"low", "medium", "high"}, "medium"),
        "idempotency_key": _normalized_text(record.get("idempotencyKey") or record.get("idempotency_key"), _prefixed_id("key_")),
        "current_step_id": _normalized_text(record.get("currentStepId") or record.get("current_step_id"), ""),
        "needs_help_reason": _normalized_text(record.get("needsHelpReason") or record.get("needs_help_reason"), ""),
        "clarification_reason": _normalized_text(record.get("clarificationReason") or record.get("clarification_reason"), ""),
        "lease_epoch": _normalized_int(record.get("leaseEpoch") or record.get("lease_epoch"), 0, minimum=0),
        "recovery_state_json": _json_dump(recovery_state, {}),
        "plan_json": _json_dump(plan, {}),
        "summary_json": _json_dump(summary, {}),
        "started_at": _normalized_text(record.get("startedAt") or record.get("started_at"), created_at),
        "finished_at": _normalized_text(record.get("finishedAt") or record.get("finished_at"), ""),
        "created_by": _normalized_text(record.get("createdBy") or record.get("created_by"), ""),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_run(openclaw_dir, payload):
    normalized = _normalize_computer_run(payload)
    if not normalized:
        raise RuntimeError("computer run payload is invalid")
    _upsert_row(openclaw_dir, "computer_runs", "run_id", normalized)
    return get_computer_run(openclaw_dir, normalized["run_id"])


def create_computer_run(openclaw_dir, payload):
    return save_computer_run(openclaw_dir, payload)


def update_computer_run(openclaw_dir, payload):
    return save_computer_run(openclaw_dir, payload)


def get_computer_run(openclaw_dir, run_id):
    row = _get_row(openclaw_dir, "computer_runs", "run_id", run_id)
    return _run_row_to_dict(row)


def list_computer_runs(openclaw_dir, tenant_id="", device_id="", thread_id="", status="", limit=100):
    clauses = []
    params = []
    tenant_value = _normalized_text(tenant_id)
    device_value = _normalized_text(device_id)
    thread_value = _normalized_text(thread_id)
    status_value = _normalized_text(status)
    if tenant_value:
        clauses.append("tenant_id = ?")
        params.append(tenant_value)
    if device_value:
        clauses.append("device_id = ?")
        params.append(device_value)
    if thread_value:
        clauses.append("thread_id = ?")
        params.append(thread_value)
    if status_value:
        clauses.append("status = ?")
        params.append(status_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_run_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_runs", where_clause, params, limit=limit)]


def _step_row_to_dict(row):
    if not row:
        return None
    try:
        checkpoint = json.loads(row["checkpoint_json"] or "{}")
    except Exception:
        checkpoint = {}
    try:
        observation = json.loads(row["observation_summary_json"] or "{}")
    except Exception:
        observation = {}
    try:
        verification = json.loads(row["verification_summary_json"] or "{}")
    except Exception:
        verification = {}
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["step_id"],
        "runId": row["run_id"],
        "stepKey": row["step_key"],
        "intent": row["intent"],
        "status": row["status"],
        "attemptCount": row["attempt_count"],
        "budgetMs": row["budget_ms"],
        "actionCount": row["action_count"],
        "errorCode": row["error_code"],
        "errorMessage": row["error_message"],
        "checkpoint": checkpoint,
        "observationSummary": observation,
        "verificationSummary": verification,
        "startedAt": row["started_at"],
        "finishedAt": row["finished_at"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_step(record):
    if not isinstance(record, dict):
        return None
    step_id = _normalized_text(record.get("id") or record.get("stepId") or record.get("step_id"))
    if not step_id:
        step_id = _prefixed_id("cstep_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "step_id": step_id,
        "run_id": _normalized_text(record.get("runId") or record.get("run_id")),
        "step_key": _normalized_text(record.get("stepKey") or record.get("step_key")),
        "intent": _normalized_text(record.get("intent")),
        "status": _normalized_status(record.get("status"), {"pending", "observing", "deciding", "clarifying", "acting", "verifying", "retrying", "waiting", "manual", "done", "failed"}, "pending"),
        "attempt_count": _normalized_int(record.get("attemptCount") or record.get("attempt_count"), 0, minimum=0),
        "budget_ms": _normalized_int(record.get("budgetMs") or record.get("budget_ms"), 0, minimum=0),
        "action_count": _normalized_int(record.get("actionCount") or record.get("action_count"), 0, minimum=0),
        "error_code": _normalized_text(record.get("errorCode") or record.get("error_code"), ""),
        "error_message": _normalized_text(record.get("errorMessage") or record.get("error_message"), ""),
        "checkpoint_json": _json_dump(record.get("checkpoint") if isinstance(record.get("checkpoint"), dict) else {}, {}),
        "observation_summary_json": _json_dump(record.get("observationSummary") if isinstance(record.get("observationSummary"), dict) else {}, {}),
        "verification_summary_json": _json_dump(record.get("verificationSummary") if isinstance(record.get("verificationSummary"), dict) else {}, {}),
        "started_at": _normalized_text(record.get("startedAt") or record.get("started_at"), ""),
        "finished_at": _normalized_text(record.get("finishedAt") or record.get("finished_at"), ""),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_step(openclaw_dir, payload):
    normalized = _normalize_computer_step(payload)
    if not normalized:
        raise RuntimeError("computer step payload is invalid")
    _upsert_row(openclaw_dir, "computer_steps", "step_id", normalized)
    return get_computer_step(openclaw_dir, normalized["step_id"])


def create_computer_step(openclaw_dir, payload):
    return save_computer_step(openclaw_dir, payload)


def update_computer_step(openclaw_dir, payload):
    return save_computer_step(openclaw_dir, payload)


def append_computer_step(openclaw_dir, payload):
    return save_computer_step(openclaw_dir, payload)


def get_computer_step(openclaw_dir, step_id):
    row = _get_row(openclaw_dir, "computer_steps", "step_id", step_id)
    return _step_row_to_dict(row)


def list_computer_steps(openclaw_dir, run_id="", status="", limit=200):
    clauses = []
    params = []
    run_value = _normalized_text(run_id)
    status_value = _normalized_text(status)
    if run_value:
        clauses.append("run_id = ?")
        params.append(run_value)
    if status_value:
        clauses.append("status = ?")
        params.append(status_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_step_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_steps", where_clause, params, limit=limit)]


def _action_row_to_dict(row):
    if not row:
        return None
    try:
        target = json.loads(row["target_json"] or "{}")
    except Exception:
        target = {}
    try:
        input_payload = json.loads(row["input_json"] or "{}")
    except Exception:
        input_payload = {}
    try:
        result = json.loads(row["result_json"] or "{}")
    except Exception:
        result = {}
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["action_id"],
        "runId": row["run_id"],
        "stepId": row["step_id"],
        "actorType": row["actor_type"],
        "actionKey": row["action_key"],
        "actionType": row["action_type"],
        "sideEffectLevel": row["side_effect_level"],
        "idempotencyKey": row["idempotency_key"],
        "target": target,
        "input": input_payload,
        "result": result,
        "success": bool(row["success"]),
        "latencyMs": row["latency_ms"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_action(record):
    if not isinstance(record, dict):
        return None
    action_id = _normalized_text(record.get("id") or record.get("actionId") or record.get("action_id"))
    if not action_id:
        action_id = _prefixed_id("cact_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "action_id": action_id,
        "run_id": _normalized_text(record.get("runId") or record.get("run_id")),
        "step_id": _normalized_text(record.get("stepId") or record.get("step_id")),
        "actor_type": _normalized_status(record.get("actorType") or record.get("actor_type"), {"agent", "human"}, "agent"),
        "action_key": _normalized_text(record.get("actionKey") or record.get("action_key")),
        "action_type": _normalized_text(record.get("actionType") or record.get("action_type")),
        "side_effect_level": _normalized_status(record.get("sideEffectLevel") or record.get("side_effect_level"), {"read_only", "idempotent_write", "non_idempotent", "irreversible"}, "read_only"),
        "idempotency_key": _normalized_text(record.get("idempotencyKey") or record.get("idempotency_key"), _prefixed_id("key_")),
        "target_json": _json_dump(record.get("target") if isinstance(record.get("target"), dict) else {}, {}),
        "input_json": _json_dump(record.get("input") if isinstance(record.get("input"), dict) else {}, {}),
        "result_json": _json_dump(record.get("result") if isinstance(record.get("result"), dict) else {}, {}),
        "success": 1 if _normalized_bool(record.get("success"), False) else 0,
        "latency_ms": _normalized_int(record.get("latencyMs") or record.get("latency_ms"), 0, minimum=0),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_action(openclaw_dir, payload):
    normalized = _normalize_computer_action(payload)
    if not normalized:
        raise RuntimeError("computer action payload is invalid")
    _upsert_row(openclaw_dir, "computer_actions", "action_id", normalized)
    return get_computer_action(openclaw_dir, normalized["action_id"])


def create_computer_action(openclaw_dir, payload):
    return save_computer_action(openclaw_dir, payload)


def update_computer_action(openclaw_dir, payload):
    return save_computer_action(openclaw_dir, payload)


def get_computer_action(openclaw_dir, action_id):
    row = _get_row(openclaw_dir, "computer_actions", "action_id", action_id)
    return _action_row_to_dict(row)


def list_computer_actions(openclaw_dir, run_id="", step_id="", limit=200):
    clauses = []
    params = []
    run_value = _normalized_text(run_id)
    step_value = _normalized_text(step_id)
    if run_value:
        clauses.append("run_id = ?")
        params.append(run_value)
    if step_value:
        clauses.append("step_id = ?")
        params.append(step_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_action_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_actions", where_clause, params, limit=limit)]


def _artifact_row_to_dict(row):
    if not row:
        return None
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["artifact_id"],
        "runId": row["run_id"],
        "stepId": row["step_id"],
        "type": row["type"],
        "title": row["title"],
        "path": row["path"],
        "mimeType": row["mime_type"],
        "hash": row["hash"],
        "sizeBytes": row["size_bytes"],
        "encrypted": bool(row["encrypted"]),
        "retentionPolicy": row["retention_policy"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_artifact(record):
    if not isinstance(record, dict):
        return None
    artifact_id = _normalized_text(record.get("id") or record.get("artifactId") or record.get("artifact_id"))
    if not artifact_id:
        artifact_id = _prefixed_id("cart_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "artifact_id": artifact_id,
        "run_id": _normalized_text(record.get("runId") or record.get("run_id")),
        "step_id": _normalized_text(record.get("stepId") or record.get("step_id")),
        "type": _normalized_status(record.get("type"), {"screenshot", "dom", "ocr", "download", "upload", "replay", "output"}, "output"),
        "title": _normalized_text(record.get("title"), "artifact"),
        "path": _normalized_text(record.get("path"), ""),
        "mime_type": _normalized_text(record.get("mimeType") or record.get("mime_type"), "application/octet-stream"),
        "hash": _normalized_text(record.get("hash"), ""),
        "size_bytes": _normalized_int(record.get("sizeBytes") or record.get("size_bytes"), 0, minimum=0),
        "encrypted": 1 if _normalized_bool(record.get("encrypted"), False) else 0,
        "retention_policy": _normalized_text(record.get("retentionPolicy") or record.get("retention_policy"), "default"),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_artifact(openclaw_dir, payload):
    normalized = _normalize_computer_artifact(payload)
    if not normalized:
        raise RuntimeError("computer artifact payload is invalid")
    _upsert_row(openclaw_dir, "computer_artifacts", "artifact_id", normalized)
    return get_computer_artifact(openclaw_dir, normalized["artifact_id"])


def create_computer_artifact(openclaw_dir, payload):
    return save_computer_artifact(openclaw_dir, payload)


def update_computer_artifact(openclaw_dir, payload):
    return save_computer_artifact(openclaw_dir, payload)


def get_computer_artifact(openclaw_dir, artifact_id):
    row = _get_row(openclaw_dir, "computer_artifacts", "artifact_id", artifact_id)
    return _artifact_row_to_dict(row)


def list_computer_artifacts(openclaw_dir, run_id="", step_id="", limit=200):
    clauses = []
    params = []
    run_value = _normalized_text(run_id)
    step_value = _normalized_text(step_id)
    if run_value:
        clauses.append("run_id = ?")
        params.append(run_value)
    if step_value:
        clauses.append("step_id = ?")
        params.append(step_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_artifact_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_artifacts", where_clause, params, limit=limit)]


def _takeover_row_to_dict(row):
    if not row:
        return None
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}
    return {
        "id": row["takeover_id"],
        "runId": row["run_id"],
        "startedBy": row["started_by"],
        "startReason": row["start_reason"],
        "transportMode": row["transport_mode"],
        "controllerSessionId": row["controller_session_id"],
        "startedAt": row["started_at"],
        "endedAt": row["ended_at"],
        "summary": row["summary"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "meta": meta,
    }


def _normalize_computer_takeover(record):
    if not isinstance(record, dict):
        return None
    takeover_id = _normalized_text(record.get("id") or record.get("takeoverId") or record.get("takeover_id"))
    if not takeover_id:
        takeover_id = _prefixed_id("ctake_")
    created_at = _normalized_text(record.get("createdAt") or record.get("created_at"), now_iso())
    updated_at = _normalized_text(record.get("updatedAt") or record.get("updated_at"), created_at)
    meta = record.get("meta") if isinstance(record.get("meta"), dict) else {}
    return {
        "takeover_id": takeover_id,
        "run_id": _normalized_text(record.get("runId") or record.get("run_id")),
        "started_by": _normalized_text(record.get("startedBy") or record.get("started_by"), ""),
        "start_reason": _normalized_text(record.get("startReason") or record.get("start_reason"), ""),
        "transport_mode": _normalized_text(record.get("transportMode") or record.get("transport_mode"), "preview"),
        "controller_session_id": _normalized_text(record.get("controllerSessionId") or record.get("controller_session_id"), ""),
        "started_at": _normalized_text(record.get("startedAt") or record.get("started_at"), created_at),
        "ended_at": _normalized_text(record.get("endedAt") or record.get("ended_at"), ""),
        "summary": _normalized_text(record.get("summary"), ""),
        "created_at": created_at,
        "updated_at": updated_at,
        "meta_json": _json_dump(meta, {}),
    }


def save_computer_takeover(openclaw_dir, payload):
    normalized = _normalize_computer_takeover(payload)
    if not normalized:
        raise RuntimeError("computer takeover payload is invalid")
    _upsert_row(openclaw_dir, "computer_takeovers", "takeover_id", normalized)
    return get_computer_takeover(openclaw_dir, normalized["takeover_id"])


def create_computer_takeover(openclaw_dir, payload):
    return save_computer_takeover(openclaw_dir, payload)


def update_computer_takeover(openclaw_dir, payload):
    return save_computer_takeover(openclaw_dir, payload)


def start_computer_takeover(openclaw_dir, payload):
    return save_computer_takeover(openclaw_dir, payload)


def get_computer_takeover(openclaw_dir, takeover_id):
    row = _get_row(openclaw_dir, "computer_takeovers", "takeover_id", takeover_id)
    return _takeover_row_to_dict(row)


def list_computer_takeovers(openclaw_dir, run_id="", limit=50):
    clauses = []
    params = []
    run_value = _normalized_text(run_id)
    if run_value:
        clauses.append("run_id = ?")
        params.append(run_value)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [_takeover_row_to_dict(row) for row in _list_rows(openclaw_dir, "computer_takeovers", where_clause, params, limit=limit)]


def finish_computer_takeover(openclaw_dir, payload):
    return save_computer_takeover(openclaw_dir, payload)
