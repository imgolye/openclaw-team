from __future__ import annotations

import json
import os
import secrets
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from backend.adapters.storage.computer_use import (
    append_computer_step,
    create_computer_run,
    finish_computer_takeover,
    list_computer_actions,
    get_computer_device,
    get_computer_run,
    list_computer_artifacts,
    list_computer_devices,
    list_computer_runs,
    list_computer_steps,
    list_computer_takeovers,
    save_computer_artifact,
    save_computer_device,
    start_computer_takeover,
    update_computer_run,
    update_computer_step,
)
from .computer_use_paths import build_computer_use_workspace_paths


COMPUTER_USE_RUN_STATUSES = {
    "queued",
    "planning",
    "claiming_device",
    "clarifying",
    "running",
    "paused",
    "needs_help",
    "takeover",
    "resuming",
    "succeeded",
    "failed",
    "canceled",
}
COMPUTER_USE_STEP_STATUSES = {
    "pending",
    "observing",
    "deciding",
    "clarifying",
    "acting",
    "verifying",
    "retrying",
    "waiting",
    "manual",
    "done",
    "failed",
}
COMPUTER_USE_DEFAULT_MIDDLEWARE_CHAIN = [
    "policy",
    "credential",
    "workspace",
    "planning",
    "summarization",
    "clarification",
    "execution",
    "checkpoint",
]


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_dict(value):
    return deepcopy(value) if isinstance(value, dict) else {}


def _safe_list(value):
    return deepcopy(value) if isinstance(value, list) else []


def _computer_use_source_context(payload):
    payload = payload if isinstance(payload, dict) else {}
    source = _safe_dict(payload.get("source"))
    explicit_source_keys = {
        "sourceKind",
        "sourceType",
        "sourceLabel",
        "conversationAgentId",
        "conversationSessionId",
        "conversationThreadId",
        "conversationMessageId",
        "linkedThreadId",
        "threadId",
        "sourceThreadId",
        "sourceMessageId",
        "initiatingMessageId",
        "messageId",
    }
    has_explicit_source = bool(source) or any(_normalized_text(payload.get(key)) for key in explicit_source_keys)
    if not has_explicit_source:
        return {}
    conversation = {
        "agentId": _normalized_text(
            source.get("conversationAgentId")
            or payload.get("conversationAgentId")
            or source.get("agentId")
            or payload.get("agentId"),
        ),
        "sessionId": _normalized_text(
            source.get("conversationSessionId")
            or payload.get("conversationSessionId")
            or source.get("sessionId")
            or payload.get("sessionId"),
        ),
        "threadId": _normalized_text(
            source.get("conversationThreadId")
            or payload.get("conversationThreadId")
            or source.get("threadId")
            or payload.get("conversationThreadId"),
        ),
        "messageId": _normalized_text(
            source.get("conversationMessageId")
            or payload.get("conversationMessageId")
            or source.get("messageId")
            or payload.get("initiatingMessageId")
            or payload.get("messageId"),
        ),
    }
    thread = {
        "threadId": _normalized_text(
            source.get("threadId")
            or payload.get("linkedThreadId")
            or payload.get("threadId"),
        ),
        "messageId": _normalized_text(
            source.get("threadMessageId")
            or payload.get("sourceMessageId")
            or payload.get("initiatingMessageId")
            or payload.get("messageId"),
        ),
    }
    label = _normalized_text(source.get("label") or payload.get("sourceLabel"))
    source_kind = _normalized_text(
        source.get("kind")
        or payload.get("sourceKind")
        or payload.get("sourceType")
        or "",
    ).lower()
    if not source_kind:
        if conversation["agentId"] or conversation["sessionId"] or conversation["messageId"]:
            source_kind = "conversation"
        elif thread["threadId"] or thread["messageId"]:
            source_kind = "thread"
        else:
            source_kind = "manual"
    source_meta = {"kind": source_kind}
    if label:
        source_meta["label"] = label
    if any(conversation.values()):
        source_meta["conversation"] = {key: value for key, value in conversation.items() if value}
    if any(thread.values()):
        source_meta["thread"] = {key: value for key, value in thread.items() if value}
    source_message_id = _normalized_text(
        source.get("messageId")
        or payload.get("sourceMessageId")
        or payload.get("initiatingMessageId")
        or conversation.get("messageId")
        or thread.get("messageId"),
    )
    if source_message_id:
        source_meta["messageId"] = source_message_id
    return source_meta


def _computer_use_source_summary(source_meta):
    source_meta = _safe_dict(source_meta)
    if not source_meta:
        return ""
    parts = []
    label = _normalized_text(source_meta.get("label"))
    if label:
        parts.append(label)
    kind = _normalized_text(source_meta.get("kind"))
    if kind == "conversation":
        conversation = _safe_dict(source_meta.get("conversation"))
        conversation_bits = [
            _normalized_text(conversation.get("agentId")),
            _normalized_text(conversation.get("sessionId")),
        ]
        conversation_label = "/".join([item for item in conversation_bits if item])
        if not conversation_label:
            conversation_label = _normalized_text(conversation.get("threadId")) or _normalized_text(conversation.get("messageId"))
        if conversation_label:
            parts.append(f"conversation {conversation_label}")
    elif kind == "thread":
        thread = _safe_dict(source_meta.get("thread"))
        thread_bits = [
            _normalized_text(thread.get("threadId")),
            _normalized_text(thread.get("messageId")),
        ]
        thread_label = "/".join([item for item in thread_bits if item])
        if thread_label:
            parts.append(f"thread {thread_label}")
    else:
        message_id = _normalized_text(source_meta.get("messageId"))
        if message_id:
            parts.append(message_id)
    return " · ".join([item for item in parts if item])


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


def _bounded_int(value, default, minimum=1, maximum=100):
    try:
        normalized = int(value if value is not None else default)
    except (TypeError, ValueError):
        normalized = int(default)
    return max(minimum, min(normalized, maximum))


def _resolve_computer_use_transport_template(template, **replacements):
    resolved = _normalized_text(template)
    if not resolved:
        return ""
    for key, value in replacements.items():
        resolved = resolved.replace(f"{{{key}}}", _normalized_text(value))
    return resolved


def _computer_use_vnc_ws_url(**replacements):
    proxy_path = _resolve_computer_use_transport_template(
        os.environ.get("MISSION_CONTROL_COMPUTER_USE_VNC_PROXY_PATH")
        or os.environ.get("MISSION_CONTROL_DESKTOP_VNC_PROXY_PATH"),
        **replacements,
    )
    if proxy_path:
        return proxy_path
    return _resolve_computer_use_transport_template(
        os.environ.get("MISSION_CONTROL_COMPUTER_USE_VNC_WS_URL"),
        **replacements,
    )


def _computer_use_data_dir(openclaw_dir):
    path = Path(openclaw_dir).expanduser().resolve() / "dashboard" / "computer-use"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _computer_use_run_dir(openclaw_dir, run_id):
    path = _computer_use_data_dir(openclaw_dir) / "runs" / _normalized_text(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path, payload):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _execution_environment_fingerprint(device):
    device = device if isinstance(device, dict) else {}
    meta = device.get("meta") if isinstance(device.get("meta"), dict) else {}
    fingerprint = {
        "deviceId": _normalized_text(device.get("id")),
        "kind": _normalized_text(device.get("kind")),
        "osFamily": _normalized_text(device.get("osFamily")),
        "executorVersion": _normalized_text(device.get("executorVersion")),
        "protocolVersion": _normalized_text(device.get("protocolVersion")),
        "locale": _normalized_text(meta.get("locale"), "zh-CN"),
        "timezone": _normalized_text(meta.get("timezone"), "Asia/Shanghai"),
        "viewport": meta.get("viewport") if isinstance(meta.get("viewport"), dict) else {},
        "profileStrategy": _normalized_text(meta.get("profileStrategy"), "ephemeral_per_run"),
    }
    return fingerprint


def ensure_default_computer_use_device(openclaw_dir):
    existing = get_computer_device(openclaw_dir, "local-browser")
    if existing:
        return existing
    meta = {
        "browser": {
            "engine": "playwright",
            "headless": False,
        },
        "locale": "zh-CN",
        "timezone": "Asia/Shanghai",
        "viewport": {"width": 1440, "height": 960},
        "profileStrategy": "ephemeral_per_run",
    }
    return save_computer_device(
        openclaw_dir,
        {
            "id": "local-browser",
            "tenantId": "",
            "name": "Local Browser Executor",
            "kind": "workstation",
            "osFamily": sys.platform,
            "status": "active",
            "enrollmentStatus": "enrolled",
            "authMode": "local",
            "capabilities": ["browser", "downloads", "uploads", "takeover", "artifacts"],
            "lastSeenAt": now_iso(),
            "lastHeartbeatAt": now_iso(),
            "executorVersion": "bootstrap",
            "protocolVersion": "v1",
            "minControlPlaneVersion": "v1",
            "meta": meta,
        },
    )


def _display_name_from_actor(actor):
    actor = actor if isinstance(actor, dict) else {}
    return (
        _normalized_text(actor.get("displayName"))
        or _normalized_text(actor.get("username"))
        or _normalized_text(actor.get("name"))
        or "OpenClaw Team"
    )


def build_computer_use_run_harness(openclaw_dir, run_id, device=None, actor=None):
    device = device if isinstance(device, dict) else ensure_default_computer_use_device(openclaw_dir)
    actor = actor if isinstance(actor, dict) else {}
    run_dir = _computer_use_run_dir(openclaw_dir, run_id)
    workspace_paths = build_computer_use_workspace_paths(run_dir)
    harness = {
        "runId": run_id,
        "middlewareChain": list(COMPUTER_USE_DEFAULT_MIDDLEWARE_CHAIN),
        "workspace": workspace_paths,
        "clarification": {
            "enabled": True,
            "modes": ["awaiting_credential", "awaiting_2fa", "awaiting_captcha", "awaiting_sso_redirect"],
        },
        "memory": {
            "enabled": True,
            "strategy": "runbook_learning",
        },
        "locks": {
            "uiControlLock": f"computer-use:{run_id}:ui",
            "backgroundAnalysisSlots": 2,
        },
        "createdBy": {
            "displayName": _display_name_from_actor(actor),
            "username": _normalized_text(actor.get("username")),
            "role": _normalized_text(actor.get("role")),
        },
        "executionEnvironmentFingerprint": _execution_environment_fingerprint(device),
    }
    return harness


def build_computer_use_plan(objective, device=None, policy=None):
    objective_text = _normalized_text(objective)
    if not objective_text:
        raise RuntimeError("电脑执行目标不能为空。")
    policy = policy if isinstance(policy, dict) else {}
    device = device if isinstance(device, dict) else {}
    return {
        "version": "v1",
        "plannerMode": "bootstrap",
        "objective": objective_text,
        "riskLevel": _normalized_text(policy.get("riskLevel"), "medium"),
        "manualAssistTriggers": [
            "captcha",
            "unexpected_dialog",
            "2fa",
            "sso_confirmation",
        ],
        "successCriteria": [
            "目标页面已完成关键动作",
            "关键结果已可验证",
            "若涉及文件，产物已出现在 run downloads/result 目录",
        ],
        "steps": [
            {
                "id": "prepare-environment",
                "stepKey": "prepare-environment",
                "intent": "准备执行环境并打开目标系统入口。",
                "budgetMs": 30000,
            },
            {
                "id": "execute-objective",
                "stepKey": "execute-objective",
                "intent": objective_text,
                "budgetMs": 120000,
            },
            {
                "id": "verify-result",
                "stepKey": "verify-result",
                "intent": "验证执行结果并整理关键产物。",
                "budgetMs": 45000,
            },
        ],
        "deviceProfile": {
            "deviceId": _normalized_text(device.get("id")),
            "kind": _normalized_text(device.get("kind")),
        },
    }


def _approval_state_from_run(run):
    run = run if isinstance(run, dict) else {}
    meta = _safe_dict(run.get("meta"))
    summary = _safe_dict(run.get("summary"))
    approval = _safe_dict(meta.get("approval"))
    required = bool(approval.get("required") or summary.get("approvalRequired"))
    status = _normalized_text(
        approval.get("status") or summary.get("approvalStatus"),
        "pending" if required else "not_required",
    )
    if status not in {"pending", "approved", "not_required"}:
        status = "pending" if required else "not_required"
    if not required:
        status = "not_required"
    return {
        "required": required,
        "status": status,
        "reason": _normalized_text(approval.get("reason") or summary.get("approvalReason")),
        "requestedAt": _normalized_text(approval.get("requestedAt") or summary.get("approvalRequestedAt")),
        "requestedBy": _normalized_text(approval.get("requestedBy") or summary.get("approvalRequestedBy")),
        "approvedAt": _normalized_text(approval.get("approvedAt") or summary.get("approvedAt")),
        "approvedBy": _normalized_text(approval.get("approvedBy") or summary.get("approvedBy")),
        "note": _normalized_text(approval.get("note")),
    }


def _approval_summary_fields(approval_state):
    approval_state = approval_state if isinstance(approval_state, dict) else {}
    return {
        "approvalRequired": bool(approval_state.get("required")),
        "approvalStatus": _normalized_text(approval_state.get("status"), "not_required"),
        "approvalReason": _normalized_text(approval_state.get("reason")),
        "approvalRequestedAt": _normalized_text(approval_state.get("requestedAt")),
        "approvalRequestedBy": _normalized_text(approval_state.get("requestedBy")),
        "approvedAt": _normalized_text(approval_state.get("approvedAt")),
        "approvedBy": _normalized_text(approval_state.get("approvedBy")),
    }


def _sorted_steps(items):
    return sorted(
        [item for item in items if isinstance(item, dict)],
        key=lambda item: (
            _normalized_text(item.get("createdAt")),
            _normalized_text(item.get("updatedAt")),
            _normalized_text(item.get("id")),
        ),
    )


def _latest_open_takeover(openclaw_dir, run_id):
    takeovers = [
        item
        for item in _safe_list(list_computer_takeovers(openclaw_dir, run_id=run_id, limit=50))
        if isinstance(item, dict) and not _normalized_text(item.get("endedAt"))
    ]
    takeovers.sort(key=lambda item: _normalized_text(item.get("startedAt")), reverse=True)
    return takeovers[0] if takeovers else None


def _run_summary(run, steps=None, artifacts=None, device=None):
    run = run if isinstance(run, dict) else {}
    steps = _sorted_steps(steps or [])
    artifacts = [item for item in artifacts or [] if isinstance(item, dict)]
    device = device if isinstance(device, dict) else {}
    source_meta = _safe_dict(_safe_dict(run.get("meta")).get("source"))
    current_step_id = _normalized_text(run.get("currentStepId"))
    current_step = next((item for item in steps if _normalized_text(item.get("id")) == current_step_id), None)
    if current_step is None:
        current_step = next((item for item in steps if _normalized_text(item.get("status")) not in {"done", "failed"}), None)
    summary = {
        "id": _normalized_text(run.get("id")),
        "objective": _normalized_text(run.get("objective")),
        "status": _normalized_text(run.get("status")),
        "mode": _normalized_text(run.get("mode")),
        "riskLevel": _normalized_text(run.get("riskLevel")),
        "deviceId": _normalized_text(run.get("deviceId")),
        "deviceName": _normalized_text(device.get("name")),
        "updatedAt": _normalized_text(run.get("updatedAt")),
        "createdAt": _normalized_text(run.get("createdAt")),
        "currentStepId": _normalized_text((current_step or {}).get("id") or run.get("currentStepId")),
        "currentStepStatus": _normalized_text((current_step or {}).get("status")),
        "currentStepIntent": _normalized_text((current_step or {}).get("intent")),
        "stepCount": len(steps),
        "artifactCount": len(artifacts),
        "needsHelpReason": _normalized_text(run.get("needsHelpReason")),
        "clarificationReason": _normalized_text(run.get("clarificationReason")),
    }
    if source_meta:
        conversation = _safe_dict(source_meta.get("conversation"))
        thread = _safe_dict(source_meta.get("thread"))
        summary.update(
            {
                "sourceKind": _normalized_text(source_meta.get("kind")),
                "sourceLabel": _normalized_text(source_meta.get("label")),
                "sourceConversationAgentId": _normalized_text(conversation.get("agentId")),
                "sourceConversationSessionId": _normalized_text(conversation.get("sessionId")),
                "sourceConversationThreadId": _normalized_text(conversation.get("threadId")),
                "sourceConversationMessageId": _normalized_text(conversation.get("messageId")),
                "sourceThreadId": _normalized_text(thread.get("threadId")),
                "sourceThreadMessageId": _normalized_text(thread.get("messageId")),
                "sourceMessageId": _normalized_text(source_meta.get("messageId")),
                "sourceSummary": _computer_use_source_summary(source_meta),
            }
        )
    summary.update(_approval_summary_fields(_approval_state_from_run(run)))
    return {key: value for key, value in summary.items() if value not in ("", None)}


def _ensure_run_manifest(openclaw_dir, run, harness):
    run = run if isinstance(run, dict) else {}
    harness = harness if isinstance(harness, dict) else {}
    run_dir = _computer_use_run_dir(openclaw_dir, run.get("id"))
    manifest = {
        "run": {
            "id": run.get("id"),
            "objective": run.get("objective"),
            "status": run.get("status"),
            "createdAt": run.get("createdAt"),
        },
        "harness": harness,
    }
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "plan.json", run.get("plan") if isinstance(run.get("plan"), dict) else {})


def build_computer_use_devices_snapshot(openclaw_dir):
    default_device = ensure_default_computer_use_device(openclaw_dir)
    items = list_computer_devices(openclaw_dir, limit=100)
    if default_device and not any(_normalized_text(item.get("id")) == _normalized_text(default_device.get("id")) for item in items):
        items.insert(0, default_device)
    return {
        "items": items,
        "defaultDeviceId": _normalized_text((default_device or {}).get("id")),
        "summary": {
            "total": len(items),
            "active": sum(1 for item in items if _normalized_text(item.get("status")) == "active"),
        },
    }


def build_computer_use_runs_snapshot(openclaw_dir, page=1, page_size=20, status="", device_id="", thread_id=""):
    ensure_default_computer_use_device(openclaw_dir)
    page_value = _bounded_int(page, 1, minimum=1, maximum=1000)
    page_size_value = _bounded_int(page_size, 20, minimum=1, maximum=50)
    all_runs = list_computer_runs(
        openclaw_dir,
        device_id=device_id,
        thread_id=thread_id,
        status=status,
        limit=500,
    )
    all_runs = sorted(all_runs, key=lambda item: (_normalized_text(item.get("updatedAt")), _normalized_text(item.get("createdAt"))), reverse=True)
    start = (page_value - 1) * page_size_value
    items = []
    for run in all_runs[start : start + page_size_value]:
        device = get_computer_device(openclaw_dir, run.get("deviceId")) or {}
        items.append(_run_summary(run, device=device))
    total = len(all_runs)
    return {
        "items": items,
        "page": page_value,
        "pageSize": page_size_value,
        "total": total,
        "hasMore": start + page_size_value < total,
        "summary": {
            "total": total,
            "running": sum(1 for item in all_runs if _normalized_text(item.get("status")) == "running"),
            "needsHelp": sum(1 for item in all_runs if _normalized_text(item.get("status")) == "needs_help"),
            "takeover": sum(1 for item in all_runs if _normalized_text(item.get("status")) == "takeover"),
            "failed": sum(1 for item in all_runs if _normalized_text(item.get("status")) == "failed"),
            "pendingApproval": sum(1 for item in all_runs if _approval_state_from_run(item).get("status") == "pending"),
        },
    }


def build_computer_use_run_snapshot(openclaw_dir, run_id):
    ensure_default_computer_use_device(openclaw_dir)
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    source_meta = _safe_dict(_safe_dict(run.get("meta")).get("source"))
    steps = _sorted_steps(list_computer_steps(openclaw_dir, run_id=run_id, limit=500))
    artifacts = sorted(
        list_computer_artifacts(openclaw_dir, run_id=run_id, limit=200),
        key=lambda item: (_normalized_text(item.get("createdAt")), _normalized_text(item.get("updatedAt"))),
        reverse=True,
    )
    device = get_computer_device(openclaw_dir, run.get("deviceId")) or {}
    takeovers = sorted(
        list_computer_takeovers(openclaw_dir, run_id=run_id, limit=20),
        key=lambda item: (_normalized_text(item.get("startedAt")), _normalized_text(item.get("createdAt"))),
        reverse=True,
    )
    current_step_id = _normalized_text(run.get("currentStepId"))
    current_step = next((item for item in steps if _normalized_text(item.get("id")) == current_step_id), None)
    detail = {
        **run,
        "summary": {
            **_safe_dict(run.get("summary")),
            **_run_summary(run, steps=steps, artifacts=artifacts, device=device),
        },
        "approval": _approval_state_from_run(run),
        "source": source_meta,
        "sourceSummary": _computer_use_source_summary(source_meta),
        "device": device,
        "currentStep": current_step or {},
        "takeovers": takeovers,
        "artifactSummary": {
            "total": len(artifacts),
            "downloads": sum(1 for item in artifacts if _normalized_text(item.get("type")) == "download"),
            "screenshots": sum(1 for item in artifacts if _normalized_text(item.get("type")) == "screenshot"),
        },
        "stepSummary": {
            "total": len(steps),
            "done": sum(1 for item in steps if _normalized_text(item.get("status")) == "done"),
            "failed": sum(1 for item in steps if _normalized_text(item.get("status")) == "failed"),
        },
    }
    return detail


def build_computer_use_run_steps_snapshot(openclaw_dir, run_id, page=1, page_size=50):
    if not _normalized_text(run_id):
        raise RuntimeError("需要 runId。")
    steps = _sorted_steps(list_computer_steps(openclaw_dir, run_id=run_id, limit=500))
    page_value = _bounded_int(page, 1, minimum=1, maximum=1000)
    page_size_value = _bounded_int(page_size, 50, minimum=1, maximum=50)
    start = (page_value - 1) * page_size_value
    total = len(steps)
    return {
        "items": steps[start : start + page_size_value],
        "page": page_value,
        "pageSize": page_size_value,
        "total": total,
        "hasMore": start + page_size_value < total,
    }


def build_computer_use_run_artifacts_snapshot(openclaw_dir, run_id, page=1, page_size=30):
    if not _normalized_text(run_id):
        raise RuntimeError("需要 runId。")
    artifacts = sorted(
        list_computer_artifacts(openclaw_dir, run_id=run_id, limit=500),
        key=lambda item: (_normalized_text(item.get("createdAt")), _normalized_text(item.get("updatedAt"))),
        reverse=True,
    )
    page_value = _bounded_int(page, 1, minimum=1, maximum=1000)
    page_size_value = _bounded_int(page_size, 30, minimum=1, maximum=30)
    start = (page_value - 1) * page_size_value
    total = len(artifacts)
    return {
        "items": artifacts[start : start + page_size_value],
        "page": page_value,
        "pageSize": page_size_value,
        "total": total,
        "hasMore": start + page_size_value < total,
    }


def build_computer_use_run_preview_snapshot(openclaw_dir, run_id):
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    harness = _safe_dict(_safe_dict(run.get("meta")).get("harness"))
    source_meta = _safe_dict(_safe_dict(run.get("meta")).get("source"))
    workspace = _safe_dict(harness.get("workspace"))
    active_takeover = _latest_open_takeover(openclaw_dir, run_id)
    recent_actions = [
        item
        for item in list_computer_actions(openclaw_dir, run_id=run_id, limit=20)
        if isinstance(item, dict)
    ]
    recent_actions = sorted(
        recent_actions,
        key=lambda item: (_normalized_text(item.get("createdAt")), _normalized_text(item.get("updatedAt"))),
        reverse=True,
    )[:8]
    artifacts = sorted(
        list_computer_artifacts(openclaw_dir, run_id=run_id, limit=100),
        key=lambda item: (_normalized_text(item.get("createdAt")), _normalized_text(item.get("updatedAt"))),
        reverse=True,
    )
    preview_artifact = next(
        (
            item
            for item in artifacts
            if isinstance(item, dict)
            and _normalized_text((_safe_dict(item.get("meta"))).get("afterAction") or (_safe_dict(item.get("meta"))).get("action")) == "screenshot"
        ),
        artifacts[0] if artifacts else None,
    )
    preview_text = ""
    if isinstance(preview_artifact, dict):
        try:
            path = Path(str(preview_artifact.get("path") or "")).expanduser().resolve()
            if path.exists() and path.is_file():
                mime_type = _normalized_text(preview_artifact.get("mimeType")).lower()
                if mime_type.startswith("text/") or mime_type in {"application/json", "application/xml"}:
                    preview_text = path.read_text(encoding="utf-8")[:4000]
        except Exception:
            preview_text = ""
        if not preview_text:
            preview_text = _normalized_text(_safe_dict(preview_artifact.get("meta")).get("summaryText"))
    preview_meta = _safe_dict((preview_artifact or {}).get("meta"))
    display = _safe_dict(preview_meta.get("display"))
    display_size = _safe_dict(preview_meta.get("displaySize")) or display
    coordinate_space = _normalized_text(preview_meta.get("coordinateSpace"))
    capture_rect = _safe_dict(preview_meta.get("captureRect"))
    image_size = _safe_dict(preview_meta.get("imageSize"))
    preview_artifact_payload = {
        **(preview_artifact or {}),
        "display": _safe_dict((preview_artifact or {}).get("display")) or display,
        "displaySize": _safe_dict((preview_artifact or {}).get("displaySize")) or display_size,
    }
    controller_session_id = _normalized_text((active_takeover or {}).get("controllerSessionId"))
    terminal_statuses = {"succeeded", "failed", "canceled"}
    vnc_url = ""
    if _normalized_text(run.get("status")).lower() not in terminal_statuses:
        vnc_url = _computer_use_vnc_ws_url(runId=run_id, controllerSessionId=controller_session_id)
    live_preview_url = (
        f"/api/computer-use/run/live-preview?runId={run_id}&controllerSessionId={controller_session_id}"
        if active_takeover and controller_session_id
        else ""
    )
    return {
        "runId": run_id,
        "status": _normalized_text(run.get("status")),
        "transportMode": "vnc" if vnc_url else _normalized_text((active_takeover or {}).get("transportMode"), "preview"),
        "controllerSessionId": controller_session_id,
        "controlState": "user_control" if active_takeover else "agent_control",
        "streamMode": "websocket" if vnc_url else ("multipart" if active_takeover else ""),
        "streamUrl": vnc_url or live_preview_url,
        "livePreviewUrl": live_preview_url,
        "vncUrl": vnc_url,
        "capabilities": ["view"]
        + (["relay_actions"] if active_takeover else []),
        "takeover": active_takeover or {},
        "previewArtifact": preview_artifact_payload,
        "previewKind": "image" if _normalized_text((preview_artifact or {}).get("mimeType")).startswith("image/") else "text",
        "previewText": preview_text,
        "source": source_meta,
        "sourceSummary": _computer_use_source_summary(source_meta),
        "cursor": _safe_dict(preview_meta.get("cursor")),
        "display": display,
        "displaySize": display_size,
        "coordinateSpace": coordinate_space,
        "captureRect": capture_rect,
        "imageSize": image_size,
        "recentActions": [
            {
                "id": _normalized_text(item.get("id")),
                "actorType": _normalized_text(item.get("actorType")),
                "actionKey": _normalized_text(item.get("actionKey")),
                "success": bool(item.get("success")),
                "latencyMs": int(item.get("latencyMs") or 0),
                "createdAt": _normalized_text(item.get("createdAt")),
            }
            for item in recent_actions
        ],
        "recommendedRefreshMs": 1000 if _normalized_text(run.get("status")) == "takeover" else 4000,
        "workspace": {
            "virtual": _safe_dict(workspace.get("virtual")),
        },
        "updatedAt": _normalized_text((preview_artifact or {}).get("updatedAt") or (preview_artifact or {}).get("createdAt") or run.get("updatedAt")),
    }


def build_computer_use_run_actions_snapshot(openclaw_dir, run_id, page=1, page_size=50, step_id=""):
    if not _normalized_text(run_id):
        raise RuntimeError("需要 runId。")
    actions = list_computer_actions(
        openclaw_dir,
        run_id=run_id,
        step_id=step_id,
        limit=500,
    )
    actions = sorted(
        [item for item in actions if isinstance(item, dict)],
        key=lambda item: (_normalized_text(item.get("createdAt")), _normalized_text(item.get("updatedAt"))),
        reverse=True,
    )
    page_value = _bounded_int(page, 1, minimum=1, maximum=1000)
    page_size_value = _bounded_int(page_size, 50, minimum=1, maximum=50)
    start = (page_value - 1) * page_size_value
    total = len(actions)
    return {
        "items": actions[start : start + page_size_value],
        "page": page_value,
        "pageSize": page_size_value,
        "total": total,
        "hasMore": start + page_size_value < total,
        "summary": {
            "total": total,
            "succeeded": sum(1 for item in actions if item.get("success")),
            "failed": sum(1 for item in actions if not item.get("success")),
        },
    }


def _seed_run_steps(openclaw_dir, run_id, plan):
    items = []
    for index, step in enumerate(_safe_list(plan.get("steps")), start=1):
        step_id = f"{run_id}:{_normalized_text(step.get('id') or step.get('stepKey') or f'step-{index}')}"
        items.append(
            append_computer_step(
                openclaw_dir,
                {
                    "id": step_id,
                    "runId": run_id,
                    "stepKey": _normalized_text(step.get("stepKey") or step.get("id") or f"step-{index}"),
                    "intent": _normalized_text(step.get("intent")),
                    "status": "pending",
                    "budgetMs": step.get("budgetMs") or 0,
                    "meta": {"order": index},
                },
            )
        )
    return items


def perform_computer_use_run_create(openclaw_dir, payload, actor=None):
    payload = payload if isinstance(payload, dict) else {}
    actor = actor if isinstance(actor, dict) else {}
    objective = _normalized_text(payload.get("objective"))
    if not objective:
        raise RuntimeError("电脑执行目标不能为空。")
    device = get_computer_device(openclaw_dir, payload.get("deviceId")) or ensure_default_computer_use_device(openclaw_dir)
    run_id = _normalized_text(payload.get("runId"))
    if not run_id:
        run_id = f"crun_{secrets.token_hex(6)}"
    plan = build_computer_use_plan(objective, device=device, policy=payload.get("policy") if isinstance(payload.get("policy"), dict) else {})
    harness = build_computer_use_run_harness(openclaw_dir, run_id, device=device, actor=actor)
    steps = _seed_run_steps(openclaw_dir, run_id, plan)
    first_step_id = _normalized_text((steps[0] if steps else {}).get("id"))
    source_meta = _computer_use_source_context(payload)
    approval_required = bool(payload.get("requireApproval")) or _normalized_text(plan.get("riskLevel"), "medium") == "high"
    approval_reason = _normalized_text(
        payload.get("approvalReason"),
        "High-risk runs require approval before execution." if approval_required and _normalized_text(plan.get("riskLevel"), "medium") == "high"
        else ("Awaiting operator approval before execution." if approval_required else ""),
    )
    approval_state = {
        "required": approval_required,
        "status": "pending" if approval_required else "not_required",
        "reason": approval_reason,
        "requestedAt": now_iso() if approval_required else "",
        "requestedBy": _display_name_from_actor(actor) if approval_required else "",
        "approvedAt": "",
        "approvedBy": "",
        "note": "",
    }
    summary = {
        "plannerMode": _normalized_text(plan.get("plannerMode"), "bootstrap"),
        "stepCount": len(steps),
        "artifactCount": 0,
        "middlewareChain": list(COMPUTER_USE_DEFAULT_MIDDLEWARE_CHAIN),
        "workspace": harness.get("workspace", {}),
        **_approval_summary_fields(approval_state),
    }
    if source_meta:
        conversation = _safe_dict(source_meta.get("conversation"))
        thread = _safe_dict(source_meta.get("thread"))
        summary.update(
            {
                "sourceKind": _normalized_text(source_meta.get("kind")),
                "sourceLabel": _normalized_text(source_meta.get("label")),
                "sourceConversationAgentId": _normalized_text(conversation.get("agentId")),
                "sourceConversationSessionId": _normalized_text(conversation.get("sessionId")),
                "sourceConversationThreadId": _normalized_text(conversation.get("threadId")),
                "sourceConversationMessageId": _normalized_text(conversation.get("messageId")),
                "sourceThreadId": _normalized_text(thread.get("threadId")),
                "sourceThreadMessageId": _normalized_text(thread.get("messageId")),
                "sourceMessageId": _normalized_text(source_meta.get("messageId")),
                "sourceSummary": _computer_use_source_summary(source_meta),
            }
        )
    run = create_computer_run(
        openclaw_dir,
        {
            "id": run_id,
            "tenantId": _normalized_text(payload.get("tenantId")),
            "deviceId": _normalized_text(device.get("id")),
            "taskId": _normalized_text(payload.get("linkedTaskId") or payload.get("taskId")),
            "threadId": _normalized_text(payload.get("linkedThreadId") or payload.get("threadId")),
            "planVersion": _normalized_text(plan.get("version"), "v1"),
            "objective": objective,
            "mode": _normalized_text(payload.get("mode"), "semi_auto"),
            "status": "queued",
            "riskLevel": _normalized_text(plan.get("riskLevel"), "medium"),
            "idempotencyKey": _normalized_text(payload.get("idempotencyKey"), f"key_{secrets.token_hex(6)}"),
            "currentStepId": first_step_id,
            "createdBy": _display_name_from_actor(actor),
            "plan": plan,
            "summary": summary,
            "meta": {
                "harness": harness,
                "targetUrl": _normalized_text(payload.get("targetUrl") or payload.get("url")),
                "credentialBindings": _safe_list(payload.get("credentialBindings")),
                "policyId": _normalized_text(payload.get("policyId")),
                "source": source_meta,
                "approval": approval_state,
            },
        },
    )
    _ensure_run_manifest(openclaw_dir, run, harness)
    return build_computer_use_run_snapshot(openclaw_dir, run.get("id"))


def _transition_run_status(openclaw_dir, run_id, target_status, *, needs_help_reason=None, clarification_reason=None):
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    next_payload = {
        **run,
        "status": target_status,
        "updatedAt": now_iso(),
        "needsHelpReason": _normalized_text(needs_help_reason, run.get("needsHelpReason")),
        "clarificationReason": _normalized_text(clarification_reason, run.get("clarificationReason")),
    }
    update_computer_run(openclaw_dir, next_payload)
    return build_computer_use_run_snapshot(openclaw_dir, run_id)


def _update_current_step_status(openclaw_dir, run_id, target_status, *, error_code="", error_message=""):
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        return None
    current_step_id = _normalized_text(run.get("currentStepId"))
    if not current_step_id:
        return None
    steps = _sorted_steps(list_computer_steps(openclaw_dir, run_id=run_id, limit=200))
    step = next((item for item in steps if _normalized_text(item.get("id")) == current_step_id), None)
    if not isinstance(step, dict):
        return None
    updated = {
        **step,
        "status": target_status,
        "updatedAt": now_iso(),
    }
    if error_code:
        updated["errorCode"] = error_code
    if error_message:
        updated["errorMessage"] = error_message
    update_computer_step(openclaw_dir, updated)
    return updated


def perform_computer_use_run_pause(openclaw_dir, run_id, actor=None):
    return _transition_run_status(openclaw_dir, run_id, "paused")


def perform_computer_use_run_resume(openclaw_dir, run_id, actor=None):
    return _transition_run_status(openclaw_dir, run_id, "resuming")


def perform_computer_use_run_cancel(openclaw_dir, run_id, actor=None):
    return _transition_run_status(openclaw_dir, run_id, "canceled")


def perform_computer_use_run_approve(openclaw_dir, run_id, actor=None, payload=None):
    actor = actor if isinstance(actor, dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    approval_state = _approval_state_from_run(run)
    if not approval_state.get("required"):
        return build_computer_use_run_snapshot(openclaw_dir, run_id)
    approval_state.update(
        {
            "status": "approved",
            "approvedAt": now_iso(),
            "approvedBy": _display_name_from_actor(actor),
            "note": _normalized_text(payload.get("note")),
        }
    )
    summary = _safe_dict(run.get("summary"))
    summary.update(_approval_summary_fields(approval_state))
    meta = _safe_dict(run.get("meta"))
    meta["approval"] = approval_state
    update_computer_run(
        openclaw_dir,
        {
            **run,
            "summary": summary,
            "meta": meta,
            "updatedAt": now_iso(),
        },
    )
    return build_computer_use_run_snapshot(openclaw_dir, run_id)


def perform_computer_use_takeover_start(openclaw_dir, run_id, actor=None, payload=None):
    actor = actor if isinstance(actor, dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    active_takeover = _latest_open_takeover(openclaw_dir, run_id)
    if active_takeover:
        return build_computer_use_run_snapshot(openclaw_dir, run_id)
    start_computer_takeover(
        openclaw_dir,
        {
            "runId": run_id,
            "startedBy": _display_name_from_actor(actor),
            "startReason": _normalized_text(payload.get("reason"), "manual_takeover"),
            "transportMode": _normalized_text(payload.get("transportMode"), "live_preview"),
            "controllerSessionId": _normalized_text(payload.get("controllerSessionId"), f"ctsession_{secrets.token_hex(6)}"),
            "meta": {
                "recommendedRefreshMs": 1000,
                "source": "mission-control-web",
            },
        },
    )
    return _transition_run_status(openclaw_dir, run_id, "takeover")


def perform_computer_use_takeover_stop(openclaw_dir, run_id, actor=None, payload=None):
    payload = payload if isinstance(payload, dict) else {}
    takeover = _latest_open_takeover(openclaw_dir, run_id)
    if takeover:
        finish_computer_takeover(
            openclaw_dir,
            {
                **takeover,
                "endedAt": now_iso(),
                "summary": _normalized_text(payload.get("summary"), "manual takeover completed"),
                "updatedAt": now_iso(),
            },
        )
    return _transition_run_status(openclaw_dir, run_id, "resuming")


def perform_computer_use_request_clarification(openclaw_dir, run_id, payload=None, actor=None):
    payload = payload if isinstance(payload, dict) else {}
    actor = actor if isinstance(actor, dict) else {}
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    question = _normalized_text(payload.get("question"))
    if not question:
        raise RuntimeError("clarification question 不能为空。")
    context = _normalized_text(payload.get("context"))
    clarification_type = _normalized_text(payload.get("clarificationType") or payload.get("clarification_type"), "missing_info")
    options = _safe_list(payload.get("options"))
    meta = _safe_dict(run.get("meta"))
    clarification_request = {
        "question": question,
        "context": context,
        "clarificationType": clarification_type,
        "options": [str(item).strip() for item in options if str(item or "").strip()],
        "requestedAt": now_iso(),
        "requestedBy": _display_name_from_actor(actor),
    }
    _update_current_step_status(
        openclaw_dir,
        run_id,
        "clarifying",
        error_code="clarification_required",
        error_message=question,
    )
    update_computer_run(
        openclaw_dir,
        {
            **run,
            "status": "clarifying",
            "clarificationReason": question,
            "updatedAt": now_iso(),
            "meta": {
                **meta,
                "clarificationRequest": clarification_request,
            },
            "summary": {
                **_safe_dict(run.get("summary")),
                "lastClarificationAt": now_iso(),
                "lastClarificationType": clarification_type,
            },
        },
    )
    return build_computer_use_run_snapshot(openclaw_dir, run_id)


def perform_computer_use_answer_clarification(openclaw_dir, run_id, payload=None, actor=None):
    payload = payload if isinstance(payload, dict) else {}
    actor = actor if isinstance(actor, dict) else {}
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    meta = _safe_dict(run.get("meta"))
    clarification_request = _safe_dict(meta.get("clarificationRequest"))
    answer = _normalized_text(payload.get("answer"))
    selected_option = _normalized_text(payload.get("selectedOption") or payload.get("selected_option"))
    target_url = _normalized_text(payload.get("targetUrl") or payload.get("url"))
    if not answer and not selected_option and not target_url:
        raise RuntimeError("需要提供 clarification answer、selectedOption 或 targetUrl。")
    answer_payload = {
        "answeredAt": now_iso(),
        "answeredBy": _display_name_from_actor(actor),
        "answer": answer,
        "selectedOption": selected_option,
        "targetUrl": target_url,
    }
    history = _safe_list(meta.get("clarificationHistory"))
    history.append(
        {
            **clarification_request,
            **answer_payload,
        }
    )
    _update_current_step_status(openclaw_dir, run_id, "observing", error_code="", error_message="")
    update_computer_run(
        openclaw_dir,
        {
            **run,
            "status": "resuming",
            "clarificationReason": "",
            "updatedAt": now_iso(),
            "meta": {
                **meta,
                "targetUrl": target_url or _normalized_text(meta.get("targetUrl")),
                "clarificationRequest": {},
                "clarificationHistory": history,
                "latestClarificationAnswer": answer_payload,
            },
            "summary": {
                **_safe_dict(run.get("summary")),
                "lastClarificationAnsweredAt": now_iso(),
            },
        },
    )
    return build_computer_use_run_snapshot(openclaw_dir, run_id)
