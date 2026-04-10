from __future__ import annotations

import base64
import hashlib
import time
from datetime import datetime, timezone

from backend.adapters.integrations.computer_use.browser_executor import (
    BrowserComputerUseExecutor,
    resolve_executor_profile,
)
from backend.adapters.integrations.computer_use.desktop_executor import (
    DesktopComputerUseExecutor,
)
from backend.adapters.integrations.computer_use.filesystem_executor import (
    WorkspaceFileComputerUseExecutor,
)
from backend.adapters.storage.computer_use import (
    get_computer_run,
    list_computer_artifacts,
    list_computer_runs,
    list_computer_steps,
    list_computer_takeovers,
    save_computer_action,
    save_computer_artifact,
    update_computer_run,
    update_computer_step,
)
from .computer_use_actions import (
    computer_action_side_effect_level,
    computer_action_target,
    normalize_computer_action,
    should_capture_snapshot_after_action,
)
from .computer_use_core import _computer_use_run_dir, _normalized_text, perform_computer_use_request_clarification
from .computer_use_paths import (
    mask_paths_in_text_block,
)


COMPUTER_USE_ENGINE_RUN_STATUSES = ("resuming", "running")
COMPUTER_USE_TERMINAL_RUN_STATUSES = {"succeeded", "failed", "canceled"}
COMPUTER_USE_BLOCKED_RUN_STATUSES = {"paused", "takeover", "clarifying"}


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _safe_list(value):
    return value if isinstance(value, list) else []


def _sorted_steps(steps):
    return sorted(
        [item for item in _safe_list(steps) if isinstance(item, dict)],
        key=lambda item: (
            _normalized_text(item.get("createdAt")),
            _normalized_text(item.get("updatedAt")),
            _normalized_text(item.get("id")),
        ),
    )


def _current_and_next_step(steps, current_step_id=""):
    ordered = _sorted_steps(steps)
    current_id = _normalized_text(current_step_id)
    current = next((item for item in ordered if _normalized_text(item.get("id")) == current_id), None)
    if current is None:
        current = next((item for item in ordered if _normalized_text(item.get("status")) not in {"done", "failed"}), None)
    if current is None and ordered:
        current = ordered[0]
    next_step = None
    if current:
        found = False
        for item in ordered:
            if found:
                next_step = item
                break
            if _normalized_text(item.get("id")) == _normalized_text(current.get("id")):
                found = True
    return current, next_step


def _default_snapshot_path(openclaw_dir, run_id):
    run_dir = _computer_use_run_dir(openclaw_dir, run_id)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir / f"bootstrap-snapshot-{now_iso().replace(':', '-').replace('.', '-')}.txt"


def _write_snapshot_artifact(openclaw_dir, run_id, step_id, title, text, meta=None):
    text_value = str(text or "").strip()
    if not text_value:
        return None
    output_path = _default_snapshot_path(openclaw_dir, run_id)
    output_path.write_text(text_value + "\n", encoding="utf-8")
    digest = hashlib.sha256(text_value.encode("utf-8")).hexdigest()
    stat = output_path.stat()
    return save_computer_artifact(
        openclaw_dir,
        {
            "runId": run_id,
            "stepId": step_id,
            "type": "output",
            "title": title,
            "path": str(output_path),
            "mimeType": "text/plain",
            "hash": digest,
            "sizeBytes": stat.st_size,
            "retentionPolicy": "default",
            "meta": _safe_dict(meta),
        },
    )


def _write_binary_artifact(openclaw_dir, run_id, step_id, *, title, file_name, data, mime_type, artifact_type="output", meta=None):
    blob = data if isinstance(data, bytes) else bytes(data or b"")
    if not blob:
        return None
    run_dir = _computer_use_run_dir(openclaw_dir, run_id)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _normalized_text(file_name, "artifact.bin").replace("/", "_")
    output_path = artifacts_dir / f"{now_iso().replace(':', '-').replace('.', '-')}-{safe_name}"
    output_path.write_bytes(blob)
    digest = hashlib.sha256(blob).hexdigest()
    return save_computer_artifact(
        openclaw_dir,
        {
            "runId": run_id,
            "stepId": step_id,
            "type": artifact_type,
            "title": title,
            "path": str(output_path),
            "mimeType": _normalized_text(mime_type, "application/octet-stream"),
            "hash": digest,
            "sizeBytes": output_path.stat().st_size,
            "retentionPolicy": "default",
            "meta": _safe_dict(meta),
        },
    )


def _plan_step_map(run):
    plan = _safe_dict((run or {}).get("plan"))
    mapping = {}
    for item in _safe_list(plan.get("steps")):
        if not isinstance(item, dict):
            continue
        key = _normalized_text(item.get("stepKey") or item.get("id"))
        if key:
            mapping[key] = item
    return mapping


def _default_actions_for_step(step_key):
    normalized = _normalized_text(step_key)
    if normalized == "prepare-environment":
        return [{"action": "application", "application": "firefox"}]
    if normalized == "execute-objective":
        return [{"action": "wait", "duration": 750}, {"action": "screenshot"}]
    if normalized == "verify-result":
        return [{"action": "screenshot"}]
    return [{"action": "screenshot"}]


def _is_browser_gateway_fallback_error(error):
    message = _normalized_text(error).lower()
    if not message:
        return False
    fallback_markers = (
        "gateway connect failed",
        "gateway token missing",
        "gateway token mismatch",
        "pairing required",
        "gateway closed",
    )
    return any(marker in message for marker in fallback_markers)


def _record_action(openclaw_dir, run_id, step_id, action, result, *, success, latency_ms, error_message=""):
    meta = _safe_dict(result.get("meta"))
    if error_message:
        meta["errorMessage"] = error_message
    return save_computer_action(
        openclaw_dir,
        {
            "runId": run_id,
            "stepId": step_id,
            "actorType": "agent",
            "actionKey": _normalized_text(action.get("action")),
            "actionType": _normalized_text(action.get("action")),
            "sideEffectLevel": computer_action_side_effect_level(action),
            "target": computer_action_target(action),
            "input": action,
            "result": {
                key: value
                for key, value in {
                    **_safe_dict(result),
                    "errorMessage": error_message,
                }.items()
                if value not in ("", None, [], {})
            },
            "success": success,
            "latencyMs": latency_ms,
            "meta": meta,
        },
    )


def _ensure_executor_adapters(openclaw_dir, profile, workspace):
    return {
        "browser": BrowserComputerUseExecutor(openclaw_dir, profile),
        "desktop": DesktopComputerUseExecutor(),
        "filesystem": WorkspaceFileComputerUseExecutor(workspace),
    }


def _execute_normalized_action(openclaw_dir, action, *, run, step, profile, workspace, target_url="", adapters=None):
    action_name = _normalized_text(action.get("action"))
    step_id = _normalized_text((step or {}).get("id"))
    adapters = adapters if isinstance(adapters, dict) else {}
    browser_adapter = adapters.get("browser")
    desktop_adapter = adapters.get("desktop")
    filesystem_adapter = adapters.get("filesystem")
    action_surface = _normalized_text(action.get("surface"))
    application = _normalized_text(action.get("application"))

    if action_name == "application" and application == "firefox" and action_surface != "desktop":
        try:
            result = browser_adapter.execute(action, target_url=target_url, step_key=_normalized_text((step or {}).get("stepKey")))
            return result
        except Exception as exc:
            if not _is_browser_gateway_fallback_error(exc):
                raise
            desktop_result = desktop_adapter.execute({
                **action,
                "surface": "desktop",
                "url": target_url,
            })
            return {
                **desktop_result,
                "fallback": "desktop_application",
                "meta": {
                    **_safe_dict(desktop_result.get("meta")),
                    "surface": "desktop",
                    "fallbackFrom": "browser",
                    "fallbackReason": _normalized_text(exc),
                    "targetUrl": target_url,
                },
            }

    if action_name == "wait":
        result = browser_adapter.execute(action, target_url=target_url, step_key=_normalized_text((step or {}).get("stepKey")))
        return result

    if action_name == "screenshot" and action_surface != "desktop":
        try:
            result = browser_adapter.execute(action, target_url=target_url, step_key=_normalized_text((step or {}).get("stepKey")))
            if action_name == "screenshot":
                snapshot = _safe_dict(result.get("snapshot"))
                artifact = _write_snapshot_artifact(
                    openclaw_dir,
                    _normalized_text((run or {}).get("id")),
                    step_id,
                    _normalized_text((step or {}).get("intent"), "Computer Use snapshot"),
                    mask_paths_in_text_block(snapshot.get("output"), workspace),
                    meta={
                        "profile": profile,
                        "action": action_name,
                        "stepKey": _normalized_text((step or {}).get("stepKey")),
                    },
                )
                return {
                    **result,
                    "artifactId": _normalized_text((artifact or {}).get("id")),
                    "meta": {
                        **_safe_dict(result.get("meta")),
                        "artifactId": _normalized_text((artifact or {}).get("id")),
                    },
                }
        except Exception as exc:
            if not _is_browser_gateway_fallback_error(exc):
                raise
            result = desktop_adapter.execute({"action": "screenshot", "surface": "desktop"})
            image_bytes = result.pop("imageBytes", b"")
            artifact = _write_binary_artifact(
                openclaw_dir,
                _normalized_text((run or {}).get("id")),
                step_id,
                title=_normalized_text((step or {}).get("intent"), "Desktop screenshot"),
                file_name="desktop-preview.png",
                data=image_bytes,
                mime_type=_normalized_text(result.get("mimeType"), "image/png"),
                artifact_type="screenshot",
                meta={
                    "profile": profile,
                    "action": action_name,
                    "surface": "desktop",
                    "stepKey": _normalized_text((step or {}).get("stepKey")),
                    "summaryText": _normalized_text(result.get("summaryText")),
                    "cursor": _safe_dict(result.get("cursor")),
                    "display": _safe_dict(result.get("display")),
                    "displaySize": _safe_dict(result.get("displaySize")),
                    "coordinateSpace": _normalized_text(_safe_dict(result.get("meta")).get("coordinateSpace")),
                    "captureRect": _safe_dict(_safe_dict(result.get("meta")).get("captureRect")),
                    "imageSize": _safe_dict(_safe_dict(result.get("meta")).get("imageSize")),
                    "fallbackFrom": "browser",
                    "fallbackReason": _normalized_text(exc),
                },
            )
            return {
                **result,
                "artifactId": _normalized_text((artifact or {}).get("id")),
                "fallback": "desktop_screenshot",
                "meta": {
                    **_safe_dict(result.get("meta")),
                    "artifactId": _normalized_text((artifact or {}).get("id")),
                    "surface": "desktop",
                    "fallbackFrom": "browser",
                    "fallbackReason": _normalized_text(exc),
                },
            }

    if action_name in {"click_mouse", "type_text", "paste_text"} and (
        action_surface == "browser"
        or _normalized_text(action.get("ref"))
        or _normalized_text(action.get("selector"))
        or isinstance(action.get("fields"), list)
    ):
        return browser_adapter.execute(action, target_url=target_url, step_key=_normalized_text((step or {}).get("stepKey")))

    if action_name in {
        "application",
        "move_mouse",
        "trace_mouse",
        "click_mouse",
        "press_mouse",
        "drag_mouse",
        "scroll",
        "type_keys",
        "paste_text",
        "press_keys",
        "type_text",
        "screenshot",
        "cursor_position",
    }:
        result = desktop_adapter.execute(action)
        if action_name == "screenshot":
            image_bytes = result.pop("imageBytes", b"")
            artifact = _write_binary_artifact(
                openclaw_dir,
                _normalized_text((run or {}).get("id")),
                step_id,
                title=_normalized_text((step or {}).get("intent"), "Desktop screenshot"),
                file_name="desktop-preview.png",
                data=image_bytes,
                mime_type=_normalized_text(result.get("mimeType"), "image/png"),
                artifact_type="screenshot",
                meta={
                    "profile": profile,
                    "action": action_name,
                    "surface": "desktop",
                    "stepKey": _normalized_text((step or {}).get("stepKey")),
                    "summaryText": _normalized_text(result.get("summaryText")),
                    "cursor": _safe_dict(result.get("cursor")),
                    "display": _safe_dict(result.get("display")),
                    "displaySize": _safe_dict(result.get("displaySize")),
                    "coordinateSpace": _normalized_text(_safe_dict(result.get("meta")).get("coordinateSpace")),
                    "captureRect": _safe_dict(_safe_dict(result.get("meta")).get("captureRect")),
                    "imageSize": _safe_dict(_safe_dict(result.get("meta")).get("imageSize")),
                },
            )
            return {
                **result,
                "artifactId": _normalized_text((artifact or {}).get("id")),
                "meta": {
                    **_safe_dict(result.get("meta")),
                    "artifactId": _normalized_text((artifact or {}).get("id")),
                    "surface": "desktop",
                    "summaryText": _normalized_text(result.get("summaryText")),
                    "cursor": _safe_dict(result.get("cursor")),
                    "display": _safe_dict(result.get("display")),
                    "displaySize": _safe_dict(result.get("displaySize")),
                    "coordinateSpace": _normalized_text(_safe_dict(result.get("meta")).get("coordinateSpace")),
                    "captureRect": _safe_dict(_safe_dict(result.get("meta")).get("captureRect")),
                    "imageSize": _safe_dict(_safe_dict(result.get("meta")).get("imageSize")),
                },
            }
        return result

    if action_name == "write_file":
        payload = base64.b64decode(str(action.get("data") or "").encode("utf-8"))
        return filesystem_adapter.execute({**action, "_decodedData": payload})

    if action_name == "read_file":
        result = filesystem_adapter.execute(action)
        payload = result.pop("payload", b"")
        mime_type = _normalized_text(result.get("mimeType"), "application/octet-stream")
        source_name = _normalized_text(result.get("name"), "file")
        artifact = None
        if result.get("egressEligible") and payload and source_name.lower().endswith((".txt", ".md", ".json", ".csv", ".log")):
            artifact = _write_binary_artifact(
                openclaw_dir,
                _normalized_text((run or {}).get("id")),
                step_id,
                title=f"Read file {source_name}",
                file_name=source_name,
                data=payload,
                mime_type=mime_type,
                artifact_type="output",
                meta={"sourcePath": _normalized_text(result.get("path"))},
            )
        return {
            **result,
            "artifactId": _normalized_text((artifact or {}).get("id")),
            "meta": {
                **_safe_dict(result.get("meta")),
                "artifactId": _normalized_text((artifact or {}).get("id")),
            },
        }

    raise RuntimeError(f"当前执行器暂不支持低阶动作：{action_name}")


def _capture_post_action_snapshot(openclaw_dir, run_id, step, action, profile, workspace):
    action_name = _normalized_text((action or {}).get("action"))
    desktop_actions = {
        "move_mouse",
        "trace_mouse",
        "click_mouse",
        "press_mouse",
        "drag_mouse",
        "scroll",
        "type_keys",
        "paste_text",
        "press_keys",
        "type_text",
        "cursor_position",
    }
    if _normalized_text((action or {}).get("surface")) == "desktop" or action_name in desktop_actions:
        result = DesktopComputerUseExecutor().execute({"action": "screenshot", "surface": "desktop"})
        artifact = _write_binary_artifact(
            openclaw_dir,
            run_id,
            _normalized_text((step or {}).get("id")),
            title=f"{_normalized_text((step or {}).get('intent'), 'Computer Use step')} · {action_name}",
            file_name="desktop-preview.png",
            data=result.pop("imageBytes", b""),
            mime_type=_normalized_text(result.get("mimeType"), "image/png"),
            artifact_type="screenshot",
            meta={
                "profile": profile,
                "afterAction": action_name,
                "surface": "desktop",
                "stepKey": _normalized_text((step or {}).get("stepKey")),
                "summaryText": _normalized_text(result.get("summaryText")),
                "cursor": _safe_dict(result.get("cursor")),
                "display": _safe_dict(result.get("display")),
                "displaySize": _safe_dict(result.get("displaySize")),
                "coordinateSpace": _normalized_text(_safe_dict(result.get("meta")).get("coordinateSpace")),
                "captureRect": _safe_dict(_safe_dict(result.get("meta")).get("captureRect")),
                "imageSize": _safe_dict(_safe_dict(result.get("meta")).get("imageSize")),
            },
        )
        return result, artifact
    try:
        snapshot = BrowserComputerUseExecutor(openclaw_dir, profile).execute({"action": "screenshot", "limit": 120}).get("snapshot") or {}
        artifact = _write_snapshot_artifact(
            openclaw_dir,
            run_id,
            _normalized_text((step or {}).get("id")),
            f"{_normalized_text((step or {}).get('intent'), 'Computer Use step')} · {_normalized_text((action or {}).get('action'))}",
            mask_paths_in_text_block(snapshot.get("output"), workspace),
            meta={
                "profile": profile,
                "afterAction": _normalized_text((action or {}).get("action")),
                "stepKey": _normalized_text((step or {}).get("stepKey")),
            },
        )
        return snapshot, artifact
    except Exception as exc:
        if not _is_browser_gateway_fallback_error(exc):
            raise
        result = DesktopComputerUseExecutor().execute({"action": "screenshot", "surface": "desktop"})
        artifact = _write_binary_artifact(
            openclaw_dir,
            run_id,
            _normalized_text((step or {}).get("id")),
            title=f"{_normalized_text((step or {}).get('intent'), 'Computer Use step')} · {_normalized_text((action or {}).get('action'))}",
            file_name="desktop-preview.png",
            data=result.pop("imageBytes", b""),
            mime_type=_normalized_text(result.get("mimeType"), "image/png"),
            artifact_type="screenshot",
            meta={
                "profile": profile,
                "afterAction": _normalized_text((action or {}).get("action")),
                "surface": "desktop",
                "stepKey": _normalized_text((step or {}).get("stepKey")),
                "summaryText": _normalized_text(result.get("summaryText")),
                "cursor": _safe_dict(result.get("cursor")),
                "display": _safe_dict(result.get("display")),
                "displaySize": _safe_dict(result.get("displaySize")),
                "coordinateSpace": _normalized_text(_safe_dict(result.get("meta")).get("coordinateSpace")),
                "captureRect": _safe_dict(_safe_dict(result.get("meta")).get("captureRect")),
                "imageSize": _safe_dict(_safe_dict(result.get("meta")).get("imageSize")),
                "fallbackFrom": "browser",
                "fallbackReason": _normalized_text(exc),
            },
        )
        return result, artifact


def _latest_open_takeover(openclaw_dir, run_id):
    takeovers = [
        item
        for item in _safe_list(list_computer_takeovers(openclaw_dir, run_id=run_id, limit=50))
        if isinstance(item, dict) and not _normalized_text(item.get("endedAt"))
    ]
    takeovers.sort(key=lambda item: _normalized_text(item.get("startedAt")), reverse=True)
    return takeovers[0] if takeovers else None


def _latest_preview_artifact_id(openclaw_dir, run_id):
    artifacts = sorted(
        list_computer_artifacts(openclaw_dir, run_id=run_id, limit=100),
        key=lambda item: (_normalized_text(item.get("createdAt")), _normalized_text(item.get("updatedAt"))),
        reverse=True,
    )
    artifact = next(
        (
            item
            for item in artifacts
            if isinstance(item, dict)
            and _normalized_text((_safe_dict(item.get("meta"))).get("afterAction") or (_safe_dict(item.get("meta"))).get("action")) == "screenshot"
        ),
        artifacts[0] if artifacts else None,
    )
    return _normalized_text((artifact or {}).get("id"))


def fail_computer_use_run_execution(openclaw_dir, run_id, error, *, step_id="", error_code="execution_failed"):
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        return {}
    message = _normalized_text(str(error), "Computer Use execution failed.")
    steps = list_computer_steps(openclaw_dir, run_id=run_id, limit=200)
    current_step, _next_step = _current_and_next_step(steps, step_id or run.get("currentStepId"))
    if isinstance(current_step, dict):
        update_computer_step(
            openclaw_dir,
            {
                **current_step,
                "status": "failed",
                "errorCode": _normalized_text(error_code, "execution_failed"),
                "errorMessage": message,
                "finishedAt": now_iso(),
                "updatedAt": now_iso(),
            },
        )
    meta = _safe_dict(run.get("meta"))
    execution_meta = _safe_dict(meta.get("execution"))
    summary = _safe_dict(run.get("summary"))
    summary.update(
        {
            "lastExecutionAt": now_iso(),
            "lastErrorAt": now_iso(),
            "lastErrorMessage": message,
        }
    )
    updated_run = {
        **run,
        "status": "needs_help",
        "needsHelpReason": message,
        "updatedAt": now_iso(),
        "summary": summary,
        "meta": {
            **meta,
            "execution": {
                **execution_meta,
                "lastErrorAt": now_iso(),
                "lastErrorMessage": message,
            },
        },
    }
    update_computer_run(openclaw_dir, updated_run)
    return updated_run


def perform_computer_use_takeover_action(openclaw_dir, run_id, payload=None, actor=None):
    payload = payload if isinstance(payload, dict) else {}
    actor = actor if isinstance(actor, dict) else {}
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    if _normalized_text(run.get("status")) != "takeover":
        raise RuntimeError("当前 run 不在接管状态。")
    takeover = _latest_open_takeover(openclaw_dir, run_id)
    if not isinstance(takeover, dict):
        raise RuntimeError("当前 run 没有有效的 takeover session。")
    controller_session_id = _normalized_text(payload.get("controllerSessionId"))
    expected_session_id = _normalized_text(takeover.get("controllerSessionId"))
    if expected_session_id and controller_session_id != expected_session_id:
        raise RuntimeError("当前 takeover session 已切换，请刷新后重试。")

    meta = _safe_dict(run.get("meta"))
    harness = _safe_dict(meta.get("harness"))
    env_fingerprint = _safe_dict(harness.get("executionEnvironmentFingerprint"))
    workspace = _safe_dict(harness.get("workspace"))
    profile = resolve_executor_profile(
        openclaw_dir,
        requested_profile=_normalized_text(payload.get("profile")),
        env_fingerprint=env_fingerprint,
    )
    action_payload = payload.get("action") if isinstance(payload.get("action"), dict) else payload
    normalized_action = normalize_computer_action(action_payload)
    adapters = _ensure_executor_adapters(openclaw_dir, profile, workspace)
    current_step = next(
        (
            item
            for item in _sorted_steps(list_computer_steps(openclaw_dir, run_id=run_id, limit=200))
            if _normalized_text(item.get("id")) == _normalized_text(run.get("currentStepId"))
        ),
        None,
    ) or {
        "id": "",
        "stepKey": "manual-takeover",
        "intent": "Manual takeover action",
    }
    started_at = time.perf_counter()
    result = _execute_normalized_action(
        openclaw_dir,
        normalized_action,
        run=run,
        step=current_step,
        profile=profile,
        workspace=workspace,
        target_url=_normalized_text(payload.get("url"), _normalized_text(meta.get("targetUrl"))),
        adapters=adapters,
    )
    latency_ms = max(int((time.perf_counter() - started_at) * 1000), 0)
    action_record = save_computer_action(
        openclaw_dir,
        {
            "runId": run_id,
            "stepId": _normalized_text(current_step.get("id")),
            "actorType": "human",
            "actionKey": _normalized_text(normalized_action.get("action")),
            "actionType": _normalized_text(normalized_action.get("action")),
            "sideEffectLevel": computer_action_side_effect_level(normalized_action),
            "target": computer_action_target(normalized_action),
            "input": normalized_action,
            "result": {
                key: value
                for key, value in _safe_dict(result).items()
                if key != "payload"
            },
            "success": True,
            "latencyMs": latency_ms,
            "meta": {
                **_safe_dict(result.get("meta")),
                "controllerSessionId": _normalized_text(takeover.get("controllerSessionId")),
                "startedBy": _normalized_text(actor.get("displayName") or actor.get("username")),
            },
        },
    )
    preview_artifact_id = _normalized_text(result.get("artifactId"))
    if not preview_artifact_id and should_capture_snapshot_after_action(normalized_action):
        _snapshot, artifact = _capture_post_action_snapshot(
            openclaw_dir,
            run_id,
            current_step,
            normalized_action,
            profile,
            workspace,
        )
        preview_artifact_id = _normalized_text((artifact or {}).get("id"))
    if not preview_artifact_id:
        preview_artifact_id = _latest_preview_artifact_id(openclaw_dir, run_id)
    return {
        "runId": run_id,
        "status": "takeover",
        "action": {
            "id": _normalized_text(action_record.get("id")),
            "action": _normalized_text(action_record.get("actionKey")),
            "actionKey": _normalized_text(action_record.get("actionKey")),
            "actorType": _normalized_text(action_record.get("actorType"), "human"),
            "success": bool(action_record.get("success")),
            "latencyMs": int(action_record.get("latencyMs") or 0),
            "createdAt": _normalized_text(action_record.get("createdAt")),
            "sideEffectLevel": _normalized_text(action_record.get("sideEffectLevel")),
        },
        "controllerSessionId": expected_session_id,
        "result": {
            key: value
            for key, value in _safe_dict(result).items()
            if key != "payload"
        },
        "previewArtifactId": preview_artifact_id,
    }


def perform_computer_use_run_execute(openclaw_dir, run_id, payload=None, actor=None):
    payload = payload if isinstance(payload, dict) else {}
    actor = actor if isinstance(actor, dict) else {}
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    current_status = _normalized_text(run.get("status"))
    if current_status in COMPUTER_USE_TERMINAL_RUN_STATUSES:
        raise RuntimeError("当前 run 已结束，不能继续执行。")
    if current_status in COMPUTER_USE_BLOCKED_RUN_STATUSES:
        raise RuntimeError("当前 run 正处于暂停或人工接管状态。")
    approval = _safe_dict(_safe_dict(run.get("meta")).get("approval"))
    approval_required = bool(approval.get("required"))
    approval_status = _normalized_text(approval.get("status"), "pending" if approval_required else "not_required")
    if approval_required and approval_status != "approved":
        approver = _normalized_text(approval.get("approvedBy"))
        raise RuntimeError(
            f"当前 run 需要先批准后才能执行。{f' 最近批准人：{approver}。' if approver else ''}".strip()
        )

    steps = list_computer_steps(openclaw_dir, run_id=run_id, limit=200)
    current_step, next_step = _current_and_next_step(steps, run.get("currentStepId"))
    if not isinstance(current_step, dict):
        raise RuntimeError("当前 run 没有可执行步骤。")

    meta = _safe_dict(run.get("meta"))
    harness = _safe_dict(meta.get("harness"))
    env_fingerprint = _safe_dict(harness.get("executionEnvironmentFingerprint"))
    workspace = _safe_dict(harness.get("workspace"))
    profile = resolve_executor_profile(
        openclaw_dir,
        requested_profile=_normalized_text(payload.get("profile")),
        env_fingerprint=env_fingerprint,
    )
    target_url = _normalized_text(
        payload.get("url"),
        _normalized_text(meta.get("targetUrl")),
    )
    capture_snapshot = bool(payload.get("captureSnapshot", True))
    step_key = _normalized_text(current_step.get("stepKey"))
    if not target_url and step_key == "prepare-environment":
        clarification = perform_computer_use_request_clarification(
            openclaw_dir,
            run_id,
            payload={
                "question": "需要目标 URL 才能继续准备浏览器执行环境。",
                "context": "当前 run 尚未配置 targetUrl。",
                "clarificationType": "missing_info",
                "options": [],
            },
            actor=actor,
        )
        return {
            "runId": run_id,
            "status": "clarifying",
            "currentStepId": _normalized_text(clarification.get("currentStepId")),
            "clarificationReason": _normalized_text(clarification.get("clarificationReason")),
        }

    plan_step = _plan_step_map(run).get(step_key, {})
    raw_actions = payload.get("actions")
    if not isinstance(raw_actions, list) or not raw_actions:
        raw_actions = _safe_list(_safe_dict(current_step.get("meta")).get("actions"))
    if not isinstance(raw_actions, list) or not raw_actions:
        raw_actions = _safe_list(plan_step.get("actions"))
    if not isinstance(raw_actions, list) or not raw_actions:
        raw_actions = _default_actions_for_step(step_key)
    normalized_actions = [normalize_computer_action(item) for item in raw_actions]
    adapters = _ensure_executor_adapters(openclaw_dir, profile, workspace)

    current_step_update = {
        **current_step,
        "status": "acting",
        "attemptCount": int(current_step.get("attemptCount") or 0) + 1,
        "startedAt": _normalized_text(current_step.get("startedAt"), now_iso()),
        "updatedAt": now_iso(),
        "observationSummary": {
            **_safe_dict(current_step.get("observationSummary")),
            "profile": profile,
            "targetUrl": target_url,
            "plannedActionCount": len(normalized_actions),
        },
    }
    update_computer_step(openclaw_dir, current_step_update)
    try:
        action_records = []
        artifact_ids = []
        last_result = {}
        browser_start_result = None
        browser_open_result = None
        browser_snapshot_result = None
        for normalized_action in normalized_actions:
            started_at = time.perf_counter()
            try:
                last_result = _execute_normalized_action(
                    openclaw_dir,
                    normalized_action,
                    run=run,
                    step=current_step,
                    profile=profile,
                    workspace=workspace,
                    target_url=target_url,
                    adapters=adapters,
                )
                latency_ms = max(int((time.perf_counter() - started_at) * 1000), 0)
                action_record = _record_action(
                    openclaw_dir,
                    run_id,
                    _normalized_text(current_step.get("id")),
                    normalized_action,
                    last_result,
                    success=True,
                    latency_ms=latency_ms,
                )
                action_records.append(action_record)
                if _normalized_text(normalized_action.get("action")) == "application":
                    browser_start_result = _safe_dict(last_result.get("browserStart"))
                    browser_open_result = _safe_dict(last_result.get("browserOpen"))
                if _normalized_text(normalized_action.get("action")) == "screenshot":
                    browser_snapshot_result = _safe_dict(last_result.get("snapshot"))
                    artifact_id = _normalized_text(last_result.get("artifactId"))
                    if artifact_id:
                        artifact_ids.append(artifact_id)
                elif capture_snapshot and should_capture_snapshot_after_action(normalized_action):
                    browser_snapshot_result, snapshot_artifact = _capture_post_action_snapshot(
                        openclaw_dir,
                        run_id,
                        current_step,
                        normalized_action,
                        profile,
                        workspace,
                    )
                    artifact_id = _normalized_text((snapshot_artifact or {}).get("id"))
                    if artifact_id:
                        artifact_ids.append(artifact_id)
            except Exception as action_error:
                latency_ms = max(int((time.perf_counter() - started_at) * 1000), 0)
                _record_action(
                    openclaw_dir,
                    run_id,
                    _normalized_text(current_step.get("id")),
                    normalized_action,
                    {},
                    success=False,
                    latency_ms=latency_ms,
                    error_message=mask_paths_in_text_block(str(action_error), workspace),
                )
                raise

        last_artifact_id = artifact_ids[-1] if artifact_ids else ""

        current_step_done = {
            **current_step_update,
            "status": "done",
            "actionCount": int(current_step.get("actionCount") or 0) + len(normalized_actions),
            "finishedAt": now_iso(),
            "updatedAt": now_iso(),
            "verificationSummary": {
                "browserStarted": bool(browser_start_result),
                "openedUrl": target_url if browser_open_result else "",
                "snapshotCaptured": bool(browser_snapshot_result),
                "artifactIds": artifact_ids,
                "lastArtifactId": last_artifact_id,
                "actionsExecuted": [_normalized_text(item.get("actionKey")) for item in action_records],
            },
        }
        update_computer_step(openclaw_dir, current_step_done)

        if isinstance(next_step, dict) and _normalized_text(next_step.get("status")) in {"pending", "waiting"}:
            update_computer_step(
                openclaw_dir,
                {
                    **next_step,
                    "status": "observing",
                    "updatedAt": now_iso(),
                },
            )

        finished = not isinstance(next_step, dict)
        summary = _safe_dict(run.get("summary"))
        summary.update(
            {
                "bootstrapProfile": profile,
                "bootstrapTargetUrl": target_url,
                "lastExecutionAt": now_iso(),
                "lastSnapshotArtifactId": last_artifact_id,
                "lastActionCount": len(normalized_actions),
            }
        )
        updated_meta = {
            **meta,
            "targetUrl": target_url or _normalized_text(meta.get("targetUrl")),
            "execution": {
                "profile": profile,
                "targetUrl": target_url,
                "lastExecutionAt": now_iso(),
                "browserStartResult": _safe_dict(browser_start_result),
                "browserOpenResult": _safe_dict(browser_open_result),
                "snapshotCaptured": bool(browser_snapshot_result),
                "actions": normalized_actions,
            },
        }
        updated_run = {
            **run,
            "status": "succeeded" if finished else "running",
            "startedAt": _normalized_text(run.get("startedAt"), now_iso()),
            "finishedAt": now_iso() if finished else _normalized_text(run.get("finishedAt")),
            "updatedAt": now_iso(),
            "currentStepId": _normalized_text((next_step or {}).get("id")),
            "summary": summary,
            "meta": updated_meta,
        }
        update_computer_run(openclaw_dir, updated_run)
        return {
            "runId": run_id,
            "profile": profile,
            "targetUrl": target_url,
            "artifact": {"id": last_artifact_id} if last_artifact_id else {},
            "artifacts": artifact_ids,
            "actions": [
                {
                    "id": _normalized_text(item.get("id")),
                    "action": _normalized_text(item.get("actionKey")),
                    "success": bool(item.get("success")),
                }
                for item in action_records
            ],
            "browserStart": _safe_dict(browser_start_result),
            "browserOpen": _safe_dict(browser_open_result),
            "browserSnapshot": _safe_dict(browser_snapshot_result),
            "status": updated_run["status"],
            "currentStepId": updated_run["currentStepId"],
        }
    except Exception as error:
        fail_computer_use_run_execution(
            openclaw_dir,
            run_id,
            error,
            step_id=_normalized_text(current_step.get("id")),
        )
        raise


def _candidate_runs_for_engine(openclaw_dir, limit=5):
    limit_value = max(int(limit or 0), 1)
    items = []
    seen = set()
    for status in COMPUTER_USE_ENGINE_RUN_STATUSES:
        for run in _safe_list(list_computer_runs(openclaw_dir, status=status, limit=max(limit_value * 4, 12))):
            run_id = _normalized_text((run or {}).get("id"))
            if not run_id or run_id in seen:
                continue
            seen.add(run_id)
            items.append(run)
    status_rank = {status: index for index, status in enumerate(COMPUTER_USE_ENGINE_RUN_STATUSES)}
    items.sort(
        key=lambda item: (
            status_rank.get(_normalized_text(item.get("status")), len(status_rank)),
            _normalized_text(item.get("updatedAt")),
            _normalized_text(item.get("createdAt")),
            _normalized_text(item.get("id")),
        )
    )
    return items[:limit_value]


def _advance_run_until_blocked(openclaw_dir, run_id, *, max_step_advances=3):
    run = get_computer_run(openclaw_dir, run_id)
    if not isinstance(run, dict):
        raise RuntimeError("未找到对应的 computer-use run。")
    seen_steps = set()
    step_advances = 0
    artifact_ids = []
    starting_status = _normalized_text(run.get("status"))
    while step_advances < max(int(max_step_advances or 1), 1):
        run = get_computer_run(openclaw_dir, run_id)
        if not isinstance(run, dict):
            break
        status = _normalized_text(run.get("status"))
        current_step_id = _normalized_text(run.get("currentStepId"))
        if status not in COMPUTER_USE_ENGINE_RUN_STATUSES or not current_step_id or current_step_id in seen_steps:
            break
        seen_steps.add(current_step_id)
        result = perform_computer_use_run_execute(openclaw_dir, run_id, payload={"captureSnapshot": True})
        artifact_id = _normalized_text((_safe_dict(result.get("artifact"))).get("id"))
        if artifact_id:
            artifact_ids.append(artifact_id)
        step_advances += 1
        latest_run = get_computer_run(openclaw_dir, run_id)
        latest_status = _normalized_text((latest_run or {}).get("status"))
        if latest_status != "running":
            break
    final_run = get_computer_run(openclaw_dir, run_id) or {}
    return {
        "runId": run_id,
        "startingStatus": starting_status,
        "finalStatus": _normalized_text(final_run.get("status")),
        "currentStepId": _normalized_text(final_run.get("currentStepId")),
        "stepAdvanceCount": step_advances,
        "artifactIds": artifact_ids,
    }


def run_computer_use_engine_cycle(openclaw_dir, source="manual", limit=2, max_step_advances=3):
    started_at = now_iso()
    processed = []
    failed = []
    for run in _candidate_runs_for_engine(openclaw_dir, limit=limit):
        run_id = _normalized_text(run.get("id"))
        if not run_id:
            continue
        try:
            processed.append(
                _advance_run_until_blocked(
                    openclaw_dir,
                    run_id,
                    max_step_advances=max_step_advances,
                )
            )
        except Exception as error:
            failed.append({"runId": run_id, "error": str(error)})
    return {
        "source": _normalized_text(source, "manual"),
        "startedAt": started_at,
        "finishedAt": now_iso(),
        "processedCount": len(processed),
        "failedCount": len(failed),
        "succeededCount": sum(1 for item in processed if _normalized_text(item.get("finalStatus")) == "succeeded"),
        "needsHelpCount": sum(1 for item in processed if _normalized_text(item.get("finalStatus")) == "needs_help"),
        "items": processed,
        "errors": failed,
    }
