from __future__ import annotations

import json
import os
import secrets
import threading
from pathlib import Path
import time
from copy import deepcopy
from datetime import datetime, timezone

from backend.adapters.storage.computer_use import get_computer_artifact
from backend.adapters.integrations.computer_use.desktop_executor import DesktopComputerUseExecutor
from backend.application.services.computer_use_actions import normalize_computer_action
from backend.application.services.live_events import publish_live_event


_DESKTOP_SESSION_LOCK = threading.Lock()


def _parse_bounded_int(value, default, minimum=1, maximum=100):
    try:
        normalized = int(value or default)
    except (TypeError, ValueError):
        normalized = int(default)
    return max(minimum, min(normalized, maximum))


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _desktop_session_key(openclaw_dir):
    try:
        return str(Path(openclaw_dir).expanduser().resolve())
    except Exception:
        return str(openclaw_dir or "").strip()


def _desktop_session_store(server):
    store = getattr(server, "_computer_use_desktop_sessions", None)
    if not isinstance(store, dict):
        store = {}
        setattr(server, "_computer_use_desktop_sessions", store)
    return store


def _resolve_desktop_vnc_url():
    proxy_path = _normalized_text(
        os.environ.get("MISSION_CONTROL_COMPUTER_USE_VNC_PROXY_PATH")
        or os.environ.get("MISSION_CONTROL_DESKTOP_VNC_PROXY_PATH")
    )
    if proxy_path:
        return proxy_path
    for key in (
        "MISSION_CONTROL_DESKTOP_VNC_WS_URL",
        "MISSION_CONTROL_DESKTOP_VNC_URL",
        "MISSION_CONTROL_COMPUTER_USE_VNC_WS_URL",
        "BYTEBOT_DESKTOP_VNC_URL",
    ):
        value = _normalized_text(os.environ.get(key))
        if value:
            return value
    return ""


def _desktop_session_for_openclaw(server, openclaw_dir):
    key = _desktop_session_key(openclaw_dir)
    store = _desktop_session_store(server)
    with _DESKTOP_SESSION_LOCK:
        session = store.get(key)
        return deepcopy(session) if isinstance(session, dict) else None


def _persist_desktop_session(server, openclaw_dir, session):
    key = _desktop_session_key(openclaw_dir)
    store = _desktop_session_store(server)
    with _DESKTOP_SESSION_LOCK:
        store[key] = deepcopy(session if isinstance(session, dict) else {})


def _desktop_session_snapshot(session, *, include_live_preview_url=True):
    session = session if isinstance(session, dict) else {}
    preview = _safe_dict(session.get("preview"))
    if not preview and any(
        session.get(key)
        for key in ("previewKind", "previewText", "display", "displaySize", "coordinateSpace", "captureRect", "imageSize")
    ):
        preview = {
            "previewKind": session.get("previewKind"),
            "previewText": session.get("previewText"),
            "cursor": session.get("cursor"),
            "display": session.get("display"),
            "displaySize": session.get("displaySize"),
            "coordinateSpace": session.get("coordinateSpace"),
            "captureRect": session.get("captureRect"),
            "imageSize": session.get("imageSize"),
            "previewArtifact": session.get("previewArtifact"),
        }
    active = _normalized_text(session.get("status"), "idle") == "active"
    session_id = _normalized_text(session.get("sessionId"))
    preview_supported = bool(session.get("previewSupported", True))
    relay_supported = bool(session.get("relaySupported", True))
    configured_vnc_url = _resolve_desktop_vnc_url()
    transport_mode = "vnc" if active and configured_vnc_url else ("desktop" if preview_supported else "degraded")
    stream_mode = "websocket" if active and configured_vnc_url else ("multipart" if active and preview_supported else "")
    live_preview_url = (
        configured_vnc_url
        if active and configured_vnc_url and include_live_preview_url
        else _normalized_text(session.get("livePreviewUrl")) if include_live_preview_url and preview_supported else ""
    )
    if active and include_live_preview_url and not live_preview_url and preview_supported and session_id:
        live_preview_url = f"/api/computer-use/desktop/live-preview?sessionId={session_id}"
    preview_artifact = _safe_dict(preview.get("previewArtifact"))
    capabilities = ["view"]
    if active and relay_supported:
        capabilities.append("relay_actions")
    return {
        "sessionId": session_id,
        "status": _normalized_text(session.get("status"), "idle"),
        "mode": "free_desktop",
        "recorded": False,
        "startedAt": _normalized_text(session.get("startedAt")),
        "startedBy": _normalized_text(session.get("startedBy")),
        "endedAt": _normalized_text(session.get("endedAt")),
        "updatedAt": _normalized_text(session.get("updatedAt")),
        "lastActionAt": _normalized_text(session.get("lastActionAt")),
        "actionCount": int(session.get("actionCount") or 0),
        "controlState": "free_desktop" if active else "idle",
        "transportMode": transport_mode,
        "streamMode": stream_mode,
        "streamUrl": live_preview_url,
        "livePreviewUrl": live_preview_url,
        "vncUrl": configured_vnc_url if active and configured_vnc_url else "",
        "capabilities": capabilities,
        "previewSupported": preview_supported,
        "relaySupported": relay_supported,
        "recommendedRefreshMs": int(session.get("recommendedRefreshMs") or (1000 if active else 4000)),
        "previewKind": _normalized_text(preview.get("previewKind"), "text"),
        "previewText": _normalized_text(preview.get("previewText")),
        "cursor": _safe_dict(preview.get("cursor")),
        "display": _safe_dict(preview.get("display")),
        "displaySize": _safe_dict(preview.get("displaySize")),
        "coordinateSpace": _normalized_text(preview.get("coordinateSpace")),
        "captureRect": _safe_dict(preview.get("captureRect")),
        "imageSize": _safe_dict(preview.get("imageSize")),
        "previewArtifact": {
            "id": _normalized_text(preview_artifact.get("id")),
            "title": _normalized_text(preview_artifact.get("title")),
            "type": _normalized_text(preview_artifact.get("type")),
            "mimeType": _normalized_text(preview_artifact.get("mimeType")),
            "updatedAt": _normalized_text(preview_artifact.get("updatedAt")),
            "createdAt": _normalized_text(preview_artifact.get("createdAt")),
            "display": _safe_dict(preview_artifact.get("display")),
            "displaySize": _safe_dict(preview_artifact.get("displaySize")),
        },
        "lastAction": _safe_dict(session.get("lastAction")),
    }


def _desktop_preview_snapshot_from_result(session, result, *, status=None):
    session = session if isinstance(session, dict) else {}
    result = result if isinstance(result, dict) else {}
    preview = {
        "previewKind": "image" if _normalized_text(result.get("mimeType")).startswith("image/") else "text",
        "previewText": _normalized_text(result.get("summaryText")),
        "cursor": _safe_dict(result.get("cursor")),
        "display": _safe_dict(result.get("display")),
        "displaySize": _safe_dict(result.get("displaySize")) or _safe_dict(result.get("display")),
        "coordinateSpace": _normalized_text(_safe_dict(result.get("meta")).get("coordinateSpace"), "desktop_pixels"),
        "captureRect": _safe_dict(_safe_dict(result.get("meta")).get("captureRect")),
        "imageSize": _safe_dict(_safe_dict(result.get("meta")).get("imageSize")) or _safe_dict(result.get("display")),
        "recommendedRefreshMs": int(session.get("recommendedRefreshMs") or 1000),
    }
    if isinstance(result.get("imageBytes"), (bytes, bytearray)) and result.get("imageBytes"):
        preview["previewArtifact"] = {
            "id": _normalized_text(session.get("sessionId"), "desktop-session"),
            "title": "Desktop live preview",
            "type": "screenshot",
            "mimeType": _normalized_text(result.get("mimeType"), "image/png"),
            "updatedAt": _now_iso(),
            "createdAt": _normalized_text(session.get("startedAt"), _now_iso()),
            "display": _safe_dict(result.get("display")),
            "displaySize": _safe_dict(result.get("displaySize")) or _safe_dict(result.get("display")),
        }
    return {
        "status": _normalized_text(status or session.get("status") or "active"),
        "updatedAt": _now_iso(),
        "preview": preview,
    }


def _desktop_session_response(session):
    return {"session": _desktop_session_snapshot(session)}


def _desktop_degraded_preview(capabilities):
    capabilities = capabilities if isinstance(capabilities, dict) else {}
    summary_parts = []
    if not capabilities.get("screenshot"):
        summary_parts.append("当前运行环境缺少原生桌面截图工具")
    if not capabilities.get("pointer") and not capabilities.get("keyboard"):
        summary_parts.append("也缺少原生桌面输入工具")
    summary_text = "，".join(summary_parts) or "当前运行环境暂时无法提供原生桌面能力。"
    return {
        "previewKind": "text",
        "previewText": f"{summary_text}。自由桌面已切换为降级模式，可见状态说明，但不提供实时桌面画面。",
        "cursor": {},
        "display": {"width": 1440, "height": 960},
        "displaySize": {"width": 1440, "height": 960},
        "coordinateSpace": "desktop_pixels",
        "captureRect": {"x": 0, "y": 0, "width": 1440, "height": 960},
        "imageSize": {"width": 1440, "height": 960},
        "recommendedRefreshMs": 4000,
    }


def _publish_desktop_session_sync(handler, session, *, stage="updated", action_entry=None):
    session = _desktop_session_snapshot(session)
    session_id = _normalized_text(session.get("sessionId"))
    if not session_id:
        return
    try:
        publish_live_event(
            handler.server,
            "computer_use_desktop_session",
            {
                "sessionId": session_id,
                "stage": _normalized_text(stage, "updated"),
                "status": _normalized_text(session.get("status"), "idle"),
                "updatedAt": _normalized_text(session.get("updatedAt")),
                "startedAt": _normalized_text(session.get("startedAt")),
                "endedAt": _normalized_text(session.get("endedAt")),
                "lastActionAt": _normalized_text(session.get("lastActionAt")),
                "actionCount": int(session.get("actionCount") or 0),
            },
        )
    except Exception:
        pass
    if isinstance(action_entry, dict) and _normalized_text(action_entry.get("action")):
        try:
            publish_live_event(
                handler.server,
                "computer_use_desktop_action",
                {
                    "sessionId": session_id,
                    "stage": _normalized_text(stage, "updated"),
                    "updatedAt": _normalized_text(action_entry.get("createdAt") or action_entry.get("updatedAt")),
                    "action": action_entry,
                },
            )
        except Exception:
            pass


def _publish_desktop_preview_sync(handler, session, *, stage="updated"):
    session = _desktop_session_snapshot(session)
    session_id = _normalized_text(session.get("sessionId"))
    if not session_id:
        return
    try:
        publish_live_event(
            handler.server,
            "computer_use_desktop_preview",
            {
                "sessionId": session_id,
                "stage": _normalized_text(stage, "updated"),
                "status": _normalized_text(session.get("status"), "idle"),
                "updatedAt": _normalized_text(session.get("updatedAt")),
                "controlState": "free_desktop" if _normalized_text(session.get("status"), "idle") == "active" else "idle",
                "transportMode": _normalized_text(session.get("transportMode"), "desktop"),
                "streamMode": _normalized_text(session.get("streamMode")),
                "streamUrl": _normalized_text(session.get("streamUrl")),
                "livePreviewUrl": _normalized_text(session.get("livePreviewUrl")),
                "vncUrl": _normalized_text(session.get("vncUrl")),
                "capabilities": session.get("capabilities") if isinstance(session.get("capabilities"), list) else [],
                "previewKind": _normalized_text(session.get("previewKind"), "text"),
                "cursor": _safe_dict(session.get("cursor")),
                "display": _safe_dict(session.get("display")),
                "displaySize": _safe_dict(session.get("displaySize")),
                "coordinateSpace": _normalized_text(session.get("coordinateSpace")),
                "captureRect": _safe_dict(session.get("captureRect")),
                "imageSize": _safe_dict(session.get("imageSize")),
                "previewText": _normalized_text(session.get("previewText")),
                "previewArtifact": _safe_dict(session.get("previewArtifact")),
                "lastAction": _safe_dict(session.get("lastAction")),
            },
        )
    except Exception:
        pass


def _invalidate_computer_use_payloads(svc, openclaw_dir):
    invalidate = getattr(svc, "invalidate_computer_use_payload_cache", None)
    if callable(invalidate):
        try:
            invalidate(openclaw_dir)
            return
        except Exception:
            pass
    try:
        svc.clear_cached_payloads()
    except Exception:
        pass


def _compact_run_stream_payload(run, *, stage="updated"):
    run = run if isinstance(run, dict) else {}
    summary = run.get("summary") if isinstance(run.get("summary"), dict) else {}
    current_step = run.get("currentStep") if isinstance(run.get("currentStep"), dict) else {}
    approval = run.get("approval") if isinstance(run.get("approval"), dict) else {}
    return {
        "runId": str(run.get("id") or "").strip(),
        "stage": str(stage or "updated").strip(),
        "status": str(run.get("status") or "").strip(),
        "objective": str(run.get("objective") or "").strip(),
        "updatedAt": str(run.get("updatedAt") or "").strip(),
        "currentStepId": str(run.get("currentStepId") or "").strip(),
        "currentStepStatus": str(current_step.get("status") or summary.get("currentStepStatus") or "").strip(),
        "currentStepIntent": str(current_step.get("intent") or summary.get("currentStepIntent") or "").strip(),
        "needsHelpReason": str(run.get("needsHelpReason") or "").strip(),
        "clarificationReason": str(run.get("clarificationReason") or "").strip(),
        "approvalRequired": bool(approval.get("required") or summary.get("approvalRequired")),
        "approvalStatus": str(approval.get("status") or summary.get("approvalStatus") or "").strip(),
        "approvalReason": str(approval.get("reason") or summary.get("approvalReason") or "").strip(),
        "approvalRequestedAt": str(approval.get("requestedAt") or summary.get("approvalRequestedAt") or "").strip(),
        "approvalRequestedBy": str(approval.get("requestedBy") or summary.get("approvalRequestedBy") or "").strip(),
        "approvedAt": str(approval.get("approvedAt") or summary.get("approvedAt") or "").strip(),
        "approvedBy": str(approval.get("approvedBy") or summary.get("approvedBy") or "").strip(),
        "sourceKind": str(summary.get("sourceKind") or "").strip(),
        "sourceLabel": str(summary.get("sourceLabel") or "").strip(),
        "sourceConversationAgentId": str(summary.get("sourceConversationAgentId") or "").strip(),
        "sourceConversationSessionId": str(summary.get("sourceConversationSessionId") or "").strip(),
        "sourceConversationThreadId": str(summary.get("sourceConversationThreadId") or "").strip(),
        "sourceConversationMessageId": str(summary.get("sourceConversationMessageId") or "").strip(),
        "sourceThreadId": str(summary.get("sourceThreadId") or "").strip(),
        "sourceThreadMessageId": str(summary.get("sourceThreadMessageId") or "").strip(),
        "sourceMessageId": str(summary.get("sourceMessageId") or "").strip(),
        "sourceSummary": str(summary.get("sourceSummary") or "").strip(),
        "counts": {
            "artifactCount": int((run.get("artifactSummary") or {}).get("total") or summary.get("artifactCount") or 0),
            "stepCount": int((run.get("stepSummary") or {}).get("total") or summary.get("stepCount") or 0),
        },
    }


def _compact_preview_stream_payload(preview, *, stage="updated"):
    preview = preview if isinstance(preview, dict) else {}
    artifact = preview.get("previewArtifact") if isinstance(preview.get("previewArtifact"), dict) else {}
    takeover = preview.get("takeover") if isinstance(preview.get("takeover"), dict) else {}
    return {
        "runId": str(preview.get("runId") or "").strip(),
        "stage": str(stage or "updated").strip(),
        "status": str(preview.get("status") or "").strip(),
        "updatedAt": str(preview.get("updatedAt") or "").strip(),
        "transportMode": str(preview.get("transportMode") or "").strip(),
        "controllerSessionId": str(preview.get("controllerSessionId") or "").strip(),
        "controlState": str(preview.get("controlState") or "").strip(),
        "streamMode": str(preview.get("streamMode") or "").strip(),
        "streamUrl": str(preview.get("streamUrl") or "").strip(),
        "vncUrl": str(preview.get("vncUrl") or "").strip(),
        "capabilities": preview.get("capabilities") if isinstance(preview.get("capabilities"), list) else [],
        "previewKind": str(preview.get("previewKind") or "").strip(),
        "cursor": preview.get("cursor") if isinstance(preview.get("cursor"), dict) else {},
        "display": preview.get("display") if isinstance(preview.get("display"), dict) else {},
        "displaySize": preview.get("displaySize") if isinstance(preview.get("displaySize"), dict) else {},
        "coordinateSpace": str(preview.get("coordinateSpace") or "").strip(),
        "captureRect": preview.get("captureRect") if isinstance(preview.get("captureRect"), dict) else {},
        "imageSize": preview.get("imageSize") if isinstance(preview.get("imageSize"), dict) else {},
        "recommendedRefreshMs": int(preview.get("recommendedRefreshMs") or 0),
        "previewText": str(preview.get("previewText") or "").strip()[:4000],
        "source": preview.get("source") if isinstance(preview.get("source"), dict) else {},
        "sourceSummary": str(preview.get("sourceSummary") or "").strip(),
        "previewArtifact": {
            "id": str(artifact.get("id") or "").strip(),
            "title": str(artifact.get("title") or "").strip(),
            "type": str(artifact.get("type") or "").strip(),
            "mimeType": str(artifact.get("mimeType") or "").strip(),
            "updatedAt": str(artifact.get("updatedAt") or "").strip(),
            "createdAt": str(artifact.get("createdAt") or "").strip(),
            "display": artifact.get("display") if isinstance(artifact.get("display"), dict) else {},
            "displaySize": artifact.get("displaySize") if isinstance(artifact.get("displaySize"), dict) else {},
        },
        "takeover": {
            "id": str(takeover.get("id") or "").strip(),
            "startedBy": str(takeover.get("startedBy") or "").strip(),
            "startedAt": str(takeover.get("startedAt") or "").strip(),
            "endedAt": str(takeover.get("endedAt") or "").strip(),
        },
        "recentActions": preview.get("recentActions") if isinstance(preview.get("recentActions"), list) else [],
    }


def _publish_computer_use_run_sync(handler, svc, run_id, *, stage="updated", include_preview=False, action_entry=None):
    run_id = str(run_id or "").strip()
    if not run_id:
        return
    try:
        run_snapshot = svc.build_computer_use_run_snapshot(handler.server.openclaw_dir, run_id)
    except Exception:
        run_snapshot = {}
    if isinstance(run_snapshot, dict) and run_snapshot:
        try:
            publish_live_event(handler.server, "computer_use_run", _compact_run_stream_payload(run_snapshot, stage=stage))
        except Exception:
            pass
    if include_preview:
        try:
            preview_snapshot = svc.build_computer_use_run_preview_snapshot(handler.server.openclaw_dir, run_id)
        except Exception:
            preview_snapshot = {}
        if isinstance(preview_snapshot, dict) and preview_snapshot:
            try:
                publish_live_event(handler.server, "computer_use_preview", _compact_preview_stream_payload(preview_snapshot, stage=stage))
            except Exception:
                pass
    if isinstance(action_entry, dict) and str(action_entry.get("id") or "").strip():
        try:
            publish_live_event(
                handler.server,
                "computer_use_action",
                {
                    "runId": run_id,
                    "stage": stage,
                    "updatedAt": str(action_entry.get("createdAt") or action_entry.get("updatedAt") or "").strip(),
                    "action": action_entry,
                },
            )
        except Exception:
            pass


def _serve_computer_use_live_preview(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    if not handler._require_capability("read", "当前账号没有访问 Computer Use 实时预览的权限。"):
        return True
    query = handler._query()
    run_id = str(query.get("runId", [""])[0] or "").strip()
    controller_session_id = str(query.get("controllerSessionId", [""])[0] or "").strip()
    if not run_id or not controller_session_id:
        handler._send_json({"ok": False, "error": "missing_params", "message": "需要 runId 和 controllerSessionId。"}, status=400)
        return True
    try:
        snapshot = services["build_computer_use_run_preview_snapshot"](openclaw_dir, run_id)
    except RuntimeError as error:
        handler._send_json({"ok": False, "error": "computer_use_run_preview_failed", "message": str(error)}, status=404)
        return True
    if str(snapshot.get("controllerSessionId") or "").strip() != controller_session_id:
        handler._send_json({"ok": False, "error": "computer_use_takeover_session_mismatch", "message": "当前 takeover session 已切换。"}, status=409)
        return True

    boundary = "frame"
    frame_interval_ms = max(250, min(int(snapshot.get("recommendedRefreshMs") or 1000), 5000))
    executor = DesktopComputerUseExecutor()
    handler.send_response(200)
    handler.send_header("Content-Type", f"multipart/x-mixed-replace; boundary={boundary}")
    handler.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
    handler.send_header("Connection", "keep-alive")
    for key, value in handler._cors_headers():
        handler.send_header(key, value)
    handler.end_headers()

    try:
        while True:
            frame = executor.execute({"action": "screenshot", "surface": "desktop"})
            image_bytes = frame.get("imageBytes", b"")
            if image_bytes:
                handler.wfile.write(f"--{boundary}\r\n".encode("utf-8"))
                handler.wfile.write(b"Content-Type: image/png\r\n")
                handler.wfile.write(f"Content-Length: {len(image_bytes)}\r\n\r\n".encode("utf-8"))
                handler.wfile.write(image_bytes)
                handler.wfile.write(b"\r\n")
                handler.wfile.flush()
            latest_snapshot = services["build_computer_use_run_preview_snapshot"](openclaw_dir, run_id)
            if str(latest_snapshot.get("controllerSessionId") or "").strip() != controller_session_id:
                break
            time.sleep(frame_interval_ms / 1000.0)
    except (BrokenPipeError, ConnectionResetError, TimeoutError):
        return True
    return True


def _serve_computer_use_desktop_live_preview(handler):
    openclaw_dir = handler.server.openclaw_dir
    if not handler._require_capability("read", "当前账号没有访问 Desktop 实时预览的权限。"):
        return True
    query = handler._query()
    session_id = _normalized_text(query.get("sessionId", [""])[0])
    session = _desktop_session_for_openclaw(handler.server, openclaw_dir)
    if not session or _normalized_text(session.get("status"), "idle") != "active":
        handler._send_json({"ok": False, "error": "computer_use_desktop_session_inactive", "message": "当前没有激活的 Desktop 会话。"}, status=404)
        return True
    active_session_id = _normalized_text(session.get("sessionId"))
    if session_id and session_id != active_session_id:
        handler._send_json({"ok": False, "error": "computer_use_desktop_session_mismatch", "message": "当前 Desktop 会话已切换。"}, status=409)
        return True
    session_id = session_id or active_session_id

    boundary = "frame"
    frame_interval_ms = max(250, min(int(session.get("recommendedRefreshMs") or 1000), 5000))
    executor = DesktopComputerUseExecutor()
    if hasattr(executor, "supports_screenshot") and not executor.supports_screenshot():
        handler._send_json(
            {
                "ok": False,
                "error": "computer_use_desktop_preview_unavailable",
                "message": "当前运行环境没有可用的原生桌面截图工具。",
            },
            status=409,
        )
        return True
    handler.send_response(200)
    handler.send_header("Content-Type", f"multipart/x-mixed-replace; boundary={boundary}")
    handler.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
    handler.send_header("Connection", "keep-alive")
    for key, value in handler._cors_headers():
        handler.send_header(key, value)
    handler.end_headers()

    try:
        while True:
            frame = executor.execute({"action": "screenshot", "surface": "desktop"})
            image_bytes = frame.get("imageBytes", b"")
            if image_bytes:
                handler.wfile.write(f"--{boundary}\r\n".encode("utf-8"))
                handler.wfile.write(b"Content-Type: image/png\r\n")
                handler.wfile.write(f"Content-Length: {len(image_bytes)}\r\n\r\n".encode("utf-8"))
                handler.wfile.write(image_bytes)
                handler.wfile.write(b"\r\n")
                handler.wfile.flush()
            latest_session = _desktop_session_for_openclaw(handler.server, openclaw_dir)
            if not latest_session or _normalized_text(latest_session.get("status"), "idle") != "active":
                break
            if _normalized_text(latest_session.get("sessionId")) != session_id:
                break
            preview_state = _desktop_preview_snapshot_from_result(latest_session, frame, status="active")
            latest_session["preview"] = deepcopy(preview_state.get("preview") or {})
            latest_session["updatedAt"] = _normalized_text(preview_state.get("updatedAt"), _now_iso())
            latest_session["recommendedRefreshMs"] = int((preview_state.get("preview") or {}).get("recommendedRefreshMs") or latest_session.get("recommendedRefreshMs") or 1000)
            _persist_desktop_session(handler.server, openclaw_dir, latest_session)
            _publish_desktop_preview_sync(handler, latest_session, stage="live_preview")
            time.sleep(frame_interval_ms / 1000.0)
    except (BrokenPipeError, ConnectionResetError, TimeoutError):
        return True
    return True


def _desktop_session_start(handler, payload):
    openclaw_dir = handler.server.openclaw_dir
    payload = payload if isinstance(payload, dict) else {}
    existing = _desktop_session_for_openclaw(handler.server, openclaw_dir)
    if isinstance(existing, dict) and _normalized_text(existing.get("status"), "idle") == "active":
        return _desktop_session_snapshot(existing)

    session_id = _normalized_text(payload.get("sessionId"), f"dsession_{secrets.token_hex(6)}")
    actor = getattr(handler, "_current_actor", lambda: {})()
    started_by = _normalized_text((actor if isinstance(actor, dict) else {}).get("displayName") or (actor if isinstance(actor, dict) else {}).get("username"))
    executor = DesktopComputerUseExecutor()
    capabilities = executor.capability_summary() if hasattr(executor, "capability_summary") else {}
    preview_supported = bool(capabilities.get("screenshot", True))
    relay_supported = bool(
        capabilities.get("pointer", True)
        or capabilities.get("keyboard", True)
        or capabilities.get("application", True)
    )
    session = {
        "sessionId": session_id,
        "status": "active",
        "mode": "free_desktop",
        "recorded": False,
        "startedAt": _now_iso(),
        "startedBy": started_by,
        "endedAt": "",
        "updatedAt": _now_iso(),
        "lastActionAt": "",
        "actionCount": 0,
        "recommendedRefreshMs": 1000,
        "preview": {},
        "previewSupported": preview_supported,
        "relaySupported": relay_supported,
        "lastAction": {},
        "livePreviewUrl": f"/api/computer-use/desktop/live-preview?sessionId={session_id}" if preview_supported else "",
    }
    if preview_supported:
        try:
            frame = executor.execute({"action": "screenshot", "surface": "desktop"})
            preview_state = _desktop_preview_snapshot_from_result(session, frame, status="active")
            session["preview"] = deepcopy(preview_state.get("preview") or {})
            session["recommendedRefreshMs"] = int((preview_state.get("preview") or {}).get("recommendedRefreshMs") or 1000)
            session["updatedAt"] = _normalized_text(preview_state.get("updatedAt"), session["updatedAt"])
        except Exception:
            session["previewSupported"] = False
            session["livePreviewUrl"] = ""
            session["preview"] = _desktop_degraded_preview(capabilities)
            session["recommendedRefreshMs"] = int((session["preview"] or {}).get("recommendedRefreshMs") or 4000)
    else:
        session["preview"] = _desktop_degraded_preview(capabilities)
        session["recommendedRefreshMs"] = int((session["preview"] or {}).get("recommendedRefreshMs") or 4000)
    _persist_desktop_session(handler.server, openclaw_dir, session)
    _publish_desktop_session_sync(handler, session, stage="started")
    _publish_desktop_preview_sync(handler, session, stage="started")
    return _desktop_session_snapshot(session)


def _desktop_session_stop(handler, payload):
    openclaw_dir = handler.server.openclaw_dir
    payload = payload if isinstance(payload, dict) else {}
    session = _desktop_session_for_openclaw(handler.server, openclaw_dir)
    if not isinstance(session, dict) or _normalized_text(session.get("status"), "idle") != "active":
        raise RuntimeError("当前没有激活的 Desktop 会话。")
    requested_session_id = _normalized_text(payload.get("sessionId"))
    active_session_id = _normalized_text(session.get("sessionId"))
    if requested_session_id and requested_session_id != active_session_id:
        raise RuntimeError("当前 Desktop 会话已切换。")
    session["status"] = "stopped"
    session["endedAt"] = _now_iso()
    session["updatedAt"] = session["endedAt"]
    session["livePreviewUrl"] = ""
    _persist_desktop_session(handler.server, openclaw_dir, session)
    _publish_desktop_session_sync(handler, session, stage="stopped")
    _publish_desktop_preview_sync(handler, session, stage="stopped")
    return _desktop_session_snapshot(session)


def _desktop_session_relay_action(handler, payload):
    openclaw_dir = handler.server.openclaw_dir
    payload = payload if isinstance(payload, dict) else {}
    session = _desktop_session_for_openclaw(handler.server, openclaw_dir)
    if not isinstance(session, dict) or _normalized_text(session.get("status"), "idle") != "active":
        raise RuntimeError("当前没有激活的 Desktop 会话。")
    if not bool(session.get("relaySupported", True)):
        raise RuntimeError("当前运行环境没有可用的原生桌面输入工具，暂时无法转发桌面动作。")
    requested_session_id = _normalized_text(payload.get("sessionId"))
    active_session_id = _normalized_text(session.get("sessionId"))
    if requested_session_id and requested_session_id != active_session_id:
        raise RuntimeError("当前 Desktop 会话已切换。")
    action = payload.get("action")
    action = action if isinstance(action, dict) else payload
    result = DesktopComputerUseExecutor().execute(normalize_computer_action(action))
    session["actionCount"] = int(session.get("actionCount") or 0) + 1
    session["lastActionAt"] = _now_iso()
    session["lastAction"] = {
        "action": _normalized_text(action.get("action")),
        "success": True,
        "createdAt": session["lastActionAt"],
        "result": {k: v for k, v in (result or {}).items() if k != "imageBytes"},
    }
    try:
        if _normalized_text(action.get("action")) != "screenshot":
            frame = DesktopComputerUseExecutor().execute({"action": "screenshot", "surface": "desktop"})
            preview_state = _desktop_preview_snapshot_from_result(session, frame, status="active")
        else:
            preview_state = _desktop_preview_snapshot_from_result(session, result, status="active")
        session["preview"] = deepcopy(preview_state.get("preview") or {})
        session["recommendedRefreshMs"] = int((preview_state.get("preview") or {}).get("recommendedRefreshMs") or session.get("recommendedRefreshMs") or 1000)
        session["updatedAt"] = _normalized_text(preview_state.get("updatedAt"), _now_iso())
    except Exception:
        session["updatedAt"] = _now_iso()
    _persist_desktop_session(handler.server, openclaw_dir, session)
    _publish_desktop_session_sync(handler, session, stage="action", action_entry=session.get("lastAction"))
    _publish_desktop_preview_sync(handler, session, stage="action")
    return {"session": _desktop_session_snapshot(session), "result": result}


def handle_computer_use_read_route(handler, path, services):
    openclaw_dir = handler.server.openclaw_dir

    if path == "/api/computer-use/desktop/session":
        if not handler._require_capability("read", "当前账号没有访问 Desktop 会话的权限。"):
            return True
        payload = _desktop_session_response(_desktop_session_for_openclaw(handler.server, openclaw_dir))
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    if path == "/api/computer-use/desktop/live-preview":
        return _serve_computer_use_desktop_live_preview(handler)

    if path == "/api/computer-use/run/live-preview":
        return _serve_computer_use_live_preview(handler, services)

    if path == "/api/computer-use/devices":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use 设备数据的权限。"):
            return True
        payload = services["cached_payload"](
            ("computer-use-devices", str(openclaw_dir)),
            2.0,
            lambda: {"devices": services["build_computer_use_devices_snapshot"](openclaw_dir)},
        )
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    if path == "/api/computer-use/runs":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use Run 列表的权限。"):
            return True
        query = handler._query()
        page = _parse_bounded_int(query.get("page", ["1"])[0], 1, minimum=1, maximum=1000)
        page_size = _parse_bounded_int(query.get("pageSize", ["20"])[0], 20, minimum=1, maximum=50)
        status = str(query.get("status", [""])[0] or "").strip()
        device_id = str(query.get("deviceId", [""])[0] or "").strip()
        thread_id = str(query.get("threadId", [""])[0] or "").strip()
        payload = services["cached_payload"](
            ("computer-use-runs", str(openclaw_dir), page, page_size, status, device_id, thread_id),
            2.0,
            lambda: {
                "runs": services["build_computer_use_runs_snapshot"](
                    openclaw_dir,
                    page=page,
                    page_size=page_size,
                    status=status,
                    device_id=device_id,
                    thread_id=thread_id,
                )
            },
        )
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    if path == "/api/computer-use/run":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use Run 详情的权限。"):
            return True
        run_id = str(handler._query().get("runId", [""])[0] or "").strip()
        if not run_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 runId。"}, status=400)
            return True
        try:
            payload = services["cached_payload"](
                ("computer-use-run", str(openclaw_dir), run_id),
                1.0,
                lambda: {"run": services["build_computer_use_run_snapshot"](openclaw_dir, run_id)},
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "computer_use_run_not_found", "message": str(error)}, status=404)
            return True
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    if path == "/api/computer-use/run/steps":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use Run 步骤的权限。"):
            return True
        query = handler._query()
        run_id = str(query.get("runId", [""])[0] or "").strip()
        if not run_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 runId。"}, status=400)
            return True
        page = _parse_bounded_int(query.get("page", ["1"])[0], 1, minimum=1, maximum=1000)
        page_size = _parse_bounded_int(query.get("pageSize", ["50"])[0], 50, minimum=1, maximum=50)
        try:
            payload = services["cached_payload"](
                ("computer-use-run-steps", str(openclaw_dir), run_id, page, page_size),
                1.0,
                lambda: {
                    "steps": services["build_computer_use_run_steps_snapshot"](
                        openclaw_dir,
                        run_id,
                        page=page,
                        page_size=page_size,
                    )
                },
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "computer_use_run_steps_failed", "message": str(error)}, status=404)
            return True
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    if path == "/api/computer-use/run/artifacts":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use Run 工件的权限。"):
            return True
        query = handler._query()
        run_id = str(query.get("runId", [""])[0] or "").strip()
        if not run_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 runId。"}, status=400)
            return True
        page = _parse_bounded_int(query.get("page", ["1"])[0], 1, minimum=1, maximum=1000)
        page_size = _parse_bounded_int(query.get("pageSize", ["30"])[0], 30, minimum=1, maximum=30)
        try:
            payload = services["cached_payload"](
                ("computer-use-run-artifacts", str(openclaw_dir), run_id, page, page_size),
                1.0,
                lambda: {
                    "artifacts": services["build_computer_use_run_artifacts_snapshot"](
                        openclaw_dir,
                        run_id,
                        page=page,
                        page_size=page_size,
                    )
                },
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "computer_use_run_artifacts_failed", "message": str(error)}, status=404)
            return True
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    if path == "/api/computer-use/artifact/content":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use 工件内容的权限。"):
            return True
        artifact_id = str(handler._query().get("artifactId", [""])[0] or "").strip()
        if not artifact_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 artifactId。"}, status=400)
            return True
        artifact = get_computer_artifact(openclaw_dir, artifact_id)
        if not isinstance(artifact, dict):
            handler._send_json({"ok": False, "error": "computer_use_artifact_not_found", "message": "未找到对应工件。"}, status=404)
            return True
        try:
            artifact_path = Path(str(artifact.get("path") or "")).expanduser().resolve()
        except Exception:
            artifact_path = None
        if artifact_path is None or not artifact_path.exists() or not artifact_path.is_file():
            handler._send_json({"ok": False, "error": "computer_use_artifact_missing", "message": "工件文件不存在。"}, status=404)
            return True
        handler._send_bytes(
            artifact_path.read_bytes(),
            str(artifact.get("mimeType") or "application/octet-stream"),
            extra_headers=[("Cache-Control", "no-store")],
        )
        return True

    if path == "/api/computer-use/run/preview":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use Run 预览的权限。"):
            return True
        query = handler._query()
        run_id = str(query.get("runId", [""])[0] or "").strip()
        if not run_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 runId。"}, status=400)
            return True
        try:
            payload = services["cached_payload"](
                ("computer-use-run-preview", str(openclaw_dir), run_id),
                1.0,
                lambda: {"preview": services["build_computer_use_run_preview_snapshot"](openclaw_dir, run_id)},
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "computer_use_run_preview_failed", "message": str(error)}, status=404)
            return True
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    if path == "/api/computer-use/run/actions":
        if not handler._require_capability("read", "当前账号没有访问 Computer Use Run 动作记录的权限。"):
            return True
        query = handler._query()
        run_id = str(query.get("runId", [""])[0] or "").strip()
        step_id = str(query.get("stepId", [""])[0] or "").strip()
        if not run_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 runId。"}, status=400)
            return True
        page = _parse_bounded_int(query.get("page", ["1"])[0], 1, minimum=1, maximum=1000)
        page_size = _parse_bounded_int(query.get("pageSize", ["50"])[0], 50, minimum=1, maximum=50)
        try:
            payload = services["cached_payload"](
                ("computer-use-run-actions", str(openclaw_dir), run_id, step_id, page, page_size),
                1.0,
                lambda: {
                    "actions": services["build_computer_use_run_actions_snapshot"](
                        openclaw_dir,
                        run_id,
                        step_id=step_id,
                        page=page,
                        page_size=page_size,
                    )
                },
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "computer_use_run_actions_failed", "message": str(error)}, status=404)
            return True
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True

    return False


def _handle_computer_use_commands(handler, path, payload, svc):
    payload = payload if isinstance(payload, dict) else {}

    if path == "/api/actions/computer-use/desktop/start":
        if not handler._require_capability("taskWrite", "当前账号没有启动 Desktop 会话的权限。"):
            return True
        session = _desktop_session_start(handler, payload)
        session_snapshot = _desktop_session_snapshot(session)
        handler._audit(
            "computer_use_desktop_start",
            detail=f"启动 Desktop 会话 {session_snapshot.get('sessionId', '')}",
            meta={"sessionId": session_snapshot.get("sessionId", "")},
        )
        handler._send_json({"ok": True, "message": "Desktop 会话已启动。", "session": session_snapshot})
        return True

    if path == "/api/actions/computer-use/desktop/stop":
        if not handler._require_capability("taskWrite", "当前账号没有结束 Desktop 会话的权限。"):
            return True
        session = _desktop_session_stop(handler, payload)
        session_snapshot = _desktop_session_snapshot(session)
        handler._audit(
            "computer_use_desktop_stop",
            detail=f"结束 Desktop 会话 {session_snapshot.get('sessionId', '')}",
            meta={"sessionId": session_snapshot.get("sessionId", "")},
        )
        handler._send_json({"ok": True, "message": "Desktop 会话已结束。", "session": session_snapshot})
        return True

    if path == "/api/actions/computer-use/desktop/action":
        if not handler._require_capability("taskWrite", "当前账号没有执行 Desktop 动作的权限。"):
            return True
        result = _desktop_session_relay_action(handler, payload)
        session_snapshot = _desktop_session_snapshot(result.get("session"))
        handler._audit(
            "computer_use_desktop_action",
            detail=f"执行 Desktop 动作 {((payload.get('action') if isinstance(payload.get('action'), dict) else payload) or {}).get('action') or ''}",
            meta={"sessionId": session_snapshot.get("sessionId", "")},
        )
        handler._send_json({"ok": True, "message": "Desktop 动作已执行。", "session": session_snapshot, "result": result.get("result")})
        return True

    if path == "/api/actions/computer-use/run/create":
        if not handler._require_capability("taskWrite", "当前账号没有创建 Computer Use Run 的权限。"):
            return True
        run = svc.perform_computer_use_run_create(
            handler.server.openclaw_dir,
            payload,
            actor=handler._current_actor(),
        )
        handler._audit(
            "computer_use_run_create",
            detail=f"创建 Computer Use Run {run.get('objective', run.get('id', ''))}",
            meta={
                "runId": run.get("id", ""),
                "deviceId": str((run.get("device") or {}).get("id") or run.get("deviceId") or "").strip(),
                "threadId": str(run.get("threadId") or "").strip(),
            },
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="created")
        handler._send_json({"ok": True, "message": "Computer Use Run 已创建。", "run": run})
        return True

    if path == "/api/actions/computer-use/run/approval/approve":
        if not handler._require_capability("taskWrite", "当前账号没有批准 Computer Use Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        run = svc.perform_computer_use_run_approve(
            handler.server.openclaw_dir,
            run_id,
            actor=handler._current_actor(),
            payload=payload,
        )
        handler._audit(
            "computer_use_run_approve",
            detail=f"批准 Computer Use Run {run.get('id', run_id)}",
            meta={"runId": run.get("id", run_id)},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="approved")
        handler._send_json({"ok": True, "message": "Computer Use Run 已批准。", "run": run})
        return True

    if path == "/api/actions/computer-use/run/pause":
        if not handler._require_capability("taskWrite", "当前账号没有暂停 Computer Use Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        run = svc.perform_computer_use_run_pause(
            handler.server.openclaw_dir,
            run_id,
            actor=handler._current_actor(),
        )
        handler._audit(
            "computer_use_run_pause",
            detail=f"暂停 Computer Use Run {run.get('id', run_id)}",
            meta={"runId": run.get("id", run_id)},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="paused")
        handler._send_json({"ok": True, "message": "Computer Use Run 已暂停。", "run": run})
        return True

    if path == "/api/actions/computer-use/run/resume":
        if not handler._require_capability("taskWrite", "当前账号没有恢复 Computer Use Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        run = svc.perform_computer_use_run_resume(
            handler.server.openclaw_dir,
            run_id,
            actor=handler._current_actor(),
        )
        handler._audit(
            "computer_use_run_resume",
            detail=f"恢复 Computer Use Run {run.get('id', run_id)}",
            meta={"runId": run.get("id", run_id)},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="resumed")
        handler._send_json({"ok": True, "message": "Computer Use Run 已恢复。", "run": run})
        return True

    if path == "/api/actions/computer-use/run/cancel":
        if not handler._require_capability("taskWrite", "当前账号没有取消 Computer Use Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        run = svc.perform_computer_use_run_cancel(
            handler.server.openclaw_dir,
            run_id,
            actor=handler._current_actor(),
        )
        handler._audit(
            "computer_use_run_cancel",
            detail=f"取消 Computer Use Run {run.get('id', run_id)}",
            meta={"runId": run.get("id", run_id)},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="canceled")
        handler._send_json({"ok": True, "message": "Computer Use Run 已取消。", "run": run})
        return True

    if path == "/api/actions/computer-use/run/execute":
        if not handler._require_capability("taskWrite", "当前账号没有执行 Computer Use Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        result = svc.perform_computer_use_run_execute(
            handler.server.openclaw_dir,
            run_id,
            payload=payload,
            actor=handler._current_actor(),
        )
        try:
            run = svc.build_computer_use_run_snapshot(handler.server.openclaw_dir, run_id)
        except Exception:
            run = {
                "id": run_id,
                "status": str(result.get("status") or "").strip(),
                "currentStepId": str(result.get("currentStepId") or "").strip(),
                "summary": {},
                "artifactSummary": {},
                "stepSummary": {},
            }
        try:
            handler._audit(
                "computer_use_run_execute",
                detail=f"执行 Computer Use Run {run.get('id', run_id)}",
                meta={"runId": run.get("id", run_id), "profile": str(result.get("profile") or "").strip()},
            )
        except Exception:
            pass
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run_id, stage="executed", include_preview=True)
        handler._send_json({"ok": True, "message": "Computer Use Run 已触发 browser bootstrap。", "result": result, "run": run})
        return True

    if path == "/api/actions/computer-use/run/takeover/start":
        if not handler._require_capability("taskWrite", "当前账号没有接管 Computer Use Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        run = svc.perform_computer_use_takeover_start(
            handler.server.openclaw_dir,
            run_id,
            actor=handler._current_actor(),
            payload=payload,
        )
        handler._audit(
            "computer_use_takeover_start",
            detail=f"开始接管 Computer Use Run {run.get('id', run_id)}",
            meta={"runId": run.get("id", run_id)},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="takeover_started", include_preview=True)
        handler._send_json({"ok": True, "message": "Computer Use Run 已进入接管状态。", "run": run})
        return True

    if path == "/api/actions/computer-use/run/takeover/stop":
        if not handler._require_capability("taskWrite", "当前账号没有结束 Computer Use Run 接管的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        run = svc.perform_computer_use_takeover_stop(
            handler.server.openclaw_dir,
            run_id,
            actor=handler._current_actor(),
            payload=payload,
        )
        handler._audit(
            "computer_use_takeover_stop",
            detail=f"结束接管 Computer Use Run {run.get('id', run_id)}",
            meta={"runId": run.get("id", run_id)},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="takeover_stopped", include_preview=True)
        handler._send_json({"ok": True, "message": "Computer Use Run 已退出接管状态。", "run": run})
        return True

    if path == "/api/actions/computer-use/run/takeover/action":
        if not handler._require_capability("taskWrite", "当前账号没有执行 Computer Use takeover action 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        result = svc.perform_computer_use_takeover_action(
            handler.server.openclaw_dir,
            run_id,
            payload=payload,
            actor=handler._current_actor(),
        )
        handler._audit(
            "computer_use_takeover_action",
            detail=f"执行 Computer Use takeover action {run_id}",
            meta={"runId": run_id, "action": str(((payload.get('action') if isinstance(payload.get('action'), dict) else payload) or {}).get('action') or '').strip()},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(
            handler,
            svc,
            run_id,
            stage="takeover_action",
            include_preview=True,
            action_entry=result.get("action") if isinstance(result.get("action"), dict) else None,
        )
        handler._send_json({"ok": True, "message": "Computer Use takeover action 已执行。", "result": result})
        return True

    if path == "/api/actions/computer-use/run/clarification/answer":
        if not handler._require_capability("taskWrite", "当前账号没有回答 Computer Use clarification 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            raise RuntimeError("需要 runId。")
        run = svc.perform_computer_use_answer_clarification(
            handler.server.openclaw_dir,
            run_id,
            payload=payload,
            actor=handler._current_actor(),
        )
        handler._audit(
            "computer_use_clarification_answer",
            detail=f"回答 Computer Use clarification {run.get('id', run_id)}",
            meta={"runId": run.get("id", run_id)},
        )
        _invalidate_computer_use_payloads(svc, handler.server.openclaw_dir)
        _publish_computer_use_run_sync(handler, svc, run.get("id"), stage="clarification_answered")
        handler._send_json({"ok": True, "message": "Computer Use clarification 已提交。", "run": run})
        return True

    return False
