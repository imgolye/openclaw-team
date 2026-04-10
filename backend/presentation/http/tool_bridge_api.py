"""Tool Bridge HTTP API — endpoints for desktop ↔ server tool proxying.

GET  /api/desktop/tool-bridge/poll?threadId=xxx   — desktop polls for pending requests
POST /api/desktop/tool-bridge/result              — desktop submits execution result
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def handle_tool_bridge_route(handler, services, path):
    """Route tool-bridge API requests.  Returns True if handled."""
    method = handler.command

    # --- Poll for pending tool requests ---
    if path == "/api/desktop/tool-bridge/poll" and method == "GET":
        return _handle_poll(handler)

    # --- Submit tool execution result ---
    if path == "/api/desktop/tool-bridge/result" and method == "POST":
        return _handle_result(handler)

    return False


def _handle_poll(handler):
    """Desktop client polls for pending tool execution requests."""
    from backend.domain.core.tool_bridge import ToolBridgeManager

    # Parse threadId from query string
    query_string = handler.path.split("?", 1)[1] if "?" in handler.path else ""
    params = {}
    for pair in query_string.split("&"):
        if "=" in pair:
            k, v = pair.split("=", 1)
            params[k] = v

    thread_id = params.get("threadId", "").strip()
    if not thread_id:
        handler._send_json({"ok": False, "error": "Missing threadId"}, status=400)
        return True

    bridge = ToolBridgeManager.get()
    requests = bridge.poll(thread_id)

    handler._send_json({"ok": True, "requests": requests})
    return True


def _handle_result(handler):
    """Desktop client submits a tool execution result."""
    from backend.domain.core.tool_bridge import ToolBridgeManager

    try:
        content_length = int(handler.headers.get("Content-Length", 0))
        body = handler.rfile.read(content_length) if content_length else b""
        payload = json.loads(body) if body else {}
    except Exception:
        handler._send_json({"ok": False, "error": "Invalid JSON body"}, status=400)
        return True

    request_id = str(payload.get("requestId", "")).strip()
    result = payload.get("result", {})

    if not request_id:
        handler._send_json({"ok": False, "error": "Missing requestId"}, status=400)
        return True

    bridge = ToolBridgeManager.get()
    found = bridge.submit_result(request_id, result)

    if not found:
        handler._send_json({"ok": False, "error": "Request not found or already completed"}, status=404)
        return True

    handler._send_json({"ok": True})
    return True
