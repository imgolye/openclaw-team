"""Tool Bridge — proxies tool execution between server and desktop clients.

When a chat thread uses a desktop-local workspace, the server cannot execute
tools (bash, file_read, glob, grep) locally because the files exist on the
user's machine, not on the server.  The ToolBridge solves this by:

1. Server-side dispatch puts a tool request into a thread-safe queue.
2. Desktop client polls ``GET /api/desktop/tool-bridge/poll`` to pick it up.
3. Desktop executes the tool locally and posts the result via
   ``POST /api/desktop/tool-bridge/result``.
4. Server-side dispatch receives the result and continues the agent loop.

All state is in-memory (no DB needed) — requests are transient and cleaned
up after completion or timeout.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)

# How long a dispatch worker will block-wait for a desktop response (seconds).
_DEFAULT_TIMEOUT = 180

# Stale request cleanup threshold (seconds).
_STALE_THRESHOLD = 300


class ToolBridgeManager:
    """Singleton in-memory queue for server ↔ desktop tool execution."""

    _instance: ToolBridgeManager | None = None
    _init_lock = threading.Lock()

    @classmethod
    def get(cls) -> ToolBridgeManager:
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        # request_id -> request dict
        self._pending: dict[str, dict[str, Any]] = {}
        # thread_id -> [request_id, ...]
        self._thread_requests: dict[str, list[str]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Server-side: called by RemoteToolExecutor in the dispatch worker
    # ------------------------------------------------------------------

    def request(
        self,
        thread_id: str,
        tool_name: str,
        tool_args: dict,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> dict[str, Any]:
        """Submit a tool request and block until the desktop responds.

        Returns the tool result dict, or an error dict on timeout.
        """
        req_id = uuid.uuid4().hex
        event = threading.Event()

        with self._lock:
            self._pending[req_id] = {
                "id": req_id,
                "threadId": thread_id,
                "toolName": tool_name,
                "toolArgs": tool_args,
                "event": event,
                "result": None,
                "createdAt": time.time(),
            }
            self._thread_requests.setdefault(thread_id, []).append(req_id)

        logger.info(
            "[tool-bridge] request %s queued: thread=%s tool=%s",
            req_id, thread_id, tool_name,
        )

        # Block the dispatch worker thread until desktop responds
        if not event.wait(timeout=timeout):
            logger.warning(
                "[tool-bridge] request %s timed out after %.0fs", req_id, timeout,
            )
            self._cleanup(req_id, thread_id)
            return {"error": f"Tool execution timed out ({timeout}s) — desktop client did not respond"}

        with self._lock:
            result = self._pending.get(req_id, {}).get("result") or {}

        self._cleanup(req_id, thread_id)
        logger.info("[tool-bridge] request %s completed: tool=%s", req_id, tool_name)
        return result

    # ------------------------------------------------------------------
    # Desktop-side: called by HTTP poll / result endpoints
    # ------------------------------------------------------------------

    def poll(self, thread_id: str) -> list[dict[str, Any]]:
        """Return pending tool requests for *thread_id* (desktop polls this)."""
        with self._lock:
            self._gc_stale()
            out: list[dict[str, Any]] = []
            for req_id in list(self._thread_requests.get(thread_id, [])):
                req = self._pending.get(req_id)
                if req and req["result"] is None:
                    out.append({
                        "id": req["id"],
                        "toolName": req["toolName"],
                        "toolArgs": req["toolArgs"],
                    })
            return out

    def submit_result(self, request_id: str, result: dict[str, Any]) -> bool:
        """Desktop posts the execution result back; unblocks the dispatch worker."""
        with self._lock:
            req = self._pending.get(request_id)
            if not req:
                logger.warning("[tool-bridge] submit_result for unknown request %s", request_id)
                return False
            req["result"] = result
            req["event"].set()
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def active_thread_ids(self) -> list[str]:
        """Return thread IDs that have pending requests (for diagnostics)."""
        with self._lock:
            return [tid for tid, rids in self._thread_requests.items() if rids]

    def _cleanup(self, req_id: str, thread_id: str) -> None:
        with self._lock:
            self._pending.pop(req_id, None)
            rids = self._thread_requests.get(thread_id, [])
            if req_id in rids:
                rids.remove(req_id)
            if not rids:
                self._thread_requests.pop(thread_id, None)

    def _gc_stale(self) -> None:
        """Remove requests that are older than *_STALE_THRESHOLD*."""
        now = time.time()
        stale = [
            (rid, req.get("threadId", ""))
            for rid, req in self._pending.items()
            if now - req.get("createdAt", now) > _STALE_THRESHOLD
        ]
        for rid, tid in stale:
            logger.info("[tool-bridge] gc stale request %s", rid)
            req = self._pending.get(rid)
            if req:
                req["result"] = {"error": "Request expired (stale)"}
                req["event"].set()
            self._pending.pop(rid, None)
            rids = self._thread_requests.get(tid, [])
            if rid in rids:
                rids.remove(rid)
