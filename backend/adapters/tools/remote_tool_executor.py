"""Remote Tool Executor — proxies tool calls to a desktop client.

Instead of running bash/file_read/glob/grep on the server, this executor
puts the request into the ToolBridge queue and blocks until the desktop
client picks it up, executes it locally, and posts the result back.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from backend.domain.core.query_engine import ToolExecutor
from backend.domain.core.tool_bridge import ToolBridgeManager

logger = logging.getLogger(__name__)

# Tools that should execute on the desktop (workspace-dependent).
REMOTE_TOOLS = frozenset({"bash", "file_read", "file_write", "file_edit", "glob", "grep", "file_delete"})


class RemoteToolExecutor(ToolExecutor):
    """Proxies workspace-scoped tool execution to the desktop client via ToolBridge."""

    def __init__(self, *, thread_id: str, workspace_path: str = ""):
        self._thread_id = thread_id
        self._workspace = workspace_path

    @property
    def workspace(self) -> str:
        return self._workspace

    async def execute(
        self,
        tool_name: str,
        tool_args: dict,
        *,
        agent_id: str = "",
        context: dict | None = None,
    ) -> dict[str, Any]:
        """Queue tool for desktop execution and wait for the result."""
        if tool_name not in REMOTE_TOOLS:
            return {"error": f"Tool '{tool_name}' is not supported for remote execution"}

        bridge = ToolBridgeManager.get()

        # bridge.request() is blocking (threading.Event.wait), so run it in
        # a thread-pool executor to avoid blocking the asyncio event loop.
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            bridge.request,
            self._thread_id,
            tool_name,
            tool_args,
        )
        return result
