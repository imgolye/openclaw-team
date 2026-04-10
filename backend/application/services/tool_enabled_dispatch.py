"""Tool-enabled conversation dispatch via QueryEngine.

Replaces the single-turn ``openclaw gateway call chat.send`` subprocess
with a multi-turn recursive loop that supports tool calling.

The QueryEngine runs:  prompt → LLM → tool_use → execute → recurse
until the LLM produces a final text response or hits the turn limit.

Each step emits SSE events so the frontend can render tool activity
in real time.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Callable, Optional

from backend.domain.core.query_engine import (
    QueryConfig,
    QueryEngine,
    QueryEvent,
    QueryEventType,
)
from backend.adapters.llm.gateway_provider import GatewayLLMProvider, get_tool_schemas
from backend.adapters.tools.harness_executor import HarnessToolExecutor
from backend.domain.core.file_path_validator import FilePathValidator

logger = logging.getLogger(__name__)

# Default model for tool-enabled queries
DEFAULT_TOOL_MODEL = "claude-sonnet-4-20250514"


def build_tool_query_config(
    *,
    model: str = "",
    max_turns: int = 25,
    max_tokens: int = 8192,
    temperature: float = 0.0,
    thinking: str = "low",
    workspace_authorized: bool = False,
) -> QueryConfig:
    """Build a QueryConfig for tool-enabled conversation."""
    return QueryConfig(
        model=model or DEFAULT_TOOL_MODEL,
        max_turns=max_turns,
        max_tokens=max_tokens,
        temperature=temperature,
        auto_continue=True,
        auto_compact=True,
        parallel_tool_calls=True,
        workspace_authorized=workspace_authorized,
    )


def build_tool_system_prompt(
    *,
    agent_display_name: str = "",
    agent_job_title: str = "",
    workspace_path: str = "",
    team_context: str = "",
) -> str:
    """Build a system prompt for a tool-enabled agent."""
    parts = []

    identity = agent_display_name or "AI Assistant"
    if agent_job_title:
        identity = f"{identity} ({agent_job_title})"
    parts.append(f"You are {identity}.")

    if workspace_path:
        parts.append(f"Your workspace is: {workspace_path}")
        parts.append(
            "You have access to tools for reading, writing, and searching files "
            "within the workspace, as well as running shell commands. "
            "When the user asks about files, directories, code, or anything "
            "requiring system access, you MUST invoke the provided tools. "
            "Never output shell commands as plain text — always call the tool."
        )

    parts.append(
        "When using file tools, always use absolute paths within the workspace. "
        "For shell commands, be careful and avoid destructive operations."
    )

    if team_context:
        parts.append(f"\n{team_context}")

    return "\n\n".join(parts)


# ── Pending approval registry ─────────────────────────────────────────


class ToolApprovalRegistry:
    """Thread-safe registry for pending tool approval requests.

    When the QueryEngine encounters a tool that needs user approval,
    the dispatch loop stores the request here. The frontend polls or
    receives an SSE event and calls the approval endpoint to resolve it.
    """

    def __init__(self):
        self._pending: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def request_approval(
        self,
        request_id: str,
        tool_name: str,
        tool_args: dict,
        agent_id: str = "",
    ) -> bool:
        """Block until the user approves or denies.

        Returns True if approved, False if denied.
        """
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        async with self._lock:
            self._pending[request_id] = future
        try:
            return await asyncio.wait_for(future, timeout=300)  # 5 min timeout
        except asyncio.TimeoutError:
            return False
        finally:
            async with self._lock:
                self._pending.pop(request_id, None)

    async def resolve(self, request_id: str, approved: bool) -> bool:
        """Resolve a pending approval. Returns True if found."""
        async with self._lock:
            future = self._pending.get(request_id)
            if future and not future.done():
                future.set_result(approved)
                return True
        return False

    async def pending_count(self) -> int:
        async with self._lock:
            return len(self._pending)


# Global approval registry
_approval_registry = ToolApprovalRegistry()


def get_approval_registry() -> ToolApprovalRegistry:
    return _approval_registry


# ── Core dispatch ──────────────────────────────────────────────────────


async def perform_tool_enabled_conversation_send(
    *,
    query_engine: QueryEngine,
    agent_id: str,
    message: str,
    system_prompt: str = "",
    conversation_history: list[dict] | None = None,
    config: QueryConfig | None = None,
    tools: list[dict] | None = None,
    thread_id: str = "",
    stream_callback: Callable[[dict], None] | None = None,
) -> dict[str, Any]:
    """Run a tool-enabled conversation turn via QueryEngine.

    This is the async replacement for perform_conversation_send_via_gateway_call.
    It runs the full recursive loop and emits events through stream_callback.

    Parameters
    ----------
    query_engine : QueryEngine
        Wired engine with LLM provider, tool executor, permissions.
    agent_id : str
        Agent handling this conversation.
    message : str
        The user's message.
    system_prompt : str
        System prompt for the agent.
    conversation_history : list[dict]
        Prior messages in the conversation.
    config : QueryConfig
        Execution configuration (model, max_turns, etc.).
    tools : list[dict]
        Tool schemas. Defaults to all available tools.
    thread_id : str
        Chat thread ID for SSE event routing.
    stream_callback : Callable
        Called for each event (text delta, tool_call, tool_result, etc.).

    Returns
    -------
    dict with keys: status, reply_text, events, stats
    """
    config = config or build_tool_query_config()
    tools = tools or get_tool_schemas()
    query_id = f"toolquery-{uuid.uuid4().hex[:12]}"

    # Build messages list
    messages = list(conversation_history or [])
    messages.append({"role": "user", "content": message})

    reply_parts: list[str] = []
    all_events: list[dict] = []
    tool_call_count = 0
    start_time = time.time()

    def emit(event_type: str, **payload):
        """Emit an event to the stream callback."""
        event_data = {
            "type": event_type,
            "agentId": agent_id,
            "threadId": thread_id,
            "queryId": query_id,
            "at": time.time(),
            **payload,
        }
        all_events.append(event_data)
        if stream_callback:
            try:
                stream_callback(event_data)
            except Exception:
                logger.warning("Stream callback error", exc_info=True)

    emit("query_started", message=message[:200])

    try:
        async for event in query_engine.run(
            messages,
            system_prompt=system_prompt,
            config=config,
            agent_id=agent_id,
            query_id=query_id,
            tools=tools,
        ):
            if event.type == QueryEventType.TEXT:
                reply_parts.append(event.content)
                emit("delta", content=event.content, delta=event.content)

            elif event.type == QueryEventType.TOOL_CALL:
                tool_call_count += 1
                emit(
                    "tool_call",
                    toolName=event.tool_name,
                    toolArgs=event.tool_args,
                    turn=event.turn,
                )

            elif event.type == QueryEventType.TOOL_RESULT:
                result_preview = str(event.tool_result or "")[:500]
                emit(
                    "tool_result",
                    toolName=event.tool_name,
                    result=result_preview,
                    turn=event.turn,
                )

            elif event.type == QueryEventType.TOOL_ERROR:
                emit(
                    "tool_error",
                    toolName=event.tool_name,
                    error=event.error,
                    turn=event.turn,
                )

            elif event.type == QueryEventType.PERMISSION:
                # Tool needs user approval — emit event and wait
                request_id = f"approve-{uuid.uuid4().hex[:8]}"
                emit(
                    "permission_request",
                    requestId=request_id,
                    toolName=event.tool_name,
                    toolArgs=event.tool_args,
                    turn=event.turn,
                )
                # The approval flow is handled by the QueryEngine's
                # permission model. For now, we emit the event and
                # let the engine handle it via its internal logic.

            elif event.type == QueryEventType.COMPACT:
                emit("context_compacted", turn=event.turn)

            elif event.type == QueryEventType.RETRY:
                emit("retry", error=event.error, turn=event.turn)

            elif event.type == QueryEventType.COMPLETE:
                emit(
                    "query_completed",
                    stopReason=event.metadata.get("stop_reason", "end_turn"),
                    totalTurns=event.turn,
                )

            elif event.type == QueryEventType.ERROR:
                emit("query_error", error=event.error, turn=event.turn)

    except Exception as exc:
        logger.error("Tool-enabled query failed: %s", exc, exc_info=True)
        emit("query_error", error=str(exc))
        return {
            "status": "error",
            "reply_text": "",
            "error": str(exc),
            "events": all_events,
            "stats": {
                "tool_calls": tool_call_count,
                "duration_ms": (time.time() - start_time) * 1000,
            },
        }

    reply_text = "".join(reply_parts).strip()
    duration_ms = (time.time() - start_time) * 1000

    return {
        "status": "ok",
        "reply_text": reply_text,
        "events": all_events,
        "stats": {
            "tool_calls": tool_call_count,
            "duration_ms": duration_ms,
            "query_id": query_id,
        },
        "result": {
            "meta": {
                "agentMeta": {
                    "agentId": agent_id,
                    "queryId": query_id,
                    "toolEnabled": True,
                },
            },
            "payloads": [{"text": reply_text, "source": "query_engine"}],
            "transport": "query-engine",
        },
    }


# ── Synchronous wrapper for thread pool dispatch ────────────────────────


def perform_tool_enabled_conversation_send_sync(
    *,
    query_engine: QueryEngine,
    agent_id: str,
    message: str,
    system_prompt: str = "",
    conversation_history: list[dict] | None = None,
    config: QueryConfig | None = None,
    tools: list[dict] | None = None,
    thread_id: str = "",
    stream_callback: Callable[[dict], None] | None = None,
) -> dict[str, Any]:
    """Synchronous wrapper for use in ThreadPoolExecutor dispatch workers.

    The existing dispatch infrastructure (schedule_chat_thread_dispatch)
    runs workers in a ThreadPoolExecutor. This wrapper creates an event
    loop and runs the async dispatch function.
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(
            perform_tool_enabled_conversation_send(
                query_engine=query_engine,
                agent_id=agent_id,
                message=message,
                system_prompt=system_prompt,
                conversation_history=conversation_history,
                config=config,
                tools=tools,
                thread_id=thread_id,
                stream_callback=stream_callback,
            )
        )
    finally:
        loop.close()
