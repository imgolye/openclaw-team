"""Agent Harness — Query Engine (recursive execution loop).

Implements the core "prompt → LLM → tool_use → result → recurse" loop
inspired by Claude Code's query.ts (1730-line recursive generator).

The QueryEngine manages the full lifecycle of an agent turn:

1. Assemble context (system prompt, project memory, conversation history)
2. Call the LLM (via a pluggable provider)
3. Parse the response for tool_use blocks
4. Execute tool calls through the ToolRegistry + PermissionModel
5. Append results back into the conversation
6. Recurse until the LLM produces a final text response (no tool calls)

Supports:
- Auto-continuation when the LLM response is truncated
- Error retry with exponential backoff
- Context overflow detection with automatic compaction
- Per-turn cost tracking
- Configurable recursion depth limits

Usage::

    from backend.domain.core.query_engine import QueryEngine, QueryConfig

    engine = QueryEngine.default()
    config = QueryConfig(max_turns=25)

    async for event in engine.run(messages, config=config):
        if event.type == "text":
            print(event.content)
        elif event.type == "tool_call":
            print(f"Calling {event.tool_name}...")
        elif event.type == "complete":
            print("Done!")
"""

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional

logger = logging.getLogger(__name__)


# ── Enums ───────────────────────────────────────────────────────────────


class QueryEventType(str, Enum):
    """Types of events emitted during query execution."""

    TEXT = "text"                  # LLM produced text output
    TOOL_CALL = "tool_call"       # LLM wants to call a tool
    TOOL_RESULT = "tool_result"   # tool execution completed
    TOOL_ERROR = "tool_error"     # tool execution failed
    COMPACT = "compact"           # context was compacted
    RETRY = "retry"               # retrying after error
    PERMISSION = "permission"     # tool needs approval
    COMPLETE = "complete"         # query loop finished
    ERROR = "error"               # unrecoverable error
    TRUNCATED = "truncated"       # response was truncated, continuing


class StopReason(str, Enum):
    """Why the query loop stopped."""

    END_TURN = "end_turn"             # LLM ended naturally
    MAX_TURNS = "max_turns"           # hit recursion limit
    ERROR = "error"                   # unrecoverable error
    APPROVAL_NEEDED = "approval_needed"  # waiting for user approval
    CANCELLED = "cancelled"           # externally cancelled
    CONTEXT_OVERFLOW = "context_overflow"  # couldn't compact enough


# ── Configuration ──────────────────────────────────────────────────────


@dataclass
class QueryConfig:
    """Configuration for a query execution."""

    max_turns: int = 25                  # max recursion depth
    max_retries_per_turn: int = 3        # retries on transient errors
    retry_base_delay_ms: int = 1000      # exponential backoff base
    auto_continue: bool = True           # continue on truncated responses
    auto_compact: bool = True            # compact on context overflow
    compact_threshold: float = 0.80      # compact when usage > this
    timeout_per_turn_seconds: float = 120  # LLM call timeout
    parallel_tool_calls: bool = True     # execute independent tools in parallel
    model: str = "claude-sonnet-4-20250514"   # default model
    temperature: float = 0.0
    max_tokens: int = 8192               # max tokens per LLM response
    workspace_authorized: bool = False   # skip permission checks for workspace-scoped tools


# ── Events ─────────────────────────────────────────────────────────────


@dataclass
class QueryEvent:
    """An event emitted during query execution."""

    type: QueryEventType
    content: str = ""
    tool_name: str = ""
    tool_args: dict = field(default_factory=dict)
    tool_result: Any = None
    error: str = ""
    turn: int = 0
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = {
            "type": self.type.value,
            "turn": self.turn,
            "timestamp": self.timestamp,
        }
        if self.content:
            d["content"] = self.content
        if self.tool_name:
            d["tool_name"] = self.tool_name
        if self.error:
            d["error"] = self.error
        if self.metadata:
            d["metadata"] = self.metadata
        return d


# ── Turn Result ────────────────────────────────────────────────────────


@dataclass
class TurnResult:
    """Result of a single LLM turn."""

    text: str = ""
    tool_calls: list[dict] = field(default_factory=list)
    stop_reason: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    model: str = ""
    raw_response: Any = None

    @property
    def has_tool_calls(self) -> bool:
        return len(self.tool_calls) > 0

    @property
    def is_truncated(self) -> bool:
        return self.stop_reason == "max_tokens"


@dataclass
class QueryResult:
    """Final result of a complete query execution."""

    messages: list[dict]         # updated message history
    events: list[QueryEvent]     # all events during execution
    stop_reason: StopReason
    total_turns: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_tool_calls: int = 0
    duration_ms: float = 0.0
    compactions: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "stop_reason": self.stop_reason.value,
            "total_turns": self.total_turns,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tool_calls": self.total_tool_calls,
            "duration_ms": self.duration_ms,
            "compactions": self.compactions,
            "event_count": len(self.events),
        }


# ── Tool Executor Interface ────────────────────────────────────────────


class ToolExecutor:
    """Interface for executing tool calls.

    Subclass this to provide actual tool implementations.
    The default implementation returns a mock result.
    """

    async def execute(
        self,
        tool_name: str,
        tool_args: dict,
        *,
        agent_id: str = "",
        context: dict | None = None,
    ) -> dict[str, Any]:
        """Execute a tool and return the result.

        Returns a dict with at least {"result": ...} or {"error": ...}.
        """
        return {"result": f"[Mock] Tool '{tool_name}' executed", "mock": True}


# ── LLM Provider Interface ────────────────────────────────────────────


class LLMProvider:
    """Interface for calling the language model.

    Subclass this to connect to an actual LLM API.
    The default implementation returns a mock response.
    """

    async def complete(
        self,
        messages: list[dict],
        *,
        system: str = "",
        model: str = "claude-sonnet-4-20250514",
        max_tokens: int = 8192,
        temperature: float = 0.0,
        tools: list[dict] | None = None,
    ) -> TurnResult:
        """Call the LLM and return a TurnResult."""
        return TurnResult(
            text="[Mock LLM response]",
            stop_reason="end_turn",
            model=model,
        )


# ── Query Engine ───────────────────────────────────────────────────────


class QueryEngine:
    """Recursive query execution engine.

    Manages the full prompt → LLM → tool_use → result → recurse loop.
    Inspired by Claude Code's query.ts generator pattern.
    """

    _default: Optional["QueryEngine"] = None

    def __init__(
        self,
        *,
        llm_provider: LLMProvider | None = None,
        tool_executor: ToolExecutor | None = None,
    ) -> None:
        self._llm = llm_provider or LLMProvider()
        self._tool_executor = tool_executor or ToolExecutor()
        self._context_compactor = None
        self._permission_model = None
        self._cost_tracker = None
        self._hook_registry = None
        self._active_queries: dict[str, bool] = {}  # query_id -> running
        self._stats = {
            "total_queries": 0,
            "total_turns": 0,
            "total_tool_calls": 0,
            "total_compactions": 0,
        }

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "QueryEngine":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    # ── Wiring ──────────────────────────────────────────────────────

    def attach_compactor(self, compactor) -> None:
        self._context_compactor = compactor

    def attach_permission_model(self, model) -> None:
        self._permission_model = model

    def attach_cost_tracker(self, tracker) -> None:
        self._cost_tracker = tracker

    def attach_hooks(self, registry) -> None:
        self._hook_registry = registry

    def set_llm_provider(self, provider: LLMProvider) -> None:
        self._llm = provider

    def set_tool_executor(self, executor: ToolExecutor) -> None:
        self._tool_executor = executor

    # ── Main execution loop ─────────────────────────────────────────

    async def run(
        self,
        messages: list[dict],
        *,
        system_prompt: str = "",
        config: QueryConfig | None = None,
        agent_id: str = "",
        query_id: str = "",
        tools: list[dict] | None = None,
    ) -> AsyncIterator[QueryEvent]:
        """Run the recursive query loop.

        Yields QueryEvent objects as the execution progresses.
        The final event is always COMPLETE or ERROR.
        """
        config = config or QueryConfig()
        query_id = query_id or f"q-{int(time.time() * 1000)}"
        start_time = time.time()

        self._active_queries[query_id] = True
        self._stats["total_queries"] += 1

        events: list[QueryEvent] = []
        total_input = 0
        total_output = 0
        total_tools = 0
        compactions = 0

        try:
            for turn in range(config.max_turns):
                if not self._active_queries.get(query_id, False):
                    yield QueryEvent(type=QueryEventType.COMPLETE, turn=turn,
                                     content="Query cancelled")
                    return

                self._stats["total_turns"] += 1

                # ── Check context size, compact if needed ────────
                if config.auto_compact and self._context_compactor:
                    if self._context_compactor.should_compact(messages, model=config.model):
                        compact_result = self._context_compactor.compact(
                            messages, agent_id=agent_id,
                        )
                        messages = compact_result.compacted_messages
                        compactions += 1
                        self._stats["total_compactions"] += 1

                        evt = QueryEvent(
                            type=QueryEventType.COMPACT, turn=turn,
                            content=f"Compacted: {compact_result.original_count}→{compact_result.compacted_count} messages",
                            metadata=compact_result.to_dict(),
                        )
                        events.append(evt)
                        yield evt

                # ── Call LLM ─────────────────────────────────────
                turn_result = await self._call_llm_with_retry(
                    messages, system_prompt, config, turn, tools,
                )

                total_input += turn_result.input_tokens
                total_output += turn_result.output_tokens

                # Track cost
                if self._cost_tracker:
                    try:
                        self._cost_tracker.record_llm_call(
                            model=turn_result.model or config.model,
                            input_tokens=turn_result.input_tokens,
                            output_tokens=turn_result.output_tokens,
                            agent_id=agent_id,
                        )
                    except Exception:
                        logger.debug("Cost tracking failed", exc_info=True)

                # ── Handle text output ───────────────────────────
                if turn_result.text:
                    evt = QueryEvent(
                        type=QueryEventType.TEXT, turn=turn,
                        content=turn_result.text,
                    )
                    events.append(evt)
                    yield evt

                # ── Add assistant message (with tool_calls if present) ──
                if turn_result.text or turn_result.has_tool_calls:
                    assistant_msg: dict[str, Any] = {
                        "role": "assistant",
                        "content": turn_result.text or "",
                    }
                    if turn_result.has_tool_calls:
                        # Preserve tool_calls in the assistant message so
                        # OpenAI-compatible APIs can match tool results to
                        # their originating tool_call IDs.
                        assistant_msg["tool_calls"] = [
                            {
                                "id": tc.get("id", ""),
                                "type": "function",
                                "function": {
                                    "name": tc.get("name", ""),
                                    "arguments": (
                                        __import__("json").dumps(tc.get("input", {}))
                                        if isinstance(tc.get("input"), dict)
                                        else str(tc.get("input", "{}"))
                                    ),
                                },
                            }
                            for tc in turn_result.tool_calls
                        ]
                    messages.append(assistant_msg)

                # ── No tool calls → done ─────────────────────────
                if not turn_result.has_tool_calls:
                    # Check for truncation
                    if turn_result.is_truncated and config.auto_continue:
                        evt = QueryEvent(
                            type=QueryEventType.TRUNCATED, turn=turn,
                            content="Response truncated, continuing...",
                        )
                        events.append(evt)
                        yield evt
                        # Add continuation prompt
                        messages.append({
                            "role": "user",
                            "content": "[System: Your response was truncated. Please continue from where you left off.]",
                        })
                        continue
                    else:
                        # Natural end
                        evt = QueryEvent(
                            type=QueryEventType.COMPLETE, turn=turn,
                            content="Query completed",
                            metadata=QueryResult(
                                messages=messages,
                                events=events,
                                stop_reason=StopReason.END_TURN,
                                total_turns=turn + 1,
                                total_input_tokens=total_input,
                                total_output_tokens=total_output,
                                total_tool_calls=total_tools,
                                duration_ms=round((time.time() - start_time) * 1000, 1),
                                compactions=compactions,
                            ).to_dict(),
                        )
                        events.append(evt)
                        yield evt
                        return

                # ── Execute tool calls ───────────────────────────
                tool_results = await self._execute_tools(
                    turn_result.tool_calls, turn, config, agent_id, events,
                )

                for evt in tool_results["events"]:
                    yield evt

                total_tools += len(turn_result.tool_calls)
                self._stats["total_tool_calls"] += len(turn_result.tool_calls)

                # Check for approval needed
                if tool_results.get("needs_approval"):
                    evt = QueryEvent(
                        type=QueryEventType.PERMISSION, turn=turn,
                        content="Tool requires approval",
                        tool_name=tool_results["approval_tool"],
                    )
                    events.append(evt)
                    yield evt

                    # Pause loop — caller should resume after approval
                    evt = QueryEvent(
                        type=QueryEventType.COMPLETE, turn=turn,
                        content="Paused for approval",
                        metadata={"stop_reason": StopReason.APPROVAL_NEEDED.value},
                    )
                    events.append(evt)
                    yield evt
                    return

                # Add tool results to messages
                for tr in tool_results["results"]:
                    messages.append({
                        "role": "tool",
                        "name": tr["tool_name"],
                        "tool_use_id": tr.get("tool_use_id", ""),
                        "content": str(tr.get("result", tr.get("error", ""))),
                    })

            # ── Max turns reached ────────────────────────────────
            evt = QueryEvent(
                type=QueryEventType.COMPLETE, turn=config.max_turns,
                content=f"Reached max turns ({config.max_turns})",
                metadata={"stop_reason": StopReason.MAX_TURNS.value},
            )
            events.append(evt)
            yield evt

        except Exception as exc:
            evt = QueryEvent(
                type=QueryEventType.ERROR,
                content=f"Query engine error: {exc}",
                error=traceback.format_exc(),
            )
            events.append(evt)
            yield evt

        finally:
            self._active_queries.pop(query_id, None)

    # ── LLM call with retry ─────────────────────────────────────────

    async def _call_llm_with_retry(
        self,
        messages: list[dict],
        system_prompt: str,
        config: QueryConfig,
        turn: int,
        tools: list[dict] | None,
    ) -> TurnResult:
        """Call the LLM with exponential backoff retry."""
        last_error = None

        for attempt in range(config.max_retries_per_turn + 1):
            try:
                result = await asyncio.wait_for(
                    self._llm.complete(
                        messages,
                        system=system_prompt,
                        model=config.model,
                        max_tokens=config.max_tokens,
                        temperature=config.temperature,
                        tools=tools,
                    ),
                    timeout=config.timeout_per_turn_seconds,
                )
                return result
            except asyncio.TimeoutError:
                last_error = TimeoutError(
                    f"LLM call timed out after {config.timeout_per_turn_seconds}s"
                )
                logger.warning("LLM timeout on turn %d attempt %d", turn, attempt)
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "LLM error on turn %d attempt %d: %s",
                    turn, attempt, exc,
                )

            if attempt < config.max_retries_per_turn:
                delay = config.retry_base_delay_ms * (2 ** attempt) / 1000
                logger.info("Retrying in %.1fs...", delay)
                await asyncio.sleep(delay)

        raise last_error or RuntimeError("LLM call failed with no error details")

    # ── Tool execution ──────────────────────────────────────────────

    async def _execute_tools(
        self,
        tool_calls: list[dict],
        turn: int,
        config: QueryConfig,
        agent_id: str,
        events: list[QueryEvent],
    ) -> dict[str, Any]:
        """Execute tool calls, possibly in parallel."""
        results = []
        tool_events = []
        needs_approval = False
        approval_tool = ""

        for tc in tool_calls:
            tool_name = tc.get("name", "")
            tool_args = tc.get("input", tc.get("arguments", {}))
            tool_use_id = tc.get("id", "")

            # Emit tool_call event
            evt = QueryEvent(
                type=QueryEventType.TOOL_CALL, turn=turn,
                tool_name=tool_name, tool_args=tool_args,
            )
            events.append(evt)
            tool_events.append(evt)

            # Permission check (skip when workspace is fully authorized)
            if self._permission_model and not config.workspace_authorized:
                decision = self._permission_model.evaluate(
                    tool_name, agent_id=agent_id, context=tool_args,
                )
                if decision.needs_approval:
                    needs_approval = True
                    approval_tool = tool_name
                    results.append({
                        "tool_name": tool_name,
                        "tool_use_id": tool_use_id,
                        "error": f"Approval required for {tool_name}",
                    })
                    continue
                elif not decision.is_allowed:
                    evt = QueryEvent(
                        type=QueryEventType.TOOL_ERROR, turn=turn,
                        tool_name=tool_name,
                        error=f"Permission denied: {decision.reason}",
                    )
                    events.append(evt)
                    tool_events.append(evt)
                    results.append({
                        "tool_name": tool_name,
                        "tool_use_id": tool_use_id,
                        "error": f"Permission denied: {decision.reason}",
                    })
                    continue

            # Execute
            try:
                result = await self._tool_executor.execute(
                    tool_name, tool_args, agent_id=agent_id,
                )
                evt = QueryEvent(
                    type=QueryEventType.TOOL_RESULT, turn=turn,
                    tool_name=tool_name,
                    tool_result=result.get("result", ""),
                )
                events.append(evt)
                tool_events.append(evt)
                results.append({
                    "tool_name": tool_name,
                    "tool_use_id": tool_use_id,
                    "result": result.get("result", ""),
                })
            except Exception as exc:
                evt = QueryEvent(
                    type=QueryEventType.TOOL_ERROR, turn=turn,
                    tool_name=tool_name,
                    error=str(exc),
                )
                events.append(evt)
                tool_events.append(evt)
                results.append({
                    "tool_name": tool_name,
                    "tool_use_id": tool_use_id,
                    "error": str(exc),
                })

        return {
            "results": results,
            "events": tool_events,
            "needs_approval": needs_approval,
            "approval_tool": approval_tool,
        }

    # ── Cancel ──────────────────────────────────────────────────────

    def cancel(self, query_id: str) -> bool:
        """Cancel a running query."""
        if query_id in self._active_queries:
            self._active_queries[query_id] = False
            return True
        return False

    # ── Introspection ───────────────────────────────────────────────

    @property
    def active_query_count(self) -> int:
        return sum(1 for v in self._active_queries.values() if v)

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def __repr__(self) -> str:
        return (
            f"<QueryEngine active={self.active_query_count} "
            f"queries={self._stats['total_queries']}>"
        )
