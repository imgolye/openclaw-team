"""Agent Harness — Hook lifecycle system.

Inspired by Claude Code's 104-hook architecture, adapted for OpenClaw Team's
multi-agent orchestration platform.

The HookRegistry provides a pub/sub lifecycle bus.  Any subsystem can register
handlers for specific phases, and the runtime emits events as agents, tools,
tasks, and routing decisions progress through their lifecycle.

Usage::

    from backend.domain.core.hooks import HookPhase, HookRegistry

    registry = HookRegistry.default()

    # Register a handler
    @registry.on(HookPhase.AFTER_TOOL_CALL)
    def log_tool_usage(ctx):
        print(f"Agent {ctx['agent_id']} called {ctx['tool_name']}")

    # Emit an event
    registry.emit(HookPhase.AFTER_TOOL_CALL, {
        "agent_id": "agent-eng-01",
        "tool_name": "bash",
        "duration_ms": 120,
    })
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


# ── Hook Phases ─────────────────────────────────────────────────────────


class HookPhase(str, Enum):
    """Lifecycle phases that can trigger hooks.

    Phases are grouped by subsystem for clarity.
    """

    # ── Tool lifecycle ──────────────────────────────────────────────
    BEFORE_TOOL_CALL = "before_tool_call"
    AFTER_TOOL_CALL = "after_tool_call"
    TOOL_PERMISSION_CHECK = "tool_permission_check"
    TOOL_ERROR = "tool_error"

    # ── Task lifecycle ──────────────────────────────────────────────
    TASK_CREATED = "task_created"
    TASK_STATE_CHANGE = "task_state_change"
    TASK_ASSIGNED = "task_assigned"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"

    # ── Routing ─────────────────────────────────────────────────────
    BEFORE_ROUTING = "before_routing"
    AFTER_ROUTING = "after_routing"
    ROUTING_FALLBACK = "routing_fallback"

    # ── Agent lifecycle ─────────────────────────────────────────────
    AGENT_STARTED = "agent_started"
    AGENT_STOPPED = "agent_stopped"
    AGENT_HEALTH_CHANGE = "agent_health_change"
    AGENT_DISPATCH = "agent_dispatch"
    AGENT_DISPATCH_COMPLETE = "agent_dispatch_complete"

    # ── Cost & resource ─────────────────────────────────────────────
    COST_EVENT = "cost_event"
    RATE_LIMIT_HIT = "rate_limit_hit"

    # ── Coordinator ─────────────────────────────────────────────────
    COORDINATOR_DECISION = "coordinator_decision"
    COORDINATOR_PLAN_CREATED = "coordinator_plan_created"
    COORDINATOR_VERIFY_START = "coordinator_verify_start"
    COORDINATOR_VERIFY_RESULT = "coordinator_verify_result"

    # ── System ──────────────────────────────────────────────────────
    ON_ERROR = "on_error"
    ON_STARTUP = "on_startup"
    ON_SHUTDOWN = "on_shutdown"


# ── Hook Context ────────────────────────────────────────────────────────


@dataclass
class HookContext:
    """Structured context passed to every hook handler.

    Provides metadata about the event and a mutable ``data`` dict for
    handler-specific payload.  Handlers can set ``cancelled = True`` to
    signal that a BEFORE_* phase should abort the operation.
    """

    phase: HookPhase
    timestamp: float = field(default_factory=time.time)
    data: dict[str, Any] = field(default_factory=dict)
    cancelled: bool = False
    cancel_reason: str = ""
    source: str = ""  # subsystem that emitted the event

    # Convenience accessors — safe for missing keys

    @property
    def agent_id(self) -> str | None:
        return self.data.get("agent_id")

    @property
    def tool_name(self) -> str | None:
        return self.data.get("tool_name")

    @property
    def task_id(self) -> str | None:
        return self.data.get("task_id")

    def cancel(self, reason: str = "") -> None:
        """Mark this context as cancelled — used by BEFORE_* hooks."""
        self.cancelled = True
        self.cancel_reason = reason


# ── Handler wrapper ─────────────────────────────────────────────────────


@dataclass
class _HookHandler:
    callback: Callable
    priority: int = 0          # lower = runs first
    name: str = ""
    once: bool = False
    _fired: bool = field(default=False, init=False, repr=False)


# ── HookRegistry ────────────────────────────────────────────────────────


class HookRegistry:
    """Central registry for lifecycle hooks.

    Supports both sync and async handlers.  Handlers are executed in
    priority order (lowest value first).  A handler registered with
    ``once=True`` will be automatically removed after its first invocation.
    """

    _default: Optional["HookRegistry"] = None

    def __init__(self) -> None:
        self._handlers: dict[HookPhase, list[_HookHandler]] = {}
        self._global_handlers: list[_HookHandler] = []  # fire on every phase
        self._emit_log: list[dict] = []  # recent events for debugging
        self._max_log_size: int = 500

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "HookRegistry":
        """Return (or create) the process-wide default registry."""
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        """Reset the default registry — primarily for testing."""
        cls._default = None

    # ── Registration ────────────────────────────────────────────────

    def register(
        self,
        phase: HookPhase,
        callback: Callable,
        *,
        priority: int = 0,
        name: str = "",
        once: bool = False,
    ) -> None:
        """Register a handler for a specific phase."""
        handler = _HookHandler(
            callback=callback,
            priority=priority,
            name=name or getattr(callback, "__name__", "anonymous"),
            once=once,
        )
        self._handlers.setdefault(phase, []).append(handler)
        self._handlers[phase].sort(key=lambda h: h.priority)

    def register_global(
        self,
        callback: Callable,
        *,
        priority: int = 0,
        name: str = "",
    ) -> None:
        """Register a handler that fires on *every* phase."""
        handler = _HookHandler(
            callback=callback,
            priority=priority,
            name=name or getattr(callback, "__name__", "anonymous"),
        )
        self._global_handlers.append(handler)
        self._global_handlers.sort(key=lambda h: h.priority)

    def on(
        self,
        phase: HookPhase,
        *,
        priority: int = 0,
        once: bool = False,
    ) -> Callable:
        """Decorator form of :meth:`register`."""
        def decorator(fn: Callable) -> Callable:
            self.register(phase, fn, priority=priority, name=fn.__name__, once=once)
            return fn
        return decorator

    def unregister(self, phase: HookPhase, callback: Callable) -> bool:
        """Remove a handler.  Returns True if found and removed."""
        handlers = self._handlers.get(phase, [])
        for i, h in enumerate(handlers):
            if h.callback is callback:
                handlers.pop(i)
                return True
        return False

    def clear(self, phase: HookPhase | None = None) -> None:
        """Remove all handlers for a phase, or all handlers if phase is None."""
        if phase is None:
            self._handlers.clear()
            self._global_handlers.clear()
        else:
            self._handlers.pop(phase, None)

    # ── Emission ────────────────────────────────────────────────────

    def emit(
        self,
        phase: HookPhase,
        data: dict[str, Any] | None = None,
        *,
        source: str = "",
    ) -> HookContext:
        """Emit an event synchronously.  Returns the context (check ``cancelled``)."""
        ctx = HookContext(phase=phase, data=data or {}, source=source)
        handlers = list(self._handlers.get(phase, [])) + list(self._global_handlers)
        handlers.sort(key=lambda h: h.priority)

        to_remove: list[tuple[HookPhase | None, _HookHandler]] = []

        for handler in handlers:
            try:
                result = handler.callback(ctx)
                # If the callback is a coroutine, warn — caller should use emit_async
                if asyncio.iscoroutine(result):
                    logger.warning(
                        "Hook %s returned a coroutine — use emit_async instead",
                        handler.name,
                    )
                    # Close the coroutine to prevent ResourceWarning
                    result.close()
            except Exception:
                logger.exception(
                    "Hook handler %r failed for phase %s", handler.name, phase.value,
                )

            if handler.once:
                if phase in self._handlers:
                    to_remove.append((phase, handler))
                else:
                    to_remove.append((None, handler))

            if ctx.cancelled:
                logger.info(
                    "Phase %s cancelled by %s: %s",
                    phase.value, handler.name, ctx.cancel_reason,
                )
                break

        # Remove once-handlers
        for phase_key, handler in to_remove:
            if phase_key is not None:
                try:
                    self._handlers[phase_key].remove(handler)
                except ValueError:
                    pass
            else:
                try:
                    self._global_handlers.remove(handler)
                except ValueError:
                    pass

        # Append to debug log
        self._record_emit(ctx)

        return ctx

    async def emit_async(
        self,
        phase: HookPhase,
        data: dict[str, Any] | None = None,
        *,
        source: str = "",
    ) -> HookContext:
        """Emit an event, awaiting async handlers."""
        ctx = HookContext(phase=phase, data=data or {}, source=source)
        handlers = list(self._handlers.get(phase, [])) + list(self._global_handlers)
        handlers.sort(key=lambda h: h.priority)

        to_remove: list[tuple[HookPhase | None, _HookHandler]] = []

        for handler in handlers:
            try:
                result = handler.callback(ctx)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception(
                    "Hook handler %r failed for phase %s", handler.name, phase.value,
                )

            if handler.once:
                if phase in self._handlers:
                    to_remove.append((phase, handler))
                else:
                    to_remove.append((None, handler))

            if ctx.cancelled:
                break

        for phase_key, handler in to_remove:
            if phase_key is not None:
                try:
                    self._handlers[phase_key].remove(handler)
                except ValueError:
                    pass
            else:
                try:
                    self._global_handlers.remove(handler)
                except ValueError:
                    pass

        self._record_emit(ctx)
        return ctx

    # ── Introspection ───────────────────────────────────────────────

    def handler_count(self, phase: HookPhase | None = None) -> int:
        """Number of registered handlers, optionally filtered by phase."""
        if phase is not None:
            return len(self._handlers.get(phase, []))
        return sum(len(hs) for hs in self._handlers.values()) + len(self._global_handlers)

    def list_handlers(self, phase: HookPhase) -> list[str]:
        """Return handler names for a phase."""
        return [h.name for h in self._handlers.get(phase, [])]

    @property
    def recent_events(self) -> list[dict]:
        """Return recent emitted events (for debug / dashboard)."""
        return list(self._emit_log)

    # ── Internal ────────────────────────────────────────────────────

    def _record_emit(self, ctx: HookContext) -> None:
        entry = {
            "phase": ctx.phase.value,
            "timestamp": ctx.timestamp,
            "source": ctx.source,
            "cancelled": ctx.cancelled,
            "agent_id": ctx.agent_id,
            "tool_name": ctx.tool_name,
            "task_id": ctx.task_id,
        }
        self._emit_log.append(entry)
        if len(self._emit_log) > self._max_log_size:
            self._emit_log = self._emit_log[-self._max_log_size:]

    def __repr__(self) -> str:
        total = self.handler_count()
        phases = len(self._handlers)
        return f"<HookRegistry handlers={total} phases={phases}>"
