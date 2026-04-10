"""Agent Harness — Sub-agent dispatch system.

Implements the AgentTool pattern from Claude Code: treating sub-agent
invocation as a first-class tool call.  The Dispatcher manages the
lifecycle of delegated work — from dispatch through execution to result
collection and merging.

Usage::

    from backend.domain.core.agent_dispatch import AgentDispatcher, AgentCapability

    dispatcher = AgentDispatcher.default()

    # Register agent capabilities
    dispatcher.register_agent("eng-01", [
        AgentCapability.EXECUTION,
        AgentCapability.REVIEW,
    ])

    # Dispatch a task
    handle = dispatcher.dispatch(
        capability=AgentCapability.EXECUTION,
        task_description="Implement the Hook system",
        context={"files": ["hooks.py"]},
        parent_agent="coordinator",
    )

    # Parallel dispatch
    handles = dispatcher.dispatch_parallel([
        {"capability": AgentCapability.EXECUTION, "task": "Build frontend"},
        {"capability": AgentCapability.EXECUTION, "task": "Build backend"},
    ])
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


# ── Agent Capabilities ──────────────────────────────────────────────────


class AgentCapability(str, Enum):
    """What an agent can do — used for capability-based dispatch."""

    PLANNING = "planning"             # task decomposition, architecture
    EXECUTION = "execution"           # build, implement, create
    VERIFICATION = "verification"     # test, validate, check
    EXPLORATION = "exploration"       # research, search, gather info
    REVIEW = "review"                 # code review, doc review
    COMMUNICATION = "communication"   # chat, notifications, reports
    ANALYSIS = "analysis"             # data analysis, metrics
    OPERATIONS = "operations"         # deploy, monitor, maintain


# ── Dispatch Status ─────────────────────────────────────────────────────


class DispatchStatus(str, Enum):
    """Lifecycle status of a dispatched task."""

    QUEUED = "queued"
    DISPATCHED = "dispatched"
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class MergeStrategy(str, Enum):
    """How to combine results from parallel dispatches."""

    CONCAT = "concat"                 # concatenate all results
    FIRST_SUCCESS = "first_success"   # return first successful result
    VOTE = "vote"                     # majority vote (for verification)
    AGGREGATE = "aggregate"           # structured aggregation
    CUSTOM = "custom"                 # caller provides merge function


# ── Data structures ─────────────────────────────────────────────────────


@dataclass
class AgentProfile:
    """Describes an agent's capabilities and status."""

    agent_id: str
    capabilities: list[AgentCapability] = field(default_factory=list)
    max_concurrent: int = 3
    current_load: int = 0
    is_available: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_capacity(self) -> bool:
        return self.is_available and self.current_load < self.max_concurrent

    def supports(self, capability: AgentCapability) -> bool:
        return capability in self.capabilities

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "capabilities": [c.value for c in self.capabilities],
            "max_concurrent": self.max_concurrent,
            "current_load": self.current_load,
            "is_available": self.is_available,
            "has_capacity": self.has_capacity,
        }


@dataclass
class DispatchHandle:
    """A handle to a dispatched task — used to track and collect results."""

    id: str = field(default_factory=lambda: f"dsp-{uuid.uuid4().hex[:8]}")
    agent_id: str = ""
    capability: AgentCapability = AgentCapability.EXECUTION
    task_description: str = ""
    parent_agent: str = ""
    status: DispatchStatus = DispatchStatus.QUEUED
    result: Any = None
    error: str = ""
    created_at: float = field(default_factory=time.time)
    started_at: float = 0.0
    completed_at: float = 0.0
    context: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        if self.started_at and self.completed_at:
            return round((self.completed_at - self.started_at) * 1000, 1)
        return 0.0

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            DispatchStatus.COMPLETED,
            DispatchStatus.FAILED,
            DispatchStatus.TIMEOUT,
            DispatchStatus.CANCELLED,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "agent_id": self.agent_id,
            "capability": self.capability.value,
            "task_description": self.task_description[:200],
            "parent_agent": self.parent_agent,
            "status": self.status.value,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "created_at": self.created_at,
        }


@dataclass
class MergedResult:
    """Result of merging multiple dispatch results."""

    handles: list[DispatchHandle]
    strategy: MergeStrategy
    merged_output: Any = None
    all_succeeded: bool = False
    success_count: int = 0
    failure_count: int = 0
    total_duration_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "all_succeeded": self.all_succeeded,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "total_duration_ms": self.total_duration_ms,
            "handles": [h.to_dict() for h in self.handles],
        }


# ── Agent Dispatcher ────────────────────────────────────────────────────


class AgentDispatcher:
    """Manages the dispatch of tasks to sub-agents.

    The dispatcher maintains a registry of agent profiles and their
    capabilities, selects the best agent for each task, and manages
    the dispatch lifecycle.

    Integration points:
    - Hook system: emits AGENT_DISPATCH and AGENT_DISPATCH_COMPLETE
    - Cost tracker: records dispatch events for cost attribution
    - Coordinator: uses dispatcher for executing subtasks
    """

    _default: Optional["AgentDispatcher"] = None

    def __init__(self) -> None:
        self._agents: dict[str, AgentProfile] = {}
        self._active_dispatches: dict[str, DispatchHandle] = {}
        self._completed_dispatches: list[DispatchHandle] = []
        self._max_completed: int = 500
        self._hook_registry = None
        self._on_dispatch_callback: Callable | None = None  # integration point

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "AgentDispatcher":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    # ── Wiring ──────────────────────────────────────────────────────

    def attach_hooks(self, registry) -> None:
        self._hook_registry = registry

    def set_dispatch_callback(self, callback: Callable) -> None:
        """Set the actual dispatch implementation.

        The callback receives (agent_id, task_description, context) and
        should return the result or raise on failure.  This is the
        integration point with OpenClaw Team's existing agent
        communication (e.g., perform_conversation_fanout).
        """
        self._on_dispatch_callback = callback

    # ── Agent registration ──────────────────────────────────────────

    def register_agent(
        self,
        agent_id: str,
        capabilities: list[AgentCapability],
        *,
        max_concurrent: int = 3,
        metadata: dict[str, Any] | None = None,
    ) -> AgentProfile:
        """Register an agent with its capabilities."""
        profile = AgentProfile(
            agent_id=agent_id,
            capabilities=capabilities,
            max_concurrent=max_concurrent,
            metadata=metadata or {},
        )
        self._agents[agent_id] = profile
        logger.info(
            "Registered agent %s with capabilities: %s",
            agent_id,
            [c.value for c in capabilities],
        )
        return profile

    def unregister_agent(self, agent_id: str) -> bool:
        return self._agents.pop(agent_id, None) is not None

    def get_agent(self, agent_id: str) -> AgentProfile | None:
        return self._agents.get(agent_id)

    def set_agent_availability(self, agent_id: str, available: bool) -> None:
        if agent_id in self._agents:
            self._agents[agent_id].is_available = available

    # ── Dispatch ────────────────────────────────────────────────────

    def dispatch(
        self,
        *,
        capability: AgentCapability,
        task_description: str,
        context: dict[str, Any] | None = None,
        parent_agent: str = "",
        preferred_agent: str = "",
    ) -> DispatchHandle:
        """Dispatch a task to the best available agent.

        Selection priority:
        1. preferred_agent (if specified and capable)
        2. Agent with matching capability and lowest load
        3. Any available agent (fallback)
        """
        # Select agent
        agent_id = self._select_agent(capability, preferred_agent)

        handle = DispatchHandle(
            agent_id=agent_id,
            capability=capability,
            task_description=task_description,
            parent_agent=parent_agent,
            context=context or {},
        )

        if not agent_id:
            handle.status = DispatchStatus.FAILED
            handle.error = f"No agent available for capability: {capability.value}"
            self._completed_dispatches.append(handle)
            return handle

        # Update load
        profile = self._agents.get(agent_id)
        if profile:
            profile.current_load += 1

        handle.status = DispatchStatus.DISPATCHED
        self._active_dispatches[handle.id] = handle

        # Emit hook
        self._emit_dispatch_hook(handle)

        logger.info(
            "Dispatched %s to %s: %s",
            handle.id, agent_id, task_description[:80],
        )

        return handle

    def dispatch_parallel(
        self,
        tasks: list[dict[str, Any]],
        *,
        merge_strategy: MergeStrategy = MergeStrategy.CONCAT,
        parent_agent: str = "",
    ) -> list[DispatchHandle]:
        """Dispatch multiple tasks in parallel.

        Each task dict should have:
        - capability: AgentCapability or str
        - task_description: str (or "task": str)
        - context: dict (optional)
        - preferred_agent: str (optional)
        """
        handles = []
        for task_spec in tasks:
            cap = task_spec.get("capability", AgentCapability.EXECUTION)
            if isinstance(cap, str):
                try:
                    cap = AgentCapability(cap)
                except ValueError:
                    cap = AgentCapability.EXECUTION

            handle = self.dispatch(
                capability=cap,
                task_description=task_spec.get("task_description", task_spec.get("task", "")),
                context=task_spec.get("context"),
                parent_agent=parent_agent,
                preferred_agent=task_spec.get("preferred_agent", ""),
            )
            handles.append(handle)

        return handles

    # ── Result collection ───────────────────────────────────────────

    def report_started(self, dispatch_id: str) -> DispatchHandle | None:
        """Agent reports it has started working on the task."""
        handle = self._active_dispatches.get(dispatch_id)
        if handle:
            handle.status = DispatchStatus.IN_PROGRESS
            handle.started_at = time.time()
        return handle

    def report_result(
        self,
        dispatch_id: str,
        *,
        result: Any = None,
        error: str = "",
    ) -> DispatchHandle | None:
        """Agent reports the result of a dispatched task."""
        handle = self._active_dispatches.pop(dispatch_id, None)
        if handle is None:
            logger.warning("Result reported for unknown dispatch: %s", dispatch_id)
            return None

        handle.completed_at = time.time()
        if not handle.started_at:
            handle.started_at = handle.created_at

        if error:
            handle.status = DispatchStatus.FAILED
            handle.error = error
        else:
            handle.status = DispatchStatus.COMPLETED
            handle.result = result

        # Update load
        profile = self._agents.get(handle.agent_id)
        if profile and profile.current_load > 0:
            profile.current_load -= 1

        # Archive
        self._completed_dispatches.append(handle)
        if len(self._completed_dispatches) > self._max_completed:
            self._completed_dispatches = self._completed_dispatches[-self._max_completed:]

        # Emit hook
        self._emit_completion_hook(handle)

        return handle

    def cancel_dispatch(self, dispatch_id: str, reason: str = "") -> bool:
        """Cancel an active dispatch."""
        handle = self._active_dispatches.pop(dispatch_id, None)
        if handle is None:
            return False

        handle.status = DispatchStatus.CANCELLED
        handle.error = reason or "Cancelled"
        handle.completed_at = time.time()

        profile = self._agents.get(handle.agent_id)
        if profile and profile.current_load > 0:
            profile.current_load -= 1

        self._completed_dispatches.append(handle)
        return True

    # ── Merging ─────────────────────────────────────────────────────

    def merge_results(
        self,
        handles: list[DispatchHandle],
        *,
        strategy: MergeStrategy = MergeStrategy.CONCAT,
        custom_merger: Callable | None = None,
    ) -> MergedResult:
        """Merge results from multiple dispatches."""
        completed = [h for h in handles if h.status == DispatchStatus.COMPLETED]
        failed = [h for h in handles if h.status == DispatchStatus.FAILED]

        merged = MergedResult(
            handles=handles,
            strategy=strategy,
            success_count=len(completed),
            failure_count=len(failed),
            all_succeeded=len(failed) == 0 and len(completed) == len(handles),
            total_duration_ms=sum(h.duration_ms for h in handles),
        )

        if strategy == MergeStrategy.CONCAT:
            merged.merged_output = [h.result for h in completed]

        elif strategy == MergeStrategy.FIRST_SUCCESS:
            merged.merged_output = completed[0].result if completed else None

        elif strategy == MergeStrategy.VOTE:
            # Simple majority
            from collections import Counter
            votes = Counter(str(h.result) for h in completed)
            if votes:
                merged.merged_output = votes.most_common(1)[0][0]

        elif strategy == MergeStrategy.AGGREGATE:
            merged.merged_output = {
                h.agent_id: h.result for h in completed
            }

        elif strategy == MergeStrategy.CUSTOM and custom_merger:
            try:
                merged.merged_output = custom_merger([h.result for h in completed])
            except Exception as e:
                logger.exception("Custom merger failed")
                merged.merged_output = None

        return merged

    # ── Agent selection ─────────────────────────────────────────────

    def _select_agent(
        self,
        capability: AgentCapability,
        preferred: str = "",
    ) -> str:
        """Select the best agent for a capability."""
        # Preferred agent
        if preferred:
            profile = self._agents.get(preferred)
            if profile and profile.supports(capability) and profile.has_capacity:
                return preferred

        # Capability match with lowest load
        candidates = [
            profile for profile in self._agents.values()
            if profile.supports(capability) and profile.has_capacity
        ]

        if candidates:
            candidates.sort(key=lambda p: p.current_load)
            return candidates[0].agent_id

        # Fallback: any available agent
        fallbacks = [
            profile for profile in self._agents.values()
            if profile.has_capacity
        ]
        if fallbacks:
            fallbacks.sort(key=lambda p: p.current_load)
            return fallbacks[0].agent_id

        return ""

    # ── Queries ─────────────────────────────────────────────────────

    def list_agents(self, capability: AgentCapability | None = None) -> list[AgentProfile]:
        """List agents, optionally filtered by capability."""
        agents = list(self._agents.values())
        if capability:
            agents = [a for a in agents if a.supports(capability)]
        return sorted(agents, key=lambda a: a.agent_id)

    def list_active_dispatches(self) -> list[DispatchHandle]:
        return list(self._active_dispatches.values())

    def list_recent_completed(self, limit: int = 20) -> list[DispatchHandle]:
        return self._completed_dispatches[-limit:]

    @property
    def active_count(self) -> int:
        return len(self._active_dispatches)

    @property
    def total_completed(self) -> int:
        return len(self._completed_dispatches)

    # ── Hook emission ───────────────────────────────────────────────

    def _emit_dispatch_hook(self, handle: DispatchHandle) -> None:
        if self._hook_registry is None:
            return
        try:
            from backend.domain.core.hooks import HookPhase
            self._hook_registry.emit(
                HookPhase.AGENT_DISPATCH,
                handle.to_dict(),
                source="agent_dispatcher",
            )
        except Exception:
            logger.exception("Failed to emit AGENT_DISPATCH hook")

    def _emit_completion_hook(self, handle: DispatchHandle) -> None:
        if self._hook_registry is None:
            return
        try:
            from backend.domain.core.hooks import HookPhase
            self._hook_registry.emit(
                HookPhase.AGENT_DISPATCH_COMPLETE,
                handle.to_dict(),
                source="agent_dispatcher",
            )
        except Exception:
            logger.exception("Failed to emit AGENT_DISPATCH_COMPLETE hook")

    def __repr__(self) -> str:
        return (
            f"<AgentDispatcher agents={len(self._agents)} "
            f"active={self.active_count} completed={self.total_completed}>"
        )
