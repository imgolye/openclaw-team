"""Agent Harness — Cost tracking system.

Records token consumption and estimated cost for every LLM interaction and
tool call.  Integrates with the Hook system via ``HookPhase.COST_EVENT``
and ``HookPhase.AFTER_TOOL_CALL``.

Usage::

    from backend.domain.core.cost_tracker import CostTracker

    tracker = CostTracker.default()

    # Record a cost event
    tracker.record(
        agent_id="agent-eng-01",
        model="claude-sonnet-4-20250514",
        input_tokens=1200,
        output_tokens=350,
        tool_name="bash",
        task_id="task-42",
    )

    # Query costs
    summary = tracker.get_agent_summary("agent-eng-01")
    daily = tracker.get_daily_burn_rate()
"""

from __future__ import annotations

import logging
import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ── Pricing table (USD per 1M tokens) ──────────────────────────────────

MODEL_PRICING: dict[str, dict[str, float]] = {
    # Claude models
    "claude-sonnet-4-20250514": {"input": 3.0, "output": 15.0},
    "claude-3-5-sonnet": {"input": 3.0, "output": 15.0},
    "claude-3-5-haiku": {"input": 0.80, "output": 4.0},
    "claude-3-opus": {"input": 15.0, "output": 75.0},
    "claude-opus-4-6": {"input": 15.0, "output": 75.0},
    # OpenAI models
    "gpt-4o": {"input": 2.50, "output": 10.0},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4-turbo": {"input": 10.0, "output": 30.0},
    "o1": {"input": 15.0, "output": 60.0},
    "o1-mini": {"input": 3.0, "output": 12.0},
    # Default fallback
    "_default": {"input": 3.0, "output": 15.0},
}


def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Estimate USD cost based on model pricing table."""
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["_default"])
    cost = (
        input_tokens * pricing["input"] / 1_000_000
        + output_tokens * pricing["output"] / 1_000_000
    )
    return round(cost, 6)


# ── Cost Event ──────────────────────────────────────────────────────────


@dataclass
class CostEvent:
    """A single cost-incurring event."""

    agent_id: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    timestamp: float = field(default_factory=time.time)
    tool_name: str = ""
    task_id: str = ""
    run_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)

    @property
    def date_key(self) -> str:
        """YYYY-MM-DD string for daily aggregation."""
        return self.datetime_utc.strftime("%Y-%m-%d")

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cost_usd": self.cost_usd,
            "total_tokens": self.total_tokens,
            "timestamp": self.timestamp,
            "tool_name": self.tool_name,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "date": self.date_key,
        }


# ── Summaries ───────────────────────────────────────────────────────────


@dataclass
class AgentCostSummary:
    """Aggregated cost summary for a single agent."""

    agent_id: str
    total_cost_usd: float = 0.0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_events: int = 0
    models_used: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    tools_used: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def total_tokens(self) -> int:
        return self.total_input_tokens + self.total_output_tokens

    @property
    def avg_cost_per_event(self) -> float:
        if self.total_events == 0:
            return 0.0
        return round(self.total_cost_usd / self.total_events, 6)

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_tokens,
            "total_events": self.total_events,
            "avg_cost_per_event": self.avg_cost_per_event,
            "models_used": dict(self.models_used),
            "tools_used": dict(self.tools_used),
        }


@dataclass
class DailyCostSummary:
    """Aggregated cost for a single day."""

    date: str  # YYYY-MM-DD
    total_cost_usd: float = 0.0
    total_tokens: int = 0
    total_events: int = 0
    by_agent: dict[str, float] = field(default_factory=lambda: defaultdict(float))

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "total_tokens": self.total_tokens,
            "total_events": self.total_events,
            "by_agent": dict(self.by_agent),
        }


# ── Cost Tracker ────────────────────────────────────────────────────────


class CostTracker:
    """Central cost tracking service.

    Thread-safe.  Maintains an in-memory event log with configurable max
    size.  Emits ``HookPhase.COST_EVENT`` on every record if a HookRegistry
    is attached.

    For persistence, attach a storage adapter via :meth:`set_storage` or
    register a ``COST_EVENT`` hook that writes to SQLite/PostgreSQL.
    """

    _default: Optional["CostTracker"] = None

    def __init__(self, *, max_events: int = 10_000) -> None:
        self._events: list[CostEvent] = []
        self._max_events = max_events
        self._lock = threading.Lock()
        self._hook_registry = None  # set via attach_hooks()

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "CostTracker":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    # ── Hook integration ────────────────────────────────────────────

    def attach_hooks(self, registry) -> None:
        """Attach to a HookRegistry to emit COST_EVENT on record."""
        self._hook_registry = registry

    # ── Recording ───────────────────────────────────────────────────

    def record(
        self,
        *,
        agent_id: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        tool_name: str = "",
        task_id: str = "",
        run_id: str = "",
        cost_usd: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> CostEvent:
        """Record a cost event and return it."""
        if cost_usd is None:
            cost_usd = estimate_cost(model, input_tokens, output_tokens)

        event = CostEvent(
            agent_id=agent_id,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
            tool_name=tool_name,
            task_id=task_id,
            run_id=run_id,
            metadata=metadata or {},
        )

        with self._lock:
            self._events.append(event)
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events:]

        # Emit hook
        if self._hook_registry is not None:
            try:
                from backend.domain.core.hooks import HookPhase
                self._hook_registry.emit(
                    HookPhase.COST_EVENT,
                    event.to_dict(),
                    source="cost_tracker",
                )
            except Exception:
                logger.exception("Failed to emit COST_EVENT hook")

        return event

    # ── Queries ─────────────────────────────────────────────────────

    def get_agent_summary(
        self,
        agent_id: str,
        *,
        since: float | None = None,
    ) -> AgentCostSummary:
        """Aggregate cost data for a specific agent."""
        summary = AgentCostSummary(agent_id=agent_id)
        with self._lock:
            for ev in self._events:
                if ev.agent_id != agent_id:
                    continue
                if since and ev.timestamp < since:
                    continue
                summary.total_cost_usd += ev.cost_usd
                summary.total_input_tokens += ev.input_tokens
                summary.total_output_tokens += ev.output_tokens
                summary.total_events += 1
                summary.models_used[ev.model] += 1
                if ev.tool_name:
                    summary.tools_used[ev.tool_name] += 1
        summary.total_cost_usd = round(summary.total_cost_usd, 6)
        return summary

    def get_task_cost(self, task_id: str) -> float:
        """Total cost attributed to a specific task."""
        total = 0.0
        with self._lock:
            for ev in self._events:
                if ev.task_id == task_id:
                    total += ev.cost_usd
        return round(total, 6)

    def get_daily_summary(self, date: str | None = None) -> DailyCostSummary:
        """Get cost summary for a specific day (default: today UTC)."""
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        summary = DailyCostSummary(date=date)
        with self._lock:
            for ev in self._events:
                if ev.date_key != date:
                    continue
                summary.total_cost_usd += ev.cost_usd
                summary.total_tokens += ev.total_tokens
                summary.total_events += 1
                summary.by_agent[ev.agent_id] += ev.cost_usd
        summary.total_cost_usd = round(summary.total_cost_usd, 6)
        return summary

    def get_daily_burn_rate(self, days: int = 7) -> float:
        """Average daily cost over the last N days."""
        now = time.time()
        cutoff = now - days * 86400
        total = 0.0
        with self._lock:
            for ev in self._events:
                if ev.timestamp >= cutoff:
                    total += ev.cost_usd
        return round(total / max(days, 1), 4)

    def get_all_agent_summaries(
        self,
        *,
        since: float | None = None,
    ) -> list[AgentCostSummary]:
        """Summaries for all agents with recorded events."""
        agent_ids: set[str] = set()
        with self._lock:
            for ev in self._events:
                if since and ev.timestamp < since:
                    continue
                agent_ids.add(ev.agent_id)
        return [self.get_agent_summary(aid, since=since) for aid in sorted(agent_ids)]

    def get_recent_events(self, limit: int = 50) -> list[dict]:
        """Return recent events as dicts (for API / dashboard)."""
        with self._lock:
            return [ev.to_dict() for ev in self._events[-limit:]]

    # ── Totals ──────────────────────────────────────────────────────

    @property
    def total_cost(self) -> float:
        with self._lock:
            return round(sum(ev.cost_usd for ev in self._events), 4)

    @property
    def total_tokens(self) -> int:
        with self._lock:
            return sum(ev.total_tokens for ev in self._events)

    @property
    def event_count(self) -> int:
        with self._lock:
            return len(self._events)

    def __repr__(self) -> str:
        return (
            f"<CostTracker events={self.event_count} "
            f"total=${self.total_cost:.4f}>"
        )
