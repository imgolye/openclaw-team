"""Agent Harness — Coordinator decision engine.

The Coordinator sits above individual agents and provides intelligent
task decomposition, strategy selection, and result aggregation.

Inspired by Claude Code's Coordinator-Assistant separation where:
- Coordinator handles high-level strategy (what to do, who does it)
- Assistants handle execution (how to do it)

In OpenClaw Team's context, the Coordinator enhances the existing
multi-agent routing by adding:
1. Intent analysis — understanding *what* the user/system wants
2. Strategy selection — deciding the execution pattern
3. Plan decomposition — breaking complex tasks into subtasks
4. Verification — checking results before delivery
5. Aggregation — merging results from parallel agents

Usage::

    from backend.domain.core.coordinator import AgentCoordinator

    coordinator = AgentCoordinator.default()

    # Process a task through the coordinator
    decision = coordinator.decide(
        intent="Build a dashboard for sales metrics",
        available_agents=["eng-01", "design-01", "qa-01"],
        context={"project": "mission-control"},
    )

    # Execute the decision
    result = coordinator.execute(decision)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ── Execution strategies ────────────────────────────────────────────────


class ExecutionStrategy(str, Enum):
    """How the Coordinator will execute a task."""

    DIRECT = "direct"                 # single agent, straightforward
    PLAN_THEN_EXECUTE = "plan_then_execute"  # decompose → assign → merge
    FAN_OUT = "fan_out"               # parallel dispatch to multiple agents
    SEQUENTIAL_PIPELINE = "sequential_pipeline"  # ordered chain A → B → C
    VERIFY_LOOP = "verify_loop"       # execute → verify → retry if needed
    ESCALATE = "escalate"             # too complex, needs human


class TaskComplexity(str, Enum):
    """Estimated complexity for strategy selection."""

    TRIVIAL = "trivial"     # single step, obvious agent
    SIMPLE = "simple"       # 1-2 steps, one agent
    MODERATE = "moderate"   # multi-step, may need 2+ agents
    COMPLEX = "complex"     # needs planning, multi-agent, verification
    CRITICAL = "critical"   # high stakes, requires human oversight


class SubTaskStatus(str, Enum):
    """Status of a coordinator-managed subtask."""

    PENDING = "pending"
    ASSIGNED = "assigned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


# ── Data structures ────────────────────────────────────────────────────


@dataclass
class IntentAnalysis:
    """Result of analyzing user/system intent."""

    raw_input: str
    intent_type: str = ""       # e.g., "build", "fix", "analyze", "deploy"
    domain: str = ""            # e.g., "frontend", "backend", "data", "ops"
    complexity: TaskComplexity = TaskComplexity.SIMPLE
    requires_planning: bool = False
    requires_verification: bool = False
    keywords: list[str] = field(default_factory=list)
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_input": self.raw_input[:200],
            "intent_type": self.intent_type,
            "domain": self.domain,
            "complexity": self.complexity.value,
            "requires_planning": self.requires_planning,
            "requires_verification": self.requires_verification,
            "keywords": self.keywords,
            "confidence": self.confidence,
        }


@dataclass
class SubTask:
    """A decomposed unit of work within a coordinator plan."""

    id: str
    description: str
    assigned_agent: str = ""
    capability_required: str = ""   # maps to AgentCapability
    status: SubTaskStatus = SubTaskStatus.PENDING
    depends_on: list[str] = field(default_factory=list)
    result: Any = None
    error: str = ""
    started_at: float = 0.0
    completed_at: float = 0.0

    @property
    def duration_ms(self) -> float:
        if self.started_at and self.completed_at:
            return round((self.completed_at - self.started_at) * 1000, 1)
        return 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "description": self.description,
            "assigned_agent": self.assigned_agent,
            "capability_required": self.capability_required,
            "status": self.status.value,
            "depends_on": self.depends_on,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


@dataclass
class CoordinatorDecision:
    """The Coordinator's decision on how to handle a task."""

    strategy: ExecutionStrategy
    intent: IntentAnalysis
    subtasks: list[SubTask] = field(default_factory=list)
    selected_agents: list[str] = field(default_factory=list)
    requires_verification: bool = False
    requires_approval: bool = False
    estimated_steps: int = 1
    reasoning: str = ""
    created_at: float = field(default_factory=time.time)

    @property
    def is_multi_agent(self) -> bool:
        return len(self.selected_agents) > 1

    @property
    def subtask_count(self) -> int:
        return len(self.subtasks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "intent": self.intent.to_dict(),
            "subtasks": [st.to_dict() for st in self.subtasks],
            "selected_agents": self.selected_agents,
            "requires_verification": self.requires_verification,
            "requires_approval": self.requires_approval,
            "estimated_steps": self.estimated_steps,
            "reasoning": self.reasoning,
            "is_multi_agent": self.is_multi_agent,
        }


@dataclass
class CoordinatorResult:
    """Final result after the Coordinator has executed a decision."""

    decision: CoordinatorDecision
    success: bool = False
    outputs: list[Any] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    total_duration_ms: float = 0.0
    verification_passed: bool | None = None
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "strategy": self.decision.strategy.value,
            "subtask_results": [st.to_dict() for st in self.decision.subtasks],
            "errors": self.errors,
            "total_duration_ms": self.total_duration_ms,
            "verification_passed": self.verification_passed,
            "summary": self.summary,
        }


# ── Intent analysis helpers ─────────────────────────────────────────────

# Keyword-to-intent mapping for rule-based analysis
_INTENT_KEYWORDS: dict[str, list[str]] = {
    "build": ["build", "create", "implement", "develop", "add", "new", "构建", "创建", "开发", "新增"],
    "fix": ["fix", "debug", "repair", "resolve", "patch", "修复", "调试", "修改"],
    "analyze": ["analyze", "review", "audit", "check", "inspect", "分析", "审查", "检查"],
    "deploy": ["deploy", "release", "publish", "ship", "部署", "发布", "上线"],
    "refactor": ["refactor", "restructure", "optimize", "improve", "重构", "优化", "改进"],
    "test": ["test", "verify", "validate", "测试", "验证"],
    "document": ["document", "describe", "explain", "文档", "说明"],
    "research": ["research", "investigate", "explore", "study", "调研", "研究", "探索"],
}

_DOMAIN_KEYWORDS: dict[str, list[str]] = {
    "frontend": ["frontend", "ui", "ux", "react", "component", "page", "view", "css", "前端", "界面", "页面"],
    "backend": ["backend", "api", "server", "database", "sql", "后端", "接口", "数据库"],
    "ops": ["deploy", "monitor", "alert", "health", "docker", "ci", "cd", "运维", "监控", "告警"],
    "data": ["data", "analytics", "report", "metrics", "chart", "数据", "报表", "分析"],
    "design": ["design", "layout", "theme", "color", "typography", "设计", "布局", "主题"],
}


def analyze_intent(text: str) -> IntentAnalysis:
    """Rule-based intent analysis.

    This provides a baseline.  In production, this can be enhanced with
    LLM-based classification or the existing semantic scoring from
    ``task_routing.py``.
    """
    text_lower = text.lower()
    analysis = IntentAnalysis(raw_input=text)

    # Detect intent type
    best_intent = ""
    best_score = 0
    for intent, keywords in _INTENT_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > best_score:
            best_score = score
            best_intent = intent
    analysis.intent_type = best_intent or "general"

    # Detect domain
    best_domain = ""
    best_domain_score = 0
    for domain, keywords in _DOMAIN_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > best_domain_score:
            best_domain_score = score
            best_domain = domain
    analysis.domain = best_domain or "general"

    # Extract keywords
    all_keywords = []
    for keywords in list(_INTENT_KEYWORDS.values()) + list(_DOMAIN_KEYWORDS.values()):
        for kw in keywords:
            if kw in text_lower and len(kw) > 2:
                all_keywords.append(kw)
    analysis.keywords = list(set(all_keywords))

    # Estimate complexity
    word_count = len(text.split())
    if word_count < 10:
        analysis.complexity = TaskComplexity.TRIVIAL
    elif word_count < 30:
        analysis.complexity = TaskComplexity.SIMPLE
    elif word_count < 80:
        analysis.complexity = TaskComplexity.MODERATE
    else:
        analysis.complexity = TaskComplexity.COMPLEX

    # Heuristics for planning/verification
    planning_signals = ["plan", "break down", "step by step", "phases", "拆解", "分步", "阶段"]
    verify_signals = ["verify", "test", "check", "validate", "确认", "验证", "校验"]

    analysis.requires_planning = (
        analysis.complexity in (TaskComplexity.MODERATE, TaskComplexity.COMPLEX)
        or any(s in text_lower for s in planning_signals)
    )
    analysis.requires_verification = (
        analysis.complexity in (TaskComplexity.COMPLEX, TaskComplexity.CRITICAL)
        or analysis.intent_type in ("build", "fix", "deploy")
        or any(s in text_lower for s in verify_signals)
    )

    analysis.confidence = min(1.0, (best_score + best_domain_score) * 0.2 + 0.3)

    return analysis


# ── Coordinator ─────────────────────────────────────────────────────────


class AgentCoordinator:
    """Decision engine that sits above individual agents.

    Responsibilities:
    1. Analyze intent of incoming tasks
    2. Choose execution strategy
    3. Decompose into subtasks if needed
    4. Select agents for each subtask
    5. Coordinate execution and merge results
    """

    _default: Optional["AgentCoordinator"] = None

    def __init__(self) -> None:
        self._hook_registry = None
        self._tool_registry = None
        self._cost_tracker = None
        self._agent_capabilities: dict[str, list[str]] = {}
        self._decision_log: list[dict] = []
        self._max_log_size: int = 200

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "AgentCoordinator":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    # ── Wiring ──────────────────────────────────────────────────────

    def attach_hooks(self, registry) -> None:
        self._hook_registry = registry

    def attach_tool_registry(self, registry) -> None:
        self._tool_registry = registry

    def attach_cost_tracker(self, tracker) -> None:
        self._cost_tracker = tracker

    def register_agent_capabilities(
        self,
        agent_id: str,
        capabilities: list[str],
    ) -> None:
        """Tell the coordinator what each agent can do."""
        self._agent_capabilities[agent_id] = capabilities

    # ── Core decision flow ──────────────────────────────────────────

    def decide(
        self,
        intent: str,
        *,
        available_agents: list[str] | None = None,
        context: dict[str, Any] | None = None,
    ) -> CoordinatorDecision:
        """Analyze intent and produce an execution decision.

        This is the main entry point.  It:
        1. Analyzes the intent
        2. Selects a strategy
        3. Decomposes subtasks (if needed)
        4. Selects agents
        """
        analysis = analyze_intent(intent)

        # Select strategy
        strategy = self._select_strategy(analysis)

        # Build decision
        decision = CoordinatorDecision(
            strategy=strategy,
            intent=analysis,
            requires_verification=analysis.requires_verification,
        )

        # Decompose if needed
        if strategy in (
            ExecutionStrategy.PLAN_THEN_EXECUTE,
            ExecutionStrategy.FAN_OUT,
            ExecutionStrategy.SEQUENTIAL_PIPELINE,
        ):
            decision.subtasks = self._decompose(analysis, context or {})
            decision.estimated_steps = len(decision.subtasks)
        else:
            decision.estimated_steps = 1

        # Select agents
        agents = available_agents or list(self._agent_capabilities.keys())
        decision.selected_agents = self._select_agents(
            analysis, decision.subtasks, agents,
        )

        # Assign agents to subtasks
        self._assign_agents_to_subtasks(decision)

        # Generate reasoning
        decision.reasoning = self._generate_reasoning(decision)

        # Emit hook
        self._emit_decision_hook(decision)

        # Log
        self._log_decision(decision)

        return decision

    # ── Strategy selection ──────────────────────────────────────────

    def _select_strategy(self, analysis: IntentAnalysis) -> ExecutionStrategy:
        """Choose the best execution strategy based on intent analysis."""

        if analysis.complexity == TaskComplexity.TRIVIAL:
            return ExecutionStrategy.DIRECT

        if analysis.complexity == TaskComplexity.SIMPLE:
            if analysis.requires_verification:
                return ExecutionStrategy.VERIFY_LOOP
            return ExecutionStrategy.DIRECT

        if analysis.complexity == TaskComplexity.CRITICAL:
            return ExecutionStrategy.ESCALATE

        # Moderate or Complex
        if analysis.requires_planning:
            return ExecutionStrategy.PLAN_THEN_EXECUTE

        if analysis.domain in ("ops", "data"):
            return ExecutionStrategy.SEQUENTIAL_PIPELINE

        return ExecutionStrategy.FAN_OUT

    # ── Task decomposition ──────────────────────────────────────────

    def _decompose(
        self,
        analysis: IntentAnalysis,
        context: dict[str, Any],
    ) -> list[SubTask]:
        """Break a complex task into subtasks.

        This is a rule-based decomposition.  For production use, integrate
        with LLM-based planning (the planAgent pattern from Claude Code).
        """
        subtasks: list[SubTask] = []

        # Planning phase
        if analysis.requires_planning:
            subtasks.append(SubTask(
                id="plan",
                description=f"Create execution plan for: {analysis.raw_input[:100]}",
                capability_required="planning",
            ))

        # Main execution — based on intent type
        if analysis.intent_type == "build":
            subtasks.append(SubTask(
                id="implement",
                description=f"Implement: {analysis.raw_input[:100]}",
                capability_required="execution",
                depends_on=["plan"] if analysis.requires_planning else [],
            ))

        elif analysis.intent_type == "fix":
            subtasks.append(SubTask(
                id="diagnose",
                description="Diagnose the issue",
                capability_required="exploration",
                depends_on=["plan"] if analysis.requires_planning else [],
            ))
            subtasks.append(SubTask(
                id="fix",
                description=f"Apply fix: {analysis.raw_input[:100]}",
                capability_required="execution",
                depends_on=["diagnose"],
            ))

        elif analysis.intent_type == "analyze":
            subtasks.append(SubTask(
                id="explore",
                description=f"Gather information: {analysis.raw_input[:100]}",
                capability_required="exploration",
            ))
            subtasks.append(SubTask(
                id="analyze",
                description="Synthesize findings into report",
                capability_required="review",
                depends_on=["explore"],
            ))

        else:
            subtasks.append(SubTask(
                id="execute",
                description=analysis.raw_input[:200],
                capability_required="execution",
                depends_on=["plan"] if analysis.requires_planning else [],
            ))

        # Verification phase
        if analysis.requires_verification:
            execution_ids = [
                st.id for st in subtasks
                if st.capability_required in ("execution", "review")
            ]
            subtasks.append(SubTask(
                id="verify",
                description="Verify deliverables and run checks",
                capability_required="verification",
                depends_on=execution_ids or [subtasks[-1].id] if subtasks else [],
            ))

        return subtasks

    # ── Agent selection ─────────────────────────────────────────────

    def _select_agents(
        self,
        analysis: IntentAnalysis,
        subtasks: list[SubTask],
        available: list[str],
    ) -> list[str]:
        """Select agents based on capabilities and task requirements."""
        if not available:
            return []

        if not subtasks:
            # Direct execution — pick best single agent
            return [self._best_agent_for_capability("execution", available)]

        # Collect required capabilities
        required = set(st.capability_required for st in subtasks if st.capability_required)

        selected = set()
        for cap in required:
            agent = self._best_agent_for_capability(cap, available)
            if agent:
                selected.add(agent)

        return sorted(selected) if selected else [available[0]]

    def _best_agent_for_capability(
        self,
        capability: str,
        available: list[str],
    ) -> str:
        """Find the best agent for a given capability."""
        # Check registered capabilities
        for agent_id in available:
            caps = self._agent_capabilities.get(agent_id, [])
            if capability in caps:
                return agent_id

        # Fallback: first available
        return available[0] if available else ""

    def _assign_agents_to_subtasks(self, decision: CoordinatorDecision) -> None:
        """Assign specific agents to subtasks based on capabilities."""
        for subtask in decision.subtasks:
            if subtask.assigned_agent:
                continue
            agent = self._best_agent_for_capability(
                subtask.capability_required,
                decision.selected_agents,
            )
            subtask.assigned_agent = agent

    # ── Reasoning ───────────────────────────────────────────────────

    def _generate_reasoning(self, decision: CoordinatorDecision) -> str:
        """Generate human-readable reasoning for the decision."""
        parts = []
        intent = decision.intent

        parts.append(
            f"Intent: {intent.intent_type} ({intent.domain}), "
            f"complexity={intent.complexity.value}"
        )
        parts.append(f"Strategy: {decision.strategy.value}")

        if decision.subtasks:
            task_summary = ", ".join(
                f"{st.id}→{st.assigned_agent}" for st in decision.subtasks
            )
            parts.append(f"Subtasks: {task_summary}")

        if decision.requires_verification:
            parts.append("Verification: required")

        return " | ".join(parts)

    # ── Subtask lifecycle management ────────────────────────────────

    def start_subtask(self, decision: CoordinatorDecision, subtask_id: str) -> SubTask | None:
        """Mark a subtask as in-progress."""
        for st in decision.subtasks:
            if st.id == subtask_id:
                st.status = SubTaskStatus.IN_PROGRESS
                st.started_at = time.time()
                return st
        return None

    def complete_subtask(
        self,
        decision: CoordinatorDecision,
        subtask_id: str,
        *,
        result: Any = None,
        error: str = "",
    ) -> SubTask | None:
        """Mark a subtask as completed or failed."""
        for st in decision.subtasks:
            if st.id == subtask_id:
                st.completed_at = time.time()
                if error:
                    st.status = SubTaskStatus.FAILED
                    st.error = error
                else:
                    st.status = SubTaskStatus.COMPLETED
                    st.result = result
                return st
        return None

    def get_ready_subtasks(self, decision: CoordinatorDecision) -> list[SubTask]:
        """Return subtasks whose dependencies are all completed."""
        completed_ids = {
            st.id for st in decision.subtasks
            if st.status == SubTaskStatus.COMPLETED
        }
        ready = []
        for st in decision.subtasks:
            if st.status != SubTaskStatus.PENDING:
                continue
            if all(dep in completed_ids for dep in st.depends_on):
                ready.append(st)
        return ready

    def is_decision_complete(self, decision: CoordinatorDecision) -> bool:
        """Check if all subtasks are done (completed or failed)."""
        terminal = {SubTaskStatus.COMPLETED, SubTaskStatus.FAILED, SubTaskStatus.SKIPPED}
        return all(st.status in terminal for st in decision.subtasks)

    # ── Hook emission ───────────────────────────────────────────────

    def _emit_decision_hook(self, decision: CoordinatorDecision) -> None:
        if self._hook_registry is None:
            return
        try:
            from backend.domain.core.hooks import HookPhase
            self._hook_registry.emit(
                HookPhase.COORDINATOR_DECISION,
                decision.to_dict(),
                source="coordinator",
            )
        except Exception:
            logger.exception("Failed to emit COORDINATOR_DECISION hook")

    # ── Logging ─────────────────────────────────────────────────────

    def _log_decision(self, decision: CoordinatorDecision) -> None:
        entry = {
            "timestamp": decision.created_at,
            "strategy": decision.strategy.value,
            "intent_type": decision.intent.intent_type,
            "domain": decision.intent.domain,
            "complexity": decision.intent.complexity.value,
            "agents": decision.selected_agents,
            "subtask_count": decision.subtask_count,
        }
        self._decision_log.append(entry)
        if len(self._decision_log) > self._max_log_size:
            self._decision_log = self._decision_log[-self._max_log_size:]

    @property
    def recent_decisions(self) -> list[dict]:
        return list(self._decision_log)

    # ── Introspection ───────────────────────────────────────────────

    def get_agent_capabilities(self) -> dict[str, list[str]]:
        return dict(self._agent_capabilities)

    def __repr__(self) -> str:
        agents = len(self._agent_capabilities)
        decisions = len(self._decision_log)
        return f"<AgentCoordinator agents={agents} decisions={decisions}>"
