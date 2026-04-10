"""Agent Harness bootstrap — wires all harness subsystems together.

Call ``bootstrap_harness()`` at application startup to initialize and
connect all harness subsystems:

- Hook system (pub/sub event bus)
- Cost Tracker (token/cost tracking)
- Tool Registry (tool permission gating)
- Coordinator (task routing & decomposition)
- Agent Dispatcher (sub-agent management)
- Context Compactor (context window compaction)
- Project Memory (CONTEXT.md discovery)
- Permission Model (progressive 3-tier permissions)
- Query Engine (recursive execution loop)

This module is the single integration point between the harness layer
and the rest of OpenClaw Team.

Usage::

    from backend.application.services.harness_bootstrap import bootstrap_harness

    harness = bootstrap_harness()
    # harness.hooks, harness.cost_tracker, harness.tool_registry, etc.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from backend.domain.core.hooks import HookPhase, HookContext, HookRegistry
from backend.domain.core.cost_tracker import CostTracker
from backend.domain.core.tool_registry import ToolRegistry
from backend.domain.core.coordinator import AgentCoordinator
from backend.domain.core.agent_dispatch import AgentCapability, AgentDispatcher
from backend.domain.core.context_compact import ContextCompactor
from backend.domain.core.project_memory import ProjectMemory
from backend.domain.core.permission_model import PermissionModel
from backend.domain.core.query_engine import QueryEngine
from backend.domain.core.file_path_validator import FilePathValidator
from backend.adapters.tools.harness_executor import HarnessToolExecutor
from backend.adapters.llm.gateway_provider import GatewayLLMProvider

logger = logging.getLogger(__name__)


# ── Harness container ──────────────────────────────────────────────────


@dataclass
class AgentHarness:
    """Container holding all harness subsystem instances."""

    hooks: HookRegistry
    cost_tracker: CostTracker
    tool_registry: ToolRegistry
    coordinator: AgentCoordinator
    dispatcher: AgentDispatcher
    compactor: ContextCompactor
    project_memory: ProjectMemory
    permission_model: PermissionModel
    query_engine: QueryEngine
    llm_provider: GatewayLLMProvider | None = None
    tool_executor: HarnessToolExecutor | None = None
    path_validator: FilePathValidator | None = None

    def to_dict(self) -> dict:
        return {
            "hooks": repr(self.hooks),
            "cost_tracker": repr(self.cost_tracker),
            "tool_registry": repr(self.tool_registry),
            "coordinator": repr(self.coordinator),
            "dispatcher": repr(self.dispatcher),
            "compactor": repr(self.compactor),
            "project_memory": repr(self.project_memory),
            "permission_model": repr(self.permission_model),
            "query_engine": repr(self.query_engine),
            "llm_provider": repr(self.llm_provider),
            "tool_executor": repr(self.tool_executor),
            "path_validator": repr(self.path_validator),
        }


# ── Default hooks ──────────────────────────────────────────────────────


def _register_audit_hooks(hooks: HookRegistry) -> None:
    """Register default audit / observability hooks."""

    @hooks.on(HookPhase.AFTER_TOOL_CALL, priority=100)
    def audit_tool_call(ctx: HookContext) -> None:
        """Log every tool call for audit trail."""
        logger.info(
            "[audit] tool_call agent=%s tool=%s task=%s",
            ctx.agent_id or "unknown",
            ctx.tool_name or "unknown",
            ctx.task_id or "-",
        )

    @hooks.on(HookPhase.TASK_STATE_CHANGE, priority=100)
    def audit_task_change(ctx: HookContext) -> None:
        """Log task state transitions."""
        logger.info(
            "[audit] task_state task=%s from=%s to=%s agent=%s",
            ctx.data.get("task_id", "?"),
            ctx.data.get("from_state", "?"),
            ctx.data.get("to_state", "?"),
            ctx.agent_id or "-",
        )

    @hooks.on(HookPhase.COORDINATOR_DECISION, priority=100)
    def audit_coordinator(ctx: HookContext) -> None:
        """Log coordinator decisions."""
        logger.info(
            "[audit] coordinator strategy=%s agents=%s subtasks=%d",
            ctx.data.get("strategy", "?"),
            ctx.data.get("selected_agents", []),
            ctx.data.get("subtask_count", 0),
        )

    @hooks.on(HookPhase.ON_ERROR, priority=0)
    def log_errors(ctx: HookContext) -> None:
        """Ensure errors are always logged with high priority."""
        logger.error(
            "[harness] error source=%s agent=%s: %s",
            ctx.source or "unknown",
            ctx.agent_id or "unknown",
            ctx.data.get("error", "unknown error"),
        )


def _register_cost_hooks(hooks: HookRegistry, tracker: CostTracker) -> None:
    """Wire cost tracking into the hook system."""

    @hooks.on(HookPhase.AFTER_TOOL_CALL, priority=50)
    def track_tool_cost(ctx: HookContext) -> None:
        """Record cost for tool calls that involve LLM usage."""
        if not ctx.data.get("model"):
            return  # not an LLM call
        tracker.record(
            agent_id=ctx.data.get("agent_id", "unknown"),
            model=ctx.data.get("model", "unknown"),
            input_tokens=ctx.data.get("input_tokens", 0),
            output_tokens=ctx.data.get("output_tokens", 0),
            tool_name=ctx.data.get("tool_name", ""),
            task_id=ctx.data.get("task_id", ""),
            run_id=ctx.data.get("run_id", ""),
        )


def _register_safety_hooks(hooks: HookRegistry, tools: ToolRegistry) -> None:
    """Register safety-oriented hooks for dangerous operations."""

    @hooks.on(HookPhase.BEFORE_TOOL_CALL, priority=0)
    def check_tool_permission(ctx: HookContext) -> None:
        """Gate tool calls through the permission system."""
        tool_name = ctx.data.get("tool_name")
        if not tool_name:
            return

        result = tools.check_permission(
            tool_name,
            agent_role=ctx.data.get("agent_role", ""),
            mode=ctx.data.get("mode", "coordinated"),
            tenant_id=ctx.data.get("tenant_id", ""),
        )

        if not result.allowed:
            ctx.cancel(f"Permission denied: {result.reason}")

        if result.requires_approval:
            ctx.data["requires_approval"] = True
            ctx.data["risk_level"] = result.risk_level.value


def _register_agent_health_hooks(hooks: HookRegistry) -> None:
    """Track agent health events."""

    @hooks.on(HookPhase.AGENT_DISPATCH_COMPLETE, priority=80)
    def track_dispatch_health(ctx: HookContext) -> None:
        """Monitor dispatch success/failure rates."""
        status = ctx.data.get("status", "")
        agent_id = ctx.data.get("agent_id", "")
        if status == "failed":
            logger.warning(
                "[health] dispatch failed for agent=%s: %s",
                agent_id,
                ctx.data.get("error", "unknown"),
            )

    @hooks.on(HookPhase.RATE_LIMIT_HIT, priority=0)
    def handle_rate_limit(ctx: HookContext) -> None:
        """Log rate limit events for monitoring."""
        logger.warning(
            "[health] rate_limit agent=%s model=%s",
            ctx.agent_id or "unknown",
            ctx.data.get("model", "unknown"),
        )


# ── Default agent capabilities ──────────────────────────────────────────

# Maps OpenClaw Team theme roles to harness capabilities
DEFAULT_ROLE_CAPABILITIES: dict[str, list[AgentCapability]] = {
    # Corporate theme roles
    "project-coordinator": [
        AgentCapability.PLANNING,
        AgentCapability.COMMUNICATION,
    ],
    "product-strategist": [
        AgentCapability.PLANNING,
        AgentCapability.ANALYSIS,
    ],
    "engineering": [
        AgentCapability.EXECUTION,
        AgentCapability.REVIEW,
        AgentCapability.EXPLORATION,
    ],
    "qa": [
        AgentCapability.VERIFICATION,
        AgentCapability.EXPLORATION,
    ],
    "design": [
        AgentCapability.EXECUTION,
        AgentCapability.REVIEW,
    ],
    "operations": [
        AgentCapability.OPERATIONS,
        AgentCapability.ANALYSIS,
    ],
    # Generic fallback
    "general": [
        AgentCapability.EXECUTION,
        AgentCapability.EXPLORATION,
    ],
}


# ── Bootstrap ──────────────────────────────────────────────────────────


def bootstrap_harness(
    *,
    agent_roles: dict[str, str] | None = None,
    register_defaults: bool = True,
    openclaw_dir: str = "",
    workspace_path: str = "",
    remote_thread_id: str = "",
) -> AgentHarness:
    """Initialize and wire up the complete Agent Harness.

    Parameters
    ----------
    agent_roles
        Mapping of agent_id → role name (e.g., {"agent-eng-01": "engineering"}).
        Used to register agent capabilities with the dispatcher.
    register_defaults
        If True (default), register audit, cost, safety, and health hooks.
    openclaw_dir
        OpenClaw directory for persistence. If set, harness tables are created
        and the permission model loads saved patterns.
    workspace_path
        Workspace directory for project memory (CONTEXT.md) resolution.

    Returns
    -------
    AgentHarness
        Container with all subsystem instances.
    """

    # 1. Create instances (using singletons)
    hooks = HookRegistry.default()
    cost_tracker = CostTracker.default()
    tool_registry = ToolRegistry.default()
    coordinator = AgentCoordinator.default()
    dispatcher = AgentDispatcher.default()
    compactor = ContextCompactor.default()
    project_memory = ProjectMemory.default()
    permission_model = PermissionModel.default()

    # 1b. Create concrete LLM provider, path validator, and tool executor
    if remote_thread_id:
        # Desktop-local workspace: proxy tools to the desktop client via ToolBridge
        from backend.adapters.tools.remote_tool_executor import RemoteToolExecutor
        path_validator = FilePathValidator(
            allowed_roots=[workspace_path] if workspace_path else []
        )
        tool_executor = RemoteToolExecutor(
            thread_id=remote_thread_id,
            workspace_path=workspace_path,
        )
    else:
        path_validator = FilePathValidator(
            allowed_roots=[workspace_path] if workspace_path else []
        )
        tool_executor = HarnessToolExecutor(
            workspace_path=workspace_path,
            path_validator=path_validator,
        )
    llm_provider = GatewayLLMProvider(openclaw_dir=openclaw_dir)

    # 1c. Create QueryEngine with concrete providers
    query_engine = QueryEngine(
        llm_provider=llm_provider,
        tool_executor=tool_executor,
    )

    # 2. Wire subsystems together (original 5)
    cost_tracker.attach_hooks(hooks)
    tool_registry.attach_hooks(hooks)
    coordinator.attach_hooks(hooks)
    coordinator.attach_tool_registry(tool_registry)
    coordinator.attach_cost_tracker(cost_tracker)
    dispatcher.attach_hooks(hooks)

    # 2b. Wire new subsystems
    compactor.attach_hooks(hooks)
    permission_model.attach_tool_registry(tool_registry)
    permission_model.attach_hooks(hooks)
    query_engine.attach_compactor(compactor)
    query_engine.attach_permission_model(permission_model)
    query_engine.attach_cost_tracker(cost_tracker)
    query_engine.attach_hooks(hooks)

    # 3. Register default hooks
    if register_defaults:
        _register_audit_hooks(hooks)
        _register_cost_hooks(hooks, cost_tracker)
        _register_safety_hooks(hooks, tool_registry)
        _register_agent_health_hooks(hooks)

    # 4. Register agent capabilities
    if agent_roles:
        for agent_id, role in agent_roles.items():
            caps = DEFAULT_ROLE_CAPABILITIES.get(
                role,
                DEFAULT_ROLE_CAPABILITIES["general"],
            )
            dispatcher.register_agent(agent_id, caps)
            coordinator.register_agent_capabilities(agent_id, [c.value for c in caps])

    # 5. Persistence setup
    if openclaw_dir:
        try:
            from backend.adapters.storage.harness import HarnessStore
            store = HarnessStore(openclaw_dir)
            store.ensure_tables()
            logger.info("Harness persistence tables ensured")

            # Load saved permission patterns
            import os
            pattern_file = os.path.join(openclaw_dir, "harness_permissions.json")
            loaded = permission_model.load_patterns(pattern_file)
            if loaded:
                logger.info("Loaded %d saved permission patterns", loaded)
        except Exception:
            logger.warning("Harness persistence setup failed", exc_info=True)

    # 6. Project memory resolution
    if workspace_path:
        try:
            ctx = project_memory.resolve(workspace_path)
            logger.info(
                "Project memory resolved: %d layers, ~%d tokens",
                len(ctx.layers), ctx.total_tokens,
            )
        except Exception:
            logger.warning("Project memory resolution failed", exc_info=True)

    # 7. Emit startup hook
    hooks.emit(HookPhase.ON_STARTUP, {
        "harness_version": "2.0.0",
        "registered_tools": tool_registry.tool_count,
        "registered_agents": len(agent_roles or {}),
        "default_hooks": register_defaults,
        "new_subsystems": [
            "context_compact", "project_memory",
            "permission_model", "query_engine",
        ],
    }, source="harness_bootstrap")

    logger.info(
        "Agent Harness v2 bootstrapped: hooks=%d tools=%d agents=%d "
        "compactor=%s permissions=%s query_engine=%s "
        "llm_provider=%s tool_executor=%s path_validator=%s",
        hooks.handler_count(),
        tool_registry.tool_count,
        len(agent_roles or {}),
        repr(compactor),
        repr(permission_model),
        repr(query_engine),
        repr(llm_provider),
        repr(tool_executor),
        repr(path_validator),
    )

    return AgentHarness(
        hooks=hooks,
        cost_tracker=cost_tracker,
        tool_registry=tool_registry,
        coordinator=coordinator,
        dispatcher=dispatcher,
        compactor=compactor,
        project_memory=project_memory,
        permission_model=permission_model,
        query_engine=query_engine,
        llm_provider=llm_provider,
        tool_executor=tool_executor,
        path_validator=path_validator,
    )
