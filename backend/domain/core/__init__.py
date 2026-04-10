"""Shared domain constants and helpers for OpenClaw Team.

Agent Harness modules (added in 1.19):
- hooks: Lifecycle hook system (pub/sub event bus)
- cost_tracker: Token/cost tracking per agent/task
- tool_registry: Tool registration and permission gating
- coordinator: Intelligent task routing and decomposition
- agent_dispatch: Sub-agent dispatch and result merging
- context_compact: Context window compaction (Compact pattern)
- project_memory: CONTEXT.md project memory discovery
- permission_model: Progressive 3-tier permission model
- query_engine: Recursive query execution loop
"""

# ── Agent Harness public API ────────────────────────────────────────────

from backend.domain.core.hooks import HookPhase, HookContext, HookRegistry  # noqa: F401
from backend.domain.core.cost_tracker import CostEvent, CostTracker  # noqa: F401
from backend.domain.core.tool_registry import (  # noqa: F401
    ToolCategory,
    RiskLevel,
    ExecutionMode,
    ToolDefinition,
    ToolRegistry,
)
from backend.domain.core.coordinator import (  # noqa: F401
    AgentCoordinator,
    CoordinatorDecision,
    CoordinatorResult,
    ExecutionStrategy,
    TaskComplexity,
)
from backend.domain.core.agent_dispatch import (  # noqa: F401
    AgentCapability,
    AgentDispatcher,
    DispatchHandle,
    MergedResult,
    MergeStrategy,
)
from backend.domain.core.context_compact import (  # noqa: F401
    ContextCompactor,
    CompactResult,
    CompactStrategy,
)
from backend.domain.core.project_memory import (  # noqa: F401
    ProjectMemory,
    ResolvedContext,
)
from backend.domain.core.permission_model import (  # noqa: F401
    PermissionModel,
    PermissionTier,
    PermissionDecision,
    ApprovalPattern,
)
from backend.domain.core.query_engine import (  # noqa: F401
    QueryEngine,
    QueryConfig,
    QueryEvent,
    QueryEventType,
    QueryResult,
)
