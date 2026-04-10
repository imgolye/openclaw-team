"""Agent Harness — Tool registry and permission gate.

Provides a central registry of all tools available to agents, with per-tool
risk classification and role-based permission gating.

Inspired by Claude Code's tool permission model where each tool has an
explicit risk level and interactive/coordinated mode determines whether
user confirmation is required.

Usage::

    from backend.domain.core.tool_registry import ToolRegistry, RiskLevel

    registry = ToolRegistry.default()

    # Check permission before executing
    allowed, reason = registry.check_permission(
        tool_name="bash",
        agent_role="engineering",
        mode="coordinated",
    )
    if not allowed:
        raise PermissionError(reason)

    # List tools available to a role
    tools = registry.list_tools(role="engineering")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ── Enums ───────────────────────────────────────────────────────────────


class ToolCategory(str, Enum):
    """Logical grouping for tools."""

    SYSTEM = "system"               # bash, file ops, process management
    INTELLIGENCE = "intelligence"   # LLM calls, search, analysis
    ORCHESTRATION = "orchestration"  # agent dispatch, task routing
    COMMUNICATION = "communication"  # chat, notifications, channels
    DATA = "data"                   # database, storage, exports
    INTEGRATION = "integration"     # external APIs, webhooks, MCP


class RiskLevel(str, Enum):
    """Risk classification for tool operations."""

    LOW = "low"           # read-only, no side effects
    MEDIUM = "medium"     # writes data, but reversible
    HIGH = "high"         # system mutations, hard to reverse
    CRITICAL = "critical"  # destructive ops, security-sensitive


class ExecutionMode(str, Enum):
    """How the agent is running — determines permission behavior."""

    INTERACTIVE = "interactive"      # user is present, can approve
    COORDINATED = "coordinated"      # autonomous, pre-approved scope
    SUPERVISED = "supervised"        # auto-run but logged for review


# ── Tool Definition ─────────────────────────────────────────────────────


@dataclass
class ToolDefinition:
    """Describes a tool's identity, risk, and access rules."""

    name: str
    category: ToolCategory
    risk_level: RiskLevel
    description: str = ""

    # Permission rules
    requires_approval: bool = False       # needs explicit user/admin approval
    allowed_roles: list[str] = field(default_factory=list)  # empty = all roles
    denied_roles: list[str] = field(default_factory=list)
    allowed_modes: list[ExecutionMode] = field(
        default_factory=lambda: list(ExecutionMode),
    )

    # Metadata
    version: str = "1.0"
    tags: list[str] = field(default_factory=list)
    max_calls_per_task: int = 0  # 0 = unlimited
    timeout_seconds: int = 300

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category.value,
            "risk_level": self.risk_level.value,
            "description": self.description,
            "requires_approval": self.requires_approval,
            "allowed_roles": self.allowed_roles,
            "denied_roles": self.denied_roles,
            "allowed_modes": [m.value for m in self.allowed_modes],
            "version": self.version,
            "tags": self.tags,
        }


# ── Permission Result ───────────────────────────────────────────────────


@dataclass
class PermissionResult:
    """Result of a permission check."""

    allowed: bool
    reason: str = ""
    requires_approval: bool = False
    risk_level: RiskLevel = RiskLevel.LOW

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "requires_approval": self.requires_approval,
            "risk_level": self.risk_level.value,
        }


# ── Tool Registry ──────────────────────────────────────────────────────


class ToolRegistry:
    """Central registry for all tools available in the platform.

    Thread-safe for reads.  Writes (register/unregister) are expected to
    happen at startup or via admin actions.
    """

    _default: Optional["ToolRegistry"] = None

    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}
        self._role_overrides: dict[str, dict[str, bool]] = {}  # role -> {tool: allow}
        self._tenant_policies: dict[str, set[str]] = {}  # tenant -> blocked tools
        self._hook_registry = None

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "ToolRegistry":
        if cls._default is None:
            cls._default = cls()
            cls._default._register_builtin_tools()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    # ── Hook integration ────────────────────────────────────────────

    def attach_hooks(self, registry) -> None:
        self._hook_registry = registry

    # ── Registration ────────────────────────────────────────────────

    def register(self, tool: ToolDefinition) -> None:
        """Register or update a tool definition."""
        self._tools[tool.name] = tool
        logger.debug("Registered tool: %s [%s/%s]", tool.name, tool.category.value, tool.risk_level.value)

    def unregister(self, name: str) -> bool:
        """Remove a tool. Returns True if found."""
        return self._tools.pop(name, None) is not None

    def get(self, name: str) -> ToolDefinition | None:
        """Look up a tool by name."""
        return self._tools.get(name)

    # ── Permission checking ─────────────────────────────────────────

    def check_permission(
        self,
        tool_name: str,
        *,
        agent_role: str = "",
        mode: str | ExecutionMode = ExecutionMode.COORDINATED,
        tenant_id: str = "",
    ) -> PermissionResult:
        """Check whether a tool call is allowed.

        Returns a PermissionResult with ``allowed``, ``reason``, and
        ``requires_approval`` fields.
        """
        tool = self._tools.get(tool_name)
        if tool is None:
            return PermissionResult(
                allowed=False,
                reason=f"Tool '{tool_name}' is not registered",
            )

        if isinstance(mode, str):
            try:
                mode = ExecutionMode(mode)
            except ValueError:
                mode = ExecutionMode.COORDINATED

        # 1. Tenant-level block
        if tenant_id and tenant_id in self._tenant_policies:
            if tool_name in self._tenant_policies[tenant_id]:
                return PermissionResult(
                    allowed=False,
                    reason=f"Tool '{tool_name}' is blocked for tenant '{tenant_id}'",
                    risk_level=tool.risk_level,
                )

        # 2. Explicit role denial
        if agent_role and agent_role in tool.denied_roles:
            return PermissionResult(
                allowed=False,
                reason=f"Role '{agent_role}' is explicitly denied for '{tool_name}'",
                risk_level=tool.risk_level,
            )

        # 3. Role whitelist (if set)
        if tool.allowed_roles and agent_role:
            if agent_role not in tool.allowed_roles:
                # Check overrides
                override = self._role_overrides.get(agent_role, {}).get(tool_name)
                if override is not True:
                    return PermissionResult(
                        allowed=False,
                        reason=f"Role '{agent_role}' is not in allowed_roles for '{tool_name}'",
                        risk_level=tool.risk_level,
                    )

        # 4. Execution mode check
        if tool.allowed_modes and mode not in tool.allowed_modes:
            return PermissionResult(
                allowed=False,
                reason=f"Mode '{mode.value}' is not allowed for '{tool_name}'",
                risk_level=tool.risk_level,
            )

        # 5. Approval requirement
        needs_approval = tool.requires_approval
        if tool.risk_level == RiskLevel.CRITICAL and mode == ExecutionMode.COORDINATED:
            needs_approval = True

        # Emit hook
        if self._hook_registry is not None:
            try:
                from backend.domain.core.hooks import HookPhase
                ctx = self._hook_registry.emit(
                    HookPhase.TOOL_PERMISSION_CHECK,
                    {
                        "tool_name": tool_name,
                        "agent_role": agent_role,
                        "mode": mode.value,
                        "risk_level": tool.risk_level.value,
                        "requires_approval": needs_approval,
                    },
                    source="tool_registry",
                )
                if ctx.cancelled:
                    return PermissionResult(
                        allowed=False,
                        reason=f"Blocked by hook: {ctx.cancel_reason}",
                        risk_level=tool.risk_level,
                    )
            except Exception:
                logger.exception("Failed to emit TOOL_PERMISSION_CHECK hook")

        return PermissionResult(
            allowed=True,
            requires_approval=needs_approval,
            risk_level=tool.risk_level,
        )

    # ── Role overrides ──────────────────────────────────────────────

    def set_role_override(self, role: str, tool_name: str, allowed: bool) -> None:
        """Grant or revoke a specific tool for a role, overriding defaults."""
        self._role_overrides.setdefault(role, {})[tool_name] = allowed

    # ── Tenant policies ─────────────────────────────────────────────

    def block_tool_for_tenant(self, tenant_id: str, tool_name: str) -> None:
        """Block a tool at the tenant level."""
        self._tenant_policies.setdefault(tenant_id, set()).add(tool_name)

    def unblock_tool_for_tenant(self, tenant_id: str, tool_name: str) -> None:
        """Remove a tenant-level block."""
        if tenant_id in self._tenant_policies:
            self._tenant_policies[tenant_id].discard(tool_name)

    # ── Queries ─────────────────────────────────────────────────────

    def list_tools(
        self,
        *,
        category: ToolCategory | None = None,
        risk_level: RiskLevel | None = None,
        role: str = "",
    ) -> list[ToolDefinition]:
        """List tools, optionally filtered."""
        results = []
        for tool in self._tools.values():
            if category and tool.category != category:
                continue
            if risk_level and tool.risk_level != risk_level:
                continue
            if role and tool.allowed_roles and role not in tool.allowed_roles:
                continue
            if role and role in tool.denied_roles:
                continue
            results.append(tool)
        return sorted(results, key=lambda t: (t.category.value, t.name))

    def list_tools_dict(self, **kwargs) -> list[dict]:
        """Same as list_tools but returns dicts."""
        return [t.to_dict() for t in self.list_tools(**kwargs)]

    @property
    def tool_count(self) -> int:
        return len(self._tools)

    # ── Built-in tools ──────────────────────────────────────────────

    def _register_builtin_tools(self) -> None:
        """Register the standard OpenClaw/OpenClaw Team tool set."""

        builtins = [
            # ── System tools ────────────────────────────────────
            ToolDefinition(
                name="bash",
                category=ToolCategory.SYSTEM,
                risk_level=RiskLevel.HIGH,
                description="Execute shell commands",
                requires_approval=False,
                tags=["system", "execution"],
                timeout_seconds=600,
            ),
            ToolDefinition(
                name="file_read",
                category=ToolCategory.SYSTEM,
                risk_level=RiskLevel.LOW,
                description="Read file contents",
                tags=["system", "read"],
            ),
            ToolDefinition(
                name="file_write",
                category=ToolCategory.SYSTEM,
                risk_level=RiskLevel.MEDIUM,
                description="Write or create files",
                tags=["system", "write"],
            ),
            ToolDefinition(
                name="file_edit",
                category=ToolCategory.SYSTEM,
                risk_level=RiskLevel.MEDIUM,
                description="Edit files via find-and-replace",
                tags=["system", "write"],
            ),
            ToolDefinition(
                name="file_delete",
                category=ToolCategory.SYSTEM,
                risk_level=RiskLevel.CRITICAL,
                description="Delete files (should use trash)",
                requires_approval=True,
                tags=["system", "destructive"],
            ),
            ToolDefinition(
                name="glob",
                category=ToolCategory.SYSTEM,
                risk_level=RiskLevel.LOW,
                description="Find files matching glob patterns",
                tags=["system", "search"],
            ),
            ToolDefinition(
                name="grep",
                category=ToolCategory.SYSTEM,
                risk_level=RiskLevel.LOW,
                description="Search file contents with regex",
                tags=["system", "search"],
            ),

            # ── Intelligence tools ─────────────────────────────
            ToolDefinition(
                name="web_search",
                category=ToolCategory.INTELLIGENCE,
                risk_level=RiskLevel.LOW,
                description="Search the web for information",
                tags=["intelligence", "search"],
            ),
            ToolDefinition(
                name="web_fetch",
                category=ToolCategory.INTELLIGENCE,
                risk_level=RiskLevel.LOW,
                description="Fetch content from URLs",
                tags=["intelligence", "fetch"],
            ),

            # ── Orchestration tools ─────────────────────────────
            ToolDefinition(
                name="agent_dispatch",
                category=ToolCategory.ORCHESTRATION,
                risk_level=RiskLevel.MEDIUM,
                description="Dispatch a sub-agent for a task",
                tags=["orchestration", "delegation"],
            ),
            ToolDefinition(
                name="task_create",
                category=ToolCategory.ORCHESTRATION,
                risk_level=RiskLevel.LOW,
                description="Create a new task",
                tags=["orchestration", "task"],
            ),
            ToolDefinition(
                name="task_route",
                category=ToolCategory.ORCHESTRATION,
                risk_level=RiskLevel.MEDIUM,
                description="Route a task to the best agent",
                tags=["orchestration", "routing"],
            ),

            # ── Communication tools ────────────────────────────
            ToolDefinition(
                name="chat_send",
                category=ToolCategory.COMMUNICATION,
                risk_level=RiskLevel.LOW,
                description="Send a message to a conversation",
                tags=["communication", "chat"],
            ),
            ToolDefinition(
                name="notification_send",
                category=ToolCategory.COMMUNICATION,
                risk_level=RiskLevel.LOW,
                description="Send a notification to users",
                tags=["communication", "notification"],
            ),

            # ── Data tools ──────────────────────────────────────
            ToolDefinition(
                name="db_query",
                category=ToolCategory.DATA,
                risk_level=RiskLevel.MEDIUM,
                description="Query the dashboard database",
                tags=["data", "query"],
            ),
            ToolDefinition(
                name="db_write",
                category=ToolCategory.DATA,
                risk_level=RiskLevel.HIGH,
                description="Write to the dashboard database",
                requires_approval=True,
                tags=["data", "write"],
            ),

            # ── Integration tools ──────────────────────────────
            ToolDefinition(
                name="mcp_call",
                category=ToolCategory.INTEGRATION,
                risk_level=RiskLevel.MEDIUM,
                description="Call an MCP tool on a remote server",
                tags=["integration", "mcp"],
            ),
            ToolDefinition(
                name="webhook_trigger",
                category=ToolCategory.INTEGRATION,
                risk_level=RiskLevel.HIGH,
                description="Trigger an outbound webhook",
                tags=["integration", "webhook"],
            ),
        ]

        for tool in builtins:
            self.register(tool)

        logger.info("Registered %d built-in tools", len(builtins))

    def __repr__(self) -> str:
        return f"<ToolRegistry tools={self.tool_count}>"
