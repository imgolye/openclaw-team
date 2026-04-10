"""Agent Harness — Progressive permission model.

Implements Claude Code's three-tier permission model with learning:

1. **Restricted** — all tool calls require explicit approval
2. **Auto-approve** — low/medium risk tools run automatically; high/critical need approval
3. **YOLO** — everything runs without approval (dev/test only)

Plus an "Allow Always" learning mechanism that remembers approved
tool+context patterns and auto-approves matching future requests.

Integrates with the existing ToolRegistry for tool definitions and
risk levels, adding a runtime permission layer on top.

Usage::

    from backend.domain.core.permission_model import PermissionModel, PermissionTier

    pm = PermissionModel.default()
    pm.set_tier(PermissionTier.AUTO_APPROVE)

    # Check if a tool call needs approval
    decision = pm.evaluate(
        tool_name="bash",
        agent_id="eng-01",
        context={"command": "ls -la"},
    )
    if decision.needs_approval:
        # present to user
        ...

    # Learn from user approval
    pm.learn_approval(
        tool_name="bash",
        agent_id="eng-01",
        context={"command_prefix": "ls"},
    )
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ── Enums ───────────────────────────────────────────────────────────────


class PermissionTier(str, Enum):
    """Three-tier permission model inspired by Claude Code."""

    RESTRICTED = "restricted"       # everything needs approval
    AUTO_APPROVE = "auto_approve"   # risk-based auto-approval
    YOLO = "yolo"                   # no approvals needed (dev mode)


class ApprovalDecision(str, Enum):
    """Outcome of a permission evaluation."""

    ALLOWED = "allowed"               # proceed without approval
    NEEDS_APPROVAL = "needs_approval"  # user must approve
    DENIED = "denied"                  # permanently blocked
    LEARNED = "learned"               # auto-approved via learned pattern


# ── Data structures ────────────────────────────────────────────────────


@dataclass
class ApprovalPattern:
    """A learned pattern for auto-approval.

    Captures a tool + context combination that the user has approved,
    so future matching calls can be auto-approved.
    """

    tool_name: str
    agent_id: str = ""           # empty = any agent
    context_key: str = ""        # e.g. "command_prefix:ls" or "file_pattern:*.py"
    approved_at: float = field(default_factory=time.time)
    approved_by: str = ""        # who approved (user ID or "system")
    use_count: int = 0           # how many times this pattern was used
    last_used_at: float = 0.0
    expires_at: float = 0.0      # 0 = never expires

    @property
    def is_expired(self) -> bool:
        if self.expires_at == 0:
            return False
        return time.time() > self.expires_at

    def matches(self, tool_name: str, agent_id: str = "", context: dict | None = None) -> bool:
        """Check if this pattern matches a tool call."""
        if self.tool_name != tool_name:
            return False
        if self.agent_id and agent_id and self.agent_id != agent_id:
            return False
        if self.is_expired:
            return False
        # Context key matching
        if self.context_key and context:
            key_name, _, key_value = self.context_key.partition(":")
            if key_name in context:
                ctx_value = str(context[key_name])
                if not ctx_value.startswith(key_value):
                    return False
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "agent_id": self.agent_id,
            "context_key": self.context_key,
            "approved_at": self.approved_at,
            "approved_by": self.approved_by,
            "use_count": self.use_count,
            "last_used_at": self.last_used_at,
            "expires_at": self.expires_at,
        }


@dataclass
class PermissionDecision:
    """Result of evaluating a tool call against the permission model."""

    decision: ApprovalDecision
    reason: str = ""
    tool_name: str = ""
    tier: PermissionTier = PermissionTier.AUTO_APPROVE
    risk_level: str = ""
    matched_pattern: ApprovalPattern | None = None
    timestamp: float = field(default_factory=time.time)

    @property
    def needs_approval(self) -> bool:
        return self.decision == ApprovalDecision.NEEDS_APPROVAL

    @property
    def is_allowed(self) -> bool:
        return self.decision in (ApprovalDecision.ALLOWED, ApprovalDecision.LEARNED)

    def to_dict(self) -> dict[str, Any]:
        d = {
            "decision": self.decision.value,
            "reason": self.reason,
            "tool_name": self.tool_name,
            "tier": self.tier.value,
            "risk_level": self.risk_level,
            "timestamp": self.timestamp,
        }
        if self.matched_pattern:
            d["matched_pattern"] = self.matched_pattern.context_key
        return d


# ── Risk-tier auto-approve matrix ──────────────────────────────────────

# In AUTO_APPROVE tier, which risk levels are auto-approved?
AUTO_APPROVE_MATRIX: dict[str, bool] = {
    "low": True,        # always auto-approve
    "medium": True,     # auto-approve
    "high": False,      # needs approval
    "critical": False,  # always needs approval
}


# ── Permission Model ──────────────────────────────────────────────────


class PermissionModel:
    """Progressive permission model with learning.

    Wraps the ToolRegistry to add runtime permission decisions based
    on the current tier, learned approval patterns, and tool risk levels.
    """

    _default: Optional["PermissionModel"] = None

    def __init__(
        self,
        *,
        tier: PermissionTier = PermissionTier.AUTO_APPROVE,
        max_patterns: int = 500,
    ) -> None:
        self._tier = tier
        self._max_patterns = max_patterns
        self._patterns: list[ApprovalPattern] = []
        self._decision_log: list[dict] = []
        self._max_log = 200
        self._tool_registry = None
        self._hook_registry = None

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "PermissionModel":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    # ── Wiring ──────────────────────────────────────────────────────

    def attach_tool_registry(self, registry) -> None:
        self._tool_registry = registry

    def attach_hooks(self, registry) -> None:
        self._hook_registry = registry

    @property
    def tier(self) -> PermissionTier:
        return self._tier

    def set_tier(self, tier: PermissionTier) -> None:
        """Change the permission tier."""
        old = self._tier
        self._tier = tier
        logger.info("Permission tier changed: %s → %s", old.value, tier.value)

    # ── Evaluate ────────────────────────────────────────────────────

    def evaluate(
        self,
        tool_name: str,
        *,
        agent_id: str = "",
        context: dict | None = None,
        tenant_id: str = "",
    ) -> PermissionDecision:
        """Evaluate whether a tool call is permitted.

        Uses the three-tier model plus learned patterns to determine
        if the call should proceed, needs approval, or is denied.
        """
        # Get risk level from registry
        risk_level = "medium"
        if self._tool_registry:
            tool_def = self._tool_registry.get(tool_name)
            if tool_def:
                risk_level = tool_def.risk_level.value

        # ── Tier: YOLO — allow everything ────────────────────────
        if self._tier == PermissionTier.YOLO:
            decision = PermissionDecision(
                decision=ApprovalDecision.ALLOWED,
                reason="YOLO mode: all tools auto-approved",
                tool_name=tool_name,
                tier=self._tier,
                risk_level=risk_level,
            )
            self._log_decision(decision)
            return decision

        # ── Check learned patterns first ──────────────────────────
        matched = self._find_matching_pattern(tool_name, agent_id, context)
        if matched:
            matched.use_count += 1
            matched.last_used_at = time.time()
            decision = PermissionDecision(
                decision=ApprovalDecision.LEARNED,
                reason=f"Auto-approved via learned pattern: {matched.context_key or 'any'}",
                tool_name=tool_name,
                tier=self._tier,
                risk_level=risk_level,
                matched_pattern=matched,
            )
            self._log_decision(decision)
            return decision

        # ── Tier: RESTRICTED — everything needs approval ──────────
        if self._tier == PermissionTier.RESTRICTED:
            decision = PermissionDecision(
                decision=ApprovalDecision.NEEDS_APPROVAL,
                reason="Restricted mode: all tools require approval",
                tool_name=tool_name,
                tier=self._tier,
                risk_level=risk_level,
            )
            self._log_decision(decision)
            return decision

        # ── Tier: AUTO_APPROVE — use risk matrix ─────────────────
        auto_approved = AUTO_APPROVE_MATRIX.get(risk_level, False)

        if auto_approved:
            decision = PermissionDecision(
                decision=ApprovalDecision.ALLOWED,
                reason=f"Auto-approved: {risk_level} risk in auto_approve tier",
                tool_name=tool_name,
                tier=self._tier,
                risk_level=risk_level,
            )
        else:
            decision = PermissionDecision(
                decision=ApprovalDecision.NEEDS_APPROVAL,
                reason=f"Requires approval: {risk_level} risk in auto_approve tier",
                tool_name=tool_name,
                tier=self._tier,
                risk_level=risk_level,
            )

        self._log_decision(decision)
        return decision

    # ── Learn ───────────────────────────────────────────────────────

    def learn_approval(
        self,
        tool_name: str,
        *,
        agent_id: str = "",
        context_key: str = "",
        approved_by: str = "user",
        ttl_seconds: float = 0,
    ) -> ApprovalPattern:
        """Record an approval pattern for future auto-approval.

        Parameters
        ----------
        tool_name
            The tool that was approved.
        agent_id
            Which agent (empty = any agent can use this pattern).
        context_key
            A context matcher like "command_prefix:ls" or "file_pattern:*.py".
        approved_by
            Who approved it.
        ttl_seconds
            How long the pattern is valid (0 = forever).
        """
        pattern = ApprovalPattern(
            tool_name=tool_name,
            agent_id=agent_id,
            context_key=context_key,
            approved_by=approved_by,
            expires_at=time.time() + ttl_seconds if ttl_seconds > 0 else 0,
        )

        # Dedup: if an identical pattern exists, just update it
        for existing in self._patterns:
            if (
                existing.tool_name == tool_name
                and existing.agent_id == agent_id
                and existing.context_key == context_key
            ):
                existing.approved_at = time.time()
                existing.expires_at = pattern.expires_at
                existing.approved_by = approved_by
                logger.info("Updated existing approval pattern: %s/%s", tool_name, context_key)
                return existing

        self._patterns.append(pattern)

        # Trim if over capacity (remove oldest expired or least-used)
        if len(self._patterns) > self._max_patterns:
            self._cleanup_patterns()

        logger.info("Learned approval pattern: %s agent=%s key=%s", tool_name, agent_id, context_key)
        return pattern

    def revoke_approval(
        self,
        tool_name: str,
        *,
        agent_id: str = "",
        context_key: str = "",
    ) -> int:
        """Remove matching approval patterns. Returns count removed."""
        before = len(self._patterns)
        self._patterns = [
            p for p in self._patterns
            if not (
                p.tool_name == tool_name
                and (not agent_id or p.agent_id == agent_id)
                and (not context_key or p.context_key == context_key)
            )
        ]
        removed = before - len(self._patterns)
        if removed:
            logger.info("Revoked %d approval pattern(s) for %s", removed, tool_name)
        return removed

    # ── Pattern lookup ──────────────────────────────────────────────

    def _find_matching_pattern(
        self,
        tool_name: str,
        agent_id: str,
        context: dict | None,
    ) -> ApprovalPattern | None:
        """Find the most specific matching approval pattern."""
        candidates = [
            p for p in self._patterns
            if p.matches(tool_name, agent_id, context)
        ]
        if not candidates:
            return None

        # Prefer most specific: with context_key > with agent_id > generic
        candidates.sort(
            key=lambda p: (
                bool(p.context_key),
                bool(p.agent_id),
                p.use_count,
            ),
            reverse=True,
        )
        return candidates[0]

    def _cleanup_patterns(self) -> None:
        """Remove expired patterns and trim to capacity."""
        now = time.time()
        # Remove expired
        self._patterns = [p for p in self._patterns if not p.is_expired]
        # If still over, remove least-used
        if len(self._patterns) > self._max_patterns:
            self._patterns.sort(key=lambda p: p.use_count, reverse=True)
            self._patterns = self._patterns[:self._max_patterns]

    # ── Decision logging ────────────────────────────────────────────

    def _log_decision(self, decision: PermissionDecision) -> None:
        self._decision_log.append(decision.to_dict())
        if len(self._decision_log) > self._max_log:
            self._decision_log = self._decision_log[-self._max_log:]

    # ── Persistence ─────────────────────────────────────────────────

    def save_patterns(self, path: str | Path) -> None:
        """Save learned patterns to a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "tier": self._tier.value,
            "patterns": [p.to_dict() for p in self._patterns if not p.is_expired],
            "saved_at": time.time(),
        }
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved %d permission patterns to %s", len(self._patterns), path)

    def load_patterns(self, path: str | Path) -> int:
        """Load learned patterns from a JSON file. Returns count loaded."""
        path = Path(path)
        if not path.is_file():
            return 0

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to load permission patterns from %s", path)
            return 0

        # Restore tier
        if "tier" in data:
            try:
                self._tier = PermissionTier(data["tier"])
            except ValueError:
                pass

        # Restore patterns
        loaded = 0
        for pd in data.get("patterns", []):
            try:
                pattern = ApprovalPattern(
                    tool_name=pd["tool_name"],
                    agent_id=pd.get("agent_id", ""),
                    context_key=pd.get("context_key", ""),
                    approved_at=pd.get("approved_at", time.time()),
                    approved_by=pd.get("approved_by", ""),
                    use_count=pd.get("use_count", 0),
                    last_used_at=pd.get("last_used_at", 0),
                    expires_at=pd.get("expires_at", 0),
                )
                if not pattern.is_expired:
                    self._patterns.append(pattern)
                    loaded += 1
            except (KeyError, TypeError):
                logger.warning("Skipped invalid pattern: %s", pd)

        logger.info("Loaded %d permission patterns from %s", loaded, path)
        return loaded

    # ── Introspection ───────────────────────────────────────────────

    @property
    def patterns(self) -> list[ApprovalPattern]:
        return list(self._patterns)

    @property
    def pattern_count(self) -> int:
        return len(self._patterns)

    @property
    def decision_log(self) -> list[dict]:
        return list(self._decision_log)

    def stats(self) -> dict[str, Any]:
        """Return permission model statistics."""
        decisions_by_type: dict[str, int] = {}
        for d in self._decision_log:
            key = d.get("decision", "unknown")
            decisions_by_type[key] = decisions_by_type.get(key, 0) + 1

        return {
            "tier": self._tier.value,
            "pattern_count": len(self._patterns),
            "total_decisions": len(self._decision_log),
            "decisions_by_type": decisions_by_type,
            "active_patterns": sum(1 for p in self._patterns if not p.is_expired),
        }

    def __repr__(self) -> str:
        return (
            f"<PermissionModel tier={self._tier.value} "
            f"patterns={len(self._patterns)}>"
        )
