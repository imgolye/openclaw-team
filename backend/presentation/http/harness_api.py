"""REST API endpoints for the Agent Harness subsystem.

Exposes harness data (costs, tools, permissions, query stats) via
JSON endpoints under /api/v1/harness/*.

Endpoint map::

    GET  /api/v1/harness/status         — Overall harness status
    GET  /api/v1/harness/costs          — Cost events list
    GET  /api/v1/harness/costs/summary  — Aggregated cost summary
    GET  /api/v1/harness/tools          — Registered tools list
    GET  /api/v1/harness/tools/{name}   — Single tool detail
    GET  /api/v1/harness/permissions    — Permission decision log
    GET  /api/v1/harness/permissions/patterns — Learned approval patterns
    POST /api/v1/harness/permissions/tier     — Set permission tier
    GET  /api/v1/harness/compact        — Compaction history
    GET  /api/v1/harness/queries        — Query execution stats

Usage::

    # Typically called from the CollaborationDashboardHandler
    from backend.presentation.http.harness_api import handle_harness_api

    response = handle_harness_api(method, path, params, body, openclaw_dir)
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional
from urllib.parse import parse_qs

logger = logging.getLogger(__name__)


def handle_harness_api(
    method: str,
    path: str,
    params: dict[str, str] | None = None,
    body: str = "",
    openclaw_dir: str = "",
) -> dict[str, Any]:
    """Route and handle a harness API request.

    Parameters
    ----------
    method : str
        HTTP method (GET, POST, etc.)
    path : str
        API path after /api/v1/harness/ (e.g. "costs", "costs/summary")
    params : dict
        Query parameters
    body : str
        Request body (for POST)
    openclaw_dir : str
        OpenClaw directory for database access

    Returns
    -------
    dict with "status" (int), "data" (any), and optionally "error" (str)
    """
    params = params or {}

    # Normalize path
    path = path.strip("/")

    try:
        if path == "status":
            return _handle_status()
        elif path == "costs":
            return _handle_costs_list(params, openclaw_dir)
        elif path == "costs/summary":
            return _handle_costs_summary(params, openclaw_dir)
        elif path == "tools":
            return _handle_tools_list(params)
        elif path.startswith("tools/"):
            tool_name = path[6:]
            return _handle_tool_detail(tool_name)
        elif path == "permissions":
            return _handle_permissions_log(params, openclaw_dir)
        elif path == "permissions/patterns":
            return _handle_permission_patterns(openclaw_dir)
        elif path == "permissions/tier":
            if method == "POST":
                return _handle_set_tier(body)
            return _handle_get_tier()
        elif path == "compact":
            return _handle_compact_history(openclaw_dir)
        elif path == "queries":
            return _handle_query_stats(params, openclaw_dir)
        else:
            return {"status": 404, "error": f"Unknown harness endpoint: {path}"}
    except Exception as exc:
        logger.exception("Harness API error: %s %s", method, path)
        return {"status": 500, "error": str(exc)}


# ── Status ──────────────────────────────────────────────────────────────


def _handle_status() -> dict[str, Any]:
    """Return overall harness subsystem status."""
    data: dict[str, Any] = {"harness": "active", "timestamp": time.time()}

    # Tool registry
    try:
        from backend.domain.core.tool_registry import ToolRegistry
        registry = ToolRegistry.default()
        data["tools"] = {"registered": registry.tool_count}
    except Exception:
        data["tools"] = {"error": "not initialized"}

    # Cost tracker
    try:
        from backend.domain.core.cost_tracker import CostTracker
        tracker = CostTracker.default()
        data["costs"] = {
            "total_events": len(tracker._events),
            "total_cost_usd": round(tracker.total_cost_usd, 6),
        }
    except Exception:
        data["costs"] = {"error": "not initialized"}

    # Permission model
    try:
        from backend.domain.core.permission_model import PermissionModel
        pm = PermissionModel.default()
        data["permissions"] = pm.stats()
    except Exception:
        data["permissions"] = {"error": "not initialized"}

    # Context compactor
    try:
        from backend.domain.core.context_compact import ContextCompactor
        compactor = ContextCompactor.default()
        data["compact"] = {"compactions": len(compactor.compact_history)}
    except Exception:
        data["compact"] = {"error": "not initialized"}

    # Query engine
    try:
        from backend.domain.core.query_engine import QueryEngine
        engine = QueryEngine.default()
        data["query_engine"] = engine.stats
    except Exception:
        data["query_engine"] = {"error": "not initialized"}

    return {"status": 200, "data": data}


# ── Costs ───────────────────────────────────────────────────────────────


def _handle_costs_list(params: dict, openclaw_dir: str) -> dict[str, Any]:
    """List cost events."""
    # Try persistent store first
    if openclaw_dir:
        try:
            from backend.adapters.storage.harness import HarnessStore
            store = HarnessStore(openclaw_dir)
            events = store.list_cost_events(
                agent_id=params.get("agent_id", ""),
                since=float(params.get("since", "0")),
                limit=int(params.get("limit", "100")),
            )
            return {"status": 200, "data": {"events": events, "source": "database"}}
        except Exception:
            pass

    # Fallback to in-memory
    try:
        from backend.domain.core.cost_tracker import CostTracker
        tracker = CostTracker.default()
        events = [e.to_dict() if hasattr(e, "to_dict") else e for e in tracker._events]
        agent_id = params.get("agent_id", "")
        if agent_id:
            events = [e for e in events if e.get("agent_id") == agent_id]
        limit = int(params.get("limit", "100"))
        events = events[-limit:]
        return {"status": 200, "data": {"events": events, "source": "memory"}}
    except Exception:
        return {"status": 200, "data": {"events": [], "source": "unavailable"}}


def _handle_costs_summary(params: dict, openclaw_dir: str) -> dict[str, Any]:
    """Aggregated cost summary."""
    days = int(params.get("days", "7"))

    # Try persistent store
    if openclaw_dir:
        try:
            from backend.adapters.storage.harness import HarnessStore
            store = HarnessStore(openclaw_dir)
            summary = store.get_cost_summary(
                days=days,
                agent_id=params.get("agent_id", ""),
            )
            return {"status": 200, "data": summary}
        except Exception:
            pass

    # Fallback to in-memory
    try:
        from backend.domain.core.cost_tracker import CostTracker
        tracker = CostTracker.default()
        # Simple in-memory summary
        cutoff = time.time() - days * 86400
        events = [e for e in tracker._events if e.timestamp >= cutoff]
        return {
            "status": 200,
            "data": {
                "days": days,
                "total_events": len(events),
                "total_cost_usd": round(sum(e.cost_usd for e in events), 6),
            },
        }
    except Exception:
        return {"status": 200, "data": {"days": days, "total_events": 0}}


# ── Tools ───────────────────────────────────────────────────────────────


def _handle_tools_list(params: dict) -> dict[str, Any]:
    """List registered tools."""
    try:
        from backend.domain.core.tool_registry import ToolRegistry
        registry = ToolRegistry.default()
        tools = registry.list_tools_dict(
            role=params.get("role", ""),
        )
        return {"status": 200, "data": {"tools": tools, "count": len(tools)}}
    except Exception:
        return {"status": 200, "data": {"tools": [], "count": 0}}


def _handle_tool_detail(tool_name: str) -> dict[str, Any]:
    """Get a single tool's details."""
    try:
        from backend.domain.core.tool_registry import ToolRegistry
        registry = ToolRegistry.default()
        tool = registry.get(tool_name)
        if tool:
            return {"status": 200, "data": tool.to_dict()}
        return {"status": 404, "error": f"Tool '{tool_name}' not found"}
    except Exception:
        return {"status": 500, "error": "Tool registry not initialized"}


# ── Permissions ─────────────────────────────────────────────────────────


def _handle_permissions_log(params: dict, openclaw_dir: str) -> dict[str, Any]:
    """Permission decision log."""
    # Try persistent store
    if openclaw_dir:
        try:
            from backend.adapters.storage.harness import HarnessStore
            store = HarnessStore(openclaw_dir)
            decisions = store.list_permission_decisions(
                tool_name=params.get("tool_name", ""),
                limit=int(params.get("limit", "100")),
            )
            return {"status": 200, "data": {"decisions": decisions, "source": "database"}}
        except Exception:
            pass

    # Fallback
    try:
        from backend.domain.core.permission_model import PermissionModel
        pm = PermissionModel.default()
        return {"status": 200, "data": {"decisions": pm.decision_log, "source": "memory"}}
    except Exception:
        return {"status": 200, "data": {"decisions": [], "source": "unavailable"}}


def _handle_permission_patterns(openclaw_dir: str) -> dict[str, Any]:
    """List learned approval patterns."""
    try:
        from backend.domain.core.permission_model import PermissionModel
        pm = PermissionModel.default()
        patterns = [p.to_dict() for p in pm.patterns]
        return {"status": 200, "data": {"patterns": patterns, "count": len(patterns)}}
    except Exception:
        return {"status": 200, "data": {"patterns": [], "count": 0}}


def _handle_get_tier() -> dict[str, Any]:
    """Get current permission tier."""
    try:
        from backend.domain.core.permission_model import PermissionModel
        pm = PermissionModel.default()
        return {"status": 200, "data": {"tier": pm.tier.value}}
    except Exception:
        return {"status": 200, "data": {"tier": "unknown"}}


def _handle_set_tier(body: str) -> dict[str, Any]:
    """Set the permission tier."""
    try:
        payload = json.loads(body) if body else {}
        tier_value = payload.get("tier", "")
        if not tier_value:
            return {"status": 400, "error": "Missing 'tier' in request body"}

        from backend.domain.core.permission_model import PermissionModel, PermissionTier
        try:
            new_tier = PermissionTier(tier_value)
        except ValueError:
            return {
                "status": 400,
                "error": f"Invalid tier '{tier_value}'. Valid: restricted, auto_approve, yolo",
            }

        pm = PermissionModel.default()
        pm.set_tier(new_tier)
        return {"status": 200, "data": {"tier": pm.tier.value, "updated": True}}
    except json.JSONDecodeError:
        return {"status": 400, "error": "Invalid JSON body"}


# ── Compact ─────────────────────────────────────────────────────────────


def _handle_compact_history(openclaw_dir: str) -> dict[str, Any]:
    """Compaction history."""
    try:
        from backend.domain.core.context_compact import ContextCompactor
        compactor = ContextCompactor.default()
        return {
            "status": 200,
            "data": {
                "history": compactor.compact_history,
                "count": len(compactor.compact_history),
            },
        }
    except Exception:
        return {"status": 200, "data": {"history": [], "count": 0}}


# ── Query Stats ─────────────────────────────────────────────────────────


def _handle_query_stats(params: dict, openclaw_dir: str) -> dict[str, Any]:
    """Query engine execution stats."""
    try:
        from backend.domain.core.query_engine import QueryEngine
        engine = QueryEngine.default()
        return {"status": 200, "data": engine.stats}
    except Exception:
        return {"status": 200, "data": {"error": "query engine not initialized"}}
