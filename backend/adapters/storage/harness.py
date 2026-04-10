"""Persistence adapter for the Agent Harness subsystems.

Stores cost events, permission decisions, compact operations,
and query execution stats to the database.

Uses the same _connect / _StoreConnection pattern as the rest
of the storage layer. Tables are auto-created on first use.

Usage::

    from backend.adapters.storage.harness import HarnessStore

    store = HarnessStore(openclaw_dir)
    store.ensure_tables()

    # Record a cost event
    store.insert_cost_event({
        "agent_id": "eng-01",
        "model": "claude-sonnet-4-20250514",
        "input_tokens": 1500,
        "output_tokens": 800,
        "cost_usd": 0.015,
        "timestamp": 1711900000.0,
    })

    # Query cost data
    events = store.list_cost_events(agent_id="eng-01", limit=100)
    summary = store.get_cost_summary(days=7)
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Import the project's connection helper
try:
    from backend.adapters.storage.connection import _connect, _adapt_sql
except ImportError:
    # Fallback for standalone testing
    _connect = None
    _adapt_sql = None


# ── Table DDL ───────────────────────────────────────────────────────────

HARNESS_TABLES_DDL = [
    # Cost events
    """
    CREATE TABLE IF NOT EXISTS harness_cost_events (
        id          SERIAL PRIMARY KEY,
        agent_id    TEXT NOT NULL DEFAULT '',
        task_id     TEXT NOT NULL DEFAULT '',
        model       TEXT NOT NULL DEFAULT '',
        input_tokens  INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cost_usd    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        event_type  TEXT NOT NULL DEFAULT 'llm_call',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at  DOUBLE PRECISION NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_hce_agent ON harness_cost_events(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_hce_created ON harness_cost_events(created_at)",

    # Permission decisions
    """
    CREATE TABLE IF NOT EXISTS harness_permission_log (
        id          SERIAL PRIMARY KEY,
        tool_name   TEXT NOT NULL,
        agent_id    TEXT NOT NULL DEFAULT '',
        decision    TEXT NOT NULL,
        reason      TEXT NOT NULL DEFAULT '',
        risk_level  TEXT NOT NULL DEFAULT '',
        tier        TEXT NOT NULL DEFAULT '',
        context_json TEXT NOT NULL DEFAULT '{}',
        created_at  DOUBLE PRECISION NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_hpl_tool ON harness_permission_log(tool_name)",
    "CREATE INDEX IF NOT EXISTS idx_hpl_created ON harness_permission_log(created_at)",

    # Learned approval patterns
    """
    CREATE TABLE IF NOT EXISTS harness_approval_patterns (
        id           SERIAL PRIMARY KEY,
        tool_name    TEXT NOT NULL,
        agent_id     TEXT NOT NULL DEFAULT '',
        context_key  TEXT NOT NULL DEFAULT '',
        approved_by  TEXT NOT NULL DEFAULT '',
        use_count    INTEGER NOT NULL DEFAULT 0,
        approved_at  DOUBLE PRECISION NOT NULL,
        last_used_at DOUBLE PRECISION NOT NULL DEFAULT 0,
        expires_at   DOUBLE PRECISION NOT NULL DEFAULT 0,
        UNIQUE(tool_name, agent_id, context_key)
    )
    """,

    # Compact operations
    """
    CREATE TABLE IF NOT EXISTS harness_compact_log (
        id              SERIAL PRIMARY KEY,
        agent_id        TEXT NOT NULL DEFAULT '',
        original_count  INTEGER NOT NULL DEFAULT 0,
        compacted_count INTEGER NOT NULL DEFAULT 0,
        tokens_before   INTEGER NOT NULL DEFAULT 0,
        tokens_after    INTEGER NOT NULL DEFAULT 0,
        duration_ms     DOUBLE PRECISION NOT NULL DEFAULT 0,
        created_at      DOUBLE PRECISION NOT NULL
    )
    """,

    # Query execution stats
    """
    CREATE TABLE IF NOT EXISTS harness_query_stats (
        id              SERIAL PRIMARY KEY,
        query_id        TEXT NOT NULL DEFAULT '',
        agent_id        TEXT NOT NULL DEFAULT '',
        total_turns     INTEGER NOT NULL DEFAULT 0,
        total_input_tokens  INTEGER NOT NULL DEFAULT 0,
        total_output_tokens INTEGER NOT NULL DEFAULT 0,
        total_tool_calls    INTEGER NOT NULL DEFAULT 0,
        compactions     INTEGER NOT NULL DEFAULT 0,
        stop_reason     TEXT NOT NULL DEFAULT '',
        duration_ms     DOUBLE PRECISION NOT NULL DEFAULT 0,
        created_at      DOUBLE PRECISION NOT NULL
    )
    """,
]


# ── Harness Store ──────────────────────────────────────────────────────


class HarnessStore:
    """Database persistence for all harness subsystem data."""

    def __init__(self, openclaw_dir: str) -> None:
        self._openclaw_dir = openclaw_dir
        self._tables_created = False

    # ── Schema ──────────────────────────────────────────────────────

    def ensure_tables(self) -> None:
        """Create harness tables if they don't exist."""
        if self._tables_created:
            return
        try:
            with _connect(self._openclaw_dir) as conn:
                for ddl in HARNESS_TABLES_DDL:
                    conn.execute(ddl)
            self._tables_created = True
            logger.info("Harness tables ensured")
        except Exception:
            logger.exception("Failed to create harness tables")

    # ── Cost Events ─────────────────────────────────────────────────

    def insert_cost_event(self, event: dict[str, Any]) -> None:
        """Insert a cost event record."""
        self.ensure_tables()
        try:
            with _connect(self._openclaw_dir) as conn:
                conn.execute(
                    """INSERT INTO harness_cost_events
                       (agent_id, task_id, model, input_tokens, output_tokens,
                        cost_usd, event_type, metadata_json, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event.get("agent_id", ""),
                        event.get("task_id", ""),
                        event.get("model", ""),
                        event.get("input_tokens", 0),
                        event.get("output_tokens", 0),
                        event.get("cost_usd", 0.0),
                        event.get("event_type", "llm_call"),
                        json.dumps(event.get("metadata", {}), ensure_ascii=False),
                        event.get("timestamp", time.time()),
                    ),
                )
        except Exception:
            logger.exception("Failed to insert cost event")

    def list_cost_events(
        self,
        *,
        agent_id: str = "",
        since: float = 0.0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query cost events."""
        self.ensure_tables()
        try:
            clauses = ["1=1"]
            params: list[Any] = []
            if agent_id:
                clauses.append("agent_id = ?")
                params.append(agent_id)
            if since > 0:
                clauses.append("created_at >= ?")
                params.append(since)

            where = " AND ".join(clauses)
            params.append(limit)

            with _connect(self._openclaw_dir) as conn:
                rows = conn.execute(
                    f"SELECT * FROM harness_cost_events WHERE {where} ORDER BY created_at DESC LIMIT ?",
                    tuple(params),
                ).fetchall()

            return [self._row_to_dict(r) for r in rows]
        except Exception:
            logger.exception("Failed to list cost events")
            return []

    def get_cost_summary(
        self,
        *,
        days: int = 7,
        agent_id: str = "",
    ) -> dict[str, Any]:
        """Get aggregated cost summary."""
        self.ensure_tables()
        since = time.time() - days * 86400
        try:
            clauses = ["created_at >= ?"]
            params: list[Any] = [since]
            if agent_id:
                clauses.append("agent_id = ?")
                params.append(agent_id)

            where = " AND ".join(clauses)

            with _connect(self._openclaw_dir) as conn:
                row = conn.execute(
                    f"""SELECT
                        COUNT(*) as total_events,
                        SUM(input_tokens) as total_input,
                        SUM(output_tokens) as total_output,
                        SUM(cost_usd) as total_cost,
                        COUNT(DISTINCT agent_id) as unique_agents,
                        COUNT(DISTINCT model) as unique_models
                    FROM harness_cost_events
                    WHERE {where}""",
                    tuple(params),
                ).fetchone()

            if row:
                return {
                    "days": days,
                    "total_events": row[0] or 0,
                    "total_input_tokens": row[1] or 0,
                    "total_output_tokens": row[2] or 0,
                    "total_cost_usd": round(row[3] or 0, 6),
                    "unique_agents": row[4] or 0,
                    "unique_models": row[5] or 0,
                }
            return {"days": days, "total_events": 0}
        except Exception:
            logger.exception("Failed to get cost summary")
            return {"days": days, "error": "query failed"}

    # ── Permission Log ──────────────────────────────────────────────

    def insert_permission_decision(self, decision: dict[str, Any]) -> None:
        """Record a permission decision."""
        self.ensure_tables()
        try:
            with _connect(self._openclaw_dir) as conn:
                conn.execute(
                    """INSERT INTO harness_permission_log
                       (tool_name, agent_id, decision, reason, risk_level, tier,
                        context_json, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        decision.get("tool_name", ""),
                        decision.get("agent_id", ""),
                        decision.get("decision", ""),
                        decision.get("reason", ""),
                        decision.get("risk_level", ""),
                        decision.get("tier", ""),
                        json.dumps(decision.get("context", {}), ensure_ascii=False),
                        decision.get("timestamp", time.time()),
                    ),
                )
        except Exception:
            logger.exception("Failed to insert permission decision")

    def list_permission_decisions(
        self,
        *,
        tool_name: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query permission decisions."""
        self.ensure_tables()
        try:
            clauses = ["1=1"]
            params: list[Any] = []
            if tool_name:
                clauses.append("tool_name = ?")
                params.append(tool_name)
            where = " AND ".join(clauses)
            params.append(limit)

            with _connect(self._openclaw_dir) as conn:
                rows = conn.execute(
                    f"SELECT * FROM harness_permission_log WHERE {where} ORDER BY created_at DESC LIMIT ?",
                    tuple(params),
                ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        except Exception:
            logger.exception("Failed to list permission decisions")
            return []

    # ── Approval Patterns ───────────────────────────────────────────

    def save_approval_patterns(self, patterns: list[dict]) -> None:
        """Persist approval patterns (upsert)."""
        self.ensure_tables()
        try:
            with _connect(self._openclaw_dir) as conn:
                for p in patterns:
                    conn.execute(
                        """INSERT INTO harness_approval_patterns
                           (tool_name, agent_id, context_key, approved_by,
                            use_count, approved_at, last_used_at, expires_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT (tool_name, agent_id, context_key)
                           DO UPDATE SET approved_by = EXCLUDED.approved_by,
                                         use_count = EXCLUDED.use_count,
                                         approved_at = EXCLUDED.approved_at,
                                         last_used_at = EXCLUDED.last_used_at,
                                         expires_at = EXCLUDED.expires_at""",
                        (
                            p.get("tool_name", ""),
                            p.get("agent_id", ""),
                            p.get("context_key", ""),
                            p.get("approved_by", ""),
                            p.get("use_count", 0),
                            p.get("approved_at", time.time()),
                            p.get("last_used_at", 0),
                            p.get("expires_at", 0),
                        ),
                    )
        except Exception:
            logger.exception("Failed to save approval patterns")

    def load_approval_patterns(self) -> list[dict]:
        """Load all non-expired approval patterns."""
        self.ensure_tables()
        try:
            now = time.time()
            with _connect(self._openclaw_dir) as conn:
                rows = conn.execute(
                    "SELECT * FROM harness_approval_patterns WHERE expires_at = 0 OR expires_at > ?",
                    (now,),
                ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        except Exception:
            logger.exception("Failed to load approval patterns")
            return []

    # ── Compact Log ─────────────────────────────────────────────────

    def insert_compact_event(self, event: dict[str, Any]) -> None:
        """Record a compaction event."""
        self.ensure_tables()
        try:
            with _connect(self._openclaw_dir) as conn:
                conn.execute(
                    """INSERT INTO harness_compact_log
                       (agent_id, original_count, compacted_count,
                        tokens_before, tokens_after, duration_ms, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event.get("agent_id", ""),
                        event.get("original_count", 0),
                        event.get("compacted_count", 0),
                        event.get("tokens_before", 0),
                        event.get("tokens_after", 0),
                        event.get("duration_ms", 0.0),
                        event.get("timestamp", time.time()),
                    ),
                )
        except Exception:
            logger.exception("Failed to insert compact event")

    # ── Query Stats ─────────────────────────────────────────────────

    def insert_query_stats(self, stats: dict[str, Any]) -> None:
        """Record a query execution's stats."""
        self.ensure_tables()
        try:
            with _connect(self._openclaw_dir) as conn:
                conn.execute(
                    """INSERT INTO harness_query_stats
                       (query_id, agent_id, total_turns, total_input_tokens,
                        total_output_tokens, total_tool_calls, compactions,
                        stop_reason, duration_ms, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        stats.get("query_id", ""),
                        stats.get("agent_id", ""),
                        stats.get("total_turns", 0),
                        stats.get("total_input_tokens", 0),
                        stats.get("total_output_tokens", 0),
                        stats.get("total_tool_calls", 0),
                        stats.get("compactions", 0),
                        stats.get("stop_reason", ""),
                        stats.get("duration_ms", 0.0),
                        stats.get("timestamp", time.time()),
                    ),
                )
        except Exception:
            logger.exception("Failed to insert query stats")

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row) -> dict[str, Any]:
        """Convert a database row to a dict."""
        if hasattr(row, "_asdict"):
            return row._asdict()
        if hasattr(row, "keys"):
            return dict(row)
        # sqlite3.Row-like or tuple
        return {f"col_{i}": v for i, v in enumerate(row)}
