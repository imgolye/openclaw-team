#!/usr/bin/env python3
"""DDL schema definitions and migration logic."""
from __future__ import annotations

import json
from pathlib import Path

from backend.adapters.storage.connection import (
    _connect, _adapt_sql, _load_json,
    legacy_users_path, legacy_audit_path,
    SCHEMA_VERSION, INITIALIZED_SCHEMA_DATABASE_URLS,
)


def _metadata(conn, key, default=""):
    row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def _set_metadata(conn, key, value):
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def _computer_use_schema_sql():
    return """
        CREATE TABLE IF NOT EXISTS computer_devices (
            device_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            os_family TEXT NOT NULL,
            status TEXT NOT NULL,
            enrollment_status TEXT NOT NULL,
            auth_mode TEXT NOT NULL,
            capabilities_json TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_heartbeat_at TEXT NOT NULL,
            lease_owner TEXT NOT NULL,
            lease_expires_at TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            executor_version TEXT NOT NULL,
            protocol_version TEXT NOT NULL,
            min_control_plane_version TEXT NOT NULL,
            public_key_fingerprint TEXT NOT NULL,
            revoked_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS computer_credentials (
            credential_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL,
            provider TEXT NOT NULL,
            scope TEXT NOT NULL,
            secret_ref TEXT NOT NULL,
            status TEXT NOT NULL,
            allowed_domains_json TEXT NOT NULL,
            rotation_at TEXT NOT NULL,
            last_rotated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS computer_policies (
            policy_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            allowed_domains_json TEXT NOT NULL,
            allowed_apps_json TEXT NOT NULL,
            download_path_rules_json TEXT NOT NULL,
            requires_confirmation_json TEXT NOT NULL,
            profile_strategy TEXT NOT NULL,
            max_runtime_seconds INTEGER NOT NULL,
            max_actions INTEGER NOT NULL,
            artifact_retention_days INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS computer_runs (
            run_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            plan_version TEXT NOT NULL,
            objective TEXT NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            risk_level TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            current_step_id TEXT NOT NULL,
            needs_help_reason TEXT NOT NULL,
            clarification_reason TEXT NOT NULL,
            lease_epoch INTEGER NOT NULL,
            recovery_state_json TEXT NOT NULL,
            plan_json TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS computer_steps (
            step_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            step_key TEXT NOT NULL,
            intent TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            budget_ms INTEGER NOT NULL,
            action_count INTEGER NOT NULL,
            error_code TEXT NOT NULL,
            error_message TEXT NOT NULL,
            checkpoint_json TEXT NOT NULL,
            observation_summary_json TEXT NOT NULL,
            verification_summary_json TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS computer_actions (
            action_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            action_key TEXT NOT NULL,
            action_type TEXT NOT NULL,
            side_effect_level TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            target_json TEXT NOT NULL,
            input_json TEXT NOT NULL,
            result_json TEXT NOT NULL,
            success INTEGER NOT NULL,
            latency_ms INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS computer_artifacts (
            artifact_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            type TEXT NOT NULL,
            title TEXT NOT NULL,
            path TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            hash TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            encrypted INTEGER NOT NULL,
            retention_policy TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS computer_takeovers (
            takeover_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            started_by TEXT NOT NULL,
            start_reason TEXT NOT NULL,
            transport_mode TEXT NOT NULL,
            controller_session_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL,
            summary TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_computer_devices_tenant_updated_at ON computer_devices(tenant_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_devices_lease ON computer_devices(lease_owner, lease_expires_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_credentials_tenant_updated_at ON computer_credentials(tenant_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_policies_tenant_updated_at ON computer_policies(tenant_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_runs_tenant_updated_at ON computer_runs(tenant_id, updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_runs_device_updated_at ON computer_runs(device_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_runs_thread_updated_at ON computer_runs(thread_id, updated_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_computer_runs_idempotency_key ON computer_runs(idempotency_key);
        CREATE INDEX IF NOT EXISTS idx_computer_steps_run_created_at ON computer_steps(run_id, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_computer_steps_run_status ON computer_steps(run_id, status, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_actions_run_created_at ON computer_actions(run_id, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_computer_actions_step_created_at ON computer_actions(step_id, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_computer_artifacts_run_created_at ON computer_artifacts(run_id, created_at DESC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_artifacts_step_created_at ON computer_artifacts(step_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_computer_takeovers_run_started_at ON computer_takeovers(run_id, started_at DESC);
    """


def _ensure_schema(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS product_users (
            username TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_login_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS audit_events (
            id TEXT PRIMARY KEY,
            at TEXT NOT NULL,
            action TEXT NOT NULL,
            outcome TEXT NOT NULL,
            detail TEXT NOT NULL,
            actor_json TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS product_installations (
            openclaw_dir TEXT PRIMARY KEY,
            installation_id TEXT NOT NULL,
            label TEXT NOT NULL,
            project_dir TEXT NOT NULL,
            theme TEXT NOT NULL,
            router_agent_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS management_runs (
            run_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            goal TEXT NOT NULL,
            owner TEXT NOT NULL,
            status TEXT NOT NULL,
            stage_key TEXT NOT NULL,
            linked_task_id TEXT NOT NULL,
            linked_agent_id TEXT NOT NULL,
            linked_session_key TEXT NOT NULL,
            release_channel TEXT NOT NULL,
            risk_level TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            stages_json TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS automation_rules (
            rule_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            threshold_minutes INTEGER NOT NULL,
            cooldown_minutes INTEGER NOT NULL,
            severity TEXT NOT NULL,
            match_text TEXT NOT NULL,
            channel_ids_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS notification_channels (
            channel_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            channel_type TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            secret TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS customer_access_channels (
            channel_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            channel_type TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            entry_url TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS model_provider_configs (
            openclaw_dir TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            provider_label TEXT NOT NULL,
            key_value TEXT NOT NULL,
            status TEXT NOT NULL,
            env_keys_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL,
            PRIMARY KEY (openclaw_dir, provider_id)
        );

        CREATE TABLE IF NOT EXISTS automation_alerts (
            alert_id TEXT PRIMARY KEY,
            rule_id TEXT NOT NULL,
            event_key TEXT NOT NULL,
            title TEXT NOT NULL,
            detail TEXT NOT NULL,
            severity TEXT NOT NULL,
            status TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            triggered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS notification_deliveries (
            delivery_id TEXT PRIMARY KEY,
            alert_id TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            outcome TEXT NOT NULL,
            detail TEXT NOT NULL,
            delivered_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orchestration_workflows (
            workflow_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            lanes_json TEXT NOT NULL,
            nodes_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orchestration_workflow_versions (
            version_id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL,
            version_number INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            lanes_json TEXT NOT NULL,
            nodes_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS routing_policies (
            policy_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            keyword TEXT NOT NULL,
            target_agent_id TEXT NOT NULL,
            priority_level TEXT NOT NULL,
            queue_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS routing_decisions (
            decision_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            task_title TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            policy_name TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            workflow_version_id TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            matched_keyword TEXT NOT NULL,
            queue_name TEXT NOT NULL,
            priority_level TEXT NOT NULL,
            target_agent_id TEXT NOT NULL,
            source_text TEXT NOT NULL,
            decided_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tenants (
            tenant_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            primary_openclaw_dir TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tenant_installations (
            tenant_id TEXT NOT NULL,
            openclaw_dir TEXT NOT NULL,
            label TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL,
            PRIMARY KEY (tenant_id, openclaw_dir)
        );

        CREATE TABLE IF NOT EXISTS tenant_api_keys (
            key_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            prefix TEXT NOT NULL,
            status TEXT NOT NULL,
            scopes_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_threads (
            thread_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            channel TEXT NOT NULL,
            owner TEXT NOT NULL,
            primary_agent_id TEXT NOT NULL,
            current_target_agent_id TEXT NOT NULL,
            linked_task_id TEXT NOT NULL,
            linked_deliverable_id TEXT NOT NULL,
            linked_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_messages (
            message_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
            sender_kind TEXT NOT NULL,
            sender_id TEXT NOT NULL,
            sender_label TEXT NOT NULL,
            direction TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_thread_context_snapshots (
            snapshot_key TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
            snapshot_kind TEXT NOT NULL,
            summary TEXT NOT NULL,
            context_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_thread_context_segments (
            segment_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
            segment_kind TEXT NOT NULL,
            segment_order INTEGER NOT NULL,
            summary TEXT NOT NULL,
            segment_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_thread_events (
            event_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            summary TEXT NOT NULL,
            event_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS agent_teams (
            team_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            lead_agent_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS skill_role_profiles (
            skill_slug TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            stage TEXT NOT NULL,
            recommended_entry TEXT NOT NULL,
            output_contract_json TEXT NOT NULL,
            requires_runtime_json TEXT NOT NULL,
            handoff_artifacts_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workflow_packs (
            pack_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL,
            starter INTEGER NOT NULL,
            default_entry TEXT NOT NULL,
            recommended_team_id TEXT NOT NULL,
            stages_json TEXT NOT NULL,
            skills_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_snapshots (
            snapshot_key TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            label TEXT NOT NULL,
            summary TEXT NOT NULL,
            learning_json TEXT NOT NULL,
            notes_json TEXT NOT NULL,
            related_task_id TEXT NOT NULL,
            related_thread_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_events (
            event_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            summary TEXT NOT NULL,
            related_task_id TEXT NOT NULL,
            related_thread_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            meta_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_records (
            task_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            workspace_path TEXT NOT NULL,
            position_index INTEGER NOT NULL,
            title TEXT NOT NULL,
            state TEXT NOT NULL,
            owner TEXT NOT NULL,
            org TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            task_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_audit_events_at ON audit_events(at DESC);
        CREATE INDEX IF NOT EXISTS idx_product_users_created_at ON product_users(created_at);
        CREATE INDEX IF NOT EXISTS idx_product_installations_updated_at ON product_installations(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_management_runs_updated_at ON management_runs(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_automation_rules_updated_at ON automation_rules(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_notification_channels_updated_at ON notification_channels(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_customer_access_channels_updated_at ON customer_access_channels(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_model_provider_configs_updated_at ON model_provider_configs(openclaw_dir, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_automation_alerts_updated_at ON automation_alerts(updated_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_automation_alerts_unique_event ON automation_alerts(rule_id, event_key);
        CREATE INDEX IF NOT EXISTS idx_notification_deliveries_alert_id ON notification_deliveries(alert_id, delivered_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_delivery_unique_channel ON notification_deliveries(alert_id, channel_id);
        CREATE INDEX IF NOT EXISTS idx_orchestration_workflows_updated_at ON orchestration_workflows(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_orchestration_workflow_versions_workflow ON orchestration_workflow_versions(workflow_id, version_number DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_policies_updated_at ON routing_policies(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_decisions_decided_at ON routing_decisions(decided_at DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_decisions_policy_id ON routing_decisions(policy_id, decided_at DESC);
        CREATE INDEX IF NOT EXISTS idx_routing_decisions_task_id ON routing_decisions(task_id, decided_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tenants_updated_at ON tenants(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tenant_installations_tenant ON tenant_installations(tenant_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_tenant ON tenant_api_keys(tenant_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_threads_updated_at ON chat_threads(updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_messages_thread ON chat_messages(thread_id, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_chat_thread_context_snapshots_thread_updated_at ON chat_thread_context_snapshots(thread_id, updated_at DESC, created_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_thread_context_snapshots_thread_kind ON chat_thread_context_snapshots(thread_id, snapshot_kind);
        CREATE INDEX IF NOT EXISTS idx_chat_thread_context_segments_thread_order ON chat_thread_context_segments(thread_id, segment_order ASC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_thread_context_segments_thread_kind ON chat_thread_context_segments(thread_id, segment_kind, segment_order ASC);
        CREATE INDEX IF NOT EXISTS idx_chat_thread_events_thread_created_at ON chat_thread_events(thread_id, created_at DESC, event_id DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_thread_events_thread_type ON chat_thread_events(thread_id, event_type, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_agent_teams_updated_at ON agent_teams(updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_skill_role_profiles_mode_stage ON skill_role_profiles(mode, stage, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_skill_role_profiles_updated_at ON skill_role_profiles(updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_workflow_packs_status_updated_at ON workflow_packs(status, updated_at DESC, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_workflow_packs_mode ON workflow_packs(mode, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_snapshots_scope_updated_at ON memory_snapshots(scope, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_snapshots_related_task ON memory_snapshots(related_task_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_events_scope_created_at ON memory_events(scope, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_events_owner_created_at ON memory_events(owner_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_events_related_task_created_at ON memory_events(related_task_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_task_records_workspace ON task_records(workspace_id, position_index ASC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_task_records_updated_at ON task_records(updated_at DESC, created_at DESC);
        """
    )
    conn.executescript(_computer_use_schema_sql())
    _set_metadata(conn, "schema_version", str(SCHEMA_VERSION))
    conn.commit()


def _ensure_legacy_migration(openclaw_dir, conn):
    if _metadata(conn, "legacy_users_migrated") != "1":
        from backend.adapters.storage.auth import _normalize_user_record
        legacy_users = _load_json(legacy_users_path(openclaw_dir), {"users": []})
        for user in legacy_users.get("users", []) if isinstance(legacy_users, dict) else []:
            normalized = _normalize_user_record(user)
            if not normalized:
                continue
            conn.execute(
                """
                INSERT INTO product_users(
                    username, user_id, display_name, role, password_hash, status, created_at, last_login_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    user_id = excluded.user_id,
                    display_name = excluded.display_name,
                    role = excluded.role,
                    password_hash = excluded.password_hash,
                    status = excluded.status,
                    created_at = excluded.created_at,
                    last_login_at = excluded.last_login_at
                """,
                (
                    normalized["username"],
                    normalized["user_id"],
                    normalized["display_name"],
                    normalized["role"],
                    normalized["password_hash"],
                    normalized["status"],
                    normalized["created_at"],
                    normalized["last_login_at"],
                ),
            )
        _set_metadata(conn, "legacy_users_migrated", "1")

    if _metadata(conn, "legacy_audit_migrated") != "1":
        legacy_path = legacy_audit_path(openclaw_dir)
        if legacy_path.exists():
            for line in legacy_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                from backend.adapters.storage.auth import _normalize_audit_event
                normalized = _normalize_audit_event(event)
                if not normalized:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO audit_events(
                        id, at, action, outcome, detail, actor_json, meta_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized["id"],
                        normalized["at"],
                        normalized["action"],
                        normalized["outcome"],
                        normalized["detail"],
                        normalized["actor_json"],
                        normalized["meta_json"],
                    ),
                )
        _set_metadata(conn, "legacy_audit_migrated", "1")

    if _metadata(conn, "legacy_task_records_migrated") != "1":
        _set_metadata(conn, "legacy_task_records_migrated", "1")

    # Migration: add FK constraint to chat_messages.thread_id (SQLite requires table rebuild)
    if _metadata(conn, "chat_messages_fk_migrated") != "1":
        _migrate_add_chat_messages_fk(conn)
        _set_metadata(conn, "chat_messages_fk_migrated", "1")

    conn.commit()


def _migrate_add_chat_messages_fk(conn):
    """Rebuild chat_messages table to add FOREIGN KEY on thread_id."""
    if getattr(conn, "backend_kind", "") == "postgres":
        # PostgreSQL databases created from the current schema already include
        # the FK, so the SQLite table-rebuild migration is not needed here.
        return
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("ALTER TABLE chat_messages RENAME TO _chat_messages_old")
        conn.execute(
            """
            CREATE TABLE chat_messages (
                message_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
                sender_kind TEXT NOT NULL,
                sender_id TEXT NOT NULL,
                sender_label TEXT NOT NULL,
                direction TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL,
                meta_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO chat_messages SELECT * FROM _chat_messages_old"
        )
        conn.execute("DROP TABLE _chat_messages_old")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_messages_thread ON chat_messages(thread_id, created_at ASC)")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()
    except Exception:
        conn.execute("PRAGMA foreign_keys = ON")
        raise
