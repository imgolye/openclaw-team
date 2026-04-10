#!/usr/bin/env python3
"""Generate and serve a visual collaboration dashboard for all agents."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import hmac
import io
import json
import logging
import mimetypes
import os
import queue
import re
import secrets
import shlex
import shutil
import subprocess
import threading
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict, deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import zipfile

try:
    import psycopg
except Exception:  # pragma: no cover - optional during import-time analysis
    psycopg = None

try:
    import redis
except Exception:  # pragma: no cover - optional during import-time analysis
    redis = None

from backend.adapters.storage.dashboard import (
    append_audit_event as store_append_audit_event,
    check_storage_readiness as store_check_storage_readiness,
    create_management_run as store_create_management_run,
    delete_chat_thread as store_delete_chat_thread,
    append_memory_event as store_append_memory_event,
    count_chat_messages_before as store_count_chat_messages_before,
    list_chat_messages as store_list_chat_messages,
    list_chat_messages_before as store_list_chat_messages_before,
    list_recent_chat_messages as store_list_recent_chat_messages,
    list_chat_thread_message_summaries as store_list_chat_thread_message_summaries,
    list_chat_threads as store_list_chat_threads,
    list_memory_events as store_list_memory_events,
    list_memory_snapshots as store_list_memory_snapshots,
    list_task_records as store_list_task_records,
    get_chat_thread as store_get_chat_thread,
    get_memory_snapshot as store_get_memory_snapshot,
    get_task_record as store_get_task_record,
    list_agent_teams as store_list_agent_teams,
    create_tenant_api_key as store_create_tenant_api_key,
    delete_product_installation as store_delete_product_installation,
    list_automation_alerts as store_list_automation_alerts,
    list_automation_rules as store_list_automation_rules,
    list_management_runs as store_list_management_runs,
    list_model_provider_configs as store_list_model_provider_configs,
    list_orchestration_workflow_versions as store_list_orchestration_workflow_versions,
    list_orchestration_workflows as store_list_orchestration_workflows,
    list_notification_channels as store_list_notification_channels,
    list_customer_access_channels as store_list_customer_access_channels,
    list_notification_deliveries as store_list_notification_deliveries,
    list_routing_decisions as store_list_routing_decisions,
    list_routing_policies as store_list_routing_policies,
    list_skill_role_profiles as store_list_skill_role_profiles,
    list_theme_workforce_profiles as store_list_theme_workforce_profiles,
    list_tenant_api_keys as store_list_tenant_api_keys,
    list_tenant_installations as store_list_tenant_installations,
    list_tenants as store_list_tenants,
    list_workflow_packs as store_list_workflow_packs,
    load_audit_events as store_load_audit_events,
    load_product_installations as store_load_product_installations,
    load_product_users as store_load_product_users,
    resolve_automation_alerts as store_resolve_automation_alerts,
    resolve_tenant_api_key as store_resolve_tenant_api_key,
    restore_orchestration_workflow_version as store_restore_orchestration_workflow_version,
    save_product_users as store_save_product_users,
    save_automation_rule as store_save_automation_rule,
    save_notification_channel as store_save_notification_channel,
    save_customer_access_channel as store_save_customer_access_channel,
    save_notification_delivery as store_save_notification_delivery,
    save_orchestration_workflow as store_save_orchestration_workflow,
    save_management_run_record as store_save_management_run_record,
    save_model_provider_config as store_save_model_provider_config,
    save_routing_decision as store_save_routing_decision,
    save_routing_policy as store_save_routing_policy,
    save_skill_role_profile as store_save_skill_role_profile,
    save_theme_workforce_profile as store_save_theme_workforce_profile,
    save_workflow_pack as store_save_workflow_pack,
    save_management_run_pack_binding as store_save_management_run_pack_binding,
    save_management_run_planning_binding as store_save_management_run_planning_binding,
    save_chat_message as store_save_chat_message,
    save_chat_thread as store_save_chat_thread,
    save_memory_snapshot as store_save_memory_snapshot,
    save_agent_team as store_save_agent_team,
    save_tenant as store_save_tenant,
    save_tenant_installation as store_save_tenant_installation,
    store_path as dashboard_store_path,
    touch_product_user_login as store_touch_product_user_login,
    touch_tenant_api_key as store_touch_tenant_api_key,
    replace_task_records_for_workspace as store_replace_task_records_for_workspace,
    upsert_automation_alert as store_upsert_automation_alert,
    upsert_product_user as store_upsert_product_user,
    update_management_run as store_update_management_run,
    upsert_product_installation as store_upsert_product_installation,
)
from backend.presentation.http.http import (
    handle_http_delete as dispatch_http_delete,
    handle_http_get as dispatch_http_get,
    handle_http_head as dispatch_http_head,
    handle_http_options as dispatch_http_options,
    handle_http_post as dispatch_http_post,
    handle_http_put as dispatch_http_put,
)
from backend.adapters.integrations.openclaw import (
    browser_profile_candidate_names,
    configured_browser_default_profile,
    default_managed_openclaw_dirs,
    fallback_openclaw_installation_payload,
    guarded_http_request,
    join_command_output,
    load_browser_profiles_payload,
    load_browser_status_payload,
    load_openclaw_agent_auth_payload,
    load_openclaw_control_data,
    load_openclaw_control_data_subprocess,
    load_openclaw_dashboard_summary,
    load_openclaw_gateway_panel_data,
    load_openclaw_installation_payload,
    load_openclaw_browser_panel_data,
    load_openclaw_memory_panel_data,
    load_openclaw_memory_workflow_panel_data,
    load_openclaw_message_gateway_panel_data,
    load_openclaw_sandbox_panel_data,
    load_openclaw_skill_growth_panel_data,
    load_openclaw_voice_workflow_panel_data,
    load_openclaw_runtime_overview_panel_data,
    load_openclaw_orchestration_panel_data,
    load_openclaw_session_governance_panel_data,
    load_openclaw_local_runtime_panel_data,
    load_openclaw_models_panel_data,
    load_openclaw_skills_check_panel_data,
    load_openclaw_agent_params_panel_data,
    load_local_runtime_payload,
    load_model_execution_architecture_payload,
    load_runtime_sync_payload,
    inspect_local_runtime_model_dir,
    normalize_browser_profiles_payload,
    normalize_browser_status_payload,
    openclaw_agent_auth_targets,
    openclaw_auth_sync_script_path,
    openclaw_browser_command,
    openclaw_command_bin_dirs,
    openclaw_command_env,
    openclaw_installer_script_path,
    perform_local_model_runtime_configure,
    perform_local_model_runtime_use_recommended_profile,
    perform_local_model_runtime_start,
    perform_local_model_runtime_stop,
    perform_openclaw_browser_probe_refresh,
    perform_openclaw_execution_configure,
    perform_openclaw_speech_runtime_configure,
    perform_openclaw_speech_runtime_use_local_preset,
    perform_openclaw_auth_sync,
    perform_openclaw_gateway_probe_refresh,
    perform_model_rollout,
    perform_openclaw_cli_install_action,
    perform_openclaw_provider_setup,
    perform_runtime_sync_action,
    resolve_available_browser_profile,
    resolve_browser_command_profile,
    resolve_openclaw_fetch_guard_module,
    resolve_planning_project_dir,
    resolve_project_dir,
    run_command,
    run_python_script,
    runtime_script_path,
    runtime_sync_script_path,
)
from backend.adapters.integrations.wechat import (
    build_wechat_text_reply,
    chat_thread_message_requests_voice_reply,
    customer_access_callback_path,
    customer_channel_is_wechat,
    customer_channel_voice_reply_config,
    dispatch_customer_wechat_message,
    generate_customer_voice_reply_audio,
    handle_wechat_customer_access_get,
    handle_wechat_customer_access_post,
    load_openclaw_weixin_runtime,
    maybe_attach_chat_thread_voice_reply,
    maybe_forward_chat_thread_reply_to_wechat,
    parse_wechat_xml_message,
    perform_customer_channel_voice_test,
    send_openclaw_weixin_customer_reply,
    send_wechat_customer_service_text,
    send_wechat_customer_service_voice,
    summarize_wechat_inbound_message,
    try_send_wechat_customer_service_voice,
    upload_wechat_temporary_media,
    verify_wechat_signature,
    wechat_official_channel_config,
)
from backend.model_decision_adapter import run_task_decision as run_model_task_decision
from backend.application.services.admin import (
    append_audit_event,
    admin_data_cache_key,
    api_scope_allows,
    build_admin_audit_logs_snapshot,
    build_admin_api_keys_snapshot,
    build_admin_bootstrap_snapshot,
    build_admin_data,
    build_admin_installations_snapshot,
    build_admin_tenants_snapshot,
    build_admin_users_snapshot,
    build_external_api_reference,
    build_tenant_admin_data,
    create_product_user,
    decode_session_cookie,
    default_installation_label,
    encode_session_cookie,
    ensure_default_install_bootstrap,
    ensure_default_openclaw_config_bootstrap,
    ensure_default_product_users_bootstrap,
    ensure_active_owner_guard,
    expected_action_value,
    find_product_user_entry,
    find_tenant_record,
    hash_password,
    load_audit_events,
    load_product_users,
    normalize_username,
    permissions_for_role,
    register_installation,
    remove_installation,
    reset_product_user_password,
    resolve_dashboard_auth_token,
    role_meta,
    safe_user_record,
    sync_current_installation_registry,
    tenant_primary_openclaw_dir,
    tenant_rest_catalog_payload,
    touch_product_user_login,
    update_product_user_access,
    update_product_user_login,
    upsert_product_user,
    verify_password,
)
from backend.application.services.auth_session import (
    actor_from_session,
    api_key_record as resolve_api_key_record,
    api_key_value as resolve_api_key_value,
    audit as write_auth_audit,
    auth_mode as resolve_auth_mode,
    auth_payload as build_auth_payload,
    authenticate_password as perform_password_auth,
    build_session_data as build_session_data_for_auth,
    can as session_can,
    clear_cookie_header as clear_auth_cookie_header,
    current_actor as resolve_current_actor,
    current_session as resolve_current_session,
    find_product_user,
    handle_auth_login_json as handle_auth_login_json_request,
    handle_auth_logout_json as handle_auth_logout_json_request,
    handle_auth_session_get as handle_auth_session_get_request,
    handle_login_get as handle_login_get_request,
    handle_logout_post as handle_logout_post_request,
    is_authenticated as session_is_authenticated,
    login_cookie_header as build_login_cookie_header,
    permissions as resolve_session_permissions,
    require_action_token as require_action_token_from_payload,
    require_auth as require_session_auth,
    require_capability as require_permission_capability,
    rest_auth_context as build_rest_auth_context,
    safe_next_path,
    session_for_client,
    tenant_openclaw_dir as resolve_tenant_openclaw_dir,
)
from backend.application.services.chat import (
    build_chat_thread_compressed_dispatch_message,
    build_chat_data,
    chat_thread_attachment_content,
    chat_thread_rotation_session_id,
    chat_thread_session_id,
    compact_chat_thread_detail,
    maybe_prepare_chat_thread_context_compression,
    load_chat_thread_detail,
)
from backend.application.services.customer_access import (
    auto_selected_skill_slugs_for_message,
    build_customer_access_snapshot,
    customer_voice_custom_sample_payload,
    customer_voice_custom_voice_id,
    customer_voice_custom_voices,
    customer_voice_runtime_builtin_voices,
    enrich_customer_access_channel_payload,
    ensure_customer_access_thread,
    extract_json_object_from_output,
    parse_boolish,
    perform_disable_customer_access_channel,
    perform_enable_customer_access_channel,
    remove_agent_voice_reply_sample,
    resolve_customer_access_channel,
    save_agent_voice_reply_sample,
    update_agent_voice_reply_sample_prompt_text,
)
from backend.application.services.dashboard import (
    backfill_planning_bundles,
    build_activity_snapshot,
    build_agent_cards_snapshot,
    build_agent_team_options_snapshot,
    build_agent_teams_snapshot,
    backfill_task_intelligence,
    build_action_dashboard_payload,
    build_chat_catalog_page_snapshot,
    build_communications_audit_snapshot,
    build_communications_commands_snapshot,
    build_communications_delivery_snapshot,
    build_communications_failures_snapshot,
    build_communications_summary_snapshot,
    build_communications_terminals_snapshot,
    build_conversations_catalog_snapshot,
    build_deliverables_snapshot,
    build_management_automation_summary_snapshot,
    build_management_automation_alerts_snapshot,
    build_management_automation_delivery_snapshot,
    build_management_automation_rules_snapshot,
    build_management_decision_intelligence_snapshot,
    build_management_decision_quality_snapshot,
    build_management_decision_recommendations_snapshot,
    build_management_decision_sources_snapshot,
    build_management_insights_conversations_snapshot,
    build_management_insights_health_agents_snapshot,
    build_management_insights_health_summary_snapshot,
    build_management_recommendations_preview_snapshot,
    build_management_reports_overview_snapshot,
    build_management_run_snapshot,
    build_management_runs_snapshot,
    build_management_summary_snapshot,
    build_metrics_snapshot,
    build_orchestration_overview_snapshot,
    build_orchestration_planning_snapshot,
    build_orchestration_replays_snapshot,
    build_orchestration_review_snapshot,
    build_orchestration_routing_snapshot,
    build_orchestration_suggestions_snapshot,
    build_orchestration_task_index_snapshot,
    build_orchestration_workflows_snapshot,
    build_theme_snapshot,
    build_label_maps,
    build_dashboard_data,
    compact_dashboard_bootstrap_payload,
    compact_management_bootstrap_payload,
    compact_openclaw_bootstrap_payload,
    get_router_agent_id,
    load_agents,
    load_conversation_catalog,
    load_config,
    load_context_hub_data,
    load_kanban_config,
    load_skills_detail,
    load_skill_pack_detail,
    load_task_detail,
    merge_tasks,
    now_utc,
)
from backend.presentation.http.runtime_parts.utils import (
    build_platform_runtime_governance_browser_sessions,
    build_platform_runtime_governance_packs,
    build_platform_runtime_governance_runtimes,
    build_platform_runtime_governance_summary,
    epoch_ms_to_iso,
)
from backend.application.services.desktop import (
    build_chat_thread_quick_snapshot,
)
from backend.application.services.http_shell import (
    bundle as build_handler_bundle,
    cors_headers as resolve_handler_cors_headers,
    deployment_metadata as load_handler_deployment_metadata,
    frontend_dist as resolve_handler_frontend_dist,
    health_payload as build_handler_health_payload,
    next_path as resolve_handler_next_path,
    path as resolve_handler_path,
    query as resolve_handler_query,
    read_json_body as read_handler_json_body,
    refreshed_bundle as build_handler_refreshed_bundle,
    runtime_data as build_handler_runtime_data,
    send_bytes as send_handler_bytes,
    send_frontend_unavailable as send_handler_frontend_unavailable,
    send_json as send_handler_json,
    send_preflight as send_handler_preflight,
    send_redirect as send_handler_redirect,
    serve_frontend_asset as serve_handler_frontend_asset,
    serve_frontend_index as serve_handler_frontend_index,
    state_dir_readiness as build_handler_state_dir_readiness,
    task_action_dashboard as build_task_action_dashboard,
)
from backend.presentation.http.service_catalog import (
    build_api_read_services,
    build_command_action_services,
    build_http_get_services,
    build_http_post_services,
    build_rest_get_services,
    build_rest_post_services,
    build_task_action_services,
)
from backend.application.services.management import (
    backfill_agent_team_policy_defaults,
    build_agent_team_data,
    build_communications_data,
    build_default_agent_team_blueprints,
    build_management_data,
    build_management_runs_data,
    communication_audit_category,
    ensure_default_management_bootstrap,
    ensure_default_team_mode,
    pick_team_agents,
    team_mode_agent_text,
    team_mode_copy,
    team_mode_language,
    team_runtime_wake_fields,
)
from backend.application.services.management_automation import (
    bootstrap_management_rules,
    build_incomplete_task_supervision_note,
    build_operational_reports,
    compute_conversation_quality_data,
    daily_review_due,
    export_management_daily_review,
    export_management_weekly_report,
    normalize_daily_review_schedule,
    normalize_escalation_steps,
    normalize_weekly_report_schedule,
    notification_channel_available,
    notification_channel_health,
    probe_notification_channel,
    process_daily_review_push,
    process_weekly_report_push,
    recommended_management_rules,
    refresh_notification_channel_health,
    render_management_daily_review,
    render_management_weekly_report,
    resolve_alert_escalation,
    summarize_notification_target,
    task_started_reference_at,
    task_supervision_team_id,
    weekly_report_due,
)
from backend.application.services.computer_use import (
    build_computer_use_devices_snapshot,
    build_computer_use_run_preview_snapshot,
    build_computer_use_run_actions_snapshot,
    build_computer_use_run_artifacts_snapshot,
    build_computer_use_run_snapshot,
    build_computer_use_run_steps_snapshot,
    build_computer_use_runs_snapshot,
    perform_computer_use_answer_clarification,
    perform_computer_use_run_approve,
    perform_computer_use_run_cancel,
    perform_computer_use_run_create,
    perform_computer_use_run_pause,
    perform_computer_use_run_resume,
    perform_computer_use_takeover_start,
    perform_computer_use_takeover_stop,
)
from backend.application.services.computer_use_execution import (
    perform_computer_use_takeover_action,
    perform_computer_use_run_execute,
    run_computer_use_engine_cycle,
)
from backend.application.services.live_events import (
    MissionControlHTTPServer,
    RedisLiveEventBus,
    publish_chat_thread_stream_event,
    publish_conversation_stream_event,
    publish_live_event,
    resolve_live_event_redis_url,
    serve_live_events,
)
from backend.application.services.memory import (
    build_memory_authority_context_message,
    build_memory_projection_payloads,
    current_memory_system,
    default_memory_system_payload,
    ensure_default_memory_authority_seed,
    ensure_default_memory_bootstrap,
    hydrate_thread_memory_authority,
    memory_authority_query_matches,
    memory_system_status_payload,
    normalize_memory_system_payload,
    project_memory_records,
    project_memory_records_async,
    should_project_memory_async,
    task_memory_template,
)
from backend.application.services.task_payload_compact import (
    compact_agent_team_payload,
    compact_auto_operation_profile,
    compact_chat_thread_deliverable_reference,
    compact_chat_thread_pack_reference,
    compact_chat_thread_run_reference,
    compact_chat_thread_summary_payload,
    compact_chat_thread_task_reference,
    compact_chat_thread_team_reference,
    compact_company_auto_operation_runtime,
    compact_task_dashboard_payload,
    compact_task_intelligence,
    compact_task_long_term_memory,
    compact_task_model_decision,
    compact_task_planning_bundle,
    compact_task_reference,
    compact_task_route_decision,
    compact_task_run_reference,
    compact_task_team_dispatch,
    compact_task_team_reference,
    compact_task_team_selection,
    compact_task_workflow_binding,
    summarize_task_execution_text,
)
from backend.application.services.bootstrap_defaults import (
    AGENT_ROLE_LABELS,
    AGENT_ROLE_WORKING_STYLE,
    ARTIFACT_TYPE_LABELS,
    DEFAULT_AGENT_PERSONA_BY_ID,
    DEFAULT_AGENT_PROFILE_BY_ID,
    DEFAULT_MANAGED_SKILL_SLUGS,
    DEFAULT_RECOMMENDED_TEAM_BY_MODE,
    GSTACK_SYNC_SKILLS,
    HANDOFF_ARTIFACT_TYPE_MAP,
    LOCAL_CODEX_IMPORTED_SKILL_ROLE_PROFILES,
    LOCAL_CODEX_IMPORTED_SKILL_SLUGS,
    PRODUCT_STARTER_SKILL_ROLE_PROFILES,
    SKILL_CATEGORY_LABELS,
    STARTER_SKILL_ROLE_PROFILES,
    STARTER_WORKFLOW_PACKS,
    artifact_type_from_handoff,
    artifact_type_label,
    augment_skills_payload_with_gstack_scaffolds,
    clean_unique_strings,
    dedupe_skills_payload_entries,
    ensure_gstack_skill_scaffold,
    gstack_skill_scaffold_markdown,
    gstack_skill_specs_by_slug,
    merge_workflow_pack_meta,
    merge_workflow_pack_record,
    merged_agent_runtime_profile,
    normalize_flag,
    product_default_agent_profile,
    recommended_team_id_for_pack_mode,
    refresh_skill_catalog_summary,
)
from backend.application.services.team_interaction import (
    agent_identity_display_name,
    agent_runtime_identity_payload,
    agent_runtime_profile_payload,
    agent_turn_priority_from_profile,
    build_human_turn_anchor_payload,
    build_human_turn_profile_payload,
    order_agent_ids_for_human_turns,
    pick_internal_discussion_specialist,
    select_human_turn_targets,
    should_task_start_with_internal_discussion,
    task_internal_discussion_plan,
    team_response_kind_map,
)
from backend.application.services.workflow_pack import (
    artifact_deliverable_payload,
    build_pack_workflow_lanes,
    build_pack_workflow_nodes,
    build_run_artifact_summary,
    default_pack_browser_profile,
    hydrate_chat_thread_pack_context,
    hydrate_linked_pack,
    hydrate_management_run_pack_context,
    normalize_pack_artifact_templates,
    normalize_pack_qa_policy,
    normalize_pack_release_policy,
    normalize_pack_review_gates,
    normalize_pack_runtime_policy,
    normalize_run_artifacts,
    normalize_run_qa_automation,
    normalize_run_release_automation,
    normalize_run_review_gates,
    normalize_run_runtime_sessions,
    normalize_workflow_pack_stages,
    pack_required_runtimes,
    resolve_pack_launch_target,
    resolve_workflow_pack_or_mode_record,
    resolve_workflow_pack_record,
    seed_run_qa_automation,
    seed_run_release_automation,
    seed_run_review_gates,
    seed_run_runtime_sessions,
    seeded_run_meta_from_pack,
    stage_skill_refs_for_pack,
    workflow_pack_binding_payload,
    workflow_pack_capabilities,
    workflow_pack_map_from_skills_payload,
    workflow_pack_meta_from_payload,
)
from backend.application.services.orchestration import (
    apply_conversation_fanout_stagger,
    apply_turn_guidance_to_message,
    build_task_execution_message,
    build_task_internal_discussion_message,
    build_task_team_fanout_message,
    build_task_team_member_message,
    build_team_collaboration_summary,
    build_team_coordination_relay_message,
    classify_team_collaboration_reply,
    conversation_reply_preview,
    coordination_reply_entries,
    coordination_reply_signature,
    dispatch_task_execution_team_members,
    existing_task_team_thread,
    human_resolution_contract_lines,
    is_managed_task_execution_thread,
    normalize_team_collaboration_reply,
    perform_task_block,
    perform_task_create,
    perform_task_done,
    perform_task_preview,
    perform_task_progress,
    perform_task_team_sync,
    latest_task_execution_completion_candidate,
    latest_task_execution_sync_candidate,
    relay_team_coordination_updates,
    repair_task_execution_backlog,
    resolve_task_dispatch_plan,
    schedule_chat_thread_coordination_relay,
    schedule_chat_thread_dispatch,
    start_task_execution_dispatch,
    summarize_internal_discussion_context,
    task_coordination_prompt_lines,
    task_coordination_protocol_snapshot,
    task_execution_bootstrap_for_task,
    task_execution_session_id,
    task_execution_sync_for_task,
    task_has_meaningful_progress,
    task_has_progress_after,
    task_execution_meta_for_thread,
    task_team_member_focus_hint,
    task_team_participant_agent_ids,
    task_team_thread_id,
    update_task_execution_bootstrap,
    update_task_team_dispatch_state,
    update_task_execution_sync_state,
)
from backend.presentation.http.command import handle_action_post as dispatch_action_post
from backend.presentation.http.query import (
    handle_api_read_route as dispatch_api_read_route,
    handle_api_write_route as dispatch_api_write_route,
)
from backend.presentation.http.runtime_parts.memory import (
    load_agent_memory_data,
    load_shared_context_data,
)
from backend.presentation.http.rest import (
    handle_rest_get as dispatch_rest_get,
    handle_rest_post as dispatch_rest_post,
)
from backend.presentation.http.aliases import (
    canonical_action_path as resolve_canonical_action_path,
    canonical_query_path as resolve_canonical_query_path,
    is_command_path as is_command_api_path,
    task_action_kind as resolve_task_action_kind,
)
from backend.presentation.http.task import handle_task_action_post as dispatch_task_action_post


TERMINAL_STATES = {"done", "cancelled", "canceled"}
# Repair thresholds: how long a task may sit in each transient state before the
# repair daemon considers it stuck and requeues it.
TASK_EXECUTION_REPAIR_SCHEDULED_MINUTES = 2   # queued but not yet dispatched
TASK_EXECUTION_REPAIR_DISPATCHED_MINUTES = 8  # dispatched but no heartbeat received
TASK_EXECUTION_REPAIR_FAILED_MINUTES = 5      # failed but not yet retried
TASK_EXECUTION_REPAIR_MAX_ATTEMPTS = 3        # max automatic retry attempts before giving up
TASK_EXECUTION_REPAIR_RESET_MINUTES = 30      # cooldown before retry counter resets
TASK_EXECUTION_REPAIR_BATCH_SIZE = 4          # tasks re-queued per repair cycle
TASK_EXECUTION_MAX_PARALLEL_DISPATCHES = 2    # concurrent agent dispatches (keeps CPU headroom)
TASK_EXECUTION_SYNC_BATCH_SIZE = 4            # tasks synced per status-sync cycle
TASK_EXECUTION_COMPLETION_SYNC_BATCH_SIZE = 4 # completion events processed per cycle
TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS = 60  # max wait for all agents to acknowledge fanout
TEAM_CONVERSATION_STAGGER_SECONDS = 0.65
TEAM_CONVERSATION_RELAY_STAGGER_SECONDS = 0.45
TEAM_CONVERSATION_MAX_PARALLEL = 2
TEAM_CONVERSATION_BROADCAST_REPLY_LIMIT = 3
TEAM_CONVERSATION_SYNC_REPLY_LIMIT = 3
TEAM_CONVERSATION_RELAY_REPLY_LIMIT = 2
CHAT_THREAD_CONTEXT_COMPRESSION_MESSAGE_THRESHOLD = 24
CHAT_THREAD_CONTEXT_COMPRESSION_CHARACTER_THRESHOLD = 12000
CHAT_THREAD_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES = 8
CHAT_THREAD_CONTEXT_COMPRESSION_MAX_HIGHLIGHTS = 18
CHAT_THREAD_CONTEXT_COMPRESSION_MAX_NEW_HIGHLIGHTS = 10
TASK_CREATE_MODEL_DECISION_TIMEOUT_SECONDS = max(
    3,
    min(int(os.environ.get("MISSION_CONTROL_TASK_CREATE_MODEL_TIMEOUT", "3") or 3), 12),
)  # override via env var; clamped to [3, 12] seconds
FAST_TASK_CREATE_HEURISTIC_CONFIDENCE_THRESHOLD = min(
    0.92,
    max(float(os.environ.get("MISSION_CONTROL_FAST_TASK_HEURISTIC_CONFIDENCE", "0.68") or 0.68), 0.45),
)
LIVE_EVENT_QUEUE_SIZE = 96
LIVE_EVENT_REDIS_CHANNEL = "mission_control_live_events"
LIVE_EVENT_WAIT_SECONDS = 0.35
LIVE_EVENT_PING_SECONDS = 12.0
PRODUCT_VERSION = "1.18.0"
OPENCLAW_BASELINE_RELEASE = "2026.3.12"
TEAM_RUNTIME_MODES = {"quiet", "lead_standby", "all_standby", "custom"}
TEAM_RUNTIME_DEFAULT_EVERY = "30m"
TEAM_ALWAYS_ON_RUNTIME_MODE = "all_standby"
TEAM_ALWAYS_ON_RUNTIME_EVERY = "15m"

TASK_EXECUTION_DISPATCH_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=TASK_EXECUTION_MAX_PARALLEL_DISPATCHES,
    thread_name_prefix="task-dispatch",
)
TASK_EXECUTION_DISPATCH_FUTURES = set()
TASK_EXECUTION_DISPATCH_FUTURES_LOCK = threading.Lock()
TASK_EXECUTION_AGENT_LOCKS = {}
TASK_EXECUTION_AGENT_LOCKS_GUARD = threading.Lock()
MEMORY_PROJECTION_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="memory-projection",
)
PASSWORD_HASH_ITERATIONS = 600_000  # OWASP PBKDF2-HMAC-SHA256 recommendation (2024)
USER_ROLES = {
    "owner": {
        "label": "Owner",
        "description": "管理产品、成员、主题和高风险动作。",
        "permissions": {"read", "task_write", "conversation_write", "theme_write", "admin_write", "audit_view"},
    },
    "operator": {
        "label": "Operator",
        "description": "负责推进任务、维护交付和处理运营现场。",
        "permissions": {"read", "task_write", "conversation_write", "audit_view"},
    },
    "viewer": {
        "label": "Viewer",
        "description": "只读查看现场、交付和协同动态。",
        "permissions": {"read"},
    },
}
DEFAULT_THEME_NAME = "corporate"
THEME_STYLES = {
    "corporate": {
        "bg": "#e7ece8",
        "bg2": "#f4f7f4",
        "ink": "#1f2e27",
        "muted": "#587064",
        "accent": "#1f7a63",
        "accentStrong": "#12503f",
        "accentSoft": "#a8d5bf",
        "panel": "rgba(246, 249, 246, 0.86)",
        "line": "rgba(43, 77, 61, 0.14)",
        "ok": "#2d7a4e",
        "warn": "#a66b1d",
        "danger": "#8c3232",
    },
}

THEME_CATALOG = {
    "corporate": {
        "displayName": "现代企业",
        "language": "zh-CN",
        "tagline": "更像真实团队分工的现代协同方式",
        "bestFor": "企业团队、项目协作、跨职能配合",
        "summary": "用更贴近真实岗位和团队分工的命名方式，让多 Agent 协同更容易被业务团队理解和接纳。",
    },
}
SELECTABLE_THEME_NAMES = ("corporate",)
SELECTABLE_THEME_CATALOG = {
    theme_name: THEME_CATALOG[theme_name]
    for theme_name in SELECTABLE_THEME_NAMES
    if theme_name in THEME_CATALOG
}

SESSION_COOKIE_NAME = "openclaw_team_session"
SESSION_COOKIE_MAX_AGE = 60 * 60 * 12  # 12-hour session lifetime
PROJECT_METADATA_FILENAME = "mission-control.json"
PROJECT_METADATA_KEY = "missionControl"
PRODUCT_MODE_TEAM = "team"
TEAM_BOOTSTRAP_VERSION = 10
SKILL_BOOTSTRAP_VERSION = 4
AGENT_PROFILE_BOOTSTRAP_VERSION = 8
MANAGEMENT_BOOTSTRAP_VERSION = 2
MEMORY_BOOTSTRAP_VERSION = 1
MANAGEMENT_AUTOMATION_MODE_MANUAL = "manual"
MANAGEMENT_AUTOMATION_MODE_ASSISTIVE = "assistive"
MANAGEMENT_AUTOMATION_MODE_FULL_AUTO = "full_auto"
MANAGEMENT_AUTOMATION_MODES = {
    MANAGEMENT_AUTOMATION_MODE_MANUAL,
    MANAGEMENT_AUTOMATION_MODE_ASSISTIVE,
    MANAGEMENT_AUTOMATION_MODE_FULL_AUTO,
}
DEFAULT_FRONTEND_ORIGINS = {
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:4173",
    "http://localhost:4173",
}
HANDLER_IGNORED_EXCEPTIONS = (
    BrokenPipeError,
    ConnectionResetError,
    TimeoutError,
)
EVENT_STREAM_STOP_EXCEPTIONS = (
    RuntimeError,
    FileNotFoundError,
) + ((psycopg.Error,) if psycopg is not None else ())



from pathlib import Path as _RuntimePartPath

_RUNTIME_PART_ORDER = (
    'utils',
    'task_execution',
    'dashboard',
    'chat',
    'team',
    'memory',
    'task',
    'agent',
    'theme',
    'management',
    'gateway',
    'auth',
    'session',
    'context_hub',
    'browser',
    'constants',
    'bootstrap',
)
_RUNTIME_PARTS_DIR = _RuntimePartPath(__file__).resolve().with_name('runtime_parts')
for _runtime_part_name in _RUNTIME_PART_ORDER:
    _runtime_part_path = _RUNTIME_PARTS_DIR / f'{_runtime_part_name}.py'
    exec(compile(_runtime_part_path.read_text(encoding='utf-8'), str(_runtime_part_path), 'exec'), globals(), globals())
del _runtime_part_name
del _runtime_part_path
del _RUNTIME_PART_ORDER
del _RUNTIME_PARTS_DIR
del _RuntimePartPath
