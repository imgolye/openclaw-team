--
-- OpenClaw Team — PostgreSQL schema baseline
-- Exported from PostgreSQL 16. Reference only — the backend auto-creates
-- tables on first start via backend/adapters/storage/schema.py.
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: agent_teams; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_teams (
    team_id text NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    lead_agent_id text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: audit_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_events (
    id text NOT NULL,
    at text NOT NULL,
    action text NOT NULL,
    outcome text NOT NULL,
    detail text NOT NULL,
    actor_json text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: automation_alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.automation_alerts (
    alert_id text NOT NULL,
    rule_id text NOT NULL,
    event_key text NOT NULL,
    title text NOT NULL,
    detail text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL,
    source_type text NOT NULL,
    source_id text NOT NULL,
    triggered_at text NOT NULL,
    updated_at text NOT NULL,
    resolved_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: automation_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.automation_rules (
    rule_id text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    status text NOT NULL,
    trigger_type text NOT NULL,
    threshold_minutes integer NOT NULL,
    cooldown_minutes integer NOT NULL,
    severity text NOT NULL,
    match_text text NOT NULL,
    channel_ids_json text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: chat_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_messages (
    message_id text NOT NULL,
    thread_id text NOT NULL,
    sender_kind text NOT NULL,
    sender_id text NOT NULL,
    sender_label text NOT NULL,
    direction text NOT NULL,
    body text NOT NULL,
    created_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: chat_thread_context_segments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_thread_context_segments (
    segment_id text NOT NULL,
    thread_id text NOT NULL,
    segment_kind text NOT NULL,
    segment_order integer NOT NULL,
    summary text NOT NULL,
    segment_json text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: chat_thread_context_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_thread_context_snapshots (
    snapshot_key text NOT NULL,
    thread_id text NOT NULL,
    snapshot_kind text NOT NULL,
    summary text NOT NULL,
    context_json text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: chat_thread_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_thread_events (
    event_id text NOT NULL,
    thread_id text NOT NULL,
    event_type text NOT NULL,
    summary text NOT NULL,
    event_json text NOT NULL,
    created_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: chat_threads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_threads (
    thread_id text NOT NULL,
    title text NOT NULL,
    status text NOT NULL,
    channel text NOT NULL,
    owner text NOT NULL,
    primary_agent_id text NOT NULL,
    current_target_agent_id text NOT NULL,
    linked_task_id text NOT NULL,
    linked_deliverable_id text NOT NULL,
    linked_run_id text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_actions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_actions (
    action_id text NOT NULL,
    run_id text NOT NULL,
    step_id text NOT NULL,
    actor_type text NOT NULL,
    action_key text NOT NULL,
    action_type text NOT NULL,
    side_effect_level text NOT NULL,
    idempotency_key text NOT NULL,
    target_json text NOT NULL,
    input_json text NOT NULL,
    result_json text NOT NULL,
    success integer NOT NULL,
    latency_ms integer NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_artifacts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_artifacts (
    artifact_id text NOT NULL,
    run_id text NOT NULL,
    step_id text NOT NULL,
    type text NOT NULL,
    title text NOT NULL,
    path text NOT NULL,
    mime_type text NOT NULL,
    hash text NOT NULL,
    size_bytes integer NOT NULL,
    encrypted integer NOT NULL,
    retention_policy text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_credentials; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_credentials (
    credential_id text NOT NULL,
    tenant_id text NOT NULL,
    name text NOT NULL,
    provider text NOT NULL,
    scope text NOT NULL,
    secret_ref text NOT NULL,
    status text NOT NULL,
    allowed_domains_json text NOT NULL,
    rotation_at text NOT NULL,
    last_rotated_at text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_devices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_devices (
    device_id text NOT NULL,
    tenant_id text NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    os_family text NOT NULL,
    status text NOT NULL,
    enrollment_status text NOT NULL,
    auth_mode text NOT NULL,
    capabilities_json text NOT NULL,
    last_seen_at text NOT NULL,
    last_heartbeat_at text NOT NULL,
    lease_owner text NOT NULL,
    lease_expires_at text NOT NULL,
    policy_id text NOT NULL,
    executor_version text NOT NULL,
    protocol_version text NOT NULL,
    min_control_plane_version text NOT NULL,
    public_key_fingerprint text NOT NULL,
    revoked_at text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_policies (
    policy_id text NOT NULL,
    tenant_id text NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    allowed_domains_json text NOT NULL,
    allowed_apps_json text NOT NULL,
    download_path_rules_json text NOT NULL,
    requires_confirmation_json text NOT NULL,
    profile_strategy text NOT NULL,
    max_runtime_seconds integer NOT NULL,
    max_actions integer NOT NULL,
    artifact_retention_days integer NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_runs (
    run_id text NOT NULL,
    tenant_id text NOT NULL,
    device_id text NOT NULL,
    task_id text NOT NULL,
    thread_id text NOT NULL,
    plan_version text NOT NULL,
    objective text NOT NULL,
    mode text NOT NULL,
    status text NOT NULL,
    risk_level text NOT NULL,
    idempotency_key text NOT NULL,
    current_step_id text NOT NULL,
    needs_help_reason text NOT NULL,
    clarification_reason text NOT NULL,
    lease_epoch integer NOT NULL,
    recovery_state_json text NOT NULL,
    plan_json text NOT NULL,
    summary_json text NOT NULL,
    started_at text NOT NULL,
    finished_at text NOT NULL,
    created_by text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_steps; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_steps (
    step_id text NOT NULL,
    run_id text NOT NULL,
    step_key text NOT NULL,
    intent text NOT NULL,
    status text NOT NULL,
    attempt_count integer NOT NULL,
    budget_ms integer NOT NULL,
    action_count integer NOT NULL,
    error_code text NOT NULL,
    error_message text NOT NULL,
    checkpoint_json text NOT NULL,
    observation_summary_json text NOT NULL,
    verification_summary_json text NOT NULL,
    started_at text NOT NULL,
    finished_at text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: computer_takeovers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.computer_takeovers (
    takeover_id text NOT NULL,
    run_id text NOT NULL,
    started_by text NOT NULL,
    start_reason text NOT NULL,
    transport_mode text NOT NULL,
    controller_session_id text NOT NULL,
    started_at text NOT NULL,
    ended_at text NOT NULL,
    summary text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: customer_access_channels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.customer_access_channels (
    channel_id text NOT NULL,
    name text NOT NULL,
    channel_type text NOT NULL,
    status text NOT NULL,
    target text NOT NULL,
    entry_url text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: management_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.management_runs (
    run_id text NOT NULL,
    title text NOT NULL,
    goal text NOT NULL,
    owner text NOT NULL,
    status text NOT NULL,
    stage_key text NOT NULL,
    linked_task_id text NOT NULL,
    linked_agent_id text NOT NULL,
    linked_session_key text NOT NULL,
    release_channel text NOT NULL,
    risk_level text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    started_at text NOT NULL,
    completed_at text NOT NULL,
    stages_json text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: memory_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.memory_events (
    event_id text NOT NULL,
    scope text NOT NULL,
    owner_id text NOT NULL,
    event_type text NOT NULL,
    summary text NOT NULL,
    related_task_id text NOT NULL,
    related_thread_id text NOT NULL,
    created_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: memory_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.memory_snapshots (
    snapshot_key text NOT NULL,
    scope text NOT NULL,
    owner_id text NOT NULL,
    label text NOT NULL,
    summary text NOT NULL,
    learning_json text NOT NULL,
    notes_json text NOT NULL,
    related_task_id text NOT NULL,
    related_thread_id text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metadata (
    key text NOT NULL,
    value text NOT NULL
);


--
-- Name: model_provider_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_provider_configs (
    openclaw_dir text NOT NULL,
    provider_id text NOT NULL,
    provider_label text NOT NULL,
    key_value text NOT NULL,
    status text NOT NULL,
    env_keys_json text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: notification_channels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.notification_channels (
    channel_id text NOT NULL,
    name text NOT NULL,
    channel_type text NOT NULL,
    status text NOT NULL,
    target text NOT NULL,
    secret text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: notification_deliveries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.notification_deliveries (
    delivery_id text NOT NULL,
    alert_id text NOT NULL,
    channel_id text NOT NULL,
    outcome text NOT NULL,
    detail text NOT NULL,
    delivered_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: orchestration_workflow_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.orchestration_workflow_versions (
    version_id text NOT NULL,
    workflow_id text NOT NULL,
    version_number integer NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    status text NOT NULL,
    lanes_json text NOT NULL,
    nodes_json text NOT NULL,
    created_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: orchestration_workflows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.orchestration_workflows (
    workflow_id text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    status text NOT NULL,
    lanes_json text NOT NULL,
    nodes_json text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: product_installations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.product_installations (
    openclaw_dir text NOT NULL,
    installation_id text NOT NULL,
    label text NOT NULL,
    project_dir text NOT NULL,
    theme text NOT NULL,
    router_agent_id text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL
);


--
-- Name: product_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.product_users (
    username text NOT NULL,
    user_id text NOT NULL,
    display_name text NOT NULL,
    role text NOT NULL,
    password_hash text NOT NULL,
    status text NOT NULL,
    created_at text NOT NULL,
    last_login_at text NOT NULL
);


--
-- Name: routing_decisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.routing_decisions (
    decision_id text NOT NULL,
    task_id text NOT NULL,
    task_title text NOT NULL,
    policy_id text NOT NULL,
    policy_name text NOT NULL,
    workflow_id text NOT NULL,
    workflow_version_id text NOT NULL,
    strategy_type text NOT NULL,
    matched_keyword text NOT NULL,
    queue_name text NOT NULL,
    priority_level text NOT NULL,
    target_agent_id text NOT NULL,
    source_text text NOT NULL,
    decided_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: routing_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.routing_policies (
    policy_id text NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    strategy_type text NOT NULL,
    keyword text NOT NULL,
    target_agent_id text NOT NULL,
    priority_level text NOT NULL,
    queue_name text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: skill_role_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.skill_role_profiles (
    skill_slug text NOT NULL,
    mode text NOT NULL,
    stage text NOT NULL,
    recommended_entry text NOT NULL,
    output_contract_json text NOT NULL,
    requires_runtime_json text NOT NULL,
    handoff_artifacts_json text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: task_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_records (
    task_id text NOT NULL,
    workspace_id text NOT NULL,
    workspace_path text NOT NULL,
    position_index integer NOT NULL,
    title text NOT NULL,
    state text NOT NULL,
    owner text NOT NULL,
    org text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    task_json text NOT NULL
);


--
-- Name: tenant_api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenant_api_keys (
    key_id text NOT NULL,
    tenant_id text NOT NULL,
    name text NOT NULL,
    key_hash text NOT NULL,
    prefix text NOT NULL,
    status text NOT NULL,
    scopes_json text NOT NULL,
    created_at text NOT NULL,
    last_used_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: tenant_installations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenant_installations (
    tenant_id text NOT NULL,
    openclaw_dir text NOT NULL,
    label text NOT NULL,
    role text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: tenants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenants (
    tenant_id text NOT NULL,
    name text NOT NULL,
    slug text NOT NULL,
    status text NOT NULL,
    primary_openclaw_dir text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: workflow_packs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_packs (
    pack_id text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    status text NOT NULL,
    mode text NOT NULL,
    starter integer NOT NULL,
    default_entry text NOT NULL,
    recommended_team_id text NOT NULL,
    stages_json text NOT NULL,
    skills_json text NOT NULL,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    meta_json text NOT NULL
);


--
-- Name: agent_teams agent_teams_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_teams
    ADD CONSTRAINT agent_teams_pkey PRIMARY KEY (team_id);


--
-- Name: audit_events audit_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_pkey PRIMARY KEY (id);


--
-- Name: automation_alerts automation_alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.automation_alerts
    ADD CONSTRAINT automation_alerts_pkey PRIMARY KEY (alert_id);


--
-- Name: automation_rules automation_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.automation_rules
    ADD CONSTRAINT automation_rules_pkey PRIMARY KEY (rule_id);


--
-- Name: chat_messages chat_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_messages
    ADD CONSTRAINT chat_messages_pkey PRIMARY KEY (message_id);


--
-- Name: chat_thread_context_segments chat_thread_context_segments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_context_segments
    ADD CONSTRAINT chat_thread_context_segments_pkey PRIMARY KEY (segment_id);


--
-- Name: chat_thread_context_snapshots chat_thread_context_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_context_snapshots
    ADD CONSTRAINT chat_thread_context_snapshots_pkey PRIMARY KEY (snapshot_key);


--
-- Name: chat_thread_events chat_thread_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_events
    ADD CONSTRAINT chat_thread_events_pkey PRIMARY KEY (event_id);


--
-- Name: chat_threads chat_threads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_threads
    ADD CONSTRAINT chat_threads_pkey PRIMARY KEY (thread_id);


--
-- Name: computer_actions computer_actions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_actions
    ADD CONSTRAINT computer_actions_pkey PRIMARY KEY (action_id);


--
-- Name: computer_artifacts computer_artifacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_artifacts
    ADD CONSTRAINT computer_artifacts_pkey PRIMARY KEY (artifact_id);


--
-- Name: computer_credentials computer_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_credentials
    ADD CONSTRAINT computer_credentials_pkey PRIMARY KEY (credential_id);


--
-- Name: computer_devices computer_devices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_devices
    ADD CONSTRAINT computer_devices_pkey PRIMARY KEY (device_id);


--
-- Name: computer_policies computer_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_policies
    ADD CONSTRAINT computer_policies_pkey PRIMARY KEY (policy_id);


--
-- Name: computer_runs computer_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_runs
    ADD CONSTRAINT computer_runs_pkey PRIMARY KEY (run_id);


--
-- Name: computer_steps computer_steps_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_steps
    ADD CONSTRAINT computer_steps_pkey PRIMARY KEY (step_id);


--
-- Name: computer_takeovers computer_takeovers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.computer_takeovers
    ADD CONSTRAINT computer_takeovers_pkey PRIMARY KEY (takeover_id);


--
-- Name: customer_access_channels customer_access_channels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customer_access_channels
    ADD CONSTRAINT customer_access_channels_pkey PRIMARY KEY (channel_id);


--
-- Name: management_runs management_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.management_runs
    ADD CONSTRAINT management_runs_pkey PRIMARY KEY (run_id);


--
-- Name: memory_events memory_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memory_events
    ADD CONSTRAINT memory_events_pkey PRIMARY KEY (event_id);


--
-- Name: memory_snapshots memory_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.memory_snapshots
    ADD CONSTRAINT memory_snapshots_pkey PRIMARY KEY (snapshot_key);


--
-- Name: metadata metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metadata
    ADD CONSTRAINT metadata_pkey PRIMARY KEY (key);


--
-- Name: model_provider_configs model_provider_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_provider_configs
    ADD CONSTRAINT model_provider_configs_pkey PRIMARY KEY (openclaw_dir, provider_id);


--
-- Name: notification_channels notification_channels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.notification_channels
    ADD CONSTRAINT notification_channels_pkey PRIMARY KEY (channel_id);


--
-- Name: notification_deliveries notification_deliveries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.notification_deliveries
    ADD CONSTRAINT notification_deliveries_pkey PRIMARY KEY (delivery_id);


--
-- Name: orchestration_workflow_versions orchestration_workflow_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.orchestration_workflow_versions
    ADD CONSTRAINT orchestration_workflow_versions_pkey PRIMARY KEY (version_id);


--
-- Name: orchestration_workflows orchestration_workflows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.orchestration_workflows
    ADD CONSTRAINT orchestration_workflows_pkey PRIMARY KEY (workflow_id);


--
-- Name: product_installations product_installations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.product_installations
    ADD CONSTRAINT product_installations_pkey PRIMARY KEY (openclaw_dir);


--
-- Name: product_users product_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.product_users
    ADD CONSTRAINT product_users_pkey PRIMARY KEY (username);


--
-- Name: routing_decisions routing_decisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routing_decisions
    ADD CONSTRAINT routing_decisions_pkey PRIMARY KEY (decision_id);


--
-- Name: routing_policies routing_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routing_policies
    ADD CONSTRAINT routing_policies_pkey PRIMARY KEY (policy_id);


--
-- Name: skill_role_profiles skill_role_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_role_profiles
    ADD CONSTRAINT skill_role_profiles_pkey PRIMARY KEY (skill_slug);


--
-- Name: task_records task_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_records
    ADD CONSTRAINT task_records_pkey PRIMARY KEY (task_id);


--
-- Name: tenant_api_keys tenant_api_keys_key_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_api_keys
    ADD CONSTRAINT tenant_api_keys_key_hash_key UNIQUE (key_hash);


--
-- Name: tenant_api_keys tenant_api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_api_keys
    ADD CONSTRAINT tenant_api_keys_pkey PRIMARY KEY (key_id);


--
-- Name: tenant_installations tenant_installations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_installations
    ADD CONSTRAINT tenant_installations_pkey PRIMARY KEY (tenant_id, openclaw_dir);


--
-- Name: tenants tenants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenants
    ADD CONSTRAINT tenants_pkey PRIMARY KEY (tenant_id);


--
-- Name: tenants tenants_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenants
    ADD CONSTRAINT tenants_slug_key UNIQUE (slug);


--
-- Name: workflow_packs workflow_packs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_packs
    ADD CONSTRAINT workflow_packs_pkey PRIMARY KEY (pack_id);


--
-- Name: idx_agent_teams_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_teams_updated_at ON public.agent_teams USING btree (updated_at DESC, created_at DESC);


--
-- Name: idx_audit_events_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_audit_events_at ON public.audit_events USING btree (at DESC);


--
-- Name: idx_automation_alerts_unique_event; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_automation_alerts_unique_event ON public.automation_alerts USING btree (rule_id, event_key);


--
-- Name: idx_automation_alerts_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_automation_alerts_updated_at ON public.automation_alerts USING btree (updated_at DESC);


--
-- Name: idx_automation_rules_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_automation_rules_updated_at ON public.automation_rules USING btree (updated_at DESC);


--
-- Name: idx_chat_messages_thread; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_messages_thread ON public.chat_messages USING btree (thread_id, created_at);


--
-- Name: idx_chat_thread_context_segments_thread_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_thread_context_segments_thread_kind ON public.chat_thread_context_segments USING btree (thread_id, segment_kind, segment_order);


--
-- Name: idx_chat_thread_context_segments_thread_order; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_thread_context_segments_thread_order ON public.chat_thread_context_segments USING btree (thread_id, segment_order, updated_at DESC);


--
-- Name: idx_chat_thread_context_snapshots_thread_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_chat_thread_context_snapshots_thread_kind ON public.chat_thread_context_snapshots USING btree (thread_id, snapshot_kind);


--
-- Name: idx_chat_thread_context_snapshots_thread_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_thread_context_snapshots_thread_updated_at ON public.chat_thread_context_snapshots USING btree (thread_id, updated_at DESC, created_at DESC);


--
-- Name: idx_chat_thread_events_thread_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_thread_events_thread_created_at ON public.chat_thread_events USING btree (thread_id, created_at DESC, event_id DESC);


--
-- Name: idx_chat_thread_events_thread_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_thread_events_thread_type ON public.chat_thread_events USING btree (thread_id, event_type, created_at DESC);


--
-- Name: idx_chat_threads_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_threads_updated_at ON public.chat_threads USING btree (updated_at DESC, created_at DESC);


--
-- Name: idx_computer_actions_run_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_actions_run_created_at ON public.computer_actions USING btree (run_id, created_at);


--
-- Name: idx_computer_actions_step_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_actions_step_created_at ON public.computer_actions USING btree (step_id, created_at);


--
-- Name: idx_computer_artifacts_run_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_artifacts_run_created_at ON public.computer_artifacts USING btree (run_id, created_at DESC, updated_at DESC);


--
-- Name: idx_computer_artifacts_step_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_artifacts_step_created_at ON public.computer_artifacts USING btree (step_id, created_at DESC);


--
-- Name: idx_computer_credentials_tenant_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_credentials_tenant_updated_at ON public.computer_credentials USING btree (tenant_id, updated_at DESC);


--
-- Name: idx_computer_devices_lease; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_devices_lease ON public.computer_devices USING btree (lease_owner, lease_expires_at DESC);


--
-- Name: idx_computer_devices_tenant_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_devices_tenant_updated_at ON public.computer_devices USING btree (tenant_id, updated_at DESC);


--
-- Name: idx_computer_policies_tenant_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_policies_tenant_updated_at ON public.computer_policies USING btree (tenant_id, updated_at DESC);


--
-- Name: idx_computer_runs_device_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_runs_device_updated_at ON public.computer_runs USING btree (device_id, updated_at DESC);


--
-- Name: idx_computer_runs_idempotency_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_computer_runs_idempotency_key ON public.computer_runs USING btree (idempotency_key);


--
-- Name: idx_computer_runs_tenant_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_runs_tenant_updated_at ON public.computer_runs USING btree (tenant_id, updated_at DESC, created_at DESC);


--
-- Name: idx_computer_runs_thread_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_runs_thread_updated_at ON public.computer_runs USING btree (thread_id, updated_at DESC);


--
-- Name: idx_computer_steps_run_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_steps_run_created_at ON public.computer_steps USING btree (run_id, created_at);


--
-- Name: idx_computer_steps_run_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_steps_run_status ON public.computer_steps USING btree (run_id, status, updated_at DESC);


--
-- Name: idx_computer_takeovers_run_started_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_computer_takeovers_run_started_at ON public.computer_takeovers USING btree (run_id, started_at DESC);


--
-- Name: idx_customer_access_channels_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_customer_access_channels_updated_at ON public.customer_access_channels USING btree (updated_at DESC);


--
-- Name: idx_management_runs_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_management_runs_updated_at ON public.management_runs USING btree (updated_at DESC);


--
-- Name: idx_memory_events_owner_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memory_events_owner_created_at ON public.memory_events USING btree (owner_id, created_at DESC);


--
-- Name: idx_memory_events_related_task_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memory_events_related_task_created_at ON public.memory_events USING btree (related_task_id, created_at DESC);


--
-- Name: idx_memory_events_scope_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memory_events_scope_created_at ON public.memory_events USING btree (scope, created_at DESC);


--
-- Name: idx_memory_snapshots_related_task; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memory_snapshots_related_task ON public.memory_snapshots USING btree (related_task_id, updated_at DESC);


--
-- Name: idx_memory_snapshots_scope_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_memory_snapshots_scope_updated_at ON public.memory_snapshots USING btree (scope, updated_at DESC);


--
-- Name: idx_model_provider_configs_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_model_provider_configs_updated_at ON public.model_provider_configs USING btree (openclaw_dir, updated_at DESC);


--
-- Name: idx_notification_channels_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_notification_channels_updated_at ON public.notification_channels USING btree (updated_at DESC);


--
-- Name: idx_notification_deliveries_alert_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_notification_deliveries_alert_id ON public.notification_deliveries USING btree (alert_id, delivered_at DESC);


--
-- Name: idx_notification_delivery_unique_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_notification_delivery_unique_channel ON public.notification_deliveries USING btree (alert_id, channel_id);


--
-- Name: idx_orchestration_workflow_versions_workflow; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orchestration_workflow_versions_workflow ON public.orchestration_workflow_versions USING btree (workflow_id, version_number DESC);


--
-- Name: idx_orchestration_workflows_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orchestration_workflows_updated_at ON public.orchestration_workflows USING btree (updated_at DESC);


--
-- Name: idx_product_installations_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_product_installations_updated_at ON public.product_installations USING btree (updated_at DESC);


--
-- Name: idx_product_users_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_product_users_created_at ON public.product_users USING btree (created_at);


--
-- Name: idx_routing_decisions_decided_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_routing_decisions_decided_at ON public.routing_decisions USING btree (decided_at DESC);


--
-- Name: idx_routing_decisions_policy_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_routing_decisions_policy_id ON public.routing_decisions USING btree (policy_id, decided_at DESC);


--
-- Name: idx_routing_decisions_task_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_routing_decisions_task_id ON public.routing_decisions USING btree (task_id, decided_at DESC);


--
-- Name: idx_routing_policies_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_routing_policies_updated_at ON public.routing_policies USING btree (updated_at DESC);


--
-- Name: idx_skill_role_profiles_mode_stage; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_skill_role_profiles_mode_stage ON public.skill_role_profiles USING btree (mode, stage, updated_at DESC);


--
-- Name: idx_skill_role_profiles_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_skill_role_profiles_updated_at ON public.skill_role_profiles USING btree (updated_at DESC, created_at DESC);


--
-- Name: idx_task_records_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_records_updated_at ON public.task_records USING btree (updated_at DESC, created_at DESC);


--
-- Name: idx_task_records_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_records_workspace ON public.task_records USING btree (workspace_id, position_index, updated_at DESC);


--
-- Name: idx_tenant_api_keys_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tenant_api_keys_tenant ON public.tenant_api_keys USING btree (tenant_id, created_at DESC);


--
-- Name: idx_tenant_installations_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tenant_installations_tenant ON public.tenant_installations USING btree (tenant_id, updated_at DESC);


--
-- Name: idx_tenants_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tenants_updated_at ON public.tenants USING btree (updated_at DESC);


--
-- Name: idx_workflow_packs_mode; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_packs_mode ON public.workflow_packs USING btree (mode, updated_at DESC);


--
-- Name: idx_workflow_packs_status_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_packs_status_updated_at ON public.workflow_packs USING btree (status, updated_at DESC, created_at DESC);


--
-- Name: chat_thread_context_segments chat_thread_context_segments_thread_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_context_segments
    ADD CONSTRAINT chat_thread_context_segments_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.chat_threads(thread_id) ON DELETE CASCADE;


--
-- Name: chat_thread_context_snapshots chat_thread_context_snapshots_thread_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_context_snapshots
    ADD CONSTRAINT chat_thread_context_snapshots_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.chat_threads(thread_id) ON DELETE CASCADE;


--
-- Name: chat_thread_events chat_thread_events_thread_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_events
    ADD CONSTRAINT chat_thread_events_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.chat_threads(thread_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
