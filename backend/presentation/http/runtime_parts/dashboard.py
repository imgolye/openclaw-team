"""Runtime part: dashboard."""

def dashboard_dirty_marker_path(openclaw_dir):
    return Path(openclaw_dir).expanduser().resolve() / "dashboard" / ".dashboard-dirty.stamp"


def read_dashboard_dirty_marker(openclaw_dir):
    path = dashboard_dirty_marker_path(openclaw_dir)
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return 0


def mark_dashboard_dirty(openclaw_dir):
    path = dashboard_dirty_marker_path(openclaw_dir)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(now_iso(), encoding="utf-8")
    except OSError:
        pass


def dashboard_state_cache_entry(openclaw_dir, data):
    return {
        "ts": time.time(),
        "value": deepcopy(data),
        "marker": read_dashboard_dirty_marker(openclaw_dir),
    }


def _payload_cache_set(cache_key, entry):
    """Write to PAYLOAD_CACHE with size cap; caller must hold BUNDLE_CACHE_LOCK."""
    if cache_key not in PAYLOAD_CACHE and len(PAYLOAD_CACHE) >= PAYLOAD_CACHE_MAX_SIZE:
        oldest_key = min(PAYLOAD_CACHE, key=lambda k: PAYLOAD_CACHE[k]["ts"])
        del PAYLOAD_CACHE[oldest_key]
    PAYLOAD_CACHE[cache_key] = entry


def cached_payload(cache_key, ttl_seconds, builder):
    builder_event = None
    while True:
        now = time.time()
        with BUNDLE_CACHE_LOCK:
            cached = PAYLOAD_CACHE.get(cache_key)
            if cached and now - cached["ts"] < ttl_seconds:
                return deepcopy(cached["value"])
            builder_event = CACHE_BUILD_EVENTS.get(cache_key)
            if builder_event is None:
                builder_event = threading.Event()
                CACHE_BUILD_EVENTS[cache_key] = builder_event
                break
        builder_event.wait(timeout=max(float(ttl_seconds or 0.0), 1.0))
    try:
        value = builder()
        cache_entry = {"ts": time.time(), "value": deepcopy(value)}
        with BUNDLE_CACHE_LOCK:
            cached = PAYLOAD_CACHE.get(cache_key)
            if cached and time.time() - cached["ts"] < ttl_seconds:
                return deepcopy(cached["value"])
            _payload_cache_set(cache_key, cache_entry)
        return value
    finally:
        with BUNDLE_CACHE_LOCK:
            event = CACHE_BUILD_EVENTS.pop(cache_key, None)
            if event is not None:
                event.set()


def clear_cached_payloads():
    global FORCE_SYNC_DASHBOARD_REFRESH
    with BUNDLE_CACHE_LOCK:
        PAYLOAD_CACHE.clear()
        BACKGROUND_CACHE_KEYS.clear()
        FORCE_SYNC_DASHBOARD_REFRESH = True
        for event in CACHE_BUILD_EVENTS.values():
            event.set()
        CACHE_BUILD_EVENTS.clear()


def _refresh_cached_payload_async(cache_key, builder):
    try:
        value = builder()
    except (OSError, ValueError, RuntimeError) as exc:
        logging.warning("background cache refresh failed for %r: %s", cache_key, exc)
        value = None
    cache_entry = {"ts": time.time(), "value": deepcopy(value)} if value is not None else None
    with BUNDLE_CACHE_LOCK:
        if cache_entry is not None:
            _payload_cache_set(cache_key, cache_entry)
        BACKGROUND_CACHE_KEYS.discard(cache_key)


def cached_payload_background(cache_key, ttl_seconds, builder, fallback):
    now = time.time()
    with BUNDLE_CACHE_LOCK:
        cached = PAYLOAD_CACHE.get(cache_key)
        if cached and now - cached["ts"] < ttl_seconds:
            return deepcopy(cached["value"])
    should_refresh = False
    with BUNDLE_CACHE_LOCK:
        if cache_key not in BACKGROUND_CACHE_KEYS:
            BACKGROUND_CACHE_KEYS.add(cache_key)
            should_refresh = True
    if should_refresh:
        worker = threading.Thread(
            target=_refresh_cached_payload_async,
            args=(cache_key, builder),
            daemon=True,
        )
        worker.start()
    if cached:
        return deepcopy(cached["value"])
    value = fallback() if callable(fallback) else fallback
    cache_entry = {"ts": now, "value": deepcopy(value)}
    with BUNDLE_CACHE_LOCK:
        cached = PAYLOAD_CACHE.get(cache_key)
        if cached and time.time() - cached["ts"] < ttl_seconds:
            return deepcopy(cached["value"])
        _payload_cache_set(cache_key, cache_entry)
    return deepcopy(value)


def bundle_cache_key(openclaw_dir, output_dir=None):
    openclaw_path = str(Path(openclaw_dir).expanduser().resolve())
    output_path = str((Path(output_dir).expanduser().resolve() if output_dir else dashboard_dir(openclaw_dir)))
    return f"dashboard_bundle::{openclaw_path}::{output_path}"


def dashboard_state_cache_key(openclaw_dir):
    openclaw_path = str(Path(openclaw_dir).expanduser().resolve())
    return f"dashboard_state::{openclaw_path}"


def planning_bundle_dir(project_dir, bundle_kind, bundle_id):
    plural = "tasks" if bundle_kind == "task" else "runs"
    path = planning_root(project_dir) / plural / str(bundle_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def summarize_planning_bundle(bundle_dir, project_dir=None):
    bundle_dir = Path(bundle_dir)
    task_plan_path = bundle_dir / "task_plan.md"
    findings_path = bundle_dir / "findings.md"
    progress_path = bundle_dir / "progress.md"
    task_plan_text = task_plan_path.read_text(encoding="utf-8") if task_plan_path.exists() else ""
    progress_text = progress_path.read_text(encoding="utf-8") if progress_path.exists() else ""
    counts = planning_phase_counts(task_plan_text)
    touched = [
        path.stat().st_mtime
        for path in (task_plan_path, findings_path, progress_path)
        if path.exists()
    ]
    updated_at = (
        datetime.fromtimestamp(max(touched), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        if touched
        else ""
    )
    progress_percent = int(round((counts["complete"] / counts["total"]) * 100)) if counts["total"] else 0
    summary = {
        "bundleDir": str(bundle_dir),
        "taskPlanPath": str(task_plan_path),
        "findingsPath": str(findings_path),
        "progressPath": str(progress_path),
        "relativeDir": planning_relative_path(project_dir, bundle_dir) if project_dir else str(bundle_dir),
        "currentPhase": extract_current_phase(task_plan_text),
        "progressPercent": progress_percent,
        "latestProgress": extract_latest_progress(progress_text),
        "updatedAt": updated_at,
        **counts,
    }
    return summary


def ensure_planning_bundle(openclaw_dir, project_dir, bundle_kind, bundle_id, title, goal="", meta=None):
    project_dir = Path(project_dir).resolve()
    bundle_dir = planning_bundle_dir(project_dir, bundle_kind, bundle_id)
    replacements = {
        "[Brief Description]": title or bundle_id,
        "[One sentence describing the end state]": goal or (title or bundle_id),
        "[DATE]": datetime.now().astimezone().strftime("%Y-%m-%d"),
        "[timestamp]": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M"),
    }
    templates = {
        "task_plan.md": """# Task Plan: [Brief Description]\n\n## Goal\n[One sentence describing the end state]\n\n## Current Phase\nPhase 1\n\n## Phases\n\n### Phase 1: Requirements & Discovery\n- [ ] Understand intent and constraints\n- [ ] Capture findings in findings.md\n- **Status:** in_progress\n\n### Phase 2: Planning & Structure\n- [ ] Define the approach\n- [ ] Confirm workflow and routing choices\n- **Status:** pending\n\n### Phase 3: Execution\n- [ ] Execute the work\n- [ ] Update progress.md as milestones land\n- **Status:** pending\n\n### Phase 4: Verification\n- [ ] Verify outputs and linked assets\n- [ ] Record checks and failures\n- **Status:** pending\n\n### Phase 5: Delivery\n- [ ] Wrap up deliverables\n- [ ] Capture follow-up actions\n- **Status:** pending\n\n## Decisions Made\n| Decision | Rationale |\n|----------|-----------|\n\n## Errors Encountered\n| Error | Attempt | Resolution |\n|-------|---------|------------|\n""",
        "findings.md": "# Findings & Decisions\n\n## Requirements\n-\n\n## Research Findings\n-\n\n## Technical Decisions\n| Decision | Rationale |\n|----------|-----------|\n\n## Issues Encountered\n| Issue | Resolution |\n|-------|------------|\n\n## Resources\n-\n",
        "progress.md": "# Progress Log\n\n## Session: [DATE]\n\n### Current Status\n- **Phase:** 1 - Requirements & Discovery\n- **Started:** [timestamp]\n\n### Actions Taken\n-\n\n### Test Results\n| Test | Expected | Actual | Status |\n|------|----------|--------|--------|\n\n### Errors\n| Error | Resolution |\n|-------|------------|\n",
    }
    for name, fallback_text in templates.items():
        target = bundle_dir / name
        if target.exists():
            continue
        template_text = load_planning_template(openclaw_dir, project_dir, name, fallback_text)
        target.write_text(render_planning_text(template_text, replacements), encoding="utf-8")
    bundle_meta = {
        "kind": bundle_kind,
        "bundleId": str(bundle_id),
        "title": title,
        "goal": goal,
        "updatedAt": now_iso(),
        "meta": meta or {},
    }
    (bundle_dir / "bundle.json").write_text(json.dumps(bundle_meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary = summarize_planning_bundle(bundle_dir, project_dir=project_dir)
    return {
        "kind": bundle_kind,
        "bundleId": str(bundle_id),
        "title": title,
        "goal": goal,
        **summary,
    }


def dashboard_signature(data):
    raw = json.dumps(normalize_for_signature(data), ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def write_dashboard_files(openclaw_dir, data, output_dir=None):
    openclaw_dir = Path(openclaw_dir)
    output_dir = Path(output_dir) if output_dir else openclaw_dir / "dashboard"
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "collaboration-dashboard.json"
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"json": json_path}


def build_dashboard_bundle(openclaw_dir, output_dir=None, skip_automation_cycle=True):
    ensure_default_install_bootstrap(openclaw_dir)
    data = build_dashboard_state(openclaw_dir, skip_automation_cycle=skip_automation_cycle)
    paths = write_dashboard_files(openclaw_dir, data, output_dir=output_dir)
    return data, paths


def build_dashboard_state(openclaw_dir, skip_automation_cycle=False):
    data = build_dashboard_data(openclaw_dir, skip_automation_cycle=skip_automation_cycle)
    data["signature"] = dashboard_signature(data)
    cache_key = dashboard_state_cache_key(openclaw_dir)
    cache_entry = dashboard_state_cache_entry(openclaw_dir, data)
    global FORCE_SYNC_DASHBOARD_REFRESH
    with BUNDLE_CACHE_LOCK:
        _payload_cache_set(cache_key, cache_entry)
        BACKGROUND_CACHE_KEYS.discard(cache_key)
        FORCE_SYNC_DASHBOARD_REFRESH = False
    return data


def build_dashboard_state_placeholder(openclaw_dir):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    theme_name = metadata.get("theme", DEFAULT_THEME_NAME)
    theme_style = THEME_STYLES.get(theme_name, THEME_STYLES[DEFAULT_THEME_NAME])
    router_agent_id = get_router_agent_id(config)
    theme_catalog = []
    for theme_key, meta in SELECTABLE_THEME_CATALOG.items():
        theme_catalog.append(
            {
                "name": theme_key,
                "displayName": meta["displayName"],
                "language": meta.get("language", "zh-CN"),
                "tagline": meta["tagline"],
                "bestFor": meta["bestFor"],
                "summary": meta["summary"],
                "current": theme_key == theme_name,
            }
        )
    management_mode = management_automation_mode_payload(current_management_automation_mode(metadata))
    placeholder = {
        "generatedAt": now_iso(),
        "generatedAgo": "刚刚",
        "openclawDir": str(openclaw_dir),
        "routerAgentId": router_agent_id,
        "theme": {
            "name": theme_name,
            "displayName": metadata.get("displayName", theme_name),
            "language": THEME_CATALOG.get(theme_name, {}).get("language", THEME_CATALOG[DEFAULT_THEME_NAME].get("language", "zh-CN")),
            "styles": theme_style,
        },
        "themeCatalog": theme_catalog,
        "themeHistory": [],
        "ownerTitle": "用户",
        "agents": [],
        "agentTeams": {"summary": {"total": 0, "active": 0, "memberCount": 0}, "teams": [], "members": [], "roles": [], "relationships": []},
        "taskIndex": [],
        "deliverables": [],
        "events": [],
        "relays": [],
        "commands": [],
        "admin": {"hasUsers": False, "users": [], "roles": [], "auditEvents": [], "summary": {}},
        "management": {
            "summary": {},
            "runs": [],
            "agentHealth": {"summary": {}, "agents": [], "teams": [], "agentsByTeam": []},
            "conversationQuality": {"summary": {}, "agents": [], "sessions": []},
            "reports": {"daily": [], "weekly": {}, "bottlenecks": [], "relayLeaders": []},
            "automation": {
                **build_management_automation_placeholder(openclaw_dir, metadata),
                "mode": management_mode,
            },
            "intelligence": {},
            "decisionQuality": {},
            "decisionSourceReview": {},
            "recommendationReview": {},
        },
        "orchestration": {"summary": {}, "workflow": {}, "lanes": [], "agents": [], "runways": []},
        "communications": {"summary": {}, "channels": [], "failures": [], "audit": []},
        "chat": {"summary": {}, "threads": [], "presence": [], "directMessages": []},
        "conversations": {"summary": {}, "sessions": [], "threads": []},
        "platform": {"apiReference": build_external_api_reference(), "runtimeGovernance": {"summary": {}, "runtimes": [], "packs": [], "browserSessions": []}},
        "skills": {"summary": {}, "skills": [], "packs": [], "commands": [], "supported": True},
        "openclaw": {"summary": {}, "profiles": [], "channels": [], "localBridge": {}, "browserControlTarget": ""},
        "contextHub": {
            "installed": False,
            "version": "",
            "annotations": {"items": [], "total": 0},
            "recommended": [],
            "cache": {"sources": []},
            "config": {},
            "agentMemory": {"summary": {"agentCount": 0, "readyAgentCount": 0, "documentCount": 0}, "agents": []},
            "sharedContext": {"summary": {"documentCount": 0, "knowledgeBaseCount": 0}, "documents": []},
        },
        "metrics": {
            "activeTasks": 0,
            "activeAgents": 0,
            "blockedTasks": 0,
            "completedToday": 0,
            "handoffs24h": 0,
            "signals1h": 0,
            "needsAttentionAgents": 0,
            "stalledAgents": 0,
        },
    }
    placeholder["signature"] = dashboard_signature(placeholder)
    return placeholder


def merge_dashboard_state_fallback(snapshot, placeholder):
    if not isinstance(snapshot, dict):
        return deepcopy(placeholder)
    merged = deepcopy(snapshot)
    for key, placeholder_value in placeholder.items():
        current_value = merged.get(key)
        if isinstance(placeholder_value, dict):
            if not isinstance(current_value, dict):
                merged[key] = deepcopy(placeholder_value)
            else:
                merged[key] = merge_dashboard_state_fallback(current_value, placeholder_value)
        elif isinstance(placeholder_value, list):
            if key not in merged:
                merged[key] = deepcopy(placeholder_value)
        else:
            if current_value in (None, ""):
                merged[key] = placeholder_value
    merged["signature"] = dashboard_signature(merged)
    return merged


def load_dashboard_state_fallback(openclaw_dir):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    placeholder = build_dashboard_state_placeholder(openclaw_dir)
    json_path = openclaw_dir / "dashboard" / "collaboration-dashboard.json"
    if json_path.exists():
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and payload:
                return merge_dashboard_state_fallback(payload, placeholder)
        except Exception:
            pass
    return placeholder


def build_dashboard_state_cached(openclaw_dir, ttl_seconds=5.0):
    global FORCE_SYNC_DASHBOARD_REFRESH
    cache_key = dashboard_state_cache_key(openclaw_dir)
    builder = lambda: build_dashboard_state(openclaw_dir)
    fallback = lambda: load_dashboard_state_fallback(openclaw_dir)
    now = time.time()
    current_marker = read_dashboard_dirty_marker(openclaw_dir)
    fixture_mode = str(read_env_value(openclaw_dir, "MISSION_CONTROL_MODEL_ADAPTER_MODE") or os.environ.get("MISSION_CONTROL_MODEL_ADAPTER_MODE") or "").strip().lower() == "fixture"
    force_sync = FORCE_SYNC_DASHBOARD_REFRESH or fixture_mode
    with BUNDLE_CACHE_LOCK:
        cached = PAYLOAD_CACHE.get(cache_key)
        if cached and now - cached["ts"] < ttl_seconds and cached.get("marker", 0) == current_marker and not force_sync:
            return deepcopy(cached["value"])
        if not force_sync and cache_key not in BACKGROUND_CACHE_KEYS:
            BACKGROUND_CACHE_KEYS.add(cache_key)
            worker = threading.Thread(target=_refresh_dashboard_state_cache_async, args=(cache_key, openclaw_dir), daemon=True)
            worker.start()
    if force_sync:
        try:
            value = builder()
        except (OSError, ValueError, RuntimeError) as exc:
            logging.warning("dashboard cache builder fell back for %r: %s", cache_key, exc)
            value = fallback()
        cache_entry = dashboard_state_cache_entry(openclaw_dir, value)
        with BUNDLE_CACHE_LOCK:
            _payload_cache_set(cache_key, cache_entry)
            FORCE_SYNC_DASHBOARD_REFRESH = False
            BACKGROUND_CACHE_KEYS.discard(cache_key)
        return value
    if cached:
        return deepcopy(cached["value"])
    return fallback()


def _refresh_dashboard_state_cache_async(cache_key, openclaw_dir):
    try:
        value = build_dashboard_state(openclaw_dir, skip_automation_cycle=True)
    except (OSError, ValueError, RuntimeError) as exc:
        logging.warning("background dashboard refresh failed for %r: %s", cache_key, exc)
        value = None
    cache_entry = dashboard_state_cache_entry(openclaw_dir, value) if value is not None else None
    with BUNDLE_CACHE_LOCK:
        if cache_entry is not None:
            _payload_cache_set(cache_key, cache_entry)
        BACKGROUND_CACHE_KEYS.discard(cache_key)


def build_dashboard_bundle_cached(openclaw_dir, output_dir=None, ttl_seconds=5.0):
    cache_key = bundle_cache_key(openclaw_dir, output_dir)
    return cached_payload(
        cache_key,
        ttl_seconds,
        lambda: build_dashboard_bundle(openclaw_dir, output_dir=output_dir),
    )


def _run_dashboard_bundle_warm(cache_key, openclaw_dir, output_dir=None):
    worker = threading.current_thread()
    try:
        build_dashboard_state_cached(openclaw_dir, ttl_seconds=15.0)
    except EVENT_STREAM_STOP_EXCEPTIONS:
        return
    finally:
        with BUNDLE_WARM_WORKERS_LOCK:
            workers = BUNDLE_WARM_WORKERS.get(cache_key)
            if workers is not None:
                workers.discard(worker)
                if not workers:
                    BUNDLE_WARM_WORKERS.pop(cache_key, None)


def warm_dashboard_bundle_async(openclaw_dir, output_dir=None):
    cache_key = bundle_cache_key(openclaw_dir, output_dir)
    worker = threading.Thread(
        target=_run_dashboard_bundle_warm,
        args=(cache_key, openclaw_dir, output_dir),
        daemon=True,
    )
    with BUNDLE_WARM_WORKERS_LOCK:
        BUNDLE_WARM_WORKERS[cache_key].add(worker)
    worker.start()
    return worker


def wait_for_dashboard_bundle_workers(openclaw_dir, output_dir=None, timeout=5.0):
    cache_key = bundle_cache_key(openclaw_dir, output_dir)
    deadline = time.time() + max(float(timeout or 0.0), 0.0)
    while True:
        with BUNDLE_WARM_WORKERS_LOCK:
            workers = [worker for worker in BUNDLE_WARM_WORKERS.get(cache_key, set()) if worker.is_alive()]
            if not workers:
                BUNDLE_WARM_WORKERS.pop(cache_key, None)
                return
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        workers[0].join(timeout=min(remaining, 0.2))


def dashboard_dir(openclaw_dir):
    path = Path(openclaw_dir) / "dashboard"
    path.mkdir(parents=True, exist_ok=True)
    return path


def invalidate_skills_payload_cache(openclaw_dir, config=None):
    project_dir, _cli_path = skills_cli_path(openclaw_dir, config=config)
    openclaw_key = str(Path(openclaw_dir).expanduser().resolve())
    with BUNDLE_CACHE_LOCK:
        PAYLOAD_CACHE.pop(("skills-detail", openclaw_key), None)
        if project_dir:
            PAYLOAD_CACHE.pop(("local-skills", str(project_dir), openclaw_key), None)


def compute_agent_health_data(openclaw_dir, agents, task_index, deliverables, now):
    completed_by_agent = Counter()
    recent_completed_by_agent = Counter()
    for task in task_index:
        if str(task.get("state", "")).lower() not in TERMINAL_STATES:
            continue
        agent_id = task.get("currentAgent") or ""
        if not agent_id:
            continue
        completed_by_agent[agent_id] += 1
        updated_dt = parse_iso(task.get("updatedAt"))
        if updated_dt and updated_dt >= now - timedelta(days=7):
            recent_completed_by_agent[agent_id] += 1

    cards = []
    score_bands = {"excellent": 0, "stable": 0, "watch": 0, "critical": 0}
    for agent in agents:
        latency_samples = agent_response_latency_samples(openclaw_dir, agent.get("id"))
        avg_latency_seconds = round(sum(latency_samples) / len(latency_samples), 1) if latency_samples else 0.0
        active = int(agent.get("activeTasks") or 0)
        blocked = int(agent.get("blockedTasks") or 0)
        completed = recent_completed_by_agent[agent.get("id")] or completed_by_agent[agent.get("id")]
        throughput_base = max(active + blocked + completed, 1)
        completion_rate = round((completed / throughput_base) * 100)
        block_rate = round((blocked / throughput_base) * 100)
        if not latency_samples:
            latency_score = 72
        elif avg_latency_seconds <= 90:
            latency_score = 96
        elif avg_latency_seconds <= 240:
            latency_score = 86
        elif avg_latency_seconds <= 480:
            latency_score = 70
        else:
            latency_score = 52
        completion_score = min(100, 50 + completion_rate)
        block_score = max(24, 100 - block_rate)
        score = round(latency_score * 0.25 + completion_score * 0.45 + block_score * 0.30)
        if score >= 85:
            band = "excellent"
        elif score >= 70:
            band = "stable"
        elif score >= 55:
            band = "watch"
        else:
            band = "critical"
        score_bands[band] += 1
        cards.append(
            {
                "id": agent.get("id"),
                "title": agent.get("title") or agent.get("id"),
                "status": agent.get("status"),
                "score": score,
                "band": band,
                "activeTasks": active,
                "blockedTasks": blocked,
                "completedTasks7d": completed,
                "completionRate": completion_rate,
                "blockRate": block_rate,
                "avgResponseSeconds": avg_latency_seconds,
                "handoffs24h": int(agent.get("handoffs24h") or 0),
                "focus": agent.get("focus", ""),
                "lastSeenAgo": agent.get("lastSeenAgo", ""),
            }
        )
    cards.sort(key=lambda item: (-item["score"], item["blockedTasks"], item["title"]))
    summary = {
        "averageScore": round(sum(item["score"] for item in cards) / len(cards)) if cards else 0,
        "excellent": score_bands["excellent"],
        "stable": score_bands["stable"],
        "watch": score_bands["watch"],
        "critical": score_bands["critical"],
    }
    return {"summary": summary, "agents": cards}


def invalidate_dashboard_bundle_cache(openclaw_dir, output_dir=None, force_sync=True):
    global FORCE_SYNC_DASHBOARD_REFRESH
    bundle_key = bundle_cache_key(openclaw_dir, output_dir)
    state_key = dashboard_state_cache_key(openclaw_dir)
    mark_dashboard_dirty(openclaw_dir)
    with BUNDLE_CACHE_LOCK:
        PAYLOAD_CACHE.pop(bundle_key, None)
        PAYLOAD_CACHE.pop(state_key, None)
        if force_sync:
            FORCE_SYNC_DASHBOARD_REFRESH = True


def invalidate_management_payload_cache(openclaw_dir, include_automation=True):
    raw_openclaw_dir = str(openclaw_dir)
    normalized_openclaw_dir = str(Path(openclaw_dir).expanduser().resolve())
    target_dirs = {raw_openclaw_dir, normalized_openclaw_dir}
    keys = [
        ("management-health", raw_openclaw_dir),
        ("management-health", normalized_openclaw_dir),
        ("management-reports", raw_openclaw_dir),
        ("management-reports", normalized_openclaw_dir),
        admin_data_cache_key(raw_openclaw_dir, include_sensitive=False),
        admin_data_cache_key(raw_openclaw_dir, include_sensitive=True),
        admin_data_cache_key(normalized_openclaw_dir, include_sensitive=False),
        admin_data_cache_key(normalized_openclaw_dir, include_sensitive=True),
    ]
    if include_automation:
        keys.extend(
            [
                ("management-automation", raw_openclaw_dir),
                ("management-automation", normalized_openclaw_dir),
            ]
        )
    with BUNDLE_CACHE_LOCK:
        for key in keys:
            PAYLOAD_CACHE.pop(key, None)
        stale_detail_keys = [
            key
            for key in PAYLOAD_CACHE
            if isinstance(key, tuple)
            and len(key) >= 2
            and key[0] in {"management-detail", "management-run-detail"}
            and str(key[1]) in target_dirs
        ]
        for key in stale_detail_keys:
            PAYLOAD_CACHE.pop(key, None)


def invalidate_computer_use_payload_cache(openclaw_dir):
    raw_openclaw_dir = str(openclaw_dir)
    normalized_openclaw_dir = str(Path(openclaw_dir).expanduser().resolve())
    target_dirs = {raw_openclaw_dir, normalized_openclaw_dir}
    target_prefixes = {
        "computer-use-devices",
        "computer-use-runs",
        "computer-use-run",
        "computer-use-run-preview",
        "computer-use-run-actions",
        "computer-use-run-steps",
        "computer-use-run-artifacts",
    }
    with BUNDLE_CACHE_LOCK:
        stale_keys = [
            key
            for key in PAYLOAD_CACHE
            if isinstance(key, tuple)
            and len(key) >= 2
            and key[0] in target_prefixes
            and str(key[1]) in target_dirs
        ]
        for key in stale_keys:
            PAYLOAD_CACHE.pop(key, None)


def invalidate_openclaw_payload_cache(openclaw_dir):
    raw_openclaw_dir = str(openclaw_dir)
    normalized_openclaw_dir = str(Path(openclaw_dir).expanduser().resolve())
    with BUNDLE_CACHE_LOCK:
        for cache_key in (
            ("openclaw-control", raw_openclaw_dir),
            ("openclaw-control", normalized_openclaw_dir),
            ("openclaw-control-route", raw_openclaw_dir),
            ("openclaw-control-route", normalized_openclaw_dir),
            ("openclaw-summary", raw_openclaw_dir),
            ("openclaw-summary", normalized_openclaw_dir),
            ("openclaw-overview-route", raw_openclaw_dir),
            ("openclaw-overview-route", normalized_openclaw_dir),
            ("openclaw-models-route", raw_openclaw_dir),
            ("openclaw-models-route", normalized_openclaw_dir),
            ("openclaw-runtime-local-runtime-route", raw_openclaw_dir),
            ("openclaw-runtime-local-runtime-route", normalized_openclaw_dir),
            ("openclaw-skills-check-panel", raw_openclaw_dir),
            ("openclaw-skills-check-panel", normalized_openclaw_dir),
            ("openclaw-agent-params-panel", raw_openclaw_dir),
            ("openclaw-agent-params-panel", normalized_openclaw_dir),
            ("openclaw-skills-check-route", raw_openclaw_dir),
            ("openclaw-skills-check-route", normalized_openclaw_dir),
            ("openclaw-skills-agent-params-route", raw_openclaw_dir),
            ("openclaw-skills-agent-params-route", normalized_openclaw_dir),
            ("openclaw-gateway-summary-v2", raw_openclaw_dir),
            ("openclaw-gateway-summary-v2", normalized_openclaw_dir),
            ("openclaw-browser-summary-v2", raw_openclaw_dir),
            ("openclaw-browser-summary-v2", normalized_openclaw_dir),
        ):
            PAYLOAD_CACHE.pop(cache_key, None)


def resolve_frontend_dist(openclaw_dir, config=None, explicit_path=""):
    explicit = str(explicit_path or "").strip()
    if explicit:
        candidate = Path(explicit).expanduser().resolve()
        return candidate if candidate.exists() else None
    project_dir = resolve_project_dir(openclaw_dir, config=config)
    if not project_dir:
        return None
    # Check apps/frontend/dist (development) then apps/frontend (release bundle)
    for subpath in ("apps/frontend/dist", "apps/frontend"):
        candidate = (Path(project_dir) / subpath).resolve()
        if (candidate / "index.html").exists():
            return candidate
    return None
