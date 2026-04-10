"""Runtime part: agent."""


def normalize_agent_voice_reply_voice(value, default="serena"):
    normalized = str(value or "").strip()
    fallback = str(default or "serena").strip() or "serena"
    return normalized or fallback


def normalize_agent_voice_reply_speed(value, default=1.0):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = float(default or 1.0)
    numeric = max(0.8, min(1.2, numeric))
    return round(numeric, 2)

def resolve_skill_instruction_path(project_dir, skill):
    if not project_dir:
        return ""
    skill = skill if isinstance(skill, dict) else {}
    relative_path = str(skill.get("relativePath") or "").strip()
    package = skill.get("package") if isinstance(skill.get("package"), dict) else {}
    package_path = str(package.get("path") or "").strip()
    candidates = []
    if package_path:
        package_candidate = Path(package_path).expanduser()
        if package_candidate.name != "SKILL.md":
            package_candidate = package_candidate / "SKILL.md"
        candidates.append(package_candidate)
    if relative_path:
        candidates.append((Path(project_dir) / relative_path).expanduser())
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            candidate = candidate / "SKILL.md"
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate
        if resolved.exists():
            return str(resolved)
    return ""


def skill_prompt_usage_note(skill):
    slug = str((skill or {}).get("slug") or "").strip().lower()
    if slug == "web-content-fetcher":
        return (
            "Use this first for webpage/article extraction, especially WeChat public account articles. "
            "Do not jump to browser/search fallback unless the fetch comes back blocked, empty, or clearly incomplete."
        )
    if slug == "browse":
        return "Use this when the request is to open a page or inspect a website."
    if slug == "playwright":
        return "Use this when the request needs browser actions, clicks, or page automation."
    if slug == "openai-docs":
        return "Use this when the request is specifically about OpenAI APIs or product docs."
    return ""


def paused_agent_ids(openclaw_dir, config=None):
    metadata = load_project_metadata(openclaw_dir, config=config)
    paused = set()
    for agent_id, override in agent_runtime_overrides(metadata).items():
        if isinstance(override, dict) and override.get("paused"):
            paused.add(str(agent_id))
    return paused


def set_agent_paused(openclaw_dir, agent_id, paused):
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    overrides = dict(agent_runtime_overrides(metadata))
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        raise RuntimeError("agentId 不能为空。")
    entry = overrides.get(normalized_agent_id) if isinstance(overrides.get(normalized_agent_id), dict) else {}
    entry = {
        **entry,
        "paused": bool(paused),
        "updatedAt": now_iso(),
    }
    overrides[normalized_agent_id] = entry
    metadata["agentOverrides"] = overrides
    save_project_metadata(openclaw_dir, metadata)
    return entry


def set_agent_profile(
    openclaw_dir,
    agent_id,
    role="",
    skills=None,
    human_name=None,
    job_title=None,
    working_style=None,
    department=None,
    capability_tags=None,
    notes=None,
    voice_reply_voice=None,
    voice_reply_speed=None,
    voice_reply_instructions=None,
    voice_reply_sample_path=None,
    voice_reply_sample_name=None,
    voice_reply_sample_prompt_text=None,
    clear_voice_reply_sample=False,
):
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    overrides = dict(agent_runtime_overrides(metadata))
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        raise RuntimeError("agentId 不能为空。")
    entry = overrides.get(normalized_agent_id) if isinstance(overrides.get(normalized_agent_id), dict) else {}
    normalized_role = str(role or "").strip()
    normalized_skills = []
    for item in skills or []:
        value = str(item or "").strip()
        if value and value not in normalized_skills:
            normalized_skills.append(value)
    existing_sample_path = str(entry.get("voiceReplySamplePath") or "").strip()
    existing_sample_name = str(entry.get("voiceReplySampleName") or "").strip()
    existing_sample_prompt_text = str(entry.get("voiceReplySamplePromptText") or "").strip()
    entry = {
        **entry,
        "role": normalized_role,
        "skills": normalized_skills,
        "source": "manual",
        "humanName": str(entry.get("humanName") or "").strip() if human_name is None else str(human_name or "").strip(),
        "jobTitle": str(entry.get("jobTitle") or "").strip() if job_title is None else str(job_title or "").strip(),
        "workingStyle": str(entry.get("workingStyle") or "").strip() if working_style is None else str(working_style or "").strip(),
        "department": str(entry.get("department") or "").strip() if department is None else str(department or "").strip(),
        "capabilityTags": clean_unique_strings(entry.get("capabilityTags") or []) if capability_tags is None else clean_unique_strings(capability_tags or []),
        "notes": str(entry.get("notes") or "").strip() if notes is None else str(notes or "").strip(),
        "voiceReplyVoice": normalize_agent_voice_reply_voice(
            entry.get("voiceReplyVoice") if voice_reply_voice is None else voice_reply_voice
        ),
        "voiceReplySpeed": normalize_agent_voice_reply_speed(
            entry.get("voiceReplySpeed") if voice_reply_speed is None else voice_reply_speed,
            default=1.0,
        ),
        "voiceReplyInstructions": str(entry.get("voiceReplyInstructions") or "").strip()
        if voice_reply_instructions is None
        else str(voice_reply_instructions or "").strip(),
        "updatedAt": now_iso(),
    }
    if clear_voice_reply_sample:
        entry.pop("voiceReplySamplePath", None)
        entry.pop("voiceReplySampleName", None)
        entry.pop("voiceReplySamplePromptText", None)
    else:
        next_sample_path = existing_sample_path if voice_reply_sample_path is None else str(voice_reply_sample_path or "").strip()
        next_sample_name = existing_sample_name if voice_reply_sample_name is None else str(voice_reply_sample_name or "").strip()
        next_sample_prompt_text = existing_sample_prompt_text if voice_reply_sample_prompt_text is None else str(voice_reply_sample_prompt_text or "").strip()
        if next_sample_path:
            entry["voiceReplySamplePath"] = next_sample_path
            entry["voiceReplySampleName"] = next_sample_name
            entry["voiceReplySamplePromptText"] = next_sample_prompt_text
        else:
            entry.pop("voiceReplySamplePath", None)
            entry.pop("voiceReplySampleName", None)
            entry.pop("voiceReplySamplePromptText", None)
    overrides[normalized_agent_id] = entry
    metadata["agentOverrides"] = overrides
    save_project_metadata(openclaw_dir, metadata)
    store_save_theme_workforce_profile(
        openclaw_dir,
        {
            "agentId": normalized_agent_id,
            "humanName": entry.get("humanName", ""),
            "role": entry.get("role", ""),
            "jobTitle": entry.get("jobTitle", ""),
            "workingStyle": entry.get("workingStyle", ""),
            "department": entry.get("department", ""),
            "skills": entry.get("skills", []),
            "capabilityTags": entry.get("capabilityTags", []),
            "notes": entry.get("notes", ""),
            "voiceReplyVoice": entry.get("voiceReplyVoice", ""),
            "voiceReplySpeed": entry.get("voiceReplySpeed", 1.0),
            "voiceReplyInstructions": entry.get("voiceReplyInstructions", ""),
            "meta": {
                "source": "agent-profile",
                "voiceReplySamplePath": str(entry.get("voiceReplySamplePath") or "").strip(),
                "voiceReplySampleName": str(entry.get("voiceReplySampleName") or "").strip(),
                "voiceReplySamplePromptText": str(entry.get("voiceReplySamplePromptText") or "").strip(),
            },
        },
    )
    return entry


def ensure_default_agent_profile_bootstrap(openclaw_dir, metadata, agents):
    current_metadata = deepcopy(metadata if isinstance(metadata, dict) else {})
    overrides = dict(agent_runtime_overrides(current_metadata))
    try:
        bootstrap_version = int(current_metadata.get("agentProfileBootstrapVersion") or 0)
    except (TypeError, ValueError):
        bootstrap_version = 0
    needs_self_heal = bootstrap_version < AGENT_PROFILE_BOOTSTRAP_VERSION
    if not needs_self_heal:
        for agent in safe_list(agents):
            agent_id = str((agent or {}).get("id") or "").strip()
            if not agent_id:
                continue
            default_profile = merged_agent_runtime_profile(agent_id)
            if not default_profile:
                continue
            existing = overrides.get(agent_id) if isinstance(overrides.get(agent_id), dict) else {}
            source = str(existing.get("source") or "").strip().lower()
            has_existing_profile = bool(str(existing.get("role") or "").strip() or safe_list(existing.get("skills")))
            if has_existing_profile and source not in {"", "product-default"}:
                continue
            try:
                existing_bootstrap_version = int(existing.get("profileBootstrapVersion") or 0)
            except (TypeError, ValueError):
                existing_bootstrap_version = 0
            if (
                str(existing.get("role") or "").strip() != str(default_profile.get("role") or "").strip()
                or clean_unique_strings(existing.get("skills") or []) != clean_unique_strings(default_profile.get("skills") or [])
                or str(existing.get("humanName") or "").strip() != str(default_profile.get("humanName") or "").strip()
                or str(existing.get("jobTitle") or "").strip() != str(default_profile.get("jobTitle") or "").strip()
                or str(existing.get("workingStyle") or "").strip() != str(default_profile.get("workingStyle") or "").strip()
                or str(existing.get("department") or "").strip() != str(default_profile.get("department") or "").strip()
                or clean_unique_strings(existing.get("capabilityTags") or []) != clean_unique_strings(default_profile.get("capabilityTags") or [])
                or str(existing.get("notes") or "").strip() != str(default_profile.get("notes") or "").strip()
                or normalize_agent_voice_reply_voice(existing.get("voiceReplyVoice")) != normalize_agent_voice_reply_voice(default_profile.get("voiceReplyVoice"))
                or normalize_agent_voice_reply_speed(existing.get("voiceReplySpeed"), default=1.0) != normalize_agent_voice_reply_speed(default_profile.get("voiceReplySpeed"), default=1.0)
                or source != "product-default"
                or existing_bootstrap_version != AGENT_PROFILE_BOOTSTRAP_VERSION
            ):
                needs_self_heal = True
                break
    if not needs_self_heal:
        return current_metadata

    changed = False
    for agent in safe_list(agents):
        agent_id = str((agent or {}).get("id") or "").strip()
        if not agent_id:
            continue
        existing = overrides.get(agent_id) if isinstance(overrides.get(agent_id), dict) else {}
        source = str(existing.get("source") or "").strip().lower()
        has_existing_profile = bool(str(existing.get("role") or "").strip() or safe_list(existing.get("skills")))
        if has_existing_profile and source not in {"", "product-default"}:
            continue
        default_profile = merged_agent_runtime_profile(agent_id)
        if not default_profile:
            continue
        next_entry = {
            **existing,
            "role": str(default_profile.get("role") or "").strip(),
            "skills": clean_unique_strings(default_profile.get("skills") or []),
            "humanName": str(default_profile.get("humanName") or "").strip(),
            "jobTitle": str(default_profile.get("jobTitle") or "").strip(),
            "workingStyle": str(default_profile.get("workingStyle") or "").strip(),
            "department": str(default_profile.get("department") or "").strip(),
            "capabilityTags": clean_unique_strings(default_profile.get("capabilityTags") or []),
            "notes": str(default_profile.get("notes") or "").strip(),
            "voiceReplyVoice": normalize_agent_voice_reply_voice(default_profile.get("voiceReplyVoice")),
            "voiceReplySpeed": normalize_agent_voice_reply_speed(default_profile.get("voiceReplySpeed"), default=1.0),
            "source": "product-default",
            "profileBootstrapVersion": AGENT_PROFILE_BOOTSTRAP_VERSION,
            "updatedAt": now_iso(),
        }
        try:
            existing_bootstrap_version = int(existing.get("profileBootstrapVersion") or 0)
        except (TypeError, ValueError):
            existing_bootstrap_version = 0
        if (
            str(existing.get("role") or "").strip() == next_entry["role"]
            and clean_unique_strings(existing.get("skills") or []) == next_entry["skills"]
            and str(existing.get("humanName") or "").strip() == next_entry["humanName"]
            and str(existing.get("jobTitle") or "").strip() == next_entry["jobTitle"]
            and str(existing.get("workingStyle") or "").strip() == next_entry["workingStyle"]
            and str(existing.get("department") or "").strip() == next_entry["department"]
            and clean_unique_strings(existing.get("capabilityTags") or []) == next_entry["capabilityTags"]
            and str(existing.get("notes") or "").strip() == next_entry["notes"]
            and normalize_agent_voice_reply_voice(existing.get("voiceReplyVoice")) == next_entry["voiceReplyVoice"]
            and normalize_agent_voice_reply_speed(existing.get("voiceReplySpeed"), default=1.0) == next_entry["voiceReplySpeed"]
            and str(existing.get("source") or "").strip().lower() == "product-default"
            and existing_bootstrap_version == AGENT_PROFILE_BOOTSTRAP_VERSION
        ):
            continue
        overrides[agent_id] = next_entry
        store_save_theme_workforce_profile(
            openclaw_dir,
            {
                "agentId": agent_id,
                "humanName": next_entry.get("humanName", ""),
                "role": next_entry.get("role", ""),
                "jobTitle": next_entry.get("jobTitle", ""),
                "workingStyle": next_entry.get("workingStyle", ""),
                "department": next_entry.get("department", ""),
                "skills": next_entry.get("skills", []),
                "capabilityTags": next_entry.get("capabilityTags", []),
                "notes": next_entry.get("notes", ""),
                "voiceReplyVoice": next_entry.get("voiceReplyVoice", ""),
                "voiceReplySpeed": next_entry.get("voiceReplySpeed", 1.0),
                "meta": {
                    "source": "agent-profile-bootstrap",
                    "bootstrapVersion": AGENT_PROFILE_BOOTSTRAP_VERSION,
                },
            },
        )
        changed = True

    current_metadata["agentProfileBootstrapVersion"] = AGENT_PROFILE_BOOTSTRAP_VERSION
    current_metadata["agentProfileBootstrapSource"] = (
        "product-default-self-heal"
        if bootstrap_version >= AGENT_PROFILE_BOOTSTRAP_VERSION
        else "product-default"
    )
    if changed or needs_self_heal:
        current_metadata["agentOverrides"] = overrides
        current_metadata["agentProfileBootstrapAt"] = now_iso()
        save_project_metadata(openclaw_dir, current_metadata)
    return current_metadata


def resolve_planning_skill_dir(openclaw_dir, project_dir=None):
    candidates = []
    if project_dir:
        candidates.append(Path(project_dir) / "platform" / "vendor" / "openclaw-skills" / "planning-with-files")
    candidates.append(Path(openclaw_dir) / "skills" / "planning-with-files")
    for parent in Path(__file__).resolve().parents:
        candidates.append(parent / "platform" / "vendor" / "openclaw-skills" / "planning-with-files")
    for candidate in candidates:
        if candidate.exists() and (candidate / "SKILL.md").exists():
            return candidate
    return None


def planning_root(project_dir):
    path = Path(project_dir) / ".planning"
    path.mkdir(parents=True, exist_ok=True)
    return path


def planning_relative_path(project_dir, path):
    try:
        return str(Path(path).resolve().relative_to(Path(project_dir).resolve()))
    except ValueError:
        return str(Path(path).resolve())


def load_planning_template(openclaw_dir, project_dir, template_name, fallback_text):
    skill_dir = resolve_planning_skill_dir(openclaw_dir, project_dir=project_dir)
    if skill_dir:
        candidate = skill_dir / "templates" / template_name
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")
    return fallback_text


def render_planning_text(template_text, replacements):
    text = str(template_text or "")
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def planning_phase_counts(task_plan_text):
    text = str(task_plan_text or "")
    total = len(re.findall(r"^### Phase", text, flags=re.MULTILINE))
    complete = len(re.findall(r"\*\*Status:\*\*\s*complete", text, flags=re.IGNORECASE))
    in_progress = len(re.findall(r"\*\*Status:\*\*\s*in_progress", text, flags=re.IGNORECASE))
    pending = len(re.findall(r"\*\*Status:\*\*\s*pending", text, flags=re.IGNORECASE))
    return {
        "total": total,
        "complete": complete,
        "inProgress": in_progress,
        "pending": pending,
    }


def load_agents(config):
    config = config or {}
    return config.get("agents", {}).get("list", [])


def get_router_agent_id(config):
    for agent in load_agents(config):
        if agent.get("default"):
            return agent["id"]
    agents = load_agents(config)
    return agents[0]["id"] if agents else "router"


def infer_model_provider(model_name):
    normalized = str(model_name or "").strip().lower()
    if not normalized:
        return {"id": "unassigned", "label": "未设置", "env": ()}
    candidates = [normalized]
    if "/" in normalized:
        candidates.append(normalized.split("/", 1)[1])
    for provider in MODEL_PROVIDER_CATALOG:
        prefixes = provider.get("prefixes", ())
        if any(any(candidate.startswith(prefix) for prefix in prefixes) for candidate in candidates):
            return provider
    return {"id": "custom", "label": "兼容 / 自定义", "env": ()}


def provider_key_status(openclaw_dir, provider):
    configured_keys = []
    stored_env_keys = set()
    try:
        for config in store_list_model_provider_configs(openclaw_dir):
            if not isinstance(config, dict):
                continue
            if str(config.get("status") or "active").strip().lower() == "disabled":
                continue
            if not str(config.get("keyValue") or "").strip():
                continue
            env_keys = config.get("envKeys") if isinstance(config.get("envKeys"), list) else []
            for key in env_keys:
                normalized_key = str(key or "").strip()
                if normalized_key:
                    stored_env_keys.add(normalized_key)
    except Exception:
        stored_env_keys = set()
    for key in provider.get("env", ()):
        if os.environ.get(key) or key in stored_env_keys or read_env_value(openclaw_dir, key):
            configured_keys.append(key)
    return {
        "configured": bool(configured_keys),
        "configuredKeys": configured_keys,
    }


def choose_load_balanced_agent(agents, task_index, router_agent_id, excluded_agent_ids=None):
    excluded = set(str(item) for item in (excluded_agent_ids or set()) if str(item).strip())
    eligible = [agent for agent in agents if agent.get("id") not in {router_agent_id, "briefing", *excluded}]
    if not eligible:
        eligible = [agent for agent in agents if agent.get("id") not in {"briefing", *excluded}]
    if not eligible:
        return None
    load_map = Counter()
    for task in task_index:
        if task.get("active") and task.get("currentAgent"):
            load_map[task["currentAgent"]] += 1
    return sorted(eligible, key=lambda item: (load_map[item.get("id", "")], item.get("id", "")))[0]


def planning_binding_from_payload(item):
    if not isinstance(item, dict):
        return {}
    direct = item.get("planningBundle") or item.get("planningBinding")
    if isinstance(direct, dict) and direct:
        return direct
    meta = item.get("meta")
    if isinstance(meta, dict):
        nested = meta.get("planningBundle") or meta.get("planningBinding")
        if isinstance(nested, dict) and nested:
            return nested
    return {}


def status_for_agent(active_count, blocked_count, signal_dt, last_seen, now):
    if blocked_count:
        return "blocked"
    if active_count and signal_dt and now - signal_dt <= AGENT_PROGRESS_FRESHNESS_WINDOW:
        return "active"
    if active_count:
        return "waiting"
    if last_seen and now - last_seen <= AGENT_PROGRESS_FRESHNESS_WINDOW:
        return "standby"
    return "idle"


def work_guard_for_agent(active_count, blocked_count, signal_dt, last_seen, workspace_dt, session_dt, now, paused=False):
    fresh_progress = bool(signal_dt and now - signal_dt <= AGENT_PROGRESS_FRESHNESS_WINDOW)
    reference_dt = signal_dt or last_seen
    stale_minutes = elapsed_minutes(reference_dt, now) if (active_count or blocked_count) else None

    if signal_dt:
        evidence_source = "progress"
    elif session_dt and (not workspace_dt or session_dt >= workspace_dt):
        evidence_source = "session"
    elif workspace_dt:
        evidence_source = "workspace"
    else:
        evidence_source = "none"

    if paused:
        state = "paused"
        reason = "paused"
        needs_attention = False
        attention_level = "none"
    elif blocked_count:
        state = "blocked"
        reason = "blocked"
        needs_attention = True
        attention_level = "high"
    elif active_count and fresh_progress:
        state = "working"
        reason = "fresh_progress"
        needs_attention = False
        attention_level = "none"
    elif active_count:
        state = "stalled"
        reason = "stale_progress"
        needs_attention = True
        attention_level = (
            "high"
            if stale_minutes is not None and stale_minutes >= int(AGENT_STALE_PROGRESS_ESCALATION_WINDOW.total_seconds() // 60)
            else "medium"
        )
    elif last_seen:
        state = "ready"
        reason = "ready"
        needs_attention = False
        attention_level = "none"
    else:
        state = "offline"
        reason = "offline"
        needs_attention = False
        attention_level = "none"

    return {
        "state": state,
        "reason": reason,
        "needsAttention": needs_attention,
        "attentionLevel": attention_level,
        "hasActiveTask": bool(active_count),
        "hasFreshProgress": fresh_progress,
        "canTakeNewTask": state == "ready",
        "staleMinutes": stale_minutes,
        "evidenceSource": evidence_source,
        "lastProgressAt": signal_dt.isoformat().replace("+00:00", "Z") if signal_dt else "",
        "lastProgressAgo": format_age(signal_dt, now),
        "lastPresenceAt": last_seen.isoformat().replace("+00:00", "Z") if last_seen else "",
        "lastPresenceAgo": format_age(last_seen, now),
        "workspaceLastSeenAt": workspace_dt.isoformat().replace("+00:00", "Z") if workspace_dt else "",
        "workspaceLastSeenAgo": format_age(workspace_dt, now),
        "sessionLastSeenAt": session_dt.isoformat().replace("+00:00", "Z") if session_dt else "",
        "sessionLastSeenAgo": format_age(session_dt, now),
    }


def resolve_agent_workspace(openclaw_dir, config, agent_id):
    normalized_id = str(agent_id or "").strip()
    if not normalized_id:
        raise RuntimeError("Agent ID 不能为空。")
    agent = next((item for item in load_agents(config) if item.get("id") == normalized_id), None)
    if not agent:
        raise RuntimeError(f"未知 Agent：{normalized_id}")
    return Path(agent.get("workspace") or (Path(openclaw_dir) / f"workspace-{normalized_id}")).expanduser().resolve(), agent


def run_streaming_agent_command(
    args,
    *,
    openclaw_dir,
    agent_id,
    session_id="",
    env=None,
    timeout=None,
    stream_observer=None,
):
    expected_session_id = str(session_id or "").strip()
    baseline_text = ""
    if expected_session_id:
        baseline_text = latest_assistant_transcript_text(
            parse_transcript_items(session_transcript_path(openclaw_dir, agent_id, expected_session_id), limit=220)
        )
    last_stream_text = baseline_text
    if callable(stream_observer):
        stream_observer(
            {
                "stage": "started",
                "agentId": str(agent_id or "").strip(),
                "sessionId": expected_session_id,
                "content": "",
                "delta": "",
            }
        )

    stdout_chunks = []
    stderr_chunks = []

    def drain_stream(stream, sink):
        if stream is None:
            return
        while True:
            chunk = stream.read(4096)
            if not chunk:
                break
            sink.append(chunk)

    process = subprocess.Popen(
        [str(arg) for arg in args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=None,
        env=env,
    )
    stdout_thread = threading.Thread(
        target=drain_stream,
        args=(process.stdout, stdout_chunks),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=drain_stream,
        args=(process.stderr, stderr_chunks),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    deadline = time.monotonic() + float(timeout) if timeout else None
    while True:
        if expected_session_id and callable(stream_observer):
            latest_text = latest_assistant_transcript_text(
                parse_transcript_items(session_transcript_path(openclaw_dir, agent_id, expected_session_id), limit=220)
            )
            if latest_text and latest_text != baseline_text and latest_text != last_stream_text:
                delta = stream_text_delta(last_stream_text, latest_text)
                last_stream_text = latest_text
                stream_observer(
                    {
                        "stage": "delta",
                        "agentId": str(agent_id or "").strip(),
                        "sessionId": expected_session_id,
                        "content": latest_text,
                        "delta": delta,
                    }
                )
        if process.poll() is not None:
            break
        if deadline and time.monotonic() >= deadline:
            process.kill()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            stdout_thread.join(timeout=2)
            stderr_thread.join(timeout=2)
            stdout = "".join(stdout_chunks)
            stderr = "".join(stderr_chunks)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
            raise subprocess.TimeoutExpired(args, timeout, output=stdout, stderr=stderr)
        time.sleep(0.15)
    stdout_thread.join(timeout=2)
    stderr_thread.join(timeout=2)
    stdout = "".join(stdout_chunks)
    stderr = "".join(stderr_chunks)
    if process.stdout is not None:
        process.stdout.close()
    if process.stderr is not None:
        process.stderr.close()
    return subprocess.CompletedProcess(args, process.returncode, stdout, stderr), last_stream_text


def skills_cli_path(openclaw_dir, config=None):
    project_dir = resolve_project_dir(openclaw_dir, config=config)
    if not project_dir:
        return None, None
    cli_path = project_dir / "platform" / "bin" / "install" / "skill_utils.py"
    if not cli_path.exists():
        return project_dir, None
    return project_dir, cli_path


def empty_skills_catalog_payload(error="", supported=False):
    return {
        "supported": supported,
        "error": error,
        "summary": {"total": 0, "ready": 0, "warning": 0, "error": 0, "packaged": 0, "categories": {}},
        "skills": [],
        "guidance": [],
        "commands": [],
        "packs": [],
        "roleSummary": {},
        "stageSummary": {},
        "runtimeSummary": {},
        "packSummary": {"total": 0, "starter": 0, "active": 0, "incomplete": 0},
    }


def sync_gstack_skill_library(openclaw_dir):
    project_dir = resolve_project_dir(openclaw_dir)
    if not project_dir:
        raise RuntimeError("当前安装没有关联可写入的项目目录，无法同步 gstack 技能库。")
    created_skills = []
    existing_skills = []
    for spec in GSTACK_SYNC_SKILLS:
        result = ensure_gstack_skill_scaffold(project_dir, spec)
        if result.get("created"):
            created_skills.append(result)
        else:
            existing_skills.append(result)
        store_save_skill_role_profile(
            openclaw_dir,
            {
                "skillSlug": spec["slug"],
                "mode": spec["mode"],
                "stage": spec["stage"],
                "recommendedEntry": spec["recommendedEntry"],
                "outputContract": spec["outputContract"],
                "requiresRuntime": spec["requiresRuntime"],
                "handoffArtifacts": spec["handoffArtifacts"],
                "meta": {"starter": True, "source": "gstack", "label": spec["title"]},
            },
        )
    saved_packs = []
    for pack in STARTER_WORKFLOW_PACKS:
        saved_packs.append(
            store_save_workflow_pack(
                openclaw_dir,
                {
                    **pack,
                    "meta": {**(pack.get("meta") or {}), "source": "gstack"},
                },
            )
        )
    invalidate_skills_payload_cache(openclaw_dir)
    return {
        "createdSkills": created_skills,
        "existingSkills": existing_skills,
        "createdSkillCount": len(created_skills),
        "existingSkillCount": len(existing_skills),
        "savedRoleProfileCount": len(GSTACK_SYNC_SKILLS),
        "savedPackCount": len(saved_packs),
        "projectDir": str(project_dir),
    }


def discover_installable_skill_slugs(project_dir):
    root = Path(project_dir).expanduser().resolve()
    installable = set()
    for candidate_root in (root / "platform" / "skills", root / "platform" / "vendor" / "openclaw-skills"):
        if not candidate_root.exists() or not candidate_root.is_dir():
            continue
        for skill_dir in candidate_root.iterdir():
            if skill_dir.is_dir() and (skill_dir / "SKILL.md").exists():
                installable.add(skill_dir.name)
    return installable


def ensure_default_skill_library_bootstrap(openclaw_dir, config=None, metadata=None):
    local_config = config or load_config(openclaw_dir)
    current_metadata = deepcopy(metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config))
    try:
        bootstrap_version = int(current_metadata.get("skillBootstrapVersion") or 0)
    except (TypeError, ValueError):
        bootstrap_version = 0
    saved_profiles = {
        str(item.get("skillSlug") or "").strip()
        for item in store_list_skill_role_profiles(openclaw_dir)
        if str(item.get("skillSlug") or "").strip()
    }
    missing_profile_slugs = [
        str(profile.get("skillSlug") or "").strip()
        for profile in PRODUCT_STARTER_SKILL_ROLE_PROFILES
        if str(profile.get("skillSlug") or "").strip() and str(profile.get("skillSlug") or "").strip() not in saved_profiles
    ]
    tracked_default_skills = clean_unique_strings(current_metadata.get("defaultInstalledSkills") or [])
    project_dir, cli_path = skills_cli_path(openclaw_dir, config=local_config)
    if not project_dir or not cli_path:
        return current_metadata
    installable_skill_slugs = discover_installable_skill_slugs(project_dir)
    desired_default_skills = clean_unique_strings(DEFAULT_MANAGED_SKILL_SLUGS)
    install_targets = [
        slug for slug in desired_default_skills if slug not in tracked_default_skills and slug in installable_skill_slugs
    ]
    skipped_targets = [
        slug for slug in desired_default_skills if slug not in tracked_default_skills and slug not in installable_skill_slugs
    ]
    tracked_skipped_skills = clean_unique_strings(current_metadata.get("defaultSkippedSkills") or [])
    needs_self_heal = (
        bootstrap_version < SKILL_BOOTSTRAP_VERSION
        or bool(missing_profile_slugs)
        or bool(install_targets)
        or tracked_skipped_skills != skipped_targets
    )
    if not needs_self_heal:
        return current_metadata

    sync_result = sync_gstack_skill_library(openclaw_dir)
    for profile in PRODUCT_STARTER_SKILL_ROLE_PROFILES:
        store_save_skill_role_profile(openclaw_dir, profile)

    installed_skills = []
    install_errors = []
    for slug in install_targets:
        try:
            perform_skill_install(openclaw_dir, slug)
            installed_skills.append(slug)
        except RuntimeError as error:
            install_errors.append({"skill": slug, "error": str(error)})

    current_metadata["skillBootstrapVersion"] = SKILL_BOOTSTRAP_VERSION
    current_metadata["skillBootstrapSource"] = (
        "product-default-self-heal"
        if bootstrap_version >= SKILL_BOOTSTRAP_VERSION
        else "product-default"
    )
    current_metadata["skillBootstrapAt"] = now_iso()
    current_metadata["defaultInstalledSkills"] = clean_unique_strings(tracked_default_skills + installed_skills)
    current_metadata["defaultSkippedSkills"] = [
        slug for slug in skipped_targets if slug not in set(installed_skills)
    ]
    if install_errors:
        current_metadata["skillBootstrapErrors"] = install_errors
    else:
        current_metadata.pop("skillBootstrapErrors", None)
    if current_metadata["defaultSkippedSkills"]:
        current_metadata["skillBootstrapSkipped"] = [
            {
                "skill": slug,
                "reason": "starter_not_packaged",
                "detail": "This optional default skill is not packaged in the current project or vendored starter library.",
            }
            for slug in current_metadata["defaultSkippedSkills"]
        ]
    else:
        current_metadata.pop("skillBootstrapSkipped", None)
    current_metadata["gstackBootstrap"] = {
        "createdSkillCount": sync_result.get("createdSkillCount", 0),
        "savedPackCount": sync_result.get("savedPackCount", 0),
    }
    save_project_metadata(openclaw_dir, current_metadata)
    invalidate_skills_payload_cache(openclaw_dir, config=local_config)
    return current_metadata


def skill_role_profile_from_catalog(skill):
    if not isinstance(skill, dict):
        return None
    mode = str(skill.get("mode") or "").strip().lower()
    stage = str(skill.get("stage") or "").strip().lower()
    if not mode or not stage:
        return None
    return {
        "skillSlug": str(skill.get("slug") or "").strip(),
        "mode": mode,
        "stage": stage,
        "recommendedEntry": str(skill.get("recommendedEntry") or "skills").strip().lower() or "skills",
        "outputContract": clean_unique_strings(skill.get("outputContract") or []),
        "requiresRuntime": clean_unique_strings(skill.get("requiresRuntime") or []),
        "handoffArtifacts": clean_unique_strings(skill.get("handoffArtifacts") or []),
        "meta": {"source": "catalog"},
    }


def infer_skill_role_profile(skill):
    if not isinstance(skill, dict):
        return None
    slug = str(skill.get("slug") or "").strip().lower()
    if not slug:
        return None
    starter = next((item for item in STARTER_SKILL_ROLE_PROFILES if item.get("skillSlug") == slug), None)
    if starter:
        profile = deepcopy(starter)
        profile["meta"] = {**(profile.get("meta") or {}), "source": "starter"}
        return profile
    if "qa" in slug:
        return {
            "skillSlug": slug,
            "mode": "qa",
            "stage": "verify",
            "recommendedEntry": "run",
            "outputContract": ["report"],
            "requiresRuntime": [],
            "handoffArtifacts": ["qa-report"],
            "meta": {"source": "inferred"},
        }
    if "review" in slug:
        return {
            "skillSlug": slug,
            "mode": "eng-review",
            "stage": "review",
            "recommendedEntry": "run",
            "outputContract": ["report"],
            "requiresRuntime": [],
            "handoffArtifacts": ["findings"],
            "meta": {"source": "inferred"},
        }
    if "retro" in slug:
        return {
            "skillSlug": slug,
            "mode": "retro",
            "stage": "reflect",
            "recommendedEntry": "run",
            "outputContract": ["report"],
            "requiresRuntime": [],
            "handoffArtifacts": ["findings"],
            "meta": {"source": "inferred"},
        }
    return None


def fallback_skill_record_for_slug(slug):
    normalized_slug = str(slug or "").strip().lower()
    if not normalized_slug:
        return {}
    spec = gstack_skill_specs_by_slug().get(normalized_slug)
    if spec:
        return {
            "slug": normalized_slug,
            "name": normalized_slug,
            "displayName": str(spec.get("title") or normalized_slug).strip() or normalized_slug,
            "mode": str(spec.get("mode") or "").strip(),
            "stage": str(spec.get("stage") or "").strip(),
            "recommendedEntry": str(spec.get("recommendedEntry") or "").strip(),
            "requiresRuntime": clean_unique_strings(spec.get("requiresRuntime") or []),
            "handoffArtifacts": clean_unique_strings(spec.get("handoffArtifacts") or []),
            "outputContract": clean_unique_strings(spec.get("outputContract") or []),
            "managedSource": "gstack-starter",
        }
    inferred = infer_skill_role_profile({"slug": normalized_slug}) or {}
    if not inferred:
        return {}
    return {
        "slug": normalized_slug,
        "name": normalized_slug,
        "displayName": normalized_slug,
        "mode": str(inferred.get("mode") or "").strip(),
        "stage": str(inferred.get("stage") or "").strip(),
        "recommendedEntry": str(inferred.get("recommendedEntry") or "").strip(),
        "requiresRuntime": clean_unique_strings(inferred.get("requiresRuntime") or []),
        "handoffArtifacts": clean_unique_strings(inferred.get("handoffArtifacts") or []),
        "outputContract": clean_unique_strings(inferred.get("outputContract") or []),
        "managedSource": "inferred",
    }


def load_skills_catalog(openclaw_dir, config=None):
    config = config or load_config(openclaw_dir)
    ensure_default_skill_library_bootstrap(openclaw_dir, config=config)
    project_dir, cli_path = skills_cli_path(openclaw_dir, config=config)
    if not project_dir or not cli_path:
        return empty_skills_catalog_payload("当前安装没有关联可用的 skill 工具脚本。", supported=False)

    def build():
        result, output = run_python_script(
            cli_path,
            ["list", "--project-dir", str(project_dir), "--openclaw-dir", str(Path(openclaw_dir).expanduser().resolve())],
            cwd=project_dir,
        )
        if result.returncode != 0:
            return empty_skills_catalog_payload(output or "读取技能目录失败。", supported=False)

        payload = parse_json_payload(result.stdout, output, default=None)
        if payload is None:
            return empty_skills_catalog_payload(output or "技能目录输出不是合法 JSON。", supported=False)

        payload["supported"] = True
        payload["commands"] = [
            {
                "label": "扫描技能目录",
                "command": f"python3 {cli_path} list --project-dir {project_dir}",
                "description": "查看当前 skills 目录、校验状态和打包准备度。",
            },
            {
                "label": "校验技能质量",
                "command": f"python3 {cli_path} validate --project-dir {project_dir}",
                "description": "按 Anthropic Skills 指南检查 frontmatter、结构和触发质量。",
            },
            {
                "label": "同步 gstack 技能集",
                "command": "mission-control /api/actions/skills/gstack/sync",
                "description": "把 gstack 的角色工作流技能同步成OpenClaw Team 本地受控技能目录。",
            },
        ]
        payload = augment_skills_payload_with_gstack_scaffolds(project_dir, payload)
        return enrich_skills_payload_with_role_workflows(openclaw_dir, payload)

    return cached_payload(("local-skills", str(project_dir), str(Path(openclaw_dir).expanduser().resolve())), 10, build)


def agent_response_latency_samples(openclaw_dir, agent_id, limit=8):
    sessions_dir = Path(openclaw_dir) / "agents" / str(agent_id or "").strip() / "sessions"
    if not sessions_dir.exists():
        return []
    samples = []
    session_files = sorted(
        sessions_dir.glob("*.jsonl"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0,
        reverse=True,
    )[:3]
    for path in session_files:
        pending_user_at = None
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            tail_lines = list(deque(handle, maxlen=140))
        for raw in tail_lines:
            line = raw.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("type") != "message":
                continue
            payload = entry.get("message", {}) if isinstance(entry.get("message"), dict) else {}
            role = payload.get("role")
            at = parse_iso(entry.get("timestamp") or payload.get("timestamp"))
            if not at:
                continue
            if role == "user":
                pending_user_at = at
            elif role == "assistant" and pending_user_at:
                delta = max((at - pending_user_at).total_seconds(), 0)
                samples.append(delta)
                pending_user_at = None
    return samples[-limit:]


def perform_skill_scaffold(
    openclaw_dir,
    slug,
    title,
    description,
    trigger_phrase,
    category,
    include_scripts=False,
    include_references=True,
    include_assets=False,
    mcp_server="",
):
    project_dir, cli_path = skills_cli_path(openclaw_dir)
    if not project_dir or not cli_path:
        raise RuntimeError("当前安装没有关联 skill 工具脚本，无法创建新技能。")
    args = [
        "scaffold",
        "--project-dir",
        str(project_dir),
        "--slug",
        slug,
        "--title",
        title,
        "--description",
        description,
        "--trigger-phrase",
        trigger_phrase,
        "--category",
        category,
        "--version",
        "1.0.0",
    ]
    if include_scripts:
        args.append("--include-scripts")
    if include_references:
        args.append("--include-references")
    if include_assets:
        args.append("--include-assets")
    if mcp_server:
        args.extend(["--mcp-server", mcp_server])
    result, output = run_python_script(cli_path, args, cwd=project_dir)
    if result.returncode != 0:
        raise RuntimeError(output or f"创建技能失败: {slug}")
    try:
        return json.loads(result.stdout or output or "{}")
    except json.JSONDecodeError as error:
        raise RuntimeError(f"技能脚手架输出异常: {error}") from error


def perform_skill_package(openclaw_dir, slug):
    project_dir, cli_path = skills_cli_path(openclaw_dir)
    if not project_dir or not cli_path:
        raise RuntimeError("当前安装没有关联 skill 工具脚本，无法打包技能。")
    result, output = run_python_script(
        cli_path,
        ["package", "--project-dir", str(project_dir), "--skill", slug],
        cwd=project_dir,
    )
    if result.returncode != 0:
        raise RuntimeError(output or f"打包技能失败: {slug}")
    try:
        return json.loads(result.stdout or output or "{}")
    except json.JSONDecodeError as error:
        raise RuntimeError(f"技能打包输出异常: {error}") from error


def perform_skill_publish(openclaw_dir, slug):
    project_dir, cli_path = skills_cli_path(openclaw_dir)
    if not project_dir or not cli_path:
        raise RuntimeError("当前安装没有关联 skill 工具脚本，无法发布技能到 OpenClaw。")
    result, output = run_python_script(
        cli_path,
        [
            "publish",
            "--project-dir",
            str(project_dir),
            "--openclaw-dir",
            str(Path(openclaw_dir).expanduser().resolve()),
            "--skill",
            slug,
        ],
        cwd=project_dir,
    )
    if result.returncode != 0:
        raise RuntimeError(output or f"发布技能失败: {slug}")
    try:
        return json.loads(result.stdout or output or "{}")
    except json.JSONDecodeError as error:
        raise RuntimeError(f"技能发布输出异常: {error}") from error


def perform_skill_install(openclaw_dir, slug):
    project_dir, cli_path = skills_cli_path(openclaw_dir)
    if not project_dir or not cli_path:
        raise RuntimeError("当前安装没有关联 skill 工具脚本，无法安装技能。")
    result, output = run_python_script(
        cli_path,
        [
            "install",
            "--project-dir",
            str(project_dir),
            "--openclaw-dir",
            str(Path(openclaw_dir).expanduser().resolve()),
            "--skill",
            slug,
        ],
        cwd=project_dir,
    )
    if result.returncode != 0:
        raise RuntimeError(output or f"安装技能失败: {slug}")
    try:
        return json.loads(result.stdout or output or "{}")
    except json.JSONDecodeError as error:
        raise RuntimeError(f"技能安装输出异常: {error}") from error


def perform_skill_uninstall(openclaw_dir, slug):
    project_dir, cli_path = skills_cli_path(openclaw_dir)
    if not project_dir or not cli_path:
        raise RuntimeError("当前安装没有关联 skill 工具脚本，无法卸载技能。")
    result, output = run_python_script(
        cli_path,
        [
            "uninstall",
            "--openclaw-dir",
            str(Path(openclaw_dir).expanduser().resolve()),
            "--skill",
            slug,
        ],
        cwd=project_dir,
    )
    if result.returncode != 0:
        raise RuntimeError(output or f"卸载技能失败: {slug}")
    try:
        return json.loads(result.stdout or output or "{}")
    except json.JSONDecodeError as error:
        raise RuntimeError(f"技能卸载输出异常: {error}") from error
