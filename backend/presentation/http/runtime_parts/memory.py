"""Runtime part: memory."""

def team_memory_text(team):
    team_meta = team_runtime_meta(team)
    return str(team_meta.get("teamMemory") or "").strip()


def build_team_working_memory(dispatch_state, limit=4):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    memory_lines = []
    response_entries = coordination_reply_entries(dispatch_state.get("responses"), limit=limit)
    for item in response_entries:
        line = f"{str(item.get('agentDisplayName') or item.get('agentId') or '').strip()}：{str(item.get('replyPreview') or '').strip()}"
        if line and line not in memory_lines:
            memory_lines.append(line)
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    relay_entries = coordination_reply_entries(relay.get("responses"), limit=max(1, limit - len(memory_lines)))
    for item in relay_entries:
        line = f"{str(item.get('agentDisplayName') or item.get('agentId') or '').strip()}：{str(item.get('replyPreview') or '').strip()}"
        if line and line not in memory_lines:
            memory_lines.append(line)
    collaboration = dispatch_state.get("collaboration") if isinstance(dispatch_state.get("collaboration"), dict) else {}
    if collaboration.get("blockerCount"):
        memory_lines.append(f"当前提醒：{int(collaboration.get('blockerCount') or 0)} 人提到了卡点或需要支援。")
    if not memory_lines:
        return ""
    return "\n".join(f"- {line}" for line in memory_lines[: max(1, int(limit or 4))])


def apply_team_working_memory(team_policy, dispatch_state):
    next_policy = deepcopy(team_policy) if isinstance(team_policy, dict) else {}
    updated_at = str(dispatch_state.get("at") or now_iso()).strip()
    working_memory = build_team_working_memory(dispatch_state, limit=4)
    current_focus_items = build_team_current_focus_items(dispatch_state, limit=3)
    current_focus = [str(item.get("text") or "").strip() for item in current_focus_items if str(item.get("text") or "").strip()]
    if not current_focus:
        current_focus = build_team_current_focus(dispatch_state, limit=3)
    open_loop_items = build_team_open_loop_items(dispatch_state, limit=4)
    open_loops = [str(item.get("text") or "").strip() for item in open_loop_items if str(item.get("text") or "").strip()]
    if not open_loops:
        open_loops = build_team_open_loops(dispatch_state, limit=4)
    active_owners = build_team_active_owners(current_focus_items, open_loop_items, limit=4)
    next_policy["workingMemory"] = working_memory
    next_policy["workingMemoryUpdatedAt"] = updated_at
    next_policy["currentFocus"] = "\n".join(f"- {line}" for line in current_focus) if current_focus else ""
    next_policy["currentFocusUpdatedAt"] = updated_at if current_focus else ""
    next_policy["currentFocusItems"] = current_focus_items
    next_policy["openLoops"] = clean_unique_strings(open_loops)
    next_policy["openLoopItems"] = open_loop_items
    next_policy["activeOwners"] = active_owners
    next_policy["taskLongTermMemory"] = build_task_long_term_memory_payload(
        next_policy.get("taskLongTermMemory"),
        dispatch_state,
        fallback_title=str(dispatch_state.get("taskTitle") or dispatch_state.get("contextLabel") or "").strip(),
        fallback_note=str(dispatch_state.get("note") or "").strip(),
    )
    return next_policy


def team_memory_trace_payload(team_policy):
    team_policy = team_policy if isinstance(team_policy, dict) else {}
    working_memory = str(team_policy.get("workingMemory") or "").strip()
    team_memory = str(team_policy.get("teamMemory") or "").strip()
    decision_log = str(team_policy.get("decisionLog") or "").strip()
    current_focus = str(team_policy.get("currentFocus") or "").strip()
    current_focus_items = safe_list(team_policy.get("currentFocusItems"))
    open_loops = normalize_team_context_lines(team_policy.get("openLoops"), limit=4)
    open_loop_items = safe_list(team_policy.get("openLoopItems"))
    active_owners = safe_list(team_policy.get("activeOwners"))
    task_long_term_memory = compact_task_long_term_memory(team_policy.get("taskLongTermMemory"))

    def first_memory_line(value):
        raw_lines = value if isinstance(value, list) else str(value or "").splitlines()
        for raw_line in raw_lines:
            normalized = re.sub(r"^[\-\u2022]\s*", "", raw_line.strip())
            if normalized:
                return normalized
        return ""

    def first_owned_item(items):
        fallback_item = {}
        for item in safe_list(items):
            if not isinstance(item, dict):
                continue
            if not fallback_item:
                fallback_item = item
            owner_label = str(item.get("ownerLabel") or item.get("agentId") or "").strip()
            if owner_label:
                return item
        return fallback_item

    payload = {}
    if active_owners:
        payload["activeOwnerLabels"] = [
            str(item.get("ownerLabel") or item.get("agentId") or "").strip()
            for item in active_owners
            if isinstance(item, dict) and str(item.get("ownerLabel") or item.get("agentId") or "").strip()
        ][:3]
    if current_focus:
        payload["currentFocusApplied"] = True
        payload["currentFocusPreview"] = first_memory_line(current_focus)
        if current_focus_items:
            first_focus = first_owned_item(current_focus_items)
            owner_label = str(first_focus.get("ownerLabel") or first_focus.get("agentId") or "").strip()
            if owner_label:
                payload["currentFocusOwnerLabel"] = owner_label
    if open_loops:
        payload["openLoopApplied"] = True
        payload["openLoopPreview"] = first_memory_line(open_loops)
        if open_loop_items:
            first_loop = first_owned_item(open_loop_items)
            owner_label = str(first_loop.get("ownerLabel") or first_loop.get("agentId") or "").strip()
            if owner_label:
                payload["openLoopOwnerLabel"] = owner_label
            open_loop_kind = str(first_loop.get("kind") or "").strip()
            if open_loop_kind:
                payload["openLoopKind"] = open_loop_kind
    if working_memory:
        payload["workingMemoryApplied"] = True
        payload["workingMemoryPreview"] = first_memory_line(working_memory)
    if team_memory or decision_log:
        payload["teamGuidanceApplied"] = True
        payload["teamGuidancePreview"] = first_memory_line(team_memory) or first_memory_line(decision_log)
    if task_long_term_memory.get("longTermMemory"):
        payload["taskLongTermMemoryApplied"] = True
        payload["taskLongTermMemoryPreview"] = first_memory_line(task_long_term_memory.get("longTermMemory"))
    return payload


def load_agent_memory_data(openclaw_dir, config, agent_cards=None):
    now = datetime.now(timezone.utc)
    cards_by_id = {
        item.get("id"): item
        for item in safe_list(agent_cards)
        if isinstance(item, dict) and item.get("id")
    }
    metadata = load_project_metadata(openclaw_dir, config=config)
    memory_system = memory_system_status_payload(
        metadata,
        agents=load_agents(config),
        teams=store_list_agent_teams(openclaw_dir),
    )
    agents_payload = []
    total_documents = 0
    ready_agents = 0
    for agent in load_agents(config):
        agent_id = agent.get("id", "")
        if not agent_id:
            continue
        workspace = Path(agent.get("workspace") or (Path(openclaw_dir) / f"workspace-{agent_id}")).expanduser()
        card = cards_by_id.get(agent_id, {})
        documents = []
        root_doc = build_markdown_document(
            workspace / "MEMORY.md",
            root=workspace,
            now=now,
            kind="rootMemory",
            agent_id=agent_id,
            agent_title=card.get("title", agent_id),
            label="MEMORY.md",
        )
        if root_doc:
            documents.append(root_doc)
        for doc_path in sorted((workspace / "memory").glob("*.md")):
            record = build_markdown_document(
                doc_path,
                root=workspace,
                now=now,
                kind="memorySeed",
                agent_id=agent_id,
                agent_title=card.get("title", agent_id),
            )
            if record:
                documents.append(record)
        if documents:
            ready_agents += 1
        total_documents += len(documents)
        last_updated = max((item.get("updatedAt", "") for item in documents), default="")
        agents_payload.append(
            {
                "id": agent_id,
                "name": agent.get("identity", {}).get("name", agent_id),
                "title": card.get("title", agent_id),
                "workspace": str(workspace),
                "model": agent.get("model", ""),
                "status": card.get("status", "idle"),
                "documentCount": len(documents),
                "lastUpdatedAt": last_updated,
                "documents": documents,
            }
        )
    agents_payload.sort(key=lambda item: (-item.get("documentCount", 0), item.get("title", "")))
    return {
        "supported": True,
        "enabled": bool(memory_system.get("enabled")),
        "provider": str(memory_system.get("provider") or "local").strip() or "local",
        "summary": {
            "agentCount": len(agents_payload),
            "readyAgentCount": ready_agents,
            "documentCount": total_documents,
        },
        "agents": agents_payload,
    }


def load_agent_memory_summary_data(openclaw_dir, config):
    metadata = load_project_metadata(openclaw_dir, config=config)
    memory_system = memory_system_status_payload(
        metadata,
        agents=load_agents(config),
        teams=store_list_agent_teams(openclaw_dir),
    )
    agent_count = 0
    ready_agents = 0
    total_documents = 0
    for agent in load_agents(config):
        agent_id = agent.get("id", "")
        if not agent_id:
            continue
        agent_count += 1
        workspace = Path(agent.get("workspace") or (Path(openclaw_dir) / f"workspace-{agent_id}")).expanduser()
        document_count = 1 if (workspace / "MEMORY.md").exists() else 0
        document_count += sum(1 for _ in (workspace / "memory").glob("*.md"))
        total_documents += document_count
        if document_count:
            ready_agents += 1
    return {
        "supported": True,
        "enabled": bool(memory_system.get("enabled")),
        "provider": str(memory_system.get("provider") or "local").strip() or "local",
        "summary": {
            "agentCount": agent_count,
            "readyAgentCount": ready_agents,
            "documentCount": total_documents,
        },
        "agents": [],
    }


def load_shared_context_data(openclaw_dir, config, router_agent_id="", agent_cards=None):
    now = datetime.now(timezone.utc)
    router_id = router_agent_id or get_router_agent_id(config)
    cards_by_id = {
        item.get("id"): item
        for item in safe_list(agent_cards)
        if isinstance(item, dict) and item.get("id")
    }
    router_agent = next((item for item in load_agents(config) if item.get("id") == router_id), {})
    workspace = Path(router_agent.get("workspace") or (Path(openclaw_dir) / f"workspace-{router_id}")).expanduser()
    shared_root = workspace / "shared-context"
    documents = []
    seeded_docs = [
        ("THESIS.md", "thesis", "THESIS.md"),
        ("FEEDBACK-LOG.md", "feedbackLog", "FEEDBACK-LOG.md"),
        ("ORG-STRUCTURE.md", "orgStructure", "ORG-STRUCTURE.md"),
    ]
    for filename, kind, label in seeded_docs:
        record = build_markdown_document(
            shared_root / filename,
            root=shared_root,
            now=now,
            kind=kind,
            agent_id=router_id,
            agent_title=cards_by_id.get(router_id, {}).get("title", router_id),
            label=label,
        )
        if record:
            documents.append(record)
    for doc_path in sorted((shared_root / "knowledge-base").rglob("*.md")):
        record = build_markdown_document(
            doc_path,
            root=shared_root,
            now=now,
            kind="knowledgeBase",
            agent_id=router_id,
            agent_title=cards_by_id.get(router_id, {}).get("title", router_id),
        )
        if record:
            documents.append(record)
    knowledge_base_count = len([item for item in documents if item.get("kind") == "knowledgeBase"])
    return {
        "supported": True,
        "rootPath": str(shared_root),
        "routerAgentId": router_id,
        "routerAgentTitle": cards_by_id.get(router_id, {}).get("title", router_id),
        "workspace": str(workspace),
        "summary": {
            "documentCount": len(documents),
            "knowledgeBaseCount": knowledge_base_count,
        },
        "documents": documents,
    }


def load_shared_context_summary_data(openclaw_dir, config, router_agent_id=""):
    router_id = router_agent_id or get_router_agent_id(config)
    router_agent = next((item for item in load_agents(config) if item.get("id") == router_id), {})
    workspace = Path(router_agent.get("workspace") or (Path(openclaw_dir) / f"workspace-{router_id}")).expanduser()
    shared_root = workspace / "shared-context"
    seeded_docs = [
        "THESIS.md",
        "FEEDBACK-LOG.md",
        "ORG-STRUCTURE.md",
    ]
    seeded_count = sum(1 for filename in seeded_docs if (shared_root / filename).exists())
    knowledge_base_count = sum(1 for _ in (shared_root / "knowledge-base").rglob("*.md"))
    identity = router_agent.get("identity") if isinstance(router_agent.get("identity"), dict) else {}
    router_title = str(identity.get("name") or router_id).strip() or router_id
    return {
        "supported": True,
        "rootPath": str(shared_root),
        "routerAgentId": router_id,
        "routerAgentTitle": router_title,
        "workspace": str(workspace),
        "summary": {
            "documentCount": seeded_count + knowledge_base_count,
            "knowledgeBaseCount": knowledge_base_count,
        },
        "documents": [],
    }


def resolve_layered_memory_document(openclaw_dir, scope, relative_path, agent_id=""):
    config = load_config(openclaw_dir)
    normalized_scope = str(scope or "").strip()
    normalized_relative = str(relative_path or "").strip().replace("\\", "/").lstrip("/")
    if not normalized_scope:
        raise RuntimeError("记忆文档 scope 不能为空。")
    if not normalized_relative:
        raise RuntimeError("记忆文档路径不能为空。")
    if ".." in normalized_relative.split("/"):
        raise RuntimeError("不允许访问上级目录。")

    if normalized_scope == "agentMemory":
        workspace, agent = resolve_agent_workspace(openclaw_dir, config, agent_id)
        allowed = normalized_relative == "MEMORY.md" or bool(re.fullmatch(r"memory/[^/]+\.md", normalized_relative))
        if not allowed:
            raise RuntimeError("当前只允许编辑 MEMORY.md 和 memory/*.md。")
        candidate = (workspace / normalized_relative).resolve()
        if candidate != workspace and workspace not in candidate.parents:
            raise RuntimeError("文档路径超出 Agent workspace。")
        if not candidate.exists() or not candidate.is_file():
            raise RuntimeError(f"记忆文档不存在：{normalized_relative}")
        return {
            "path": candidate,
            "scope": normalized_scope,
            "relativePath": normalized_relative,
            "agentId": agent.get("id", ""),
            "workspace": workspace,
        }

    if normalized_scope == "sharedContext":
        router_agent_id = get_router_agent_id(config)
        workspace, _agent = resolve_agent_workspace(openclaw_dir, config, router_agent_id)
        shared_root = (workspace / "shared-context").resolve()
        allowed = normalized_relative in {"THESIS.md", "FEEDBACK-LOG.md"} or bool(re.fullmatch(r"knowledge-base/.+\.md", normalized_relative))
        if not allowed:
            raise RuntimeError("当前只允许编辑 THESIS.md、FEEDBACK-LOG.md 和 knowledge-base/*.md。")
        candidate = (shared_root / normalized_relative).resolve()
        if candidate != shared_root and shared_root not in candidate.parents:
            raise RuntimeError("文档路径超出 shared-context 目录。")
        if not candidate.exists() or not candidate.is_file():
            raise RuntimeError(f"共享上下文文档不存在：{normalized_relative}")
        return {
            "path": candidate,
            "scope": normalized_scope,
            "relativePath": normalized_relative,
            "agentId": router_agent_id,
            "workspace": workspace,
        }

    raise RuntimeError(f"不支持的记忆文档 scope：{normalized_scope}")


def save_layered_memory_document(openclaw_dir, scope, relative_path, content, agent_id=""):
    target = resolve_layered_memory_document(openclaw_dir, scope, relative_path, agent_id=agent_id)
    text = str(content or "")
    if len(text) > 200000:
        raise RuntimeError("记忆文档内容过长，请控制在 200000 个字符以内。")
    target["path"].write_text(text, encoding="utf-8")
    return build_markdown_document(
        target["path"],
        root=target["workspace"] if target["scope"] == "agentMemory" else target["workspace"] / "shared-context",
        now=datetime.now(timezone.utc),
        kind="editableMemory",
        agent_id=target["agentId"],
        label=Path(target["relativePath"]).name,
    ) | {
        "scope": target["scope"],
        "relativePath": target["relativePath"],
        "agentId": target["agentId"],
    }
