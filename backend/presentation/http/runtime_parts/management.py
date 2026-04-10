"""Runtime part: management."""

from backend.application.services.computer_use import (
    build_computer_use_run_preview_snapshot,
    build_computer_use_run_snapshot,
)

def normalize_management_automation_mode(value):
    normalized = str(value or "").strip().lower()
    if normalized in MANAGEMENT_AUTOMATION_MODES:
        return normalized
    return MANAGEMENT_AUTOMATION_MODE_FULL_AUTO


def management_automation_mode_payload(mode):
    normalized = normalize_management_automation_mode(mode)
    payload = {
        MANAGEMENT_AUTOMATION_MODE_MANUAL: {
            "value": MANAGEMENT_AUTOMATION_MODE_MANUAL,
            "label": "手动观察",
            "description": "保留看盘和提醒视图。新任务先建好，不自动开工，也不自动复盘或修复。",
            "autoReviews": False,
            "autoFollowUps": False,
            "autoRemediation": False,
        },
        MANAGEMENT_AUTOMATION_MODE_ASSISTIVE: {
            "value": MANAGEMENT_AUTOMATION_MODE_ASSISTIVE,
            "label": "协作辅助",
            "description": "自动生成复盘和提醒。新任务先准备好分工建议，等你点头后再推进。",
            "autoReviews": True,
            "autoFollowUps": False,
            "autoRemediation": False,
        },
        MANAGEMENT_AUTOMATION_MODE_FULL_AUTO: {
            "value": MANAGEMENT_AUTOMATION_MODE_FULL_AUTO,
            "label": "全自动运营",
            "description": "新任务进来后会先内部讨论，再自动复盘、督办和修复，像运营搭档一样持续推进。",
            "autoReviews": True,
            "autoFollowUps": True,
            "autoRemediation": True,
        },
    }
    current = deepcopy(payload[normalized])
    current["options"] = [deepcopy(payload[item]) for item in (
        MANAGEMENT_AUTOMATION_MODE_MANUAL,
        MANAGEMENT_AUTOMATION_MODE_ASSISTIVE,
        MANAGEMENT_AUTOMATION_MODE_FULL_AUTO,
    )]
    return current


def current_management_automation_mode(metadata):
    metadata = metadata if isinstance(metadata, dict) else {}
    return normalize_management_automation_mode(metadata.get("managementAutomationMode"))


def set_management_automation_mode(openclaw_dir, mode, actor=None):
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    normalized = normalize_management_automation_mode(mode)
    metadata["managementAutomationMode"] = normalized
    metadata["managementAutomationUpdatedAt"] = now_iso()
    if actor:
        metadata["managementAutomationUpdatedBy"] = actor
    save_project_metadata(openclaw_dir, metadata)
    return management_automation_mode_payload(normalized)


def build_company_auto_operation_profile(title="", remark="", automation_mode="", team=None):
    if not is_company_auto_operation_task(title, remark):
        return {}
    team = team if isinstance(team, dict) else {}
    normalized_mode = normalize_management_automation_mode(automation_mode)
    objective = summarize_task_execution_text(str(remark or title or "持续经营公司并产出经营结论。").strip(), limit=180)
    team_name = str(team.get("name") or "").strip()
    focus_areas = infer_company_operation_focus_areas(title, remark)
    profile = {
        "enabled": True,
        "taskType": "company_auto_operation",
        "mode": normalized_mode,
        "objective": objective or "持续经营公司并产出经营结论。",
        "teamName": team_name,
        "focusAreas": focus_areas,
        "operatingCadence": [
            "每天先看经营信号、风险和机会，再决定今天最该推进的动作。",
            "默认自己拆出下一步，不等用户逐条下指令。",
            "每轮动作后都要复盘结果，沉淀新的判断和经营规律。",
        ],
        "learningLoop": [
            "观察经营信号",
            "判断最值得推进的问题",
            "拆出动作并推进",
            "复盘结果并更新方法",
        ],
        "autonomyRules": [
            "把它当成长期经营任务，不是一次性交付物。",
            "默认先内部讨论，再决定主攻方向和今日分工。",
            "如果没有新的用户指令，也要基于现有上下文继续提出下一步。",
            "每天都要给出经营结论、关键变化和下一步动作。",
        ],
        "outputContract": "每轮至少给出：经营判断、今天动作、风险或阻塞、下一步。",
        "autoStart": normalized_mode == MANAGEMENT_AUTOMATION_MODE_FULL_AUTO,
    }
    return profile


def build_company_auto_operation_memory_payload(
    profile,
    runtime,
    current_focus="",
    summary_text="",
    next_move="",
    open_loop_items=None,
    active_owners=None,
    day_key="",
    reviewed_at="",
):
    profile = compact_auto_operation_profile(profile)
    runtime_payload = compact_company_auto_operation_runtime(runtime)
    focus_areas = clean_unique_strings(profile.get("focusAreas") or [])[:3]
    current_focus = summarize_task_execution_text(current_focus or runtime_payload.get("currentFocus") or "", limit=140)
    summary_text = summarize_task_execution_text(summary_text or runtime_payload.get("summaryText") or "", limit=180)
    next_move = summarize_task_execution_text(next_move or runtime_payload.get("nextMove") or "", limit=160)
    long_term_seed = clean_unique_strings(
        [
            f"长期围绕 {'、'.join(focus_areas)} 持续经营。" if focus_areas else "",
            runtime_payload.get("longTermMemory") or "",
            f"最近稳定判断：{summary_text}" if summary_text else "",
            f"当前推进：{next_move}" if next_move else "",
        ]
    )
    long_term_memory = summarize_task_execution_text(" ".join(long_term_seed), limit=220)
    learning_candidates = clean_unique_strings(
        [
            *(runtime_payload.get("learningHighlights") or []),
            current_focus,
            next_move,
            *[
                summarize_task_execution_text((item or {}).get("text") or "", limit=96)
                for item in safe_list(open_loop_items)
                if isinstance(item, dict)
            ],
        ]
    )[:4]
    review_summary = summarize_task_execution_text(summary_text or current_focus or next_move or "", limit=140)
    latest_note = {
        "dayKey": str(day_key or runtime_payload.get("lastReviewDayKey") or "").strip(),
        "focus": current_focus,
        "nextMove": next_move,
        "summary": review_summary,
        "ownerLabel": str((safe_list(active_owners)[0] if safe_list(active_owners) else "") or "").strip(),
    }
    merged_notes = []
    seen_review_keys = set()
    for item in [latest_note, *safe_list(runtime_payload.get("recentReviewNotes"))]:
        if not isinstance(item, dict):
            continue
        normalized = {
            "dayKey": str(item.get("dayKey") or "").strip(),
            "focus": summarize_task_execution_text(item.get("focus") or "", limit=96),
            "nextMove": summarize_task_execution_text(item.get("nextMove") or "", limit=96),
            "summary": summarize_task_execution_text(item.get("summary") or item.get("focus") or item.get("nextMove") or "", limit=140),
        }
        if not any(normalized.values()):
            continue
        review_key = (normalized.get("dayKey"), normalized.get("summary") or normalized.get("focus") or normalized.get("nextMove"))
        if review_key in seen_review_keys:
            continue
        seen_review_keys.add(review_key)
        merged_notes.append(normalized)
    return {
        "currentFocus": current_focus,
        "summaryText": summary_text,
        "nextMove": next_move,
        "longTermMemory": long_term_memory,
        "learningHighlights": learning_candidates,
        "recentReviewNotes": merged_notes[:4],
        "memoryUpdatedAt": str(reviewed_at or runtime_payload.get("memoryUpdatedAt") or now_iso()).strip(),
    }


def company_auto_operation_prompt_lines(profile, audience="lead", runtime=None):
    profile = compact_auto_operation_profile(profile)
    if not profile.get("enabled"):
        return []
    normalized_audience = str(audience or "").strip().lower()
    runtime_payload = compact_company_auto_operation_runtime(runtime)
    lines = [
        "这是“经营公司”类任务：把它当成持续经营，不是做完一轮就停。",
        f"经营目标：{profile.get('objective') or '持续经营公司并产出经营结论。'}",
    ]
    focus_areas = clean_unique_strings(profile.get("focusAreas") or [])
    if focus_areas:
        lines.append("本轮重点：")
        lines.extend(f"- {item}" for item in focus_areas[:3])
    cadence = clean_unique_strings(profile.get("operatingCadence") or [])
    if cadence:
        lines.append("经营节奏：")
        lines.extend(f"- {item}" for item in cadence[:3])
    learning_loop = clean_unique_strings(profile.get("learningLoop") or [])
    if learning_loop:
        lines.append("学习闭环：")
        lines.extend(f"- {item}" for item in learning_loop[:4])
    autonomy_rules = clean_unique_strings(profile.get("autonomyRules") or [])
    if autonomy_rules:
        lines.append("自动经营约定：")
        lines.extend(f"- {item}" for item in autonomy_rules[:4])
    long_term_memory = str(runtime_payload.get("longTermMemory") or "").strip()
    if long_term_memory:
        lines.append(f"长期经营记忆：{long_term_memory}")
    learning_highlights = clean_unique_strings(runtime_payload.get("learningHighlights") or [])
    if learning_highlights:
        lines.append("最近学到的经营规律：")
        lines.extend(f"- {item}" for item in learning_highlights[:3])
    recent_review_notes = safe_list(runtime_payload.get("recentReviewNotes"))
    if recent_review_notes:
        lines.append("近几轮经营复盘：")
        for item in recent_review_notes[:2]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("dayKey") or "").strip()
            summary = str(item.get("summary") or item.get("focus") or item.get("nextMove") or "").strip()
            if summary:
                lines.append(f"- {label + '：' if label else ''}{summary}")
    if normalized_audience == "lead":
        lines.append("你要像经营负责人一样先判断今天最该推进什么，再组织团队推进，不要等用户逐条派活。")
        lines.append("如果今天的方向已经清楚，就直接往前推进，并把新的经营结论沉淀下来。")
    else:
        lines.append("你要像真实经营团队成员一样补位：接住当前重点、主动提出动作，并把结果反馈回团队。")
        lines.append("不要只等 lead 点名；看到你能推进的经营动作就直接认领。")
    output_contract = str(profile.get("outputContract") or "").strip()
    if output_contract:
        lines.append(f"输出要求：{output_contract}")
    return lines


def build_company_auto_operation_cycle_item(task, now, team=None, thread=None):
    task = task if isinstance(task, dict) else {}
    profile = task_auto_operation_profile_payload(task)
    if not profile.get("enabled"):
        return {}
    route_meta = task_route_meta_payload(task)
    runtime = compact_company_auto_operation_runtime(route_meta.get("autoOperationRuntime"))
    thread = thread if isinstance(thread, dict) else {}
    thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_policy = thread_meta.get("teamPolicy") if isinstance(thread_meta.get("teamPolicy"), dict) else {}
    local_now = (now or now_utc()).astimezone()
    day_key = local_now.strftime("%Y-%m-%d")
    task_id = str(task.get("id") or "").strip()
    title = str(task.get("title") or task_id).strip() or task_id
    focus_areas = clean_unique_strings(profile.get("focusAreas") or [])[:3]
    current_update = summarize_task_execution_text(task_display_update(task), limit=160)
    team_name = (
        str((team or {}).get("name") or "").strip()
        or str(route_meta.get("teamName") or task.get("teamLabel") or "").strip()
    )
    execution_bootstrap = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
    status = str(execution_bootstrap.get("status") or task.get("state") or task.get("status") or "").strip()
    current_focus = summarize_task_execution_text(
        str(team_policy.get("currentFocus") or runtime.get("currentFocus") or current_update or "").strip(),
        limit=140,
    )
    if not current_focus:
        current_focus = (
            f"今天先看 {focus_areas[0]}，再决定最值得推进的一步。"
            if focus_areas
            else "今天先判断最值得推进的经营动作，再拉齐分工。"
        )
    open_loops = []
    raw_loop_items = team_policy.get("openLoopItems") if isinstance(team_policy.get("openLoopItems"), list) else runtime.get("openLoopItems")
    for item in safe_list(raw_loop_items):
        if isinstance(item, dict):
            text = summarize_task_execution_text(item.get("text") or "", limit=120)
            owner_label = str(item.get("ownerLabel") or "").strip()
        else:
            text = summarize_task_execution_text(item, limit=120)
            owner_label = ""
        if text:
            open_loops.append({"text": text, "ownerLabel": owner_label})
    if not open_loops:
        generated_loops = []
        if focus_areas:
            generated_loops.append(f"围绕 {focus_areas[0]} 拿到今天的经营判断")
        if len(focus_areas) > 1:
            generated_loops.append(f"把 {focus_areas[1]} 拆成一个今天能推进的动作")
        generated_loops.append("收尾同步今天结论和明日动作")
        open_loops = [{"text": text, "ownerLabel": ""} for text in generated_loops[:3]]
    active_owners = clean_unique_strings(team_policy.get("activeOwners") or route_meta.get("autoOperationRuntime", {}).get("activeOwners") or [])
    summary_text = summarize_task_execution_text(
        str(team_policy.get("workingMemory") or runtime.get("summaryText") or "").strip(),
        limit=180,
    )
    if not summary_text:
        summary_text = f"这是一条长期经营任务，默认围绕 {' / '.join(focus_areas) or '增长、产品和营收'} 持续观察、判断、推进、复盘。"
    next_move = summarize_task_execution_text(str(runtime.get("nextMove") or "").strip(), limit=160)
    if not next_move:
        next_move = open_loops[0]["text"] if open_loops else "先确认今天最值得推进的一步，再同步分工。"
    memory_payload = build_company_auto_operation_memory_payload(
        profile,
        runtime,
        current_focus=current_focus,
        summary_text=summary_text,
        next_move=next_move,
        open_loop_items=open_loops,
        active_owners=active_owners,
        day_key=day_key,
        reviewed_at=str(runtime.get("lastReviewedAt") or "").strip(),
    )
    return {
        "taskId": task_id,
        "title": title,
        "teamId": str((team or {}).get("id") or route_meta.get("teamId") or task.get("teamId") or "").strip(),
        "teamName": team_name,
        "mode": str(profile.get("mode") or "").strip(),
        "status": status,
        "focusAreas": focus_areas,
        "currentFocus": memory_payload.get("currentFocus") or current_focus,
        "summaryText": memory_payload.get("summaryText") or summary_text,
        "nextMove": memory_payload.get("nextMove") or next_move,
        "openLoopItems": open_loops[:3],
        "activeOwners": active_owners[:4],
        "longTermMemory": memory_payload.get("longTermMemory") or "",
        "learningHighlights": clean_unique_strings(memory_payload.get("learningHighlights") or [])[:4],
        "recentReviewNotes": safe_list(memory_payload.get("recentReviewNotes"))[:4],
        "memoryUpdatedAt": str(memory_payload.get("memoryUpdatedAt") or runtime.get("memoryUpdatedAt") or "").strip(),
        "updatedAt": str(task.get("updatedAt") or "").strip(),
        "updatedAgo": format_age(parse_iso(task.get("updatedAt")), now),
        "lastReviewDayKey": str(runtime.get("lastReviewDayKey") or "").strip(),
        "lastReviewedAt": str(runtime.get("lastReviewedAt") or "").strip(),
        "reviewDue": str(runtime.get("lastReviewDayKey") or "").strip() != day_key,
        "dayKey": day_key,
    }


def process_company_auto_operation_cycle(openclaw_dir, tasks, now, auto_refresh=False, config=None):
    config = config or load_config(openclaw_dir)
    team_map = {item.get("id"): item for item in store_list_agent_teams(openclaw_dir) if item.get("id")}
    items = []
    refreshed_count = 0
    for task in safe_list(tasks):
        if not isinstance(task, dict):
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        if state in TERMINAL_STATES:
            continue
        route_meta = task_route_meta_payload(task)
        profile = compact_auto_operation_profile(route_meta.get("autoOperationProfile"))
        if not profile.get("enabled"):
            latest_decision = latest_routing_decision_for_task(openclaw_dir, str(task.get("id") or "").strip())
            latest_meta = (latest_decision.get("meta") or {}) if isinstance(latest_decision, dict) else {}
            profile = compact_auto_operation_profile(latest_meta.get("autoOperationProfile"))
            if profile.get("enabled"):
                patched_task = deepcopy(task)
                patched_task["routeDecision"] = {
                    **(route_meta if isinstance(route_meta, dict) else {}),
                    "autoOperationProfile": profile,
                }
                task = patched_task
                route_meta = task_route_meta_payload(task)
        if not profile.get("enabled"):
            continue
        team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
        team_id = str(route_meta.get("teamId") or team_assignment.get("teamId") or task.get("teamId") or "").strip()
        team = team_map.get(team_id) if team_id else None
        thread = existing_task_team_thread(openclaw_dir, str(task.get("id") or "").strip(), team_id=team_id)
        item = build_company_auto_operation_cycle_item(task, now, team=team, thread=thread)
        if not item:
            continue
        if item.get("reviewDue"):
            memory_payload = build_company_auto_operation_memory_payload(
                profile,
                route_meta.get("autoOperationRuntime"),
                current_focus=item.get("currentFocus"),
                summary_text=item.get("summaryText"),
                next_move=item.get("nextMove"),
                open_loop_items=item.get("openLoopItems"),
                active_owners=item.get("activeOwners"),
                day_key=item.get("dayKey"),
                reviewed_at=now_iso(),
            )
            runtime_payload = {
                "lastReviewDayKey": item.get("dayKey"),
                "lastReviewedAt": now_iso(),
                "currentFocus": item.get("currentFocus"),
                "summaryText": item.get("summaryText"),
                "nextMove": item.get("nextMove"),
                "openLoopItems": item.get("openLoopItems"),
                "activeOwners": item.get("activeOwners"),
                "focusAreas": item.get("focusAreas"),
                "longTermMemory": memory_payload.get("longTermMemory"),
                "learningHighlights": memory_payload.get("learningHighlights"),
                "recentReviewNotes": memory_payload.get("recentReviewNotes"),
                "memoryUpdatedAt": memory_payload.get("memoryUpdatedAt"),
            }
            if update_task_auto_operation_runtime_metadata(
                openclaw_dir,
                str(task.get("id") or "").strip(),
                runtime_payload,
            ):
                refreshed_count += 1
            project_memory_records_async(
                openclaw_dir,
                [
                    {
                        "scope": "company",
                        "ownerId": "company",
                        "label": "公司级长期记忆",
                        "summary": str(memory_payload.get("longTermMemory") or "").strip(),
                        "learningHighlights": clean_unique_strings(memory_payload.get("learningHighlights") or []),
                        "recentNotes": safe_list(memory_payload.get("recentReviewNotes"))[:4],
                        "relatedTaskId": str(task.get("id") or "").strip(),
                        "relatedThreadId": str((thread or {}).get("id") or "").strip(),
                        "updatedAt": str(memory_payload.get("memoryUpdatedAt") or now_iso()).strip(),
                        "meta": {"source": "company-auto-operation"},
                    }
                ],
                [
                    {
                        "scope": "company",
                        "ownerId": "company",
                        "eventType": "review_update",
                        "summary": str(runtime_payload.get("summaryText") or runtime_payload.get("currentFocus") or "").strip(),
                        "relatedTaskId": str(task.get("id") or "").strip(),
                        "relatedThreadId": str((thread or {}).get("id") or "").strip(),
                        "meta": {
                            "source": "company-auto-operation",
                            "focusAreas": safe_list(runtime_payload.get("focusAreas")),
                        },
                    }
                ],
            )
            if auto_refresh and thread:
                refresh_company_auto_operation_thread_policy(
                    openclaw_dir,
                    thread,
                    {
                        **item,
                        **runtime_payload,
                    },
                )
                item["threadSynced"] = True
            item = {
                **item,
                **runtime_payload,
                "reviewDue": False,
            }
        items.append(item)
    items.sort(
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    return {
        "count": len(items),
        "activeCount": len(items),
        "refreshedCount": refreshed_count,
        "items": items[:8],
    }


def automation_engine_status_path(openclaw_dir):
    return dashboard_dir(openclaw_dir) / "automation-engine-status.json"


def load_automation_engine_status(openclaw_dir):
    path = automation_engine_status_path(openclaw_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


def save_automation_engine_status(openclaw_dir, payload):
    path = automation_engine_status_path(openclaw_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def build_automation_optimization_recommendations(rules, channels, alerts, delivery_analytics):
    channel_map = {
        str(item.get("id") or ""): item
        for item in safe_list(channels)
        if isinstance(item, dict) and item.get("id")
    }
    recommendations = []

    for channel_stat in safe_list((delivery_analytics or {}).get("channels")):
        channel = channel_map.get(str(channel_stat.get("id") or ""))
        if not channel or str(channel.get("status") or "") != "active":
            continue
        if channel_stat.get("errors", 0) <= 0:
            continue
        if str(channel_stat.get("healthStatus") or "") not in {"error", "disabled"} and float(channel_stat.get("successRate") or 0) >= 50:
            continue
        recommendations.append(
            {
                "title": f"暂停通道 {channel.get('name', channel.get('id', ''))}",
                "detail": f"这个通道最近失败 {channel_stat.get('errors', 0)} 次，成功率 {channel_stat.get('successRate', 0)}%，适合先停用避免继续消耗告警。",
                "severity": "warning",
                "action": {
                    "type": "disable_channel",
                    "label": "停用通道",
                    "payload": {
                        "channelId": channel.get("id", ""),
                    },
                },
            }
        )

    for channel in safe_list(channels):
        if str(channel.get("status") or "") != "disabled":
            continue
        meta = channel.get("meta") if isinstance(channel.get("meta"), dict) else {}
        remediation = meta.get("automationRemediation") if isinstance(meta.get("automationRemediation"), dict) else {}
        if not remediation.get("disabledByAutomation"):
            continue
        health = meta.get("health") if isinstance(meta.get("health"), dict) else {}
        if str(health.get("status") or "") != "ok":
            continue
        recommendations.append(
            {
                "title": f"恢复通道 {channel.get('name', channel.get('id', ''))}",
                "detail": f"这个通道最近探活已恢复正常，建议重新启用，重新纳入告警链路。",
                "severity": "info",
                "action": {
                    "type": "enable_channel",
                    "label": "重新启用通道",
                    "payload": {
                        "channelId": channel.get("id", ""),
                    },
                },
            }
        )

    healthy_channels = [
        item
        for item in safe_list(channels)
        if str(item.get("status") or "") == "active" and str((item.get("meta") or {}).get("health", {}).get("status") or "") == "ok"
    ]
    alerts_by_rule = defaultdict(list)
    for alert in safe_list(alerts):
        rule_id = str(alert.get("ruleId") or "").strip()
        if rule_id:
            alerts_by_rule[rule_id].append(alert)

    for rule in safe_list(rules):
        if str(rule.get("status") or "") != "active":
            continue
        rule_alerts = alerts_by_rule.get(str(rule.get("id") or ""), [])
        if not rule_alerts:
            continue
        steps = safe_list(((rule.get("meta") or {}) if isinstance(rule.get("meta"), dict) else {}).get("escalationSteps"))
        if any(bool((step or {}).get("manual")) for step in steps):
            continue
        failed_alerts = [
            alert
            for alert in rule_alerts
            if str(alert.get("status") or "") in {"open", "error"}
            or any(str(delivery.get("outcome") or "") != "success" for delivery in safe_list(alert.get("deliveries")))
        ]
        if not failed_alerts:
            continue
        next_after_minutes = max([int((step or {}).get("afterMinutes") or 0) for step in steps] + [0]) + 30
        recommendations.append(
            {
                "title": f"给规则 {rule.get('name', '')} 补人工接管",
                "detail": f"这条规则最近已有 {len(failed_alerts)} 次未顺利收口，建议补人工接管节点，避免告警悬空。",
                "severity": "warning",
                "action": {
                    "type": "add_manual_escalation",
                    "label": "补人工接管",
                    "payload": {
                        "ruleId": rule.get("id", ""),
                        "afterMinutes": next_after_minutes,
                        "label": f"{next_after_minutes} 分钟后人工接管",
                    },
                },
            }
        )

    if healthy_channels:
        for rule in safe_list(rules):
            if str(rule.get("status") or "") != "active":
                continue
            rule_alerts = alerts_by_rule.get(str(rule.get("id") or ""), [])
            if not rule_alerts:
                continue
            has_failure = any(
                str(delivery.get("outcome") or "") != "success"
                for alert in rule_alerts
                for delivery in safe_list(alert.get("deliveries"))
            )
            if not has_failure:
                continue
            existing_channel_ids = set(rule_channel_ids(rule))
            backup_channel = next((item for item in healthy_channels if item.get("id") not in existing_channel_ids), None)
            if not backup_channel:
                continue
            steps = safe_list(((rule.get("meta") or {}) if isinstance(rule.get("meta"), dict) else {}).get("escalationSteps"))
            next_after_minutes = max([int((step or {}).get("afterMinutes") or 0) for step in steps] + [0]) + 15
            recommendations.append(
                {
                    "title": f"给规则 {rule.get('name', '')} 补兜底通道",
                    "detail": f"这条规则已有失败投递，建议把健康通道 {backup_channel.get('name', backup_channel.get('id', ''))} 加入升级链兜底。",
                    "severity": "info",
                    "action": {
                        "type": "add_backup_channel",
                        "label": "加入兜底通道",
                        "payload": {
                            "ruleId": rule.get("id", ""),
                            "channelId": backup_channel.get("id", ""),
                            "afterMinutes": next_after_minutes,
                            "label": f"{next_after_minutes} 分钟后补发到 {backup_channel.get('name', backup_channel.get('id', ''))}",
                        },
                    },
                }
            )
    return recommendations[:4]


def build_automation_rule_effectiveness_summary(rules, alerts):
    alerts_by_rule = defaultdict(list)
    for alert in safe_list(alerts):
        rule_id = str(alert.get("ruleId") or "").strip()
        if rule_id:
            alerts_by_rule[rule_id].append(alert)

    rows = []
    suggestions = []
    for rule in safe_list(rules):
        rule_id = str(rule.get("id") or "").strip()
        rule_alerts = alerts_by_rule.get(rule_id, [])
        triggered_count = len(rule_alerts)
        resolved_count = sum(1 for item in rule_alerts if str(item.get("status") or "") == "resolved")
        notified_count = sum(1 for item in rule_alerts if str(item.get("status") or "") == "notified")
        open_count = sum(1 for item in rule_alerts if str(item.get("status") or "") == "open")
        error_count = sum(1 for item in rule_alerts if str(item.get("status") or "") == "error")
        delivery_success = sum(
            1
            for item in rule_alerts
            for delivery in safe_list(item.get("deliveries"))
            if str(delivery.get("outcome") or "") == "success"
        )
        delivery_errors = sum(
            1
            for item in rule_alerts
            for delivery in safe_list(item.get("deliveries"))
            if str(delivery.get("outcome") or "") != "success"
        )
        unresolved_count = open_count + error_count
        noise_score = unresolved_count * 2 + max(delivery_errors - delivery_success, 0)
        rows.append(
            {
                "ruleId": rule_id,
                "ruleName": rule.get("name", ""),
                "status": rule.get("status", ""),
                "triggerType": rule.get("triggerType", ""),
                "thresholdMinutes": int(rule.get("thresholdMinutes") or 0),
                "cooldownMinutes": int(rule.get("cooldownMinutes") or 0),
                "triggeredCount": triggered_count,
                "resolvedCount": resolved_count,
                "notifiedCount": notified_count,
                "openCount": open_count,
                "errorCount": error_count,
                "deliverySuccessCount": delivery_success,
                "deliveryErrorCount": delivery_errors,
                "unresolvedRate": int(round((unresolved_count / max(triggered_count, 1)) * 100)) if triggered_count else 0,
                "noiseScore": noise_score,
            }
        )

    rows.sort(key=lambda item: (-item.get("noiseScore", 0), -item.get("triggeredCount", 0), item.get("ruleName", "")))
    for row in rows:
        if str(row.get("status") or "") != "active":
            continue
        if row.get("triggeredCount", 0) < 1:
            continue
        trigger_type = str(row.get("triggerType") or "").strip()
        threshold_minutes = int(row.get("thresholdMinutes") or 0)
        cooldown_minutes = int(row.get("cooldownMinutes") or 0)
        if trigger_type in {"blocked_task_timeout", "agent_offline"} and row.get("unresolvedRate", 0) >= 50 and threshold_minutes < 180:
            next_threshold = min(180, max(threshold_minutes + (30 if threshold_minutes >= 30 else 15), 15))
            suggestions.append(
                {
                    "title": f"提高规则 {row.get('ruleName', '')} 的触发阈值",
                    "detail": f"这条规则当前未收口占比 {row.get('unresolvedRate', 0)}%，建议把阈值从 {threshold_minutes} 分钟提高到 {next_threshold} 分钟，减少过早触发带来的噪音。",
                    "severity": "info",
                    "action": {
                        "type": "tune_rule",
                        "label": f"改成 {next_threshold} 分钟触发",
                        "payload": {
                            "ruleId": row.get("ruleId", ""),
                            "thresholdMinutes": next_threshold,
                        },
                    },
                }
            )
        if row.get("triggeredCount", 0) >= 3 and cooldown_minutes < 360:
            next_cooldown = min(360, max(cooldown_minutes + (60 if cooldown_minutes >= 60 else 30), 30))
            suggestions.append(
                {
                    "title": f"拉长规则 {row.get('ruleName', '')} 的冷却时间",
                    "detail": f"这条规则最近已命中 {row.get('triggeredCount', 0)} 次，建议把冷却时间从 {cooldown_minutes} 分钟延长到 {next_cooldown} 分钟，避免短时间重复打扰。",
                    "severity": "info",
                    "action": {
                        "type": "tune_rule",
                        "label": f"冷却延长到 {next_cooldown} 分钟",
                        "payload": {
                            "ruleId": row.get("ruleId", ""),
                            "cooldownMinutes": next_cooldown,
                        },
                    },
                }
            )
        if row.get("unresolvedRate", 0) < 70 and row.get("deliveryErrorCount", 0) <= row.get("deliverySuccessCount", 0):
            continue
        suggestions.append(
            {
                "title": f"暂停规则 {row.get('ruleName', '')}",
                "detail": f"这条规则当前未收口占比 {row.get('unresolvedRate', 0)}%，继续运行会放大噪音，建议先暂停再复盘。",
                "severity": "warning",
                "action": {
                    "type": "pause_rule",
                    "label": "暂停规则",
                    "payload": {
                        "ruleId": row.get("ruleId", ""),
                    },
                },
            }
        )

    return {
        "rows": rows[:8],
        "suggestions": suggestions[:3],
    }


def build_automation_tuning_review(openclaw_dir, rules, rule_effectiveness, now):
    effect_rows = {
        str(item.get("ruleId") or "").strip(): item
        for item in safe_list((rule_effectiveness or {}).get("rows"))
        if str(item.get("ruleId") or "").strip()
    }
    rule_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(rules)
        if str(item.get("id") or "").strip()
    }
    tuning_events = []
    manual_count = 0
    auto_count = 0
    watch_count = 0
    stabilized_count = 0
    for event in load_audit_events(openclaw_dir, limit=120):
        if event.get("action") not in {"management_rule_tune", "automation_rule_tune"}:
            continue
        meta = event.get("meta", {}) if isinstance(event.get("meta"), dict) else {}
        rule_id = str(meta.get("ruleId") or "").strip()
        rule = rule_map.get(rule_id, {})
        effect = effect_rows.get(rule_id, {})
        source = "automation" if event.get("action") == "automation_rule_tune" else "manual"
        if source == "automation":
            auto_count += 1
        else:
            manual_count += 1
        unresolved_rate = int(effect.get("unresolvedRate") or 0)
        if unresolved_rate >= 50:
            verdict = "needs_follow_up"
            watch_count += 1
        elif unresolved_rate >= 20:
            verdict = "watch"
            watch_count += 1
        else:
            verdict = "stabilizing"
            stabilized_count += 1
        at = parse_iso(event.get("at"))
        tuning_events.append(
            {
                "id": event.get("id", ""),
                "ruleId": rule_id,
                "ruleName": rule.get("name") or meta.get("ruleName") or "未知规则",
                "source": source,
                "at": event.get("at", ""),
                "atAgo": format_age(at, now) if at else "未知时间",
                "thresholdMinutes": int(meta.get("thresholdMinutes") or rule.get("thresholdMinutes") or 0),
                "cooldownMinutes": int(meta.get("cooldownMinutes") or rule.get("cooldownMinutes") or 0),
                "unresolvedRate": unresolved_rate,
                "deliveryErrorCount": int(effect.get("deliveryErrorCount") or 0),
                "triggeredCount": int(effect.get("triggeredCount") or 0),
                "verdict": verdict,
                "detail": event.get("detail", ""),
            }
        )
    tuning_events.sort(key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    return {
        "summary": {
            "total": len(tuning_events),
            "manualCount": manual_count,
            "autoCount": auto_count,
            "watchCount": watch_count,
            "stabilizedCount": stabilized_count,
        },
        "rows": tuning_events[:6],
    }


def build_automation_snapshot(openclaw_dir, rules, channels):
    rule_map = {item["id"]: item for item in rules}
    refreshed_alerts = store_list_automation_alerts(openclaw_dir, limit=80)
    refreshed_deliveries = store_list_notification_deliveries(openclaw_dir, limit=200)
    channel_list = list(channels.values()) if isinstance(channels, dict) else safe_list(channels)
    recent = []
    for alert in refreshed_alerts[:12]:
        deliveries_for_alert = [item for item in refreshed_deliveries if item.get("alertId") == alert.get("id")]
        recent.append(
            {
                **alert,
                "ruleName": (rule_map.get(alert.get("ruleId")) or {}).get("name", ""),
                "deliveries": [
                    {
                        **item,
                        "channelName": (channels.get(item.get("channelId")) or {}).get("name", item.get("channelId", "")),
                    }
                    for item in deliveries_for_alert
                ],
            }
        )
    delivery_analytics = build_delivery_analytics(refreshed_deliveries, channel_list, now_utc())
    recommendations = build_automation_optimization_recommendations(rules, channel_list, recent, delivery_analytics)
    rule_effectiveness = build_automation_rule_effectiveness_summary(rules, recent)
    tuning_review = build_automation_tuning_review(openclaw_dir, rules, rule_effectiveness, now_utc())
    remediation_actions = [
        deepcopy(item.get("action"))
        for item in recommendations
        if isinstance(item, dict)
        and isinstance(item.get("action"), dict)
        and str((item.get("action") or {}).get("type") or "").strip()
    ][:4]
    previous_engine = load_automation_engine_status(openclaw_dir)
    engine = {
        **(previous_engine if isinstance(previous_engine, dict) else {}),
        "status": str((previous_engine or {}).get("status") or "ok"),
        "autoRemediationCount": max(
            int(((previous_engine or {}).get("autoRemediationCount") or 0)),
            len(remediation_actions),
        ),
        "manualEscalationCount": int(((previous_engine or {}).get("manualEscalationCount") or 0)),
    }
    return {
        "rules": rules,
        "channels": list(channels.values()),
        "alerts": recent,
        "deliveryAnalytics": delivery_analytics,
        "recommendations": recommendations,
        "remediation": {
            "count": len(remediation_actions),
            "actions": remediation_actions,
        },
        "engine": engine,
        "ruleEffectiveness": rule_effectiveness,
        "tuningReview": tuning_review,
        "summary": {
            "activeRules": sum(1 for item in rules if item.get("status") == "active"),
            "openAlerts": sum(1 for item in refreshed_alerts if item.get("status") in {"open", "error"}),
            "notifiedAlerts": sum(1 for item in refreshed_alerts if item.get("status") == "notified"),
            "activeChannels": sum(1 for item in channels.values() if item.get("status") == "active"),
            "healthyChannels": sum(1 for item in channels.values() if str(notification_channel_health(item).get("status") or "") == "ok"),
            "unhealthyChannels": sum(
                1 for item in channels.values() if str(notification_channel_health(item).get("status") or "") in {"error", "disabled"}
            ),
            "deliverySuccessRate": delivery_analytics.get("successRate", 0.0),
            "deliveryFailures": delivery_analytics.get("errorCount", 0),
        },
    }


def should_skip_management_automation_cycle():
    return str(os.environ.get("MISSION_CONTROL_SKIP_AUTOMATION_CYCLE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def empty_automation_delivery_payload():
    return {"count": 0, "deliveryAttemptCount": 0, "deliverySuccessCount": 0, "manualEscalationCount": 0}


def empty_company_auto_operation_payload():
    return {"count": 0, "activeCount": 0, "refreshedCount": 0, "items": []}


def build_management_automation_placeholder(openclaw_dir, management_metadata=None):
    management_metadata = management_metadata if isinstance(management_metadata, dict) else load_project_metadata(openclaw_dir)
    rules = store_list_automation_rules(openclaw_dir)
    channels = {item["id"]: item for item in store_list_notification_channels(openclaw_dir) if item.get("id")}
    config = load_config(openclaw_dir)
    agents = [
        {
            "id": str(agent.get("id") or "").strip(),
            "title": str(agent.get("id") or "").strip(),
            "name": "",
            "role": "",
            "skills": [],
        }
        for agent in load_agents(config)
        if str(agent.get("id") or "").strip()
    ]
    teams = store_list_agent_teams(openclaw_dir)
    snapshot = build_automation_snapshot(openclaw_dir, rules, channels)
    mode_payload = management_automation_mode_payload(current_management_automation_mode(management_metadata))
    mode_name = str(mode_payload.get("mode") or "").strip().lower()
    snapshot["mode"] = mode_payload
    engine_payload = snapshot.get("engine") if isinstance(snapshot.get("engine"), dict) else {}
    live_engine_status = load_automation_engine_status(openclaw_dir)
    snapshot["engine"] = {
        **live_engine_status,
        **engine_payload,
    }
    snapshot["dailyReview"] = empty_automation_delivery_payload()
    snapshot["weeklyReport"] = empty_automation_delivery_payload()
    if not isinstance(snapshot.get("remediation"), dict):
        snapshot["remediation"] = {"count": 0, "actions": []}
    snapshot["companyAutoOperation"] = empty_company_auto_operation_payload()
    snapshot["taskExecutionRepair"] = {"count": 0, "items": [], "mode": mode_name}
    snapshot["taskExecutionSync"] = {"count": 0, "items": [], "mode": mode_name}
    snapshot["taskExecutionCompletionSync"] = {"count": 0, "items": [], "mode": mode_name}
    snapshot["memorySystem"] = memory_system_status_payload(management_metadata, agents=agents, teams=teams)
    snapshot["customerChannels"] = build_customer_access_snapshot(openclaw_dir)
    return snapshot


def perform_pause_automation_rule(openclaw_dir, rule_id):
    rule_id = str(rule_id or "").strip()
    if not rule_id:
        raise RuntimeError("请提供规则编号。")
    rules = store_list_automation_rules(openclaw_dir)
    rule = next((item for item in rules if item.get("id") == rule_id), None)
    if not rule:
        raise RuntimeError("自动化规则不存在。")
    if str(rule.get("status") or "") == "disabled":
        return rule
    return store_save_automation_rule(
        openclaw_dir,
        {
            **rule,
            "status": "disabled",
        },
    )


def perform_set_management_automation_mode(openclaw_dir, mode, actor=None):
    payload = set_management_automation_mode(openclaw_dir, mode, actor=actor)
    invalidate_management_payload_cache(openclaw_dir)
    return payload


def perform_tune_automation_rule(openclaw_dir, rule_id, threshold_minutes=None, cooldown_minutes=None, source="", recommendation=None):
    rule_id = str(rule_id or "").strip()
    if not rule_id:
        raise RuntimeError("请提供规则编号。")
    rules = store_list_automation_rules(openclaw_dir)
    rule = next((item for item in rules if item.get("id") == rule_id), None)
    if not rule:
        raise RuntimeError("自动化规则不存在。")
    payload = {**rule}
    if threshold_minutes is not None:
        payload["thresholdMinutes"] = max(int(threshold_minutes or 0), 0)
    if cooldown_minutes is not None:
        payload["cooldownMinutes"] = max(int(cooldown_minutes or 0), 0)
    if payload.get("thresholdMinutes") == rule.get("thresholdMinutes") and payload.get("cooldownMinutes") == rule.get("cooldownMinutes"):
        return rule
    meta = deepcopy(rule.get("meta", {})) if isinstance(rule.get("meta"), dict) else {}
    if source:
        tuning_meta = deepcopy(meta.get("automationTuning", {})) if isinstance(meta.get("automationTuning"), dict) else {}
        tuning_meta.update(
            {
                "source": source,
                "lastAppliedAt": now_iso(),
                "thresholdMinutes": payload.get("thresholdMinutes"),
                "cooldownMinutes": payload.get("cooldownMinutes"),
                "recommendation": recommendation or "",
            }
        )
        meta["automationTuning"] = tuning_meta
        payload["meta"] = meta
    return store_save_automation_rule(openclaw_dir, payload)


def perform_update_automation_alert_status(openclaw_dir, alert_id, status, actor=None):
    alert_id = str(alert_id or "").strip()
    status = str(status or "").strip().lower()
    if not alert_id:
        raise RuntimeError("请提供告警编号。")
    if status not in {"open", "notified", "resolved"}:
        raise RuntimeError("不支持的告警状态。")
    alerts = store_list_automation_alerts(openclaw_dir, limit=256)
    alert = next((item for item in alerts if item.get("id") == alert_id), None)
    if not alert:
        raise RuntimeError("运营告警不存在。")
    meta = deepcopy(alert.get("meta", {})) if isinstance(alert.get("meta"), dict) else {}
    resolution = deepcopy(meta.get("resolution", {})) if isinstance(meta.get("resolution"), dict) else {}
    actor = actor or automation_system_actor()
    actor_label = actor.get("displayName") or actor.get("username") or "Unknown"
    if status == "resolved":
        resolution.update(
            {
                "status": "resolved",
                "actor": actor_label,
                "at": now_iso(),
            }
        )
    elif status == "notified":
        resolution.update(
            {
                "status": "acknowledged",
                "actor": actor_label,
                "at": now_iso(),
            }
        )
    else:
        resolution = {
            "status": "reopened",
            "actor": actor_label,
            "at": now_iso(),
        }
    meta["resolution"] = resolution
    updated = store_upsert_automation_alert(
        openclaw_dir,
        {
            **alert,
            "status": status,
            "updatedAt": now_iso(),
            "resolvedAt": now_iso() if status == "resolved" else "",
            "meta": meta,
        },
    )
    return updated


def perform_automation_auto_remediation(openclaw_dir, automation):
    automation = automation if isinstance(automation, dict) else {}
    actions = []
    recommendation_items = safe_list(automation.get("recommendations")) + safe_list(
        ((automation.get("ruleEffectiveness") or {}) if isinstance(automation.get("ruleEffectiveness"), dict) else {}).get("suggestions")
    )
    for item in recommendation_items:
        action = item.get("action") if isinstance(item, dict) else None
        if not isinstance(action, dict):
            continue
        action_type = str(action.get("type") or "").strip()
        payload = action.get("payload") if isinstance(action.get("payload"), dict) else {}
        try:
            if action_type == "disable_channel":
                channel = perform_disable_notification_channel(openclaw_dir, payload.get("channelId"))
                append_audit_event(
                    openclaw_dir,
                    "automation_channel_disable",
                    automation_system_actor(),
                    detail=f"自动停用通知渠道 {channel.get('name', channel.get('id', ''))}",
                    meta={"channelId": channel.get("id", ""), "source": "automation_engine"},
                )
                actions.append(
                    {
                        "type": action_type,
                        "channelId": channel.get("id", ""),
                        "channelName": channel.get("name", ""),
                    }
                )
            elif action_type == "enable_channel":
                channel = perform_enable_notification_channel(openclaw_dir, payload.get("channelId"))
                append_audit_event(
                    openclaw_dir,
                    "automation_channel_enable",
                    automation_system_actor(),
                    detail=f"自动恢复通知渠道 {channel.get('name', channel.get('id', ''))}",
                    meta={"channelId": channel.get("id", ""), "source": "automation_engine"},
                )
                actions.append(
                    {
                        "type": action_type,
                        "channelId": channel.get("id", ""),
                        "channelName": channel.get("name", ""),
                    }
                )
            elif action_type == "add_backup_channel":
                rule = perform_append_rule_backup_channel(
                    openclaw_dir,
                    payload.get("ruleId"),
                    payload.get("channelId"),
                    after_minutes=payload.get("afterMinutes") or 15,
                    label=payload.get("label") or "",
                )
                append_audit_event(
                    openclaw_dir,
                    "automation_rule_add_backup_channel",
                    automation_system_actor(),
                    detail=f"自动给规则 {rule.get('name', rule.get('id', ''))} 补入兜底通道",
                    meta={
                        "ruleId": rule.get("id", ""),
                        "channelId": str(payload.get("channelId") or ""),
                        "source": "automation_engine",
                    },
                )
                actions.append(
                    {
                        "type": action_type,
                        "ruleId": rule.get("id", ""),
                        "channelId": str(payload.get("channelId") or ""),
                    }
                )
            elif action_type == "tune_rule":
                rule_id = str(payload.get("ruleId") or "").strip()
                rule = next((entry for entry in store_list_automation_rules(openclaw_dir) if entry.get("id") == rule_id), None)
                if not rule:
                    continue
                tuning_meta = (rule.get("meta") or {}).get("automationTuning") if isinstance(rule.get("meta"), dict) else {}
                last_applied = parse_iso((tuning_meta or {}).get("lastAppliedAt")) if isinstance(tuning_meta, dict) else None
                if last_applied and datetime.now(timezone.utc) - last_applied < timedelta(hours=24):
                    continue
                updated_rule = perform_tune_automation_rule(
                    openclaw_dir,
                    rule_id,
                    threshold_minutes=payload.get("thresholdMinutes") if "thresholdMinutes" in payload else None,
                    cooldown_minutes=payload.get("cooldownMinutes") if "cooldownMinutes" in payload else None,
                    source="automation_engine",
                    recommendation=str(item.get("title") or "").strip(),
                )
                append_audit_event(
                    openclaw_dir,
                    "automation_rule_tune",
                    automation_system_actor(),
                    detail=f"自动调整规则 {updated_rule.get('name', updated_rule.get('id', ''))}",
                    meta={
                        "ruleId": updated_rule.get("id", ""),
                        "thresholdMinutes": updated_rule.get("thresholdMinutes", 0),
                        "cooldownMinutes": updated_rule.get("cooldownMinutes", 0),
                        "source": "automation_engine",
                    },
                )
                actions.append(
                    {
                        "type": action_type,
                        "ruleId": updated_rule.get("id", ""),
                        "ruleName": updated_rule.get("name", ""),
                        "thresholdMinutes": updated_rule.get("thresholdMinutes", 0),
                        "cooldownMinutes": updated_rule.get("cooldownMinutes", 0),
                    }
                )
        except Exception as error:
            append_audit_event(
                openclaw_dir,
                "automation_remediation_error",
                automation_system_actor(),
                outcome="error",
                detail=str(error),
                meta={"actionType": action_type},
            )
    return actions


def evaluate_automation_rules(openclaw_dir, task_index, agents, management_runs, now, config=None, dispatch_supervision=True):
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    rules = store_list_automation_rules(openclaw_dir)
    refreshed_channels, probe_count = refresh_notification_channel_health(
        openclaw_dir,
        store_list_notification_channels(openclaw_dir),
        now,
        force=False,
    )
    channels = {item["id"]: item for item in refreshed_channels}
    existing_deliveries = store_list_notification_deliveries(openclaw_dir, limit=240)
    delivery_map = {(item.get("alertId"), item.get("channelId")): item for item in existing_deliveries}
    rule_map = {item["id"]: item for item in rules}
    existing_alert_map = {
        (item.get("ruleId"), item.get("eventKey")): item
        for item in store_list_automation_alerts(openclaw_dir, limit=256)
    }
    active_keys_by_rule = defaultdict(set)
    triggered = []
    delivery_attempt_count = 0
    delivery_success_count = 0

    for rule in rules:
        if rule.get("status") != "active":
            continue
        trigger_type = rule.get("triggerType")
        threshold_minutes = int(rule.get("thresholdMinutes") or 0)
        match_text = str(rule.get("matchText") or "").strip().lower()
        if trigger_type == "blocked_task_timeout":
            for task in task_index:
                updated_dt = parse_iso(task.get("updatedAt"))
                if not task.get("blocked") or not updated_dt:
                    continue
                age_minutes = int((now - updated_dt).total_seconds() // 60)
                if age_minutes < threshold_minutes:
                    continue
                active_keys_by_rule[rule["id"]].add(task["id"])
                alert = store_upsert_automation_alert(
                    openclaw_dir,
                    {
                        "ruleId": rule["id"],
                        "eventKey": task["id"],
                        "title": f"任务 {task['id']} 已阻塞 {age_minutes} 分钟",
                        "detail": task.get("title") or "需要介入处理的阻塞任务。",
                        "severity": rule.get("severity", "warning"),
                        "status": "open",
                        "sourceType": "task",
                        "sourceId": task["id"],
                        "meta": {"ageMinutes": age_minutes, "triggerType": trigger_type},
                    },
                )
                triggered.append(alert)
        elif trigger_type == "task_incomplete_timeout":
            for task in task_index:
                task_id = str(task.get("id") or "").strip()
                if not task_id:
                    continue
                state = str(task.get("state") or task.get("status") or "").strip().lower()
                if state in TERMINAL_STATES or task.get("output") or task.get("blocked"):
                    continue
                started_at = task_started_reference_at(task)
                if not started_at:
                    continue
                age_minutes = int((now - started_at).total_seconds() // 60)
                if age_minutes < threshold_minutes:
                    continue
                haystack = " ".join(
                    [
                        task_id,
                        str(task.get("title") or "").strip(),
                        str(task.get("teamLabel") or "").strip(),
                        str(task.get("teamId") or "").strip(),
                    ]
                ).lower()
                if match_text and match_text not in haystack:
                    continue
                active_keys_by_rule[rule["id"]].add(task_id)
                existing_alert = existing_alert_map.get((rule["id"], task_id))
                alert_meta = deepcopy((existing_alert or {}).get("meta", {})) if isinstance((existing_alert or {}).get("meta"), dict) else {}
                alert_meta.update(
                    {
                        "ageMinutes": age_minutes,
                        "triggerType": trigger_type,
                        "referenceAt": started_at.isoformat().replace("+00:00", "Z"),
                        "teamId": task_supervision_team_id(task),
                    }
                )
                cooldown_minutes = max(int(rule.get("cooldownMinutes") or 0), 1)
                last_supervision_at = parse_iso(alert_meta.get("lastSupervisionAt"))
                if dispatch_supervision and (not last_supervision_at or now - last_supervision_at >= timedelta(minutes=cooldown_minutes)):
                    team_id = task_supervision_team_id(task)
                    team = resolve_agent_team_record(openclaw_dir, team_id) if team_id else None
                    if team:
                        try:
                            sync_result = perform_task_team_sync(
                                openclaw_dir,
                                task,
                                team=team,
                                note=build_incomplete_task_supervision_note(task, age_minutes, rule),
                                requester_label="监督协调员",
                                router_agent_id=get_router_agent_id(config),
                            )
                            alert_meta["lastSupervisionAt"] = now.isoformat().replace("+00:00", "Z")
                            alert_meta["supervisionCount"] = int(alert_meta.get("supervisionCount") or 0) + 1
                            alert_meta["lastSupervisionThreadId"] = str((sync_result.get("dispatch") or {}).get("threadId") or "").strip()
                            alert_meta["lastSupervisionReplyCount"] = int((sync_result.get("dispatch") or {}).get("replyCount") or 0)
                            alert_meta.pop("lastSupervisionError", None)
                        except Exception as error:
                            alert_meta["lastSupervisionError"] = str(error or "监督同步失败。").strip()
                alert = store_upsert_automation_alert(
                    openclaw_dir,
                    {
                        "ruleId": rule["id"],
                        "eventKey": task_id,
                        "title": f"任务 {task_id} 超时未完成 {age_minutes} 分钟",
                        "detail": task.get("title") or "活跃任务已超出期望完成窗口，需要立即督办。",
                        "severity": rule.get("severity", "warning"),
                        "status": "open",
                        "sourceType": "task",
                        "sourceId": task_id,
                        "meta": alert_meta,
                    },
                )
                existing_alert_map[(rule["id"], task_id)] = alert
                triggered.append(alert)
        elif trigger_type == "critical_task_done":
            for task in task_index:
                updated_dt = parse_iso(task.get("updatedAt"))
                if str(task.get("state", "")).lower() not in TERMINAL_STATES or not updated_dt:
                    continue
                if updated_dt < now - timedelta(days=1):
                    continue
                haystack = f"{task.get('id', '')} {task.get('title', '')}".lower()
                if match_text and match_text not in haystack:
                    continue
                active_keys_by_rule[rule["id"]].add(task["id"])
                alert = store_upsert_automation_alert(
                    openclaw_dir,
                    {
                        "ruleId": rule["id"],
                        "eventKey": task["id"],
                        "title": f"关键任务 {task['id']} 已完成",
                        "detail": task.get("title") or "关键任务完成，建议同步通知。",
                        "severity": rule.get("severity", "critical"),
                        "status": "open",
                        "sourceType": "task",
                        "sourceId": task["id"],
                        "meta": {"triggerType": trigger_type},
                    },
                )
                triggered.append(alert)
        elif trigger_type == "agent_offline":
            for agent in agents:
                if agent.get("status") not in {"idle", "blocked"}:
                    continue
                if not agent.get("lastSeenAt"):
                    continue
                last_seen = parse_iso(agent.get("lastSeenAt"))
                if not last_seen:
                    continue
                age_minutes = int((now - last_seen).total_seconds() // 60)
                if age_minutes < threshold_minutes:
                    continue
                active_keys_by_rule[rule["id"]].add(agent["id"])
                alert = store_upsert_automation_alert(
                    openclaw_dir,
                    {
                        "ruleId": rule["id"],
                        "eventKey": agent["id"],
                        "title": f"Agent {agent['title']} 失去信号 {age_minutes} 分钟",
                        "detail": agent.get("focus") or "最近没有新的工作信号，请确认运行状态。",
                        "severity": rule.get("severity", "warning"),
                        "status": "open",
                        "sourceType": "agent",
                        "sourceId": agent["id"],
                        "meta": {"ageMinutes": age_minutes, "triggerType": trigger_type},
                    },
                )
                triggered.append(alert)

    for rule in rules:
        store_resolve_automation_alerts(openclaw_dir, rule.get("id"), active_keys_by_rule.get(rule.get("id"), set()))

    alerts = store_list_automation_alerts(openclaw_dir, limit=80)
    deliveries = store_list_notification_deliveries(openclaw_dir, limit=200)
    delivery_by_alert = defaultdict(list)
    for delivery in deliveries:
        delivery_by_alert[delivery.get("alertId")].append(delivery)

    for alert in alerts:
        rule = rule_map.get(alert.get("ruleId"))
        if not rule or alert.get("status") == "resolved":
            continue
        escalation = resolve_alert_escalation(rule, alert, now, channels)
        channel_ids = escalation.get("eligibleChannelIds", [])
        any_success = False
        for channel_id in channel_ids:
            pair = (alert.get("id"), channel_id)
            prior_delivery = delivery_map.get(pair)
            cooldown_minutes = int(rule.get("cooldownMinutes") or 0)
            if prior_delivery:
                delivered_at = parse_iso(prior_delivery.get("deliveredAt"))
                within_cooldown = delivered_at and cooldown_minutes > 0 and now - delivered_at < timedelta(minutes=cooldown_minutes)
                if prior_delivery.get("outcome") == "success" and within_cooldown:
                    any_success = True
                    continue
            channel = channels.get(channel_id)
            if not channel:
                continue
            try:
                result = send_notification_message(channel, alert, openclaw_dir=openclaw_dir)
                outcome = "success" if result.get("ok") else "error"
                detail = result.get("detail", "")
            except Exception as error:
                outcome = "error"
                detail = str(error)
            delivery_attempt_count += 1
            delivery = store_save_notification_delivery(
                openclaw_dir,
                alert["id"],
                channel_id,
                outcome,
                detail=detail,
                meta={"channelType": channel.get("type", "")},
            )
            delivery_by_alert[alert.get("id")].append(delivery)
            delivery_map[pair] = delivery
            if outcome == "success":
                any_success = True
                delivery_success_count += 1
        alert_meta = deepcopy(alert.get("meta", {})) if isinstance(alert.get("meta"), dict) else {}
        alert_meta["escalation"] = {
            "ageMinutes": escalation.get("ageMinutes", 0),
            "reachedSteps": escalation.get("reachedSteps", []),
            "currentStep": escalation.get("currentStep"),
            "eligibleChannelIds": channel_ids,
        }
        if alert_meta != (alert.get("meta") or {}):
            alert = store_upsert_automation_alert(
                openclaw_dir,
                {
                    "id": alert["id"],
                    "ruleId": alert["ruleId"],
                    "eventKey": alert["eventKey"],
                    "title": alert["title"],
                    "detail": alert["detail"],
                    "severity": alert["severity"],
                    "status": alert.get("status", "open"),
                    "sourceType": alert.get("sourceType", ""),
                    "sourceId": alert.get("sourceId", ""),
                    "triggeredAt": alert.get("triggeredAt", now_iso()),
                    "meta": alert_meta,
                },
            )
        if any_success and alert.get("status") != "notified":
            store_upsert_automation_alert(
                openclaw_dir,
                {
                    "id": alert["id"],
                    "ruleId": alert["ruleId"],
                    "eventKey": alert["eventKey"],
                    "title": alert["title"],
                    "detail": alert["detail"],
                    "severity": alert["severity"],
                    "status": "notified",
                    "sourceType": alert.get("sourceType", ""),
                    "sourceId": alert.get("sourceId", ""),
                    "triggeredAt": alert.get("triggeredAt", now_iso()),
                    "meta": alert_meta,
                },
            )

    snapshot = build_automation_snapshot(openclaw_dir, rules, channels)
    snapshot["engine"] = {
        "evaluatedAt": now_iso(),
        "triggeredCount": len(triggered),
        "deliveryAttemptCount": delivery_attempt_count,
        "deliverySuccessCount": delivery_success_count,
        "probeCount": probe_count,
    }
    return snapshot


def run_automation_engine_cycle(openclaw_dir, source="manual", now=None):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    started_at = now_utc()
    reference_now = now or started_at
    try:
        config = load_config(openclaw_dir)
        metadata = load_project_metadata(openclaw_dir, config=config)
        ensure_default_management_bootstrap(openclaw_dir, metadata)
        automation_mode = current_management_automation_mode(metadata)
        mode_payload = management_automation_mode_payload(automation_mode)
        auto_reviews_enabled = automation_mode in {MANAGEMENT_AUTOMATION_MODE_ASSISTIVE, MANAGEMENT_AUTOMATION_MODE_FULL_AUTO}
        full_auto_enabled = automation_mode == MANAGEMENT_AUTOMATION_MODE_FULL_AUTO
        agents = load_agents(config)
        tasks = merge_tasks(openclaw_dir, config)
        task_execution_repair = {"count": 0, "items": [], "mode": automation_mode}
        task_execution_sync = {"count": 0, "items": [], "mode": automation_mode}
        task_execution_completion_sync = {"count": 0, "items": [], "mode": automation_mode}
        if full_auto_enabled:
            task_execution_repair = repair_task_execution_backlog(openclaw_dir, config=config, tasks=tasks, now=reference_now)
            task_execution_sync = sync_task_execution_progress_backlog(openclaw_dir, config=config, tasks=tasks)
            task_execution_completion_sync = sync_task_execution_completion_backlog(openclaw_dir, config=config, tasks=tasks)
        if task_execution_repair.get("count") or task_execution_sync.get("count") or task_execution_completion_sync.get("count"):
            tasks = merge_tasks(openclaw_dir, config)
        company_auto_operation = process_company_auto_operation_cycle(
            openclaw_dir,
            tasks,
            reference_now,
            auto_refresh=full_auto_enabled,
            config=config,
        )
        if company_auto_operation.get("refreshedCount"):
            tasks = merge_tasks(openclaw_dir, config)
        runs = store_list_management_runs(openclaw_dir, limit=48)
        should_dispatch_supervision = str(source or "").strip().lower() in {"daemon", "test"}
        automation = evaluate_automation_rules(
            openclaw_dir,
            tasks,
            agents,
            runs,
            reference_now,
            config=config,
            dispatch_supervision=should_dispatch_supervision,
        )
        automation["mode"] = mode_payload
        automation["taskExecutionRepair"] = task_execution_repair
        automation["taskExecutionSync"] = task_execution_sync
        automation["taskExecutionCompletionSync"] = task_execution_completion_sync
        automation["companyAutoOperation"] = company_auto_operation
        current_delivery_analytics = (
            automation.get("deliveryAnalytics") if isinstance(automation.get("deliveryAnalytics"), dict) else {}
        )
        early_finished_at = now_utc()
        early_engine_status = {
            "status": "ok",
            "mode": automation_mode,
            "source": str(source or "manual"),
            "startedAt": started_at.isoformat().replace("+00:00", "Z"),
            "finishedAt": early_finished_at.isoformat().replace("+00:00", "Z"),
            "durationMs": int((early_finished_at - started_at).total_seconds() * 1000),
            "taskCount": len(tasks),
            "agentCount": len(agents),
            "runCount": len(runs),
            "triggeredCount": int((automation.get("engine") or {}).get("triggeredCount") or 0),
            "deliveryAttemptCount": max(
                int((automation.get("engine") or {}).get("deliveryAttemptCount") or 0),
                int(current_delivery_analytics.get("attemptCount") or 0),
            ),
            "deliverySuccessCount": max(
                int((automation.get("engine") or {}).get("deliverySuccessCount") or 0),
                int(current_delivery_analytics.get("successCount") or 0),
            ),
            "probeCount": int((automation.get("engine") or {}).get("probeCount") or 0),
            "dailyReviewCount": 0,
            "weeklyReportCount": 0,
            "manualEscalationCount": int((automation.get("engine") or {}).get("manualEscalationCount") or 0),
            "autoRemediationCount": int((automation.get("engine") or {}).get("autoRemediationCount") or 0),
            "companyAutoOperationCount": int((automation.get("companyAutoOperation") or {}).get("activeCount") or 0),
            "companyAutoOperationRefreshCount": int((automation.get("companyAutoOperation") or {}).get("refreshedCount") or 0),
            "taskExecutionRepairCount": int((automation.get("taskExecutionRepair") or {}).get("count") or 0),
            "taskExecutionSyncCount": int((automation.get("taskExecutionSync") or {}).get("count") or 0),
            "taskExecutionCompletionSyncCount": int((automation.get("taskExecutionCompletionSync") or {}).get("count") or 0),
            "summary": deepcopy(automation.get("summary", {})),
        }
        save_automation_engine_status(openclaw_dir, early_engine_status)
        automation["engine"] = early_engine_status
        if auto_reviews_enabled:
            automation = process_daily_review_push(openclaw_dir, automation, reference_now)
            automation = process_weekly_report_push(openclaw_dir, automation, reference_now)
        else:
            automation["dailyReview"] = {"count": 0, "deliveryAttemptCount": 0, "deliverySuccessCount": 0, "manualEscalationCount": 0}
            automation["weeklyReport"] = {"count": 0, "deliveryAttemptCount": 0, "deliverySuccessCount": 0, "manualEscalationCount": 0}
        initial_engine = automation.get("engine") if isinstance(automation.get("engine"), dict) else {}
        auto_actions = perform_automation_auto_remediation(openclaw_dir, automation) if full_auto_enabled else []
        if auto_actions:
            tasks = merge_tasks(openclaw_dir, config)
            runs = store_list_management_runs(openclaw_dir, limit=48)
            automation = evaluate_automation_rules(
                openclaw_dir,
                tasks,
                agents,
                runs,
                reference_now,
                config=config,
                dispatch_supervision=should_dispatch_supervision,
            )
            automation["mode"] = mode_payload
            automation["companyAutoOperation"] = process_company_auto_operation_cycle(
                openclaw_dir,
                tasks,
                reference_now,
                auto_refresh=full_auto_enabled,
                config=config,
            )
            if auto_reviews_enabled:
                automation = process_daily_review_push(openclaw_dir, automation, reference_now)
                automation = process_weekly_report_push(openclaw_dir, automation, reference_now)
            else:
                automation["dailyReview"] = {"count": 0, "deliveryAttemptCount": 0, "deliverySuccessCount": 0, "manualEscalationCount": 0}
                automation["weeklyReport"] = {"count": 0, "deliveryAttemptCount": 0, "deliverySuccessCount": 0, "manualEscalationCount": 0}
            current_engine = automation.get("engine") if isinstance(automation.get("engine"), dict) else {}
            automation["engine"] = {
                **current_engine,
                "triggeredCount": max(
                    int(current_engine.get("triggeredCount") or 0),
                    int(initial_engine.get("triggeredCount") or 0),
                ),
                "deliveryAttemptCount": int(current_engine.get("deliveryAttemptCount") or 0)
                + int(initial_engine.get("deliveryAttemptCount") or 0),
                "deliverySuccessCount": int(current_engine.get("deliverySuccessCount") or 0)
                + int(initial_engine.get("deliverySuccessCount") or 0),
                "probeCount": max(
                    int(current_engine.get("probeCount") or 0),
                    int(initial_engine.get("probeCount") or 0),
                ),
            }
            automation["remediation"] = {
                "count": len(auto_actions),
                "actions": auto_actions,
            }
        else:
            automation["remediation"] = {"count": 0, "actions": []}
        automation["mode"] = mode_payload
        automation["taskExecutionRepair"] = task_execution_repair
        automation["taskExecutionSync"] = task_execution_sync
        automation["taskExecutionCompletionSync"] = task_execution_completion_sync
        current_delivery_analytics = (
            automation.get("deliveryAnalytics") if isinstance(automation.get("deliveryAnalytics"), dict) else {}
        )
        current_engine = automation.get("engine") if isinstance(automation.get("engine"), dict) else {}
        finished_at = now_utc()
        engine_status = {
            "status": "ok",
            "mode": automation_mode,
            "source": str(source or "manual"),
            "startedAt": started_at.isoformat().replace("+00:00", "Z"),
            "finishedAt": finished_at.isoformat().replace("+00:00", "Z"),
            "durationMs": int((finished_at - started_at).total_seconds() * 1000),
            "taskCount": len(tasks),
            "agentCount": len(agents),
            "runCount": len(runs),
            "triggeredCount": int((automation.get("engine") or {}).get("triggeredCount") or 0),
            "deliveryAttemptCount": max(
                int((automation.get("engine") or {}).get("deliveryAttemptCount") or 0),
                int(current_delivery_analytics.get("attemptCount") or 0),
            ),
            "deliverySuccessCount": max(
                int((automation.get("engine") or {}).get("deliverySuccessCount") or 0),
                int(current_delivery_analytics.get("successCount") or 0),
            ),
            "probeCount": int(current_engine.get("probeCount") or 0),
            "dailyReviewCount": int(current_engine.get("dailyReviewCount") or 0),
            "weeklyReportCount": int(current_engine.get("weeklyReportCount") or 0),
            "manualEscalationCount": max(
                int(current_engine.get("manualEscalationCount") or 0),
                int(initial_engine.get("manualEscalationCount") or 0),
            ),
            "autoRemediationCount": max(
                int((automation.get("remediation") or {}).get("count") or 0),
                int(current_engine.get("autoRemediationCount") or 0),
                int(initial_engine.get("autoRemediationCount") or 0),
            ),
            "companyAutoOperationCount": int((automation.get("companyAutoOperation") or {}).get("activeCount") or 0),
            "companyAutoOperationRefreshCount": int((automation.get("companyAutoOperation") or {}).get("refreshedCount") or 0),
            "taskExecutionRepairCount": int((automation.get("taskExecutionRepair") or {}).get("count") or 0),
            "taskExecutionSyncCount": int((automation.get("taskExecutionSync") or {}).get("count") or 0),
            "taskExecutionCompletionSyncCount": int((automation.get("taskExecutionCompletionSync") or {}).get("count") or 0),
            "summary": deepcopy(automation.get("summary", {})),
        }
        save_automation_engine_status(openclaw_dir, engine_status)
        automation["engine"] = engine_status
        PAYLOAD_CACHE[("management-automation", str(openclaw_dir))] = {"ts": time.time(), "value": deepcopy(automation)}
        return automation
    except Exception as error:
        failed_at = now_utc()
        save_automation_engine_status(
            openclaw_dir,
            {
                "status": "error",
                "source": str(source or "manual"),
                "startedAt": started_at.isoformat().replace("+00:00", "Z"),
                "finishedAt": failed_at.isoformat().replace("+00:00", "Z"),
                "durationMs": int((failed_at - started_at).total_seconds() * 1000),
                "error": str(error),
            },
        )
        raise


def automation_engine_loop(server):
    interval = max(float(getattr(server, "automation_interval", 60.0) or 60.0), 5.0)
    consecutive_failures = 0
    while not server.automation_stop.is_set():
        try:
            run_automation_engine_cycle(server.openclaw_dir, source="daemon")
            try:
                from backend.presentation.http.automation_api import run_custom_automation_cycle

                custom_cycle = run_custom_automation_cycle(server.openclaw_dir)
                if int(custom_cycle.get("triggeredCount") or 0) > 0:
                    publish_live_event(
                        server,
                        "automation_run",
                        {
                            "stage": "schedule_cycle",
                            "at": now_iso(),
                            **custom_cycle,
                        },
                    )
            except Exception as custom_exc:  # noqa: BLE001 - custom cycle should not break the main daemon
                logging.warning("custom automation cycle error: %s", custom_exc)
            consecutive_failures = 0
        except Exception as exc:  # noqa: BLE001 — daemon loop must not crash
            consecutive_failures += 1
            logging.warning("automation engine cycle error (consecutive=%d): %s", consecutive_failures, exc)
            if consecutive_failures >= 3:
                logging.error(
                    "automation engine: %d consecutive failures — last error: %s",
                    consecutive_failures, exc,
                )
                try:
                    save_automation_engine_status(
                        server.openclaw_dir,
                        {
                            "status": "degraded",
                            "source": "daemon",
                            "consecutiveFailures": consecutive_failures,
                            "lastError": str(exc),
                            "at": now_utc().isoformat().replace("+00:00", "Z"),
                        },
                    )
                except Exception:
                    pass
        try:
            computer_use_cycle = run_computer_use_engine_cycle(server.openclaw_dir, source="daemon", limit=2)
            if int(computer_use_cycle.get("processedCount") or 0) > 0 or int(computer_use_cycle.get("failedCount") or 0) > 0:
                publish_live_event(
                    server,
                    "computer_use_run",
                    {
                        "stage": "engine_cycle",
                        "at": now_iso(),
                        **computer_use_cycle,
                    },
                )
                for item in computer_use_cycle.get("items") if isinstance(computer_use_cycle.get("items"), list) else []:
                    run_id = str((item or {}).get("runId") or "").strip()
                    if not run_id:
                        continue
                    try:
                        run_snapshot = build_computer_use_run_snapshot(server.openclaw_dir, run_id)
                        publish_live_event(
                            server,
                            "computer_use_run",
                            {
                                "runId": run_id,
                                "stage": "engine_cycle",
                                "status": str(run_snapshot.get("status") or "").strip(),
                                "objective": str(run_snapshot.get("objective") or "").strip(),
                                "updatedAt": str(run_snapshot.get("updatedAt") or "").strip(),
                                "currentStepId": str(run_snapshot.get("currentStepId") or "").strip(),
                                "currentStepStatus": str(((run_snapshot.get("currentStep") or {}).get("status")) or "").strip(),
                                "currentStepIntent": str(((run_snapshot.get("currentStep") or {}).get("intent")) or "").strip(),
                                "needsHelpReason": str(run_snapshot.get("needsHelpReason") or "").strip(),
                                "clarificationReason": str(run_snapshot.get("clarificationReason") or "").strip(),
                                "counts": {
                                    "artifactCount": int(((run_snapshot.get("artifactSummary") or {}).get("total")) or 0),
                                    "stepCount": int(((run_snapshot.get("stepSummary") or {}).get("total")) or 0),
                                },
                            },
                        )
                        if (item or {}).get("artifactIds"):
                            preview_snapshot = build_computer_use_run_preview_snapshot(server.openclaw_dir, run_id)
                            preview_artifact = preview_snapshot.get("previewArtifact") if isinstance(preview_snapshot.get("previewArtifact"), dict) else {}
                            publish_live_event(
                                server,
                                "computer_use_preview",
                                {
                                    "runId": run_id,
                                    "stage": "engine_cycle",
                                    "status": str(preview_snapshot.get("status") or "").strip(),
                                    "updatedAt": str(preview_snapshot.get("updatedAt") or "").strip(),
                                    "transportMode": str(preview_snapshot.get("transportMode") or "").strip(),
                                    "controllerSessionId": str(preview_snapshot.get("controllerSessionId") or "").strip(),
                                    "controlState": str(preview_snapshot.get("controlState") or "").strip(),
                                    "streamMode": str(preview_snapshot.get("streamMode") or "").strip(),
                                    "streamUrl": str(preview_snapshot.get("streamUrl") or "").strip(),
                                    "vncUrl": str(preview_snapshot.get("vncUrl") or "").strip(),
                                    "capabilities": preview_snapshot.get("capabilities") if isinstance(preview_snapshot.get("capabilities"), list) else [],
                                    "previewKind": str(preview_snapshot.get("previewKind") or "").strip(),
                                    "cursor": preview_snapshot.get("cursor") if isinstance(preview_snapshot.get("cursor"), dict) else {},
                                    "display": preview_snapshot.get("display") if isinstance(preview_snapshot.get("display"), dict) else {},
                                    "displaySize": preview_snapshot.get("displaySize") if isinstance(preview_snapshot.get("displaySize"), dict) else {},
                                    "coordinateSpace": str(preview_snapshot.get("coordinateSpace") or "").strip(),
                                    "captureRect": preview_snapshot.get("captureRect") if isinstance(preview_snapshot.get("captureRect"), dict) else {},
                                    "imageSize": preview_snapshot.get("imageSize") if isinstance(preview_snapshot.get("imageSize"), dict) else {},
                                    "recommendedRefreshMs": int(preview_snapshot.get("recommendedRefreshMs") or 0),
                                    "previewText": str(preview_snapshot.get("previewText") or "").strip()[:4000],
                                    "previewArtifact": {
                                        "id": str(preview_artifact.get("id") or "").strip(),
                                        "title": str(preview_artifact.get("title") or "").strip(),
                                        "type": str(preview_artifact.get("type") or "").strip(),
                                        "mimeType": str(preview_artifact.get("mimeType") or "").strip(),
                                        "updatedAt": str(preview_artifact.get("updatedAt") or "").strip(),
                                        "createdAt": str(preview_artifact.get("createdAt") or "").strip(),
                                        "display": preview_artifact.get("display") if isinstance(preview_artifact.get("display"), dict) else {},
                                        "displaySize": preview_artifact.get("displaySize") if isinstance(preview_artifact.get("displaySize"), dict) else {},
                                    },
                                    "takeover": {
                                        "id": str(((preview_snapshot.get("takeover") or {}).get("id")) or "").strip(),
                                        "startedBy": str(((preview_snapshot.get("takeover") or {}).get("startedBy")) or "").strip(),
                                        "startedAt": str(((preview_snapshot.get("takeover") or {}).get("startedAt")) or "").strip(),
                                        "endedAt": str(((preview_snapshot.get("takeover") or {}).get("endedAt")) or "").strip(),
                                    },
                                    "recentActions": preview_snapshot.get("recentActions") if isinstance(preview_snapshot.get("recentActions"), list) else [],
                                },
                            )
                    except Exception:
                        pass
        except Exception as exc:  # noqa: BLE001 — daemon loop must not crash
            logging.warning("computer use engine cycle error: %s", exc)
        invalidate_dashboard_bundle_cache(server.openclaw_dir, server.output_dir)
        invalidate_management_payload_cache(server.openclaw_dir, include_automation=False)
        invalidate_computer_use_payload_cache(server.openclaw_dir)
        warm_dashboard_bundle_async(server.openclaw_dir, server.output_dir)
        if server.automation_stop.wait(interval):
            break


def start_automation_engine(server):
    stop_event = getattr(server, "automation_stop", None)
    if stop_event is None:
        server.automation_stop = threading.Event()
    worker = threading.Thread(target=automation_engine_loop, args=(server,), daemon=True)
    worker.start()
    return worker
