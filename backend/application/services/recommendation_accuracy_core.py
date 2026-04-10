from __future__ import annotations

import sys


class _DelegatedSymbol:
    def __init__(self, name):
        self._name = name

    def _resolve(self):
        return getattr(_svc(), self._name)

    def __call__(self, *args, **kwargs):
        return self._resolve()(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._resolve(), attr)

    def __iter__(self):
        return iter(self._resolve())

    def __bool__(self):
        return bool(self._resolve())

    def __len__(self):
        return len(self._resolve())

    def __contains__(self, item):
        return item in self._resolve()

    def __getitem__(self, key):
        return self._resolve()[key]

    def __eq__(self, other):
        return self._resolve() == other

    def __hash__(self):
        return hash(self._resolve())

    def __repr__(self):
        return repr(self._resolve())

    def __str__(self):
        return str(self._resolve())


def _svc():
    module = sys.modules.get("backend.collaboration_dashboard")
    if module is not None:
        return module
    module = sys.modules.get("collaboration_dashboard")
    if module is not None:
        return module
    main = sys.modules.get("__main__")
    if main is not None and str(getattr(main, "__file__", "")).endswith("collaboration_dashboard.py"):
        return main
    import importlib

    try:
        return importlib.import_module("backend.collaboration_dashboard")
    except ModuleNotFoundError:
        return importlib.import_module("collaboration_dashboard")


TERMINAL_STATES = _DelegatedSymbol("TERMINAL_STATES")
build_management_approval_action = _DelegatedSymbol("build_management_approval_action")
build_orchestration_replay = _DelegatedSymbol("build_orchestration_replay")
build_orchestration_linked_review = _DelegatedSymbol("build_orchestration_linked_review")
build_orchestration_policy_trends = _DelegatedSymbol("build_orchestration_policy_trends")
build_orchestration_workflow_review = _DelegatedSymbol("build_orchestration_workflow_review")
build_routing_effectiveness_summary = _DelegatedSymbol("build_routing_effectiveness_summary")
build_task_intelligence_summary = _DelegatedSymbol("build_task_intelligence_summary")
find_workflow_weak_handoff_node = _DelegatedSymbol("find_workflow_weak_handoff_node")
format_age = _DelegatedSymbol("format_age")
load_audit_events = _DelegatedSymbol("load_audit_events")
parse_iso = _DelegatedSymbol("parse_iso")
safe_list = _DelegatedSymbol("safe_list")
store_list_orchestration_workflows = _DelegatedSymbol("store_list_orchestration_workflows")
store_list_routing_decisions = _DelegatedSymbol("store_list_routing_decisions")
store_list_routing_policies = _DelegatedSymbol("store_list_routing_policies")
timedelta = _DelegatedSymbol("timedelta")


def build_recommendation_accuracy_review(openclaw_dir, task_index, now):
    intelligence = build_task_intelligence_summary(task_index)
    routing_decisions = store_list_routing_decisions(openclaw_dir, limit=180)
    decision_quality = build_routing_effectiveness_summary(task_index, routing_decisions)
    workflows = store_list_orchestration_workflows(openclaw_dir)
    routing_policies = store_list_routing_policies(openclaw_dir)
    replays = []
    for task in safe_list(task_index)[:24]:
        if not task.get("id"):
            continue
        replay = build_orchestration_replay(task)
        replay.update(
            {
                "state": task.get("state", ""),
                "updatedAgo": task.get("updatedAgo", ""),
                "route": task.get("route", []),
                "blocked": bool(task.get("blocked")),
            }
        )
        replays.append(replay)
    workflow_review = build_orchestration_workflow_review(workflows, task_index, replays)
    policy_trends = build_orchestration_policy_trends(routing_decisions, task_index)
    linked_review = build_orchestration_linked_review(workflows, routing_policies, policy_trends, workflow_review, task_index)

    confidence_band = "watch"
    avg_confidence = int(round(float(decision_quality.get("avgConfidence") or 0) * 100))
    if avg_confidence >= 75:
        confidence_band = "stable"
    elif avg_confidence >= 85:
        confidence_band = "excellent"
    routing_band = "watch"
    if int(decision_quality.get("blockRate") or 0) < 15 and int(decision_quality.get("completionRate") or 0) >= 75:
        routing_band = "stable"
    if int(decision_quality.get("blockRate") or 0) < 8 and int(decision_quality.get("completionRate") or 0) >= 85:
        routing_band = "excellent"
    linked_band = "watch"
    if int(linked_review["summary"].get("followUpCount") or 0) == 0 and int(linked_review["summary"].get("stabilizedCount") or 0) > 0:
        linked_band = "stable"
    if int(linked_review["summary"].get("followUpCount") or 0) == 0 and int(linked_review["summary"].get("watchCount") or 0) == 0 and int(linked_review["summary"].get("stabilizedCount") or 0) > 0:
        linked_band = "excellent"

    score = max(
        0,
        min(
            100,
            int(
                round(
                    100
                    - min(int(intelligence.get("lowConfidenceCount") or 0) * 6, 24)
                    - min(int(intelligence.get("riskyFallbackCount") or 0) * 8, 24)
                    - min(int(decision_quality.get("blockRate") or 0) * 0.45, 24)
                    - min(int(linked_review["summary"].get("followUpCount") or 0) * 10, 20)
                    + min(int(decision_quality.get("completionRate") or 0) * 0.18, 12)
                )
            ),
        ),
    )

    suggestions = []
    if int(intelligence.get("lowConfidenceCount") or 0) >= 2:
        suggestions.append(
            {
                "title": "低把握任务还偏多",
                "detail": f"当前还有 {intelligence.get('lowConfidenceCount', 0)} 条低把握任务，建议继续补分流条件或前置人工复核。",
                "action": {"type": "open_orchestration", "label": "去流程编排", "path": "/orchestration"},
            }
        )
    if int(decision_quality.get("blockRate") or 0) >= 25:
        suggestions.append(
            {
                "title": "分流收口率还有提升空间",
                "detail": f"当前分流阻塞率 {decision_quality.get('blockRate', 0)}%，建议优先复盘高阻塞策略。",
                "action": {"type": "open_orchestration", "label": "看分流策略", "path": "/orchestration"},
            }
        )
    if int(linked_review["summary"].get("followUpCount") or 0) > 0:
        suggestions.append(
            {
                "title": "联动收口还没完全稳住",
                "detail": f"还有 {linked_review['summary'].get('followUpCount', 0)} 条 workflow 需要同时调整分流和流程。",
                "action": {"type": "open_orchestration", "label": "看联动复盘", "path": "/orchestration"},
            }
        )

    causes = []
    if int(intelligence.get("riskyFallbackCount") or 0) > 0:
        causes.append(
            {
                "title": "默认回退仍然偏多",
                "detail": f"{intelligence.get('riskyFallbackCount', 0)} 条高风险任务还在走默认路由，说明分流规则覆盖还不够。",
                "band": "critical" if int(intelligence.get("riskyFallbackCount") or 0) >= 2 else "watch",
                "action": {"type": "open_orchestration", "label": "补分流规则", "path": "/orchestration"},
            }
        )
    if int(intelligence.get("lowConfidenceCount") or 0) > 0:
        causes.append(
            {
                "title": "任务理解把握还不稳",
                "detail": f"{intelligence.get('lowConfidenceCount', 0)} 条任务判断把握偏低，建议继续补关键词、类型信号或人工复核。",
                "band": "watch",
                "action": {"type": "open_orchestration", "label": "看流程编排", "path": "/orchestration"},
            }
        )
    if int(decision_quality.get("blockRate") or 0) >= 20:
        causes.append(
            {
                "title": "分流决策阻塞率偏高",
                "detail": f"当前分流阻塞率 {decision_quality.get('blockRate', 0)}%，说明策略命中后仍有较多任务卡在执行链路里。",
                "band": "critical" if int(decision_quality.get("blockRate") or 0) >= 35 else "watch",
                "action": {"type": "open_orchestration", "label": "看策略复盘", "path": "/orchestration"},
            }
        )
    if int(linked_review["summary"].get("followUpCount") or 0) > 0:
        causes.append(
            {
                "title": "流程联动还没完全稳住",
                "detail": f"{linked_review['summary'].get('followUpCount', 0)} 条 workflow 在分流和流程两侧仍需同时跟进。",
                "band": "watch",
                "action": {"type": "open_orchestration", "label": "看联动复盘", "path": "/orchestration"},
            }
        )
    if not causes:
        causes.append(
            {
                "title": "当前没有明显偏差来源",
                "detail": "最近这轮推荐准确率没有看到集中的漂移来源，可以继续观察趋势变化。",
                "band": "stable",
            }
        )

    repair_bundles = []
    risky_fallback_tasks = safe_list(intelligence.get("riskyFallbackTasks"))
    low_confidence_tasks = safe_list(intelligence.get("lowConfidenceTasks"))
    manual_review_tasks = safe_list(intelligence.get("manualReviewTasks"))
    if risky_fallback_tasks:
        sample = risky_fallback_tasks[0]
        sample_route = sample.get("routeDecision") if isinstance(sample.get("routeDecision"), dict) else {}
        sample_intelligence = sample_route.get("intelligence") if isinstance(sample_route.get("intelligence"), dict) else {}
        keyword = (
            safe_list(sample_intelligence.get("matchedKeywords"))[:1]
            or safe_list(sample.get("title", "").split())[:1]
        )
        actions = [
            {
                "type": "create_policy",
                "label": "补分流规则",
                "payload": {
                    "name": f"补充 {sample.get('id', '任务')} 分流",
                    "strategyType": "keyword_department",
                    "keyword": (keyword[0] if keyword else "").strip(",.，。 "),
                    "targetAgentId": sample.get("targetAgentId") or sample_route.get("targetAgentId", ""),
                    "priorityLevel": sample_route.get("priorityLevel", "high"),
                    "queueName": "",
                    "status": "active",
                },
            }
        ]
        review_sample = low_confidence_tasks[0] if low_confidence_tasks else (manual_review_tasks[0] if manual_review_tasks else None)
        if review_sample:
            actions.append(
                build_management_approval_action(
                    review_sample,
                    "推荐修复包：默认回退和低把握任务同时偏高，建议补分流并前置人工复核。",
                )
            )
        repair_bundles.append(
            {
                "title": "先收紧高风险回退",
                "detail": f"这一组动作会先补分流规则，再把低把握任务前置人工复核，优先收住高风险默认回退。",
                "actions": actions[:2],
            }
        )
    if int(linked_review["summary"].get("followUpCount") or 0) > 0:
        actions = [{"type": "open_orchestration", "label": "查看联动复盘", "path": "/orchestration"}]
        if low_confidence_tasks:
            actions.append(
                build_management_approval_action(
                    low_confidence_tasks[0],
                    "推荐修复包：联动链路仍需跟进，建议继续把低把握任务前置人工复核。",
                )
            )
        repair_bundles.append(
            {
                "title": "继续收口联动链路",
                "detail": f"当前还有 {linked_review['summary'].get('followUpCount', 0)} 条 workflow 没稳住，这组动作会带你先看联动复盘，再补人工复核兜底。",
                "actions": actions[:2],
            }
        )

    day_buckets = {}
    for offset in range(6, -1, -1):
        day = (now - timedelta(days=offset)).strftime("%m-%d")
        day_buckets[day] = {
            "date": day,
            "total": 0,
            "lowConfidenceCount": 0,
            "manualReviewCount": 0,
            "blockedCount": 0,
            "completedCount": 0,
            "confidenceTotal": 0.0,
            "confidenceCount": 0,
        }
    for task in safe_list(task_index):
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        if not route:
            continue
        updated_at = parse_iso(task.get("updatedAt")) or now
        day = updated_at.strftime("%m-%d")
        if day not in day_buckets:
            continue
        bucket = day_buckets[day]
        bucket["total"] += 1
        confidence = float(route.get("confidence") or (route.get("intelligence") or {}).get("confidence") or 0)
        if confidence:
            bucket["confidenceTotal"] += confidence
            bucket["confidenceCount"] += 1
            if confidence < 0.65:
                bucket["lowConfidenceCount"] += 1
        if bool(route.get("manualReview") or (route.get("intelligence") or {}).get("manualReview")):
            bucket["manualReviewCount"] += 1
        if task.get("blocked"):
            bucket["blockedCount"] += 1
        elif str(task.get("state") or "").lower() in TERMINAL_STATES:
            bucket["completedCount"] += 1
    trend = []
    for bucket in day_buckets.values():
        evaluated = max(bucket["completedCount"] + bucket["blockedCount"], 1)
        trend_score = max(
            0,
            min(
                100,
                int(
                    round(
                        100
                        - min(bucket["lowConfidenceCount"] * 12, 36)
                        - min(int(round((bucket["blockedCount"] / evaluated) * 100)) * 0.4, 24)
                        + min(int(round((bucket["completedCount"] / evaluated) * 100)) * 0.15, 12)
                    )
                ),
            ),
        )
        trend.append(
            {
                "date": bucket["date"],
                "score": trend_score,
                "avgConfidence": int(round((bucket["confidenceTotal"] / bucket["confidenceCount"]) * 100)) if bucket["confidenceCount"] else 0,
                "lowConfidenceCount": bucket["lowConfidenceCount"],
                "manualReviewCount": bucket["manualReviewCount"],
                "blockRate": int(round((bucket["blockedCount"] / evaluated) * 100)) if evaluated else 0,
                "completionRate": int(round((bucket["completedCount"] / evaluated) * 100)) if evaluated else 0,
            }
        )
    trend_direction = "stable"
    if len(trend) >= 2:
        delta = trend[-1]["score"] - trend[0]["score"]
        if delta >= 8:
            trend_direction = "up"
        elif delta <= -8:
            trend_direction = "down"

    bundle_review = build_recommendation_bundle_review(
        openclaw_dir,
        now,
        {
            "score": score,
            "avgConfidence": avg_confidence,
            "lowConfidenceCount": int(intelligence.get("lowConfidenceCount") or 0),
            "manualReviewCount": int(intelligence.get("manualReviewCount") or 0),
            "completionRate": int(decision_quality.get("completionRate") or 0),
            "blockRate": int(decision_quality.get("blockRate") or 0),
            "linkedFollowUpCount": int(linked_review["summary"].get("followUpCount") or 0),
            "riskyFallbackCount": int(intelligence.get("riskyFallbackCount") or 0),
        },
    )
    bundle_follow_ups = build_recommendation_bundle_follow_ups(
        bundle_review,
        risky_fallback_tasks,
        low_confidence_tasks,
        manual_review_tasks,
        linked_review,
    )
    bundle_follow_up_trend = build_recommendation_bundle_follow_up_trend(bundle_review, now)
    bundle_follow_up_breakdown = build_recommendation_bundle_follow_up_breakdown(
        bundle_review,
        now,
        workflows,
        risky_fallback_tasks,
        low_confidence_tasks,
        manual_review_tasks,
        linked_review,
    )
    bundle_priority_queue = build_recommendation_bundle_priority_queue(bundle_follow_up_breakdown)
    bundle_priority_review = build_recommendation_bundle_priority_review(bundle_priority_queue, bundle_follow_up_breakdown)
    bundle_priority_handoff = build_recommendation_bundle_priority_handoff(bundle_priority_queue, bundle_priority_review)
    bundle_operating_summary = build_recommendation_operating_summary(
        bundle_priority_queue,
        bundle_priority_review,
        bundle_priority_handoff,
    )

    return {
        "score": score,
        "summary": {
            "avgConfidence": avg_confidence,
            "lowConfidenceCount": intelligence.get("lowConfidenceCount", 0),
            "manualReviewCount": intelligence.get("manualReviewCount", 0),
            "completionRate": decision_quality.get("completionRate", 0),
            "blockRate": decision_quality.get("blockRate", 0),
            "linkedStableCount": linked_review["summary"].get("stabilizedCount", 0),
            "linkedFollowUpCount": linked_review["summary"].get("followUpCount", 0),
        },
        "bands": {
            "understanding": confidence_band,
            "routing": routing_band,
            "orchestration": linked_band,
        },
        "items": [
            {
                "title": "任务理解",
                "score": avg_confidence,
                "band": confidence_band,
                "detail": f"低把握 {intelligence.get('lowConfidenceCount', 0)} 条 · 人工复核 {intelligence.get('manualReviewCount', 0)} 条",
            },
            {
                "title": "分流决策",
                "score": decision_quality.get("completionRate", 0),
                "band": routing_band,
                "detail": f"收口 {decision_quality.get('completionRate', 0)}% · 阻塞 {decision_quality.get('blockRate', 0)}%",
            },
            {
                "title": "流程联动",
                "score": max(0, 100 - int(linked_review["summary"].get("followUpCount", 0)) * 20),
                "band": linked_band,
                "detail": f"双线趋稳 {linked_review['summary'].get('stabilizedCount', 0)} 条 · 待跟进 {linked_review['summary'].get('followUpCount', 0)} 条",
            },
        ],
        "trend": trend,
        "trendDirection": trend_direction,
        "causes": causes[:4],
        "repairBundles": repair_bundles[:3],
        "bundleReview": bundle_review,
        "bundleFollowUps": bundle_follow_ups[:3],
        "bundleFollowUpTrend": bundle_follow_up_trend,
        "bundleFollowUpBreakdown": bundle_follow_up_breakdown,
        "bundlePriorityQueue": bundle_priority_queue,
        "bundlePriorityReview": bundle_priority_review,
        "bundlePriorityHandoff": bundle_priority_handoff,
        "bundleOperatingSummary": bundle_operating_summary,
        "suggestions": suggestions[:3],
    }


def build_recommendation_bundle_review(openclaw_dir, now, metrics):
    relevant_actions = {
        "orchestration_policy_save": "补分流规则",
        "orchestration_workflow_insert_approval": "前置人工复核",
        "orchestration_workflow_strengthen_handoff": "强化交接模板",
    }
    events = [
        item
        for item in load_audit_events(openclaw_dir, limit=180)
        if item.get("outcome") == "success" and item.get("action") in relevant_actions
    ]
    if not events:
        return {"summary": {"total": 0, "stabilizedCount": 0, "watchCount": 0, "followUpCount": 0}, "rows": []}

    grouped = []
    ordered = sorted(events, key=lambda item: (str(item.get("at") or ""), str(item.get("id") or "")))
    for event in ordered:
        event_at = parse_iso(event.get("at"))
        actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
        actor_name = str(actor.get("displayName") or actor.get("username") or "system").strip()
        if grouped:
            previous = grouped[-1]
            previous_at = previous.get("lastAt")
            if (
                event_at
                and previous_at
                and actor_name == previous.get("actorName")
                and (event_at - previous_at).total_seconds() <= 240
            ):
                previous["events"].append(event)
                previous["lastAt"] = event_at
                continue
        grouped.append(
            {
                "actorName": actor_name,
                "events": [event],
                "lastAt": event_at,
            }
        )

    def _verdict_for(actions):
        actions = set(actions)
        risky_fallback_count = int(metrics.get("riskyFallbackCount") or 0)
        low_confidence_count = int(metrics.get("lowConfidenceCount") or 0)
        manual_review_count = int(metrics.get("manualReviewCount") or 0)
        completion_rate = int(metrics.get("completionRate") or 0)
        block_rate = int(metrics.get("blockRate") or 0)
        linked_follow_up_count = int(metrics.get("linkedFollowUpCount") or 0)
        score = int(metrics.get("score") or 0)

        if "orchestration_policy_save" in actions:
            if risky_fallback_count == 0 and completion_rate >= 78 and block_rate < 18:
                return "stabilized"
            if risky_fallback_count > 0 or block_rate >= 28:
                return "follow_up"
        if "orchestration_workflow_insert_approval" in actions:
            if low_confidence_count <= 1 and manual_review_count <= 1 and completion_rate >= 75:
                return "stabilized"
            if low_confidence_count >= 4 or block_rate >= 30:
                return "follow_up"
        if "orchestration_workflow_strengthen_handoff" in actions:
            if linked_follow_up_count == 0 and block_rate < 18:
                return "stabilized"
            if linked_follow_up_count >= 2:
                return "follow_up"
        if score >= 82 and linked_follow_up_count == 0 and block_rate < 16:
            return "stabilized"
        if score < 68 or linked_follow_up_count > 0 or block_rate >= 25:
            return "follow_up"
        return "watch"

    rows = []
    for group in reversed(grouped[-6:]):
        actions = [str(item.get("action") or "").strip() for item in group["events"]]
        labels = []
        for action in actions:
            label = relevant_actions.get(action)
            if label and label not in labels:
                labels.append(label)
        latest = group["events"][-1]
        latest_at = parse_iso(latest.get("at"))
        verdict = _verdict_for(actions)
        rows.append(
            {
                "title": " + ".join(labels[:3]) if labels else "修复包执行",
                "detail": f"当前综合得分 {int(metrics.get('score') or 0)} · 分流收口 {int(metrics.get('completionRate') or 0)}% · 联动待跟进 {int(metrics.get('linkedFollowUpCount') or 0)} 条",
                "actorName": group.get("actorName") or "system",
                "appliedAt": latest.get("at", ""),
                "appliedAgo": format_age(latest_at, now) if latest_at else "",
                "actionCount": len(group["events"]),
                "actions": labels,
                "verdict": verdict,
            }
        )

    return {
        "summary": {
            "total": len(rows),
            "stabilizedCount": sum(1 for item in rows if item["verdict"] == "stabilized"),
            "watchCount": sum(1 for item in rows if item["verdict"] == "watch"),
            "followUpCount": sum(1 for item in rows if item["verdict"] == "follow_up"),
        },
        "rows": rows,
    }


def build_recommendation_bundle_follow_ups(bundle_review, risky_fallback_tasks, low_confidence_tasks, manual_review_tasks, linked_review):
    rows = safe_list((bundle_review or {}).get("rows"))
    follow_ups = []
    review_task = low_confidence_tasks[0] if low_confidence_tasks else (manual_review_tasks[0] if manual_review_tasks else None)
    for row in rows:
        verdict = str(row.get("verdict") or "").strip()
        if verdict not in {"follow_up", "watch"}:
            continue
        actions = []
        labels = [str(item or "").strip() for item in safe_list(row.get("actions")) if str(item or "").strip()]
        title = str(row.get("title") or "修复包").strip()
        if "补分流规则" in labels and risky_fallback_tasks:
            sample = risky_fallback_tasks[0]
            sample_route = sample.get("routeDecision") if isinstance(sample.get("routeDecision"), dict) else {}
            sample_intelligence = sample_route.get("intelligence") if isinstance(sample_route.get("intelligence"), dict) else {}
            keyword = (
                safe_list(sample_intelligence.get("matchedKeywords"))[:1]
                or safe_list(sample.get("title", "").split())[:1]
            )
            actions.append(
                {
                    "type": "create_policy",
                    "label": "继续补分流规则",
                    "payload": {
                        "name": f"继续收口 {sample.get('id', '任务')} 分流",
                        "strategyType": "keyword_department",
                        "keyword": (keyword[0] if keyword else "").strip(",.，。 "),
                        "targetAgentId": sample.get("targetAgentId") or sample_route.get("targetAgentId", ""),
                        "priorityLevel": sample_route.get("priorityLevel", "high"),
                        "queueName": "",
                        "status": "active",
                    },
                }
            )
        if "前置人工复核" in labels and review_task:
            actions.append(
                build_management_approval_action(
                    review_task,
                    f"{title} 复盘后仍需跟进，继续把低把握任务前置人工复核。",
                )
            )
        if "强化交接模板" in labels or int(linked_review["summary"].get("followUpCount") or 0) > 0:
            actions.append({"type": "open_orchestration", "label": "继续看联动复盘", "path": "/orchestration"})
        if not actions:
            continue
        follow_ups.append(
            {
                "title": f"{title} 还需续修",
                "detail": (
                    "这组修复包执行后还没有完全稳住，建议按顺序继续收紧分流、补人工复核或回到编排页继续收口。"
                    if verdict == "follow_up"
                    else "这组修复包还在观察期，建议先做一轮补强，再继续看趋势变化。"
                ),
                "verdict": verdict,
                "actions": actions[:3],
            }
        )
    return follow_ups


def build_recommendation_bundle_follow_up_trend(bundle_review, now):
    rows = safe_list((bundle_review or {}).get("rows"))
    buckets = {}
    for offset in range(6, -1, -1):
        day = (now - timedelta(days=offset)).strftime("%m-%d")
        buckets[day] = {
            "date": day,
            "stabilizedCount": 0,
            "watchCount": 0,
            "followUpCount": 0,
            "actionCount": 0,
            "score": 70,
        }
    for row in rows:
        applied_at = parse_iso(row.get("appliedAt"))
        if not applied_at:
            continue
        day = applied_at.strftime("%m-%d")
        if day not in buckets:
            continue
        bucket = buckets[day]
        verdict = str(row.get("verdict") or "").strip()
        if verdict == "stabilized":
            bucket["stabilizedCount"] += 1
        elif verdict == "follow_up":
            bucket["followUpCount"] += 1
        else:
            bucket["watchCount"] += 1
        bucket["actionCount"] += int(row.get("actionCount") or 0)
    trend = []
    for bucket in buckets.values():
        score = 70 + bucket["stabilizedCount"] * 12 - bucket["followUpCount"] * 14 - bucket["watchCount"] * 4
        bucket["score"] = max(0, min(100, score))
        trend.append(bucket)
    direction = "stable"
    if len(trend) >= 2:
        delta = int(trend[-1]["score"] or 0) - int(trend[0]["score"] or 0)
        if delta >= 8:
            direction = "up"
        elif delta <= -8:
            direction = "down"
    return {
        "direction": direction,
        "rows": trend,
    }


def build_recommendation_bundle_follow_up_breakdown(bundle_review, now, workflows, risky_fallback_tasks, low_confidence_tasks, manual_review_tasks, linked_review):
    rows = safe_list((bundle_review or {}).get("rows"))
    groups = [
        ("补分流规则", "分流规则"),
        ("前置人工复核", "人工复核"),
        ("强化交接模板", "交接模板"),
    ]
    result = []
    for label, title in groups:
        matched = [
            row for row in rows if label in [str(item or "").strip() for item in safe_list(row.get("actions"))]
        ]
        review_task = low_confidence_tasks[0] if low_confidence_tasks else (manual_review_tasks[0] if manual_review_tasks else None)
        actions = []
        if label == "补分流规则" and risky_fallback_tasks:
            sample = risky_fallback_tasks[0]
            sample_route = sample.get("routeDecision") if isinstance(sample.get("routeDecision"), dict) else {}
            sample_intelligence = sample_route.get("intelligence") if isinstance(sample_route.get("intelligence"), dict) else {}
            keyword = (
                safe_list(sample_intelligence.get("matchedKeywords"))[:1]
                or safe_list(sample.get("title", "").split())[:1]
            )
            actions.append(
                {
                    "type": "create_policy",
                    "label": "补分流规则",
                    "payload": {
                        "name": f"继续收口 {sample.get('id', '任务')} 分流",
                        "strategyType": "keyword_department",
                        "keyword": (keyword[0] if keyword else "").strip(",.，。 "),
                        "targetAgentId": sample.get("targetAgentId") or sample_route.get("targetAgentId", ""),
                        "priorityLevel": sample_route.get("priorityLevel", "high"),
                        "queueName": "",
                        "status": "active",
                    },
                }
            )
        elif label == "前置人工复核" and review_task:
            actions.append(
                build_management_approval_action(
                    review_task,
                    "按原因续修：当前人工复核链路仍需加强，建议继续把低把握任务前置人工复核。",
                )
            )
        elif label == "强化交接模板":
            target_row = next((row for row in safe_list(linked_review.get("rows")) if row.get("overallVerdict") == "follow_up"), None)
            target_workflow_id = str((target_row or {}).get("workflowId") or "").strip()
            target_workflow = next((workflow for workflow in safe_list(workflows) if str(workflow.get("id") or "").strip() == target_workflow_id), None)
            if not target_workflow:
                target_workflow = next((workflow for workflow in safe_list(workflows) if find_workflow_weak_handoff_node(workflow)), None)
            weak_node = find_workflow_weak_handoff_node(target_workflow) if target_workflow else None
            if target_workflow and weak_node:
                actions.append(
                    {
                        "type": "strengthen_handoff_note",
                        "label": "强化交接模板",
                        "payload": {
                            "workflowId": str(target_workflow.get("id") or "").strip(),
                            "nodeId": str(weak_node.get("id") or "").strip(),
                            "title": str(weak_node.get("title") or weak_node.get("name") or "交接节点").strip(),
                            "reason": "按原因续修：交接模板仍然偏弱，建议继续补齐结构化交接清单。",
                        },
                    }
                )
            else:
                actions.append({"type": "open_orchestration", "label": "查看联动复盘", "path": "/orchestration"})
        if not matched:
            result.append(
                {
                    "key": label,
                    "title": title,
                    "direction": "stable",
                    "summary": {"stabilizedCount": 0, "watchCount": 0, "followUpCount": 0, "actionCount": 0},
                    "latest": None,
                    "actions": actions[:2],
                }
            )
            continue
        stabilized_count = sum(1 for row in matched if row.get("verdict") == "stabilized")
        watch_count = sum(1 for row in matched if row.get("verdict") == "watch")
        follow_up_count = sum(1 for row in matched if row.get("verdict") == "follow_up")
        action_count = sum(int(row.get("actionCount") or 0) for row in matched)
        direction = "stable"
        if stabilized_count > follow_up_count and follow_up_count == 0:
            direction = "up"
        elif follow_up_count > 0:
            direction = "down"
        latest = None
        latest_at = None
        for row in matched:
            row_at = parse_iso(row.get("appliedAt"))
            if row_at and (latest_at is None or row_at > latest_at):
                latest_at = row_at
                latest = row
        result.append(
            {
                "key": label,
                "title": title,
                "direction": direction,
                "summary": {
                    "stabilizedCount": stabilized_count,
                    "watchCount": watch_count,
                    "followUpCount": follow_up_count,
                    "actionCount": action_count,
                },
                "latest": {
                    "title": latest.get("title", "") if latest else "",
                    "appliedAgo": format_age(latest_at, now) if latest_at else "",
                    "verdict": latest.get("verdict", "watch") if latest else "watch",
                } if latest else None,
                "actions": actions[:2],
            }
        )
    return result


def build_recommendation_bundle_priority_queue(breakdown):
    items = []
    for item in safe_list(breakdown):
        summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
        follow_up_count = int(summary.get("followUpCount") or 0)
        watch_count = int(summary.get("watchCount") or 0)
        stabilized_count = int(summary.get("stabilizedCount") or 0)
        action_count = int(summary.get("actionCount") or 0)
        direction = str(item.get("direction") or "stable").strip()
        score = follow_up_count * 10 + watch_count * 4 + action_count
        if direction == "down":
            score += 8
        elif direction == "up":
            score -= 6
        priority = "watch"
        if score >= 20 or follow_up_count >= 2:
            priority = "critical"
        elif score <= 0 and stabilized_count > 0:
            priority = "stable"
        title = str(item.get("title") or "续修原因").strip()
        detail = (
            f"{title} 当前待跟进 {follow_up_count} 项，观察中 {watch_count} 项。"
            if priority != "stable"
            else f"{title} 当前已经基本稳住，可以降低优先级。"
        )
        items.append(
            {
                "title": title,
                "priority": priority,
                "score": max(score, 0),
                "detail": detail,
                "actions": safe_list(item.get("actions"))[:2],
            }
        )
    items.sort(
        key=lambda item: (
            0 if item["priority"] == "critical" else 1 if item["priority"] == "watch" else 2,
            -int(item.get("score") or 0),
            item.get("title", ""),
        )
    )
    return items[:3]


def build_recommendation_bundle_priority_review(priority_queue, breakdown):
    breakdown_map = {str(item.get("title") or "").strip(): item for item in safe_list(breakdown)}
    rows = []
    for item in safe_list(priority_queue):
        title = str(item.get("title") or "").strip()
        detail = breakdown_map.get(title) or {}
        latest = detail.get("latest") if isinstance(detail.get("latest"), dict) else {}
        summary = detail.get("summary") if isinstance(detail.get("summary"), dict) else {}
        direction = str(detail.get("direction") or "stable").strip()
        follow_up_count = int(summary.get("followUpCount") or 0)
        verdict = "watch"
        if direction == "up" and follow_up_count == 0:
            verdict = "stabilized"
        elif direction == "down" or follow_up_count > 0:
            verdict = "follow_up"
        rows.append(
            {
                "title": title,
                "priority": str(item.get("priority") or "watch").strip(),
                "verdict": verdict,
                "detail": (
                    f"最近优先处理的是 {title}，当前待跟进 {follow_up_count} 项。"
                    if verdict != "stabilized"
                    else f"{title} 这条优先链路已经基本稳住，可以把注意力转到下一类问题。"
                ),
                "latestAppliedAgo": str(latest.get("appliedAgo") or "").strip(),
            }
        )
    return rows


def build_recommendation_bundle_priority_handoff(priority_queue, priority_review):
    queue = safe_list(priority_queue)
    review_map = {str(item.get("title") or "").strip(): item for item in safe_list(priority_review)}
    if not queue:
        return None
    current = queue[0]
    current_title = str(current.get("title") or "").strip()
    current_review = review_map.get(current_title) or {}
    current_verdict = str(current_review.get("verdict") or "watch").strip()
    if current_verdict != "stabilized":
        return {
            "status": "stay",
            "title": current_title,
            "detail": f"{current_title} 还没稳住，先继续收这一类问题。",
            "actions": safe_list(current.get("actions"))[:2],
        }
    next_item = next(
        (
            item
            for item in queue[1:]
            if str((review_map.get(str(item.get('title') or '').strip()) or {}).get("verdict") or "watch").strip() != "stabilized"
        ),
        None,
    )
    if next_item:
        next_title = str(next_item.get("title") or "").strip()
        return {
            "status": "switch",
            "title": next_title,
            "detail": f"{current_title} 已经基本稳住，建议把下一优先级切到 {next_title}。",
            "actions": safe_list(next_item.get("actions"))[:2],
        }
    return {
        "status": "done",
        "title": current_title,
        "detail": "当前优先队列里的问题都已经基本稳住，可以把注意力转到更长期的优化项。",
        "actions": [],
    }


def build_recommendation_operating_summary(priority_queue, priority_review, priority_handoff):
    queue = safe_list(priority_queue)
    review_map = {str(item.get("title") or "").strip(): item for item in safe_list(priority_review)}
    current = queue[0] if queue else {}
    current_title = str(current.get("title") or "当前无优先项").strip()
    current_priority = str(current.get("priority") or "stable").strip()
    current_review = review_map.get(current_title) or {}
    current_verdict = str(current_review.get("verdict") or "watch").strip()
    handoff = priority_handoff if isinstance(priority_handoff, dict) else {}
    status = "stable"
    if current_priority == "critical" or current_verdict == "follow_up":
        status = "critical"
    elif current_priority == "watch" or current_verdict == "watch":
        status = "watch"
    headline = f"先盯住 {current_title}"
    detail = str(current.get("detail") or "").strip() or "当前没有新的优先续修项。"
    next_title = ""
    if str(handoff.get("status") or "").strip() == "switch":
        next_title = str(handoff.get("title") or "").strip()
    elif str(handoff.get("status") or "").strip() == "done":
        next_title = "长期优化"
    action_bundle = safe_list(current.get("actions"))[:2]
    if not action_bundle and safe_list(handoff.get("actions")):
        action_bundle = safe_list(handoff.get("actions"))[:2]
    return {
        "headline": headline,
        "status": status,
        "currentTitle": current_title,
        "currentPriority": current_priority,
        "currentVerdict": current_verdict,
        "detail": detail,
        "nextTitle": next_title,
        "handoffStatus": str(handoff.get("status") or "").strip() or "stay",
        "handoffDetail": str(handoff.get("detail") or "").strip(),
        "actions": action_bundle,
    }
