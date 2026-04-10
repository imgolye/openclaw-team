from __future__ import annotations

import sys
from copy import deepcopy


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

    def __int__(self):
        return int(self._resolve())

    def __float__(self):
        return float(self._resolve())

    def __index__(self):
        return int(self._resolve())

    def __lt__(self, other):
        return self._resolve() < other

    def __le__(self, other):
        return self._resolve() <= other

    def __gt__(self, other):
        return self._resolve() > other

    def __ge__(self, other):
        return self._resolve() >= other


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


clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
safe_list = _DelegatedSymbol("safe_list")


def summarize_task_execution_text(text, limit=220):
    normalized = str(text or "").strip()
    if not normalized:
        return ""
    for prefix in ("已收到：", "收到："):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :].strip()
    if not normalized:
        return ""
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def compact_task_long_term_memory(memory):
    memory = memory if isinstance(memory, dict) else {}
    recent_notes = []
    seen_recent = set()
    for item in safe_list(memory.get("recentNotes")):
        if not isinstance(item, dict):
            continue
        note = {
            "at": str(item.get("at") or "").strip(),
            "summary": summarize_task_execution_text(item.get("summary") or "", limit=140),
            "focus": summarize_task_execution_text(item.get("focus") or "", limit=96),
            "ownerLabel": str(item.get("ownerLabel") or "").strip(),
        }
        if not any(note.values()):
            continue
        note_key = (note.get("at"), note.get("summary") or note.get("focus"))
        if note_key in seen_recent:
            continue
        seen_recent.add(note_key)
        recent_notes.append(note)
    return {
        "longTermMemory": summarize_task_execution_text(memory.get("longTermMemory") or "", limit=220),
        "learningHighlights": clean_unique_strings(memory.get("learningHighlights") or [])[:4],
        "recentNotes": recent_notes[:4],
        "updatedAt": str(memory.get("updatedAt") or "").strip(),
    }


def compact_auto_operation_profile(profile):
    profile = profile if isinstance(profile, dict) else {}
    if not profile.get("enabled"):
        return {}
    return {
        "enabled": True,
        "taskType": str(profile.get("taskType") or "").strip(),
        "mode": str(profile.get("mode") or "").strip(),
        "objective": str(profile.get("objective") or "").strip(),
        "teamName": str(profile.get("teamName") or "").strip(),
        "focusAreas": clean_unique_strings(profile.get("focusAreas") or [])[:3],
        "operatingCadence": clean_unique_strings(profile.get("operatingCadence") or [])[:3],
        "learningLoop": clean_unique_strings(profile.get("learningLoop") or [])[:4],
        "autonomyRules": clean_unique_strings(profile.get("autonomyRules") or [])[:4],
        "outputContract": str(profile.get("outputContract") or "").strip(),
        "autoStart": bool(profile.get("autoStart")),
    }


def compact_company_auto_operation_runtime(runtime):
    runtime = runtime if isinstance(runtime, dict) else {}
    review_notes = []
    seen_reviews = set()
    for item in safe_list(runtime.get("recentReviewNotes")):
        if not isinstance(item, dict):
            continue
        day_key = str(item.get("dayKey") or "").strip()
        focus = summarize_task_execution_text(item.get("focus") or "", limit=96)
        next_move = summarize_task_execution_text(item.get("nextMove") or "", limit=96)
        summary = summarize_task_execution_text(item.get("summary") or focus or next_move or "", limit=140)
        if not any([day_key, focus, next_move, summary]):
            continue
        review_key = (day_key, summary or focus or next_move)
        if review_key in seen_reviews:
            continue
        seen_reviews.add(review_key)
        review_notes.append(
            {
                "dayKey": day_key,
                "focus": focus,
                "nextMove": next_move,
                "summary": summary,
            }
        )
    return {
        "lastReviewDayKey": str(runtime.get("lastReviewDayKey") or "").strip(),
        "lastReviewedAt": str(runtime.get("lastReviewedAt") or "").strip(),
        "memoryUpdatedAt": str(runtime.get("memoryUpdatedAt") or runtime.get("lastReviewedAt") or "").strip(),
        "currentFocus": summarize_task_execution_text(runtime.get("currentFocus") or "", limit=140),
        "summaryText": summarize_task_execution_text(runtime.get("summaryText") or "", limit=180),
        "nextMove": summarize_task_execution_text(runtime.get("nextMove") or "", limit=160),
        "longTermMemory": summarize_task_execution_text(runtime.get("longTermMemory") or "", limit=220),
        "learningHighlights": clean_unique_strings(runtime.get("learningHighlights") or [])[:4],
        "recentReviewNotes": review_notes[:4],
    }


def compact_chat_thread_pack_reference(pack):
    pack = pack if isinstance(pack, dict) else {}
    return {
        "id": str(pack.get("id") or "").strip(),
        "name": str(pack.get("name") or "").strip(),
        "description": str(pack.get("description") or "").strip(),
        "mode": str(pack.get("mode") or "").strip(),
        "defaultEntry": str(pack.get("defaultEntry") or "").strip(),
        "recommendedTeamId": str(pack.get("recommendedTeamId") or "").strip(),
        "requiredRuntimes": clean_unique_strings(pack.get("requiredRuntimes") or []),
        "skillCount": int(pack.get("skillCount") or 0),
        "hydrationStatus": str(pack.get("hydrationStatus") or "").strip(),
    }


def compact_chat_thread_task_reference(task):
    task = task if isinstance(task, dict) else {}
    return {
        "id": str(task.get("id") or "").strip(),
        "title": str(task.get("title") or "").strip(),
        "status": str(task.get("status") or "").strip(),
        "currentUpdate": str(task.get("currentUpdate") or "").strip(),
        "updatedAt": str(task.get("updatedAt") or "").strip(),
    }


def compact_chat_thread_run_reference(run):
    run = run if isinstance(run, dict) else {}
    return {
        "id": str(run.get("id") or "").strip(),
        "title": str(run.get("title") or "").strip(),
        "status": str(run.get("status") or "").strip(),
        "stage": str(run.get("stage") or "").strip(),
        "updatedAt": str(run.get("updatedAt") or "").strip(),
    }


def compact_chat_thread_team_reference(team):
    team = team if isinstance(team, dict) else {}
    return {
        "id": str(team.get("id") or "").strip(),
        "name": str(team.get("name") or "").strip(),
        "leadAgentId": str(team.get("leadAgentId") or "").strip(),
        "status": str(team.get("status") or "").strip(),
    }


def compact_task_reference(task, include_route=False):
    task = task if isinstance(task, dict) else {}
    compact = {
        "id": str(task.get("id") or "").strip(),
        "title": str(task.get("title") or "").strip(),
        "state": str(task.get("state") or "").strip(),
        "status": str(task.get("status") or "").strip(),
        "activityAt": str(task.get("activityAt") or "").strip(),
        "updatedAt": str(task.get("updatedAt") or "").strip(),
        "teamId": str(task.get("teamId") or "").strip(),
        "currentAgent": str(task.get("currentAgent") or "").strip(),
        "currentAgentLabel": str(task.get("currentAgentLabel") or "").strip(),
        "targetAgentId": str(task.get("targetAgentId") or "").strip(),
        "org": str(task.get("org") or "").strip(),
        "linkedTeam": compact_chat_thread_team_reference(task.get("linkedTeam")),
        "todo": {
            "ratio": (
                (task.get("todo") or {}).get("ratio")
                if isinstance(task.get("todo"), dict)
                else None
            ),
        },
    }
    if include_route:
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        intelligence = route.get("intelligence") if isinstance(route.get("intelligence"), dict) else {}
        model_decision = route.get("modelDecision") if isinstance(route.get("modelDecision"), dict) else {}
        team_selection = route.get("teamSelection") if isinstance(route.get("teamSelection"), dict) else {}
        dispatch_validation = route.get("dispatchValidation") if isinstance(route.get("dispatchValidation"), dict) else {}
        execution_bootstrap = route.get("executionBootstrap") if isinstance(route.get("executionBootstrap"), dict) else {}
        team_assignment = route.get("teamAssignment") if isinstance(route.get("teamAssignment"), dict) else {}
        team_dispatch = route.get("teamDispatch") if isinstance(route.get("teamDispatch"), dict) else {}
        compact_team_selection = {}
        if team_selection:
            compact_team_selection = {
                "selectedTeamId": str(team_selection.get("selectedTeamId") or "").strip(),
                "selectedTeamName": str(team_selection.get("selectedTeamName") or "").strip(),
                "selectedLeadAgentId": str(team_selection.get("selectedLeadAgentId") or "").strip(),
                "selectedExecutionAgentId": str(team_selection.get("selectedExecutionAgentId") or "").strip(),
                "selectedExecutionAgentLabel": str(team_selection.get("selectedExecutionAgentLabel") or "").strip(),
                "requestedLeadAgentId": str(team_selection.get("requestedLeadAgentId") or "").strip(),
                "routedTargetAgentId": str(team_selection.get("routedTargetAgentId") or "").strip(),
                "routedTargetAgentLabel": str(team_selection.get("routedTargetAgentLabel") or "").strip(),
                "recommendedTeamId": str(team_selection.get("recommendedTeamId") or "").strip(),
                "recommendedTeamName": str(team_selection.get("recommendedTeamName") or "").strip(),
                "recommendedConfidence": team_selection.get("recommendedConfidence"),
                "confidence": team_selection.get("confidence"),
                "preferred": bool(team_selection.get("preferred")),
                "manualReviewRecommended": bool(team_selection.get("manualReviewRecommended")),
                "overrideMatchesRecommendation": bool(team_selection.get("overrideMatchesRecommendation")),
                "recommendedDifferent": bool(team_selection.get("recommendedDifferent")),
                "overrideReason": str(team_selection.get("overrideReason") or "").strip(),
                "overrideReasonRequired": bool(team_selection.get("overrideReasonRequired")),
                "overrideReasonMissing": bool(team_selection.get("overrideReasonMissing")),
                "reasons": safe_list(team_selection.get("reasons"))[:4],
                "recommendedReasons": safe_list(team_selection.get("recommendedReasons"))[:3],
                "alternativeTeams": safe_list(team_selection.get("alternativeTeams"))[:3],
            }
        compact_dispatch_validation = {}
        if dispatch_validation:
            compact_dispatch_validation = {
                "blocking": bool(dispatch_validation.get("blocking")),
                "summary": str(dispatch_validation.get("summary") or "").strip(),
                "issues": safe_list(dispatch_validation.get("issues"))[:3],
                "taskKinds": (
                    deepcopy(dispatch_validation.get("taskKinds"))
                    if isinstance(dispatch_validation.get("taskKinds"), dict)
                    else {}
                ),
                "acceptanceObjectPresent": bool(dispatch_validation.get("acceptanceObjectPresent")),
                "requestedLeadAgentId": str(dispatch_validation.get("requestedLeadAgentId") or "").strip(),
                "teamParticipantAgentIds": safe_list(dispatch_validation.get("teamParticipantAgentIds"))[:8],
            }
        compact_execution_bootstrap = {}
        if execution_bootstrap:
            compact_execution_bootstrap = {
                "status": str(execution_bootstrap.get("status") or "").strip(),
                "agentId": str(execution_bootstrap.get("agentId") or "").strip(),
                "agentLabel": str(execution_bootstrap.get("agentLabel") or "").strip(),
                "sessionId": str(execution_bootstrap.get("sessionId") or "").strip(),
                "note": str(execution_bootstrap.get("note") or "").strip(),
                "attempts": execution_bootstrap.get("attempts"),
                "at": str(execution_bootstrap.get("at") or "").strip(),
            }
        compact_team_assignment = {}
        if team_assignment:
            compact_team_assignment = {
                "teamId": str(team_assignment.get("teamId") or "").strip(),
                "teamName": str(team_assignment.get("teamName") or "").strip(),
                "teamLeadAgentId": str(team_assignment.get("teamLeadAgentId") or "").strip(),
                "teamLeadAgentLabel": str(team_assignment.get("teamLeadAgentLabel") or "").strip(),
                "assignedAt": str(team_assignment.get("assignedAt") or "").strip(),
            }
        compact_team_dispatch = {}
        if team_dispatch:
            compact_team_dispatch = {
                key: deepcopy(value)
                for key, value in team_dispatch.items()
                if key != "coordinationProtocol"
            }
        compact["routeDecision"] = {
            "targetAgentId": str(route.get("targetAgentId") or "").strip(),
            "targetAgentLabel": str(route.get("targetAgentLabel") or "").strip(),
            "strategyType": str(route.get("strategyType") or "").strip(),
            "decisionSource": str(route.get("decisionSource") or "").strip(),
            "category": str(route.get("category") or intelligence.get("category") or "").strip(),
            "confidence": route.get("confidence"),
            "manualReview": bool(route.get("manualReview")),
            "fallback": bool(route.get("fallback")),
            "trace": safe_list(route.get("trace"))[:2],
            "modelDecision": {
                "source": str(model_decision.get("source") or "").strip(),
                "sourceLabel": str(model_decision.get("sourceLabel") or "").strip(),
                "confidence": model_decision.get("confidence"),
                "category": str(model_decision.get("category") or "").strip(),
            },
            "intelligence": {
                "confidence": intelligence.get("confidence"),
            },
            "teamSelection": compact_team_selection,
            "dispatchValidation": compact_dispatch_validation,
            "executionBootstrap": compact_execution_bootstrap,
            "teamAssignment": compact_team_assignment,
            "teamDispatch": compact_team_dispatch,
        }
    return compact


def compact_task_run_reference(run):
    run = run if isinstance(run, dict) else {}
    return {
        "id": str(run.get("id") or "").strip(),
        "title": str(run.get("title") or "").strip(),
        "status": str(run.get("status") or "").strip(),
        "stageKey": str(run.get("stageKey") or "").strip(),
        "stageLabel": str(run.get("stageLabel") or "").strip(),
        "riskLevel": str(run.get("riskLevel") or "").strip(),
        "updatedAt": str(run.get("updatedAt") or "").strip(),
        "linkedTeamId": str(run.get("linkedTeamId") or "").strip(),
    }


def compact_task_team_reference(team):
    team = team if isinstance(team, dict) else {}
    return {
        "id": str(team.get("id") or "").strip(),
        "name": str(team.get("name") or "").strip(),
        "status": str(team.get("status") or "").strip(),
        "leadAgentId": str(team.get("leadAgentId") or "").strip(),
        "runtimeState": str(team.get("runtimeState") or "").strip(),
        "defaultDispatchMode": str(team.get("defaultDispatchMode") or "").strip(),
        "linkedTaskIds": clean_unique_strings(team.get("linkedTaskIds") or []),
    }


def compact_task_workflow_binding(binding):
    binding = binding if isinstance(binding, dict) else {}
    selected_branch = binding.get("selectedBranch") if isinstance(binding.get("selectedBranch"), dict) else {}
    return {
        "workflowId": str(binding.get("workflowId") or "").strip(),
        "workflowName": str(binding.get("workflowName") or "").strip(),
        "workflowVersionId": str(binding.get("workflowVersionId") or "").strip(),
        "workflowVersionNumber": binding.get("workflowVersionNumber"),
        "selectionReason": str(binding.get("selectionReason") or "").strip(),
        "selectedBranch": {
            "targetLaneId": str(selected_branch.get("targetLaneId") or "").strip(),
            "targetLaneTitle": str(selected_branch.get("targetLaneTitle") or "").strip(),
            "targetNodeId": str(selected_branch.get("targetNodeId") or "").strip(),
            "targetNodeTitle": str(selected_branch.get("targetNodeTitle") or "").strip(),
        },
    }


def compact_task_planning_bundle(bundle):
    bundle = bundle if isinstance(bundle, dict) else {}
    return {
        "bundleId": str(bundle.get("bundleId") or "").strip(),
        "relativeDir": str(bundle.get("relativeDir") or "").strip(),
        "goal": str(bundle.get("goal") or "").strip(),
    }


def compact_task_model_decision(model_decision):
    model_decision = model_decision if isinstance(model_decision, dict) else {}
    return {
        "used": bool(model_decision.get("used")),
        "source": str(model_decision.get("source") or "").strip(),
        "sourceLabel": str(model_decision.get("sourceLabel") or "").strip(),
        "providerLabel": str(model_decision.get("providerLabel") or "").strip(),
        "model": str(model_decision.get("model") or "").strip(),
    }


def compact_task_intelligence(intelligence):
    intelligence = intelligence if isinstance(intelligence, dict) else {}
    return {
        "confidence": intelligence.get("confidence"),
        "category": str(intelligence.get("category") or "").strip(),
        "categoryLabel": str(intelligence.get("categoryLabel") or "").strip(),
        "riskLevel": str(intelligence.get("riskLevel") or "").strip(),
        "decisionSource": str(intelligence.get("decisionSource") or "").strip(),
        "modelDecision": compact_task_model_decision(intelligence.get("modelDecision")),
    }


def compact_task_team_selection(selection):
    selection = selection if isinstance(selection, dict) else {}
    alternative_teams = []
    for item in safe_list(selection.get("alternativeTeams"))[:2]:
        if not isinstance(item, dict):
            continue
        alternative_teams.append(
            {
                "teamId": str(item.get("teamId") or "").strip(),
                "teamName": str(item.get("teamName") or "").strip(),
                "preferred": bool(item.get("preferred")),
                "score": item.get("score"),
                "historicalCompletionRate": item.get("historicalCompletionRate"),
                "historicalBlockRate": item.get("historicalBlockRate"),
                "historicalRecommendedFrom": str(item.get("historicalRecommendedFrom") or "").strip(),
            }
        )
    return {
        "selectedTeamId": str(selection.get("selectedTeamId") or "").strip(),
        "selectedTeamName": str(selection.get("selectedTeamName") or "").strip(),
        "recommendedTeamId": str(selection.get("recommendedTeamId") or "").strip(),
        "recommendedTeamName": str(selection.get("recommendedTeamName") or "").strip(),
        "selectedExecutionAgentId": str(selection.get("selectedExecutionAgentId") or "").strip(),
        "selectedExecutionAgentLabel": str(selection.get("selectedExecutionAgentLabel") or "").strip(),
        "routedTargetAgentId": str(selection.get("routedTargetAgentId") or "").strip(),
        "preferred": bool(selection.get("preferred")),
        "overrideReason": str(selection.get("overrideReason") or "").strip(),
        "overrideReasonMissing": bool(selection.get("overrideReasonMissing")),
        "manualReviewRecommended": bool(selection.get("manualReviewRecommended")),
        "historicalOverrideCount": int(selection.get("historicalOverrideCount") or 0),
        "historicalOutcomeCount": int(selection.get("historicalOutcomeCount") or 0),
        "historicalCompletionRate": selection.get("historicalCompletionRate"),
        "historicalBlockRate": selection.get("historicalBlockRate"),
        "historicalAvgCompletionMinutes": selection.get("historicalAvgCompletionMinutes"),
        "historicalRecommendedFrom": str(selection.get("historicalRecommendedFrom") or "").strip(),
        "reasons": [
            str(item or "").strip()
            for item in safe_list(selection.get("reasons"))[:2]
            if str(item or "").strip()
        ],
        "alternativeTeams": alternative_teams,
    }


def compact_task_team_dispatch(dispatch):
    dispatch = dispatch if isinstance(dispatch, dict) else {}
    internal_discussion = dispatch.get("internalDiscussion") if isinstance(dispatch.get("internalDiscussion"), dict) else {}
    coordination_relay = dispatch.get("coordinationRelay") if isinstance(dispatch.get("coordinationRelay"), dict) else {}
    collaboration = dispatch.get("collaboration") if isinstance(dispatch.get("collaboration"), dict) else {}
    requested_dispatch_agent_ids = clean_unique_strings(
        dispatch.get("requestedDispatchAgentIds")
        or dispatch.get("dispatchAgentIds")
        or dispatch.get("participantAgentIds")
        or []
    )
    targeted_dispatch_agent_ids = clean_unique_strings(
        dispatch.get("targetedAgentIds")
        or dispatch.get("dispatchAgentIds")
        or requested_dispatch_agent_ids
        or []
    )
    responses = []
    for item in safe_list(dispatch.get("responses"))[:4]:
        if not isinstance(item, dict):
            continue
        responses.append(
            {
                "agentId": str(item.get("agentId") or "").strip(),
                "agentLabel": str(item.get("agentLabel") or item.get("agentId") or "").strip(),
                "replyPreview": summarize_task_execution_text(item.get("replyPreview") or item.get("text") or "", limit=120),
            }
        )
    failed_agents = []
    for item in safe_list(dispatch.get("failedAgents"))[:4]:
        if not isinstance(item, dict):
            continue
        agent_id = str(item.get("agentId") or "").strip()
        if not agent_id:
            continue
        failed_agents.append(
            {
                "agentId": agent_id,
                "agentLabel": str(item.get("agentLabel") or agent_id or "").strip(),
                "error": summarize_task_execution_text(item.get("error") or item.get("reason") or "", limit=120),
            }
        )
    collaboration_requested_agent_ids = clean_unique_strings(
        collaboration.get("requestedDispatchAgentIds")
        or requested_dispatch_agent_ids
        or []
    )
    collaboration_targeted_agent_ids = clean_unique_strings(
        collaboration.get("targetedAgentIds")
        or targeted_dispatch_agent_ids
        or []
    )
    collaboration_responded_agent_ids = clean_unique_strings(
        collaboration.get("respondedAgentIds")
        or [item.get("agentId") for item in responses if item.get("agentId")]
        or []
    )
    collaboration_waiting_agent_ids = clean_unique_strings(collaboration.get("waitingAgentIds") or [])
    collaboration_committed_agent_ids = clean_unique_strings(collaboration.get("committedAgentIds") or [])
    collaboration_standby_agent_ids = clean_unique_strings(collaboration.get("standbyAgentIds") or [])
    collaboration_blocker_agent_ids = clean_unique_strings(collaboration.get("blockerAgentIds") or [])
    collaboration_failed_agent_ids = clean_unique_strings(
        collaboration.get("failedAgentIds")
        or [item.get("agentId") for item in failed_agents if item.get("agentId")]
        or []
    )
    collaboration_member_count = int(
        collaboration.get("memberCount")
        or len(collaboration_targeted_agent_ids)
        or len(collaboration_requested_agent_ids)
        or len(collaboration_responded_agent_ids)
    )
    collaboration_response_count = int(
        collaboration.get("responseCount")
        or len(collaboration_responded_agent_ids)
        or len(responses)
    )
    collaboration_waiting_count = int(
        collaboration.get("waitingCount")
        or max(
            collaboration_member_count - len(collaboration_responded_agent_ids) - len(collaboration_failed_agent_ids),
            0,
        )
    )
    return {
        "syncType": str(dispatch.get("syncType") or "").strip(),
        "summaryText": summarize_task_execution_text(dispatch.get("summaryText") or "", limit=220),
        "requestedDispatchAgentIds": requested_dispatch_agent_ids,
        "dispatchAgentIds": clean_unique_strings(dispatch.get("dispatchAgentIds") or []),
        "targetedAgentIds": targeted_dispatch_agent_ids,
        "responses": [item for item in responses if item.get("agentId")],
        "failedAgents": failed_agents,
        "internalDiscussion": {
            "enabled": bool(internal_discussion.get("enabled")),
            "discussionAgentIds": clean_unique_strings(internal_discussion.get("discussionAgentIds") or []),
            "advisorAgentIds": clean_unique_strings(internal_discussion.get("advisorAgentIds") or []),
            "executionAgentIds": clean_unique_strings(internal_discussion.get("executionAgentIds") or []),
            "summary": summarize_task_execution_text(internal_discussion.get("summary") or "", limit=180),
            "replyCount": int(internal_discussion.get("replyCount") or 0),
        },
        "coordinationRelay": {
            "sent": bool(coordination_relay.get("sent")),
            "replyCount": int(coordination_relay.get("replyCount") or 0),
            "targetAgentIds": clean_unique_strings(coordination_relay.get("targetAgentIds") or []),
        },
        "collaboration": {
            "status": str(collaboration.get("status") or "").strip(),
            "headline": summarize_task_execution_text(collaboration.get("headline") or "", limit=120),
            "memberCount": collaboration_member_count,
            "responseCount": collaboration_response_count,
            "waitingCount": collaboration_waiting_count,
            "committedCount": int(collaboration.get("committedCount") or len(collaboration_committed_agent_ids)),
            "standbyCount": int(collaboration.get("standbyCount") or len(collaboration_standby_agent_ids)),
            "blockerCount": int(collaboration.get("blockerCount") or len(collaboration_blocker_agent_ids)),
            "failureCount": int(collaboration.get("failureCount") or len(collaboration_failed_agent_ids)),
            "relaySent": bool(collaboration.get("relaySent") or coordination_relay.get("sent")),
            "relayReplyCount": int(collaboration.get("relayReplyCount") or coordination_relay.get("replyCount") or 0),
            "requestedDispatchAgentIds": collaboration_requested_agent_ids,
            "targetedAgentIds": collaboration_targeted_agent_ids,
            "respondedAgentIds": collaboration_responded_agent_ids,
            "waitingAgentIds": collaboration_waiting_agent_ids,
            "committedAgentIds": collaboration_committed_agent_ids,
            "standbyAgentIds": collaboration_standby_agent_ids,
            "blockerAgentIds": collaboration_blocker_agent_ids,
            "failedAgentIds": collaboration_failed_agent_ids,
        },
    }


def compact_task_route_decision(route):
    route = route if isinstance(route, dict) else {}
    team_assignment = route.get("teamAssignment") if isinstance(route.get("teamAssignment"), dict) else {}
    dispatch_validation = route.get("dispatchValidation") if isinstance(route.get("dispatchValidation"), dict) else {}
    auto_operation_profile = compact_auto_operation_profile(route.get("autoOperationProfile"))
    auto_operation_runtime = compact_company_auto_operation_runtime(route.get("autoOperationRuntime"))
    task_long_term_memory = compact_task_long_term_memory(route.get("taskLongTermMemory"))
    execution_bootstrap = route.get("executionBootstrap") if isinstance(route.get("executionBootstrap"), dict) else {}
    return {
        "strategyType": str(route.get("strategyType") or "").strip(),
        "matchedKeyword": str(route.get("matchedKeyword") or "").strip(),
        "targetAgentId": str(route.get("targetAgentId") or "").strip(),
        "targetAgentLabel": str(route.get("targetAgentLabel") or "").strip(),
        "priorityLevel": str(route.get("priorityLevel") or "").strip(),
        "reason": str(route.get("reason") or "").strip(),
        "fallback": bool(route.get("fallback")),
        "decidedAt": str(route.get("decidedAt") or "").strip(),
        "decisionSource": str(route.get("decisionSource") or "").strip(),
        "manualReview": bool(route.get("manualReview")),
        "confidence": route.get("confidence"),
        "category": str(route.get("category") or "").strip(),
        "categoryLabel": str(route.get("categoryLabel") or "").strip(),
        "riskLevel": str(route.get("riskLevel") or "").strip(),
        "suggestedWorkflowTemplate": str(route.get("suggestedWorkflowTemplate") or "").strip(),
        "teamId": str(route.get("teamId") or "").strip(),
        "teamName": str(route.get("teamName") or "").strip(),
        "intelligence": compact_task_intelligence(route.get("intelligence")),
        "modelDecision": compact_task_model_decision(route.get("modelDecision")),
        "teamSelection": compact_task_team_selection(route.get("teamSelection")),
        "teamAssignment": {
            "teamId": str(team_assignment.get("teamId") or "").strip(),
            "teamName": str(team_assignment.get("teamName") or "").strip(),
        },
        "dispatchValidation": {
            "blocking": bool(dispatch_validation.get("blocking")),
            "summary": str(dispatch_validation.get("summary") or "").strip(),
            "issues": safe_list(dispatch_validation.get("issues"))[:3],
            "taskKinds": (
                deepcopy(dispatch_validation.get("taskKinds"))
                if isinstance(dispatch_validation.get("taskKinds"), dict)
                else {}
            ),
            "acceptanceObjectPresent": bool(dispatch_validation.get("acceptanceObjectPresent")),
            "requestedLeadAgentId": str(dispatch_validation.get("requestedLeadAgentId") or "").strip(),
            "teamParticipantAgentIds": clean_unique_strings(dispatch_validation.get("teamParticipantAgentIds") or []),
        },
        "executionBootstrap": {
            "status": str(execution_bootstrap.get("status") or "").strip(),
            "agentId": str(execution_bootstrap.get("agentId") or "").strip(),
            "agentLabel": str(execution_bootstrap.get("agentLabel") or "").strip(),
            "note": str(execution_bootstrap.get("note") or "").strip(),
            "attempts": execution_bootstrap.get("attempts"),
        },
        "teamDispatch": compact_task_team_dispatch(route.get("teamDispatch")),
        "autoOperationProfile": auto_operation_profile,
        "autoOperationRuntime": auto_operation_runtime,
        "taskLongTermMemory": task_long_term_memory,
        "trace": [
            summarize_task_execution_text(item or "", limit=120)
            for item in safe_list(route.get("trace"))[:2]
            if str(item or "").strip()
        ],
    }


def compact_chat_thread_deliverable_reference(deliverable):
    deliverable = deliverable if isinstance(deliverable, dict) else {}
    return {
        "id": str(deliverable.get("id") or "").strip(),
        "title": str(deliverable.get("title") or "").strip(),
        "status": str(deliverable.get("status") or "").strip(),
        "updatedAt": str(deliverable.get("updatedAt") or "").strip(),
    }


def compact_chat_thread_summary_payload(thread):
    summary = deepcopy(thread) if isinstance(thread, dict) else {}
    meta = summary.get("meta") if isinstance(summary.get("meta"), dict) else {}
    task_execution = meta.get("taskExecution") if isinstance(meta.get("taskExecution"), dict) else {}
    summary["linkedPack"] = compact_chat_thread_pack_reference(summary.get("linkedPack"))
    summary["linkedTask"] = compact_chat_thread_task_reference(summary.get("linkedTask"))
    summary["linkedRun"] = compact_chat_thread_run_reference(summary.get("linkedRun"))
    summary["linkedTeam"] = compact_chat_thread_team_reference(summary.get("linkedTeam"))
    summary["linkedTeams"] = [
        compact_chat_thread_team_reference(item)
        for item in safe_list(summary.get("linkedTeams"))
        if isinstance(item, dict)
    ]
    summary["managedTaskThread"] = bool(
        str(task_execution.get("managedBy") or "").strip() == "mission-control"
        and str(task_execution.get("source") or "").strip() == "task_dispatch"
    )
    summary["participantAgentCount"] = len(safe_list(summary.get("participantAgentIds"))) or len(safe_list(summary.get("participantAgents")))
    summary["participantHumanCount"] = len(safe_list(summary.get("participantHumans")))
    summary["agentId"] = str(summary.get("agentId") or "").strip()
    summary["sessionId"] = str(summary.get("sessionId") or "").strip()
    summary["conversationKey"] = str(summary.get("conversationKey") or "").strip()
    summary["talkable"] = bool(summary.get("talkable"))
    summary["eligible"] = bool(summary.get("eligible"))
    for key in (
        "reviewGates",
        "artifactTemplates",
        "runtimePolicy",
        "linkedDeliverable",
        "meta",
        "lastDispatch",
        "participantAgents",
        "participantHumans",
    ):
        summary.pop(key, None)
    for key in ("linkedPack", "linkedTask", "linkedRun", "linkedTeam"):
        value = summary.get(key)
        if isinstance(value, dict) and not any(str(item or "").strip() for item in value.values() if not isinstance(item, list)):
            if not any(value.values()):
                summary.pop(key, None)
    if not safe_list(summary.get("linkedTeams")):
        summary.pop("linkedTeams", None)
    return summary


def compact_agent_team_payload(team):
    team = deepcopy(team) if isinstance(team, dict) else {}
    team["linkedTasks"] = [
        compact_task_reference(task)
        for task in safe_list(team.get("linkedTasks"))
        if isinstance(task, dict)
    ]
    for key in ("meta", "coordinationProtocol"):
        team.pop(key, None)
    return team


def compact_task_dashboard_payload(task):
    task = deepcopy(task) if isinstance(task, dict) else {}
    task["linkedRun"] = compact_task_run_reference(task.get("linkedRun"))
    task["linkedTeam"] = compact_task_team_reference(task.get("linkedTeam"))
    task["routeDecision"] = compact_task_route_decision(task.get("routeDecision"))
    task["workflowBinding"] = compact_task_workflow_binding(task.get("workflowBinding"))
    task["planningBundle"] = compact_task_planning_bundle(task.get("planningBundle"))
    task["replayCount"] = len(safe_list(task.get("replay")))
    task.pop("replay", None)
    task.pop("linkedRuns", None)
    task.pop("linkedRunIds", None)
    return task
