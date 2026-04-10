#!/usr/bin/env python3
"""Model-assisted task decision adapter for OpenClaw Team.

This adapter prefers reusing the existing OpenClaw runtime via `openclaw agent`
instead of calling provider APIs directly. It can also run in a deterministic
fixture mode for tests.
"""

from __future__ import annotations

import json
import os
import subprocess
from copy import deepcopy
from pathlib import Path

from backend.adapters.storage.dashboard import list_model_provider_configs


DEFAULT_DECISION_MODEL = "glm-5-turbo"
DEFAULT_DECISION_SESSION = "mission-control-model-decision"
DEFAULT_DECISION_TIMEOUT_SECONDS = 12
ADAPTER_MODE_KEYS = (
    "MISSION_CONTROL_MODEL_ADAPTER_MODE",
)
ZHIPU_ENV_KEYS = ("BIGMODEL_API_KEY", "ZHIPUAI_API_KEY", "ZAI_API_KEY")
DECISION_TIMEOUT_KEYS = (
    "MISSION_CONTROL_MODEL_DECISION_TIMEOUT",
)


def _read_env_value(openclaw_dir, key):
    env_path = Path(openclaw_dir) / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key == key:
            return value.strip()
    return ""


def adapter_mode(openclaw_dir):
    for key in ADAPTER_MODE_KEYS:
        value = str(os.environ.get(key) or _read_env_value(openclaw_dir, key) or "").strip().lower()
        if value:
            return value
    return "openclaw"


def decision_timeout_seconds(openclaw_dir):
    for key in DECISION_TIMEOUT_KEYS:
        value = str(os.environ.get(key) or _read_env_value(openclaw_dir, key) or "").strip()
        if not value:
            continue
        try:
            return max(3, min(int(value), 45))
        except ValueError:
            continue
    return DEFAULT_DECISION_TIMEOUT_SECONDS


def openclaw_command_env(openclaw_dir):
    env = os.environ.copy()
    resolved_dir = str(Path(openclaw_dir).expanduser().resolve())
    env["OPENCLAW_STATE_DIR"] = resolved_dir
    env["OPENCLAW_CONFIG_PATH"] = str(Path(resolved_dir) / "openclaw.json")
    try:
        provider_configs = list_model_provider_configs(openclaw_dir)
    except Exception:
        provider_configs = []
    for config in provider_configs:
        if not isinstance(config, dict):
            continue
        if str(config.get("status") or "active").strip().lower() == "disabled":
            continue
        key_value = str(config.get("keyValue") or "").strip()
        if not key_value:
            continue
        env_keys = config.get("envKeys") if isinstance(config.get("envKeys"), list) else []
        for key in env_keys:
            normalized_key = str(key or "").strip()
            if normalized_key and not env.get(normalized_key):
                env[normalized_key] = key_value
    return env


def run_command(args, env=None, timeout=None):
    cmd = [str(arg) for arg in args]
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout or ""
        stderr = error.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="ignore")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="ignore")
        return subprocess.CompletedProcess(
            cmd,
            124,
            stdout=stdout,
            stderr=stderr + f"\nTimed out after {timeout}s",
        )


def join_command_output(process):
    parts = []
    for part in (process.stdout, process.stderr):
        if isinstance(part, bytes):
            part = part.decode("utf-8", errors="ignore")
        if part and str(part).strip():
            parts.append(str(part).strip())
    return "\n".join(parts).strip()


def parse_json_payload(*candidates, default=None):
    decoder = json.JSONDecoder()
    for candidate in candidates:
        if not candidate:
            continue
        text = str(candidate).strip()
        if not text:
            continue
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        for index, char in enumerate(text):
            if char not in "[{":
                continue
            try:
                payload, _end = decoder.raw_decode(text[index:])
                return payload
            except json.JSONDecodeError:
                continue
    return deepcopy(default)


def _agent_label(agent):
    identity = agent.get("identity") if isinstance(agent.get("identity"), dict) else {}
    return str(identity.get("name") or agent.get("title") or agent.get("id") or "").strip()


def _is_zhipu_model(model_name):
    normalized = str(model_name or "").strip().lower()
    return normalized.startswith("zai/") or normalized.startswith("glm") or "/glm" in normalized


def _provider_label(model_name):
    return "Zhipu GLM" if _is_zhipu_model(model_name) else "OpenClaw Runtime"


def _agent_sandbox_mode(agent):
    sandbox = agent.get("sandbox") if isinstance(agent, dict) and isinstance(agent.get("sandbox"), dict) else {}
    return str(sandbox.get("mode") or "").strip().lower()


def _supports_lightweight_decision(agent):
    return _agent_sandbox_mode(agent) != "all"


def select_decision_agent(config, preferred_model=DEFAULT_DECISION_MODEL):
    agents = ((config.get("agents", {}) if isinstance(config, dict) else {}) or {}).get("list", [])
    if not isinstance(agents, list):
        return {}
    normalized_preferred = str(preferred_model or "").strip().lower()
    router = next((item for item in agents if isinstance(item, dict) and item.get("default")), None)
    safe_router = router if isinstance(router, dict) and _supports_lightweight_decision(router) else None
    exact = next(
        (
            item
            for item in agents
            if isinstance(item, dict) and str(item.get("model") or "").strip().lower() == normalized_preferred
        ),
        None,
    )
    provider_match = next(
        (
            item
            for item in agents
            if isinstance(item, dict) and _is_zhipu_model(item.get("model", ""))
        ),
        None,
    )
    safe_exact = next(
        (
            item
            for item in agents
            if isinstance(item, dict)
            and str(item.get("model") or "").strip().lower() == normalized_preferred
            and _supports_lightweight_decision(item)
        ),
        None,
    )
    safe_provider_match = next(
        (
            item
            for item in agents
            if isinstance(item, dict)
            and _is_zhipu_model(item.get("model", ""))
            and _supports_lightweight_decision(item)
        ),
        None,
    )
    briefing = next(
        (
            item
            for item in agents
            if isinstance(item, dict) and str(item.get("id") or "").strip() == "briefing"
        ),
        None,
    )
    safe_briefing = briefing if isinstance(briefing, dict) and _supports_lightweight_decision(briefing) else None
    selected = (
        safe_exact
        or safe_provider_match
        or safe_router
        or safe_briefing
        or exact
        or provider_match
        or briefing
        or router
        or next((item for item in agents if isinstance(item, dict)), None)
    )
    if not isinstance(selected, dict):
        return {}
    model_name = str(selected.get("model") or "").strip()
    return {
        "id": str(selected.get("id") or "").strip(),
        "label": _agent_label(selected) or str(selected.get("id") or "").strip(),
        "model": model_name,
        "providerId": "zhipu" if _is_zhipu_model(model_name) else "openclaw",
        "providerLabel": _provider_label(model_name),
    }


def _load_map(task_index):
    loads = {}
    for task in task_index or []:
        if not isinstance(task, dict) or not task.get("active"):
            continue
        agent_id = str(task.get("currentAgent") or "").strip()
        if not agent_id:
            continue
        loads[agent_id] = loads.get(agent_id, 0) + 1
    return loads


def _compact_agents(agents, router_agent_id, task_index, paused_agent_ids):
    load_map = _load_map(task_index)
    rows = []
    for agent in agents if isinstance(agents, list) else []:
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        rows.append(
            {
                "id": agent_id,
                "title": _agent_label(agent) or agent_id,
                "model": str(agent.get("model") or "").strip(),
                "activeTasks": int(load_map.get(agent_id, 0)),
                "paused": agent_id in set(paused_agent_ids or []),
                "isRouter": agent_id == str(router_agent_id or "").strip(),
            }
        )
    return rows[:12]


def _compact_workflows(workflows):
    rows = []
    for workflow in workflows if isinstance(workflows, list) else []:
        if not isinstance(workflow, dict):
            continue
        meta = workflow.get("meta") if isinstance(workflow.get("meta"), dict) else {}
        rows.append(
            {
                "id": str(workflow.get("id") or "").strip(),
                "name": str(workflow.get("name") or "").strip(),
                "templateId": str(meta.get("templateId") or "").strip(),
                "lanes": [
                    str(lane.get("id") or lane.get("title") or "").strip()
                    for lane in workflow.get("lanes", [])
                    if isinstance(lane, dict)
                ][:6],
            }
        )
    return rows[:8]


def _compact_policies(policies):
    rows = []
    for policy in policies if isinstance(policies, list) else []:
        if not isinstance(policy, dict):
            continue
        rows.append(
            {
                "id": str(policy.get("id") or "").strip(),
                "name": str(policy.get("name") or "").strip(),
                "strategyType": str(policy.get("strategyType") or "").strip(),
                "keyword": str(policy.get("keyword") or "").strip(),
                "targetAgentId": str(policy.get("targetAgentId") or "").strip(),
                "priorityLevel": str(policy.get("priorityLevel") or "").strip(),
            }
        )
    return rows[:10]


def _normalize_lane_hints(value):
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, list):
        items = value
    else:
        items = []
    normalized = []
    for item in items:
        text = str(item or "").strip().lower()
        if not text:
            continue
        if text not in normalized:
            normalized.append(text)
    return normalized[:6]


def _runtime_provider_meta(runtime_meta, selected_agent):
    runtime_provider = str((runtime_meta or {}).get("provider") or "").strip().lower()
    runtime_model = str((runtime_meta or {}).get("model") or "").strip()
    effective_model = runtime_model or str(selected_agent.get("model") or "").strip()
    if runtime_provider in {"zai", "zhipu", "bigmodel"} or _is_zhipu_model(effective_model):
        provider_id = "zhipu"
        provider_label = "Zhipu GLM"
    else:
        provider_id = str(selected_agent.get("providerId") or "openclaw").strip() or "openclaw"
        provider_label = str(selected_agent.get("providerLabel") or "OpenClaw Runtime").strip() or "OpenClaw Runtime"
    return provider_id, provider_label, effective_model


def normalize_model_decision(payload, selected_agent, agents, workflows, router_agent_id, paused_agent_ids, runtime_meta=None):
    valid_agent_ids = {
        str(agent.get("id") or "").strip()
        for agent in agents if isinstance(agents, list) for agent in [agent]
        if isinstance(agent, dict) and str(agent.get("id") or "").strip()
    }
    valid_workflow_ids = {
        str(workflow.get("id") or "").strip()
        for workflow in workflows if isinstance(workflows, list) for workflow in [workflow]
        if isinstance(workflow, dict) and str(workflow.get("id") or "").strip()
    }
    category = str(payload.get("category") or "general").strip() or "general"
    category_label = str(payload.get("categoryLabel") or category).strip() or category
    priority = str(payload.get("priorityLevel") or "normal").strip().lower() or "normal"
    if priority not in {"low", "normal", "high", "critical"}:
        priority = "normal"
    risk_level = str(payload.get("riskLevel") or "good").strip().lower() or "good"
    if risk_level not in {"good", "watch", "high"}:
        risk_level = "watch"
    confidence = payload.get("confidence", 0.0)
    try:
        confidence = max(0.0, min(float(confidence), 0.99))
    except Exception:
        confidence = 0.0
    target_agent_id = str(payload.get("targetAgentId") or "").strip()
    if target_agent_id not in valid_agent_ids or target_agent_id in set(paused_agent_ids or []):
        target_agent_id = ""
    workflow_id = str(payload.get("workflowId") or "").strip()
    if workflow_id not in valid_workflow_ids:
        workflow_id = ""
    suggested_template = str(payload.get("suggestedWorkflowTemplate") or "").strip().lower()
    if not suggested_template and workflow_id:
        for workflow in workflows if isinstance(workflows, list) else []:
            if isinstance(workflow, dict) and str(workflow.get("id") or "").strip() == workflow_id:
                meta = workflow.get("meta") if isinstance(workflow.get("meta"), dict) else {}
                suggested_template = str(meta.get("templateId") or "").strip().lower()
                break
    reason = str(payload.get("reason") or "").strip() or "模型给出了更贴近当前任务语义的建议。"
    trace = [str(item).strip() for item in payload.get("trace", []) if str(item or "").strip()] if isinstance(payload.get("trace"), list) else []
    if not trace:
        trace = [reason]
    provider_id, provider_label, effective_model = _runtime_provider_meta(runtime_meta, selected_agent)
    return {
        "used": True,
        "source": str(payload.get("source") or "openclaw_agent").strip() or "openclaw_agent",
        "sourceLabel": f"{provider_label} · {effective_model}".strip(" ·"),
        "providerId": provider_id,
        "providerLabel": provider_label,
        "agentId": selected_agent.get("id", ""),
        "agentLabel": selected_agent.get("label", ""),
        "model": effective_model,
        "category": category,
        "categoryLabel": category_label,
        "priorityLevel": priority,
        "riskLevel": risk_level,
        "confidence": round(confidence, 2),
        "manualReview": bool(payload.get("manualReview")),
        "suggestedWorkflowTemplate": suggested_template or "delivery",
        "workflowId": workflow_id,
        "targetAgentId": target_agent_id if target_agent_id != str(router_agent_id or "").strip() else "",
        "laneHints": _normalize_lane_hints(payload.get("laneHints")),
        "reason": reason,
        "trace": trace[:8],
    }


def build_task_decision_prompt(title, remark, agents, workflows, policies, router_agent_id, paused_agent_ids):
    payload = {
        "task": {
            "title": str(title or "").strip(),
            "remark": str(remark or "").strip(),
        },
        "routerAgentId": str(router_agent_id or "").strip(),
        "pausedAgents": list(paused_agent_ids or []),
        "agents": _compact_agents(agents, router_agent_id, [], paused_agent_ids),
        "workflows": _compact_workflows(workflows),
        "policies": _compact_policies(policies),
    }
    instructions = {
        "role": "You are a routing copilot for a multi-agent operations system.",
        "output": "Return JSON only. No markdown. No prose outside JSON.",
        "goal": "Classify the task, estimate risk, suggest workflow, and recommend the best target agent.",
        "schema": {
            "category": "string",
            "categoryLabel": "string",
            "priorityLevel": "low|normal|high|critical",
            "riskLevel": "good|watch|high",
            "confidence": "0-1 number",
            "manualReview": "boolean",
            "suggestedWorkflowTemplate": "delivery|incident|growth|other",
            "workflowId": "optional workflow id from workflows",
            "targetAgentId": "optional agent id from agents",
            "laneHints": ["optional lane hints"],
            "reason": "short explanation",
            "trace": ["short reasoning bullets"],
        },
    }
    return (
        "OpenClaw Team model decision request.\n"
        + json.dumps({"instructions": instructions, "context": payload}, ensure_ascii=False)
    )


def extract_agent_response_text(payload):
    result = (payload.get("result", {}) if isinstance(payload, dict) else {}) or {}
    payloads = result.get("payloads", []) if isinstance(result.get("payloads"), list) else []
    texts = [str(item.get("text") or "").strip() for item in payloads if isinstance(item, dict) and str(item.get("text") or "").strip()]
    if texts:
        return "\n\n".join(texts).strip()
    return ""


def extract_runtime_meta(payload):
    result = (payload.get("result", {}) if isinstance(payload, dict) else {}) or {}
    meta = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    agent_meta = meta.get("agentMeta", {}) if isinstance(meta.get("agentMeta"), dict) else {}
    prompt_report = meta.get("systemPromptReport", {}) if isinstance(meta.get("systemPromptReport"), dict) else {}
    return {
        "provider": str(agent_meta.get("provider") or prompt_report.get("provider") or "").strip(),
        "model": str(agent_meta.get("model") or prompt_report.get("model") or "").strip(),
    }


def fixture_task_decision(title, remark, config, agents, router_agent_id):
    source_text = " ".join(part for part in [str(title or "").strip(), str(remark or "").strip()] if part).lower()
    selected = select_decision_agent(config)
    selected = (
        selected
        if selected and selected.get("providerId") == "zhipu"
        else {"id": "briefing", "label": "Briefing", "model": DEFAULT_DECISION_MODEL, "providerId": "zhipu", "providerLabel": "Zhipu GLM"}
    )
    fallback_target_agent_id = next(
        (
            str(agent.get("id") or "").strip()
            for agent in agents if isinstance(agents, list) for agent in [agent]
            if isinstance(agent, dict) and str(agent.get("id") or "").strip() not in {str(router_agent_id or "").strip(), ""}
        ),
        "",
    )
    category = "engineering"
    category_label = "研发实现"
    workflow_template = "delivery"
    lane_hints = ["build", "verify"]
    risk_level = "watch"
    manual_review = False
    target_agent_id = fallback_target_agent_id
    if any(token in source_text for token in ("incident", "故障", "异常", "宕机", "告警")):
        category = "incident"
        category_label = "故障响应"
        workflow_template = "incident"
        lane_hints = ["triage", "recovery"]
        risk_level = "high"
        manual_review = True
        target_agent_id = ""
    elif any(token in source_text for token in ("release", "上线", "发布", "deploy", "ship")):
        category = "release"
        category_label = "发布交付"
        workflow_template = "delivery"
        lane_hints = ["ops", "quality", "build"]
        risk_level = "watch"
        target_agent_id = ""
    elif any(token in source_text for token in ("沟通", "客户", "reply", "message", "协调")):
        category = "communication"
        category_label = "沟通协调"
        workflow_template = "delivery"
        lane_hints = ["intake", "triage"]
        risk_level = "good"
    return normalize_model_decision(
        {
            "source": "fixture",
            "category": category,
            "categoryLabel": category_label,
            "priorityLevel": "high" if "紧急" in source_text or "urgent" in source_text else "normal",
            "riskLevel": risk_level,
            "confidence": 0.86,
            "manualReview": manual_review,
            "suggestedWorkflowTemplate": workflow_template,
            "targetAgentId": target_agent_id,
            "laneHints": lane_hints,
            "reason": "Fixture adapter used GLM-style reasoning via OpenClaw test shim.",
            "trace": [
                "Fixture mode enabled for deterministic model-assisted routing.",
                f"Suggested category {category_label}.",
            ],
        },
        selected or {"id": "", "label": "fixture", "model": DEFAULT_DECISION_MODEL, "providerId": "zhipu", "providerLabel": "Zhipu GLM"},
        agents,
        [],
        router_agent_id,
        [],
    )


def run_task_decision(
    openclaw_dir,
    config,
    title,
    remark="",
    agents=None,
    workflows=None,
    policies=None,
    router_agent_id="",
    paused_agent_ids=None,
    timeout_seconds_override=None,
):
    mode = adapter_mode(openclaw_dir)
    selected_agent = select_decision_agent(config)
    agent_list = agents if isinstance(agents, list) else []
    workflow_list = workflows if isinstance(workflows, list) else []
    paused = list(paused_agent_ids or [])
    if mode == "disabled":
        return {"ok": False, "mode": mode, "reason": "adapter_disabled"}
    if mode == "fixture":
        return {"ok": True, "mode": mode, "decision": fixture_task_decision(title, remark, config, agent_list, router_agent_id)}
    if not selected_agent.get("id"):
        return {"ok": False, "mode": mode, "reason": "no_decision_agent"}
    prompt = build_task_decision_prompt(title, remark, agent_list, workflow_list, policies, router_agent_id, paused)
    env = openclaw_command_env(openclaw_dir)
    if timeout_seconds_override is None:
        timeout_seconds = decision_timeout_seconds(openclaw_dir)
    else:
        timeout_seconds = max(3, min(int(timeout_seconds_override), 45))
    process = run_command(
        [
            "openclaw",
            "agent",
            "--agent",
            selected_agent["id"],
            "--session-id",
            DEFAULT_DECISION_SESSION,
            "--message",
            prompt,
            "--json",
            "--thinking",
            "low",
            "--timeout",
            str(timeout_seconds),
        ],
        env=env,
        timeout=timeout_seconds + 2,
    )
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if process.returncode != 0 or not isinstance(payload, dict):
        return {
            "ok": False,
            "mode": mode,
            "reason": "agent_failed",
            "error": join_command_output(process) or "openclaw agent failed",
            "agent": selected_agent,
        }
    text = extract_agent_response_text(payload)
    decision_payload = parse_json_payload(text, default=None)
    if not isinstance(decision_payload, dict):
        return {
            "ok": False,
            "mode": mode,
            "reason": "invalid_response",
            "error": text or "model decision response was not valid JSON",
            "agent": selected_agent,
        }
    normalized = normalize_model_decision(
        decision_payload,
        selected_agent,
        agent_list,
        workflow_list,
        router_agent_id,
        paused,
        runtime_meta=extract_runtime_meta(payload),
    )
    return {"ok": True, "mode": mode, "decision": normalized}
