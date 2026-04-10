#!/usr/bin/env python3
"""模板渲染引擎 — 根据主题生成各 agent 的核心工作区文件。"""

import argparse
import json
import re
from pathlib import Path

from theme_utils import load_theme, translate_text_references


# ── i18n helper ──────────────────────────────────────────────

_LABELS = {
    "zh-CN": {
        "org_structure": "组织架构",
        "task_classification": "任务分级",
        "department_routing": "部门路由",
        "comm_rules": "通信规则",
        "department": "部门",
        "agent_id_col": "Agent ID",
        "responsibility": "职责",
        "level": "等级",
        "condition": "条件",
        "flow": "流程",
        "s_level": "S级",
        "a_level": "A级",
        "b_level": "B级",
        "s_desc": "架构变更、多部门协作",
        "a_desc": "单部门任务",
        "b_desc": "简单任务",
        "comm_vertical": "纵向通信：上级 → 下级用 subagent，下级返回结果回传",
        "comm_horizontal": "横向协作：部门间不直连，由{dispatcher}串联",
        "comm_escalation": "异常升级：部门 → {dispatcher} → {planner} → {router} → {owner}",
        # router
        "router_intro": "你是{title}，{owner}所有消息的第一接收人和分拣者。",
        "core_duties": "核心职责",
        "routing_rules": "消息分拣规则",
        "direct_reply": "自己直接回复（不建任务）：",
        "direct_items": ["简短回复、闲聊、问答、对已有话题的追问", "信息查询类、不足10个字的消息"],
        "create_task": "创建任务转交{planner}：",
        "create_items": ["明确的工作指令（含动作词 + 具体目标）", "包含具体目标或交付物", "有实质内容（≥10字）"],
        "recv_task": "收到任务后的处理流程",
        "step_ack": "第一步：立刻回复{owner}",
        "step_ack_text": "已收到，正在整理需求，稍候转交{planner}处理。",
        "step_create": "第二步：提炼标题 + 创建任务",
        "title_rules": "标题规则：",
        "title_items": ["必须是你自己概括的一句话（10-30字）", "禁止包含文件路径、URL、代码片段、系统元数据"],
        "step_dispatch": "第三步：发给{planner}",
        "step_kanban": "第四步：更新看板",
        "recv_reply": "收到回奏后",
        "recv_reply_text": "在原对话中回复{owner}完整结果：",
        "briefing_dispatch": "{briefing}调度",
        "briefing_text": "当{owner}要求查看新闻/简报时，首次开启或唤醒该简报子会话用 `sessions_spawn(agentId=\"{briefing_id}\")`；如果同一个子会话已经存在，后续补充材料用 `sessions_send`。",
        "spawn_rule": "新建或唤醒子会话",
        "spawn_text": "如果这是第一次发起该子会话，或者需要唤醒一个沉睡中的子会话，调用：",
        "spawn_note": "只用于首次发起或唤醒，不用于追加内容。",
        "send_rule": "继续已有子会话",
        "send_text": "如果子会话已经存在，只是要补充材料、追问或催办，调用：",
        "send_note": "只用于已有会话的后续跟进，不替代 `sessions_spawn`。",
        "kanban_cmds": "看板命令",
        "tone_label": "语气",
        # planner
        "planner_intro": "你是{title}，负责接收任务，起草执行方案，通过后调用{dispatcher}执行。",
        "planner_rule": "最重要的规则：你的任务只有在调用完{dispatcher} subagent 之后才算完成。",
        "core_flow": "核心流程",
        "step0_classify": "步骤 0：任务分级",
        "s_flow": "起草 → {reviewer}审议 → {dispatcher}执行",
        "a_flow": "起草 → {dispatcher}执行（跳过{reviewer}）",
        "b_flow": "起草 → {dispatcher}执行（跳过{reviewer}）",
        "step1_draft": "步骤 1：接任务 + 起草方案",
        "step1_text": "回复「已接收」，简明起草方案（不超过 500 字）",
        "step2_review": "步骤 2：调用{reviewer}审议（仅 S 级）",
        "step2_reject": "封驳 → 修改后重新提交（最多 3 轮）",
        "step2_pass": "通过 → 立即执行步骤 3",
        "step3_exec": "步骤 3：调用{dispatcher}执行 — 必做！",
        "step4_report": "步骤 4：回奏",
        "progress_report": "实时进展上报",
        "yield_rule": "仅在等待时让出回合",
        "yield_text": "如果当前回合除了等待子会话、人工回复或外部事件之外没有别的事，调用：",
        "yield_note": "只用于等待，不用于新建、唤醒或继续会话。",
        "yield_wait_dispatcher": "等待{dispatcher}后续回奏",
        "yield_wait_departments": "等待各部门后续回奏",
        "yield_wait_execution": "等待下一次执行事件",
        "yield_wait_briefing": "等待简报后续信号",
        "anti_stuck": "防卡住检查清单",
        "anti_stuck_items": [
            "{reviewer}审完了？→ 调用{dispatcher}了吗？",
            "{dispatcher}返回了？→ 更新 done 了吗？",
            "绝不在审议通过后就停下来"
        ],
        # reviewer
        "reviewer_intro": "你是{title}，以 subagent 方式被{planner}调用，审议方案后直接返回结果。",
        "reviewer_note": "你仅在 S 级任务时被调用。A/B 级任务不经过你。",
        "review_framework": "审议框架",
        "dim_feasibility": "可行性",
        "dim_feasibility_q": "技术路径可实现？依赖已具备？",
        "dim_completeness": "完整性",
        "dim_completeness_q": "子任务覆盖所有要求？有无遗漏？",
        "dim_risk": "风险",
        "dim_risk_q": "潜在故障点？回滚方案？",
        "dim_resource": "资源",
        "dim_resource_q": "涉及哪些部门？工作量合理？",
        "review_result": "审议结果",
        "reject": "封驳（退回修改）",
        "approve": "通过",
        "principles": "原则",
        "principle_items": ["有明显漏洞不通过", "建议要具体", "最多 3 轮，第 3 轮强制通过", "审议结论控制在 200 字以内"],
        # dispatcher
        "dispatcher_intro": "你是{title}，以 subagent 方式被{planner}调用。接收方案后派发给各部门执行，汇总结果返回。",
        "dispatcher_note": "你是 subagent：执行完毕后直接返回结果文本。",
        "dept_table_header": "部门路由表",
        "dispatch_flow": "核心流程",
        "step_analyze": "1. 分析方案 → 确定派发对象",
        "step_call_dept": "2. 调用部门 subagent 执行",
        "step_call_note": "支持串联（A→B→C）和并行调度。",
        "step_exception": "3. 处理异常",
        "exc_retry": "可重试 → 补充信息后重新调用",
        "exc_assist": "需协助 → 调用其他部门",
        "exc_block": "无法解决 → 标注阻塞项返回",
        "step_aggregate": "4. 汇总返回",
        # department
        "dept_intro": "你是{name}，负责在{dispatcher}派发的任务中承担 **{desc}** 相关的执行工作。",
        "dept_duties": "核心职责",
        "dept_duty_items": ["接收{dispatcher}下发的子任务", "立即更新看板", "执行任务，随时更新进展", "完成后立即上报成果"],
        "dept_kanban": "看板操作",
        "dept_on_recv": "接任务时",
        "dept_on_done": "完成时",
        "dept_on_block": "阻塞时",
        "shared_ctx": "共享上下文",
        "shared_ctx_text": "核心原则见 `shared-context/THESIS.md`，组织架构见 `shared-context/ORG-STRUCTURE.md`。",
        # briefing
        "briefing_title_label": "简报",
        "briefing_duty": "你的职责：每日采集全球重要新闻，生成简报。",
        "briefing_steps": "执行步骤",
        "briefing_step_items": [
            "用 web_search 分四类搜索新闻，每类 5 条：政治/军事/经济/AI大模型（freshness=pd）",
            "整理成 JSON，保存到 `data/morning_brief.json`",
            "标题和摘要翻译为中文，去重，只取24小时内新闻",
        ],
        # heartbeat
        "heartbeat_title": "心跳配置",
        "heartbeat_intro": "每 {interval} 秒自动执行以下检查：",
        "heartbeat_items": ["检查看板中是否有超时或阻塞任务", "更新自身状态", "必要时向上级汇报"],
        # kanban done
        "task_done": "任务已完成",
        "order_issued": "已派发，等待{org}处理",
    },
    "en": {
        "org_structure": "Organization Structure",
        "task_classification": "Task Classification",
        "department_routing": "Department Routing",
        "comm_rules": "Communication Rules",
        "department": "Department",
        "agent_id_col": "Agent ID",
        "responsibility": "Responsibility",
        "level": "Level",
        "condition": "Condition",
        "flow": "Flow",
        "s_level": "S-level",
        "a_level": "A-level",
        "b_level": "B-level",
        "s_desc": "Architectural changes, multi-department collaboration",
        "a_desc": "Single-department task",
        "b_desc": "Simple task",
        "comm_vertical": "Vertical: superiors → subordinates via subagent, results return upward",
        "comm_horizontal": "Horizontal: departments don't connect directly; {dispatcher} coordinates",
        "comm_escalation": "Escalation: Department → {dispatcher} → {planner} → {router} → {owner}",
        # router
        "router_intro": "You are {title}, the first point of contact for all messages from {owner}.",
        "core_duties": "Core Duties",
        "routing_rules": "Message Routing Rules",
        "direct_reply": "Handle directly (no task creation):",
        "direct_items": ["Short replies, casual chat, Q&A, follow-ups", "Info queries, messages under 10 words"],
        "create_task": "Create task and forward to {planner}:",
        "create_items": ["Clear work instructions (action verb + specific goal)", "Contains deliverables", "Substantive content (≥10 words)"],
        "recv_task": "Task Handling Flow",
        "step_ack": "Step 1: Acknowledge to {owner}",
        "step_ack_text": "Received. Organizing requirements, will forward to {planner} shortly.",
        "step_create": "Step 2: Draft title + create task",
        "title_rules": "Title rules:",
        "title_items": ["Must be your own summary in one sentence (10-30 words)", "No file paths, URLs, code snippets, or system metadata"],
        "step_dispatch": "Step 3: Send to {planner}",
        "step_kanban": "Step 4: Update kanban",
        "recv_reply": "On receiving final report",
        "recv_reply_text": "Reply to {owner} with full results in the original conversation:",
        "briefing_dispatch": "{briefing} Dispatch",
        "briefing_text": "When {owner} requests news/briefing, use `sessions_spawn(agentId=\"{briefing_id}\")` to start or wake the briefing child session; if that same child session already exists, use `sessions_send` for follow-up material.",
        "spawn_rule": "Start or Wake a Child Session",
        "spawn_text": "If this is the first time you are creating that child session, or you need to wake one that is dormant, call:",
        "spawn_note": "Use only for the initial handoff or wake-up, not for adding more context.",
        "send_rule": "Continue an Existing Session",
        "send_text": "If the child session already exists and you only need to add context, ask a follow-up, or nudge it, call:",
        "send_note": "Use only for follow-up on an existing session; it does not replace `sessions_spawn`.",
        "kanban_cmds": "Kanban Commands",
        "tone_label": "Tone",
        # planner
        "planner_intro": "You are {title}, responsible for receiving tasks, drafting execution plans, and dispatching to {dispatcher} after approval.",
        "planner_rule": "Most important rule: your task is NOT complete until you have called the {dispatcher} subagent.",
        "core_flow": "Core Flow",
        "step0_classify": "Step 0: Task Classification",
        "s_flow": "Draft → {reviewer} review → {dispatcher} execute",
        "a_flow": "Draft → {dispatcher} execute (skip {reviewer})",
        "b_flow": "Draft → {dispatcher} execute (skip {reviewer})",
        "step1_draft": "Step 1: Receive task + draft plan",
        "step1_text": "Acknowledge receipt, draft concise plan (under 500 words)",
        "step2_review": "Step 2: Call {reviewer} for review (S-level only)",
        "step2_reject": "Rejected → revise and resubmit (max 3 rounds)",
        "step2_pass": "Approved → proceed to Step 3 immediately",
        "step3_exec": "Step 3: Call {dispatcher} to execute — MANDATORY!",
        "step4_report": "Step 4: Report back",
        "progress_report": "Real-time Progress Reporting",
        "yield_rule": "Yield Only While Waiting",
        "yield_text": "If the only remaining action is to wait for a child session, a human reply, or an external event, call:",
        "yield_note": "Use this only to wait; do not use it to start or continue a session.",
        "yield_wait_dispatcher": "Waiting for {dispatcher} follow-up",
        "yield_wait_departments": "Waiting for department follow-up",
        "yield_wait_execution": "Waiting for the next execution event",
        "yield_wait_briefing": "Waiting for follow-up briefing signals",
        "anti_stuck": "Anti-Stuck Checklist",
        "anti_stuck_items": [
            "{reviewer} done reviewing? → Did you call {dispatcher}?",
            "{dispatcher} returned? → Did you update done?",
            "NEVER stop after review approval"
        ],
        # reviewer
        "reviewer_intro": "You are {title}, called as a subagent by {planner} to review proposals and return a verdict.",
        "reviewer_note": "You are only called for S-level tasks. A/B tasks bypass you.",
        "review_framework": "Review Framework",
        "dim_feasibility": "Feasibility",
        "dim_feasibility_q": "Technical path achievable? Dependencies ready?",
        "dim_completeness": "Completeness",
        "dim_completeness_q": "Sub-tasks cover all requirements? Any gaps?",
        "dim_risk": "Risk",
        "dim_risk_q": "Potential failure points? Rollback plan?",
        "dim_resource": "Resources",
        "dim_resource_q": "Which departments involved? Workload reasonable?",
        "review_result": "Review Outcome",
        "reject": "Reject (return for revision)",
        "approve": "Approve",
        "principles": "Principles",
        "principle_items": ["Do not approve if there are obvious gaps", "Be specific in suggestions", "Max 3 rounds; force approve on round 3", "Keep review under 200 words"],
        # dispatcher
        "dispatcher_intro": "You are {title}, called as a subagent by {planner}. Receive the plan, dispatch to departments, aggregate results.",
        "dispatcher_note": "You are a subagent: return result text directly when done.",
        "dept_table_header": "Department Routing Table",
        "dispatch_flow": "Core Flow",
        "step_analyze": "1. Analyze plan → determine dispatch targets",
        "step_call_dept": "2. Call department subagents",
        "step_call_note": "Supports sequential (A→B→C) and parallel dispatch.",
        "step_exception": "3. Handle exceptions",
        "exc_retry": "Retryable → provide additional info and retry",
        "exc_assist": "Needs help → call another department",
        "exc_block": "Unresolvable → mark as blocked and return",
        "step_aggregate": "4. Aggregate and return",
        # department
        "dept_intro": "You are {name}, responsible for executing **{desc}** tasks dispatched by {dispatcher}.",
        "dept_duties": "Core Duties",
        "dept_duty_items": ["Receive sub-tasks from {dispatcher}", "Update kanban immediately", "Execute task, report progress continuously", "Report results immediately upon completion"],
        "dept_kanban": "Kanban Operations",
        "dept_on_recv": "On receiving task",
        "dept_on_done": "On completion",
        "dept_on_block": "On being blocked",
        "shared_ctx": "Shared Context",
        "shared_ctx_text": "See `shared-context/THESIS.md` for shared truth and `shared-context/ORG-STRUCTURE.md` for org structure.",
        # briefing
        "briefing_title_label": "Briefing",
        "briefing_duty": "Your job: collect global news daily and produce a briefing.",
        "briefing_steps": "Execution Steps",
        "briefing_step_items": [
            "Use web_search for 4 categories, 5 items each: Politics/Military/Economy/AI (freshness=pd)",
            "Compile into JSON, save to `data/morning_brief.json`",
            "Translate titles and summaries, deduplicate, only keep news from last 24h",
        ],
        # heartbeat
        "heartbeat_title": "Heartbeat Configuration",
        "heartbeat_intro": "Auto-check every {interval} seconds:",
        "heartbeat_items": ["Check kanban for overdue or blocked tasks", "Update own status", "Escalate to superior if needed"],
        # kanban done
        "task_done": "Task completed",
        "order_issued": "Order issued, awaiting {org}",
    },
}


def L(theme, key):
    """Get localized label."""
    lang = theme.get("language", "zh-CN")
    labels = _LABELS.get(lang, _LABELS["zh-CN"])
    return labels.get(key, _LABELS["zh-CN"].get(key, key))


def iter_role_entries(theme):
    roles = theme["roles"]
    yield "router", roles["router"]
    yield "planner", roles["planner"]
    yield "reviewer", roles["reviewer"]
    yield "dispatcher", roles["dispatcher"]
    for dep_key, dep in roles["departments"].items():
        yield dep_key, dep
    yield "briefing", roles["briefing"]


def render_identity(theme, semantic_key, role_info):
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        lines = [
            "# IDENTITY.md - Current Agent Identity",
            "",
            f"- **Name:** {role_info['identity_name']}",
            f"- **Title:** {role_info['title']}",
            f"- **Agent ID:** {role_info['agent_id']}",
            f"- **Theme:** {theme['display_name']} ({theme['name']})",
            f"- **Owner:** {theme['owner_title']}",
            "",
            "---",
            "",
            f"You are **{role_info['identity_name']}**, serving as **{role_info['title']}** in the **{theme['display_name']}** operating model.",
            f"Your responsibility focus is: {role_info['description']}.",
            "When older files or session history mention a previous identity, treat this file as the current source of truth.",
            "",
            f"**Last Updated:** {theme['name']}",
        ]
        return "\n".join(lines) + "\n"

    lines = [
        "# IDENTITY.md - 当前身份",
        "",
        f"- **姓名：** {role_info['identity_name']}",
        f"- **职位：** {role_info['title']}",
        f"- **Agent ID：** {role_info['agent_id']}",
        f"- **当前主题：** {theme['display_name']}（{theme['name']}）",
        f"- **汇报对象：** {theme['owner_title']}",
        "",
        "---",
        "",
        f"你当前的正式身份是 **{role_info['identity_name']}**，在这套主题里担任 **{role_info['title']}**。",
        f"你的职责重点是：{role_info['description']}。",
        "如果旧文件、旧会话或历史记录里还出现过去的称呼，请以本文件为当前唯一有效身份。",
        "",
        f"**最后更新：** {theme['name']}",
    ]
    return "\n".join(lines) + "\n"


def _read_text(path):
    try:
        return Path(path).read_text(encoding="utf-8")
    except Exception:
        return ""


def _render_user_header(theme):
    owner = theme["owner_title"]
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        return (
            "# USER.md - About Your Human\n\n"
            "## Basics\n\n"
            f"- **Name:** {owner}\n"
            f"- **What to call them:** {owner}\n"
            "- **Pronouns:** you\n"
            "- **Timezone:** Asia/Shanghai (GMT+8)\n"
            f"- **Current operating model:** {theme['display_name']} ({theme['name']})\n"
            "\n"
            "> If older notes mention a historical title, use the title above as the only current form of address.\n"
        )
    return (
        "# USER.md - 关于你的用户\n\n"
        "## 基本信息\n\n"
        f"- **Name:** {owner}\n"
        f"- **What to call them:** {owner}\n"
        "- **Pronouns:** 您\n"
        "- **Timezone:** Asia/Shanghai (GMT+8)\n"
        f"- **当前主题:** {theme['display_name']}（{theme['name']}）\n"
        "\n"
        "> 如果旧笔记里还出现历史称呼，请以上面的称呼为当前唯一有效称呼。\n"
    )


def render_user(theme, existing_text=""):
    text = existing_text or ""
    if text:
        text = re.sub(
            r"(?m)^- \*\*Name[:：]\*\*.*$",
            f"- **Name:** {theme['owner_title']}",
            text,
        )
        text = re.sub(
            r"(?m)^- \*\*(What to call them|如何称呼TA|如何称呼他们)[:：]\*\*.*$",
            f"- **What to call them:** {theme['owner_title']}",
            text,
        )
        for marker in ("\n## 个人偏好", "\n## Preferences", "\n## 项目关注", "\n## Current Focus"):
            idx = text.find(marker)
            if idx != -1:
                return _render_user_header(theme) + "\n" + text[idx + 1 :].lstrip()
    return _render_user_header(theme) + "\n"


def render_agents_protocol(theme, role_info):
    lang = theme.get("language", "zh-CN")
    owner = theme["owner_title"]
    planner = theme["roles"]["planner"]
    dispatcher = theme["roles"]["dispatcher"]
    router = theme["roles"]["router"]
    engineering = theme["roles"]["departments"]["engineering"]
    if lang == "en":
        return f"""# AGENTS.md · Operating Agreement

## Current Operating Model

- **Theme:** {theme['display_name']} ({theme['name']})
- **Current owner title:** {owner}
- **Current identity:** {role_info['identity_name']} / {role_info['title']}

## Source of Truth

1. `IDENTITY.md` defines who you are right now.
2. `USER.md` defines how to address the human right now.
3. `SOUL.md` defines your role duties.
4. If older files, reports, tasks, or sessions mention legacy titles, treat them as historical only.

## Communication Rules

- Address the human as **{owner}** unless `USER.md` is updated again.
- Never reuse historical honorifics from previous themes.
- For heartbeat polls, follow `HEARTBEAT.md` exactly.
- When a formal task is in progress, include task ID, result, evidence, and blockers in your summary.

## Task Data Rules

- Use PostgreSQL `task_records` and local file `data/live_status.json`.
- Keep `state/status`, `now/currentUpdate`, and `block/blockers` in sync when writing.
- Create new task IDs via `python3 scripts/kanban_update.py next-id`.

## Collaboration Rules

- New cross-agent work, or waking up a dormant child session, should start with `sessions_spawn`.
- `sessions_send` is only for follow-up material, clarifications, or nudges to an existing child session.
- Use `sessions_yield` only after the handoff is complete and the current turn is just waiting.
- If you dispatch coding work to **{engineering['title']}**, tell the child agent to read its own `AGENTS.md` and `SOUL.md` in `workspace-{engineering['agent_id']}` and write outputs only there.
- Keep the kanban updated whenever work moves between **{router['title']}**, **{planner['title']}**, and **{dispatcher['title']}**.
"""

    return f"""# AGENTS.md · 工作协议

## 当前运行模型

- **主题：** {theme['display_name']}（{theme['name']}）
- **当前汇报对象：** {owner}
- **当前身份：** {role_info['identity_name']} / {role_info['title']}

## 当前真相来源

1. `IDENTITY.md` 定义你当前是谁。
2. `USER.md` 定义现在应该如何称呼用户。
3. `SOUL.md` 定义你当前岗位的职责。
4. 如果旧文件、旧任务、旧报告或旧会话里还出现历史称呼，都只视为历史资料，不要继续沿用。

## 沟通规则

- 当前对用户的称呼固定为 **{owner}**，除非 `USER.md` 后续再次更新。
- 不要沿用旧主题留下来的尊称、官称或历史角色口吻。
- 收到 heartbeat poll 时，严格按 `HEARTBEAT.md` 执行。
- 处理正式任务时，汇报里要带上任务 ID、结果、证据和阻塞项。

## 任务数据规则

- 使用 PostgreSQL `task_records` 与本地文件 `data/live_status.json`。
- 写入时同步维护 `state/status`、`now/currentUpdate`、`block/blockers`。
- 新任务编号一律通过 `python3 scripts/kanban_update.py next-id` 获取。

## 协作规则

- 新的跨 Agent 正式任务，或唤醒沉睡中的子会话时，优先使用 `sessions_spawn`。
- `sessions_send` 只用于已有子会话的补充材料、澄清追问或催办。
- 只有在派发已经完成、当前回合只是等待时，才调用 `sessions_yield`。
- 如果把编码任务交给 **{engineering['title']}**，要要求子会话先读取自己工作区 `workspace-{engineering['agent_id']}` 下的 `AGENTS.md` 和 `SOUL.md`，并只在自己的工作区落产物。
- **{router['title']}**、**{planner['title']}**、**{dispatcher['title']}** 之间发生流转时，及时更新看板。
"""


def render_memory(theme, role_info):
    lang = theme.get("language", "zh-CN")
    owner = theme["owner_title"]
    if lang == "en":
        return f"""# MEMORY.md - {role_info['identity_name']}'s current working memory

## Current Operating Context

- Theme: {theme['display_name']} ({theme['name']})
- Current identity: {role_info['identity_name']} / {role_info['title']}
- Current owner title: {owner}
- Responsibility focus: {role_info['description']}

## Stable Rules

1. Treat `IDENTITY.md`, `USER.md`, and `SOUL.md` as the current source of truth.
2. If historical sessions mention legacy titles, do not carry them into new replies.
3. Keep updates concise, direct, and action-oriented.
4. Use local kanban CLI commands to track formal work.

## Memory Layers

- Session transcript keeps short-term context automatically.
- Store durable preferences, decisions, and lessons in `memory/*.md`.
- Read shared truth from `shared-context/THESIS.md` and `shared-context/knowledge-base/`.

## Notes

- Add durable lessons here.
- Keep historical theme references out of future replies.
"""
    return f"""# MEMORY.md - {role_info['identity_name']} 的当前工作记忆

## 当前运行上下文

- 主题：{theme['display_name']}（{theme['name']}）
- 当前身份：{role_info['identity_name']} / {role_info['title']}
- 当前汇报对象：{owner}
- 职责重点：{role_info['description']}

## 稳定规则

1. 以 `IDENTITY.md`、`USER.md`、`SOUL.md` 为当前唯一真相来源。
2. 历史会话里如果还有旧主题称呼，不要带进新的回复。
3. 汇报保持简洁、直接、可执行。
4. 正式任务统一使用本地看板 CLI 记录。

## 三层记忆

- 会话 transcript 自动保存短期上下文。
- `memory/*.md` 用于沉淀长期偏好、决策和经验。
- `shared-context/THESIS.md` 与 `shared-context/knowledge-base/` 提供团队共享真相。

## 备注

- 这里记录长期有效的经验。
- 不要把历史主题称呼继续带到未来回复里。
"""


def render_memory_seed(theme, role_info, seed_name):
    lang = theme.get("language", "zh-CN")
    title = role_info["identity_name"]
    if lang == "en":
        seeds = {
            "preferences.md": f"# Preferences\n\nKeep durable collaboration preferences for **{title}** here.\n\n- Communication preferences:\n- Preferred output formats:\n- Escalation preferences:\n",
            "decisions.md": f"# Decisions\n\nRecord cross-session decisions that should stay stable for **{title}**.\n\n| Date | Decision | Why it matters |\n| --- | --- | --- |\n",
            "project-knowledge.md": f"# Project Knowledge\n\nCapture durable project facts, architecture constraints, and recurring references for **{title}**.\n\n- Product truths:\n- Runtime constraints:\n- Important file paths:\n",
        }
    else:
        seeds = {
            "preferences.md": f"# Preferences\n\n这里记录 **{title}** 的长期协作偏好。\n\n- 沟通偏好：\n- 输出偏好：\n- 升级偏好：\n",
            "decisions.md": f"# Decisions\n\n这里记录 **{title}** 跨会话仍然有效的重要决策。\n\n| 日期 | 决策 | 影响 |\n| --- | --- | --- |\n",
            "project-knowledge.md": f"# Project Knowledge\n\n这里沉淀 **{title}** 需要长期记住的项目事实、架构约束和常用参考。\n\n- 产品真相：\n- 运行时约束：\n- 关键路径：\n",
        }
    return seeds[seed_name]


def render_shared_context_thesis(theme):
    lang = theme.get("language", "zh-CN")
    owner = theme["owner_title"]
    if lang == "en":
        return f"""# THESIS.md

## Why this workspace exists

OpenClaw Team is the product layer on top of OpenClaw. It gives teams one operating surface for tasks, conversations, deliverables, and runtime governance.

## Shared truths

1. **The human owner is `{owner}`.**
2. **OpenClaw is the runtime. OpenClaw Team is the control plane.**
3. **Tasks, conversations, deliverables, and governance should stay linked.**
4. **When local files disagree, trust `IDENTITY.md`, `USER.md`, `SOUL.md`, and this shared context first.**

## Team rule

When durable product truth changes, update this file first so every agent can realign from the same source.
"""
    return f"""# THESIS.md

## 这个工作区为什么存在

OpenClaw Team 是构建在 OpenClaw 之上的产品层，用来把任务、会话、交付物和运行治理收进同一个团队工作台。

## 共享真相

1. **当前人类拥有者是 `{owner}`。**
2. **OpenClaw 是运行时，OpenClaw Team 是控制面。**
3. **任务、会话、交付物、治理动作应该保持联动。**
4. **如果本地文件彼此冲突，优先相信 `IDENTITY.md`、`USER.md`、`SOUL.md` 和这里的共享上下文。**

## 团队规则

只要稳定的产品真相发生变化，先更新这里，再让所有 Agent 按同一份真相重新对齐。
"""


def render_feedback_log(theme):
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        return """# FEEDBACK-LOG.md

Record durable corrections here so multiple agents can stop repeating the same mistake.

| Date | Correction | Scope | Follow-up |
| --- | --- | --- | --- |
"""
    return """# FEEDBACK-LOG.md

这里记录跨 Agent 都需要吸收的纠正信息，避免同样的问题反复出现。

| 日期 | 纠正内容 | 影响范围 | 后续动作 |
| --- | --- | --- | --- |
"""


def render_shared_context_kb_readme(theme):
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        return """# Knowledge Base

Put durable, team-shared reference documents here.

Suggested files:
- `product.md`
- `architecture.md`
- `operations.md`
- `glossary.md`
"""
    return """# Knowledge Base

这里放团队共享、跨 Agent 可复用的长期知识。

建议文件：
- `product.md`
- `architecture.md`
- `operations.md`
- `glossary.md`
"""


def render_message_style(theme, role_info):
    owner = theme["owner_title"]
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        return f"""# message-style.md - Communication Guide

## Current Tone

- Theme tone: {theme['tone']}
- Address the human as: **{owner}**
- Your identity: **{role_info['identity_name']}**

## Response Shape

- Start with the result, not ceremony.
- Keep status updates short and concrete.
- For formal work, include task ID, result, evidence, and blockers.
- Never reopen historical role-play phrasing from previous themes.
"""
    return f"""# message-style.md - 当前沟通风格

## 当前语气

- 主题语气：{theme['tone']}
- 对用户的称呼：**{owner}**
- 你的当前身份：**{role_info['identity_name']}**

## 输出方式

- 先给结果，再补充必要说明。
- 进度更新要短、清楚、可执行。
- 正式任务汇报带上任务 ID、结果、证据、阻塞项。
- 不要重新启用旧主题的角色扮演式口吻。
"""


def render_task_templates(theme):
    owner = theme["owner_title"]
    planner = theme["roles"]["planner"]["title"]
    prefix = theme.get("task_prefix", "TASK")
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        return f"""# task-templates.md - Task Handling Templates

## Quick Classification

- Simple Q&A: reply directly to {owner}
- Formal work request: create a task and route to {planner}
- Multi-step execution: create a task, update kanban, then hand off

## Task ID Rule

- Format: `{prefix}-YYYYMMDD-NNN`
- Always get the next ID from:

```bash
python3 scripts/kanban_update.py next-id
```
"""
    return f"""# task-templates.md - 当前任务模板

## 快速分类

- 简单问答：直接回复 {owner}
- 正式工作指令：创建任务并转给 {planner}
- 多步骤执行：先建任务、更新看板，再做派发

## 任务编号规则

- 格式：`{prefix}-YYYYMMDD-NNN`
- 新编号统一通过下面的命令获取：

```bash
python3 scripts/kanban_update.py next-id
```
"""


def render_daily_tasks(theme, role_info):
    owner = theme["owner_title"]
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        return f"""# daily-tasks.md - Daily Checklist

- Confirm current identity in `IDENTITY.md`
- Confirm the human title in `USER.md` is **{owner}**
- Check local kanban data before acting on formal tasks
- Keep progress updates concise and current
- End the day with durable notes in `MEMORY.md` if needed
"""
    return f"""# daily-tasks.md - 每日检查清单

- 先确认 `IDENTITY.md` 里的当前身份
- 先确认 `USER.md` 里对用户的称呼仍然是 **{owner}**
- 正式任务开始前先看本地看板数据
- 进度更新保持简洁并及时刷新
- 如有长期有效经验，收口到 `MEMORY.md`
"""


def render_examples(theme, role_info):
    owner = theme["owner_title"]
    planner = theme["roles"]["planner"]["title"]
    lang = theme.get("language", "zh-CN")
    if lang == "en":
        return f"""# examples.md - Working Examples

## Direct reply

- Human asks a short question → reply directly to {owner}

## Formal task

- Human gives a concrete work instruction → create a task, summarize it clearly, and forward it to {planner}

## Identity conflict

- If an old file says a legacy title but `USER.md` says **{owner}**, use **{owner}**
"""
    return f"""# examples.md - 当前工作示例

## 直接回复

- 用户给出简短问题 → 直接回复 {owner}

## 正式任务

- 用户提出明确工作指令 → 创建任务、概括标题、转给 {planner}

## 身份冲突

- 如果旧文件还是历史称呼，但 `USER.md` 已写明当前称呼是 **{owner}**，一律使用 **{owner}**
"""


# ── SOUL.md renderers ────────────────────────────────────────

def render_router_soul(theme, task_prefix):
    r = theme["roles"]
    router = r["router"]
    planner = r["planner"]
    briefing = r["briefing"]
    owner = theme["owner_title"]
    tone = theme["tone"]

    ctx = {"title": router["title"], "owner": owner, "planner": planner["title"],
           "briefing": briefing["title"], "briefing_id": briefing["agent_id"]}

    direct_items = "\n".join(f"- {x}" for x in L(theme, "direct_items"))
    create_items = "\n".join(f"- {x}" for x in L(theme, "create_items"))
    title_items = "\n".join(f"- {x}" for x in L(theme, "title_items"))

    return f"""# {router['title']} · {router['description']}

{L(theme, 'router_intro').format(**ctx)}

## {L(theme, 'core_duties')}
1. {L(theme, 'router_intro').format(**ctx).split('，')[-1] if '，' in L(theme, 'router_intro').format(**ctx) else 'Route messages'}
2. Classify: chat vs formal task
3. Simple → reply directly
4. Formal task → summarize and forward to {planner['title']} (create {task_prefix} task)
5. Receive final report → reply to {owner}

---

## {L(theme, 'routing_rules')}

### {L(theme, 'direct_reply')}
{direct_items}

### {L(theme, 'create_task').format(**ctx)}
{create_items}

---

## {L(theme, 'recv_task')}

### {L(theme, 'step_ack').format(**ctx)}
```
{L(theme, 'step_ack_text').format(**ctx)}
```

### {L(theme, 'step_create')}
```bash
TASK_ID=$(python3 scripts/kanban_update.py next-id)
python3 scripts/kanban_update.py create "$TASK_ID" "your summary title" Planning {planner['title']} {planner['identity_name']}
```

**{L(theme, 'title_rules')}**
{title_items}

### {L(theme, 'step_dispatch').format(**ctx)}
`sessions_spawn(agentId="{planner['agent_id']}")`

### {L(theme, 'step_kanban')}
```bash
python3 scripts/kanban_update.py flow $TASK_ID "{router['title']}" "{planner['title']}" "Task forwarded: [summary]"
```

---

## {L(theme, 'recv_reply')}
{L(theme, 'recv_reply_text').format(**ctx)}
```bash
python3 scripts/kanban_update.py flow $TASK_ID "{router['title']}" "{owner}" "Report: [summary]"
```

## {L(theme, 'briefing_dispatch').format(**ctx)}
{L(theme, 'briefing_text').format(**ctx)}

## {L(theme, 'kanban_cmds')}
```bash
python3 scripts/kanban_update.py create <id> "<title>" <state> <org> <official>
python3 scripts/kanban_update.py state <id> <state> "<note>"
python3 scripts/kanban_update.py flow <id> "<from>" "<to>" "<remark>"
python3 scripts/kanban_update.py done <id> "<output>" "<summary>"
python3 scripts/kanban_update.py progress <id> "<current>" "<plan1|plan2>"
```

## {L(theme, 'tone_label')}
{tone}
"""


def render_planner_soul(theme, task_prefix):
    r = theme["roles"]
    planner = r["planner"]
    reviewer = r["reviewer"]
    dispatcher = r["dispatcher"]
    owner = theme["owner_title"]

    ctx = {"title": planner["title"], "owner": owner, "reviewer": reviewer["title"],
           "dispatcher": dispatcher["title"], "planner": planner["title"]}

    anti_stuck = "\n".join(f"{i+1}. {x.format(**ctx)}" for i, x in enumerate(L(theme, "anti_stuck_items")))

    return f"""# {planner['title']} · {planner['description']}

{L(theme, 'planner_intro').format(**ctx)}

> **{L(theme, 'planner_rule').format(**ctx)}**

---

## {L(theme, 'core_flow')}

### {L(theme, 'step0_classify')}

| {L(theme, 'level')} | {L(theme, 'condition')} | {L(theme, 'flow')} |
|------|------|------|
| **{L(theme, 's_level')}** | {L(theme, 's_desc')} | {L(theme, 's_flow').format(**ctx)} |
| **{L(theme, 'a_level')}** | {L(theme, 'a_desc')} | {L(theme, 'a_flow').format(**ctx)} |
| **{L(theme, 'b_level')}** | {L(theme, 'b_desc')} | {L(theme, 'b_flow').format(**ctx)} |

### {L(theme, 'step1_draft')}
- {L(theme, 'step1_text')}

```bash
python3 scripts/kanban_update.py state $TASK_ID Planning "{planner['title']} received, drafting plan"
```

### {L(theme, 'step2_review').format(**ctx)}
```bash
python3 scripts/kanban_update.py flow $TASK_ID "{planner['title']}" "{reviewer['title']}" "Submitted for review"
```
- {L(theme, 'step2_reject')}
- {L(theme, 'step2_pass')}

### {L(theme, 'step3_exec').format(**ctx)}
```bash
python3 scripts/kanban_update.py state $TASK_ID Assigned "Forward to {dispatcher['title']}"
python3 scripts/kanban_update.py flow $TASK_ID "{planner['title']}" "{dispatcher['title']}" "Dispatch to {dispatcher['title']}"
```

### {L(theme, 'step4_report')}
```bash
python3 scripts/kanban_update.py done $TASK_ID "<output>" "<summary>"
```

---

## {L(theme, 'progress_report')}
```bash
python3 scripts/kanban_update.py progress $TASK_ID "[level] analyzing task" "analyze|draft|review|execute|report"
```

## {L(theme, 'yield_rule')}
{L(theme, 'yield_text')}
```bash
sessions_yield(message="{L(theme, 'yield_wait_dispatcher').format(dispatcher=dispatcher['title'])}")
```
{L(theme, 'yield_note')}

## {L(theme, 'anti_stuck')}
{anti_stuck}

## {L(theme, 'tone_label')}
{theme['tone']}
"""


def render_reviewer_soul(theme):
    r = theme["roles"]
    reviewer = r["reviewer"]
    planner = r["planner"]

    ctx = {"title": reviewer["title"], "planner": planner["title"],
           "reviewer": reviewer["title"]}

    principle_items = "\n".join(f"- {x}" for x in L(theme, "principle_items"))

    return f"""# {reviewer['title']} · {reviewer['description']}

{L(theme, 'reviewer_intro').format(**ctx)}

> {L(theme, 'reviewer_note')}

## {L(theme, 'review_framework')}

| Dimension | Review Points |
|-----------|---------------|
| **{L(theme, 'dim_feasibility')}** | {L(theme, 'dim_feasibility_q')} |
| **{L(theme, 'dim_completeness')}** | {L(theme, 'dim_completeness_q')} |
| **{L(theme, 'dim_risk')}** | {L(theme, 'dim_risk_q')} |
| **{L(theme, 'dim_resource')}** | {L(theme, 'dim_resource_q')} |

## {L(theme, 'review_result')}

### {L(theme, 'reject')}
```bash
python3 scripts/kanban_update.py state $TASK_ID Planning "{reviewer['title']} rejected, return to {planner['title']}"
python3 scripts/kanban_update.py flow $TASK_ID "{reviewer['title']}" "{planner['title']}" "Rejected: [summary]"
```

### {L(theme, 'approve')}
```bash
python3 scripts/kanban_update.py state $TASK_ID Assigned "{reviewer['title']} approved"
python3 scripts/kanban_update.py flow $TASK_ID "{reviewer['title']}" "{planner['title']}" "Approved"
```

## {L(theme, 'principles')}
{principle_items}
"""


def render_dispatcher_soul(theme):
    r = theme["roles"]
    dispatcher = r["dispatcher"]
    planner = r["planner"]
    deps = r["departments"]

    ctx = {"title": dispatcher["title"], "planner": planner["title"],
           "dispatcher": dispatcher["title"]}

    dep_table = ""
    for key, dep in deps.items():
        dep_table += f"| {dep['title']} | {dep['agent_id']} | {dep['description']} |\n"

    return f"""# {dispatcher['title']} · {dispatcher['description']}

{L(theme, 'dispatcher_intro').format(**ctx)}

> {L(theme, 'dispatcher_note')}

## {L(theme, 'dept_table_header')}

| {L(theme, 'department')} | {L(theme, 'agent_id_col')} | {L(theme, 'responsibility')} |
|------|----------|------|
{dep_table}
## {L(theme, 'dispatch_flow')}

### {L(theme, 'step_analyze')}
```bash
python3 scripts/kanban_update.py state $TASK_ID Doing "{dispatcher['title']} dispatching"
```

### {L(theme, 'step_call_dept')}
{L(theme, 'step_call_note')}

### {L(theme, 'step_exception')}
- {L(theme, 'exc_retry')}
- {L(theme, 'exc_assist')}
- {L(theme, 'exc_block')}

### {L(theme, 'step_aggregate')}
```bash
python3 scripts/kanban_update.py done $TASK_ID "<output>" "<summary>"
```

## {L(theme, 'progress_report')}
```bash
python3 scripts/kanban_update.py progress $TASK_ID "Dispatching to departments" "analyze|deptA|deptB|aggregate|return"
```

## {L(theme, 'yield_rule')}
{L(theme, 'yield_text')}
```bash
sessions_yield(message="{L(theme, 'yield_wait_departments')}")
```
{L(theme, 'yield_note')}

## {L(theme, 'tone_label')}
{theme['tone']}
"""


def render_department_soul(theme, dep_key, dep_info):
    dispatcher = theme["roles"]["dispatcher"]

    ctx = {"name": dep_info["identity_name"], "dispatcher": dispatcher["title"],
           "desc": dep_info["description"], "title": dep_info["title"]}

    duty_items = "\n".join(f"{i+1}. {x.format(**ctx)}" for i, x in enumerate(L(theme, "dept_duty_items")))

    return f"""# {dep_info['title']} · {dep_info['description']}

{L(theme, 'dept_intro').format(**ctx)}

## {L(theme, 'dept_duties')}
{duty_items}

---

## {L(theme, 'dept_kanban')}

### {L(theme, 'dept_on_recv')}
```bash
python3 scripts/kanban_update.py state $TASK_ID Doing "{dep_info['title']} executing"
python3 scripts/kanban_update.py flow $TASK_ID "{dep_info['title']}" "{dep_info['title']}" "Started"
```

### {L(theme, 'dept_on_done')}
```bash
python3 scripts/kanban_update.py flow $TASK_ID "{dep_info['title']}" "{dispatcher['title']}" "Done: [summary]"
```

### {L(theme, 'dept_on_block')}
```bash
python3 scripts/kanban_update.py state $TASK_ID Blocked "[reason]"
python3 scripts/kanban_update.py flow $TASK_ID "{dep_info['title']}" "{dispatcher['title']}" "Blocked: [reason]"
```

## {L(theme, 'progress_report')}
```bash
python3 scripts/kanban_update.py progress $TASK_ID "Executing XX" "analyze|design|implement|test|deliver"
```

## {L(theme, 'yield_rule')}
{L(theme, 'yield_text')}
```bash
sessions_yield(message="{L(theme, 'yield_wait_execution')}")
```
{L(theme, 'yield_note')}

## {L(theme, 'shared_ctx')}
- {L(theme, 'shared_ctx_text')}

## {L(theme, 'tone_label')}
{theme['tone']}
"""


def render_briefing_soul(theme):
    briefing = theme["roles"]["briefing"]

    step_items = "\n".join(f"{i+1}. {x}" for i, x in enumerate(L(theme, "briefing_step_items")))

    return f"""# {briefing['title']}

{L(theme, 'briefing_duty')}

## {L(theme, 'briefing_steps')}

{step_items}

## {L(theme, 'progress_report')}
```bash
python3 scripts/kanban_update.py progress $TASK_ID "Collecting news" "politics|military|economy|AI|compile"
```

## {L(theme, 'yield_rule')}
{L(theme, 'yield_text')}
```bash
sessions_yield(message="{L(theme, 'yield_wait_briefing')}")
```
{L(theme, 'yield_note')}
"""


def render_org_structure(theme):
    r = theme["roles"]
    owner = theme["owner_title"]
    router = r["router"]
    planner = r["planner"]
    reviewer = r["reviewer"]
    dispatcher = r["dispatcher"]
    briefing = r["briefing"]
    deps = r["departments"]

    ctx = {"dispatcher": dispatcher["title"], "planner": planner["title"],
           "router": router["title"], "owner": owner}

    dep_tree = ""
    dep_table = ""
    for key, dep in deps.items():
        dep_tree += f"  |                 +-- {dep['title']} ({dep['agent_id']}) -- {dep['description']}\n"
        dep_table += f"| {dep['title']} | {dep['agent_id']} | {dep['description']} |\n"

    comm_rules = "\n".join([
        f"1. {L(theme, 'comm_vertical')}",
        f"2. {L(theme, 'comm_horizontal').format(**ctx)}",
        f"3. {L(theme, 'comm_escalation').format(**ctx)}",
    ])

    return f"""# {L(theme, 'org_structure')}

```
{owner}
  |
  +-- {router['title']} ({router['agent_id']}) -- {router['description']}
  |     |
  |     +-- {briefing['title']} ({briefing['agent_id']}) -- {briefing['description']}
  |     |
  |     +-- {planner['title']} ({planner['agent_id']}) -- {planner['description']}
  |           |
  |           +-- {reviewer['title']} ({reviewer['agent_id']}) -- {reviewer['description']}
  |           |
  |           +-- {dispatcher['title']} ({dispatcher['agent_id']}) -- {dispatcher['description']}
  |                 |
{dep_tree}```

## {L(theme, 'task_classification')}

| {L(theme, 'level')} | {L(theme, 'condition')} | {L(theme, 'flow')} |
|------|------|------|
| {L(theme, 's_level')} | {L(theme, 's_desc')} | {planner['title']} → {reviewer['title']} → {dispatcher['title']} |
| {L(theme, 'a_level')} | {L(theme, 'a_desc')} | {planner['title']} → {dispatcher['title']} |
| {L(theme, 'b_level')} | {L(theme, 'b_desc')} | {planner['title']} → {dispatcher['title']} |

## {L(theme, 'department_routing')}

| {L(theme, 'department')} | {L(theme, 'agent_id_col')} | {L(theme, 'responsibility')} |
|------|----------|------|
{dep_table}
## {L(theme, 'comm_rules')}
{comm_rules}
"""


def render_heartbeat(theme, interval=120):
    """Generate HEARTBEAT.md for agents that need heartbeat."""
    hb_items = "\n".join(f"- {x}" for x in L(theme, "heartbeat_items"))
    return f"""# {L(theme, 'heartbeat_title')}

{L(theme, 'heartbeat_intro').format(interval=interval)}

{hb_items}
"""


def generate_kanban_config(theme):
    """Generate kanban_config.json with theme-specific mappings.

    This replaces the hardcoded dicts in kanban_update.py so the same script
    works across all themes.
    """
    r = theme["roles"]
    owner = theme["owner_title"]
    task_prefix = theme.get("task_prefix", "TASK")
    is_chinese = str(theme.get("language") or "").lower().startswith("zh")
    doing_label = "执行中" if is_chinese else "Executing"
    done_label = "完成" if is_chinese else "Done"
    blocked_label = "阻塞" if is_chinese else "Blocked"

    # STATE_ORG_MAP: state code → display name of the org handling it
    state_org_map = {
        "Intake": r["router"]["title"],
        "Planning": r["planner"]["title"],
        "QualityReview": r["reviewer"]["title"],
        "Assigned": r["dispatcher"]["title"],
        "Doing": doing_label,
        "Review": r["dispatcher"]["title"],
        "Done": done_label,
        "Blocked": blocked_label,
    }

    # _STATE_AGENT_MAP: state code → agent_id
    state_agent_map = {
        "Intake": "main",
        "Planning": r["planner"]["agent_id"],
        "QualityReview": r["reviewer"]["agent_id"],
        "Assigned": r["dispatcher"]["agent_id"],
        "Review": r["dispatcher"]["agent_id"],
        "Pending": r["planner"]["agent_id"],
    }

    # _ORG_AGENT_MAP: display name → agent_id
    org_agent_map = {}
    for dep_key, dep in r["departments"].items():
        org_agent_map[dep["title"]] = dep["agent_id"]
    org_agent_map[r["planner"]["title"]] = r["planner"]["agent_id"]
    org_agent_map[r["reviewer"]["title"]] = r["reviewer"]["agent_id"]
    org_agent_map[r["dispatcher"]["title"]] = r["dispatcher"]["agent_id"]
    org_agent_map[r["router"]["title"]] = r["router"]["agent_id"]
    org_agent_map[r["briefing"]["title"]] = r["briefing"]["agent_id"]

    # _AGENT_LABELS: agent_id → display name
    agent_labels = {
        "main": r["router"]["title"],
        r["router"]["agent_id"]: r["router"]["title"],
        r["planner"]["agent_id"]: r["planner"]["title"],
        r["reviewer"]["agent_id"]: r["reviewer"]["title"],
        r["dispatcher"]["agent_id"]: r["dispatcher"]["title"],
        r["briefing"]["agent_id"]: r["briefing"]["title"],
    }
    for dep_key, dep in r["departments"].items():
        agent_labels[dep["agent_id"]] = dep["title"]

    return {
        "owner_title": owner,
        "task_prefix": task_prefix,
        "state_org_map": state_org_map,
        "state_agent_map": state_agent_map,
        "org_agent_map": org_agent_map,
        "agent_labels": agent_labels,
    }


def render_theme(theme, openclaw_dir, task_prefix, previous_theme=None):
    oc_dir = Path(openclaw_dir)
    r = theme["roles"]

    def write_file(agent_id, filename, content):
        ws = oc_dir / f"workspace-{agent_id}"
        ws.mkdir(parents=True, exist_ok=True)
        path = ws / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def write_if_missing(agent_id, filename, content):
        ws = oc_dir / f"workspace-{agent_id}"
        ws.mkdir(parents=True, exist_ok=True)
        path = ws / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(content, encoding="utf-8")

    # SOUL.md for each agent
    write_file(r["router"]["agent_id"], "SOUL.md", render_router_soul(theme, task_prefix))
    write_file(r["planner"]["agent_id"], "SOUL.md", render_planner_soul(theme, task_prefix))
    write_file(r["reviewer"]["agent_id"], "SOUL.md", render_reviewer_soul(theme))
    write_file(r["dispatcher"]["agent_id"], "SOUL.md", render_dispatcher_soul(theme))

    for dep_key, dep_info in r["departments"].items():
        write_file(dep_info["agent_id"], "SOUL.md", render_department_soul(theme, dep_key, dep_info))

    write_file(r["briefing"]["agent_id"], "SOUL.md", render_briefing_soul(theme))

    for semantic_key, role_info in iter_role_entries(theme):
        agent_id = role_info["agent_id"]
        ws = oc_dir / f"workspace-{agent_id}"
        existing_user = _read_text(ws / "USER.md")
        if existing_user and previous_theme:
            existing_user = translate_text_references(existing_user, previous_theme, theme)
        write_file(agent_id, "IDENTITY.md", render_identity(theme, semantic_key, role_info))
        write_file(agent_id, "USER.md", render_user(theme, existing_user))
        write_file(agent_id, "AGENTS.md", render_agents_protocol(theme, role_info))
        write_file(agent_id, "MEMORY.md", render_memory(theme, role_info))
        write_if_missing(agent_id, "memory/preferences.md", render_memory_seed(theme, role_info, "preferences.md"))
        write_if_missing(agent_id, "memory/decisions.md", render_memory_seed(theme, role_info, "decisions.md"))
        write_if_missing(agent_id, "memory/project-knowledge.md", render_memory_seed(theme, role_info, "project-knowledge.md"))
        write_file(agent_id, "message-style.md", render_message_style(theme, role_info))
        write_file(agent_id, "task-templates.md", render_task_templates(theme))
        write_file(agent_id, "daily-tasks.md", render_daily_tasks(theme, role_info))
        write_file(agent_id, "examples.md", render_examples(theme, role_info))

    # Shared context + kanban_config.json + HEARTBEAT.md to all workspaces
    org_md = render_org_structure(theme)
    kanban_cfg = generate_kanban_config(theme)
    heartbeat_md = render_heartbeat(theme)

    all_agents = [
        r["router"]["agent_id"], r["planner"]["agent_id"],
        r["reviewer"]["agent_id"], r["dispatcher"]["agent_id"],
        r["briefing"]["agent_id"],
    ] + [d["agent_id"] for d in r["departments"].values()]

    for agent_id in all_agents:
        ws = oc_dir / f"workspace-{agent_id}"
        ctx_dir = ws / "shared-context"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        kb_dir = ctx_dir / "knowledge-base"
        kb_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "ORG-STRUCTURE.md").write_text(org_md, encoding="utf-8")
        if not (ctx_dir / "THESIS.md").exists():
            (ctx_dir / "THESIS.md").write_text(render_shared_context_thesis(theme), encoding="utf-8")
        if not (ctx_dir / "FEEDBACK-LOG.md").exists():
            (ctx_dir / "FEEDBACK-LOG.md").write_text(render_feedback_log(theme), encoding="utf-8")
        if not (kb_dir / "README.md").exists():
            (kb_dir / "README.md").write_text(render_shared_context_kb_readme(theme), encoding="utf-8")

        # kanban_config.json in each workspace (used by kanban_update.py)
        (ws / "data").mkdir(parents=True, exist_ok=True)
        (ws / "data" / "kanban_config.json").write_text(
            json.dumps(kanban_cfg, ensure_ascii=False, indent=2) + "\n"
        )

    # HEARTBEAT.md for router and planner (agents that need heartbeat)
    for agent_id in [r["router"]["agent_id"], r["planner"]["agent_id"]]:
        write_file(agent_id, "HEARTBEAT.md", heartbeat_md)

    print(
        f"Rendered {len(all_agents)} workspaces with SOUL.md / IDENTITY.md / USER.md / "
        "AGENTS.md / MEMORY.md / memory/ / message-style.md / task-templates.md / daily-tasks.md / "
        "examples.md / shared-context / kanban_config.json / HEARTBEAT.md"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--theme", required=True)
    parser.add_argument("--openclaw-dir", required=True)
    parser.add_argument("--primary-model", default="gemma-4-e2b-edge")
    parser.add_argument("--light-model", default="gemma-4-e2b-edge")
    parser.add_argument("--task-prefix", default="TASK")
    args = parser.parse_args()

    theme = load_theme(args.theme)
    render_theme(theme, args.openclaw_dir, args.task_prefix)


if __name__ == "__main__":
    main()
