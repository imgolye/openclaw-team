"""Canonical bundled kanban defaults for the modern corporate theme."""

from __future__ import annotations

from copy import deepcopy


KANBAN_DEFAULTS = {
    "owner_title": "负责人",
    "task_prefix": "TASK",
    "state_org_map": {
        "Intake": "项目协调助理",
        "Planning": "产品策略负责人",
        "QualityReview": "质量与风控负责人",
        "Assigned": "项目运营负责人",
        "Doing": "执行中",
        "Review": "项目运营负责人",
        "Done": "完成",
        "Blocked": "阻塞",
    },
    "state_agent_map": {
        "Intake": "main",
        "Planning": "vp_strategy",
        "QualityReview": "vp_compliance",
        "Assigned": "coo",
        "Review": "coo",
        "Pending": "vp_strategy",
    },
    "org_agent_map": {
        "项目协调助理": "assistant",
        "产品策略负责人": "vp_strategy",
        "质量与风控负责人": "vp_compliance",
        "项目运营负责人": "coo",
        "研发负责人": "engineering",
        "平台运维负责人": "devops",
        "数据分析负责人": "data_team",
        "市场运营负责人": "marketing",
        "测试负责人": "qa",
        "人力支持负责人": "hr",
        "情报简报专员": "briefing",
    },
    "agent_labels": {
        "main": "项目协调助理",
        "assistant": "项目协调助理",
        "vp_strategy": "产品策略负责人",
        "vp_compliance": "质量与风控负责人",
        "coo": "项目运营负责人",
        "engineering": "研发负责人",
        "devops": "平台运维负责人",
        "data_team": "数据分析负责人",
        "marketing": "市场运营负责人",
        "qa": "测试负责人",
        "hr": "人力支持负责人",
        "briefing": "情报简报专员",
    },
}


def bundled_kanban_defaults():
    """Return a defensive copy of the shipped kanban defaults."""
    return deepcopy(KANBAN_DEFAULTS)
