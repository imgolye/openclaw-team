#!/usr/bin/env python3
"""Canonical API route aliases for OpenClaw Team."""

from __future__ import annotations

QUERY_ROUTE_ALIASES = {}

ACTION_ROUTE_ALIASES = {}

COMMAND_ROUTE_PREFIXES = (
    "/api/actions/",
)

TASK_ACTION_KIND_ALIASES = {
    "/api/actions/task/create": "create",
    "/api/actions/task/preview": "preview",
    "/api/actions/task/progress": "progress",
    "/api/actions/task/block": "block",
    "/api/actions/task/done": "done",
    "/api/actions/task/assign": "assign",
    "/api/actions/task/team-sync": "team-sync",
}


def canonical_query_path(path):
    return QUERY_ROUTE_ALIASES.get(path, path)


def canonical_action_path(path):
    return ACTION_ROUTE_ALIASES.get(path, path)


def is_command_path(path):
    return path.startswith(COMMAND_ROUTE_PREFIXES)


def task_action_kind(path):
    return TASK_ACTION_KIND_ALIASES.get(path, "")
