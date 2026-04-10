#!/usr/bin/env python3
"""OpenClaw Team 健康看板 - 一键查看所有 agent 状态和最近活动"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

from backend.adapters.storage.dashboard import list_task_records


_DEFAULT_AGENTS = [
    ("router", "Router", "消息路由"),
    ("planner", "Planner", "规划决策"),
    ("reviewer", "Reviewer", "审议把关"),
    ("dispatcher", "Dispatcher", "执行调度"),
]


def infer_openclaw_dir(explicit_dir=None):
    if explicit_dir:
        return Path(explicit_dir).expanduser().resolve()

    env_dir = os.environ.get("OPENCLAW_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()

    script_path = Path(__file__).resolve()
    for parent in script_path.parents:
        if parent.name.startswith("workspace-"):
            return parent.parent

    return Path.home() / ".openclaw"


def load_openclaw_config(openclaw_dir):
    config_file = Path(openclaw_dir) / "openclaw.json"
    try:
        with open(config_file, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def load_agents_from_config(config):
    try:
        agents = []
        for agent in config.get("agents", {}).get("list", []):
            aid = agent["id"]
            name = agent.get("identity", {}).get("name", aid)
            desc = agent.get("description", "")
            agents.append((aid, name, desc))
        if agents:
            return agents
    except Exception:
        pass
    return _DEFAULT_AGENTS


def get_router_agent_id(config):
    for agent in config.get("agents", {}).get("list", []):
        if agent.get("default"):
            return agent["id"]
    agents = config.get("agents", {}).get("list", [])
    if agents:
        return agents[0]["id"]
    return "router"


def get_gateway_health():
    try:
        result = subprocess.run(
            ["openclaw", "gateway", "health"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout.strip() or result.stderr.strip() or "No output"
    except Exception as exc:
        return f"ERROR: {exc}"


def get_workspace_activity(openclaw_dir, agent_id):
    ws = Path(openclaw_dir) / f"workspace-{agent_id}"
    if not ws.exists():
        return None
    latest = None
    for path in ws.rglob("*"):
        if path.is_file() and ".git" not in str(path):
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            if latest is None or mtime > latest:
                latest = mtime
    return latest


def get_session_activity(openclaw_dir, agent_id):
    sessions_dir = Path(openclaw_dir) / "agents" / agent_id / "sessions"
    if not sessions_dir.exists():
        return None
    latest = None
    for path in sessions_dir.rglob("*"):
        if path.is_file():
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            if latest is None or mtime > latest:
                latest = mtime
    return latest


def format_age(dt):
    if dt is None:
        return "无记录"
    delta = datetime.now() - dt
    if delta < timedelta(minutes=5):
        return "刚刚"
    if delta < timedelta(hours=1):
        return f"{int(delta.total_seconds() / 60)}分钟前"
    if delta < timedelta(days=1):
        return f"{int(delta.total_seconds() / 3600)}小时前"
    return f"{delta.days}天前"


def get_active_tasks(openclaw_dir, router_agent_id):
    workspace = Path(openclaw_dir) / f"workspace-{router_agent_id}"
    try:
        tasks = list_task_records(
            openclaw_dir,
            workspace_id=router_agent_id,
            workspace_path=str(workspace),
        )
    except Exception:
        return []

    active = []
    for task in tasks:
        state = str(task.get("state", task.get("status", ""))).lower()
        if state and state not in ("done", "cancelled", "canceled"):
            active.append(task)
    return active


def get_model_info(config):
    try:
        return {
            agent["id"]: agent.get("model", "default")
            for agent in config.get("agents", {}).get("list", [])
        }
    except Exception:
        return {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="")
    args = parser.parse_args()

    openclaw_dir = infer_openclaw_dir(args.dir)
    config = load_openclaw_config(openclaw_dir)
    agents = load_agents_from_config(config)
    router_agent_id = get_router_agent_id(config)
    models = get_model_info(config)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 70)
    print(f"  Health Dashboard  |  {now}")
    print("=" * 70)

    print("\n--- 网关状态 ---")
    health = get_gateway_health()
    for line in health.splitlines():
        if line.strip():
            print(f"  {line.strip()}")

    print("\n--- Agent 状态 ---")
    print(f"  {'Agent':<12} {'名称':<10} {'模型':<24} {'工作区活动':<12} {'会话活动':<12}")
    print(f"  {'-' * 12} {'-' * 10} {'-' * 24} {'-' * 12} {'-' * 12}")

    for agent_id, name, _role in agents:
        ws_time = get_workspace_activity(openclaw_dir, agent_id)
        sess_time = get_session_activity(openclaw_dir, agent_id)
        model = models.get(agent_id, "?")
        if len(model) > 22:
            model = model[-22:]
        print(
            f"  {agent_id:<12} {name:<10} {model:<24} "
            f"{format_age(ws_time):<12} {format_age(sess_time):<12}"
        )

    active = get_active_tasks(openclaw_dir, router_agent_id)
    print(f"\n--- 活跃任务 ({len(active)}) ---")
    if active:
        for task in active[:10]:
            task_id = task.get("id", "?")
            title = task.get("title", "?")[:30]
            state = task.get("state", task.get("status", "?"))
            now_field = task.get("now", task.get("currentUpdate", ""))[:40]
            print(f"  {task_id}  [{state}]  {title}")
            if now_field:
                print(f"    -> {now_field}")
    else:
        print("  无活跃任务")

    print("\n--- 日志状态 ---")
    log_dir = Path(openclaw_dir) / "logs"
    if log_dir.exists():
        for log_file in sorted(log_dir.glob("*.log")):
            size = log_file.stat().st_size
            mtime = format_age(datetime.fromtimestamp(log_file.stat().st_mtime))
            size_str = f"{size / 1024:.1f}KB" if size > 1024 else f"{size}B"
            print(f"  {log_file.name:<30} {size_str:<10} {mtime}")
    else:
        print("  日志目录不存在")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
