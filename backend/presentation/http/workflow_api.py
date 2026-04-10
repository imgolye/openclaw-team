#!/usr/bin/env python3
"""Workflow API endpoints for desktop client."""

from __future__ import annotations

import json
import math
import os
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
import shutil
import struct
import wave
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml


# In-memory storage with file persistence
_workflows_db = {}
_workflow_runs_db = {}
_db_lock = threading.Lock()
_db_initialized = False
CURRENT_DSL_VERSION = "0.1.0"

WORKFLOW_NODE_LIBRARY = [
    {
        "id": "start",
        "name": "User Input",
        "emoji": "⚡",
        "color": "#f59e0b",
        "description": "Traditional start node for user input",
        "category": "entry",
        "difyType": "start",
        "isStart": True,
    },
    {
        "id": "trigger-schedule",
        "name": "Schedule Trigger",
        "emoji": "⏰",
        "color": "#3b82f6",
        "description": "Time-based workflow trigger",
        "category": "entry",
        "difyType": "trigger-schedule",
        "isStart": True,
    },
    {
        "id": "trigger-webhook",
        "name": "Webhook Trigger",
        "emoji": "🪝",
        "color": "#0f766e",
        "description": "HTTP callback trigger",
        "category": "entry",
        "difyType": "trigger-webhook",
        "isStart": True,
    },
    {
        "id": "agent",
        "name": "Agent",
        "emoji": "🤖",
        "color": "#1ead6f",
        "description": "Delegate a step to an agent",
        "category": "default",
        "difyType": "agent",
    },
    {
        "id": "llm",
        "name": "LLM",
        "emoji": "🧠",
        "color": "#7c3aed",
        "description": "Prompt a model directly",
        "category": "default",
        "difyType": "llm",
    },
    {
        "id": "knowledge-retrieval",
        "name": "Knowledge Retrieval",
        "emoji": "📚",
        "color": "#2563eb",
        "description": "Query knowledge before generation",
        "category": "default",
        "difyType": "knowledge-retrieval",
    },
    {
        "id": "if-else",
        "name": "IF / ELSE",
        "emoji": "🔀",
        "color": "#0891b2",
        "description": "Branch on conditions",
        "category": "logic",
        "difyType": "if-else",
    },
    {
        "id": "code",
        "name": "Code",
        "emoji": "💻",
        "color": "#4f46e5",
        "description": "Transform data with code",
        "category": "transform",
        "difyType": "code",
    },
    {
        "id": "variable-assigner",
        "name": "Variable Assigner",
        "emoji": "🧩",
        "color": "#9333ea",
        "description": "Store workflow variables",
        "category": "transform",
        "difyType": "variable-assigner",
    },
    {
        "id": "http-request",
        "name": "HTTP Request",
        "emoji": "🌐",
        "color": "#ef4444",
        "description": "Call an external HTTP endpoint",
        "category": "utilities",
        "difyType": "http-request",
    },
    {
        "id": "tool",
        "name": "Tool",
        "emoji": "🛠️",
        "color": "#ea580c",
        "description": "Invoke a tool or integration",
        "category": "utilities",
        "difyType": "tool",
    },
    {
        "id": "end",
        "name": "End",
        "emoji": "🏁",
        "color": "#6b7280",
        "description": "Finish the workflow",
        "category": "default",
        "difyType": "end",
        "isTerminal": True,
    },
]

WORKFLOW_TEMPLATES = [
    {
        "id": "support-triage",
        "name": "Support Triage",
        "description": "User input -> classify -> retrieve context -> agent response -> end",
        "entryType": "start",
        "nodes": [
            {"type": "start", "title": "User Input", "description": "Receive the support request", "config": {}},
            {"type": "if-else", "title": "Classify Intent", "description": "Route urgent vs normal requests", "config": {"rules": []}},
            {"type": "knowledge-retrieval", "title": "Retrieve Context", "description": "Fetch relevant runbooks", "config": {}},
            {"type": "agent", "title": "Support Agent", "description": "Draft the response", "config": {}},
            {"type": "end", "title": "Done", "description": "Return the result", "config": {}},
        ],
    },
    {
        "id": "ops-webhook",
        "name": "Ops Webhook Intake",
        "description": "Webhook trigger -> agent analysis -> HTTP callback -> end",
        "entryType": "trigger-webhook",
        "nodes": [
            {"type": "trigger-webhook", "title": "Webhook Trigger", "description": "Receive an external event", "config": {}},
            {"type": "agent", "title": "Ops Agent", "description": "Analyze the alert payload", "config": {}},
            {"type": "http-request", "title": "Notify External System", "description": "Call downstream workflow", "config": {"method": "POST"}},
            {"type": "end", "title": "Done", "description": "Mark the flow complete", "config": {}},
        ],
    },
    {
        "id": "scheduled-digest",
        "name": "Scheduled Digest",
        "description": "Schedule trigger -> retrieval -> LLM summary -> end",
        "entryType": "trigger-schedule",
        "nodes": [
            {"type": "trigger-schedule", "title": "Schedule Trigger", "description": "Run on a timed schedule", "config": {"cron": "0 9 * * *"}},
            {"type": "knowledge-retrieval", "title": "Collect Signals", "description": "Gather recent context", "config": {}},
            {"type": "llm", "title": "Draft Digest", "description": "Generate the daily summary", "config": {}},
            {"type": "end", "title": "Done", "description": "Output the digest", "config": {}},
        ],
    },
    {
        "id": "short-video-production",
        "name": "短视频制作",
        "description": "创意简报 -> 品牌检索 -> 脚本分镜 -> 旁白 -> 渲染包 -> 发布文案",
        "entryType": "start",
        "nodes": [
            {
                "type": "start",
                "title": "创意简报",
                "description": "定义主题、平台、时长与转化目标",
                "x": 80,
                "y": 210,
                "config": {
                    "brief": {
                        "topic": "介绍 OpenClaw Team 的桌面工作台",
                        "audience": "企业内部产品与运营团队",
                        "platform": "视频号",
                        "durationSeconds": 45,
                        "visualStyle": "轻科技、白底工作台、快速切镜",
                        "cta": "关注并私信领取产品体验名额",
                    },
                    "inputs": [
                        {"name": "topic", "type": "text", "description": "视频主题", "example": "介绍 OpenClaw Team 的桌面工作台"},
                        {"name": "audience", "type": "text", "description": "目标人群", "example": "企业内部产品与运营团队"},
                        {"name": "platform", "type": "text", "description": "发布平台", "example": "视频号"},
                        {"name": "durationSeconds", "type": "number", "description": "目标时长（秒）", "example": "45"},
                        {"name": "visualStyle", "type": "text", "description": "视觉风格", "example": "轻科技、白底工作台、快速切镜"},
                        {"name": "cta", "type": "text", "description": "结尾动作", "example": "关注并私信领取产品体验名额"},
                    ],
                },
            },
            {
                "type": "knowledge-retrieval",
                "title": "品牌素材检索",
                "description": "提取品牌语气、产品卖点和历史高表现内容",
                "x": 370,
                "y": 210,
                "config": {
                    "sources": ["brand-voice", "product-highlights", "winning-short-clips"],
                    "query": "{{input.topic}}",
                },
            },
            {
                "type": "llm",
                "title": "生成脚本与钩子",
                "description": "输出开场钩子、主体节奏和结尾 CTA",
                "x": 660,
                "y": 210,
                "config": {
                    "prompt": "为短视频生成 3 秒钩子、主体三段节奏、结尾 CTA 和配音旁白。",
                },
            },
            {
                "type": "code",
                "title": "拆解分镜与字幕",
                "description": "把脚本转换成镜头表、字幕表和素材清单",
                "x": 950,
                "y": 210,
                "config": {
                    "outputs": ["storyboard", "subtitleDraft", "assetChecklist"],
                },
            },
            {
                "type": "tool",
                "title": "生成旁白音频",
                "description": "用语音服务生成本次视频旁白",
                "x": 1240,
                "y": 210,
                "config": {
                    "toolId": "speech.generate",
                    "voice": "zf_001",
                    "format": "wav",
                },
            },
            {
                "type": "tool",
                "title": "组装渲染包",
                "description": "整理渲染所需的视频脚本、旁白、字幕与封面文案",
                "x": 1530,
                "y": 210,
                "config": {
                    "toolId": "video.render-package",
                    "provider": "sora",
                    "model": "sora-2",
                    "size": "720x1280",
                    "pollAttempts": 6,
                    "pollIntervalSeconds": 5,
                    "outputs": ["scenePlan", "subtitles", "voiceover", "coverCopy"],
                },
            },
            {
                "type": "variable-assigner",
                "title": "发布文案变量",
                "description": "准备标题、描述、话题标签和评论区引导",
                "x": 1820,
                "y": 210,
                "config": {
                    "assignments": [
                        {"name": "publishTitle", "value": "3 分钟看懂 OpenClaw Team 如何统一团队协作"},
                        {"name": "publishCaption", "value": "从消息、工作流到语音自动化，一条链讲清楚 OpenClaw Team 的桌面工作台。"},
                        {"name": "hashtags", "value": "#企业协作 #AI工作流 #产品运营"},
                    ],
                },
            },
            {
                "type": "end",
                "title": "交付审阅",
                "description": "输出可交付的短视频制作包",
                "x": 2110,
                "y": 210,
                "config": {
                    "deliverables": ["script.md", "storyboard.json", "narration.wav", "subtitle.srt", "publish-pack.json"],
                },
            },
        ],
    },
    # ── 飞书文档 → 微信公众号 ──
    {
        "id": "feishu-to-wechat",
        "name": "飞书文档发布公众号",
        "description": "飞书文档 -> 格式转换 -> 图片上传 -> HTML 渲染 -> 创建草稿 -> 预览确认",
        "entryType": "start",
        "nodes": [
            {
                "type": "start",
                "title": "飞书文档输入",
                "description": "输入飞书文档链接和发布参数",
                "x": 80,
                "y": 210,
                "config": {
                    "inputs": [
                        {"name": "docUrl", "type": "text", "description": "飞书文档链接", "example": "https://your-domain.feishu.cn/docx/DocID"},
                        {"name": "author", "type": "text", "description": "作者名", "example": "OpenClaw Team"},
                        {"name": "thumbMediaId", "type": "text", "description": "封面图 media_id"},
                        {"name": "styleProfile", "type": "select", "description": "排版方案", "options": ["doocs", "default", "classic", "minimal"]},
                        {"name": "theme", "type": "select", "description": "主题风格", "options": ["grace", "simple", "default"]},
                        {"name": "fontSize", "type": "number", "description": "正文字号", "example": "14"},
                    ],
                },
            },
            {
                "type": "tool",
                "title": "获取飞书文档",
                "description": "通过飞书 CLI / API 下载文档内容与图片资源",
                "x": 370,
                "y": 210,
                "config": {
                    "toolId": "feishu.doc-fetch",
                    "method": "lark-cli",
                    "outputs": ["documentBlocks", "imageAssets", "documentMeta"],
                },
            },
            {
                "type": "code",
                "title": "格式转换",
                "description": "飞书 Block 结构 → 标准化 Markdown",
                "x": 660,
                "y": 210,
                "config": {
                    "language": "python",
                    "outputs": ["markdown", "imageReferences"],
                },
            },
            {
                "type": "tool",
                "title": "图片上传素材库",
                "description": "将文档图片上传到微信永久素材库，获取 CDN 链接",
                "x": 950,
                "y": 210,
                "config": {
                    "toolId": "wechat.material-upload",
                    "mediaType": "image",
                    "maxSizeMB": 10,
                    "autoCompress": True,
                    "outputs": ["imageUrlMap"],
                },
            },
            {
                "type": "code",
                "title": "HTML 渲染",
                "description": "按 Doocs 风格渲染微信兼容 HTML",
                "x": 1240,
                "y": 210,
                "config": {
                    "language": "python",
                    "styleOptions": {
                        "profile": "doocs",
                        "theme": "{{input.theme}}",
                        "fontSize": "{{input.fontSize}}",
                        "macCodeBlock": True,
                        "headingStyle": "solid",
                        "codeTheme": "github",
                    },
                    "outputs": ["htmlContent", "previewHtml"],
                },
            },
            {
                "type": "if-else",
                "title": "预览确认",
                "description": "人工确认排版效果后继续发布",
                "x": 1530,
                "y": 210,
                "config": {
                    "rules": [
                        {"label": "确认发布", "condition": "approved === true"},
                        {"label": "返回修改", "condition": "approved === false"},
                    ],
                    "requireApproval": True,
                },
            },
            {
                "type": "http-request",
                "title": "创建公众号草稿",
                "description": "调用微信 API 创建公众号草稿",
                "x": 1820,
                "y": 210,
                "config": {
                    "method": "POST",
                    "url": "https://api.weixin.qq.com/cgi-bin/draft/add",
                    "outputs": ["draftMediaId"],
                },
            },
            {
                "type": "end",
                "title": "发布完成",
                "description": "输出草稿 media_id，可在公众号后台草稿箱查看",
                "x": 2110,
                "y": 210,
                "config": {
                    "deliverables": ["draftMediaId", "previewHtml"],
                },
            },
        ],
    },
]


def _get_storage_dir(openclaw_dir):
    """Get the storage directory for workflow data."""
    storage_dir = Path(openclaw_dir) / ".mission-control" / "workflows"
    storage_dir.mkdir(parents=True, exist_ok=True)
    return storage_dir


def _get_workflows_file(openclaw_dir):
    """Get the workflows storage file path."""
    return _get_storage_dir(openclaw_dir) / "workflows.json"


def _get_runs_file(openclaw_dir):
    """Get the workflow runs storage file path."""
    return _get_storage_dir(openclaw_dir) / "workflow_runs.json"


def _load_from_disk(openclaw_dir):
    """Load workflows from disk storage."""
    global _workflows_db, _workflow_runs_db, _db_initialized
    
    if _db_initialized:
        return
    
    with _db_lock:
        if _db_initialized:
            return
        
        workflows_file = _get_workflows_file(openclaw_dir)
        runs_file = _get_runs_file(openclaw_dir)
        
        if workflows_file.exists():
            try:
                with open(workflows_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        _workflows_db = data
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load workflows: {e}")
        
        if runs_file.exists():
            try:
                with open(runs_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        _workflow_runs_db = data
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load workflow runs: {e}")
        
        _db_initialized = True


def _save_to_disk(openclaw_dir):
    """Save workflows to disk storage."""
    with _db_lock:
        workflows_file = _get_workflows_file(openclaw_dir)
        runs_file = _get_runs_file(openclaw_dir)
        
        try:
            with open(workflows_file, 'w', encoding='utf-8') as f:
                json.dump(_workflows_db, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save workflows: {e}")
        
        try:
            with open(runs_file, 'w', encoding='utf-8') as f:
                json.dump(_workflow_runs_db, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save workflow runs: {e}")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _generate_id():
    return str(uuid.uuid4())


def _normalize_text(value, fallback=""):
    text = str(value or "").strip()
    return text or fallback


def _normalize_float(value, fallback=0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _safe_list(value):
    return value if isinstance(value, list) else []


def _clone_node_library():
    return [dict(item) for item in WORKFLOW_NODE_LIBRARY]


def _workflow_meta_payload():
    return {
        "dslVersion": CURRENT_DSL_VERSION,
        "nodeTypes": _clone_node_library(),
        "templates": [dict(item) for item in WORKFLOW_TEMPLATES],
        "categories": [
            {"id": "entry", "name": "Entry"},
            {"id": "default", "name": "Core"},
            {"id": "logic", "name": "Logic"},
            {"id": "transform", "name": "Transform"},
            {"id": "utilities", "name": "Utilities"},
        ],
    }


def _entry_node_types(nodes):
    types = []
    for node in _safe_list(nodes):
        node_type = _normalize_text((node or {}).get("type"))
        if node_type in {"start", "trigger-schedule", "trigger-webhook"} and node_type not in types:
            types.append(node_type)
    return types


def _normalize_node(node, index):
    source = node if isinstance(node, dict) else {}
    position = source.get("position") if isinstance(source.get("position"), dict) else {}
    x = _normalize_float(source.get("x", position.get("x")), index * 320)
    y = _normalize_float(source.get("y", position.get("y")), 220)
    return {
        "id": _normalize_text(source.get("id"), f"node-{index + 1}"),
        "type": _normalize_text(source.get("type"), "agent"),
        "title": _normalize_text(source.get("title"), _normalize_text(source.get("name"), f"Node {index + 1}")),
        "description": _normalize_text(source.get("description"), ""),
        "config": source.get("config") if isinstance(source.get("config"), dict) else {},
        "x": x,
        "y": y,
        "position": {"x": x, "y": y},
    }


def _normalize_nodes(nodes):
    return [_normalize_node(node, index) for index, node in enumerate(_safe_list(nodes))]


def _default_edges_from_nodes(nodes):
    edges = []
    for index in range(len(nodes) - 1):
        source = nodes[index]
        target = nodes[index + 1]
        if not source.get("id") or not target.get("id"):
            continue
        edges.append({
            "id": f"edge-{source['id']}-{target['id']}",
            "source": source["id"],
            "target": target["id"],
            "type": "custom",
        })
    return edges


def _normalize_edges(edges, nodes):
    valid_node_ids = {node.get("id") for node in nodes if node.get("id")}
    normalized = []
    for index, edge in enumerate(_safe_list(edges)):
        source = edge if isinstance(edge, dict) else {}
        source_id = _normalize_text(source.get("source"))
        target_id = _normalize_text(source.get("target"))
        if not source_id or not target_id:
            continue
        if source_id not in valid_node_ids or target_id not in valid_node_ids:
            continue
        normalized.append({
            "id": _normalize_text(source.get("id"), f"edge-{source_id}-{target_id}-{index + 1}"),
            "source": source_id,
            "target": target_id,
            "type": _normalize_text(source.get("type"), "custom"),
        })
    return normalized


def _normalize_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(value)


def _normalize_workspace_path(value):
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return str(Path(text).expanduser().resolve())
    except Exception:
        return ""


def _normalize_workflow_payload(body, existing=None):
    source = body if isinstance(body, dict) else {}
    previous = existing if isinstance(existing, dict) else {}
    name = _normalize_text(source.get("name"), _normalize_text(previous.get("name"), "未命名工作流"))
    description = _normalize_text(source.get("description"), _normalize_text(previous.get("description"), ""))
    status = _normalize_text(source.get("status"), _normalize_text(previous.get("status"), "draft"))
    template_id = _normalize_text(source.get("templateId"), _normalize_text(previous.get("templateId"), ""))
    nodes = _normalize_nodes(source.get("nodes"))
    edges = _normalize_edges(source.get("edges"), nodes)
    if not edges and len(nodes) > 1:
        edges = _default_edges_from_nodes(nodes)
    viewport = source.get("viewport") if isinstance(source.get("viewport"), dict) else previous.get("viewport")
    workspace_path = _normalize_workspace_path(
        source.get("workspacePath") if source.get("workspacePath") is not None else previous.get("workspacePath")
    )
    workspace_authorized = source.get("workspaceAuthorized")
    if workspace_authorized is None:
        workspace_authorized = previous.get("workspaceAuthorized")
    return {
        "name": name,
        "description": description,
        "status": status,
        "templateId": template_id,
        "nodes": nodes,
        "edges": edges,
        "entryNodeTypes": _entry_node_types(nodes),
        "viewport": viewport if isinstance(viewport, dict) else {"x": 0, "y": 0, "zoom": 1},
        "workspacePath": workspace_path,
        "workspaceAuthorized": _normalize_bool(workspace_authorized),
    }


def _workflow_to_dsl(workflow):
    item = workflow if isinstance(workflow, dict) else {}
    return {
        "version": CURRENT_DSL_VERSION,
        "kind": "app",
        "app": {
            "name": _normalize_text(item.get("name"), "Imported Workflow"),
            "mode": "workflow",
            "description": _normalize_text(item.get("description"), ""),
        },
        "workflow": {
            "status": _normalize_text(item.get("status"), "draft"),
            "graph": {
                "nodes": _safe_list(item.get("nodes")),
                "edges": _safe_list(item.get("edges")),
                "viewport": item.get("viewport") if isinstance(item.get("viewport"), dict) else {"x": 0, "y": 0, "zoom": 1},
            },
            "features": {
                "entry_node_types": _entry_node_types(item.get("nodes")),
                "template_id": _normalize_text(item.get("templateId"), ""),
                "workspace_path": _normalize_text(item.get("workspacePath"), ""),
                "workspace_authorized": _normalize_bool(item.get("workspaceAuthorized")),
                "source": "mission-control",
            },
        },
    }


def _workflow_from_dsl_payload(payload):
    source = payload if isinstance(payload, dict) else {}
    if source.get("kind") not in {"app", "workflow", None}:
        raise ValueError("仅支持 kind=app 或 kind=workflow 的 DSL。")
    workflow_data = source.get("workflow")
    if not isinstance(workflow_data, dict):
        raise ValueError("DSL 缺少 workflow 段。")
    graph = workflow_data.get("graph") if isinstance(workflow_data.get("graph"), dict) else workflow_data
    if not isinstance(graph, dict):
        raise ValueError("DSL 缺少 workflow.graph。")
    nodes = _normalize_nodes(graph.get("nodes"))
    edges = _normalize_edges(graph.get("edges"), nodes)
    if not edges and len(nodes) > 1:
        edges = _default_edges_from_nodes(nodes)
    app_data = source.get("app") if isinstance(source.get("app"), dict) else {}
    name = _normalize_text(app_data.get("name"), _normalize_text(workflow_data.get("name"), "导入工作流"))
    description = _normalize_text(app_data.get("description"), _normalize_text(workflow_data.get("description"), ""))
    status = _normalize_text(workflow_data.get("status"), "draft")
    features = workflow_data.get("features") if isinstance(workflow_data.get("features"), dict) else {}
    return {
        "name": name,
        "description": description,
        "status": status,
        "templateId": _normalize_text(features.get("template_id"), ""),
        "nodes": nodes,
        "edges": edges,
        "entryNodeTypes": _entry_node_types(nodes),
        "viewport": graph.get("viewport") if isinstance(graph.get("viewport"), dict) else {"x": 0, "y": 0, "zoom": 1},
        "workspacePath": _normalize_workspace_path(features.get("workspace_path")),
        "workspaceAuthorized": _normalize_bool(features.get("workspace_authorized")),
    }


def _slugify(value):
    parts = []
    for char in _normalize_text(value).lower():
        if char.isalnum():
            parts.append(char)
        elif parts and parts[-1] != "-":
            parts.append("-")
    return "".join(parts).strip("-") or "workflow"


def _inputs_to_example_map(inputs):
    values = {}
    for item in _safe_list(inputs):
        if isinstance(item, dict):
            key = _normalize_text(item.get("name") or item.get("key") or item.get("label"))
            if key:
                values[key] = item.get("example") or item.get("value")
    return values


def _normalize_run_inputs(payload):
    source = payload if isinstance(payload, dict) else {}
    normalized = {}
    for key, value in source.items():
        text_key = _normalize_text(key)
        if text_key:
            normalized[text_key] = value
    return normalized


def _extract_video_brief(workflow, input_overrides=None):
    default_brief = {
        "topic": "介绍 OpenClaw Team 的桌面工作台",
        "audience": "企业内部产品与运营团队",
        "platform": "视频号",
        "durationSeconds": 45,
        "visualStyle": "轻科技、白底工作台、快速切镜",
        "cta": "关注并私信领取产品体验名额",
    }
    for node in _safe_list((workflow or {}).get("nodes")):
        if _normalize_text(node.get("type")) not in {"start", "trigger-schedule", "trigger-webhook"}:
            continue
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        brief = config.get("brief") if isinstance(config.get("brief"), dict) else {}
        examples = _inputs_to_example_map(config.get("inputs"))
        overrides = _normalize_run_inputs(input_overrides)
        candidate = {
            "topic": _normalize_text(overrides.get("topic") or brief.get("topic") or examples.get("topic"), default_brief["topic"]),
            "audience": _normalize_text(overrides.get("audience") or brief.get("audience") or examples.get("audience"), default_brief["audience"]),
            "platform": _normalize_text(overrides.get("platform") or brief.get("platform") or examples.get("platform"), default_brief["platform"]),
            "durationSeconds": int(_normalize_float(overrides.get("durationSeconds") or brief.get("durationSeconds") or examples.get("durationSeconds"), default_brief["durationSeconds"])),
            "visualStyle": _normalize_text(overrides.get("visualStyle") or brief.get("visualStyle") or examples.get("visualStyle"), default_brief["visualStyle"]),
            "cta": _normalize_text(overrides.get("cta") or brief.get("cta") or examples.get("cta"), default_brief["cta"]),
        }
        return candidate
    return default_brief


def _build_run_result(step, node, status, message, output):
    return {
        "step": step,
        "nodeId": _normalize_text(node.get("id"), f"step-{step}"),
        "nodeType": _normalize_text(node.get("type"), "agent"),
        "nodeTitle": _normalize_text(node.get("title"), _normalize_text(node.get("type"), f"Step {step}")),
        "status": status,
        "message": message,
        "output": output,
    }


def _resolve_workflow_workspace_path(workflow, input_payload=None, trigger=None):
    workflow = workflow if isinstance(workflow, dict) else {}
    candidates = []
    if isinstance(input_payload, dict):
        candidates.extend([
            input_payload.get("workspacePath"),
            input_payload.get("workspace_path"),
        ])
    if isinstance(trigger, dict):
        candidates.extend([
            trigger.get("workspacePath"),
            trigger.get("workspace_path"),
        ])
    candidates.extend([
        workflow.get("workspacePath"),
        workflow.get("workspace_path"),
    ])
    for candidate in candidates:
        normalized = _normalize_workspace_path(candidate)
        if normalized:
            return normalized
    return ""


def _get_run_output_dir(openclaw_dir, workflow_id, run_id, workspace_path=""):
    root_dir = Path(workspace_path or openclaw_dir).expanduser().resolve()
    path = root_dir / ".mission-control" / "workflows" / "artifacts" / _normalize_text(workflow_id, "workflow") / _normalize_text(run_id, "run")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(content or ""), encoding="utf-8")
    return path


def _seconds_to_srt_timestamp(value):
    total_ms = max(int(round(float(value or 0) * 1000)), 0)
    hours = total_ms // 3_600_000
    minutes = (total_ms % 3_600_000) // 60_000
    seconds = (total_ms % 60_000) // 1000
    millis = total_ms % 1000
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"


def _build_subtitle_srt(lines, total_duration_seconds):
    subtitles = _safe_list(lines)
    if not subtitles:
        return ""
    total_duration = max(int(total_duration_seconds or 0), len(subtitles) * 3, 6)
    slot = max(total_duration / max(len(subtitles), 1), 2.5)
    chunks = []
    for index, line in enumerate(subtitles, start=1):
        start = (index - 1) * slot
        end = min(index * slot, total_duration)
        text = _normalize_text(line)
        if not text:
            continue
        chunks.append(
            "\n".join(
                [
                    str(index),
                    f"{_seconds_to_srt_timestamp(start)} --> {_seconds_to_srt_timestamp(end)}",
                    text,
                    "",
                ]
            )
        )
    return "\n".join(chunks).strip()


def _build_script_markdown(brief, brand_context, script):
    beats = _safe_list(script.get("beats"))
    highlights = _safe_list(brand_context.get("highlights"))
    lines = [
        f"# 短视频脚本：{_normalize_text(brief.get('topic'), '未命名视频')}",
        "",
        "## 创意简报",
        f"- 平台：{_normalize_text(brief.get('platform'), '未设置')}",
        f"- 受众：{_normalize_text(brief.get('audience'), '未设置')}",
        f"- 时长：{brief.get('durationSeconds') or '--'} 秒",
        f"- 风格：{_normalize_text(brief.get('visualStyle'), '未设置')}",
        f"- CTA：{_normalize_text(brief.get('cta'), '未设置')}",
        "",
        "## 开场钩子",
        _normalize_text(script.get("hook"), "待生成"),
        "",
        "## 旁白",
        _normalize_text(script.get("voiceover"), "待生成"),
        "",
        "## 节奏拆分",
    ]
    if beats:
        lines.extend([f"{index}. {_normalize_text(item)}" for index, item in enumerate(beats, start=1)])
    else:
        lines.append("- 暂无节奏拆分")
    lines.extend(["", "## 品牌参考"])
    if highlights:
        lines.extend([f"- {_normalize_text(item)}" for item in highlights])
    else:
        lines.append("- 暂无品牌参考")
    return "\n".join(lines).strip() + "\n"


def _write_binary(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content or b"")
    return path


def _build_video_generation_prompt(brief, brand_context, script, storyboard, publish_pack):
    highlights = [item for item in _safe_list(brand_context.get("highlights")) if _normalize_text(item)]
    beats = [item for item in _safe_list(script.get("beats")) if _normalize_text(item)]
    storyboard_items = [item for item in _safe_list(storyboard.get("storyboard")) if isinstance(item, dict)]
    preview = publish_pack.get("preview") if isinstance(publish_pack.get("preview"), dict) else {}
    lines = [
        f"Create a polished short-form vertical product video for {_normalize_text(brief.get('platform'), 'social media')}.",
        f"Topic: {_normalize_text(brief.get('topic'), 'OpenClaw Team product walkthrough')}.",
        f"Target audience: {_normalize_text(brief.get('audience'), 'product and operations teams')}.",
        f"Duration: {brief.get('durationSeconds') or 45} seconds.",
        f"Visual style: {_normalize_text(brief.get('visualStyle'), 'clean white desktop UI, quick cuts, modern product cinematography')}.",
        f"CTA: {_normalize_text(brief.get('cta'), 'invite viewers to learn more')}.",
        "",
        "Narrative direction:",
        f"- Hook: {_normalize_text(script.get('hook'), 'Show how multiple tools collapse into one desktop workflow.')}",
        f"- Voiceover: {_normalize_text(script.get('voiceover'), 'Introduce the product clearly and confidently.')}",
    ]
    if beats:
        lines.append("- Story beats:")
        lines.extend([f"  {index}. {_normalize_text(item)}" for index, item in enumerate(beats, start=1)])
    if storyboard_items:
        lines.append("- Shot list:")
        for index, item in enumerate(storyboard_items, start=1):
            lines.append(
                f"  {index}. {_normalize_text(item.get('shot'), 'Desktop product shot')} | "
                f"{_normalize_text(item.get('subtitle'), 'Show concise on-screen caption')}"
            )
    if highlights:
        lines.append("- Brand/product highlights to weave in:")
        lines.extend([f"  - {_normalize_text(item)}" for item in highlights[:5]])
    if preview:
        lines.append("- Publish framing:")
        lines.append(f"  - Title: {_normalize_text(preview.get('title'))}")
        lines.append(f"  - Caption: {_normalize_text(preview.get('caption'))}")
        lines.append(f"  - Hashtags: {_normalize_text(preview.get('hashtags'))}")
    lines.extend(
        [
            "",
            "Output guidance:",
            "- Vertical 9:16 framing, screen-first product storytelling.",
            "- Keep motion brisk and premium, with readable UI moments.",
            "- Do not add watermarks or unrelated branding.",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _build_cover_poster_svg(brief, script, publish_pack):
    preview = publish_pack.get("preview") if isinstance(publish_pack.get("preview"), dict) else {}
    title = _normalize_text(preview.get("title"), _normalize_text(brief.get("topic"), "短视频封面"))
    caption = _normalize_text(preview.get("caption"), _normalize_text(script.get("hook"), "把多工具协作收进一个桌面工作台"))
    hashtags = _normalize_text(preview.get("hashtags"), "#AI工作流 #企业协作")
    def _escape(value):
        return (
            str(value or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
    return "\n".join(
        [
            '<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="1920" viewBox="0 0 1080 1920" fill="none">',
            '  <defs>',
            '    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">',
            '      <stop offset="0%" stop-color="#f6fbf8"/>',
            '      <stop offset="100%" stop-color="#e4f5eb"/>',
            '    </linearGradient>',
            '  </defs>',
            '  <rect width="1080" height="1920" rx="56" fill="url(#bg)"/>',
            '  <rect x="88" y="164" width="904" height="560" rx="36" fill="#ffffff" stroke="#d5eadd" stroke-width="4"/>',
            '  <rect x="140" y="228" width="800" height="36" rx="18" fill="#1ead6f" opacity="0.14"/>',
            '  <rect x="140" y="298" width="620" height="28" rx="14" fill="#0f172a" opacity="0.09"/>',
            '  <rect x="140" y="348" width="540" height="28" rx="14" fill="#0f172a" opacity="0.06"/>',
            '  <rect x="140" y="430" width="364" height="210" rx="28" fill="#ebfff4" stroke="#c5ebd4"/>',
            '  <rect x="530" y="430" width="410" height="210" rx="28" fill="#f8fafc" stroke="#dbe4ef"/>',
            '  <text x="100" y="860" fill="#1ead6f" font-size="34" font-family="PingFang SC, Helvetica Neue, Arial, sans-serif" font-weight="700">OpenClaw Team · 短视频制作</text>',
            f'  <text x="100" y="960" fill="#07111f" font-size="92" font-family="PingFang SC, Helvetica Neue, Arial, sans-serif" font-weight="800">{_escape(title)}</text>',
            f'  <text x="100" y="1080" fill="#334155" font-size="42" font-family="PingFang SC, Helvetica Neue, Arial, sans-serif">{_escape(caption)}</text>',
            f'  <text x="100" y="1180" fill="#1f7a4d" font-size="34" font-family="PingFang SC, Helvetica Neue, Arial, sans-serif">{_escape(hashtags)}</text>',
            '  <text x="100" y="1360" fill="#0f172a" font-size="40" font-family="PingFang SC, Helvetica Neue, Arial, sans-serif">桌面工作台 · 工作流 · 语音自动化 · 交付闭环</text>',
            '  <text x="100" y="1460" fill="#475569" font-size="30" font-family="PingFang SC, Helvetica Neue, Arial, sans-serif">本封面预览由工作流自动生成，可直接交给剪辑与发布团队。</text>',
            '  <rect x="100" y="1560" width="320" height="92" rx="46" fill="#1ead6f"/>',
            '  <text x="164" y="1618" fill="#ffffff" font-size="36" font-family="PingFang SC, Helvetica Neue, Arial, sans-serif" font-weight="700">获取产品体验</text>',
            '</svg>',
            "",
        ]
    )


def _http_response_bytes(url, *, method="GET", headers=None, data=None, timeout=120):
    request = Request(url, headers=headers or {}, data=data, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read(), int(response.status), dict(response.headers.items())
    except HTTPError as error:
        body = b""
        try:
            body = error.read()
        except Exception:
            body = b""
        detail = body.decode("utf-8", "replace").strip()
        raise RuntimeError(detail or f"HTTP {error.code}") from error
    except URLError as error:
        raise RuntimeError(str(error.reason)) from error


def _http_response_json(url, *, method="GET", headers=None, data=None, timeout=120):
    body, status, response_headers = _http_response_bytes(
        url,
        method=method,
        headers=headers,
        data=data,
        timeout=timeout,
    )
    if not body:
        return {}, status, response_headers
    try:
        return json.loads(body.decode("utf-8", "replace")), status, response_headers
    except json.JSONDecodeError as exc:
        raise RuntimeError("视频服务返回了不可解析的响应。") from exc


def _encode_multipart_form(fields):
    boundary = f"----MissionControlWorkflow{uuid.uuid4().hex}"
    chunks = []
    for key, value in fields.items():
        if value is None:
            continue
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                str(value).encode("utf-8"),
                b"\r\n",
            ]
        )
    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    return boundary, b"".join(chunks)


def _extract_video_payload(response_payload):
    payload = response_payload if isinstance(response_payload, dict) else {}
    if isinstance(payload.get("data"), dict):
        payload = payload.get("data")
    elif isinstance(payload.get("data"), list) and payload.get("data"):
        first = payload.get("data")[0]
        if isinstance(first, dict):
            payload = first
    return payload if isinstance(payload, dict) else {}


def _normalize_video_job_status(value):
    normalized = _normalize_text(value, "queued").lower()
    if normalized in {"succeeded", "success", "completed", "complete", "ready"}:
        return "completed"
    if normalized in {"failed", "error", "cancelled", "canceled"}:
        return "failed"
    if normalized in {"processing", "running", "in_progress"}:
        return "processing"
    return normalized or "queued"


def _summarize_video_error(error):
    normalized = _normalize_text(error)
    if not normalized:
        return ""
    try:
        parsed = json.loads(normalized)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict):
        payload = parsed.get("error") if isinstance(parsed.get("error"), dict) else parsed
        code = _normalize_text(payload.get("code")).lower()
        message = _normalize_text(payload.get("message"))
        if code == "billing_hard_limit_reached":
            return "视频生成服务额度已用尽，请先恢复 OpenAI 账户账单额度。"
        if message:
            return message
    return normalized


def _normalize_video_seconds(value):
    seconds = max(int(_normalize_float(value, 12)), 4)
    if seconds <= 4:
        return 4
    if seconds <= 8:
        return 8
    return 12


def _normalize_video_size(value, brief):
    requested = _normalize_text(value, "720x1280")
    platform = _normalize_text((brief or {}).get("platform")).lower()
    if requested == "1280x720" and any(keyword in platform for keyword in ("视频号", "抖音", "reels", "short", "shorts", "vertical")):
        return "720x1280"
    return requested


def _generate_video_assets(openclaw_dir, brief, brand_context, script, storyboard, publish_pack, config, run_output_dir):
    from backend.application.services.runtime_core import openclaw_command_env

    prompt_path = _write_text(
        run_output_dir / "video-prompt.txt",
        _build_video_generation_prompt(brief, brand_context, script, storyboard, publish_pack),
    )
    poster_path = _write_text(
        run_output_dir / "cover-poster.svg",
        _build_cover_poster_svg(brief, script, publish_pack),
    )

    provider = _normalize_text(config.get("provider"), "sora")
    model = _normalize_text(config.get("model"), "sora-2")
    if model == "sora":
        model = "sora-2"
    size = _normalize_video_size(config.get("size"), brief)
    api_key_env = _normalize_text(config.get("apiKeyEnv"), "OPENAI_API_KEY")
    base_url = _normalize_text(config.get("baseUrl"), "https://api.openai.com/v1")
    env = openclaw_command_env(openclaw_dir)
    api_key = _normalize_text(env.get(api_key_env))
    poll_attempts = max(int(_normalize_float(config.get("pollAttempts"), 6)), 1)
    poll_interval = max(float(_normalize_float(config.get("pollIntervalSeconds"), 5)), 1.0)
    desired_seconds = max(int(_normalize_float(brief.get("durationSeconds"), 12)), 4)
    seconds = _normalize_video_seconds(config.get("seconds") or desired_seconds)

    job_path = run_output_dir / "video-job.json"
    result = {
        "provider": provider,
        "model": model,
        "size": size,
        "desiredSeconds": desired_seconds,
        "seconds": seconds,
        "promptArtifact": prompt_path.name,
        "posterArtifact": poster_path.name,
        "configured": bool(api_key),
        "status": "skipped",
        "artifact": job_path.name,
        "path": str(job_path),
    }

    if not api_key:
        result["reason"] = "missing_api_key"
        result["message"] = "未配置 OPENAI_API_KEY，已保留视频任务清单与封面预览。"
        _write_json(job_path, result)
        return result, [prompt_path, poster_path, job_path]

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    fields = {
        "model": model,
        "prompt": prompt_path.read_text(encoding="utf-8"),
        "seconds": seconds,
        "size": size,
    }
    boundary, body = _encode_multipart_form(fields)
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    video_id = ""
    try:
        response_payload, _, _ = _http_response_json(
            f"{base_url.rstrip('/')}/videos",
            method="POST",
            headers=headers,
            data=body,
            timeout=180,
        )
        job_payload = _extract_video_payload(response_payload)
        video_id = _normalize_text(job_payload.get("id"))
        result.update(
            {
                "videoId": video_id,
                "status": _normalize_video_job_status(job_payload.get("status")),
                "createdAt": job_payload.get("created_at") or job_payload.get("createdAt") or _now_iso(),
            }
        )
        latest_payload = job_payload
        attempts_used = 0
        while video_id and result["status"] in {"queued", "processing"} and attempts_used < poll_attempts:
            attempts_used += 1
            time.sleep(poll_interval)
            polled_payload, _, _ = _http_response_json(
                f"{base_url.rstrip('/')}/videos/{video_id}",
                headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
                timeout=120,
            )
            latest_payload = _extract_video_payload(polled_payload) or latest_payload
            result["status"] = _normalize_video_job_status(latest_payload.get("status"))
            result["checkedAt"] = _now_iso()

        if video_id and result["status"] == "completed":
            video_bytes, _, _ = _http_response_bytes(
                f"{base_url.rstrip('/')}/videos/{video_id}/content?variant=video",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=300,
            )
            video_path = _write_binary(run_output_dir / "final-video.mp4", video_bytes)
            result.update(
                {
                    "artifact": video_path.name,
                    "path": str(video_path),
                    "bytes": video_path.stat().st_size,
                    "videoReady": True,
                }
            )
            try:
                thumb_bytes, _, thumb_headers = _http_response_bytes(
                    f"{base_url.rstrip('/')}/videos/{video_id}/content?variant=thumbnail",
                    headers={"Authorization": f"Bearer {api_key}"},
                    timeout=120,
                )
                ext = ".webp"
                content_type = _normalize_text((thumb_headers or {}).get("Content-Type")).lower()
                if "png" in content_type:
                    ext = ".png"
                elif "jpeg" in content_type or "jpg" in content_type:
                    ext = ".jpg"
                thumbnail_path = _write_binary(run_output_dir / f"thumbnail{ext}", thumb_bytes)
                result["thumbnailArtifact"] = thumbnail_path.name
                result["thumbnailPath"] = str(thumbnail_path)
            except Exception as thumbnail_error:
                result["thumbnailError"] = str(thumbnail_error)
            _write_json(job_path, {**result, "response": latest_payload})
            created = [prompt_path, poster_path, job_path, video_path]
            if result.get("thumbnailPath"):
                created.append(Path(result["thumbnailPath"]))
            return result, created
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        result["message"] = _summarize_video_error(exc)

    _write_json(job_path, result)
    return result, [prompt_path, poster_path, job_path]


def _copy_audio_into_output(audio_path, output_path):
    source = Path(audio_path).expanduser()
    if not source.exists():
        return None
    try:
        if source.resolve() == output_path.resolve():
            return output_path
    except OSError:
        pass
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, output_path)
    return output_path


def _generate_fallback_voiceover_file(output_path, duration_seconds=6):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sample_rate = 16000
    duration = max(min(int(duration_seconds or 6), 12), 3)
    total_frames = sample_rate * duration
    amplitude = 2200
    frequency = 440.0
    with wave.open(str(output_path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        frames = bytearray()
        for index in range(total_frames):
            envelope = 0.35 if index < sample_rate else 0.18
            sample = int(amplitude * envelope * math.sin((2.0 * math.pi * frequency * index) / sample_rate))
            frames.extend(struct.pack("<h", sample))
        wav_file.writeframes(bytes(frames))
    return output_path


def _generate_real_voiceover(openclaw_dir, brief, script, config, run_output_dir):
    voice_text = _normalize_text(
        script.get("voiceover"),
        f"今天用 {brief.get('durationSeconds') or 45} 秒，带你了解 {_normalize_text(brief.get('topic'), '本次内容')}。",
    )
    voice = _normalize_text(config.get("voice"), "zf_001")
    target_name = "narration.wav"
    target_path = run_output_dir / target_name
    fallback_reason = ""
    try:
        from backend.application.services.customer_access_core import generate_customer_voice_reply_audio

        source_audio = generate_customer_voice_reply_audio(openclaw_dir, voice_text, voice=voice)
        copied = _copy_audio_into_output(source_audio, target_path)
        if copied is None:
            raise RuntimeError("旁白音频生成成功，但未找到输出文件。")
        return {
            "toolId": _normalize_text(config.get("toolId"), "speech.generate"),
            "voice": voice,
            "format": _normalize_text(config.get("format"), copied.suffix.lstrip(".")),
            "artifact": copied.name,
            "path": str(copied),
            "durationSeconds": brief.get("durationSeconds"),
            "text": voice_text,
            "bytes": copied.stat().st_size,
            "fallback": False,
        }
    except Exception as exc:
        fallback_reason = str(exc)

    generated = _generate_fallback_voiceover_file(target_path, min(int(brief.get("durationSeconds") or 6), 8))
    return {
        "toolId": _normalize_text(config.get("toolId"), "speech.generate"),
        "voice": voice,
        "format": "wav",
        "artifact": generated.name,
        "path": str(generated),
        "durationSeconds": brief.get("durationSeconds"),
        "text": voice_text,
        "bytes": generated.stat().st_size,
        "fallback": True,
        "fallbackReason": fallback_reason,
    }


def _materialize_video_deliverables(openclaw_dir, workflow, run, context):
    brief = context.get("brief") if isinstance(context.get("brief"), dict) else {}
    brand_context = context.get("brandContext") if isinstance(context.get("brandContext"), dict) else {}
    script = context.get("script") if isinstance(context.get("script"), dict) else {}
    storyboard = context.get("storyboard") if isinstance(context.get("storyboard"), dict) else {}
    publish_pack = context.get("publishPack") if isinstance(context.get("publishPack"), dict) else {}
    voiceover = context.get("voiceover") if isinstance(context.get("voiceover"), dict) else {}
    final = context.get("final") if isinstance(context.get("final"), dict) else {}
    workspace_path = _resolve_workflow_workspace_path(
        workflow,
        input_payload=context.get("inputs"),
        trigger=context.get("trigger"),
    )
    output_dir = _get_run_output_dir(openclaw_dir, workflow.get("id"), run.get("id"), workspace_path=workspace_path)

    created_files = []

    script_path = _write_text(output_dir / "script.md", _build_script_markdown(brief, brand_context, script))
    created_files.append(script_path)

    storyboard_payload = {
        "brief": brief,
        "storyboard": _safe_list(storyboard.get("storyboard")),
        "assetChecklist": _safe_list(storyboard.get("assetChecklist")),
    }
    storyboard_path = _write_json(output_dir / "storyboard.json", storyboard_payload)
    created_files.append(storyboard_path)

    subtitle_srt = _build_subtitle_srt(
        storyboard.get("subtitleDraft"),
        brief.get("durationSeconds") or 45,
    )
    subtitle_path = _write_text(output_dir / "subtitle.srt", subtitle_srt)
    created_files.append(subtitle_path)

    publish_payload = {
        "preview": publish_pack.get("preview") if isinstance(publish_pack.get("preview"), dict) else {},
        "assignments": publish_pack.get("assignments") if isinstance(publish_pack.get("assignments"), dict) else {},
        "topic": brief.get("topic"),
        "platform": brief.get("platform"),
    }
    publish_path = _write_json(output_dir / "publish-pack.json", publish_payload)
    created_files.append(publish_path)

    cover_copy = publish_payload.get("preview") or {}
    cover_text = "\n".join(
        [
            _normalize_text(cover_copy.get("title"), _normalize_text(brief.get("topic"), "短视频封面")),
            "",
            _normalize_text(cover_copy.get("caption"), _normalize_text(script.get("hook"), "")),
            "",
            _normalize_text(cover_copy.get("hashtags"), ""),
        ]
    ).strip()
    cover_path = _write_text(output_dir / "cover-copy.txt", cover_text + ("\n" if cover_text else ""))
    created_files.append(cover_path)

    if voiceover.get("path"):
        copied_voice = _copy_audio_into_output(voiceover.get("path"), output_dir / "narration.wav")
        if copied_voice is not None:
            voiceover = {
                **voiceover,
                "artifact": copied_voice.name,
                "path": str(copied_voice),
                "bytes": copied_voice.stat().st_size,
            }
            created_files.append(copied_voice)

    render_config = context.get("renderPackage") if isinstance(context.get("renderPackage"), dict) else {}
    video_render, video_files = _generate_video_assets(
        openclaw_dir,
        brief,
        brand_context,
        script,
        storyboard,
        publish_pack,
        render_config,
        output_dir,
    )
    for item in _safe_list(video_files):
        path = Path(item)
        if path.exists():
            created_files.append(path)

    render_zip = output_dir / "render-package.zip"
    with zipfile.ZipFile(render_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in created_files:
            if item.exists() and item.is_file():
                archive.write(item, arcname=item.name)

    render_package = {
        **render_config,
        "artifact": render_zip.name,
        "path": str(render_zip),
        "includes": [item.name for item in created_files if item.exists()],
        "nextStep": render_config.get("nextStep") or (
            "视频已生成，可直接进入审阅与发布。"
            if video_render.get("videoReady")
            else "渲染包已准备完成，可直接交给视频生成或剪辑链路。"
        ),
    }
    context["renderPackage"] = render_package
    context["voiceover"] = voiceover
    context["videoRender"] = video_render

    deliverable_names = []
    for item in _safe_list(final.get("deliverables")):
        normalized = _normalize_text(item)
        if normalized and normalized not in deliverable_names:
            deliverable_names.append(normalized)
    for item in created_files:
        if item.exists() and item.name not in deliverable_names:
            deliverable_names.append(item.name)
    deliverable_records = []
    for name in deliverable_names:
        target = output_dir / _normalize_text(name)
        if target.exists():
            deliverable_records.append(
                {
                    "name": target.name,
                    "path": str(target),
                    "size": target.stat().st_size,
                }
            )
    deliverable_records.append(
        {
            "name": render_zip.name,
            "path": str(render_zip),
            "size": render_zip.stat().st_size,
        }
    )
    context["final"] = {
        **final,
        "deliverables": [item.get("name") for item in deliverable_records],
        "deliverableRecords": deliverable_records,
        "outputDir": str(output_dir),
    }
    run["outputDir"] = str(output_dir)
    if workspace_path:
        run["workspacePath"] = workspace_path


def _simulate_generic_step(node, step, context):
    node_type = _normalize_text(node.get("type"), "agent")
    node_title = _normalize_text(node.get("title"), f"Step {step}")
    config = node.get("config") if isinstance(node.get("config"), dict) else {}

    if node_type in {"start", "trigger-schedule", "trigger-webhook"}:
        input_overrides = _normalize_run_inputs(context.get("inputs"))
        payload = {
            "entryType": node_type,
            "inputs": {
                **_inputs_to_example_map(config.get("inputs")),
                **input_overrides,
            },
            "path": _normalize_text(config.get("path"), "/api/workflows/trigger"),
            "cron": _normalize_text(config.get("cron"), ""),
        }
        context["latest"] = payload
        return _build_run_result(step, node, "success", f"{node_title} 已准备触发输入。", payload)

    if node_type == "knowledge-retrieval":
        output = {
            "query": _normalize_text(config.get("query"), context.get("latest", {}).get("topic", "workflow context")),
            "sources": _safe_list(config.get("sources")) or ["knowledge-base", "deliverables"],
            "highlights": [
                "提取到 3 条相关背景资料",
                "发现 1 条可复用的历史流程片段",
            ],
        }
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已完成资料检索。", output)

    if node_type in {"agent", "llm"}:
        output = {
            "summary": f"{node_title} 已生成下一步建议。",
            "draft": f"基于当前上下文，建议优先推进「{context.get('latest', {}).get('query', '主任务')}」并输出可执行结果。",
        }
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已生成内容草案。", output)

    if node_type == "if-else":
        rules = _safe_list(config.get("rules"))
        branch = _normalize_text((rules[0] or {}).get("label"), "default") if rules else "default"
        output = {"branch": branch, "reason": "当前示例数据命中了首个可用分支。"}
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已路由到 {branch} 分支。", output)

    if node_type == "code":
        output = {
            "transform": _normalize_text(node_title),
            "result": {
                "normalized": True,
                "fields": _safe_list(config.get("outputs")) or ["result"],
            },
        }
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已完成数据整理。", output)

    if node_type == "variable-assigner":
        assignments = {}
        for item in _safe_list(config.get("assignments")):
            if not isinstance(item, dict):
                continue
            key = _normalize_text(item.get("name") or item.get("key") or item.get("variable"))
            if not key:
                continue
            assignments[key] = item.get("value")
        output = {"assignments": assignments}
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已写入 {len(assignments)} 个变量。", output)

    if node_type == "http-request":
        output = {
            "method": _normalize_text(config.get("method"), "POST"),
            "url": _normalize_text(config.get("url"), "https://example.com/webhook"),
            "statusCode": 202,
        }
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已完成外部回调。", output)

    if node_type == "tool":
        output = {
            "toolId": _normalize_text(config.get("toolId"), "tool.run"),
            "result": "Tool execution completed",
        }
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已完成工具执行。", output)

    if node_type == "end":
        output = {"summary": "Workflow finished successfully."}
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已输出最终结果。", output)

    output = {"result": "Step completed"}
    context["latest"] = output
    return _build_run_result(step, node, "success", f"{node_title} 执行完成。", output)


def _simulate_video_step(node, step, context):
    node_type = _normalize_text(node.get("type"), "agent")
    node_title = _normalize_text(node.get("title"), f"Step {step}")
    config = node.get("config") if isinstance(node.get("config"), dict) else {}
    brief = context.setdefault("brief", _extract_video_brief(context.get("workflow"), context.get("inputs")))

    if node_type == "start":
        output = {
            "brief": brief,
            "goal": f"为 {brief['platform']} 生成 {brief['durationSeconds']} 秒短视频内容。",
        }
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已确认本次视频创意简报。", output)

    if node_type == "knowledge-retrieval":
        output = {
            "sources": _safe_list(config.get("sources")) or ["brand-voice", "product-highlights", "winning-short-clips"],
            "highlights": [
                "品牌语气偏直接、干净、结果导向。",
                "推荐突出“统一工作流、消息、自动化”的产品卖点。",
                "历史高表现内容更偏 3 秒钩子 + 3 段式快节奏结构。",
            ],
        }
        context["brandContext"] = output
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已取回品牌语气和高表现样本。", output)

    if node_type in {"agent", "llm"}:
        output = {
            "hook": f"还在用 4 个工具来回切？{brief['platform']} 这条 45 秒短视频帮你一次讲清。",
            "beats": [
                "前 3 秒：痛点钩子，强调信息割裂。",
                "中段：展示工作流、消息、自动化一体化工作台。",
                f"结尾：落回 CTA，邀请用户{brief['cta']}。",
            ],
            "voiceover": f"今天用 {brief['durationSeconds']} 秒，带你看懂 OpenClaw Team 如何把团队协作、工作流和自动化放进一个桌面工作台。",
            "cta": brief["cta"],
        }
        context["script"] = output
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已生成脚本、钩子和旁白。", output)

    if node_type == "code":
        output = {
            "storyboard": [
                {"scene": 1, "shot": "桌面消息和任务快速切换", "subtitle": "信息分散，跟进变慢"},
                {"scene": 2, "shot": "切到工作流编辑器展示自动化编排", "subtitle": "把流程直接画进工作台"},
                {"scene": 3, "shot": "展示消息渠道、语音、执行闭环", "subtitle": "从消息到执行，一条链跑通"},
            ],
            "subtitleDraft": [
                "还在用 4 个工具来回切？",
                "OpenClaw Team 把消息、工作流和自动化统一到一个桌面工作台。",
                brief["cta"],
            ],
            "assetChecklist": ["产品截图", "工作流画布录屏", "语音样本", "品牌封面素材"],
        }
        context["storyboard"] = output
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已拆解出分镜、字幕和素材清单。", output)

    if node_type == "tool":
        tool_id = _normalize_text(config.get("toolId"))
        if "speech" in tool_id or "旁白" in node_title:
            run_output_dir = context.setdefault(
                "runOutputDir",
                _get_run_output_dir(
                    context.get("openclaw_dir"),
                    context.get("workflow", {}).get("id"),
                    context.get("run", {}).get("id"),
                    workspace_path=context.get("workspacePath", ""),
                ),
            )
            output = _generate_real_voiceover(
                context.get("openclaw_dir"),
                brief,
                context.get("script") if isinstance(context.get("script"), dict) else {},
                config,
                run_output_dir,
            )
            context["voiceover"] = output
            context["latest"] = output
            message = f"{node_title} 已生成旁白音频文件。"
            if output.get("fallback"):
                message = f"{node_title} 已生成本地占位旁白文件。"
            return _build_run_result(step, node, "success", message, output)

        if "video" in tool_id or "render" in tool_id or "渲染" in node_title:
            output = {
                "toolId": tool_id or "video.render-package",
                "provider": _normalize_text(config.get("provider"), "sora"),
                "model": _normalize_text(config.get("model"), "sora-2"),
                "size": _normalize_video_size(config.get("size"), brief),
                "pollAttempts": max(int(_normalize_float(config.get("pollAttempts"), 6)), 1),
                "pollIntervalSeconds": max(float(_normalize_float(config.get("pollIntervalSeconds"), 5)), 1.0),
                "artifact": "render-package.zip",
                "includes": [
                    "script.md",
                    "storyboard.json",
                    "narration.wav",
                    "subtitle.srt",
                    "cover-copy.txt",
                    "video-prompt.txt",
                    "cover-poster.svg",
                    "video-job.json",
                ],
                "nextStep": "最终交付节点会尝试发起视频生成任务，并把结果组装成渲染包。",
            }
            context["renderPackage"] = output
            context["latest"] = output
            return _build_run_result(step, node, "success", f"{node_title} 已准备渲染包和视频任务参数。", output)

    if node_type == "variable-assigner":
        assignments = {}
        for item in _safe_list(config.get("assignments")):
            if not isinstance(item, dict):
                continue
            key = _normalize_text(item.get("name") or item.get("key") or item.get("variable"))
            if key:
                assignments[key] = item.get("value")
        output = {
            "assignments": assignments,
            "preview": {
                "title": assignments.get("publishTitle"),
                "caption": assignments.get("publishCaption"),
                "hashtags": assignments.get("hashtags"),
            },
        }
        context["publishPack"] = output
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已生成发布文案和话题标签。", output)

    if node_type == "end":
        deliverables = _safe_list(config.get("deliverables")) or ["script.md", "storyboard.json", "narration.wav", "subtitle.srt", "publish-pack.json"]
        output = {
            "deliverables": deliverables,
            "handoff": "ready-for-review",
            "reviewChecklist": [
                "确认钩子是否在 3 秒内给出核心痛点",
                "确认镜头和旁白节奏是否与平台匹配",
                "确认 CTA 与发布文案一致",
            ],
        }
        context["final"] = output
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已输出可交付的视频制作包。", output)

    return _simulate_generic_step(node, step, context)


def _extract_feishu_doc_config(workflow, inputs):
    """Extract feishu-to-wechat document configuration from workflow start node and run inputs."""
    default_config = {
        "docUrl": "https://example.feishu.cn/docx/demo-doc-id",
        "author": "OpenClaw Team",
        "thumbMediaId": "",
        "styleProfile": "doocs",
        "theme": "grace",
        "fontSize": 14,
    }
    overrides = inputs if isinstance(inputs, dict) else {}
    nodes = _safe_list(workflow.get("nodes")) if isinstance(workflow, dict) else []
    start_config = {}
    for n in nodes:
        if _normalize_text(n.get("type")) == "start":
            start_config = n.get("config") if isinstance(n.get("config"), dict) else {}
            break
    examples = _inputs_to_example_map(start_config.get("inputs"))
    return {
        "docUrl": _normalize_text(overrides.get("docUrl") or examples.get("docUrl"), default_config["docUrl"]),
        "author": _normalize_text(overrides.get("author") or examples.get("author"), default_config["author"]),
        "thumbMediaId": _normalize_text(overrides.get("thumbMediaId") or examples.get("thumbMediaId"), ""),
        "styleProfile": _normalize_text(overrides.get("styleProfile") or examples.get("styleProfile"), default_config["styleProfile"]),
        "theme": _normalize_text(overrides.get("theme") or examples.get("theme"), default_config["theme"]),
        "fontSize": int(_normalize_float(overrides.get("fontSize") or examples.get("fontSize"), default_config["fontSize"])),
    }


def _simulate_feishu_wechat_step(node, step, context):
    """Simulate execution of a feishu-to-wechat workflow node."""
    node_type = _normalize_text(node.get("type"), "agent")
    node_title = _normalize_text(node.get("title"), f"Step {step}")
    config = node.get("config") if isinstance(node.get("config"), dict) else {}
    doc_config = context.setdefault("docConfig", _extract_feishu_doc_config(context.get("workflow"), context.get("inputs")))

    if node_type == "start":
        output = {
            "docConfig": doc_config,
            "goal": f"将飞书文档发布到微信公众号，排版方案: {doc_config['styleProfile']}，主题: {doc_config['theme']}。",
        }
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已确认文档发布参数。", output)

    if node_type == "tool":
        tool_id = _normalize_text(config.get("toolId"))

        if "feishu" in tool_id or "飞书" in node_title:
            doc_title = "用 Hermes 封装飞书文档发布技能" if "demo" in doc_config["docUrl"] else f"飞书文档 ({doc_config['docUrl'].split('/')[-1]})"
            output = {
                "documentMeta": {
                    "title": doc_title,
                    "author": doc_config["author"],
                    "createdAt": datetime.now(timezone.utc).isoformat(),
                    "wordCount": 2450,
                    "blockCount": 37,
                },
                "documentBlocks": [
                    {"type": "heading1", "content": doc_title},
                    {"type": "paragraph", "content": "本文介绍如何将飞书文档一键发布到微信公众号…"},
                    {"type": "heading2", "content": "前置准备"},
                    {"type": "paragraph", "content": "使用之前需要准备飞书 CLI 和微信公众号凭证。"},
                    {"type": "code", "language": "bash", "content": "npm install -g @larksuiteoapi/lark-cli"},
                    {"type": "heading2", "content": "技术实现"},
                    {"type": "paragraph", "content": "工具的工作流程分为获取文档、格式转换、HTML 渲染和发布草稿四步。"},
                    {"type": "table", "rows": 3, "cols": 2, "content": "步骤 | 说明"},
                    {"type": "image", "token": "img_v3_001", "width": 800, "height": 450},
                    {"type": "image", "token": "img_v3_002", "width": 600, "height": 400},
                ],
                "imageAssets": [
                    {"token": "img_v3_001", "name": "workflow-overview.png", "size": 245000, "mimeType": "image/png"},
                    {"token": "img_v3_002", "name": "code-example.png", "size": 128000, "mimeType": "image/png"},
                ],
            }
            context["feishuDoc"] = output
            context["latest"] = output
            return _build_run_result(step, node, "success", f"{node_title} 已获取飞书文档，共 {output['documentMeta']['blockCount']} 个内容块、{len(output['imageAssets'])} 张图片。", output)

        if "wechat" in tool_id or "素材" in node_title or "图片" in node_title:
            feishu_doc = context.get("feishuDoc") if isinstance(context.get("feishuDoc"), dict) else {}
            image_assets = _safe_list(feishu_doc.get("imageAssets"))
            image_url_map = {}
            for img in image_assets:
                token = _normalize_text(img.get("token"), f"img_{len(image_url_map)}")
                image_url_map[token] = f"https://mmbiz.qpic.cn/mmbiz_png/simulated_{token}/640?wx_fmt=png"
            output = {
                "uploadedCount": len(image_url_map),
                "totalSize": sum(img.get("size", 0) for img in image_assets),
                "imageUrlMap": image_url_map,
                "compressed": 0,
            }
            context["imageMap"] = output
            context["latest"] = output
            return _build_run_result(step, node, "success", f"{node_title} 已上传 {len(image_url_map)} 张图片到微信素材库。", output)

    if node_type == "code":
        feishu_doc = context.get("feishuDoc") if isinstance(context.get("feishuDoc"), dict) else {}
        image_map = context.get("imageMap") if isinstance(context.get("imageMap"), dict) else {}
        doc_meta = feishu_doc.get("documentMeta") if isinstance(feishu_doc.get("documentMeta"), dict) else {}
        doc_title = _normalize_text(doc_meta.get("title"), "飞书文档")

        if not context.get("markdown"):
            markdown_content = f"""# {doc_title}

## 前置准备

使用之前需要准备飞书 CLI 和微信公众号凭证。

```bash
npm install -g @larksuiteoapi/lark-cli
lark-cli login
```

## 技术实现

工具的工作流程分为获取文档、格式转换、HTML 渲染和发布草稿四步。

| 步骤 | 说明 |
|------|------|
| 获取文档 | 用 lark-cli 下载飞书文档内容和图片 |
| 格式转换 | 飞书格式标准化为 Markdown |
| HTML 渲染 | 按 Doocs 风格生成微信兼容 HTML |

> 本文由飞书文档自动转换生成。

![workflow-overview](img_v3_001)
![code-example](img_v3_002)
"""
            image_references = [
                {"token": "img_v3_001", "altText": "workflow-overview", "position": "inline"},
                {"token": "img_v3_002", "altText": "code-example", "position": "inline"},
            ]
            output = {
                "markdown": markdown_content,
                "imageReferences": image_references,
                "stats": {"paragraphs": 8, "codeBlocks": 1, "tables": 1, "images": 2, "headings": 3},
            }
            context["markdown"] = output
            context["latest"] = output
            return _build_run_result(step, node, "success", f"{node_title} 已将飞书文档转换为 Markdown，共 {output['stats']['paragraphs']} 段、{output['stats']['images']} 张图片。", output)

        else:
            style_options = config.get("styleOptions") if isinstance(config.get("styleOptions"), dict) else {}
            theme = _normalize_text(style_options.get("theme") or doc_config.get("theme"), "grace")
            font_size = int(_normalize_float(style_options.get("fontSize") or doc_config.get("fontSize"), 14))
            profile = _normalize_text(style_options.get("profile") or doc_config.get("styleProfile"), "doocs")
            url_map = image_map.get("imageUrlMap") if isinstance(image_map.get("imageUrlMap"), dict) else {}

            html_content = f"""<section style="font-size:{font_size}px;line-height:1.75;color:#333;font-family:-apple-system,BlinkMacSystemFont,PingFangSC,sans-serif;">
  <h1 style="font-size:1.6em;font-weight:bold;border-bottom:2px solid #1ead6f;padding-bottom:8px;">{doc_title}</h1>
  <h2 style="font-size:1.3em;font-weight:bold;background:#1ead6f;color:#fff;padding:4px 12px;border-radius:4px;display:inline-block;">前置准备</h2>
  <p style="text-align:justify;">使用之前需要准备飞书 CLI 和微信公众号凭证。</p>
  <pre style="background:#f6f8fa;border-radius:8px;padding:16px;overflow-x:auto;"><code>npm install -g @larksuiteoapi/lark-cli
lark-cli login</code></pre>
  <h2 style="font-size:1.3em;font-weight:bold;background:#1ead6f;color:#fff;padding:4px 12px;border-radius:4px;display:inline-block;">技术实现</h2>
  <p style="text-align:justify;">工具的工作流程分为获取文档、格式转换、HTML 渲染和发布草稿四步。</p>
  <table style="border-collapse:collapse;width:100%;margin:16px 0;"><thead><tr><th style="border:1px solid #ddd;padding:8px 12px;background:#f9f9f9;">步骤</th><th style="border:1px solid #ddd;padding:8px 12px;background:#f9f9f9;">说明</th></tr></thead>
  <tbody><tr><td style="border:1px solid #ddd;padding:8px 12px;">获取文档</td><td style="border:1px solid #ddd;padding:8px 12px;">用 lark-cli 下载飞书文档内容和图片</td></tr>
  <tr><td style="border:1px solid #ddd;padding:8px 12px;">格式转换</td><td style="border:1px solid #ddd;padding:8px 12px;">飞书格式标准化为 Markdown</td></tr>
  <tr><td style="border:1px solid #ddd;padding:8px 12px;">HTML 渲染</td><td style="border:1px solid #ddd;padding:8px 12px;">按 Doocs 风格生成微信兼容 HTML</td></tr></tbody></table>
  <blockquote style="border-left:4px solid #1ead6f;padding:8px 16px;color:#666;margin:16px 0;">本文由飞书文档自动转换生成。</blockquote>
</section>"""

            for token, cdn_url in url_map.items():
                html_content = html_content.replace(f"img_ref_{token}", cdn_url)

            preview_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{doc_title} - 预览</title></head><body style="max-width:680px;margin:40px auto;padding:0 20px;">{html_content}</body></html>"""

            output = {
                "htmlContent": html_content,
                "previewHtml": preview_html,
                "styleApplied": {
                    "profile": profile,
                    "theme": theme,
                    "fontSize": font_size,
                    "macCodeBlock": bool(style_options.get("macCodeBlock")),
                    "headingStyle": _normalize_text(style_options.get("headingStyle"), "solid"),
                    "codeTheme": _normalize_text(style_options.get("codeTheme"), "github"),
                },
                "stats": {"htmlLength": len(html_content), "imagesReplaced": len(url_map)},
            }
            context["htmlContent"] = output
            context["latest"] = output
            return _build_run_result(step, node, "success", f"{node_title} 已渲染 HTML（{profile}/{theme}，{font_size}px），替换 {len(url_map)} 张图片链接。", output)

    if node_type == "if-else":
        output = {"branch": "确认发布", "approved": True, "reason": "预览效果符合预期，自动确认发布。"}
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已确认排版效果，继续发布。", output)

    if node_type == "http-request":
        feishu_doc = context.get("feishuDoc") if isinstance(context.get("feishuDoc"), dict) else {}
        doc_meta = feishu_doc.get("documentMeta") if isinstance(feishu_doc.get("documentMeta"), dict) else {}
        draft_media_id = f"draft_{uuid.uuid4().hex[:16]}"
        output = {
            "statusCode": 200,
            "response": {
                "errcode": 0,
                "errmsg": "ok",
                "media_id": draft_media_id,
            },
            "draftMediaId": draft_media_id,
            "articleTitle": _normalize_text(doc_meta.get("title"), "飞书文档"),
            "author": doc_config.get("author", ""),
        }
        context["draftResult"] = output
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} 已创建公众号草稿（media_id: {draft_media_id[:12]}…）。", output)

    if node_type == "end":
        draft_result = context.get("draftResult") if isinstance(context.get("draftResult"), dict) else {}
        html_content = context.get("htmlContent") if isinstance(context.get("htmlContent"), dict) else {}
        output = {
            "deliverables": ["article.md", "article.html", "preview.html", "publish-result.json"],
            "draftMediaId": draft_result.get("draftMediaId", ""),
            "summary": f"文档「{draft_result.get('articleTitle', '飞书文档')}」已成功推送到公众号草稿箱。",
            "nextSteps": [
                "登录微信公众平台，在「草稿箱」中查看并编辑",
                "确认排版无误后点击「发布」",
            ],
        }
        context["final"] = output
        context["latest"] = output
        return _build_run_result(step, node, "success", f"{node_title} — {output['summary']}", output)

    return _simulate_generic_step(node, step, context)


def _materialize_feishu_wechat_deliverables(openclaw_dir, workflow, run, context):
    """Write feishu-to-wechat deliverable files to the run output directory."""
    doc_config = context.get("docConfig") if isinstance(context.get("docConfig"), dict) else {}
    feishu_doc = context.get("feishuDoc") if isinstance(context.get("feishuDoc"), dict) else {}
    markdown_data = context.get("markdown") if isinstance(context.get("markdown"), dict) else {}
    html_data = context.get("htmlContent") if isinstance(context.get("htmlContent"), dict) else {}
    draft_result = context.get("draftResult") if isinstance(context.get("draftResult"), dict) else {}
    final = context.get("final") if isinstance(context.get("final"), dict) else {}
    doc_meta = feishu_doc.get("documentMeta") if isinstance(feishu_doc.get("documentMeta"), dict) else {}

    workspace_path = _resolve_workflow_workspace_path(
        workflow,
        input_payload=context.get("inputs"),
        trigger=context.get("trigger"),
    )
    output_dir = _get_run_output_dir(openclaw_dir, workflow.get("id"), run.get("id"), workspace_path=workspace_path)

    created_files = []

    md_content = markdown_data.get("markdown", f"# {_normalize_text(doc_meta.get('title'), '飞书文档')}\n\n（文档内容）\n")
    md_path = _write_text(output_dir / "article.md", md_content)
    created_files.append(md_path)

    html_content = html_data.get("htmlContent", "<p>（渲染内容）</p>")
    html_path = _write_text(output_dir / "article.html", html_content)
    created_files.append(html_path)

    preview_html = html_data.get("previewHtml", f"<!DOCTYPE html><html><body>{html_content}</body></html>")
    preview_path = _write_text(output_dir / "preview.html", preview_html)
    created_files.append(preview_path)

    publish_result = {
        "draftMediaId": draft_result.get("draftMediaId", ""),
        "articleTitle": _normalize_text(doc_meta.get("title"), "飞书文档"),
        "author": doc_config.get("author", ""),
        "docUrl": doc_config.get("docUrl", ""),
        "publishedAt": datetime.now(timezone.utc).isoformat(),
        "styleApplied": html_data.get("styleApplied") if isinstance(html_data.get("styleApplied"), dict) else {},
    }
    publish_path = _write_json(output_dir / "publish-result.json", publish_result)
    created_files.append(publish_path)

    run.setdefault("artifacts", {})["createdFiles"] = [str(f) for f in created_files if f]


def _simulate_workflow_step(node, step, context):
    template_id = _normalize_text(context.get("workflow", {}).get("templateId"))
    if template_id == "short-video-production":
        return _simulate_video_step(node, step, context)
    if template_id == "feishu-to-wechat":
        return _simulate_feishu_wechat_step(node, step, context)
    return _simulate_generic_step(node, step, context)


def _build_run_artifacts(context):
    context = context if isinstance(context, dict) else {}
    final = context.get("final") if isinstance(context.get("final"), dict) else {}
    render_package = context.get("renderPackage") if isinstance(context.get("renderPackage"), dict) else {}
    publish_pack = context.get("publishPack") if isinstance(context.get("publishPack"), dict) else {}
    voiceover = context.get("voiceover") if isinstance(context.get("voiceover"), dict) else {}
    script = context.get("script") if isinstance(context.get("script"), dict) else {}
    storyboard = context.get("storyboard") if isinstance(context.get("storyboard"), dict) else {}
    brief = context.get("brief") if isinstance(context.get("brief"), dict) else {}
    brand_context = context.get("brandContext") if isinstance(context.get("brandContext"), dict) else {}

    deliverables = _safe_list(final.get("deliverables")) or _safe_list(render_package.get("includes"))
    artifacts = {
        "brief": brief,
        "brandContext": brand_context,
        "script": script,
        "storyboard": storyboard,
        "voiceover": voiceover,
        "videoRender": context.get("videoRender") if isinstance(context.get("videoRender"), dict) else {},
        "renderPackage": render_package,
        "publishPack": publish_pack,
        "final": final,
        "deliverables": deliverables,
        "workspacePath": _normalize_text(context.get("workspacePath"), ""),
        "workspaceAuthorized": bool(context.get("workspaceAuthorized")),
    }
    # Feishu-to-wechat specific artifacts
    doc_config = context.get("docConfig") if isinstance(context.get("docConfig"), dict) else {}
    if doc_config:
        artifacts["docConfig"] = doc_config
        artifacts["feishuDoc"] = context.get("feishuDoc") if isinstance(context.get("feishuDoc"), dict) else {}
        artifacts["markdown"] = context.get("markdown") if isinstance(context.get("markdown"), dict) else {}
        artifacts["htmlContent"] = context.get("htmlContent") if isinstance(context.get("htmlContent"), dict) else {}
        artifacts["imageMap"] = context.get("imageMap") if isinstance(context.get("imageMap"), dict) else {}
        artifacts["draftResult"] = context.get("draftResult") if isinstance(context.get("draftResult"), dict) else {}
    return artifacts


def _execute_workflow_run(openclaw_dir, workflow, run, inputs=None, trigger=None, workspace_path="", workspace_authorized=False, execution_mode="simulate"):
    try:
        resolved_workspace_path = _resolve_workflow_workspace_path(
            workflow,
            input_payload={
                **(_normalize_run_inputs(inputs) if isinstance(inputs, dict) else {}),
                **({"workspacePath": workspace_path} if workspace_path else {}),
            },
            trigger=trigger,
        )
        if not resolved_workspace_path:
            resolved_workspace_path = _normalize_workspace_path(workspace_path)
        run_output_dir = _get_run_output_dir(
            openclaw_dir,
            workflow.get("id"),
            run.get("id"),
            workspace_path=resolved_workspace_path,
        )
        context = {
            "workflow": workflow,
            "run": run,
            "latest": {},
            "openclaw_dir": openclaw_dir,
            "inputs": _normalize_run_inputs(inputs),
            "input": _normalize_run_inputs(inputs),
            "trigger": trigger if isinstance(trigger, dict) else {},
            "workspacePath": resolved_workspace_path,
            "workspaceAuthorized": _normalize_bool(workspace_authorized),
            "runOutputDir": run_output_dir,
            "nodes": {},
            "variables": {},
        }
        if resolved_workspace_path:
            run["workspacePath"] = resolved_workspace_path
            run["workspaceAuthorized"] = _normalize_bool(workspace_authorized)
        run["outputDir"] = str(run_output_dir)
        run["mode"] = execution_mode

        use_real = execution_mode == "real"

        # Build adjacency from edges for graph-based execution
        edges = _safe_list(workflow.get("edges"))
        nodes_list = _safe_list(workflow.get("nodes"))
        nodes_by_id = {n.get("id"): n for n in nodes_list if n.get("id")}

        def _outgoing_edges(node_id, label=None):
            """Return edges from node_id, optionally filtered by label."""
            return [e for e in edges if e.get("source") == node_id and (label is None or _normalize_text(e.get("label")) == label)]

        def _execute_single_node(node, step_index, ctx):
            """Execute one node (real or simulated) and update run results."""
            node_title = _normalize_text(node.get("title"), f"Step {step_index}")
            if use_real:
                running_result = _build_run_result(step_index, node, "running", f"{node_title} 执行中…", {})
                run["results"].append(running_result)
                run["summary"] = running_result.get("message", "")
                _save_to_disk(openclaw_dir)
                try:
                    import asyncio as _asyncio
                    from backend.domain.core.workflow_executor import execute_node
                    _loop = _asyncio.new_event_loop()
                    output = _loop.run_until_complete(execute_node(node, ctx))
                    _loop.close()
                    status = output.get("status", "success") if isinstance(output, dict) else "success"
                    if status == "skipped":
                        message = f"{node_title} 已跳过: {output.get('error', '')}"
                    else:
                        status = "success"
                        message = f"{node_title} 执行完成。"
                    run["results"][-1] = _build_run_result(step_index, node, status, message, output)
                except Exception as exc:
                    run["results"][-1] = _build_run_result(step_index, node, "failed", f"{node_title} 执行失败: {exc}", {"error": str(exc)})
                    run["summary"] = run["results"][-1].get("message", "")
                    _save_to_disk(openclaw_dir)
                    raise
                run["summary"] = run["results"][-1].get("message", "")
                _save_to_disk(openclaw_dir)
                time.sleep(0.05)
                return output if isinstance(output, dict) else {}
            else:
                result = _simulate_workflow_step(node, step_index, ctx)
                run["results"].append(result)
                run["summary"] = result.get("message", "")
                _save_to_disk(openclaw_dir)
                time.sleep(0.18)
                return result.get("output", {})

        def _walk_branch(start_node_id, ctx, step_counter):
            """Walk a linear branch from start_node_id until no more edges or a merge point."""
            current_id = start_node_id
            while current_id:
                node = nodes_by_id.get(current_id)
                if not node:
                    break
                step_counter[0] += 1
                output = _execute_single_node(node, step_counter[0], ctx)
                # Follow the single outgoing edge (no branching in sub-branch)
                out_edges = _outgoing_edges(current_id)
                current_id = out_edges[0].get("target") if len(out_edges) == 1 else None

        # ── Main execution: sequential with branch support ──
        step_counter = [0]
        visited = set()

        def _run_node(node_id):
            """Recursively execute the workflow graph from node_id."""
            if not node_id or node_id in visited:
                return
            visited.add(node_id)
            node = nodes_by_id.get(node_id)
            if not node:
                return
            step_counter[0] += 1
            node_type = _normalize_text(node.get("type"), "agent")
            output = _execute_single_node(node, step_counter[0], context)

            if node_type == "if-else":
                # Route to the branch matching the condition result
                matched_branch = _normalize_text((output or {}).get("branch"), "true")
                branch_edges = _outgoing_edges(node_id, matched_branch)
                if not branch_edges:
                    branch_edges = _outgoing_edges(node_id)  # fallback: follow any
                for edge in branch_edges:
                    _run_node(edge.get("target"))

            elif node_type == "iteration":
                # Loop: execute "loop-body" branch for each item, then "done"
                items = _safe_list((output or {}).get("items"))
                var_name = (output or {}).get("variable", "item")
                body_edges = _outgoing_edges(node_id, "loop-body")
                done_edges = _outgoing_edges(node_id, "done")
                iteration_results = []
                for i, item in enumerate(items):
                    context.setdefault("variables", {})[var_name] = item
                    context["variables"]["_index"] = i
                    context["latest"] = item if isinstance(item, dict) else {"value": item, "index": i}
                    # Walk the body branch (linear sub-chain)
                    for edge in body_edges:
                        body_visited = set()
                        _walk_linear(edge.get("target"), context, step_counter, body_visited)
                    iteration_results.append(context.get("latest", {}))
                # Store aggregated results
                context["latest"] = {"iterations": iteration_results, "count": len(items)}
                context.setdefault("variables", {})["_iteration_results"] = iteration_results
                # Continue to "done" branch
                for edge in done_edges:
                    _run_node(edge.get("target"))

            elif node_type == "parallel":
                # Fan-out: run each branch, collect results, then continue
                branches = _safe_list((output or {}).get("branches"))
                branch_results = {}
                saved_latest = context.get("latest", {})
                for branch_label in branches:
                    context["latest"] = saved_latest  # each branch starts from same input
                    branch_edges = _outgoing_edges(node_id, branch_label)
                    for edge in branch_edges:
                        branch_visited = set()
                        _walk_linear(edge.get("target"), context, step_counter, branch_visited)
                    branch_results[branch_label] = context.get("latest", {})
                # Merge results
                context["latest"] = {"branches": branch_results, "count": len(branches)}
                context.setdefault("variables", {})["_parallel_results"] = branch_results
                # Follow edges without a branch label (merge point)
                merge_edges = [e for e in _outgoing_edges(node_id) if not _normalize_text(e.get("label"))]
                for edge in merge_edges:
                    _run_node(edge.get("target"))

            else:
                # Default: follow all outgoing edges
                for edge in _outgoing_edges(node_id):
                    _run_node(edge.get("target"))

        def _walk_linear(node_id, ctx, counter, branch_visited):
            """Walk a linear sub-branch (for iteration body / parallel branch)."""
            current_id = node_id
            while current_id and current_id not in branch_visited:
                branch_visited.add(current_id)
                node = nodes_by_id.get(current_id)
                if not node:
                    break
                counter[0] += 1
                _execute_single_node(node, counter[0], ctx)
                out_edges = _outgoing_edges(current_id)
                current_id = out_edges[0].get("target") if len(out_edges) == 1 else None

        # Find start node and begin graph walk
        if edges and nodes_by_id:
            start_node = next((n for n in nodes_list if _normalize_text(n.get("type")) in ("start", "trigger-schedule", "trigger-webhook")), None)
            if start_node:
                _run_node(start_node.get("id"))
            else:
                # Fallback: sequential execution
                for index, node in enumerate(nodes_list, start=1):
                    _execute_single_node(node, index, context)
        else:
            # No edges defined — fall back to sequential
            for index, node in enumerate(nodes_list, start=1):
                _execute_single_node(node, index, context)

        if _normalize_text(workflow.get("templateId")) == "short-video-production":
            _materialize_video_deliverables(openclaw_dir, workflow, run, context)
        if _normalize_text(workflow.get("templateId")) == "feishu-to-wechat":
            _materialize_feishu_wechat_deliverables(openclaw_dir, workflow, run, context)

        run["status"] = "success"
        run["completedAt"] = datetime.now(timezone.utc).isoformat()
        run["outputs"] = context.get("latest", {})
        run["artifacts"] = _build_run_artifacts(context)
        _save_to_disk(openclaw_dir)
    except Exception as exc:
        run["status"] = "failed"
        run["completedAt"] = datetime.now(timezone.utc).isoformat()
        run["error"] = str(exc)
        _save_to_disk(openclaw_dir)


def start_workflow_run(openclaw_dir, workflow_id, input_payload=None, trigger=None, workspace_path="", workspace_authorized=False, execution_mode="simulate"):
    _load_from_disk(openclaw_dir)

    workflow = _workflows_db.get(workflow_id)
    if not workflow:
        raise KeyError("workflow_not_found")

    normalized_inputs = _normalize_run_inputs(input_payload)
    resolved_workspace_path = _resolve_workflow_workspace_path(
        workflow,
        input_payload={**normalized_inputs, **({"workspacePath": workspace_path} if workspace_path else {})},
        trigger=trigger,
    )
    if not resolved_workspace_path:
        resolved_workspace_path = _normalize_workspace_path(workspace_path)
    resolved_workspace_authorized = _normalize_bool(
        normalized_inputs.get("workspaceAuthorized")
        if "workspaceAuthorized" in normalized_inputs
        else normalized_inputs.get("workspace_authorized")
    )
    if not resolved_workspace_authorized and isinstance(trigger, dict):
        resolved_workspace_authorized = _normalize_bool(
            trigger.get("workspaceAuthorized") if trigger.get("workspaceAuthorized") is not None else trigger.get("workspace_authorized")
        )

    run_id = _generate_id()
    now = _now_iso()
    run = {
        "id": run_id,
        "workflowId": workflow_id,
        "status": "running",
        "startedAt": now,
        "completedAt": None,
        "results": [],
        "summary": "",
        "inputs": normalized_inputs,
        "trigger": trigger if isinstance(trigger, dict) else {},
        "workspacePath": resolved_workspace_path,
        "workspaceAuthorized": _normalize_bool(resolved_workspace_authorized),
    }

    _workflow_runs_db[run_id] = run
    workflow["lastRun"] = now
    workflow["runCount"] = workflow.get("runCount", 0) + 1
    workflow["updatedAt"] = now
    if resolved_workspace_path:
        workflow["workspacePath"] = resolved_workspace_path
        workflow["workspaceAuthorized"] = _normalize_bool(resolved_workspace_authorized)
    _save_to_disk(openclaw_dir)

    threading.Thread(
        target=_execute_workflow_run,
        args=(openclaw_dir, workflow, run, normalized_inputs, trigger, resolved_workspace_path, resolved_workspace_authorized, execution_mode),
        daemon=True,
    ).start()
    return workflow, run


def _get_openclaw_dir(handler):
    """Get openclaw directory from handler."""
    return getattr(handler.server, 'openclaw_dir', os.getcwd())


def handle_workflow_get_list(handler, services):
    """GET /api/workflows - List all workflows."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    workflows = list(_workflows_db.values())
    handler._send_json({"ok": True, "workflows": workflows})
    return True


def handle_workflow_get_meta(handler, services):
    handler._send_json({"ok": True, "meta": _workflow_meta_payload()})
    return True


def handle_workflow_get_detail(handler, services, workflow_id):
    """GET /api/workflows/{id} - Get workflow detail."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    workflow = _workflows_db.get(workflow_id)
    if not workflow:
        handler._send_json({"ok": False, "error": "not_found", "message": "工作流不存在"}, status=404)
        return True
    handler._send_json({"ok": True, "workflow": workflow})
    return True


def handle_workflow_create(handler, services):
    """POST /api/workflows - Create new workflow."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True

    workflow_id = _generate_id()
    now = _now_iso()
    
    normalized = _normalize_workflow_payload(body)
    workflow = {
        "id": workflow_id,
        **normalized,
        "createdAt": now,
        "updatedAt": now,
        "lastRun": None,
        "runCount": 0,
    }
    
    _workflows_db[workflow_id] = workflow
    _save_to_disk(openclaw_dir)
    
    handler._send_json({"ok": True, "workflow": workflow}, status=201)
    return True


def handle_workflow_update(handler, services, workflow_id):
    """PUT /api/workflows/{id} - Update workflow."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    workflow = _workflows_db.get(workflow_id)
    if not workflow:
        handler._send_json({"ok": False, "error": "not_found", "message": "工作流不存在"}, status=404)
        return True
    
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True
    
    workflow.update(_normalize_workflow_payload(body, existing=workflow))
    workflow["updatedAt"] = _now_iso()
    _save_to_disk(openclaw_dir)
    
    handler._send_json({"ok": True, "workflow": workflow})
    return True


def handle_workflow_export_dsl(handler, services, workflow_id):
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)

    workflow = _workflows_db.get(workflow_id)
    if not workflow:
        handler._send_json({"ok": False, "error": "not_found", "message": "工作流不存在"}, status=404)
        return True

    dsl_content = yaml.safe_dump(_workflow_to_dsl(workflow), allow_unicode=True, sort_keys=False)
    handler._send_json({"ok": True, "dsl": dsl_content, "version": CURRENT_DSL_VERSION, "workflowId": workflow_id})
    return True


def handle_workflow_import_dsl(handler, services):
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)

    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True

    yaml_content = _normalize_text(body.get("yamlContent"))
    if not yaml_content:
        handler._send_json({"ok": False, "error": "missing_yaml", "message": "请提供 yamlContent。"}, status=400)
        return True

    try:
        parsed = yaml.safe_load(yaml_content)
    except yaml.YAMLError as exc:
        handler._send_json({"ok": False, "error": "invalid_yaml", "message": f"YAML 解析失败: {exc}"}, status=400)
        return True

    try:
        normalized = _workflow_from_dsl_payload(parsed)
    except ValueError as exc:
        handler._send_json({"ok": False, "error": "invalid_dsl", "message": str(exc)}, status=400)
        return True

    workflow_id = _generate_id()
    now = _now_iso()
    workflow = {
        "id": workflow_id,
        **normalized,
        "createdAt": now,
        "updatedAt": now,
        "lastRun": None,
        "runCount": 0,
    }
    _workflows_db[workflow_id] = workflow
    _save_to_disk(openclaw_dir)
    handler._send_json({"ok": True, "workflow": workflow, "message": "DSL 已导入为新工作流。"}, status=201)
    return True


def handle_workflow_delete(handler, services, workflow_id):
    """DELETE /api/workflows/{id} - Delete workflow."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    workflow = _workflows_db.pop(workflow_id, None)
    if not workflow:
        handler._send_json({"ok": False, "error": "not_found", "message": "工作流不存在"}, status=404)
        return True
    
    _save_to_disk(openclaw_dir)
    handler._send_json({"ok": True, "message": "工作流已删除"})
    return True


def handle_workflow_run(handler, services, workflow_id):
    """POST /api/workflows/{id}/run - Run workflow."""
    openclaw_dir = _get_openclaw_dir(handler)
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        body = {}

    execution_mode = _normalize_text(body.get("mode"), "simulate")
    if execution_mode not in ("simulate", "real"):
        execution_mode = "simulate"

    try:
        _, run = start_workflow_run(
            openclaw_dir,
            workflow_id,
            input_payload=body.get("inputs"),
            trigger=body.get("trigger"),
            workspace_path=body.get("workspacePath") or body.get("workspace_path") or "",
            workspace_authorized=body.get("workspaceAuthorized") if body.get("workspaceAuthorized") is not None else body.get("workspace_authorized"),
            execution_mode=execution_mode,
        )
    except KeyError:
        handler._send_json({"ok": False, "error": "not_found", "message": "工作流不存在"}, status=404)
        return True

    handler._send_json({"ok": True, "run": run})
    return True


def handle_workflow_toggle(handler, services, workflow_id):
    """POST /api/workflows/{id}/toggle - Enable/disable workflow."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    workflow = _workflows_db.get(workflow_id)
    if not workflow:
        handler._send_json({"ok": False, "error": "not_found", "message": "工作流不存在"}, status=404)
        return True
    
    try:
        body = json.loads(handler._read_body() or "{}")
        enabled = body.get("enabled", True)
    except json.JSONDecodeError:
        enabled = True
    
    workflow["status"] = "active" if enabled else "paused"
    workflow["updatedAt"] = _now_iso()
    
    _save_to_disk(openclaw_dir)
    handler._send_json({"ok": True, "workflow": workflow})
    return True


def handle_workflow_get_runs(handler, services, workflow_id):
    """GET /api/workflows/{id}/runs - Get workflow run history."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    workflow = _workflows_db.get(workflow_id)
    if not workflow:
        handler._send_json({"ok": False, "error": "not_found", "message": "工作流不存在"}, status=404)
        return True
    
    runs = [run for run in _workflow_runs_db.values() if run.get("workflowId") == workflow_id]
    runs.sort(key=lambda x: x.get("startedAt", ""), reverse=True)
    
    handler._send_json({"ok": True, "runs": runs})
    return True


def handle_workflow_route(handler, services, path):
    """Route workflow API requests."""
    method = handler.command
    
    if path == "/api/workflows/meta" and method == "GET":
        return handle_workflow_get_meta(handler, services)

    if path == "/api/workflows/import" and method == "POST":
        return handle_workflow_import_dsl(handler, services)

    # List workflows
    if path == "/api/workflows" and method == "GET":
        return handle_workflow_get_list(handler, services)
    
    # Create workflow
    if path == "/api/workflows" and method == "POST":
        return handle_workflow_create(handler, services)
    
    # Single workflow operations
    if path.startswith("/api/workflows/"):
        parts = path.split("/")
        if len(parts) >= 4:
            workflow_id = parts[3]
            
            # Run workflow
            if len(parts) == 5 and parts[4] == "run" and method == "POST":
                return handle_workflow_run(handler, services, workflow_id)
            
            # Toggle workflow
            if len(parts) == 5 and parts[4] == "toggle" and method == "POST":
                return handle_workflow_toggle(handler, services, workflow_id)
            
            # Get workflow runs
            if len(parts) == 5 and parts[4] == "runs" and method == "GET":
                return handle_workflow_get_runs(handler, services, workflow_id)

            if len(parts) == 5 and parts[4] == "dsl" and method == "GET":
                return handle_workflow_export_dsl(handler, services, workflow_id)
            
            # CRUD operations
            if len(parts) == 4:
                if method == "GET":
                    return handle_workflow_get_detail(handler, services, workflow_id)
                elif method == "PUT":
                    return handle_workflow_update(handler, services, workflow_id)
                elif method == "DELETE":
                    return handle_workflow_delete(handler, services, workflow_id)
    
    return False
