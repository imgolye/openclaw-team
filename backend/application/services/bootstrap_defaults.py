from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path


SKILL_CATEGORY_LABELS = {
    "document-asset-creation": "Document & Asset Creation",
    "workflow-automation": "Workflow Automation",
    "mcp-enhancement": "MCP Enhancement",
}

GSTACK_SYNC_SKILLS = [
    {
        "slug": "plan-ceo-review",
        "title": "Founder Review",
        "description": "Founder-mode plan review that pressure-tests the product direction, scope, and expected user outcome before execution.",
        "triggerPhrase": "pressure-test product direction, rethink scope, or challenge assumptions before implementation",
        "category": "workflow-automation",
        "mode": "founder",
        "stage": "plan",
        "recommendedEntry": "chat",
        "outputContract": ["markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["plan"],
        "packId": "founder-review",
        "packName": "Founder Review",
        "packDescription": "在立项和扩 scope 之前，用创始人视角重新定义问题和期望结果。",
        "defaultEntry": "chat",
    },
    {
        "slug": "plan-eng-review",
        "title": "Eng Review",
        "description": "Engineering review mode that locks architecture, boundaries, risks, test coverage, and execution sequencing before building.",
        "triggerPhrase": "lock architecture, edge cases, and execution planning before implementation",
        "category": "workflow-automation",
        "mode": "eng-review",
        "stage": "plan",
        "recommendedEntry": "studio",
        "outputContract": ["markdown", "checklist"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["plan", "test-matrix"],
        "packId": "eng-review",
        "packName": "Eng Review",
        "packDescription": "在实现前收敛架构、边界、测试矩阵和关键风险。",
        "defaultEntry": "studio",
    },
    {
        "slug": "plan-design-review",
        "title": "Design Review",
        "description": "Design audit mode that scores visual hierarchy, typography, spacing, interaction feel, and AI-slop patterns without editing code.",
        "triggerPhrase": "audit a live experience for design quality, hierarchy, or AI-slop signals without changing code",
        "category": "document-asset-creation",
        "mode": "design-review",
        "stage": "review",
        "recommendedEntry": "chat",
        "outputContract": ["report"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["findings"],
        "packId": "design-review",
        "packName": "Design Review",
        "packDescription": "对现有体验做结构化设计审阅，输出问题分级和设计改进方向。",
        "defaultEntry": "chat",
    },
    {
        "slug": "design-consultation",
        "title": "Design Consultation",
        "description": "Design consultation mode that researches references, proposes a design system, and writes a reusable visual direction brief.",
        "triggerPhrase": "define a design system, research references, or draft a design direction brief from scratch",
        "category": "document-asset-creation",
        "mode": "design-review",
        "stage": "plan",
        "recommendedEntry": "skills",
        "outputContract": ["markdown", "report"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["plan", "findings"],
        "packId": "design-consultation",
        "packName": "Design Consultation",
        "packDescription": "从零定义设计系统、视觉方向和参考竞品，形成团队可复用的设计基线。",
        "defaultEntry": "skills",
    },
    {
        "slug": "review",
        "title": "Pre-Landing Review",
        "description": "Pre-landing review mode that inspects diffs for trust boundaries, production risks, and structural issues before merge or release.",
        "triggerPhrase": "review a diff or change set for production risk before merge or release",
        "category": "workflow-automation",
        "mode": "eng-review",
        "stage": "review",
        "recommendedEntry": "run",
        "outputContract": ["checklist", "report"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["findings"],
        "packId": "pre-landing-review",
        "packName": "Pre-Landing Review",
        "packDescription": "在合并或交付前做问题扫描、缺口确认和修复优先级判断。",
        "defaultEntry": "run",
    },
    {
        "slug": "ship",
        "title": "Release Run",
        "description": "Release workflow mode that prepares a ready branch for shipping, runs tests, updates release metadata, and drives PR delivery.",
        "triggerPhrase": "prepare a ready branch for release, PR creation, or final delivery checks",
        "category": "workflow-automation",
        "mode": "release",
        "stage": "ship",
        "recommendedEntry": "run",
        "outputContract": ["checklist", "pr-link"],
        "requiresRuntime": ["git", "gh"],
        "handoffArtifacts": ["release-note"],
        "packId": "release-run",
        "packName": "Release Run",
        "packDescription": "把已经准备好的分支推进到发布阶段，补齐测试、版本和 PR 交付动作。",
        "defaultEntry": "run",
    },
    {
        "slug": "browse",
        "title": "Browser Session",
        "description": "Browser QA mode that navigates web experiences, captures evidence, validates UI state, and checks live user flows with screenshots.",
        "triggerPhrase": "inspect a web flow, verify a deployment, or collect browser evidence for QA",
        "category": "mcp-enhancement",
        "mode": "browser",
        "stage": "verify",
        "recommendedEntry": "chat",
        "outputContract": ["screenshot-set", "report"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["qa-report"],
        "packId": "browser-session",
        "packName": "Browser Session",
        "packDescription": "给 Agent 一条带证据的浏览器验证链路，用于 QA、验收和线上巡检。",
        "defaultEntry": "chat",
    },
    {
        "slug": "qa",
        "title": "QA Fix Loop",
        "description": "QA plus fix loop mode that tests a web application, fixes found bugs iteratively, and re-verifies before shipping.",
        "triggerPhrase": "test a web application, fix what is broken, and re-verify the result",
        "category": "workflow-automation",
        "mode": "qa",
        "stage": "verify",
        "recommendedEntry": "run",
        "outputContract": ["report", "screenshot-set", "commit-series"],
        "requiresRuntime": ["browser", "git"],
        "handoffArtifacts": ["qa-report"],
        "packId": "qa-fix-loop",
        "packName": "QA Fix Loop",
        "packDescription": "执行测试、修复、再验证的闭环，适合落地前质量收口。",
        "defaultEntry": "run",
    },
    {
        "slug": "qa-only",
        "title": "QA Report",
        "description": "Report-only QA mode that exercises a product, captures issues, and produces a structured health report without touching code.",
        "triggerPhrase": "produce a QA report without applying any code changes",
        "category": "workflow-automation",
        "mode": "qa",
        "stage": "verify",
        "recommendedEntry": "run",
        "outputContract": ["report", "screenshot-set"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["qa-report"],
        "packId": "qa-report",
        "packName": "QA Report",
        "packDescription": "输出结构化 QA 报告和验证证据，不直接改代码。",
        "defaultEntry": "run",
    },
    {
        "slug": "qa-design-review",
        "title": "Design Fix Loop",
        "description": "Design fix loop mode that audits visual quality, implements targeted frontend fixes, and validates improvements with before/after evidence.",
        "triggerPhrase": "fix visual quality issues after a design audit and re-verify the improved experience",
        "category": "workflow-automation",
        "mode": "design-review",
        "stage": "verify",
        "recommendedEntry": "run",
        "outputContract": ["report", "screenshot-set", "commit-series"],
        "requiresRuntime": ["browser", "git"],
        "handoffArtifacts": ["findings", "qa-report"],
        "packId": "design-fix-loop",
        "packName": "Design Fix Loop",
        "packDescription": "在设计审阅之后进入修复闭环，把问题和证据一起收口。",
        "defaultEntry": "run",
    },
    {
        "slug": "setup-browser-cookies",
        "title": "Browser Session Bootstrap",
        "description": "Session bootstrap mode that prepares authenticated browser testing by importing cookies into a controlled headless session.",
        "triggerPhrase": "prepare an authenticated browser session before QA or live-site verification",
        "category": "mcp-enhancement",
        "mode": "browser",
        "stage": "verify",
        "recommendedEntry": "chat",
        "outputContract": ["checklist"],
        "requiresRuntime": ["browser", "cookies"],
        "handoffArtifacts": [],
        "packId": "browser-session-bootstrap",
        "packName": "Browser Session Bootstrap",
        "packDescription": "为浏览器验证链路补齐登录态和会话准备，适合需要鉴权的验收场景。",
        "defaultEntry": "chat",
    },
    {
        "slug": "retro",
        "title": "Team Retro",
        "description": "Retrospective mode that reviews delivery patterns, contribution signals, and improvement opportunities after a cycle completes.",
        "triggerPhrase": "run a delivery retrospective, summarize lessons, or capture follow-up improvements",
        "category": "workflow-automation",
        "mode": "retro",
        "stage": "reflect",
        "recommendedEntry": "run",
        "outputContract": ["report"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["findings"],
        "packId": "team-retro",
        "packName": "Team Retro",
        "packDescription": "对交付过程和团队贡献做复盘，沉淀下一轮优化方向。",
        "defaultEntry": "run",
    },
    {
        "slug": "document-release",
        "title": "Docs Sync",
        "description": "Documentation sync mode that updates release-facing docs, architecture notes, and delivery context after a change lands.",
        "triggerPhrase": "sync release documentation, README updates, or architecture notes after shipping",
        "category": "document-asset-creation",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "run",
        "outputContract": ["markdown"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["release-note"],
        "packId": "docs-sync",
        "packName": "Docs Sync",
        "packDescription": "在发布前后同步 README、架构说明和版本相关文档。",
        "defaultEntry": "run",
    },
]

DEFAULT_RECOMMENDED_TEAM_BY_MODE = {
    "founder": "team-core",
    "eng-review": "team-delivery",
    "release": "team-release",
    "qa": "team-release",
    "browser": "team-release",
    "docs": "team-signals",
    "retro": "team-core",
    "design-review": "team-signals",
}


def recommended_team_id_for_pack_mode(mode):
    normalized_mode = str(mode or "").strip().lower()
    return DEFAULT_RECOMMENDED_TEAM_BY_MODE.get(normalized_mode, "")

def clean_unique_strings(values):
    cleaned = []
    if not isinstance(values, list):
        return cleaned
    for item in values:
        value = str(item or "").strip()
        if value and value not in cleaned:
            cleaned.append(value)
    return cleaned


STARTER_SKILL_ROLE_PROFILES = [
    {
        "skillSlug": item["slug"],
        "mode": item["mode"],
        "stage": item["stage"],
        "recommendedEntry": item["recommendedEntry"],
        "outputContract": clean_unique_strings(item["outputContract"]),
        "requiresRuntime": clean_unique_strings(item["requiresRuntime"]),
        "handoffArtifacts": clean_unique_strings(item["handoffArtifacts"]),
        "meta": {"starter": True, "label": item["title"], "source": "gstack"},
    }
    for item in GSTACK_SYNC_SKILLS
]

STARTER_WORKFLOW_PACKS = [
    {
        "id": item["packId"],
        "name": item["packName"],
        "description": item["packDescription"],
        "status": "active",
        "mode": item["mode"],
        "starter": True,
        "defaultEntry": item["defaultEntry"],
        "recommendedTeamId": recommended_team_id_for_pack_mode(item["mode"]),
        "stages": [
            {"key": item["packId"], "title": item["packName"], "mode": item["mode"], "stage": item["stage"]},
        ],
        "skills": [item["slug"]],
        "meta": {"starter": True, "source": "gstack"},
    }
    for item in GSTACK_SYNC_SKILLS
]

LOCAL_CODEX_IMPORTED_SKILL_ROLE_PROFILES = [
    {
        "skillSlug": "cloudflare-deploy",
        "mode": "release",
        "stage": "ship",
        "recommendedEntry": "run",
        "outputContract": ["deployment-link", "markdown"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Cloudflare Deploy", "source": "codex-import"},
    },
    {
        "skillSlug": "develop-web-game",
        "mode": "execute",
        "stage": "implement",
        "recommendedEntry": "skills",
        "outputContract": ["code", "report", "markdown"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Develop Web Game", "source": "codex-import"},
    },
    {
        "skillSlug": "doc",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["docx", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Doc", "source": "codex-import"},
    },
    {
        "skillSlug": "figma",
        "mode": "design-review",
        "stage": "review",
        "recommendedEntry": "skills",
        "outputContract": ["markdown", "image"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Figma", "source": "codex-import"},
    },
    {
        "skillSlug": "figma-implement-design",
        "mode": "execute",
        "stage": "implement",
        "recommendedEntry": "skills",
        "outputContract": ["code", "markdown"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Figma Implement Design", "source": "codex-import"},
    },
    {
        "skillSlug": "gh-address-comments",
        "mode": "eng-review",
        "stage": "review",
        "recommendedEntry": "run",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": ["git", "gh"],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "GH Address Comments", "source": "codex-import"},
    },
    {
        "skillSlug": "gh-fix-ci",
        "mode": "qa",
        "stage": "verify",
        "recommendedEntry": "run",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": ["git", "gh"],
        "handoffArtifacts": ["qa-report"],
        "meta": {"starter": True, "label": "GH Fix CI", "source": "codex-import"},
    },
    {
        "skillSlug": "imagegen",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["image", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "ImageGen", "source": "codex-import"},
    },
    {
        "skillSlug": "jupyter-notebook",
        "mode": "execute",
        "stage": "implement",
        "recommendedEntry": "skills",
        "outputContract": ["ipynb", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Jupyter Notebook", "source": "codex-import"},
    },
    {
        "skillSlug": "linear",
        "mode": "execute",
        "stage": "plan",
        "recommendedEntry": "skills",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Linear", "source": "codex-import"},
    },
    {
        "skillSlug": "netlify-deploy",
        "mode": "release",
        "stage": "ship",
        "recommendedEntry": "run",
        "outputContract": ["deployment-link", "markdown"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Netlify Deploy", "source": "codex-import"},
    },
    {
        "skillSlug": "notion-knowledge-capture",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["markdown", "report"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Notion Knowledge Capture", "source": "codex-import"},
    },
    {
        "skillSlug": "notion-meeting-intelligence",
        "mode": "founder",
        "stage": "plan",
        "recommendedEntry": "skills",
        "outputContract": ["agenda", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Notion Meeting Intelligence", "source": "codex-import"},
    },
    {
        "skillSlug": "notion-research-documentation",
        "mode": "founder",
        "stage": "plan",
        "recommendedEntry": "skills",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Notion Research Documentation", "source": "codex-import"},
    },
    {
        "skillSlug": "notion-spec-to-implementation",
        "mode": "execute",
        "stage": "plan",
        "recommendedEntry": "studio",
        "outputContract": ["plan", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Notion Spec To Implementation", "source": "codex-import"},
    },
    {
        "skillSlug": "openai-docs",
        "mode": "founder",
        "stage": "plan",
        "recommendedEntry": "skills",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "OpenAI Docs", "source": "codex-import"},
    },
    {
        "skillSlug": "pdf",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["pdf", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "PDF", "source": "codex-import"},
    },
    {
        "skillSlug": "playwright",
        "mode": "qa",
        "stage": "verify",
        "recommendedEntry": "run",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["qa-report"],
        "meta": {"starter": True, "label": "Playwright", "source": "codex-import"},
    },
    {
        "skillSlug": "render-deploy",
        "mode": "release",
        "stage": "ship",
        "recommendedEntry": "run",
        "outputContract": ["deployment-link", "markdown"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Render Deploy", "source": "codex-import"},
    },
    {
        "skillSlug": "screenshot",
        "mode": "qa",
        "stage": "verify",
        "recommendedEntry": "skills",
        "outputContract": ["image", "markdown"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Screenshot", "source": "codex-import"},
    },
    {
        "skillSlug": "security-best-practices",
        "mode": "eng-review",
        "stage": "review",
        "recommendedEntry": "skills",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Security Best Practices", "source": "codex-import"},
    },
    {
        "skillSlug": "security-ownership-map",
        "mode": "eng-review",
        "stage": "review",
        "recommendedEntry": "skills",
        "outputContract": ["csv", "json", "markdown"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Security Ownership Map", "source": "codex-import"},
    },
    {
        "skillSlug": "security-threat-model",
        "mode": "eng-review",
        "stage": "review",
        "recommendedEntry": "skills",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Security Threat Model", "source": "codex-import"},
    },
    {
        "skillSlug": "sentry",
        "mode": "qa",
        "stage": "verify",
        "recommendedEntry": "run",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["qa-report"],
        "meta": {"starter": True, "label": "Sentry", "source": "codex-import"},
    },
    {
        "skillSlug": "sora",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["video", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Sora", "source": "codex-import"},
    },
    {
        "skillSlug": "speech",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["audio", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Speech", "source": "codex-import"},
    },
    {
        "skillSlug": "spreadsheet",
        "mode": "execute",
        "stage": "implement",
        "recommendedEntry": "skills",
        "outputContract": ["xlsx", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Spreadsheet", "source": "codex-import"},
    },
    {
        "skillSlug": "transcribe",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["transcript", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Transcribe", "source": "codex-import"},
    },
    {
        "skillSlug": "vercel-deploy",
        "mode": "release",
        "stage": "ship",
        "recommendedEntry": "run",
        "outputContract": ["deployment-link", "markdown"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Vercel Deploy", "source": "codex-import"},
    },
    {
        "skillSlug": "yeet",
        "mode": "release",
        "stage": "ship",
        "recommendedEntry": "run",
        "outputContract": ["pull-request", "markdown"],
        "requiresRuntime": ["git", "gh"],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Yeet", "source": "codex-import"},
    },
]

LOCAL_CODEX_IMPORTED_SKILL_SLUGS = [
    item["skillSlug"] for item in LOCAL_CODEX_IMPORTED_SKILL_ROLE_PROFILES if item.get("skillSlug")
]

PRODUCT_STARTER_SKILL_ROLE_PROFILES = [
    {
        "skillSlug": "planning-with-files",
        "mode": "execute",
        "stage": "plan",
        "recommendedEntry": "run",
        "outputContract": ["markdown", "checklist"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Planning with Files", "source": "mission-control"},
    },
    {
        "skillSlug": "mission-control-release-ops",
        "mode": "release",
        "stage": "ship",
        "recommendedEntry": "run",
        "outputContract": ["checklist", "report", "markdown"],
        "requiresRuntime": ["git"],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "OpenClaw Team Release Ops", "source": "mission-control"},
    },
    {
        "skillSlug": "skywork-ppt",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["pptx", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Skywork PPT", "source": "skywork"},
    },
    {
        "skillSlug": "skywork-excel",
        "mode": "execute",
        "stage": "implement",
        "recommendedEntry": "skills",
        "outputContract": ["xlsx", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["plan"],
        "meta": {"starter": True, "label": "Skywork Excel", "source": "skywork"},
    },
    {
        "skillSlug": "skywork-document",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["docx", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Skywork Document", "source": "skywork"},
    },
    {
        "skillSlug": "skywork-design",
        "mode": "design-review",
        "stage": "review",
        "recommendedEntry": "skills",
        "outputContract": ["image", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Skywork Design", "source": "skywork"},
    },
    {
        "skillSlug": "skywork-music-maker",
        "mode": "docs",
        "stage": "document",
        "recommendedEntry": "skills",
        "outputContract": ["audio", "markdown"],
        "requiresRuntime": [],
        "handoffArtifacts": ["release-note"],
        "meta": {"starter": True, "label": "Skywork Music Maker", "source": "skywork"},
    },
    {
        "skillSlug": "skywork-search",
        "mode": "founder",
        "stage": "plan",
        "recommendedEntry": "skills",
        "outputContract": ["report", "markdown"],
        "requiresRuntime": ["browser"],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Skywork Search", "source": "skywork"},
    },
    {
        "skillSlug": "web-content-fetcher",
        "mode": "founder",
        "stage": "plan",
        "recommendedEntry": "skills",
        "outputContract": ["markdown", "report"],
        "requiresRuntime": [],
        "handoffArtifacts": ["findings"],
        "meta": {"starter": True, "label": "Web Content Fetcher", "source": "local-starter"},
    },
] + LOCAL_CODEX_IMPORTED_SKILL_ROLE_PROFILES

DEFAULT_MANAGED_SKILL_SLUGS = [
    "planning-with-files",
    "mission-control-release-ops",
    "plan-ceo-review",
    "plan-eng-review",
    "review",
    "qa-only",
    "ship",
    "browse",
    "document-release",
    "retro",
    "skywork-ppt",
    "skywork-excel",
    "skywork-document",
    "skywork-design",
    "skywork-music-maker",
    "skywork-search",
    "web-content-fetcher",
] + LOCAL_CODEX_IMPORTED_SKILL_SLUGS

DEFAULT_AGENT_PROFILE_BY_ID = {
    "assistant": {
        "role": "router",
        "skills": [
            "plan-ceo-review",
            "retro",
            "planning-with-files",
            "skywork-search",
            "skywork-document",
            "web-content-fetcher",
            "openai-docs",
            "playwright",
            "screenshot",
            "notion-research-documentation",
            "notion-spec-to-implementation",
            "linear",
        ],
    },
    "vp_strategy": {
        "role": "planner",
        "skills": [
            "plan-ceo-review",
            "plan-eng-review",
            "retro",
            "planning-with-files",
            "skywork-search",
            "skywork-ppt",
            "web-content-fetcher",
            "openai-docs",
            "notion-research-documentation",
            "notion-meeting-intelligence",
            "notion-spec-to-implementation",
            "linear",
        ],
    },
    "vp_compliance": {
        "role": "reviewer",
        "skills": [
            "review",
            "qa-only",
            "mission-control-release-ops",
            "gh-address-comments",
            "gh-fix-ci",
            "security-best-practices",
            "security-threat-model",
            "security-ownership-map",
            "sentry",
            "playwright",
            "web-content-fetcher",
            "openai-docs",
        ],
    },
    "coo": {
        "role": "operator",
        "skills": [
            "planning-with-files",
            "mission-control-release-ops",
            "document-release",
            "linear",
            "notion-spec-to-implementation",
            "notion-meeting-intelligence",
            "web-content-fetcher",
            "doc",
        ],
    },
    "engineering": {
        "role": "executor",
        "skills": [
            "plan-eng-review",
            "review",
            "planning-with-files",
            "playwright",
            "gh-fix-ci",
            "gh-address-comments",
            "linear",
            "notion-spec-to-implementation",
            "doc",
            "pdf",
            "jupyter-notebook",
            "develop-web-game",
            "figma-implement-design",
            "web-content-fetcher",
            "openai-docs",
        ],
    },
    "devops": {
        "role": "executor",
        "skills": [
            "ship",
            "review",
            "planning-with-files",
            "render-deploy",
            "vercel-deploy",
            "netlify-deploy",
            "cloudflare-deploy",
            "sentry",
            "gh-fix-ci",
            "gh-address-comments",
            "web-content-fetcher",
            "openai-docs",
            "yeet",
        ],
    },
    "data_team": {
        "role": "executor",
        "skills": [
            "plan-eng-review",
            "planning-with-files",
            "skywork-excel",
            "skywork-search",
            "web-content-fetcher",
            "spreadsheet",
            "jupyter-notebook",
            "linear",
            "notion-research-documentation",
            "pdf",
            "doc",
            "openai-docs",
        ],
    },
    "marketing": {
        "role": "operator",
        "skills": [
            "document-release",
            "browse",
            "skywork-design",
            "skywork-music-maker",
            "skywork-ppt",
            "web-content-fetcher",
            "imagegen",
            "sora",
            "speech",
            "screenshot",
            "figma",
            "notion-research-documentation",
            "linear",
        ],
    },
    "qa": {
        "role": "reviewer",
        "skills": [
            "qa-only",
            "browse",
            "review",
            "playwright",
            "screenshot",
            "sentry",
            "gh-address-comments",
            "gh-fix-ci",
            "security-best-practices",
            "web-content-fetcher",
            "openai-docs",
        ],
    },
    "hr": {
        "role": "monitor",
        "skills": [
            "document-release",
            "retro",
            "skywork-document",
            "notion-knowledge-capture",
            "notion-meeting-intelligence",
            "notion-research-documentation",
            "linear",
            "doc",
            "pdf",
            "transcribe",
            "web-content-fetcher",
        ],
    },
    "briefing": {
        "role": "operator",
        "skills": [
            "document-release",
            "retro",
            "skywork-document",
            "skywork-search",
            "skywork-ppt",
            "web-content-fetcher",
            "notion-research-documentation",
            "notion-meeting-intelligence",
            "notion-knowledge-capture",
            "openai-docs",
            "doc",
            "pdf",
            "linear",
        ],
    },
}


DEFAULT_AGENT_PERSONA_BY_ID = {
    "assistant": {
        "humanName": "林可",
        "jobTitle": "项目协调助理",
        "department": "项目协调中心",
        "capabilityTags": ["协调", "推进", "会话"],
        "workingStyle": "先确认优先级和对象，再把任务拆开、对齐和跟进。",
        "voiceReplySpeed": 1.0,
    },
    "vp_strategy": {
        "humanName": "周叙",
        "jobTitle": "产品策略负责人",
        "department": "产品策略部",
        "capabilityTags": ["策略", "规划", "优先级"],
        "workingStyle": "先澄清目标与范围，再给判断、方案和推进节奏。",
        "voiceReplySpeed": 0.94,
    },
    "vp_compliance": {
        "humanName": "顾宁",
        "jobTitle": "质量与风控负责人",
        "department": "质量与风控部",
        "capabilityTags": ["质量", "风控", "审查"],
        "workingStyle": "先给是否可放行的结论，再补证据、风险和限制条件。",
        "voiceReplySpeed": 0.9,
    },
    "coo": {
        "humanName": "程远",
        "jobTitle": "项目运营负责人",
        "department": "项目运营部",
        "capabilityTags": ["运营", "协调", "交付"],
        "workingStyle": "先把人和事项拉齐，再盯节点、依赖和落地结果。",
        "voiceReplySpeed": 1.04,
    },
    "engineering": {
        "humanName": "沈砚",
        "jobTitle": "研发负责人",
        "department": "研发交付部",
        "capabilityTags": ["研发", "实现", "联调"],
        "workingStyle": "先确认实现边界，再推进开发、联调和技术落地。",
        "voiceReplySpeed": 0.92,
    },
    "devops": {
        "humanName": "韩川",
        "jobTitle": "平台运维负责人",
        "department": "平台运维部",
        "capabilityTags": ["运维", "上线", "恢复"],
        "workingStyle": "先看稳定性和发布风险，再处理环境、部署和恢复方案。",
        "voiceReplySpeed": 0.88,
    },
    "data_team": {
        "humanName": "许衡",
        "jobTitle": "数据分析负责人",
        "department": "数据分析部",
        "capabilityTags": ["数据", "分析", "洞察"],
        "workingStyle": "先确认指标口径，再给数据结论、洞察和建议动作。",
        "voiceReplySpeed": 0.97,
    },
    "marketing": {
        "humanName": "宋禾",
        "jobTitle": "市场运营负责人",
        "department": "市场运营部",
        "capabilityTags": ["市场", "内容", "传播"],
        "workingStyle": "先确认受众和信息目标，再产出内容和传播动作。",
        "voiceReplySpeed": 1.08,
    },
    "qa": {
        "humanName": "叶清",
        "jobTitle": "测试负责人",
        "department": "测试验证部",
        "capabilityTags": ["测试", "验证", "复验"],
        "workingStyle": "先明确验证范围，再给问题证据、风险判断和复验计划。",
        "voiceReplySpeed": 0.91,
    },
    "hr": {
        "humanName": "温妍",
        "jobTitle": "人力支持负责人",
        "department": "组织支持部",
        "capabilityTags": ["人力", "培训", "支持"],
        "workingStyle": "先确认对象和诉求，再整理材料、安排沟通和后续跟进。",
        "voiceReplySpeed": 1.06,
    },
    "briefing": {
        "humanName": "乔知",
        "jobTitle": "情报简报专员",
        "department": "情报简报部",
        "capabilityTags": ["情报", "简报", "同步"],
        "workingStyle": "先筛出重点，再整理结论、背景和推荐行动。",
        "voiceReplySpeed": 1.01,
    },
}

DEFAULT_SHARED_VOICE_SKILLS = ("speech", "transcribe")
DEFAULT_AGENT_VOICE_REPLY_VOICE = "serena"


def default_agent_voice_reply_instructions(human_name="", job_title="", working_style=""):
    parts = [
        "请像真人在微信里发语音一样自然说话。",
        "语气要温和、口语化、有轻微情绪起伏，不要像播报器、朗读器或客服录音。",
        "停顿自然，句子不要太书面，像同事当面回复一样。",
    ]
    normalized_job_title = str(job_title or "").strip()
    normalized_human_name = str(human_name or "").strip()
    normalized_working_style = str(working_style or "").strip()
    if normalized_job_title or normalized_human_name:
        identity = "，".join(item for item in [normalized_human_name, normalized_job_title] if item)
        parts.append(f"保持这个成员的身份感：{identity}。")
    if normalized_working_style:
        parts.append(f"协作和表达风格参考：{normalized_working_style}")
    return " ".join(part for part in parts if part).strip()


AGENT_ROLE_LABELS = {
    "router": "协作协调人",
    "planner": "产品项目负责人",
    "reviewer": "质量与风险负责人",
    "executor": "交付执行负责人",
    "operator": "业务推进负责人",
    "monitor": "支持与关怀负责人",
}


AGENT_ROLE_WORKING_STYLE = {
    "router": "先确认对象和优先级，再把人和事项对齐起来。",
    "planner": "先澄清目标与边界，再给判断、方案和推进顺序。",
    "reviewer": "先给风险和结论，再补证据、限制条件和放行建议。",
    "executor": "先说当前进展，再同步卡点、依赖和下一步动作。",
    "operator": "先把协作对象、节奏和结果拉齐，再推进落地。",
    "monitor": "先确认对象和需求，再整理材料、记录结论和后续跟进。",
}

TEAM_CONVERSATION_ROLE_PRIORITY = {
    "router": 0,
    "planner": 1,
    "executor": 2,
    "reviewer": 3,
    "operator": 4,
    "monitor": 5,
}

TEAM_CONVERSATION_JOB_TITLE_PRIORITY = (
    (("协调", "项目"), 0),
    (("产品", "策略"), 1),
    (("研发", "开发", "平台", "运维"), 2),
    (("测试", "质量", "风控"), 3),
    (("运营", "市场"), 4),
    (("数据",), 5),
    (("人力", "情报", "支持"), 6),
)


HANDOFF_ARTIFACT_TYPE_MAP = {
    "plan": "plan-brief",
    "findings": "review-findings",
    "test-matrix": "test-matrix",
    "qa-report": "qa-report",
    "release-note": "release-note",
}


ARTIFACT_TYPE_LABELS = {
    "plan-brief": "Plan brief",
    "eng-plan": "Eng plan",
    "test-matrix": "Test matrix",
    "design-audit": "Design audit",
    "review-findings": "Review findings",
    "qa-report": "QA report",
    "release-checklist": "Release checklist",
    "release-doc": "Release doc",
    "release-note": "Release note",
    "retro-summary": "Retro summary",
    "browser-snapshot": "Browser snapshot",
    "cookie-bootstrap": "Cookie bootstrap",
    "ship-pr": "Ship PR",
    "heal-plan": "Self-heal plan",
}


def safe_list(value):
    return value if isinstance(value, list) else []


def agent_role_label(role):
    normalized_role = str(role or "").strip().lower()
    if not normalized_role:
        return ""
    return AGENT_ROLE_LABELS.get(normalized_role, normalized_role.replace("_", " ").title())


def product_default_agent_profile(agent_id):
    normalized_agent_id = str(agent_id or "").strip().lower()
    if not normalized_agent_id:
        return {}
    profile = {}
    if normalized_agent_id in DEFAULT_AGENT_PROFILE_BY_ID:
        profile = deepcopy(DEFAULT_AGENT_PROFILE_BY_ID[normalized_agent_id])
    elif any(token in normalized_agent_id for token in ("strategy", "planner", "founder")):
        profile = {
            "role": "planner",
            "skills": [
                "plan-ceo-review",
                "plan-eng-review",
                "planning-with-files",
                "retro",
                "web-content-fetcher",
                "notion-meeting-intelligence",
            ],
        }
    elif any(token in normalized_agent_id for token in ("compliance", "qa", "review")):
        profile = {
            "role": "reviewer",
            "skills": ["review", "qa-only", "playwright", "web-content-fetcher", "openai-docs"],
        }
    elif any(token in normalized_agent_id for token in ("eng", "dev", "data", "build")):
        profile = {
            "role": "executor",
            "skills": [
                "plan-eng-review",
                "planning-with-files",
                "review",
                "web-content-fetcher",
                "linear",
                "openai-docs",
            ],
        }
    elif any(token in normalized_agent_id for token in ("ops", "brief", "market", "signal")):
        profile = {
            "role": "operator",
            "skills": [
                "document-release",
                "retro",
                "web-content-fetcher",
                "notion-research-documentation",
                "linear",
            ],
        }
    else:
        profile = {
            "role": "operator",
            "skills": ["planning-with-files", "web-content-fetcher", "linear", "retro"],
        }
    profile["role"] = str(profile.get("role") or "").strip()
    profile["skills"] = clean_unique_strings(list(profile.get("skills") or []) + list(DEFAULT_SHARED_VOICE_SKILLS))
    return profile


def normalize_agent_voice_reply_voice(value, default=DEFAULT_AGENT_VOICE_REPLY_VOICE):
    normalized = str(value or "").strip()
    return normalized or str(default or DEFAULT_AGENT_VOICE_REPLY_VOICE).strip() or DEFAULT_AGENT_VOICE_REPLY_VOICE


def normalize_agent_voice_reply_speed(value, default=1.0):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = float(default or 1.0)
    numeric = max(0.8, min(1.2, numeric))
    return round(numeric, 2)


def merged_agent_runtime_profile(agent_id, override=None):
    override = override if isinstance(override, dict) else {}
    default_profile = product_default_agent_profile(agent_id)
    persona = deepcopy(DEFAULT_AGENT_PERSONA_BY_ID.get(str(agent_id or "").strip().lower(), {}))
    role = str(override.get("role") or default_profile.get("role") or "").strip()
    skills = clean_unique_strings(override.get("skills") or default_profile.get("skills") or [])
    default_speed = normalize_agent_voice_reply_speed(persona.get("voiceReplySpeed"), default=1.0)
    return {
        "role": role,
        "roleLabel": agent_role_label(role),
        "humanName": str(
            override.get("humanName")
            or persona.get("humanName")
            or ""
        ).strip(),
        "jobTitle": str(override.get("jobTitle") or persona.get("jobTitle") or agent_role_label(role)).strip(),
        "workingStyle": str(
            override.get("workingStyle")
            or persona.get("workingStyle")
            or AGENT_ROLE_WORKING_STYLE.get(role, "")
        ).strip(),
        "department": str(override.get("department") or persona.get("department") or "").strip(),
        "capabilityTags": clean_unique_strings(override.get("capabilityTags") or persona.get("capabilityTags") or []),
        "notes": str(override.get("notes") or persona.get("notes") or "").strip(),
        "voiceReplyVoice": normalize_agent_voice_reply_voice(
            override.get("voiceReplyVoice") or persona.get("voiceReplyVoice") or DEFAULT_AGENT_VOICE_REPLY_VOICE
        ),
        "voiceReplySpeed": normalize_agent_voice_reply_speed(
            override.get("voiceReplySpeed") if "voiceReplySpeed" in override else persona.get("voiceReplySpeed"),
            default=default_speed,
        ),
        "voiceReplyInstructions": str(
            override.get("voiceReplyInstructions")
            or persona.get("voiceReplyInstructions")
            or default_agent_voice_reply_instructions(
                override.get("humanName") or persona.get("humanName") or "",
                override.get("jobTitle") or persona.get("jobTitle") or agent_role_label(role),
                override.get("workingStyle") or persona.get("workingStyle") or AGENT_ROLE_WORKING_STYLE.get(role, ""),
            )
        ).strip(),
        "voiceReplySamplePath": str(override.get("voiceReplySamplePath") or "").strip(),
        "voiceReplySampleName": str(override.get("voiceReplySampleName") or "").strip(),
        "voiceReplySamplePromptText": str(override.get("voiceReplySamplePromptText") or "").strip(),
        "voiceReplySampleConfigured": bool(str(override.get("voiceReplySamplePath") or "").strip()),
        "skills": skills,
        "skillCount": len(skills),
        "topSkills": skills[:4],
    }


def gstack_skill_specs_by_slug():
    return {item["slug"]: item for item in GSTACK_SYNC_SKILLS if item.get("slug")}


def merge_workflow_pack_meta(base_meta, overlay_meta):
    base_meta = deepcopy(base_meta) if isinstance(base_meta, dict) else {}
    overlay_meta = overlay_meta if isinstance(overlay_meta, dict) else {}
    if not overlay_meta:
        return base_meta
    merged = deepcopy(base_meta)
    for key, value in overlay_meta.items():
        if key in {"reviewGates", "artifactTemplates", "modeAliases"}:
            if isinstance(value, list) and value:
                merged[key] = deepcopy(value)
            elif key not in merged:
                merged[key] = []
            continue
        if key in {"runtimePolicy", "releasePolicy", "qaPolicy"}:
            if isinstance(value, dict) and value:
                base_value = merged.get(key) if isinstance(merged.get(key), dict) else {}
                merged[key] = {**base_value, **deepcopy(value)}
            elif key not in merged:
                merged[key] = {}
            continue
        if isinstance(value, str):
            if value.strip():
                merged[key] = value
            continue
        if isinstance(value, list):
            if value:
                merged[key] = deepcopy(value)
            continue
        if isinstance(value, dict):
            if value:
                merged[key] = deepcopy(value)
            continue
        if value is not None:
            merged[key] = deepcopy(value)
    return merged


def merge_workflow_pack_record(base_pack, overlay_pack):
    if not isinstance(base_pack, dict) or not base_pack:
        return deepcopy(overlay_pack) if isinstance(overlay_pack, dict) else {}
    overlay_pack = overlay_pack if isinstance(overlay_pack, dict) else {}
    if not overlay_pack:
        return deepcopy(base_pack)
    merged = deepcopy(base_pack)
    for key, value in overlay_pack.items():
        if key == "meta":
            merged["meta"] = merge_workflow_pack_meta(base_pack.get("meta"), value)
            continue
        if key in {"skills", "stages"}:
            if isinstance(value, list) and value:
                merged[key] = deepcopy(value)
            continue
        if isinstance(value, str):
            if value.strip():
                merged[key] = value
            continue
        if isinstance(value, list):
            if value:
                merged[key] = deepcopy(value)
            continue
        if isinstance(value, dict):
            if value:
                merged[key] = deepcopy(value)
            continue
        if value is not None:
            merged[key] = deepcopy(value)
    return merged


def normalize_flag(value, default=False):
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def artifact_type_label(value):
    normalized = str(value or "").strip()
    return ARTIFACT_TYPE_LABELS.get(normalized, normalized.replace("-", " ").title() or "Artifact")


def artifact_type_from_handoff(value):
    normalized = str(value or "").strip().lower()
    return HANDOFF_ARTIFACT_TYPE_MAP.get(normalized, normalized or "")


def dedupe_skills_payload_entries(payload):
    payload = payload if isinstance(payload, dict) else {}
    skills = safe_list(payload.get("skills"))
    if not skills:
        payload["skills"] = []
        return payload
    deduped = {}
    order = []
    for skill in skills:
        if not isinstance(skill, dict):
            continue
        slug = str(skill.get("slug") or "").strip()
        if not slug:
            continue
        current = deduped.get(slug)
        candidate_score = (
            2 if str(skill.get("rootKind") or "").strip() == "project" else 0,
            1 if skill.get("package", {}).get("exists") else 0,
            int(skill.get("qualityScore") or 0),
        )
        current_score = (
            2 if str((current or {}).get("rootKind") or "").strip() == "project" else 0,
            1 if (current or {}).get("package", {}).get("exists") else 0,
            int((current or {}).get("qualityScore") or 0),
        )
        if current is None:
            deduped[slug] = deepcopy(skill)
            order.append(slug)
        elif candidate_score > current_score:
            merged = {**current, **skill}
            if isinstance(current.get("metadata"), dict) and isinstance(skill.get("metadata"), dict):
                merged["metadata"] = {**current["metadata"], **skill["metadata"]}
            deduped[slug] = merged
    payload["skills"] = [deduped[slug] for slug in order if slug in deduped]
    return payload


def refresh_skill_catalog_summary(payload):
    payload = payload if isinstance(payload, dict) else {}
    skills = safe_list(payload.get("skills"))
    status_counter = Counter(str(skill.get("status") or "").strip().lower() or "ready" for skill in skills)
    category_counter = Counter(
        str(skill.get("categoryLabel") or skill.get("category") or "").strip()
        for skill in skills
        if str(skill.get("categoryLabel") or skill.get("category") or "").strip()
    )
    payload["summary"] = {
        "total": len(skills),
        "ready": status_counter.get("ready", 0),
        "warning": status_counter.get("warning", 0),
        "error": status_counter.get("error", 0),
        "packaged": sum(1 for skill in skills if isinstance(skill.get("package"), dict) and skill["package"].get("exists")),
        "categories": dict(category_counter),
    }
    return payload


def gstack_skill_scaffold_markdown(spec):
    spec = spec if isinstance(spec, dict) else {}
    slug = str(spec.get("slug") or "").strip()
    title = str(spec.get("title") or slug).strip() or slug
    description = str(spec.get("description") or "").strip()
    trigger_phrase = str(spec.get("triggerPhrase") or "use this workflow").strip()
    mode = str(spec.get("mode") or "").strip()
    stage = str(spec.get("stage") or "").strip()
    recommended_entry = str(spec.get("recommendedEntry") or "skills").strip()
    output_contract = ", ".join(clean_unique_strings(spec.get("outputContract") or [])) or "markdown"
    runtime_label = ", ".join(clean_unique_strings(spec.get("requiresRuntime") or [])) or "none"
    handoff_label = ", ".join(clean_unique_strings(spec.get("handoffArtifacts") or [])) or "none"
    return "\n".join(
        [
            "---",
            f"name: {slug}",
            f"description: {description} Use when the user asks to {trigger_phrase}.",
            "compatibility: Claude Code, Claude.ai, and API environments that support local files and optional helper scripts.",
            "metadata:",
            "  author: OpenClaw Team",
            "  version: 1.0.0",
            "  upstream-repo: garrytan/gstack",
            f"  upstream-skill: {slug}",
            "  import-mode: managed-sync",
            "---",
            f"# {title}",
            "",
            "## Instructions",
            "- Restate the user's target outcome in one sentence before acting.",
            f"- Operate in `{mode}` mode and keep the work grounded in the `{stage}` stage.",
            f"- Prefer the `{recommended_entry}` entry pattern when this skill needs to hand off into OpenClaw Team.",
            f"- Produce outputs in the style of: {output_contract}.",
            f"- Call out runtime needs early if the flow depends on: {runtime_label}.",
            f"- Leave explicit handoff artifacts when relevant: {handoff_label}.",
            "",
            "## Workflow",
            "### Step 1: Frame the task",
            "- Confirm the decision, artifact, or validation the user actually needs.",
            "- Identify the immediate risk if this step is skipped.",
            "",
            "### Step 2: Run the specialist loop",
            "- Focus on the job this skill is meant to do, not the entire project lifecycle.",
            "- Keep outputs structured so they can be attached to a Run, Chat thread, or Workflow Pack.",
            "- Prefer evidence, concrete findings, and explicit next steps over vague advice.",
            "",
            "### Step 3: Hand off cleanly",
            "- Summarize what is now settled.",
            "- Name the next recommended skill or workflow stage.",
            "- List the artifact or checkpoint the next role should consume.",
            "",
            "## Examples",
            f'- User says: "Help me {trigger_phrase}."',
            f"  Result: Claude uses the {title} workflow, produces a structured output, and hands off the next step clearly.",
            "",
            "## Troubleshooting",
            "- If required runtime access is missing, explain the smallest unblock clearly instead of guessing.",
            "- If the request drifts into another role mode, stop and hand off to the right skill or workflow pack.",
            "- If evidence is weak, ask for the minimum additional context needed to continue reliably.",
            "",
        ]
    )


def ensure_gstack_skill_scaffold(project_dir, spec):
    project_dir = Path(project_dir).expanduser().resolve()
    skill_dir = project_dir / "platform" / "skills" / str(spec.get("slug") or "").strip()
    if not str(spec.get("slug") or "").strip():
        raise RuntimeError("missing gstack skill slug")
    if skill_dir.exists():
        return {"slug": str(spec.get("slug") or "").strip(), "created": False, "path": str(skill_dir)}
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(gstack_skill_scaffold_markdown(spec), encoding="utf-8")
    return {"slug": str(spec.get("slug") or "").strip(), "created": True, "path": str(skill_dir)}


def augment_skills_payload_with_gstack_scaffolds(project_dir, payload):
    project_dir = Path(project_dir).expanduser().resolve() if project_dir else None
    if not project_dir:
        return payload
    payload = payload if isinstance(payload, dict) else {}
    payload["skills"] = safe_list(payload.get("skills"))
    existing_slugs = {str(item.get("slug") or "").strip() for item in payload["skills"] if isinstance(item, dict)}
    for spec in GSTACK_SYNC_SKILLS:
        slug = str(spec.get("slug") or "").strip()
        skill_dir = project_dir / "platform" / "skills" / slug
        skill_md = skill_dir / "SKILL.md"
        if not slug or slug in existing_slugs or not skill_md.exists():
            continue
        try:
            body_text = skill_md.read_text(encoding="utf-8")
            word_count = len(body_text.split())
        except OSError:
            word_count = 0
        package_path = project_dir / "platform" / "dist" / "skills" / f"{slug}.zip"
        payload["skills"].append(
            {
                "slug": slug,
                "name": slug,
                "displayName": str(spec.get("title") or slug).strip() or slug,
                "description": str(spec.get("description") or "").strip(),
                "category": str(spec.get("category") or "workflow-automation").strip(),
                "categoryLabel": SKILL_CATEGORY_LABELS.get(str(spec.get("category") or "workflow-automation").strip(), "Workflow Automation"),
                "relativePath": str(skill_md.relative_to(project_dir)),
                "path": str(skill_md),
                "status": "ready",
                "qualityScore": 100,
                "wordCount": word_count,
                "hasScripts": False,
                "hasReferences": False,
                "hasAssets": False,
                "rootKind": "project",
                "metadata": {
                    "managedSource": "gstack",
                    "importMode": "managed-sync",
                    "upstreamRepo": "garrytan/gstack",
                    "upstreamSkill": slug,
                },
                "package": {
                    "exists": package_path.exists(),
                    "path": str(package_path),
                    "updatedAt": datetime.fromtimestamp(package_path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
                    if package_path.exists()
                    else "",
                },
            }
        )
        existing_slugs.add(slug)
    payload = dedupe_skills_payload_entries(payload)
    payload = refresh_skill_catalog_summary(payload)
    return payload
