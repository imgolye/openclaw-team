"""OpenAPI 3 spec for OpenClaw Team."""

OPENAPI_SPEC = {
    "openapi": "3.0.3",
    "info": {
        "title": "OpenClaw Team API",
        "version": "1.18.0",
        "description": "REST and action API for OpenClaw Team — an open-source AI Agent operations and orchestration platform.",
        "license": {"name": "MIT"},
    },
    "servers": [
        {"url": "/api", "description": "Legacy API"},
        {"url": "/api/v1", "description": "Versioned API"},
    ],
    "tags": [
        {"name": "auth", "description": "Authentication and session management"},
        {"name": "dashboard", "description": "Dashboard data and overview"},
        {"name": "agents", "description": "Agent management and team operations"},
        {"name": "tasks", "description": "Task lifecycle and dispatch"},
        {"name": "chat", "description": "Chat threads and messaging"},
        {"name": "conversations", "description": "Conversation sessions and transcripts"},
        {"name": "workflows", "description": "Workflow orchestration"},
        {"name": "management", "description": "Operations management and automation"},
        {"name": "openclaw", "description": "OpenClaw runtime management"},
        {"name": "skills", "description": "Skills catalog and management"},
        {"name": "platform", "description": "Platform admin, tenants, API keys"},
        {"name": "computer-use", "description": "Browser automation and desktop runs"},
        {"name": "monitoring", "description": "Health, metrics, and readiness"},
    ],
    "paths": {
        # ── Monitoring ──
        "/health": {"get": {"tags": ["monitoring"], "summary": "Health check", "responses": {"200": {"description": "OK"}}}},
        "/healthz": {"get": {"tags": ["monitoring"], "summary": "Liveness probe", "responses": {"200": {"description": "OK"}}}},
        "/readinessz": {"get": {"tags": ["monitoring"], "summary": "Readiness probe", "responses": {"200": {"description": "OK"}, "503": {"description": "Not ready"}}}},
        "/metrics": {"get": {"tags": ["monitoring"], "summary": "Process metrics", "responses": {"200": {"description": "OK"}}}},
        # ── Auth ──
        "/api/auth/session": {"get": {"tags": ["auth"], "summary": "Current session info", "responses": {"200": {"description": "Session payload"}}}},
        "/api/auth/login": {"post": {"tags": ["auth"], "summary": "Password login", "responses": {"200": {"description": "Login result"}}}},
        "/api/auth/logout": {"post": {"tags": ["auth"], "summary": "Logout", "responses": {"200": {"description": "OK"}}}},
        # ── Dashboard ──
        "/api/dashboard": {"get": {"tags": ["dashboard"], "summary": "Full dashboard bundle", "responses": {"200": {"description": "Dashboard data"}}}},
        # ── Agents ──
        "/api/agents": {"get": {"tags": ["agents"], "summary": "Agent list with status", "responses": {"200": {"description": "Agent list"}}}},
        "/api/agent-teams": {"get": {"tags": ["agents"], "summary": "Agent team configurations", "responses": {"200": {"description": "Team list"}}}},
        "/api/actions/agents/pause": {"post": {"tags": ["agents"], "summary": "Pause an agent", "responses": {"200": {"description": "OK"}}}},
        "/api/actions/agents/resume": {"post": {"tags": ["agents"], "summary": "Resume an agent", "responses": {"200": {"description": "OK"}}}},
        # ── Tasks ──
        "/api/tasks": {"get": {"tags": ["tasks"], "summary": "Task list", "responses": {"200": {"description": "Task list"}}}},
        "/api/tasks/{taskId}": {"get": {"tags": ["tasks"], "summary": "Task detail (REST)", "parameters": [{"name": "taskId", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Task detail"}}}},
        "/api/task": {"get": {"tags": ["tasks"], "summary": "Task detail (query-param, deprecated)", "parameters": [{"name": "taskId", "in": "query", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Task detail"}}}},
        "/api/actions/task/create": {"post": {"tags": ["tasks"], "summary": "Create a task", "responses": {"200": {"description": "Created task"}}}},
        # ── Chat ──
        "/api/chat": {"get": {"tags": ["chat"], "summary": "Chat catalog", "responses": {"200": {"description": "Thread list"}}}},
        "/api/chat/threads/{threadId}": {"get": {"tags": ["chat"], "summary": "Thread detail (REST)", "parameters": [{"name": "threadId", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Thread data"}}}},
        "/api/chat/thread": {"get": {"tags": ["chat"], "summary": "Thread detail (query-param, deprecated)", "parameters": [{"name": "threadId", "in": "query", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Thread data"}}}},
        "/api/actions/chat/thread/send": {"post": {"tags": ["chat"], "summary": "Send message", "responses": {"200": {"description": "Send result"}}}},
        # ── Conversations ──
        "/api/conversations": {"get": {"tags": ["conversations"], "summary": "Conversation list", "responses": {"200": {"description": "Conversation list"}}}},
        "/api/conversations/transcript": {"get": {"tags": ["conversations"], "summary": "Conversation transcript", "responses": {"200": {"description": "Transcript data"}}}},
        # ── Workflows ──
        "/api/orchestration/overview": {"get": {"tags": ["workflows"], "summary": "Orchestration overview", "responses": {"200": {"description": "Overview data"}}}},
        "/api/orchestration/workflows": {"get": {"tags": ["workflows"], "summary": "Workflow list", "responses": {"200": {"description": "Workflow list"}}}},
        # ── Management ──
        "/api/management/runs": {"get": {"tags": ["management"], "summary": "Management run list", "responses": {"200": {"description": "Run list"}}}},
        "/api/management/automation/rules": {"get": {"tags": ["management"], "summary": "Automation rules", "responses": {"200": {"description": "Rule list"}}}},
        # ── OpenClaw ──
        "/api/openclaw/overview": {"get": {"tags": ["openclaw"], "summary": "OpenClaw runtime overview", "responses": {"200": {"description": "Runtime status"}}}},
        "/api/openclaw/models": {"get": {"tags": ["openclaw"], "summary": "Model configuration", "responses": {"200": {"description": "Model list"}}}},
        # ── Skills ──
        "/api/skills": {"get": {"tags": ["skills"], "summary": "Skills catalog", "responses": {"200": {"description": "Skill list"}}}},
        # ── Platform ──
        "/api/platform": {"get": {"tags": ["platform"], "summary": "Platform overview", "responses": {"200": {"description": "Platform data"}}}},
        "/api/platform/tenants": {"get": {"tags": ["platform"], "summary": "Tenant list", "responses": {"200": {"description": "Tenant list"}}}},
        "/api/platform/api-keys": {"get": {"tags": ["platform"], "summary": "API key list", "responses": {"200": {"description": "Key list"}}}},
        # ── Computer Use ──
        "/api/computer-use/runs": {"get": {"tags": ["computer-use"], "summary": "Computer use run list", "responses": {"200": {"description": "Run list"}}}},
        "/api/actions/computer-use/run/create": {"post": {"tags": ["computer-use"], "summary": "Create a run", "responses": {"200": {"description": "Created run"}}}},
        # ── REST (v1) ──
        "/api/v1/tenants/{tenantId}": {"get": {"tags": ["platform"], "summary": "Tenant detail (REST)", "responses": {"200": {"description": "Tenant data"}}}},
        # ── Events ──
        "/events": {"get": {"tags": ["monitoring"], "summary": "Server-sent events stream", "responses": {"200": {"description": "SSE stream"}}}},
        # ── Docs ──
        "/api/v1/docs": {"get": {"tags": ["monitoring"], "summary": "This OpenAPI spec", "responses": {"200": {"description": "JSON spec"}}}},
    },
}
