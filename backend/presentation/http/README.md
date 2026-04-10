# HTTP Layer

API server for OpenClaw Team.

## Files

| File | Purpose |
|------|---------|
| `http.py` | Top-level GET/POST/PUT/DELETE routing |
| `handler.py` | HTTP request handler class |
| `runtime.py` | Shared runtime namespace (assembled from `runtime_parts/`) |
| `query.py` | Read-only API routes |
| `command.py` | Write/action API fanout |
| `rest.py` | `/api/v1/tenants/...` REST endpoints |
| `aliases.py` | Canonical route aliases |
| `task.py` | Task action routes |
| `agent.py` | Agent and team routes |
| `chat.py` | Chat/thread routes |
| `management.py` | Management/automation routes |
| `platform.py` | Platform, themes, skills routes |
| `computer_use.py` | Computer use run routes |
| `workflow_api.py` | Workflow orchestration routes |
| `service_catalog.py` | Service name registry |

## Architecture Rules

- Handler stays thin — delegates to this layer
- Route modules coordinate request/response only
- Business logic → `backend/application/services/`
- Persistence → `backend/adapters/storage/`
- External adapters → `backend/adapters/integrations/`
- Domain primitives → `backend/domain/core/`

## API Spec

Interactive spec at `/api/v1/docs` when the server is running.
