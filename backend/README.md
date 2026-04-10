# Backend

OpenClaw Team server — pure Python, no framework.

## Structure

```
backend/
├── presentation/http/    # HTTP API layer (routes, handlers)
├── application/services/ # Business logic and orchestration
├── domain/core/          # Shared domain primitives and config
├── adapters/
│   ├── storage/          # PostgreSQL persistence
│   ├── integrations/     # OpenClaw, WeChat, external adapters
│   ├── llm/              # LLM provider adapters
│   └── tools/            # Tool execution adapters
└── *.py                  # Root-level entry points and utilities
```

## Where to put new code

| Type | Location |
|------|----------|
| New API endpoint | `presentation/http/` |
| Business rules | `application/services/` |
| Shared constants/helpers | `domain/core/` |
| Database access | `adapters/storage/` |
| External integrations | `adapters/integrations/` |

## API Docs

Start the server and visit `/api/v1/docs` for the OpenAPI spec.
