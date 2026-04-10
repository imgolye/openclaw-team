# OpenClaw Team — System Overview

How the product is put together.

## System Layers

```
User
  └─ Web App (apps/frontend/)
       └─ HTTP API (backend/presentation/http/)
            └─ Application Services (backend/application/)
                 ├─ Domain Core (backend/domain/)
                 ├─ Storage & Adapters (backend/adapters/)
                 └─ OpenClaw / LLM / Tool Runtimes
                      └─ Local State (local/, .mission-control/)
```

## Product Modules

| Module | What It Does |
|--------|-------------|
| **Workspace** | Day-to-day surface: tasks, conversations, deliverables, runs |
| **Ops** | Operational visibility: overview, alerts, reports, agent health |
| **Studio** | Workflow orchestration and multi-agent process design |
| **Platform** | Runtime config, OpenClaw, skills, themes, deployment governance |

## Repository Ownership

| Directory | Responsibility |
|-----------|---------------|
| `backend/` | HTTP APIs, orchestration, workflow execution, chat, storage |
| `apps/frontend/` | Browser UI for all product surfaces |
| `platform/bin/` | Install, deploy, bootstrap, runtime sync scripts |
| `platform/infra/` | Docker recipes, database baselines |
| `platform/config/` | Themes, runtime profiles |
| `platform/skills/` | Built-in skill packs |
| `platform/tests/` | API regression and E2E tests |

## State Boundaries

- **Product code**: `backend/`, `apps/frontend/`, `platform/`
- **Runtime state** (gitignored): `local/`, `.mission-control/`
- **Docs**: `docs/`

## Documentation Map

- Repo structure: [`project/repo-structure.md`](../project/repo-structure.md)
- Local state rules: [`project/local-state.md`](../project/local-state.md)
- Runtime config: [`runtime/runtime-profiles.md`](../runtime/runtime-profiles.md)
- Design system: [`architecture/design-system.md`](design-system.md)
- Computer use: [`architecture/computer-use.md`](computer-use.md)
