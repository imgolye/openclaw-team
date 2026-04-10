# OpenClaw Team — Repo Structure

## Product Source

| Directory | What It Is |
|-----------|-----------|
| `backend/` | Python backend: HTTP runtime, services, domain logic, adapters |
| `apps/frontend/` | Web product: React + Vite |
| `platform/bin/` | Install, deploy, verify, runtime scripts |
| `platform/infra/` | Docker recipes and database baselines |
| `platform/config/` | Themes and runtime profiles |
| `platform/skills/` | Built-in skill packs |
| `platform/vendor/` | Vendored dependencies |
| `platform/tests/` | Integration and E2E tests |
| `docs/` | Product and engineering documentation |

## Root Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | One-command Docker deployment |
| `Dockerfile` | Container image build |
| `.env.example` | Environment variable template |
| `conftest.py` | Pytest bootstrap for imports |
| `CONTRIBUTING.md` | How to contribute |

## Runtime State (gitignored)

These directories are created at runtime. Not product source.

| Directory | Contents |
|-----------|---------|
| `local/` | Agents, dashboard, logs, memory, models |
| `.mission-control/` | Product state, automations, knowledge base |

## Working Rules

- Edit code in `backend/`, `apps/frontend/`, `platform/`. That's the main surface.
- Treat `local/` and `.mission-control/` as runtime data — don't commit them.
- Keep docs under `docs/`, not scattered at root.
- New skills go in `platform/skills/`.

See also: [Local State Guide](local-state.md)
