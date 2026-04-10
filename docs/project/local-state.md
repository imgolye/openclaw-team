# OpenClaw Team — Local State Guide

Runtime state created during development and operation. All gitignored.

See also: [Repo Structure](repo-structure.md)

## State Directories

| Path | Contents |
|------|---------|
| `local/agents/` | Agent metadata, auth profiles, session indices |
| `local/dashboard/` | Dashboard data, attachments, run snapshots |
| `local/identity/` | Device identity and auth snapshots |
| `local/logs/` | Host product logs and PID files |
| `local/memory/` | Memory databases |
| `local/models/` | Model files for Docker / local runtime |
| `local/runtime/` | Runtime assets (e.g. voice speakers) |
| `local/output/` | Exports and scratch artifacts |
| `.mission-control/` | Automations, knowledge base, control-plane state |

## Agent Workspace Pattern

Each `workspace-{agent_id}/` directory follows this shape:

```
workspace-assistant/
  MEMORY.md                   # High-level memory
  memory/preferences.md       # Stable preferences
  memory/decisions.md         # Key decisions
  memory/project-knowledge.md # Shared context
  data/                       # Per-workspace runtime data
```

## Working Rules

- Don't commit these directories — they're runtime data.
- Don't delete `local/memory/*.sqlite` or `workspace-*/memory/` unless you intend to reset state.
- Safe to clean: `local/output/`, transient logs, debug exports.
- Keep code changes in `backend/`, `apps/frontend/`, `platform/`.

## Safe Cleanup

| Safe to Delete | Don't Delete Without Intent |
|----------------|---------------------------|
| `local/output/*` | `local/memory/*.sqlite` |
| Transient logs | `workspace-*/memory/` |
| Debug exports | Active `.pid` files |
| `.pytest_cache/` | `local/dashboard/computer-use/runs/` |
