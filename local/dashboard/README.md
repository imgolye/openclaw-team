# Dashboard Runtime Data

This directory stores generated dashboard-side runtime data for the local Mission Control instance.

## Typical Contents

- `computer-use/runs/`
  Computer-use run manifests, plans, and artifacts. These are generated runtime files and are now ignored by default.
- `conversation-attachments/`
  Saved chat attachment payloads by agent/workspace
- `identity/`
  Local device identity information
- `logs/`
  Dashboard-specific runtime logs and health snapshots
- `openclaw.json`
  Runtime copy of OpenClaw-related state for the dashboard surface

## Notes

- Treat this directory as generated state unless the task is specifically about operations, debugging, or runtime-data repair.
- Avoid manual cleanup of active run data while the product is using it.
- Historical run snapshots should not be committed back into the repository.
