# PostgreSQL Schema

## Files

| File | Purpose |
|------|---------|
| `schema.sql` | Full schema baseline exported from PostgreSQL 16. Reference only. |

## How it works

You **don't need to import this file manually**. The backend auto-creates all tables on first start via `backend/adapters/storage/schema.py`.

This file is kept as a reference for:
- Reviewing the full table structure at a glance
- Diffing schema changes between versions
- Manual recovery if needed

## Default data

Default users, teams, skills, and automation rules are created automatically by the bootstrap process (`ensure_default_install_bootstrap`). No seed file needed.

## Default accounts

Three accounts are created on first start:

| Username | Role | Password |
|----------|------|----------|
| `owner` | Owner | Set via `MISSION_CONTROL_OWNER_PASSWORD` |
| `operator` | Operator | Set via `MISSION_CONTROL_OPERATOR_PASSWORD` |
| `viewer` | Viewer | Set via `MISSION_CONTROL_VIEWER_PASSWORD` |

If environment variables are not set, random passwords are generated and printed to the console on first boot. Set passwords in `.env` for stable access.

## Refresh schema baseline

```bash
bash platform/bin/deploy/refresh_product_database_schema.sh
```
