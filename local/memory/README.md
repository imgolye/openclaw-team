# Local Memory Databases

This directory stores local SQLite memory databases used by the running product instance.

## Typical Files

- `main.sqlite`
- `assistant.sqlite`

## Notes

- These are stateful runtime databases, not generated throwaway cache.
- Do not remove or rename them unless you are intentionally resetting or migrating local memory.
- Editable workspace memory documents live under `workspace-*/MEMORY.md` and `workspace-*/memory/`; this directory is the database side of the memory system.
