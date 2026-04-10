# Local State Root

This directory holds repo-local runtime state and developer-only artifacts that do not belong to the main product source tree.

## Contents

- `agents/`
  Repo-local agent auth/session files used for compatibility and local debugging.
- `dashboard/`
  Local dashboard snapshots and generated runtime state.
- `identity/`
  Local device identity and auth files.
- `logs/`
  Host-product and local runtime logs.
- `memory/`
  Local sqlite memory stores and related runtime memory data.
- `output/`
  Local package exports, scratch output, and generated delivery bundles.
- `runtime/`
  Repo-local runtime support assets such as customer voice speakers.
- `claude-code/`
  Nested companion repository used by desktop Claude engine tooling.

## Rule

Treat everything here as local state unless a task is explicitly about operations, migration, or runtime debugging.
