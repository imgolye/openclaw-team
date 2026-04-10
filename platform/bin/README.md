# Bin Layout

`bin/` is now organized around four operational lanes:

- `platform/bin/install/`: install, initialize, theme, auth, permissions
- `platform/bin/runtime/`: runtime sync and frontend build
- `platform/bin/deploy/`: Docker bootstrap and delivery packaging
- `platform/bin/verify/`: installation validation and smoke checks

These four subdirectories are the only supported entrypoints.

Shared helper modules now live under `platform/bin/install/lib/`, and the top-level
`bin/` root should stay free of wrappers and one-off utilities.
