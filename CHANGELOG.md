# OpenClaw Team Changelog

## 1.18.0 - 2026-03-14

### Added
- Added a multi-tenant SaaS control plane to Mission Control, including tenant records, tenant-to-installation bindings, tenant API keys, and tenant-level admin summaries.
- Added tenant-scoped REST API endpoints under `/api/v1/tenants/...` so external CI/CD or webhook systems can read dashboards, list agents/tasks, and create tasks with API-key authentication.
- Added a commercial Admin workspace extension for tenant creation, installation binding, API-key issuance, and SaaS readiness guidance.
- Added Docker delivery artifacts (`Dockerfile`, `docker-compose.yml`, `platform/bin/deploy/docker_bootstrap.sh`) for one-command remote deployment of Mission Control plus OpenClaw runtime bootstrap.
- Added `platform/bin/deploy/export_dashboard_postgres.py` to export the SQLite product kernel into PostgreSQL-friendly schema and CSV bundles for larger-scale persistence.

### Changed
- Expanded Mission Control from a powerful local operator console into a tenant-aware product foundation that can be handed to other teams.
- Reworked tenant summaries to use lightweight task/agent aggregation so multi-tenant admin views stay stable without recursively rebuilding nested dashboards.
- Updated product/runtime version metadata to `1.18.0`.

## 1.17.0 - 2026-03-14

### Added
- Added broader frontend internationalization coverage for the high-frequency Mission Control workspaces, including Tasks, Conversations, Themes, Skills, and OpenClaw views.
- Added a mobile-friendly task workspace mode with responsive card layouts so the local product remains usable on phone-sized screens.
- Added a PWA shell for the separated frontend, including `manifest.webmanifest`, service-worker registration, installable metadata, and offline static-asset caching.
- Added API integration tests plus Playwright end-to-end coverage for login, overview analytics, mobile task rendering, and PWA surface checks.

### Changed
- Tightened the React frontend delivery baseline by syncing the new PWA assets and test harness into the runtime/tooling flow.
- Rebalanced Vite manual chunking so the commercial frontend no longer emits the previous `>500 kB` production bundle warning while preserving route-level lazy loading.
- Updated setup/runtime version metadata to `1.17.0`.

## 1.16.0 - 2026-03-14

### Added
- Added a separated `apps/frontend/` application built with React, Vite, and Ant Design so Mission Control now has a real product frontend instead of only inline HTML generated from Python.
- Added JSON auth endpoints for the new frontend, including `/api/auth/session`, `/api/auth/login`, and `/api/auth/logout`.
- Added frontend-aware serving mode to `collaboration_dashboard.py`, with automatic `apps/frontend/dist` detection, SPA route support, and `/legacy` fallback for the previous monolithic dashboard.
- Added `platform/bin/runtime/build_frontend.sh` and `platform/bin/runtime/sync_runtime_assets.sh` so existing installs can build the new SPA and sync runtime assets without rerunning the full interactive installer.
- Added chart-driven overview analytics with a task funnel, agent load distribution, and 24-hour activity trend rendered directly inside the React Mission Control shell.
- Added a relay network visualization to the Activity workspace plus a card/table toggle in Agent Ops so operators can scan coordination and load in more than one view.

### Changed
- Promoted the local product from a server-rendered single-file console into an API-first backend plus independent frontend architecture.
- Updated Mission Control runtime serving to support local frontend development via CORS-enabled API access, while preserving same-origin production serving after `frontend` build output exists.
- Split the new React frontend by route and moved drawers/modals behind lazy-loaded secondary chunks, reducing the largest shared frontend bundle from roughly `917 kB` to about `581 kB` in production build output.
- Deferred global search filtering in the React shell so typing across agent, task, and conversation datasets stays responsive under larger local runtimes.
- Reframed the login screen, product shell, and overview workspace into a more commercial control-plane layout with stronger brand hierarchy, operator summary surfaces, and executive-facing first-screen presentation.
- Added theme-aware frontend language switching and localized the new overview, activity, and agent visualization surfaces for Chinese and English themes.
- Added offline snapshot fallback for `/api/dashboard` so the product can keep rendering the latest local operational picture even when the live runtime is temporarily unavailable.
- Updated setup/runtime version metadata to `1.16.0`.

## 1.15.0 - 2026-03-14

### Added
- Added a managed installation registry to the Mission Control product kernel so one local control plane can track multiple OpenClaw installs.
- Added an installation fleet workspace inside `/admin`, including instance cards, local-path registration, and stale-instance removal actions for Owners.
- Added installation-registry coverage to `tests/test_dashboard_store.py`, extending the automated baseline from users/audit into multi-instance product state.

### Changed
- Expanded the commercial admin workspace from “seat governance” into the first multi-install control plane, with per-instance theme, router, task-count, and status visibility.
- Promoted the current install to auto-register itself into the fleet registry so the product can always reason about “current instance vs. other managed instances”.

## 1.14.0 - 2026-03-14

### Added
- Added `backend/dashboard_store.py` as the first Mission Control product-kernel module, backed by SQLite and responsible for product-user plus audit-event storage.
- Added automatic migration from legacy `product_users.json` and `audit-log.jsonl` into `dashboard/dashboard.db` so existing installs can move forward without losing local product state.
- Added `tests/test_dashboard_store.py` and a GitHub Actions CI workflow to establish the first automated regression baseline for the product kernel.

### Changed
- Switched Mission Control account and audit persistence from ad-hoc JSON / JSONL writes to the new SQLite-backed storage layer while keeping the public product behavior unchanged.
- Extended install, validation, and theme-switch runtime script deployment so `dashboard_store.py` is present everywhere the Mission Control app runs.

## 1.13.0 - 2026-03-14

### Added
- Added direct per-agent dialogue entrypoints across Mission Control, including one-click conversation launch from Agent cards, Agent drawers, and a new per-agent launcher inside `/conversations`.
- Added explicit main-session targeting in the Conversation Center so operators can start from an Agent first instead of hunting through the session list.

### Changed
- Reframed the conversation workspace from a session browser into an agent-first communication surface, making every configured agent visibly reachable as a dialogue partner.
- Updated the conversation focus summary so the product clearly shows whether you are looking at a real selected session or preparing to talk to an agent's main session.

## 1.12.0 - 2026-03-14

### Added
- Added a Conversation Center inside Mission Control with `/conversations`, real OpenClaw session browsing, transcript inspection, and direct in-product dialogue with agents.
- Added `/api/conversations` and `/api/conversations/transcript` so the product can read native OpenClaw sessions and per-session jsonl transcripts without inventing a parallel chat store.
- Added authenticated in-product conversation actions backed by `openclaw agent --json`, including transcript refresh after each message.

### Changed
- Expanded Mission Control from “see + operate” into “see + operate + converse”, so operators can stay inside the product for both task control and live agent dialogue.
- Introduced a dedicated conversation permission for `Owner / Operator`, keeping `Viewer` read-only while still exposing real transcript visibility.

### Fixed
- Fixed mixed-output parsing for `openclaw agent --json` and `openclaw sessions --json`, so plugin logs no longer break conversation data or chat actions.
- Fixed product refresh behavior so sending a conversation message immediately refreshes the session list and transcript instead of waiting for passive polling.

## 1.11.0 - 2026-03-14

### Added
- Added an OpenClaw Control Center inside Mission Control with `/openclaw` and `/api/openclaw` surfaces for native version, schema, gateway, and skills visibility.
- Added in-product publishing from local repo skills into the current OpenClaw managed skills directory, so owner users can promote a skill without leaving the product.
- Added managed-skills directory visibility and OpenClaw baseline compatibility signals for `2026.3.12+`.

### Changed
- Tightened Skills Center to focus on repo-local skills while letting the OpenClaw view own native managed-skill runtime visibility.
- Upgraded OpenClaw JSON parsing to tolerate mixed CLI output where plugin or warning logs are interleaved with structured JSON.

### Fixed
- Fixed stale post-action UI state by clearing cached OpenClaw and skill payloads after product mutations such as skill publish, theme switch, or packaging.
- Fixed deep-link support for the OpenClaw workspace so `/openclaw` now renders like the rest of the product routes.
- Fixed local publish-state detection to work even when `openclaw skills list --json` truncates discovery on very large managed skill roots.

## 1.10.0 - 2026-03-14

### Added
- Added `platform/bin/install/skill_utils.py` to scan, validate, scaffold, and package Claude-style skill folders using Anthropic's published skill structure patterns.
- Added a new Skills Center product workspace in Mission Control with skill catalog visibility, quality signals, scaffold actions, and zip packaging.
- Added a sample `mission-control-release-ops` skill to demonstrate progressive disclosure with `references/` and release-oriented workflow guidance.

### Changed
- Expanded Mission Control from an operations/admin surface into a local skills product as well, with `/skills` and `/api/skills` routes.
- Wired the dashboard runtime to read skill metadata from the source repository and surface distribution-ready commands directly in the UI.

### Fixed
- Ensured generated skill scaffolds now pass their first validation instead of emitting malformed YAML frontmatter.
- Ignored packaged `dist/` artifacts so skill zip creation does not dirty the repository with generated files.

## 1.9.0 - 2026-03-14

### Added
- Added multi-user Mission Control access with local product accounts, role-based permissions, and Owner Token fallback bootstrap.
- Added a commercial admin workspace inside Mission Control for seat provisioning, role/state governance, password resets, and audit visibility.
- Added secure password hashing plus signed session cookies for local product login.

### Changed
- Split dashboard admin data into public summaries and authenticated sensitive detail, so static snapshots no longer expose seat rosters or audit trails.
- Elevated Mission Control from a protected local tool to a more commercial-grade product surface with account lifecycle and governance workflows.

### Fixed
- Restored the missing `now_iso()` helper so account, audit, and session features no longer fail at runtime.
- Prevented the last active Owner from being downgraded or suspended, avoiding accidental product lockout.
- Normalized account status handling so suspended seats are enforced consistently across login, admin UI, and stored records.

## 1.8.0 - 2026-03-14

### Added
- Added an in-product task action studio so users can create work directly from the Tasks module without leaving Mission Control.
- Added task-operation forms inside the task replay drawer for progress updates, blocking, and completion handoff.
- Added authenticated product action endpoints for task creation, progress, blocking, completion, and live theme switching.

### Changed
- Linked generated installs back to the source repository so the product can invoke `switch_theme.py` safely from inside the UI.
- Promoted Mission Control from a read-only dashboard into an operational product surface with toast feedback and action-aware runtime state.

### Fixed
- Returned a non-zero exit code when task creation is rejected, so product actions no longer report false success.
- Allowed navigation to continue while a drawer is open instead of letting the scrim block the left menu.
- Kept generated config metadata in sync with the current release version.

## 1.7.0 - 2026-03-14

### Added
- Added a local login flow at `/login` with cookie-based session access for Mission Control routes, APIs, and live events.
- Added layout controls for collapsing the left menu and switching between operations, focus, and compact product layouts with persisted preferences.

### Changed
- Turned the Mission Control shell into a protected local product surface with navigation, authenticated app routes, and signed local session state.
- Expanded the top navigation to include menu controls, layout switching, and authenticated sign-out actions.

### Fixed
- Ensured unauthenticated requests to product APIs and SSE streams now return proper auth gating instead of exposing data directly.
- Preserved layout choices across reloads so the product opens in the same working mode the user last selected.

## 1.6.0 - 2026-03-14

### Added
- Added a product-style Mission Control app shell with dedicated Overview, Agents, Tasks, Activity, and Themes modules.
- Added local API endpoints for agents, tasks, events, themes, and deliverables so the dashboard can act as a real product surface instead of a single visual page.
- Added integrated product runbook cards that expose common local commands directly inside the app.

### Changed
- Reframed `collaboration_dashboard.py --serve` as a local multi-view application with navigation, search, deep-linkable routes, and dedicated work areas for operations and delivery.
- Expanded dashboard payloads with theme catalog, router context, command palette data, and deliverables inventory to support richer product workflows.

### Fixed
- Kept browser console output clean across the new multi-route app experience.
- Preserved drawer-based drill-downs while expanding the app beyond a single-page overview.

## 1.5.0 - 2026-03-14

### Added
- Added clickable agent inspector drawers so users can open any agent card and inspect live focus, recent signals, and in-hand tasks.
- Added task replay drawers that expose route, TODO progress, and chronological handoff/progress history from both task cards and timeline events.

### Changed
- Refactored `backend/collaboration_dashboard.py` into a richer mission-control UI with drill-down interactions while keeping HTML + JSON snapshot generation and the live SSE server.
- Expanded dashboard JSON payloads with `taskIndex`, replay entries, recent agent signals, and active task cards to support richer downstream visualizations.

### Fixed
- Counted only non-terminal tasks toward agent active-state metrics so completed work no longer leaves agents falsely marked active.
- Suppressed the browser `favicon.ico` 404 noise in the live dashboard UI.

## 1.4.0 - 2026-03-14

### Added
- Added a live local web panel for the collaboration dashboard with `/api/dashboard` and Server-Sent Events (`/events`) endpoints.
- Added real-time client-side updates for the mission-control UI without full-page reloads.

### Changed
- Upgraded `collaboration_dashboard.py --serve` from static file hosting to a real-time local dashboard server.
- Updated collaboration dashboard signatures so change events only fire on substantive task/agent updates.

### Fixed
- Avoided noisy continuous dashboard events caused by timestamp-only changes.
- Preserved static snapshot generation while enabling a live browser panel for the same dashboard.

## 1.3.0 - 2026-03-14

### Added
- Added `backend/collaboration_dashboard.py` to generate an HTML + JSON mission-control view for live multi-agent collaboration.
- Added automatic collaboration dashboard generation during install, task refresh, and theme switching.

### Changed
- Updated the README to surface the collaboration dashboard and visual coordination workflow.
- Extended installation validation to require the collaboration dashboard runtime script.

### Fixed
- Ensured theme switching preserves the visual dashboard capability after agent/workspace migrations.

## 1.2.0 - 2026-03-14

### Added
- Added `platform/bin/install/switch_theme.py` and `platform/bin/install/switch_theme.sh` so existing installations can switch themes without reinstalling.
- Added theme schema validation and shared migration helpers in `platform/bin/install/lib/theme_utils.py`.

### Changed
- Preserved existing channel config, models, memory search settings, and task prefixes when regenerating `openclaw.json`.
- Preserved existing secrets and gateway token when rerunning `setup.sh`.
- Tightened default elevated allowlists so new installs no longer inherit wildcard access by default.

### Fixed
- Replaced the old `eval`-based theme loading in `setup.sh` with validated Python parsing.
- Deployed `health_dashboard.py` to every workspace and updated validation to check it.
- Made `health_dashboard.py` support `--dir`, `OPENCLAW_DIR`, and per-workspace auto-detection.
- Replaced the Unix-only `fcntl` JSON lock with a cross-platform lock implementation.
- Migrated task boards, agent session directories, and workspace artifacts across themes when switching.

## 1.1.0 - 2026-03-14

### Changed
- Upgraded the installer and validator to fully provision and verify all 11 agents, including the briefing role.
- Aligned generated `openclaw.json` defaults with the current OpenClaw 2026.3.12 session and gateway settings.
- Added `sessions_yield` to theme tool allowlists and generated SOUL guidance for planner, dispatcher, departments, and briefing roles.

### Fixed
- Fixed `setup.sh` so the theme task prefix is honored by default and the briefing agent is no longer skipped during installation.
- Fixed `validate.sh` so `--dir` works as documented and incomplete installations fail validation.
- Fixed `kanban_update.py` so missing tasks return non-zero exit codes instead of logging false success.
- Fixed task state transitions to preserve the real responsible department during `Doing` and `Done` phases.
- Fixed `health_dashboard.py` so active tasks are loaded from the current router workspace instead of hardcoding `workspace-assistant`.

### Compatibility
- Verified against OpenClaw `2026.3.12`.
- Confirmed clean isolated installs for the `corporate` theme.

## 1.0.0 - 2026-03-12

- Initial release of the Mission Control multi-agent orchestration template for OpenClaw.
