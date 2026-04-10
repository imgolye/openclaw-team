# Runtime Profiles

OpenClaw Team now has one shared runtime profile source for host mode and container mode:

- `/Users/gaolei/Documents/AI/mission-control/platform/config/runtime-profiles.json`

This file defines the default product port, bind host, OpenClaw state location, local model runtime URL, and speech runtime URL for each startup shape.

## Profiles

- `host`
  - product: `http://127.0.0.1:18891`
  - speech runtime: `http://127.0.0.1:8090/v1`
  - local runtime: `http://127.0.0.1:11434/v1`
- `container`
  - product: `http://127.0.0.1:18890`
  - speech runtime: `http://sherpa-onnx-tts:8080/v1`
  - local runtime: `http://host.docker.internal:11434/v1`

## Resolver

Use the resolver to inspect the effective defaults:

```bash
python3 platform/bin/runtime/resolve_runtime_profile.py --profile host --field product.baseUrl
python3 platform/bin/runtime/resolve_runtime_profile.py --profile container --field speechRuntime.baseUrl
python3 platform/bin/runtime/resolve_runtime_profile.py --profile host --format json
```

## Host Startup Scripts

These scripts now read `MISSION_CONTROL_RUNTIME_PROFILE` and default to `host`:

- `platform/bin/deploy/start_host_product.sh`
- `platform/bin/deploy/status_host_product.sh`
- `platform/bin/deploy/stop_host_product.sh`

Examples:

```bash
bash platform/bin/deploy/start_host_product.sh
bash platform/bin/deploy/status_host_product.sh --json
MISSION_CONTROL_RUNTIME_PROFILE=host bash platform/bin/deploy/stop_host_product.sh
```

## Container Bootstrap

`platform/bin/deploy/docker_bootstrap.sh` now reads the same shared profile source and defaults to `container`.

`docker-compose.yml` also exports:

```bash
MISSION_CONTROL_RUNTIME_PROFILE=container
```

So the product container, its speech runtime default, and its local model runtime default all come from the same profile model.

## Override Priority

The precedence is:

1. explicit CLI arguments
2. explicit environment variables
3. shared runtime profile defaults

This keeps host mode and container mode distinguishable, but managed from one place.

## Frontend And Desktop Client

The browser app and Electron desktop app now read their default backend target from the same root profile source instead of duplicating `18891` / `18890` locally.

- `/Users/gaolei/Documents/AI/mission-control/apps/frontend/config/runtimeConfig.js`
- `/Users/gaolei/Documents/AI/mission-control/apps/desktop-client/config/runtimeConfig.js`
- `/Users/gaolei/Documents/AI/mission-control/apps/desktop-client/config/runtimeConfig.cjs`

Their local `runtime-config.json` files still own app-specific routes and dev ports, but the product base URLs now come from `/Users/gaolei/Documents/AI/mission-control/platform/config/runtime-profiles.json`.
