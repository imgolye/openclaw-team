# Voice Runtime

This project can run speech / TTS as a standalone OpenAI-compatible service so both host mode and Docker mode reuse the same endpoint.

Shared host/container defaults now come from:

- `/Users/gaolei/Documents/AI/mission-control/platform/config/runtime-profiles.json`
- detailed install + integration guide: `/Users/gaolei/Documents/AI/mission-control/docs/runtime/voice-service-integration.md`

## Recommended Shape

- Run a dedicated TTS service on `127.0.0.1:8090`
- Keep local LLM runtime on `127.0.0.1:11434`
- Point both host and container builds to the same speech runtime URL

Default product config now treats speech as a separate runtime:

- Host default: `http://127.0.0.1:8090/v1`
- Container default: `http://sherpa-onnx-tts:8080/v1`

## Start Only The Voice Service

Host mode now prefers a local managed process, so Docker is no longer required on the host.

Managed scripts:

```bash
MISSION_CONTROL_RUNTIME_PROFILE=host bash platform/bin/deploy/start_voice_runtime.sh --json
MISSION_CONTROL_RUNTIME_PROFILE=host bash platform/bin/deploy/status_voice_runtime.sh --json
MISSION_CONTROL_RUNTIME_PROFILE=host bash platform/bin/deploy/stop_voice_runtime.sh --json
```

Container mode can still use Docker directly:

```bash
MISSION_CONTROL_RUNTIME_PROFILE=container bash platform/bin/deploy/start_voice_runtime.sh --json
docker compose -f platform/infra/docker/compose.voice-runtime.yml up -d --build
```

Health check:

```bash
curl http://127.0.0.1:8090/healthz
```

## Make Host And Container Share One Voice Service

If the standalone service runs on the host, set:

```bash
MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL=http://127.0.0.1:8090/v1
```

If the main app runs in Docker and should still use the sidecar in the same compose project, keep:

```bash
MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL=http://sherpa-onnx-tts:8080/v1
```

If the containerized app should call a host-level standalone service instead, use:

```bash
MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL=http://host.docker.internal:8090/v1
```

## Supported Overrides

General overrides:

- `MISSION_CONTROL_SPEECH_RUNTIME_PROVIDER`
- `MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL`
- `MISSION_CONTROL_SPEECH_RUNTIME_MODEL`
- `MISSION_CONTROL_SPEECH_RUNTIME_API_KEY_ENV`

Provider-specific overrides:

- `MISSION_CONTROL_SHERPA_ONNX_RUNTIME_BASE_URL`
- `MISSION_CONTROL_SHERPA_ONNX_RUNTIME_MODEL`
- `MISSION_CONTROL_SHERPA_ONNX_RUNTIME_API_KEY_ENV`
- `MISSION_CONTROL_QWEN3_TTS_RUNTIME_BASE_URL`
- `MISSION_CONTROL_QWEN3_TTS_RUNTIME_MODEL`
- `MISSION_CONTROL_QWEN3_TTS_RUNTIME_API_KEY_ENV`
- `MISSION_CONTROL_ZHIPU_TTS_RUNTIME_BASE_URL`
- `MISSION_CONTROL_ZHIPU_TTS_RUNTIME_MODEL`
- `MISSION_CONTROL_ZHIPU_TTS_RUNTIME_API_KEY_ENV`

These overrides are respected by:

- OpenClaw runtime speech configuration
- customer access / WeChat voice reply generation
- host mode and container mode runtime resolution

## Why Port 8090

`8080` is already commonly used by local model runtimes in this project. Moving speech to `8090` prevents the TTS runtime from colliding with local LLM serving.
