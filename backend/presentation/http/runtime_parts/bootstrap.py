"""Runtime part: bootstrap entrypoints."""

import argparse
import json

from backend.presentation.http.handler import CollaborationDashboardHandler
from backend.presentation.http.runtime import PRODUCT_VERSION

CollaborationDashboardHandler.server_version = f"OpenClawTeam/{PRODUCT_VERSION}"


def _bootstrap_agent_harness(openclaw_dir):
    """Initialize the Agent Harness (hooks, cost tracking, tool registry, coordinator, dispatcher).

    Auto-discovers agent roles from the OpenClaw config to register capabilities.
    """
    try:
        from backend.application.services.harness_bootstrap import bootstrap_harness
        from backend.health_dashboard import load_openclaw_config, load_agents_from_config

        config = load_openclaw_config(openclaw_dir)
        agents = load_agents_from_config(config)

        # Map agent_id → role name for capability registration
        # load_agents_from_config returns (id, name, description) tuples
        agent_roles = {}
        for agent_id, name, desc in agents:
            # Infer role from agent id or name
            name_lower = (name or agent_id).lower()
            if any(k in name_lower for k in ("engineer", "dev", "eng")):
                agent_roles[agent_id] = "engineering"
            elif any(k in name_lower for k in ("qa", "test", "quality")):
                agent_roles[agent_id] = "qa"
            elif any(k in name_lower for k in ("design", "ui", "ux")):
                agent_roles[agent_id] = "design"
            elif any(k in name_lower for k in ("ops", "devops", "sre", "运维")):
                agent_roles[agent_id] = "operations"
            elif any(k in name_lower for k in ("pm", "product", "strateg", "产品")):
                agent_roles[agent_id] = "product-strategist"
            elif any(k in name_lower for k in ("coord", "router", "协调")):
                agent_roles[agent_id] = "project-coordinator"
            else:
                agent_roles[agent_id] = "general"

        harness = bootstrap_harness(agent_roles=agent_roles)
        print(f"Agent Harness initialized: {harness.hooks.handler_count()} hooks, "
              f"{harness.tool_registry.tool_count} tools, "
              f"{len(agent_roles)} agents")
        return harness
    except Exception as exc:
        import traceback
        print(f"[warn] Agent Harness bootstrap skipped: {exc}")
        traceback.print_exc()
        return None


def serve_dashboard(openclaw_dir, output_dir, port, live_interval, frontend_dist="", cors_origins="", automation_interval=60.0, host="127.0.0.1"):
    bind_host = str(host or "127.0.0.1").strip() or "127.0.0.1"
    ensure_default_install_bootstrap(openclaw_dir)
    _bootstrap_agent_harness(openclaw_dir)
    server = MissionControlHTTPServer((bind_host, port), CollaborationDashboardHandler)
    server.openclaw_dir = Path(openclaw_dir)
    server.output_dir = Path(output_dir) if output_dir else Path(openclaw_dir) / "dashboard"
    server.live_interval = live_interval
    server.automation_interval = automation_interval
    server.automation_stop = threading.Event()
    server.allow_runtime_restart = True
    server.allow_gateway_restart = True
    server.dashboard_auth_token = resolve_dashboard_auth_token(server.openclaw_dir)
    server.live_event_bus = RedisLiveEventBus(
        resolve_live_event_redis_url(server.openclaw_dir),
        LIVE_EVENT_REDIS_CHANNEL,
        server.server_instance_id,
        server._publish_live_event_local,
    )
    server.frontend_dist = resolve_frontend_dist(server.openclaw_dir, explicit_path=frontend_dist)
    server.cors_origins = parse_cors_origins(cors_origins)
    warm_dashboard_bundle_async(server.openclaw_dir, server.output_dir)
    automation_worker = start_automation_engine(server)
    display_host = bind_host if bind_host not in {"0.0.0.0", "::"} else "127.0.0.1"
    if server.frontend_dist:
        print(f"Serving OpenClaw Team API at http://{display_host}:{port}/api/dashboard")
        print(f"Serving separated frontend at http://{display_host}:{port}/")
    else:
        print(f"Serving OpenClaw Team API at http://{display_host}:{port}/api/dashboard")
        print("Frontend dist is not configured; UI routes will return 503 until apps/frontend/dist is built.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # Emit harness shutdown hook
        try:
            from backend.domain.core.hooks import HookPhase, HookRegistry
            HookRegistry.default().emit(HookPhase.ON_SHUTDOWN, source="serve_dashboard")
        except Exception:
            pass
        server.automation_stop.set()
        if server.live_event_bus is not None:
            server.live_event_bus.close()
        server.server_close()
        automation_worker.join(timeout=5)
        wait_for_task_execution_dispatches(timeout=5.0)
        wait_for_dashboard_bundle_workers(server.openclaw_dir, server.output_dir, timeout=5.0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--dump-openclaw-control", action="store_true")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--port", type=int, default=18890)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--live-interval", type=float, default=2.0)
    parser.add_argument("--automation-interval", type=float, default=15.0)
    parser.add_argument("--frontend-dist", default="")
    parser.add_argument("--cors-origins", default=",".join(sorted(DEFAULT_FRONTEND_ORIGINS)))
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    openclaw_dir = infer_openclaw_dir(args.dir)
    ensure_default_install_bootstrap(openclaw_dir)
    if args.dump_openclaw_control:
        payload = load_openclaw_control_data(openclaw_dir)
        print(json.dumps(payload, ensure_ascii=False))
        return
    if args.serve:
        serve_dashboard(
            openclaw_dir,
            args.output_dir or None,
            args.port,
            args.live_interval,
            frontend_dist=args.frontend_dist,
            cors_origins=args.cors_origins,
            automation_interval=args.automation_interval,
            host=args.host,
        )
        return
    data, paths = build_dashboard_bundle(openclaw_dir, args.output_dir or None)
    if not args.quiet:
        print(f"Generated dashboard JSON: {paths['json']}")


if __name__ == "__main__":
    main()
