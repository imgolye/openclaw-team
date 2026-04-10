"""Runtime part: theme."""

def infer_theme_name_from_agents(config):
    return DEFAULT_THEME_NAME


def theme_switch_status_path(openclaw_dir):
    return dashboard_dir(openclaw_dir) / "theme-switch-status.json"


def load_theme_switch_status(openclaw_dir):
    path = theme_switch_status_path(openclaw_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


def save_theme_switch_status(openclaw_dir, payload):
    path = theme_switch_status_path(openclaw_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def perform_theme_switch(openclaw_dir, theme_name):
    project_dir = resolve_project_dir(openclaw_dir)
    if not project_dir:
        raise RuntimeError("当前安装没有关联仓库目录，暂时无法在产品内切换主题。")
    switch_script = project_dir / "platform" / "bin" / "install" / "switch_theme.py"
    if not switch_script.exists():
        raise RuntimeError(f"缺少主题切换脚本: {switch_script}")
    result, output = run_python_script(
        switch_script,
        ["--theme", theme_name, "--dir", str(Path(openclaw_dir).expanduser().resolve())],
        cwd=project_dir,
    )
    if result.returncode != 0:
        raise RuntimeError(output or f"切换主题失败: {theme_name}")


def preview_theme_switch(openclaw_dir, theme_name):
    theme_key = str(theme_name or "").strip()
    preview = {
        "name": theme_key,
        "displayName": THEME_CATALOG.get(theme_key, {}).get("displayName", theme_key),
        "routerAgentId": "",
    }
    project_dir = resolve_project_dir(openclaw_dir)
    if not project_dir:
        return preview
    theme_file = Path(project_dir) / "platform" / "config" / "themes" / theme_key / "theme.json"
    if not theme_file.exists():
        return preview
    try:
        theme_payload = json.loads(theme_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return preview
    preview["displayName"] = theme_payload.get("display_name", preview["displayName"])
    preview["routerAgentId"] = (
        ((theme_payload.get("roles") or {}).get("router") or {}).get("agent_id", "")
    )
    return preview


def run_theme_switch_job(server, job_id, theme_name, actor):
    openclaw_dir = Path(getattr(server, "openclaw_dir", "")).expanduser().resolve()

    def update_status(**updates):
        current = load_theme_switch_status(openclaw_dir)
        if current.get("id") != job_id:
            current = {"id": job_id}
        current.update(updates)
        if "updatedAt" not in updates:
            current["updatedAt"] = now_iso()
        save_theme_switch_status(openclaw_dir, current)
        return current

    try:
        update_status(status="running", stage="switching_theme", updatedAt=now_iso())
        perform_theme_switch(openclaw_dir, theme_name)
        config = load_config(openclaw_dir)
        metadata = load_project_metadata(openclaw_dir, config=config)
        router_agent_id = get_router_agent_id(config)
        update_status(
            status="running",
            stage="refreshing_gateway",
            themePreview={
                "name": metadata.get("theme", theme_name),
                "displayName": metadata.get(
                    "displayName",
                    THEME_CATALOG.get(theme_name, {}).get("displayName", theme_name),
                ),
                "routerAgentId": router_agent_id,
            },
            updatedAt=now_iso(),
        )
        append_audit_event(
            openclaw_dir,
            "theme_switch",
            actor,
            detail=f"切换主题到 {metadata.get('theme', theme_name)}",
            meta={"theme": metadata.get("theme", theme_name)},
        )
        try:
            gateway_result = perform_gateway_service_action(openclaw_dir, "restart")
        except (OSError, RuntimeError):
            gateway_result = perform_gateway_service_action(openclaw_dir, "start")
        update_status(
            status="running",
            stage="restarting_runtime",
            gatewayRestart=gateway_result,
            updatedAt=now_iso(),
        )
        runtime_result = schedule_runtime_restart(server, router_agent_id)
        update_status(
            status="completed",
            stage="completed",
            completedAt=now_iso(),
            runtimeRestart=runtime_result,
            message=f"主题已切换到 {metadata.get('displayName', theme_name)}。",
            updatedAt=now_iso(),
        )
    except Exception as error:
        update_status(
            status="failed",
            stage="failed",
            completedAt=now_iso(),
            message=str(error),
            error=str(error),
            updatedAt=now_iso(),
        )
        append_audit_event(
            openclaw_dir,
            "theme_switch",
            actor,
            outcome="error",
            detail=str(error),
            meta={"theme": theme_name},
        )


def start_theme_switch_job(server, theme_name, actor):
    openclaw_dir = Path(getattr(server, "openclaw_dir", "")).expanduser().resolve()
    current = load_theme_switch_status(openclaw_dir)
    if current.get("status") in {"queued", "running"}:
        return current

    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    preview = preview_theme_switch(openclaw_dir, theme_name)
    payload = {
        "id": secrets.token_hex(8),
        "status": "queued",
        "stage": "queued",
        "message": f"主题切换已开始，正在切到 {preview['displayName']}。",
        "fromThemeName": metadata.get("theme", ""),
        "fromThemeDisplayName": metadata.get("displayName", metadata.get("theme", "")),
        "fromRouterAgentId": get_router_agent_id(config),
        "targetTheme": theme_name,
        "targetThemeDisplayName": preview.get("displayName", theme_name),
        "targetRouterAgentId": preview.get("routerAgentId", ""),
        "themePreview": preview,
        "actor": actor,
        "createdAt": now_iso(),
        "updatedAt": now_iso(),
    }
    save_theme_switch_status(openclaw_dir, payload)
    run_inline = not getattr(server, "allow_runtime_restart", True) and not getattr(server, "allow_gateway_restart", True)
    if run_inline:
        run_theme_switch_job(server, payload["id"], theme_name, actor)
        return load_theme_switch_status(openclaw_dir)
    worker = threading.Thread(
        target=run_theme_switch_job,
        args=(server, payload["id"], theme_name, actor),
        daemon=True,
    )
    worker.start()
    return payload
