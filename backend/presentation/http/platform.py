from __future__ import annotations

def _handle_platform_commands(handler, path, payload, svc):
    if path == "/api/actions/theme/switch":
        if not handler._require_capability("themeWrite", "只有 Owner 可以切换主题。"):
            return True
        theme_name = str(payload.get("theme", "")).strip()
        if theme_name not in svc.SELECTABLE_THEME_CATALOG:
            raise RuntimeError(f"未知主题：{theme_name}")
        job = svc.start_theme_switch_job(handler.server, theme_name, handler._current_actor())
        theme_preview = job.get("themePreview", svc.preview_theme_switch(handler.server.openclaw_dir, theme_name))
        display_name = theme_preview.get("displayName", theme_name)
        handler._send_json({"ok": True, "message": job.get("message", f"主题切换已开始，正在切到 {display_name}。"), "result": {"job": job, "themePreview": theme_preview}})
        return True

    if path == "/api/actions/context-hub/install":
        if not handler._require_capability("adminWrite", "只有 Owner 可以安装和维护 Context Hub CLI。"):
            return True
        result = svc.perform_context_hub_install()
        handler._audit("context_hub_install", detail="安装 Context Hub CLI")
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Context Hub CLI 已安装。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["contextHub"])})
        return True

    if path == "/api/actions/context-hub/update":
        if not handler._require_capability("adminWrite", "只有 Owner 可以刷新 Context Hub registry。"):
            return True
        result = svc.perform_context_hub_update()
        handler._audit("context_hub_update", detail="刷新 Context Hub registry")
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Context Hub registry 已刷新。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["contextHub"])})
        return True

    if path == "/api/actions/context-hub/search":
        result = svc.perform_context_hub_search(
            query=str(payload.get("query", "")).strip(),
            lang=str(payload.get("lang", "")).strip(),
            tags=str(payload.get("tags", "")).strip(),
            limit=int(payload.get("limit", 8) or 8),
            openclaw_dir=handler.server.openclaw_dir,
        )
        handler._audit("context_hub_search", detail="检索 Context Hub", meta={"query": str(payload.get("query", "")).strip()})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Context Hub 检索已完成。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["contextHub"])})
        return True

    if path == "/api/actions/context-hub/get":
        result = svc.perform_context_hub_get(
            entry_id=str(payload.get("id", "")).strip(),
            lang=str(payload.get("lang", "")).strip(),
            full=bool(payload.get("full")),
            files=str(payload.get("files", "")).strip(),
            openclaw_dir=handler.server.openclaw_dir,
        )
        handler._audit("context_hub_get", detail=f"获取 Context Hub 文档 {result.get('id', '')}", meta={"id": result.get("id", "")})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Context Hub 文档已获取。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["contextHub"])})
        return True

    if path == "/api/actions/context-hub/layered-memory/save":
        if not handler._require_capability("taskWrite", "只有 Operator / Owner 可以编辑长期记忆和共享上下文。"):
            return True
        scope = str(payload.get("scope", "")).strip()
        relative_path = str(payload.get("relativePath", "")).strip()
        agent_id = str(payload.get("agentId", "")).strip()
        result = svc.save_layered_memory_document(
            handler.server.openclaw_dir,
            scope=scope,
            relative_path=relative_path,
            content=payload.get("content", ""),
            agent_id=agent_id,
        )
        handler._audit(
            "context_hub_layered_memory_save",
            detail=f"更新 {scope} 文档 {relative_path}",
            meta={"scope": scope, "relativePath": relative_path, "agentId": agent_id or result.get("agentId", "")},
        )
        svc.invalidate_context_hub_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "记忆文档已更新。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["contextHub"])})
        return True

    if path == "/api/actions/context-hub/annotate":
        if not handler._require_capability("taskWrite", "只有 Operator / Owner 可以保存 Context Hub 注释。"):
            return True
        entry_id = str(payload.get("id", "")).strip()
        clear = bool(payload.get("clear"))
        result = svc.perform_context_hub_annotate(entry_id=entry_id, note=str(payload.get("note", "")).strip(), clear=clear)
        handler._audit("context_hub_annotate", detail=f"{'清除' if clear else '保存'} Context Hub annotation", meta={"id": entry_id})
        svc.invalidate_context_hub_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Context Hub annotation 已更新。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["contextHub"])})
        return True

    if path == "/api/actions/context-hub/feedback":
        if not handler._require_capability("adminWrite", "只有 Owner 可以发送 Context Hub feedback。"):
            return True
        labels = payload.get("labels", [])
        if isinstance(labels, str):
            labels = [item.strip() for item in labels.split(",") if item.strip()]
        result = svc.perform_context_hub_feedback(
            entry_id=str(payload.get("id", "")).strip(),
            rating=str(payload.get("rating", "")).strip(),
            comment=str(payload.get("comment", "")).strip(),
            labels=labels if isinstance(labels, list) else [],
            lang=str(payload.get("lang", "")).strip(),
            file_path=str(payload.get("file", "")).strip(),
            agent=str(payload.get("agent", "")).strip(),
            model=str(payload.get("model", "")).strip(),
        )
        handler._audit("context_hub_feedback", detail="发送 Context Hub feedback", meta={"id": str(payload.get("id", "")).strip(), "rating": str(payload.get("rating", "")).strip()})
        svc.invalidate_context_hub_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Context Hub feedback 已发送。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["contextHub"])})
        return True

    if path in {"/api/actions/openclaw/gateway/start", "/api/actions/openclaw/gateway/restart"}:
        if not handler._require_capability("adminWrite", "只有 Owner 可以管理 Gateway 服务。"):
            return True
        action_name = "restart" if path.endswith("/restart") else "start"
        result = svc.perform_gateway_service_action(handler.server.openclaw_dir, action_name)
        handler._audit(f"openclaw_gateway_{action_name}", detail=f"{'重启' if action_name == 'restart' else '启动'} Gateway 服务")
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"Gateway {'重启' if action_name == 'restart' else '启动'}命令已执行。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/runtime/gateway/refresh":
        if not handler._require_capability("adminWrite", "只有 Owner 可以执行 Gateway 实时探测。"):
            return True
        probe_payload = svc.perform_openclaw_gateway_probe_refresh(handler.server.openclaw_dir)
        handler._audit("openclaw_runtime_gateway_refresh", detail="执行 Gateway 实时探测")
        handler._send_json(
            {
                "ok": True,
                "message": "Gateway 实时探测已完成。",
                "result": {"section": "gateway", "refreshedAt": ((probe_payload.get("gateway") or {}).get("probeUpdatedAt") or "")},
                "openclaw": probe_payload,
            }
        )
        return True

    if path in {"/api/actions/openclaw/install", "/api/actions/openclaw/update"}:
        if not handler._require_capability("adminWrite", "只有 Owner 可以安装 OpenClaw。" if path.endswith("/install") else "只有 Owner 可以升级 OpenClaw。"):
            return True
        action_name = "install" if path.endswith("/install") else "update"
        result = svc.perform_openclaw_cli_install_action(
            handler.server.openclaw_dir,
            action_name=action_name,
            method=str(payload.get("method", "")).strip(),
            version=str(payload.get("version", "latest")).strip() or "latest",
        )
        if bool(payload.get("syncRuntime", True)):
            result["runtimeSync"] = svc.perform_runtime_sync_action(handler.server.openclaw_dir, build_frontend=bool(payload.get("buildFrontend", True)))
        handler._audit(
            f"openclaw_{action_name}",
            detail=f"{'安装' if action_name == 'install' else '升级'} OpenClaw ({result.get('method') or 'auto'})",
            meta={
                "method": result.get("method", ""),
                "version": ((result.get("installation") or {}).get("version") or {}).get("release", ""),
                "syncRuntime": bool(payload.get("syncRuntime", True)),
                "buildFrontend": bool(payload.get("buildFrontend", True)),
            },
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"OpenClaw {'安装' if action_name == 'install' else '升级'}已执行。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/runtime-sync":
        if not handler._require_capability("adminWrite", "只有 Owner 可以同步OpenClaw Team 运行时。"):
            return True
        result = svc.perform_runtime_sync_action(handler.server.openclaw_dir, build_frontend=bool(payload.get("buildFrontend", True)))
        handler._audit("openclaw_runtime_sync", detail="同步OpenClaw Team 运行时资产", meta={"buildFrontend": bool(payload.get("buildFrontend", True))})
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "OpenClaw Team 运行时已同步。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/provider/configure":
        if not handler._require_capability("adminWrite", "只有 Owner 可以配置模型供应商。"):
            return True
        provider_id = str(payload.get("providerId", "")).strip()
        result = svc.perform_openclaw_provider_setup(
            handler.server.openclaw_dir,
            provider_id=provider_id,
            api_key=str(payload.get("apiKey", "")).strip(),
            sync_auth=bool(payload.get("syncAuth", True)),
            rollout_model=str(payload.get("rolloutModel", "")).strip(),
            rollout_scope=str(payload.get("rolloutScope", "none")).strip(),
            rollout_agent_id=str(payload.get("rolloutAgentId", "")).strip(),
            set_preferred_provider=bool(payload.get("setPreferredProvider", True)),
        )
        handler._audit(
            "openclaw_provider_configure",
            detail=f"配置模型供应商 {result.get('providerLabel') or provider_id}",
            meta={"providerId": provider_id, "syncAuth": bool(payload.get("syncAuth", True))},
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        rollout = result.get("rollout") if isinstance(result.get("rollout"), dict) else {}
        rollout_count = int(rollout.get("updatedAgents") or 0)
        if rollout_count > 0:
            message = f"{result.get('providerLabel') or provider_id} 已配置完成，并同步到 {rollout_count} 个 Agent。"
        else:
            message = f"{result.get('providerLabel') or provider_id} 已配置完成。"
        handler._send_json({"ok": True, "message": message, "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/execution/configure":
        if not handler._require_capability("adminWrite", "只有 Owner 可以配置执行架构。"):
            return True
        result = svc.perform_openclaw_execution_configure(
            handler.server.openclaw_dir,
            primary_path=str(payload.get("primaryPath", "")).strip(),
            fallback_path=str(payload.get("fallbackPath", "")).strip(),
            context_mode=str(payload.get("contextMode", "")).strip(),
            local_runtime_role=str(payload.get("localRuntimeRole", "")).strip(),
            preferred_provider_id=str(payload.get("preferredProviderId", "")).strip(),
            hosted_provider_context_budget_policy=str(payload.get("hostedProviderContextBudgetPolicy", "")).strip(),
            transport=str(payload.get("transport", "")).strip(),
        )
        handler._audit(
            "openclaw_execution_configure",
            detail=f"配置执行架构 {result.get('primaryPath') or 'provider_api'}",
            meta={
                "primaryPath": result.get("primaryPath", ""),
                "fallbackPath": result.get("fallbackPath", ""),
                "preferredProviderId": str((result.get("preferences") or {}).get("preferredProviderId") or ""),
            },
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "执行架构已保存。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/speech-runtime/configure":
        if not handler._require_capability("adminWrite", "只有 Owner 可以配置语音运行时。"):
            return True
        result = svc.perform_openclaw_speech_runtime_configure(
            handler.server.openclaw_dir,
            provider=str(payload.get("provider", "")).strip(),
            base_url=str(payload.get("baseUrl", "")).strip(),
            model=str(payload.get("model", "")).strip(),
            api_key_env=str(payload.get("apiKeyEnv", "")).strip(),
        )
        speech_runtime = result.get("speechRuntime") if isinstance(result.get("speechRuntime"), dict) else {}
        handler._audit(
            "openclaw_speech_runtime_configure",
            detail=f"配置语音运行时 {speech_runtime.get('provider') or 'openai'}",
            meta={
                "provider": speech_runtime.get("provider", ""),
                "model": speech_runtime.get("model", ""),
                "apiKeyEnv": speech_runtime.get("apiKeyEnv", ""),
            },
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "语音运行时已保存。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/speech-runtime/use-local-preset":
        if not handler._require_capability("adminWrite", "只有 Owner 可以配置语音运行时。"):
            return True
        result = svc.perform_openclaw_speech_runtime_use_local_preset(handler.server.openclaw_dir)
        speech_runtime = result.get("speechRuntime") if isinstance(result.get("speechRuntime"), dict) else {}
        handler._audit(
            "openclaw_speech_runtime_local_preset",
            detail=f"切换语音运行时到本地 {speech_runtime.get('provider') or 'sherpa_onnx'}",
            meta={
                "provider": speech_runtime.get("provider", ""),
                "model": speech_runtime.get("model", ""),
                "baseUrl": speech_runtime.get("baseUrl", ""),
            },
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "已切换到本地语音预设。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/auth/sync":
        if not handler._require_capability("adminWrite", "只有 Owner 可以同步团队鉴权。"):
            return True
        result = svc.perform_openclaw_auth_sync(handler.server.openclaw_dir, overwrite=bool(payload.get("overwrite", True)))
        handler._audit("openclaw_auth_sync", detail="同步团队鉴权", meta={"overwrite": bool(payload.get("overwrite", True))})
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "团队鉴权已同步。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/local-runtime/configure":
        if not handler._require_capability("adminWrite", "只有 Owner 可以配置本地模型运行时。"):
            return True
        result = svc.perform_local_model_runtime_configure(
            handler.server.openclaw_dir,
            mode=str(payload.get("mode", "")).strip(),
            backend=str(payload.get("backend", "")).strip(),
            base_url=str(payload.get("baseUrl", "")).strip(),
            entrypoint=str(payload.get("entrypoint", "")).strip(),
            model_path=str(payload.get("modelPath", "")).strip(),
            host=str(payload.get("host", "")).strip(),
            port=payload.get("port"),
            context_length=payload.get("contextLength"),
            gpu_layers=payload.get("gpuLayers"),
            kv_cache_enabled=payload.get("kvCacheEnabled"),
            cache_type_k=str(payload.get("cacheTypeK", "")).strip(),
            cache_type_v=str(payload.get("cacheTypeV", "")).strip(),
            extra_args=payload.get("extraArgs", ""),
            enabled=payload.get("enabled"),
        )
        handler._audit(
            "openclaw_local_runtime_configure",
            detail=f"配置本地运行时 {result.get('backend') or 'llama_cpp'}",
            meta={"backend": result.get("backend", ""), "kvCache": bool((result.get("kvCache") or {}).get("enabled"))},
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "本地运行时配置已保存。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/local-runtime/use-recommended-profile" or path == "/api/actions/openclaw/local-runtime/recommend":
        if not handler._require_capability("adminWrite", "只有 Owner 可以配置本地模型运行时。"):
            return True
        profile_id = str(payload.get("profileId", "")).strip()
        result = svc.perform_local_model_runtime_use_recommended_profile(
            handler.server.openclaw_dir,
            profile_id=profile_id,
        )
        recommended_profile = result.get("recommendedProfile") if isinstance(result.get("recommendedProfile"), dict) else {}
        handler._audit(
            "openclaw_local_runtime_use_recommended_profile",
            detail=f"应用本地运行时预设 {recommended_profile.get('label') or profile_id}",
            meta={"profileId": profile_id, "providerId": recommended_profile.get("providerId", "")},
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "本地运行时预设已应用。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/local-runtime/inspect-model-dir" or path == "/api/actions/openclaw/local-runtime/inspect":
        if not handler._require_capability("adminWrite", "只有 Owner 可以检查本地模型目录。"):
            return True
        profile_id = str(payload.get("profileId", "")).strip()
        result = svc.inspect_local_runtime_model_dir(
            handler.server.openclaw_dir,
            profile_id=profile_id,
        )
        handler._audit(
            "openclaw_local_runtime_inspect_model_dir",
            detail=f"检查本地模型目录 {result.get('profileLabel') or profile_id}",
            meta={"profileId": profile_id, "modelsDir": result.get("modelsDir", ""), "readyToStart": bool(result.get("readyToStart"))},
        )
        handler._send_json({"ok": True, "message": "本地模型目录检查完成。", "result": result})
        return True

    if path == "/api/actions/openclaw/local-runtime/start":
        if not handler._require_capability("adminWrite", "只有 Owner 可以启动本地模型运行时。"):
            return True
        result = svc.perform_local_model_runtime_start(
            handler.server.openclaw_dir,
            mode=str(payload.get("mode", "")).strip(),
            backend=str(payload.get("backend", "")).strip(),
            base_url=str(payload.get("baseUrl", "")).strip(),
            entrypoint=str(payload.get("entrypoint", "")).strip(),
            model_path=str(payload.get("modelPath", "")).strip(),
            host=str(payload.get("host", "")).strip(),
            port=payload.get("port"),
            context_length=payload.get("contextLength"),
            gpu_layers=payload.get("gpuLayers"),
            kv_cache_enabled=payload.get("kvCacheEnabled"),
            cache_type_k=str(payload.get("cacheTypeK", "")).strip(),
            cache_type_v=str(payload.get("cacheTypeV", "")).strip(),
            extra_args=payload.get("extraArgs", ""),
            enabled=payload.get("enabled"),
        )
        handler._audit(
            "openclaw_local_runtime_start",
            detail=f"启动本地运行时 {result.get('backend') or 'llama_cpp'}",
            meta={"pid": result.get("pid", ""), "kvCache": bool((result.get("kvCache") or {}).get("enabled"))},
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "本地运行时已启动。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/local-runtime/stop":
        if not handler._require_capability("adminWrite", "只有 Owner 可以停止本地模型运行时。"):
            return True
        result = svc.perform_local_model_runtime_stop(handler.server.openclaw_dir)
        handler._audit(
            "openclaw_local_runtime_stop",
            detail="停止本地运行时",
            meta={"previousPid": result.get("previousPid", ""), "forced": bool(result.get("forced"))},
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "本地运行时已停止。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/browser/start":
        if not handler._require_capability("adminWrite", "只有 Owner 可以启动浏览器运行时。"):
            return True
        profile = str(payload.get("profile", "")).strip()
        result = svc.perform_browser_start(handler.server.openclaw_dir, profile=profile)
        handler._audit("openclaw_browser_start", detail="启动 Browser 运行时", meta={"profile": profile})
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Browser 启动命令已执行。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/runtime/browser/refresh":
        if not handler._require_capability("adminWrite", "只有 Owner 可以执行 Browser 实时探测。"):
            return True
        probe_payload = svc.perform_openclaw_browser_probe_refresh(handler.server.openclaw_dir)
        handler._audit("openclaw_runtime_browser_refresh", detail="执行 Browser 实时探测")
        handler._send_json(
            {
                "ok": True,
                "message": "Browser 实时探测已完成。",
                "result": {"section": "browser", "refreshedAt": ((probe_payload.get("browser") or {}).get("probeUpdatedAt") or "")},
                "openclaw": probe_payload,
            }
        )
        return True

    if path == "/api/actions/openclaw/browser/profile/create":
        if not handler._require_capability("adminWrite", "只有 Owner 可以创建浏览器 profile。"):
            return True
        result = svc.perform_browser_create_profile(
            handler.server.openclaw_dir,
            name=str(payload.get("name", "")).strip(),
            driver=str(payload.get("driver", "")).strip() or "openclaw",
            color=str(payload.get("color", "")).strip(),
            cdp_url=str(payload.get("cdpUrl", "")).strip(),
        )
        handler._audit("openclaw_browser_profile_create", detail=f"创建 Browser Profile {result['name']}", meta={"profile": result["name"]})
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"Browser profile {result['name']} 已创建。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/browser/open":
        if not handler._require_capability("adminWrite", "只有 Owner 可以控制本地浏览器工作台。"):
            return True
        profile = str(payload.get("profile", "")).strip()
        result = svc.perform_browser_open(handler.server.openclaw_dir, url=str(payload.get("url", "")).strip(), profile=profile)
        handler._audit("openclaw_browser_open", detail="在 Browser 中打开页面", meta={"profile": profile, "url": result["url"]})
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"已在 Browser 中打开 {result['url']}。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/browser/snapshot":
        if not handler._require_capability("adminWrite", "只有 Owner 可以抓取浏览器快照。"):
            return True
        result = svc.perform_browser_snapshot(
            handler.server.openclaw_dir,
            profile=str(payload.get("profile", "")).strip(),
            selector=str(payload.get("selector", "")).strip(),
            target_id=str(payload.get("targetId", "")).strip(),
            limit=int(payload.get("limit", 120) or 120),
        )
        handler._audit("openclaw_browser_snapshot", detail="抓取 Browser Snapshot")
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "Browser snapshot 已返回。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/browser/plan":
        if not handler._require_capability("adminWrite", "只有 Owner 可以执行浏览器动作计划。"):
            return True
        result = svc.perform_browser_plan(
            handler.server.openclaw_dir,
            steps=payload.get("steps", []),
            profile=str(payload.get("profile", "")).strip(),
        )
        handler._audit("openclaw_browser_plan", detail="执行 Browser 动作计划", meta={"steps": len(result.get("results", []))})
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"Browser 动作计划已执行 {len(result.get('results', []))} 步。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw"])})
        return True

    if path == "/api/actions/openclaw/models/apply":
        if not handler._require_capability("adminWrite", "只有 Owner 可以调整 Agent 模型。"):
            return True
        result = svc.perform_model_rollout(
            handler.server.openclaw_dir,
            model_name=str(payload.get("model", "")).strip(),
            scope=str(payload.get("scope", "all")).strip(),
            agent_id=str(payload.get("agentId", "")).strip(),
        )
        handler._audit(
            "openclaw_model_apply",
            detail=f"将 {max(result.get('updatedAgents', 0), len(result.get('matchedAgents', [])))} 个 Agent 切到模型 {result['model']}",
            meta={"scope": result.get("scope", ""), "model": result.get("model", ""), "updatedAgents": result.get("updatedAgents", 0)},
        )
        svc.invalidate_openclaw_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        data, _paths = handler._refreshed_bundle()
        updated_agents = result.get("updatedAgents", 0)
        message = f"已将 {updated_agents} 个 Agent 切到 {result['model']}。" if updated_agents else f"当前目标 Agent 已经在使用 {result['model']}。"
        handler._send_json({"ok": True, "message": message, "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["openclaw", "platform"])})
        return True

    if path == "/api/actions/admin/user/create":
        if not handler._require_capability("adminWrite", "只有 Owner 可以管理团队席位。"):
            return True
        username = str(payload.get("username", "")).strip()
        display_name = str(payload.get("displayName", "")).strip()
        role = str(payload.get("role", "")).strip()
        password = str(payload.get("password", "")).strip()
        user = svc.create_product_user(handler.server.openclaw_dir, username, display_name, role, password)
        handler._audit("user_create", detail=f"创建团队席位 {user['displayName']}", meta={"username": user["username"], "role": user["role"]})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"团队账号 {user['displayName']} 已创建。", "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/admin/user/update_access":
        if not handler._require_capability("adminWrite", "只有 Owner 可以调整团队席位。"):
            return True
        username = str(payload.get("username", "")).strip()
        role = str(payload.get("role", "")).strip()
        status = str(payload.get("status", "")).strip()
        user = svc.update_product_user_access(handler.server.openclaw_dir, username, role, status)
        handler._audit("user_access_update", detail=f"更新团队席位 {user['displayName']}", meta={"username": user["username"], "role": user["role"], "status": user["status"]})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"团队席位 {user['displayName']} 已更新为 {user['roleLabel']} / {user['status']}。", "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/admin/user/reset_password":
        if not handler._require_capability("adminWrite", "只有 Owner 可以重置团队账号密码。"):
            return True
        username = str(payload.get("username", "")).strip()
        password = str(payload.get("password", "")).strip()
        user = svc.reset_product_user_password(handler.server.openclaw_dir, username, password)
        handler._audit("user_password_reset", detail=f"重置团队账号 {user['displayName']} 的密码", meta={"username": user["username"]})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"团队账号 {user['displayName']} 的密码已经重置。", "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/admin/instance/register":
        if not handler._require_capability("adminWrite", "只有 Owner 可以登记和维护安装实例。"):
            return True
        target_dir = str(payload.get("openclawDir", "")).strip()
        label = str(payload.get("label", "")).strip()
        installation = svc.register_installation(handler.server.openclaw_dir, target_dir, label=label)
        handler._audit("installation_register", detail=f"登记安装实例 {installation['label']}", meta={"openclawDir": installation["openclawDir"], "theme": installation.get("theme", "")})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"安装实例 {installation['label']} 已登记进控制平面。", "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/admin/instance/remove":
        if not handler._require_capability("adminWrite", "只有 Owner 可以登记和维护安装实例。"):
            return True
        target_dir = str(payload.get("openclawDir", "")).strip()
        removed_dir = svc.remove_installation(handler.server.openclaw_dir, target_dir)
        handler._audit("installation_remove", detail=f"移除安装实例 {removed_dir}", meta={"openclawDir": removed_dir})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": "安装实例已从控制平面移除。", "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/admin/tenant/save":
        if not handler._require_capability("adminWrite", "只有 Owner 可以管理租户。"):
            return True
        name = str(payload.get("name", "")).strip()
        if not name:
            raise RuntimeError("请先填写租户名称。")
        tenant = svc.store_save_tenant(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": name,
                "slug": str(payload.get("slug", "")).strip(),
                "status": str(payload.get("status", "")).strip() or "active",
                "primaryOpenclawDir": str(payload.get("primaryOpenclawDir", "")).strip(),
                "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
            },
        )
        handler._audit("tenant_save", detail=f"保存租户 {tenant['name']}", meta={"tenantId": tenant["id"], "slug": tenant.get("slug", "")})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"租户 {tenant['name']} 已保存。", "tenant": tenant, "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/admin/tenant/installation/save":
        if not handler._require_capability("adminWrite", "只有 Owner 可以绑定租户安装。"):
            return True
        tenant_id = str(payload.get("tenantId", "")).strip()
        target_dir = str(payload.get("openclawDir", "")).strip()
        if not tenant_id or not target_dir:
            raise RuntimeError("请先选择租户并填写 OpenClaw 目录。")
        installation = svc.register_installation(handler.server.openclaw_dir, target_dir, label=str(payload.get("label", "")).strip())
        binding = svc.store_save_tenant_installation(
            handler.server.openclaw_dir,
            {
                "tenantId": tenant_id,
                "openclawDir": installation["openclawDir"],
                "label": str(payload.get("bindingLabel", "")).strip() or installation["label"],
                "role": str(payload.get("role", "")).strip() or "primary",
            },
        )
        tenant = svc.find_tenant_record(handler.server.openclaw_dir, tenant_id)
        if tenant and (binding.get("role") == "primary" or not tenant.get("primaryOpenclawDir")):
            tenant = svc.store_save_tenant(handler.server.openclaw_dir, {**tenant, "primaryOpenclawDir": installation["openclawDir"]})
        handler._audit(
            "tenant_installation_save",
            detail=f"绑定租户安装 {binding['label']}",
            meta={"tenantId": tenant_id, "openclawDir": binding["openclawDir"], "role": binding.get("role", "")},
        )
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"租户安装 {binding['label']} 已绑定。", "tenant": tenant, "binding": binding, "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/admin/tenant/api-key/create":
        if not handler._require_capability("adminWrite", "只有 Owner 可以创建租户 API Key。"):
            return True
        tenant_id = str(payload.get("tenantId", "")).strip()
        name = str(payload.get("name", "")).strip()
        scopes = payload.get("scopes") if isinstance(payload.get("scopes"), list) else []
        if not tenant_id or not name:
            raise RuntimeError("请先选择租户并填写 API Key 名称。")
        result = svc.store_create_tenant_api_key(handler.server.openclaw_dir, tenant_id, name, scopes=scopes or None)
        handler._audit("tenant_api_key_create", detail=f"创建租户 API Key {name}", meta={"tenantId": tenant_id, "keyId": (result.get('key') or {}).get('id', '')})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"租户 API Key {name} 已生成，请立即妥善保存。", "apiKey": result, "dashboard": handler._action_dashboard_payload(data, sections=["platform"])})
        return True

    if path == "/api/actions/skills/scaffold":
        if not handler._require_capability("adminWrite", "只有 Owner 可以创建和维护技能目录。"):
            return True
        slug = str(payload.get("slug", "")).strip()
        title = str(payload.get("title", "")).strip()
        description = str(payload.get("description", "")).strip()
        trigger_phrase = str(payload.get("triggerPhrase", "")).strip()
        category = str(payload.get("category", "")).strip() or "workflow-automation"
        mcp_server = str(payload.get("mcpServer", "")).strip()
        skill = svc.perform_skill_scaffold(
            handler.server.openclaw_dir,
            slug=slug,
            title=title,
            description=description,
            trigger_phrase=trigger_phrase,
            category=category,
            include_scripts=bool(payload.get("includeScripts")),
            include_references=payload.get("includeReferences", True) is not False,
            include_assets=bool(payload.get("includeAssets")),
            mcp_server=mcp_server,
        )
        handler._audit("skill_scaffold", detail=f"创建技能 {skill.get('slug', slug)}", meta={"skill": skill.get("slug", slug), "category": category})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"技能 {skill.get('displayName', slug)} 已创建，并完成首次校验。", "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"])})
        return True

    if path == "/api/actions/skills/role-profile/save":
        if not handler._require_capability("adminWrite", "只有 Owner 可以维护技能角色元数据。"):
            return True
        slug = str(payload.get("skillSlug", "")).strip() or str(payload.get("slug", "")).strip()
        if not slug:
            raise RuntimeError("请先选择要保存的技能。")
        profile = svc.store_save_skill_role_profile(
            handler.server.openclaw_dir,
            {
                "skillSlug": slug,
                "mode": str(payload.get("mode", "")).strip(),
                "stage": str(payload.get("stage", "")).strip(),
                "recommendedEntry": str(payload.get("recommendedEntry", "")).strip() or "skills",
                "outputContract": payload.get("outputContract") if isinstance(payload.get("outputContract"), list) else [],
                "requiresRuntime": payload.get("requiresRuntime") if isinstance(payload.get("requiresRuntime"), list) else [],
                "handoffArtifacts": payload.get("handoffArtifacts") if isinstance(payload.get("handoffArtifacts"), list) else [],
                "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
            },
        )
        handler._audit("skill_role_profile_save", detail=f"保存技能 {slug} 的角色元数据", meta={"skillSlug": slug, "mode": profile.get("mode", ""), "stage": profile.get("stage", "")})
        svc.invalidate_skills_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        hydrated_skill = next((item for item in svc.safe_list((data.get("skills", {}) or {}).get("skills")) if item.get("slug") == slug), profile)
        handler._send_json({"ok": True, "message": f"技能 {hydrated_skill.get('displayName', slug)} 的角色元数据已保存。", "profile": profile, "skill": hydrated_skill, "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"])})
        return True

    if path == "/api/actions/skills/gstack/sync":
        if not handler._require_capability("adminWrite", "只有 Owner 可以同步 gstack 技能集。"):
            return True
        result = svc.sync_gstack_skill_library(handler.server.openclaw_dir)
        handler._audit("skills_gstack_sync", detail="同步 gstack 技能集到OpenClaw Team", meta={"createdSkillCount": result.get("createdSkillCount", 0), "savedPackCount": result.get("savedPackCount", 0), "projectDir": result.get("projectDir", "")})
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"gstack 技能集已同步，新增 {result.get('createdSkillCount', 0)} 个本地技能脚手架。", "result": result, "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"])})
        return True

    if path == "/api/actions/skills/pack/save":
        if not handler._require_capability("adminWrite", "只有 Owner 可以维护 workflow packs。"):
            return True
        name = str(payload.get("name", "")).strip()
        if not name:
            raise RuntimeError("请先填写 workflow pack 名称。")
        skills_payload = svc.load_skills_catalog(handler.server.openclaw_dir)
        pack = svc.store_save_workflow_pack(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": name,
                "description": str(payload.get("description", "")).strip(),
                "status": str(payload.get("status", "")).strip() or "draft",
                "mode": str(payload.get("mode", "")).strip(),
                "starter": bool(payload.get("starter")),
                "defaultEntry": str(payload.get("defaultEntry", "")).strip() or "skills",
                "recommendedTeamId": str(payload.get("recommendedTeamId", "")).strip() or svc.recommended_team_id_for_pack_mode(str(payload.get("mode", "")).strip()),
                "stages": svc.normalize_workflow_pack_stages(payload.get("stages"), fallback_name=name, fallback_mode=str(payload.get("mode", "")).strip()),
                "skills": payload.get("skills") if isinstance(payload.get("skills"), list) else [],
                "meta": svc.workflow_pack_meta_from_payload(payload, skills_data=skills_payload),
            },
        )
        handler._audit("skill_pack_save", detail=f"保存 workflow pack {pack.get('name', name)}", meta={"packId": pack.get("id", ""), "mode": pack.get("mode", "")})
        svc.invalidate_skills_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        full_pack = svc.resolve_workflow_pack_or_mode_record(
            handler.server.openclaw_dir,
            str(pack.get("id", "")).strip(),
            mode=str(pack.get("mode", "")).strip(),
            target="run",
            skills_data=svc.load_skills_catalog(handler.server.openclaw_dir),
        )
        handler._send_json({"ok": True, "message": f"workflow pack {pack.get('name', name)} 已保存。", "pack": full_pack, "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"])})
        return True

    if path == "/api/actions/skills/pack/launch":
        target = svc.resolve_pack_launch_target(str(payload.get("target", "")).strip(), payload.get("defaultEntry"))
        if target == "chat":
            if not handler._require_capability("conversationWrite", "当前账号没有按 workflow pack 发起聊天线程的权限。"):
                return True
        else:
            if not handler._require_capability("taskWrite", "当前账号没有按 workflow pack 发起工作流的权限。"):
                return True
        skills_payload = svc.load_skills_catalog(handler.server.openclaw_dir)
        pack = svc.resolve_workflow_pack_or_mode_record(
            handler.server.openclaw_dir,
            str(payload.get("packId", "")).strip(),
            mode=str(payload.get("mode", "")).strip(),
            target=target,
            skills_data=skills_payload,
        )
        actor = handler._current_actor()
        result_payload = {}
        if target == "chat":
            thread = svc.perform_workflow_pack_launch_to_chat(handler.server.openclaw_dir, pack, payload, actor)
            result_payload["threadId"] = thread.get("id", "")
        elif target == "studio":
            workflow = svc.perform_workflow_pack_launch_to_studio(handler.server.openclaw_dir, pack, payload)
            result_payload["workflowId"] = workflow.get("id", "")
        else:
            run = svc.perform_workflow_pack_launch_to_run(handler.server.openclaw_dir, pack, payload, actor)
            result_payload["runId"] = run.get("id", "")
        handler._audit("skill_pack_launch", detail=f"从 workflow pack {pack.get('name', pack.get('id', ''))} 发起 {target}", meta={"packId": pack.get("id", ""), "target": target, **result_payload})
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        response = {
            "ok": True,
            "message": f"{pack.get('name', pack.get('id', 'workflow pack'))} 已发起到 {target}。",
            "target": target,
            "pack": next((item for item in svc.safe_list((data.get("skills", {}) or {}).get("packs")) if item.get("id") == pack.get("id")), pack),
            "dashboard": handler._action_dashboard_payload(data, sections=["skills", "agents", "agentTeams", "taskIndex", "management", "orchestration", "deliverables", "chat"]),
        }
        if target == "chat":
            response["thread"] = svc.compact_chat_thread_detail(
                svc.load_chat_thread_detail(
                    handler.server.openclaw_dir,
                    result_payload.get("threadId", ""),
                    agents=data.get("agents", []),
                    tasks=data.get("taskIndex", []),
                    deliverables=data.get("deliverables", []),
                    management_runs=(data.get("management", {}) or {}).get("runs", []),
                    agent_teams=svc.safe_list((data.get("agentTeams", {}) or {}).get("items")),
                    skills_data=data.get("skills", {}),
                )
            )
        elif target == "studio":
            response["workflow"] = next((item for item in svc.safe_list((data.get("orchestration", {}) or {}).get("workflows")) if item.get("id") == result_payload.get("workflowId")), {})
        else:
            response["run"] = svc.build_management_run_snapshot(handler.server.openclaw_dir, result_payload.get("runId", ""))
        handler._send_json(response)
        return True

    if path == "/api/actions/skills/install":
        if not handler._require_capability("adminWrite", "只有 Owner 可以安装技能。"):
            return True
        slug = str(payload.get("slug", "")).strip()
        if not slug:
            raise RuntimeError("请先选择要安装的 skill。")
        installed = svc.perform_skill_install(handler.server.openclaw_dir, slug)
        handler._audit("skill_install", detail=f"安装技能 {slug} 到 OpenClaw", meta={"skill": slug, "targetPath": installed.get("targetPath", ""), "sourceKind": installed.get("sourceKind", "")})
        svc.invalidate_skills_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"技能 {slug} 已安装到当前 OpenClaw 运行时。", "result": installed, "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"])})
        return True

    if path == "/api/actions/skills/uninstall":
        if not handler._require_capability("adminWrite", "只有 Owner 可以卸载技能。"):
            return True
        slug = str(payload.get("slug", "")).strip()
        if not slug:
            raise RuntimeError("请先选择要卸载的 skill。")
        removed = svc.perform_skill_uninstall(handler.server.openclaw_dir, slug)
        handler._audit("skill_uninstall", detail=f"卸载技能 {slug}", meta={"skill": slug, "targetPath": removed.get("targetPath", ""), "removed": bool(removed.get("removed"))})
        svc.invalidate_skills_payload_cache(handler.server.openclaw_dir)
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        data, _paths = handler._refreshed_bundle()
        handler._send_json(
            {
                "ok": True,
                "message": (
                    f"技能 {slug} 已从当前 OpenClaw 运行时移除。"
                    if removed.get("removed")
                    else f"技能 {slug} 当前没有安装在 OpenClaw 运行时里。"
                ),
                "result": removed,
                "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"]),
            }
        )
        return True

    if path == "/api/actions/skills/package":
        if not handler._require_capability("adminWrite", "只有 Owner 可以打包和分发技能。"):
            return True
        slug = str(payload.get("slug", "")).strip()
        bundle = svc.perform_skill_package(handler.server.openclaw_dir, slug)
        handler._audit("skill_package", detail=f"打包技能 {slug}", meta={"skill": slug, "archivePath": bundle.get("archivePath", "")})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"技能 {slug} 已打包到 {bundle.get('archivePath', '')}。", "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"])})
        return True

    if path == "/api/actions/skills/publish":
        if not handler._require_capability("adminWrite", "只有 Owner 可以把技能发布到 OpenClaw。"):
            return True
        slug = str(payload.get("slug", "")).strip()
        if not slug:
            raise RuntimeError("请先选择要发布的 skill。")
        published = svc.perform_skill_publish(handler.server.openclaw_dir, slug)
        handler._audit("skill_publish", detail=f"发布技能 {slug} 到 OpenClaw", meta={"skill": slug, "targetPath": published.get("targetPath", "")})
        data, _paths = handler._refreshed_bundle()
        handler._send_json({"ok": True, "message": f"技能 {slug} 已发布到 {published.get('targetPath', '')}。", "dashboard": handler._action_dashboard_payload(data, sections=["skills", "platform"])})
        return True

    return False
