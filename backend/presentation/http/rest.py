from __future__ import annotations

import json


def handle_rest_get(handler, path, services):
    try:
        if path == "/api/v1/tenants":
            context = handler._rest_auth_context(required_scope="tenant:read")
            if not context:
                return True
            tenant_admin = services["build_tenant_admin_data"](handler.server.openclaw_dir, services["now_utc"]())
            if context.get("mode") == "api_key":
                tenant = context.get("tenant") or services["find_tenant_record"](
                    handler.server.openclaw_dir,
                    context["apiKey"].get("tenantId", ""),
                )
                items = [item for item in tenant_admin["items"] if item.get("id") == (tenant or {}).get("id")]
            else:
                items = tenant_admin["items"]
            handler._send_json({"ok": True, "tenants": items, "summary": tenant_admin["summary"]})
            return True

        parts = [segment for segment in path.split("/") if segment]
        if len(parts) < 5 or parts[:3] != ["api", "v1", "tenants"]:
            return False
        tenant_ref = parts[3]
        resource = parts[4]
        scope_map = {
            "catalog": "tenant:read",
            "dashboard": "dashboard:read",
            "tasks": "tasks:read",
            "agents": "agents:read",
            "management": "tenant:read",
            "runs": "tasks:read",
            "deliverables": "tasks:read",
            "conversations": "tenant:read",
            "communications": "tenant:read",
            "context": "tenant:read",
        }
        context = handler._rest_auth_context(required_scope=scope_map.get(resource, "tenant:read"), tenant_ref=tenant_ref)
        if not context:
            return True
        tenant = context.get("tenant")
        tenant_dir = handler._tenant_openclaw_dir(tenant)
        config = services["load_config"](tenant_dir)
        now = services["now_utc"]()
        task_snapshot = None

        def task_index_snapshot():
            nonlocal task_snapshot
            if task_snapshot is None:
                task_snapshot = services["cached_payload"](
                    ("tenant-rest-task-index", str(tenant_dir)),
                    10.0,
                    lambda: services["build_orchestration_task_index_snapshot"](tenant_dir, config=config, now=now),
                )
            return task_snapshot

        def compact_dashboard():
            payload = {
                "generatedAt": now.isoformat().replace("+00:00", "Z"),
                "generatedAgo": "刚刚",
                "routerAgentId": str(services["get_router_agent_id"](config) or "").strip(),
                "runtime": {},
            }
            payload["signature"] = str(services["dashboard_signature"](payload) or "").strip()
            return payload
        tenant_payload = {
            "id": tenant.get("id"),
            "name": tenant.get("name"),
            "slug": tenant.get("slug"),
            "status": tenant.get("status"),
            "primaryOpenclawDir": str(tenant_dir),
        }
        if resource == "catalog":
            handler._send_json(
                {"ok": True, "tenant": tenant_payload, "catalog": services["tenant_rest_catalog_payload"](tenant, tenant_dir)}
            )
            return True
        if resource == "dashboard":
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "dashboard": compact_dashboard(),
                }
            )
            return True
        if resource == "tasks":
            handler._send_json({"ok": True, "tenant": tenant_payload, "tasks": task_index_snapshot().get("taskIndex", [])})
            return True
        if resource == "agents":
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "agents": services["build_agent_cards_snapshot"](
                        tenant_dir,
                        config=config,
                        now=now,
                        task_snapshot=task_index_snapshot(),
                    ),
                }
            )
            return True
        if resource == "management":
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "management": services["build_management_summary_snapshot"](tenant_dir),
                }
            )
            return True
        if resource == "runs":
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "runs": (services["build_management_runs_snapshot"](tenant_dir) or {}).get("runs", []),
                }
            )
            return True
        if resource == "deliverables":
            if len(parts) >= 7 and parts[6] == "download":
                deliverable_id = parts[5]
                deliverables = services["build_deliverables_snapshot"](
                    tenant_dir,
                    config=config,
                    now=now,
                    task_snapshot=task_index_snapshot(),
                )
                archive = services["build_deliverable_zip_bytes"](tenant_dir, {"deliverables": deliverables}, deliverable_id)
                handler._audit(
                    "tenant_deliverable_download",
                    detail=f"通过开放 API 下载租户 {tenant.get('name', tenant.get('id', ''))} 的交付产物 {deliverable_id}",
                    meta={"tenantId": tenant.get("id", ""), "deliverableId": deliverable_id},
                )
                handler._send_bytes(
                    archive["body"],
                    "application/zip",
                    extra_headers=[("Content-Disposition", f'attachment; filename="{archive["filename"]}"')],
                )
                return True
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "deliverables": services["build_deliverables_snapshot"](
                        tenant_dir,
                        config=config,
                        now=now,
                        task_snapshot=task_index_snapshot(),
                    ),
                }
            )
            return True
        if resource == "conversations":
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "conversations": services["build_conversations_catalog_snapshot"](tenant_dir),
                }
            )
            return True
        if resource == "communications":
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "communications": services["build_communications_summary_snapshot"](
                        tenant_dir,
                        config=config,
                        now=now,
                    ),
                }
            )
            return True
        if resource == "context":
            handler._send_json(
                {
                    "ok": True,
                    "tenant": tenant_payload,
                    "contextHub": services["load_context_hub_data"](
                        tenant_dir,
                        router_agent_id=services["get_router_agent_id"](config),
                    ),
                }
            )
            return True
        handler._send_json({"ok": False, "error": "not_found", "message": "未知 REST 资源。"}, status=404)
        return True
    except RuntimeError as error:
        handler._send_json({"ok": False, "error": "rest_failed", "message": str(error)}, status=400)
        return True


def handle_rest_post(handler, path, services):
    try:
        parts = [segment for segment in path.split("/") if segment]
        if len(parts) < 5 or parts[:3] != ["api", "v1", "tenants"]:
            return False
        tenant_ref = parts[3]
        resource = parts[4]
        try:
            payload = handler._read_json_body()
        except json.JSONDecodeError:
            handler._send_json({"ok": False, "error": "invalid_json", "message": "请求体不是合法 JSON。"}, status=400)
            return True
        context = handler._rest_auth_context(required_scope="tasks:write", tenant_ref=tenant_ref)
        if not context:
            return True
        tenant = context.get("tenant")
        tenant_dir = handler._tenant_openclaw_dir(tenant)
        if resource == "tasks":
            title = str(payload.get("title", "")).strip()
            remark = str(payload.get("remark", "")).strip()
            prefer_fast_routing = bool(payload.get("preferFastRouting"))
            if not title:
                handler._send_json({"ok": False, "error": "missing_title", "message": "任务标题不能为空。"}, status=400)
                return True
            task_result = services["perform_task_create"](
                tenant_dir,
                title,
                remark=remark,
                run_owner=str(tenant.get("name") or tenant.get("id") or "Tenant API").strip(),
                prefer_fast_routing=prefer_fast_routing,
            )
            task_id = task_result["taskId"]
            handler._audit(
                "tenant_task_create",
                detail=f"通过开放 API 为租户 {tenant.get('name', tenant.get('id', ''))} 创建任务 {task_id}",
                meta={"tenantId": tenant.get("id", ""), "taskId": task_id, "title": title},
            )
            handler._send_json(
                {
                    "ok": True,
                    "tenant": {"id": tenant.get("id"), "name": tenant.get("name"), "slug": tenant.get("slug")},
                    "taskId": task_id,
                    "routingDecision": task_result.get("routingDecision", {}),
                    "workflowBinding": task_result.get("workflowBinding", {}),
                    "planningBundle": task_result.get("planningBundle", {}),
                    "execution": task_result.get("execution", {}),
                    "team": task_result.get("team"),
                    "teamSelection": task_result.get("teamSelection", {}),
                    "run": task_result.get("run"),
                    "deduped": bool(task_result.get("deduped")),
                    "duplicateOfTaskId": str(task_result.get("duplicateOfTaskId") or "").strip(),
                    "message": (
                        f"检测到相同任务仍在协同中，已复用现有任务 {task_id}。"
                        if task_result.get("deduped")
                        else f"任务 {task_id} 已进入租户协同链路。"
                    ),
                }
            )
            return True
        handler._send_json({"ok": False, "error": "not_found", "message": "未知 REST 写入资源。"}, status=404)
        return True
    except RuntimeError as error:
        handler._send_json({"ok": False, "error": "rest_failed", "message": str(error)}, status=400)
        return True
