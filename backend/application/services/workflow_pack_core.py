from __future__ import annotations

import secrets
import sys
from collections import defaultdict


class _DelegatedSymbol:
    def __init__(self, name):
        self._name = name

    def _resolve(self):
        return getattr(_svc(), self._name)

    def __call__(self, *args, **kwargs):
        return self._resolve()(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._resolve(), attr)

    def __iter__(self):
        return iter(self._resolve())

    def __bool__(self):
        return bool(self._resolve())

    def __len__(self):
        return len(self._resolve())

    def __contains__(self, item):
        return item in self._resolve()

    def __getitem__(self, key):
        return self._resolve()[key]

    def __eq__(self, other):
        return self._resolve() == other

    def __hash__(self):
        return hash(self._resolve())

    def __repr__(self):
        return repr(self._resolve())

    def __str__(self):
        return str(self._resolve())

    def __int__(self):
        return int(self._resolve())

    def __float__(self):
        return float(self._resolve())

    def __index__(self):
        return int(self._resolve())

    def __lt__(self, other):
        return self._resolve() < other

    def __le__(self, other):
        return self._resolve() <= other

    def __gt__(self, other):
        return self._resolve() > other

    def __ge__(self, other):
        return self._resolve() >= other


def _svc():
    module = sys.modules.get("backend.collaboration_dashboard")
    if module is not None:
        return module
    module = sys.modules.get("collaboration_dashboard")
    if module is not None:
        return module
    main = sys.modules.get("__main__")
    if main is not None and str(getattr(main, "__file__", "")).endswith("collaboration_dashboard.py"):
        return main
    import importlib

    try:
        return importlib.import_module("backend.collaboration_dashboard")
    except ModuleNotFoundError:
        return importlib.import_module("collaboration_dashboard")


PACK_RUNTIME_BROWSER_MODES = {"browser", "qa", "design-review"}
PACK_RELEASE_PROVIDERS = {"manual", "dry-run", "fixture", "github-pr"}
STAGE_DEFAULT_ARTIFACT_TYPES = {
    "plan": "plan-brief",
    "review": "review-findings",
    "implement": "eng-plan",
    "verify": "qa-report",
    "ship": "release-note",
    "document": "release-doc",
    "reflect": "retro-summary",
}
STAGE_GATE_SUFFIX_LABELS = {
    "plan": "approved",
    "review": "approved",
    "implement": "completed",
    "verify": "verified",
    "ship": "release-ready",
    "document": "docs-synced",
    "reflect": "captured",
}

artifact_type_from_handoff = _DelegatedSymbol("artifact_type_from_handoff")
artifact_type_label = _DelegatedSymbol("artifact_type_label")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
datetime = _DelegatedSymbol("datetime")
deepcopy = _DelegatedSymbol("deepcopy")
format_age = _DelegatedSymbol("format_age")
load_skills_catalog = _DelegatedSymbol("load_skills_catalog")
normalize_flag = _DelegatedSymbol("normalize_flag")
now_iso = _DelegatedSymbol("now_iso")
orchestration_slug = _DelegatedSymbol("orchestration_slug")
parse_iso = _DelegatedSymbol("parse_iso")
safe_list = _DelegatedSymbol("safe_list")
timezone = _DelegatedSymbol("timezone")


def workflow_pack_map_from_skills_payload(skills_data):
    return {
        str(item.get("id") or "").strip(): deepcopy(item)
        for item in safe_list((skills_data or {}).get("packs"))
        if str(item.get("id") or "").strip()
    }


def resolve_pack_launch_target(requested_target="", default_entry=""):
    normalized_requested = str(requested_target or "").strip().lower()
    if normalized_requested in {"chat", "run", "studio"}:
        return normalized_requested
    normalized_default = str(default_entry or "").strip().lower()
    if normalized_default in {"chat", "run", "studio"}:
        return normalized_default
    return "run"


def resolve_workflow_pack_record(openclaw_dir, pack_id, skills_data=None):
    normalized_pack_id = str(pack_id or "").strip()
    if not normalized_pack_id:
        raise RuntimeError("请先选择要发起的 workflow pack。")
    payload = skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir)
    pack = workflow_pack_map_from_skills_payload(payload).get(normalized_pack_id)
    if not pack:
        raise RuntimeError(f"未找到 workflow pack：{normalized_pack_id}")
    if str(pack.get("status") or "active").strip().lower() == "disabled":
        raise RuntimeError(f"workflow pack {pack.get('name') or normalized_pack_id} 已停用，暂时不能发起。")
    return pack


def resolve_workflow_pack_or_mode_record(openclaw_dir, pack_id="", mode="", target="", skills_data=None):
    normalized_pack_id = str(pack_id or "").strip()
    if normalized_pack_id:
        return resolve_workflow_pack_record(openclaw_dir, normalized_pack_id, skills_data=skills_data)
    normalized_mode = str(mode or "").strip().lower()
    if not normalized_mode:
        raise RuntimeError("请先选择 workflow pack 或岗位模式。")
    payload = skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir)
    preferred_target = resolve_pack_launch_target(target, "")
    packs = [
        item
        for item in safe_list((payload or {}).get("packs"))
        if str(item.get("mode") or "").strip().lower() == normalized_mode
        and str(item.get("status") or "active").strip().lower() != "disabled"
    ]
    if not packs:
        raise RuntimeError(f"当前没有可用的 {normalized_mode} workflow pack。")
    packs.sort(
        key=lambda item: (
            not bool(item.get("starter")),
            item.get("defaultEntry") != preferred_target,
            bool(item.get("incomplete")),
            item.get("name") or item.get("id") or "",
        )
    )
    return packs[0]


def workflow_pack_binding_payload(pack, source="skills", target=""):
    pack = pack if isinstance(pack, dict) else {}
    capabilities = workflow_pack_capabilities(pack)
    return {
        "packId": str(pack.get("id") or "").strip(),
        "packName": str(pack.get("name") or "").strip(),
        "source": str(source or "skills").strip() or "skills",
        "target": resolve_pack_launch_target(target, pack.get("defaultEntry")),
        "mode": str(pack.get("mode") or "").strip(),
        "defaultEntry": str(pack.get("defaultEntry") or "").strip(),
        "recommendedTeamId": str(pack.get("recommendedTeamId") or "").strip(),
        "stageCount": len(safe_list(pack.get("stages"))),
        "skillCount": int(pack.get("skillCount") or len(clean_unique_strings(pack.get("skills") or []))),
        "requiredRuntimes": clean_unique_strings(pack.get("requiredRuntimes") or []),
        "missingSkillSlugs": clean_unique_strings(pack.get("missingSkillSlugs") or []),
        "hydrationStatus": str(pack.get("hydrationStatus") or ("incomplete" if pack.get("incomplete") else "ready")).strip(),
        "reviewGates": capabilities.get("reviewGates", []),
        "artifactTemplates": capabilities.get("artifactTemplates", []),
        "runtimePolicy": capabilities.get("runtimePolicy", {}),
        "releasePolicy": capabilities.get("releasePolicy", {}),
        "qaPolicy": capabilities.get("qaPolicy", {}),
        "launchedAt": now_iso(),
    }


def hydrate_linked_pack(pack_binding, pack_map):
    if not isinstance(pack_binding, dict) or not pack_binding:
        return {}
    normalized_pack_id = str(pack_binding.get("packId") or pack_binding.get("id") or "").strip()
    pack = pack_map.get(normalized_pack_id, {}) if normalized_pack_id else {}
    if pack:
        capabilities = workflow_pack_capabilities(pack)
        return {
            "id": normalized_pack_id,
            "name": str(pack.get("name") or normalized_pack_id).strip(),
            "description": str(pack.get("description") or "").strip(),
            "mode": str(pack.get("mode") or "").strip(),
            "defaultEntry": str(pack.get("defaultEntry") or "").strip(),
            "recommendedTeamId": str(pack.get("recommendedTeamId") or "").strip(),
            "requiredRuntimes": clean_unique_strings(pack.get("requiredRuntimes") or []),
            "stages": safe_list(pack.get("stages")),
            "skillCount": int(pack.get("skillCount") or len(clean_unique_strings(pack.get("skills") or []))),
            "resolvedSkillCount": int(pack.get("resolvedSkillCount") or 0),
            "missingSkillSlugs": clean_unique_strings(pack.get("missingSkillSlugs") or []),
            "hydrationStatus": str(pack.get("hydrationStatus") or "").strip(),
            "reviewGates": capabilities.get("reviewGates", []),
            "artifactTemplates": capabilities.get("artifactTemplates", []),
            "runtimePolicy": capabilities.get("runtimePolicy", {}),
            "releasePolicy": capabilities.get("releasePolicy", {}),
            "qaPolicy": capabilities.get("qaPolicy", {}),
            "modeAliases": capabilities.get("modeAliases", []),
            "source": str(pack_binding.get("source") or "").strip(),
            "target": str(pack_binding.get("target") or "").strip(),
            "launchedAt": str(pack_binding.get("launchedAt") or "").strip(),
        }
    return {
        "id": normalized_pack_id,
        "name": str(pack_binding.get("packName") or normalized_pack_id or "").strip(),
        "description": str(pack_binding.get("description") or "").strip(),
        "mode": str(pack_binding.get("mode") or "").strip(),
        "defaultEntry": str(pack_binding.get("defaultEntry") or "").strip(),
        "recommendedTeamId": str(pack_binding.get("recommendedTeamId") or "").strip(),
        "requiredRuntimes": clean_unique_strings(pack_binding.get("requiredRuntimes") or []),
        "stages": [],
        "skillCount": int(pack_binding.get("skillCount") or 0),
        "resolvedSkillCount": 0,
        "missingSkillSlugs": clean_unique_strings(pack_binding.get("missingSkillSlugs") or []),
        "hydrationStatus": str(pack_binding.get("hydrationStatus") or "").strip(),
        "reviewGates": normalize_pack_review_gates(pack_binding.get("reviewGates"), {"stages": pack_binding.get("stages") or [], "mode": pack_binding.get("mode")}),
        "artifactTemplates": normalize_pack_artifact_templates(pack_binding.get("artifactTemplates"), {"stages": pack_binding.get("stages") or [], "mode": pack_binding.get("mode")}),
        "runtimePolicy": pack_binding.get("runtimePolicy") if isinstance(pack_binding.get("runtimePolicy"), dict) else {},
        "releasePolicy": pack_binding.get("releasePolicy") if isinstance(pack_binding.get("releasePolicy"), dict) else {},
        "qaPolicy": pack_binding.get("qaPolicy") if isinstance(pack_binding.get("qaPolicy"), dict) else {},
        "modeAliases": clean_unique_strings(pack_binding.get("modeAliases") or [pack_binding.get("mode")]),
        "source": str(pack_binding.get("source") or "").strip(),
        "target": str(pack_binding.get("target") or "").strip(),
        "launchedAt": str(pack_binding.get("launchedAt") or "").strip(),
    }


def normalize_workflow_pack_stages(stages, fallback_name="", fallback_mode=""):
    normalized = []
    for index, item in enumerate(stages if isinstance(stages, list) else []):
        if isinstance(item, dict):
            key = orchestration_slug(item.get("key") or item.get("title") or f"stage-{index + 1}") or f"stage-{index + 1}"
            title = str(item.get("title") or item.get("label") or key).strip() or key
            normalized.append(
                {
                    "key": key,
                    "title": title,
                    "mode": str(item.get("mode") or fallback_mode or "").strip().lower(),
                    "stage": str(item.get("stage") or "").strip().lower(),
                    "description": str(item.get("description") or item.get("note") or "").strip(),
                }
            )
            continue
        value = str(item or "").strip()
        if not value:
            continue
        normalized.append(
            {
                "key": orchestration_slug(value) or f"stage-{index + 1}",
                "title": value,
                "mode": str(fallback_mode or "").strip().lower(),
                "stage": "",
                "description": "",
            }
        )
    if normalized:
        return normalized
    fallback_title = str(fallback_name or "Workflow Pack").strip() or "Workflow Pack"
    return [
        {
            "key": orchestration_slug(fallback_title) or "stage-1",
            "title": fallback_title,
            "mode": str(fallback_mode or "").strip().lower(),
            "stage": "",
            "description": "",
        }
    ]


def stage_skill_refs_for_pack(pack):
    pack = pack if isinstance(pack, dict) else {}
    stages = normalize_workflow_pack_stages(pack.get("stages"), fallback_name=pack.get("name"), fallback_mode=pack.get("mode"))
    remaining = {}
    for item in safe_list(pack.get("skillRefs")):
        if not isinstance(item, dict):
            continue
        slug = str(item.get("slug") or "").strip()
        if slug:
            remaining[slug] = deepcopy(item)
    groups = []
    for stage in stages:
        matched = []
        stage_mode = str(stage.get("mode") or "").strip().lower()
        stage_key = str(stage.get("stage") or "").strip().lower()
        for slug, ref in list(remaining.items()):
            ref_mode = str(ref.get("mode") or "").strip().lower()
            ref_stage = str(ref.get("stage") or "").strip().lower()
            if (stage_key and ref_stage == stage_key) or (stage_mode and ref_mode == stage_mode):
                matched.append(ref)
                remaining.pop(slug, None)
        if not matched and remaining:
            first_slug = next(iter(remaining))
            matched.append(remaining.pop(first_slug))
        groups.append({"stage": stage, "skillRefs": matched})
    if remaining:
        leftovers = list(remaining.values())
        if groups:
            groups[-1]["skillRefs"].extend(leftovers)
        else:
            groups.append({"stage": stages[0], "skillRefs": leftovers})
    return groups


def pack_required_runtimes(pack):
    runtimes = clean_unique_strings(pack.get("requiredRuntimes") or [])
    if runtimes:
        return runtimes
    derived = []
    for item in safe_list(pack.get("skillRefs")):
        if isinstance(item, dict):
            derived.extend(clean_unique_strings(item.get("requiresRuntime") or []))
    return clean_unique_strings(derived)


def default_pack_browser_profile(pack):
    runtimes = pack_required_runtimes(pack)
    if "browser" not in runtimes:
        return ""
    if "cookies" in runtimes:
        return "user"
    if str(pack.get("mode") or "").strip() in PACK_RUNTIME_BROWSER_MODES:
        return "user"
    return "openclaw"


def normalize_pack_artifact_templates(templates, pack):
    normalized = []
    seen = set()
    for index, item in enumerate(templates if isinstance(templates, list) else [], start=1):
        if not isinstance(item, dict):
            continue
        artifact_type = str(item.get("type") or item.get("artifactType") or "").strip().lower()
        title = str(item.get("title") or artifact_type_label(artifact_type)).strip()
        if not artifact_type:
            continue
        artifact_id = orchestration_slug(item.get("id") or item.get("key") or title or artifact_type) or f"artifact-{index}"
        if artifact_id in seen:
            continue
        seen.add(artifact_id)
        normalized.append(
            {
                "id": artifact_id,
                "type": artifact_type,
                "title": title or artifact_type_label(artifact_type),
                "stageKey": str(item.get("stageKey") or item.get("stage") or "").strip(),
                "required": normalize_flag(item.get("required"), default=False),
                "description": str(item.get("description") or item.get("note") or "").strip(),
            }
        )
    if normalized:
        return normalized
    derived = []
    for group in stage_skill_refs_for_pack(pack):
        stage = group.get("stage") if isinstance(group.get("stage"), dict) else {}
        stage_key = str(stage.get("key") or "").strip()
        stage_kind = str(stage.get("stage") or "").strip().lower()
        artifact_types = []
        for skill in safe_list(group.get("skillRefs")):
            if not isinstance(skill, dict):
                continue
            artifact_types.extend(
                [
                    artifact_type_from_handoff(item)
                    for item in clean_unique_strings(skill.get("handoffArtifacts") or [])
                    if artifact_type_from_handoff(item)
                ]
            )
        if not artifact_types and stage_kind:
            default_type = STAGE_DEFAULT_ARTIFACT_TYPES.get(stage_kind)
            if default_type:
                artifact_types.append(default_type)
        for artifact_type in clean_unique_strings(artifact_types):
            artifact_id = orchestration_slug(f"{stage_key}-{artifact_type}") or artifact_type
            derived.append(
                {
                    "id": artifact_id,
                    "type": artifact_type,
                    "title": artifact_type_label(artifact_type),
                    "stageKey": stage_key,
                    "required": True,
                    "description": str(stage.get("description") or "").strip(),
                }
            )
    return derived


def normalize_pack_review_gates(gates, pack):
    normalized = []
    seen = set()
    for index, item in enumerate(gates if isinstance(gates, list) else [], start=1):
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("label") or "").strip()
        gate_id = orchestration_slug(item.get("id") or item.get("key") or title or f"gate-{index}") or f"gate-{index}"
        if gate_id in seen:
            continue
        seen.add(gate_id)
        normalized.append(
            {
                "id": gate_id,
                "title": title or gate_id.replace("-", " ").title(),
                "stageKey": str(item.get("stageKey") or item.get("stage") or "").strip(),
                "blocking": normalize_flag(item.get("blocking"), default=True),
                "description": str(item.get("description") or item.get("note") or "").strip(),
                "requiredArtifacts": clean_unique_strings(item.get("requiredArtifacts") or item.get("artifactTypes") or []),
            }
        )
    if normalized:
        return normalized
    artifact_templates = normalize_pack_artifact_templates((pack.get("meta") or {}).get("artifactTemplates"), pack)
    required_by_stage = defaultdict(list)
    for item in artifact_templates:
        stage_key = str(item.get("stageKey") or "").strip()
        if stage_key:
            required_by_stage[stage_key].append(str(item.get("type") or "").strip())
    derived = []
    for index, stage in enumerate(normalize_workflow_pack_stages(pack.get("stages"), fallback_name=pack.get("name"), fallback_mode=pack.get("mode")), start=1):
        stage_key = str(stage.get("key") or f"stage-{index}").strip() or f"stage-{index}"
        stage_title = str(stage.get("title") or stage_key).strip() or stage_key
        stage_kind = str(stage.get("stage") or "").strip().lower()
        suffix = STAGE_GATE_SUFFIX_LABELS.get(stage_kind, "ready")
        derived.append(
            {
                "id": f"{stage_key}-gate",
                "title": f"{stage_title} {suffix}",
                "stageKey": stage_key,
                "blocking": True,
                "description": str(stage.get("description") or "").strip(),
                "requiredArtifacts": clean_unique_strings(required_by_stage.get(stage_key, [])),
            }
        )
    return derived


def normalize_pack_runtime_policy(policy, pack):
    policy = policy if isinstance(policy, dict) else {}
    runtimes = pack_required_runtimes(pack)
    browser_enabled = "browser" in runtimes
    return {
        "browserProfile": str(policy.get("browserProfile") or default_pack_browser_profile(pack)).strip(),
        "requiresCookieBootstrap": browser_enabled and (
            normalize_flag(policy.get("requiresCookieBootstrap"), default="cookies" in runtimes)
        ),
        "allowBrowserAutomation": browser_enabled and normalize_flag(policy.get("allowBrowserAutomation"), default=browser_enabled),
        "snapshotOnStart": browser_enabled and normalize_flag(policy.get("snapshotOnStart"), default=False),
    }


def normalize_pack_release_policy(policy, pack):
    policy = policy if isinstance(policy, dict) else {}
    runtimes = pack_required_runtimes(pack)
    default_provider = "github-pr" if "gh" in runtimes or str(pack.get("mode") or "").strip() == "release" else "manual"
    provider = str(policy.get("provider") or default_provider).strip().lower()
    if provider not in PACK_RELEASE_PROVIDERS:
        provider = default_provider
    return {
        "provider": provider,
        "dryRun": normalize_flag(policy.get("dryRun"), default=provider != "github-pr"),
        "requireAllGates": normalize_flag(policy.get("requireAllGates"), default=True),
        "completeOnSuccess": normalize_flag(policy.get("completeOnSuccess"), default=str(pack.get("mode") or "").strip() == "release"),
        "prTitleTemplate": str(policy.get("prTitleTemplate") or "{runTitle}").strip() or "{runTitle}",
        "branchPrefix": str(policy.get("branchPrefix") or "mission-control/").strip() or "mission-control/",
    }


def normalize_pack_qa_policy(policy, pack):
    policy = policy if isinstance(policy, dict) else {}
    runtimes = pack_required_runtimes(pack)
    qa_enabled = str(pack.get("mode") or "").strip() in {"qa", "design-review", "browser"} or "browser" in runtimes
    return {
        "enabled": normalize_flag(policy.get("enabled"), default=qa_enabled),
        "autoFix": normalize_flag(policy.get("autoFix"), default=str(pack.get("mode") or "").strip() == "qa" and "git" in runtimes),
        "verifyAfterFix": normalize_flag(policy.get("verifyAfterFix"), default="browser" in runtimes),
        "createRemediationTask": normalize_flag(policy.get("createRemediationTask"), default=True),
    }


def workflow_pack_capabilities(pack):
    meta = pack.get("meta") if isinstance(pack.get("meta"), dict) else {}
    artifact_templates = normalize_pack_artifact_templates(meta.get("artifactTemplates"), pack)
    review_gates = normalize_pack_review_gates(meta.get("reviewGates"), {**pack, "meta": {**meta, "artifactTemplates": artifact_templates}})
    return {
        "artifactTemplates": artifact_templates,
        "reviewGates": review_gates,
        "runtimePolicy": normalize_pack_runtime_policy(meta.get("runtimePolicy"), pack),
        "releasePolicy": normalize_pack_release_policy(meta.get("releasePolicy"), pack),
        "qaPolicy": normalize_pack_qa_policy(meta.get("qaPolicy"), pack),
        "modeAliases": clean_unique_strings(meta.get("modeAliases") or [str(pack.get("mode") or "").strip()]),
    }


def workflow_pack_meta_from_payload(payload, skills_data=None):
    payload = payload if isinstance(payload, dict) else {}
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    skill_map = {
        str(item.get("slug") or "").strip(): item
        for item in safe_list((skills_data or {}).get("skills"))
        if str(item.get("slug") or "").strip()
    }
    pack_context = {
        "name": str(payload.get("name") or "").strip(),
        "mode": str(payload.get("mode") or "").strip().lower(),
        "stages": normalize_workflow_pack_stages(
            payload.get("stages"),
            fallback_name=str(payload.get("name") or "").strip(),
            fallback_mode=str(payload.get("mode") or "").strip(),
        ),
        "skills": payload.get("skills") if isinstance(payload.get("skills"), list) else [],
        "skillRefs": [
            deepcopy(skill_map.get(slug, {"slug": slug}))
            for slug in clean_unique_strings(payload.get("skills") if isinstance(payload.get("skills"), list) else [])
        ],
        "requiredRuntimes": clean_unique_strings([
            runtime_name
            for slug in clean_unique_strings(payload.get("skills") if isinstance(payload.get("skills"), list) else [])
            for runtime_name in clean_unique_strings((skill_map.get(slug) or {}).get("requiresRuntime") or [])
        ]),
    }
    artifact_templates = normalize_pack_artifact_templates(meta.get("artifactTemplates"), pack_context)
    return {
        **{key: value for key, value in meta.items() if key not in {"reviewGates", "artifactTemplates", "runtimePolicy", "releasePolicy", "qaPolicy", "modeAliases"}},
        "modeAliases": clean_unique_strings(meta.get("modeAliases") or [pack_context.get("mode")]),
        "artifactTemplates": artifact_templates,
        "reviewGates": normalize_pack_review_gates(meta.get("reviewGates"), {**pack_context, "meta": {"artifactTemplates": artifact_templates}}),
        "runtimePolicy": normalize_pack_runtime_policy(meta.get("runtimePolicy"), pack_context),
        "releasePolicy": normalize_pack_release_policy(meta.get("releasePolicy"), pack_context),
        "qaPolicy": normalize_pack_qa_policy(meta.get("qaPolicy"), pack_context),
    }


def seed_run_review_gates(pack):
    first_stage_key = str(
        (normalize_workflow_pack_stages(pack.get("stages"), fallback_name=pack.get("name"), fallback_mode=pack.get("mode"))[0] or {}).get("key") or ""
    ).strip()
    now = now_iso()
    gates = []
    for item in workflow_pack_capabilities(pack).get("reviewGates", []):
        gate = deepcopy(item)
        gate["status"] = "active" if str(item.get("stageKey") or "").strip() == first_stage_key else "pending"
        gate["updatedAt"] = now if gate["status"] == "active" else ""
        gate["passedAt"] = ""
        gate["note"] = ""
        gate["artifactIds"] = []
        gates.append(gate)
    return gates


def normalize_run_review_gates(gates, pack):
    template_map = {
        item["id"]: item
        for item in workflow_pack_capabilities(pack).get("reviewGates", [])
        if item.get("id")
    }
    normalized = []
    source = gates if isinstance(gates, list) and gates else seed_run_review_gates(pack)
    for item in source:
        if not isinstance(item, dict):
            continue
        template = template_map.get(str(item.get("id") or "").strip(), {})
        merged = {**template, **item}
        status = str(merged.get("status") or "pending").strip().lower()
        if status not in {"pending", "active", "passed", "blocked"}:
            status = "pending"
        normalized.append(
            {
                "id": str(merged.get("id") or "").strip(),
                "title": str(merged.get("title") or "").strip(),
                "stageKey": str(merged.get("stageKey") or "").strip(),
                "blocking": normalize_flag(merged.get("blocking"), default=True),
                "description": str(merged.get("description") or "").strip(),
                "requiredArtifacts": clean_unique_strings(merged.get("requiredArtifacts") or []),
                "status": status,
                "updatedAt": str(merged.get("updatedAt") or "").strip(),
                "passedAt": str(merged.get("passedAt") or "").strip(),
                "note": str(merged.get("note") or "").strip(),
                "artifactIds": clean_unique_strings(merged.get("artifactIds") or []),
            }
        )
    return normalized


def normalize_run_artifacts(artifacts):
    normalized = []
    for item in artifacts if isinstance(artifacts, list) else []:
        if not isinstance(item, dict):
            continue
        artifact_type = str(item.get("type") or "").strip().lower()
        title = str(item.get("title") or artifact_type_label(artifact_type)).strip()
        artifact_id = str(item.get("id") or secrets.token_hex(6)).strip()
        if not artifact_type or not artifact_id:
            continue
        normalized.append(
            {
                "id": artifact_id,
                "type": artifact_type,
                "title": title or artifact_type_label(artifact_type),
                "status": str(item.get("status") or "ready").strip().lower() or "ready",
                "summary": str(item.get("summary") or "").strip(),
                "body": str(item.get("body") or "").strip(),
                "stageKey": str(item.get("stageKey") or item.get("stage") or "").strip(),
                "path": str(item.get("path") or item.get("output") or "").strip(),
                "createdAt": str(item.get("createdAt") or item.get("updatedAt") or now_iso()).strip(),
                "updatedAt": str(item.get("updatedAt") or item.get("createdAt") or now_iso()).strip(),
                "meta": item.get("meta") if isinstance(item.get("meta"), dict) else {},
            }
        )
    return normalized


def seed_run_runtime_sessions(pack):
    policy = workflow_pack_capabilities(pack).get("runtimePolicy", {})
    runtimes = pack_required_runtimes(pack)
    if "browser" not in runtimes:
        return {}
    return {
        "browser": {
            "enabled": True,
            "status": "idle",
            "profile": str(policy.get("browserProfile") or "").strip(),
            "requiresCookieBootstrap": bool(policy.get("requiresCookieBootstrap")),
            "cookieBootstrapStatus": "pending" if policy.get("requiresCookieBootstrap") else "ready",
            "lastSnapshotAt": "",
            "lastSnapshotSummary": "",
            "targetUrl": "",
            "bootstrappedAt": "",
        }
    }


def normalize_run_runtime_sessions(runtime_sessions, pack):
    base = seed_run_runtime_sessions(pack)
    runtime_sessions = runtime_sessions if isinstance(runtime_sessions, dict) else {}
    if not base and not runtime_sessions:
        return {}
    browser = runtime_sessions.get("browser") if isinstance(runtime_sessions.get("browser"), dict) else {}
    seeded_browser = base.get("browser", {})
    if browser or seeded_browser:
        base["browser"] = {
            **seeded_browser,
            **browser,
            "enabled": normalize_flag(browser.get("enabled"), default=seeded_browser.get("enabled", False)),
            "requiresCookieBootstrap": normalize_flag(
                browser.get("requiresCookieBootstrap"),
                default=seeded_browser.get("requiresCookieBootstrap", False),
            ),
        }
    return base


def seed_run_release_automation(pack):
    policy = workflow_pack_capabilities(pack).get("releasePolicy", {})
    return {
        "provider": str(policy.get("provider") or "manual").strip(),
        "dryRun": bool(policy.get("dryRun")),
        "requireAllGates": bool(policy.get("requireAllGates")),
        "completeOnSuccess": bool(policy.get("completeOnSuccess")),
        "status": "idle",
        "lastAttemptAt": "",
        "prNumber": "",
        "prUrl": "",
        "headBranch": "",
        "baseBranch": "",
        "output": "",
    }


def normalize_run_release_automation(state, pack):
    seeded = seed_run_release_automation(pack)
    state = state if isinstance(state, dict) else {}
    return {**seeded, **state}


def seed_run_qa_automation(pack):
    policy = workflow_pack_capabilities(pack).get("qaPolicy", {})
    return {
        "enabled": bool(policy.get("enabled")),
        "autoFix": bool(policy.get("autoFix")),
        "verifyAfterFix": bool(policy.get("verifyAfterFix")),
        "createRemediationTask": bool(policy.get("createRemediationTask")),
        "status": "idle",
        "lastRunAt": "",
        "issueCount": 0,
        "followupTaskId": "",
        "summary": "",
    }


def normalize_run_qa_automation(state, pack):
    seeded = seed_run_qa_automation(pack)
    state = state if isinstance(state, dict) else {}
    return {**seeded, **state}


def seeded_run_meta_from_pack(pack, existing_meta=None):
    existing_meta = existing_meta if isinstance(existing_meta, dict) else {}
    artifact_templates = normalize_pack_artifact_templates(existing_meta.get("artifactTemplates"), pack)
    return {
        **existing_meta,
        "artifactTemplates": artifact_templates,
        "reviewGates": normalize_run_review_gates(existing_meta.get("reviewGates"), {**pack, "meta": {"artifactTemplates": artifact_templates}}),
        "artifacts": normalize_run_artifacts(existing_meta.get("artifacts")),
        "runtimeSessions": normalize_run_runtime_sessions(existing_meta.get("runtimeSessions"), pack),
        "releaseAutomation": normalize_run_release_automation(existing_meta.get("releaseAutomation"), pack),
        "qaAutomation": normalize_run_qa_automation(existing_meta.get("qaAutomation"), pack),
    }


def build_pack_handoff_note(pack, stage, skill_refs):
    pack = pack if isinstance(pack, dict) else {}
    stage = stage if isinstance(stage, dict) else {}
    artifacts = []
    runtimes = []
    for item in safe_list(skill_refs):
        if not isinstance(item, dict):
            continue
        artifacts.extend(clean_unique_strings(item.get("handoffArtifacts") or []))
        runtimes.extend(clean_unique_strings(item.get("requiresRuntime") or []))
    stage_title = str(stage.get("title") or pack.get("name") or "当前阶段").strip() or "当前阶段"
    parts = [f"{stage_title} 交接说明："]
    if skill_refs:
        parts.append("优先技能：" + "、".join(str(item.get("displayName") or item.get("slug") or "").strip() for item in skill_refs if str(item.get("displayName") or item.get("slug") or "").strip()))
    if artifacts:
        parts.append("建议产物：" + "、".join(clean_unique_strings(artifacts)))
    if runtimes:
        parts.append("运行时依赖：" + "、".join(clean_unique_strings(runtimes)))
    description = str(stage.get("description") or pack.get("description") or "").strip()
    if description:
        parts.append(f"阶段目标：{description}")
    parts.append("接手后请先回报：当前判断 / 下一步动作 / 风险。")
    return "\n".join(parts)


def build_pack_workflow_lanes(pack):
    lanes = []
    for stage in normalize_workflow_pack_stages(pack.get("stages"), fallback_name=pack.get("name"), fallback_mode=pack.get("mode")):
        lanes.append(
            {
                "id": str(stage.get("key") or "").strip(),
                "title": str(stage.get("title") or stage.get("key") or "").strip(),
                "subtitle": str(stage.get("description") or "").strip(),
            }
        )
    return lanes


def build_pack_workflow_nodes(pack, agents, router_agent_id=""):
    agent_ids = [str(item.get("id") or "").strip() for item in safe_list(agents) if isinstance(item, dict) and str(item.get("id") or "").strip()]
    default_agent_id = str(router_agent_id or "").strip() or (agent_ids[0] if agent_ids else "")
    nodes = []
    for group in stage_skill_refs_for_pack(pack):
        stage = group.get("stage") if isinstance(group.get("stage"), dict) else {}
        lane_id = str(stage.get("key") or "").strip()
        skill_refs = safe_list(group.get("skillRefs"))
        if not skill_refs:
            skill_refs = [{}]
        for index, skill in enumerate(skill_refs, start=1):
            skill = skill if isinstance(skill, dict) else {}
            slug = str(skill.get("slug") or "").strip()
            stage_title = str(stage.get("title") or lane_id or f"Stage {index}").strip()
            node_title = str(skill.get("displayName") or skill.get("slug") or "").strip() or stage_title
            nodes.append(
                {
                    "id": f"{lane_id or 'lane'}-node-{index}",
                    "laneId": lane_id,
                    "title": node_title,
                    "type": "agent",
                    "config": {
                        "skillSlug": slug,
                        "packId": str(pack.get("id") or "").strip(),
                        "mode": str(skill.get("mode") or stage.get("mode") or pack.get("mode") or "").strip(),
                        "stage": str(skill.get("stage") or stage.get("stage") or "").strip(),
                        "requiredRuntimes": clean_unique_strings(skill.get("requiresRuntime") or []),
                    },
                    "agentId": default_agent_id,
                    "handoffNote": build_pack_handoff_note(pack, stage, skill_refs),
                    "conditions": [],
                }
            )
    return nodes


def artifact_deliverable_payload(artifact, run, now=None):
    artifact = artifact if isinstance(artifact, dict) else {}
    run = run if isinstance(run, dict) else {}
    updated_at = str(artifact.get("updatedAt") or artifact.get("createdAt") or "").strip()
    updated_dt = parse_iso(updated_at)
    return {
        "id": str(artifact.get("id") or "").strip(),
        "title": str(artifact.get("title") or artifact_type_label(artifact.get("type"))).strip(),
        "state": str(artifact.get("status") or "ready").strip(),
        "status": str(artifact.get("status") or "ready").strip(),
        "statusLabel": str(artifact.get("status") or "ready").strip(),
        "owner": str(run.get("owner") or "").strip(),
        "updatedAt": updated_at,
        "updatedAgo": format_age(updated_dt, now or datetime.now(timezone.utc)) if updated_dt else "",
        "summary": str(artifact.get("summary") or artifact.get("body") or "").strip()[:220],
        "output": str(artifact.get("path") or "").strip(),
        "path": str(artifact.get("path") or "").strip(),
        "sourceTask": str(run.get("linkedTaskId") or "").strip(),
        "sourceRun": str(run.get("id") or "").strip(),
        "type": str(artifact.get("type") or "").strip(),
        "artifact": True,
    }


def build_run_artifact_summary(artifacts, templates):
    required_ids = {str(item.get("id") or "").strip() for item in templates if item.get("required")}
    required_types = {str(item.get("type") or "").strip() for item in templates if item.get("required")}
    ready_required = 0
    for item in artifacts:
        artifact_id = str(item.get("id") or "").strip()
        artifact_type = str(item.get("type") or "").strip()
        if artifact_id in required_ids or artifact_type in required_types:
            ready_required += 1
    return {
        "total": len(artifacts),
        "requiredCount": len(required_ids) or len(required_types),
        "readyRequiredCount": ready_required,
    }


def hydrate_management_run_pack_context(run, pack_map):
    run = deepcopy(run) if isinstance(run, dict) else {}
    pack_binding = run.get("packBinding") if isinstance(run.get("packBinding"), dict) else {}
    linked_pack = hydrate_linked_pack(pack_binding, pack_map)
    run["linkedPackId"] = linked_pack.get("id", "")
    run["linkedPack"] = linked_pack
    run_meta = run.get("meta") if isinstance(run.get("meta"), dict) else {}
    pack_context = linked_pack if linked_pack.get("id") else {
        "stages": run.get("stages") or [],
        "mode": str(run.get("mode") or pack_binding.get("mode") or "").strip(),
        "requiredRuntimes": clean_unique_strings(pack_binding.get("requiredRuntimes") or []),
    }
    artifact_templates = normalize_pack_artifact_templates(run_meta.get("artifactTemplates"), pack_context)
    review_gates = normalize_run_review_gates(run_meta.get("reviewGates"), {**pack_context, "meta": {"artifactTemplates": artifact_templates}})
    artifacts = normalize_run_artifacts(run_meta.get("artifacts"))
    runtime_sessions = normalize_run_runtime_sessions(run_meta.get("runtimeSessions"), pack_context)
    release_automation = normalize_run_release_automation(run_meta.get("releaseAutomation"), pack_context)
    qa_automation = normalize_run_qa_automation(run_meta.get("qaAutomation"), pack_context)
    run["artifactTemplates"] = artifact_templates
    run["reviewGates"] = review_gates
    run["artifacts"] = artifacts
    run["artifactSummary"] = build_run_artifact_summary(artifacts, artifact_templates)
    run["gateSummary"] = {
        "total": len(review_gates),
        "passed": sum(1 for item in review_gates if item.get("status") == "passed"),
        "blocked": sum(1 for item in review_gates if item.get("status") == "blocked"),
        "active": sum(1 for item in review_gates if item.get("status") == "active"),
        "pending": sum(1 for item in review_gates if item.get("status") == "pending"),
    }
    run["runtimeSessions"] = runtime_sessions
    run["browserSession"] = runtime_sessions.get("browser") if isinstance(runtime_sessions.get("browser"), dict) else {}
    run["releaseAutomation"] = release_automation
    run["qaAutomation"] = qa_automation
    return run


def hydrate_chat_thread_pack_context(thread, pack_map):
    thread = deepcopy(thread) if isinstance(thread, dict) else {}
    pack_binding = thread.get("packBinding") if isinstance(thread.get("packBinding"), dict) else {}
    if not pack_binding:
        meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
        pack_binding = meta.get("packBinding") if isinstance(meta.get("packBinding"), dict) else {}
    linked_pack = hydrate_linked_pack(pack_binding, pack_map)
    thread["packBinding"] = pack_binding
    thread["linkedPackId"] = linked_pack.get("id", "")
    thread["linkedPack"] = linked_pack
    meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    thread["reviewGates"] = safe_list(meta.get("reviewGates")) or safe_list(linked_pack.get("reviewGates"))
    thread["artifactTemplates"] = safe_list(meta.get("artifactTemplates")) or safe_list(linked_pack.get("artifactTemplates"))
    thread["runtimePolicy"] = meta.get("runtimePolicy") if isinstance(meta.get("runtimePolicy"), dict) else (linked_pack.get("runtimePolicy") or {})
    return thread
