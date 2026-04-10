from __future__ import annotations

import sys


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


recommended_team_id_for_pack_mode = _DelegatedSymbol("recommended_team_id_for_pack_mode")
safe_list = _DelegatedSymbol("safe_list")

TEAM_OWNERSHIP_ROLE_ORDER = ("command", "execution", "gate", "signals")


def team_reference_payload(team_map, team_id="", fallback_name=""):
    normalized_team_id = str(team_id or "").strip()
    team = team_map.get(normalized_team_id, {}) if isinstance(team_map, dict) and normalized_team_id else {}
    resolved_team_id = normalized_team_id or str((team or {}).get("id") or "").strip()
    resolved_team_name = (
        str((team or {}).get("name") or "").strip()
        or str(fallback_name or "").strip()
        or resolved_team_id
    )
    if not resolved_team_id and not resolved_team_name:
        return {}
    return {
        "teamId": resolved_team_id,
        "teamName": resolved_team_name,
    }


def default_execution_team_id(team_map, mode=""):
    normalized_mode = str(mode or "").strip().lower()
    preferred_ids = [
        recommended_team_id_for_pack_mode(normalized_mode),
        "team-delivery",
        "team-core",
        "team-release",
        "team-signals",
    ]
    for candidate in preferred_ids:
        if candidate and candidate in (team_map or {}):
            return candidate
    return next(iter((team_map or {}).keys()), "")


def build_team_ownership_payload(team_map, execution_team_id="", recommended_team_id="", mode="", source="derived"):
    team_map = team_map if isinstance(team_map, dict) else {}
    normalized_mode = str(mode or "").strip().lower()
    recommended_execution_team_id = str(recommended_team_id or "").strip() or recommended_team_id_for_pack_mode(normalized_mode)
    resolved_execution_team_id = (
        str(execution_team_id or "").strip()
        or recommended_execution_team_id
        or default_execution_team_id(team_map, normalized_mode)
    )
    command_team_id = "team-core" if "team-core" in team_map else resolved_execution_team_id
    gate_team_id = "team-release" if "team-release" in team_map else resolved_execution_team_id
    signals_team_id = "team-signals" if "team-signals" in team_map else resolved_execution_team_id

    command_team = team_reference_payload(team_map, command_team_id)
    execution_team = team_reference_payload(team_map, resolved_execution_team_id)
    gate_team = team_reference_payload(team_map, gate_team_id)
    signals_team = team_reference_payload(team_map, signals_team_id)
    recommended_execution_team = team_reference_payload(team_map, recommended_execution_team_id)

    roles = []
    role_map = {
        "command": command_team,
        "execution": execution_team,
        "gate": gate_team,
        "signals": signals_team,
    }
    execution_role_id = str(execution_team.get("teamId") or "").strip()
    for role in TEAM_OWNERSHIP_ROLE_ORDER:
        team_ref = role_map.get(role) if isinstance(role_map.get(role), dict) else {}
        team_ref_id = str(team_ref.get("teamId") or "").strip()
        team_ref_name = str(team_ref.get("teamName") or "").strip()
        if not team_ref_id and not team_ref_name:
            continue
        roles.append(
            {
                "role": role,
                "teamId": team_ref_id,
                "teamName": team_ref_name or team_ref_id,
                "sameAsExecution": bool(role != "execution" and team_ref_id and team_ref_id == execution_role_id),
            }
        )

    return {
        "mode": normalized_mode,
        "source": str(source or "derived").strip() or "derived",
        "commandTeam": command_team,
        "executionTeam": execution_team,
        "gateTeam": gate_team,
        "signalsTeam": signals_team,
        "recommendedExecutionTeam": recommended_execution_team,
        "executionDiffersFromRecommended": bool(
            recommended_execution_team.get("teamId")
            and execution_team.get("teamId")
            and recommended_execution_team.get("teamId") != execution_team.get("teamId")
        ),
        "roles": roles,
    }


def enrich_task_team_ownership(task_items, team_map):
    for task in safe_list(task_items):
        if not isinstance(task, dict):
            continue
        linked_run = task.get("linkedRun") if isinstance(task.get("linkedRun"), dict) else {}
        linked_pack = linked_run.get("linkedPack") if isinstance(linked_run.get("linkedPack"), dict) else {}
        route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        team_selection = route_meta.get("teamSelection") if isinstance(route_meta.get("teamSelection"), dict) else {}
        execution_team_id = (
            str(task.get("teamId") or "").strip()
            or str((task.get("linkedTeam") or {}).get("id") or "").strip()
            or str(linked_run.get("linkedTeamId") or "").strip()
            or str((linked_run.get("linkedTeam") or {}).get("id") or "").strip()
            or str(team_selection.get("selectedTeamId") or "").strip()
        )
        recommended_team_id = (
            str(team_selection.get("recommendedTeamId") or "").strip()
            or str(linked_pack.get("recommendedTeamId") or "").strip()
        )
        mode = str(linked_pack.get("mode") or "").strip()
        source = "run" if linked_run.get("id") else "task"
        task["teamOwnership"] = build_team_ownership_payload(
            team_map,
            execution_team_id=execution_team_id,
            recommended_team_id=recommended_team_id,
            mode=mode,
            source=source,
        )
