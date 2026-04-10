"""Core config functions — re-exported from collaboration_dashboard."""

from backend.collaboration_dashboard import (  # noqa: F401
    infer_theme_name_from_agents,
    legacy_project_metadata,
    load_config,
    load_project_metadata,
    normalize_project_metadata,
    project_metadata_candidate_paths,
    project_metadata_path,
    sanitize_openclaw_config_for_write,
    sanitize_runtime_secret_placeholders,
    save_config,
    save_project_metadata,
)
