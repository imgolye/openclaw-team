#!/usr/bin/env python3
"""Utilities for Claude-style skills managed by OpenClaw Team."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except Exception:  # pragma: no cover - fallback keeps the tool portable.
    yaml = None


FRONTMATTER_DELIMITER = "---"
KEBAB_CASE_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
MAX_DESCRIPTION_LENGTH = 1024
MAX_SKILL_WORDS = 5000
VALID_CATEGORIES = {
    "document-asset-creation": {
        "label": "Document & Asset Creation",
        "summary": "Create consistent output such as docs, apps, assets, and designs.",
    },
    "workflow-automation": {
        "label": "Workflow Automation",
        "summary": "Automate repeatable multi-step processes with a consistent method.",
    },
    "mcp-enhancement": {
        "label": "MCP Enhancement",
        "summary": "Teach Claude how to use connected tools and MCP workflows reliably.",
    },
}


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_kebab_case(value, field_name):
    if not isinstance(value, str) or not KEBAB_CASE_RE.match(value.strip()):
        return f"{field_name} must be kebab-case with lowercase letters, numbers, and hyphens only."
    return None


def skill_roots(project_dir, openclaw_dir=None):
    roots = []
    project_dir = Path(project_dir).expanduser().resolve()
    roots.append(("project", project_dir / "platform" / "skills"))
    if openclaw_dir:
        openclaw_dir = Path(openclaw_dir).expanduser().resolve()
        roots.append(("workspace", openclaw_dir / "skills"))
    return roots


def vendor_skill_root(project_dir):
    return Path(project_dir).expanduser().resolve() / "platform" / "vendor" / "openclaw-skills"


def resolve_installable_skill_dir(project_dir, slug):
    project_dir = Path(project_dir).expanduser().resolve()
    normalized_slug = str(slug or "").strip()
    if not normalized_slug:
        raise RuntimeError("Skill slug is required.")
    candidates = [
        ("project", project_dir / "platform" / "skills" / normalized_slug),
        ("vendored", vendor_skill_root(project_dir) / normalized_slug),
    ]
    for source_kind, skill_dir in candidates:
        if (skill_dir / "SKILL.md").exists():
            return source_kind, skill_dir
    raise RuntimeError(f"Skill {normalized_slug} is not available in project or vendored starter libraries.")


def install_skill(project_dir, openclaw_dir, slug):
    project_dir = Path(project_dir).expanduser().resolve()
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    normalized_slug = str(slug or "").strip()
    source_kind, source_dir = resolve_installable_skill_dir(project_dir, normalized_slug)
    target_root = openclaw_dir / "skills"
    target_root.mkdir(parents=True, exist_ok=True)
    target_dir = target_root / normalized_slug
    existed = target_dir.exists()
    if target_dir.exists():
        shutil.rmtree(target_dir)
    shutil.copytree(source_dir, target_dir)
    skill_md = target_dir / "SKILL.md"
    if not skill_md.exists():
        raise RuntimeError("Installed skill is missing SKILL.md after copy.")
    return {
        "skill": normalized_slug,
        "sourceKind": source_kind,
        "sourcePath": str(source_dir),
        "targetPath": str(target_dir),
        "reinstalled": bool(existed),
        "updatedAt": now_iso(),
    }


def uninstall_skill(openclaw_dir, slug):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    normalized_slug = str(slug or "").strip()
    if not normalized_slug:
        raise RuntimeError("Skill slug is required.")
    target_dir = openclaw_dir / "skills" / normalized_slug
    existed = target_dir.exists()
    if target_dir.exists():
        shutil.rmtree(target_dir)
    return {
        "skill": normalized_slug,
        "targetPath": str(target_dir),
        "removed": bool(existed),
        "updatedAt": now_iso(),
    }


def dist_root(project_dir):
    return Path(project_dir).expanduser().resolve() / "platform" / "dist" / "skills"


def split_frontmatter(text):
    lines = text.splitlines()
    if not lines or lines[0].strip() != FRONTMATTER_DELIMITER:
        return None, None, ["SKILL.md must start with YAML frontmatter delimited by ---."]
    for index in range(1, len(lines)):
        if lines[index].strip() == FRONTMATTER_DELIMITER:
            frontmatter_text = "\n".join(lines[1:index]).strip()
            body_text = "\n".join(lines[index + 1 :]).strip()
            return frontmatter_text, body_text, []
    return None, None, ["SKILL.md frontmatter is missing the closing --- delimiter."]


def parse_frontmatter(frontmatter_text):
    if "<" in frontmatter_text or ">" in frontmatter_text:
        return None, ["Frontmatter cannot contain < or > characters."]
    if yaml is not None:
        try:
            data = yaml.safe_load(frontmatter_text) or {}
        except yaml.YAMLError as error:
            return None, [f"Invalid YAML frontmatter: {error}"]
    else:
        data = {}
        current_parent = None
        for raw_line in frontmatter_text.splitlines():
            line = raw_line.rstrip()
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if line.startswith("  "):
                if not current_parent or not isinstance(data.get(current_parent), dict) or ":" not in line.strip():
                    return None, ["Frontmatter indentation is invalid and PyYAML is not available for fallback parsing."]
                subkey, subvalue = line.strip().split(":", 1)
                data[current_parent][subkey.strip()] = subvalue.strip().strip("'\"")
                continue
            if ":" not in line:
                return None, ["Frontmatter contains a line without a key/value separator."]
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if not value:
                data[key] = {}
                current_parent = key
            else:
                data[key] = value.strip("'\"")
                current_parent = None
    if not isinstance(data, dict):
        return None, ["Frontmatter must parse to an object."]
    return data, []


def infer_category(frontmatter, body_text, scripts_count, references_count):
    metadata = frontmatter.get("metadata") if isinstance(frontmatter.get("metadata"), dict) else {}
    compatibility = str(frontmatter.get("compatibility", "") or "")
    description = str(frontmatter.get("description", "") or "")
    haystack = " ".join([json.dumps(metadata, ensure_ascii=False), compatibility, description, body_text]).lower()
    if "mcp" in haystack or "connector" in haystack or "tool access" in haystack:
        return "mcp-enhancement"
    if scripts_count or "workflow" in haystack or "step 1" in haystack or "phase 1" in haystack:
        return "workflow-automation"
    if references_count or "template" in haystack or "design" in haystack or "document" in haystack:
        return "document-asset-creation"
    return "workflow-automation"


def detect_examples(body_text, skill_dir):
    lowered = body_text.lower()
    if "example:" in lowered or "example 1:" in lowered or "user says:" in lowered:
        return True
    examples_dir = skill_dir / "references" / "examples"
    return examples_dir.exists()


def detect_error_handling(body_text):
    lowered = body_text.lower()
    return any(token in lowered for token in ("troubleshoot", "error:", "common issue", "if you see", "failed"))


def detect_reference_linking(body_text):
    return "references/" in body_text or "`references/" in body_text


def readiness_level(errors, warnings):
    if errors:
        return "error"
    if warnings:
        return "warning"
    return "ready"


def quality_score(errors, warnings):
    score = 100 - len(errors) * 25 - len(warnings) * 8
    return max(score, 0)


def normalize_issue(kind, message):
    return {"kind": kind, "message": message}


def validate_skill_dir(skill_dir, project_dir):
    skill_dir = Path(skill_dir).resolve()
    project_dir = Path(project_dir).resolve()
    errors = []
    warnings = []
    info = []

    folder_name = skill_dir.name
    maybe_kebab = ensure_kebab_case(folder_name, "Skill folder name")
    if maybe_kebab:
        errors.append(maybe_kebab)

    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        errors.append("Missing required SKILL.md file.")
        frontmatter = {}
        body_text = ""
    else:
        text = skill_md.read_text(encoding="utf-8")
        frontmatter_text, body_text, split_errors = split_frontmatter(text)
        errors.extend(split_errors)
        if frontmatter_text is not None:
            frontmatter, parse_errors = parse_frontmatter(frontmatter_text)
            errors.extend(parse_errors)
            if frontmatter is None:
                frontmatter = {}
        else:
            frontmatter = {}

    name = str(frontmatter.get("name", "") or "")
    description = str(frontmatter.get("description", "") or "")
    license_name = str(frontmatter.get("license", "") or "")
    compatibility = str(frontmatter.get("compatibility", "") or "")
    metadata = frontmatter.get("metadata") if isinstance(frontmatter.get("metadata"), dict) else {}

    if not name:
        errors.append("Frontmatter is missing required field: name.")
    else:
        name_issue = ensure_kebab_case(name, "Frontmatter name")
        if name_issue:
            errors.append(name_issue)
        if name != folder_name:
            warnings.append("Frontmatter name does not match the skill folder name.")
        if "claude" in name or "anthropic" in name:
            errors.append("Skill name cannot contain reserved words 'claude' or 'anthropic'.")

    if not description:
        errors.append("Frontmatter is missing required field: description.")
    else:
        if len(description) > MAX_DESCRIPTION_LENGTH:
            errors.append(f"description must be under {MAX_DESCRIPTION_LENGTH} characters.")
        lowered = description.lower()
        if "use when" not in lowered and "use for" not in lowered:
            errors.append("description should include explicit usage triggers such as 'Use when ...'.")
        if len(description.split()) < 8:
            warnings.append("description is likely too short to explain both the capability and trigger conditions.")

    readme_path = skill_dir / "README.md"
    if readme_path.exists():
        warnings.append("Skill folders should not contain README.md; keep human-facing docs at the repo level instead.")

    scripts_dir = skill_dir / "scripts"
    references_dir = skill_dir / "references"
    assets_dir = skill_dir / "assets"
    scripts_count = len([item for item in scripts_dir.rglob("*") if item.is_file()]) if scripts_dir.exists() else 0
    references_count = (
        len([item for item in references_dir.rglob("*") if item.is_file()]) if references_dir.exists() else 0
    )
    assets_count = len([item for item in assets_dir.rglob("*") if item.is_file()]) if assets_dir.exists() else 0

    word_count = len(body_text.split())
    if word_count > MAX_SKILL_WORDS:
        warnings.append(
            f"SKILL.md body is {word_count} words; Anthropic recommends keeping it under {MAX_SKILL_WORDS} words."
        )

    if "## instructions" not in body_text.lower() and "# instructions" not in body_text.lower():
        warnings.append("SKILL.md does not include an obvious Instructions section.")
    if not detect_examples(body_text, skill_dir):
        warnings.append("Skill does not include concrete examples or an examples reference.")
    if not detect_error_handling(body_text):
        warnings.append("Skill does not appear to include troubleshooting or error handling guidance.")
    if references_count and not detect_reference_linking(body_text):
        warnings.append("Skill has references/ files but does not clearly link to them from SKILL.md.")

    category = infer_category(frontmatter, body_text, scripts_count, references_count)
    category_meta = VALID_CATEGORIES[category]
    package_path = dist_root(project_dir) / f"{folder_name}.zip"

    if scripts_count:
        info.append("Includes executable helper scripts.")
    if references_count:
        info.append("Uses progressive disclosure via references/.")
    if assets_count:
        info.append("Includes bundled assets/templates.")
    if metadata.get("version"):
        info.append(f"Version {metadata['version']}.")
    if metadata.get("mcp-server"):
        info.append(f"Targets MCP server {metadata['mcp-server']}.")

    created_heading = ""
    if body_text:
        for line in body_text.splitlines():
            stripped = line.strip()
            if stripped.startswith("# "):
                created_heading = stripped[2:].strip()
                break

    return {
        "slug": folder_name,
        "displayName": created_heading or name or folder_name,
        "name": name or folder_name,
        "description": description,
        "license": license_name,
        "compatibility": compatibility,
        "metadata": metadata,
        "category": category,
        "categoryLabel": category_meta["label"],
        "categorySummary": category_meta["summary"],
        "path": str(skill_dir),
        "relativePath": str(skill_dir.relative_to(project_dir)) if skill_dir.is_relative_to(project_dir) else str(skill_dir),
        "hasScripts": bool(scripts_count),
        "hasReferences": bool(references_count),
        "hasAssets": bool(assets_count),
        "scriptsCount": scripts_count,
        "referencesCount": references_count,
        "assetsCount": assets_count,
        "wordCount": word_count,
        "status": readiness_level(errors, warnings),
        "qualityScore": quality_score(errors, warnings),
        "issues": [normalize_issue("error", item) for item in errors]
        + [normalize_issue("warning", item) for item in warnings],
        "notes": info,
        "triggerHint": description.split("Use ", 1)[1].strip() if "Use " in description else description,
        "package": {
            "path": str(package_path),
            "exists": package_path.exists(),
            "updatedAt": datetime.fromtimestamp(package_path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if package_path.exists()
            else "",
        },
    }


def scan_skills(project_dir, openclaw_dir=None):
    project_dir = Path(project_dir).expanduser().resolve()
    skills = []
    roots_info = []
    for root_kind, root_path in skill_roots(project_dir, openclaw_dir):
        roots_info.append(
            {
                "kind": root_kind,
                "path": str(root_path),
                "exists": root_path.exists(),
            }
        )
        if not root_path.exists():
            continue
        for child in sorted(root_path.iterdir()):
            if child.is_dir() and not child.name.startswith("."):
                skill = validate_skill_dir(child, project_dir)
                skill["rootKind"] = root_kind
                skills.append(skill)

    status_counts = Counter(skill["status"] for skill in skills)
    category_counts = Counter(skill["category"] for skill in skills)
    packaged_count = sum(1 for skill in skills if skill["package"]["exists"])
    summary = {
        "total": len(skills),
        "ready": status_counts["ready"],
        "warning": status_counts["warning"],
        "error": status_counts["error"],
        "packaged": packaged_count,
        "categories": dict(category_counts),
    }
    return {
        "generatedAt": now_iso(),
        "projectDir": str(project_dir),
        "roots": roots_info,
        "summary": summary,
        "skills": skills,
        "guidance": [
            {
                "title": "Progressive disclosure",
                "summary": "Keep SKILL.md focused, push heavy docs into references/, and only load them when needed.",
            },
            {
                "title": "Trigger quality",
                "summary": "Descriptions should say what the skill does and exactly when to use it, ideally with phrases users really say.",
            },
            {
                "title": "Distribution readiness",
                "summary": "Each skill should be easy to zip, upload, share on GitHub, and test in real conversations.",
            },
        ],
    }


def scaffold_template(
    slug,
    title,
    description,
    trigger_phrase,
    category,
    author="",
    version="1.0.0",
    mcp_server="",
):
    category_summary = VALID_CATEGORIES[category]["summary"]
    metadata_lines = []
    if author:
        metadata_lines.append(f"  author: {author}")
    if version:
        metadata_lines.append(f"  version: {version}")
    if mcp_server:
        metadata_lines.append(f"  mcp-server: {mcp_server}")
    trigger_sentence = trigger_phrase.strip() or "the user asks for this workflow"
    lines = [
        "---",
        f"name: {slug}",
        f"description: {description.strip()} Use when the user asks to {trigger_sentence}.",
        "compatibility: Claude Code, Claude.ai, and API environments that support local files and optional helper scripts.",
    ]
    if metadata_lines:
        lines.append("metadata:")
        lines.extend(metadata_lines)
    lines.extend(
        [
            "---",
            f"# {title}",
            "",
            "## Instructions",
            "- Start by restating the user's outcome in one sentence and identify the success condition.",
            "- Choose the smallest reliable workflow that gets to value quickly.",
            "- Prefer progressive disclosure: keep the main workflow here and load `references/` only when the task truly needs more detail.",
            "- If helper scripts exist in `scripts/`, run them instead of manually repeating long mechanical work.",
            "- Explain what you are doing when a step changes external state, creates files, or triggers a longer workflow.",
            "",
            "## Workflow",
            "### Step 1: Understand the request",
            "- Confirm the concrete output the user wants.",
            "- Capture important constraints such as deadline, audience, file format, or system boundaries.",
            "",
            "### Step 2: Execute the core flow",
            "- Follow the shortest path to produce value.",
            "- Use bundled references, scripts, and assets when they improve consistency.",
            f"- Keep outputs aligned with this skill's goal: {category_summary}",
            "",
            "### Step 3: Validate before finishing",
            "- Review the result for completeness, formatting quality, and missing assumptions.",
            "- If a validation script exists, run it.",
            "- Call out any remaining risks or manual follow-up clearly.",
            "",
            "## Examples",
            f'- User says: "{trigger_phrase.strip() or "Help me use this skill"}"',
            "  Result: Claude follows the workflow above and produces a complete, validated output.",
            "",
            "## Troubleshooting",
            "- If required files or tools are missing, explain the blocker clearly and suggest the smallest fix.",
            "- If results are inconsistent, tighten instructions and move bulky detail into `references/`.",
            "- If the skill starts triggering too often, narrow the description with more specific trigger phrases.",
            "",
        ]
    )
    return "\n".join(lines)


def create_skill_scaffold(
    project_dir,
    slug,
    title,
    description,
    trigger_phrase,
    category,
    include_scripts=False,
    include_references=True,
    include_assets=False,
    author="",
    version="1.0.0",
    mcp_server="",
):
    project_dir = Path(project_dir).expanduser().resolve()
    skills_root = project_dir / "platform" / "skills"
    skills_root.mkdir(parents=True, exist_ok=True)
    slug_issue = ensure_kebab_case(slug, "Skill slug")
    if slug_issue:
        raise RuntimeError(slug_issue)
    if category not in VALID_CATEGORIES:
        raise RuntimeError(f"Unknown skill category: {category}")

    skill_dir = skills_root / slug
    if skill_dir.exists():
        raise RuntimeError(f"Skill {slug} already exists.")

    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        scaffold_template(
            slug=slug,
            title=title,
            description=description,
            trigger_phrase=trigger_phrase,
            category=category,
            author=author,
            version=version,
            mcp_server=mcp_server,
        ),
        encoding="utf-8",
    )

    if include_scripts:
        scripts_dir = skill_dir / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "README.txt").write_text(
            "Place helper scripts here. Prefer deterministic validation or formatting steps.\n",
            encoding="utf-8",
        )
    if include_references:
        references_dir = skill_dir / "references"
        references_dir.mkdir()
        (references_dir / "reference-notes.md").write_text(
            "# Reference Notes\n\nAdd detailed guidance, API patterns, or style rules here.\n",
            encoding="utf-8",
        )
    if include_assets:
        assets_dir = skill_dir / "assets"
        assets_dir.mkdir()
        (assets_dir / ".gitkeep").write_text("", encoding="utf-8")

    return validate_skill_dir(skill_dir, project_dir)


def package_skill(project_dir, slug, output_dir=None):
    project_dir = Path(project_dir).expanduser().resolve()
    skill_dir = project_dir / "platform" / "skills" / slug
    if not skill_dir.exists():
        raise RuntimeError(f"Skill {slug} does not exist at {skill_dir}.")

    output_root = Path(output_dir).expanduser().resolve() if output_dir else dist_root(project_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    archive_base = output_root / slug
    archive_path = shutil.make_archive(str(archive_base), "zip", root_dir=skill_dir.parent, base_dir=skill_dir.name)

    # Verify archive contents are readable and include SKILL.md.
    with zipfile.ZipFile(archive_path) as bundle:
        expected = f"{slug}/SKILL.md"
        if expected not in bundle.namelist():
            raise RuntimeError("Packaged archive is missing SKILL.md.")

    return {
        "skill": slug,
        "archivePath": archive_path,
        "archiveSize": Path(archive_path).stat().st_size,
        "updatedAt": now_iso(),
    }


def publish_skill(project_dir, openclaw_dir, slug):
    project_dir = Path(project_dir).expanduser().resolve()
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    skill_dir = project_dir / "platform" / "skills" / slug
    if not skill_dir.exists():
        raise RuntimeError(f"Skill {slug} does not exist at {skill_dir}.")

    target_root = openclaw_dir / "skills"
    target_root.mkdir(parents=True, exist_ok=True)
    target_dir = target_root / slug
    if target_dir.exists():
        shutil.rmtree(target_dir)
    shutil.copytree(skill_dir, target_dir)

    skill_md = target_dir / "SKILL.md"
    if not skill_md.exists():
        raise RuntimeError("Published skill is missing SKILL.md after copy.")

    return {
        "skill": slug,
        "targetPath": str(target_dir),
        "updatedAt": now_iso(),
    }


def select_skill(payload, slug):
    if slug:
        for skill in payload["skills"]:
            if skill["slug"] == slug:
                return skill
        raise RuntimeError(f"Skill {slug} was not found.")
    return payload


def list_command(args):
    payload = scan_skills(args.project_dir, args.openclaw_dir)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def validate_command(args):
    payload = scan_skills(args.project_dir, args.openclaw_dir)
    result = select_skill(payload, args.skill)
    if args.skill and isinstance(result, dict):
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def scaffold_command(args):
    payload = create_skill_scaffold(
        project_dir=args.project_dir,
        slug=args.slug,
        title=args.title,
        description=args.description,
        trigger_phrase=args.trigger_phrase,
        category=args.category,
        include_scripts=args.include_scripts,
        include_references=args.include_references,
        include_assets=args.include_assets,
        author=args.author,
        version=args.version,
        mcp_server=args.mcp_server,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def package_command(args):
    payload = package_skill(args.project_dir, args.skill, output_dir=args.output_dir)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def publish_command(args):
    payload = publish_skill(args.project_dir, args.openclaw_dir, args.skill)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def install_command(args):
    payload = install_skill(args.project_dir, args.openclaw_dir, args.skill)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def uninstall_command(args):
    payload = uninstall_skill(args.openclaw_dir, args.skill)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def build_parser():
    parser = argparse.ArgumentParser(description="Manage Claude-style skills for OpenClaw Team.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List and validate local skills.")
    list_parser.add_argument("--project-dir", required=True)
    list_parser.add_argument("--openclaw-dir", default="")
    list_parser.set_defaults(func=list_command)

    validate_parser = subparsers.add_parser("validate", help="Validate all skills or a single skill.")
    validate_parser.add_argument("--project-dir", required=True)
    validate_parser.add_argument("--openclaw-dir", default="")
    validate_parser.add_argument("--skill", default="")
    validate_parser.set_defaults(func=validate_command)

    scaffold_parser = subparsers.add_parser("scaffold", help="Create a new skill scaffold.")
    scaffold_parser.add_argument("--project-dir", required=True)
    scaffold_parser.add_argument("--slug", required=True)
    scaffold_parser.add_argument("--title", required=True)
    scaffold_parser.add_argument("--description", required=True)
    scaffold_parser.add_argument("--trigger-phrase", default="")
    scaffold_parser.add_argument("--category", choices=sorted(VALID_CATEGORIES), default="workflow-automation")
    scaffold_parser.add_argument("--include-scripts", action="store_true")
    scaffold_parser.add_argument("--include-references", action="store_true")
    scaffold_parser.add_argument("--include-assets", action="store_true")
    scaffold_parser.add_argument("--author", default="")
    scaffold_parser.add_argument("--version", default="1.0.0")
    scaffold_parser.add_argument("--mcp-server", default="")
    scaffold_parser.set_defaults(func=scaffold_command)

    package_parser = subparsers.add_parser("package", help="Package a skill as a zip archive.")
    package_parser.add_argument("--project-dir", required=True)
    package_parser.add_argument("--skill", required=True)
    package_parser.add_argument("--output-dir", default="")
    package_parser.set_defaults(func=package_command)

    publish_parser = subparsers.add_parser("publish", help="Publish a skill into the local OpenClaw managed skills directory.")
    publish_parser.add_argument("--project-dir", required=True)
    publish_parser.add_argument("--openclaw-dir", required=True)
    publish_parser.add_argument("--skill", required=True)
    publish_parser.set_defaults(func=publish_command)

    install_parser = subparsers.add_parser("install", help="Install a project or vendored starter skill into OpenClaw managed skills.")
    install_parser.add_argument("--project-dir", required=True)
    install_parser.add_argument("--openclaw-dir", required=True)
    install_parser.add_argument("--skill", required=True)
    install_parser.set_defaults(func=install_command)

    uninstall_parser = subparsers.add_parser("uninstall", help="Remove a managed skill from OpenClaw.")
    uninstall_parser.add_argument("--openclaw-dir", required=True)
    uninstall_parser.add_argument("--skill", required=True)
    uninstall_parser.set_defaults(func=uninstall_command)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except RuntimeError as error:
        print(json.dumps({"ok": False, "error": str(error)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
