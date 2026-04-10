#!/usr/bin/env python3
"""Import local Codex skills into the product-owned skills library."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default="~/.codex/skills",
        help="Source Codex skills directory. Defaults to ~/.codex/skills.",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="Target product skills directory. Defaults to <repo>/skills.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace existing target skill directories.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[3]
    source_root = Path(args.source).expanduser().resolve()
    target_root = Path(args.target).expanduser().resolve() if args.target else repo_root / "platform" / "skills"

    if not source_root.exists():
        raise SystemExit(f"source directory does not exist: {source_root}")

    target_root.mkdir(parents=True, exist_ok=True)
    imported = []
    skipped = []

    for skill_dir in sorted(source_root.iterdir()):
        if not skill_dir.is_dir() or skill_dir.name.startswith("."):
            continue
        if not (skill_dir / "SKILL.md").exists():
            continue
        target_dir = target_root / skill_dir.name
        if target_dir.exists():
            if not args.force:
                skipped.append({"skill": skill_dir.name, "reason": "exists", "path": str(target_dir)})
                continue
            shutil.rmtree(target_dir)
        shutil.copytree(skill_dir, target_dir)
        imported.append({"skill": skill_dir.name, "path": str(target_dir)})

    print(
        json.dumps(
            {
                "ok": True,
                "source": str(source_root),
                "target": str(target_root),
                "imported": imported,
                "importedCount": len(imported),
                "skipped": skipped,
                "skippedCount": len(skipped),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
