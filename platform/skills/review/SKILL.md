---
name: review
description: Pre-landing review mode that inspects diffs for trust boundaries, production risks, and structural issues before merge or release. Use when the user asks to review a diff or change set for production risk before merge or release.
compatibility: Claude Code, Claude.ai, and API environments that support local files and optional helper scripts.
metadata:
  author: OpenClaw Team
  version: 1.0.0
  upstream-repo: garrytan/gstack
  upstream-skill: review
  import-mode: managed-sync
---
# Pre-Landing Review

## Instructions
- Restate the user's target outcome in one sentence before acting.
- Operate in `eng-review` mode and keep the work grounded in the `review` stage.
- Prefer the `run` entry pattern when this skill needs to hand off into OpenClaw Team.
- Produce outputs in the style of: checklist, report.
- Call out runtime needs early if the flow depends on: git.
- Leave explicit handoff artifacts when relevant: findings.

## Workflow
### Step 1: Frame the task
- Confirm the decision, artifact, or validation the user actually needs.
- Identify the immediate risk if this step is skipped.

### Step 2: Run the specialist loop
- Focus on the job this skill is meant to do, not the entire project lifecycle.
- Keep outputs structured so they can be attached to a Run, Chat thread, or Workflow Pack.
- Prefer evidence, concrete findings, and explicit next steps over vague advice.

### Step 3: Hand off cleanly
- Summarize what is now settled.
- Name the next recommended skill or workflow stage.
- List the artifact or checkpoint the next role should consume.

## Examples
- User says: "Help me review a diff or change set for production risk before merge or release."
  Result: Claude uses the Pre-Landing Review workflow, produces a structured output, and hands off the next step clearly.

## Troubleshooting
- If required runtime access is missing, explain the smallest unblock clearly instead of guessing.
- If the request drifts into another role mode, stop and hand off to the right skill or workflow pack.
- If evidence is weak, ask for the minimum additional context needed to continue reliably.
