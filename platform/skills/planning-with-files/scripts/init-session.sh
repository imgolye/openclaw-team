#!/bin/bash
set -euo pipefail

TARGET_DIR="${1:-.}"
PROJECT_NAME="${2:-planning-session}"
DATE="$(date +%Y-%m-%d)"

mkdir -p "$TARGET_DIR"
cd "$TARGET_DIR"

if [ ! -f "task_plan.md" ]; then
  cat > task_plan.md <<EOF
# Task Plan: $PROJECT_NAME

## Goal
[One sentence describing the end state]

## Current Phase
Phase 1

## Phases

### Phase 1: Requirements & Discovery
- [ ] Understand intent and constraints
- [ ] Capture findings in findings.md
- **Status:** in_progress

### Phase 2: Planning & Structure
- [ ] Define the approach
- [ ] Confirm workflow and routing choices
- **Status:** pending

### Phase 3: Execution
- [ ] Execute the work
- [ ] Update progress.md as milestones land
- **Status:** pending

### Phase 4: Verification
- [ ] Verify outputs and linked assets
- [ ] Record checks and failures
- **Status:** pending

### Phase 5: Delivery
- [ ] Wrap up deliverables
- [ ] Capture follow-up actions
- **Status:** pending

## Decisions Made
| Decision | Rationale |
|----------|-----------|

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
EOF
fi

if [ ! -f "findings.md" ]; then
  cat > findings.md <<'EOF'
# Findings & Decisions

## Requirements
-

## Research Findings
-

## Technical Decisions
| Decision | Rationale |
|----------|-----------|

## Issues Encountered
| Issue | Resolution |
|-------|------------|

## Resources
-
EOF
fi

if [ ! -f "progress.md" ]; then
  cat > progress.md <<EOF
# Progress Log

## Session: $DATE

### Current Status
- **Phase:** 1 - Requirements & Discovery
- **Started:** $DATE

### Actions Taken
-

### Test Results
| Test | Expected | Actual | Status |
|------|----------|--------|--------|

### Errors
| Error | Resolution |
|-------|------------|
EOF
fi

echo "[planning-with-files] Planning bundle ready at: $(pwd)"
