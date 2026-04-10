#!/bin/bash
set -euo pipefail

PLAN_FILE="${1:-task_plan.md}"

if [ ! -f "$PLAN_FILE" ]; then
  echo "[planning-with-files] No task_plan.md found."
  exit 0
fi

TOTAL=$(grep -c "### Phase" "$PLAN_FILE" || true)
COMPLETE=$(grep -cF "**Status:** complete" "$PLAN_FILE" || true)
IN_PROGRESS=$(grep -cF "**Status:** in_progress" "$PLAN_FILE" || true)
PENDING=$(grep -cF "**Status:** pending" "$PLAN_FILE" || true)

: "${TOTAL:=0}"
: "${COMPLETE:=0}"
: "${IN_PROGRESS:=0}"
: "${PENDING:=0}"

if [ "$TOTAL" -gt 0 ] && [ "$COMPLETE" -eq "$TOTAL" ]; then
  echo "[planning-with-files] ALL PHASES COMPLETE ($COMPLETE/$TOTAL)"
else
  echo "[planning-with-files] Task in progress ($COMPLETE/$TOTAL phases complete)"
  if [ "$IN_PROGRESS" -gt 0 ]; then
    echo "[planning-with-files] $IN_PROGRESS phase(s) still in progress."
  fi
  if [ "$PENDING" -gt 0 ]; then
    echo "[planning-with-files] $PENDING phase(s) pending."
  fi
fi
exit 0
