#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-/opt/gomining/multiplier_logger}"
SERVICE="${SERVICE:-mw-multi-sheet-logger.service}"

if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$SERVICE"; then
  echo "Refusing cleanup while $SERVICE is running. Stop it first." >&2
  exit 1
fi

cd "$ROOT"

rm -rf __pycache__ .pytest_cache .VSCodeCounter api_debug
find . -type d -name __pycache__ -prune -exec rm -rf {} +
find . -type f -name '*.pyc' -delete
rm -f sync.lock

echo "Runtime cleanup done in $ROOT"
