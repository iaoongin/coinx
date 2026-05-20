#!/usr/bin/env bash

# CoinX Management Script
# Usage: ./start.sh [action]
# Actions:
#   (empty)  : Run in foreground (default)
#   start    : Run in background
#   stop     : Stop the application
#   restart  : Restart the application
#   status   : Check status

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION="${1:-run}"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

if [[ -x "$VENV_PYTHON" ]]; then
  PYTHON_BIN="$VENV_PYTHON"
  echo "Using project virtualenv: $PYTHON_BIN"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
  echo "Project virtualenv not found, falling back to system python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
  echo "Project virtualenv not found, falling back to system python"
else
  cat >&2 <<EOF
Python not found.

Recommended setup:
  python3.10 -m venv .venv
  ./.venv/bin/pip install -r requirements.txt
EOF
  exit 1
fi

"$PYTHON_BIN" "$SCRIPT_DIR/scripts/start_app.py" "$ACTION"
