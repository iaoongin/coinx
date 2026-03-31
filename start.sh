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

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python not found. Please install Python 3." >&2
  exit 1
fi

"$PYTHON_BIN" "$SCRIPT_DIR/scripts/start_app.py" "$ACTION"
