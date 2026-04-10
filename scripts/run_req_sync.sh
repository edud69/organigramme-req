#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_DIR/.env.local"
LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE" >&2
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

export AUTO_SYNC_ENABLED=0
export FLASK_DEBUG=0

cd "$PROJECT_DIR"
"$PROJECT_DIR/.venv/bin/python" app.py sync >> "$LOG_DIR/req-sync.log" 2>&1
