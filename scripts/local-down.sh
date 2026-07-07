#!/usr/bin/env bash
# Stop the local-mode loreserver started by local-up.sh.
set -euo pipefail
PID_FILE="${LORESERVER_LOG_DIR:-$HOME/.local/state/loreserver}/loreserver.pid"
if [[ ! -f "$PID_FILE" ]]; then
  echo "no pid file at $PID_FILE — nothing to stop" >&2
  exit 0
fi
PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  for _ in {1..20}; do
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -0 "$PID" 2>/dev/null && kill -9 "$PID" || true
  echo "stopped loreserver (pid $PID)"
else
  echo "pid $PID not running (stale pid file)"
fi
rm -f "$PID_FILE"
