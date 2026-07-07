#!/usr/bin/env bash
# Start the stock loreserver in local single-machine mode (no R2, no Postgres).
#
# Runtime state (certs, stores, log) lives under XDG dirs OUTSIDE the workspace
# — it is operator-local runtime data, not source-of-truth.
#
# Companion script to compose-up.sh (the Docker / PG+MinIO path).

set -euo pipefail

CONFIG_DIR="$(cd "$(dirname "$0")/../config" && pwd)"
STATE_DIR="${LORESERVER_STATE_DIR:-$HOME/.local/share/loreserver}"
LOG_DIR="${LORESERVER_LOG_DIR:-$HOME/.local/state/loreserver}"
CERT_DIR="$STATE_DIR/certs"
STORE_DIR="$STATE_DIR/store"
PID_FILE="$LOG_DIR/loreserver.pid"
LOG_FILE="$LOG_DIR/loreserver.log"

mkdir -p "$STORE_DIR" "$CERT_DIR" "$LOG_DIR"

# Generate a self-signed cert for localhost on first run.
if [[ ! -f "$CERT_DIR/cert.pem" || ! -f "$CERT_DIR/key.pem" ]]; then
  echo "generating self-signed cert at $CERT_DIR ..."
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$CERT_DIR/key.pem" \
    -out    "$CERT_DIR/cert.pem" \
    -days 365 -subj "/CN=localhost" \
    -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
    >/dev/null 2>&1
fi

# Refuse to double-start.
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "loreserver already running (pid $(cat "$PID_FILE")); log: $LOG_FILE" >&2
  exit 0
fi

# Inject absolute paths via env (overrides win over TOML — see lore-server-config ref).
export LORE__SERVER__QUIC__CERTIFICATE__CERT_FILE="$CERT_DIR/cert.pem"
export LORE__SERVER__QUIC__CERTIFICATE__PKEY_FILE="$CERT_DIR/key.pem"
export LORE__IMMUTABLE_STORE__LOCAL__PATH="$STORE_DIR"
export LORE__MUTABLE_STORE__LOCAL__PATH="$STORE_DIR"

echo "starting loreserver"
echo "  config: $CONFIG_DIR (local.toml)"
echo "  store : $STORE_DIR"
echo "  certs : $CERT_DIR"
echo "  log   : $LOG_FILE"
echo "  ports : 41337/tcp+udp (QUIC/gRPC), 41339/tcp (HTTP)"

nohup loreserver --config "$CONFIG_DIR" >>"$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

# Wait for /health_check to come up.
for i in {1..30}; do
  if curl -sf http://127.0.0.1:41339/health_check >/dev/null 2>&1; then
    echo "loreserver healthy (pid $(cat "$PID_FILE"))"
    exit 0
  fi
  sleep 0.2
done

echo "loreserver did not become healthy within 6s — tail of log:" >&2
tail -n 30 "$LOG_FILE" >&2
exit 1
