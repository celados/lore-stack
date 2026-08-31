#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROXY_URL="${LORE_BUILD_PROXY_URL:-http://127.0.0.1:7890}"
BUILDER="celados-lore-stack-builder:amd64"
HOST_UID="$(id -u)"
HOST_GID="$(id -g)"

mkdir -p "$ROOT/.build/amd64" "$ROOT/.build/cargo" "$ROOT/dist"

# OH's proxy listens on host loopback, so the Linux build containers must share
# the host network instead of changing Mihomo's listener or firewall policy.
docker build \
  --network host \
  --build-arg HTTP_PROXY="$PROXY_URL" \
  --build-arg HTTPS_PROXY="$PROXY_URL" \
  --file "$ROOT/docker/build.Dockerfile" \
  --tag "$BUILDER" \
  "$ROOT/docker"

docker run --rm \
  --network host \
  --user "$HOST_UID:$HOST_GID" \
  --env BUILD_DIR=/work/.build/amd64 \
  --env CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-16}" \
  --env CARGO_HOME=/work/.build/cargo \
  --env HTTP_PROXY="$PROXY_URL" \
  --env HTTPS_PROXY="$PROXY_URL" \
  --env HOME=/tmp \
  --env TARGET=x86_64-unknown-linux-gnu \
  --volume "$ROOT:/work" \
  --workdir /work \
  "$BUILDER" \
  bash scripts/build.sh

cp "$ROOT/dist/loreserver" "$ROOT/dist/loreserver-amd64"
chmod 755 "$ROOT/dist/loreserver-amd64"
file "$ROOT/dist/loreserver-amd64"
sha256sum "$ROOT/dist/loreserver-amd64"
