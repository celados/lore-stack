#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE="${LORE_DEPLOY_HOST:-oh}"
REMOTE_DIR="${LORE_DEPLOY_DIR:-/home/oh/services/lore-stack}"
ENV_FILE="$ROOT/deploy/oh/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing $ENV_FILE; render deploy/oh/.env.tpl first" >&2
  exit 1
fi
docker compose --env-file "$ENV_FILE" -f "$ROOT/docker-compose.yml" config --quiet

ssh "$REMOTE" "mkdir -p '$REMOTE_DIR/config' '$REMOTE_DIR/deploy/oh' '$REMOTE_DIR/docker' '$REMOTE_DIR/lore-pg' '$REMOTE_DIR/overlay' '$REMOTE_DIR/scripts'"
COPYFILE_DISABLE=1 tar --no-xattrs -C "$ROOT" -czf - \
  Dockerfile \
  docker-compose.yml \
  config/compose.toml \
  docker \
  lore-pg \
  overlay \
  scripts/build.sh \
  scripts/build-linux-amd64.sh \
  | ssh "$REMOTE" "tar -xzf - -C '$REMOTE_DIR'"
scp -q "$ENV_FILE" "$REMOTE:$REMOTE_DIR/deploy/oh/.env"

ssh "$REMOTE" "chmod 600 '$REMOTE_DIR/deploy/oh/.env' && chmod 755 '$REMOTE_DIR/scripts/build.sh' '$REMOTE_DIR/scripts/build-linux-amd64.sh' && cd '$REMOTE_DIR' && ./scripts/build-linux-amd64.sh && docker compose --env-file deploy/oh/.env pull postgres silo createbucket && docker compose --env-file deploy/oh/.env up -d --build && curl --fail --silent --show-error --retry 30 --retry-delay 1 --retry-connrefused --retry-all-errors http://127.0.0.1:41339/health_check"
