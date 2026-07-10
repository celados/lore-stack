#!/usr/bin/env bash
# Bring up the full self-hosted Lore stack locally (Postgres + MinIO + loreserver).
# Downloads the latest release binary for this host's arch, builds the image, starts everything.
#
#   ./scripts/compose-up.sh        # foreground
#   ./scripts/compose-up.sh -d     # detached
set -euo pipefail
cd "$(dirname "$0")/.."

case "$(uname -m)" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64)  ARCH=amd64 ;;
  *) echo "unsupported arch: $(uname -m)"; exit 1 ;;
esac

mkdir -p dist
if [ ! -f "dist/loreserver-$ARCH" ]; then
  echo ">> downloading latest loreserver-$ARCH from GitHub Releases"
  curl -fsSL "https://github.com/celados/lore-stack/releases/latest/download/loreserver-$ARCH" -o "dist/loreserver-$ARCH"
fi

docker compose up --build "$@"
