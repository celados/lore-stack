#!/usr/bin/env bash
# Build a custom loreserver by overlaying our Postgres + S3 storage plugin onto a
# pinned upstream Lore checkout. See ../docs/design.md ("Plugin build model").
#
# Phases (so CI can insert rust-cache between fetch and build):
#   build.sh fetch   # clone pinned lore + overlay our plugin
#   build.sh build   # cargo build (assumes fetched)
#   build.sh         # both (local use)
#
set -euo pipefail

LORE_TAG="${LORE_TAG:-v0.12.0}"                 # server/client release baseline
LORE_REPO="${LORE_REPO:-https://github.com/EpicGames/lore.git}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"   # server runs on Linux only

ROOT="$(cd "$(dirname "$0")/.." && pwd)"       # projects/lore-stack
BUILD="${BUILD_DIR:-$ROOT/.build}"
SRC="$BUILD/lore"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$BUILD/target}"

do_fetch() {
  echo ">> fetching lore @ $LORE_TAG"
  rm -rf "$SRC"; mkdir -p "$BUILD"
  git clone --depth 1 --branch "$LORE_TAG" "$LORE_REPO" "$SRC"

  if [ -f "$ROOT/overlay/pg.rs" ]; then
    echo ">> overlaying lore-pg crate + plugins/pg.rs + Cargo wiring"
    rm -rf "$SRC/lore-pg"; cp -R "$ROOT/lore-pg" "$SRC/lore-pg"
    cp "$ROOT/overlay/pg.rs" "$SRC/lore-server/src/plugins/pg.rs"   # build.rs auto-registers it
    # Wire lore-pg into the fetched workspace (idempotent; python3 is TOML-safe where sed isn't).
    python3 - "$SRC" <<'PY'
import sys, pathlib
src = pathlib.Path(sys.argv[1])
root = src / "Cargo.toml"
t = root.read_text()
if '"lore-pg"' not in t:
    t = t.replace("members = [", 'members = [\n    "lore-pg",', 1)
if "\nlore-pg = " not in t:
    t = t.replace("[workspace.dependencies]", '[workspace.dependencies]\nlore-pg = { path = "lore-pg" }', 1)
root.write_text(t)
srv = src / "lore-server" / "Cargo.toml"
s = srv.read_text()
if "lore-pg" not in s:
    s = s.replace("[dependencies]", "[dependencies]\nlore-pg = { workspace = true }", 1)
srv.write_text(s)
print("wired lore-pg into workspace members + dependencies")
PY
  else
    echo ">> overlay/pg.rs absent — refusing to build a server without the required plugin"
    exit 1
  fi
}

do_build() {
  echo ">> building loreserver ($TARGET)"
  cd "$SRC"
  rustup target add "$TARGET" 2>/dev/null || true
  cargo build --release -p lore-server --bin loreserver --target "$TARGET" \
    --target-dir "$CARGO_TARGET_DIR"
  strip "$CARGO_TARGET_DIR/$TARGET/release/loreserver" 2>/dev/null || true
  # The build name is no longer compiled in; lore-stamp writes it into the binary.
  cargo run --release -p lore-base --bin lore-stamp --target "$TARGET" \
    --target-dir "$CARGO_TARGET_DIR" -- --build "$LORE_TAG" \
    "$CARGO_TARGET_DIR/$TARGET/release/loreserver"
  mkdir -p "$ROOT/dist"
  cp "$CARGO_TARGET_DIR/$TARGET/release/loreserver" "$ROOT/dist/loreserver"
  echo ">> built: $ROOT/dist/loreserver"
  "$ROOT/dist/loreserver" --version || true
}

case "${1:-all}" in
  fetch) do_fetch ;;
  build) do_build ;;
  all)   do_fetch; do_build ;;
  *) echo "usage: build.sh [fetch|build]"; exit 2 ;;
esac
