#!/usr/bin/env bash
# Build a custom loreserver by overlaying our Postgres + R2 storage plugin onto a
# pinned upstream Lore checkout. See ../docs/design.md ("Plugin build model").
#
# Phases (so CI can insert rust-cache between fetch and build):
#   build.sh fetch   # clone pinned lore + overlay our plugin
#   build.sh build   # cargo build (assumes fetched)
#   build.sh         # both (local use)
#
# Until overlay/pg.rs and the lore-pg rewrite land, this builds a STOCK loreserver
# from the pinned source — proving the fetch+build pipeline (design Phase 2).
set -euo pipefail

LORE_TAG="${LORE_TAG:-v0.8.3}"                 # pin to the installed binaries
LORE_REPO="${LORE_REPO:-https://github.com/EpicGames/lore.git}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"   # server runs on Linux only

ROOT="$(cd "$(dirname "$0")/.." && pwd)"       # projects/lore-stack
BUILD="${BUILD_DIR:-$ROOT/.build}"
SRC="$BUILD/lore"

do_fetch() {
  echo ">> fetching lore @ $LORE_TAG"
  rm -rf "$SRC"; mkdir -p "$BUILD"
  git clone --depth 1 --branch "$LORE_TAG" "$LORE_REPO" "$SRC"

  # Upstream hardcodes the aarch64 CPU to neoverse-512tvb (Graviton3+, armv8.4)
  # in TWO kinds of places: .cargo/config.toml rustflags AND lore-base/build.rs's
  # cc flag for rpmalloc — C allocator code that runs before main(), so a wrong
  # -mcpu SIGILLs (stlur, armv8.4 LRCPC2) at startup with zero logs on any
  # armv8.2 host (Neoverse-N1/Ampere/Graviton2; bit us on Hetzner CAX). Sweep
  # EVERY occurrence in the tree, not just the top-level cargo config.
  ARM64_TARGET_CPU="${ARM64_TARGET_CPU:-generic}"
  # NB: options BEFORE the directory operand — BSD grep (macOS) silently treats
  # trailing options as filenames, turning the sweep into a no-op.
  grep -rl --include="*.toml" --include="*.rs" --exclude-dir=.git \
    --exclude-dir=target "neoverse-512tvb" "$SRC" 2>/dev/null | while IFS= read -r f; do
    sed -i.bak "s/neoverse-512tvb/$ARM64_TARGET_CPU/g" "$f" && rm -f "$f.bak"
    echo ">> arm64 cpu repointed to '$ARM64_TARGET_CPU' in ${f#"$SRC"/}"
  done

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
    echo ">> overlay/pg.rs absent — STOCK loreserver (pipeline proof only)"
  fi
}

do_build() {
  echo ">> building loreserver ($TARGET)"
  cd "$SRC"
  rustup target add "$TARGET" 2>/dev/null || true
  cargo build --release -p lore-server --bin loreserver --target "$TARGET"
  strip "target/$TARGET/release/loreserver" 2>/dev/null || true   # shrink the distributed binary (native runner)
  mkdir -p "$ROOT/dist"
  cp "target/$TARGET/release/loreserver" "$ROOT/dist/loreserver"
  echo ">> built: $ROOT/dist/loreserver"
  "$ROOT/dist/loreserver" --version || true
}

case "${1:-all}" in
  fetch) do_fetch ;;
  build) do_build ;;
  all)   do_fetch; do_build ;;
  *) echo "usage: build.sh [fetch|build]"; exit 2 ;;
esac
