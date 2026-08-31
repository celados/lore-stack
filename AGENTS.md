# projects/lore-stack/

The custom `loreserver` build and single-node Compose deployment that back this
workspace. Its storage plugin uses **Postgres + any core S3-compatible object
store**.

- Authoritative design: `docs/design.md` (OKF).
- Lore is **vendored/overlaid, not forked wholesale** — see the design doc's
  *Plugin build model*. Authored storage code lives in `lore-pg/`, factory glue in
  `overlay/pg.rs`, and the build recipe overlays both onto a pinned Lore checkout.
- Pinned upstream: **Lore 0.9.0**.
- **Clean-break on upgrades:** re-overlay onto the new tag, don't carry shims.
- The root Compose file is the deployment contract for OH. PostgreSQL and Silo
  stay on its internal Docker network; only Lore binds the Tailscale address.
- `deploy/oh/.env.tpl` references Vaultwarden. Its rendered `.env` is runtime
  state and must never be committed.
- Linux amd64 release builds run natively on OH through
  `scripts/build-linux-amd64.sh`; do not make the Mac operator host the build
  machine for OH deployments.
- **CI build boundary:** Linux ARM64 and AMD64 binaries compile inside Docker on
  the celados self-hosted macOS ARM64 runner. Keep release publication and image
  assembly on GitHub-hosted runners; they are cheap glue jobs. The persistent
  `.build/` cache is bounded by the workflow and must never enter Docker's build
  context. CI starts OrbStack explicitly because runner availability does not
  imply that its Docker daemon is already running.
