# projects/lore-stack/

The self-hosted **Lore** deployment that backs this workspace: a custom
`loreserver` binary with a **Postgres + Cloudflare R2** storage backend, plus its
config and deploy recipes. This is the storage substrate the rest of `~/workspace`
sits on.

- Authoritative design: `docs/design.md` (OKF).
- Lore is **vendored/overlaid, not forked wholesale** — see the design doc's
  *Plugin build model*. Authored plugin source lives in `plugins/`; a build recipe
  overlays it onto a pinned Lore checkout and builds `loreserver`.
- Pinned upstream: **Lore 0.8.6**.
- **Clean-break on upgrades:** re-overlay onto the new tag, don't carry shims.
- **Production deployment** (compose, Dockerfile, TLS/cert automation, secrets
  templates) lives in `projects/berth/docker/lore/`, not here — this repo only
  produces the versioned `loreserver` release binary consumed from there.
- **CI build boundary:** Linux ARM64 and AMD64 binaries compile inside Docker on
  the celados self-hosted macOS ARM64 runner. Keep release publication and image
  assembly on GitHub-hosted runners; they are cheap glue jobs. The persistent
  `.build/` cache is bounded by the workflow and must never enter Docker's build
  context.
