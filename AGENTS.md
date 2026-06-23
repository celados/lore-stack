# projects/lore-stack/

The self-hosted **Lore** deployment that backs this workspace: a custom
`loreserver` binary with a **Postgres + Cloudflare R2** storage backend, plus its
config and deploy recipes. This is the storage substrate the rest of `~/workspace`
sits on.

- Authoritative design: `docs/design.md` (OKF).
- Lore is **vendored/overlaid, not forked wholesale** — see the design doc's
  *Plugin build model*. Authored plugin source lives in `plugins/`; a build recipe
  overlays it onto a pinned Lore checkout and builds `loreserver`.
- Pinned upstream: **Lore 0.8.3** (matches the binaries installed in `~/.local/bin`).
- **Clean-break on upgrades:** re-overlay onto the new tag, don't carry shims.
