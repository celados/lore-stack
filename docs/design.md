---
type: Design
title: lore-stack — self-hosted Lore for ~/workspace (R2 + Postgres)
status: draft
version: 0.1
timestamp: 2026-06-23T00:00:00Z
---

# lore-stack

The storage substrate for `~/workspace`: a self-hosted Lore server whose durable
backend is **Cloudflare R2** (blob bytes) + **Postgres** (everything that needs a
key-value store with compare-and-swap), with **no AWS dependency**.

## What is installed today

- `lore 0.8.3+201` and `loreserver 0.8.3` in `~/.local/bin` (from the official
  GitHub release, platform `aarch64-apple-darwin`).
- The stock `loreserver` runs **zero-config in `local` mode** (immutable + mutable
  + lock stores all on local disk; `notification.mode = local` = in-process
  broadcast). Good enough for local development right now — **no R2/PG needed to
  start building conventions and actions.**

## Decisive constraint: the plugin build model

Lore's cloud backends are **plugins compiled into the server binary, not loaded at
runtime**. `lore-server/build.rs` auto-generates `register_all_plugins()` by
scanning `lore-server/src/plugins/*.rs` (`cargo:rerun-if-changed=src/plugins`), and
each plugin factory implements traits from `crate::plugins::traits` — i.e. the
lore-server crate's *internal* traits. The stock `loreserver` registers **no**
plugins; selecting `mode = "aws"` etc. fails at startup with `PluginNotFound`.

**Implication:** an R2/PG backend cannot be an out-of-tree crate. It must be a
`.rs` file inside `lore-server/src/plugins/`, built into a custom `loreserver`.

**Our approach — overlay, not hard fork.** This project holds only *our* authored
plugin source plus a build recipe. The recipe:

1. checks out Lore at a **pinned tag** (target: the 0.8.3 release matching our
   binaries — exact tag string TBD),
2. copies `plugins/*.rs` into `lore-server/src/plugins/`,
3. wires the extra crate deps (Postgres + S3 client) into the build,
4. `cargo build -p lore-server` → our `loreserver`.

On upstream upgrades we **re-overlay onto the new tag and clean-break** — no shims.

## Target backend map

| Stores what | Backend | Work |
| --- | --- | --- |
| immutable fragment **payload bytes** | **R2** | reuse lore-aws's S3 client via `s3_endpoint_url` + path-style |
| immutable **index / metadata** | **Postgres** | port lore-aws's DynamoDB index to PG |
| mutable store (branch pointers, **CAS**) | **Postgres** | `MutableStore` trait: `load/store/compare_and_swap/list/flush`; CAS = conditional write |
| lock store | **Postgres** | `LockStore` trait: `lock_resources/query_locks/check_locks_status/unlock_resources` |

Reference implementation to port from: the `lore-aws` crate (keep its S3 half →
point at R2; rewrite its DynamoDB half → Postgres). Footprint: **one managed
Postgres + one R2 bucket**, zero AWS.

## Planned project layout

```
projects/lore-stack/
  AGENTS.md
  docs/design.md          # this doc
  plugins/                # authored plugin source (overlaid into lore-server/src/plugins/)
  config/                 # loreserver TOML (local dev; cloud R2+PG)
  scripts/build.sh        # the overlay + build recipe
  .action/                # build / deploy actions
```

(Only `AGENTS.md` + `docs/` exist so far; the rest is created when we write code.)

## Open questions to resolve before coding

1. **Pin the exact upstream tag** for 0.8.3 and decide vendoring mechanism
   (git submodule vs shallow fetch in the build recipe).
2. **Plugin config schema** — the `[plugins.<name>]` keys our PG/R2 plugin reads
   (the plugin owns its own config parsing).
3. **Where PG and R2 live** — managed Postgres (Neon/Supabase) + R2 bucket; creds
   handling (the aws plugin pulls creds from the SDK default chain, not config).
4. **Auth** — JWKS endpoint (OIDC provider) vs mTLS for the public QUIC/gRPC.
5. **Does `local` mode unblock all early work?** (Almost certainly yes — build the
   R2/PG plugin in parallel, not on the critical path to conventions/actions.)

## Build phases (dependency order)

1. Confirm `local`-mode server runs end-to-end (repo create / add / commit / push)
   — gives a working Lore to develop conventions against now.
2. Stand up the overlay build recipe against the pinned tag, producing a stock
   `loreserver` from source (no custom plugin yet) — proves the build path.
3. Write the Postgres `MutableStore` + `LockStore` plugin; test CAS/locking.
4. Write the R2 + Postgres `ImmutableStore` plugin (S3→R2 + DynamoDB→PG index).
5. Cloud deploy + auth; point a client at it; wire the notification-stream bridge.

## Verified findings (2026-06-23, against the 0.8.3 source)

- **Registration model confirmed.** `lore-aws` is a pure store-impl crate (deps:
  lore-storage/lore-revision/lore-proto; **no lore-server dep**) implementing the
  public store traits. The factory glue lives in `lore-server/src/plugins/aws.rs`
  (**715 lines** — implements the internal `*PluginFactory` traits, parses config,
  calls `register_*_plugin`). So: copy `lore-aws` → our `lore-pg`; adapt `aws.rs`
  → `overlay/pg.rs`. Vendored as `lore-pg/` (see its `REWRITE.md`).

- **Effort corrected: ~60% rewrite, not 10%.** The S3→R2 side is genuinely small
  (~2000 of 8955 lines kept). The DynamoDB→Postgres side is ~6000 lines re-authored
  (lock_store 1989 + mutable_store 986 + dynamodb 914 + immutable_store's Dynamo
  index ~2300). Easier than the original (PG has real txns/locks) but a real
  implementation project.

- **R2 compat is small but not zero.** S3 ops used are all R2-supported. Two fixes:
  (1) `list_versions`/ListObjectVersions is unsupported by R2 — avoid/stub it;
  (2) set `request_checksum_calculation = WhenRequired` (aws-sdk-s3 default
  flexible checksums can trip R2).

- **Build shape confirmed.** `lore-pg` uses `{ workspace = true }` deps → it must
  build **as a member of the fetched Lore workspace**. `scripts/build.sh` is the
  overlay recipe; `.github/workflows/release.yml` runs it on `ubuntu-latest`
  (Linux-only server, single target). Client stays on official prebuilt `lore`.

- **Current milestone (Phase 2):** `build.sh` builds a **stock** loreserver from
  the pinned tag until `overlay/pg.rs` exists — this proves the fetch+build+release
  pipeline. The lore-pg rewrite + `pg.rs` glue are Phase 3–4.
