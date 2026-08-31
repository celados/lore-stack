---
type: Design
title: lore-stack self-hosted server build
description: Custom Lore server build with PostgreSQL metadata and core S3-compatible payload storage.
status: active
version: 0.5
generated: { by: codex/gpt-5, at: 2026-08-31T00:00:00Z }
---

# lore-stack

This repository owns the custom `loreserver` binary and the single-node Compose
deployment used by the self-hosted Lore stack. Credentials remain in
Vaultwarden and are rendered only on the operator and OH hosts.

## Supported baseline

- Upstream source: [`EpicGames/lore` tag `v0.9.0`](https://github.com/EpicGames/lore/releases/tag/v0.9.0).
- Upgrade policy: clean re-overlay onto the pinned tag; no compatibility shim for
  older Lore storage traits.
- `scripts/build.sh` always overlays `lore-pg` and `overlay/pg.rs`; a missing
  plugin is a build error rather than a silent stock-server fallback.
- Upstream's `v0.9.0` tag still declares Cargo package version
  `0.8.7-nightly`. The custom build injects `LORE_BUILD_VERSION_NAME=v0.9.0`,
  so server startup reports the complete identity `0.8.7-nightly+v0.9.0` even
  though Clap's short `--version` output shows only the package version.

## Storage boundary

| Data | Backend | Required semantics |
| --- | --- | --- |
| Immutable payload bytes | Core S3-compatible object store | `GetObject`, `PutObject`, `DeleteObject`, path-style endpoint support |
| Immutable association and metadata index | PostgreSQL | Partition-isolated exact matches and tombstone-safe obliteration |
| Mutable pointers | PostgreSQL | Atomic compare-and-swap |
| Distributed locks | PostgreSQL | Transactional ownership checks |

The OH deployment uses Silo's immutable
`RELEASE.2026-08-06T00-00-00Z` image. It remains a deployment choice behind the
core S3 boundary; the storage plugin does not depend on Silo-specific APIs.

## Plugin build model

Lore discovers store factories at compile time from
`lore-server/src/plugins/*.rs`; these are not runtime-loadable plugins.
`scripts/build.sh` therefore:

1. clones the pinned upstream tag;
2. overlays this repository's `lore-pg` crate and `pg.rs` factory;
3. wires the crate into the fetched Cargo workspace;
4. builds the complete `loreserver` binary.

The root Compose file assembles the release binary, PostgreSQL 17, and Silo.
PostgreSQL and Silo have no host ports and remain only on an internal backend
network. Lore also joins a narrow edge bridge so Docker can publish the
configured Tailscale address, while its HTTP health endpoint binds localhost.
The deployment uses named volumes so restarts and image upgrades do not replace
data.

`deploy/oh/.env.tpl` is the committed credential reference. The rendered `.env`
is mode `0600`, ignored by Git, and transferred to OH by `scripts/deploy-oh.sh`.
The script deliberately does not change Tailscale, Mihomo, DNS, or the HZ stack.
It synchronizes the authored overlay to OH and builds the Linux amd64 binary
there before starting Compose. The build containers use host networking only so
they can reach OH's existing loopback-only Mihomo proxy; no proxy listener or
route is changed.

## Lore 0.9 storage contract

The durable PG/S3 store isolates partitions and deliberately reports only exact
`MatchFull` associations. Reporting sibling-context `MatchPartition` without
per-address tombstones makes an obliterated address appear to exist when another
context still references the same hash. Lore permits stores to under-report, and
the upstream AWS store makes the same conservative choice.

`copy` still accepts a zero-context source and resolves any association in the
named source partition. This preserves the explicit copy contract without making
unsafe claims during `query`.

The active implementations are:

- `PgImmutableStore` for PostgreSQL index plus S3 payloads;
- `PgMutableStore` for PostgreSQL mutable values;
- `PgLockStore` for PostgreSQL locks.

The copied DynamoDB implementations and their SDK dependencies were removed in
the 0.9 upgrade because no runtime path used them.

## Verification gate

An upstream upgrade is complete only when all of the following pass:

1. the full custom `loreserver` compiles from the pinned tag;
2. Lore's immutable and mutable conformance batteries pass against disposable
   real PostgreSQL and S3-compatible services;
3. all PG lock and mutable integration tests pass;
4. the release binary starts with the PG plugin, creates all four tables, and
   returns HTTP 200 from `/health_check` with store health checking enabled.

The 0.9.0 upgrade and OH deployment passed this gate on 2026-08-31. The release
binary was built on OH, all 21 PG/S3 tests passed there, and both Lore 0.9.0 and
the installed 0.8.6 client completed create, commit, push, clone, and byte-level
comparison through the Tailscale address. A full PostgreSQL, Silo, and Lore
restart preserved the pushed revision. DNS cutover and HZ drain remain separate
operations.
