---
type: Review
title: Codex review of lore-pg (Postgres + R2 storage)
resource: /projects/lore-stack/docs/review/review-request.md
verdict: request-changes
timestamp: 2026-06-23T15:56:02Z
---

# Codex review of lore-pg

## Findings

### high — `lock_resources` can deadlock on overlapping multi-lock batches

[`lore-pg/src/store/pg_lock_store.rs:180`](/Users/dio/workspace/projects/lore-stack/lore-pg/src/store/pg_lock_store.rs:180)

`lock_resources` iterates the caller-provided `resources` order inside one Postgres transaction and inserts each `(repository, branch, hash)` row one at a time. Unlike `unlock_resources`, which sorts and dedups before deleting, the acquire path does not canonicalize the lock order.

This is a real concurrency bug under Postgres. If owner A calls `lock_resources([r1, r2])` while owner B calls `lock_resources([r2, r1])`, A can insert `r1` and wait on B's uncommitted `r2`, while B inserted `r2` and waits on A's uncommitted `r1`. Postgres will abort one transaction with a deadlock error. The implementation maps that to `LockError::internal` at the insert site, not to the contract's different-owner `LockNotOwned` conflict result, and the failed owner cannot distinguish a normal lock conflict from storage corruption.

Concrete fix: normalize the acquire set the same way the unlock path does before starting inserts: clone, sort, and dedup `resources`, then iterate that canonical order. Keep returning only newly inserted locks. Add a two-transaction regression test that acquires the same two resources in opposite orders and asserts one succeeds while the other returns a lock conflict, never an internal deadlock error.

### low — PG `force_write` exists in settings but cannot be enabled through plugin config

[`overlay/pg.rs:40`](/Users/dio/workspace/projects/lore-stack/overlay/pg.rs:40)

`PgImmutableStoreSettings` defines `force_write`, stores it on `PgImmutableStore`, and uses it to bypass the existing-fragment query/collision path and to read migration-era payloads with an extra `Fragment` prefix. But `PgImmutableStorePluginConfig` has no `force_write` field, and `PgImmutableStore::from_config` does not accept or pass one into settings. Because the shared `[plugins.pg]` table intentionally ignores unknown fields, an operator adding `force_write = true` will get a clean startup with force mode still disabled.

This is a real config correctness issue when using the documented migration aid: the code advertises a mode that cannot be reached through the only production factory in scope. It also diverges from the vendored AWS plugin, whose immutable config exposes `force_write` and passes it into store settings.

Concrete fix: either remove `force_write` from PG settings and the PG read/write branches if PG does not support that migration mode, or add `#[serde(default)] pub force_write: bool` to `PgImmutableStorePluginConfig`, pass it through `PgImmutableStore::from_config`, and add a config deserialization test that proves the value takes effect. Given this is a small self-hosted plugin, avoid a silent compatibility shim; make the config contract explicit.

## Clean Categories

- MutableStore CAS: the current CTE forms are single-statement operations and match the requested witness behavior for insert-if-absent, update-if-matches, mismatch, and absent-row cases.
- MutableStore list/load/store: typed-key rewriting, zero-value delete, `Untyped` empty list, and zero-value filtering match the requested contract.
- Lock query/status/unlock: the nine query variants bind request values as parameters; `check_locks_status` derives parallel arrays from one deduped vector; validate-user and no-validate unlock behavior matches the requested error/idempotency contract.
- ImmutableStore R2 path: the PG/R2 implementation does not call `ListObjectVersions`; `delete_payload` uses plain `delete_object`, and the real `from_config` S3 client sets `request_checksum_calculation(WhenRequired)`.
- ImmutableStore PG index write: metadata upsert and fragment association are committed in one transaction after the R2 write, so there is no metadata-only or association-only PG index window.
- SQL values: request-derived repository, branch, hash, context, owner, description, and payload metadata are bound through `$1..` parameters. The only formatted SQL pieces are table identifiers sourced from server config.
- Credential logging: the PG factories pass DSNs into pool config but do not log the DSN; startup logs include bucket/table names only.
- Runtime panics: I did not find runtime-reachable `unwrap`/`expect`/`panic!` in the PG implementation paths reviewed. The remaining unwraps are either guarded by prior length checks or in tests/vendored AWS code.

## Verification

Ran `cargo test -p lore-pg --lib` in `.build/lore`: 71 passed, 0 failed, 26 ignored. The ignored tests include the live Postgres/MinIO PG integration tests, so this review is primarily source-backed plus non-live unit coverage.

## Verdict

Must fix the lock acquisition ordering before release; the `force_write` config gap can be fixed or intentionally removed before depending on migration mode.
