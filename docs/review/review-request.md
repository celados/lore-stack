---
type: ReviewRequest
title: External review request — lore-pg Postgres + R2 storage plugin
description: Scope, contracts, and instructions for a full external code review of the PG/R2 storage backend.
status: open
reviewer: codex
timestamp: 2026-06-23T00:00:00Z
---

# Review request: `lore-pg` (Postgres + R2 storage plugin for Lore)

## What to do

Perform a **comprehensive correctness + security review** of the storage plugin
described below, then **write your findings to a new file**:
`docs/review/findings-codex.md` (OKF format — see *Output* at the end).

Be adversarial. The happy paths already pass integration tests; we want the
**subtle** bugs those miss. Do **not** fix code — report only.

## What this is

`lore-pg` is a self-hosted storage backend for **Lore** (Epic Games' open-source
content-addressed VCS, https://github.com/EpicGames/lore, pinned tag `v0.8.3`).
It implements Lore's three storage traits against **Postgres** (mutable, lock,
and the immutable index) + **Cloudflare R2 / any S3-compatible store** (immutable
fragment payloads). It was ported from the upstream AWS plugin (`lore-aws`), which
uses **DynamoDB + S3**. Those DynamoDB/S3 originals are vendored alongside as the
behavioral spec.

It is compiled into a custom `loreserver` via a thin plugin glue. End-to-end it
has been verified: a real `lore` CLI did `repository create → stage → commit →
push → history` against a `loreserver` running on this backend, and the data was
confirmed physically present in Postgres (fragment index + mutable revision
pointers) and MinIO (content-addressed payload objects).

## Files in scope

| File | Role |
|---|---|
| `lore-pg/src/store/pg_mutable_store.rs` | `MutableStore`: branch/ref pointers, **compare-and-swap** |
| `lore-pg/src/store/pg_lock_store.rs` | `LockStore`: transactional acquire/unlock, 9 query variants |
| `lore-pg/src/store/pg_immutable_store.rs` | `ImmutableStore`: **R2 payloads + PG index** (largest, ~1.6k LOC) |
| `overlay/pg.rs` | lore-server plugin glue: 3 factories + `register()` (config parsing, `block_in_place`+`block_on`) |

Reference (the DynamoDB originals these port from — the behavioral spec):
`lore-pg/src/store/{mutable_store,lock_store,immutable_store}.rs`.

## Contracts to check against (authoritative)

### MutableStore (`load`, `store`, `compare_and_swap`, `list`, `flush`)
- Key is a 32-byte `Hash` with **byte[0] overwritten by `key_type`**; row = `(repository, key, value)`.
- `store(value=0)` → delete the row; non-zero → upsert.
- `compare_and_swap(expected, value)` returns the **prior value (witness)**: success → `expected`;
  if `expected==0` it means *insert-if-absent* (conflict → return existing value); if `expected!=0`
  it means *update-if-matches* (mismatch → return current value; row absent → return `Hash::default()`).
  The witness MUST be consistent (this was recently changed to single-statement CTEs to be atomic).
- `list(Untyped)` → empty; else stream `(key, value)` for rows whose `key[0]==key_type` and value≠0.

### LockStore (`lock_resources`, `query_locks`, `check_locks_status`, `unlock_resources`)
- Lock identity = `(repository, branch, hash)`; row also holds `owner_id`, `description`, `locked_at` (ms).
- `lock_resources`: all-or-nothing; same-owner re-lock is **idempotent and excluded from the returned Vec**;
  different-owner conflict → roll back, return `LockNotOwned`.
- `unlock_resources(validate_user=true)`: wrong owner → `LockNotOwned`, absent → `LockNotFound`, all-or-nothing;
  `validate_user=false` → unconditional idempotent delete (never errors on absent).
- `query_locks`: 9 variants (Hash / HashRepository / HashRepositoryBranch / Owner / OwnerRepository /
  OwnerRepositoryBranch / Repository / RepositoryBranch / RepositoryBranchDescription) → parameterized WHERE.
- `check_locks_status`: dedup, return LockData only for currently-locked resources.

### ImmutableStore (content-addressed; key methods `put`, `get`, `exist`/`exist_batch`, `query`, `obliterate`, `fragment_count`, `flush`, `evict`, `compact`, `verify`)
- `put`: write payload bytes to R2 (key = content hash), then the PG index (metadata row + fragment
  association row). `get`: read payload from R2 + metadata from PG. Bytes must round-trip exactly.
- The storage protocol must stay **byte-for-byte compatible** with stock Lore clients.
- `evict`/`compact`/`compact_resume_at`/`compact_stop`/`verify`/`flush` are intentional **no-ops**
  (remote store; same as the AWS original) — not bugs.

## Already verified — focus elsewhere
- Integration tests pass against **real Postgres 17 + MinIO**: mutable 4/4, lock 13/13, immutable 7/7 (24/24).
- End-to-end `lore` CLI round-trip verified, data confirmed in PG + MinIO.
- A prior adversarial pass already **fixed** these (do not re-report unless still wrong):
  1. CAS made atomic via single-statement CTEs (was a read-after-write race under READ COMMITTED).
  2. `pg_immutable_store::write_payload` now wraps the two PG index writes (metadata + association) in one transaction.
  3. `pg_immutable_store::load_metadata` no longer misclassifies query/transport errors as `AddressNotFound`.
  4. `pg_lock_store::check_locks_status` builds the `unnest` parallel arrays from one ordered `Vec`, not two `HashSet` passes.

## What to hunt for
1. **Correctness vs the contracts above** and vs the vendored DynamoDB originals — every return value and
   error branch, especially CAS witness edge cases, lock conflict/ownership error mapping, and immutable
   `put`/`get`/`obliterate` partial-failure behavior (orphaned R2 objects vs index rows).
2. **Concurrency / transactions**: CAS and lock correctness under concurrent writers; isolation-level
   assumptions (default READ COMMITTED); lost-update / phantom risks; pool-connection error handling;
   detached `tokio::spawn` streams (error visibility, connection-hold-time / pool exhaustion).
3. **Security**: SQL injection (confirm `format!` is used ONLY for table names from trusted config, never
   request data; confirm ALL values are parameterized `$1..`); panics on malformed input (hash byte-length
   assumptions); credential leakage (DSN with password in `Debug`/tracing).
4. **R2/S3 specifics**: is `request_checksum_calculation = WhenRequired` applied on the real (non-test) S3
   client path? Is `ListObjectVersions` avoided everywhere? Any other S3 op R2 doesn't support?
5. **Resource/error hygiene**: swallowed `.await` errors, transactions not rolled back on early return,
   `unwrap()`/`expect()`/`panic!` on runtime-reachable paths, pool exhaustion.

## Reference material for deep checks
- Upstream Lore source (traits + DynamoDB originals): clone `https://github.com/EpicGames/lore` at tag `v0.8.3`.
  Trait defs: `lore-storage/src/{mutable_store,immutable_store}.rs`, `lore-revision/src/lock.rs`.
- This repo is the projection of `~/workspace/projects/lore-stack`; the canonical source lives there.

## Output

Write `docs/review/findings-codex.md` in **OKF format**:

```
---
type: Review
title: Codex review of lore-pg (Postgres + R2 storage)
resource: /projects/lore-stack/docs/review/review-request.md
verdict: approve | request-changes
timestamp: <ISO8601>
---
```

Then, per finding: **severity** (critical/high/medium/low), **file:line**, what's wrong,
**why it is a real bug** (not theoretical), and a concrete fix. State which whole
categories are clean. End with a one-line verdict: **safe to release as-is, or must-fix items?**
