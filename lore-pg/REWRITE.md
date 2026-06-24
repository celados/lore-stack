# lore-pg — rewrite map

This crate is **vendored from upstream `lore-aws` @ Lore 0.8.3** (MIT) as the
starting point. The job: keep the S3 side (re-point at R2), rewrite the DynamoDB
side as Postgres. Measured split below — this is **~60% rewrite, not a 10% diff**.

## Keep (S3 → R2) — small changes

| File | Lines | Change |
| --- | --- | --- |
| `src/s3.rs` | 271 | keep. Ops used (`head/get/put/delete/list_objects_v2/bucket_exists`) are all R2-supported. |
| `src/clients.rs` (S3 half) | ~160 | keep; `endpoint_url` + `force_path_style` already present → point at `https://<acct>.r2.cloudflarestorage.com`. |
| `src/store/immutable_store.rs` (S3 paths) | ~1500 of 4030 | keep the payload read/write paths. |

**R2 caveats to handle:**
1. **`list_versions` (ListObjectVersions) is NOT supported by R2.** It's defined in
   `s3.rs` and likely only used on obliterate/lifecycle paths — avoid or stub it.
2. **aws-sdk-s3 default flexible checksums** can trip R2 — set
   `request_checksum_calculation = WhenRequired` on the S3 client config.

## Rewrite (DynamoDB → Postgres) — the real work

| File | Lines | Becomes |
| --- | --- | --- |
| `src/store/lock_store.rs` | 1989 | `LockStore` (4 methods) on PG — `SELECT FOR UPDATE` / advisory locks. Should get **shorter**. |
| `src/store/mutable_store.rs` | 986 | `MutableStore` (5 methods); `compare_and_swap` = `INSERT … ON CONFLICT` / conditional `UPDATE`. |
| `src/dynamodb.rs` + `src/dynamodb/` | 914 | replace with a `pg` module (pool, query helpers). |
| `src/store/immutable_store.rs` (Dynamo index) | ~2300 of 4030 | `ImmutableStore` index ops (`exist/query/list/obliterate/compact/verify`) over PG tables instead of Dynamo. |

Treat the DynamoDB code as the **behavioral spec**, not a transcription target —
Postgres has real transactions/locks, so the re-implementation is simpler.

## Cargo.toml dep swaps

- Remove: `aws-sdk-dynamodb`, `serde_dynamo`.
- Add: `sqlx` (or `tokio-postgres` + `deadpool-postgres`).
- Keep: `aws-sdk-s3`, `aws-config`, `aws-smithy-*`.

## Build constraint

`lore-pg` uses `{ workspace = true }` deps → it must be built **as a member of the
fetched Lore workspace** (see `../scripts/build.sh`). Internal type names still say
`Aws*`; rename to `Pg*` during the rewrite.

## Glue (separate, lives in `../overlay/pg.rs`)

Adapted from upstream `lore-server/src/plugins/aws.rs` (**715 lines**, not a
one-liner): implements the lore-server-internal `*PluginFactory` traits, parses
`[plugins.pg]` config (R2 endpoint + PG connection), and calls
`register_immutable_store_plugin` / `register_mutable_store_plugin` /
`register_lock_store_plugin`. `build.rs` auto-discovers it once dropped in.
