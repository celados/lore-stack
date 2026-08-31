# lore-pg

Lore storage implementations for the self-hosted server:

- immutable payloads in a core S3-compatible object store;
- immutable associations and metadata in PostgreSQL;
- mutable pointers and distributed locks in PostgreSQL.

The crate is overlaid into the pinned upstream Lore workspace by
`../scripts/build.sh`; it is not published or built independently. It deliberately
contains no DynamoDB implementation or dependency.

Live storage verification is opt-in because it requires disposable Postgres and
S3 services:

```sh
PG_TEST_DSN='...' \
S3_TEST_ENDPOINT='...' \
S3_TEST_BUCKET='...' \
cargo test -p lore-pg -- --ignored --test-threads=1
```

The ignored suite includes Lore 0.9's immutable and mutable conformance batteries.
