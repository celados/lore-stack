// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use lore_base::error::LockNotFound;
use lore_base::error::LockNotOwned;
use lore_base::types::Hash;
use lore_base::types::LockData;
use lore_base::types::LockResource;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use serde::Deserialize;
use tracing::debug;
use tracing::warn;
use zerocopy::IntoBytes;

#[derive(Clone, Debug, Deserialize)]
pub struct PgLockStoreSettings {
    pub table_name: String,
}

impl PgLockStoreSettings {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

impl Default for PgLockStoreSettings {
    fn default() -> Self {
        Self::new("locks")
    }
}

pub struct PgLockStore {
    pool: Pool,
    table_name: Arc<str>,
}

impl PgLockStore {
    pub fn new(pool: Pool, settings: &PgLockStoreSettings) -> Self {
        Self {
            pool,
            table_name: Arc::from(settings.table_name.as_str()),
        }
    }

    /// Build a pool from a libpq DSN, create the table schema, and return a ready store.
    ///
    /// All deadpool imports stay inside lore-pg; callers (e.g. the server plugin glue)
    /// do not need to pull in those crates directly.
    pub async fn from_config(
        dsn: &str,
        table: &str,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        use deadpool_postgres::Config as DeadpoolConfig;
        use deadpool_postgres::Runtime;

        let mut cfg = DeadpoolConfig::new();
        cfg.url = Some(dsn.to_owned());
        // Always TLS-capable connector; DSN sslmode decides whether TLS is actually
        // negotiated (see crate::tls for why this must not be NoTls).
        let connector = crate::tls::make_connector()?;
        let pool = cfg.create_pool(Some(Runtime::Tokio1), connector)?;
        let settings = PgLockStoreSettings::new(table);
        let store = Arc::new(Self::new(pool, &settings));
        store.ensure_schema().await?;
        Ok(store)
    }

    /// Create the locks table and indexes if they do not already exist.
    ///
    /// Table name comes from trusted server config, so format! is safe — no user input reaches it.
    pub async fn ensure_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                repository  BYTEA  NOT NULL,
                branch      BYTEA  NOT NULL,
                hash        BYTEA  NOT NULL,
                owner_id    TEXT   NOT NULL,
                description TEXT   NOT NULL,
                locked_at   BIGINT NOT NULL,
                PRIMARY KEY (repository, branch, hash)
            );
            CREATE INDEX IF NOT EXISTS {table}_hash_idx  ON {table} (hash);
            CREATE INDEX IF NOT EXISTS {table}_owner_idx ON {table} (owner_id);
            CREATE INDEX IF NOT EXISTS {table}_repo_idx  ON {table} (repository);",
            table = self.table_name
        );
        client.batch_execute(&sql).await?;
        Ok(())
    }
}

/// Reconstruct a `Hash` (32 bytes) from a BYTEA column value.
fn hash_from_pg(bytes: &[u8]) -> Result<Hash, LockError> {
    let arr: [u8; 32] = bytes.try_into().map_err(|_| {
        LockError::internal(format!("pg: expected 32-byte hash, got {}", bytes.len()))
    })?;
    Ok(Hash::from(arr))
}

/// Reconstruct a `BranchId` (alias for `Context`, 16 bytes) from a BYTEA column value.
fn branch_from_pg(bytes: &[u8]) -> Result<BranchId, LockError> {
    let arr: [u8; 16] = bytes.try_into().map_err(|_| {
        LockError::internal(format!("pg: expected 16-byte branch, got {}", bytes.len()))
    })?;
    Ok(BranchId::from(arr))
}

/// Build a `LockData` from a row with columns: hash, branch, owner_id, description, locked_at.
/// Column index order must match the SELECT list.
fn lock_data_from_row(row: &tokio_postgres::Row) -> Result<LockData, LockError> {
    let hash_bytes: Vec<u8> = row.get(0);
    let branch_bytes: Vec<u8> = row.get(1);
    let owner_id: String = row.get(2);
    let description: String = row.get(3);
    let locked_at: i64 = row.get(4);

    let hash = hash_from_pg(&hash_bytes)?;
    let branch = branch_from_pg(&branch_bytes)?;

    Ok(LockData {
        resource: LockResource {
            branch,
            hash,
            description,
        },
        owner: owner_id,
        locked_at: locked_at as u64,
    })
}

#[async_trait]
impl LockStore for PgLockStore {
    /// Acquire locks for all requested resources in a single transaction.
    ///
    /// Idempotent for the same owner: if a resource is already locked by `owner_id`, it is
    /// silently skipped and excluded from the returned Vec. If any resource is locked by a
    /// *different* owner the whole transaction is rolled back and `LockNotOwned` is returned.
    async fn lock_resources(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        // Canonicalize the acquire order (sort + dedup) so two concurrent lockers
        // with overlapping resources in opposite order cannot deadlock in Postgres.
        let mut resources = resources.to_vec();
        resources.sort();
        resources.dedup();

        let mut client = self.pool.get().await.map_err(|e| {
            warn!("PgLockStore: pool checkout failed: {e}");
            LockError::internal(format!("pg pool error: {e}"))
        })?;

        let repo_bytes: Vec<u8> = repository.as_bytes().to_vec();
        // Shared timestamp for all locks in this batch (millis since epoch).
        let locked_at = chrono::Utc::now().timestamp_millis();

        let txn = client.transaction().await.map_err(|e| {
            warn!("PgLockStore: BEGIN failed: {e}");
            LockError::internal(format!("pg begin error: {e}"))
        })?;

        let insert_sql = format!(
            "INSERT INTO {table} (repository, branch, hash, owner_id, description, locked_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (repository, branch, hash) DO NOTHING",
            table = self.table_name
        );
        let select_owner_sql = format!(
            "SELECT owner_id FROM {table} WHERE repository = $1 AND branch = $2 AND hash = $3",
            table = self.table_name
        );

        let mut newly_locked: Vec<LockData> = Vec::with_capacity(resources.len());

        for resource in &resources {
            let branch_bytes: Vec<u8> = resource.branch.as_bytes().to_vec();
            let hash_bytes: Vec<u8> = resource.hash.as_bytes().to_vec();

            let rows_affected = txn
                .execute(
                    &insert_sql,
                    &[
                        &repo_bytes.as_slice(),
                        &branch_bytes.as_slice(),
                        &hash_bytes.as_slice(),
                        &owner_id,
                        &resource.description.as_str(),
                        &locked_at,
                    ],
                )
                .await
                .map_err(|e| {
                    warn!("PgLockStore: INSERT lock failed: {e}");
                    LockError::internal(format!("pg insert error: {e}"))
                })?;

            if rows_affected == 0 {
                // Row already existed — check if it belongs to us.
                let row = txn
                    .query_opt(
                        &select_owner_sql,
                        &[
                            &repo_bytes.as_slice(),
                            &branch_bytes.as_slice(),
                            &hash_bytes.as_slice(),
                        ],
                    )
                    .await
                    .map_err(|e| {
                        warn!("PgLockStore: SELECT owner failed: {e}");
                        LockError::internal(format!("pg select error: {e}"))
                    })?;

                match row {
                    Some(r) => {
                        let existing_owner: String = r.get(0);
                        if existing_owner == owner_id {
                            // Idempotent: same owner already holds this lock, skip it.
                            debug!(
                                "PgLockStore: lock already held by same owner, skipping: {:?}",
                                resource.description
                            );
                        } else {
                            // Conflict: roll back and report.
                            warn!(
                                "PgLockStore: lock held by different owner ({existing_owner} != {owner_id})"
                            );
                            txn.rollback().await.ok();
                            return Err(LockNotOwned.into());
                        }
                    }
                    None => {
                        // Row vanished between INSERT and SELECT — treat as a transient conflict.
                        warn!("PgLockStore: INSERT returned 0 rows but row not found on re-select");
                        txn.rollback().await.ok();
                        return Err(LockError::internal(
                            "pg: lock row disappeared between insert and select",
                        ));
                    }
                }
            } else {
                // Newly inserted.
                newly_locked.push(LockData {
                    resource: resource.clone(),
                    owner: owner_id.to_string(),
                    locked_at: locked_at as u64,
                });
            }
        }

        txn.commit().await.map_err(|e| {
            warn!("PgLockStore: COMMIT failed: {e}");
            LockError::internal(format!("pg commit error: {e}"))
        })?;

        Ok(newly_locked)
    }

    /// Query locks with a variety of filters, translated directly to WHERE clauses.
    ///
    /// No DynamoDB GSI required — plain indexed columns cover all 9 variants.
    async fn query_locks(&self, query: LockQuery) -> Result<Vec<LockData>, LockError> {
        let client = self.pool.get().await.map_err(|e| {
            warn!("PgLockStore: pool checkout failed: {e}");
            LockError::internal(format!("pg pool error: {e}"))
        })?;

        // Build the WHERE clause and bind params for each variant.
        // Column order in SELECT must match lock_data_from_row: hash, branch, owner_id, description, locked_at.
        let base_select = format!(
            "SELECT hash, branch, owner_id, description, locked_at FROM {table}",
            table = self.table_name
        );

        let rows: Vec<tokio_postgres::Row> = match &query {
            LockQuery::Hash(hash) => {
                let sql = format!("{base_select} WHERE hash = $1");
                let h: Vec<u8> = hash.as_bytes().to_vec();
                client.query(&sql, &[&h.as_slice()]).await
            }
            LockQuery::HashRepository(hash, repository) => {
                let sql = format!("{base_select} WHERE hash = $1 AND repository = $2");
                let h: Vec<u8> = hash.as_bytes().to_vec();
                let r: Vec<u8> = repository.as_bytes().to_vec();
                client.query(&sql, &[&h.as_slice(), &r.as_slice()]).await
            }
            LockQuery::HashRepositoryBranch(hash, repository, branch) => {
                let sql = format!(
                    "{base_select} WHERE hash = $1 AND repository = $2 AND branch = $3"
                );
                let h: Vec<u8> = hash.as_bytes().to_vec();
                let r: Vec<u8> = repository.as_bytes().to_vec();
                let b: Vec<u8> = branch.as_bytes().to_vec();
                client
                    .query(&sql, &[&h.as_slice(), &r.as_slice(), &b.as_slice()])
                    .await
            }
            LockQuery::Owner(owner) => {
                let sql = format!("{base_select} WHERE owner_id = $1");
                client.query(&sql, &[owner]).await
            }
            LockQuery::OwnerRepository(owner, repository) => {
                let sql = format!("{base_select} WHERE owner_id = $1 AND repository = $2");
                let r: Vec<u8> = repository.as_bytes().to_vec();
                client.query(&sql, &[owner, &r.as_slice()]).await
            }
            LockQuery::OwnerRepositoryBranch(owner, repository, branch) => {
                let sql = format!(
                    "{base_select} WHERE owner_id = $1 AND repository = $2 AND branch = $3"
                );
                let r: Vec<u8> = repository.as_bytes().to_vec();
                let b: Vec<u8> = branch.as_bytes().to_vec();
                client
                    .query(&sql, &[owner, &r.as_slice(), &b.as_slice()])
                    .await
            }
            LockQuery::Repository(repository) => {
                let sql = format!("{base_select} WHERE repository = $1");
                let r: Vec<u8> = repository.as_bytes().to_vec();
                client.query(&sql, &[&r.as_slice()]).await
            }
            LockQuery::RepositoryBranch(repository, branch) => {
                let sql =
                    format!("{base_select} WHERE repository = $1 AND branch = $2");
                let r: Vec<u8> = repository.as_bytes().to_vec();
                let b: Vec<u8> = branch.as_bytes().to_vec();
                client.query(&sql, &[&r.as_slice(), &b.as_slice()]).await
            }
            LockQuery::RepositoryBranchDescription(repository, branch, description) => {
                let sql = format!(
                    "{base_select} WHERE repository = $1 AND branch = $2 AND description = $3"
                );
                let r: Vec<u8> = repository.as_bytes().to_vec();
                let b: Vec<u8> = branch.as_bytes().to_vec();
                client
                    .query(&sql, &[&r.as_slice(), &b.as_slice(), description])
                    .await
            }
        }
        .map_err(|e| {
            warn!("PgLockStore: query_locks SELECT failed: {e}");
            LockError::internal(format!("pg query error: {e}"))
        })?;

        rows.iter().map(lock_data_from_row).collect()
    }

    /// Return LockData for those resources that are currently locked; omit unlocked ones.
    async fn check_locks_status(
        &self,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let deduplicated: HashSet<&LockResource> = resources.iter().collect();
        if deduplicated.len() != resources.len() {
            debug!(
                "PgLockStore: {} duplicate resources deduped in check_locks_status",
                resources.len() - deduplicated.len()
            );
        }

        if deduplicated.is_empty() {
            return Ok(vec![]);
        }

        let client = self.pool.get().await.map_err(|e| {
            warn!("PgLockStore: pool checkout failed: {e}");
            LockError::internal(format!("pg pool error: {e}"))
        })?;

        let repo_bytes: Vec<u8> = repository.as_bytes().to_vec();

        // Collect into a stable-ordered Vec first so both parallel arrays are derived from
        // the same iteration — two separate passes over a HashSet could produce different
        // orderings and misalign (branch[i], hash[i]) pairs.
        let deduped: Vec<&LockResource> = deduplicated.into_iter().collect();
        let branch_vecs: Vec<Vec<u8>> = deduped
            .iter()
            .map(|r| r.branch.as_bytes().to_vec())
            .collect();
        let hash_vecs: Vec<Vec<u8>> = deduped
            .iter()
            .map(|r| r.hash.as_bytes().to_vec())
            .collect();

        // tokio-postgres doesn't accept Vec<Vec<u8>> directly as an array param — use unnest.
        let sql = format!(
            "SELECT hash, branch, owner_id, description, locked_at FROM {table}
             WHERE repository = $1
               AND (branch, hash) IN (
                   SELECT * FROM unnest($2::bytea[], $3::bytea[])
               )",
            table = self.table_name
        );

        let branch_refs: Vec<&[u8]> = branch_vecs.iter().map(|v| v.as_slice()).collect();
        let hash_refs: Vec<&[u8]> = hash_vecs.iter().map(|v| v.as_slice()).collect();

        let rows = client
            .query(
                &sql,
                &[&repo_bytes.as_slice(), &branch_refs, &hash_refs],
            )
            .await
            .map_err(|e| {
                warn!("PgLockStore: check_locks_status SELECT failed: {e}");
                LockError::internal(format!("pg query error: {e}"))
            })?;

        rows.iter().map(lock_data_from_row).collect()
    }

    /// Delete lock rows for all requested resources, optionally validating ownership.
    ///
    /// With `validate_user=true`: errors if a row is absent (`LockNotFound`) or owned by
    /// a different user (`LockNotOwned`), and rolls back on the first such error.
    /// With `validate_user=false`: unconditional delete; absent rows are silently ignored.
    async fn unlock_resources(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        let len = resources.len();
        let mut resources = resources.to_vec();
        resources.sort();
        resources.dedup();
        if resources.len() != len {
            debug!(
                "PgLockStore: {} duplicate resources deduped in unlock_resources",
                len - resources.len()
            );
        }

        let mut client = self.pool.get().await.map_err(|e| {
            warn!("PgLockStore: pool checkout failed: {e}");
            LockError::internal(format!("pg pool error: {e}"))
        })?;

        let repo_bytes: Vec<u8> = repository.as_bytes().to_vec();

        let txn = client.transaction().await.map_err(|e| {
            warn!("PgLockStore: BEGIN failed: {e}");
            LockError::internal(format!("pg begin error: {e}"))
        })?;

        if validate_user {
            let delete_sql = format!(
                "DELETE FROM {table}
                 WHERE repository = $1 AND branch = $2 AND hash = $3 AND owner_id = $4",
                table = self.table_name
            );
            let select_owner_sql = format!(
                "SELECT owner_id FROM {table} WHERE repository = $1 AND branch = $2 AND hash = $3",
                table = self.table_name
            );

            for resource in &resources {
                let branch_bytes: Vec<u8> = resource.branch.as_bytes().to_vec();
                let hash_bytes: Vec<u8> = resource.hash.as_bytes().to_vec();

                let deleted = txn
                    .execute(
                        &delete_sql,
                        &[
                            &repo_bytes.as_slice(),
                            &branch_bytes.as_slice(),
                            &hash_bytes.as_slice(),
                            &owner_id,
                        ],
                    )
                    .await
                    .map_err(|e| {
                        warn!("PgLockStore: DELETE failed: {e}");
                        LockError::internal(format!("pg delete error: {e}"))
                    })?;

                if deleted == 0 {
                    // Row absent or owned by someone else — disambiguate.
                    let row = txn
                        .query_opt(
                            &select_owner_sql,
                            &[
                                &repo_bytes.as_slice(),
                                &branch_bytes.as_slice(),
                                &hash_bytes.as_slice(),
                            ],
                        )
                        .await
                        .map_err(|e| {
                            warn!("PgLockStore: SELECT owner on delete failed: {e}");
                            LockError::internal(format!("pg select error: {e}"))
                        })?;

                    txn.rollback().await.ok();
                    return match row {
                        Some(r) => {
                            let existing_owner: String = r.get(0);
                            warn!(
                                "PgLockStore: cannot unlock {:?} — owned by {existing_owner}, not {owner_id}",
                                resource.description
                            );
                            Err(LockNotOwned.into())
                        }
                        None => {
                            warn!(
                                "PgLockStore: cannot unlock {:?} — row not found",
                                resource.description
                            );
                            Err(LockNotFound.into())
                        }
                    };
                }
            }
        } else {
            // Unconditional delete: idempotent, never errors on absent rows.
            let delete_sql = format!(
                "DELETE FROM {table}
                 WHERE repository = $1 AND branch = $2 AND hash = $3",
                table = self.table_name
            );

            for resource in &resources {
                let branch_bytes: Vec<u8> = resource.branch.as_bytes().to_vec();
                let hash_bytes: Vec<u8> = resource.hash.as_bytes().to_vec();

                txn.execute(
                    &delete_sql,
                    &[
                        &repo_bytes.as_slice(),
                        &branch_bytes.as_slice(),
                        &hash_bytes.as_slice(),
                    ],
                )
                .await
                .map_err(|e| {
                    warn!("PgLockStore: DELETE (unconditional) failed: {e}");
                    LockError::internal(format!("pg delete error: {e}"))
                })?;
            }
        }

        txn.commit().await.map_err(|e| {
            warn!("PgLockStore: COMMIT failed: {e}");
            LockError::internal(format!("pg commit error: {e}"))
        })?;

        Ok(resources)
    }
}

#[cfg(test)]
mod tests {
    //! Integration tests against a real Postgres instance.
    //!
    //! These tests are ignored by default. To run:
    //!
    //!   docker run -d --name lock-pg-test -e POSTGRES_PASSWORD=lore -p 5434:5432 postgres:17
    //!   # wait for pg_isready, then:
    //!   PG_TEST_DSN="host=localhost port=5434 user=postgres password=lore dbname=postgres" \
    //!       cargo test -p lore-pg --lib pg_lock_store -- --ignored --test-threads=1
    //!   docker rm -f lock-pg-test

    use deadpool_postgres::Config as DeadpoolConfig;
    use deadpool_postgres::Runtime;
    use lore_base::types::BranchId;
    use lore_base::types::Hash;
    use lore_base::types::LockResource;
    use lore_revision::lock::LockError;
    use lore_revision::lock::LockQuery;
    use lore_revision::lock::LockStore;
    use lore_revision::lore::RepositoryId;
    use rand::random;

    use super::PgLockStore;
    use super::PgLockStoreSettings;

    async fn setup_store() -> Option<PgLockStore> {
        let dsn = std::env::var("PG_TEST_DSN").ok()?;
        let mut cfg = DeadpoolConfig::new();
        cfg.url = Some(dsn);
        let connector = crate::tls::make_connector().ok()?;
        let pool = cfg.create_pool(Some(Runtime::Tokio1), connector).ok()?;
        let settings = PgLockStoreSettings::new("locks_test");
        let store = PgLockStore::new(pool.clone(), &settings);
        store.ensure_schema().await.ok()?;
        // Wipe state from previous runs.
        let client = pool.get().await.ok()?;
        client.execute("DELETE FROM locks_test", &[]).await.ok()?;
        Some(store)
    }

    fn make_resource(branch: BranchId, hash: Hash, desc: &str) -> LockResource {
        LockResource {
            branch,
            hash,
            description: desc.to_string(),
        }
    }

    /// Acquiring a new lock succeeds and returns the newly locked resource.
    #[tokio::test]
    #[ignore]
    async fn test_acquire_success() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let hash: Hash = random();
        let resource = make_resource(branch, hash, "/a/file.txt");

        let locks = store
            .lock_resources("alice", repo, &[resource.clone()])
            .await
            .expect("lock should succeed");

        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].owner, "alice");
        assert_eq!(locks[0].resource, resource);
    }

    /// Re-acquiring a lock owned by the same owner is idempotent: returns empty vec.
    #[tokio::test]
    #[ignore]
    async fn test_acquire_idempotent_same_owner() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let hash: Hash = random();
        let resource = make_resource(branch, hash, "/b/file.txt");

        store
            .lock_resources("alice", repo, &[resource.clone()])
            .await
            .expect("first lock should succeed");

        let locks = store
            .lock_resources("alice", repo, &[resource.clone()])
            .await
            .expect("second lock should succeed idempotently");

        // Same-owner idempotent re-lock is skipped — not included in returned vec.
        assert_eq!(locks.len(), 0);
    }

    /// Attempting to lock a resource already owned by a different user returns LockNotOwned,
    /// and leaves the original lock intact (transaction rolled back).
    #[tokio::test]
    #[ignore]
    async fn test_acquire_conflict_other_owner() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let hash: Hash = random();
        let resource = make_resource(branch, hash, "/c/file.txt");

        // Alice locks first.
        store
            .lock_resources("alice", repo, &[resource.clone()])
            .await
            .expect("alice lock should succeed");

        // Bob tries to lock the same resource.
        let err = store
            .lock_resources("bob", repo, &[resource.clone()])
            .await
            .expect_err("bob lock should fail");

        assert!(
            matches!(err, LockError::LockNotOwned(_)),
            "expected LockNotOwned, got {err:?}"
        );

        // Alice's lock must still exist.
        let status = store
            .check_locks_status(repo, &[resource.clone()])
            .await
            .expect("check status should succeed");
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].owner, "alice");
    }

    /// Query by hash finds all locks with that hash regardless of repo/branch.
    #[tokio::test]
    #[ignore]
    async fn test_query_hash() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch1: BranchId = random();
        let branch2: BranchId = random();
        let shared_hash: Hash = random();

        store
            .lock_resources(
                "alice",
                repo,
                &[
                    make_resource(branch1, shared_hash, "/h1.txt"),
                    make_resource(branch2, shared_hash, "/h2.txt"),
                ],
            )
            .await
            .expect("lock");

        let locks = store
            .query_locks(LockQuery::Hash(shared_hash))
            .await
            .expect("query");

        assert_eq!(locks.len(), 2);
        assert!(locks.iter().all(|l| l.resource.hash == shared_hash));
    }

    /// Query by owner returns all locks for that owner.
    #[tokio::test]
    #[ignore]
    async fn test_query_owner() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let h1: Hash = random();
        let h2: Hash = random();

        store
            .lock_resources(
                "alice",
                repo,
                &[
                    make_resource(branch, h1, "/o1.txt"),
                    make_resource(branch, h2, "/o2.txt"),
                ],
            )
            .await
            .expect("lock");

        let locks = store
            .query_locks(LockQuery::Owner("alice".to_string()))
            .await
            .expect("query");

        // At least the two we just inserted (other tests may add rows for "alice" too if they
        // accidentally share a table, but setup_store() wipes it so we're safe).
        assert!(locks.len() >= 2);
        assert!(locks.iter().all(|l| l.owner == "alice"));
    }

    /// Query by repository returns all locks for that repository.
    #[tokio::test]
    #[ignore]
    async fn test_query_repository() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();

        store
            .lock_resources(
                "alice",
                repo,
                &[
                    make_resource(branch, random(), "/r1.txt"),
                    make_resource(branch, random(), "/r2.txt"),
                ],
            )
            .await
            .expect("lock");

        let locks = store
            .query_locks(LockQuery::Repository(repo))
            .await
            .expect("query");

        assert_eq!(locks.len(), 2);
    }

    /// Query by repository + branch filters correctly.
    #[tokio::test]
    #[ignore]
    async fn test_query_repository_branch() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch_a: BranchId = random();
        let branch_b: BranchId = random();

        store
            .lock_resources(
                "alice",
                repo,
                &[
                    make_resource(branch_a, random(), "/rb1.txt"),
                    make_resource(branch_b, random(), "/rb2.txt"),
                ],
            )
            .await
            .expect("lock");

        let locks = store
            .query_locks(LockQuery::RepositoryBranch(repo, branch_a))
            .await
            .expect("query");

        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].resource.branch, branch_a);
    }

    /// Query by repository + branch + description narrows to a single row.
    #[tokio::test]
    #[ignore]
    async fn test_query_repository_branch_description() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let desc = "/rbd-unique.txt";

        store
            .lock_resources(
                "alice",
                repo,
                &[
                    make_resource(branch, random(), desc),
                    make_resource(branch, random(), "/rbd-other.txt"),
                ],
            )
            .await
            .expect("lock");

        let locks = store
            .query_locks(LockQuery::RepositoryBranchDescription(
                repo,
                branch,
                desc.to_string(),
            ))
            .await
            .expect("query");

        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].resource.description, desc);
    }

    /// check_locks_status returns only locked resources, omitting absent ones.
    #[tokio::test]
    #[ignore]
    async fn test_check_locks_status() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let locked_hash: Hash = random();
        let unlocked_hash: Hash = random();

        store
            .lock_resources(
                "alice",
                repo,
                &[make_resource(branch, locked_hash, "/locked.txt")],
            )
            .await
            .expect("lock");

        let status = store
            .check_locks_status(
                repo,
                &[
                    make_resource(branch, locked_hash, "/locked.txt"),
                    make_resource(branch, unlocked_hash, "/unlocked.txt"),
                ],
            )
            .await
            .expect("check_locks_status");

        assert_eq!(status.len(), 1);
        assert_eq!(status[0].resource.hash, locked_hash);
    }

    /// Unlock without validate_user is idempotent — succeeds even if the resource isn't locked.
    #[tokio::test]
    #[ignore]
    async fn test_unlock_no_validate() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let hash: Hash = random();
        let resource = make_resource(branch, hash, "/unv.txt");

        store
            .lock_resources("alice", repo, &[resource.clone()])
            .await
            .expect("lock");

        // First unlock: removes the row.
        store
            .unlock_resources("bob", false, repo, &[resource.clone()])
            .await
            .expect("first unlock (no validate) should succeed");

        // Second unlock on absent row: must not error.
        store
            .unlock_resources("bob", false, repo, &[resource.clone()])
            .await
            .expect("second unlock on absent row should succeed");
    }

    /// Unlock with validate_user succeeds when the owner matches.
    #[tokio::test]
    #[ignore]
    async fn test_unlock_validate_success() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let hash: Hash = random();
        let resource = make_resource(branch, hash, "/vs.txt");

        store
            .lock_resources("alice", repo, &[resource.clone()])
            .await
            .expect("lock");

        let result = store
            .unlock_resources("alice", true, repo, &[resource.clone()])
            .await
            .expect("unlock should succeed");

        assert_eq!(result.len(), 1);

        // Confirm the lock is gone.
        let status = store
            .check_locks_status(repo, &[resource.clone()])
            .await
            .expect("check");
        assert!(status.is_empty());
    }

    /// Unlock with validate_user and wrong owner returns LockNotOwned.
    #[tokio::test]
    #[ignore]
    async fn test_unlock_validate_wrong_owner() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let hash: Hash = random();
        let resource = make_resource(branch, hash, "/vwo.txt");

        store
            .lock_resources("alice", repo, &[resource.clone()])
            .await
            .expect("lock");

        let err = store
            .unlock_resources("bob", true, repo, &[resource.clone()])
            .await
            .expect_err("should fail");

        assert!(
            matches!(err, LockError::LockNotOwned(_)),
            "expected LockNotOwned, got {err:?}"
        );

        // Alice's lock should still exist.
        let status = store
            .check_locks_status(repo, &[resource.clone()])
            .await
            .expect("check");
        assert_eq!(status.len(), 1);
    }

    /// Unlock with validate_user on a non-existent lock returns LockNotFound.
    #[tokio::test]
    #[ignore]
    async fn test_unlock_validate_not_found() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let repo: RepositoryId = random();
        let branch: BranchId = random();
        let hash: Hash = random();
        let resource = make_resource(branch, hash, "/vnf.txt");

        // Never locked this resource.
        let err = store
            .unlock_resources("alice", true, repo, &[resource.clone()])
            .await
            .expect_err("should fail with LockNotFound");

        assert!(
            matches!(err, LockError::LockNotFound(_)),
            "expected LockNotFound, got {err:?}"
        );
    }
}
