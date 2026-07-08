// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use lore_base::error::AddressNotFound;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::KeyType;
use lore_base::types::Partition;
use lore_storage::KeyValueStream;
use lore_storage::MutableStore as MutableStoreTrait;
use lore_storage::StoreError;
use serde::Deserialize;
use tracing::debug;
use tracing::warn;
use zerocopy::IntoBytes;

#[derive(Clone, Debug, Deserialize)]
pub struct PgMutableStoreSettings {
    pub table_name: String,
}

impl PgMutableStoreSettings {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

impl Default for PgMutableStoreSettings {
    fn default() -> Self {
        Self::new("mutable_store")
    }
}

pub struct PgMutableStore {
    pool: Pool,
    table_name: Arc<str>,
}

impl PgMutableStore {
    pub fn new(pool: Pool, settings: &PgMutableStoreSettings) -> Self {
        Self {
            pool,
            table_name: Arc::from(settings.table_name.as_str()),
        }
    }

    /// Overwrite `key.data[0]` with `key_type as u8` — matches the DynamoDB typed_key contract.
    fn typed_key(mut key: Hash, key_type: KeyType) -> Hash {
        key.data_mut()[0] = key_type as u8;
        key
    }

    /// Build a pool from a libpq DSN, create the table schema, and return a ready store.
    ///
    /// All aws-sdk / deadpool imports stay inside lore-pg; callers (e.g. the server plugin glue)
    /// do not need to pull in those crates directly.
    pub async fn from_config(
        dsn: &str,
        table: &str,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        // Size-capped, TLS-capable pool (see crate::tls::make_pool for why the
        // cap matters on a shared managed-Postgres ceiling).
        let pool = crate::tls::make_pool(dsn)?;
        let settings = PgMutableStoreSettings::new(table);
        let store = Arc::new(Self::new(pool, &settings));
        store.ensure_schema().await?;
        Ok(store)
    }

    /// Run CREATE TABLE IF NOT EXISTS so the caller does not need to manage schema migrations manually.
    ///
    /// Returns a `Box<dyn Error>` because the pool checkout error and the pg query error are
    /// different types — callers typically call this once at startup and treat failure as fatal.
    pub async fn ensure_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        // $1 interpolation is not available for identifiers; format! is safe because the table
        // name comes from trusted server config, not user input.
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                repository_id BYTEA NOT NULL,
                key           BYTEA NOT NULL,
                value         BYTEA NOT NULL,
                PRIMARY KEY (repository_id, key)
            )",
            table = self.table_name
        );
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    /// Fetch the current value for (repository_id, key). Returns `None` when the row is absent.
    async fn fetch_current(
        &self,
        repository: &[u8],
        key: &[u8],
    ) -> Result<Option<Hash>, StoreError> {
        let client = self.pool.get().await.map_err(|e| {
            warn!("PgMutableStore: pool checkout failed: {e}");
            StoreError::internal(format!("pg pool error: {e}"))
        })?;
        let sql = format!(
            "SELECT value FROM {table} WHERE repository_id = $1 AND key = $2",
            table = self.table_name
        );
        match client.query_opt(&sql, &[&repository, &key]).await {
            Ok(Some(row)) => {
                let bytes: Vec<u8> = row.get(0);
                Ok(Some(hash_from_bytes(&bytes)?))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                warn!("PgMutableStore: SELECT failed: {e}");
                Err(StoreError::internal(format!("pg select error: {e}")))
            }
        }
    }

    async fn load_typed(
        self: Arc<Self>,
        repository: Context,
        key: Hash,
    ) -> Result<Hash, StoreError> {
        match self.fetch_current(repository.as_ref(), key.as_bytes()).await? {
            Some(value) if !value.is_zero() => Ok(value),
            _ => Err(StoreError::from(AddressNotFound::from(
                Address::zero_context_hash(key),
            ))),
        }
    }

    async fn store_typed(
        self: Arc<Self>,
        repository: Context,
        key: Hash,
        value: Hash,
    ) -> Result<(), StoreError> {
        let client = self.pool.get().await.map_err(|e| {
            warn!("PgMutableStore: pool checkout failed: {e}");
            StoreError::internal(format!("pg pool error: {e}"))
        })?;
        let repo_bytes: &[u8] = repository.as_ref();
        let key_bytes: &[u8] = key.as_bytes();

        if value.is_zero() {
            // Storing a zero hash means "delete the row".
            let sql = format!(
                "DELETE FROM {table} WHERE repository_id = $1 AND key = $2",
                table = self.table_name
            );
            client
                .execute(&sql, &[&repo_bytes, &key_bytes])
                .await
                .map_err(|e| {
                    warn!("PgMutableStore: DELETE failed: {e}");
                    StoreError::internal(format!("pg delete error: {e}"))
                })?;
        } else {
            let value_bytes: &[u8] = value.as_bytes();
            let sql = format!(
                "INSERT INTO {table} (repository_id, key, value)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (repository_id, key) DO UPDATE SET value = EXCLUDED.value",
                table = self.table_name
            );
            client
                .execute(&sql, &[&repo_bytes, &key_bytes, &value_bytes])
                .await
                .map_err(|e| {
                    warn!("PgMutableStore: UPSERT failed: {e}");
                    StoreError::internal(format!("pg upsert error: {e}"))
                })?;
        }
        Ok(())
    }

    async fn compare_and_swap_typed(
        self: Arc<Self>,
        repository: Context,
        key: Hash,
        expected: Hash,
        value: Hash,
    ) -> Result<Hash, StoreError> {
        let client = self.pool.get().await.map_err(|e| {
            warn!("PgMutableStore: pool checkout failed: {e}");
            StoreError::internal(format!("pg pool error: {e}"))
        })?;

        let repo_bytes: Vec<u8> = repository.as_ref().to_vec();
        let key_bytes: Vec<u8> = key.as_bytes().to_vec();
        let value_bytes: Vec<u8> = value.as_bytes().to_vec();

        if expected.is_zero() {
            // Insert-if-absent: a single CTE is atomic — no read-after-write race.
            // On insert success the RETURNING clause gives back the inserted value; the
            // CASE picks $4 (zero/expected) to signal "prior value was zero".
            // On conflict (row already present) the subquery returns the current value.
            let sql = format!(
                "WITH ins AS (
                     INSERT INTO {t} (repository_id, key, value) VALUES ($1, $2, $3)
                     ON CONFLICT (repository_id, key) DO NOTHING
                     RETURNING value
                 )
                 SELECT CASE WHEN EXISTS(SELECT 1 FROM ins)
                             THEN $4
                             ELSE (SELECT value FROM {t} WHERE repository_id = $1 AND key = $2)
                        END",
                t = self.table_name
            );
            // $4 = zero bytes (the expected/prior value when insert succeeds)
            let zero_bytes: Vec<u8> = expected.as_bytes().to_vec();
            let row = client
                .query_one(
                    &sql,
                    &[
                        &repo_bytes.as_slice(),
                        &key_bytes.as_slice(),
                        &value_bytes.as_slice(),
                        &zero_bytes.as_slice(),
                    ],
                )
                .await
                .map_err(|e| {
                    warn!("PgMutableStore: CAS insert-if-absent failed: {e}");
                    StoreError::internal(format!("pg cas insert error: {e}"))
                })?;
            let bytes: Vec<u8> = row.get(0);
            hash_from_bytes(&bytes)
        } else {
            // Update-if-matches: single CTE, one round-trip, atomic.
            // On match the RETURNING clause gives back $4 (expected / prior value).
            // On mismatch the subquery returns the current value, or $5 (zero) if row absent.
            let expected_bytes: Vec<u8> = expected.as_bytes().to_vec();
            let zero_bytes: Vec<u8> = Hash::default().as_bytes().to_vec();
            let sql = format!(
                "WITH upd AS (
                     UPDATE {t} SET value = $3
                     WHERE repository_id = $1 AND key = $2 AND value = $4
                     RETURNING value
                 )
                 SELECT CASE WHEN EXISTS(SELECT 1 FROM upd)
                             THEN $4
                             ELSE COALESCE(
                                     (SELECT value FROM {t} WHERE repository_id = $1 AND key = $2),
                                     $5
                                  )
                        END",
                t = self.table_name
            );
            let row = client
                .query_one(
                    &sql,
                    &[
                        &repo_bytes.as_slice(),
                        &key_bytes.as_slice(),
                        &value_bytes.as_slice(),
                        &expected_bytes.as_slice(),
                        &zero_bytes.as_slice(),
                    ],
                )
                .await
                .map_err(|e| {
                    warn!("PgMutableStore: CAS update failed: {e}");
                    StoreError::internal(format!("pg cas update error: {e}"))
                })?;
            let bytes: Vec<u8> = row.get(0);
            hash_from_bytes(&bytes)
        }
    }

    fn list_typed(
        self: Arc<Self>,
        repository: Context,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        let (stream, sender) = KeyValueStream::new();

        // Untyped means no type filtering — return an empty stream immediately.
        if key_type == KeyType::Untyped {
            return Ok(stream);
        }

        // key BETWEEN [key_type, 0x00..] AND [key_type, 0xFF..] replicates the DynamoDB BETWEEN range.
        let mut key_start = [0u8; 32];
        key_start[0] = key_type as u8;
        let mut key_end = [0xFFu8; 32];
        key_end[0] = key_type as u8;

        let pool = self.pool.clone();
        let table_name = self.table_name.clone();
        let repo_bytes: Vec<u8> = repository.as_ref().to_vec();

        tokio::spawn(async move {
            let client = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    warn!("PgMutableStore list: pool checkout failed: {e}");
                    return;
                }
            };
            let sql = format!(
                "SELECT key, value FROM {table}
                     WHERE repository_id = $1
                       AND key >= $2
                       AND key <= $3
                       AND value IS NOT NULL",
                table = table_name
            );
            let key_start_ref: &[u8] = &key_start;
            let key_end_ref: &[u8] = &key_end;
            let rows = match client
                .query(&sql, &[&repo_bytes.as_slice(), &key_start_ref, &key_end_ref])
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    warn!("PgMutableStore list: SELECT failed: {e}");
                    return;
                }
            };
            for row in rows {
                let key_bytes: Vec<u8> = row.get(0);
                let value_bytes: Vec<u8> = row.get(1);
                let key = match hash_from_bytes(&key_bytes) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!("PgMutableStore list: bad key bytes: {e:?}");
                        continue;
                    }
                };
                let value = match hash_from_bytes(&value_bytes) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!("PgMutableStore list: bad value bytes: {e:?}");
                        continue;
                    }
                };
                if value.is_zero() {
                    continue;
                }
                if let Err(e) = sender.send((key, value)) {
                    debug!("PgMutableStore list: channel closed: {e}");
                    return;
                }
            }
        });

        Ok(stream)
    }
}

/// Deserialize a 32-byte BYTEA column value into a `Hash`.
fn hash_from_bytes(bytes: &[u8]) -> Result<Hash, StoreError> {
    if bytes.len() != 32 {
        return Err(StoreError::internal(format!(
            "pg: expected 32-byte hash, got {}",
            bytes.len()
        )));
    }
    let arr: [u8; 32] = bytes.try_into().unwrap();
    Ok(Hash::from(arr))
}

#[async_trait]
impl MutableStoreTrait for PgMutableStore {
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        let repository: Context = partition.into();
        let typed_key = Self::typed_key(key, key_type);
        self.load_typed(repository, typed_key).await
    }

    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError> {
        let repository: Context = partition.into();
        let typed_key = Self::typed_key(key, key_type);
        self.store_typed(repository, typed_key, value).await
    }

    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        let repository: Context = partition.into();
        let typed_key = Self::typed_key(key, key_type);
        self.compare_and_swap_typed(repository, typed_key, expected, value)
            .await
    }

    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        let repository: Context = partition.into();
        self.list_typed(repository, key_type)
    }

    /// No-op: Postgres transactions provide durability without an explicit flush.
    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Live integration tests against a real Postgres instance.
    //!
    //! These tests are ignored by default. To run:
    //!
    //!   PG_TEST_DSN="host=localhost user=postgres dbname=lore_test" \
    //!       cargo test -p lore-pg -- --ignored
    //!
    //! The DSN is a libpq connection string accepted by `tokio_postgres::connect`.
    //! The connecting role must have CREATE TABLE privileges in the target database.

    use std::sync::Arc;

    use deadpool_postgres::Config as DeadpoolConfig;
    use deadpool_postgres::Runtime;
    use lore_base::types::Hash;
    use lore_base::types::KeyType;
    use lore_base::types::Partition;
    use lore_storage::MutableStore;
    use rand::random;

    use super::PgMutableStore;
    use super::PgMutableStoreSettings;

    async fn setup_store() -> Option<Arc<PgMutableStore>> {
        let dsn = std::env::var("PG_TEST_DSN").ok()?;
        let mut cfg = DeadpoolConfig::new();
        cfg.url = Some(dsn);
        let connector = crate::tls::make_connector().ok()?;
        let pool = cfg.create_pool(Some(Runtime::Tokio1), connector).ok()?;
        let settings = PgMutableStoreSettings::new("mutable_store_test");
        let store = Arc::new(PgMutableStore::new(pool.clone(), &settings));
        store.ensure_schema().await.ok()?;
        // Wipe state left from previous runs.
        let client = pool.get().await.ok()?;
        client
            .execute("DELETE FROM mutable_store_test", &[])
            .await
            .ok()?;
        Some(store)
    }

    /// CAS insert-if-absent succeeds when no row exists.
    #[tokio::test]
    #[ignore]
    async fn test_compare_and_swap_mutable() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let key = random::<Hash>();
        let value = random::<Hash>();
        let key_type = KeyType::BranchLatestPointer;

        let prior = store
            .clone()
            .compare_and_swap(partition, key, Hash::default(), value, key_type)
            .await
            .expect("CAS should succeed");
        // On success, returns the prior value (zero/expected).
        assert_eq!(prior, Hash::default());

        // Confirm the value is now stored.
        let loaded = store
            .clone()
            .load(partition, key, key_type)
            .await
            .expect("should load after CAS");
        assert_eq!(loaded, value);
    }

    /// CAS insert-if-absent fails when a row already exists — returns the existing value.
    #[tokio::test]
    #[ignore]
    async fn test_compare_and_swap_mismatch() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let key = random::<Hash>();
        let value1 = random::<Hash>();
        let value2 = random::<Hash>();
        let key_type = KeyType::BranchLatestPointer;

        // First insert.
        store
            .clone()
            .store(partition, key, value1, key_type)
            .await
            .expect("store should succeed");

        // Try to CAS with expected=zero (insert-if-absent), but row exists -> returns current.
        let witness = store
            .clone()
            .compare_and_swap(partition, key, Hash::default(), value2, key_type)
            .await
            .expect("CAS should not error");
        assert_eq!(witness, value1);

        // The stored value should remain value1.
        let loaded = store
            .clone()
            .load(partition, key, key_type)
            .await
            .expect("load");
        assert_eq!(loaded, value1);
    }

    /// CAS update-if-matches: key does not exist — returns Hash::default() (zero).
    #[tokio::test]
    #[ignore]
    async fn test_compare_and_swap_not_found() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let key = random::<Hash>();
        let expected = random::<Hash>(); // non-zero expected, key doesn't exist
        let value = random::<Hash>();
        let key_type = KeyType::BranchLatestPointer;

        let witness = store
            .clone()
            .compare_and_swap(partition, key, expected, value, key_type)
            .await
            .expect("CAS should not error");
        // Row absent + expected != zero -> return Hash::default().
        assert_eq!(witness, Hash::default());
    }

    /// CAS update-if-matches: key exists but value doesn't match expected — returns current value.
    #[tokio::test]
    #[ignore]
    async fn test_compare_and_swap_not_found_expected() {
        let store = setup_store().await.expect("PG_TEST_DSN not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let key = random::<Hash>();
        let actual_value = random::<Hash>();
        let wrong_expected = random::<Hash>();
        let new_value = random::<Hash>();
        let key_type = KeyType::BranchLatestPointer;

        store
            .clone()
            .store(partition, key, actual_value, key_type)
            .await
            .expect("store");

        let witness = store
            .clone()
            .compare_and_swap(partition, key, wrong_expected, new_value, key_type)
            .await
            .expect("CAS should not error");
        // Existing value != expected -> return the existing value.
        assert_eq!(witness, actual_value);
    }
}
