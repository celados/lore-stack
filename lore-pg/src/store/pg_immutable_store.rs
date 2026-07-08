// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! PostgreSQL + R2 (S3-compatible) implementation of [`ImmutableStore`].
//!
//! # Storage split
//! - **R2 (via S3 SDK)**: payload bytes, keyed by `hex(hash)` — same as the AWS version.
//! - **PostgreSQL**: two tables that replace DynamoDB:
//!   - `{fragments_table}`: (hash BYTEA, repository BYTEA, context BYTEA) — the association
//!     index. Equivalent to the DynamoDB fragments table with PK=hash, SK=repository_context.
//!   - `{metadata_table}`: (hash BYTEA, flags INT8, size_payload INT8, size_content INT8) —
//!     per-payload metadata. Equivalent to the DynamoDB metadata table.
//!
//! # R2 caveats applied
//! 1. `ListObjectVersions` is not supported by R2 — `delete_payload` uses `list_objects_v2`
//!    to check existence then issues a plain `delete_object` with no version_id.
//! 2. `request_checksum_calculation` must be "when_required" — callers building the S3 client
//!    must set `force_path_style(true)` and configure the SDK appropriately; this file assumes
//!    the `S3Impl` is already configured correctly by the caller.

use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectError;
use bytes::Bytes;
use bytes::BytesMut;
use deadpool_postgres::Pool;
use lore_base::error::AddressNotFound;
use lore_base::error::SlowDown;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::FRAGMENT_SIZE_THRESHOLD;
use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;
use lore_base::types::FragmentReference;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_base::types::TypedBytes;
use lore_storage::ImmutableStore as ImmutableStoreTrait;
use lore_storage::StoreError;
use lore_storage::StoreMatch;
use lore_storage::StoreObliterateStats;
use lore_storage::StoreQueryResult;
use lore_storage::immutable_store::sanitise_fragment_behavior_flags;
use serde::Deserialize;
use tokio::task::JoinSet;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::aws_error::AwsError;
// Always use the concrete S3Impl here — this store does not use mock injection
// (unit tests for this module use real MinIO).
use crate::s3::S3Impl;

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
pub struct PgImmutableStoreSettings {
    /// Table that stores fragment → (repository, context) associations.
    pub fragments_table: String,
    /// Table that stores per-hash fragment metadata (flags, sizes).
    pub metadata_table: String,
    /// R2 / S3-compatible bucket name.
    pub bucket: String,
    /// Optional S3-compatible endpoint URL (e.g. for R2 or local MinIO).
    pub endpoint_url: Option<String>,
    /// Optional AWS region override.
    pub region: Option<String>,
    /// When true, skip the content-size collision check on put (migration aid).
    #[serde(default)]
    pub force_write: bool,
}

impl PgImmutableStoreSettings {
    pub fn new(
        fragments_table: impl Into<String>,
        metadata_table: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        Self {
            fragments_table: fragments_table.into(),
            metadata_table: metadata_table.into(),
            bucket: bucket.into(),
            endpoint_url: None,
            region: None,
            force_write: false,
        }
    }

    pub fn with_endpoint(mut self, endpoint_url: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint_url.into());
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn with_force_write(mut self, force_write: bool) -> Self {
        self.force_write = force_write;
        self
    }
}

// ---------------------------------------------------------------------------
// Store struct
// ---------------------------------------------------------------------------

pub struct PgImmutableStore {
    pool: Pool,
    s3: S3Impl,
    bucket: String,
    fragments_table: Arc<str>,
    metadata_table: Arc<str>,
    force_write: bool,
}

impl PgImmutableStore {
    pub fn new(pool: Pool, s3: S3Impl, settings: &PgImmutableStoreSettings) -> Self {
        Self {
            pool,
            s3,
            bucket: settings.bucket.clone(),
            fragments_table: Arc::from(settings.fragments_table.as_str()),
            metadata_table: Arc::from(settings.metadata_table.as_str()),
            force_write: settings.force_write,
        }
    }

    /// Build pool + S3Impl from config primitives, create schema, return ready store.
    ///
    /// Encapsulates all aws-sdk and deadpool wiring so the server plugin glue never needs to
    /// import those crates directly. `endpoint` / `region` are `None` for real AWS; set them for
    /// R2 / MinIO. `force_path_style = true` is required for non-AWS S3-compatible services.
    #[allow(clippy::too_many_arguments)]
    pub async fn from_config(
        dsn: &str,
        bucket: &str,
        endpoint: Option<&str>,
        region: Option<&str>,
        force_path_style: bool,
        fragments_table: &str,
        metadata_table: &str,
        force_write: bool,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        use std::time::Duration;

        use aws_smithy_http_client::Builder as HttpClientBuilder;
        use aws_smithy_http_client::tls;
        use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
        use aws_smithy_runtime_api::client::behavior_version::BehaviorVersion;
        use aws_types::region::Region;

        // Size-capped, TLS-capable Postgres pool (see crate::tls::make_pool for
        // why the cap matters on a shared managed-Postgres ceiling).
        let pool = crate::tls::make_pool(dsn)?;

        // S3 / R2 client — uses rustls+ring to avoid linking aws-lc-sys.
        // Mirrors the test setup_store pattern exactly.
        let http_client = HttpClientBuilder::new().build_with_connector_fn(|_, _| {
            aws_smithy_http_client::Connector::builder()
                .tls_provider(tls::Provider::Rustls(CryptoMode::Ring))
                .build()
        });

        let region_str = region.unwrap_or("auto");
        let mut aws_builder = aws_config::defaults(BehaviorVersion::latest())
            .http_client(http_client)
            .region(Region::new(region_str.to_owned()));
        if let Some(ep) = endpoint {
            aws_builder = aws_builder.endpoint_url(ep);
        }
        let aws_config = aws_builder.load().await;

        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(force_path_style)
            // R2 fix: only send checksums when the endpoint requires them.
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
        let s3 = S3Impl::new(s3_client, Duration::from_secs(30));

        let mut settings =
            PgImmutableStoreSettings::new(fragments_table, metadata_table, bucket.to_owned());
        if let Some(ep) = endpoint {
            settings = settings.with_endpoint(ep.to_owned());
        }
        if let Some(r) = region {
            settings = settings.with_region(r.to_owned());
        }
        settings = settings.with_force_write(force_write);

        let store = Arc::new(Self::new(pool, s3, &settings));
        store.ensure_schema().await?;
        Ok(store)
    }

    // -----------------------------------------------------------------------
    // Schema management
    // -----------------------------------------------------------------------

    /// Create the two Postgres tables if they do not already exist.
    ///
    /// Table names come from trusted server config, not user input — `format!` is safe here.
    pub async fn ensure_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;

        // Fragment association index — one row per (hash, repository, context) tuple.
        // PK on all three columns mirrors the DynamoDB PK=hash / SK=repository_context layout.
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                hash        BYTEA   NOT NULL,
                repository  BYTEA   NOT NULL,
                context     BYTEA   NOT NULL,
                PRIMARY KEY (hash, repository, context)
            );
            CREATE INDEX IF NOT EXISTS {table}_hash_repository
                ON {table} (hash, repository);
            CREATE INDEX IF NOT EXISTS {table}_hash_idx
                ON {table} (hash);",
            table = self.fragments_table,
        );
        client.batch_execute(&sql).await?;

        // Per-payload metadata table — one row per unique hash.
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                hash         BYTEA   NOT NULL PRIMARY KEY,
                flags        BIGINT  NOT NULL DEFAULT 0,
                size_payload BIGINT  NOT NULL DEFAULT 0,
                size_content BIGINT  NOT NULL DEFAULT 0
            )",
            table = self.metadata_table,
        );
        client.execute(&sql, &[]).await?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Pool helper
    // -----------------------------------------------------------------------

    async fn get_client(&self) -> Result<deadpool_postgres::Object, StoreError> {
        self.pool.get().await.map_err(|e| {
            warn!("PgImmutableStore: pool checkout failed: {e}");
            StoreError::internal(format!("pg pool error: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // Existence checks (mirrors AWS exists_exact / exists_repository / exists_hash)
    // -----------------------------------------------------------------------

    /// Exact match: hash + repository + context all match.
    async fn exists_exact(
        &self,
        hash: &[u8],
        repository: &[u8],
        context: &[u8],
    ) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let sql = format!(
            "SELECT 1 FROM {table} WHERE hash = $1 AND repository = $2 AND context = $3 LIMIT 1",
            table = self.fragments_table,
        );
        client
            .query_opt(&sql, &[&hash, &repository, &context])
            .await
            .map(|r| r.is_some())
            .map_err(|e| {
                warn!("PgImmutableStore: exists_exact failed: {e}");
                StoreError::internal(format!("pg exists_exact error: {e}"))
            })
    }

    /// Repository match: hash + repository (any context).
    async fn exists_repository(
        &self,
        hash: &[u8],
        repository: &[u8],
    ) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let sql = format!(
            "SELECT 1 FROM {table} WHERE hash = $1 AND repository = $2 LIMIT 1",
            table = self.fragments_table,
        );
        client
            .query_opt(&sql, &[&hash, &repository])
            .await
            .map(|r| r.is_some())
            .map_err(|e| {
                warn!("PgImmutableStore: exists_repository failed: {e}");
                StoreError::internal(format!("pg exists_repository error: {e}"))
            })
    }

    /// Hash match: any row with this hash (any repository/context).
    async fn exists_hash(&self, hash: &[u8]) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let sql = format!(
            "SELECT 1 FROM {table} WHERE hash = $1 LIMIT 1",
            table = self.fragments_table,
        );
        client
            .query_opt(&sql, &[&hash])
            .await
            .map(|r| r.is_some())
            .map_err(|e| {
                warn!("PgImmutableStore: exists_hash failed: {e}");
                StoreError::internal(format!("pg exists_hash error: {e}"))
            })
    }

    /// Count how many rows exist for this hash across all repositories/contexts.
    async fn count_associations(&self, hash: &[u8]) -> Result<i64, StoreError> {
        let client = self.get_client().await?;
        let sql = format!(
            "SELECT COUNT(*) FROM {table} WHERE hash = $1",
            table = self.fragments_table,
        );
        let row = client
            .query_one(&sql, &[&hash])
            .await
            .map_err(|e| {
                warn!("PgImmutableStore: count_associations failed: {e}");
                StoreError::internal(format!("pg count_associations error: {e}"))
            })?;
        Ok(row.get::<_, i64>(0))
    }

    // -----------------------------------------------------------------------
    // Existence dispatch
    // -----------------------------------------------------------------------

    async fn exists(
        &self,
        repository: Context,
        address: Address,
        match_requested: StoreMatch,
    ) -> Result<bool, StoreError> {
        if match_requested == StoreMatch::MatchNone {
            return Ok(false);
        }

        let hash_bytes: &[u8] = address.hash.as_ref();
        let repo_bytes: &[u8] = repository.as_ref();
        let ctx_bytes: &[u8] = address.context.as_ref();

        let result = match match_requested {
            StoreMatch::MatchFull => {
                self.exists_exact(hash_bytes, repo_bytes, ctx_bytes).await
            }
            StoreMatch::MatchPartition => {
                self.exists_repository(hash_bytes, repo_bytes).await
            }
            StoreMatch::MatchHash => self.exists_hash(hash_bytes).await,
            StoreMatch::MatchNone => Ok(false),
        };

        result.inspect(|matched| {
            if !matched {
                debug!(
                    "Fragment does not exist for repository: {repository} and address: {address} \
                     with match required: {match_requested:?}."
                );
            }
        })
    }

    /// Walk down the match hierarchy until a match is found or all options are exhausted.
    async fn lookup(
        &self,
        repository: Context,
        address: Address,
        match_requested: StoreMatch,
    ) -> Result<StoreMatch, StoreError> {
        let mut match_requested = match_requested;
        let mut found = self.exists(repository, address, match_requested).await?;

        // Full match not found — short circuit; no partial-upload support.
        if !found && match_requested == StoreMatch::MatchFull {
            return Ok(StoreMatch::MatchNone);
        }

        while !found {
            match match_requested.prev() {
                Some(prev) => {
                    match_requested = prev;
                    found = self.exists(repository, address, match_requested).await?;
                }
                None => break,
            }
        }

        Ok(if found {
            match_requested
        } else {
            StoreMatch::MatchNone
        })
    }

    async fn ensure_exists(
        &self,
        repository: Context,
        address: Address,
        match_required: StoreMatch,
    ) -> Result<(), StoreError> {
        if !self.exists(repository, address, match_required).await? {
            return Err(StoreError::from(AddressNotFound::from(address)));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Batch existence (MatchFull only uses direct SQL; others fan out)
    // -----------------------------------------------------------------------

    async fn exist_batch_exact(
        &self,
        repository: Context,
        addresses: &[Address],
    ) -> Result<Vec<StoreMatch>, StoreError> {
        // Build (hash, repo, ctx) tuples and check presence in a single query using unnest.
        let repo_bytes: Vec<u8> = repository.as_ref().to_vec();

        let hashes: Vec<Vec<u8>> = addresses
            .iter()
            .map(|a| a.hash.as_ref().to_vec())
            .collect();
        let contexts: Vec<Vec<u8>> = addresses
            .iter()
            .map(|a| a.context.as_ref().to_vec())
            .collect();

        let client = self.get_client().await?;
        let sql = format!(
            "SELECT i.hash, i.ctx FROM
                unnest($1::bytea[], $2::bytea[]) AS i(hash, ctx)
             WHERE EXISTS (
                SELECT 1 FROM {table} f
                WHERE f.hash = i.hash
                  AND f.repository = $3
                  AND f.context = i.ctx
             )",
            table = self.fragments_table,
        );

        let hashes_ref: Vec<&[u8]> = hashes.iter().map(|v| v.as_slice()).collect();
        let contexts_ref: Vec<&[u8]> = contexts.iter().map(|v| v.as_slice()).collect();

        let rows = client
            .query(&sql, &[&hashes_ref, &contexts_ref, &repo_bytes.as_slice()])
            .await
            .map_err(|e| {
                warn!("PgImmutableStore: exist_batch_exact failed: {e}");
                StoreError::internal(format!("pg exist_batch_exact error: {e}"))
            })?;

        // Build a lookup set of (hash, context) pairs that were found.
        let mut found_set: std::collections::HashSet<(Vec<u8>, Vec<u8>)> =
            std::collections::HashSet::new();
        for row in rows {
            let h: Vec<u8> = row.get(0);
            let c: Vec<u8> = row.get(1);
            found_set.insert((h, c));
        }

        Ok(addresses
            .iter()
            .map(|a| {
                let h = a.hash.as_ref().to_vec();
                let c = a.context.as_ref().to_vec();
                if found_set.contains(&(h, c)) {
                    StoreMatch::MatchFull
                } else {
                    StoreMatch::MatchNone
                }
            })
            .collect())
    }

    async fn exist_batch_inexact(
        &self,
        repository: Context,
        addresses: &[Address],
        match_requested: StoreMatch,
    ) -> Result<Vec<StoreMatch>, StoreError> {
        let mut join_set: JoinSet<(usize, bool)> = JoinSet::new();

        for (pos, &address) in addresses.iter().enumerate() {
            let pool = self.pool.clone();
            let table = self.fragments_table.clone();
            let hash_bytes = address.hash.as_ref().to_vec();
            let repo_bytes: Vec<u8> = repository.as_ref().to_vec();

            join_set.spawn(async move {
                let client = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("PgImmutableStore: exist_batch_inexact pool error: {e}");
                        return (pos, false);
                    }
                };

                let result = match match_requested {
                    StoreMatch::MatchPartition => {
                        let sql = format!(
                            "SELECT 1 FROM {table} WHERE hash = $1 AND repository = $2 LIMIT 1"
                        );
                        client
                            .query_opt(&sql, &[&hash_bytes.as_slice(), &repo_bytes.as_slice()])
                            .await
                            .map(|r| r.is_some())
                    }
                    StoreMatch::MatchHash => {
                        let sql =
                            format!("SELECT 1 FROM {table} WHERE hash = $1 LIMIT 1");
                        client
                            .query_opt(&sql, &[&hash_bytes.as_slice()])
                            .await
                            .map(|r| r.is_some())
                    }
                    _ => Ok(false),
                };

                match result {
                    Ok(found) => (pos, found),
                    Err(e) => {
                        warn!("PgImmutableStore: exist_batch_inexact query error: {e}");
                        (pos, false)
                    }
                }
            });
        }

        let mut output: Vec<StoreMatch> = addresses.iter().map(|_| StoreMatch::MatchNone).collect();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((pos, true)) => output[pos] = match_requested,
                Ok((_, false)) => {}
                Err(e) => {
                    warn!("PgImmutableStore: exist_batch_inexact join error: {e}");
                }
            }
        }

        Ok(output)
    }

    // -----------------------------------------------------------------------
    // Metadata (PG metadata table)
    // -----------------------------------------------------------------------

    async fn write_metadata(
        &self,
        client: &impl deadpool_postgres::GenericClient,
        hash: &[u8],
        fragment: Fragment,
    ) -> Result<(), StoreError> {
        let sql = format!(
            "INSERT INTO {table} (hash, flags, size_payload, size_content)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (hash) DO UPDATE
               SET flags = EXCLUDED.flags,
                   size_payload = EXCLUDED.size_payload,
                   size_content = EXCLUDED.size_content",
            table = self.metadata_table,
        );
        client
            .execute(
                &sql,
                &[
                    &hash,
                    &(fragment.flags as i64),
                    &(fragment.size_payload as i64),
                    &(fragment.size_content as i64),
                ],
            )
            .await
            .map_err(|e| {
                warn!("PgImmutableStore: write_metadata failed: {e}");
                StoreError::internal(format!("pg write_metadata error: {e}"))
            })?;
        Ok(())
    }

    /// CAS update: only update if current row matches `expected`.
    /// Returns Ok(()) on success, Err(internal) on conflict.
    async fn update_metadata_cas(
        &self,
        hash: &[u8],
        updated: Fragment,
        expected: Fragment,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let sql = format!(
            "UPDATE {table}
             SET flags = $1, size_payload = $2, size_content = $3
             WHERE hash = $4
               AND flags = $5
               AND size_payload = $6
               AND size_content = $7",
            table = self.metadata_table,
        );
        let rows_affected = client
            .execute(
                &sql,
                &[
                    &(updated.flags as i64),
                    &(updated.size_payload as i64),
                    &(updated.size_content as i64),
                    &hash,
                    &(expected.flags as i64),
                    &(expected.size_payload as i64),
                    &(expected.size_content as i64),
                ],
            )
            .await
            .map_err(|e| {
                warn!("PgImmutableStore: update_metadata_cas failed: {e}");
                StoreError::internal(format!("pg update_metadata_cas error: {e}"))
            })?;

        if rows_affected == 0 {
            warn!("PgImmutableStore: update_metadata_cas conflict for hash {}", hex::encode(hash));
            Err(StoreError::internal("Failed to update metadata due to conflict"))
        } else {
            Ok(())
        }
    }

    async fn load_metadata(&self, hash: Hash) -> Result<Fragment, StoreError> {
        let client = self.get_client().await?;
        let hash_bytes: &[u8] = hash.as_ref();
        let sql = format!(
            "SELECT flags, size_payload, size_content FROM {table} WHERE hash = $1",
            table = self.metadata_table,
        );
        let row = client
            .query_opt(&sql, &[&hash_bytes])
            .await
            .map_err(|e| {
                // Transport / SQL error — not a "row absent" signal; preserve the real cause.
                warn!("PgImmutableStore: load_metadata query failed: {e}");
                StoreError::internal(format!("pg load_metadata error: {e}"))
            })?;

        match row {
            Some(r) => {
                let flags: i64 = r.get(0);
                let size_payload: i64 = r.get(1);
                let size_content: i64 = r.get(2);
                Ok(Fragment {
                    flags: flags as u32,
                    size_payload: size_payload as u32,
                    size_content: size_content as u64,
                })
            }
            None => {
                warn!("PgImmutableStore: no metadata found for hash {}", hex::encode(hash.as_ref()));
                Err(StoreError::from(AddressNotFound::from(
                    Address::zero_context_hash(hash),
                )))
            }
        }
    }

    async fn metadata_with_size_validation(&self, hash: Hash) -> Result<Fragment, StoreError> {
        let metadata = self.load_metadata(hash).await?;
        lore_storage::validate_fragment_size(&metadata)?;
        Ok(metadata)
    }

    async fn metadata_with_load_validation(&self, hash: Hash) -> Result<Fragment, StoreError> {
        let metadata = self.metadata_with_size_validation(hash).await?;
        if (metadata.flags & FragmentFlags::PayloadObliteration) != 0 {
            return Err(StoreError::from(AddressNotFound::from(
                Address::zero_context_hash(hash),
            )));
        }
        Ok(metadata)
    }

    // -----------------------------------------------------------------------
    // Fragment association table
    // -----------------------------------------------------------------------

    async fn associate_fragment(
        &self,
        repository: Context,
        address: Address,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        self.associate_fragment_with(&client, repository, address)
            .await
    }

    async fn associate_fragment_with(
        &self,
        client: &impl deadpool_postgres::GenericClient,
        repository: Context,
        address: Address,
    ) -> Result<(), StoreError> {
        let hash_bytes: &[u8] = address.hash.as_ref();
        let repo_bytes: &[u8] = repository.as_ref();
        let ctx_bytes: &[u8] = address.context.as_ref();
        let sql = format!(
            "INSERT INTO {table} (hash, repository, context)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
            table = self.fragments_table,
        );
        client
            .execute(&sql, &[&hash_bytes, &repo_bytes, &ctx_bytes])
            .await
            .map_err(|e| {
                warn!("PgImmutableStore: associate_fragment failed: {e}");
                StoreError::internal(format!("pg associate_fragment error: {e}"))
            })?;
        Ok(())
    }

    async fn delete_association(
        &self,
        repository: Context,
        address: Address,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let hash_bytes: &[u8] = address.hash.as_ref();
        let repo_bytes: &[u8] = repository.as_ref();
        let ctx_bytes: &[u8] = address.context.as_ref();
        let sql = format!(
            "DELETE FROM {table} WHERE hash = $1 AND repository = $2 AND context = $3",
            table = self.fragments_table,
        );
        client
            .execute(&sql, &[&hash_bytes, &repo_bytes, &ctx_bytes])
            .await
            .map_err(|e| {
                warn!("PgImmutableStore: delete_association failed: {e}");
                StoreError::internal(format!("pg delete_association error: {e}"))
            })?;
        Ok(())
    }

    async fn has_associations(&self, hash: Hash) -> Result<bool, StoreError> {
        let count = self.count_associations(hash.as_ref()).await?;
        Ok(count > 0)
    }

    // -----------------------------------------------------------------------
    // S3 / R2 payload operations
    // -----------------------------------------------------------------------

    fn hash_to_hex(hash: Hash) -> String {
        let mut dst = [0u8; 64];
        lore_revision::util::to_hex_str(hash.data(), &mut dst).to_owned()
    }

    async fn write_payload(
        &self,
        repository: Context,
        address: Address,
        fragment: Fragment,
        payload: Bytes,
    ) -> Result<(), StoreError> {
        if payload.len() != fragment.size_payload as usize {
            warn!(
                "PgImmutableStore: payload size mismatch for {}: expected {} got {}",
                address.hash,
                fragment.size_payload,
                payload.len()
            );
            return Err(StoreError::internal(format!(
                "Failed to store in immutable store for put {}",
                address.hash
            )));
        }

        let key = Self::hash_to_hex(address.hash);

        // S3 write happens first — if the PG transaction below fails the object is
        // unreachable until a later re-put (self-healing, same as the AWS path).
        self.s3
            .put_object(self.bucket.as_str(), &key, payload.to_vec())
            .await
            .map(|_| ())
            .map_err(|e| {
                warn!("PgImmutableStore: S3 put_object failed for {}: {e:?}", address.hash);
                StoreError::internal_with_context(e, "S3 put object failed")
            })?;

        // Both PG index writes (metadata upsert + fragment association) run inside a
        // single transaction so they are committed atomically — no partial-index window.
        let mut client = self.get_client().await?;
        let txn = client.transaction().await.map_err(|e| {
            warn!("PgImmutableStore: BEGIN failed for write_payload: {e}");
            StoreError::internal(format!("pg begin error: {e}"))
        })?;
        self.write_metadata(&txn, address.hash.as_ref(), fragment).await?;
        self.associate_fragment_with(&txn, repository, address).await?;
        txn.commit().await.map_err(|e| {
            warn!("PgImmutableStore: COMMIT failed for write_payload: {e}");
            StoreError::internal(format!("pg commit error: {e}"))
        })?;

        Ok(())
    }

    /// Delete a payload from R2. R2 does NOT support ListObjectVersions, so we use a
    /// plain DeleteObject with no version_id (R2 objects are never versioned).
    async fn delete_payload(&self, hash: Hash) -> Result<(), StoreError> {
        let key = Self::hash_to_hex(hash);
        self.s3
            .delete_object(self.bucket.as_str(), &key, None)
            .await
            .map(|_| ())
            .map_err(|e| {
                warn!("PgImmutableStore: S3 delete_object failed for {}: {e:?}", hash);
                StoreError::internal_with_context(e, "S3 delete object failed")
            })
    }

    async fn get_s3_object_contents(
        &self,
        hash: Hash,
    ) -> Result<(BytesMut, usize), StoreError> {
        let key = Self::hash_to_hex(hash);
        let mut output = self
            .s3
            .get_object(self.bucket.as_str(), &key, None)
            .await
            .map_err(|e| {
                if let AwsError::AwsSdkError(sdk_error) = e {
                    debug!(
                        hash = %hash,
                        error = ?sdk_error,
                        "PgImmutableStore: S3 get_object SDK error"
                    );
                    match sdk_error.into_service_error() {
                        GetObjectError::NoSuchKey(_) => StoreError::from(AddressNotFound::from(
                            Address::zero_context_hash(hash),
                        )),
                        _ => StoreError::from(SlowDown),
                    }
                } else {
                    StoreError::internal_with_context(e, "S3 get object failed")
                }
            })?;

        let mut buffer = BytesMut::with_capacity(FRAGMENT_SIZE_THRESHOLD);
        let mut read = 0_usize;
        while let Some(bytes) = output.body.next().await {
            let bytes = bytes.map_err(|e| {
                warn!("PgImmutableStore: S3 stream read error for {hash}: {e:?}");
                StoreError::internal_with_context(e, "Failed to read bytes from S3 response stream")
            })?;
            read += bytes.len();
            buffer.extend_from_slice(bytes.as_ref());
        }

        Ok((buffer, read))
    }

    fn read_payload(
        &self,
        bytes: BytesMut,
        read: usize,
        hash: Hash,
        fragment: Fragment,
    ) -> Result<Bytes, StoreError> {
        let payload_size = fragment.size_payload as usize;
        let buffer_size = bytes.len();

        // Migration compat: if size is off by exactly Fragment header size, strip prefix.
        let buffer = if buffer_size > payload_size
            && (buffer_size - payload_size) == size_of::<Fragment>()
            && self.force_write
        {
            let mut b = bytes;
            b.split_off(size_of::<Fragment>()).freeze()
        } else {
            bytes.freeze()
        };

        if buffer.len() == payload_size {
            Ok(buffer)
        } else {
            warn!(
                "PgImmutableStore: size mismatch for {hash}: expected {payload_size} got {buffer_size} ({read} bytes read)"
            );
            Err(StoreError::internal(format!(
                "Failed to load from immutable store, size mismatch (load {buffer_size}, expected {payload_size}) for get {hash}"
            )))
        }
    }

    async fn load(&self, hash: Hash) -> Result<(Fragment, Bytes), StoreError> {
        let metadata_fut = self.metadata_with_load_validation(hash);
        let s3_fut = self.get_s3_object_contents(hash);
        tokio::pin!(metadata_fut, s3_fut);

        let mut s3_result = None;
        let metadata_result = loop {
            tokio::select! {
                result = &mut metadata_fut => break result,
                result = &mut s3_fut, if s3_result.is_none() => {
                    s3_result = Some(result);
                }
            }
        };

        let fragment = metadata_result?;

        let (bytes, read) = match s3_result {
            Some(r) => r?,
            None => s3_fut.await?,
        };

        let payload = self.read_payload(bytes, read, hash, fragment)?;
        Ok((fragment, payload))
    }

    // -----------------------------------------------------------------------
    // Query helper
    // -----------------------------------------------------------------------

    async fn do_query(
        &self,
        repository: Context,
        address: Address,
        match_requested: StoreMatch,
        hide_obliterates: bool,
    ) -> Result<StoreQueryResult, StoreError> {
        let match_made = self.lookup(repository, address, match_requested).await?;

        if match_made == StoreMatch::MatchNone {
            return Ok(StoreQueryResult {
                fragment: Fragment::default(),
                match_made,
            });
        }

        let fragment = self.load_metadata(address.hash).await.map_err(|e| {
            warn!(
                "PgImmutableStore: load_metadata failed for {address:?} in {repository:?}: {e:?}"
            );
            StoreError::internal_with_context(e, "Failed to load metadata after fragment lookup")
        })?;

        if (fragment.flags & FragmentFlags::PayloadObliteration) != 0 && hide_obliterates {
            debug!("PgImmutableStore: query found obliterated fragment at {address}");
            Ok(StoreQueryResult {
                fragment: Fragment::default(),
                match_made: StoreMatch::MatchNone,
            })
        } else {
            Ok(StoreQueryResult {
                fragment,
                match_made,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// ImmutableStore trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl ImmutableStoreTrait for PgImmutableStore {
    async fn exist(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        match_requested: StoreMatch,
    ) -> Result<StoreMatch, StoreError> {
        let repository: Context = partition.into();
        if self.exists(repository, address, match_requested).await? {
            Ok(match_requested)
        } else {
            Ok(StoreMatch::MatchNone)
        }
    }

    async fn exist_batch(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        match_requested: StoreMatch,
    ) -> Result<Vec<StoreMatch>, StoreError> {
        let repository: Context = partition.into();
        match match_requested {
            StoreMatch::MatchNone => {
                Ok(addresses.iter().map(|_| StoreMatch::MatchNone).collect())
            }
            StoreMatch::MatchFull => self.exist_batch_exact(repository, addresses).await,
            StoreMatch::MatchHash | StoreMatch::MatchPartition => {
                self.exist_batch_inexact(repository, addresses, match_requested)
                    .await
            }
        }
    }

    async fn query(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        match_requested: StoreMatch,
    ) -> Result<StoreQueryResult, StoreError> {
        let repository: Context = partition.into();
        self.do_query(
            repository,
            address,
            match_requested,
            true, /* hide obliterates */
        )
        .await
    }

    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        match_required: StoreMatch,
    ) -> Result<(Fragment, Bytes), StoreError> {
        let repository: Context = partition.into();

        // Run existence and S3 load concurrently; existence error takes priority.
        let exists_fut = self.ensure_exists(repository, address, match_required);
        let load_fut = self.load(address.hash);
        tokio::pin!(exists_fut, load_fut);

        let mut load_result = None;
        let exists_result = loop {
            tokio::select! {
                result = &mut exists_fut => break result,
                result = &mut load_fut, if load_result.is_none() => {
                    load_result = Some(result);
                }
            }
        };
        exists_result?;

        let (fragment, payload) = match load_result {
            Some(r) => r?,
            None => load_fut.await?,
        };

        lore_storage::validate_fragment_payload(&fragment, payload.len())?;
        Ok((fragment, payload))
    }

    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        mut fragment: Fragment,
        payload: Option<Bytes>,
        _force: bool,
    ) -> Result<(), StoreError> {
        sanitise_fragment_behavior_flags(&mut fragment);

        if let Some(payload) = payload.as_ref() {
            lore_storage::validate_fragment_payload(&fragment, payload.len())?;
        } else {
            lore_storage::validate_fragment_size(&fragment)?;
        }

        let repository: Context = partition.into();

        let query = self
            .do_query(
                repository,
                address,
                StoreMatch::MatchFull,
                false, /* don't hide obliterates */
            )
            .await;

        let match_made = if !self.force_write && query.is_ok() {
            let query = query?;

            if (query.fragment.flags & FragmentFlags::PayloadObliterating)
                == FragmentFlags::PayloadObliterating
            {
                info!(
                    "PgImmutableStore: put at {address} is currently being obliterated"
                );
                return Err(StoreError::internal(format!(
                    "Failed to obliterate immutable {address}"
                )));
            }

            if query.match_made != StoreMatch::MatchNone
                && fragment.size_content != query.fragment.size_content
                && (query.fragment.flags & FragmentFlags::PayloadObliterated)
                    != FragmentFlags::PayloadObliterated
            {
                return Err(StoreError::internal("Hash collision"));
            }

            query.match_made
        } else {
            if let Err(e) = query {
                warn!(
                    "PgImmutableStore: query failed for {address:?} in {repository}: {e:?}"
                );
            }
            StoreMatch::MatchNone
        };

        match match_made {
            // Already exists with full match — nothing to do.
            StoreMatch::MatchFull => Ok(()),

            // Hash + repo match — associate the new context. Payload already in R2.
            StoreMatch::MatchPartition => {
                self.associate_fragment(repository, address).await
            }

            // Hash-only match + payload provided — associate.
            StoreMatch::MatchHash if payload.is_some() => {
                self.associate_fragment(repository, address).await
            }

            // No match + payload provided — full write.
            StoreMatch::MatchNone if payload.is_some() => {
                self.write_payload(repository, address, fragment, payload.unwrap())
                    .await
            }

            // No payload and no existing record — cannot proceed.
            StoreMatch::MatchHash | StoreMatch::MatchNone => {
                Err(StoreError::internal("Payload buffer required"))
            }
        }
    }

    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        let repository: Context = partition.into();

        let original_metadata = self
            .metadata_with_size_validation(address.hash)
            .await?;

        info!("PgImmutableStore: obliterate original metadata: {original_metadata:?}");

        // Acquire the obliteration lock via CAS.
        let updated_metadata =
            if original_metadata.flags & FragmentFlags::PayloadObliteration == 0 {
                let mut updated = original_metadata;
                updated.flags |= FragmentFlags::PayloadObliterating;

                self.update_metadata_cas(address.hash.as_ref(), updated, original_metadata)
                    .await?;

                info!("PgImmutableStore: acquired obliteration lock for {address}");
                updated
            } else {
                info!(
                    "PgImmutableStore: fragment {address} is already being or has been obliterated"
                );
                return Ok(());
            };

        // If fragmented, obliterate each sub-fragment first.
        if updated_metadata.flags & FragmentFlags::PayloadFragmented != 0 {
            info!("PgImmutableStore: fragment {address} is fragmented, obliterating sub-fragments");
            let (bytes, read) = self.get_s3_object_contents(address.hash).await?;
            let payload = self
                .read_payload(bytes, read, address.hash, original_metadata)?
                .to_aligned::<FragmentReference>();
            let sub_fragments = payload.as_type_slice::<FragmentReference>();
            info!(
                "PgImmutableStore: {} sub-fragments to obliterate",
                sub_fragments.len()
            );

            let mut join_set: JoinSet<Result<(), (Address, StoreError)>> = JoinSet::new();
            for reference in sub_fragments.iter() {
                let self_clone = self.clone();
                let stats = stats.clone();
                let sub_address = Address {
                    hash: reference.hash,
                    context: address.context,
                };
                join_set.spawn(async move {
                    self_clone
                        .obliterate(repository.into(), sub_address, stats)
                        .await
                        .map_err(|e| (sub_address, e))
                });
            }

            let mut failures = false;
            while let Some(result) = join_set.join_next().await {
                match result {
                    Err(e) => {
                        failures = true;
                        warn!("PgImmutableStore: join error in sub-fragment obliterate: {e:?}");
                    }
                    Ok(Err((sub_addr, e))) => {
                        failures = true;
                        warn!(
                            "PgImmutableStore: sub-fragment obliterate failed for {sub_addr}: {e:?}"
                        );
                    }
                    Ok(Ok(())) => {}
                }
            }

            if failures {
                warn!("PgImmutableStore: obliterate sub-fragment failures for {address}");
                return Err(StoreError::internal(format!(
                    "Failed to obliterate immutable {address}"
                )));
            }
        }

        self.delete_association(repository, address).await?;
        stats.num_fragments.fetch_add(1, Ordering::Relaxed);

        let remain_associated = self.has_associations(address.hash).await?;
        if remain_associated {
            info!("PgImmutableStore: {address} still has associations, restoring metadata");
            return self
                .update_metadata_cas(address.hash.as_ref(), original_metadata, updated_metadata)
                .await
                .inspect_err(|e| {
                    warn!(
                        "PgImmutableStore: failed to restore metadata for {address}: {e:?}"
                    );
                });
        }

        self.delete_payload(address.hash).await?;
        stats.num_payloads.fetch_add(1, Ordering::Relaxed);

        let mut obliterated = updated_metadata;
        obliterated.flags = FragmentFlags::PayloadObliterated.bits();
        obliterated.size_payload = 0;
        obliterated.size_content = 0;

        self.update_metadata_cas(address.hash.as_ref(), obliterated, updated_metadata)
            .await
            .inspect_err(|e| {
                warn!(
                    "PgImmutableStore: failed to finalize obliterate for {address}: {e:?}"
                );
            })
    }

    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        _durable: bool,
    ) -> Result<(), StoreError> {
        let source_repository: Context = source_partition.into();
        let destination_repository: Context = destination_partition.into();
        let destination_address = Address {
            hash: source_address.hash,
            context: destination_context,
        };

        let query = self
            .do_query(source_repository, source_address, StoreMatch::MatchFull, false)
            .await?;

        if query.match_made != StoreMatch::MatchFull {
            return Err(StoreError::from(AddressNotFound::from(source_address)));
        }

        self.associate_fragment(destination_repository, destination_address)
            .await
    }

    async fn evict(
        self: Arc<Self>,
        _max_capacity: usize,
        _sync_data: bool,
    ) -> Result<usize, StoreError> {
        // Remote store never evicts.
        Ok(0)
    }

    async fn compact(
        self: Arc<Self>,
        _max_size: usize,
        _at: Option<usize>,
        _sync_data: bool,
    ) -> Result<Option<usize>, StoreError> {
        Ok(None)
    }

    async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
        None
    }

    async fn compact_stop(self: Arc<Self>) {}

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        // Postgres writes are immediately durable; R2 puts are synchronous. Nothing to flush.
        Ok(())
    }

    async fn verify(self: Arc<Self>, _heal: bool) -> Result<(), StoreError> {
        Ok(())
    }

    fn max_query_batch(&self) -> Option<usize> {
        // Postgres can handle large batches; mirror the AWS 100-item limit as a safe default.
        Some(100)
    }

    async fn fragment_count(self: Arc<Self>) -> Option<usize> {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!("PgImmutableStore: fragment_count pool error: {e}");
                return None;
            }
        };
        let sql = format!(
            "SELECT COUNT(*) FROM {table}",
            table = self.metadata_table,
        );
        match client.query_one(&sql, &[]).await {
            Ok(row) => {
                let count: i64 = row.get(0);
                Some(count as usize)
            }
            Err(e) => {
                warn!("PgImmutableStore: fragment_count query failed: {e}");
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    //! Integration tests against real Postgres + MinIO/R2-compatible S3.
    //!
    //! Skipped unless both env vars are set. Run with:
    //!
    //! ```text
    //! PG_TEST_DSN="host=localhost port=5435 user=postgres password=lore dbname=postgres" \
    //! S3_TEST_ENDPOINT="http://localhost:9000" \
    //! S3_TEST_BUCKET="lore-test" \
    //! AWS_ACCESS_KEY_ID=minio \
    //! AWS_SECRET_ACCESS_KEY=minio12345 \
    //! AWS_REGION=us-east-1 \
    //! cargo test -p lore-pg --lib pg_immutable_store -- --ignored --test-threads=1
    //! ```

    use std::sync::Arc;
    use std::time::Duration;

    use aws_smithy_http_client::Builder as HttpClientBuilder;
    use aws_smithy_http_client::Connector;
    use aws_smithy_http_client::tls;
    use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
    use aws_smithy_runtime_api::client::behavior_version::BehaviorVersion;
    use deadpool_postgres::Config as DeadpoolConfig;
    use deadpool_postgres::Runtime;
    use lore_base::types::Address;
    use lore_base::types::Context;
    use lore_base::types::Fragment;
    use lore_base::types::Partition;
    use lore_storage::ImmutableStore;
    use lore_storage::StoreMatch;
    use lore_storage::StoreObliterateStats;
    use rand::random;

    use super::PgImmutableStore;
    use super::PgImmutableStoreSettings;
    use crate::s3::S3Impl;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn random_bytes(len: usize) -> bytes::Bytes {
        let data: Vec<u8> = (0..len).map(|_| random::<u8>()).collect();
        bytes::Bytes::from(data)
    }

    /// Build a fragment and address for the given payload.
    /// Uses a random Hash since content-addressing correctness is not what we are testing —
    /// we just need a stable unique key per payload in each test.
    fn make_fragment_and_address(
        payload: &bytes::Bytes,
        context: Context,
    ) -> (Address, Fragment) {
        // Use a random hash to avoid collisions across test runs.
        let hash = random::<[u8; 32]>().into();
        let address = Address { hash, context };
        let fragment = Fragment {
            flags: 0,
            size_payload: payload.len() as u32,
            size_content: payload.len() as u64,
        };
        (address, fragment)
    }

    async fn setup_store(suffix: &str) -> Option<Arc<PgImmutableStore>> {
        let dsn = std::env::var("PG_TEST_DSN").ok()?;
        let endpoint = std::env::var("S3_TEST_ENDPOINT").ok()?;
        let bucket = std::env::var("S3_TEST_BUCKET").ok()?;

        let mut cfg = DeadpoolConfig::new();
        cfg.url = Some(dsn);
        let connector = crate::tls::make_connector().ok()?;
        let pool = cfg.create_pool(Some(Runtime::Tokio1), connector).ok()?;

        // Build an HTTP client with Rustls+Ring — same as clients.rs — because
        // the default-https-client feature is not enabled in this crate.
        let http_client = HttpClientBuilder::new().build_with_connector_fn(|_, _| {
            Connector::builder()
                .tls_provider(tls::Provider::Rustls(CryptoMode::Ring))
                .build()
        });

        // Use a concrete region rather than RegionProviderChain::default_provider():
        // the default chain tries to build an IMDS client which panics when no HTTP
        // client is wired up yet (before the config is loaded).
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_owned());
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .http_client(http_client)
            .region(aws_types::region::Region::new(region))
            .endpoint_url(&endpoint)
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(true)
            // R2 fix: only send checksums when the endpoint requires them.
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
        let s3 = S3Impl::new(s3_client, Duration::from_secs(30));

        let settings = PgImmutableStoreSettings::new(
            format!("immutable_fragments_{suffix}"),
            format!("immutable_metadata_{suffix}"),
            bucket,
        )
        .with_endpoint(endpoint);

        let store = Arc::new(PgImmutableStore::new(pool.clone(), s3, &settings));

        store.ensure_schema().await.ok()?;

        // Clean slate for each test run.
        let client = pool.get().await.ok()?;
        client
            .batch_execute(&format!(
                "DELETE FROM {}; DELETE FROM {}",
                settings.fragments_table, settings.metadata_table,
            ))
            .await
            .ok()?;

        Some(store)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// put followed by get returns bytes exactly.
    #[tokio::test]
    #[ignore]
    async fn test_put_get_roundtrip() {
        let store = setup_store("rtrip").await.expect("env not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let ctx: Context = random::<[u8; 16]>().into();
        let payload = random_bytes(64);
        let (address, fragment) = make_fragment_and_address(&payload, ctx);

        store
            .clone()
            .put(partition, address, fragment, Some(payload.clone()), false)
            .await
            .expect("put should succeed");

        let (returned_fragment, returned_payload) = store
            .clone()
            .get(partition, address, StoreMatch::MatchFull)
            .await
            .expect("get should succeed");

        assert_eq!(returned_payload, payload);
        assert_eq!(returned_fragment.size_payload, fragment.size_payload);
        assert_eq!(returned_fragment.size_content, fragment.size_content);
    }

    /// exist returns MatchFull after put; MatchNone for unknown address.
    #[tokio::test]
    #[ignore]
    async fn test_exist_true_and_false() {
        let store = setup_store("exist").await.expect("env not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let ctx: Context = random::<[u8; 16]>().into();
        let payload = random_bytes(32);
        let (address, fragment) = make_fragment_and_address(&payload, ctx);

        // Before put: MatchNone.
        let before = store
            .clone()
            .exist(partition, address, StoreMatch::MatchFull)
            .await
            .expect("exist should not error");
        assert_eq!(before, StoreMatch::MatchNone);

        store
            .clone()
            .put(partition, address, fragment, Some(payload), false)
            .await
            .expect("put");

        // After put: MatchFull.
        let after = store
            .clone()
            .exist(partition, address, StoreMatch::MatchFull)
            .await
            .expect("exist should not error");
        assert_eq!(after, StoreMatch::MatchFull);
    }

    /// exist_batch returns correct matches for mixed known/unknown addresses.
    #[tokio::test]
    #[ignore]
    async fn test_exist_batch() {
        let store = setup_store("batch").await.expect("env not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let ctx: Context = random::<[u8; 16]>().into();

        let payload_a = random_bytes(32);
        let payload_b = random_bytes(48);
        let (address_a, fragment_a) = make_fragment_and_address(&payload_a, ctx);
        let (address_b, fragment_b) = make_fragment_and_address(&payload_b, ctx);
        let unknown_address = Address {
            hash: random::<[u8; 32]>().into(),
            context: ctx,
        };

        store
            .clone()
            .put(partition, address_a, fragment_a, Some(payload_a), false)
            .await
            .expect("put a");
        store
            .clone()
            .put(partition, address_b, fragment_b, Some(payload_b), false)
            .await
            .expect("put b");

        let addresses = [address_a, unknown_address, address_b];
        let results = store
            .clone()
            .exist_batch(partition, &addresses, StoreMatch::MatchFull)
            .await
            .expect("exist_batch");

        assert_eq!(results[0], StoreMatch::MatchFull);
        assert_eq!(results[1], StoreMatch::MatchNone);
        assert_eq!(results[2], StoreMatch::MatchFull);
    }

    /// query returns fragment metadata for known address.
    #[tokio::test]
    #[ignore]
    async fn test_query() {
        let store = setup_store("query").await.expect("env not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let ctx: Context = random::<[u8; 16]>().into();
        let payload = random_bytes(100);
        let (address, fragment) = make_fragment_and_address(&payload, ctx);

        store
            .clone()
            .put(partition, address, fragment, Some(payload), false)
            .await
            .expect("put");

        let result = store
            .clone()
            .query(partition, address, StoreMatch::MatchFull)
            .await
            .expect("query");

        assert_eq!(result.match_made, StoreMatch::MatchFull);
        assert_eq!(result.fragment.size_payload, fragment.size_payload);
    }

    /// fragment_count tracks the number of unique metadata entries.
    #[tokio::test]
    #[ignore]
    async fn test_fragment_count() {
        let store = setup_store("fcount").await.expect("env not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let ctx: Context = random::<[u8; 16]>().into();

        let before = store.clone().fragment_count().await.unwrap_or(0);

        let payload = random_bytes(64);
        let (address, fragment) = make_fragment_and_address(&payload, ctx);
        store
            .clone()
            .put(partition, address, fragment, Some(payload), false)
            .await
            .expect("put");

        let after = store.clone().fragment_count().await.unwrap_or(0);
        assert_eq!(after, before + 1);
    }

    /// obliterate removes the payload and marks exist as false.
    #[tokio::test]
    #[ignore]
    async fn test_obliterate() {
        let store = setup_store("oblit").await.expect("env not set");
        let partition: Partition = random::<[u8; 16]>().into();
        let ctx: Context = random::<[u8; 16]>().into();
        let payload = random_bytes(64);
        let (address, fragment) = make_fragment_and_address(&payload, ctx);

        store
            .clone()
            .put(partition, address, fragment, Some(payload), false)
            .await
            .expect("put");

        let stats = Arc::new(StoreObliterateStats::default());
        store
            .clone()
            .obliterate(partition, address, stats.clone())
            .await
            .expect("obliterate");

        // exist should now return MatchNone (the query path hides obliterated entries).
        let after = store
            .clone()
            .exist(partition, address, StoreMatch::MatchFull)
            .await
            .expect("exist after obliterate");
        assert_eq!(after, StoreMatch::MatchNone);

        assert_eq!(stats.num_payloads.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    /// flush is a no-op but must not error.
    #[tokio::test]
    #[ignore]
    async fn test_flush() {
        let store = setup_store("flush").await.expect("env not set");
        store
            .clone()
            .flush(false)
            .await
            .expect("flush should succeed");
    }
}
