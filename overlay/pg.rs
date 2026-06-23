// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Postgres + R2 (S3-compatible) store plugin factories.
//!
//! This module provides plugin factories for Postgres/R2-backed stores:
//! - [`PgImmutableStorePluginFactory`] - Creates Postgres+R2-backed immutable stores
//! - [`PgMutableStorePluginFactory`] - Creates Postgres-backed mutable stores
//! - [`PgLockStorePluginFactory`] - Creates Postgres-backed lock stores
//!
//! All three factories read from the shared `[plugins.pg]` TOML table.
//! Because `serde` does not enforce `deny_unknown_fields` here, each config
//! struct silently ignores fields that belong to the other two stores.

use std::sync::Arc;

use lore_base::error::PluginConfigError;
use lore_base::error::PluginInitError;
use lore_base::runtime::runtime;
use lore_pg::store::pg_immutable_store::PgImmutableStore;
use lore_pg::store::pg_lock_store::PgLockStore;
use lore_pg::store::pg_mutable_store::PgMutableStore;
use lore_revision::lock::LockStore;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use serde::Deserialize;
use tracing::info;

use crate::plugins::ImmutableStorePluginFactory;
use crate::plugins::LockStorePluginFactory;
use crate::plugins::MutableStorePluginFactory;
use crate::plugins::PluginError;
use crate::plugins::PluginRegistry;

const PLUGIN_NAME: &str = "pg";

// =============================================================================
// Configuration Structs
// =============================================================================

/// Configuration for the Postgres immutable store plugin (reads `[plugins.pg]`).
#[derive(Debug, Clone, Deserialize)]
pub struct PgImmutableStorePluginConfig {
    /// libpq DSN, e.g. "host=localhost user=postgres password=secret dbname=lore"
    pub dsn: String,

    /// R2/S3 bucket name for storing fragment payloads.
    pub s3_bucket: String,

    /// Optional S3/R2 endpoint URL (required for non-AWS services like MinIO or R2).
    #[serde(default)]
    pub s3_endpoint_url: Option<String>,

    /// Optional AWS region (defaults to "auto" for R2).
    #[serde(default)]
    pub s3_region: Option<String>,

    /// Force S3 path-style addressing — required for R2/MinIO behind non-AWS hostnames.
    #[serde(default = "default_true")]
    pub s3_force_path_style: bool,

    /// Postgres table name for fragment associations.
    #[serde(default = "default_fragments_table")]
    pub fragments_table: String,

    /// Postgres table name for fragment metadata.
    #[serde(default = "default_metadata_table")]
    pub metadata_table: String,
}

/// Configuration for the Postgres mutable store plugin (reads `[plugins.pg]`).
#[derive(Debug, Clone, Deserialize)]
pub struct PgMutableStorePluginConfig {
    /// libpq DSN.
    pub dsn: String,

    /// Postgres table name for the mutable store.
    #[serde(default = "default_mutable_table")]
    pub mutable_table: String,
}

/// Configuration for the Postgres lock store plugin (reads `[plugins.pg]`).
#[derive(Debug, Clone, Deserialize)]
pub struct PgLockStorePluginConfig {
    /// libpq DSN.
    pub dsn: String,

    /// Postgres table name for distributed locks.
    #[serde(default = "default_locks_table")]
    pub locks_table: String,
}

fn default_true() -> bool {
    true
}

fn default_fragments_table() -> String {
    "fragments".to_string()
}

fn default_metadata_table() -> String {
    "fragments_meta".to_string()
}

fn default_mutable_table() -> String {
    "mutable_store".to_string()
}

fn default_locks_table() -> String {
    "locks".to_string()
}

// =============================================================================
// Plugin Factory Implementations
// =============================================================================

/// Plugin factory for creating Postgres+R2 immutable stores.
pub struct PgImmutableStorePluginFactory;

impl ImmutableStorePluginFactory for PgImmutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let _: PgImmutableStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: format!("Failed to deserialize pg immutable store config: {e}"),
            })
        })?;
        Ok(())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn ImmutableStore>, PluginError> {
        let cfg: PgImmutableStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: format!("Failed to deserialize pg immutable store config: {e}"),
            })
        })?;

        info!(
            plugin_name = PLUGIN_NAME,
            s3_bucket = %cfg.s3_bucket,
            fragments_table = %cfg.fragments_table,
            metadata_table = %cfg.metadata_table,
            "Creating Pg immutable store",
        );

        let store = tokio::task::block_in_place(|| {
            runtime().block_on(async {
                PgImmutableStore::from_config(
                    &cfg.dsn,
                    &cfg.s3_bucket,
                    cfg.s3_endpoint_url.as_deref(),
                    cfg.s3_region.as_deref(),
                    cfg.s3_force_path_style,
                    &cfg.fragments_table,
                    &cfg.metadata_table,
                )
                .await
                .map_err(|e| {
                    PluginError::from(PluginInitError {
                        plugin_name: PLUGIN_NAME.to_string(),
                        message: format!("Failed to create pg immutable store: {e}"),
                    })
                })
            })
        })?;

        Ok(store)
    }
}

/// Plugin factory for creating Postgres mutable stores.
pub struct PgMutableStorePluginFactory;

impl MutableStorePluginFactory for PgMutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let _: PgMutableStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: format!("Failed to deserialize pg mutable store config: {e}"),
            })
        })?;
        Ok(())
    }

    fn create(
        &self,
        config: &toml::Value,
        _immutable_store: Arc<dyn ImmutableStore>,
    ) -> Result<Arc<dyn MutableStore>, PluginError> {
        // PgMutableStore does not need the immutable store — ignore it.
        let cfg: PgMutableStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: format!("Failed to deserialize pg mutable store config: {e}"),
            })
        })?;

        info!(
            plugin_name = PLUGIN_NAME,
            mutable_table = %cfg.mutable_table,
            "Creating Pg mutable store",
        );

        let store = tokio::task::block_in_place(|| {
            runtime().block_on(async {
                PgMutableStore::from_config(&cfg.dsn, &cfg.mutable_table)
                    .await
                    .map_err(|e| {
                        PluginError::from(PluginInitError {
                            plugin_name: PLUGIN_NAME.to_string(),
                            message: format!("Failed to create pg mutable store: {e}"),
                        })
                    })
            })
        })?;

        Ok(store)
    }
}

/// Plugin factory for creating Postgres lock stores.
pub struct PgLockStorePluginFactory;

impl LockStorePluginFactory for PgLockStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let _: PgLockStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: format!("Failed to deserialize pg lock store config: {e}"),
            })
        })?;
        Ok(())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn LockStore>, PluginError> {
        let cfg: PgLockStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: format!("Failed to deserialize pg lock store config: {e}"),
            })
        })?;

        info!(
            plugin_name = PLUGIN_NAME,
            locks_table = %cfg.locks_table,
            "Creating Pg lock store",
        );

        let store = tokio::task::block_in_place(|| {
            runtime().block_on(async {
                PgLockStore::from_config(&cfg.dsn, &cfg.locks_table)
                    .await
                    .map_err(|e| {
                        PluginError::from(PluginInitError {
                            plugin_name: PLUGIN_NAME.to_string(),
                            message: format!("Failed to create pg lock store: {e}"),
                        })
                    })
            })
        })?;

        Ok(store)
    }
}

// =============================================================================
// Registration
// =============================================================================

/// Registers the Postgres plugin factories with the given registry.
pub fn register(registry: &mut PluginRegistry) {
    registry.register_immutable_store_plugin(Box::new(PgImmutableStorePluginFactory));
    registry.register_mutable_store_plugin(Box::new(PgMutableStorePluginFactory));
    registry.register_lock_store_plugin(Box::new(PgLockStorePluginFactory));
}
