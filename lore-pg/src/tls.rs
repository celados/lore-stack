// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Shared TLS connector for the `tokio_postgres` / `deadpool_postgres` pools used by all
//! three stores.
//!
//! Managed Postgres providers (e.g. PlanetScale) reject plaintext connections outright
//! (SQLSTATE 28000), so we cannot keep handing `NoTls` to `deadpool_postgres::Config::create_pool`.
//! We also cannot ask operators for `sslmode=verify-full` in the DSN — `tokio_postgres` only
//! parses `disable` / `prefer` / `require`. Instead every store always hands this rustls
//! connector to the pool and lets the DSN's `sslmode` (default `prefer`) decide whether TLS is
//! actually negotiated: `prefer` tries TLS first and falls back to plaintext, so the same
//! connector transparently keeps the plaintext docker-compose Postgres working too — one code
//! path serves both local dev and production. Whenever TLS *is* negotiated, this connector
//! always verifies the server certificate and hostname against the platform's native root
//! store (the closest behavior to `verify-full` that `tokio_postgres` allows).

use std::sync::Arc;

use deadpool_postgres::Pool;
use rustls::ClientConfig;
use rustls::RootCertStore;
use tokio_postgres_rustls::MakeRustlsConnect;

/// Default per-pool connection cap. Each of the three stores (immutable /
/// mutable / lock) builds its OWN deadpool, so total backend connections =
/// 3 × this. Kept small because managed Postgres (PlanetScale) has a low,
/// SHARED connection ceiling: deadpool's own default (num_cpus × 4) per pool
/// lets a single large push saturate every slot, which starves the server's
/// own branch-update handler AND any other tenant on the instance (e.g.
/// Vaultwarden). 3 × 5 = 15 leaves ample headroom. Override with
/// `LORE_PG_POOL_MAX_SIZE`.
const DEFAULT_POOL_MAX_SIZE: usize = 5;

/// Build the connector every store's `from_config` (and test `setup_store`) passes to
/// `deadpool_postgres::Config::create_pool` in place of `NoTls`.
///
/// Uses the `ring` crypto provider explicitly via `builder_with_provider` so this does not
/// depend on a process-wide default `CryptoProvider` (and so it cannot panic if one was never
/// installed) — mirroring how `clients.rs` pins `aws-smithy-http-client` to `CryptoMode::Ring`.
pub fn make_connector() -> Result<MakeRustlsConnect, Box<dyn std::error::Error + Send + Sync>> {
    let mut roots = RootCertStore::empty();
    let native_certs = rustls_native_certs::load_native_certs();
    if native_certs.certs.is_empty() {
        return Err(format!(
            "no native root certificates could be loaded (errors: {:?})",
            native_certs.errors
        )
        .into());
    }
    for cert in native_certs.certs {
        roots.add(cert)?;
    }

    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();

    Ok(MakeRustlsConnect::new(config))
}

/// Build a **size-capped** deadpool Postgres pool wired to the rustls connector.
///
/// Every store's `from_config` uses this instead of calling
/// `deadpool_postgres::Config::create_pool` directly, so the connection cap
/// (see [`DEFAULT_POOL_MAX_SIZE`]) is enforced in exactly one place. Cap is
/// read from `LORE_PG_POOL_MAX_SIZE` at startup, falling back to the default;
/// a non-numeric or zero value falls back too.
pub fn make_pool(dsn: &str) -> Result<Pool, Box<dyn std::error::Error + Send + Sync>> {
    use deadpool_postgres::Config as DeadpoolConfig;
    use deadpool_postgres::PoolConfig;
    use deadpool_postgres::Runtime;

    let max_size = std::env::var("LORE_PG_POOL_MAX_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_POOL_MAX_SIZE);

    let mut cfg = DeadpoolConfig::new();
    cfg.url = Some(dsn.to_owned());
    cfg.pool = Some(PoolConfig::new(max_size));
    let connector = make_connector()?;
    let pool = cfg.create_pool(Some(Runtime::Tokio1), connector)?;
    Ok(pool)
}
