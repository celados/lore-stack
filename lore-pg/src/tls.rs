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

use rustls::ClientConfig;
use rustls::RootCertStore;
use tokio_postgres_rustls::MakeRustlsConnect;

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
