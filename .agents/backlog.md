# lore-stack — deferred work

Known tails accepted while shipping v0.0.2. None block normal use.

- **`[profile.release]` has `debug-assertions = true`** (upstream Lore choice). The
  distributed binary therefore fires `debug_assert!`s at runtime. One such assert in
  `aws-smithy-http-client`'s rustls provider panics if **no native root certs** are
  present (`valid > 0`). Real deployments have `ca-certificates` (mandatory for R2
  HTTPS anyway; our distroless/cc image ships them), so it's fine in practice — but a
  true-release build would be more robust. Fix if needed: add
  `--config 'profile.release.debug-assertions=false'` to the `cargo build` in
  `scripts/build.sh do_build`.
- **Docker Hub publish is gated off** until secrets exist. The leaked PAT must be
  revoked; then `gh secret set DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` on the repo.
  The `docker` job skips cleanly (green) until then.
- **Vendored DynamoDB reference files** (`lore-pg/src/store/{mutable,lock,immutable}_store.rs`,
  the `dynamodb*` modules, `lore-aws` AWS deps in Cargo.toml) are kept only as the
  porting spec. Once the PG rewrite is settled they can be deleted to slim the crate.
- **External review pending**: `docs/review/findings-codex.md` (Codex). Triage + fix
  real findings as was done for the prior adversarial pass.
- **arm64 `target-cpu`**: defaulted to `generic` (max portability). If targeting only
  AWS Graviton2+, set `ARM64_TARGET_CPU=neoverse-n1` (or `neoverse-512tvb` for G3-only)
  for a faster binary.
