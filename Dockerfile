# Runtime image for loreserver. Built multi-arch from prebuilt binaries: CI
# compiles loreserver-amd64 / loreserver-arm64 in native-platform containers,
# and buildx selects the right one via TARGETARCH. Runtime image assembly is
# COPY-only, so the hosted publication job does not compile under emulation.
#
# distroless/cc carries glibc + libgcc + ca-certificates — enough for a Rust binary
# using rustls (no system OpenSSL). If loreserver ever needs more, switch the base
# to debian:bookworm-slim.
FROM gcr.io/distroless/cc-debian12

ARG TARGETARCH
COPY --chmod=755 dist/loreserver-${TARGETARCH} /usr/local/bin/loreserver

EXPOSE 41337 41339
# 41337 gRPC/QUIC · 41339 HTTP (health: /health_check)

ENTRYPOINT ["/usr/local/bin/loreserver"]
