# Runtime image for loreserver. Built multi-arch from prebuilt binaries: the CI
# matrix compiles loreserver-amd64 / loreserver-arm64 natively, and buildx selects
# the right one per platform via TARGETARCH. Only COPY uses the target arch, so no
# QEMU emulation is needed.
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
