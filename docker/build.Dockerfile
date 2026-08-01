FROM rust:bookworm

# The release binary is Linux-native even though CI runs on the macOS runner.
# Keep the compiler environment in Docker so each platform uses its native GNU
# linker instead of relying on host cross-linker state.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
