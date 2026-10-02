FROM rust:1.89-bookworm AS builder
WORKDIR /build
COPY proto/ ./proto/
COPY worker-rust/ ./worker-rust/
WORKDIR /build/worker-rust
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/worker-rust/target \
    cargo build --locked --release \
    && cp target/release/queryforge-datafusion-worker /build/queryforge-datafusion-worker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/queryforge-datafusion-worker /usr/local/bin/queryforge-datafusion-worker
ENTRYPOINT ["queryforge-datafusion-worker"]
