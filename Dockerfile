FROM rust:1.92-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src

RUN cargo build --release --locked --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/ledgerflow /usr/local/bin/ledgerflow
COPY --from=builder /app/target/release/web /usr/local/bin/web
COPY --from=builder /app/target/release/ledgerflow_indexer /usr/local/bin/ledgerflow_indexer
COPY --from=builder /app/target/release/ledgerflow_backfill_worker /usr/local/bin/ledgerflow_backfill_worker
COPY --from=builder /app/target/release/ledgerflow_price_worker /usr/local/bin/ledgerflow_price_worker
COPY --from=builder /app/target/release/ledgerflow_address_worker /usr/local/bin/ledgerflow_address_worker
COPY --from=builder /app/target/release/ledgerflow_trace_monitor /usr/local/bin/ledgerflow_trace_monitor
COPY --from=builder /app/target/release/ledgerflow_rpc_gateway /usr/local/bin/ledgerflow_rpc_gateway

ENV RUST_LOG=info

CMD ["ledgerflow"]
