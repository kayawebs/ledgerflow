FROM rust:1.92-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY assets ./assets
COPY config ./config
COPY schemas ./schemas
COPY demo ./demo
COPY reports ./reports
COPY src ./src

RUN cargo build --release --locked --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/config ./config
COPY --from=builder /app/schemas ./schemas
COPY --from=builder /app/demo ./demo

COPY --from=builder /app/target/release/ledgerflow /usr/local/bin/ledgerflow
COPY --from=builder /app/target/release/web /usr/local/bin/web
COPY --from=builder /app/target/release/ledgerflow_indexer /usr/local/bin/ledgerflow_indexer
COPY --from=builder /app/target/release/ledgerflow_backfill_worker /usr/local/bin/ledgerflow_backfill_worker
COPY --from=builder /app/target/release/ledgerflow_price_worker /usr/local/bin/ledgerflow_price_worker
COPY --from=builder /app/target/release/ledgerflow_address_worker /usr/local/bin/ledgerflow_address_worker
COPY --from=builder /app/target/release/ledgerflow_trace_monitor /usr/local/bin/ledgerflow_trace_monitor
COPY --from=builder /app/target/release/ledgerflow_rpc_gateway /usr/local/bin/ledgerflow_rpc_gateway
COPY --from=builder /app/target/release/ledgerflow_registry_sync /usr/local/bin/ledgerflow_registry_sync
COPY --from=builder /app/target/release/ledgerflow_discovery_worker /usr/local/bin/ledgerflow_discovery_worker
COPY --from=builder /app/target/release/ledgerflow_address_fact_producer /usr/local/bin/ledgerflow_address_fact_producer
COPY --from=builder /app/target/release/ledgerflow_behavior_worker /usr/local/bin/ledgerflow_behavior_worker
COPY --from=builder /app/target/release/ledgerflow_seed_worker /usr/local/bin/ledgerflow_seed_worker
COPY --from=builder /app/target/release/ledgerflow_economic_verifier /usr/local/bin/ledgerflow_economic_verifier
COPY --from=builder /app/target/release/ledgerflow_observation_worker /usr/local/bin/ledgerflow_observation_worker
COPY --from=builder /app/target/release/ledgerflow_searcher_cluster_worker /usr/local/bin/ledgerflow_searcher_cluster_worker
COPY --from=builder /app/target/release/ledgerflow_observation_alert_worker /usr/local/bin/ledgerflow_observation_alert_worker
COPY --from=builder /app/target/release/ledgerflow_intelligence_import /usr/local/bin/ledgerflow_intelligence_import
COPY --from=builder /app/target/release/ledgerflow_pilot_config /usr/local/bin/ledgerflow_pilot_config
COPY --from=builder /app/target/release/ledgerflow_pilot_event_worker /usr/local/bin/ledgerflow_pilot_event_worker
COPY --from=builder /app/target/release/ledgerflow_pilot_alert_worker /usr/local/bin/ledgerflow_pilot_alert_worker
COPY --from=builder /app/target/release/ledgerflow_pilot_report /usr/local/bin/ledgerflow_pilot_report
COPY --from=builder /app/target/release/ledgerflow_pilot_report_worker /usr/local/bin/ledgerflow_pilot_report_worker
COPY --from=builder /app/target/release/ledgerflow_market_seed_import /usr/local/bin/ledgerflow_market_seed_import
COPY --from=builder /app/target/release/ledgerflow_market_web /usr/local/bin/ledgerflow_market_web
COPY --from=builder /app/target/release/ledgerflow_protocol_launch_indexer /usr/local/bin/ledgerflow_protocol_launch_indexer
COPY --from=builder /app/target/release/ledgerflow_protocol_cluster_projector /usr/local/bin/ledgerflow_protocol_cluster_projector
COPY --from=builder /app/target/release/ledgerflow_protocol_market_projector /usr/local/bin/ledgerflow_protocol_market_projector

ENV RUST_LOG=info

CMD ["ledgerflow"]
