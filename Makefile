.PHONY: build run run-web run-indexer run-economic-verifier test check migrate fmt pilot-config-check pilot-runtime-check pilot-soak

build:
	cargo build

run:
	cargo run --bin ledgerflow

run-web:
	cargo run --bin web

run-indexer:
	cargo run --bin ledgerflow_indexer

run-economic-verifier:
	cargo run --bin ledgerflow_economic_verifier

test:
	cargo test

check:
	cargo check

migrate:
	sqlx migrate run

fmt:
	cargo fmt

pilot-config-check:
	cargo run --bin ledgerflow_pilot_config -- config/pilots/example-base-project.yaml --check

pilot-runtime-check:
	./scripts/verify_pilot_runtime.sh

pilot-soak:
	./scripts/soak_test.sh
