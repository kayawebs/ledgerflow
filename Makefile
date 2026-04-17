.PHONY: build run run-web run-indexer test check migrate fmt

build:
	cargo build

run:
	cargo run --bin ledgerflow

run-web:
	cargo run --bin web

run-indexer:
	cargo run --bin ledgerflow_indexer

test:
	cargo test

check:
	cargo check

migrate:
	sqlx migrate run

fmt:
	cargo fmt
