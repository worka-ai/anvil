# Contributing to Anvil

## Prerequisites

- Rust and Cargo
- Docker and Docker Compose for containerized cluster checks

## Local Development

Anvil uses native on-disk state below `STORAGE_PATH`; no external metadata database is required.

Start a local cluster:

```bash
./anvil/start-local-cluster.sh
```

Run focused checks:

```bash
cargo check -p anvil-core
cargo check -p anvil
cargo check -p anvil-test-utils
cargo check -p anvil --tests
```

Run tests with the shared Cargo target directory managed by Cargo locking. Do not create ad-hoc target directories unless the task explicitly requires isolation.
