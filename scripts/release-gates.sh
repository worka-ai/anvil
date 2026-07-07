#!/usr/bin/env bash
set -euo pipefail

./scripts/check-no-external-db.sh
./scripts/check-no-public-unfenced-journal-writes.sh
./scripts/check-docs-hardening.sh
./scripts/test-release-notes.sh
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
cargo publish --dry-run -p anvil-storage
cargo test --workspace -- --nocapture
