#!/usr/bin/env bash
set -euo pipefail

./scripts/check-no-external-db.sh
./scripts/check-no-public-unfenced-journal-writes.sh
./scripts/check-docs-hardening.sh
./scripts/test-release-notes.sh
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
cargo publish --dry-run -p anvil-storage
configured_anvil_image="${ANVIL_IMAGE:-anvil:test}"
export ANVIL_IMAGE="$(./scripts/resolve-docker-image-id.sh "$configured_anvil_image")"
cargo test --workspace --no-fail-fast
