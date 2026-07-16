#!/usr/bin/env bash
set -euo pipefail

OUT=${1:-target/anvil/perf/anvil-time-profile.trace}
shift || true

mkdir -p "$(dirname "$OUT")"

xcrun xctrace record \
  --no-prompt \
  --template 'Time Profiler' \
  --output "$OUT" \
  --target-stdout - \
  --launch -- \
  "$(command -v cargo)" test -p anvil-server --test performance_tests -- --nocapture --test-threads=1 "$@"
