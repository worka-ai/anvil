#!/usr/bin/env bash
set -euo pipefail

seeds="${ANVIL_AUTHZ_BENCH_SEEDS:-0 10 100 250}"

printf 'Running authz mutation latency benchmark with retained-history seeds: %s\n' "$seeds"
for seed in $seeds; do
  printf '\n== retained authz tuple history: %s ==\n' "$seed"
  ANVIL_RUN_AUTHZ_PERF=1 \
    ANVIL_AUTHZ_PERF_SEED="$seed" \
    cargo test -p anvil-storage-core authz_tuple_write_latency_with_retained_history_perf -- --nocapture
done
