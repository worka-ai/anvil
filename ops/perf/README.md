# Anvil performance stack

This directory contains the local performance stack used to investigate slow release gates and request-level latency. GreptimeDB and Grafana run in Docker; Anvil itself runs on the host through the normal Rust test harness.

## Start GreptimeDB and Grafana

```sh
docker compose -f ops/perf/docker-compose.yml up -d
```

Grafana listens on <http://127.0.0.1:3000>. The local credentials are `admin` / `admin`; anonymous admin access is enabled for this local-only stack.

GreptimeDB listens on:

- HTTP line protocol: <http://127.0.0.1:4000>
- MySQL protocol for Grafana: `127.0.0.1:4002`

## Run the focused performance suite

Use absolute output paths. Cargo integration tests execute with the package manifest as the current directory, so relative `target/...` paths can otherwise end up below `anvil/target/...`.

```sh
ANVIL_RUN_PERF_TESTS=1 \
ANVIL_PERF_TRACE=1 \
ANVIL_TEST_TIMINGS=1 \
ANVIL_PERF_GREPTIME_URL='http://127.0.0.1:4000/v1/influxdb/write?db=public' \
ANVIL_PERF_TRACE_FILE="$(pwd)/target/anvil/perf/anvil.line" \
ANVIL_PERF_REPORT_PATH="$(pwd)/target/anvil/perf/performance-summary.json" \
cargo test -p anvil-server --test performance_tests -- --nocapture --test-threads=1
```

The suite records two layers:

- method-level timings for CoreStore primitives such as blob put/get, append/read stream, CAS ref, fences, and mutation batches;
- end-to-end gRPC timings for bucket creation, object writes, object reads, listing, index creation, and caught-up index queries.

## Summarise local output

```sh
scripts/analyze-perf.py \
  --summary target/anvil/perf/performance-summary.json \
  --line target/anvil/perf/anvil.line \
  --release-log target/anvil/logs/release-gates.log
```

This prints the slowest measured cases, request paths, internal spans, and release-gate slow-test warnings.

## Capture a macOS Time Profiler trace

For CPU-level analysis, run the same performance suite under Instruments through `xctrace`:

```sh
ANVIL_RUN_PERF_TESTS=1 \
ANVIL_PERF_TRACE=1 \
ANVIL_TEST_TIMINGS=1 \
ANVIL_PERF_TRACE_FILE="$(pwd)/target/anvil/perf/anvil.line" \
ANVIL_PERF_REPORT_PATH="$(pwd)/target/anvil/perf/performance-summary.json" \
ops/perf/run-xctrace.sh target/anvil/perf/anvil-time-profile.trace
```

Open the resulting `.trace` file in Instruments and inspect the hot call stacks for the slow cases reported by `scripts/analyze-perf.py`.
