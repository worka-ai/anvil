#!/usr/bin/env bash
set -euo pipefail

export ANVIL_TEST_LOG="${ANVIL_TEST_LOG:-warn}"
export ANVIL_TEST_TIMINGS="${ANVIL_TEST_TIMINGS:-1}"

group="${1:-all}"

run_step() {
  local name="$1"
  shift
  local start
  start="$(date +%s)"
  echo "::group::${name}"
  echo "[anvil-gate] start ${name}"
  local timeout_seconds="${ANVIL_GATE_STEP_TIMEOUT_SECONDS:-1800}"
  local timeout_bin=""
  if [[ -n "${timeout_seconds}" && "${timeout_seconds}" != "0" ]]; then
    timeout_bin="$(command -v timeout || true)"
  fi
  set +e
  if [[ -n "${timeout_bin}" ]]; then
    "${timeout_bin}" --kill-after=30s "${timeout_seconds}s" "$@"
  else
    "$@"
  fi
  local status=$?
  set -e
  local end
  end="$(date +%s)"
  echo "[anvil-gate] finish ${name} status=${status} elapsed=$((end - start))s"
  if [[ "${status}" == "124" ]]; then
    echo "[anvil-gate] timed out ${name} after ${timeout_seconds}s" >&2
  fi
  echo "::endgroup::"
  return "$status"
}

run_cargo_test() {
  local name="$1"
  shift
  run_step "$name" cargo test --no-fail-fast "$@" -- --nocapture
}

require_image() {
  local configured_anvil_image="${ANVIL_IMAGE:-anvil:test}"
  export ANVIL_IMAGE="$(./scripts/resolve-docker-image-id.sh "$configured_anvil_image")"
  echo "[anvil-gate] using ANVIL_IMAGE=${ANVIL_IMAGE}"
}

static_gates() {
  run_step "no external database gate" ./scripts/check-no-external-db.sh
  run_step "no public unfenced journal writes gate" ./scripts/check-no-public-unfenced-journal-writes.sh
  run_step "documentation hardening gate" ./scripts/check-docs-hardening.sh
  run_step "release notes gate" ./scripts/test-release-notes.sh
  run_step "fission docs check" fission site check --project-dir documentation --release
  run_step "fission docs build" fission site build --project-dir documentation --release
  run_step "Rust client publish dry-run" cargo publish --dry-run -p anvil-storage
}

rust_unit_gates() {
  run_cargo_test "core library tests" -p anvil-storage-core --lib --bins
  run_cargo_test "server library and binary tests" -p anvil-server --lib --bins
  run_cargo_test "public CLI package tests" -p anvil-storage-cli --lib --bins --tests
  run_cargo_test "Rust client package tests" -p anvil-storage --lib --tests
  run_cargo_test "test utils package tests" -p anvil-storage-test-utils --lib
  run_cargo_test "CoreStore model package tests" -p anvil-corestore-model --lib --tests
  run_cargo_test "documentation package tests" -p anvil-documentation --lib --bins
}

server_core_integration_gates() {
  local tests=(
    admin_lifecycle
    cluster
    corestore_conformance
    corestore_conformance_durable_families
    corestore_conformance_rfc0007_byte_pipeline
    corestore_conformance_rfc0007_internal_protocols
    corestore_conformance_rfc0007_journals
    corestore_conformance_rfc0007_payloads
    corestore_conformance_rfc0007_perf
    corestore_conformance_rfc0007_query_e2e_scaffolding
    corestore_conformance_rfc0007_query_planning
    corestore_conformance_rfc0007_roots
    corestore_conformance_rfc0007_transactions
    corestore_conformance_rfc0007_writer_segments
    corestore_source_size
    grpc
    hardening_static
    performance_tests
  )
  for test_name in "${tests[@]}"; do
    run_cargo_test "server integration ${test_name}" -p anvil-server --test "${test_name}"
  done
}

docker_auth_gates() {
  require_image
  local tests=(
    auth
    auth_tests
  )
  for test_name in "${tests[@]}"; do
    run_cargo_test "Docker auth integration ${test_name}" -p anvil-server --test "${test_name}"
  done
  run_cargo_test "Docker CLI auth integration" -p anvil-storage-cli --test cli_auth
}

docker_storage_gates() {
  require_image
  local tests=(
    bucket_tests
    object_tests
    rust_client_tests
    s3_gateway_tests
  )
  for test_name in "${tests[@]}"; do
    run_cargo_test "Docker storage integration ${test_name}" -p anvil-server --test "${test_name}"
  done
}

docker_index_gates() {
  require_image
  local tests=(
    git_source_tests
    hf_ingestion_e2e
    hf_ingestion_integration
    index_tests
    internal_proxy_tests
    personaldb_tests
  )
  for test_name in "${tests[@]}"; do
    run_cargo_test "Docker index/data integration ${test_name}" -p anvil-server --test "${test_name}"
  done
}

docker_mesh_gates() {
  require_image
  local tests=(
    distributed_tests
    docker_cluster_test
  )
  for test_name in "${tests[@]}"; do
    run_cargo_test "Docker mesh integration ${test_name}" -p anvil-server --test "${test_name}"
  done
}

case "$group" in
  all)
    static_gates
    rust_unit_gates
    server_core_integration_gates
    docker_auth_gates
    docker_storage_gates
    docker_index_gates
    docker_mesh_gates
    ;;
  static)
    static_gates
    ;;
  rust)
    rust_unit_gates
    ;;
  server-core)
    server_core_integration_gates
    ;;
  docker-auth)
    docker_auth_gates
    ;;
  docker-storage)
    docker_storage_gates
    ;;
  docker-index)
    docker_index_gates
    ;;
  docker-mesh)
    docker_mesh_gates
    ;;
  *)
    cat >&2 <<USAGE
usage: $0 [all|static|rust|server-core|docker-auth|docker-storage|docker-index|docker-mesh]
USAGE
    exit 2
    ;;
esac
