#!/usr/bin/env bash
set -euo pipefail

export ANVIL_TEST_LOG="${ANVIL_TEST_LOG:-warn}"
# Timing instrumentation is intentionally opt-in. Enabling it on release gates
# can produce hundreds of thousands of log lines and materially slow Docker E2E.
if [[ -n "${ANVIL_TEST_TIMINGS:-}" ]]; then
  export ANVIL_TEST_TIMINGS
else
  unset ANVIL_TEST_TIMINGS
fi

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

run_docker_cargo_test() {
  local name="$1"
  shift
  local test_threads="${ANVIL_DOCKER_TEST_THREADS:-4}"
  run_step "$name" cargo test --no-fail-fast "$@" -- --nocapture --test-threads="${test_threads}"
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
  run_cargo_test "public CLI binary/unit tests" -p anvil-storage-cli --bins
  run_cargo_test "public CLI non-Docker integration tests" -p anvil-storage-cli \
    --test binary_names \
    --test confy_test \
    --test public_command_surface
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
    run_docker_cargo_test "Docker auth integration ${test_name}" -p anvil-server --test "${test_name}"
  done
  run_docker_cargo_test "Docker CLI auth integration" -p anvil-storage-cli --test cli_auth
}

run_docker_object_test_filter() {
  local label="$1"
  local filter="$2"
  run_docker_cargo_test "Docker storage integration object_tests ${label}" \
    -p anvil-server --test object_tests "${filter}"
}

run_docker_s3_test_filter() {
  local label="$1"
  local filter="$2"
  run_docker_cargo_test "Docker storage integration s3_gateway_tests ${label}" \
    -p anvil-server --test s3_gateway_tests "${filter}"
}

docker_storage_gates() {
  require_image
  local tests=(
    bucket_tests
    rust_client_tests
  )
  for test_name in "${tests[@]}"; do
    run_docker_cargo_test "Docker storage integration ${test_name}" -p anvil-server --test "${test_name}"
  done
  # object_tests is intentionally split by module. The suite exercises the
  # shared Docker cluster, CoreMeta quorum commits and background maintenance;
  # one aggregate process can exceed a per-step timeout while providing no extra
  # coverage beyond the same test set run as fresh filtered processes.
  run_docker_object_test_filter "batch CAS multipart" "batch_cas_multipart::"
  run_docker_object_test_filter "copy private watch stream" "copy_private_watch_stream::"
  run_docker_object_test_filter "native delete listing" "native_delete_listing::"
  run_docker_object_test_filter "mesh locator routing" "native_object_routes_use_mesh_locator_before_local_bucket_metadata"
  run_docker_object_test_filter "patch and list" "patch_and_list::"
  run_docker_object_test_filter "planner listing" "planner_listing::"
  run_docker_object_test_filter "reserved head core" "reserved_head_core::"

  # Split S3 gateway tests for the same reason: each module uses the shared
  # Docker cluster and some tests intentionally wait for asynchronous index or
  # compaction workers to catch up.
  run_docker_s3_test_filter "public/private large objects" "public_private_large_object::"
  run_docker_s3_test_filter "routing public aliases" "routing_public_alias::"
  run_docker_s3_test_filter "streaming upload" "streaming_upload::"
  run_docker_s3_test_filter "writes indexes compaction" "writes_indexes_compaction::"
  run_docker_cargo_test "Docker CLI storage integration" -p anvil-storage-cli --test cli
}

run_docker_index_test_filter() {
  local label="$1"
  local filter="$2"
  ANVIL_DOCKER_TEST_THREADS="${ANVIL_INDEX_TEST_THREADS:-1}" \
    run_docker_cargo_test "Docker index/data integration index_tests ${label}" \
      -p anvil-server --test index_tests "${filter}"
}

docker_index_gates() {
  require_image
  local tests=(
    git_source_tests
    hf_ingestion_e2e
    internal_proxy_tests
  )
  for test_name in "${tests[@]}"; do
    run_docker_cargo_test "Docker index/data integration ${test_name}" -p anvil-server --test "${test_name}"
  done
  echo "[anvil-gate] skipping Docker index/data integration personaldb_tests; tracked by https://github.com/worka-ai/anvil/issues/19"
  ANVIL_DOCKER_TEST_THREADS=1 \
    run_docker_cargo_test "Docker index/data integration hf_ingestion_integration" \
      -p anvil-server --test hf_ingestion_integration

  # index_tests intentionally runs as smaller filters. The tests exercise shared
  # clusters, background index workers and CoreMeta quorum writes; one aggregate
  # process accumulates enough state to make CI timing unreliable while adding no
  # extra coverage beyond the same test set run as fresh filtered processes.
  run_docker_index_test_filter "build_repair" "build_repair::"
  run_docker_index_test_filter "query authz" "query_spec::test_query_index_results_are_filtered_by_zanzibar_object_relationships"
  run_docker_index_test_filter "query inherited usersets" "query_spec::test_query_spec_inherit_object_filter_uses_derived_userset_grants"
  run_docker_index_test_filter "query read-scope intersection" "query_spec::test_query_spec_intersection_filters_inherit_object_hits_by_read_scope"
  run_docker_index_test_filter "query full-text intersection" "query_spec::test_query_spec_intersects_full_text_with_typed_filter_without_bucket_scan"
  run_docker_index_test_filter "query hybrid intersection" "query_spec::test_query_spec_intersects_hybrid_with_typed_filter_without_bucket_scan"
  run_docker_index_test_filter "query vector intersection" "query_spec::test_query_spec_intersects_vector_with_typed_filter_without_bucket_scan"
  run_docker_index_test_filter "query path authz" "query_spec::test_query_spec_path_filter_intersects_authz_before_results"
  run_docker_index_test_filter "query typed json" "query_spec::test_typed_json_index_queries_canonical_object_body_with_range_order_and_page_token"
  run_docker_index_test_filter "typed lifecycle" "typed_lifecycle::"
  run_docker_index_test_filter "validation diagnostics" "validation_diagnostics::"
  run_docker_index_test_filter "vector hybrid" "vector_hybrid::"

  run_docker_cargo_test "Docker CLI extended integration" -p anvil-storage-cli --test cli_extended
}

docker_mesh_gates() {
  require_image
  local tests=(
    distributed_tests
    docker_cluster_test
    grpc
  )
  for test_name in "${tests[@]}"; do
    run_docker_cargo_test "Docker mesh integration ${test_name}" -p anvil-server --test "${test_name}"
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
