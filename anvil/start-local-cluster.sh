#!/bin/bash
# Start a local three-node Anvil cluster using only Anvil's native on-disk state.

set -euo pipefail

echo "--- Anvil Local Cluster Setup ---"

NODE_COUNT=3
STORAGE_BASE_DIR="$(pwd)/.anvil-local-data"
JWT_SECRET="local-jwt-secret"
ENCRYPTION_KEY="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
CLUSTER_SECRET="$(head -c 32 /dev/urandom | base64)"
BOOTSTRAP_ADDR="/ip4/127.0.0.1/udp/7443/quic-v1"
PIDS=""

cleanup() {
  echo ""
  echo "Shutting down cluster..."
  for pid in $PIDS; do
    kill "$pid" 2>/dev/null || true
  done
  echo "Shutdown complete."
}
trap cleanup INT TERM EXIT

echo "Cleaning up previous cluster..."
pkill -f "target/.*/anvil" || true
rm -rf "${STORAGE_BASE_DIR}"
mkdir -p "${STORAGE_BASE_DIR}"

echo "Starting ${NODE_COUNT} Anvil nodes..."
for i in $(seq 1 ${NODE_COUNT}); do
  QUIC_PORT=$((7443 + i - 1))
  GRPC_PORT=$((50051 + i - 1))
  NODE_STORAGE_DIR="${STORAGE_BASE_DIR}/node-${i}"
  mkdir -p "${NODE_STORAGE_DIR}"

  args=(
    --jwt-secret "${JWT_SECRET}"
    --anvil-secret-encryption-key "${ENCRYPTION_KEY}"
    --cluster-secret "${CLUSTER_SECRET}"
    --cluster-listen-addr "/ip4/127.0.0.1/udp/${QUIC_PORT}/quic-v1"
    --public-cluster-addrs "/ip4/127.0.0.1/udp/${QUIC_PORT}/quic-v1"
    --api-listen-addr "127.0.0.1:${GRPC_PORT}"
    --public-api-addr "http://127.0.0.1:${GRPC_PORT}"
    --region "local"
    --enable-mdns false
    --storage-path "${NODE_STORAGE_DIR}"
  )

  if [ "$i" -eq 1 ]; then
    args+=(--init-cluster true)
  else
    args+=(--init-cluster false --bootstrap-addrs "${BOOTSTRAP_ADDR}")
  fi

  echo "Starting node ${i} (QUIC: ${QUIC_PORT}, gRPC: ${GRPC_PORT}, storage: ${NODE_STORAGE_DIR})"
  RUST_LOG="info,anvil=debug" cargo run -p anvil-storage -- "${args[@]}" &
  PIDS="$PIDS $!"
  sleep 1
done

DEBUG_QUIC_PORT=$((7443 + NODE_COUNT))
DEBUG_GRPC_PORT=$((50051 + NODE_COUNT))
DEBUG_STORAGE_DIR="${STORAGE_BASE_DIR}/node-debug"
mkdir -p "${DEBUG_STORAGE_DIR}"

cat <<MSG

--------------------------------------------------------------------
Local cluster started with ${NODE_COUNT} nodes.

To start a debugger-attached node, run:

cargo run -p anvil-storage -- \\
  --jwt-secret "${JWT_SECRET}" \\
  --anvil-secret-encryption-key "${ENCRYPTION_KEY}" \\
  --cluster-secret "${CLUSTER_SECRET}" \\
  --cluster-listen-addr "/ip4/127.0.0.1/udp/${DEBUG_QUIC_PORT}/quic-v1" \\
  --public-cluster-addrs "/ip4/127.0.0.1/udp/${DEBUG_QUIC_PORT}/quic-v1" \\
  --api-listen-addr "127.0.0.1:${DEBUG_GRPC_PORT}" \\
  --public-api-addr "http://127.0.0.1:${DEBUG_GRPC_PORT}" \\
  --region "local" \\
  --enable-mdns false \\
  --init-cluster false \\
  --bootstrap-addrs "${BOOTSTRAP_ADDR}" \\
  --storage-path "${DEBUG_STORAGE_DIR}"
--------------------------------------------------------------------

MSG

echo "Cluster is running. Press Ctrl+C to shut down all nodes."
wait
