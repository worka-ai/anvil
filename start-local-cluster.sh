#!/bin/bash
#
# This script sets up a local 3-node Anvil cluster without using Docker for the
# Anvil nodes themselves. It allows a 4th node to be started from an IDE with
# a debugger attached.
#
# Pre-requisites:
#   - Docker installed and running (for the Postgres instance)
#   - Rust/Cargo installed
#   - `sqlx-cli` installed (`cargo install sqlx-cli`)

set -e
echo "--- Anvil Local Cluster Setup ---"

# --- Configuration ---
PG_CONTAINER_NAME="anvil"
PG_PASSWORD="worka"
PG_USER="worka"
PG_DB="worka"
PG_PORT="5432"
export DATABASE_URL="postgres://${PG_USER}:${PG_PASSWORD}@localhost:${PG_PORT}/${PG_DB}"

# Generate a shared secret for the cluster
export WORKA_CLUSTER_SECRET=$(head -c 32 /dev/urandom | base64)

NODE_COUNT=3
STORAGE_BASE_DIR="$(pwd)/.anvil-local-data"


# --- Cleanup previous runs ---
echo "Cleaning up previous cluster..."
pkill -f "anvil" || echo "No previous anvil processes to kill."
if [ "$(docker ps -q -f name=^/${PG_CONTAINER_NAME}$)" ]; then
    echo "Stopping existing Postgres container..."
    docker stop "${PG_CONTAINER_NAME}" > /dev/null
fi
if [ "$(docker ps -aq -f status=exited -f name=^/${PG_CONTAINER_NAME}$)" ]; then
    echo "Removing old Postgres container..."
    docker rm "${PG_CONTAINER_NAME}" > /dev/null
fi
rm -rf "${STORAGE_BASE_DIR}"
echo "Cleanup complete."


# --- Start Dependencies ---
echo "Starting Postgres in Docker..."
docker run -d --name "${PG_CONTAINER_NAME}" \
  -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
  -e POSTGRES_USER="${PG_USER}" \
  -e POSTGRES_DB="${PG_DB}" \
  -p "${PG_PORT}:5432" \
  postgres:18-alpine > /dev/null

# Wait for Postgres to be ready
echo "Waiting for Postgres to accept connections..."
until docker exec "${PG_CONTAINER_NAME}" pg_isready -U "${PG_USER}" -d "${PG_DB}" -q; do
  sleep 1
done
echo "Postgres is ready."

# --- Prepare Database ---
echo "Running database migrations..."
docker exec "${PG_CONTAINER_NAME}" psql "${DATABASE_URL}" -c "CREATE DATABASE ${PG_DB}" || echo "Database already exists."
docker exec -i "${PG_CONTAINER_NAME}" psql "${DATABASE_URL}" < migrations_global/V1__initial_global_schema.sql
docker exec -i "${PG_CONTAINER_NAME}" psql "${DATABASE_URL}" < migrations_regional/V1__initial_regional_schema.sql
echo "Migrations applied."


# --- Start Cluster Nodes ---
echo "Starting ${NODE_COUNT} Anvil nodes..."
PIDS=""
BOOTSTRAP_QUIC_ADDR="127.0.0.1:7443"

for i in $(seq 1 ${NODE_COUNT}); do
  QUIC_PORT=$((7443 + i - 1))
  HTTP_PORT=$((9000 + i - 1))
  GRPC_PORT=$((50051 + i - 1))
  NODE_STORAGE_DIR="${STORAGE_BASE_DIR}/node-${i}"
  mkdir -p "${NODE_STORAGE_DIR}"

  export WORKA_BIND_QUIC="127.0.0.1:${QUIC_PORT}"
  export WORKA_BIND_HTTP="127.0.0.1:${HTTP_PORT}"
  export WORKA_BIND_GRPC="127.0.0.1:${GRPC_PORT}"
  export WORKA_STORAGE_PATH="${NODE_STORAGE_DIR}"
  export WORKA_ENABLE_MDNS="false"
  export RUST_LOG="info,anvil=debug"

  if [ "$i" -eq 1 ]; then
    export WORKA_INIT_CLUSTER="true"
  else
    export WORKA_INIT_CLUSTER="false"
    export WORKA_BOOTSTRAP_NODES="${BOOTSTRAP_QUIC_ADDR}"
  fi

  echo "Starting Node ${i} (QUIC: ${QUIC_PORT}, HTTP: ${HTTP_PORT}, gRPC: ${GRPC_PORT})"
#  cargo run --release -- --global-database-url "${DATABASE_URL}" --regional-database-url "${DATABASE_URL}" --jwt-secret "local-jwt-secret" --worka-secret-encryption-key "a-very-secret-key-that-is-32-bytes" --public-grpc-addr "127.0.0.1:${GRPC_PORT}" --region "local" &
  ../target/debug/anvil &
  PIDS="$PIDS $!"
  sleep 1 # Stagger startup
done

# --- Instructions for Debugger ---
DEBUG_QUIC_PORT=$((7443 + NODE_COUNT))
DEBUG_HTTP_PORT=$((9000 + NODE_COUNT))
DEBUG_GRPC_PORT=$((50051 + NODE_COUNT))
DEBUG_STORAGE_DIR="${STORAGE_BASE_DIR}/node-debug"
mkdir -p "${DEBUG_STORAGE_DIR}"

cat << EOF

--------------------------------------------------------------------
✅ Local cluster started with ${NODE_COUNT} nodes.

To start the 4th node with your debugger, create a run configuration
in your IDE with the following environment variables and run it:

export RUST_LOG="info,anvil=debug"
export WORKA_DB_DSN="${DATABASE_URL}"
export WORKA_BIND_QUIC="127.0.0.1:${DEBUG_QUIC_PORT}"
export WORKA_BIND_HTTP="127.0.0.1:${DEBUG_HTTP_PORT}"
export WORKA_BIND_GRPC="127.0.0.1:${DEBUG_GRPC_PORT}"
export WORKA_STORAGE_PATH="${DEBUG_STORAGE_DIR}"
export WORKA_ENABLE_MDNS="false"
export WORKA_INIT_CLUSTER="false"
export WORKA_BOOTSTRAP_NODES="${BOOTSTRAP_QUIC_ADDR}"

The command to run is simply: cargo run --release -- --global-database-url "${DATABASE_URL}" --regional-database-url "${DATABASE_URL}" --jwt-secret "local-jwt-secret" --worka-secret-encryption-key "a-very-secret-key-that-is-32-bytes" --public-grpc-addr "127.0.0.1:${DEBUG_GRPC_PORT}" --region "local" --bootstrap-nodes "${BOOTSTRAP_QUIC_ADDR}"
--------------------------------------------------------------------

EOF

# --- Shutdown ---
cleanup() {
    echo ""
    echo "Shutting down cluster..."
    for pid in $PIDS;
    do
        kill "$pid" 2>/dev/null
    done
    if [ "$(docker ps -q -f name=^/${PG_CONTAINER_NAME}$)" ]; then
        echo "Stopping Postgres container..."
        docker stop "${PG_CONTAINER_NAME}" > /dev/null
    fi
    echo "Shutdown complete."
}

trap cleanup INT

echo "Cluster is running. Press Ctrl+C to shut down all nodes."

wait
