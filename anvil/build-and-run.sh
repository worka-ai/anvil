#!/bin/bash
# This script first cross-compiles the release binaries for Linux and then starts the Docker Compose cluster.
# This is the recommended way to run the cluster locally.
set -e

TARGET="x86_64-unknown-linux-gnu"

echo "--- Building Anvil release binaries for ${TARGET} ---"
cargo build --release --bin anvil --bin admin --target ${TARGET}

# The docker-compose.yml file is now configured to look for release binaries.
# We update its BINARY_PATH argument to ensure it finds the cross-compiled ones.
echo "\n--- Building Docker image and starting cluster ---"
docker compose up --build -d

echo "\n--- Cluster is starting. Run 'docker compose logs -f' to see logs. ---"
