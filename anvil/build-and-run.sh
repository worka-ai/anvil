#!/bin/bash
# This script first builds the release binaries and then starts the Docker Compose cluster.
# This is the recommended way to run the cluster locally.
set -e

echo "--- Building Anvil release binaries ---"
cargo build --release --bin anvil --bin admin

echo "\n--- Building Docker image and starting cluster ---"
docker compose up --build -d

echo "\n--- Cluster is starting. Run 'docker compose logs -f' to see logs. ---"
