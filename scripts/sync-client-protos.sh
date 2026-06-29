#!/usr/bin/env bash
set -euo pipefail
cp anvil-core/proto/anvil.proto clients/typescript/proto/anvil.proto
cp anvil-core/proto/anvil.proto clients/python/src/anvil_storage_client/proto/anvil.proto
cp anvil-core/proto/anvil.proto clients/rust/proto/anvil.proto
