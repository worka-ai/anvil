#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PYSYNC'
from pathlib import Path

src = Path('anvil-core/proto/anvil.proto').read_text().rstrip() + '\n'
for path in [
    Path('clients/rust/proto/anvil.proto'),
    Path('clients/typescript/proto/anvil.proto'),
    Path('clients/python/src/anvil_storage_client/proto/anvil.proto'),
]:
    path.write_text(src)
PYSYNC
