#!/usr/bin/env bash
set -euo pipefail
cp anvil-core/proto/anvil.proto clients/typescript/proto/anvil.proto
cp anvil-core/proto/anvil.proto clients/python/src/anvil_storage_client/proto/anvil.proto
python3 - <<'PYFILTER'
from pathlib import Path
import re

src = Path('anvil-core/proto/anvil.proto').read_text()
for name in [
    'PutShardRequest',
    'PutShardResponse',
    'CommitShardRequest',
    'CommitShardResponse',
    'GetShardRequest',
    'GetShardResponse',
    'DeleteShardRequest',
    'DeleteShardResponse',
]:
    src = re.sub(r'\nmessage ' + name + r' \{\}\n', '\n', src)
    src = re.sub(
        r'\nmessage ' + name + r' \{\n(?:[^{}]*|\{[^{}]*\})*?\}\n',
        '\n',
        src,
        flags=re.S,
    )
src = src.replace(
    '''\n// Internal Service for node-to-node communication\nservice InternalAnvilService {\n  rpc PutShard(stream PutShardRequest) returns (PutShardResponse);\n  rpc GetShard(GetShardRequest) returns (stream GetShardResponse);\n  rpc CommitShard(CommitShardRequest) returns (CommitShardResponse);\n  rpc DeleteShard(DeleteShardRequest) returns (DeleteShardResponse);\n}\n''',
    '\n',
)
Path('clients/rust/proto/anvil.proto').write_text(src)
PYFILTER
