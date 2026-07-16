#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PYSYNC'
from pathlib import Path

src = Path('anvil-core/proto/anvil.proto').read_text().rstrip() + '\n'


def extract_message(src: str, message_name: str) -> str:
    start_token = f'message {message_name} ' + '{'
    start_index = src.find(start_token)
    if start_index == -1:
        raise SystemExit(f'missing proto message: {message_name}')

    depth = 0
    for index in range(start_index, len(src)):
        if src[index] == '{':
            depth += 1
        elif src[index] == '}':
            depth -= 1
            if depth == 0:
                return src[start_index:index + 1]

    raise SystemExit(f'unterminated proto message: {message_name}')


def public_client_proto(src: str) -> str:
    # Client packages expose the public/admin native API. Distributed node-to-node
    # protocols stay server-internal, but public mesh bootstrap messages still
    # reference the CoreMeta row shape for topology bootstrap snapshots.
    core_meta_row = extract_message(src, 'CoreMetaRowMutation')
    blocks = [
        ('service InternalProxyService', 'service AuthService'),
        ('// ---------- CoreStore Internal Services ----------', None),
    ]
    out = src
    for start, end in blocks:
        start_index = out.find(start)
        if start_index == -1:
            raise SystemExit(f'missing proto private block start: {start}')
        if end is None:
            out = out[:start_index].rstrip() + '\n'
            continue
        end_index = out.find(end, start_index)
        if end_index == -1:
            raise SystemExit(f'missing proto private block end: {end}')
        out = out[:start_index].rstrip() + '\n\n' + out[end_index:]
    if 'message CoreMetaRowMutation {' not in out:
        anchor = 'message PutRegionRequest {'
        anchor_index = out.find(anchor)
        if anchor_index == -1:
            raise SystemExit(f'missing proto insertion anchor: {anchor}')
        out = out[:anchor_index].rstrip() + '\n\n' + core_meta_row + '\n\n' + out[anchor_index:]
    return out.rstrip() + '\n'


src = public_client_proto(src)
for path in [
    Path('clients/rust/proto/anvil.proto'),
    Path('clients/typescript/proto/anvil.proto'),
    Path('clients/python/src/anvil_storage_client/proto/anvil.proto'),
]:
    path.write_text(src)
PYSYNC
