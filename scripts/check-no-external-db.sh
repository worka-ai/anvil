#!/usr/bin/env bash
set -euo pipefail

# Anvil must not depend on an external relational metadata store.
# Keep the regex self-nonmatching so this checker can scan the whole repo.
pattern='post[g]res|post[g]resql|pg[v]ector|s[q]lx|tokio[-]post[g]res|deadpool[-]post[g]res|DATABASE[_]URL|P[O]STGRES'

if rg -n -i -uu "$pattern" . \
  -g '!target/**' \
  -g '!**/.git/**'; then
  echo "External relational metadata-store reference found; Anvil must be self-contained." >&2
  exit 1
fi
