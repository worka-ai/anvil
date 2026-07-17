#!/bin/sh
set -eu

# Test-only deterministic keys. Production images never select this script.
key_dir="${STORAGE_PATH:-/var/lib/anvil}/test-personaldb-keyring"
umask 077
mkdir -p "$key_dir"

printf '%s' 'MC4CAQAwBQYDK2VwBCIEIBERERERERERERERERERERERERERERERERERERERERER' \
    | base64 -d > "$key_dir/group-control.pk8"
printf '%s' 'MC4CAQAwBQYDK2VwBCIEICIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIi' \
    | base64 -d > "$key_dir/snapshot.pk8"
printf '%s' 'MC4CAQAwBQYDK2VwBCIEIDMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMz' \
    | base64 -d > "$key_dir/witness.pk8"
chmod 600 "$key_dir"/*.pk8

cat > "$key_dir/keyring.json" <<'JSON'
{
  "format_version": 1,
  "trusted_keys": [
    {
      "format_version": 1,
      "signature_algorithm": "ed25519",
      "key_id": "sha256:8bccb90dcbfe51e7a2f87b07db965a4d6c28da955a2a09ab89335ebffa828cc7",
      "key_generation": 1,
      "purpose": "group-control",
      "public_key_b64u": "0EqyMnQrtKs6E2i9RhXk5tAiSrcaAWuvhSCjMsl3hzc",
      "database_scopes": [],
      "group_scopes": [],
      "valid_from_log_index": 0,
      "valid_until_log_index": null,
      "status": "active"
    },
    {
      "format_version": 1,
      "signature_algorithm": "ed25519",
      "key_id": "sha256:d250e62eef868a625d72894d615e6cea9d1e94919f7769236b07606ed9f75d41",
      "key_generation": 1,
      "purpose": "snapshot",
      "public_key_b64u": "oJql9HpnWYAv-VX43C0qFKXJnSO-l_hkEn_5ODRVpPA",
      "database_scopes": [],
      "group_scopes": [],
      "valid_from_log_index": 0,
      "valid_until_log_index": null,
      "status": "active"
    },
    {
      "format_version": 1,
      "signature_algorithm": "ed25519",
      "key_id": "sha256:512ae918f6ee80cdfb87093abb416a47f64c01244b5a816a84e892825394f02e",
      "key_generation": 1,
      "purpose": "witness",
      "public_key_b64u": "F8t5-ytBIPKx7GXkGY1uCLKOgT_rAeSkAIObheGAgM4",
      "database_scopes": [],
      "group_scopes": [],
      "valid_from_log_index": 0,
      "valid_until_log_index": null,
      "status": "active"
    }
  ],
  "signers": [
    {
      "key_id": "sha256:8bccb90dcbfe51e7a2f87b07db965a4d6c28da955a2a09ab89335ebffa828cc7",
      "private_key_pkcs8_path": "group-control.pk8"
    },
    {
      "key_id": "sha256:d250e62eef868a625d72894d615e6cea9d1e94919f7769236b07606ed9f75d41",
      "private_key_pkcs8_path": "snapshot.pk8"
    },
    {
      "key_id": "sha256:512ae918f6ee80cdfb87093abb416a47f64c01244b5a816a84e892825394f02e",
      "private_key_pkcs8_path": "witness.pk8"
    }
  ]
}
JSON

export PERSONALDB_PROTOCOL_KEYRING_PATH="$key_dir/keyring.json"
exec "$@"
