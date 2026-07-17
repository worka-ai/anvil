#!/bin/sh
set -eu

# Explicit test-only custody fixture. This starts three separate role-scoped signer
# processes; production coordinators never receive these PKCS#8 files.
signer_root="${STORAGE_PATH:-/var/lib/anvil}/test-personaldb-signers"
socket_root="$signer_root/sockets"
manifest_path="$signer_root/signing-manifest.json"
peer_uid="$(id -u)"

umask 077
mkdir -p \
    "$signer_root/keys" \
    "$socket_root/group-control" \
    "$socket_root/proposal-admission" \
    "$socket_root/snapshot" \
    "$socket_root/witness"
chmod 700 "$signer_root" "$signer_root/keys" "$socket_root" "$socket_root"/*

printf '%s' 'MC4CAQAwBQYDK2VwBCIEIBERERERERERERERERERERERERERERERERERERERERER' \
    | base64 -d > "$signer_root/keys/group-control.pk8"
printf '%s' 'MC4CAQAwBQYDK2VwBCIEIDw8PDw8PDw8PDw8PDw8PDw8PDw8PDw8PDw8PDw8PDw8' \
    | base64 -d > "$signer_root/keys/proposal-admission.pk8"
printf '%s' 'MC4CAQAwBQYDK2VwBCIEICIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIi' \
    | base64 -d > "$signer_root/keys/snapshot.pk8"
printf '%s' 'MC4CAQAwBQYDK2VwBCIEIDMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMz' \
    | base64 -d > "$signer_root/keys/witness.pk8"
chmod 600 "$signer_root/keys"/*.pk8

cat > "$manifest_path" <<JSON
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
      "key_id": "sha256:a87d75314f763867c676badd4d428f107dbf32e43a82f6c33dda8fa35c06e302",
      "key_generation": 1,
      "purpose": "proposal-admission",
      "public_key_b64u": "VSb3QpQXEbO8UwukT_b22rDwq3Gvgy9Bp_47n9rtnGA",
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
  "signer_endpoints": [
    {
      "purpose": "group-control",
      "key_id": "sha256:8bccb90dcbfe51e7a2f87b07db965a4d6c28da955a2a09ab89335ebffa828cc7",
      "socket_path": "$socket_root/group-control/sign.sock"
    },
    {
      "purpose": "proposal-admission",
      "key_id": "sha256:a87d75314f763867c676badd4d428f107dbf32e43a82f6c33dda8fa35c06e302",
      "socket_path": "$socket_root/proposal-admission/sign.sock"
    },
    {
      "purpose": "snapshot",
      "key_id": "sha256:d250e62eef868a625d72894d615e6cea9d1e94919f7769236b07606ed9f75d41",
      "socket_path": "$socket_root/snapshot/sign.sock"
    },
    {
      "purpose": "witness",
      "key_id": "sha256:512ae918f6ee80cdfb87093abb416a47f64c01244b5a816a84e892825394f02e",
      "socket_path": "$socket_root/witness/sign.sock"
    }
  ]
}
JSON

start_signer() {
    purpose="$1"
    key_id="$2"
    anvil-signer \
        --trust-manifest-path "$manifest_path" \
        --purpose "$purpose" \
        --key-id "$key_id" \
        --socket-path "$socket_root/$purpose/sign.sock" \
        --private-key-pkcs8-path "$signer_root/keys/$purpose.pk8" \
        --allowed-peer-uid "$peer_uid" &
}

start_signer \
    group-control \
    sha256:8bccb90dcbfe51e7a2f87b07db965a4d6c28da955a2a09ab89335ebffa828cc7
start_signer \
    proposal-admission \
    sha256:a87d75314f763867c676badd4d428f107dbf32e43a82f6c33dda8fa35c06e302
start_signer \
    snapshot \
    sha256:d250e62eef868a625d72894d615e6cea9d1e94919f7769236b07606ed9f75d41
start_signer \
    witness \
    sha256:512ae918f6ee80cdfb87093abb416a47f64c01244b5a816a84e892825394f02e

for socket in \
    "$socket_root/group-control/sign.sock" \
    "$socket_root/proposal-admission/sign.sock" \
    "$socket_root/snapshot/sign.sock" \
    "$socket_root/witness/sign.sock"
do
    attempts=0
    until [ -S "$socket" ]; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 100 ]; then
            echo "test PersonalDB signer failed to create $socket" >&2
            exit 1
        fi
        sleep 0.05
    done
done

export PERSONALDB_PROTOCOL_SIGNING_MANIFEST_PATH="$manifest_path"
exec "$@"
