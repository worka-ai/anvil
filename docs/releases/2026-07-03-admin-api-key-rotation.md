# Anvil 2026-07-03: Network admin bootstrap and secret key rotation

This release moves Anvil's bootstrap and operational administration onto the network admin API. It removes the old direct-storage admin writer, adds envelope-based secret encryption, and gives operators a safe path for generating and rotating `ANVIL_SECRET_ENCRYPTION_KEY` without giving administrative tooling access to the storage directory.

## Network-first administration

The `admin` binary is now a network client. It connects to the admin gRPC listener, normally bound to `ADMIN_LISTEN_ADDR` on an internal network, and performs tenant, application, policy, bucket, mesh lifecycle, diagnostics, repair, audit, and secret-rotation operations through authenticated admin API calls.

The old direct writer binary has been removed. Admin commands no longer need `STORAGE_PATH` and must not be given `ANVIL_SECRET_ENCRYPTION_KEY`. Server processes own the durable storage directory and the encryption keys; operators administer Anvil through the admin API.

## Bootstrap admin token

A new `ANVIL_BOOTSTRAP_ADMIN_TOKEN` setting supports first setup. When configured, the admin API accepts that bearer token and grants cluster-wide bootstrap authority. The intended sequence is:

1. start Anvil with the bootstrap token on an internal admin listener;
2. create the first tenant;
3. create the first administrative application;
4. grant that application explicit admin policy scopes;
5. remove the bootstrap token from the deployment.

The bootstrap token is not a long-term credential and should not be exposed on public networks.

## Secret encryption key envelopes

Encrypted server-side records now use Anvil key envelopes. Each envelope records a non-secret key id, nonce, and ciphertext. The active key is configured with `ANVIL_SECRET_ENCRYPTION_KEY` and `ANVIL_SECRET_ENCRYPTION_KEY_ID`.

The admin CLI now includes:

```bash
admin key generate-secret-encryption-key
```

The command prints a fresh 32-byte hex key on stdout and writes an operational warning on stderr. The key should be generated once for a storage cluster, stored in a secret manager, and kept out of logs, source control, shell history, tickets, and client configuration.

## Secret key rotation

The admin API and CLI now support rotating stored encrypted envelopes after a deployment has been restarted with a new active key and the previous key listed in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`.

Operators can dry-run and execute rotation with:

```bash
admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason "dry-run secret encryption key rotation"

admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --audit-reason "rotate secret encryption key"
```

The rotation path re-encrypts application client secrets, stored Hugging Face tokens, and local committed distributed shard files that are reachable through the node receiving the admin call. After verification, previous keys should be removed from the deployment.

This release intentionally rejects the earlier raw nonce-plus-ciphertext format. Fresh deployments should start with envelope records only.

## Admin policy management

The network admin CLI now exposes application policy grant and revoke commands:

```bash
admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id acme \
  --app-name ingest-worker \
  --action object:write \
  --resource 'raw-events/*' \
  --audit-reason "allow ingest writes"

admin --host http://127.0.0.1:50052 policy revoke \
  --tenant-id acme \
  --app-name ingest-worker \
  --action object:write \
  --resource 'raw-events/*' \
  --audit-reason "remove ingest writes"
```

These commands replace the old local mutation workflow and are recorded through the admin audit path.

## Docker and client packaging

The server Docker image now builds the `anvil` server and the network `admin` client. The Rust client crate includes the updated protocol definitions for the new admin operations.

## Verification performed on the release branch

Focused validation for this release included:

- server, core, and CLI compile checks;
- crypto envelope round-trip, previous-key re-encryption, and raw-format rejection tests;
- control-journal fenced write tests;
- admin lifecycle integration tests;
- admin API policy and secret key rotation tests;
- auth wildcard flow tests;
- CLI auth and Hugging Face key tests;
- CLI object, bucket, and ingestion tests;
- admin CLI help and key generation smoke checks.
