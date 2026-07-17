---
title: Secrets and Key Management
description: Understand Anvil server secrets, bootstrap credentials, app secrets, bearer tokens, blast radius, rotation, and recovery.
---

# Secrets and Key Management

Anvil uses several kinds of secret material, and they do different jobs. A server signing secret is not the same as an application client secret. A server-side encryption key is not an administrator password. A bootstrap credential is not a permanent bypass. A bearer token is not a long-lived identity record. Treating all of them as "tokens" leads to dangerous runbooks because rotation, blast radius, and recovery are different for each one.

This page explains the secret types operators must manage before a production deployment. Read it with [Deployment](/operators/deployment/), [Network and Ports](/operators/network-and-ports/), [Admin Plane](/operators/admin-plane/), [Security Hardening](/operators/security-hardening/), [Backup and Recovery](/operators/backup-and-recovery/), [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/), [Run Anvil Locally](/tutorials/setup-local-anvil/), [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/), [Public CLI](/reference/public-cli/), and [Admin CLI](/reference/admin-cli/).

## The Secret Inventory

Operators should be able to name every secret in a running deployment and answer four questions about it: who holds it, what it can do, what breaks when it changes, and how it is recovered.

| Secret | Who should hold it | Main blast radius |
| --- | --- | --- |
| `JWT_SECRET` | Anvil server processes that mint or verify bearer tokens | Changing it invalidates outstanding bearer tokens signed with the old value unless every verifying node changes in a coordinated window. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Anvil server processes only | Losing it can make encrypted server-side secrets unrecoverable; leaking it exposes encrypted secret envelopes if storage is also available. |
| `ANVIL_SECRET_ENCRYPTION_KEY_ID` | Anvil server configuration and operator records | Labels new encrypted envelopes; changing it without changing the key mostly changes metadata, but it must remain consistent and meaningful. |
| `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` | Anvil server processes during rotation | Allows old envelopes to decrypt while rotation rewrites them to the active key id. |
| `PERSONALDB_PROTOCOL_SIGNING_MANIFEST_PATH` | Anvil coordinators and role-scoped signer processes | Supplies public trust records and distinct Unix signer endpoints. It contains no private key. |
| Purpose-scoped PersonalDB PKCS#8 key | Only the matching `anvil-signer` process | Signs one allowed class of PersonalDB control evidence; loss prevents new signatures for that purpose, while leakage can forge evidence within the key's scope, generation, and log boundaries. |
| `CLUSTER_SECRET` | Anvil server processes in the same mesh | Protects cluster gossip metadata; a mismatch can make peers reject each other's signed cluster messages. |
| Bootstrap first-admin credential | Initial system administrators or provisioning automation | Can mint a token for a powerful system principal until rotated, deleted, or access is otherwise removed. |
| Tenant/app client secrets | The service or automation that owns the app credential | Can mint short-lived bearer tokens with that app's delegated public policy scopes. |
| Bearer tokens | Processes making API calls | Authorise one caller for a short period; current tokens are minted with a one-hour expiry. |

Store server secrets in an operator secret manager. Store application client secrets in the secret manager for the service that uses them. Do not put any of these values in source control, image layers, public Compose files, shell history, monitoring labels, support tickets, or screenshots.

## `JWT_SECRET`: Signing Short-Lived Tokens

`JWT_SECRET` is the secret Anvil uses to sign and verify bearer tokens. When an app credential calls the authentication API, Anvil returns a bearer token with a subject, tenant id, scopes, expiry, and token id. Current tokens expire after about one hour. Every Anvil process that must accept those tokens needs the same signing secret, or an otherwise compatible signing configuration for the release you operate.

The blast radius is serving authentication rather than stored data. If `JWT_SECRET` changes abruptly on one node, tokens minted or accepted by other nodes may fail verification. If it leaks, an attacker with enough knowledge of claims could forge bearer tokens until you rotate the secret and old forged tokens expire or are rejected. Rotate it as a coordinated deployment event: update the server secret, restart or roll nodes according to your deployment strategy, and expect active clients to refresh tokens.

There is no current multi-key JWT rotation surface documented in the public operator CLI. That means a rotation is more disruptive than the server-side envelope rotation described later. Plan it for a maintenance window or a controlled rolling process, and verify public authentication and admin authentication after the change.

## `ANVIL_SECRET_ENCRYPTION_KEY`: Protecting Stored Secret Envelopes

`ANVIL_SECRET_ENCRYPTION_KEY` is a 32-byte hex key used by Anvil servers to encrypt persisted server-side secrets. Examples include stored application client-secret envelopes and configured integration secrets. It is server-only material. The network `anvil-admin` CLI does not need this key because the CLI never decrypts storage itself; it asks the server to perform authorised operations through the admin API, and the server already has its configured keyring.

Generate the key with the local helper in `anvil-admin`:

```bash
anvil-admin key generate-secret-encryption-key
```

This command does not contact a server. It prints one random hex value suitable for `ANVIL_SECRET_ENCRYPTION_KEY`. The explanatory warning is printed separately so operators understand that losing the key can make encrypted secrets unrecoverable. Generate one active key for a storage cluster, store it in a secret manager, and inject it only into Anvil server processes. Do not hand it to tenant applications, CI jobs that only call APIs, public CLIs, or operators running network-only admin commands.

The recovery implication is severe: a backup of `STORAGE_PATH` without the key history needed to decrypt its envelopes may be incomplete. A key without the matching storage is also not useful. Backup plans must protect both durable storage and the relevant secret key history.

## PersonalDB Protocol Signing Keys

`PERSONALDB_PROTOCOL_SIGNING_MANIFEST_PATH` is mandatory at coordinator
startup. It names a JSON manifest containing trusted Ed25519 public keys and
three purpose-separated Unix-domain signer endpoints. The coordinator does not
load private keys and has no file-backed signing mode. These four protocol
objects have no production HMAC or unsigned fallback.

Run one `anvil-signer` process for each purpose: `group-control`, `snapshot`,
and `witness`. Use a separate Unix account or container boundary and a separate
PKCS#8 Ed25519 private key for every process. Private-key files must be regular,
non-symlink files and must not be accessible by group or other users. Create
each socket parent as a mode-`0700` directory. The signer creates a mode-`0600`
socket, verifies the connecting process UID against its explicit allowlist, and
accepts only the bounded typed PersonalDB objects allowed for its purpose.

The version-1 manifest has this shape:

```json
{
  "format_version": 1,
  "trusted_keys": [
    {
      "format_version": 1,
      "signature_algorithm": "ed25519",
      "key_id": "sha256:<64 lowercase hex>",
      "key_generation": 1,
      "purpose": "witness",
      "public_key_b64u": "<32 raw bytes as unpadded base64url>",
      "database_scopes": ["pdb_example"],
      "group_scopes": ["pdb_example"],
      "valid_from_log_index": 0,
      "valid_until_log_index": null,
      "status": "active"
    }
  ],
  "signer_endpoints": [
    {
      "purpose": "witness",
      "key_id": "sha256:<same canonical public-key ID>",
      "socket_path": "/run/anvil-signers/witness/sign.sock"
    }
  ]
}
```

Provide exactly one endpoint for each of the three purposes. Socket paths and
active key IDs must be distinct. Every endpoint key must exist as an active
trust record for the exact same purpose; missing, overlapping, or
wrong-purpose bindings stop coordinator startup. Legacy `signers` entries with
`private_key_pkcs8_path` are rejected.

Start a witness signer with configuration equivalent to:

```bash
anvil-signer \
  --trust-manifest-path /run/anvil/personaldb-signing.json \
  --purpose witness \
  --key-id 'sha256:<canonical public-key ID>' \
  --socket-path /run/anvil-signers/witness/sign.sock \
  --private-key-pkcs8-path /run/secrets/witness/key.pk8 \
  --allowed-peer-uid 10001
```

Repeat with independent directories, keys, key IDs, and processes for
`group-control` and `snapshot`. Never mount `/run/secrets/witness` or another
signer key directory into an `anvil-server` container.

Key IDs are SHA-256 over `personaldb-protocol`'s canonical deterministic
protobuf public-key envelope, not over a PEM file or textual public key. The
signer derives generation, purpose, scopes, status, and log boundaries from the
matching trusted-key record; none of those values are duplicated in a signature
envelope. The signer checks that its private key, configured purpose, endpoint,
key ID, peer allowlist, and active trust record agree before binding its socket.
The coordinator verifies every returned canonical envelope against its own
trust store before accepting it. In the current Anvil PersonalDB model, the
database ID is also the protocol group ID, so a non-empty `group_scopes` entry
must use that same value. Leave either scope list empty only when the key is
intentionally unrestricted on that dimension.

For rotation, retain old public keys as `retiring` with an exclusive
`valid_until_log_index` so historical evidence below that boundary remains
verifiable, and install a new `active` generation for new signatures.
`revoked_future` and `compromised` records also require an exclusive
`valid_until_log_index`; evidence at or after that boundary fails closed.
Removing an old public key also removes the ability to verify retained history
that it signed.

## Key IDs And Previous Keys

`ANVIL_SECRET_ENCRYPTION_KEY_ID` is written into new encrypted envelopes. It lets Anvil know which configured key should decrypt a record later. Keep ids human-readable and stable, such as `2026-07-primary`. Do not reuse the same id for different key material.

`ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` is a comma-delimited list of `key_id:hex_key` entries. It is used during rotation so records encrypted with old key ids remain readable while the active key rewrites them. A typical rotation configuration looks like this:

```bash
ANVIL_SECRET_ENCRYPTION_KEY_ID=2026-08-primary
ANVIL_SECRET_ENCRYPTION_KEY=new_64_hex_key_from_secret_manager
ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS=2026-07-primary:old_64_hex_key_from_secret_manager
```

This configuration says: encrypt new envelopes with `2026-08-primary`, but still decrypt envelopes labelled `2026-07-primary`. Previous keys should remain configured only for the rotation window and verification period. Keeping old keys forever increases the blast radius of an old leak.

## Rotating Server-Side Secret Envelopes

Server-side envelope rotation has two phases. First, deploy the new keyring configuration so servers know both the new active key and the previous key. Secondly, call the admin API to re-encrypt existing envelopes with the active key id.

Run a dry run first:

```bash
anvil-admin secret-encryption-key rotate --dry-run \
  --audit-reason 'verify secret envelope rotation to 2026-08-primary'
```

The dry run proves the authenticated admin principal has the system-realm relation for managing secret encryption keys, the admin API can inspect the relevant encrypted records, and the server can decrypt them with the configured keyring. It does not rewrite records.

Run the real rotation only after the dry run is clean:

```bash
anvil-admin secret-encryption-key rotate \
  --audit-reason 'rotate secret envelopes to 2026-08-primary'
```

The current server rotates application secret envelopes and configured integration secret envelopes that the rotation backend knows how to inspect. The response includes counts such as examined records, rotated records, already-active records, active key id, dry-run state, and audit event id. After the real run, verify that application credential rotation, token minting, and affected integrations still work. Only then remove the previous key from server configuration and restart or roll the nodes.

If the old key leaked, shorten the overlap window. If the old key was lost before records were rotated, encrypted records that still require it may be unrecoverable from backup. That is why key history is part of the backup boundary.

## `CLUSTER_SECRET`: Node-To-Node Trust

The current server environment variable is `CLUSTER_SECRET`; older snippets that use a different cluster-secret env name are stale. It is the shared secret used to sign and verify cluster gossip metadata between Anvil nodes. If it is absent, the current code can run without this shared-secret verification path, but production deployments should configure it and keep cluster traffic on private networks.

The blast radius is mesh coordination, peer metadata, and routing freshness rather than direct tenant API credentials. If two nodes use different cluster secrets, signed cluster messages can be rejected and the mesh may look split or stale even though each process still answers local requests. If the secret leaks, rotate it with a planned rolling deployment that keeps peer compatibility in mind for the version you operate. Also check network policy; a cluster secret is not a substitute for private node-to-node reachability.

Do not confuse `CLUSTER_SECRET` with the persisted libp2p cluster keypair. The cluster keypair defaults to an operator identity directory beside `STORAGE_PATH`, not below it; it is part of stable node identity. `CLUSTER_SECRET` is configuration supplied to server processes.

## Bootstrap First-Admin Credential

On a fresh storage directory where the system realm is absent, startup can create the first system administration application and write its credential JSON to `BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH`. That credential contains a client id and client secret. It is used to mint short-lived bearer tokens through the public authentication API; it is not itself a bearer token.

Bootstrap is below the API because no system administrator exists yet. After the system realm exists, bootstrap settings are ignored rather than minting more authority. From then on, admin operations authenticate normally and authorise through the built-in system realm.

The first-admin credential has a large blast radius because it belongs to a system principal. Copy it into a secret manager, restrict access, use it to create normal operator workflows, and rotate or retire it as soon as those workflows exist. If it leaks, rotate the application secret or remove the credential through the admin API from a still-authorised system principal. If every system administration credential is lost and the system realm already exists, the bootstrap environment variables will not recreate authority; treat that as a serious recovery scenario and plan operational break-glass procedures before production.

## Tenant And Application Client Secrets

Tenant and system applications authenticate with client id and client secret pairs. Admin provisioning can create the first tenant application, and tenant principals can create their own application credentials through the public API when delegated. A successful create or rotate prints the client secret once. Store it immediately.

An admin-side rotation for an application credential uses the private admin API and requires an expected generation because it updates an existing record:

```bash
anvil-admin app rotate-secret \
  --tenant-id acme \
  --app-name acme-admin \
  --expected-generation 1 \
  --audit-reason 'rotate acme-admin application secret'
```

Tenant-owned applications can also rotate through the public API when authorised:

```bash
anvil app rotate-secret docs-writer
```

Rotating an app secret stops future token minting with the old secret. Existing bearer tokens remain valid until they expire unless another control invalidates them. The blast radius is the scopes delegated to that application. A narrow object writer secret is a smaller incident than an owner credential that can delegate policy, create apps, and write broad object prefixes.

Do not use the first system-admin credential inside tenant publishing jobs. Tenant applications should hold tenant-scoped app secrets and call the public API. Operators should use system credentials only for system work such as tenant bootstrap, topology, diagnostics, repair, and secret-envelope rotation.

## Bearer Tokens

Bearer tokens are short-lived request credentials minted from app credentials. Current Anvil tokens include a subject, tenant id, scopes, token id, and expiry, and they expire after about one hour. They are presented as `Authorization: Bearer ...` metadata on public and admin API calls.

The blast radius of a leaked bearer token is usually smaller than the blast radius of the client secret that minted it, because the token expires. It can still be serious: until expiry, the holder can exercise the scopes encoded in the token. Do not log bearer tokens, S3 signatures, app secrets, first-admin credentials, or server keys. Redact them at reverse proxies, application logs, CLI transcripts, and support tooling.

Changing `JWT_SECRET` invalidates outstanding tokens signed with the old secret. Rotating an application client secret prevents new tokens from being minted with the old app secret but does not necessarily invalidate tokens already issued. Incident response should account for both behaviours.

## Why `anvil-admin` Is Network-Only

`anvil-admin` is intentionally a network client. Except for the local key-generation helper, it talks to the private admin API with a bearer token. It should not receive `STORAGE_PATH`, `ANVIL_SECRET_ENCRYPTION_KEY`, raw database files, or mounted CoreStore directories. That separation is a safety property: the server evaluates authentication, system-realm authorisation, validation, generation checks, idempotency, and audit before mutating durable state.

If an operator script needs direct storage access to rotate a secret, it is bypassing the model. Use the admin API for server-side envelope rotation, app secret rotation, and audit evidence. Use backups for recovery, not as a shadow control plane.

## Operator Runbook Guidance

Generate server-side encryption keys with the helper, store all server secrets in a secret manager, and keep key ids tied to calendar or release events. Inject secrets into containers or pods at runtime; do not bake them into images. Keep the admin API private, and keep the network admin CLI away from server key material.

Rotate when a secret leaks, when staff or automation with access leaves the trust boundary, before old key history becomes too large, or as part of scheduled security maintenance. For `ANVIL_SECRET_ENCRYPTION_KEY`, rotate with previous keys configured and verify the admin rotation response before removing old keys. For `JWT_SECRET` and `CLUSTER_SECRET`, plan coordinated node rollout because there is no current documented multi-key overlap workflow equivalent to envelope rotation. For app credentials, rotate the specific app and update only the service that owns it.

Finally, test recovery. A restore drill should prove that Anvil can start with restored `STORAGE_PATH`, the active and previous encryption keys needed for that backup, the expected JWT and cluster secret configuration, and at least one authorised admin credential. A backup that cannot decrypt app secrets or cannot authenticate an operator is not a complete recovery plan.

## Rotation evidence

A secret rotation is complete only after new credentials are stored, old credentials are no longer accepted where intended, dependent services have reloaded, and audit records show who performed the change. For server-side secret-envelope rotation, keep previous keys configured until the rotation command and application credential smoke tests both pass.

Do not confuse key generation with key installation. `anvil-admin key generate-secret-encryption-key` prints local material. The server uses it only after operators store it in configuration and restart or roll nodes according to the deployment model.
