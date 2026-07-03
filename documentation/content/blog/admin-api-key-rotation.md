---
title: Network admin bootstrap and secret key rotation
slug: /blog/admin-api-key-rotation/
description: Anvil now administers bootstrap, policy grants, and secret key rotation through the network admin API instead of direct storage writes.
---

# Network admin bootstrap and secret key rotation

Anvil stores durable object data, indexes, authz state, PersonalDB records, and operational metadata in its own storage layout. That makes administration safety critical: the tools used to create tenants, issue application credentials, and change policies must not casually mutate the storage directory or carry the same encryption keys as the server.

This release moves those responsibilities onto the admin API. The `admin` command is now a network client. It talks to the admin listener, authenticates like any other administrative caller, writes audit records, and leaves direct storage ownership with the Anvil server process.

## Why the old model had to go

The previous admin path wrote directly into Anvil's storage directory. That was useful during early bootstrap, but it created the wrong operational shape:

- provisioning jobs needed the server's durable storage mount;
- provisioning jobs needed `ANVIL_SECRET_ENCRYPTION_KEY`;
- direct writes bypassed the same network authorisation and audit boundary operators use after startup;
- deployments had to explain why a separate tool needed access to the server's private state.

The new model is simpler. Anvil owns the storage path and encryption keys. Operators call the admin API.

## First setup now uses a bootstrap token

A fresh cluster still needs a way to create its first tenant and first administrative application. `ANVIL_BOOTSTRAP_ADMIN_TOKEN` exists for that narrow window.

Start Anvil with the bootstrap token on an internal admin listener, then run:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

admin --host http://127.0.0.1:50052 tenant create \
  --name default \
  --audit-reason "initial tenant"

admin --host http://127.0.0.1:50052 app create \
  --tenant-id default \
  --app-name ops-admin \
  --audit-reason "initial admin app"

admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id default \
  --app-name ops-admin \
  --action 'anvil_admin:*' \
  --resource 'anvil_admin:cluster:default' \
  --audit-reason "grant admin capability"
```

After that, remove the bootstrap token from the deployment and use explicit application credentials for normal administration.

## The secret encryption key is server-only material

`ANVIL_SECRET_ENCRYPTION_KEY` is not an admin password. It is not a client credential. It is server-side key material used to encrypt records that Anvil must store and later decrypt, such as application client secrets, stored ingestion tokens, and encrypted distributed shard files.

Generate it with:

```bash
admin key generate-secret-encryption-key
```

The command writes the key to stdout and a warning to stderr. Store the key in a secret manager. Do not put it in source control, client configuration, logs, tickets, or shell history. If the key is lost, encrypted records that depend on it are unrecoverable. If it leaks, rotate it.

## Envelopes make rotation practical

New encrypted records are written as Anvil key envelopes. Each envelope stores:

- a non-secret key id;
- a nonce;
- the ciphertext.

The key id lets the server know which configured key can decrypt a record. During rotation, a node can read records encrypted with a previous key and write them back with the active key.

A normal rotation looks like this:

1. generate a new key;
2. restart Anvil with the new `ANVIL_SECRET_ENCRYPTION_KEY` and a new `ANVIL_SECRET_ENCRYPTION_KEY_ID`;
3. configure the old key in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` for the rotation window;
4. run a dry run;
5. run the rotation;
6. verify the counters and smoke tests;
7. restart without the old key configured.

```bash
admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason "dry-run secret encryption key rotation"

admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --audit-reason "rotate secret encryption key"
```

The old raw nonce-plus-ciphertext format is intentionally not supported by this release. New deployments should start with envelope records only.

## What changes for operators

The deployment boundary is now clearer:

- expose `API_LISTEN_ADDR` where object and native API clients need it;
- keep `ADMIN_LISTEN_ADDR` on an internal network;
- give `ANVIL_SECRET_ENCRYPTION_KEY` only to Anvil server processes;
- use `ANVIL_BOOTSTRAP_ADMIN_TOKEN` only for first setup;
- use network admin commands for tenant, application, policy, bucket, mesh, diagnostics, repair, audit, and key rotation work.

This is the direction Anvil's operator surface will continue to follow: server-owned durable state, network-administered control, explicit authorisation, and auditability for every administrative mutation.
