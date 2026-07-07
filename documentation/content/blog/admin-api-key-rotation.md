---
title: Network admin bootstrap and secret key rotation
slug: /blog/admin-api-key-rotation/
description: Anvil now administers bootstrap, policy grants, and secret key rotation through the network admin API instead of direct storage writes.
---

# Network admin bootstrap and secret key rotation

Anvil stores durable object data, indexes, authz state, PersonalDB records, and operational metadata in its own storage layout. That makes administration safety critical: the tools used to create tenants, issue application credentials, and change policies must not casually mutate the storage directory or carry the same encryption keys as the server.

This release moves those responsibilities onto the admin API. The `anvil-admin` command is now a network client. It talks to the admin listener, authenticates like any other administrative caller, writes audit records, and leaves direct storage ownership with the Anvil server process.

## Why the old model had to go

The previous admin path wrote directly into Anvil's storage directory. That was useful during early bootstrap, but it created the wrong operational shape:

- provisioning jobs needed the server's durable storage mount;
- provisioning jobs needed `ANVIL_SECRET_ENCRYPTION_KEY`;
- direct writes bypassed the same network authorisation and audit boundary operators use after startup;
- deployments had to explain why a separate tool needed access to the server's private state.

The new model is simpler. Anvil owns the storage path and encryption keys. Operators call the admin API.

## First setup uses startup system-realm bootstrap

A fresh cluster still needs an initial administrator, but that authority is no longer an API bypass. On first boot, before the public or admin listeners accept traffic, the server creates the built-in system realm, installs the administrative Zanzibar schema, creates or binds the initial admin subject, and writes the owner tuple for the mesh.

The recommended bootstrap configuration creates a first admin application and writes its client id and client secret to a root-readable file:

```bash
export BOOTSTRAP_SYSTEM_ADMIN_APP_NAME=ops-admin
export BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH=/run/secrets/anvil-bootstrap-admin.json
```

After startup, the file is just an application credential. It must be exchanged through the public AuthService for a short-lived bearer token, and every admin RPC still checks the system-realm Zanzibar relation on the private admin listener. If the system realm already exists, bootstrap configuration is ignored and cannot grant new authority.

## The secret encryption key is server-only material

`ANVIL_SECRET_ENCRYPTION_KEY` is not an admin password. It is not a client credential. It is server-side key material used to encrypt records that Anvil must store and later decrypt, such as application client secrets, stored ingestion tokens, and encrypted distributed shard files.

Generate it with:

```bash
anvil-admin key generate-secret-encryption-key
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
anvil-admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason "dry-run secret encryption key rotation"

anvil-admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --audit-reason "rotate secret encryption key"
```

The old raw nonce-plus-ciphertext format is intentionally not supported by this release. New deployments should start with envelope records only.

## What changes for operators

The deployment boundary is now clearer:

- expose `API_LISTEN_ADDR` where object and native API clients need it;
- keep `ADMIN_LISTEN_ADDR` on an internal network;
- give `ANVIL_SECRET_ENCRYPTION_KEY` only to Anvil server processes;
- keep the generated bootstrap credential file private and use it only to mint normal admin tokens;
- use network admin commands for tenant, application, policy, bucket, mesh, diagnostics, repair, audit, and key rotation work.

This is the direction Anvil's operator surface will continue to follow: server-owned durable state, network-administered control, explicit authorisation, and auditability for every administrative mutation.
