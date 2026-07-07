---
title: Run Anvil Locally
description: Start a Docker-based local Anvil node and learn the listener, credential, and CLI boundaries.
---

# Run Anvil Locally

This tutorial starts one disposable Anvil node with Docker. It is a learning environment, not a production topology, but it uses the same separation between application traffic, operator traffic, durable state, and credentials that a real deployment uses.

The command-line tools in this page are helpers over the API. They are useful for manual setup and inspection. Applications should normally use the Rust client or the native API directly, then reserve the CLI for provisioning, smoke tests, and operator checks. The detailed command references are [Public CLI](/reference/public-cli/) and [Admin CLI](/reference/admin-cli/).

## Understand the two request surfaces

Before you open a port, know which surface you are opening and who is meant to call it.

The **public API** is the application-facing surface. It listens on port `50051` in the default container configuration. Tenant applications, automation jobs, S3-compatible gateway clients, and the public `anvil` CLI use this surface for authentication, bucket and object operations, indexes, watches, PersonalDB, and tenant-owned authorisation. In a real deployment this is the surface you may put behind a public or tenant-facing load balancer. In this tutorial it is published only on `127.0.0.1`.

The **admin API** is the operator-facing surface. It listens on port `50052` by default. It changes system-level state such as the system realm, tenants, first application credentials, topology, routing, repair operations, and server-side secret envelopes. It must stay on loopback, a private management network, or an equivalent operator-only path. Do not publish it to the internet and do not treat it as an application API.

Operators sometimes call these surfaces the public plane and the admin plane. Anvil keeps them separate so that ordinary application traffic can be exposed without also exposing system administration. The split is a network boundary and a mental model: public clients should not need admin reachability, and admin automation should be deliberately routed through a private path. For a fuller treatment of bind addresses, load balancers, cluster traffic, and the `7443` node-to-node port, see [Network and Ports](/operators/network-and-ports/).

## Generate server secret material with Docker

Anvil needs server-side secret encryption material before it can persist encrypted server secrets, including stored application credential envelopes. Set `ANVIL_IMAGE` to the Anvil server image you intend to run, then generate a local-only value with that image so this setup remains Docker-first. In production, pin the image by release tag or digest and generate/store the key with your secret manager; the lifecycle is covered in [Secrets and Key Management](/operators/secrets-and-key-management/).

```bash
export ANVIL_IMAGE="${ANVIL_IMAGE:-ghcr.io/<your-org>/anvil:latest}"

export ANVIL_SECRET_ENCRYPTION_KEY="$(
  docker run --rm "$ANVIL_IMAGE" \
    anvil-admin key generate-secret-encryption-key 2>/dev/null
)"
export ANVIL_SECRET_ENCRYPTION_KEY_ID="local-tutorial"
```

After this command, your shell has the key material required to start the local server. This key is not an administrator password, not a client secret, and not a bearer token. It belongs only to the Anvil server process that owns the storage directory.

## Start a single-node container

The next command creates a named Docker volume for Anvil-owned state and starts the server. It publishes only the public API on host loopback. The admin API is bound to loopback inside the container and is not published to the host. The first-start credential file is written inside the Anvil volume so you can copy it out deliberately after startup.

```bash
docker volume create anvil-local-data

docker run -d \
  --name anvil-local \
  -p 127.0.0.1:50051:50051 \
  -v anvil-local-data:/var/lib/anvil \
  -e STORAGE_PATH=/var/lib/anvil \
  -e REGION=local \
  -e API_LISTEN_ADDR=0.0.0.0:50051 \
  -e PUBLIC_API_ADDR=http://127.0.0.1:50051 \
  -e ADMIN_LISTEN_ADDR=127.0.0.1:50052 \
  -e JWT_SECRET=local-jwt-secret-change-me \
  -e ANVIL_SECRET_ENCRYPTION_KEY_ID="$ANVIL_SECRET_ENCRYPTION_KEY_ID" \
  -e ANVIL_SECRET_ENCRYPTION_KEY="$ANVIL_SECRET_ENCRYPTION_KEY" \
  -e CLUSTER_SECRET=local-cluster-secret-change-me \
  -e INIT_CLUSTER=true \
  -e ENABLE_MDNS=false \
  -e BOOTSTRAP_SYSTEM_ADMIN_APP_NAME=system-admin \
  -e BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH=/var/lib/anvil/bootstrap/system-admin.json \
  -e ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/system-admin.json \
  -e ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051 \
  -e ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052 \
  "$ANVIL_IMAGE"
```

Docker prints the volume name and then the container id. From this point, `/var/lib/anvil` in the container is the durable state directory for this tutorial. Do not edit files in that directory by hand; the server owns CoreStore state, indexes, system-realm records, audit data, and encrypted secret envelopes.

The host can reach `http://127.0.0.1:50051` because the public API was published. The host cannot reach `50052` because the admin API was not published. That is intentional: local admin checks in this page run with `docker exec`, which keeps operator traffic inside the container boundary instead of opening the admin port.

## Wait for the public API to become ready

Use the public listener's readiness endpoint as a startup check. This confirms the container is accepting local public API traffic; it does not prove that authorisation, credentials, or later tenant setup are correct.

```bash
curl -fsS http://127.0.0.1:50051/ready
```

If the command exits successfully, the local public API listener is ready for the next step. If it fails, inspect the container logs before changing configuration; startup failures usually point to missing required environment, a reused container name, or a state directory from an earlier incompatible experiment.

When readiness does not pass, this command shows the recent server log lines without changing the container.

```bash
docker logs --tail 80 anvil-local
```

Use the log output to fix the cause, then re-run the readiness check. Avoid deleting the volume unless you are intentionally starting the tutorial from scratch.

## Understand the first administration credential

On first startup, if the system realm does not exist, Anvil initialises that realm and creates the first system administration service principal and credential. In this tutorial the principal is named `system-admin`, and the bootstrap-generated credential file is written to `/var/lib/anvil/bootstrap/system-admin.json` inside the container.

That file is long-lived credential material for the first system administration principal. It is not a request credential. It does not make API calls by itself. The CLI uses the client id and secret to ask the public API for short-lived bearer tokens, and the admin API still checks system-realm authorisation for each admin operation. If the system realm already exists, the first-start credential settings are ignored rather than creating another first administrator.

Copy the generated file out only so your local CLI can create a profile for this tutorial. The `chmod` keeps the copied secret readable only by your user on Unix-like systems.

```bash
docker cp anvil-local:/var/lib/anvil/bootstrap/system-admin.json /tmp/anvil-system-admin.json
chmod 600 /tmp/anvil-system-admin.json
```

After this command, `/tmp/anvil-system-admin.json` contains the first system administration client id and client secret. Treat it like a password file. Store real deployment credentials in a secret manager, not in `/tmp`, shell history, source control, container images, or chat transcripts.

## Configure a CLI profile for token exchange

`anvil static-config` writes a CLI profile non-interactively. A profile has a name, the public API endpoint it should call, and the client id and client secret the CLI can use for token exchange. The profile is CLI convenience: it is not a grant, not a separate security boundary, and not the primary product interface for applications.

The profile points at `http://127.0.0.1:50051` because token exchange is part of public authentication. This command reads the copied credential file with `jq`; if you do not have `jq`, copy the `client_id` and `client_secret` values from the JSON file into environment variables and pass those instead.

```bash
anvil static-config \
  --name local-system \
  --host http://127.0.0.1:50051 \
  --client-id "$(jq -r .client_id /tmp/anvil-system-admin.json)" \
  --client-secret "$(jq -r .client_secret /tmp/anvil-system-admin.json)" \
  --default
```

After this command, the public `anvil` CLI has a default profile named `local-system`. Later commands can use that profile without repeating the endpoint or long-lived credential material on every invocation. For more profile and tenant-facing command examples, see [Public CLI](/reference/public-cli/).

## Mint a short-lived bearer token

You might wonder why `anvil auth get-token` exists when the profile already has a client id and client secret. The difference is credential lifetime and purpose. The client id and client secret are long-lived credential material, so you store and rotate them carefully. A bearer token is a short-lived request credential, so individual API calls can carry it without sending the long-lived secret every time.

This command asks the public API to exchange the profile's client id and client secret for a bearer token, then stores that token in your shell for manual checks.

```bash
export ANVIL_AUTH_TOKEN="$(anvil auth get-token)"
printf 'received bearer token with %s characters\n' "${#ANVIL_AUTH_TOKEN}"
```

The printed length tells you the token exchange worked without dumping the token itself into the terminal. The CLI can also mint tokens automatically when it has a profile, but making the exchange visible here helps you understand what applications do through the API or Rust client.

## Check the private admin API without exposing it

The host still should not be able to dial the admin listener. To prove the admin API is alive without publishing port `50052`, run the admin CLI inside the container. The environment variables supplied at container start tell the CLI where the credential file, public API, and admin API are.

```bash
docker exec anvil-local anvil-admin audit list --limit 5
```

A successful response means the admin CLI authenticated through the public API, received a bearer token, reached the loopback-only admin API from inside the container, and passed the system-realm authorisation check for listing audit events. For the full operator command surface, use [Admin CLI](/reference/admin-cli/).

## Know what an app means in Anvil

The generated `system-admin` identity is an app in current CLI terminology. In Anvil, an app is an application or service principal: a credentialed identity for automation, services, importers, operators, or tenant-owned tools. It is not an S3-era artefact and it is not limited to object storage.

Separate apps let you give different services independent audit trails, rotation schedules, scopes, revocation paths, and least-privilege policies. Do not use the first system administration app as a general application credential. The next tutorial that creates tenants and service principals is [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/).

## Clean up the local node

When you are finished, remove the tutorial container, its Docker volume, the copied credential file, and the shell variables created by this page. Do not run this cleanup if you want to keep experimenting with the same local state.

```bash
docker rm -f anvil-local
docker volume rm anvil-local-data
rm -f /tmp/anvil-system-admin.json
unset ANVIL_AUTH_TOKEN ANVIL_SECRET_ENCRYPTION_KEY ANVIL_SECRET_ENCRYPTION_KEY_ID
```

After cleanup, the server state for this tutorial is gone. Your CLI profile may still exist in your user configuration; overwrite it with another profile or edit your CLI config if you no longer want `local-system` as the default.
