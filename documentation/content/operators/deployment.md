---
title: Deployment
description: Deploy Anvil with Docker, durable storage, explicit secrets, separated listeners, first-boot bootstrap, and smoke-test evidence.
---

# Deployment

Deploying Anvil is the point where the production model becomes concrete. The server needs a durable storage path, long-lived server secret material, a region identity, public and private listeners, cluster addresses, and a first administration path. If any of those are left implicit, the deployment may still start, but operators will not be able to explain which plane is exposed, which node identity is durable, or how the first administrator was created.

This page is Docker-first because the repository ships a Dockerfile that builds the server plus the public and admin CLIs into one runtime image. Running raw binaries is still possible because the same values are ordinary CLI flags and environment variables, but Docker is the clearest way to keep storage, secrets, ports, and health checks visible. Read this page with [Production Model](/operators/production-model/), [Network and Ports](/operators/network-and-ports/), [Secrets and Key Management](/operators/secrets-and-key-management/), [Admin Plane](/operators/admin-plane/), [Topology Planning](/operators/topology-planning/), [Run Anvil Locally](/tutorials/setup-local-anvil/), and [Admin CLI](/reference/admin-cli/).

## Phase 1: Prepare Storage, Secrets, And Networks

Start with the storage boundary. `STORAGE_PATH` is the directory where Anvil writes durable server state: CoreStore records, object data, metadata journals, indexes, manifests, node identity, cluster keypair, system-realm records, audit evidence, and other Anvil-owned state. In the Docker image it defaults to `/var/lib/anvil`. Mount that path on a durable volume. Do not run production nodes on an ephemeral container filesystem, and do not share one mounted storage directory between multiple live Anvil nodes.

Prepare server secret material before the container starts. `JWT_SECRET` signs Anvil bearer tokens, so nodes that need to accept the same tokens must use compatible secret configuration. `ANVIL_SECRET_ENCRYPTION_KEY` is a 32-byte hex key used to encrypt stored server-side secrets and encrypted shard payloads. `ANVIL_SECRET_ENCRYPTION_KEY_ID` labels new encrypted envelopes, and `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` keeps old key ids readable during rotation. `CLUSTER_SECRET` protects node-to-node cluster traffic. These values belong in a secret manager or orchestrator secret, not in source control, shell history, image layers, or a public Compose file.

Prepare networks as separate trust surfaces. The public listener receives tenant native API traffic and S3/static gateway traffic. The admin listener is private operator traffic and must stay internal even though it still requires authentication and system-realm authorisation. The cluster listener is node-to-node traffic only. Plan which Docker network, host port, Kubernetes Service, Ingress, firewall, or load balancer is responsible for each surface before starting the first container.

Build or pull a pinned image for the release you intend to operate. A local build from this repository looks like this:

```bash
docker build -f anvil/Dockerfile -t local/anvil:operator .
```

That command produces an image containing `anvil-server`, `anvil`, and `anvil-admin`. It does not create storage, generate secrets, or configure topology. If you pull an image from your registry instead, pin it to an immutable release tag or digest and keep the matching CLI and documentation with that release.

## Phase 2: Start The First Node

The first node creates the initial mesh state. Set `INIT_CLUSTER=true` for that node only. Joining nodes should use `BOOTSTRAP_ADDRS` instead of initialising a new cluster.

The example below publishes only the public listener on host port `50051`. The admin listener stays on loopback inside the container, so operator commands use `docker exec` rather than a published host port. The cluster listener is configured, but whether you publish or route UDP `7443` depends on your node-to-node network design.

```bash
docker network create anvil-mesh
docker volume create node-eu-west-1-a

docker run -d \
  --name node-eu-west-1-a \
  --restart unless-stopped \
  --network anvil-mesh \
  -p 50051:50051 \
  -v node-eu-west-1-a:/var/lib/anvil \
  -e STORAGE_PATH=/var/lib/anvil \
  -e REGION=eu-west-1 \
  -e CELL_ID=eu-west-1-a \
  -e API_LISTEN_ADDR=0.0.0.0:50051 \
  -e PUBLIC_API_ADDR=https://storage-eu-west-1.example.com \
  -e ADMIN_LISTEN_ADDR=127.0.0.1:50052 \
  -e JWT_SECRET="$ANVIL_JWT_SECRET" \
  -e ANVIL_SECRET_ENCRYPTION_KEY_ID=2026-07-primary \
  -e ANVIL_SECRET_ENCRYPTION_KEY="$ANVIL_SECRET_ENCRYPTION_KEY" \
  -e CLUSTER_SECRET="$CLUSTER_SECRET" \
  -e CLUSTER_LISTEN_ADDR=/ip4/0.0.0.0/udp/7443/quic-v1 \
  -e PUBLIC_CLUSTER_ADDRS=/dns4/node-eu-west-1-a/udp/7443/quic-v1 \
  -e INIT_CLUSTER=true \
  -e ENABLE_MDNS=false \
  -e BOOTSTRAP_SYSTEM_ADMIN_APP_NAME=first-admin \
  -e BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH=/var/lib/anvil/bootstrap/first-admin.json \
  local/anvil:operator
```

Each environment value has a distinct job. `REGION` and `CELL_ID` describe where this process is operating. `API_LISTEN_ADDR` is the local bind address for the public gRPC and S3/static gateway multiplexer. `PUBLIC_API_ADDR` is the stable address other systems should use for this node or regional endpoint; do not set it to `0.0.0.0`. `CLUSTER_LISTEN_ADDR` is the local libp2p/QUIC bind address, and `PUBLIC_CLUSTER_ADDRS` is what other Anvil nodes should dial on the private mesh network. `ADMIN_LISTEN_ADDR` binds the private admin gRPC service. The server refuses a non-loopback admin bind unless `ALLOW_PUBLIC_ADMIN_LISTENER=true`; that flag is only an explicit opt-in for private-network deployments, not permission to expose admin on the internet. `ENABLE_MDNS=false` makes production-style discovery explicit rather than relying on local multicast.

The bootstrap variables at the end are temporary first-start inputs. They do not grant an API bypass. They tell startup what first system administrator application to create if, and only if, the system realm is absent.

## Phase 3: Understand First-Boot Bootstrap

When the system realm does not exist, Anvil initialises it during startup before accepting public or admin requests. If `BOOTSTRAP_SYSTEM_ADMIN_APP_NAME` and `BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH` are set, startup creates that first system administration application and writes a credential JSON file to the requested path. An alternative advanced path is to provide `BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_KIND` and `BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_ID`, which grants initial system authority to an existing subject instead of creating a new app credential.

After the system realm exists, the same bootstrap settings do not mint new authority. The server logs that first-boot bootstrap configuration is ignored. From that point on, admin work must authenticate normally, authorise through the built-in system realm, validate the request, and record audit evidence. This is why the admin CLI is a network client, not a direct storage writer.

Copy the generated first credential into your secret manager and restrict access to the operators or automation that perform the next provisioning step. Do not bake it into a new image, leave it in a public volume backup, or use it as an application publishing credential. For local inspection you can copy it out deliberately:

```bash
docker cp \
  node-eu-west-1-a:/var/lib/anvil/bootstrap/first-admin.json \
  ./first-admin.json
chmod 600 ./first-admin.json
```

That file contains long-lived client credential material. The CLI exchanges it for short-lived bearer tokens by calling the public authentication API, then admin requests still go to the private admin listener.

## Phase 4: Add Subsequent Nodes

A subsequent node needs its own durable storage volume, the same server secret configuration where it must accept the same tokens, encrypted records, and cluster peers, its own stable node identity path, its own cluster keypair path, and cluster addresses other nodes can reach. The defaults store node identity and cluster keypair below `STORAGE_PATH`; that is convenient, but it also means the volume must remain attached to the same logical node across restarts.

Do not set `INIT_CLUSTER=true` on joining nodes. Set `BOOTSTRAP_ADDRS` to one or more existing peers and set `PUBLIC_CLUSTER_ADDRS` to the addresses other nodes should dial for the joining node:

```bash
docker volume create node-eu-west-1-b

docker run -d \
  --name node-eu-west-1-b \
  --restart unless-stopped \
  --network anvil-mesh \
  -v node-eu-west-1-b:/var/lib/anvil \
  -e STORAGE_PATH=/var/lib/anvil \
  -e REGION=eu-west-1 \
  -e CELL_ID=eu-west-1-b \
  -e API_LISTEN_ADDR=0.0.0.0:50051 \
  -e PUBLIC_API_ADDR=https://storage-eu-west-1-b.example.internal \
  -e ADMIN_LISTEN_ADDR=127.0.0.1:50052 \
  -e JWT_SECRET="$ANVIL_JWT_SECRET" \
  -e ANVIL_SECRET_ENCRYPTION_KEY_ID=2026-07-primary \
  -e ANVIL_SECRET_ENCRYPTION_KEY="$ANVIL_SECRET_ENCRYPTION_KEY" \
  -e CLUSTER_SECRET="$CLUSTER_SECRET" \
  -e CLUSTER_LISTEN_ADDR=/ip4/0.0.0.0/udp/7443/quic-v1 \
  -e PUBLIC_CLUSTER_ADDRS=/dns4/node-eu-west-1-b/udp/7443/quic-v1 \
  -e BOOTSTRAP_ADDRS=/dns4/node-eu-west-1-a/udp/7443/quic-v1 \
  -e ENABLE_MDNS=false \
  local/anvil:operator
```

This command demonstrates the joining shape; it is not a complete production topology. After the process is running, register the region, cells, and nodes through the private admin API where your lifecycle workflow requires it. Region activation currently requires a valid activation checkpoint, and the production-friendly checkpoint generation workflow is still a documented gap. Do not work around that by editing storage files.

## Phase 5: Compose And Kubernetes Shapes

Compose is useful for a small deployment or for a shared internal service on one host. Keep the public service publication explicit, keep the admin port unpublished unless the host network is the private operator boundary, and put cluster traffic on a private Docker network. Compose files should pass secrets from a secret mechanism or environment managed outside the file, not hard-code production keys.

A Compose shape usually has one service per Anvil node, one volume per node, a private network for node-to-node traffic, and either a published public port or a reverse proxy route to `API_LISTEN_ADDR`. Run `docker compose config` before deployment so syntax and interpolation errors are caught before containers start. That proves the Compose file is valid; it does not prove Anvil can bootstrap, join the mesh, or pass authorisation checks.

Kubernetes uses the same model, but different objects. Use persistent volumes for `STORAGE_PATH`; put server secrets in Kubernetes Secrets or an external secret operator; expose the public plane through the intended Service, Ingress, or Gateway; keep the admin plane on an internal `ClusterIP` Service with NetworkPolicy; and make sure cluster UDP traffic is allowed between nodes. This repository does not currently provide a complete Helm chart, so treat Kubernetes manifests as deployment-specific operator work rather than a documented turnkey chart.

If a reverse proxy, load balancer, or ingress terminates TLS for S3/static traffic, configure `PUBLIC_REGION_BASE_DOMAIN` for host-routed requests and `TRUSTED_PROXY_SOURCE_RANGES` for the exact proxy source IPs or CIDRs Anvil sees. S3 signatures and static host routing depend on the effective host and scheme; trusting forwarded headers from every source is a security bug, while trusting none can make legitimate signed requests fail through the proxy.

## Phase 6: Health, Readiness, And Logs

The public listener exposes a readiness endpoint:

```bash
curl -fsS http://127.0.0.1:50051/ready
```

The Dockerfile healthcheck uses the same path inside the container. A successful response proves the HTTP gateway accepted a request and the cluster peer table has at least one peer, including the node itself. It does not prove the system realm was bootstrapped correctly, admin authorisation works, object writes are durable, indexes are caught up, watches are healthy, host routing is configured, or the admin listener is private.

When readiness fails, inspect logs before changing state:

```bash
docker logs --tail 100 node-eu-west-1-a
```

Startup failures are often specific: missing `JWT_SECRET`, missing `ANVIL_SECRET_ENCRYPTION_KEY`, invalid key length, missing `PUBLIC_API_ADDR`, an admin listener bound off loopback without `ALLOW_PUBLIC_ADMIN_LISTENER=true`, an unwritable `STORAGE_PATH`, or a missing bootstrap system admin configuration when the system realm is absent. Fix the cause and restart the container. Do not delete the volume unless you are intentionally discarding the deployment.

## Phase 7: First Smoke Test

A first smoke test should prove the public plane is reachable, the admin plane is private but usable by operator automation, the first admin credential can mint a bearer token, and at least one authenticated admin request succeeds. The example below runs inside the container so the admin port can remain unpublished:

```bash
docker exec \
  -e ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/first-admin.json \
  -e ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051 \
  -e ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052 \
  node-eu-west-1-a \
  anvil-admin diagnostics list --limit 10
```

This command proves the admin CLI can read the bootstrap credential file, exchange it for a token through the public authentication API, reach the private admin listener from inside the container, and make an authorised system-realm request. It does not prove the host can reach the admin API, and that is intentional. It also does not prove tenant object traffic works.

Continue the smoke test by creating a disposable storage tenant and first application through the admin plane, then hand over to public API commands for bucket and object work as described in [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/) and [Buckets and Objects](/tutorials/buckets-and-objects/). If bucket creation fails because the target region is still joining, treat that as topology evidence, not as a reason to bypass placement. Complete the supported topology workflow or document the current activation-checkpoint gap for that environment.

Before declaring the deployment ready, also prove the negative cases: the admin listener is not reachable from a tenant or public network, `_anvil/` paths are rejected through public and gateway routes, unauthenticated requests fail cleanly, and S3/static requests use the expected host and scheme through your proxy.

## Current Cautions

Docker gives Anvil a repeatable runtime, but it does not remove operator responsibility. Each live node needs its own durable state directory. Server secret keys must be present before startup and must be rotated deliberately. The admin API must stay private even when the public API, S3 gateway, or static hosting are exposed. `/ready` is a startup signal, not a full correctness proof. Region activation checkpoint generation and some drain-completion workflows are still gaps to plan around. The current server help advertises the cluster secret environment variable as `CLUSTER_SECRET`; treat snippets that use a different name for that setting as stale until they are corrected. Kubernetes deployments are deployment-specific today because the repository does not ship a complete chart.

The safest deployment is one where every state change has a plane, a principal, an audit reason, and a recovery story. If a command or orchestration step cannot explain those four things, stop and make the boundary explicit before putting tenant data on the system.
