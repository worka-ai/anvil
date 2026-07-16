---
title: Mesh Routing and Lifecycle
description: Operate Anvil mesh routing, cross-region policy, and lifecycle records without treating the admin plane as a data path.
---

# Mesh Routing and Lifecycle

This tutorial continues from [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/), [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/), [Buckets and Objects](/tutorials/buckets-and-objects/), [S3-Compatible Gateway](/tutorials/s3-gateway/), and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/). It assumes the local `anvil-local` container is still running, the public API is reachable on `127.0.0.1:50051`, and the admin API is still private inside the container on `127.0.0.1:50052`.

Mesh operations are operator operations. A tenant application uses the public API to create buckets, write objects, query indexes, manage tenant-owned host aliases, and read its own data. A mesh operator uses the private admin API to describe topology, inspect routing records, repair derived routing projections, and move regions, cells, and nodes through lifecycle states. The `anvil-admin` CLI is a helper over that private admin API; it does not open the storage directory and it does not bypass system-realm authorisation.

Keep [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Gateways](/learn/gateways/), [Admin CLI](/reference/admin-cli/), [Public CLI](/reference/public-cli/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and [Network and Ports](/operators/network-and-ports/) nearby while reading this page.

Use this page as the operational companion to the topology tutorial. It explains how placement, route records, lifecycle states, host routing, diagnostics, and audit evidence fit together before you change them, so an operator can tell the difference between a data-plane problem and an unsafe mesh-control-plane change.

## Prerequisites and operating posture

This page is operator material. Use it with a system-admin or named operator token and a private admin endpoint; do not run these lifecycle commands from tenant application credentials. Before changing any route or lifecycle state, capture the current descriptor generation and an audit reason that would make sense to another operator during review. A command that changes routing without a reason, generation check, or before/after evidence should be treated as an unsafe runbook step.

For local examples, continue using `docker exec -e ANVIL_AUTH_TOKEN=... anvil-local anvil-admin ...` so the admin API stays private. For production, replace `docker exec` with your management-network path, but keep the same boundary: public traffic may reach Anvil's public plane; topology mutation should not.

## Understand the moving parts before changing them

A **mesh** is one Anvil routing and lifecycle universe. It has a stable `MESH_ID`, a set of regions, cells, nodes, routing records, host-alias records, and control streams that should agree about where data lives. A single local container still belongs to a mesh so the same operator vocabulary works in production.

A **region** is a placement and routing boundary. A bucket is created with a region, and that region becomes the bucket's home region. A request that arrives elsewhere can be redirected, proxied, or rejected according to the server's `CROSS_REGION_ROUTING_POLICY` and the available remote nodes.

A **cell** is a smaller failure or capacity boundary inside a region. It lets operators register and drain rack-like or zone-like groups without pretending every node in a region is interchangeable.

A **node** is an Anvil process with advertised capabilities such as `object`, `index`, `personaldb`, `gateway`, and `admin`. Routing and repair should not guess what a process can do; the node descriptor is the durable statement of intent.

A **routing record** is materialised routing state. The current families are `tenant-name`, `tenant-locator`, `bucket-locator`, and `host-alias`. Tenant and bucket creation write source records and projections so later requests can answer questions such as "which tenant is this?", "where is this bucket's home region?", and "which bucket does this hostname map to?". These projections are derived state, so they can be diagnosed and repaired from control-stream history.

A **lifecycle state** controls whether topology is eligible for work. Current region, cell, and node descriptors use states such as `joining`, `active`, `read_only`, `draining`, `drained`, `drained_with_exceptions`, `offline`, and `removed`. Not every transition is allowed. Update commands require `--expected-generation` so two operators cannot silently overwrite each other's lifecycle changes.

## Keep the admin plane private

The local setup deliberately does not publish port `50052` to the host. Continue to run admin examples with `docker exec` and pass a short-lived bearer token into the container:

```bash
export ANVIL_AUTH_TOKEN="$(anvil auth get-token)"

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region list --limit 20
```

A successful response proves the token is valid, the private admin listener is reachable from inside the container, and the system realm authorises the principal to list region descriptors. It does not prove the host can reach the admin API, and that is intentional. In production, use a private management network, a bastion, or an operator-only control path; do not expose `ADMIN_LISTEN_ADDR` merely because tenants need the public API, S3, or static hosting.

The admin CLI prints JSON. For lifecycle objects, watch the `state` and `generation` fields. The state tells you what Anvil believes is eligible for placement or routing. The generation is the optimistic-concurrency value you must pass to later update commands.

## Inspect the current topology

Before repairing or draining anything, inspect the descriptors Anvil already has. These commands are read-only admin API calls.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region list --limit 20

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin cell list --region local --limit 20

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node list --region local --cell-id local-cell-1 --limit 20
```

The region list proves the mesh has durable region descriptors. The cell list proves the `local` region has cell records. The node list proves node descriptors exist for the requested region and cell, and it shows each node's advertised capabilities. These commands do not prove the public gateway can serve traffic, that host routing is configured, or that any bucket is placed in the region.

If a list is empty, do not create new descriptors blindly. Check whether you are using the right storage directory, mesh id, admin credential, and region id. A production operator should also check recent admin audit events before assuming the topology was never created.

## Understand placement and bucket home regions

Placement happens when tenant and bucket control records are created. Tenant creation accepts a home-region value at the admin API boundary, and bucket creation stores the bucket's region in the bucket record and in the `bucket-locator` routing family. Object reads and writes then compare the requested bucket with the serving node's configured `REGION` and the bucket locator.

For buckets, the tenant-facing public command from the earlier tutorial is the normal data-plane entry point:

```bash
anvil --profile acme bucket create documents local
```

That public command proves the tenant principal can create a bucket in the selected region only if placement is currently writable. It does not make the caller a mesh operator. It also does not display the routing projection. Operators inspect the resulting home region through admin routing records.

There are two current caveats. First, `create_bucket` checks that the target region, this node's `CELL_ID`, and this node's stable node id are active when topology records exist. In the local tutorial chain the region activation checkpoint workflow is not fully exposed, so bucket creation can still fail with a placement precondition even though the region, cell, and node descriptors exist. Secondly, current tenant creation records the requested home region in the admin response and audit details, but the mesh tenant locator is written from the serving node's configured `REGION`. Until that mismatch is resolved, verify tenant locator records before relying on remote tenant home-region placement.

## Inspect routing records

Routing records are what the data plane consults when it needs to find tenants, buckets, and host aliases. List them by family when investigating misrouting, unexpected redirects, host alias behaviour, or region drain effects.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin routing list --family tenant-locator --limit 100

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin routing list --family bucket-locator --limit 100

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin routing list --family host-alias --limit 100
```

A tenant-locator record says which region owns the tenant locator for a tenant id. A bucket-locator record says which region and cell own a tenant bucket, plus the locator status. A host-alias record maps a hostname to a tenant, bucket, region, and key prefix when the alias is active.

A successful list proves the caller has system-realm routing authority and that the materialised projection can be read. It does not prove the projection is correct. Routing records are derived from control streams; a stale or missing projection can exist after bugs, partial repair, or interrupted maintenance.

Use the `record_key` from the list output when you need to repair one projection. For a bucket locator the key shape is `<tenant-id>/<bucket-name>`; for a tenant locator it is `<tenant-id>`; for a tenant-name record it is the tenant name; for a host alias it is the hostname.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin routing repair \
    --family bucket-locator \
    --record-key '<tenant-id>/documents' \
    --expected-generation '<generation-from-routing-list>' \
    --audit-reason 'repair bucket locator projection from control stream'
```

This command repairs one materialised routing record from the latest matching control-stream payload. It proves the caller can mutate routing projections and that Anvil found source control-stream history for that record key. It does not move the bucket, create a missing tenant, activate a region, or repair every record in the family. The current admin mutation context requires a non-zero `--expected-generation` for this update-style command; use the generation you just observed as operator evidence, but note that the repair service currently rebuilds from the control stream rather than treating that value as a routing-record CAS check. If the record key is wrong or the control stream has no source payload, the command fails and you should inspect diagnostics rather than guessing a new key.

## Use diagnostics before repair

Diagnostics are the safest operational starting point because they describe what Anvil believes is inconsistent without mutating state.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin diagnostics list --source mesh --limit 50

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin diagnostics list --source mesh_routing_projection --limit 50
```

The first command combines mesh lifecycle and mesh routing projection diagnostics. The second narrows to routing projection checks. A successful response proves the caller can view admin diagnostics; it does not prove every routing record is healthy unless the diagnostics page is complete and you have reviewed all pages.

Typical lifecycle diagnostics include regions, cells, or nodes that are not active. That may be expected during a planned drain, or it may explain why placement is failing. Routing projection diagnostics can propose a record-level repair when a materialised descriptor differs from control-stream history. Prefer repairing the exact record after reading the diagnostic evidence.

There is also a broad admin repair backend for mesh routing projections. The current generic `anvil-admin repair run` CLI still carries fields such as `--tenant-id` even when the mesh-routing repair kind ignores them, so this tutorial uses the clearer `routing repair --record-key ...` shape for manual work. Treat the generic repair surface as coarse operator tooling until the command shape is specialised.

## Understand cross-region routing policy

A request can arrive at a node whose configured `REGION` is not the bucket's home region. The current S3/static gateway and object manager recognise that through bucket records and bucket-locator records. The server-level `CROSS_REGION_ROUTING_POLICY` controls the broad response:

| Policy value | Current meaning |
| --- | --- |
| `redirect_preferred` | Return a region redirect when the bucket is remote. This is the default. |
| `proxy_preferred` | Proxy object operations when an eligible remote object node is known; otherwise redirect. |
| `proxy_required` | Proxy object operations when possible; otherwise return a proxy-unavailable error. |
| `local_only` | Reject remote-bucket routing instead of redirecting or proxying. |

This is runtime configuration, not a tenant grant. Set it when starting the server, for example:

```text
CROSS_REGION_ROUTING_POLICY=redirect_preferred
```

A redirect-style S3 response includes `x-amz-bucket-region` so S3 clients can retry against the right regional endpoint. `local_only` returns a structured error instead. Proxy policies depend on active node descriptors in the remote region: the current selector looks for an active node with the `object` capability and a non-empty `public_api_addr`.

Current proxying is partial. The S3 gateway has an internal proxy path for object-shaped operations such as `GET`, `HEAD`, `PUT`, and `DELETE`; it forwards the original principal and authorisation context and the destination still enforces object validation, reserved namespace checks, and object authorisation. Bucket-level and management-style operations are not a universal cross-region proxy. The native gRPC object path currently reports the remote bucket region with a gRPC status and metadata; it does not transparently proxy native public API object calls.

Do not grant `internal:proxy_object` to tenant applications. Internal proxy tokens are node-issued, tenant id `0`, and checked by the internal proxy service. Public callers keep their ordinary public policy scopes and relationship authorisation; proxying must not widen them.

## Understand host routing

Host routing is how the HTTP gateway turns a host and path into a tenant, bucket, region, and object key. The current object-route parser supports three shapes when `PUBLIC_REGION_BASE_DOMAIN` is configured:

```text
https://local.anvil.example/acme/documents/site/index.html
  -> path-style regional route

https://documents.acme.local.anvil.example/site/index.html
  -> virtual-host regional route

https://docs.example.test/index.html
  -> active host alias route
```

The region descriptor's `virtual_host_suffix` is durable topology metadata. The server's `PUBLIC_REGION_BASE_DOMAIN` is runtime configuration used by the S3/static HTTP gateway to parse native regional hosts. If Anvil sits behind a reverse proxy or load balancer, configure `TRUSTED_PROXY_SOURCE_RANGES` so forwarded host metadata is accepted only from known proxies. Ambiguous forwarded host chains are rejected.

Tenant-owned host aliases should normally be created, verified, listed, and deleted through the public API or `anvil host-alias ...` commands described in [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/). The admin host-alias commands are for operator lifecycle, repair, migration, suspension, and investigation. Operators can inspect the admin view without changing tenant content:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin host-alias list --region local --limit 50
```

A successful list proves the operator can see host-alias descriptors for the region. It does not create DNS, issue certificates, make a private bucket public, or prove the current process has `PUBLIC_REGION_BASE_DOMAIN` configured for serving host-routed requests.

## Activate a region only with a real checkpoint

Cell and node activation are straightforward generation-checked lifecycle updates. Region activation is deliberately stricter because a region must not become active while ignoring existing routing and lifecycle control-stream history. The admin CLI has a real `region activate` command, but it requires an activation checkpoint JSON file:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region activate \
    --region local \
    --activation-checkpoint /var/lib/anvil/checkpoints/local-region-activation.json \
    --expected-generation '<current-region-generation>' \
    --audit-reason 'activate local region after checkpoint verification'
```

The command reads the file and sends it to `AdminService.ActivateRegion`. A successful activation proves the checkpoint schema matched `anvil.mesh.activation_checkpoint.v1`, the checkpoint mesh id and region matched the descriptor, every existing routing and lifecycle control-stream partition was included, regional control checkpoints had reached the required sequences and digests, at least one active cell existed in the region, and at least one active node existed in an active cell.

The current documentation and CLI do not expose a production-friendly checkpoint generation workflow. Tests construct checkpoint files by reading existing routing and lifecycle control streams and writing regional control checkpoint records, but there is no documented `anvil-admin` command that does this for operators. Do not hand-write a fake checkpoint just to make activation pass; that would remove the safety property the checkpoint is meant to provide. Treat this as a current implementation/documentation gap.

## Put a region into read-only or draining deliberately

A read-only region state is an operator lifecycle signal. In the current implementation it prevents new writable placement, such as creating new buckets in that region through placement checks. Do not assume it fences every existing object write path until your deployment has verified that behaviour for the API surfaces you expose.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region set-read-only \
    --region local \
    --expected-generation '<current-region-generation>' \
    --audit-reason 'pause new writable placement in local region'
```

This command proves the region descriptor was active, the generation matched, and Anvil moved it to `read_only`. It does not migrate buckets, repair routing records, stop node processes, or close existing client sessions.

A region drain is more disruptive. It first moves the region descriptor to `draining`, then applies a drain plan to bucket locators in that region. The current CLI accepts a default disposition and optional per-bucket overrides in the shape `TENANT_ID:BUCKET_NAME:DISPOSITION:REASON`.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region drain \
    --region local \
    --default-disposition read-only-until-removed \
    --bucket-override '<tenant-id>:documents:remain-proxy-only:customer approved delayed migration' \
    --expected-generation '<current-region-generation>' \
    --audit-reason 'drain local region for maintenance rehearsal'
```

This command proves the operator can move the region to `draining`, Anvil found bucket locators in the region, and the drain plan was applied to those locators. `block-until-empty` leaves active locators in place and prevents full drain completion while buckets still name the region as primary. `remain-proxy-only` and `read-only-until-removed` write bucket drain exceptions and mark locators read-only. `delete-after-retention` marks locators draining.

Current lifecycle gaps matter here. The admin API and CLI expose starting a region drain, but they do not expose a clear operator command to complete the region into `drained` or `drained_with_exceptions`. The `region remove` command exists, but the lifecycle state machine only allows removal from drained states, so a normal `draining` region cannot simply be removed with the current public command surface. Plan production drains with that limitation in mind.

## Drain nodes before maintenance

Node drain is the safer first maintenance operation because it affects one process rather than an entire region. It records that the node should stop receiving new ownership and includes a graceful timeout policy.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node drain \
    --node-id local-node-1 \
    --graceful-timeout-ms 30000 \
    --force-after-timeout \
    --expected-generation '<current-node-generation>' \
    --audit-reason 'drain local-node-1 before maintenance'
```

A successful command proves the node was active, the generation matched, and Anvil stored a drain descriptor with the timeout and force-after-timeout flag. It does not stop the operating-system process, terminate client connections, prove background work has moved, or complete the node drain. Inspect diagnostics for runtime ownership blockers before taking the host down.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin diagnostics list --source mesh_lifecycle --limit 50
```

Current node lifecycle also has a surface gap: the state machine supports `draining -> drained`, but the admin CLI does not expose a distinct complete-drain command. The available emergency path is `node force-offline`, which expires runtime ownership and moves an active or draining node to `offline`, followed by `node remove` if the offline node should leave the topology. That is not the same as a graceful drain completion.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node force-offline \
    --node-id local-node-1 \
    --expected-generation '<current-node-generation>' \
    --audit-reason 'force local-node-1 offline after failed maintenance drain'
```

Use force-offline as an incident or controlled failover action, not as routine shutdown hygiene. It proves the operator chose to expire runtime ownership for the node; it does not guarantee every external system has stopped sending traffic to the node.

## Verify audit evidence after changes

Every mutating admin command requires `--audit-reason` and records admin audit evidence. After topology or routing work, list recent audit events filtered by action or resource.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --action admin.region.drain --limit 20

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --action admin.routing_record.repair --limit 20
```

A successful audit read proves the operator principal can view the admin audit stream and that matching events are queryable. It does not prove the maintenance was safe; it gives you attribution, request ids, audit reasons, and details to compare with your change ticket and diagnostics.

## What to take forward

Use the public API for tenant data and the private admin API for mesh lifecycle. Inspect topology before changing it. Treat routing records as repairable projections over control streams, not as a second source of truth. Treat bucket home region as a placement fact that data-plane clients must respect. Prefer regional endpoints when clients know the bucket's region; let `CROSS_REGION_ROUTING_POLICY` decide whether a wrong-region S3 request redirects, proxies, or fails. Keep host routing separate from authorisation and public-read policy. Do not hand-write activation checkpoints or rely on drain completion paths that the current CLI/API does not expose.

## Success and failure cues

A safe lifecycle change has three pieces of evidence: the descriptor generation matched, the lifecycle state changed to the intended value, and the admin audit stream records who changed it and why. Routing problems should be debugged by reading route records and diagnostics before mutating topology. If you cannot explain whether a failure is region lifecycle, host routing, bucket placement, or gateway configuration, pause before running repair or drain commands.

## Where to go next

For local setup, this page points back to [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/). For production, continue through [Topology Planning](/operators/topology-planning/), [Gateway Operations](/operators/gateway-operations/), and [Incident Response](/operators/incident-response/) before automating region activation, route changes, draining, or host-alias lifecycle.
