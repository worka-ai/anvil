---
title: Network and Ports
description: Bind and expose Anvil ports without mixing public, admin, cluster, and gateway trust boundaries.
---

# Network and Ports

A port is a trust boundary, not just a TCP socket. The address a process binds, the address clients are told to use, the proxy that rewrites host headers, and the network allowed to reach the listener all become part of Anvil's security and routing model.

Anvil has three surfaces: the tenant-facing public plane, the private admin plane, and the node-to-node cluster plane. The current server binary serves the public gRPC API and the S3/static HTTP gateway from the public listener. It serves the admin gRPC API from a separate admin listener. It also starts a libp2p/QUIC cluster listener for mesh traffic. Those surfaces may run in one process, but operators must design them as separate trust boundaries.

Read this page with [Production Model](/operators/production-model/), [Deployment](/operators/deployment/), [Security Hardening](/operators/security-hardening/), [Gateways](/learn/gateways/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [S3-Compatible Gateway](/tutorials/s3-gateway/), [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/), [Public CLI](/reference/public-cli/), and [Admin CLI](/reference/admin-cli/).

## The Public Listener

`API_LISTEN_ADDR` is the local bind address for the public server. By default it is `0.0.0.0:50051`, which means "listen on all interfaces inside this network namespace". In a container that is often correct because publishing the port is controlled by Docker, Compose, Kubernetes Services, or a load balancer. On a bare host it is more direct: any interface that can route to the host may be able to reach the listener unless a firewall says otherwise.

The public listener is allowed to be exposed when the deployment is meant to serve tenants or public objects. It is not an unauthenticated control surface. Tenant API calls still authenticate, public policy scopes still decide which API family and resource the caller may use, relationship authorisation may still filter object or query visibility, and reserved namespaces such as `_anvil/` must remain protected through gateways.

`PUBLIC_API_ADDR` is different from `API_LISTEN_ADDR`. It is the address other systems should use to reach this node or regional endpoint. It appears in node descriptors and proxy decisions, and it should usually be the stable URL or internal service address that callers can actually dial, not the wildcard bind address. A common container mistake is to bind `API_LISTEN_ADDR=0.0.0.0:50051` and also publish `PUBLIC_API_ADDR=http://0.0.0.0:50051`; the first value is a bind instruction, while the second should be a reachable endpoint such as `https://storage-eu-west-1.example.com` or an internal service DNS name.

In Compose, bind the public listener inside the container and publish the host port only when that node is meant to receive public or tenant traffic directly. If a reverse proxy is the public edge, put Anvil on a private Compose network and let the proxy be the only published service. In Kubernetes, model the public plane as the Service or Ingress intended for tenant traffic, then keep admin and cluster Services separate.

## The Admin Listener

`ADMIN_LISTEN_ADDR` is the bind address for the private admin gRPC API. It defaults to `127.0.0.1:50052`. That default is deliberate: a local single-node deployment can run admin commands from the same host without publishing the admin plane.

In production the admin API must stay private or internal. It still requires authentication and system-realm authorisation, but network privacy is another layer of defence and another way to avoid accidental use by tenant systems. The admin API creates storage tenants, manages topology, runs system diagnostics and repair, rotates server-side secret envelopes, and reads admin audit evidence. It should not sit behind the same public route as S3 or static hosting.

Anvil has a startup guardrail for this boundary. If `ADMIN_LISTEN_ADDR` is not loopback and `ALLOW_PUBLIC_ADMIN_LISTENER` is false, startup fails with an error telling you to opt in only when the admin port is protected by private networking. Setting `ALLOW_PUBLIC_ADMIN_LISTENER=true` does not make the admin API safe for the internet; it only says the operator has intentionally bound the listener to a non-loopback address and is relying on private networking, firewalls, service mesh policy, or equivalent controls.

For Docker or Compose, do not publish `50052` to the host unless the host network itself is the private operator boundary. Prefer a private network that only operator automation can join. For Kubernetes, use a `ClusterIP`-only Service, restrict it with NetworkPolicy, and keep external load balancers or Ingress objects away from it. Admin health and diagnostics should be authenticated operator actions, not public unauthenticated URLs.

## The Cluster Plane

The cluster plane is node-to-node traffic. It is not a tenant API, not an admin API, and not a gateway. Anvil uses a libp2p/QUIC multiaddress for the local cluster listener:

```bash
CLUSTER_LISTEN_ADDR=/ip4/0.0.0.0/udp/7443/quic-v1
```

That value means the process listens for QUIC cluster traffic on UDP port `7443` inside its network namespace. `PUBLIC_CLUSTER_ADDRS` is the comma-delimited set of addresses other nodes should dial for this node. `BOOTSTRAP_ADDRS` is the comma-delimited set of existing peer addresses a joining node uses to find the mesh. A first node that initialises a mesh has a different lifecycle from a joining node, so do not copy bootstrap settings blindly between them.

Cluster addresses should describe private node reachability. They are often pod IPs, internal load-balancer addresses, private host addresses, or service DNS names, depending on the deployment model. They should not be advertised as public internet endpoints merely because the multiaddr format can express a public address. Protect cluster traffic with `CLUSTER_SECRET`, private routing, and network policy. For local development, mDNS may help discovery; for production, explicit bootstrap addresses and disabled mDNS are usually easier to reason about.

When cluster addressing is wrong, the public API may still answer simple local requests. The failure appears later: a node cannot join the mesh, wrong-region proxying has no eligible target, routing gossip is stale, or drain and repair evidence disagrees across nodes. Treat cluster reachability as part of deployment validation, not as an optional optimisation.

## Gateways And Host Routing

The current public listener multiplexes native gRPC and HTTP gateway traffic. Requests with gRPC content type are routed to the public gRPC services; other HTTP requests enter the S3/static gateway router. If your deployment exposes separate hostnames such as `s3.example.com`, `static.example.com`, or tenant custom domains, those are still public data-plane routes over Anvil's public surface. They must not route to the admin listener.

`PUBLIC_REGION_BASE_DOMAIN` enables region-aware host routing in the S3/static gateway. When it is empty, ordinary path-style S3 requests can still work, but host-routed regional names and active custom host aliases are not parsed for serving. When it is set, the gateway can use the incoming host and path to resolve a tenant, bucket, region, and object key. That route selection does not grant read access, make a bucket public, create DNS records, issue TLS certificates, or bypass object authorisation.

Reverse proxies and load balancers are especially sensitive for Anvil because S3 signatures and static host routing depend on the effective host and scheme. `TRUSTED_PROXY_SOURCE_RANGES` is a comma-delimited list of exact IPs or CIDR ranges whose forwarded metadata Anvil will trust. When a request comes from one of those ranges, Anvil may use `Forwarded`, `X-Forwarded-Host`, and `X-Forwarded-Proto` to reconstruct the external host and scheme. When the peer is not trusted, forwarded host metadata is ignored. Ambiguous forwarded host chains are rejected rather than guessed.

That behaviour prevents a client from spoofing a custom domain by sending its own `X-Forwarded-Host`. It also means a legitimate proxy must preserve the host and scheme consistently. If S3 signed requests fail only through the proxy, check the effective host, TLS termination, `X-Forwarded-Proto`, and `TRUSTED_PROXY_SOURCE_RANGES` before rotating credentials or widening permissions.

## A Typical Configuration Shape

The values below are not a complete deployment file; they show which address belongs to which trust boundary.

```bash
API_LISTEN_ADDR=0.0.0.0:50051
PUBLIC_API_ADDR=https://storage-eu-west-1.example.com

ADMIN_LISTEN_ADDR=127.0.0.1:50052
ALLOW_PUBLIC_ADMIN_LISTENER=false

CLUSTER_LISTEN_ADDR=/ip4/0.0.0.0/udp/7443/quic-v1
PUBLIC_CLUSTER_ADDRS=/ip4/10.10.0.12/udp/7443/quic-v1
BOOTSTRAP_ADDRS=/ip4/10.10.0.10/udp/7443/quic-v1

PUBLIC_REGION_BASE_DOMAIN=eu-west-1.storage.example.com
TRUSTED_PROXY_SOURCE_RANGES=10.10.0.0/24
```

The first two lines say where the public server listens and what address callers should use. The admin lines keep the private plane on loopback unless you intentionally move it to an internal address. The cluster lines describe node-to-node QUIC reachability. The gateway lines allow host-routed S3/static traffic through a known proxy range.

For a Compose deployment, this usually becomes one published public port or one reverse-proxy route, no published admin port, and a private network for cluster traffic. For Kubernetes, it usually becomes an external Service or Ingress for the public plane, an internal Service for admin, a UDP-capable cluster Service or pod-network policy for the cluster plane, and proxy CIDRs that match the real source addresses Anvil sees.

## Readiness And Health Checks

The public HTTP gateway exposes `/ready` on the public listener. The container healthcheck uses the same shape:

```bash
curl -fsS http://127.0.0.1:50051/ready
```

A successful response proves the listener accepted an HTTP request and the node's cluster peer table has at least one peer, including itself. It does not prove tenant credentials work, admin authorisation works, object writes are durable, indexes are caught up, watches are healthy, a host alias is active, or the admin listener is private. Use `/ready` as a liveness/readiness input, then use authenticated public and admin smoke tests for service correctness.

There is no reason to publish an unauthenticated admin health endpoint. If operator automation needs admin-plane validation, make an authenticated admin request from the private network, such as a narrow diagnostic or audit read appropriate for your deployment, and alert on failure.

## Validation Checks Before Exposure

Operators should prove the network model from both sides of each boundary. From a tenant or public test network, the public API or gateway endpoint should be reachable and should reject unauthorised requests cleanly. From the same network, the admin listener should be unreachable. From the operator network, the admin listener should be reachable and should still reject requests without a valid admin token and system-realm authority.

For the cluster plane, prove that nodes can dial the configured `PUBLIC_CLUSTER_ADDRS` and `BOOTSTRAP_ADDRS`, but that ordinary tenant networks cannot. In Compose this means checking published ports as well as container-to-container reachability. In Kubernetes it means checking Service type, NetworkPolicy, UDP support, and whether the source IP seen by Anvil matches your trusted proxy ranges.

For gateways, verify host routing deliberately. A path-style S3 request should route to the intended bucket. A host-routed request should work only when `PUBLIC_REGION_BASE_DOMAIN`, DNS, TLS, trusted proxy metadata, host alias state, bucket policy, and object authorisation all line up. Requests for `_anvil/` paths should be rejected through public and gateway routes. A successful static or S3 read proves the public data path, not admin reachability and not broad tenant visibility.

Common misconfigurations are usually easy to name after the fact: `PUBLIC_API_ADDR` set to a bind address, admin bound to `0.0.0.0` and published by Compose, cluster UDP exposed on the internet, forwarded host headers trusted from every source, S3 signatures broken by TLS termination, or a custom host alias created without `PUBLIC_REGION_BASE_DOMAIN` on the serving process. Write these checks into the deployment runbook before the first incident.
