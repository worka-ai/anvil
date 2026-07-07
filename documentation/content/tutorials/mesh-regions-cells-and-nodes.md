---
title: Mesh Regions, Cells, and Nodes
description: Register the local topology descriptors Anvil uses for placement, routing, and lifecycle operations.
---

# Mesh Regions, Cells, and Nodes

This tutorial continues from [Run Anvil Locally](/tutorials/setup-local-anvil/) and [Bootstrap Administration](/tutorials/admin-bootstrap/). It assumes the `anvil-local` container is running and your shell has `ANVIL_AUTH_TOKEN` set to a short-lived bearer token for the bootstrap-created system administrator.

Mesh topology is system administration. The public API cannot create regions, cells, or nodes. The control path is the private admin API on port `50052`; the `anvil-admin` CLI is only a manual helper over that API. In this Docker setup the admin port is not published to the host, so the commands run inside the container with `docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local ...`.

For the conceptual model, read [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/) and [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/). For production planning, read [Topology Planning](/operators/topology-planning/) and [Network and Ports](/operators/network-and-ports/). The command reference is [Admin CLI](/reference/admin-cli/).

## Understand the topology model

A **mesh** is the whole cooperating Anvil deployment and routing universe. It is the boundary inside which regions, cells, nodes, routing records, lifecycle state, and placement decisions are meant to agree. The local tutorial has only one container, but it still belongs to a mesh so the same control-plane language works later for multi-region deployments.

A **region** is a placement, failure, and latency boundary. In production it is usually a cloud region, data centre, or other location where you can reason about user latency, regulatory placement, outage blast radius, and routing. When a bucket or route chooses a region, Anvil needs a region descriptor that says where that region is served and how it should participate in placement.

A **cell** is a smaller failure domain inside a region. In many deployments it maps to a rack, availability-zone slice, storage pool, or other unit you can drain independently. Cells let operators express, "these nodes fail or drain together", rather than treating every server in a region as identical.

A **node** is one Anvil server process participating in storage, routing, serving, indexing, PersonalDB work, gateway handling, or administrative responsibilities. A node descriptor records where the process belongs and which capabilities it should advertise. That is operational intent: routing and background work should not guess whether a node can serve object reads, index maintenance, or admin work.

You configure this topology so Anvil can make placement and routing decisions, isolate failures, drain parts of the deployment deliberately, and leave an audit trail for operator intent. Even in a one-container tutorial, registering the descriptors teaches the same shape as production.

## Register the local region

The local region is a control-plane descriptor for this tutorial's placement boundary. The `public-base-url` is the public API address clients use for this local deployment. The `virtual-host-suffix` is the suffix Anvil can use when it builds or validates virtual-host-style routes. The `default-cell` records the cell we intend to register next.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region create \
    --region local \
    --public-base-url http://127.0.0.1:50051 \
    --virtual-host-suffix local.anvil.test \
    --placement-weight 100 \
    --default-cell local-cell-1 \
    --audit-reason 'register local tutorial region'
```

After this command, Anvil has a `local` region descriptor in the mesh lifecycle state. New descriptors start in a joining lifecycle state; activation is a separate lifecycle step because production region activation must prove that routing control streams have reached a safe checkpoint.

## Register and activate the local cell

A region needs at least one cell before it can be useful for placement. This cell represents the local machine's tiny failure domain. In production, choose a cell id that maps to a real operational unit such as a rack, zone slice, or storage pool.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin cell register \
    --region local \
    --cell-id local-cell-1 \
    --placement-weight 100 \
    --audit-reason 'register local tutorial cell'
```

The cell now exists, but it is still joining. Activating it records that the cell is allowed to receive work. Update commands require `--expected-generation` so two operators or controllers do not accidentally overwrite each other's lifecycle changes; a newly registered cell has generation `1`.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin cell activate \
    --region local \
    --cell-id local-cell-1 \
    --expected-generation 1 \
    --audit-reason 'activate local tutorial cell'
```

After activation, the cell is ready for node placement. If this command reports a generation mismatch, list the cell and use the current generation from the response rather than guessing.

## Register and activate the local node

The node descriptor represents the running `anvil-local` server process. Its public API address is the address clients use from the host. Its cluster address is the private node-to-node address other Anvil nodes would dial in a real mesh. This single-node tutorial does not publish the cluster port to the host, but the descriptor still shows where that value belongs.

Capabilities are part of the contract. They tell Anvil and operators what this process is meant to do. For the local all-in-one node, it is reasonable to advertise object storage, indexing, PersonalDB work, gateway serving, and admin capability. In production, keep capabilities accurate so routing, draining, and repair decisions match reality.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node register \
    --node-id local-node-1 \
    --region local \
    --cell-id local-cell-1 \
    --libp2p-peer-id local-dev-peer \
    --public-api-addr http://127.0.0.1:50051 \
    --public-cluster-addr /ip4/127.0.0.1/udp/7443/quic-v1 \
    --capability object,index,personaldb,gateway,admin \
    --audit-reason 'register local tutorial node'
```

The node now exists in the joining state. Activating it records that the node may serve the responsibilities described by its capabilities. A newly registered node has generation `1`, so the activation command uses that expected generation.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node activate \
    --node-id local-node-1 \
    --expected-generation 1 \
    --audit-reason 'activate local tutorial node'
```

After this command, the mesh has a local region descriptor, an active local cell, and an active local node. That is enough to inspect topology and understand lifecycle state, even though the region itself remains in the joining state until the activation checkpoint workflow is available for this tutorial flow.

## Inspect the registered topology

Listing the descriptors is the safest way to confirm what Anvil recorded. These reads still use the admin API, because mesh topology is control-plane state rather than tenant data.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region list

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin cell list --region local

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node list --region local --cell-id local-cell-1
```

The output should show the `local` region, `local-cell-1`, and `local-node-1`. Pay attention to lifecycle state and generation values. State tells you whether a descriptor is joining, active, draining, drained, offline, or removed. Generation values are the optimistic-concurrency guard you pass to later lifecycle updates.

## About region activation in the current CLI

The current admin CLI does have a `region activate` command, but it requires an activation checkpoint file. That checkpoint is not a decorative flag: it proves the region has reached the required mesh control-stream positions before placement and routing treat it as active.

This page does not invent a local checkpoint generator or tell you to hand-write one. In the current documented command surface, the safe tutorial path is to register the region, activate the cell and node, and inspect the resulting topology. A production-ready region activation walkthrough needs the checkpoint creation and verification path documented alongside the admin API command.

When that path is available, it should remain an admin-plane operation. Do not perform mesh lifecycle changes through the public API, and do not work around activation checks with data-plane grants.
