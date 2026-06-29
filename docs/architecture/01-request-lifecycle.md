---
title: Request Lifecycle
description: How object requests flow through Anvil.
---

# Request Lifecycle

## Object Write

1. A client sends an authenticated write request to an Anvil node.
2. The node validates the token, bucket policy, reserved namespace rules, and request shape.
3. The node writes object bytes into Anvil-owned storage and records the resulting content hash, shard map, object metadata, version id, mutation id, and authorisation revision in the native metadata journal.
4. The node publishes object and index events so watchers and index workers can update derived indexes.
5. The request completes only after the object bytes and metadata mutation are durable in the node's storage path.

## Object Read

1. A client sends an authenticated read request to an Anvil node.
2. The node validates access and resolves the requested bucket/key/version from the native metadata journal and current indexes.
3. The node rejects reads against reserved internal namespaces before exposing metadata or bytes.
4. The node streams object bytes from Anvil-owned storage.
