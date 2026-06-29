---
title: Introduction
description: A high-level overview of Anvil.
---

# Introduction

Anvil is a distributed object store with native metadata, authorisation, indexing, and object event support built into the Anvil process. It is designed to keep the operational surface small: deploy Anvil nodes, give each node durable storage, and let Anvil manage its own object and metadata state.

Core principles:

1. **Native persistence:** object bytes, metadata journals, indexes, manifests, and control-plane records live under Anvil-managed storage paths.
2. **Distributed by default:** nodes exchange cluster metadata and object placement information without relying on an external metadata service.
3. **Predictable object paths:** callers can use stable bucket/key naming schemes while Anvil maintains the indexes needed for listing, search, authorisation, and retrieval.
4. **Fail-closed authorisation:** reserved internal namespaces are not accessible through public APIs, and user access is evaluated before object metadata or bytes are exposed.
