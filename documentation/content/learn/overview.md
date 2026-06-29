---
title: Overview
description: A progressive map of Anvil's concepts and why they fit together.
---

# Overview

**Goal:** understand what Anvil is, what problems it solves, and how the rest of the guide builds toward expert use.

Anvil is a storage system for applications that need more than a place to put files. It stores objects, tracks versions, indexes metadata and content, evaluates relationship permissions, streams change events, and acts as a PersonalDB witness. Those capabilities are built into one product because modern applications usually need all of them at the same time.

A document management system is a simple example. It needs to upload files, list folders, search document text, filter by metadata, restrict reads to authorized users, update derived views when a file changes, and prove that every write was accepted in the correct order. If those pieces are split across unrelated products, the application team has to keep them consistent. Anvil's value is that one system owns the object, the object's metadata, the indexes built from it, and the authorization revision used to expose it.

## The first mental model

Start with four nouns:

| Concept | Plain-English meaning | Why it matters |
| --- | --- | --- |
| Bucket | A named area that contains objects and policy. | Teams use buckets to separate data domains such as media, logs, source artifacts, or tenant assets. |
| Object | A stored value addressed by a key. | Objects can be files, JSON records, manifests, event frames, model files, source packs, or snapshots. |
| Index | A read-optimized view of objects or metadata. | Indexes make queries fast without forcing callers to scan every object. |
| Authorization tuple | A relationship statement such as `document:123 viewer user:amy`. | Tuples let Anvil answer who can read or mutate which object without leaking data. |

Anvil treats these as one system. An object write records object bytes, metadata, directory entries, watch events, and index input. A read checks authorization before returning metadata or bytes. A search query applies authorization before returning results. A PersonalDB commit writes a durable log entry, updates row metadata, emits watches, and returns a signed commit certificate.

## What makes Anvil different

Most object stores focus on PUT, GET, and LIST. Those operations are necessary, but they are not enough for application state. Anvil adds the services application teams usually bolt on later:

- full text search for text and extracted media content;
- vector search for embeddings over text, image, audio, and video;
- hybrid ranking that blends text, vector, path, metadata, freshness, and authorization;
- Zanzibar-style relationship authorization with derived indexes;
- watch streams so derived systems stay current without rescanning;
- PersonalDB witnessing for replicated SQLite-backed application data;
- source artifact storage and indexing for git packs and related build artifacts.

The result is not a loose bundle of add-ons. The same mutation stream powers listing, search, authz-derived indexes, PersonalDB projections, source indexes, and operational watches. That shared base is why Anvil can provide predictable correctness: a result includes the object version and authorization revision it was produced from.

## How to read this documentation

Read the Learn section in order if Anvil is new to you. It teaches the concepts before asking you to operate the product. Then move to the Developer section to build against the APIs. Operators should read the Operator section after the Learn section so deployment choices make sense.

The recommended path is:

1. [Object Storage](/learn/object-storage/) - learn what an object store is and how Anvil structures data.
2. [Keys And Paths](/learn/keys-paths-and-metadata/) - learn how predictable paths become fast directory and metadata queries.
3. [Indexes And Search](/learn/indexes-and-search/) - learn full text, vector, metadata, and hybrid search from first principles.
4. [Authorization](/learn/authorization/) - learn why relationship authorization exists and how Anvil evaluates it.
5. [Watches And Derived Data](/learn/watches-and-derived-data/) - learn how Anvil avoids repeated rescans.
6. [PersonalDB](/learn/personaldb/) - learn how Anvil witnesses distributed SQLite-style application state.

When you finish that sequence, you should understand not only which commands to run, but why Anvil has the shape it has.
