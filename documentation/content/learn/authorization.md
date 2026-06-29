---
title: Authorization
description: Learn authentication, authorization, token scopes, Zanzibar-style relationship tuples, caveats, and safe query exposure.
---

# Authorization

**What this page gives you:** a clear model of who a caller is, what that caller may do, and how Anvil protects objects, listings, search results, watches, and PersonalDB state.

Authentication and authorization are different.

Authentication answers: **who is calling?**

Authorization answers: **is this caller allowed to do this action on this resource right now?**

A valid identity is not enough. A user can be signed in and still not be allowed to read a private object, write a bucket, view a search snippet, or subscribe to a watch stream.

## Coarse scopes

Anvil uses token scopes for coarse permissions. A scope is a bounded capability such as "read objects in this bucket" or "administer this tenant". Scopes are good for broad API access and service credentials.

Scopes should be narrow. A backup job might need read access to a bucket but not permission to write authorization tuples. An ingestion worker might need write access to a prefix but not permission to delete objects.

Scopes answer the first question: is this caller even allowed to use this API family or resource area?

## Relationship authorization

Product permissions are usually more detailed than scopes. A user might be a viewer on one document, an editor in one project, and an administrator in one workspace. Hard-coding user lists into every object is brittle.

Anvil uses Zanzibar-style relationship authorization for fine-grained decisions. The model stores relationship facts called tuples:

```text
document:doc-42#viewer@user:amy
project:p-123#editor@group:legal
workspace:w-9#member@user:raj
```

A tuple says an object has a relationship to a subject. An authorization schema defines how relationships imply permissions. For example:

```text
permission read_document = viewer or editor or owner or parent_project.viewer
permission write_document = editor or owner
```

Now permissions are computed from relationships rather than copied into every object.

## Caveats

A caveat is a condition attached to a relationship. It can represent time, environment, purpose, or other policy context.

Examples:

```text
document:doc-42#viewer@user:amy with expires_at < 2026-12-31
project:p-123#contractor@user:lee with network = trusted
```

Caveats must be defined and hashed so the system can verify exactly which condition is being used. An invalid caveat hash is a security error, not a warning.

## Why search must be authorized

Authorization must protect every exposure path, not only direct object reads. If a private document appears in a result count, snippet, vector neighbor, metadata facet, or watch event, the system has leaked information.

Anvil applies authorization to:

- object `GET`, `HEAD`, range reads, writes, copies, and deletes;
- bucket listings and prefix queries;
- metadata filters and facets;
- full text results and snippets;
- vector and hybrid search;
- watch subscriptions and events;
- PersonalDB group opens, commits, snapshots, and projections;
- source and model artifact queries;
- administrative diagnostics.

The application should call Anvil with the caller identity and required action. It should not issue broad admin reads and filter later.

## Reserved namespaces

Anvil owns internal paths under `_anvil/`. Public callers cannot read, list, write, copy, compose, delete, or range-read them. This is true even if a caller has broad bucket permissions.

Reserved namespaces contain internal state such as index segments, authorization tuples, watch checkpoints, and PersonalDB material. Exposing them would bypass structured APIs and leak implementation details or sensitive policy state.

## Authorization result categories

A denied request can mean different things:

| Result | Meaning | Correct response |
| --- | --- | --- |
| Unauthenticated | No valid caller identity. | Refresh credentials or sign in. |
| Permission denied | Caller is known but lacks permission. | Hide the operation or request access. |
| Invalid caveat | Policy reference is invalid. | Fix policy; do not retry blindly. |
| Reserved namespace | Caller attempted internal path access. | Stop and fix the caller. |
| Index not ready | Required derived authorization state is not current. | Wait or choose a weaker consistency only if safe. |

## What you can do after this page

You should be able to explain scopes, relationship tuples, schemas, caveats, reserved namespaces, and why search and watches must be authorization-aware. Next, learn how watches keep indexes and derived state current.
