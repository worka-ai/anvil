---
title: Authorisation Grants and Revokes
description: Manage tenant-owned relationship authorisation with schemas, tuples, checks, revisions, and public API scope boundaries.
---

# Authorisation Grants and Revokes

This tutorial continues from [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/) and [Buckets and Objects](/tutorials/buckets-and-objects/). It assumes you have the `acme` public CLI profile and understand the `documents/tutorial/welcome.txt` tutorial object.

The commands on this page use the public `anvil authz` CLI as a manual helper over `AuthService`. Applications should normally call the public API or Rust client directly, especially when they need explicit consistency, zookies, non-default realms, batched tuple writes, or tighter integration with their own user model. Keep [Authorisation](/learn/authorisation/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and [Public CLI](/reference/public-cli/) nearby while you read.

There is one important prerequisite gap in the tutorial chain: the previous pages did not grant `acme-owner` the public policy scopes needed to manage relationship authorisation. That is intentional; authorisation administration is powerful and should be delegated deliberately. Before these commands can run, a bootstrap operator or already-authorised tenant owner must grant exact public policy scopes such as:

| Purpose | Public policy action | Resource checked by current service |
| --- | --- | --- |
| Store the `document` namespace schema | `authz:schema_write` | `document` |
| Bind a schema to the default realm | `authz:schema_write` | `default` |
| Read the stored schema revision | `authz:schema_read` | `document_access` |
| Read the default realm binding | `authz:schema_read` | `default` |
| Write `owner` tuples for the tutorial document | `authz:tuple_write` | `document/documents/tutorial/welcome.txt#owner` |
| Write, remove, read, and check `viewer` tuples | `authz:tuple_write`, `authz:tuple_read`, `authz:check` | `document/documents/tutorial/welcome.txt#viewer` |
| Watch tuple changes in the namespace | `authz:watch` | `document` |

Those are public policy scopes. They authorise the API calls that write schemas, write tuples, and perform checks. They are not relationship tuples themselves, and they are not system-realm admin relations. Do not replace this table with wildcard grants just to make a local script easier.

The goal is to make authorisation concrete on one document before you apply it across a product. You will see how public policy scopes let a service call Anvil APIs, how relationship tuples express product access, how schema bindings give those tuples meaning, and how checks, zookies, and watches fit into a safe application flow.

## Prerequisites and scope setup

The commands in this page require public policy scopes for relationship-authorisation administration. That is intentional. A service cannot write schemas or tuples just because it can read an object, and a relationship tuple cannot mint a bearer token. Before running the examples, make sure the current `acme` profile represents a tenant app that has been delegated the exact `authz:schema_write`, `authz:schema_read`, `authz:tuple_write`, `authz:tuple_read`, `authz:check`, and `authz:watch` resources named below. If a command fails with permission denied, fix the public policy grant for that API operation; do not move the operation to `anvil-admin` and do not try to edit the system realm.

The examples use `document/documents/tutorial/welcome.txt#viewer`-style resources because the document namespace, object id, and relation are part of the public policy check. The object id deliberately includes the bucket/key pair from the object tutorial so you can see how a product document maps onto an authz object.

## Keep the two authorisation layers separate

Anvil has two authorisation layers that work together but answer different questions.

**Public policy scopes** decide whether a tenant/public API principal may call an API operation over a service resource. A token scope such as `authz:tuple_write|document/documents/tutorial/welcome.txt#viewer` says the caller may ask the public API to write viewer tuples for that exact document relation. A scope such as `object:write|documents/tutorial/welcome.txt` says the caller may ask the Object API to write that object key. Scopes are stored on apps and minted into bearer tokens.

**Relationship authorisation** decides whether a subject is related to an application object. A tuple such as `document:documents/tutorial/welcome.txt#viewer@user:user-22` says `user-22` is a viewer of that document in the tenant's authorisation model. Checks and authorisation-aware reads use those relationship facts to decide whether data may be shown.

The difference matters operationally. A service principal needs a public policy scope before it can call `WriteAuthzTuple`. The tuple it writes may then grant `user-22` viewer access in the application model. The tuple does not give `user-22` a bearer token, does not let `user-22` write more tuples, and does not let the tenant modify Anvil's built-in system realm.

The **system realm** is Anvil's private admin-plane authorisation realm. It controls operations such as tenant creation, first app provisioning, topology changes, repairs, admin audit reads, and secret-envelope rotation. Tenant/public API principals can manage tenant-owned schemas and tuples when delegated, but they cannot use `anvil authz` to rewrite the system realm or mint private admin authority.

## Learn the vocabulary on one object

An **Anvil storage tenant** is the isolation boundary created in the tenant tutorial. The `acme` tenant owns its apps, buckets, objects, indexes, and tenant-owned authorisation state. It may contain many product users inside one storage tenant.

An **authz realm** is a relationship-authorisation scope inside a storage tenant. Current public CLI tuple commands use the default realm, whose realm id is `default`. The public API has an `AuthzScope` field for realm-aware callers; the current CLI exposes realm ids mainly on schema binding commands.

A **namespace** names a kind of application object. This tutorial uses `document` for product documents. Namespace names are safe components: keep them short and stable, and do not put slashes in them.

An **object id** names one object inside a namespace. We will use `documents/tutorial/welcome.txt` so the relationship example lines up with the tutorial object key. Object ids may contain slashes; they are still authorisation identifiers, not filesystem paths.

A **relation** is a named relationship on that object, such as `owner` or `viewer`.

A **subject** is the thing being granted. It has a `subject_kind` and `subject_id`. Product users might be `user:user-22`. Anvil app principals are usually `app:<numeric app id>`. A subject can also be a `userset`, which points at another relation such as `document/documents/tutorial/welcome.txt#owner`.

A **tuple** is one relationship fact:

```text
document/documents/tutorial/welcome.txt#viewer <- user:user-22
```

A **userset** is a relation used as a subject. If `viewer` includes the userset `document/documents/tutorial/welcome.txt#owner`, then every current owner is also a viewer. The current check path resolves direct tuples and userset tuples; the examples below write that userset tuple explicitly rather than assuming a schema implication is already enforced.

A **schema** is the reviewed contract for a namespace. It records which relations your application intends to use and how you expect them to relate. A **schema binding** attaches a specific schema revision and digest to a realm. A binding has a generation so updates can be compare-and-swap checked.

A **revision** is the monotonically increasing position of the authz tuple/schema state. A **zookie** is the portable string form of that revision, currently shaped like `authz:<revision>`. API callers can ask for `latest`, `at_least`, or `exact` consistency with a zookie. The current CLI prints zookies but sends `latest` for checks and reads.

## Store a simple namespace schema

The current public CLI `schema put` command sends the schema JSON string you pass on the command line. It does not read a file path for you. Put the JSON in a shell variable first so the command sends the JSON body, not the name of a local file.

```bash
DOCUMENT_SCHEMA_JSON='{
  "schema": "acme.document.authz.v1",
  "namespace": "document",
  "relations": {
    "owner": {
      "description": "A user or service that controls the document."
    },
    "viewer": {
      "description": "A user or service allowed to read the document.",
      "includes": ["owner"]
    }
  }
}'

SCHEMA_REF="$(
  anvil --profile acme authz schema put \
    document_access \
    document \
    "$DOCUMENT_SCHEMA_JSON" \
    --reason 'define tutorial document relationships'
)"

printf '%s\n' "$SCHEMA_REF"
```

This calls `AuthService.PutAuthzSchema` for schema id `document_access` and namespace `document`. A successful response prints the schema id, schema revision, and schema digest. That proves the caller had `authz:schema_write` for the `document` namespace and that Anvil stored a versioned schema revision for the tenant.

The schema JSON is a contract and audit artefact. It does not by itself create an owner or viewer. Access changes only when tuple writes commit.

Capture the revision and digest from the CLI output so the next command can bind exactly the schema body you reviewed:

```bash
SCHEMA_REVISION="$(printf '%s\n' "$SCHEMA_REF" | awk '{print $2}')"
SCHEMA_DIGEST="$(printf '%s\n' "$SCHEMA_REF" | awk '{print $3}')"
```

If either variable is empty, stop and inspect the `schema put` output. Binding an empty or guessed digest would defeat the point of reviewing the schema before activation.

## Bind the schema to the default realm

A schema revision becomes the active contract for a realm when it is bound. This tutorial uses the default realm because current tuple commands use that realm unless an API caller supplies another `AuthzScope`.

```bash
anvil --profile acme authz schema bind \
  document_access \
  "$SCHEMA_REVISION" \
  "$SCHEMA_DIGEST" \
  default \
  --reason 'activate tutorial document schema in the default realm'
```

This calls `AuthService.BindAuthzSchema` for realm `default`. A successful response prints a `binding_generation` and a zookie. The generation is the compare-and-swap token for future binding updates. The zookie records the authz revision at which the binding write became visible.

You can inspect the current binding without changing it:

```bash
anvil --profile acme authz schema binding default
```

That command proves the caller can read the binding for the default realm. It prints the schema id, revision, and binding generation. In an update workflow, read this generation and pass `--expected-generation` to the next bind command so two schema rollout jobs cannot overwrite each other silently.

You can also inspect the stored schema revision:

```bash
anvil --profile acme authz schema get document_access --schema-revision "$SCHEMA_REVISION"
```

The current CLI prints a compact count and version rather than the full JSON body. Use the API directly if your rollout tooling needs to fetch and compare the whole schema document.

## Write owner and viewer facts

Now write actual relationship tuples for the tutorial object. These commands grant `user-17` the `owner` relation and then make the `viewer` relation include the `owner` userset.

```bash
anvil --profile acme authz tuple write \
  document \
  documents/tutorial/welcome.txt \
  owner \
  user \
  user-17 \
  add \
  --reason 'make user-17 owner of the tutorial document'

anvil --profile acme authz tuple write \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  userset \
  'document/documents/tutorial/welcome.txt#owner' \
  add \
  --reason 'owners are viewers of the tutorial document'
```

Each command calls `AuthService.WriteAuthzTuple`. The `add` operation writes or supersedes the current tuple state. A successful response prints a revision and zookie. That proves the caller had `authz:tuple_write` for the relation being changed and that the tuple log advanced.

The second command is the key userset example. It does not copy the current list of owners into viewer tuples. It says the viewer relation contains the owner userset, so a check for `viewer` can follow that userset to the current owner facts.

Grant a direct viewer as well:

```bash
anvil --profile acme authz tuple write \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user \
  user-22 \
  add \
  --reason 'share the tutorial document with user-22'
```

After this command, `user-22` is a direct viewer. `user-17` is a viewer through the owner userset. Those are different facts, and a UI may want to display them differently.

## Check the decision you need

A permission check asks one precise question: is this subject allowed for this relation on this object at the selected authz revision?

```bash
anvil --profile acme authz check \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user \
  user-17

anvil --profile acme authz check \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user \
  user-22
```

Both checks should print `allowed=true` once the tuple writes above have committed. The first proves userset resolution is working for the owner-to-viewer relationship. The second proves the direct viewer tuple is visible.

Check a subject that was not granted:

```bash
anvil --profile acme authz check \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user \
  user-99
```

This should print `allowed=false`. That result is not an error. It proves the check ran and found no direct tuple or userset path for `user-99`.

The API version of this call is `AuthService.CheckPermission`. API callers can supply `consistency = "at_least"` with a zookie returned by a previous write when they need read-your-write behaviour across processes. The current CLI check helper always asks for `latest`.

## Revoke a direct tuple

Revocation is another tuple write. Current public CLI operations are `add` and `remove`.

```bash
anvil --profile acme authz tuple write \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user \
  user-22 \
  remove \
  --reason 'remove user-22 tutorial document access'
```

This writes a remove record for the same tuple key. A successful response proves the caller can write that relation and that the tuple log advanced again. It does not remove `user-17`, because `user-17` was never granted as a direct viewer; `user-17` is still an owner and still reaches viewer through the userset tuple.

Run the checks again:

```bash
anvil --profile acme authz check \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user \
  user-22

anvil --profile acme authz check \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user \
  user-17
```

The expected result is `allowed=false` for `user-22` and `allowed=true` for `user-17`. If both are false, inspect whether the owner tuple or userset tuple was removed. If both are true, inspect whether the remove operation used the same namespace, object id, relation, subject kind, and subject id as the original grant.

## Read tuples and list direct subjects

Tuple reads are for tenant administration screens, audit review, and repair tooling. They are not a substitute for performing a permission check before serving data, because effective access may come through usersets and because reads may use different filters from the final operation.

Read the current viewer tuples for this object:

```bash
anvil --profile acme authz tuple read \
  document \
  --object-id documents/tutorial/welcome.txt \
  --relation viewer
```

This calls `AuthService.ReadAuthzTuples` with a namespace, object id, and relation filter. It should show the current viewer userset tuple, and it will show any direct viewer tuples that still exist. A successful command proves the caller has `authz:tuple_read` for the filtered relation.

List direct user subjects for the viewer relation:

```bash
anvil --profile acme authz list-subjects \
  document \
  documents/tutorial/welcome.txt \
  viewer \
  user
```

This returns direct `user` subjects currently attached to `document/documents/tutorial/welcome.txt#viewer`. After the removal above, `user-22` should no longer appear. `user-17` may not appear in this direct list because `user-17` reaches viewer through the `owner` userset. Use `authz check` to answer effective access questions.

## Watch authorisation changes

Authz writes produce an ordered tuple log. Watch streams let applications, caches, and derived indexes react without scanning every tuple repeatedly.

Open a watch in one terminal:

```bash
anvil --profile acme authz watch document --after-revision 0
```

Then write or remove a tuple in another terminal. The watch prints the revision, operation, namespace, object id, and relation for each matching tuple record. This proves the caller has `authz:watch` for the `document` namespace and that tuple changes are visible as a stream.

The command keeps running. Stop it with `Ctrl-C` when you have seen the events you need. In production, consumers should store their last processed revision and restart from that value rather than always starting at zero.

## How this relates to object reads

The `document` namespace in this tutorial is an application-level model. Your web service can call `CheckPermission(document, documents/tutorial/welcome.txt, viewer, user, user-17)` before returning the tutorial document to `user-17`.

Anvil's native object read path can also use relationship authorisation for app principals. For object data, the built-in shape is the `object` namespace, object id `bucket/key`, relation `reader`, subject kind `app`, and subject id equal to the authenticated app's numeric app id. To run the optional commands below, the caller also needs exact `authz:tuple_write` and `authz:check` public policy scopes on `object/documents/tutorial/welcome.txt#reader`. For example, if `/tmp/docs-writer-app.txt` from the tenant tutorial still exists, you can capture the app id and check the corresponding object-reader relation like this:

```bash
DOCS_WRITER_APP_ID="$(sed -n 's/^app_id=//p' /tmp/docs-writer-app.txt)"

anvil --profile acme authz tuple write \
  object \
  documents/tutorial/welcome.txt \
  reader \
  app \
  "$DOCS_WRITER_APP_ID" \
  add \
  --reason 'allow docs-writer app to read the tutorial object through relationship authz'

anvil --profile acme authz check \
  object \
  documents/tutorial/welcome.txt \
  reader \
  app \
  "$DOCS_WRITER_APP_ID"
```

That relationship is separate from the public policy scope `object:read|documents/tutorial/welcome.txt`. The object service may allow a read because the token has the public policy scope, because the relationship check succeeds, or because a public-read policy applies. Keep these reasons visible in debugging output; otherwise a working read can hide the fact that the wrong layer is granting access.

## API shape for application code

At the API level, the same flow is explicit. A tenant-authorised principal calls `PutAuthzSchema` with the storage tenant id, schema id, namespace schemas, and reason. It calls `BindAuthzSchema` with an `AuthzScope` naming the storage tenant and realm, an `AuthzSchemaRef`, and optionally an expected binding generation. It calls `WriteAuthzTuple` with namespace, object id, relation, subject kind, subject id, operation, reason, and optional scope. It calls `CheckPermission` with the same tuple coordinates plus a consistency mode and optional zookie.

The CLI uses the tenant id from the bearer token, sends tuple commands to the default realm, and uses `latest` consistency for reads and checks. That is fine for manual tutorials. Application clients should use explicit request ids, scopes, consistency, zookies, and batch APIs where correctness requires them.

There is no complete public schema grammar reference page in this documentation set yet. The reference page today covers public policy actions and resource strings, not the full authz schema JSON contract. Treat the schema JSON in this tutorial as the reviewed application contract, and verify current server/client support before depending on richer schema-rule enforcement in production.

## What you should take forward

Use public policy scopes to decide who may call Anvil APIs. Use relationship tuples to describe who is related to application objects. Use schemas and bindings as reviewed, versioned contracts for a realm. Use usersets when one relation should include another relation. Use checks for effective access decisions, not tuple listings. Carry zookies when a later read must observe a previous write. And keep tenant-owned authz work on the public API; do not use tenant credentials to modify Anvil's built-in system realm.

## Success and failure cues

Schema writes and bindings prove the tenant can define a relationship vocabulary, while tuple writes prove it can record facts in that vocabulary. A successful `check` proves the effective relation at the requested consistency, not merely that a direct tuple exists. If a check denies unexpectedly, inspect the active schema binding, tuple coordinates, subject kind, zookie mode, and caveat hash before adding more public policy scopes; API-call authority and product access are separate layers.

## Where to go next

Apply the same layer separation in [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) and [Indexes, Path Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/), where relationship visibility can affect reads and search results. For the complete resource/action table, use [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/); for design background, use [Learn: Authorisation](/learn/authorisation/).
