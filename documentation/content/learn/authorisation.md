---
title: Authorisation
description: Learn authentication, tenant bootstrap, app credentials, token scopes, relationship tuples, namespaces, usersets, caveats, delegation, and fail-closed protections.
---

# Authorisation

**What this page gives you:** a complete mental model for who may call Anvil, how tenants receive credentials, what a tenant can delegate, how object-level relationships work, and why Anvil denies access whenever a decision cannot be proven safely.

Authorisation is easier to reason about when each layer has one job:

1. **Bootstrap authority** creates tenants, applications, initial secrets, and initial policy grants.
2. **Authentication** proves the caller identity.
3. **Token scopes** decide whether that identity may use a broad API family or resource envelope.
4. **Relationship authorisation** decides whether the identity is related to a particular object in the required way.
5. **Caveats** add required conditions such as expiry, tenant context, device posture, or purpose.
6. **Reserved namespace guards** block public access to Anvil-owned internal paths before normal object logic runs.

A valid token is not enough on its own. The caller can still be denied for a private object, search result, watch stream, tuple write, source artefact, PersonalDB commit, or reserved namespace probe.

## The actors

Anvil separates deployment control, tenant applications, and application-level subjects.

| Actor | Created by | Can authenticate directly to Anvil? | Main purpose |
| --- | --- | --- | --- |
| Deployment operator | Outside Anvil | Uses privileged bootstrap/admin tooling | Creates tenants, regions, application identities, initial secrets, and initial policy grants. |
| Tenant | Deployment operator or an approved control-plane service | No. A tenant is a boundary, not a credential. | Owns buckets, objects, indexes, PersonalDB groups, source artefacts, policies, and relationship facts. |
| Tenant application | Bootstrap/admin path | Yes, using `client_id` and `client_secret` exchanged for a bearer token. | Calls the native API or S3-compatible API for one tenant. |
| Service job | Bootstrap/admin path | Yes, usually as a tenant application with narrow scopes. | Performs ingestion, indexing, repair, projection, or other automated work. |
| End user subject | External identity system or tenant application | Not as a built-in Anvil user credential in the current public API. | Appears in relationship tuples, for example `user:amy`. |
| Admin user | Bootstrap/admin path | Used by operator tooling, not ordinary data-plane applications. | Operates the deployment and performs privileged setup. |

The important distinction is that a tuple subject such as `user:amy` is not automatically an Anvil login account. It is an identity label that a tenant application maps from its own identity system and then uses in relationship checks.

## Who can create tenants?

Tenants are created by the deployment bootstrap/admin path, not by ordinary tenant applications. Tenant creation establishes an isolation boundary, so it must be controlled by the deployment operator or by a separate control-plane service that already holds equivalent authority.

In the current implementation, tenant creation is an admin operation. Creating a tenant does not automatically create application credentials for that tenant. Bootstrap must also create at least one tenant application and grant that application the scopes it needs.

A normal tenant token cannot create another tenant. It cannot use object writes, tuple writes, or policy grants to escape into a new administrative boundary.

## What can a tenant do once created?

A tenant can act only through application credentials and tokens issued for applications belonging to that tenant. What it can do depends on the scopes granted to those applications.

Typical tenant capabilities include:

- create and manage buckets when an application has bucket scopes;
- write, read, list, and delete objects when an application has object scopes;
- create and query indexes when an application has index scopes;
- create PersonalDB groups, submit commits, read snapshots, and watch projections when an application has PersonalDB scopes;
- write relationship tuples when an application has `authz:tuple_write` for the target namespace/object/relation envelope;
- check relationship permissions when an application has `authz:check`;
- watch tuple, namespace, derived, bucket, object, index, source, and PersonalDB changes when an application has the matching watch scope;
- grant or revoke scoped access to other tenant applications only when the caller has `policy:grant` or `policy:revoke` for the target resource.

A tenant cannot do these things through ordinary public APIs:

- create another tenant;
- mint new application credentials by itself;
- grant itself authority it does not already have;
- define or replace namespace schemas and caveat definitions;
- read or write `_anvil/` internal paths;
- bypass relationship checks by reading search, watch, projection, repair, or diagnostic paths.

## How does a tenant get credentials?

Credentials are created by bootstrap/admin tooling.

The lifecycle is:

```text
operator/admin creates tenant
  -> operator/admin creates tenant application
  -> Anvil returns client_id and client_secret once
  -> operator/admin grants policy scopes to that application
  -> application calls GetAccessToken(client_id, client_secret, requested scopes)
  -> Anvil verifies the secret and intersects requested scopes with assigned policy
  -> Anvil returns a short-lived bearer token containing tenant id, subject id, and approved scopes
```

The returned token is the credential used for normal API calls. Requesting `*` or an empty scope list does not create new authority; it asks Anvil to issue the scopes already assigned to that application. If no assigned policy matches, token issuance fails.

Secrets should be stored by the application operator as deployment secrets. Anvil does not expect browser clients or untrusted devices to hold tenant application secrets directly.

## Can a tenant create its own users?

Not as Anvil login accounts in the current public API.

A tenant application may model users as relationship subjects:

```text
user:amy
group:engineering
service:ingest-worker
```

It may then write relationship tuples involving those subjects if its token allows tuple writes. For example:

```text
group:engineering#member@user:amy
document:doc-42#viewer@userset:group/engineering#member
```

That does not create a password, WebAuthn credential, OAuth identity, or Anvil account for Amy. The tenant application remains responsible for authenticating Amy in its own identity layer, mapping her to the stable subject id `user:amy`, and calling Anvil with the appropriate token and relationship checks.

If a deployment wants Anvil-backed end-user login, that is a control-plane feature above the current public data-plane API. It must still preserve the same rule: users may receive delegated access, but they must not be able to mint tenant, application, namespace-schema, or reserved-path authority for themselves.

## What can a tenant delegate?

A tenant can delegate only authority it already holds, and only through the appropriate API.

| Delegation type | API | Example | Required caller authority |
| --- | --- | --- | --- |
| Application scope grant | `GrantAccess` | Grant app `reader-api` `object:read` on `bucket:documents/*`. | `policy:grant` on the target resource. |
| Application scope revoke | `RevokeAccess` | Remove app `reader-api` `object:read` on `bucket:documents/*`. | `policy:revoke` on the target resource. |
| Public bucket read toggle | `SetPublicAccess` | Allow anonymous read for `public-assets`. | `policy:grant` on `bucket:public-assets`. |
| Relationship fact write | `WriteAuthzTuple` | Add `document:doc-42#viewer@user:amy`. | `authz:tuple_write` on that tuple resource envelope. |
| Relationship fact removal | `WriteAuthzTuple` with `operation = remove` | Remove `document:doc-42#viewer@user:amy`. | `authz:tuple_write` on that tuple resource envelope. |

Delegation is bounded. An application that can grant object read on one prefix cannot grant object write on another prefix unless its own token scopes permit that policy grant. A tuple writer for `document:*#viewer` cannot define a new `document` schema, register caveat code, or write unrelated tuple namespaces.

## Authentication versus authorisation

Authentication answers this question:

> Who is making the request?

Anvil authenticates applications and services with credentials and bearer tokens. A successful authentication step attaches a tenant id, subject id, and approved scopes to the request.

Authorisation answers a different question:

> May this identity perform this action on this resource now?

The action matters. `object:read`, `object:write`, `index:read`, `authz:tuple_write`, `authz:check`, and `personaldb:commit` are different permissions. The resource matters too. A caller may read one bucket, write one prefix, check permissions for one namespace, and have no access elsewhere.

## Token scopes are coarse gates

A token scope is a broad capability. It is useful for service credentials, API-family access, and resource envelopes.

Examples:

```text
object:read|bucket:documents/*
object:write|bucket:uploads/tenants/acme/*
index:read|bucket:documents
policy:grant|tenant:acme
authz:tuple_write|document/doc-42#viewer
```

The exact resource string depends on the API being called. The important point is that scopes are coarse gates: they decide whether the caller is allowed to attempt an operation against that resource envelope.

Scopes do not replace relationship authorisation. A scope can say the caller may call the object-read API for a bucket. It does not by itself say the caller is a viewer of `document:doc-42`.

## Relationship authorisation is the object-level model

Product permissions are usually object-specific:

- Amy can view one document.
- Raj can edit one folder.
- Members of a group can read all objects connected to that group.
- A user can read a database row only when a relationship on another object permits it.

Anvil represents these facts as **relationship tuples**. A tuple says that an object has a named relation to a subject.

```text
document:doc-42#viewer@user:amy
document:doc-42#editor@user:raj
group:engineering#member@user:amy
document:doc-42#viewer@userset:group/engineering#member
document:doc-42#parent@folder:folder-7
```

Read the first tuple as:

> The object `document:doc-42` has relation `viewer` to subject `user:amy`.

A permission check asks whether a subject is in the requested relation or computed relation for an object.

```text
Check: is user:amy in document:doc-42#viewer?
Answer: allowed, because the tuple exists.
```

## Tuple parts

A tuple has three visible parts: object, relation, and subject.

```text
object#relation@subject
```

The object also has two parts:

```text
namespace:object_id
```

The subject can be a direct subject:

```text
subject_kind:subject_id
```

or a userset subject:

```text
userset:namespace/object_id#relation
```

Putting those together:

| Part | Example | Meaning |
| --- | --- | --- |
| Namespace | `document` | The typed object family. |
| Object id | `doc-42` | The object inside that namespace. |
| Relation | `viewer` | The relationship being asserted or checked. |
| Subject kind | `user` | The type of subject. |
| Subject id | `amy` | The concrete subject id. |
| Userset subject | `userset:group/engineering#member` | Everyone currently in another object relation. |

Anvil API fields map directly to this structure:

| API field | Example |
| --- | --- |
| `namespace` | `document` |
| `object_id` | `doc-42` |
| `relation` | `viewer` |
| `subject_kind` | `user` or `userset` |
| `subject_id` | `amy` or `group/engineering#member` |
| `caveat_hash` | empty for unconditional, or a verified caveat hash |
| `operation` | `add` or `remove` |

## Namespace labels and schema definitions are different

The word **namespace** appears in two related but different places.

A **tuple namespace label** is the first field in a relationship fact:

```text
document:doc-42#viewer@user:amy
```

Here `document` is just the typed label carried by the tuple. It does not create an object in Anvil by itself, it does not create a bucket or prefix, and it does not automatically attach to every stored object whose key looks like a document. Application or control-plane code decides that stored object `bucket=files, key=contracts/doc-42.pdf` is represented in authorisation checks as `document:doc-42`.

A **namespace schema definition** is privileged policy. It says which relations are meaningful for a tuple namespace label, which subject kinds are allowed, which usersets may be referenced, which caveats are valid, and which computed permissions may be checked. Ordinary users and ordinary tenant applications must not invent or change those definitions through object writes.

A schema definition may describe a product-neutral model like this:

```text
namespace document
  relation owner: user
  relation editor: user | userset
  relation viewer: user | userset
  relation parent: folder

  computed read  = owner | editor | viewer | parent.viewer
  computed write = owner | editor
```

This says:

- a `document` authorisation object can have owners, editors, viewers, and a parent folder;
- editors and viewers may be direct users or usersets;
- `read` is true when the subject is an owner, editor, viewer, or viewer of the parent folder;
- `write` is true when the subject is an owner or editor.

In the current public API, tenants and users write tuple facts and ask permission questions within the scopes they have been granted. Namespace schema lifecycle is a privileged bootstrap/admin concern. If a caller has `authz:tuple_write` for the `document` namespace, that caller may write allowed `document:*` tuple facts; it does not mean the caller can define what `document` means, add new computed permissions, or bypass the mapping between stored objects and authorisation objects.

## Relationships are source facts

A tuple write is a security mutation. It changes the source facts used by permission checks, authorised search, watches, and derived userset indexes.

Examples:

```text
add    document:doc-42#viewer@user:amy
remove document:doc-42#viewer@user:amy
add    group:engineering#member@user:amy
add    document:doc-42#viewer@userset:group/engineering#member
```

Tuple writes should carry a reason and actor identity. Operators need to answer who changed access, why, and at which authorisation revision.

Tuple removal must be explicit. Deleting application data does not automatically mean every related authorisation fact is safe to ignore. The cleanup path should remove or supersede relationships deliberately.

## Usersets let relationships compose

A userset is the set of subjects in another object's relation.

```text
group:engineering#member@user:amy
document:doc-42#viewer@userset:group/engineering#member
```

The first tuple puts Amy in the `member` relation of the engineering group. The second tuple says the document's viewers include that group membership userset. A permission check for `document:doc-42#viewer@user:amy` can therefore succeed through the userset path.

Usersets are useful for groups, parent containers, shared folders, tenant membership, and derived rows. They are also a place where cycles can happen. Anvil must evaluate usersets with bounded traversal and fail closed: a cycle, missing namespace, missing relation, invalid caveat, or stale derived userset must not grant access by accident.

## Computed relations make policy reusable

A direct relation is written as a tuple. A computed relation is evaluated from other relations.

```text
computed read = owner | editor | viewer | parent.viewer
```

That expression means a subject can read a document when any one of these paths succeeds:

- the subject is an `owner` of the document;
- the subject is an `editor` of the document;
- the subject is a `viewer` of the document;
- the document has a parent folder and the subject is a `viewer` of that folder.

Computed relations keep application code from copying policy everywhere. The application asks Anvil to check `read` or `write`; Anvil evaluates the current tuple graph, usersets, caveats, and consistency requirement.

## Caveats attach conditions

A caveat is a named condition attached to a relationship. It can express policy that depends on context.

Examples:

```text
document:doc-42#viewer@user:amy with expires_before=2026-12-31T23:59:59Z
document:doc-42#viewer@user:lee with network=trusted
folder:folder-7#viewer@user:raj with purpose=audit
```

Anvil stores or references the caveat definition by hash. A tuple may carry a `caveat_hash`; the permission check must evaluate the matching caveat with request context. If the hash is invalid, missing, or points at a different caveat body, the safe result is denial or a security error.

Caveats should be deterministic and reviewable. Do not hide business logic in a string that only one service understands.

## Revisions and consistency

Authorisation state changes over time. Tuple writes produce an authorisation revision and a token-like consistency marker. A caller can use that marker to say, "answer this check at least as fresh as the tuple write I just observed."

This matters for search, watches, and local-first projections. A search index may have consumed object bytes but not the latest authorisation update. If the caller asks for strict authorisation consistency and the derived state is not ready, Anvil should return an index-readiness result rather than expose stale results.

## Admin CLI and bootstrap boundaries

The admin CLI and bootstrap path exist for privileged setup, not routine tenant behaviour. Use them to create and seed the control-plane facts that ordinary callers should not be able to invent for themselves.

| Admin/bootstrap responsibility | Why it is privileged |
| --- | --- |
| Create tenants | Establishes the administrative boundary. |
| Create applications and secrets | Mints identities that can authenticate. |
| Grant token scopes | Decides which API families and resource envelopes a caller can use. |
| Register namespace and caveat definitions | Changes how future permission checks interpret tuples. |
| Seed first owner/admin relationships | Creates the initial trusted path for tenant administration. |
| Register regions or placement policy | Affects where tenant data may be stored and served. |

After bootstrap, tenant applications and users should work through scoped APIs. They can create objects, write application data, write allowed tuple facts, check permissions, open authorised watches, and query authorised indexes only when their token scopes and relationships allow it.

They cannot:

- create a new tenant boundary;
- mint their own credentials or token scopes;
- register or redefine namespace schemas or caveats through ordinary object writes;
- treat a tuple namespace label as permission to create new authorisation semantics;
- write tuples that violate namespace policy;
- grant themselves broader administrative authority;
- bypass permission checks by reading search, watch, projection, or authorisation internals;
- access `_anvil/` paths through public object APIs.

## Reserved namespaces fail closed

Anvil owns internal paths under `_anvil/`. Public callers cannot read, list, write, copy, compose, delete, or range-read those paths. This is true even when the caller has broad bucket or object scopes.

Reserved namespaces contain internal state such as index segments, authorisation tuple material, watch checkpoints, and PersonalDB material. Returning ordinary not-found semantics can leak whether internal state exists, so Anvil uses a hard `UnauthorizedReservedNamespace` failure for public access attempts.

Structured insight should come from native or admin APIs that apply authentication, scopes, relationship checks, auditing, and redaction.

## Authorisation must protect every exposure path

Authorisation is not only for direct object downloads. These can also leak data:

- bucket and prefix listings;
- object metadata and tags;
- full text counts, snippets, highlights, and facets;
- vector neighbours and hybrid scores;
- watch subscriptions and watch events;
- source and model artefact queries;
- PersonalDB group opens, commits, snapshots, and projections;
- repair findings and diagnostics.

Applications should call Anvil with the real caller identity and requested action. They should not run broad admin reads, filter in memory, and hope every exposure path was covered.

## Request decision flow

A safe request follows this order:

```text
receive request
  -> authenticate caller
  -> reject reserved namespace access
  -> check token scope for API family and resource envelope
  -> check relationship relation or computed relation
  -> evaluate caveats with request context
  -> verify required authorisation/index revision is available
  -> return only authorised objects, rows, snippets, counts, events, or diagnostics
```

When any security step cannot prove access, the default is denial. Fail closed is the rule that keeps missing schema, stale derived state, invalid caveats, cycles, and internal-path probes from becoming data exposure.

## What you can do after this page

You should be able to explain tenant bootstrap, application credentials, token scopes, relationship authorisation, tuple syntax, namespace typing, usersets, computed relations, caveats, delegation boundaries, and fail-closed reserved namespaces. Next, learn the authorisation tutorial operations or how watches keep derived state current.
