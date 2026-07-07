---
title: Bootstrap Administration
description: Understand first-start administration, the system realm, and the private admin API boundary.
---

# Bootstrap Administration

This tutorial continues from [Run Anvil Locally](/tutorials/setup-local-anvil/). It assumes the `anvil-local` container is running, the public API is reachable on host port `50051`, and the admin API on `50052` is not published to the host.

Bootstrap is the moment a brand-new Anvil storage directory gets its first system administrator. This page shows how to inspect that first credential, how `anvil-admin` obtains a bearer token, how to call the private admin API without exposing it, and why production administration should move from one bootstrap credential to named, auditable admin principals. For exact command shapes, use [Admin CLI](/reference/admin-cli/). For tenant-facing command shapes, use [Public CLI](/reference/public-cli/).

The admin CLI is supporting tooling over the private admin API. It is not a filesystem repair program, and it is not a shortcut around authentication or authorisation. Except for local helper commands such as key generation, `anvil-admin` sends a bearer token to the admin listener and the server checks Anvil's built-in system realm.

## Prerequisites

Before starting this page, verify the local setup checkpoints:

```bash
curl -fsS http://127.0.0.1:50051/ready

docker exec anvil-local sh -c \
  'test -s /var/lib/anvil/bootstrap/system-admin.json && echo "system-admin credential file exists"'
```

The first command proves the public plane is reachable from the host. The second proves the first-start credential file exists inside the Docker volume. Neither command exposes the admin API to the host.

If your shell does not already have a system-admin bearer token, mint one from the `local-system` profile created in the setup tutorial:

```bash
export ANVIL_AUTH_TOKEN="$(anvil --profile local-system auth get-token)"
printf 'received admin bearer token with %s characters\n' "${#ANVIL_AUTH_TOKEN}"
```

The token is short-lived. When it expires, mint a new one rather than reusing old terminal output.

## What bootstrap creates

Anvil stores system administration authority in the **system realm**. The system realm is the built-in relationship-authorisation realm that decides who may create tenants, manage application credentials during bootstrap, grant public policies, change topology, run repairs, read admin diagnostics, read admin audit events, and rotate server-side secret envelopes.

A brand-new storage directory has no system realm yet. First-start bootstrap is the server transaction that creates that realm and creates or binds the first system administration service principal if none exists. In this local setup, that first principal is the `system-admin` app, and its generated credential file is `/var/lib/anvil/bootstrap/system-admin.json` inside the container.

An app is a credentialed identity for software, automation, or operators. It is not an S3-only concept. The generated `system-admin` app is powerful enough for a disposable local tutorial because the node has one operator and no production tenant data. That same shape is usually too coarse for production. Production operators should create named principals for distinct duties such as topology control, tenant provisioning, audit export, repair, secret rotation, and emergency break-glass access.

## What bootstrap does not create

Bootstrap is not an API bypass. It is not a recurring admin mode. It is not a second authentication system you can turn on later.

The first-start transaction happens before public or admin requests are accepted. Once the system realm exists, every admin operation follows the ordinary path: authenticate the caller, authorise the requested relation in the system realm, validate the request, mutate Anvil-owned state, and record audit evidence. If you restart a node with first-start bootstrap settings after the system realm already exists, those settings do not mint a fresh administrator.

This matters during incident response. If an operator cannot perform an admin action after first start, the fix is to repair or rotate the principal, credential, or system-realm relation. Do not add a secret admin flag, expose the admin listener, edit storage files, or use public policy grants to emulate system administration.

## Inspect the first credential without leaking it

The credential file contains a client id and client secret. It is long-lived credential material, so the tutorial checks only that it exists and is valid JSON enough to read the client id. Do not print the secret value into logs.

```bash
docker exec anvil-local sh -c \
  'jq -r .client_id /var/lib/anvil/bootstrap/system-admin.json | sed "s/.*/client id present/"'
```

If `jq` is not installed in the container image, use the host-side copy from the setup tutorial:

```bash
jq -r .client_id /tmp/anvil-system-admin.json | sed 's/.*/client id present/'
```

Success proves the bootstrap credential file is readable in the place the server wrote it. It does not prove the admin API will accept a request. API acceptance requires a bearer token and system-realm authorisation.

## Understand how `anvil-admin` gets a bearer token

`anvil-admin` sends bearer tokens to the admin API. The bearer token is a short-lived request credential. It is different from the client id and client secret stored in `system-admin.json`.

Token resolution follows the same broad order as the public CLI:

| Input | Behaviour |
| --- | --- |
| `ANVIL_AUTH_TOKEN` | Sent directly to the admin API. No token exchange is performed. |
| `ANVIL_BOOTSTRAP_CREDENTIAL_FILE` | Reads `client_id` and `client_secret`, then exchanges them for a token through the public API. |
| Stored profile | Uses the selected profile credentials when no token or credential file is present. |
| `ANVIL_PUBLIC_ENDPOINT` | Selects the public endpoint used for token exchange. It does not change the admin API endpoint. |

In this tutorial, passing `ANVIL_AUTH_TOKEN` into `docker exec` makes the credential source explicit:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin --host http://127.0.0.1:50052 audit list --limit 5
```

The `--host` value is the private admin endpoint as seen from inside the container. It is not the public endpoint, and it is not reachable from the host because the setup page did not publish port `50052`.

If you omit `ANVIL_AUTH_TOKEN` in this local container, `anvil-admin` can still mint a token because the setup page supplied `ANVIL_BOOTSTRAP_CREDENTIAL_FILE` and `ANVIL_PUBLIC_ENDPOINT` as container environment variables. That fallback is convenient for local inspection, but production automation should make its credential source explicit.

## Call a read-only admin operation first

Start with an admin audit read because it is read-only and easy to interpret:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --limit 10
```

A successful response proves three things at once: the token is valid, the admin API is reachable inside the container, and the system realm authorises this principal to read admin audit events. It also proves the host still does not need direct admin-port exposure.

A failure tells you where to look:

- Connection refused means the admin endpoint is wrong or the server is not listening where the CLI expects.
- Unauthenticated means the token is missing, expired, malformed, or minted by the wrong public endpoint.
- Permission denied means the principal is authenticated but lacks the required system-realm relation.
- Empty output is not a failure. It can mean there are no matching audit events in the requested page.

Keep this habit for other admin work: read first, mutate only after you know which private endpoint, principal, and relation are involved.

## Understand the generated credential's limits

The generated `system-admin` credential can mint a bearer token for the bootstrap-created system administrator. In this local tutorial that is enough to prove private admin reachability, token exchange, system-realm authorisation, and admin audit reading.

It is not a recommended daily production credential. It is the first-start credential that gets a new Anvil system out of a zero-administrator state. After named production access exists, store it in protected break-glass storage. Do not bake it into release jobs, dashboards, application containers, routine repair jobs, or tenant automation.

Creating another app is only creating identity material. `anvil-admin app create` can create a client id and client secret for a named app, but that does not by itself grant admin authority. Public policy grants are also not a substitute: they are public/data-plane scopes, not system-realm admin relations. See [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) for the distinction between public policy scopes, relationship authorisation, and system-realm relations.

## Plan named admin principals before production

A named production admin app should represent one duty or automation boundary. Examples include:

| Principal | Likely system-realm relations | Should not have |
| --- | --- | --- |
| Tenant provisioner | tenant, app, and public-policy management | topology repair, secret rotation |
| Topology controller | region, cell, node, and routing management | tenant object access |
| Audit exporter | admin audit-log viewing | tenant creation, repair execution |
| Repair service | repair execution and diagnostics viewing | app secret rotation |
| Secret-rotation job | secret-encryption-key management | bucket or topology changes |

The exact relation names are documented in [Admin CLI](/reference/admin-cli/). This tutorial does not invent a command for binding those relations if the documented CLI flow does not expose one. The safe production model remains: create named service principals, bind only the system-realm relations they need through the supported admin-plane workflow, verify unrelated admin calls fail, and record audit evidence for every mutation.

Do not try to model those duties with tenant public policy grants. A tenant app can own and manage tenant resources through the public plane where delegated, but it cannot grant itself system topology authority or rewrite the built-in system realm.

## Verify audit evidence after mutations

Every mutating admin command should carry an audit reason. The local setup page has not yet made mutating admin changes beyond first-start bootstrap, but the same audit stream is where you verify later tenant, topology, policy, repair, and lifecycle changes.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --limit 20
```

Read the response for the principal, action, resource, request id, and stated reason. A deployment that cannot explain administrative changes is not ready to host production data.

## Success and failure cues

The happy path is deliberately narrow: the credential file exists, token exchange returns a non-empty bearer token, and a read-only `anvil-admin audit list` works from inside the container boundary. The host admin port remains unpublished.

Common failures usually mean one of three things. If token exchange fails, inspect the public endpoint and credential source. If an admin command cannot connect, inspect `ANVIL_ADMIN_ENDPOINT`, `--host`, and server logs. If the command returns permission denied, treat it as a system-realm authorisation problem; do not expose `50052`, add public scopes, or edit storage files to bypass the relation check.

## Where to go next

Continue with [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/) to register the local topology descriptors through the private admin plane. When you are ready to hand work to tenants, read [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/). For production posture, keep [Admin Plane](/operators/admin-plane/) and [Security Hardening](/operators/security-hardening/) nearby.
