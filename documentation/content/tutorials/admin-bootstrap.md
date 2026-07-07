---
title: Bootstrap Administration
description: Understand first-start administration, the system realm, and the private admin API boundary.
---

# Bootstrap Administration

This page continues from [Run Anvil Locally](/tutorials/setup-local-anvil/). It assumes the `anvil-local` container is still running, the public API is reachable on host port `50051`, and the admin API on `50052` is not published to the host. Because the admin listener is private in this Docker setup, admin examples use `docker exec anvil-local ...`.

The CLI is supporting tooling over the same APIs that production automation can call directly. Use it here because it makes the request path readable. In production, a release controller, topology reconciler, audit exporter, or repair service may call the admin API directly with the same authentication, authorisation, and audit expectations. The full command surface is documented in [Admin CLI](/reference/admin-cli/) and the tenant-facing command surface is documented in [Public CLI](/reference/public-cli/).

## What bootstrap means

Anvil stores system administration authority in the **system realm**. The system realm is the built-in authorisation realm that decides who may create tenants, manage application credentials, change mesh topology, run repairs, read administrative audit streams, and rotate server-side secret envelopes.

A brand-new storage directory has no system realm yet. Bootstrap is the first-start server transaction that creates that realm and creates or binds the first system administration service principal if none exists. In the local setup, that first principal is the `system-admin` app, and its generated credential file is `/var/lib/anvil/bootstrap/system-admin.json` inside the container.

An `app` in this context is an application or service principal: a credentialed identity for software, automation, or operators. It is not an S3-specific artefact. The generated `system-admin` app is powerful enough for a disposable local tutorial because the local node has one operator and no tenant data worth separating. That same shape is usually too coarse for production.

For deeper operator design, read [Admin Plane](/operators/admin-plane/) and [Security Hardening](/operators/security-hardening/).

## What bootstrap is not

Bootstrap is not an API bypass. It is not a recurring admin mode. It is not a second authentication system you can turn on later.

The first-start transaction happens before public or admin requests are accepted. Once the system realm exists, every admin operation follows the ordinary path: authenticate the caller, authorise the requested relation in the system realm, validate the request, mutate Anvil-owned state, and record audit evidence. If you restart a node with first-start bootstrap settings after the system realm already exists, those settings do not mint fresh authority.

This distinction matters operationally. If an operator cannot perform an admin action after first start, the fix is to repair the principal, credential, or system-realm authorisation model. Do not invent a special admin mode or route around the API.

## Confirm the local first credential exists

The setup page configured the server to write the first credential file inside the Docker volume. Before using it, check that the file exists without printing its secret contents.

```bash
docker exec anvil-local sh -c 'test -s /var/lib/anvil/bootstrap/system-admin.json && echo "system-admin credential file exists"'
```

The message confirms that the container has the generated first-start credential file. It does not prove the credential is safe to expose. Keep the file private, and use it only to mint short-lived request credentials for this local bootstrap flow.

## Understand how `anvil-admin` gets a bearer token

`anvil-admin` sends a bearer token to the admin API. That token is a short-lived request credential. It is different from the client id and client secret in `system-admin.json`, which are long-lived credential material.

`ANVIL_AUTH_TOKEN` controls the first step. If `ANVIL_AUTH_TOKEN` is set, `anvil-admin` sends that token to the admin API and does not need to exchange client credentials first. If `ANVIL_AUTH_TOKEN` is absent, the CLI can mint a token through the public API: in the local container it can read the credential-file environment path configured during setup, and in a host or CI environment it can use the selected CLI profile's client id and client secret. The minted token is then sent to the admin API.

Because the setup page created a host-side `local-system` profile, you can mint a fresh token from the host public API. The command prints only the token length so you do not accidentally paste the token into logs.

```bash
export ANVIL_AUTH_TOKEN="$(anvil auth get-token)"
printf 'received admin bearer token with %s characters\n' "${#ANVIL_AUTH_TOKEN}"
```

After this command, your shell has a bearer token for the first system administration principal. The token is suitable for short manual checks. When it expires, run the token exchange again instead of reusing old output from a terminal scrollback.

## Call the private admin API from inside Docker

The admin API is still private to the container. Pass the short-lived bearer token into `docker exec` so the admin CLI can send it to `http://127.0.0.1:50052` inside the container.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --limit 10
```

A successful response proves three things: the token is valid, the admin API is reachable inside the container, and the system realm authorises the principal to read admin audit events. The host still has no direct admin-port exposure.

If you omit `-e ANVIL_AUTH_TOKEN=...` in this local container, `anvil-admin` can still mint a token because setup supplied `ANVIL_BOOTSTRAP_CREDENTIAL_FILE` and `ANVIL_PUBLIC_ENDPOINT` to the container. That fallback is convenient for local inspection, but production automation should make its credential source explicit.

## What the generated system-admin credential can and cannot do

The generated `system-admin` credential can mint a bearer token for the bootstrap-created system owner/admin principal. That bearer token can call admin API operations that the system realm authorises for that principal. In this local tutorial, that is enough to prove the private admin API is reachable, tokens work, and admin authorisation is enforced by the system realm.

The generated credential is not a recommended daily production credential. It is the first-start credential that gets a new Anvil system out of a zero-administrator state. In production, keep it in protected break-glass storage after normal operator access is established. Do not bake it into release jobs, dashboards, runbooks, or application containers.

Creating another app is only creating identity material. `anvil-admin app create` can create a client id and client secret for a named app, but that does not by itself grant admin authority. Public policy grants are also not a substitute: they are data/public API scopes, not system-realm admin relations.

## Named admin principals require system-realm relations

A named production admin app should represent one duty or automation boundary. Topology controllers, repair tooling, audit exporters, release automation, secret-rotation jobs, and emergency break-glass access should not all share one credential if they have different risk profiles. Separate principals give clearer audit identity, independent rotation, independent revocation, and least-privilege authorisation.

That separation only works after each named app is bound to the right system-realm relation. The current system-realm relation model distinguishes tenant management, app management, policy management, secret-encryption-key management, bucket management, node management, region management, routing management, host-alias management, link management, repair execution, diagnostic viewing, and audit-log viewing. A topology controller might need region, node, and routing relations; a repair service might need repair and diagnostics relations; an audit exporter might need audit-log viewing but not tenant creation or secret rotation.

This tutorial does not invent a command for binding those system-realm relations. At the time of this page, the documented CLI flow can show first-start bootstrap and local admin API access, but it does not expose a complete named-admin-principal delegation workflow. The production model remains: create named service principals, bind only the system-realm relations they need, and verify unrelated admin calls fail. The implementation and command reference must document that relation-binding path before this tutorial can safely present it as a runnable sequence.

For the intended production posture, keep [Admin Plane](/operators/admin-plane/) and [Security Hardening](/operators/security-hardening/) as the design references.

## Verify audit evidence

Admin changes should not be anonymous maintenance. List recent audit events during local bootstrap work so you can inspect the request id, principal, action, and stated reason for any recorded administrative changes.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --limit 20
```

On a fresh local node, the important check is that the authenticated audit read succeeds without exposing the admin port on the host. After mutating admin commands, use the same audit stream to confirm that the change is attributable. A deployment that cannot explain administrative changes is not ready to host production data.

## Reduce dependence on the first-start credential

For the local tutorial, you can continue using the `system-admin` profile. For production, move the first-start credential into protected break-glass storage after the named-principal and system-realm relation workflow is implemented, documented, and verified. Day-to-day automation should use its own app, its own secret, its own narrow system-realm relations, and its own audit identity.

The next operator-heavy tutorial is [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/). In the local Docker posture from this page, keep using `docker exec anvil-local ...` for admin commands unless you intentionally expose the admin listener on a protected internal endpoint.
