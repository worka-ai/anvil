# Anvil 2026-07-03: Mesh routing, fenced lifecycle, and internal object proxying

This release advances Anvil's mesh-oriented storage model. It focuses on making regional object routing, lifecycle control, ownership fencing, and operator diagnostics explicit enough for production deployment and follow-on scale validation.

## Cross-region routing and proxying

Anvil now exposes an internal streaming object proxy service for cross-region object operations. The public gateway can resolve a bucket locator, select an active remote object node, and proxy object requests to the owning region when policy requires proxying.

The proxy path preserves the original authenticated principal instead of minting broader authority. The receiving node validates the internal node-issued proxy token, decodes the original authorisation context, checks that the principal and tenant match the request header, and then executes the object operation using the original scopes.

The implemented object proxy path covers ordinary object `GET`, `HEAD`, `PUT`, and `DELETE`. It carries object metadata, content type, version metadata, user metadata, request identity, idempotency key, and conditional headers. GET responses continue to support S3-style range and precondition handling at the gateway boundary.

## Deterministic wrong-region behaviour

Cross-region routing policy is now exercised directly by the gateway:

- `redirect_preferred` returns protocol-compatible region redirects.
- `proxy_preferred` proxies only when an active remote object node is available, otherwise it redirects.
- `proxy_required` proxies only when an active remote object node is available, otherwise it returns a service-unavailable response.
- `local_only` rejects wrong-region requests.

Bucket-level operations that are not object-proxyable continue to return deterministic redirect or rejection responses according to the same policy.

## Fenced ownership for derived write paths

Derived write paths now acquire and pass explicit fenced ownership authority before committing control or derived stream updates. This includes watch checkpoints, index partition watch records, index build state, and PersonalDB ownership-sensitive paths.

The change tightens stale-owner rejection by ensuring write paths named by the RFC do not silently append control records without the current owner fence.

## Mesh lifecycle and admin surfaces

The admin service and CLI now expose a more complete lifecycle operator workflow for regions, cells, nodes, host aliases, links, routing records, diagnostics, repair, and audit listing. The admin CLI is a first-class operational asset for bootstrapping and managing mesh membership.

Lifecycle mutation responses carry request IDs, idempotency keys, generation information, and durable audit identifiers. Cursor binding checks are stronger so admin pagination cannot silently reuse cursors across incompatible filters or sort orders.

## Durable audit listing

Admin mutations now write durable audit events that can be queried through the admin surface. Audit entries include principal, action, resource, request ID, idempotency key, audit reason, details, and a revision-derived generation marker for pagination and operator review.

## Object links and aliases

Object link handling is now integrated into the admin workflow. Links are generation-checked metadata records that allow movable aliases such as `latest.exe` to point at immutable object versions or keys while retaining clear audit and repair visibility.

## Verification performed on the release branch

Focused validation for this release included:

- internal proxy unit tests;
- internal proxy integration tests for PUT, GET, and principal mismatch rejection;
- S3 gateway routing tests for redirect, proxy fallback, proxy-required unavailable, and active remote object-node selection;
- S3 gateway tests for range handling, preconditions, link handling, and reserved namespace guards;
- admin CLI parser and handler tests;
- admin lifecycle integration tests;
- compile checks for the server, CLI, and Rust client surfaces.

