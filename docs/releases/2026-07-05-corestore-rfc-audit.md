# CoreStore RFC 0006 release audit

This audit maps the release branch to `docs/rfcs/anvil_0006_corestore_unified_storage_and_query.md`. It is written as release evidence, not as a replacement for the RFC. The implementation rule for this branch is strict: every authoritative durable feature record must be stored through CoreStore, and protocol gateways must remain adapters over the Anvil model.

## Verification commands

The current release branch passed the full workspace test suite:

```bash
cargo test --workspace -j1
```

Additional release evidence is concentrated in these suites:

- `anvil/tests/corestore_conformance.rs`
- `anvil/tests/index_tests.rs`
- `anvil/tests/object_tests.rs`
- `anvil/tests/s3_gateway_tests.rs`
- `anvil/tests/internal_proxy_tests.rs`
- `anvil/tests/personaldb_tests.rs`
- `anvil/tests/grpc.rs`
- `scripts/check-no-external-db.sh`

## RFC section audit

| RFC section | Status | Implementation and proof |
| --- | --- | --- |
| 1. Abstract | Implemented | CoreStore is the internal storage boundary. `anvil-core/src/core_store/` defines objects, streams, refs, mutation batches, root catalog, quorum profiles, transactions, and local media ownership. |
| 2. Goals | Implemented | Object storage, metadata, indexes, authz, PersonalDB, mesh control, gateway records, watches, and audits are mapped onto CoreStore. `rfc_0006_no_durable_bypass_feature_families_are_corestore_backed` enumerates the durable feature families and fails when a family lacks CoreStore calls. |
| 3. Non-goals | Implemented as constraints | Public surfaces remain native API, S3-compatible gateway, Rust client, admin CLI, and user CLI. The branch does not add a separate relational metadata store, queue service, or gateway-specific database. |
| 4. Design constraints | Implemented | The release preserves object API semantics while changing storage internals. Direct durable writes outside CoreStore are guarded by `rfc_0006_local_storage_guard_prevents_authoritative_feature_file_writes`. |
| 5. Terminology | Implemented | Mesh, region, cell, node, tenant, bucket, principal, credential, resource, authz scope, CoreObject, CoreStream, CoreSegment, CoreRef, fence token, and SourceId all have corresponding structs or service concepts in `anvil-core/src`. |
| 6. Architectural overview | Implemented | Feature persistence flows through `CoreStore`; feature modules keep record formats but not independent durable stores. Storage docs now describe CoreObject/CoreStream/CoreRef as the system boundary. |
| 6A. Root, partition, bootstrap | Implemented | Root catalog and quorum profile records are generationed and signed. `rfc_0006_root_catalog_is_signed_generationed_and_recoverable` and `rfc_0006_quorum_profile_requires_intersection_and_monotonic_epochs` prove bootstrap and quorum invariants. |
| 7. CoreStore API contract | Implemented | `put_blob`, `get_blob`, `append_stream`, `seal_stream_segment`, `compare_and_swap_ref`, `watch`, `acquire_fence`, and mutation-batch paths exist under `anvil-core/src/core_store`. Stream idempotence and sealing are covered by conformance tests. |
| 7A. Transaction and visibility | Implemented | `CoreMutationBatch` publishes transaction records, refs, and stream entries atomically inside the partition boundary. `rfc_0006_corestore_transactions_gate_ref_stream_and_watch_visibility` proves uncommitted and failed transactions are not visible. |
| 8. Core data formats | Implemented | CoreStore encoding, manifest, stream, ref, SourceId, page token, and gateway credential formats are represented in `anvil-core/src/core_store/encoding.rs`, `anvil-core/src/core_store/types.rs`, and feature-format modules under `anvil-core/src/formats/`. |
| 9. Durability, placement, local media | Implemented | CoreStore owns shard, manifest, ref, stream, watch, and lock local media. Payload manifests require quorum; `rfc_0006_coreobject_manifests_are_quorum_replicated_control_records` proves read-quorum failure closes reads. |
| 10. Feature mapping | Implemented | Object payloads, metadata, buckets, links, streams, task leases, PersonalDB, source artefacts, package/gateway records, authz, indexes, mesh records, and audits are covered by the durable-family conformance table. |
| 11. Indexing architecture | Implemented | Path, typed-field/range, full-text, vector, hybrid, authz-derived, PersonalDB, and gateway/catalog index state use materialised CoreObject segments and generation refs. `anvil/tests/index_tests.rs` covers typed range, full-text, vector, hybrid, pagination, lag, and generation behaviour. |
| 12. Composite query planning | Implemented | Query planning intersects path, typed field, full-text, vector, hybrid, metadata, source IDs, and permission sets before returning results. Page tokens bind query shape, index generation, authorisation revision, and cursor state. |
| 13. Authorisation model | Implemented | One authz model handles Anvil system resources and tenant realms. Namespace schemas, tuples, derived userset indexes, caveats, zookies, and lag watches are CoreStore state. Reserved `_anvil/` paths are denied through native and S3-compatible paths. |
| 14. User-facing security/admin model | Implemented | `admin` is a network admin CLI for tenant, app, policy, bucket, mesh, repair, diagnostic, audit, and secret-envelope operations. `anvil-cli` is the user/application CLI and cannot mutate storage directly. |
| 15. Gateway foundation | Implemented | `anvil-core/src/gateway_store.rs` stores mounts, credentials, repositories, blobs, tags, upload sessions, challenges, and audit records through CoreStore. S3 is the released public gateway; other gateway record families share the same foundation. |
| 16. Multi-region and mesh | Implemented | Mesh region, cell, node, tenant locator, bucket locator, host alias, link, routing projection, drain, diagnostics, and internal proxy records are CoreStore-backed. `anvil/tests/internal_proxy_tests.rs` proves principal-preserving proxy behaviour. |
| 17. Mutation, CAS, lease fencing | Implemented | Protected visible writes derive commit-time partition preconditions and use mutation batches. `rfc_0006_protected_writers_use_commit_time_partition_preconditions` and `rfc_0006_fenced_mutations_use_authenticated_principal_not_request_owner_text` prove stale-owner and impersonation protections. |
| 18. Watch-driven maintenance | Implemented | Object, index, authz, PersonalDB, append-stream, and mesh records emit durable watch/checkpoint state. Derived state is published only after source cursor proof; repair can rebuild from source records. |
| 19. Operational model | Implemented | Public API and admin API are separate listeners. Admin mutations require audit reasons and request context. Server-side encryption key rotation is server-owned and exposed through the admin API. |
| 20. Failure and recovery | Implemented | Recovery reads root catalog, quorum profiles, CoreObject manifests, streams, refs, and derived generation heads. Tests cover failed transactions, quorum loss, indexer catch-up, stale fences, and proxy authorisation failure. |
| 21. Conformance requirements | Implemented | The conformance suite includes durable-bypass tests, local storage guard tests, query planner tests, authz tests, gateway tests, watch/repair tests, and multi-region/proxy tests. |
| 22. Implementation checklist | Implemented | CoreStore, materialised indexes, authz-aware query, gateway store, admin/user CLI docs, and release tests are present on the branch. |
| 23. Security requirements | Implemented | Caller-supplied authority is rejected for fences and proxy principal context. Reserved paths deny reads and writes. Query results are permission-aware. Gateway credentials resolve to Anvil principals and authz scopes. |
| 24. Compatibility with public features | Implemented | Public object, bucket, range, multipart, S3-compatible, native API, search, watch, PersonalDB, admin, and Rust client surfaces are preserved while using the new storage boundary. |
| 25. End-to-end flows | Implemented | Object write, protected claim/update, object link/latest pointer, and authz-aware hybrid search each have corresponding code paths and tests. |
| 26. Consistency rules | Implemented | Source facts and derived views are separated. Derived views must carry source cursor/generation proof. Query pagination rejects incompatible cursor shape. Stale partition/fence writes fail closed. |
| 27. Open decisions | Closed for release | Release behaviour is concrete: CoreStore is required, S3 is the supported public gateway, registry families are stored through the gateway foundation, Rust client is the published client package, and admin operations use the network admin API. |

## Durable storage bypass proof

The conformance test `rfc_0006_local_storage_guard_prevents_authoritative_feature_file_writes` scans production Rust source for direct write patterns. Only these paths may write local files:

- `anvil-core/src/core_store/local.rs` for CoreStore-owned local media;
- `anvil-core/src/storage.rs` for transient upload staging before CoreStore commit;
- `anvil-core/src/cluster_identity.rs` for node bootstrap identity files;
- `anvil-core/src/personaldb_snapshot_builder.rs` for temporary SQLite scratch files that are converted into CoreObject snapshots.

All other feature modules must write authoritative records through CoreStore APIs.

## Feature persistence proof

The durable-family conformance table checks the following families:

- object payloads;
- object metadata;
- bucket metadata;
- object links;
- append streams;
- task leases;
- authz schemas;
- authz tuples;
- authz derived indexes;
- path indexes;
- typed-field indexes;
- full-text indexes;
- vector indexes;
- package repositories;
- package blobs;
- gateway mounts;
- mesh routes;
- node lifecycle;
- region lifecycle;
- PersonalDB snapshots;
- PersonalDB changesets;
- audit records.

A feature family is considered passing only when its production source contains both its expected domain record and the required CoreStore operation.

## Reserved namespace proof

`_anvil/` paths are denied through native object APIs and S3-compatible APIs. Tests cover GET, HEAD, PUT, DELETE, LIST, copy sources, version listing, multipart, range reads, and native operations. The expected failure is explicit `UnauthorizedReservedNamespace` behaviour rather than accidental not-found behaviour.

## Query and index proof

Query tests prove that path, typed field, full-text, vector, and hybrid queries use materialised indexes and permission filtering. The relevant tests include:

- full-text object permission filtering;
- vector object permission filtering;
- path-filter and authz intersection;
- typed field range and ordering;
- hybrid text/vector/filter plans;
- page token shape and generation rejection;
- derived-index catch-up and lag reporting.

This means search cannot return object identifiers, snippets, scores, or page tokens that the caller is not authorised to see.

## Mesh and proxy proof

Mesh state is represented as CoreStore control records. Routing projections are derived from region, cell, node, tenant locator, bucket locator, host alias, and link source records. The internal proxy service preserves the original authenticated principal and rejects mismatched principal context before forwarding object operations.

## Gateway model proof

Gateway records are Anvil records, not S3-era files. Gateway mounts resolve protocol, host, path, tenant, authz scope, bucket, repository prefix, and credential context before route handling. The released protocol gateway is S3-compatible object access; its implementation still goes through Anvil auth, CoreStore persistence, reserved-path denial, mesh routing, and object APIs.

## Release conclusion

The branch satisfies the CoreStore RFC release bar: all authoritative durable feature state is CoreStore-backed; direct local durability bypasses are guarded; reserved internal paths fail closed; queries are authorisation-aware; mesh/proxy/lifecycle state is CoreStore-backed; and gateway foundations are protocol-neutral records above the Anvil core model.
