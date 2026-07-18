# Authorization schema contract

Authorization tuple writes are fail-closed. Every authorization realm that accepts tuple
mutations must have a bound, immutable schema revision. A tuple journal append is committed with
a precondition on that binding, so changing the binding concurrently causes the complete tuple
batch to fail.

## Members

Every `AuthzRelationSchema` declares an `AuthzSchemaMemberKind`:

- `DIRECT_RELATION` has no rewrite rules and has one or more `allowed_subjects` selectors.
- `PERMISSION` has one or more rewrite rules and has no `allowed_subjects` selectors.

Only direct relations can be added or removed as tuples. Permissions are computed by `inherit`,
`computed`, and `tuple_to_userset` rules and are never writable, including by control-plane or
privileged callers.

## Allowed subjects

An `AuthzAllowedSubject` uses one selector kind:

- `ANY_CANONICAL_ID` requires `subject_kind` and accepts any canonical ID of that kind.
- `EXACT` requires `subject_kind` and `subject_id` and accepts only that pair.
- `SAME_RESOURCE_ID` requires `subject_kind` and accepts that kind only when the subject ID equals
  the tuple resource ID.
- `PUBLIC` requires both fields to be empty and accepts only Anvil's reserved public principal.

The reserved public principal cannot be admitted through an `ANY_CANONICAL_ID` selector or encoded
as an `EXACT` selector. This keeps public access explicit in the schema.

Schema installation rejects unspecified or unknown enum values, duplicate namespaces, members,
selectors, or rules, structurally mixed direct relations and permissions, unsafe coordinates, and
unresolved tuple-to-userset targets. A tuple-to-userset source must be a direct relation, and every
allowed subject kind on that relation must resolve to a declared target namespace containing the
rule's target member.

## Installation and binding

`PutAuthzSchema` validates and stores the complete namespace set as an immutable revision. Its
digest uses a canonical ordering and includes member kinds, selectors, and rewrite rules.
`BindAuthzSchema` activates an exact schema ID, revision, and digest for one realm. `ApplyAuthzSchema`
continues to maintain legacy per-namespace snapshots but does not activate them; tuple writes require
an explicit bound revision.

An exact retry of an idempotent `WriteAuthzTuples` request returns its original revision, result
records, and zookie. New writes are validated as a whole batch against the currently bound schema;
one invalid tuple prevents every tuple in that batch from being appended.
