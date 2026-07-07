---
title: Metadata and Typed Fields
description: Learn where to put object metadata, how typed JSON indexes materialise query fields, and how selector and extractor JSON fit together.
---

# Metadata and Typed Fields

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/). That page introduced the object body, the object key, and the fact that object metadata is protected data rather than a free side channel.

The local tutorial chain currently has two practical limits: region activation may still block bucket placement, and the public CLI upload helper does not yet support a fully least-privilege metadata-rich upload path. For that reason, this page teaches the model and shows request shapes and illustrative CLI forms. Treat the commands as the shape you will use once the earlier placement, grants, and upload-helper gaps are resolved.

For full index JSON syntax, use [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/). For the permission strings behind index and object operations, use [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

This tutorial is about deciding where structured data belongs. You will compare canonical JSON body fields, object metadata, and typed index fields; then you will define extractors with JSON Pointer, create typed fields, and query them without turning object metadata into an unreviewed shadow database.

## Prerequisites and data ownership

The examples assume you understand the `documents` bucket and `tutorial/welcome.txt` object from the object tutorial. Metadata and typed fields are tenant-owned public-plane modelling choices, so use the public API or `anvil`; do not use `anvil-admin` to attach metadata or create typed indexes. The admin plane may help bootstrap tenants, but it should not become the path for application schema evolution.

As you read, keep one question in mind: if two copies of a value disagree, which one is authoritative? This tutorial treats the JSON body as the canonical business record, user metadata as compact operational labels, and typed index fields as derived query rows. That discipline prevents a later search, repair, or export from depending on stale duplicated values.

## The three places data can live

When you store JSON-looking business data in Anvil, there are three different places it can appear:

| Place | What it is | Use it for |
| --- | --- | --- |
| Object body | The canonical bytes for one object version. | The complete document, invoice, event, contract, image, or other payload your application owns. |
| Object metadata | The protected record around the body: content type, size, ETag, version id, mutation data, and user metadata. | Small operational facts that Anvil should list, filter, route, or inspect without parsing the full body. |
| Typed index fields | Materialised values extracted from object bodies or metadata into an index. | Query predicates, ordering, and stable lookup fields. They are derived; they are not the source of truth. |

A good rule is: put the domain object in the body, put small operational labels in user metadata, and put query-specific projections in typed indexes.

## Start with canonical object JSON

Suppose Acme stores invoices in the `documents` bucket under `accounting/invoices/`. The object body can be the canonical invoice document:

```json
{
  "schema": "acme.invoice.v1",
  "invoice_id": "inv-1001",
  "customer": {
    "id": "acme",
    "name": "Acme Ltd"
  },
  "amount": {
    "currency": "GBP",
    "cents": 420000
  },
  "state": "open",
  "due_at": "2026-07-30T00:00:00Z",
  "line_items": [
    {"sku": "support-annual", "description": "Annual support renewal", "quantity": 1}
  ],
  "notes": "Renewal prepared by the finance automation job."
}
```

This body is the durable business record. If the invoice schema grows later, the body can carry the extra fields without changing every index immediately.

The fields have different purposes. `invoice_id` is a stable identity inside the business domain. `customer.id` is useful for equality queries. `amount.cents` is useful for numeric ordering or range predicates. `state` and `due_at` are workflow fields. `line_items` and `notes` are important domain content, but they are not necessarily good metadata because they can be larger, nested, and less stable as operational filters.

Typed JSON indexes let you index the canonical object directly. You do not need to maintain a second "projection object" whose only job is to copy `customer`, `state`, `due_at`, and `amount` into a flatter file. Avoiding projection objects removes a synchronisation problem: there is one source payload, and indexes can be rebuilt from it.

## Choose object metadata deliberately

Object user metadata should be small, stable, and useful before the body is read. For the invoice above, a sensible user metadata object might be:

```json
{
  "domain": "accounting",
  "document_type": "invoice",
  "customer_id": "acme",
  "workflow_state": "open",
  "retention_class": "finance-7y",
  "pii": false
}
```

Each key has an operational reason to exist. `domain` and `document_type` help listing and routing tools separate invoices from unrelated documents. `customer_id` supports simple metadata filters and audit review. `workflow_state` is a cheap dashboard label, but the canonical value still lives in the body. `retention_class` and `pii` are policy hints that operators and applications may need without downloading the object.

Do not put the whole invoice into metadata. User metadata is best for compact labels, not for large text, nested business documents, or values that change independently of the body. If the field is part of the domain record, keep it in the body and extract it into an index when you need to query it.

At the public API level, `PutObject` accepts an `ObjectMetadata` frame before the body chunks. The exact request also includes a mutation context; the simplified shape below focuses on the metadata fields:

```json
{
  "bucket_name": "documents",
  "object_key": "accounting/invoices/inv-1001.json",
  "content_type": "application/json",
  "user_metadata_json": "{\"domain\":\"accounting\",\"document_type\":\"invoice\",\"customer_id\":\"acme\",\"workflow_state\":\"open\",\"retention_class\":\"finance-7y\",\"pii\":false}"
}
```

`content_type` is a first-class object metadata field. Use it to tell extractors and clients how to interpret the body. `user_metadata_json` is a JSON object encoded as a string in the API field. Current server validation rejects non-object user metadata.

The public CLI's current `anvil object put` command uploads a file body but does not expose `content_type` or `user_metadata_json` flags. Use the public API or Rust client for metadata-rich uploads until the CLI grows those options.

## Understand JSON Pointer

A JSON Pointer is a string that locates a value inside a JSON document. It always starts with `/`, and each path segment names an object key or array index.

For the invoice body above:

| Pointer | Value it selects |
| --- | --- |
| `/invoice_id` | `"inv-1001"` |
| `/customer/id` | `"acme"` |
| `/amount/cents` | `420000` |
| `/line_items/0/sku` | `"support-annual"` |

If a JSON object key itself contains `/`, write it as `~1` in the pointer. If a key contains `~`, write it as `~0`. Anvil uses standard JSON Pointer semantics through the JSON parser, so a missing pointer produces JSON `null` for typed JSON extraction unless the field is marked `required`.

## Select the source objects

Before an index extracts anything, it decides which source records are eligible. That is the job of `selector_json`.

For the invoice example, this selector keeps the index focused on invoice JSON objects:

```json
{
  "prefix": "accounting/invoices/",
  "content_type": "application/json"
}
```

`prefix` means the object key must start with `accounting/invoices/`. It is not a local directory; it is a string prefix over object keys. `content_type` means the object's content type must exactly equal `application/json`. Together they prevent a typed invoice index from trying to parse unrelated files such as PDFs, images, or exported reports under neighbouring prefixes.

Use selectors to keep indexes small and intentional. Query-time filters cannot recover records that were never selected and materialised.

## Define typed fields from the body

A typed JSON index stores named values extracted from canonical JSON. In the current API, typed field definitions live in `build_policy_json`; `extractor_json` can be `{}` for this kind of index.

This build policy creates queryable invoice fields:

```json
{
  "source_kind": "object_current",
  "fields": [
    {"name": "invoice_id", "extractor": "/invoice_id", "required": true},
    {"name": "customer_id", "extractor": "/customer/id", "required": true},
    {"name": "state", "extractor": "/state", "required": true},
    {"name": "due_at", "extractor": "/due_at", "required": true},
    {"name": "amount_cents", "extractor": "/amount/cents", "required": true}
  ],
  "default_order": [
    {"field": "due_at", "direction": "asc"},
    {"field": "amount_cents", "direction": "desc"},
    {"field": "invoice_id", "direction": "asc"}
  ]
}
```

`source_kind` tells Anvil which source family the index reads. `object_current` means the current object metadata snapshot and current body are the source. `fields` is the list of values to materialise. Each `name` is the field name you will use in predicates and ordering. Each `extractor` is a JSON Pointer into the object body. `required: true` means a missing value is an indexing error for that object version rather than silently becoming `null`.

`default_order` gives the query path a stable order when the caller does not send `typed_order_json`. Here, due invoices sort by due date, then by amount, then by invoice id. The implementation also uses the row source identity as a final tie-breaker so pagination remains stable.

Typed fields do not declare separate Anvil types such as `string` or `timestamp`. The JSON value that extraction produces is the value the query engine compares. Use strings for timestamps only if they are formatted so lexical order matches time order, such as RFC 3339 UTC timestamps.

## Extract a typed field from user metadata

Typed JSON fields can also read object user metadata. That is useful when an operational label is intentionally stored in metadata and should be queryable beside body fields.

This field extracts the `retention_class` user metadata key:

```json
{
  "name": "retention_class",
  "extractor": "object_user_metadata_json_pointer:/retention_class"
}
```

The prefix `object_user_metadata_json_pointer:` tells Anvil to apply the pointer to the object metadata JSON rather than to the object body. Use this for labels that truly belong in metadata. Do not use it to rebuild the entire business object out of metadata fields.

## Create the index when prerequisites are ready

The CLI form below is illustrative. It is valid for the current `anvil index create` surface, but it is not required to run in the local tutorial chain until the `documents` bucket exists, the region is writable, the object upload path can carry the metadata you need, and the caller has the narrow index grants described in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

```bash
anvil --profile acme index create documents invoices_by_due typed_json \
  --selector-json '{"prefix":"accounting/invoices/","content_type":"application/json"}' \
  --extractor-json '{}' \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"invoice_id","extractor":"/invoice_id","required":true},{"name":"customer_id","extractor":"/customer/id","required":true},{"name":"state","extractor":"/state","required":true},{"name":"due_at","extractor":"/due_at","required":true},{"name":"amount_cents","extractor":"/amount/cents","required":true}],"default_order":[{"field":"due_at","direction":"asc"},{"field":"amount_cents","direction":"desc"},{"field":"invoice_id","direction":"asc"}]}'
```

This command asks the public Index API to create a `typed_json` index named `invoices_by_due` in the `documents` bucket. The selector narrows the source set. The empty extractor is intentional for typed JSON. The build policy defines the materialised fields and their default ordering. A successful create schedules index build work; the index becomes useful after the builder has materialised a segment.

## Query typed fields

Once the index exists and has caught up, a query can ask for open Acme invoices due before a cut-off date:

```json
[
  {"field": "customer_id", "op": "eq", "value": "acme"},
  {"field": "state", "op": "eq", "value": "open"},
  {"field": "due_at", "op": "lte", "value": "2026-07-31T00:00:00Z"}
]
```

This is `typed_predicates_json`. It is an array, and all predicates must match. The first predicate checks equality, the second checks workflow state, and the third performs a range comparison against the extracted JSON value.

An explicit order can be supplied with `typed_order_json`:

```json
[
  {"field": "due_at", "direction": "asc"},
  {"field": "amount_cents", "direction": "desc"}
]
```

This sorts the matching invoices by due date first and larger amounts first within the same due date.

The matching illustrative CLI query is:

```bash
anvil --profile acme index query documents invoices_by_due \
  --path-prefix accounting/invoices/ \
  --typed-predicates-json '[{"field":"customer_id","op":"eq","value":"acme"},{"field":"state","op":"eq","value":"open"},{"field":"due_at","op":"lte","value":"2026-07-31T00:00:00Z"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"},{"field":"amount_cents","direction":"desc"}]'
```

The `path-prefix` argument is a query-time narrowing step. It does not replace `selector_json`; it only filters the rows already built into the index. The typed predicates and order use field names from the build policy, not JSON Pointers.

## Query metadata directly when equality is enough

Sometimes you do not need typed fields. If the only question is "show me invoice objects with this metadata label", a metadata-filter index can use object user metadata directly.

For the example metadata object, this query filter means `customer_id` must equal `acme` and `workflow_state` must equal `open`:

```json
{
  "customer_id": "acme",
  "workflow_state": "open"
}
```

`metadata_filters_json` is an exact-match object. Keys beginning with `/` are JSON Pointers into user metadata; other keys are top-level metadata keys. This model is simple and fast, but it is not a substitute for typed JSON when you need range predicates, custom ordering, or fields extracted from the body.

## What to take forward

Use object metadata for compact operational labels. Keep the canonical document in the object body. Use JSON Pointer to name body fields precisely. Use `selector_json` to decide which records enter an index. Use typed JSON fields to materialise queryable values from the canonical body, and use metadata filters for simple metadata equality checks. When in doubt, store the truth once and derive query views from it.

## Success and failure cues

A good metadata design has a small canonical body, intentionally duplicated user metadata only when equality filtering or gateway compatibility needs it, and typed fields that can be rebuilt from the source object. If queries return no rows, check selector prefix first, then JSON Pointer paths, then field type mismatches, then index diagnostics. A missing typed row is not a reason to store more truth in metadata; it is usually an extraction or derived-state problem.

## Where to go next

Continue with [Indexes, Path Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/) to turn the field choices from this page into queryable derived rows. Use [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/) as the exact contract when you add new selectors, extractors, typed fields, predicates, or ordering rules.
