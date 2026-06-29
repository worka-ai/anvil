---
title: Watch Streams
description: Learn every Anvil watch operation and where to find the language-specific examples.
---

# Watch Streams

**What this page gives you:** a single tutorial map for every watch operation in Anvil. Watches are spread across services because each domain has its own source of change, but the operating model is the same: keep a cursor, process records idempotently, and store the cursor only after your derived work is durable.

A watch is not a notification that can be forgotten. It is a durable stream position. If a client crashes, it resumes from the last committed cursor. If a derived view falls behind, the cursor tells operators where lag begins. If a stream cannot be resumed because retention has moved on, the repair or rebuild path must rebuild from source facts.

## Watch workflow

1. Read or initialize the last durable cursor for your consumer.
2. Open the watch with `after_cursor` set to that value.
3. Process each event idempotently.
4. Commit your derived output.
5. Store the event cursor only after the derived output is durable.

## Watch operations

| Operation | Area | Tutorial |
| --- | --- | --- |
| `WatchBucketMetadata` | Buckets And Policies | [Open operation](/tutorials/buckets/#watch-bucket-metadata) |
| `WatchPrefix` | Objects, Versions, Streams, And Multipart Uploads | [Open operation](/tutorials/objects/#watch-a-prefix) |
| `WatchIndexDefinition` | Indexes And Search | [Open operation](/tutorials/search/#watch-index-definitions) |
| `WatchIndexPartition` | Indexes And Search | [Open operation](/tutorials/search/#watch-index-partitions) |
| `WatchAuthzTupleLog` | Authentication And Relationship Authorization | [Open operation](/tutorials/authorization/#watch-authz-tuple-log) |
| `WatchAuthzNamespace` | Authentication And Relationship Authorization | [Open operation](/tutorials/authorization/#watch-authz-namespace) |
| `WatchAuthzDerivedLag` | Authentication And Relationship Authorization | [Open operation](/tutorials/authorization/#watch-authz-derived-lag) |
| `WatchPersonalDbGroup` | PersonalDB Witnessing | [Open operation](/tutorials/personaldb/#watch-personaldb-group) |
| `WatchPersonalDbProjection` | PersonalDB Witnessing | [Open operation](/tutorials/personaldb/#watch-personaldb-projection) |
| `WatchGitSource` | Source, Model, And Ingestion Artifacts | [Open operation](/tutorials/artifacts/#watch-git-source-artifacts) |

## Language pattern

The exact request type differs by service, but the four client shapes are the same.

```anvil-tabs
{
  "operation": "watch-pattern",
  "rust": "let mut stream = anvil.watch_prefix(request).await?;\nwhile let Some(event) = stream.next().await {\n    let event = event?;\n    process_idempotently(&event).await?;\n    save_cursor(event.cursor).await?;\n}",
  "java": "WatchStream<WatchPrefixResponse> stream = anvil.watchPrefix(request);\nfor (var event : stream) {\n    processIdempotently(event);\n    saveCursor(event.getCursor());\n}",
  "node": "for await (const event of anvil.watchPrefix(request)) {\n  await processIdempotently(event);\n  await saveCursor(event.cursor);\n}",
  "python": "for event in anvil.watch_prefix(request):\n    process_idempotently(event)\n    save_cursor(event.cursor)"
}
```

## What you can do after this page

You should understand every watch stream and know where the full operation examples live. Use watches for live views, derived indexes, projections, audit export, ingestion status, and repair-aware automation.
