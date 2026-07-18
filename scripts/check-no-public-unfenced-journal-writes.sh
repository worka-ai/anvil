#!/usr/bin/env bash
set -euo pipefail

command -v rg >/dev/null 2>&1 || { echo "ripgrep (rg) is required for this gate" >&2; exit 2; }

# Journal mutation paths must not expose direct writers that bypass partition fencing.
# Keep this list self-nonmatching by splitting sensitive function names below.
files=(
  anvil-core/src/append_journal.rs
  anvil-core/src/authz_journal.rs
  anvil-core/src/authz_segment.rs
  anvil-core/src/bucket_journal.rs
  anvil-core/src/control_journal.rs
  anvil-core/src/hf_journal.rs
  anvil-core/src/index_diagnostic_journal.rs
  anvil-core/src/index_journal.rs
  anvil-core/src/manifest_journal.rs
  anvil-core/src/metadata_journal.rs
  anvil-core/src/model_journal.rs
  anvil-core/src/multipart_journal.rs
  anvil-core/src/task_journal.rs
)

names=(
  "append_bucket_""mutation"
  "append_object_""mutation"
  "seal_object_journal_""segments"
  "write_authz_""tuple"
  "append_authz_tuple_""record"
  "write_authz_tuple_""segment"
  "write_index_""definition_event"
  "append_index_definition_""event"
  "write_index_""diagnostic"
  "compare_and_swap_""manifest"
  "create_append_""stream"
  "append_stream_""record"
  "seal_append_""stream"
  "create_multipart_""upload"
  "upsert_multipart_""part"
  "complete_multipart_""upload"
  "abort_multipart_""upload"
  "enqueue_""task"
  "claim_pending_""tasks"
  "update_task_""status"
  "fail_""task"
  "create_""region"
  "create_""tenant"
  "create_""app"
  "update_app_""secret"
  "grant_""policy"
  "revoke_""policy"
  "create_admin_""user"
  "update_admin_""user"
  "delete_admin_""user"
  "create_admin_""role"
  "update_admin_""role"
  "delete_admin_""role"
  "create_model_""artifact"
  "create_model_""tensors"
  "create_""key"
  "delete_""key"
  "create_""ingestion"
  "update_ingestion_""state"
  "cancel_""ingestion"
  "add_""item"
  "update_item_""state"
  "update_item_""success"
)

joined="$(IFS='|'; echo "${names[*]}")"
pattern="^[[:space:]]*pub(\\([^)]*\\))?[[:space:]]+async[[:space:]]+fn[[:space:]]+(${joined})[[:space:]]*\\("

if rg -n "$pattern" "${files[@]}"; then
  echo "Public or crate-public unfenced journal mutation entrypoint found; use a fenced *_with_permit API instead." >&2
  exit 1
fi
