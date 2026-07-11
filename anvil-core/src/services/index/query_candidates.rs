use super::*;
use crate::query_planner::{CandidateSet, CandidateSetKind, CandidateSetScope, CoreDocId};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct QueryCandidateMetrics {
    pub(super) input_candidate_count: u64,
    pub(super) boundary_candidate_count: u64,
    pub(super) authz_candidate_count: u64,
    pub(super) index_candidate_count: u64,
    pub(super) intersection_candidate_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct QueryCandidatePlan {
    pub(super) selected_ordinals: Vec<usize>,
    pub(super) metrics: QueryCandidateMetrics,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct LoadedTypedJsonCandidatePlan {
    pub(super) rows: Vec<typed_field_segment::TypedFieldSegmentRow>,
    pub(super) metrics: QueryCandidateMetrics,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct LoadedMetadataCandidatePlan {
    pub(super) rows: Vec<typed_field_segment::TypedFieldSegmentRow>,
    pub(super) metrics: QueryCandidateMetrics,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_metadata_backed_candidates(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
    index_generation: u64,
    authz_revision: u64,
    authz_scope: &QueryAuthzScope,
    predicate_hash: String,
    order_hash: String,
    boundary_schema_generation_hash: String,
    rows: &[typed_field_segment::TypedFieldSegmentRow],
    filters: &QueryFilters,
    boundary_predicates: &[BoundaryPredicate],
    permission_filter: Option<&QueryPermissionFilter>,
) -> Result<QueryCandidatePlan, Status> {
    plan_row_candidates(
        bucket,
        index,
        index_generation,
        authz_revision,
        authz_scope,
        predicate_hash,
        order_hash,
        boundary_schema_generation_hash,
        rows,
        None,
        |row| {
            let object_ref = QueryObjectRef::from_typed_field_row(row)?;
            filters.matches(&object_ref)
        },
        |row| {
            let object_ref = QueryObjectRef::from_typed_field_row(row)?;
            if boundary_predicates.is_empty() {
                return Ok(true);
            }
            let Some(metadata) = object_ref.user_meta.as_ref() else {
                return Ok(false);
            };
            Ok(boundary_predicates
                .iter()
                .all(|predicate| predicate.matches_metadata(metadata)))
        },
        permission_filter,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_loaded_metadata_backed_candidates(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
    row_count: u64,
    rows: Vec<typed_field_segment::TypedFieldSegmentRow>,
    filters: &QueryFilters,
    boundary_predicates: &[BoundaryPredicate],
    permission_filter: Option<&QueryPermissionFilter>,
) -> Result<LoadedMetadataCandidatePlan, Status> {
    let started_at = std::time::Instant::now();
    let mut index_count = 0u64;
    let mut boundary_count = 0u64;
    let mut authz_count = 0u64;
    let mut selected_rows = Vec::new();

    for row in rows {
        if validation::is_reserved_internal_key(&row.object_key) {
            continue;
        }
        let object_ref = QueryObjectRef::from_typed_field_row(&row)?;
        if !filters.matches(&object_ref)? {
            continue;
        }
        index_count += 1;

        let boundary_matched = if boundary_predicates.is_empty() {
            true
        } else if let Some(metadata) = object_ref.user_meta.as_ref() {
            boundary_predicates
                .iter()
                .all(|predicate| predicate.matches_metadata(metadata))
        } else {
            false
        };
        if !boundary_matched {
            continue;
        }
        boundary_count += 1;
        if !row_allowed_by_query_permission(&row, permission_filter) {
            continue;
        }
        authz_count += 1;
        selected_rows.push(row);
    }

    let metrics = QueryCandidateMetrics {
        input_candidate_count: row_count,
        boundary_candidate_count: boundary_count,
        authz_candidate_count: authz_count,
        index_candidate_count: index_count,
        intersection_candidate_count: selected_rows.len() as u64,
    };

    crate::perf::record_query_plan_duration(
        "metadata_loaded_candidates",
        &index.kind,
        "ok",
        selected_rows.len() as u64,
        started_at.elapsed(),
    );
    crate::perf::record_boundary_prune_ratio(
        "metadata_loaded_candidates",
        &index.kind,
        metrics.input_candidate_count,
        metrics.boundary_candidate_count,
    );
    crate::perf::record_authz_candidate_prune_ratio(
        "metadata_loaded_candidates",
        &index.kind,
        metrics.boundary_candidate_count,
        metrics
            .authz_candidate_count
            .min(metrics.boundary_candidate_count),
    );
    crate::perf::record_query_ranges_read_total(
        "metadata_loaded_candidates",
        &index.kind,
        selected_rows.len() as u64,
    );

    Ok(LoadedMetadataCandidatePlan {
        rows: selected_rows,
        metrics,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_typed_json_candidates(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
    index_generation: u64,
    authz_revision: u64,
    authz_scope: &QueryAuthzScope,
    predicate_hash: String,
    order_hash: String,
    boundary_schema_generation_hash: String,
    segment: &typed_field_segment::DecodedTypedFieldSegment,
    path_prefix: &str,
    typed_predicates: &[TypedPredicate],
    boundary_predicates: &[BoundaryPredicate],
    permission_filter: Option<&QueryPermissionFilter>,
) -> Result<QueryCandidatePlan, Status> {
    let rows = &segment.rows;
    let materialized_index_ordinals = typed_json_value_index_ordinals(segment, typed_predicates)?;
    plan_row_candidates(
        bucket,
        index,
        index_generation,
        authz_revision,
        authz_scope,
        predicate_hash,
        order_hash,
        boundary_schema_generation_hash,
        rows,
        materialized_index_ordinals,
        |row| {
            if !path_prefix.trim().is_empty() && !row.object_key.starts_with(path_prefix) {
                return Ok(false);
            }
            let typed = TypedIndexRow::from_segment_row(row.clone());
            Ok(typed_predicates
                .iter()
                .all(|predicate| predicate.matches(&typed)))
        },
        |row| {
            if boundary_predicates.is_empty() {
                return Ok(true);
            }
            let typed = TypedIndexRow::from_segment_row(row.clone());
            Ok(boundary_predicates
                .iter()
                .all(|predicate| predicate.matches_row(&typed)))
        },
        permission_filter,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_loaded_typed_json_candidates(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
    row_count: u64,
    rows: Vec<typed_field_segment::TypedFieldSegmentRow>,
    path_prefix: &str,
    typed_predicates: &[TypedPredicate],
    boundary_predicates: &[BoundaryPredicate],
    permission_filter: Option<&QueryPermissionFilter>,
) -> Result<LoadedTypedJsonCandidatePlan, Status> {
    let started_at = std::time::Instant::now();
    let mut index_matched = Vec::new();
    let mut index_count = 0u64;
    let mut boundary_count = 0u64;
    let mut authz_count = 0u64;

    for row in rows {
        if validation::is_reserved_internal_key(&row.object_key) {
            continue;
        }
        if !path_prefix.trim().is_empty() && !row.object_key.starts_with(path_prefix) {
            continue;
        }
        let typed = TypedIndexRow::from_segment_row(row.clone());
        if !typed_predicates
            .iter()
            .all(|predicate| predicate.matches(&typed))
        {
            continue;
        }
        index_count += 1;
        index_matched.push(row);
    }

    let mut selected_rows = Vec::new();
    for row in index_matched {
        let typed = TypedIndexRow::from_segment_row(row.clone());
        let boundary_matched = boundary_predicates
            .iter()
            .all(|predicate| predicate.matches_row(&typed));
        if !boundary_matched {
            continue;
        }
        boundary_count += 1;
        if !row_allowed_by_query_permission(&row, permission_filter) {
            continue;
        }
        authz_count += 1;
        selected_rows.push(row);
    }

    let metrics = QueryCandidateMetrics {
        input_candidate_count: row_count,
        boundary_candidate_count: boundary_count,
        authz_candidate_count: authz_count,
        index_candidate_count: index_count,
        intersection_candidate_count: selected_rows.len() as u64,
    };

    crate::perf::record_query_plan_duration(
        "typed_json_loaded_candidates",
        &index.kind,
        "ok",
        selected_rows.len() as u64,
        started_at.elapsed(),
    );
    crate::perf::record_boundary_prune_ratio(
        "typed_json_loaded_candidates",
        &index.kind,
        metrics.input_candidate_count,
        metrics.boundary_candidate_count,
    );
    crate::perf::record_authz_candidate_prune_ratio(
        "typed_json_loaded_candidates",
        &index.kind,
        metrics.boundary_candidate_count,
        metrics
            .authz_candidate_count
            .min(metrics.boundary_candidate_count),
    );
    crate::perf::record_query_ranges_read_total(
        "typed_json_loaded_candidates",
        &index.kind,
        selected_rows.len() as u64,
    );

    Ok(LoadedTypedJsonCandidatePlan {
        rows: selected_rows,
        metrics,
    })
}

fn typed_json_value_index_ordinals(
    segment: &typed_field_segment::DecodedTypedFieldSegment,
    typed_predicates: &[TypedPredicate],
) -> Result<Option<BTreeSet<u64>>, Status> {
    if typed_predicates.is_empty() {
        return Ok(None);
    }

    let mut selected: Option<BTreeSet<u64>> = None;
    for predicate in typed_predicates {
        let predicate_ordinals = typed_json_predicate_ordinals(segment, predicate)?;
        selected = Some(match selected {
            Some(existing) => existing
                .intersection(&predicate_ordinals)
                .copied()
                .collect::<BTreeSet<_>>(),
            None => predicate_ordinals,
        });
    }

    Ok(Some(selected.unwrap_or_default()))
}

fn typed_json_predicate_ordinals(
    segment: &typed_field_segment::DecodedTypedFieldSegment,
    predicate: &TypedPredicate,
) -> Result<BTreeSet<u64>, Status> {
    typed_json_predicate_ordinals_from_entries(&segment.value_index, predicate)
        .map(|ordinals| ordinals.into_iter().map(|ordinal| ordinal as u64).collect())
}

pub(super) fn typed_json_value_index_lookups_for_predicate(
    predicate: &TypedPredicate,
) -> Result<Vec<typed_field_segment::TypedFieldValueIndexLookup>, Status> {
    let expected_values = predicate
        .values
        .iter()
        .map(encoded_typed_predicate_value)
        .collect::<Result<Vec<_>, _>>()?;
    let mut lookups = Vec::new();
    match predicate.op.as_str() {
        "eq" | "=" | "==" => {
            if let Some(expected) = expected_values.first() {
                lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                    field_name: predicate.field.clone(),
                    encoded_value: Some(expected.clone()),
                });
            }
        }
        "in" => {
            for expected in expected_values {
                lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                    field_name: predicate.field.clone(),
                    encoded_value: Some(expected),
                });
            }
        }
        "is_null" => {
            lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                field_name: predicate.field.clone(),
                encoded_value: Some(encoded_typed_predicate_value(&JsonValue::Null)?),
            });
            lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                field_name: predicate.field.clone(),
                encoded_value: Some(vec![0x01]),
            });
        }
        "lt" | "<" | "lte" | "<=" | "gt" | ">" | "gte" | ">=" | "exists" => {
            lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                field_name: predicate.field.clone(),
                encoded_value: None,
            });
        }
        "prefix" => {
            lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                field_name: predicate.field.clone(),
                encoded_value: None,
            });
        }
        _ => {}
    }
    lookups.sort();
    lookups.dedup();
    Ok(lookups)
}

pub(super) fn typed_json_predicate_ordinals_from_entries(
    entries: &[typed_field_segment::TypedFieldValueIndexEntry],
    predicate: &TypedPredicate,
) -> Result<BTreeSet<usize>, Status> {
    let null_value = encoded_typed_predicate_value(&JsonValue::Null)?;
    let missing_value = vec![0x01];
    let expected_values = predicate
        .values
        .iter()
        .map(encoded_typed_predicate_value)
        .collect::<Result<Vec<_>, _>>()?;
    let mut ordinals = BTreeSet::new();

    for entry in entries {
        if entry.field_name != predicate.field {
            continue;
        }
        let matched = match predicate.op.as_str() {
            "eq" | "=" | "==" => expected_values
                .first()
                .is_some_and(|expected| entry.encoded_value == *expected),
            "in" => expected_values
                .iter()
                .any(|expected| entry.encoded_value == *expected),
            "lt" | "<" | "lte" | "<=" | "gt" | ">" | "gte" | ">=" => {
                // Range predicates are final-checked against the row's JSON value. The
                // value index narrows to the field posting list without imposing a
                // second comparison model for mixed JSON types.
                !expected_values.is_empty()
            }
            "prefix" => encoded_string_prefix(&expected_values)
                .is_some_and(|prefix| entry.encoded_value.starts_with(&prefix)),
            "exists" => entry.encoded_value != null_value && entry.encoded_value != missing_value,
            "is_null" => entry.encoded_value == null_value || entry.encoded_value == missing_value,
            _ => false,
        };
        if matched {
            ordinals.insert(entry.row_ordinal);
        }
    }

    Ok(ordinals)
}

fn encoded_string_prefix(expected_values: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut prefix = expected_values.first()?.clone();
    if prefix.len() < 3 || prefix.first().copied() != Some(0x30) {
        return None;
    }
    if prefix.ends_with(&[0, 0]) {
        prefix.truncate(prefix.len().saturating_sub(2));
    }
    Some(prefix)
}

fn encoded_typed_predicate_value(value: &JsonValue) -> Result<Vec<u8>, Status> {
    typed_field_segment::encode_json_value_for_typed_index(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid typed predicate value: {e}")))
}

#[allow(clippy::too_many_arguments)]
fn plan_row_candidates(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
    index_generation: u64,
    authz_revision: u64,
    authz_scope: &QueryAuthzScope,
    predicate_hash: String,
    order_hash: String,
    boundary_schema_generation_hash: String,
    rows: &[typed_field_segment::TypedFieldSegmentRow],
    materialized_index_ordinals: Option<BTreeSet<u64>>,
    mut index_predicate: impl FnMut(&typed_field_segment::TypedFieldSegmentRow) -> Result<bool, Status>,
    mut boundary_predicate: impl FnMut(
        &typed_field_segment::TypedFieldSegmentRow,
    ) -> Result<bool, Status>,
    permission_filter: Option<&QueryPermissionFilter>,
) -> Result<QueryCandidatePlan, Status> {
    let started_at = std::time::Instant::now();
    let partition_id = candidate_partition_id(bucket, index);
    let scope = CandidateSetScope {
        root_key_hash: authz_aware_query_scope_hash(
            "root",
            authz_scope,
            serde_json::json!({
                "schema": "anvil.query.root_key.v1",
                "tenant_id": bucket.tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": bucket.name,
                "index_id": index.id,
                "index_name": index.name,
            }),
        ),
        root_generation: index_generation,
        index_id: index.name.clone(),
        index_generation,
        authz_realm_id: authz_scope.realm_id.clone(),
        authz_scope_hash: authz_scope.scope_hash.clone(),
        authz_object_namespace: authz_scope.object_namespace.clone(),
        authz_relation: authz_scope.relation.clone(),
        authz_principal_hash: authz_scope.principal_hash.clone(),
        authz_revision,
        boundary_schema_generation_hash,
        predicate_hash,
        order_hash,
    };

    let mut visible_ordinals = Vec::new();
    let mut index_ordinals = Vec::new();
    let mut boundary_ordinals = Vec::new();
    let mut authz_ordinals = Vec::new();

    for (ordinal, row) in rows.iter().enumerate() {
        if validation::is_reserved_internal_key(&row.object_key) {
            continue;
        }
        visible_ordinals.push(ordinal as u64);
        let ordinal = ordinal as u64;
        let materialized_candidate = materialized_index_ordinals
            .as_ref()
            .is_none_or(|ordinals| ordinals.contains(&ordinal));
        if materialized_candidate && index_predicate(row)? {
            index_ordinals.push(ordinal);
        }
        if boundary_predicate(row)? {
            boundary_ordinals.push(ordinal as u64);
        }
        if row_allowed_by_query_permission(row, permission_filter) {
            authz_ordinals.push(ordinal as u64);
        }
    }

    let input_candidate_count = visible_ordinals.len() as u64;
    let index_candidates = bitmap_or_empty(scope.clone(), partition_id, index_ordinals);
    let boundary_candidates = bitmap_or_all(
        scope.clone(),
        partition_id,
        boundary_ordinals,
        input_candidate_count,
    );
    let authz_candidates = bitmap_or_all(
        scope.clone(),
        partition_id,
        authz_ordinals,
        input_candidate_count,
    );
    let intersection = index_candidates
        .intersect(&boundary_candidates)
        .map_err(|e| Status::failed_precondition(e.to_string()))?
        .intersect(&authz_candidates)
        .map_err(|e| Status::failed_precondition(e.to_string()))?;

    let selected_ordinals = ordinals_from_candidate_set(&intersection, input_candidate_count)
        .map_err(|e| Status::failed_precondition(e.to_string()))?
        .into_iter()
        .filter_map(|ordinal| usize::try_from(ordinal).ok())
        .collect::<Vec<_>>();

    let metrics = QueryCandidateMetrics {
        input_candidate_count,
        boundary_candidate_count: boundary_candidates
            .estimated_count()
            .min(input_candidate_count),
        authz_candidate_count: authz_candidates
            .estimated_count()
            .min(input_candidate_count),
        index_candidate_count: index_candidates
            .estimated_count()
            .min(input_candidate_count),
        intersection_candidate_count: intersection.estimated_count().min(input_candidate_count),
    };

    crate::perf::record_query_plan_duration(
        "index_candidates",
        &index.kind,
        "ok",
        selected_ordinals.len() as u64,
        started_at.elapsed(),
    );
    crate::perf::record_boundary_prune_ratio(
        "index_candidates",
        &index.kind,
        metrics.input_candidate_count,
        metrics.boundary_candidate_count,
    );
    crate::perf::record_authz_candidate_prune_ratio(
        "index_candidates",
        &index.kind,
        metrics.boundary_candidate_count,
        metrics
            .authz_candidate_count
            .min(metrics.boundary_candidate_count),
    );
    crate::perf::record_query_ranges_read_total(
        "index_candidates",
        &index.kind,
        selected_ordinals.len() as u64,
    );

    Ok(QueryCandidatePlan {
        selected_ordinals,
        metrics,
    })
}

fn row_allowed_by_query_permission(
    row: &typed_field_segment::TypedFieldSegmentRow,
    permission_filter: Option<&QueryPermissionFilter>,
) -> bool {
    let Some(permission_filter) = permission_filter else {
        return true;
    };
    if permission_filter.allows_object_key(&row.object_key) {
        return true;
    }
    let Ok(label) = hex::decode(row.authz_label_hash.as_str()) else {
        return false;
    };
    let Ok(label) = <[u8; 32]>::try_from(label.as_slice()) else {
        return false;
    };
    permission_filter.authorized_labels.contains(&label)
}

fn candidate_partition_id(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
) -> u64 {
    let hash = hash32(
        format!(
            "tenant:{}:bucket:{}:index:{}",
            bucket.tenant_id, bucket.id, index.id
        )
        .as_bytes(),
    );
    u64::from_le_bytes(hash[0..8].try_into().expect("hash prefix is eight bytes"))
}

fn bitmap_or_all(
    scope: CandidateSetScope,
    partition_id: u64,
    ordinals: Vec<u64>,
    visible_count: u64,
) -> CandidateSet {
    if ordinals.len() as u64 == visible_count {
        CandidateSet {
            scope,
            kind: CandidateSetKind::AllWithinPartition { partition_id },
        }
    } else {
        bitmap_or_empty(scope, partition_id, ordinals)
    }
}

fn bitmap_or_empty(
    scope: CandidateSetScope,
    partition_id: u64,
    ordinals: Vec<u64>,
) -> CandidateSet {
    if ordinals.is_empty() {
        return CandidateSet::empty(scope);
    }
    let mut roaring_bitmap_bytes = Vec::with_capacity(ordinals.len() * 8);
    for ordinal in ordinals {
        roaring_bitmap_bytes.extend_from_slice(&ordinal.to_le_bytes());
    }
    CandidateSet {
        scope,
        kind: CandidateSetKind::Bitmap {
            partition_id,
            roaring_bitmap_bytes,
        },
    }
}

fn ordinals_from_candidate_set(
    candidates: &CandidateSet,
    visible_count: u64,
) -> anyhow::Result<Vec<u64>> {
    match &candidates.kind {
        CandidateSetKind::Empty => Ok(Vec::new()),
        CandidateSetKind::AllWithinPartition { .. } => Ok((0..visible_count).collect()),
        CandidateSetKind::Bitmap {
            roaring_bitmap_bytes,
            ..
        } => {
            if roaring_bitmap_bytes.len() % 8 != 0 {
                anyhow::bail!("IndexCandidateBitmapCorrupt");
            }
            roaring_bitmap_bytes
                .chunks_exact(8)
                .map(|chunk| Ok(u64::from_le_bytes(chunk.try_into()?)))
                .collect()
        }
        CandidateSetKind::SortedDocIdRanges { ranges, .. } => {
            let mut out = Vec::new();
            for range in ranges {
                for ordinal in range.start_inclusive.ordinal()..range.end_exclusive.ordinal() {
                    out.push(ordinal);
                }
            }
            Ok(out)
        }
        CandidateSetKind::OrderedTuples { tuples, .. } => {
            Ok(tuples.iter().map(|tuple| tuple.doc_id.ordinal()).collect())
        }
    }
}

#[allow(dead_code)]
fn doc_id_for_ordinal(partition_id: u64, ordinal: u64) -> CoreDocId {
    CoreDocId::new(partition_id, ordinal)
}
