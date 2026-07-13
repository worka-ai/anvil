use super::*;
use crate::writer_segment_catalog::list_writer_segment_catalog_records;

pub(crate) async fn write_authz_tuple_delta_segment(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    previous_derived_usersets: &[AuthzDerivedUsersetEntry],
    target_revision: u64,
    source_fence_token: u64,
) -> Result<String> {
    if target_revision == 0 {
        bail!("authz delta segment revision must be nonzero");
    }
    let target_revision_i64 = i64::try_from(target_revision)
        .context("authz delta segment revision exceeds supported range")?;
    let current_records = records
        .iter()
        .filter(|record| record.revision <= target_revision_i64)
        .cloned()
        .collect::<Vec<_>>();
    let previous_records = records
        .iter()
        .filter(|record| record.revision < target_revision_i64)
        .cloned()
        .collect::<Vec<_>>();
    let delta_records = current_records
        .iter()
        .filter(|record| record.revision == target_revision_i64)
        .cloned()
        .collect::<Vec<_>>();
    let segment_records = segment_records_from_authz_records(&delta_records)?;

    let current_active = active_tuple_records(&current_records);
    let previous_active = active_tuple_records(&previous_records);
    let schema_rows = schema_descriptor_rows(storage, tenant_id, &current_active).await?;
    let previous_schema_rows = schema_descriptor_rows(storage, tenant_id, &previous_active).await?;
    let current_bound_relation_rule_rows =
        bound_relation_rule_rows(storage, tenant_id, &current_active).await?;
    let relation_rule_rows =
        all_relation_rule_rows(storage, tenant_id, &current_bound_relation_rule_rows).await?;
    let previous_bound_relation_rule_rows =
        bound_relation_rule_rows(storage, tenant_id, &previous_active).await?;
    let previous_relation_rule_rows =
        all_relation_rule_rows(storage, tenant_id, &previous_bound_relation_rule_rows).await?;
    let schema_or_binding_changed_at_target_revision =
        authz_realm_schema::list_schema_revisions(storage, tenant_id)
            .await?
            .into_iter()
            .any(|record| record.authz_revision == target_revision)
            || authz_realm_schema::list_schema_bindings(storage, tenant_id)
                .await?
                .into_iter()
                .any(|record| record.authz_revision == target_revision);
    let schema_replacement =
        schema_or_binding_changed_at_target_revision || schema_rows != previous_schema_rows;
    let relation_rule_replacement = schema_or_binding_changed_at_target_revision
        || relation_rule_rows != previous_relation_rule_rows;

    let current_view = tuple_view_from_active_records(&current_active);
    let previous_view = tuple_view_from_active_records(&previous_active);
    let current_userset_edges =
        userset_edge_rows(&current_active, derived_usersets, target_revision)?;
    let previous_userset_edges = userset_edge_rows(
        &previous_active,
        previous_derived_usersets,
        target_revision.saturating_sub(1),
    )?;
    let current_list_objects = list_object_rows(
        storage,
        tenant_id,
        &current_active,
        derived_usersets,
        &relation_rule_rows,
        &current_view,
        target_revision,
    )
    .await?;
    let previous_list_objects = list_object_rows(
        storage,
        tenant_id,
        &previous_active,
        previous_derived_usersets,
        &previous_relation_rule_rows,
        &previous_view,
        target_revision.saturating_sub(1),
    )
    .await?;
    let current_list_subjects = list_subject_rows(
        storage,
        tenant_id,
        &current_active,
        derived_usersets,
        &relation_rule_rows,
        &current_view,
        target_revision,
    )
    .await?;
    let previous_list_subjects = list_subject_rows(
        storage,
        tenant_id,
        &previous_active,
        previous_derived_usersets,
        &previous_relation_rule_rows,
        &previous_view,
        target_revision.saturating_sub(1),
    )
    .await?;

    let current_list_objects_count = current_list_objects.len() as u64;
    let current_list_subjects_count = current_list_subjects.len() as u64;
    let userset_edge_deltas = userset_edge_delta_rows(
        previous_userset_edges,
        current_userset_edges,
        target_revision,
    )?;
    let list_object_deltas =
        list_object_delta_rows(previous_list_objects, current_list_objects, target_revision)?;
    let list_subject_deltas = list_subject_delta_rows(
        previous_list_subjects,
        current_list_subjects,
        target_revision,
    )?;
    let checkpoint_rows = vec![AuthzRevisionCheckpointRow {
        tenant_id,
        revision: target_revision,
        source_fence_token,
        tuple_record_count: current_records.len() as u64,
        active_tuple_count: current_active.len() as u64,
        derived_userset_count: derived_usersets.len() as u64,
        list_objects_count: current_list_objects_count,
        list_subjects_count: current_list_subjects_count,
        tuple_records_hash: hex::encode(tuple_records_hash(&current_records)?),
    }];
    let segment_tables = vec![
        WriterBodyTable {
            table_id: TABLE_AUTHZ_SCHEMA_DESCRIPTOR,
            row_type_id: TABLE_AUTHZ_SCHEMA_DESCRIPTOR,
            rows: if schema_replacement {
                table_rows_from(
                    schema_rows,
                    schema_descriptor_key,
                    encode_schema_descriptor_row,
                )?
            } else {
                Vec::new()
            },
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_TUPLE,
            row_type_id: TABLE_AUTHZ_TUPLE,
            rows: segment_records
                .iter()
                .map(|record| TableRow {
                    key: record.key.clone(),
                    value: record.value.clone(),
                })
                .collect(),
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_RELATION_RULE,
            row_type_id: TABLE_AUTHZ_RELATION_RULE,
            rows: if relation_rule_replacement {
                table_rows_from(
                    relation_rule_rows,
                    relation_rule_key,
                    encode_relation_rule_row,
                )?
            } else {
                Vec::new()
            },
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_USERSET_EDGE,
            row_type_id: TABLE_AUTHZ_USERSET_EDGE,
            rows: table_rows_from(
                userset_edge_deltas,
                userset_edge_key,
                encode_userset_edge_row,
            )?,
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_CAVEAT_DESCRIPTOR,
            row_type_id: TABLE_AUTHZ_CAVEAT_DESCRIPTOR,
            rows: Vec::new(),
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_REVISION_LOG,
            row_type_id: TABLE_AUTHZ_REVISION_LOG,
            rows: table_rows_from(
                checkpoint_rows,
                revision_checkpoint_key,
                encode_revision_checkpoint_row,
            )?,
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_LIST_OBJECTS,
            row_type_id: TABLE_AUTHZ_LIST_OBJECTS,
            rows: table_rows_from(list_object_deltas, list_object_key, encode_list_objects_row)?,
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_LIST_SUBJECTS,
            row_type_id: TABLE_AUTHZ_LIST_SUBJECTS,
            rows: table_rows_from(
                list_subject_deltas,
                list_subject_key,
                encode_list_subjects_row,
            )?,
        },
    ];
    write_authz_tuple_segment_tables(
        storage,
        tenant_id,
        target_revision - 1,
        target_revision,
        "delta",
        schema_replacement,
        relation_rule_replacement,
        source_fence_token,
        &segment_records,
        segment_tables,
    )
    .await
}

pub(super) async fn read_authz_tuple_segment_at_revision(
    storage: &Storage,
    tenant_id: i64,
    revision: u64,
) -> Result<Option<DecodedAuthzSegment>> {
    let scope = authz_tuple_segment_scope(tenant_id)?;
    let records =
        list_writer_segment_catalog_records(storage, AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY, &scope)?;
    let mut last_generation = 0_u64;
    let mut merged_records = Vec::new();
    let mut schema_descriptors = Vec::new();
    let mut relation_rules = Vec::new();
    let mut userset_edges = BTreeMap::<Vec<u8>, AuthzUsersetEdgeRow>::new();
    let mut list_objects = BTreeMap::<Vec<u8>, AuthzListObjectsRow>::new();
    let mut list_subjects = BTreeMap::<Vec<u8>, AuthzListSubjectsRow>::new();
    let mut revision_checkpoints = Vec::new();
    let mut final_header = None;

    for record in records
        .into_iter()
        .filter(|record| record.generation <= revision)
    {
        let Some(segment) =
            read_authz_tuple_segment_ref(storage, tenant_id, &record.segment_ref).await?
        else {
            bail!("AuthzCandidateSetStale");
        };
        match segment.header.segment_kind.as_str() {
            "checkpoint" => {
                merged_records.clear();
                userset_edges.clear();
                list_objects.clear();
                list_subjects.clear();
                revision_checkpoints.clear();
                last_generation = segment.header.generation;
                schema_descriptors = segment.schema_descriptors.clone();
                relation_rules = segment.relation_rules.clone();
            }
            "delta" => {
                if segment.header.base_revision != last_generation
                    || segment.header.generation != last_generation.saturating_add(1)
                {
                    bail!("AuthzCandidateSetStale");
                }
                last_generation = segment.header.generation;
                if segment.header.schema_replacement {
                    schema_descriptors = segment.schema_descriptors.clone();
                }
                if segment.header.relation_rule_replacement {
                    relation_rules = segment.relation_rules.clone();
                }
            }
            _ => bail!("authz segment has unsupported segment kind"),
        }
        merged_records.extend(segment.records);
        apply_userset_edge_deltas(&mut userset_edges, segment.userset_edges)?;
        apply_list_object_deltas(&mut list_objects, segment.list_objects)?;
        apply_list_subject_deltas(&mut list_subjects, segment.list_subjects)?;
        revision_checkpoints.extend(segment.revision_checkpoints);
        final_header = Some(segment.header);
    }

    if last_generation != revision {
        return Ok(None);
    }
    let Some(mut header) = final_header else {
        return Ok(None);
    };
    header.base_revision = 0;
    header.segment_kind = "merged".to_string();
    Ok(Some(DecodedAuthzSegment {
        header,
        records: merged_records,
        schema_descriptors,
        relation_rules,
        userset_edges: userset_edges.into_values().collect(),
        revision_checkpoints,
        list_objects: list_objects.into_values().collect(),
        list_subjects: list_subjects.into_values().collect(),
    }))
}

fn userset_edge_delta_rows(
    previous: Vec<AuthzUsersetEdgeRow>,
    current: Vec<AuthzUsersetEdgeRow>,
    revision: u64,
) -> Result<Vec<AuthzUsersetEdgeRow>> {
    let previous = rows_by_key(previous, userset_edge_key)?;
    let current = rows_by_key(current, userset_edge_key)?;
    Ok(delta_rows(
        previous,
        current,
        revision,
        |row, revision, operation| {
            row.revision = revision;
            row.operation = operation.to_string();
        },
    ))
}

fn list_object_delta_rows(
    previous: Vec<AuthzListObjectsRow>,
    current: Vec<AuthzListObjectsRow>,
    revision: u64,
) -> Result<Vec<AuthzListObjectsRow>> {
    let previous = rows_by_key(previous, list_object_key)?;
    let current = rows_by_key(current, list_object_key)?;
    Ok(delta_rows(
        previous,
        current,
        revision,
        |row, revision, operation| {
            row.revision = revision;
            row.operation = operation.to_string();
        },
    ))
}

fn list_subject_delta_rows(
    previous: Vec<AuthzListSubjectsRow>,
    current: Vec<AuthzListSubjectsRow>,
    revision: u64,
) -> Result<Vec<AuthzListSubjectsRow>> {
    let previous = rows_by_key(previous, list_subject_key)?;
    let current = rows_by_key(current, list_subject_key)?;
    Ok(delta_rows(
        previous,
        current,
        revision,
        |row, revision, operation| {
            row.revision = revision;
            row.operation = operation.to_string();
        },
    ))
}

fn apply_userset_edge_deltas(
    state: &mut BTreeMap<Vec<u8>, AuthzUsersetEdgeRow>,
    deltas: Vec<AuthzUsersetEdgeRow>,
) -> Result<()> {
    for mut row in deltas {
        let key = userset_edge_key(&row)?;
        match row.operation.as_str() {
            "add" => {
                row.operation = "add".to_string();
                state.insert(key, row);
            }
            "remove" => {
                state.remove(&key);
            }
            _ => bail!("authz userset edge delta has invalid operation"),
        }
    }
    Ok(())
}

fn apply_list_object_deltas(
    state: &mut BTreeMap<Vec<u8>, AuthzListObjectsRow>,
    deltas: Vec<AuthzListObjectsRow>,
) -> Result<()> {
    for mut row in deltas {
        let key = list_object_key(&row)?;
        match row.operation.as_str() {
            "add" => {
                row.operation = "add".to_string();
                state.insert(key, row);
            }
            "remove" => {
                state.remove(&key);
            }
            _ => bail!("authz list-object delta has invalid operation"),
        }
    }
    Ok(())
}

fn apply_list_subject_deltas(
    state: &mut BTreeMap<Vec<u8>, AuthzListSubjectsRow>,
    deltas: Vec<AuthzListSubjectsRow>,
) -> Result<()> {
    for mut row in deltas {
        let key = list_subject_key(&row)?;
        match row.operation.as_str() {
            "add" => {
                row.operation = "add".to_string();
                state.insert(key, row);
            }
            "remove" => {
                state.remove(&key);
            }
            _ => bail!("authz list-subject delta has invalid operation"),
        }
    }
    Ok(())
}

fn rows_by_key<T: Clone>(
    rows: Vec<T>,
    key_fn: fn(&T) -> Result<Vec<u8>>,
) -> Result<BTreeMap<Vec<u8>, T>> {
    let mut by_key = BTreeMap::new();
    for row in rows {
        by_key.insert(key_fn(&row)?, row);
    }
    Ok(by_key)
}

fn delta_rows<T: Clone>(
    previous: BTreeMap<Vec<u8>, T>,
    current: BTreeMap<Vec<u8>, T>,
    revision: u64,
    mut set_delta: impl FnMut(&mut T, u64, &str),
) -> Vec<T> {
    let mut deltas = Vec::new();
    for (key, mut row) in current.iter().map(|(key, row)| (key, row.clone())) {
        if !previous.contains_key(key) {
            set_delta(&mut row, revision, "add");
            deltas.push(row);
        }
    }
    for (key, mut row) in previous {
        if !current.contains_key(&key) {
            set_delta(&mut row, revision, "remove");
            deltas.push(row);
        }
    }
    deltas
}
