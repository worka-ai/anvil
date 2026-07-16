use super::*;

pub(super) fn test_admin_context(
    label: &str,
    expected_generation: u64,
) -> anvil::anvil_api::AdminRequestContext {
    anvil::anvil_api::AdminRequestContext {
        request_id: format!("test-{label}-{}", uuid::Uuid::new_v4().simple()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        audit_reason: format!("test {label}"),
        expected_generation,
    }
}

pub(super) fn install_canonical_coremeta_bootstrap_snapshot(states: &[AppState]) {
    let Some(canonical) = states.first() else {
        return;
    };
    let snapshot = canonical
        .core_store
        .export_coremeta_snapshot_rows()
        .expect("export canonical CoreMeta bootstrap snapshot")
        .into_iter()
        .filter(|row| !is_node_local_coremeta_row(row))
        .filter(|row| !is_local_derived_coremeta_row(row))
        .filter(|row| !contains_local_corestore_locator(row))
        .collect::<Vec<_>>();

    for target in states.iter().skip(1) {
        target
            .core_store
            .install_coremeta_snapshot_rows(&snapshot)
            .expect("install canonical CoreMeta bootstrap snapshot");
    }
}

pub(super) fn is_node_local_coremeta_row(
    row: &anvil_core::core_store::CoreMetaEncodedOwnedRow,
) -> bool {
    if row.cf != anvil_core::core_store::CF_MESH
        || coremeta_table_id(row) != Some(anvil_core::core_store::TABLE_NODE_SIGNING_KEYPAIR_ROW)
    {
        return false;
    }
    let Ok(tuple_key) = anvil_core::core_store::core_meta_record_tuple_key(&row.core_meta_key)
    else {
        return false;
    };
    let local_tuples = [
        anvil_core::core_store::core_meta_tuple_key(&[
            anvil_core::core_store::CoreMetaTuplePart::Raw(b"node-signing-keypair"),
        ]),
        anvil_core::core_store::core_meta_tuple_key(&[
            anvil_core::core_store::CoreMetaTuplePart::Utf8("cluster-identity"),
            anvil_core::core_store::CoreMetaTuplePart::Utf8("local"),
        ]),
    ];
    local_tuples
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .any(|local_tuple| tuple_key == local_tuple.as_slice())
}

pub(super) fn is_local_derived_coremeta_row(
    row: &anvil_core::core_store::CoreMetaEncodedOwnedRow,
) -> bool {
    // Runtime fences, leases, and task state are node-local coordination rows.
    // Sharing them across test nodes makes one node inherit another node's
    // active ownership and masks the distributed handoff semantics under test.
    row.cf == anvil_core::core_store::CF_LEASES_FENCES
        || matches!(
            (row.cf.as_str(), coremeta_table_id(row)),
            (
                anvil_core::core_store::CF_MATERIALISATION,
                Some(anvil_core::core_store::TABLE_MATERIALISATION_CURSOR_ROW)
            ) | (
                anvil_core::core_store::CF_MATERIALISATION,
                Some(anvil_core::core_store::TABLE_WRITER_SEGMENT_ROW)
            )
        )
}

pub(super) fn contains_local_corestore_locator(
    row: &anvil_core::core_store::CoreMetaEncodedOwnedRow,
) -> bool {
    if row.cf == anvil_core::core_store::CF_ROOT_CACHE {
        return false;
    }
    row.value_envelope
        .windows(b"local-node".len())
        .any(|window| window == b"local-node")
}

pub(super) fn coremeta_table_id(
    row: &anvil_core::core_store::CoreMetaEncodedOwnedRow,
) -> Option<u16> {
    if row.core_meta_key.len() < 3 {
        return None;
    }
    Some(u16::from_le_bytes([
        row.core_meta_key[1],
        row.core_meta_key[2],
    ]))
}
