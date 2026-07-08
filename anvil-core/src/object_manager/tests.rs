use super::*;

fn boundary_schema() -> CoreBoundarySchema {
    CoreBoundarySchema {
        schema: crate::core_store::CORE_BOUNDARY_SCHEMA_SCHEMA.to_string(),
        bucket: "docs".to_string(),
        generation: 3,
        dimensions: vec![
            crate::core_store::CoreBoundaryDimension {
                name: "customer_tenant".to_string(),
                source: CoreBoundarySource::UserMetadataJsonPointer {
                    pointer: "/customer_tenant_id".to_string(),
                },
                value_type: "uuid".to_string(),
                categories: vec![
                    "security_realm".to_string(),
                    "storage_partition".to_string(),
                ],
                required: true,
                cardinality: "extreme".to_string(),
                max_values_per_block: 1,
                placement_affinity: "prefer_colocate".to_string(),
                compaction_scope: "require_same_value".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            },
            crate::core_store::CoreBoundaryDimension {
                name: "project".to_string(),
                source: CoreBoundarySource::PathTemplate {
                    template: "/customers/{customer_tenant}/projects/{project}/**".to_string(),
                },
                value_type: "string".to_string(),
                categories: vec!["query_prune".to_string()],
                required: true,
                cardinality: "high".to_string(),
                max_values_per_block: 8,
                placement_affinity: "prefer_colocate".to_string(),
                compaction_scope: "prefer_same_value".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            },
            crate::core_store::CoreBoundaryDimension {
                name: "document_day".to_string(),
                source: CoreBoundarySource::BodyJsonPointer {
                    pointer: "/document/day".to_string(),
                    max_body_bytes: 1024,
                },
                value_type: "date".to_string(),
                categories: vec!["retention_group".to_string()],
                required: false,
                cardinality: "medium".to_string(),
                max_values_per_block: 32,
                placement_affinity: "none".to_string(),
                compaction_scope: "none".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            },
        ],
        created_at: "2026-01-01T00:00:00Z".to_string(),
    }
}

#[test]
fn object_boundary_extraction_reads_metadata_path_and_body() {
    let values = extract_object_boundary_values(
        &boundary_schema(),
        "customers/8e4b4477-99d8-4f4b-89db-876d2c7f0c6a/projects/alpha/docs/a.json",
        Some("application/json"),
        Some(&serde_json::json!({
            "customer_tenant_id": "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a"
        })),
        br#"{"document":{"day":"2026-07-07"}}"#,
    )
    .unwrap();

    assert_eq!(values.len(), 3);
    assert_eq!(values[0].schema_generation, 3);
    assert_eq!(values[0].name, "customer_tenant");
    assert_eq!(values[0].value, "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a");
    assert_eq!(values[0].source_kind, "user_metadata_json_pointer");
    assert_eq!(values[1].name, "project");
    assert_eq!(values[1].value, "alpha");
    assert_eq!(values[1].source_kind, "path_template");
    assert_eq!(values[2].name, "document_day");
    assert_eq!(values[2].value, "2026-07-07");
    assert_eq!(values[2].source_kind, "body_json_pointer");
}

#[test]
fn object_boundary_extraction_rejects_missing_required_metadata() {
    let error = extract_object_boundary_values(
        &boundary_schema(),
        "customers/8e4b4477-99d8-4f4b-89db-876d2c7f0c6a/projects/alpha/docs/a.json",
        Some("application/json"),
        Some(&serde_json::json!({})),
        br#"{"document":{"day":"2026-07-07"}}"#,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(AnvilErrorCode::BoundaryRequiredMissing.as_str())
    );
}

#[test]
fn object_boundary_extraction_rejects_non_json_body_source() {
    let error = extract_object_boundary_values(
        &boundary_schema(),
        "customers/8e4b4477-99d8-4f4b-89db-876d2c7f0c6a/projects/alpha/docs/a.json",
        Some("text/plain"),
        Some(&serde_json::json!({
            "customer_tenant_id": "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a"
        })),
        b"plain",
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(AnvilErrorCode::BoundaryExtractorUnsupportedContentType.as_str())
    );
}
