use super::*;

pub(super) fn full_text_definition(
    index: &crate::persistence::IndexDefinition,
) -> Result<FullTextIndexDefinition, Status> {
    let policy = index
        .build_policy
        .get("full_text")
        .unwrap_or(&index.build_policy);
    FullTextIndexDefinition::from_json(policy).map_err(|e| Status::invalid_argument(e.to_string()))
}

pub(super) fn full_text_query_status(error: FullTextQueryError) -> Status {
    match error {
        FullTextQueryError::PositionsDisabled => {
            Status::failed_precondition(AnvilErrorCode::IndexDoesNotSupportQuery.as_str())
        }
        FullTextQueryError::EmptyPhrase => Status::invalid_argument("query_text is required"),
    }
}

pub(super) fn parse_json_field(name: &str, value: &str) -> Result<JsonValue, Status> {
    serde_json::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid {name}: {e}")))
}

pub(super) fn validate_index_name(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(Status::invalid_argument("Invalid index name"));
    }
    Ok(())
}

pub(super) fn concrete_index_kind(value: i32) -> Result<&'static str, Status> {
    let kind =
        IndexKind::try_from(value).map_err(|_| Status::invalid_argument("Invalid index kind"))?;
    match kind {
        IndexKind::Unspecified => Err(Status::invalid_argument("index kind is required")),
        IndexKind::Path => Ok("path"),
        IndexKind::MetadataFilter => Ok("metadata_filter"),
        IndexKind::FullText => Ok("full_text"),
        IndexKind::Vector => Ok("vector"),
        IndexKind::Hybrid => Ok("hybrid"),
        IndexKind::PersonaldbRowMetadata => Ok("personaldb_row_metadata"),
        IndexKind::GitSource => Ok("git_source"),
        IndexKind::TypedJson => Ok("typed_json"),
    }
}

pub(crate) fn index_kind_value_from_str(value: &str) -> Result<i32, Status> {
    Ok(match value {
        "path" => IndexKind::Path,
        "metadata_filter" => IndexKind::MetadataFilter,
        "full_text" => IndexKind::FullText,
        "vector" => IndexKind::Vector,
        "hybrid" => IndexKind::Hybrid,
        "personaldb_row_metadata" => IndexKind::PersonaldbRowMetadata,
        "git_source" => IndexKind::GitSource,
        "typed_json" => IndexKind::TypedJson,
        _ => return Err(Status::internal("Invalid stored index kind")),
    } as i32)
}

pub(super) fn validate_authorization_mode(value: &str) -> Result<(), Status> {
    match value {
        "inherit_object" | "index_only" | "public" => Ok(()),
        _ => Err(Status::invalid_argument("Invalid authorization_mode")),
    }
}

pub(super) fn validate_index_definition_shape(
    kind: &str,
    build_policy: &JsonValue,
    extractor: &JsonValue,
    config: &Config,
) -> Result<(), Status> {
    match kind {
        "full_text" => {
            crate::formats::full_text::FullTextIndexDefinition::from_json(build_policy)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        "vector" => {
            let definition = crate::formats::vector::VectorIndexDefinition::from_json(build_policy)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            validate_no_external_vector_extractor(extractor)?;
            validate_vector_embedding_provider(&definition, config)?;
        }
        "hybrid" => {
            let full_text = build_policy.get("full_text").ok_or_else(|| {
                Status::invalid_argument("Hybrid index requires full_text policy")
            })?;
            let vector = build_policy
                .get("vector")
                .ok_or_else(|| Status::invalid_argument("Hybrid index requires vector policy"))?;
            crate::formats::full_text::FullTextIndexDefinition::from_json(full_text)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let vector_definition =
                crate::formats::vector::VectorIndexDefinition::from_json(vector)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
            validate_vector_embedding_provider(&vector_definition, config)?;
        }
        "typed_json" => {
            let index = crate::persistence::IndexDefinition {
                id: 0,
                tenant_id: 0,
                bucket_id: 0,
                name: "validation".to_string(),
                kind: "typed_json".to_string(),
                selector: JsonValue::Null,
                extractor: JsonValue::Null,
                authorization_mode: "inherit_object".to_string(),
                build_policy: build_policy.clone(),
                enabled: true,
                version: 1,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            TypedJsonIndexDefinition::from_index(&index)?;
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn validate_no_external_vector_extractor(extractor: &JsonValue) -> Result<(), Status> {
    if extractor.is_null() {
        return Ok(());
    }
    if extractor.as_object().is_some_and(serde_json::Map::is_empty) {
        return Ok(());
    }
    Err(Status::invalid_argument(
        "Vector index extractor must be declared inside build_policy_json",
    ))
}

pub(super) fn validate_vector_embedding_provider(
    definition: &crate::formats::vector::VectorIndexDefinition,
    config: &Config,
) -> Result<(), Status> {
    if definition.embedding_provider == crate::embedding_provider::TEST_ONLY_EMBEDDING_PROVIDER
        && !config.allow_test_only_embedding_provider
    {
        return Err(Status::invalid_argument(
            "test_only embedding provider is disabled for this server",
        ));
    }
    let extractor_kind = definition
        .extractor
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or("object_body_utf8");
    if matches!(extractor_kind, "object_body_utf8" | "utf8" | "body")
        && definition.embedding_provider != crate::embedding_provider::TEST_ONLY_EMBEDDING_PROVIDER
    {
        let providers = crate::embedding_provider::EmbeddingProviderRegistry::from_config(config)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if !providers.has_provider(&definition.embedding_provider) {
            return Err(Status::invalid_argument(format!(
                "embedding provider `{}` is not configured",
                definition.embedding_provider
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_diagnostic_severity(value: &str) -> Result<(), Status> {
    match value {
        "info" | "warning" | "error" => Ok(()),
        _ => Err(Status::invalid_argument("Invalid diagnostic severity")),
    }
}

pub(super) fn query_limit(value: u32) -> usize {
    match value {
        0 => 10,
        other => other.min(1000) as usize,
    }
}

pub(super) fn internal_candidate_limit_for_request(
    req: &QueryIndexRequest,
    authorization_mode: &str,
) -> usize {
    let limit = query_limit(req.limit);
    let has_non_authorization_filters = !req.path_prefix.trim().is_empty()
        || !req.metadata_filters_json.trim().is_empty()
        || !req.typed_predicates_json.trim().is_empty();
    if authorization_mode == "inherit_object" || has_non_authorization_filters {
        limit.saturating_mul(20)
    } else {
        limit
    }
}

pub(super) fn score_index_candidate_limit(base_limit: usize, available_candidates: u64) -> usize {
    let base_limit = base_limit.max(1);
    let _ = available_candidates;
    base_limit
}

pub(super) fn index_resource(bucket_name: &str, index_name: &str) -> String {
    format!("{}/{}", bucket_name, index_name)
}

pub(super) fn validate_hex32(value: &str, field: &'static str) -> Result<(), Status> {
    if value.len() == 64 && value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!("{field} must be hex32")))
    }
}

pub(super) fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

pub(super) fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}

pub(super) fn index_record(
    bucket_name: &str,
    index: crate::persistence::IndexDefinition,
) -> Result<IndexDefinitionRecord, Status> {
    Ok(IndexDefinitionRecord {
        index_id: u64::try_from(index.id).map_err(|_| Status::internal("Invalid index id"))?,
        bucket_name: bucket_name.to_string(),
        name: index.name,
        kind: index_kind_value_from_str(&index.kind)?,
        selector_json: index.selector.to_string(),
        extractor_json: index.extractor.to_string(),
        authorization_mode: index.authorization_mode,
        build_policy_json: index.build_policy.to_string(),
        enabled: index.enabled,
        version: u64::try_from(index.version).map_err(|_| Status::internal("Invalid version"))?,
        created_at: index.created_at.to_string(),
        updated_at: index.updated_at.to_string(),
    })
}

pub(super) fn index_partition_event_response(
    bucket_name: &str,
    index_name: &str,
    index_storage_id: &str,
    partition_id: &str,
    event: index_partition_watch::IndexPartitionWatchEvent,
) -> Result<WatchIndexPartitionResponse, Status> {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    let (source_cursor_low, source_cursor_high) = split_u128(event.payload.source_cursor);
    let payload = event.payload;
    let index_kind = index_kind_value_from_str(&payload.index_kind)?;
    let emitted_at = payload.emitted_at.clone();
    let generation = payload.generation;
    let payload_hash = watch_envelope::payload_hash(&payload);
    Ok(WatchIndexPartitionResponse {
        cursor_low,
        cursor_high,
        bucket_name: bucket_name.to_string(),
        index_name: index_name.to_string(),
        index_storage_id: index_storage_id.to_string(),
        partition_id: partition_id.to_string(),
        event_type: payload.event_type,
        index_kind,
        generation,
        source_cursor_low,
        source_cursor_high,
        source_manifest_hash: payload.source_manifest_hash,
        proof_hash: payload.proof_hash,
        segment_hashes: payload.segment_hashes,
        authz_revision: event.authz_revision,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "index_partition",
            partition_family: "index_partition",
            partition_id: partition_id.to_string(),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "index_partition".to_string(),
            object_ref: format!("{bucket_name}/{index_name}/{partition_id}"),
            authz_revision: event.authz_revision,
            index_generation: generation,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    })
}

pub(super) fn index_definition_event_response(
    event: &crate::persistence::IndexDefinitionEvent,
) -> Result<WatchIndexDefinitionResponse, Status> {
    let cursor = u64::try_from(event.id).map_err(|_| Status::internal("Invalid watch cursor"))?;
    let emitted_at = event.created_at.to_string();
    let payload_hash = watch_envelope::payload_hash(&event.definition);
    Ok(WatchIndexDefinitionResponse {
        cursor,
        event_type: event.event_type.clone(),
        index: Some(index_record_from_event(event)?),
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "index_definition",
            partition_family: "index_definition",
            partition_id: event.bucket_id.to_string(),
            cursor: event.id as u128,
            mutation_id: event.mutation_id.to_string(),
            record_kind: "index_definition".to_string(),
            object_ref: format!("{}/{}", event.bucket_name, event.index_name),
            authz_revision: 0,
            index_generation: event.index_version as u64,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    })
}

pub(super) fn index_record_from_event(
    event: &crate::persistence::IndexDefinitionEvent,
) -> Result<IndexDefinitionRecord, Status> {
    let definition = &event.definition;
    Ok(IndexDefinitionRecord {
        index_id: u64::try_from(event.index_id)
            .map_err(|_| Status::internal("Invalid index id"))?,
        bucket_name: event.bucket_name.clone(),
        name: event.index_name.clone(),
        kind: index_kind_value_from_str(&json_string_field(definition, "kind")?)?,
        selector_json: json_string_field(definition, "selector_json")?,
        extractor_json: json_string_field(definition, "extractor_json")?,
        authorization_mode: json_string_field(definition, "authorization_mode")?,
        build_policy_json: json_string_field(definition, "build_policy_json")?,
        enabled: definition
            .get("enabled")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| Status::internal("Malformed index definition event"))?,
        version: u64::try_from(event.index_version)
            .map_err(|_| Status::internal("Invalid index version"))?,
        created_at: json_string_field(definition, "created_at")?,
        updated_at: json_string_field(definition, "updated_at")?,
    })
}

pub(super) fn json_string_field(value: &JsonValue, name: &str) -> Result<String, Status> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| Status::internal("Malformed index definition event"))
}

pub(super) fn index_diagnostic_record(
    diagnostic: crate::persistence::IndexDiagnostic,
) -> Result<IndexDiagnosticRecord, Status> {
    Ok(IndexDiagnosticRecord {
        cursor: u64::try_from(diagnostic.id)
            .map_err(|_| Status::internal("Invalid diagnostic cursor"))?,
        bucket_name: diagnostic.bucket_name,
        index_name: diagnostic.index_name,
        object_key: diagnostic.object_key,
        version_id: diagnostic
            .version_id
            .map(|version_id| version_id.to_string())
            .unwrap_or_default(),
        severity: diagnostic.severity,
        code: diagnostic.code,
        message: diagnostic.message,
        details_json: diagnostic.details.to_string(),
        created_at: diagnostic.created_at.to_string(),
    })
}
