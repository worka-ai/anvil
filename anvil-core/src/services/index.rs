use crate::anvil_api::index_service_server::IndexService;
use crate::anvil_api::*;
use crate::{
    AppState, auth, authz_journal, bucket_journal,
    formats::{
        full_text::{Bm25Config, FullTextIndexDefinition},
        vector::VectorMetric,
    },
    full_text_segment, index_journal,
    permissions::AnvilAction,
    search_query, validation, vector_segment,
};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl IndexService for AppState {
    type WatchIndexDefinitionStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchIndexDefinitionResponse, Status>> + Send>,
    >;

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        validate_index_kind(&req.kind)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        if !auth::is_authorized(AnvilAction::IndexCreate, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let selector = parse_json_field("selector_json", &req.selector_json)?;
        let extractor = parse_json_field("extractor_json", &req.extractor_json)?;
        let build_policy = parse_json_field("build_policy_json", &req.build_policy_json)?;
        validate_authorization_mode(&req.authorization_mode)?;
        validate_index_definition_shape(&req.kind, &build_policy)?;

        let index = self
            .db
            .create_index_definition(
                claims.tenant_id,
                bucket.id,
                &req.name,
                &req.kind,
                selector,
                extractor,
                &req.authorization_mode,
                build_policy,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        self.publish_index_definition_event(&bucket, &index, "create")
            .await?;

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn update_index(
        &self,
        request: Request<UpdateIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        if !auth::is_authorized(AnvilAction::IndexUpdate, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let selector = parse_json_field("selector_json", &req.selector_json)?;
        let extractor = parse_json_field("extractor_json", &req.extractor_json)?;
        let build_policy = parse_json_field("build_policy_json", &req.build_policy_json)?;
        validate_authorization_mode(&req.authorization_mode)?;
        let existing = self
            .db
            .get_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        validate_index_definition_shape(&existing.kind, &build_policy)?;

        let index = self
            .db
            .update_index_definition(
                claims.tenant_id,
                bucket.id,
                &req.name,
                selector,
                extractor,
                &req.authorization_mode,
                build_policy,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        self.publish_index_definition_event(&bucket, &index, "update")
            .await?;

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn disable_index(
        &self,
        request: Request<DisableIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        if !auth::is_authorized(AnvilAction::IndexUpdate, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .db
            .disable_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        self.publish_index_definition_event(&bucket, &index, "disable")
            .await?;

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        if !auth::is_authorized(AnvilAction::IndexDelete, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .db
            .drop_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        self.publish_index_definition_event(&bucket, &index, "drop")
            .await?;
        Ok(Response::new(DropIndexResponse {}))
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !auth::is_authorized(AnvilAction::IndexRead, &req.bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let indexes = index_journal::read_current_index_definition_events(
            &self.storage,
            claims.tenant_id,
            bucket.id,
            req.include_disabled,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .into_iter()
        .map(|event| index_record_from_event(&event))
        .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListIndexesResponse { indexes }))
    }

    async fn query_index(
        &self,
        request: Request<QueryIndexRequest>,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.index_name)?;
        if !auth::is_authorized(AnvilAction::IndexRead, &req.bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .db
            .get_index_definition(claims.tenant_id, bucket.id, &req.index_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|index| index.enabled)
            .ok_or_else(|| Status::not_found("Index definition not found"))?;

        match index.kind.as_str() {
            "full_text" => {
                self.query_full_text_index(&claims, &bucket, &index, req)
                    .await
            }
            "vector" => self.query_vector_index(&claims, &bucket, &index, req).await,
            "hybrid" => Err(Status::failed_precondition("IndexUnavailable")),
            _ => Err(Status::failed_precondition("IndexDoesNotSupportQuery")),
        }
    }

    async fn watch_index_definition(
        &self,
        request: Request<WatchIndexDefinitionRequest>,
    ) -> Result<Response<Self::WatchIndexDefinitionStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !auth::is_authorized(AnvilAction::IndexWatch, &req.bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let snapshot = index_journal::read_index_definition_events(
            &self.storage,
            claims.tenant_id,
            bucket.id,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut live = self.index_watch_tx.subscribe();

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.id);
                if tx
                    .send(index_definition_event_response(&event))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            loop {
                match live.recv().await {
                    Ok(event) => {
                        if event.tenant_id != claims.tenant_id
                            || event.bucket_id != bucket.id
                            || event.id <= last_cursor
                        {
                            continue;
                        }
                        last_cursor = event.id;
                        if tx
                            .send(index_definition_event_response(&event))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = tx
                            .send(Err(Status::data_loss(
                                "Index definition watch fell behind retained live event window",
                            )))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchIndexDefinitionStream
        ))
    }

    async fn list_index_diagnostics(
        &self,
        request: Request<ListIndexDiagnosticsRequest>,
    ) -> Result<Response<ListIndexDiagnosticsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !req.index_name.is_empty() {
            validate_index_name(&req.index_name)?;
        }
        if !req.severity.is_empty() {
            validate_diagnostic_severity(&req.severity)?;
        }
        if !auth::is_authorized(AnvilAction::IndexRead, &req.bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let limit = i32::try_from(req.limit)
            .map_err(|_| Status::invalid_argument("limit exceeds supported range"))?;
        let diagnostics = self
            .db
            .list_index_diagnostics(
                claims.tenant_id,
                bucket.id,
                &req.index_name,
                &req.severity,
                after_cursor,
                limit,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(index_diagnostic_record)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListIndexDiagnosticsResponse { diagnostics }))
    }
}

impl AppState {
    async fn get_index_bucket(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<crate::persistence::Bucket, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))
    }

    async fn publish_index_definition_event(
        &self,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        event_type: &str,
    ) -> Result<crate::persistence::IndexDefinitionEvent, Status> {
        let event =
            index_journal::write_index_definition_event(&self.storage, bucket, index, event_type)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.index_watch_tx.send(event.clone());
        Ok(event)
    }

    async fn query_full_text_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if req.query_text.trim().is_empty() {
            return Err(Status::invalid_argument("query_text is required"));
        }
        if !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_vector is not valid for full_text indexes",
            ));
        }
        let definition = FullTextIndexDefinition::from_json(&index.build_policy)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let Some(segment) =
            full_text_segment::read_latest_full_text_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        else {
            return Err(Status::failed_precondition("IndexUnavailable"));
        };
        let search_hits = search_query::query_full_text_segment(
            &segment,
            search_query::FullTextSegmentQuery {
                query: &req.query_text,
                tokenizer: &definition.tokenizer,
                positions_enabled: definition.positions_enabled,
                phrase: req.phrase,
                bm25: Bm25Config::default(),
                authorized_labels: None,
                limit: internal_candidate_limit(req.limit, &index.authorization_mode),
            },
        )
        .map_err(|e| Status::failed_precondition(format!("{e:?}")))?;
        let requested_limit = query_limit(req.limit);
        let mut hits = Vec::with_capacity(search_hits.len().min(requested_limit));
        for hit in search_hits {
            let (object_version_id, object_key) = match self
                .object_ref_for_query_hit(bucket.id, hit.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => (String::new(), String::new()),
            };
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_key,
                    segment.header.authz_revision,
                )
                .await?
            {
                continue;
            }
            hits.push(IndexQueryHit {
                kind: "full_text".to_string(),
                score: hit.score,
                object_key,
                object_version_id,
                document_id: hit.document_id,
                field_id: u32::from(hit.field_id),
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "matched_terms": hit.matched_terms,
                    "authz_label_hash": hex::encode(hit.authz_label_hash),
                })
                .to_string(),
            });
            if hits.len() >= requested_limit {
                break;
            }
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind: index.kind.clone(),
            index_generation: segment.header.generation,
            authz_revision: segment.header.authz_revision,
            scoring_recipe_json: serde_json::json!({"kind": "bm25", "k1": 1.2, "b": 0.75})
                .to_string(),
        }))
    }

    async fn query_vector_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.is_empty() {
            return Err(Status::invalid_argument(
                "query_text is not valid for vector indexes",
            ));
        }
        if req.query_vector.is_empty() {
            return Err(Status::invalid_argument("query_vector is required"));
        }
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let Some(segment) =
            vector_segment::read_latest_vector_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        else {
            return Err(Status::failed_precondition("IndexUnavailable"));
        };
        if req.query_vector.len() != usize::from(segment.header.dimension) {
            return Err(Status::invalid_argument("query_vector dimension mismatch"));
        }
        let metric = VectorMetric::from_name(&segment.header.metric)
            .map_err(|e| Status::internal(e.to_string()))?;
        let search_hits = search_query::query_vector_segment(
            &segment,
            &req.query_vector,
            metric,
            None,
            internal_candidate_limit(req.limit, &index.authorization_mode),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        let requested_limit = query_limit(req.limit);
        let mut hits = Vec::with_capacity(search_hits.len().min(requested_limit));
        for hit in search_hits {
            let (object_version_id, object_key) = match self
                .object_ref_for_query_hit(bucket.id, hit.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => (String::new(), String::new()),
            };
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_key,
                    segment.header.authz_revision,
                )
                .await?
            {
                continue;
            }
            hits.push(IndexQueryHit {
                kind: "vector".to_string(),
                score: hit.score,
                object_key,
                object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: hit.vector_id,
                chunk_id: hit.chunk_id,
                source_start: hit.source_start,
                source_len: hit.source_len,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "metric": segment.header.metric,
                    "modality": segment.header.modality,
                })
                .to_string(),
            });
            if hits.len() >= requested_limit {
                break;
            }
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind: index.kind.clone(),
            index_generation: segment.header.generation,
            authz_revision: segment.header.authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "vector",
                "metric": segment.header.metric,
                "max_candidate_multiplier": 20
            })
            .to_string(),
        }))
    }

    async fn object_ref_for_query_hit(
        &self,
        bucket_id: i64,
        version_bytes: [u8; 16],
    ) -> Result<Option<(String, String)>, Status> {
        let version_id = uuid::Uuid::from_bytes(version_bytes);
        let object = self
            .db
            .get_object_version_by_id(bucket_id, version_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(object.map(|object| (version_id.to_string(), object.key)))
    }

    async fn query_hit_visible(
        &self,
        claims: &auth::Claims,
        authorization_mode: &str,
        bucket_name: &str,
        object_key: &str,
        authz_revision: u64,
    ) -> Result<bool, Status> {
        match authorization_mode {
            "inherit_object" => {
                if object_key.is_empty() {
                    return Ok(false);
                }
                let object_resource = format!("{bucket_name}/{object_key}");
                if auth::is_authorized(AnvilAction::ObjectRead, &object_resource, &claims.scopes) {
                    return Ok(true);
                }
                let revision = i64::try_from(authz_revision)
                    .map_err(|_| Status::internal("Invalid authz revision"))?;
                let record = authz_journal::check_authz_tuple_at_revision(
                    &self.storage,
                    claims.tenant_id,
                    "object",
                    &object_resource,
                    "reader",
                    "app",
                    &claims.sub,
                    "",
                    revision,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
                Ok(record.is_some_and(|record| record.operation == "add"))
            }
            "index_only" | "public" => Ok(true),
            _ => Ok(false),
        }
    }
}

fn parse_json_field(name: &str, value: &str) -> Result<JsonValue, Status> {
    serde_json::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid {name}: {e}")))
}

fn validate_index_name(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(Status::invalid_argument("Invalid index name"));
    }
    Ok(())
}

fn validate_index_kind(value: &str) -> Result<(), Status> {
    match value {
        "path"
        | "metadata_filter"
        | "full_text"
        | "vector"
        | "hybrid"
        | "personaldb_row_metadata"
        | "git_source" => Ok(()),
        _ => Err(Status::invalid_argument("Invalid index kind")),
    }
}

fn validate_authorization_mode(value: &str) -> Result<(), Status> {
    match value {
        "inherit_object" | "index_only" | "public" => Ok(()),
        _ => Err(Status::invalid_argument("Invalid authorization_mode")),
    }
}

fn validate_index_definition_shape(kind: &str, build_policy: &JsonValue) -> Result<(), Status> {
    match kind {
        "full_text" => {
            crate::formats::full_text::FullTextIndexDefinition::from_json(build_policy)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        "vector" => {
            crate::formats::vector::VectorIndexDefinition::from_json(build_policy)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_diagnostic_severity(value: &str) -> Result<(), Status> {
    match value {
        "info" | "warning" | "error" => Ok(()),
        _ => Err(Status::invalid_argument("Invalid diagnostic severity")),
    }
}

fn query_limit(value: u32) -> usize {
    match value {
        0 => 10,
        other => other.min(1000) as usize,
    }
}

fn internal_candidate_limit(value: u32, authorization_mode: &str) -> usize {
    let limit = query_limit(value);
    if authorization_mode == "inherit_object" {
        limit.saturating_mul(20)
    } else {
        limit
    }
}

fn index_resource(bucket_name: &str, index_name: &str) -> String {
    format!("{}/{}", bucket_name, index_name)
}

fn index_record(
    bucket_name: &str,
    index: crate::persistence::IndexDefinition,
) -> Result<IndexDefinitionRecord, Status> {
    Ok(IndexDefinitionRecord {
        index_id: u64::try_from(index.id).map_err(|_| Status::internal("Invalid index id"))?,
        bucket_name: bucket_name.to_string(),
        name: index.name,
        kind: index.kind,
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

fn index_definition_event_response(
    event: &crate::persistence::IndexDefinitionEvent,
) -> Result<WatchIndexDefinitionResponse, Status> {
    Ok(WatchIndexDefinitionResponse {
        cursor: u64::try_from(event.id).map_err(|_| Status::internal("Invalid watch cursor"))?,
        event_type: event.event_type.clone(),
        index: Some(index_record_from_event(event)?),
        emitted_at: event.created_at.to_string(),
    })
}

fn index_record_from_event(
    event: &crate::persistence::IndexDefinitionEvent,
) -> Result<IndexDefinitionRecord, Status> {
    let definition = &event.definition;
    Ok(IndexDefinitionRecord {
        index_id: u64::try_from(event.index_id)
            .map_err(|_| Status::internal("Invalid index id"))?,
        bucket_name: event.bucket_name.clone(),
        name: event.index_name.clone(),
        kind: json_string_field(definition, "kind")?,
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

fn json_string_field(value: &JsonValue, name: &str) -> Result<String, Status> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| Status::internal("Malformed index definition event"))
}

fn index_diagnostic_record(
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
