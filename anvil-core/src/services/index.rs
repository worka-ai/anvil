use crate::anvil_api::index_service_server::IndexService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control, auth, bucket_journal,
    error_codes::AnvilErrorCode,
    formats::{
        full_text::{Bm25Config, FullTextIndexDefinition, FullTextQueryError},
        vector::VectorMetric,
    },
    full_text_segment, index_journal, index_partition_watch,
    permissions::AnvilAction,
    search_query, validation, vector_segment,
};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl IndexService for AppState {
    type WatchIndexDefinitionStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchIndexDefinitionResponse, Status>> + Send>,
    >;
    type WatchIndexPartitionStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchIndexPartitionResponse, Status>> + Send>,
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
            .persistence
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
        self.persistence
            .enqueue_index_build_for_index(&bucket, &index)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

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
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        validate_index_definition_shape(&existing.kind, &build_policy)?;

        let index = self
            .persistence
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
        self.persistence
            .enqueue_index_build_for_index(&bucket, &index)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

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
            .persistence
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
            .persistence
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
            .persistence
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
            "hybrid" => self.query_hybrid_index(&claims, &bucket, &index, req).await,
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

    async fn watch_index_partition(
        &self,
        request: Request<WatchIndexPartitionRequest>,
    ) -> Result<Response<Self::WatchIndexPartitionStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.index_name)?;
        if !auth::is_authorized(AnvilAction::IndexWatch, &req.bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.index_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|index| index.enabled)
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let partition_id = if req.partition_id.trim().is_empty() {
            hex::encode(crate::formats::hash32(index_storage_id.as_bytes()))
        } else {
            validate_hex32(&req.partition_id, "partition_id")?;
            req.partition_id
        };
        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let snapshot = index_partition_watch::list_index_partition_watch_events(
            &self.storage,
            claims.tenant_id,
            bucket.id,
            &index_storage_id,
            &partition_id,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let storage = self.storage.clone();
        let bucket_name = bucket.name.clone();
        let index_name = index.name.clone();
        let tenant_id = claims.tenant_id;
        let bucket_id = bucket.id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.cursor);
                if tx
                    .send(Ok(index_partition_event_response(
                        &bucket_name,
                        &index_name,
                        &index_storage_id,
                        &partition_id,
                        event,
                    )))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                let events = match index_partition_watch::list_index_partition_watch_events(
                    &storage,
                    tenant_id,
                    bucket_id,
                    &index_storage_id,
                    &partition_id,
                    last_cursor,
                    1000,
                )
                .await
                {
                    Ok(events) => events,
                    Err(error) => {
                        let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                        return;
                    }
                };
                for event in events {
                    last_cursor = last_cursor.max(event.cursor);
                    if tx
                        .send(Ok(index_partition_event_response(
                            &bucket_name,
                            &index_name,
                            &index_storage_id,
                            &partition_id,
                            event,
                        )))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchIndexPartitionStream
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
            .persistence
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
        let event = self
            .persistence
            .create_index_definition_event(
                bucket.tenant_id,
                bucket.id,
                &bucket.name,
                index,
                event_type,
            )
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
        let definition = full_text_definition(index)?;
        let filters = QueryFilters::from_request(&req)?;
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
                limit: internal_candidate_limit_for_request(&req, &index.authorization_mode),
            },
        )
        .map_err(full_text_query_status)?;
        let requested_limit = query_limit(req.limit);
        let mut hits = Vec::with_capacity(search_hits.len().min(requested_limit));
        for hit in search_hits {
            let object_ref = match self
                .object_ref_for_query_hit(bucket.id, hit.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => QueryObjectRef::default(),
            };
            if !filters.matches(&object_ref)? {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    segment.header.authz_revision,
                )
                .await?
            {
                continue;
            }
            hits.push(IndexQueryHit {
                kind: "full_text".to_string(),
                score: hit.score,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
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

    async fn query_hybrid_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if req.query_text.trim().is_empty() && req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_text or query_vector is required for hybrid indexes",
            ));
        }

        let requested_limit = query_limit(req.limit);
        let internal_limit = internal_candidate_limit_for_request(&req, &index.authorization_mode);
        let filters = QueryFilters::from_request(&req)?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let mut combined = BTreeMap::<[u8; 16], HybridAccum>::new();
        let mut generation = 0;
        let mut authz_revision = 0;
        let has_text = !req.query_text.trim().is_empty();
        let has_vector = !req.query_vector.is_empty();

        if has_text {
            let definition = full_text_definition(index)?;
            let Some(segment) =
                full_text_segment::read_latest_full_text_segment(&self.storage, &index_storage_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            else {
                return Err(Status::failed_precondition("IndexUnavailable"));
            };
            generation = generation.max(segment.header.generation);
            authz_revision = authz_revision.max(segment.header.authz_revision);
            let search_hits = search_query::query_full_text_segment(
                &segment,
                search_query::FullTextSegmentQuery {
                    query: &req.query_text,
                    tokenizer: &definition.tokenizer,
                    positions_enabled: definition.positions_enabled,
                    phrase: req.phrase,
                    bm25: Bm25Config::default(),
                    authorized_labels: None,
                    limit: internal_limit,
                },
            )
            .map_err(full_text_query_status)?;
            for hit in search_hits {
                let entry = combined
                    .entry(hit.object_version_id)
                    .or_insert_with(|| HybridAccum::new(hit.object_version_id));
                entry.text_score += hit.score;
                entry.document_id = hit.document_id;
                entry.field_id = u32::from(hit.field_id);
            }
        }

        if has_vector {
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
            generation = generation.max(segment.header.generation);
            authz_revision = authz_revision.max(segment.header.authz_revision);
            let metric = VectorMetric::from_name(&segment.header.metric)
                .map_err(|e| Status::internal(e.to_string()))?;
            let search_hits = search_query::query_vector_segment(
                &segment,
                &req.query_vector,
                metric,
                None,
                internal_limit,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            for hit in search_hits {
                let entry = combined
                    .entry(hit.object_version_id)
                    .or_insert_with(|| HybridAccum::new(hit.object_version_id));
                entry.vector_score = entry.vector_score.max(hit.score);
                entry.vector_id = hit.vector_id;
                entry.chunk_id = hit.chunk_id;
                entry.source_start = hit.source_start;
                entry.source_len = hit.source_len;
            }
        }

        let (text_weight, vector_weight) = match (has_text, has_vector) {
            (true, true) => (0.55, 0.35),
            (true, false) => (1.0, 0.0),
            (false, true) => (0.0, 1.0),
            (false, false) => unreachable!("validated above"),
        };
        let mut ranked = combined.into_values().collect::<Vec<_>>();
        for item in &mut ranked {
            item.score = item
                .text_score
                .mul_add(text_weight, item.vector_score * vector_weight);
        }
        ranked.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.object_version_id.cmp(&right.object_version_id))
        });

        let mut hits = Vec::with_capacity(ranked.len().min(requested_limit));
        for item in ranked {
            let object_ref = match self
                .object_ref_for_query_hit(bucket.id, item.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => QueryObjectRef::default(),
            };
            if !filters.matches(&object_ref)? {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    authz_revision,
                )
                .await?
            {
                continue;
            }
            hits.push(IndexQueryHit {
                kind: "hybrid".to_string(),
                score: item.score,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
                document_id: item.document_id,
                field_id: item.field_id,
                vector_id: item.vector_id,
                chunk_id: item.chunk_id,
                source_start: item.source_start,
                source_len: item.source_len,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "text_score": item.text_score,
                    "vector_score": item.vector_score,
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
            index_generation: generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "hybrid",
                "text_weight": text_weight,
                "vector_weight": vector_weight,
                "freshness_weight": if has_text && has_vector { 0.10 } else { 0.0 }
            })
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
        let filters = QueryFilters::from_request(&req)?;
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
            internal_candidate_limit_for_request(&req, &index.authorization_mode),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        let requested_limit = query_limit(req.limit);
        let mut hits = Vec::with_capacity(search_hits.len().min(requested_limit));
        for hit in search_hits {
            let object_ref = match self
                .object_ref_for_query_hit(bucket.id, hit.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => QueryObjectRef::default(),
            };
            if !filters.matches(&object_ref)? {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    segment.header.authz_revision,
                )
                .await?
            {
                continue;
            }
            hits.push(IndexQueryHit {
                kind: "vector".to_string(),
                score: hit.score,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
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
    ) -> Result<Option<QueryObjectRef>, Status> {
        let version_id = uuid::Uuid::from_bytes(version_bytes);
        let object = self
            .persistence
            .get_object_version_by_id(bucket_id, version_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(object.map(|object| QueryObjectRef {
            object_version_id: version_id.to_string(),
            object_key: object.key,
            user_meta: object.user_meta,
        }))
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
                access_control::relationship_allows(
                    &self.storage,
                    claims,
                    "object",
                    &object_resource,
                    "reader",
                    Some(revision),
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))
            }
            "index_only" | "public" => Ok(true),
            _ => Ok(false),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct QueryObjectRef {
    object_version_id: String,
    object_key: String,
    user_meta: Option<JsonValue>,
}

#[derive(Debug, Clone, Default)]
struct QueryFilters {
    path_prefix: Option<String>,
    metadata: Vec<MetadataFilter>,
}

#[derive(Debug, Clone)]
struct MetadataFilter {
    field: String,
    expected: JsonValue,
}

impl QueryFilters {
    fn from_request(req: &QueryIndexRequest) -> Result<Self, Status> {
        let path_prefix = if req.path_prefix.trim().is_empty() {
            None
        } else {
            Some(req.path_prefix.clone())
        };
        let metadata = parse_metadata_filters(&req.metadata_filters_json)?;
        Ok(Self {
            path_prefix,
            metadata,
        })
    }

    fn matches(&self, object_ref: &QueryObjectRef) -> Result<bool, Status> {
        if let Some(path_prefix) = &self.path_prefix
            && !object_ref.object_key.starts_with(path_prefix)
        {
            return Ok(false);
        }
        if self.metadata.is_empty() {
            return Ok(true);
        }
        let Some(metadata) = object_ref.user_meta.as_ref() else {
            return Ok(false);
        };
        for filter in &self.metadata {
            let Some(actual) = metadata_filter_value(metadata, &filter.field) else {
                return Ok(false);
            };
            if actual != &filter.expected {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn parse_metadata_filters(value: &str) -> Result<Vec<MetadataFilter>, Status> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: JsonValue = serde_json::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid metadata_filters_json: {e}")))?;
    if parsed.is_null() {
        return Ok(Vec::new());
    }
    let JsonValue::Object(entries) = parsed else {
        return Err(Status::invalid_argument(
            "metadata_filters_json must be a JSON object",
        ));
    };
    let mut filters = Vec::with_capacity(entries.len());
    for (field, expected) in entries {
        if field.trim().is_empty() {
            return Err(Status::invalid_argument(
                "metadata_filters_json field names must not be empty",
            ));
        }
        filters.push(MetadataFilter { field, expected });
    }
    Ok(filters)
}

fn metadata_filter_value<'a>(metadata: &'a JsonValue, field: &str) -> Option<&'a JsonValue> {
    if field.starts_with('/') {
        metadata.pointer(field)
    } else {
        metadata.get(field)
    }
}

#[derive(Debug, Clone)]
struct HybridAccum {
    object_version_id: [u8; 16],
    text_score: f32,
    vector_score: f32,
    score: f32,
    document_id: u64,
    field_id: u32,
    vector_id: u64,
    chunk_id: u32,
    source_start: u64,
    source_len: u32,
}

impl HybridAccum {
    fn new(object_version_id: [u8; 16]) -> Self {
        Self {
            object_version_id,
            text_score: 0.0,
            vector_score: 0.0,
            score: 0.0,
            document_id: 0,
            field_id: 0,
            vector_id: 0,
            chunk_id: 0,
            source_start: 0,
            source_len: 0,
        }
    }
}

fn full_text_definition(
    index: &crate::persistence::IndexDefinition,
) -> Result<FullTextIndexDefinition, Status> {
    let policy = index
        .build_policy
        .get("full_text")
        .unwrap_or(&index.build_policy);
    FullTextIndexDefinition::from_json(policy).map_err(|e| Status::invalid_argument(e.to_string()))
}

fn full_text_query_status(error: FullTextQueryError) -> Status {
    match error {
        FullTextQueryError::PositionsDisabled => {
            Status::failed_precondition(AnvilErrorCode::IndexDoesNotSupportQuery.as_str())
        }
        FullTextQueryError::EmptyPhrase => Status::invalid_argument("query_text is required"),
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
        "hybrid" => {
            let full_text = build_policy.get("full_text").ok_or_else(|| {
                Status::invalid_argument("Hybrid index requires full_text policy")
            })?;
            let vector = build_policy
                .get("vector")
                .ok_or_else(|| Status::invalid_argument("Hybrid index requires vector policy"))?;
            crate::formats::full_text::FullTextIndexDefinition::from_json(full_text)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            crate::formats::vector::VectorIndexDefinition::from_json(vector)
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

fn internal_candidate_limit_for_request(
    req: &QueryIndexRequest,
    authorization_mode: &str,
) -> usize {
    let limit = query_limit(req.limit);
    let has_non_authorization_filters =
        !req.path_prefix.trim().is_empty() || !req.metadata_filters_json.trim().is_empty();
    if authorization_mode == "inherit_object" || has_non_authorization_filters {
        limit.saturating_mul(20)
    } else {
        limit
    }
}

fn index_resource(bucket_name: &str, index_name: &str) -> String {
    format!("{}/{}", bucket_name, index_name)
}

fn validate_hex32(value: &str, field: &'static str) -> Result<(), Status> {
    if value.len() == 64 && value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!("{field} must be hex32")))
    }
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
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

fn index_partition_event_response(
    bucket_name: &str,
    index_name: &str,
    index_storage_id: &str,
    partition_id: &str,
    event: index_partition_watch::IndexPartitionWatchEvent,
) -> WatchIndexPartitionResponse {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    let (source_cursor_low, source_cursor_high) = split_u128(event.payload.source_cursor);
    WatchIndexPartitionResponse {
        cursor_low,
        cursor_high,
        bucket_name: bucket_name.to_string(),
        index_name: index_name.to_string(),
        index_storage_id: index_storage_id.to_string(),
        partition_id: partition_id.to_string(),
        event_type: event.payload.event_type,
        index_kind: event.payload.index_kind,
        generation: event.payload.generation,
        source_cursor_low,
        source_cursor_high,
        source_manifest_hash: event.payload.source_manifest_hash,
        proof_hash: event.payload.proof_hash,
        segment_hashes: event.payload.segment_hashes,
        authz_revision: event.authz_revision,
        emitted_at: event.payload.emitted_at,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_filters_match_path_prefix_and_metadata() {
        let req = QueryIndexRequest {
            path_prefix: "docs/active/".to_string(),
            metadata_filters_json: serde_json::json!({
                "tenant": "alpha",
                "/nested/state": "open"
            })
            .to_string(),
            ..Default::default()
        };
        let filters = QueryFilters::from_request(&req).unwrap();
        let object_ref = QueryObjectRef {
            object_version_id: "version-1".to_string(),
            object_key: "docs/active/item.json".to_string(),
            user_meta: Some(serde_json::json!({
                "tenant": "alpha",
                "nested": {"state": "open"}
            })),
        };

        assert!(filters.matches(&object_ref).unwrap());
    }

    #[test]
    fn query_filters_reject_non_matching_metadata_without_leaking_object() {
        let req = QueryIndexRequest {
            metadata_filters_json: serde_json::json!({"tenant": "alpha"}).to_string(),
            ..Default::default()
        };
        let filters = QueryFilters::from_request(&req).unwrap();
        let object_ref = QueryObjectRef {
            object_version_id: "version-1".to_string(),
            object_key: "docs/active/item.json".to_string(),
            user_meta: Some(serde_json::json!({"tenant": "beta"})),
        };

        assert!(!filters.matches(&object_ref).unwrap());
    }

    #[test]
    fn query_filters_reject_invalid_metadata_filter_shape() {
        let req = QueryIndexRequest {
            metadata_filters_json: "[]".to_string(),
            ..Default::default()
        };

        assert!(QueryFilters::from_request(&req).is_err());
    }
}
