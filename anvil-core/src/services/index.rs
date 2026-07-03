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
    search_query,
    services::watch_envelope::{self, WatchEnvelopeParts},
    validation, vector_segment,
};
use base64::Engine;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
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
        let kind = concrete_index_kind(req.kind)?;
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
        validate_index_definition_shape(kind, &build_policy)?;

        let index = self
            .persistence
            .create_index_definition(
                claims.tenant_id,
                bucket.id,
                &req.name,
                kind,
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
            "path" | "metadata_filter" => {
                self.query_metadata_backed_index(&claims, &bucket, &index, req)
                    .await
            }
            "typed_json" => {
                self.query_typed_json_index(&claims, &bucket, &index, req)
                    .await
            }
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
                    .send(index_partition_event_response(
                        &bucket_name,
                        &index_name,
                        &index_storage_id,
                        &partition_id,
                        event,
                    ))
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
                        .send(index_partition_event_response(
                            &bucket_name,
                            &index_name,
                            &index_storage_id,
                            &partition_id,
                            event,
                        ))
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
        let index_kind = index_kind_value_from_str(&index.kind)?;
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
                kind: index_kind,
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
            index_kind,
            index_generation: segment.header.generation,
            authz_revision: segment.header.authz_revision,
            scoring_recipe_json: serde_json::json!({"kind": "bm25", "k1": 1.2, "b": 0.75})
                .to_string(),
            next_page_token: String::new(),
            source_watch_cursor_high: 0,
            index_watch_cursor_applied: 0,
            is_caught_up: true,
            lag_record_count_hint: 0,
        }))
    }

    async fn query_metadata_backed_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.trim().is_empty() {
            return Err(Status::invalid_argument(
                "query_text is not valid for path or metadata_filter indexes",
            ));
        }
        if !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_vector is not valid for path or metadata_filter indexes",
            ));
        }
        if index.kind == "metadata_filter" && req.metadata_filters_json.trim().is_empty() {
            return Err(Status::invalid_argument(
                "metadata_filters_json is required for metadata_filter indexes",
            ));
        }
        let filters = QueryFilters::from_request(&req)?;
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let objects = self
            .persistence
            .list_current_directory_objects(bucket)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let latest_authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut hits = Vec::with_capacity(requested_limit);
        for object in objects {
            if validation::is_reserved_internal_key(&object.key) {
                continue;
            }
            let object_ref = QueryObjectRef::from_object(&object);
            if !filters.matches(&object_ref)? {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    u64::try_from(object_ref.authz_revision)
                        .map_err(|_| Status::internal("Invalid authz revision"))?,
                )
                .await?
            {
                continue;
            }
            let metadata_json = serde_json::json!({
                "bucket_name": bucket.name,
                "user_metadata": object_ref.user_meta.clone(),
                "created_at_nanos": object_ref.created_at_nanos,
                "authz_revision": object_ref.authz_revision,
            })
            .to_string();
            hits.push(IndexQueryHit {
                kind: index_kind,
                score: 1.0,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json,
            });
            if hits.len() >= requested_limit {
                break;
            }
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: index.version.max(0) as u64,
            authz_revision: latest_authz_revision.max(0) as u64,
            scoring_recipe_json: serde_json::json!({
                "kind": index.kind.as_str(),
                "score": "constant",
                "source": "object_metadata_directory",
            })
            .to_string(),
            next_page_token: String::new(),
            source_watch_cursor_high: 0,
            index_watch_cursor_applied: 0,
            is_caught_up: true,
            lag_record_count_hint: 0,
        }))
    }

    async fn query_typed_json_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.trim().is_empty() || !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_text and query_vector are not valid for typed_json indexes",
            ));
        }
        let definition = TypedJsonIndexDefinition::from_index(index)?;
        let predicates = TypedPredicate::parse_list(&req.typed_predicates_json)?;
        let order = TypedOrder::parse_list(&req.typed_order_json, &definition.default_order)?;
        let page_token = TypedPageToken::decode(req.page_token.as_str())?;
        let predicate_hash = stable_json_hash(&req.typed_predicates_json);
        let order_hash = stable_json_hash(&serde_json::to_string(&order).unwrap_or_default());
        if let Some(token) = &page_token {
            token.validate(
                claims.tenant_id,
                &bucket.name,
                &index.name,
                index.version as u64,
                &predicate_hash,
                &order_hash,
            )?;
        }

        let latest_cursor = self
            .persistence
            .latest_object_watch_cursor(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        if !req.require_caught_up_to_watch_cursor.trim().is_empty() {
            let required_cursor = req
                .require_caught_up_to_watch_cursor
                .parse::<u64>()
                .map_err(|_| {
                    Status::invalid_argument("Invalid require_caught_up_to_watch_cursor")
                })?;
            if latest_cursor < required_cursor {
                return Err(Status::failed_precondition("IndexLagging"));
            }
        }

        let mut rows = match definition.source_kind.as_str() {
            "object_current" => {
                self.query_typed_json_object_rows(
                    claims,
                    bucket,
                    index,
                    &definition,
                    &predicates,
                    &req.path_prefix,
                )
                .await?
            }
            "append_record" => {
                self.query_typed_json_append_rows(
                    claims,
                    bucket,
                    index,
                    &definition,
                    &predicates,
                    &req.path_prefix,
                )
                .await?
            }
            _ => {
                return Err(Status::failed_precondition(
                    "UnsupportedTypedJsonSourceKind",
                ));
            }
        };

        rows.sort_by(|left, right| compare_typed_rows(left, right, &order));
        if let Some(token) = page_token.as_ref() {
            rows = rows
                .into_iter()
                .filter(|row| {
                    compare_typed_row_to_cursor(
                        row,
                        &token.last_sort_values,
                        &token.last_source_identity,
                        &order,
                    )
                    .is_gt()
                })
                .collect();
        }

        let requested_limit = query_limit(req.limit);
        let has_more = rows.len() > requested_limit;
        if has_more {
            rows.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            rows.last()
                .map(|row| {
                    TypedPageToken {
                        tenant_id: claims.tenant_id,
                        bucket_name: bucket.name.clone(),
                        index_name: index.name.clone(),
                        index_generation: index.version as u64,
                        predicate_hash: predicate_hash.clone(),
                        order_hash: order_hash.clone(),
                        last_source_identity: row.source_identity.clone(),
                        last_sort_values: row.values.clone(),
                    }
                    .encode()
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let index_kind = index_kind_value_from_str(&index.kind)?;
        let hits = rows
            .into_iter()
            .map(|row| IndexQueryHit {
                kind: index_kind,
                score: 1.0,
                object_key: row.object_key,
                object_version_id: row.object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "typed_values": row.values,
                })
                .to_string(),
            })
            .collect();

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: index.version.max(0) as u64,
            authz_revision: self
                .persistence
                .latest_authz_revision(claims.tenant_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .max(0) as u64,
            scoring_recipe_json: serde_json::json!({
                "kind": "typed_json",
                "score": "constant",
                "source": definition.source_kind,
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: latest_cursor,
            is_caught_up: true,
            lag_record_count_hint: 0,
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
        let mut text_generation = 0;
        let mut vector_generation = 0;
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
            text_generation = segment.header.generation;
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
            vector_generation = segment.header.generation;
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

        let (text_weight, vector_weight, freshness_weight) = match (has_text, has_vector) {
            (true, true) => (0.55, 0.35, 0.10),
            (true, false) => (1.0, 0.0, 0.0),
            (false, true) => (0.0, 1.0, 0.0),
            (false, false) => unreachable!("validated above"),
        };

        let mut candidates = Vec::new();
        for item in combined.into_values() {
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
            candidates.push(HybridCandidate { item, object_ref });
        }

        score_hybrid_candidates(
            &mut candidates,
            has_text,
            has_vector,
            text_weight,
            vector_weight,
            freshness_weight,
        );
        candidates.sort_by(|left, right| {
            right
                .item
                .score
                .partial_cmp(&left.item.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    left.item
                        .object_version_id
                        .cmp(&right.item.object_version_id)
                })
        });

        let index_kind = index_kind_value_from_str(&index.kind)?;
        let mut hits = Vec::with_capacity(candidates.len().min(requested_limit));
        for candidate in candidates {
            let item = candidate.item;
            let object_ref = candidate.object_ref;
            hits.push(IndexQueryHit {
                kind: index_kind,
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
                    "freshness_score": item.freshness_score,
                    "normalized_text_score": item.normalized_text_score,
                    "normalized_vector_score": item.normalized_vector_score,
                })
                .to_string(),
            });
            if hits.len() >= requested_limit {
                break;
            }
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "hybrid",
                "text_weight": text_weight,
                "vector_weight": vector_weight,
                "freshness_weight": freshness_weight,
                "index_generations": {
                    "full_text": text_generation,
                    "vector": vector_generation,
                    "max": generation
                }
            })
            .to_string(),
            next_page_token: String::new(),
            source_watch_cursor_high: 0,
            index_watch_cursor_applied: 0,
            is_caught_up: true,
            lag_record_count_hint: 0,
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
        let index_kind = index_kind_value_from_str(&index.kind)?;
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
                kind: index_kind,
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
            index_kind,
            index_generation: segment.header.generation,
            authz_revision: segment.header.authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "vector",
                "metric": segment.header.metric,
                "max_candidate_multiplier": 20
            })
            .to_string(),
            next_page_token: String::new(),
            source_watch_cursor_high: 0,
            index_watch_cursor_applied: 0,
            is_caught_up: true,
            lag_record_count_hint: 0,
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
            created_at_nanos: object.created_at.timestamp_nanos_opt().unwrap_or(0),
            authz_revision: object.authz_revision,
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

    async fn query_typed_json_object_rows(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        definition: &TypedJsonIndexDefinition,
        predicates: &[TypedPredicate],
        path_prefix: &str,
    ) -> Result<Vec<TypedIndexRow>, Status> {
        let objects = self
            .persistence
            .list_current_directory_objects(bucket)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut rows = Vec::new();
        for object in objects {
            if validation::is_reserved_internal_key(&object.key) {
                continue;
            }
            if !path_prefix.trim().is_empty() && !object.key.starts_with(path_prefix) {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object.key,
                    object_authz_revision(&object)?,
                )
                .await?
            {
                continue;
            }
            let json = self
                .load_object_json(
                    Some(claims.clone()),
                    &bucket.name,
                    &object.key,
                    object.version_id,
                )
                .await?;
            let row = TypedIndexRow::from_object(definition, object, &json)?;
            if predicates.iter().all(|predicate| predicate.matches(&row)) {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn query_typed_json_append_rows(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        definition: &TypedJsonIndexDefinition,
        predicates: &[TypedPredicate],
        path_prefix: &str,
    ) -> Result<Vec<TypedIndexRow>, Status> {
        let records = self
            .persistence
            .list_append_stream_records_for_bucket(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut rows = Vec::new();
        for (stream, record) in records {
            if !path_prefix.trim().is_empty() && !stream.stream_key.starts_with(path_prefix) {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &stream.stream_key,
                    self.persistence
                        .latest_authz_revision(claims.tenant_id)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?
                        .max(0) as u64,
                )
                .await?
            {
                continue;
            }
            let payload = self
                .storage
                .retrieve_whole_object(&record.payload_hash)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            if payload.len() > 16 * 1024 * 1024 {
                return Err(Status::failed_precondition(
                    "TypedJsonAppendRecordTooLargeForInlineQuery",
                ));
            }
            let json = serde_json::from_slice(&payload).map_err(|e| {
                Status::failed_precondition(format!("TypedJsonAppendRecordInvalid: {e}"))
            })?;
            let row = TypedIndexRow::from_append_record(definition, stream, record, &json)?;
            if predicates.iter().all(|predicate| predicate.matches(&row)) {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn load_object_json(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: uuid::Uuid,
    ) -> Result<JsonValue, Status> {
        let (_object, mut stream) = self
            .object_manager
            .get_object(
                claims,
                bucket_name.to_string(),
                object_key.to_string(),
                Some(version_id),
            )
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
            if bytes.len() > 16 * 1024 * 1024 {
                return Err(Status::failed_precondition(
                    "TypedJsonObjectTooLargeForInlineQuery",
                ));
            }
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| Status::failed_precondition(format!("TypedJsonObjectInvalid: {e}")))
    }
}

#[derive(Debug, Clone, Default)]
struct QueryObjectRef {
    object_version_id: String,
    object_key: String,
    user_meta: Option<JsonValue>,
    created_at_nanos: i64,
    authz_revision: i64,
}

impl QueryObjectRef {
    fn from_object(object: &crate::persistence::Object) -> Self {
        Self {
            object_version_id: object.version_id.to_string(),
            object_key: object.key.clone(),
            user_meta: object.user_meta.clone(),
            created_at_nanos: object.created_at.timestamp_nanos_opt().unwrap_or(0),
            authz_revision: object.authz_revision,
        }
    }
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

#[derive(Debug, Clone)]
struct TypedJsonIndexDefinition {
    source_kind: String,
    fields: Vec<TypedFieldDefinition>,
    default_order: Vec<TypedOrder>,
}

#[derive(Debug, Clone)]
struct TypedFieldDefinition {
    name: String,
    extractor: String,
    required: bool,
}

impl TypedJsonIndexDefinition {
    fn from_index(index: &crate::persistence::IndexDefinition) -> Result<Self, Status> {
        let source_kind = json_optional_string_field(&index.build_policy, "source_kind")
            .or_else(|| json_optional_string_field(&index.build_policy, "source"))
            .unwrap_or_else(|| "object_current".to_string());
        let fields_json = index
            .build_policy
            .get("fields")
            .or_else(|| index.extractor.get("fields"))
            .ok_or_else(|| Status::invalid_argument("typed_json index requires fields"))?;
        let JsonValue::Array(field_values) = fields_json else {
            return Err(Status::invalid_argument(
                "typed_json fields must be an array",
            ));
        };
        let mut fields = Vec::with_capacity(field_values.len());
        for value in field_values {
            let name = json_optional_string_field(value, "name")
                .ok_or_else(|| Status::invalid_argument("typed_json field requires name"))?;
            let extractor = json_optional_string_field(value, "extractor")
                .or_else(|| json_optional_string_field(value, "json_pointer"))
                .ok_or_else(|| Status::invalid_argument("typed_json field requires extractor"))?;
            validate_typed_extractor(&source_kind, &extractor)?;
            fields.push(TypedFieldDefinition {
                name,
                extractor,
                required: value
                    .get("required")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false),
            });
        }
        let default_order = index
            .build_policy
            .get("default_order")
            .map(TypedOrder::parse_json_array)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            source_kind,
            fields,
            default_order,
        })
    }
}

#[derive(Debug, Clone)]
struct TypedIndexRow {
    object_key: String,
    object_version_id: String,
    source_identity: String,
    values: BTreeMap<String, JsonValue>,
}

impl TypedIndexRow {
    fn from_object(
        definition: &TypedJsonIndexDefinition,
        object: crate::persistence::Object,
        json: &JsonValue,
    ) -> Result<Self, Status> {
        let mut values = BTreeMap::new();
        for field in &definition.fields {
            let value = match field.extractor.as_str() {
                "object_key" => JsonValue::String(object.key.clone()),
                "object_content_type" => object
                    .content_type
                    .clone()
                    .map(JsonValue::String)
                    .unwrap_or(JsonValue::Null),
                "created_at" => JsonValue::String(object.created_at.to_rfc3339()),
                extractor if extractor.starts_with("object_body_json_pointer:") => json
                    .pointer(extractor.trim_start_matches("object_body_json_pointer:"))
                    .cloned()
                    .unwrap_or(JsonValue::Null),
                extractor if extractor.starts_with("object_user_metadata_json_pointer:") => object
                    .user_meta
                    .as_ref()
                    .and_then(|metadata| {
                        metadata.pointer(
                            extractor.trim_start_matches("object_user_metadata_json_pointer:"),
                        )
                    })
                    .cloned()
                    .unwrap_or(JsonValue::Null),
                pointer if pointer.starts_with('/') => {
                    json.pointer(pointer).cloned().unwrap_or(JsonValue::Null)
                }
                _ => JsonValue::Null,
            };
            if value.is_null() && field.required {
                return Err(Status::failed_precondition(format!(
                    "TypedJsonRequiredFieldMissing:{}",
                    field.name
                )));
            }
            values.insert(field.name.clone(), value);
        }
        Ok(Self {
            object_key: object.key.clone(),
            object_version_id: object.version_id.to_string(),
            source_identity: format!("{}#{}", object.key, object.version_id),
            values,
        })
    }

    fn from_append_record(
        definition: &TypedJsonIndexDefinition,
        stream: crate::persistence::AppendStream,
        record: crate::persistence::AppendStreamRecord,
        json: &JsonValue,
    ) -> Result<Self, Status> {
        let mut values = BTreeMap::new();
        for field in &definition.fields {
            let value = match field.extractor.as_str() {
                "append_stream_key" => JsonValue::String(stream.stream_key.clone()),
                "append_record_sequence" => JsonValue::Number(record.record_sequence.into()),
                "append_content_type" => record
                    .content_type
                    .clone()
                    .map(JsonValue::String)
                    .unwrap_or(JsonValue::Null),
                "created_at" => JsonValue::String(record.created_at.to_rfc3339()),
                extractor if extractor.starts_with("append_payload_json_pointer:") => json
                    .pointer(extractor.trim_start_matches("append_payload_json_pointer:"))
                    .cloned()
                    .unwrap_or(JsonValue::Null),
                extractor if extractor.starts_with("append_user_metadata_json_pointer:") => record
                    .user_meta
                    .as_ref()
                    .and_then(|metadata| {
                        metadata.pointer(
                            extractor.trim_start_matches("append_user_metadata_json_pointer:"),
                        )
                    })
                    .cloned()
                    .unwrap_or(JsonValue::Null),
                pointer if pointer.starts_with('/') => {
                    json.pointer(pointer).cloned().unwrap_or(JsonValue::Null)
                }
                _ => JsonValue::Null,
            };
            if value.is_null() && field.required {
                return Err(Status::failed_precondition(format!(
                    "TypedJsonRequiredFieldMissing:{}",
                    field.name
                )));
            }
            values.insert(field.name.clone(), value);
        }
        let source_identity = format!("{}#{}", stream.stream_key, record.record_sequence);
        Ok(Self {
            object_key: stream.stream_key,
            object_version_id: record.record_sequence.to_string(),
            source_identity,
            values,
        })
    }
}

#[derive(Debug, Clone)]
struct TypedPredicate {
    field: String,
    op: String,
    values: Vec<JsonValue>,
}

impl TypedPredicate {
    fn parse_list(raw: &str) -> Result<Vec<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(Vec::new());
        }
        let parsed: JsonValue = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid typed_predicates_json: {e}")))?;
        let JsonValue::Array(items) = parsed else {
            return Err(Status::invalid_argument(
                "typed_predicates_json must be an array",
            ));
        };
        items
            .iter()
            .map(|item| {
                let field = json_optional_string_field(item, "field")
                    .or_else(|| json_optional_string_field(item, "field_name"))
                    .ok_or_else(|| Status::invalid_argument("typed predicate requires field"))?;
                let op = json_optional_string_field(item, "op")
                    .or_else(|| json_optional_string_field(item, "operator"))
                    .ok_or_else(|| Status::invalid_argument("typed predicate requires op"))?
                    .to_ascii_lowercase();
                let values = if let Some(values) = item.get("values").and_then(JsonValue::as_array)
                {
                    values.clone()
                } else if let Some(value) = item.get("value") {
                    vec![value.clone()]
                } else {
                    Vec::new()
                };
                Ok(Self { field, op, values })
            })
            .collect()
    }

    fn matches(&self, row: &TypedIndexRow) -> bool {
        let actual = row.values.get(&self.field).unwrap_or(&JsonValue::Null);
        match self.op.as_str() {
            "eq" | "=" | "==" => self
                .values
                .first()
                .is_some_and(|expected| actual == expected),
            "in" => self.values.iter().any(|expected| actual == expected),
            "lt" | "<" => self
                .values
                .first()
                .is_some_and(|expected| compare_json_values(actual, expected).is_lt()),
            "lte" | "<=" => self
                .values
                .first()
                .is_some_and(|expected| !compare_json_values(actual, expected).is_gt()),
            "gt" | ">" => self
                .values
                .first()
                .is_some_and(|expected| compare_json_values(actual, expected).is_gt()),
            "gte" | ">=" => self
                .values
                .first()
                .is_some_and(|expected| !compare_json_values(actual, expected).is_lt()),
            "exists" => !actual.is_null(),
            "is_null" => actual.is_null(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedOrder {
    field: String,
    #[serde(default = "default_ascending")]
    direction: String,
}

impl TypedOrder {
    fn parse_list(raw: &str, default_order: &[TypedOrder]) -> Result<Vec<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(default_order.to_vec());
        }
        let parsed: JsonValue = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid typed_order_json: {e}")))?;
        Self::parse_json_array(&parsed)
    }

    fn parse_json_array(value: &JsonValue) -> Result<Vec<Self>, Status> {
        let JsonValue::Array(items) = value else {
            return Err(Status::invalid_argument("typed order must be an array"));
        };
        items
            .iter()
            .map(|item| {
                let field = json_optional_string_field(item, "field")
                    .or_else(|| json_optional_string_field(item, "field_name"))
                    .ok_or_else(|| Status::invalid_argument("typed order requires field"))?;
                let direction = json_optional_string_field(item, "direction")
                    .unwrap_or_else(|| "asc".to_string())
                    .to_ascii_lowercase();
                if direction != "asc" && direction != "desc" {
                    return Err(Status::invalid_argument(
                        "typed order direction must be asc or desc",
                    ));
                }
                Ok(Self { field, direction })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedPageToken {
    tenant_id: i64,
    bucket_name: String,
    index_name: String,
    index_generation: u64,
    predicate_hash: String,
    order_hash: String,
    last_source_identity: String,
    #[serde(default)]
    last_sort_values: BTreeMap<String, JsonValue>,
}

impl TypedPageToken {
    fn decode(raw: &str) -> Result<Option<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))
    }

    fn encode(&self) -> Result<String, Status> {
        let bytes = serde_json::to_vec(self)
            .map_err(|e| Status::internal(format!("Serialize page token: {e}")))?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    fn validate(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        index_name: &str,
        index_generation: u64,
        predicate_hash: &str,
        order_hash: &str,
    ) -> Result<(), Status> {
        if self.tenant_id != tenant_id
            || self.bucket_name != bucket_name
            || self.index_name != index_name
            || self.index_generation != index_generation
            || self.predicate_hash != predicate_hash
            || self.order_hash != order_hash
        {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        Ok(())
    }
}

fn compare_typed_rows(
    left: &TypedIndexRow,
    right: &TypedIndexRow,
    order: &[TypedOrder],
) -> std::cmp::Ordering {
    for term in order {
        let ordering = compare_json_values(
            left.values.get(&term.field).unwrap_or(&JsonValue::Null),
            right.values.get(&term.field).unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.source_identity.cmp(&right.source_identity)
}

fn compare_typed_row_to_cursor(
    row: &TypedIndexRow,
    cursor_values: &BTreeMap<String, JsonValue>,
    cursor_source_identity: &str,
    order: &[TypedOrder],
) -> std::cmp::Ordering {
    for term in order {
        let ordering = compare_json_values(
            row.values.get(&term.field).unwrap_or(&JsonValue::Null),
            cursor_values.get(&term.field).unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    row.source_identity.as_str().cmp(cursor_source_identity)
}

fn compare_json_values(left: &JsonValue, right: &JsonValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(Ordering::Equal),
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Null, _) => Ordering::Less,
        (_, JsonValue::Null) => Ordering::Greater,
        _ => left.to_string().cmp(&right.to_string()),
    }
}

fn json_optional_string_field(value: &JsonValue, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}

fn default_ascending() -> String {
    "asc".to_string()
}

fn validate_typed_extractor(source_kind: &str, extractor: &str) -> Result<(), Status> {
    let pointer_valid = |value: &str| value.starts_with('/');
    match (source_kind, extractor) {
        (_, "created_at") => Ok(()),
        ("object_current" | "object_version", "object_key" | "object_content_type") => Ok(()),
        ("object_current" | "object_version", value) if pointer_valid(value) => Ok(()),
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_body_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        (
            "append_record",
            "append_stream_key" | "append_record_sequence" | "append_content_type",
        ) => Ok(()),
        ("append_record", value) if pointer_valid(value) => Ok(()),
        ("append_record", value)
            if value
                .strip_prefix("append_payload_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("append_record", value)
            if value
                .strip_prefix("append_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        _ => Err(Status::invalid_argument(
            "Invalid typed_json field extractor",
        )),
    }
}

fn stable_json_hash(raw: &str) -> String {
    let canonical = if raw.trim().is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_str(raw).unwrap_or(JsonValue::String(raw.to_string()))
    };
    blake3::hash(canonical.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn object_authz_revision(object: &crate::persistence::Object) -> Result<u64, Status> {
    u64::try_from(object.authz_revision).map_err(|_| Status::internal("Invalid authz revision"))
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
    normalized_text_score: f32,
    normalized_vector_score: f32,
    freshness_score: f32,
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
            normalized_text_score: 0.0,
            normalized_vector_score: 0.0,
            freshness_score: 0.0,
            document_id: 0,
            field_id: 0,
            vector_id: 0,
            chunk_id: 0,
            source_start: 0,
            source_len: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct HybridCandidate {
    item: HybridAccum,
    object_ref: QueryObjectRef,
}

fn score_hybrid_candidates(
    candidates: &mut [HybridCandidate],
    has_text: bool,
    has_vector: bool,
    text_weight: f32,
    vector_weight: f32,
    freshness_weight: f32,
) {
    let max_text_score = candidates
        .iter()
        .map(|candidate| candidate.item.text_score.max(0.0))
        .fold(0.0_f32, f32::max);
    let max_vector_score = candidates
        .iter()
        .map(|candidate| candidate.item.vector_score.max(0.0))
        .fold(0.0_f32, f32::max);
    let (min_created_at, max_created_at) =
        candidates
            .iter()
            .fold((i64::MAX, i64::MIN), |(min_seen, max_seen), candidate| {
                (
                    min_seen.min(candidate.object_ref.created_at_nanos),
                    max_seen.max(candidate.object_ref.created_at_nanos),
                )
            });
    let created_range = max_created_at.saturating_sub(min_created_at);

    for candidate in candidates {
        candidate.item.normalized_text_score = if has_text && max_text_score > f32::EPSILON {
            candidate.item.text_score.max(0.0) / max_text_score
        } else {
            0.0
        };
        candidate.item.normalized_vector_score = if has_vector && max_vector_score > f32::EPSILON {
            candidate.item.vector_score.max(0.0) / max_vector_score
        } else {
            0.0
        };
        candidate.item.freshness_score = if freshness_weight > 0.0 {
            if created_range <= 0 {
                1.0
            } else {
                candidate
                    .object_ref
                    .created_at_nanos
                    .saturating_sub(min_created_at) as f32
                    / created_range as f32
            }
        } else {
            0.0
        };
        candidate.item.score = candidate.item.normalized_text_score.mul_add(
            text_weight,
            candidate.item.normalized_vector_score * vector_weight,
        ) + candidate.item.freshness_score * freshness_weight;
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

fn concrete_index_kind(value: i32) -> Result<&'static str, Status> {
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
    let has_non_authorization_filters = !req.path_prefix.trim().is_empty()
        || !req.metadata_filters_json.trim().is_empty()
        || !req.typed_predicates_json.trim().is_empty();
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

fn index_partition_event_response(
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

fn index_definition_event_response(
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

fn index_record_from_event(
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
            ..Default::default()
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
            ..Default::default()
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

    #[test]
    fn hybrid_scoring_normalizes_sources_and_applies_freshness() {
        let mut candidates = vec![
            HybridCandidate {
                item: HybridAccum {
                    text_score: 2.0,
                    vector_score: 2.0,
                    ..HybridAccum::new([1; 16])
                },
                object_ref: QueryObjectRef {
                    created_at_nanos: 100,
                    ..Default::default()
                },
            },
            HybridCandidate {
                item: HybridAccum {
                    text_score: 2.0,
                    vector_score: 2.0,
                    ..HybridAccum::new([2; 16])
                },
                object_ref: QueryObjectRef {
                    created_at_nanos: 200,
                    ..Default::default()
                },
            },
        ];

        score_hybrid_candidates(&mut candidates, true, true, 0.55, 0.35, 0.10);

        assert_eq!(candidates[0].item.normalized_text_score, 1.0);
        assert_eq!(candidates[0].item.normalized_vector_score, 1.0);
        assert_eq!(candidates[0].item.freshness_score, 0.0);
        assert_eq!(candidates[1].item.freshness_score, 1.0);
        assert!(candidates[1].item.score > candidates[0].item.score);
    }

    #[test]
    fn hybrid_scoring_disables_freshness_for_single_source_queries() {
        let mut candidates = vec![HybridCandidate {
            item: HybridAccum {
                text_score: 7.0,
                ..HybridAccum::new([1; 16])
            },
            object_ref: QueryObjectRef {
                created_at_nanos: 200,
                ..Default::default()
            },
        }];

        score_hybrid_candidates(&mut candidates, true, false, 1.0, 0.0, 0.0);

        assert_eq!(candidates[0].item.score, 1.0);
        assert_eq!(candidates[0].item.freshness_score, 0.0);
    }
}
