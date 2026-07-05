use crate::anvil_api::index_service_server::IndexService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control, auth, authz_journal,
    authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace},
    bucket_journal,
    config::Config,
    error_codes::AnvilErrorCode,
    formats::{
        full_text::{Bm25Config, FullTextIndexDefinition, FullTextQueryError},
        hash32,
        vector::VectorMetric,
    },
    full_text_segment, index_journal, index_partition_watch,
    permissions::AnvilAction,
    search_query,
    services::watch_envelope::{self, WatchEnvelopeParts},
    typed_field_segment, validation, vector_segment,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

type HmacSha256 = Hmac<Sha256>;

const INDEX_PAGE_TOKEN_VERSION: u8 = 1;
const INDEX_PAGE_TOKEN_DOMAIN: &[u8] = b"anvil-index-page-token-v1";
const INDEX_PAGE_TOKEN_TTL_SECONDS: i64 = 15 * 60;
const QUERY_PERMISSION_PREFIX_OBJECT_CAP: i32 = 10_000;

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
        validate_index_definition_shape(kind, &build_policy, &extractor, &self.config)?;

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
        validate_index_definition_shape(&existing.kind, &build_policy, &extractor, &self.config)?;

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

    async fn query_spec(
        &self,
        request: Request<QuerySpecRequest>,
    ) -> Result<Response<QuerySpecResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let spec = AnvilQuerySpec::parse(&req.query_spec_json)?;
        let bucket_name = spec.scope.bucket_name.trim();
        if bucket_name.is_empty() {
            return Err(Status::invalid_argument(
                "QuerySpec scope.bucket_name is required",
            ));
        }
        if let Some(storage_tenant) = spec.scope.anvil_storage_tenant_id.as_deref()
            && !storage_tenant.is_empty()
            && storage_tenant != claims.tenant_id.to_string()
        {
            return Err(Status::permission_denied(
                "QuerySpec storage tenant does not match authenticated tenant",
            ));
        }
        if !auth::is_authorized(AnvilAction::IndexRead, bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self.get_index_bucket(claims.tenant_id, bucket_name).await?;
        let plan = self
            .plan_query_spec(&claims, &bucket, &spec, req.accept_degraded)
            .await?;
        let response = if plan.typed_filter_index.is_some() {
            self.execute_composite_query_spec(
                &claims,
                &bucket,
                &plan,
                &req.page_token,
                req.lag_timeout_ms,
            )
            .await?
        } else {
            let query_req =
                plan.single_query_request(&bucket.name, &req.page_token, req.lag_timeout_ms)?;
            match plan.index.kind.as_str() {
                "path" | "metadata_filter" => {
                    self.query_metadata_backed_index(&claims, &bucket, &plan.index, query_req)
                        .await?
                }
                "typed_json" => {
                    self.query_typed_json_index(&claims, &bucket, &plan.index, query_req)
                        .await?
                }
                "full_text" => {
                    self.query_full_text_index(&claims, &bucket, &plan.index, query_req)
                        .await?
                }
                "vector" => {
                    self.query_vector_index(&claims, &bucket, &plan.index, query_req)
                        .await?
                }
                "hybrid" => {
                    self.query_hybrid_index(&claims, &bucket, &plan.index, query_req)
                        .await?
                }
                _ => return Err(Status::failed_precondition("IndexDoesNotSupportQuerySpec")),
            }
            .into_inner()
        };
        if spec.consistency.allow_stale_index == Some(false) && !response.is_caught_up {
            return Err(Status::failed_precondition("IndexLagging"));
        }

        Ok(Response::new(QuerySpecResponse {
            result: Some(response),
            canonical_query_hash: plan.canonical_query_hash,
            plan_json: plan.plan_json,
            diagnostics: plan.diagnostics,
        }))
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

    async fn plan_query_spec(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        spec: &AnvilQuerySpec,
        accept_degraded: bool,
    ) -> Result<QuerySpecPlan, Status> {
        let shape = spec.shape()?;
        if shape.can_relation.as_deref().unwrap_or("read") != "read" {
            return Err(Status::failed_precondition(
                "QuerySpec only supports read authorisation relation for index results",
            ));
        }
        let latest_authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        if let Some(min_authz_revision) = shape.min_authz_revision
            && latest_authz_revision < min_authz_revision
        {
            return Err(Status::failed_precondition("AuthzRevisionLagging"));
        }
        let indexes = self
            .persistence
            .list_index_definitions(claims.tenant_id, bucket.id, false)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let selected = select_query_spec_indexes(&indexes, &shape, accept_degraded)?;
        if selected.primary.authorization_mode == "inherit_object" && shape.can_relation.is_none() {
            return Err(Status::failed_precondition(
                "QuerySpec requires an explicit can predicate for protected resources",
            ));
        }

        let plan_json = serde_json::json!({
            "schema": "anvil.query.plan.v1",
            "planner": if selected.typed_filter.is_some() {
                "primitive-index-intersection"
            } else {
                "primitive-index-single-stage"
            },
            "selected_index": {
                "name": selected.primary.name,
                "kind": selected.primary.kind,
                "version": selected.primary.version,
                "authorization_mode": selected.primary.authorization_mode,
            },
            "filter_index": selected.typed_filter.as_ref().map(|index| serde_json::json!({
                "name": index.name,
                "kind": index.kind,
                "version": index.version,
                "authorization_mode": index.authorization_mode,
            })),
            "source_kind": shape.source_kind,
            "path_prefix": shape.path_prefix,
            "typed_predicate_count": shape.typed_predicates.len(),
            "typed_order_count": shape.typed_order.len(),
            "uses_full_text": shape.query_text.is_some(),
            "uses_vector": shape.query_vector.is_some(),
            "authz_relation": shape.can_relation,
            "authz_revision": latest_authz_revision,
            "degraded": false,
        })
        .to_string();

        Ok(QuerySpecPlan {
            index: selected.primary,
            typed_filter_index: selected.typed_filter,
            canonical_query_hash: stable_json_hash(&spec.canonical_json()?),
            plan_json,
            diagnostics: Vec::new(),
            query_text: shape.query_text.unwrap_or_default(),
            query_vector: shape.query_vector.unwrap_or_default(),
            phrase: shape.phrase,
            path_prefix: shape.path_prefix.unwrap_or_default(),
            typed_predicates: shape.typed_predicates,
            typed_order: shape.typed_order,
            limit: shape.limit,
            require_caught_up_to_watch_cursor: shape
                .min_source_cursor
                .map(|cursor| cursor.to_string())
                .unwrap_or_default(),
        })
    }

    fn index_page_token_signing_key(&self) -> Result<Vec<u8>, Status> {
        hex::decode(&self.config.anvil_secret_encryption_key)
            .map_err(|_| Status::internal("Invalid index page token signing key"))
    }

    async fn execute_composite_query_spec(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        plan: &QuerySpecPlan,
        page_token: &str,
        lag_timeout_ms: u64,
    ) -> Result<QueryIndexResponse, Status> {
        let typed_filter_index = plan
            .typed_filter_index
            .as_ref()
            .ok_or_else(|| Status::internal("composite QuerySpec missing typed filter index"))?;
        let overfetch_limit = query_spec_overfetch_limit(plan.limit);
        let primary_req = QueryIndexRequest {
            bucket_name: bucket.name.clone(),
            index_name: plan.index.name.clone(),
            query_text: plan.query_text.clone(),
            query_vector: plan.query_vector.clone(),
            limit: overfetch_limit,
            phrase: plan.phrase,
            path_prefix: plan.path_prefix.clone(),
            metadata_filters_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: plan.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        };
        let primary = match plan.index.kind.as_str() {
            "full_text" => {
                self.query_full_text_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            "vector" => {
                self.query_vector_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            "hybrid" => {
                self.query_hybrid_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            _ => {
                return Err(Status::failed_precondition(
                    "QuerySpec composite primary index must be full_text, vector or hybrid",
                ));
            }
        }
        .into_inner();

        let typed_req = QueryIndexRequest {
            bucket_name: bucket.name.clone(),
            index_name: typed_filter_index.name.clone(),
            query_text: String::new(),
            query_vector: Vec::new(),
            limit: overfetch_limit,
            phrase: false,
            path_prefix: plan.path_prefix.clone(),
            metadata_filters_json: String::new(),
            typed_predicates_json: serde_json::to_string(&plan.typed_predicates)
                .map_err(|e| Status::internal(e.to_string()))?,
            typed_order_json: serde_json::to_string(&plan.typed_order)
                .map_err(|e| Status::internal(e.to_string()))?,
            page_token: String::new(),
            require_caught_up_to_watch_cursor: plan.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        };
        let typed = self
            .query_typed_json_index(claims, bucket, typed_filter_index, typed_req)
            .await?
            .into_inner();

        let mut typed_by_version = BTreeMap::new();
        for hit in typed.hits {
            let typed_values = typed_values_from_query_hit(&hit)?;
            typed_by_version.insert(hit.object_version_id.clone(), typed_values);
        }

        let mut hits = Vec::new();
        for mut hit in primary.hits {
            let Some(typed_values) = typed_by_version.get(&hit.object_version_id) else {
                continue;
            };
            hit.metadata_json = merge_composite_metadata(&hit.metadata_json, typed_values)?;
            hits.push(hit);
        }

        if plan.typed_order.is_empty() {
            hits.sort_by(|left, right| {
                compare_score_hits(
                    left.score,
                    &left.object_version_id,
                    right.score,
                    &right.object_version_id,
                )
            });
        } else {
            hits.sort_by(|left, right| compare_query_spec_hits_by_typed_order(left, right, plan));
        }

        let authz_revision = primary.authz_revision.max(typed.authz_revision);
        let index_generation = primary.index_generation.max(typed.index_generation);
        let index_name = composite_query_spec_index_name(&plan.index, typed_filter_index);
        let predicate_hash = composite_query_spec_predicate_hash(
            plan,
            primary.index_generation,
            typed.index_generation,
        );
        let order_hash = composite_query_spec_order_hash(plan);
        let signing_key = self.index_page_token_signing_key()?;
        let index_definition_version =
            composite_index_definition_version(&plan.index, typed_filter_index);
        let binding = IndexPageTokenBinding::with_index_inputs(
            &self.config,
            claims,
            "query_spec_composite",
            &bucket.name,
            &index_name,
            index_generation,
            index_definition_version,
            vec![
                IndexPageTokenInput {
                    index_id: plan.index.name.clone(),
                    definition_hash: stable_string_hash(&format!(
                        "{}:{}",
                        plan.index.name,
                        plan.index.version.max(0)
                    )),
                    generation: primary.index_generation,
                },
                IndexPageTokenInput {
                    index_id: typed_filter_index.name.clone(),
                    definition_hash: stable_string_hash(&format!(
                        "{}:{}",
                        typed_filter_index.name,
                        typed_filter_index.version.max(0)
                    )),
                    generation: typed.index_generation,
                },
            ],
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let token = IndexPageToken::decode(page_token, &signing_key)?;
        if let Some(token) = &token {
            token.validate(&binding)?;
        }
        if let Some(token) = token.as_ref() {
            hits = hits
                .into_iter()
                .filter(|hit| query_spec_hit_after_cursor(hit, token, plan).unwrap_or(false))
                .collect();
        }

        let requested_limit = query_limit(plan.limit);
        let has_more = hits.len() > requested_limit;
        if has_more {
            hits.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            hits.last()
                .map(|hit| {
                    let last_sort_values = query_spec_hit_sort_values(hit, plan)?;
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        last_sort_values,
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(QueryIndexResponse {
            hits,
            index_kind: primary.index_kind,
            index_generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "query_spec_composite",
                "planner": "primitive-index-intersection",
                "primary_index": plan.index.name,
                "typed_filter_index": typed_filter_index.name,
                "overfetch_limit": overfetch_limit,
                "primary_scoring": serde_json::from_str::<JsonValue>(&primary.scoring_recipe_json)
                    .unwrap_or(JsonValue::String(primary.scoring_recipe_json)),
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: primary
                .source_watch_cursor_high
                .max(typed.source_watch_cursor_high),
            index_watch_cursor_applied: primary
                .index_watch_cursor_applied
                .min(typed.index_watch_cursor_applied),
            is_caught_up: primary.is_caught_up && typed.is_caught_up,
            lag_record_count_hint: primary
                .lag_record_count_hint
                .max(typed.lag_record_count_hint),
        })
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
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let authz_revision = authz_revision.max(segment.header.authz_revision);
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
        let search_hits = search_query::query_full_text_segment(
            &segment,
            search_query::FullTextSegmentQuery {
                query: &req.query_text,
                tokenizer: &definition.tokenizer,
                positions_enabled: definition.positions_enabled,
                phrase: req.phrase,
                bm25: Bm25Config::default(),
                authorized_labels: permission_filter
                    .as_ref()
                    .map(|filter| &filter.authorized_labels),
                limit: segment.postings.len().max(1),
            },
        )
        .map_err(full_text_query_status)?;
        let predicate_hash = score_based_predicate_hash("full_text", &req);
        let order_hash = score_order_hash();
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "full_text",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        let mut candidates = Vec::with_capacity(search_hits.len());
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
                    authz_revision,
                )
                .await?
            {
                continue;
            }
            let object_version_id = object_ref.object_version_id.clone();
            candidates.push(IndexQueryHit {
                kind: index_kind,
                score: hit.score,
                object_key: object_ref.object_key,
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
        }
        candidates.sort_by(|left, right| {
            compare_score_hits(
                left.score,
                &left.object_version_id,
                right.score,
                &right.object_version_id,
            )
        });
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for hit in candidates {
                if score_after_cursor(hit.score, &hit.object_version_id, token)? {
                    filtered.push(hit);
                }
            }
            candidates = filtered;
        }
        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|hit| {
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        score_sort_values(hit.score, &hit.object_version_id),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(QueryIndexResponse {
            hits: candidates,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({"kind": "bm25", "k1": 1.2, "b": 0.75})
                .to_string(),
            next_page_token,
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
        let latest_cursor = self
            .persistence
            .latest_object_watch_cursor(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let required_cursor = if !req.require_caught_up_to_watch_cursor.trim().is_empty() {
            let required_cursor = req
                .require_caught_up_to_watch_cursor
                .parse::<u64>()
                .map_err(|_| {
                    Status::invalid_argument("Invalid require_caught_up_to_watch_cursor")
                })?;
            if latest_cursor < required_cursor {
                return Err(Status::failed_precondition("IndexLagging"));
            }
            Some(required_cursor)
        } else {
            None
        };
        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let segment =
            typed_field_segment::read_latest_typed_field_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        let Some(segment) = segment else {
            if latest_cursor == 0 {
                return Ok(Response::new(QueryIndexResponse {
                    hits: Vec::new(),
                    index_kind,
                    index_generation: 0,
                    authz_revision: 0,
                    scoring_recipe_json: serde_json::json!({
                        "kind": index.kind.as_str(),
                        "score": "constant",
                        "source": "corestore_typed_field_segment",
                    })
                    .to_string(),
                    next_page_token: String::new(),
                    source_watch_cursor_high: 0,
                    index_watch_cursor_applied: 0,
                    is_caught_up: true,
                    lag_record_count_hint: 0,
                }));
            }
            return Err(Status::failed_precondition("IndexUnavailable"));
        };
        if segment.header.source_kind != "object_metadata" {
            return Err(Status::failed_precondition(
                "MetadataBackedIndexSourceKindMismatch",
            ));
        }
        if required_cursor.is_some_and(|cursor| segment.header.source_cursor < cursor) {
            return Err(Status::failed_precondition("IndexLagging"));
        }
        let latest_authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let authz_revision = latest_authz_revision.max(0) as u64;
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
        let predicate_hash = metadata_backed_predicate_hash(&index.kind, &req);
        let order_hash = stable_string_hash("object_key:asc,source_identity:asc");
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "metadata_backed",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }

        let mut rows = Vec::new();
        for row in segment.rows {
            if validation::is_reserved_internal_key(&row.object_key) {
                continue;
            }
            let object_ref = QueryObjectRef::from_typed_field_row(&row)?;
            if !filters.matches(&object_ref)? {
                continue;
            }
            if let Some(permission_filter) = permission_filter.as_ref()
                && !permission_filter.allows_object_key(&object_ref.object_key)
            {
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
            rows.push((row.source_identity.clone(), object_ref));
        }

        rows.sort_by(|(left_source, left), (right_source, right)| {
            left.object_key
                .cmp(&right.object_key)
                .then(left_source.cmp(right_source))
        });
        if let Some(token) = page_token.as_ref() {
            rows.retain(|(source_identity, object_ref)| {
                object_key_after_cursor(
                    &object_ref.object_key,
                    source_identity,
                    &token.last_sort_values,
                    &token.last_source_identity,
                )
            });
        }

        let has_more = rows.len() > requested_limit;
        if has_more {
            rows.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            rows.last()
                .map(|(source_identity, object_ref)| {
                    let mut sort_values = BTreeMap::new();
                    sort_values.insert(
                        "object_key".to_string(),
                        JsonValue::String(object_ref.object_key.clone()),
                    );
                    IndexPageToken::for_cursor(&binding, source_identity.clone(), sort_values)
                        .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let mut hits = Vec::with_capacity(rows.len());
        for (_, object_ref) in rows {
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
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision: authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": index.kind.as_str(),
                "score": "constant",
                "source": "corestore_typed_field_segment",
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: segment.header.source_cursor,
            is_caught_up: segment.header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(segment.header.source_cursor),
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
        let predicate_hash = stable_json_hash(&req.typed_predicates_json);
        let order_hash = stable_json_hash(&serde_json::to_string(&order).unwrap_or_default());
        let requested_limit = query_limit(req.limit);
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;

        let latest_cursor = if definition.source_kind == "append_record" {
            u64::try_from(
                crate::append_journal::append_record_source_cursor(
                    &self.storage,
                    claims.tenant_id,
                    bucket.id,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            )
            .unwrap_or(u64::MAX)
        } else {
            self.persistence
                .latest_object_watch_cursor(claims.tenant_id, bucket.id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .max(0) as u64
        };
        let required_cursor = if !req.require_caught_up_to_watch_cursor.trim().is_empty() {
            let required_cursor = req
                .require_caught_up_to_watch_cursor
                .parse::<u64>()
                .map_err(|_| {
                    Status::invalid_argument("Invalid require_caught_up_to_watch_cursor")
                })?;
            if latest_cursor < required_cursor {
                return Err(Status::failed_precondition("IndexLagging"));
            }
            Some(required_cursor)
        } else {
            None
        };

        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let segment =
            typed_field_segment::read_latest_typed_field_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::failed_precondition("TypedJsonIndexNotMaterialised"))?;
        if segment.header.source_kind != definition.source_kind {
            return Err(Status::failed_precondition(
                "TypedJsonIndexSourceKindMismatch",
            ));
        }
        let expected_fields = definition
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        if segment
            .header
            .field_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != expected_fields
        {
            return Err(Status::failed_precondition(
                "TypedJsonIndexFieldSetMismatch",
            ));
        }
        if required_cursor.is_some_and(|cursor| segment.header.source_cursor < cursor) {
            return Err(Status::failed_precondition("IndexLagging"));
        }
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "typed_json",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }

        let mut rows = Vec::new();
        for row in segment.rows {
            if !req.path_prefix.trim().is_empty() && !row.object_key.starts_with(&req.path_prefix) {
                continue;
            }
            if let Some(permission_filter) = permission_filter.as_ref()
                && !permission_filter.allows_object_key(&row.object_key)
            {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &row.object_key,
                    row.authz_revision,
                )
                .await?
            {
                continue;
            }
            let row = TypedIndexRow::from_segment_row(row);
            if predicates.iter().all(|predicate| predicate.matches(&row)) {
                rows.push(row);
            }
        }

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

        let has_more = rows.len() > requested_limit;
        if has_more {
            rows.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            rows.last()
                .map(|row| {
                    IndexPageToken::for_cursor(
                        &binding,
                        row.source_identity.clone(),
                        row.values.clone(),
                    )
                    .encode(&signing_key)
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
            index_generation: segment.header.generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "typed_json",
                "score": "constant",
                "source": definition.source_kind,
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: segment.header.source_cursor,
            is_caught_up: segment.header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(segment.header.source_cursor),
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
        let mut authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
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
                    authorized_labels: permission_filter
                        .as_ref()
                        .map(|filter| &filter.authorized_labels),
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
                permission_filter
                    .as_ref()
                    .map(|filter| &filter.authorized_labels),
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
        let predicate_hash = score_based_predicate_hash("hybrid", &req);
        let order_hash = score_order_hash();
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "hybrid",
            &bucket.name,
            &index.name,
            generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for candidate in candidates {
                if score_after_cursor(
                    candidate.item.score,
                    &candidate.object_ref.object_version_id,
                    token,
                )? {
                    filtered.push(candidate);
                }
            }
            candidates = filtered;
        }
        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|candidate| {
                    IndexPageToken::for_cursor(
                        &binding,
                        candidate.object_ref.object_version_id.clone(),
                        score_sort_values(
                            candidate.item.score,
                            &candidate.object_ref.object_version_id,
                        ),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

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
            next_page_token,
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
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let authz_revision = authz_revision.max(segment.header.authz_revision);
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
        let predicate_hash = score_based_predicate_hash("vector", &req);
        let order_hash = score_order_hash();
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "vector",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        let search_hits = search_query::query_vector_segment(
            &segment,
            &req.query_vector,
            metric,
            permission_filter
                .as_ref()
                .map(|filter| &filter.authorized_labels),
            segment.entries.len(),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut candidates = Vec::with_capacity(search_hits.len());
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
                    authz_revision,
                )
                .await?
            {
                continue;
            }
            let object_version_id = object_ref.object_version_id.clone();
            candidates.push(IndexQueryHit {
                kind: index_kind,
                score: hit.score,
                object_key: object_ref.object_key,
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
        }
        candidates.sort_by(|left, right| {
            compare_score_hits(
                left.score,
                &left.object_version_id,
                right.score,
                &right.object_version_id,
            )
        });
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for hit in candidates {
                if score_after_cursor(hit.score, &hit.object_version_id, token)? {
                    filtered.push(hit);
                }
            }
            candidates = filtered;
        }

        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|hit| {
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        score_sort_values(hit.score, &hit.object_version_id),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(QueryIndexResponse {
            hits: candidates,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "vector",
                "metric": segment.header.metric,
                "max_candidate_multiplier": 20
            })
            .to_string(),
            next_page_token,
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

    async fn query_permission_filter(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        authorization_mode: &str,
        authz_revision: u64,
    ) -> Result<Option<QueryPermissionFilter>, Status> {
        if authorization_mode != "inherit_object" {
            return Ok(None);
        }
        if auth::is_authorized(
            AnvilAction::ObjectRead,
            &format!("{}/", bucket.name),
            &claims.scopes,
        ) {
            return Ok(None);
        }

        let mut object_keys = BTreeSet::new();
        let mut object_key_prefixes = BTreeSet::new();
        let mut grants_bucket_read = false;
        for scope in &claims.scopes {
            collect_object_scope(
                scope,
                &bucket.name,
                &mut object_keys,
                &mut object_key_prefixes,
                &mut grants_bucket_read,
            );
        }
        if grants_bucket_read {
            return Ok(None);
        }

        let revision = i64::try_from(authz_revision)
            .map_err(|_| Status::internal("Invalid authz revision"))?;
        let direct_objects = authz_journal::list_current_authz_objects_at_revision(
            &self.storage,
            claims.tenant_id,
            &encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "object"),
            "reader",
            access_control::APP_SUBJECT_KIND,
            &claims.sub,
            "",
            revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let bucket_prefix = format!("{}/", bucket.name);
        for object_id in direct_objects {
            if let Some(key) = object_id.strip_prefix(&bucket_prefix)
                && !key.is_empty()
            {
                object_keys.insert(key.to_string());
            }
        }

        let mut existing_keys = BTreeSet::new();
        let mut authorized_labels = BTreeSet::new();
        for key in &object_keys {
            let Some(object) = self
                .persistence
                .get_object(bucket.id, key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            else {
                continue;
            };
            existing_keys.insert(key.clone());
            authorized_labels.insert(query_object_authz_label_hash(bucket, &object));
        }
        for prefix in &object_key_prefixes {
            let (objects, _) = self
                .persistence
                .list_objects(
                    bucket.id,
                    prefix,
                    "",
                    QUERY_PERMISSION_PREFIX_OBJECT_CAP.saturating_add(1),
                    "",
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            if objects.len() > QUERY_PERMISSION_PREFIX_OBJECT_CAP as usize {
                return Err(Status::failed_precondition(
                    "AuthzPermissionSetTooLargeForPrefixScope",
                ));
            }
            for object in objects {
                existing_keys.insert(object.key.clone());
                authorized_labels.insert(query_object_authz_label_hash(bucket, &object));
            }
        }

        Ok(Some(QueryPermissionFilter {
            object_keys: existing_keys,
            authorized_labels,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnvilQuerySpec {
    schema: String,
    scope: AnvilQueryScope,
    #[serde(default = "default_query_source_kind")]
    source_kind: String,
    #[serde(rename = "where", default)]
    predicates: AnvilQueryWhere,
    #[serde(default)]
    order_by: Vec<AnvilQueryOrder>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    consistency: AnvilQueryConsistency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnvilQueryScope {
    #[serde(default)]
    mesh_id: Option<String>,
    #[serde(default)]
    anvil_storage_tenant_id: Option<String>,
    #[serde(default)]
    authz_scope: Option<JsonValue>,
    bucket_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AnvilQueryWhere {
    #[serde(default)]
    all: Vec<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnvilQueryOrder {
    field: String,
    #[serde(default = "default_ascending")]
    direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AnvilQueryConsistency {
    #[serde(default)]
    min_source_cursor: Option<JsonValue>,
    #[serde(default)]
    min_authz_revision: Option<JsonValue>,
    #[serde(default)]
    allow_stale_index: Option<bool>,
}

#[derive(Debug, Clone)]
struct QuerySpecShape {
    source_kind: String,
    path_prefix: Option<String>,
    typed_predicates: Vec<JsonValue>,
    typed_order: Vec<TypedOrder>,
    query_text: Option<String>,
    query_vector: Option<Vec<f32>>,
    phrase: bool,
    can_relation: Option<String>,
    min_source_cursor: Option<u64>,
    min_authz_revision: Option<u64>,
    limit: u32,
}

#[derive(Debug, Clone)]
struct QuerySpecPlan {
    index: crate::persistence::IndexDefinition,
    typed_filter_index: Option<crate::persistence::IndexDefinition>,
    canonical_query_hash: String,
    plan_json: String,
    diagnostics: Vec<String>,
    query_text: String,
    query_vector: Vec<f32>,
    phrase: bool,
    path_prefix: String,
    typed_predicates: Vec<JsonValue>,
    typed_order: Vec<TypedOrder>,
    limit: u32,
    require_caught_up_to_watch_cursor: String,
}

#[derive(Debug, Clone)]
struct QuerySpecIndexSelection {
    primary: crate::persistence::IndexDefinition,
    typed_filter: Option<crate::persistence::IndexDefinition>,
}

impl QuerySpecPlan {
    fn single_query_request(
        &self,
        bucket_name: &str,
        page_token: &str,
        lag_timeout_ms: u64,
    ) -> Result<QueryIndexRequest, Status> {
        Ok(QueryIndexRequest {
            bucket_name: bucket_name.to_string(),
            index_name: self.index.name.clone(),
            query_text: self.query_text.clone(),
            query_vector: self.query_vector.clone(),
            limit: self.limit,
            phrase: self.phrase,
            path_prefix: self.path_prefix.clone(),
            metadata_filters_json: String::new(),
            typed_predicates_json: serde_json::to_string(&self.typed_predicates)
                .map_err(|e| Status::internal(e.to_string()))?,
            typed_order_json: serde_json::to_string(&self.typed_order)
                .map_err(|e| Status::internal(e.to_string()))?,
            page_token: page_token.to_string(),
            require_caught_up_to_watch_cursor: self.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        })
    }
}

fn default_query_source_kind() -> String {
    "object_current".to_string()
}

impl AnvilQuerySpec {
    fn parse(raw: &str) -> Result<Self, Status> {
        let spec: Self = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid QuerySpec JSON: {e}")))?;
        if spec.schema != "anvil.query.spec.v1" {
            return Err(Status::invalid_argument("Invalid QuerySpec schema"));
        }
        if let Some(mesh_id) = spec.scope.mesh_id.as_deref()
            && mesh_id.trim().is_empty()
        {
            return Err(Status::invalid_argument("QuerySpec scope.mesh_id is empty"));
        }
        if let Some(authz_scope) = spec.scope.authz_scope.as_ref()
            && !authz_scope.is_object()
        {
            return Err(Status::invalid_argument(
                "QuerySpec scope.authz_scope must be an object",
            ));
        }
        Ok(spec)
    }

    fn canonical_json(&self) -> Result<String, Status> {
        serde_json::to_string(
            &serde_json::to_value(self)
                .map_err(|e| Status::internal(format!("Serialize QuerySpec: {e}")))?,
        )
        .map_err(|e| Status::internal(format!("Canonicalize QuerySpec: {e}")))
    }

    fn shape(&self) -> Result<QuerySpecShape, Status> {
        let mut path_prefix = None;
        let mut typed_predicates = Vec::new();
        let mut query_text = None;
        let mut query_vector = None;
        let mut phrase = false;
        let mut can_relation = None;

        for predicate in &self.predicates.all {
            if let Some(value) = predicate.get("path_prefix").and_then(JsonValue::as_str) {
                path_prefix = Some(value.to_string());
                continue;
            }
            if let Some(field) = predicate.get("field").and_then(JsonValue::as_str) {
                let op = predicate
                    .get("op")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        Status::invalid_argument("QuerySpec field predicate requires op")
                    })?;
                let value = predicate
                    .get("value")
                    .or_else(|| predicate.get("values"))
                    .cloned()
                    .unwrap_or(JsonValue::Null);
                let values = match value {
                    JsonValue::Array(values) if op.eq_ignore_ascii_case("in") => values,
                    other => vec![other],
                };
                typed_predicates.push(serde_json::json!({
                    "field": field,
                    "op": op,
                    "values": values,
                }));
                continue;
            }
            if let Some(full_text) = predicate.get("full_text") {
                let query = full_text
                    .get("query")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        Status::invalid_argument("QuerySpec full_text predicate requires query")
                    })?;
                query_text = Some(query.to_string());
                phrase = full_text
                    .get("phrase")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                continue;
            }
            if let Some(vector) = predicate.get("vector") {
                let near = vector
                    .get("near")
                    .and_then(JsonValue::as_array)
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "QuerySpec vector.near must be an inline numeric vector",
                        )
                    })?;
                let mut values = Vec::with_capacity(near.len());
                for value in near {
                    let Some(value) = value.as_f64() else {
                        return Err(Status::invalid_argument(
                            "QuerySpec vector.near values must be numeric",
                        ));
                    };
                    values.push(value as f32);
                }
                query_vector = Some(values);
                continue;
            }
            if let Some(can) = predicate.get("can") {
                let relation =
                    can.get("relation")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| {
                            Status::invalid_argument("QuerySpec can predicate requires relation")
                        })?;
                can_relation = Some(relation.to_string());
                continue;
            }
            return Err(Status::invalid_argument("Unsupported QuerySpec predicate"));
        }

        let typed_order = self
            .order_by
            .iter()
            .map(|order| {
                let direction = order.direction.to_ascii_lowercase();
                if direction != "asc" && direction != "desc" {
                    return Err(Status::invalid_argument(
                        "QuerySpec order_by direction must be asc or desc",
                    ));
                }
                Ok(TypedOrder {
                    field: order.field.clone(),
                    direction,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QuerySpecShape {
            source_kind: self.source_kind.clone(),
            path_prefix,
            typed_predicates,
            typed_order,
            query_text,
            query_vector,
            phrase,
            can_relation,
            min_source_cursor: parse_optional_u64_json(
                self.consistency.min_source_cursor.as_ref(),
                "min_source_cursor",
            )?,
            min_authz_revision: parse_optional_u64_json(
                self.consistency.min_authz_revision.as_ref(),
                "min_authz_revision",
            )?,
            limit: self.limit.unwrap_or(100),
        })
    }
}

fn parse_optional_u64_json(value: Option<&JsonValue>, label: &str) -> Result<Option<u64>, Status> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(number)) => number
            .as_u64()
            .ok_or_else(|| Status::invalid_argument(format!("QuerySpec {label} must be u64")))
            .map(Some),
        Some(JsonValue::String(value)) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| Status::invalid_argument(format!("QuerySpec {label} must be u64"))),
        _ => Err(Status::invalid_argument(format!(
            "QuerySpec {label} must be a string or integer"
        ))),
    }
}

fn select_query_spec_indexes(
    indexes: &[crate::persistence::IndexDefinition],
    shape: &QuerySpecShape,
    accept_degraded: bool,
) -> Result<QuerySpecIndexSelection, Status> {
    let needs_typed = !shape.typed_predicates.is_empty() || !shape.typed_order.is_empty();
    let needs_text = shape.query_text.is_some();
    let needs_vector = shape.query_vector.is_some();
    let needs_path_only =
        shape.path_prefix.is_some() && !needs_typed && !needs_text && !needs_vector;

    let typed_filter = if needs_typed {
        Some(
            indexes
                .iter()
                .filter(|index| index.enabled && index.kind == "typed_json")
                .find(|index| typed_json_index_covers(index, shape).unwrap_or(false))
                .cloned()
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "QuerySpec has no typed_json index covering predicates",
                    )
                })?,
        )
    } else {
        None
    };

    if needs_text && needs_vector {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "hybrid")
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("QuerySpec text+vector plan requires a hybrid index")
            })?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if let Some(typed_filter) = typed_filter.clone()
        && !needs_text
        && !needs_vector
    {
        return Ok(QuerySpecIndexSelection {
            primary: typed_filter,
            typed_filter: None,
        });
    }
    if needs_text {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "full_text")
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no full_text index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if needs_vector {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "vector")
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no vector index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if needs_path_only {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && matches!(index.kind.as_str(), "path" | "typed_json"))
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no path-capable index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter: None,
        });
    }
    if accept_degraded {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "path")
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("QuerySpec has no bounded primitive index")
            })?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter: None,
        });
    }
    Err(Status::failed_precondition(
        "QuerySpec requires at least one bounded primitive predicate",
    ))
}

fn typed_json_index_covers(
    index: &crate::persistence::IndexDefinition,
    shape: &QuerySpecShape,
) -> Result<bool, Status> {
    let definition = TypedJsonIndexDefinition::from_index(index)?;
    if definition.source_kind != shape.source_kind {
        return Ok(false);
    }
    let fields = definition
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for predicate in &shape.typed_predicates {
        let Some(field) = predicate.get("field").and_then(JsonValue::as_str) else {
            return Ok(false);
        };
        if !fields.contains(field) {
            return Ok(false);
        }
    }
    for order in &shape.typed_order {
        if !fields.contains(order.field.as_str()) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn query_spec_overfetch_limit(requested: u32) -> u32 {
    requested.saturating_mul(10).clamp(100, 1000)
}

fn composite_query_spec_index_name(
    primary: &crate::persistence::IndexDefinition,
    typed_filter: &crate::persistence::IndexDefinition,
) -> String {
    format!("{}+{}", primary.name, typed_filter.name)
}

fn composite_index_definition_version(
    primary: &crate::persistence::IndexDefinition,
    typed_filter: &crate::persistence::IndexDefinition,
) -> u64 {
    let primary = u64::try_from(primary.version).unwrap_or(0);
    let typed_filter = u64::try_from(typed_filter.version).unwrap_or(0);
    primary.max(typed_filter)
}

fn composite_query_spec_predicate_hash(
    plan: &QuerySpecPlan,
    primary_generation: u64,
    typed_generation: u64,
) -> String {
    let shape = serde_json::json!({
        "canonical_query_hash": plan.canonical_query_hash,
        "primary_index": plan.index.name,
        "primary_generation": primary_generation,
        "typed_filter_index": plan.typed_filter_index.as_ref().map(|index| index.name.as_str()),
        "typed_generation": typed_generation,
    });
    blake3::hash(shape.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn composite_query_spec_order_hash(plan: &QuerySpecPlan) -> String {
    if plan.typed_order.is_empty() {
        score_order_hash()
    } else {
        stable_json_hash(&serde_json::to_string(&plan.typed_order).unwrap_or_default())
    }
}

fn typed_values_from_query_hit(hit: &IndexQueryHit) -> Result<BTreeMap<String, JsonValue>, Status> {
    let metadata: JsonValue = serde_json::from_str(&hit.metadata_json)
        .map_err(|e| Status::internal(format!("Invalid query hit metadata_json: {e}")))?;
    let Some(values) = metadata.get("typed_values").and_then(JsonValue::as_object) else {
        return Err(Status::internal(
            "typed query hit metadata is missing typed_values",
        ));
    };
    Ok(values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect())
}

fn merge_composite_metadata(
    primary_metadata_json: &str,
    typed_values: &BTreeMap<String, JsonValue>,
) -> Result<String, Status> {
    let primary = serde_json::from_str::<JsonValue>(primary_metadata_json)
        .unwrap_or_else(|_| JsonValue::String(primary_metadata_json.to_string()));
    serde_json::to_string(&serde_json::json!({
        "primary": primary,
        "typed_values": typed_values,
    }))
    .map_err(|e| Status::internal(format!("Serialize composite metadata: {e}")))
}

fn compare_query_spec_hits_by_typed_order(
    left: &IndexQueryHit,
    right: &IndexQueryHit,
    plan: &QuerySpecPlan,
) -> std::cmp::Ordering {
    let left_values = typed_values_from_query_hit(left).unwrap_or_default();
    let right_values = typed_values_from_query_hit(right).unwrap_or_default();
    for term in &plan.typed_order {
        let ordering = compare_json_values(
            left_values.get(&term.field).unwrap_or(&JsonValue::Null),
            right_values.get(&term.field).unwrap_or(&JsonValue::Null),
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
    left.object_version_id.cmp(&right.object_version_id)
}

fn query_spec_hit_sort_values(
    hit: &IndexQueryHit,
    plan: &QuerySpecPlan,
) -> Result<BTreeMap<String, JsonValue>, Status> {
    if plan.typed_order.is_empty() {
        Ok(score_sort_values(hit.score, &hit.object_version_id))
    } else {
        typed_values_from_query_hit(hit)
    }
}

fn query_spec_hit_after_cursor(
    hit: &IndexQueryHit,
    token: &IndexPageToken,
    plan: &QuerySpecPlan,
) -> Result<bool, Status> {
    if plan.typed_order.is_empty() {
        return score_after_cursor(hit.score, &hit.object_version_id, token);
    }
    let values = typed_values_from_query_hit(hit)?;
    for term in &plan.typed_order {
        let ordering = compare_json_values(
            values.get(&term.field).unwrap_or(&JsonValue::Null),
            token
                .last_sort_values
                .get(&term.field)
                .unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return Ok(ordering.is_gt());
        }
    }
    Ok(hit.object_version_id > token.last_source_identity)
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
    fn from_typed_field_row(
        row: &typed_field_segment::TypedFieldSegmentRow,
    ) -> Result<Self, Status> {
        let user_meta = JsonValue::Object(
            row.values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        Ok(Self {
            object_version_id: row.object_version_id.clone(),
            object_key: row.object_key.clone(),
            user_meta: Some(user_meta),
            created_at_nanos: 0,
            authz_revision: i64::try_from(row.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct QueryPermissionFilter {
    object_keys: BTreeSet<String>,
    authorized_labels: BTreeSet<[u8; 32]>,
}

impl QueryPermissionFilter {
    fn allows_object_key(&self, object_key: &str) -> bool {
        self.object_keys.contains(object_key)
    }
}

fn collect_object_scope(
    scope: &str,
    bucket_name: &str,
    object_keys: &mut BTreeSet<String>,
    object_key_prefixes: &mut BTreeSet<String>,
    grants_bucket_read: &mut bool,
) {
    let Some((action, resource)) = scope.split_once('|') else {
        return;
    };
    if !matches!(action, "object:read" | "object:*" | "*") {
        return;
    }
    if resource == "*" {
        *grants_bucket_read = true;
        return;
    }
    let bucket_prefix = format!("{bucket_name}/");
    let Some(key_pattern) = resource.strip_prefix(&bucket_prefix) else {
        return;
    };
    if key_pattern.is_empty() {
        *grants_bucket_read = true;
        return;
    }
    if key_pattern.ends_with('*') {
        let prefix = key_pattern.trim_end_matches('*');
        if prefix.is_empty() {
            *grants_bucket_read = true;
        } else {
            object_key_prefixes.insert(prefix.to_string());
        }
        return;
    }
    object_keys.insert(key_pattern.to_string());
}

fn query_object_authz_label_hash(
    bucket: &crate::persistence::Bucket,
    object: &crate::persistence::Object,
) -> [u8; 32] {
    hash32(
        format!(
            "tenant:{}:bucket:{}:object:{}:authz:{}",
            bucket.tenant_id, bucket.id, object.key, object.authz_revision
        )
        .as_bytes(),
    )
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
            fields.push(TypedFieldDefinition { name });
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
    fn from_segment_row(row: typed_field_segment::TypedFieldSegmentRow) -> Self {
        Self {
            object_key: row.object_key,
            object_version_id: row.object_version_id,
            source_identity: row.source_identity,
            values: row.values,
        }
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
struct IndexPageToken {
    version: u8,
    token_kind: String,
    mesh_id: String,
    anvil_storage_tenant_id: String,
    authz_realm_id: String,
    tenant_id: i64,
    bucket_name: String,
    index_name: String,
    index_generation: u64,
    index_definition_version: u64,
    index_inputs: Vec<IndexPageTokenInput>,
    authz_revision: u64,
    caller_principal_hash: String,
    query_hash: String,
    predicate_hash: String,
    order_hash: String,
    last_source_identity: String,
    #[serde(default)]
    last_sort_values: BTreeMap<String, JsonValue>,
    expires_at: String,
    signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct IndexPageTokenInput {
    index_id: String,
    definition_hash: String,
    generation: u64,
}

#[derive(Debug, Clone)]
struct IndexPageTokenBinding {
    token_kind: String,
    mesh_id: String,
    anvil_storage_tenant_id: String,
    authz_realm_id: String,
    tenant_id: i64,
    bucket_name: String,
    index_name: String,
    index_generation: u64,
    index_definition_version: u64,
    index_inputs: Vec<IndexPageTokenInput>,
    authz_revision: u64,
    caller_principal_hash: String,
    query_hash: String,
    predicate_hash: String,
    order_hash: String,
}

impl IndexPageToken {
    fn for_cursor(
        binding: &IndexPageTokenBinding,
        last_source_identity: String,
        last_sort_values: BTreeMap<String, JsonValue>,
    ) -> Self {
        Self {
            version: INDEX_PAGE_TOKEN_VERSION,
            token_kind: binding.token_kind.clone(),
            mesh_id: binding.mesh_id.clone(),
            anvil_storage_tenant_id: binding.anvil_storage_tenant_id.clone(),
            authz_realm_id: binding.authz_realm_id.clone(),
            tenant_id: binding.tenant_id,
            bucket_name: binding.bucket_name.clone(),
            index_name: binding.index_name.clone(),
            index_generation: binding.index_generation,
            index_definition_version: binding.index_definition_version,
            index_inputs: binding.index_inputs.clone(),
            authz_revision: binding.authz_revision,
            caller_principal_hash: binding.caller_principal_hash.clone(),
            query_hash: binding.query_hash.clone(),
            predicate_hash: binding.predicate_hash.clone(),
            order_hash: binding.order_hash.clone(),
            last_source_identity,
            last_sort_values,
            expires_at: (chrono::Utc::now()
                + chrono::Duration::seconds(INDEX_PAGE_TOKEN_TTL_SECONDS))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            signature: String::new(),
        }
    }

    fn decode(raw: &str, signing_key: &[u8]) -> Result<Option<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        let token: Self = serde_json::from_slice(&bytes)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        if token.version != INDEX_PAGE_TOKEN_VERSION {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        let expected = token.sign(signing_key)?;
        if !constant_time_eq::constant_time_eq(token.signature.as_bytes(), expected.as_bytes()) {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        Ok(Some(token))
    }

    fn encode(mut self, signing_key: &[u8]) -> Result<String, Status> {
        self.signature = self.sign(signing_key)?;
        let bytes = serde_json::to_vec(&self)
            .map_err(|e| Status::internal(format!("Serialize page token: {e}")))?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    fn validate(&self, binding: &IndexPageTokenBinding) -> Result<(), Status> {
        let expires_at = chrono::DateTime::parse_from_rfc3339(&self.expires_at)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?
            .with_timezone(&chrono::Utc);
        if expires_at <= chrono::Utc::now() {
            return Err(Status::invalid_argument("PageTokenExpired"));
        }
        if self.token_kind != binding.token_kind
            || self.mesh_id != binding.mesh_id
            || self.anvil_storage_tenant_id != binding.anvil_storage_tenant_id
            || self.authz_realm_id != binding.authz_realm_id
            || self.tenant_id != binding.tenant_id
            || self.bucket_name != binding.bucket_name
            || self.index_name != binding.index_name
            || self.index_generation != binding.index_generation
            || self.index_definition_version != binding.index_definition_version
            || self.index_inputs != binding.index_inputs
            || self.authz_revision != binding.authz_revision
            || self.caller_principal_hash != binding.caller_principal_hash
            || self.query_hash != binding.query_hash
            || self.predicate_hash != binding.predicate_hash
            || self.order_hash != binding.order_hash
        {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        Ok(())
    }

    fn sign(&self, signing_key: &[u8]) -> Result<String, Status> {
        let mut mac = HmacSha256::new_from_slice(signing_key)
            .map_err(|_| Status::internal("Invalid index page token signing key"))?;
        mac.update(INDEX_PAGE_TOKEN_DOMAIN);
        mac.update(&[self.version]);
        update_mac_part(&mut mac, self.token_kind.as_bytes());
        update_mac_part(&mut mac, self.mesh_id.as_bytes());
        update_mac_part(&mut mac, self.anvil_storage_tenant_id.as_bytes());
        update_mac_part(&mut mac, self.authz_realm_id.as_bytes());
        mac.update(&self.tenant_id.to_le_bytes());
        update_mac_part(&mut mac, self.bucket_name.as_bytes());
        update_mac_part(&mut mac, self.index_name.as_bytes());
        mac.update(&self.index_generation.to_le_bytes());
        mac.update(&self.index_definition_version.to_le_bytes());
        let index_inputs = serde_json::to_vec(&self.index_inputs)
            .map_err(|_| Status::internal("Failed to encode index page token index inputs"))?;
        update_mac_part(&mut mac, &index_inputs);
        mac.update(&self.authz_revision.to_le_bytes());
        update_mac_part(&mut mac, self.caller_principal_hash.as_bytes());
        update_mac_part(&mut mac, self.query_hash.as_bytes());
        update_mac_part(&mut mac, self.predicate_hash.as_bytes());
        update_mac_part(&mut mac, self.order_hash.as_bytes());
        update_mac_part(&mut mac, self.last_source_identity.as_bytes());
        let sort_values = serde_json::to_vec(&self.last_sort_values)
            .map_err(|_| Status::internal("Failed to encode index page token sort values"))?;
        update_mac_part(&mut mac, &sort_values);
        update_mac_part(&mut mac, self.expires_at.as_bytes());
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }
}

impl IndexPageTokenBinding {
    fn single_index(
        config: &Config,
        claims: &auth::Claims,
        token_kind: &str,
        bucket_name: &str,
        index_name: &str,
        index_generation: u64,
        index_definition_version: u64,
        authz_revision: u64,
        predicate_hash: String,
        order_hash: String,
    ) -> Self {
        let index_inputs = vec![IndexPageTokenInput {
            index_id: index_name.to_string(),
            definition_hash: stable_string_hash(&format!(
                "{index_name}:{index_definition_version}"
            )),
            generation: index_generation,
        }];
        Self::with_index_inputs(
            config,
            claims,
            token_kind,
            bucket_name,
            index_name,
            index_generation,
            index_definition_version,
            index_inputs,
            authz_revision,
            predicate_hash,
            order_hash,
        )
    }

    fn with_index_inputs(
        config: &Config,
        claims: &auth::Claims,
        token_kind: &str,
        bucket_name: &str,
        index_name: &str,
        index_generation: u64,
        index_definition_version: u64,
        index_inputs: Vec<IndexPageTokenInput>,
        authz_revision: u64,
        predicate_hash: String,
        order_hash: String,
    ) -> Self {
        let anvil_storage_tenant_id = claims.tenant_id.to_string();
        let authz_realm_id = DEFAULT_AUTHZ_REALM_ID.to_string();
        let caller_principal_hash = stable_string_hash(&claims.sub);
        let query_hash = stable_string_hash(
            &serde_json::json!({
                "token_kind": token_kind,
                "mesh_id": config.mesh_id,
                "anvil_storage_tenant_id": anvil_storage_tenant_id,
                "authz_realm_id": authz_realm_id,
                "tenant_id": claims.tenant_id,
                "bucket_name": bucket_name,
                "index_name": index_name,
                "index_generation": index_generation,
                "index_definition_version": index_definition_version,
                "index_inputs": index_inputs,
                "authz_revision": authz_revision,
                "caller_principal_hash": caller_principal_hash,
                "predicate_hash": predicate_hash,
                "order_hash": order_hash,
            })
            .to_string(),
        );
        Self {
            token_kind: token_kind.to_string(),
            mesh_id: config.mesh_id.clone(),
            anvil_storage_tenant_id,
            authz_realm_id,
            tenant_id: claims.tenant_id,
            bucket_name: bucket_name.to_string(),
            index_name: index_name.to_string(),
            index_generation,
            index_definition_version,
            index_inputs,
            authz_revision,
            caller_principal_hash,
            query_hash,
            predicate_hash,
            order_hash,
        }
    }
}

fn update_mac_part(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_le_bytes());
    mac.update(value);
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

fn stable_string_hash(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex().to_string()
}

fn metadata_backed_predicate_hash(index_kind: &str, req: &QueryIndexRequest) -> String {
    let shape = serde_json::json!({
        "index_kind": index_kind,
        "path_prefix": req.path_prefix,
        "metadata_filters_hash": stable_json_hash(&req.metadata_filters_json),
    });
    blake3::hash(shape.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn score_based_predicate_hash(index_kind: &str, req: &QueryIndexRequest) -> String {
    let query_vector_bits = req
        .query_vector
        .iter()
        .map(|value| value.to_bits())
        .collect::<Vec<_>>();
    let shape = serde_json::json!({
        "index_kind": index_kind,
        "query_text": req.query_text,
        "query_vector_bits": query_vector_bits,
        "phrase": req.phrase,
        "path_prefix": req.path_prefix,
        "metadata_filters_hash": stable_json_hash(&req.metadata_filters_json),
        "typed_predicates_hash": stable_json_hash(&req.typed_predicates_json),
    });
    blake3::hash(shape.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn score_order_hash() -> String {
    stable_string_hash("score:desc,object_version_id:asc")
}

fn score_sort_values(score: f32, object_version_id: &str) -> BTreeMap<String, JsonValue> {
    let mut values = BTreeMap::new();
    values.insert(
        "score".to_string(),
        serde_json::Number::from_f64(f64::from(score))
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
    );
    values.insert(
        "object_version_id".to_string(),
        JsonValue::String(object_version_id.to_string()),
    );
    values
}

fn score_after_cursor(
    score: f32,
    object_version_id: &str,
    token: &IndexPageToken,
) -> Result<bool, Status> {
    let cursor_score = token
        .last_sort_values
        .get("score")
        .and_then(JsonValue::as_f64)
        .ok_or_else(|| Status::invalid_argument("InvalidPageToken"))? as f32;
    let cursor_object_version_id = token
        .last_sort_values
        .get("object_version_id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| Status::invalid_argument("InvalidPageToken"))?;
    Ok(score
        .partial_cmp(&cursor_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .is_lt()
        || (score == cursor_score && object_version_id > cursor_object_version_id))
}

fn compare_score_hits(
    left_score: f32,
    left_object_version_id: &str,
    right_score: f32,
    right_object_version_id: &str,
) -> std::cmp::Ordering {
    right_score
        .partial_cmp(&left_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left_object_version_id.cmp(right_object_version_id))
}

fn object_key_after_cursor(
    object_key: &str,
    source_identity: &str,
    cursor_values: &BTreeMap<String, JsonValue>,
    cursor_source_identity: &str,
) -> bool {
    let cursor_key = cursor_values
        .get("object_key")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    object_key
        .cmp(cursor_key)
        .then(source_identity.cmp(cursor_source_identity))
        .is_gt()
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

fn validate_index_definition_shape(
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

fn validate_no_external_vector_extractor(extractor: &JsonValue) -> Result<(), Status> {
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

fn validate_vector_embedding_provider(
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

    #[test]
    fn index_page_token_binds_principal_mesh_authz_and_index_inputs() {
        let config = Config {
            mesh_id: "mesh-test".to_string(),
            ..Config::default()
        };
        let claims = auth::Claims {
            sub: "principal-a".to_string(),
            exp: 0,
            scopes: vec!["*|*".to_string()],
            tenant_id: 42,
            jti: Some("token-a".to_string()),
        };
        let binding = IndexPageTokenBinding::single_index(
            &config,
            &claims,
            "typed_json",
            "bucket-a",
            "idx-a",
            7,
            3,
            11,
            "predicate-a".to_string(),
            "order-a".to_string(),
        );
        let signing_key = b"page-token-test-key";
        let encoded = IndexPageToken::for_cursor(
            &binding,
            "source-a".to_string(),
            BTreeMap::from([("field".to_string(), JsonValue::String("v".to_string()))]),
        )
        .encode(signing_key)
        .unwrap();
        let decoded = IndexPageToken::decode(&encoded, signing_key)
            .unwrap()
            .expect("decoded token");
        decoded.validate(&binding).unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(&decoded.expires_at)
                .unwrap()
                .with_timezone(&chrono::Utc)
                > chrono::Utc::now(),
            "RFC 0006 page tokens must carry an authenticated expiry"
        );

        let mut other_principal = claims.clone();
        other_principal.sub = "principal-b".to_string();
        let other_principal_binding = IndexPageTokenBinding::single_index(
            &config,
            &other_principal,
            "typed_json",
            "bucket-a",
            "idx-a",
            7,
            3,
            11,
            "predicate-a".to_string(),
            "order-a".to_string(),
        );
        assert!(decoded.validate(&other_principal_binding).is_err());

        let mut other_mesh = config.clone();
        other_mesh.mesh_id = "mesh-other".to_string();
        let other_mesh_binding = IndexPageTokenBinding::single_index(
            &other_mesh,
            &claims,
            "typed_json",
            "bucket-a",
            "idx-a",
            7,
            3,
            11,
            "predicate-a".to_string(),
            "order-a".to_string(),
        );
        assert!(decoded.validate(&other_mesh_binding).is_err());

        let other_generation_binding = IndexPageTokenBinding::single_index(
            &config,
            &claims,
            "typed_json",
            "bucket-a",
            "idx-a",
            8,
            3,
            11,
            "predicate-a".to_string(),
            "order-a".to_string(),
        );
        assert!(decoded.validate(&other_generation_binding).is_err());

        let expired = IndexPageToken {
            expires_at: (chrono::Utc::now() - chrono::Duration::seconds(1))
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            ..IndexPageToken::for_cursor(
                &binding,
                "source-a".to_string(),
                BTreeMap::from([("field".to_string(), JsonValue::String("v".to_string()))]),
            )
        }
        .encode(signing_key)
        .unwrap();
        let decoded_expired = IndexPageToken::decode(&expired, signing_key)
            .unwrap()
            .expect("decoded expired token");
        let expired_err = decoded_expired.validate(&binding).unwrap_err();
        assert_eq!(expired_err.message(), "PageTokenExpired");
    }

    #[test]
    fn vector_definition_rejects_external_extractor() {
        let err = validate_index_definition_shape(
            "vector",
            &test_vector_definition("configured_provider", 4),
            &serde_json::json!({"kind": "object_body_utf8"}),
            &test_config(true),
        )
        .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("build_policy_json"));
    }

    #[test]
    fn vector_text_extractor_allows_test_only_only_when_enabled() {
        let policy = test_vector_definition("test_only", 4);
        let extractor = serde_json::json!({});

        assert!(
            validate_index_definition_shape("vector", &policy, &extractor, &test_config(true))
                .is_ok()
        );
        let err =
            validate_index_definition_shape("vector", &policy, &extractor, &test_config(false))
                .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("disabled"));
    }

    fn test_config(allow_test_only: bool) -> Config {
        Config {
            allow_test_only_embedding_provider: allow_test_only,
            ..Config::default()
        }
    }

    fn test_vector_definition(provider: &str, dimension: u16) -> serde_json::Value {
        serde_json::json!({
            "schema": crate::formats::vector::VECTOR_INDEX_SCHEMA,
            "source": {"kind": "object_current", "prefix": "docs/"},
            "extractor": {"kind": "object_body_utf8"},
            "embedding": {
                "provider": provider,
                "model": "test-text-embedding",
                "dimension": dimension,
                "modality": "text",
                "normalisation": "unit_l2",
                "chunking": {"strategy": "whole_object"}
            },
            "ann": {
                "algorithm": "hnsw",
                "metric": "cosine"
            }
        })
    }
}
