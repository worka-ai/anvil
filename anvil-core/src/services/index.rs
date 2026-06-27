use crate::anvil_api::index_service_server::IndexService;
use crate::anvil_api::*;
use crate::{AppState, auth, permissions::AnvilAction, validation};
use serde_json::{Value as JsonValue, json};
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
        self.publish_index_definition_event(claims.tenant_id, &bucket, &index, "create")
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
        self.publish_index_definition_event(claims.tenant_id, &bucket, &index, "update")
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
        self.publish_index_definition_event(claims.tenant_id, &bucket, &index, "disable")
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
        self.publish_index_definition_event(claims.tenant_id, &bucket, &index, "drop")
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
        let indexes = self
            .db
            .list_index_definitions(claims.tenant_id, bucket.id, req.include_disabled)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(|index| index_record(&bucket.name, index))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListIndexesResponse { indexes }))
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
        let snapshot = self
            .db
            .list_index_definition_events(claims.tenant_id, bucket.id, after_cursor, 1000)
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
        self.db
            .get_bucket_by_name(tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))
    }

    async fn publish_index_definition_event(
        &self,
        tenant_id: i64,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        event_type: &str,
    ) -> Result<crate::persistence::IndexDefinitionEvent, Status> {
        let event = self
            .db
            .create_index_definition_event(
                tenant_id,
                bucket.id,
                &bucket.name,
                index,
                event_type,
                index_definition_json(&bucket.name, index),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.index_watch_tx.send(event.clone());
        Ok(event)
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

fn index_definition_json(
    bucket_name: &str,
    index: &crate::persistence::IndexDefinition,
) -> JsonValue {
    json!({
        "index_id": index.id,
        "bucket_name": bucket_name,
        "name": index.name,
        "kind": index.kind,
        "selector_json": index.selector.to_string(),
        "extractor_json": index.extractor.to_string(),
        "authorization_mode": index.authorization_mode,
        "build_policy_json": index.build_policy.to_string(),
        "enabled": index.enabled,
        "version": index.version,
        "created_at": index.created_at.to_string(),
        "updated_at": index.updated_at.to_string(),
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
