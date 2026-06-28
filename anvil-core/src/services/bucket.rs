use crate::anvil_api::bucket_service_server::BucketService;
use crate::anvil_api::*;
use crate::{AppState, auth, bucket_journal, permissions::AnvilAction, validation};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl BucketService for AppState {
    type WatchBucketMetadataStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchBucketMetadataResponse, Status>> + Send>,
    >;

    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        tracing::debug!("[service] ENTERING create_bucket");
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        let req = request.get_ref();

        let bucket = self
            .bucket_manager
            .create_bucket(
                claims.tenant_id,
                &req.bucket_name,
                &req.region,
                &claims.scopes,
            )
            .await?;
        self.publish_bucket_metadata_event(claims.tenant_id, &bucket, "create", false)
            .await?;

        tracing::debug!("[service] EXITING create_bucket");
        Ok(Response::new(CreateBucketResponse {
            bucket_id: bucket.id,
        }))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let bucket = self
            .bucket_manager
            .delete_bucket(claims.tenant_id, &req.bucket_name, &claims.scopes)
            .await?;
        self.publish_bucket_metadata_event(claims.tenant_id, &bucket, "delete", true)
            .await?;

        Ok(Response::new(DeleteBucketResponse {}))
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        tracing::debug!("[service] ENTERING list_buckets");
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        let buckets = self
            .bucket_manager
            .list_buckets(claims.tenant_id, &claims.scopes)
            .await?;

        let response_buckets: Vec<crate::anvil_api::Bucket> = buckets
            .into_iter()
            .map(|b| crate::anvil_api::Bucket {
                name: b.name,
                creation_date: b.created_at.to_string(),
                region: b.region,
                is_public_read: b.is_public_read,
                deleted: false,
                bucket_id: b.id,
            })
            .collect();

        tracing::debug!(
            "[service] EXITING list_buckets, found {} buckets",
            response_buckets.len()
        );
        Ok(Response::new(ListBucketsResponse {
            buckets: response_buckets,
        }))
    }

    async fn get_bucket_policy(
        &self,
        request: Request<GetBucketPolicyRequest>,
    ) -> Result<Response<GetBucketPolicyResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let policy = self
            .bucket_manager
            .get_bucket_policy(claims.tenant_id, &req.bucket_name, &claims.scopes)
            .await?;

        Ok(Response::new(GetBucketPolicyResponse {
            policy_json: policy.to_string(),
        }))
    }

    async fn put_bucket_policy(
        &self,
        request: Request<PutBucketPolicyRequest>,
    ) -> Result<Response<PutBucketPolicyResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        // A bit of a hack: we only support is_public_read for now.
        let policy: serde_json::Value = serde_json::from_str(&req.policy_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid policy JSON: {}", e)))?;
        let is_public_read = policy["is_public_read"].as_bool().unwrap_or(false);

        let bucket = self
            .bucket_manager
            .set_bucket_public_access(
                claims.tenant_id,
                &req.bucket_name,
                is_public_read,
                &claims.scopes,
            )
            .await?;
        self.publish_bucket_metadata_event(claims.tenant_id, &bucket, "policy_update", false)
            .await?;

        Ok(Response::new(PutBucketPolicyResponse {}))
    }

    async fn watch_bucket_metadata(
        &self,
        request: Request<WatchBucketMetadataRequest>,
    ) -> Result<Response<Self::WatchBucketMetadataStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !req.bucket_name.is_empty() && !validation::is_valid_bucket_name(&req.bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        let resource = if req.bucket_name.is_empty() {
            "*"
        } else {
            req.bucket_name.as_str()
        };
        if !auth::is_authorized(AnvilAction::BucketWatch, resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let snapshot = bucket_journal::list_bucket_metadata_events(
            &self.storage,
            claims.tenant_id,
            &req.bucket_name,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut live = self.bucket_watch_tx.subscribe();

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.id);
                if tx
                    .send(bucket_metadata_event_response(&event))
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
                            || event.id <= last_cursor
                            || (!req.bucket_name.is_empty() && event.bucket_name != req.bucket_name)
                        {
                            continue;
                        }
                        last_cursor = event.id;
                        if tx
                            .send(bucket_metadata_event_response(&event))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = tx
                            .send(Err(Status::data_loss(
                                "Bucket metadata watch fell behind retained live event window",
                            )))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchBucketMetadataStream
        ))
    }
}

impl AppState {
    async fn publish_bucket_metadata_event(
        &self,
        tenant_id: i64,
        bucket: &crate::persistence::Bucket,
        event_type: &str,
        deleted: bool,
    ) -> Result<crate::persistence::BucketMetadataEvent, Status> {
        let event =
            bucket_journal::latest_bucket_metadata_event(&self.storage, tenant_id, &bucket.name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::internal("Bucket metadata journal event not found"))?;
        if event.event_type != event_type {
            tracing::debug!(
                expected = event_type,
                actual = event.event_type,
                deleted,
                "bucket metadata journal event type differs from service hint"
            );
        }
        let _ = self.bucket_watch_tx.send(event.clone());
        Ok(event)
    }
}

fn bucket_metadata_event_response(
    event: &crate::persistence::BucketMetadataEvent,
) -> Result<WatchBucketMetadataResponse, Status> {
    Ok(WatchBucketMetadataResponse {
        cursor: u64::try_from(event.id).map_err(|_| Status::internal("Invalid watch cursor"))?,
        event_type: event.event_type.clone(),
        bucket: Some(bucket_from_metadata(&event.bucket_metadata)?),
        emitted_at: event.created_at.to_string(),
    })
}

fn bucket_from_metadata(value: &JsonValue) -> Result<Bucket, Status> {
    Ok(Bucket {
        bucket_id: value
            .get("bucket_id")
            .and_then(JsonValue::as_i64)
            .ok_or_else(|| Status::internal("Malformed bucket metadata event"))?,
        name: json_string_field(value, "name")?,
        creation_date: json_string_field(value, "creation_date")?,
        region: json_string_field(value, "region")?,
        is_public_read: value
            .get("is_public_read")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| Status::internal("Malformed bucket metadata event"))?,
        deleted: value
            .get("deleted")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| Status::internal("Malformed bucket metadata event"))?,
    })
}

fn json_string_field(value: &JsonValue, name: &str) -> Result<String, Status> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| Status::internal("Malformed bucket metadata event"))
}
