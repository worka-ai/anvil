use crate::anvil_api::internal_anvil_service_server::InternalAnvilService;
use crate::anvil_api::*;
use crate::{AppState, auth, permissions::AnvilAction};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl InternalAnvilService for AppState {
    type GetShardStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<GetShardResponse, Status>> + Send>,
    >;

    async fn put_shard(
        &self,
        request: Request<tonic::Streaming<PutShardRequest>>,
    ) -> Result<Response<PutShardResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?
            .clone();
        validate_internal_claims(&claims)?;

        let mut stream = request.into_inner();

        let (upload_id, shard_index, first_chunk_data) =
            if let Some(Ok(chunk)) = stream.next().await {
                (chunk.upload_id, chunk.shard_index, chunk.data)
            } else {
                return Err(Status::invalid_argument("Empty stream"));
            };

        let resource = format!("{}/{}", upload_id, shard_index);
        if !auth::is_authorized(AnvilAction::InternalPutShard, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let mut data = first_chunk_data;
        while let Some(Ok(chunk)) = stream.next().await {
            data.extend_from_slice(&chunk.data);
        }

        self.storage
            .store_temp_shard(&upload_id, shard_index, &data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutShardResponse {}))
    }

    async fn commit_shard(
        &self,
        request: Request<CommitShardRequest>,
    ) -> Result<Response<CommitShardResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?
            .clone();
        validate_internal_claims(&claims)?;
        let req = request.into_inner();

        let resource = format!("{}/{}", req.final_object_hash, req.shard_index);
        if !auth::is_authorized(AnvilAction::InternalCommitShard, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        self.storage
            .commit_shard(&req.upload_id, req.shard_index, &req.final_object_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CommitShardResponse {}))
    }

    async fn get_shard(
        &self,
        request: Request<GetShardRequest>,
    ) -> Result<Response<Self::GetShardStream>, Status> {
        let (_metadata, extensions, req) = request.into_parts();

        let claims = extensions
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        validate_internal_claims(claims)?;

        let resource = format!("{}/{}", req.object_hash, req.shard_index);
        if !auth::is_authorized(AnvilAction::InternalGetShard, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let data = self
            .storage
            .retrieve_shard(&req.object_hash, req.shard_index)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            for chunk in data.chunks(1024 * 1024) {
                // 1MB chunks
                tx.send(Ok(GetShardResponse {
                    data: chunk.to_vec(),
                }))
                .await
                .unwrap();
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::GetShardStream
        ))
    }

    async fn delete_shard(
        &self,
        request: Request<DeleteShardRequest>,
    ) -> Result<Response<DeleteShardResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?
            .clone();
        validate_internal_claims(&claims)?;
        let req = request.into_inner();

        let resource = format!("{}/{}", req.object_hash, req.shard_index);
        if !auth::is_authorized(AnvilAction::InternalDeleteShard, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        self.storage
            .delete_shard(&req.object_hash, req.shard_index)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DeleteShardResponse {}))
    }
}

fn validate_internal_claims(claims: &auth::Claims) -> Result<(), Status> {
    if claims.tenant_id == 0 && (claims.sub == "internal" || claims.sub == "internal-worker") {
        return Ok(());
    }
    Err(Status::permission_denied(
        "Internal service requires a node-issued token",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn claims(sub: &str, tenant_id: i64) -> auth::Claims {
        auth::Claims {
            sub: sub.to_string(),
            exp: usize::MAX,
            scopes: vec!["*|*".to_string()],
            tenant_id,
        }
    }

    #[test]
    fn internal_service_accepts_only_node_issued_claims() {
        validate_internal_claims(&claims("internal", 0)).expect("internal token should pass");
        validate_internal_claims(&claims("internal-worker", 0))
            .expect("internal worker token should pass");

        let tenant_app = validate_internal_claims(&claims("customer-app", 42))
            .expect_err("tenant app must not call internal service");
        assert_eq!(tenant_app.code(), Code::PermissionDenied);

        let tenant_zero_app = validate_internal_claims(&claims("customer-app", 0))
            .expect_err("tenant-zero non-internal subject must not call internal service");
        assert_eq!(tenant_zero_app.code(), Code::PermissionDenied);
    }
}
