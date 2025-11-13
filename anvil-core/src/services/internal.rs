use crate::anvil_api::internal_anvil_service_server::InternalAnvilService;
use crate::anvil_api::*;
use crate::{AppState, auth};
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

        let mut stream = request.into_inner();

        let (upload_id, shard_index, first_chunk_data) =
            if let Some(Ok(chunk)) = stream.next().await {
                (chunk.upload_id, chunk.shard_index, chunk.data)
            } else {
                return Err(Status::invalid_argument("Empty stream"));
            };

        let resource = format!("{}/{}", upload_id, shard_index);
        if !auth::is_authorized(&format!("internal:put_shard:{}", resource), &claims.scopes) {
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
        let req = request.into_inner();

        let resource = format!("{}/{}", req.final_object_hash, req.shard_index);
        if !auth::is_authorized(
            &format!("internal:commit_shard:{}", resource),
            &claims.scopes,
        ) {
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
        let req = request.into_inner();
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
        let req = request.into_inner();

        let resource = format!(
            "internal:delete_shard:{}/{}",
            req.object_hash, req.shard_index
        );
        if !auth::is_authorized(&resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        self.storage
            .delete_shard(&req.object_hash, req.shard_index)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DeleteShardResponse {}))
    }
}
