use crate::anvil_api::git_source_service_server::GitSourceService;
use crate::anvil_api::*;
use crate::{
    AppState, auth, git_source_index, git_source_query, git_source_watch, permissions::AnvilAction,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl GitSourceService for AppState {
    type WatchGitSourceStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchGitSourceResponse, Status>> + Send>,
    >;

    async fn get_git_object(
        &self,
        request: Request<GetGitObjectRequest>,
    ) -> Result<Response<GetGitObjectResponse>, Status> {
        let claims = authorize_git_source_read(&request)?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        ensure_git_source_read(&claims, &req.repository_id)?;
        let object_id = parse_git_hex_id("object_id", &req.object_id)?;
        let index = self
            .latest_git_source_index(claims.tenant_id, &req.repository_id)
            .await?;
        let locations = git_source_query::get_git_object(&index, &object_id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?
            .into_iter()
            .map(git_blob_location)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(GetGitObjectResponse { locations }))
    }

    async fn get_git_blob_by_path(
        &self,
        request: Request<GetGitBlobByPathRequest>,
    ) -> Result<Response<GetGitBlobByPathResponse>, Status> {
        let claims = authorize_git_source_read(&request)?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        ensure_git_source_read(&claims, &req.repository_id)?;
        let commit_id = parse_git_hex_id("commit_id", &req.commit_id)?;
        let index = self
            .latest_git_source_index(claims.tenant_id, &req.repository_id)
            .await?;
        let location = git_source_query::get_git_blob_by_path(&index, &commit_id, &req.tree_path)
            .map_err(|err| Status::invalid_argument(err.to_string()))?
            .ok_or_else(|| Status::not_found("Git blob path not found"))?;

        Ok(Response::new(GetGitBlobByPathResponse {
            location: Some(git_blob_location(location)?),
        }))
    }

    async fn list_git_tree(
        &self,
        request: Request<ListGitTreeRequest>,
    ) -> Result<Response<ListGitTreeResponse>, Status> {
        let claims = authorize_git_source_read(&request)?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        ensure_git_source_read(&claims, &req.repository_id)?;
        let commit_id = parse_git_hex_id("commit_id", &req.commit_id)?;
        let index = self
            .latest_git_source_index(claims.tenant_id, &req.repository_id)
            .await?;
        let mut entries = git_source_query::list_git_tree(&index, &commit_id, &req.prefix)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let limit = usize::try_from(req.limit)
            .map_err(|_| Status::invalid_argument("limit exceeds supported range"))?;
        if limit > 0 && entries.len() > limit {
            entries.truncate(limit);
        }

        Ok(Response::new(ListGitTreeResponse {
            entries: entries
                .into_iter()
                .map(git_tree_entry_record)
                .collect::<Result<Vec<_>, _>>()?,
        }))
    }

    async fn watch_git_source(
        &self,
        request: Request<WatchGitSourceRequest>,
    ) -> Result<Response<Self::WatchGitSourceStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        let resource = git_source_resource(&req.repository_id);
        if !auth::is_authorized(AnvilAction::GitSourceWatch, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let snapshot = git_source_watch::list_git_source_watch_events(
            &self.storage,
            claims.tenant_id,
            &req.repository_id,
            after_cursor,
            1000,
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;

        let storage = self.storage.clone();
        let repository_id = req.repository_id;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.cursor);
                if tx.send(Ok(watch_git_source_response(event))).await.is_err() {
                    return;
                }
            }

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let events = match git_source_watch::list_git_source_watch_events(
                    &storage,
                    tenant_id,
                    &repository_id,
                    last_cursor,
                    1000,
                )
                .await
                {
                    Ok(events) => events,
                    Err(err) => {
                        let _ = tx.send(Err(Status::internal(err.to_string()))).await;
                        return;
                    }
                };
                for event in events {
                    last_cursor = last_cursor.max(event.cursor);
                    if tx.send(Ok(watch_git_source_response(event))).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchGitSourceStream
        ))
    }
}

impl AppState {
    async fn latest_git_source_index(
        &self,
        tenant_id: i64,
        repository_id: &str,
    ) -> Result<git_source_index::DecodedGitSourceIndex, Status> {
        git_source_query::read_latest_git_source_index(&self.storage, tenant_id, repository_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("Git source index not found"))
    }
}

fn authorize_git_source_read<T>(request: &Request<T>) -> Result<auth::Claims, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    Ok(claims)
}

fn ensure_git_source_read(claims: &auth::Claims, repository_id: &str) -> Result<(), Status> {
    let resource = git_source_resource(repository_id);
    if auth::is_authorized(AnvilAction::GitSourceRead, &resource, &claims.scopes) {
        Ok(())
    } else {
        Err(Status::permission_denied("Permission denied"))
    }
}

fn git_source_resource(repository_id: &str) -> String {
    format!("repository:{repository_id}")
}

fn validate_component(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{name} must not be empty"
        )));
    }
    if value == "." || value == ".." || value.contains('/') || value.chars().any(char::is_control) {
        return Err(Status::invalid_argument(format!(
            "{name} must be a safe path component"
        )));
    }
    Ok(())
}

fn parse_git_hex_id(name: &str, value: &str) -> Result<Vec<u8>, Status> {
    if value.len() != 40 && value.len() != 64 {
        return Err(Status::invalid_argument(format!(
            "{name} must be a SHA-1 or SHA-256 hex object id"
        )));
    }
    hex::decode(value).map_err(|_| Status::invalid_argument(format!("{name} must be hex")))
}

fn git_blob_location(
    location: git_source_query::GitObjectLookup,
) -> Result<GitBlobLocation, Status> {
    Ok(GitBlobLocation {
        repository_id: location.repository_id,
        commit_id: hex::encode(location.commit_id),
        object_id: hex::encode(location.object_id),
        tree_path: location.tree_path,
        blob_start: location.blob_start,
        blob_len: location.blob_len,
        pack_object_version_id: uuid::Uuid::from_bytes(location.pack_object_version_id).to_string(),
    })
}

fn git_tree_entry_record(
    entry: git_source_query::GitTreeEntry,
) -> Result<GitTreeEntryRecord, Status> {
    Ok(GitTreeEntryRecord {
        tree_path: entry.tree_path,
        object_id: hex::encode(entry.object_id),
        blob_start: entry.blob_start,
        blob_len: entry.blob_len,
        pack_object_version_id: uuid::Uuid::from_bytes(entry.pack_object_version_id).to_string(),
    })
}

fn watch_git_source_response(
    event: git_source_watch::GitSourceWatchEvent,
) -> WatchGitSourceResponse {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    WatchGitSourceResponse {
        cursor_low,
        cursor_high,
        repository_id: event.payload.repository_id,
        event_type: event.payload.event_type,
        generation: event.payload.generation,
        source_hash: event.payload.source_hash,
        index_path: event.payload.index_path,
        pack_object_version_id: event.payload.pack_object_version_id.unwrap_or_default(),
        authz_revision: event.authz_revision,
        emitted_at: event.payload.emitted_at,
    }
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_source_watch_cursor_split_round_trips() {
        let cursor = (u128::from(77_u64) << 64) | u128::from(33_u64);
        let (low, high) = split_u128(cursor);
        assert_eq!(join_u128(low, high), cursor);
    }
}
