use crate::anvil_api::git_source_service_server::GitSourceService;
use crate::anvil_api::*;
use crate::{AppState, auth, git_source_watch, permissions::AnvilAction};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl GitSourceService for AppState {
    type WatchGitSourceStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchGitSourceResponse, Status>> + Send>,
    >;

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
