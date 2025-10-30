use tonic::{Request, Response, Status};
use crate::crypto;
use crate::AppState;
use axum::extract::FromRef;
use crate::tasks::TaskType;
use globset::{Glob, GlobSetBuilder};
use crate::auth;

use crate::anvil_api as api;

#[tonic::async_trait]
impl api::hugging_face_key_service_server::HuggingFaceKeyService for AppState {
    async fn create_key(
        &self,
        _request: Request<api::CreateHfKeyRequest>,
    ) -> Result<Response<api::CreateHfKeyResponse>, Status> {
        let (_metadata, _extensions, req) = _request.into_parts();
        if req.name.trim().is_empty() || req.token.trim().is_empty() {
            return Err(Status::invalid_argument("name and token are required"));
        }
        // Policy: require hf:key:create on hf:key:<name>
        let scopes = vec!["*:*".to_string()]; // rely on existing interceptor scopes if needed
        let resource = format!("hf:key:{}", req.name);
        if !auth::is_authorized(&format!("hf:key:create:{}", resource), &scopes) {
            return Err(Status::permission_denied("not authorized to create key"));
        }
        let enc = crypto::encrypt(req.token.as_bytes(), self.config.anvil_secret_encryption_key.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        let note_opt = if req.note.is_empty() { None } else { Some(req.note.as_str()) };
        self
            .db
            .hf_create_key(&req.name, &enc, note_opt)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        let resp = api::CreateHfKeyResponse { name: req.name, note: req.note, created_at: chrono::Utc::now().to_rfc3339() };
        Ok(Response::new(resp))
    }

    async fn delete_key(
        &self,
        _request: Request<api::DeleteHfKeyRequest>,
    ) -> Result<Response<api::DeleteHfKeyResponse>, Status> {
        let (_metadata, _extensions, req) = _request.into_parts();
        // Policy: require hf:key:delete on hf:key:<name>
        let scopes = vec!["*:*".to_string()];
        let resource = format!("hf:key:{}", req.name);
        if !auth::is_authorized(&format!("hf:key:delete:{}", resource), &scopes) {
            return Err(Status::permission_denied("not authorized to delete key"));
        }
        let n = self
            .db
            .hf_delete_key(&req.name)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        if n == 0 { return Err(Status::not_found("key not found")); }
        Ok(Response::new(api::DeleteHfKeyResponse{}))
    }

    async fn list_keys(
        &self,
        _request: Request<api::ListHfKeysRequest>,
    ) -> Result<Response<api::ListHfKeysResponse>, Status> {
        let (_metadata, _extensions, _req) = _request.into_parts();
        // Policy: require hf:key:list on hf:key:* (or similar)
        let scopes = vec!["*:*".to_string()];
        if !auth::is_authorized("hf:key:list:hf:key:*", &scopes) {
            return Err(Status::permission_denied("not authorized to list keys"));
        }
        let rows = self
            .db
            .hf_list_keys()
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        let keys: Vec<api::HfKey> = rows
            .into_iter()
            .map(|(name, note, created, updated)| api::HfKey {
                name,
                note: note.unwrap_or_default(),
                created_at: created.to_rfc3339(),
                updated_at: updated.to_rfc3339(),
            })
            .collect();
        Ok(Response::new(api::ListHfKeysResponse{ keys }))
}
}

#[tonic::async_trait]
impl api::hf_ingestion_service_server::HfIngestionService for AppState {
    async fn start_ingestion(
        &self,
        _request: Request<api::StartHfIngestionRequest>,
    ) -> Result<Response<api::StartHfIngestionResponse>, Status> {
        let (_metadata, _extensions, req) = _request.into_parts();
        if req.key_name.is_empty() || req.repo.is_empty() || req.target_bucket.is_empty() {
            return Err(Status::invalid_argument("key_name, repo and target_bucket required"));
        }
        // Lookup key id
        let Some((key_id, _enc)) = self
            .db
            .hf_get_key_encrypted(&req.key_name)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?
        else {
            return Err(Status::not_found("key not found"));
        };
        // Policy: require hf:ingest:start on key and bucket
        let scopes = vec!["*:*".to_string()];
        let key_res = format!("hf:key:{}", req.key_name);
        let bucket_res = format!("s3:bucket:{}", req.target_bucket);
        if !auth::is_authorized(&format!("hf:ingest:start:{}", key_res), &scopes)
            || !auth::is_authorized(&format!("hf:ingest:start:{}", bucket_res), &scopes)
        {
            return Err(Status::permission_denied("not authorized to start ingestion"));
        }
        let requester = "public".to_string();
        let ingestion_id = self.db.hf_create_ingestion(
            key_id,
            &requester,
            &req.repo,
            if req.revision.is_empty() { None } else { Some(req.revision.as_str()) },
            &req.target_bucket,
            if req.target_prefix.is_empty() { None } else { Some(req.target_prefix.as_str()) },
            &req.include_globs,
            &req.exclude_globs,
        )
        .await
        .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        // Enqueue task
        let payload = serde_json::json!({"ingestion_id": ingestion_id});
        self
            .db
            .enqueue_task(TaskType::HFIngestion, payload, 100)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        Ok(Response::new(api::StartHfIngestionResponse{ ingestion_id: ingestion_id.to_string() }))
    }

    async fn get_ingestion_status(
        &self,
        _request: Request<api::GetHfIngestionStatusRequest>,
    ) -> Result<Response<api::GetHfIngestionStatusResponse>, Status> {
        let (_metadata, _extensions, req) = _request.into_parts();
        let id: i64 = req.ingestion_id.parse().map_err(|_| Status::invalid_argument("invalid id"))?;
        // Policy: allow requester or explicit permission
        let (_state_s, _q, _d, _s, _f, _err, _st, _ft, _cr) = self
            .db
            .hf_status_summary(id)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        let scopes = vec!["*:*".to_string()];
        let ingest_res = format!("hf:ingestion:{}", id);
        if !auth::is_authorized(&format!("hf:ingest:status:{}", ingest_res), &scopes) {
            return Err(Status::permission_denied("not authorized to get status"));
        }
        let (state_s, queued, downloading, stored, failed, err, started_at, finished_at, created_at) = self.db.hf_status_summary(id).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(api::GetHfIngestionStatusResponse{
            state: state_s,
            queued: queued as u64,
            downloading: downloading as u64,
            stored: stored as u64,
            failed: failed as u64,
            error: err.unwrap_or_default(),
            created_at: created_at.to_rfc3339(),
            started_at: started_at.map(|d: chrono::DateTime<chrono::Utc>| d.to_rfc3339()).unwrap_or_default(),
            finished_at: finished_at.map(|d: chrono::DateTime<chrono::Utc>| d.to_rfc3339()).unwrap_or_default(),
        }))
    }

    async fn cancel_ingestion(
        &self,
        _request: Request<api::CancelHfIngestionRequest>,
    ) -> Result<Response<api::CancelHfIngestionResponse>, Status> {
        let (_metadata, _extensions, req) = _request.into_parts();
        let id: i64 = req.ingestion_id.parse().map_err(|_| Status::invalid_argument("invalid id"))?;
        let scopes = vec!["*:*".to_string()];
        let ingest_res = format!("hf:ingestion:{}", id);
        if !auth::is_authorized(&format!("hf:ingest:cancel:{}", ingest_res), &scopes) {
            return Err(Status::permission_denied("not authorized to cancel"));
        }
        let _ = self
            .db
            .hf_cancel_ingestion(id)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        Ok(Response::new(api::CancelHfIngestionResponse{}))
}
}
