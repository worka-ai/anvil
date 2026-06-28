use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::*;
use crate::{AppState, auth, authz_journal, crypto, permissions::AnvilAction};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl AuthService for AppState {
    type WatchAuthzTupleLogStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchAuthzTupleLogResponse, Status>> + Send>,
    >;

    async fn get_access_token(
        &self,
        request: Request<GetAccessTokenRequest>,
    ) -> Result<Response<GetAccessTokenResponse>, Status> {
        let req = request.into_inner();

        // 1. Verify credentials
        let app_details = self
            .persistence
            .get_app_by_client_id(&req.client_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::unauthenticated("Invalid client ID"))?;

        let encryption_key = hex::decode(&self.config.anvil_secret_encryption_key)
            .map_err(|_| Status::internal("Invalid encryption key format"))?;
        let decrypted_secret =
            crypto::decrypt(&app_details.client_secret_encrypted, &encryption_key)
                .map_err(|_| Status::unauthenticated("Invalid client secret"))?;

        if !constant_time_eq::constant_time_eq(
            decrypted_secret.as_slice(),
            req.client_secret.as_bytes(),
        ) {
            return Err(Status::unauthenticated("Invalid client secret"));
        }

        let allowed_scopes = self
            .persistence
            .get_policies_for_app(app_details.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let approved_scopes = if req.scopes.is_empty() || req.scopes == vec!["*"] {
            allowed_scopes
        } else {
            req.scopes
                .into_iter()
                .filter(|requested_scope| {
                    let parts: Vec<&str> = requested_scope.splitn(2, '|').collect();
                    if parts.len() != 2 {
                        return false;
                    }
                    if let Ok(action) = parts[0].parse::<AnvilAction>() {
                        auth::is_authorized(action, parts[1], &allowed_scopes)
                    } else {
                        false
                    }
                })
                .collect()
        };

        if approved_scopes.is_empty() {
            return Err(Status::permission_denied("App has no assigned policies"));
        }

        // 3. Mint token
        let token = self
            .jwt_manager
            .mint_token(
                app_details.id.to_string(),
                approved_scopes,
                app_details.tenant_id,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        tracing::info!(
            "[AuthService] Returning access token for app_id={}",
            app_details.id
        );
        Ok(Response::new(GetAccessTokenResponse {
            access_token: token,
            expires_in: 3600,
        }))
    }

    async fn grant_access(
        &self,
        request: Request<GrantAccessRequest>,
    ) -> Result<Response<GrantAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        if !auth::is_authorized(AnvilAction::PolicyGrant, &req.resource, &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to grant access to this resource",
            ));
        }

        let app = self
            .persistence
            .get_app_by_name(&req.grantee_app_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Grantee app not found"))?;
        self.persistence
            .grant_policy(app.id, &req.resource, &req.action)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GrantAccessResponse {}))
    }

    async fn revoke_access(
        &self,
        request: Request<RevokeAccessRequest>,
    ) -> Result<Response<RevokeAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        if !auth::is_authorized(AnvilAction::PolicyRevoke, &req.resource, &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to revoke access to this resource",
            ));
        }

        let app = self
            .persistence
            .get_app_by_name(&req.grantee_app_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Grantee app not found"))?;

        self.persistence
            .revoke_policy(app.id, &req.resource, &req.action)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RevokeAccessResponse {}))
    }

    async fn set_public_access(
        &self,
        request: Request<SetPublicAccessRequest>,
    ) -> Result<Response<SetPublicAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let resource = format!("bucket:{}", req.bucket);
        if !auth::is_authorized(AnvilAction::PolicyGrant, &resource, &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to modify public access on this bucket",
            ));
        }

        self.persistence
            .set_bucket_public_access(claims.tenant_id, &req.bucket, req.allow_public_read)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(SetPublicAccessResponse {}))
    }

    async fn write_authz_tuple(
        &self,
        request: Request<WriteAuthzTupleRequest>,
    ) -> Result<Response<WriteAuthzTupleResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_tuple_field("namespace", &req.namespace)?;
        validate_tuple_field("object_id", &req.object_id)?;
        validate_tuple_field("relation", &req.relation)?;
        validate_tuple_field("subject_kind", &req.subject_kind)?;
        validate_tuple_field("subject_id", &req.subject_id)?;
        let operation = match req.operation.as_str() {
            "add" | "remove" => req.operation.as_str(),
            _ => return Err(Status::invalid_argument("operation must be add or remove")),
        };
        let resource = authz_resource(&req.namespace, &req.object_id, &req.relation);
        if !auth::is_authorized(AnvilAction::AuthzTupleWrite, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let record = self
            .persistence
            .write_authz_tuple(
                claims.tenant_id,
                &req.namespace,
                &req.object_id,
                &req.relation,
                &req.subject_kind,
                &req.subject_id,
                &req.caveat_hash,
                operation,
                &claims.sub,
                &req.reason,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.authz_watch_tx.send(record.clone());

        Ok(Response::new(WriteAuthzTupleResponse {
            revision: revision_to_u64(record.revision)?,
            zookie: zookie(record.revision),
            record_hash: record.record_hash,
        }))
    }

    async fn check_permission(
        &self,
        request: Request<CheckPermissionRequest>,
    ) -> Result<Response<CheckPermissionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_tuple_field("namespace", &req.namespace)?;
        validate_tuple_field("object_id", &req.object_id)?;
        validate_tuple_field("relation", &req.relation)?;
        validate_tuple_field("subject_kind", &req.subject_kind)?;
        validate_tuple_field("subject_id", &req.subject_id)?;
        let resource = authz_resource(&req.namespace, &req.object_id, &req.relation);
        if !auth::is_authorized(AnvilAction::AuthzCheck, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
        let latest_revision = authz_journal::latest_authz_revision(&self.storage, claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(required_revision) = consistency.required_revision()
            && latest_revision < required_revision
        {
            return Err(Status::failed_precondition("AuthzRevisionUnavailable"));
        }

        let (record, response_revision) = match consistency {
            AuthzConsistency::Exact(revision) => {
                let record = authz_journal::check_authz_tuple_at_revision(
                    &self.storage,
                    claims.tenant_id,
                    &req.namespace,
                    &req.object_id,
                    &req.relation,
                    &req.subject_kind,
                    &req.subject_id,
                    &req.caveat_hash,
                    revision,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
                (record, revision)
            }
            AuthzConsistency::Latest | AuthzConsistency::AtLeast(_) => {
                let record = authz_journal::check_authz_tuple(
                    &self.storage,
                    claims.tenant_id,
                    &req.namespace,
                    &req.object_id,
                    &req.relation,
                    &req.subject_kind,
                    &req.subject_id,
                    &req.caveat_hash,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
                (record, latest_revision)
            }
        };
        let allowed = record
            .as_ref()
            .is_some_and(|record| record.operation == "add");

        Ok(Response::new(CheckPermissionResponse {
            allowed,
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            explanation_ref: if allowed {
                "direct_tuple_match".to_string()
            } else {
                "no_current_tuple".to_string()
            },
        }))
    }

    async fn watch_authz_tuple_log(
        &self,
        request: Request<WatchAuthzTupleLogRequest>,
    ) -> Result<Response<Self::WatchAuthzTupleLogStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let resource = if req.namespace.is_empty() {
            "*".to_string()
        } else {
            req.namespace.clone()
        };
        if !auth::is_authorized(AnvilAction::AuthzWatch, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let after_revision = i64::try_from(req.after_revision)
            .map_err(|_| Status::invalid_argument("after_revision exceeds supported range"))?;
        let mut live = self.authz_watch_tx.subscribe();
        let snapshot = authz_journal::list_authz_tuple_log(
            &self.storage,
            claims.tenant_id,
            after_revision,
            &req.namespace,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_revision = after_revision;
            for record in snapshot {
                last_revision = last_revision.max(record.revision);
                if tx
                    .send(Ok(authz_tuple_log_response(&record)))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            loop {
                match live.recv().await {
                    Ok(record) => {
                        if record.tenant_id != claims.tenant_id
                            || record.revision <= last_revision
                            || (!req.namespace.is_empty() && record.namespace != req.namespace)
                        {
                            continue;
                        }
                        last_revision = record.revision;
                        if tx
                            .send(Ok(authz_tuple_log_response(&record)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = tx
                            .send(Err(Status::data_loss(
                                "Authz tuple watch fell behind retained live event window",
                            )))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchAuthzTupleLogStream
        ))
    }
}

fn authz_resource(namespace: &str, object_id: &str, relation: &str) -> String {
    format!("{}/{}#{}", namespace, object_id, relation)
}

fn validate_tuple_field(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{name} must not be empty"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(Status::invalid_argument(format!(
            "{name} must not contain control characters"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthzConsistency {
    Latest,
    AtLeast(i64),
    Exact(i64),
}

impl AuthzConsistency {
    fn from_request(consistency: &str, zookie: &str) -> Result<Self, Status> {
        match consistency {
            "" | "latest" => Ok(Self::Latest),
            "at_least" => Ok(Self::AtLeast(parse_authz_zookie(zookie)?)),
            "exact" => Ok(Self::Exact(parse_authz_zookie(zookie)?)),
            _ => Err(Status::invalid_argument(
                "consistency must be latest, at_least, exact, or empty",
            )),
        }
    }

    fn required_revision(self) -> Option<i64> {
        match self {
            Self::Latest => None,
            Self::AtLeast(revision) | Self::Exact(revision) => Some(revision),
        }
    }
}

fn parse_authz_zookie(value: &str) -> Result<i64, Status> {
    let Some(revision) = value.strip_prefix("authz:") else {
        return Err(Status::invalid_argument(
            "zookie must use authz:<revision> format",
        ));
    };
    let revision = revision
        .parse::<i64>()
        .map_err(|_| Status::invalid_argument("zookie revision must be an integer"))?;
    if revision < 0 {
        return Err(Status::invalid_argument(
            "zookie revision must not be negative",
        ));
    }
    Ok(revision)
}

fn revision_to_u64(revision: i64) -> Result<u64, Status> {
    u64::try_from(revision).map_err(|_| Status::internal("Invalid authz revision"))
}

fn zookie(revision: i64) -> String {
    format!("authz:{}", revision.max(0))
}

fn authz_tuple_log_response(
    record: &crate::persistence::AuthzTupleRecord,
) -> WatchAuthzTupleLogResponse {
    WatchAuthzTupleLogResponse {
        revision: revision_to_u64(record.revision).unwrap_or_default(),
        namespace: record.namespace.clone(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: record.subject_id.clone(),
        caveat_hash: record.caveat_hash.clone(),
        operation: record.operation.clone(),
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        record_hash: record.record_hash.clone(),
        written_at: record.written_at.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authz_consistency_parses_latest_without_zookie() {
        assert_eq!(
            AuthzConsistency::from_request("", "").unwrap(),
            AuthzConsistency::Latest
        );
        assert_eq!(
            AuthzConsistency::from_request("latest", "").unwrap(),
            AuthzConsistency::Latest
        );
    }

    #[test]
    fn authz_consistency_requires_zookie_for_at_least_and_exact() {
        assert_eq!(
            AuthzConsistency::from_request("at_least", "authz:42").unwrap(),
            AuthzConsistency::AtLeast(42)
        );
        assert_eq!(
            AuthzConsistency::from_request("exact", "authz:7").unwrap(),
            AuthzConsistency::Exact(7)
        );
        assert_eq!(
            AuthzConsistency::from_request("exact", "")
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            AuthzConsistency::from_request("at_least", "authz:-1")
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}
