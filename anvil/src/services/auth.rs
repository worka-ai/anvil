use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::*;
use crate::{AppState, auth, crypto};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl AuthService for AppState {
    async fn get_access_token(
        &self,
        request: Request<GetAccessTokenRequest>,
    ) -> Result<Response<GetAccessTokenResponse>, Status> {
        let req = request.into_inner();

        // 1. Verify credentials
        let app_details = self
            .db
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
            .db
            .get_policies_for_app(app_details.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let approved_scopes = if req.scopes.is_empty() || req.scopes == vec!["*"] {
            allowed_scopes
        } else {
            req.scopes
                .into_iter()
                .filter(|requested_scope| auth::is_authorized(requested_scope, &allowed_scopes))
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

        if !auth::is_authorized(&format!("grant:{}", req.resource), &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to grant access to this resource",
            ));
        }

        let app = self
            .db
            .get_app_by_name(&req.grantee_app_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Grantee app not found"))?;
        self.db
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

        if !auth::is_authorized(&format!("grant:{}", req.resource), &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to revoke access to this resource",
            ));
        }

        let app = self
            .db
            .get_app_by_name(&req.grantee_app_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Grantee app not found"))?;

        self.db
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
        if !auth::is_authorized(&format!("grant:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to modify public access on this bucket",
            ));
        }

        self.db
            .set_bucket_public_access(&req.bucket, req.allow_public_read)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(SetPublicAccessResponse {}))
    }
}
