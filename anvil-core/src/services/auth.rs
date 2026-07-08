use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::*;
use crate::{
    AppState, auth, authz_derived_lag_watch, authz_journal, authz_namespace_watch,
    authz_realm_schema, authz_schema,
    authz_scope::{
        DEFAULT_AUTHZ_REALM_ID, decode_realm_namespace, decode_userset_subject_realm,
        encode_optional_realm_namespace, encode_realm_namespace, encode_userset_subject_realm,
    },
    authz_userset_index,
    formats::hash32,
    permissions::AnvilAction,
    services::watch_envelope::{self, WatchEnvelopeParts},
};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl AuthService for AppState {
    type WatchAuthzTupleLogStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchAuthzTupleLogResponse, Status>> + Send>,
    >;
    type WatchAuthzNamespaceStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchAuthzNamespaceResponse, Status>> + Send>,
    >;
    type WatchAuthzDerivedLagStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchAuthzDerivedLagResponse, Status>> + Send>,
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

        let decrypted_secret = self
            .secret_keyring
            .decrypt(&app_details.client_secret_encrypted)
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

        let has_system_admin_relation = if approved_scopes.is_empty() {
            crate::system_realm::principal_has_any_admin_relation(
                &self.storage,
                &self.config.mesh_id,
                app_details.id,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        } else {
            false
        };

        if approved_scopes.is_empty() && !has_system_admin_relation {
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

    async fn create_application_credential(
        &self,
        request: Request<CreateApplicationCredentialRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_scope(&claims, AnvilAction::AppCreate)?;
        validate_public_app_request(&req.app_name, &req.request_id, &req.idempotency_key)?;

        let client_id = format!("app_{}", uuid::Uuid::new_v4().simple());
        let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
        let encrypted_secret = self
            .secret_keyring
            .encrypt(client_secret.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        let app = self
            .persistence
            .create_app(
                claims.tenant_id,
                &req.app_name,
                &client_id,
                &encrypted_secret,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.create",
            serde_json::json!({ "app_id": app.id, "client_id": client_id }),
        )
        .await?;

        Ok(Response::new(ApplicationSecretResponse {
            request_id: req.request_id,
            tenant_id: claims.tenant_id.to_string(),
            app_name: app.name,
            client_id,
            client_secret,
            audit_event_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn rotate_application_credential_secret(
        &self,
        request: Request<RotateApplicationCredentialSecretRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_scope(&claims, AnvilAction::AppRotateSecret)?;
        validate_public_app_request(&req.app_name, &req.request_id, &req.idempotency_key)?;
        let app = app_in_claims_tenant(self, claims.tenant_id, &req.app_name).await?;

        let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
        let encrypted_secret = self
            .secret_keyring
            .encrypt(client_secret.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        self.persistence
            .update_app_secret(app.id, &encrypted_secret)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.rotate_secret",
            serde_json::json!({ "app_id": app.id, "client_id": app.client_id }),
        )
        .await?;

        Ok(Response::new(ApplicationSecretResponse {
            request_id: req.request_id,
            tenant_id: claims.tenant_id.to_string(),
            app_name: app.name,
            client_id: app.client_id,
            client_secret,
            audit_event_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn delete_application_credential(
        &self,
        request: Request<DeleteApplicationCredentialRequest>,
    ) -> Result<Response<DeleteApplicationCredentialResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_scope(&claims, AnvilAction::AppDelete)?;
        validate_public_app_request(&req.app_name, &req.request_id, &req.idempotency_key)?;
        let app = app_in_claims_tenant(self, claims.tenant_id, &req.app_name).await?;

        self.persistence
            .delete_app(app.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.delete",
            serde_json::json!({ "app_id": app.id }),
        )
        .await?;

        Ok(Response::new(DeleteApplicationCredentialResponse {
            request_id: req.request_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn list_applications(
        &self,
        request: Request<ListApplicationsRequest>,
    ) -> Result<Response<ListApplicationsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let _ = request.into_inner();
        require_app_management_scope(&claims, AnvilAction::AppRead)?;
        let applications = self
            .persistence
            .list_apps_for_tenant(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(|app| ApplicationDescriptor {
                tenant_id: claims.tenant_id.to_string(),
                app_id: app.id.to_string(),
                app_name: app.name,
                client_id: app.client_id,
            })
            .collect();
        Ok(Response::new(ListApplicationsResponse { applications }))
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
        let delegated_action = req
            .action
            .parse::<AnvilAction>()
            .map_err(|_| Status::invalid_argument("Invalid delegated action"))?;
        validate_public_delegation_resource(claims, &req.resource)?;
        if matches!(delegated_action, AnvilAction::All)
            || req.action.trim().ends_with(":*")
            || req.resource.trim() == "*"
        {
            return Err(Status::permission_denied(
                "Public policy delegation cannot grant wildcard authority",
            ));
        }
        if !auth::is_authorized(delegated_action, &req.resource, &claims.scopes) {
            return Err(Status::permission_denied(
                "Caller cannot delegate permissions it does not already hold",
            ));
        }

        let app = app_in_claims_tenant(self, claims.tenant_id, &req.grantee_app_id).await?;
        self.persistence
            .grant_policy(app.id, &req.resource, &req.action)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::services::audit::record_tenant_audit_event(
            self,
            claims,
            "policy-grant",
            &req.resource,
            "policy.grant",
            serde_json::json!({ "grantee_app_id": app.id, "action": req.action }),
        )
        .await?;

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

        validate_public_delegation_resource(claims, &req.resource)?;
        if !auth::is_authorized(AnvilAction::PolicyRevoke, &req.resource, &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to revoke access to this resource",
            ));
        }

        let app = app_in_claims_tenant(self, claims.tenant_id, &req.grantee_app_id).await?;

        self.persistence
            .revoke_policy(app.id, &req.resource, &req.action)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::services::audit::record_tenant_audit_event(
            self,
            claims,
            "policy-revoke",
            &req.resource,
            "policy.revoke",
            serde_json::json!({ "grantee_app_id": app.id, "action": req.action }),
        )
        .await?;

        Ok(Response::new(RevokeAccessResponse {}))
    }

    async fn list_access_grants(
        &self,
        request: Request<ListAccessGrantsRequest>,
    ) -> Result<Response<ListAccessGrantsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_scope(&claims, AnvilAction::PolicyRead)?;
        let app = app_in_claims_tenant(self, claims.tenant_id, &req.app).await?;
        let grants = self
            .persistence
            .list_policies_for_app(app.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .filter(|grant| {
                auth::is_authorized(AnvilAction::PolicyRead, &grant.resource, &claims.scopes)
                    || auth::is_authorized(
                        AnvilAction::PolicyGrant,
                        &grant.resource,
                        &claims.scopes,
                    )
                    || auth::is_authorized(
                        AnvilAction::PolicyRevoke,
                        &grant.resource,
                        &claims.scopes,
                    )
            })
            .map(|grant| AccessGrantRecord {
                app_id: app.id.to_string(),
                app_name: app.name.clone(),
                action: grant.action,
                resource: grant.resource,
            })
            .collect();

        Ok(Response::new(ListAccessGrantsResponse { grants }))
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
        let record = write_authz_tuple_record(
            self,
            &claims,
            AuthzTupleMutation {
                namespace: req.namespace,
                object_id: req.object_id,
                relation: req.relation,
                subject_kind: req.subject_kind,
                subject_id: req.subject_id,
                caveat_hash: req.caveat_hash,
                operation: req.operation,
                reason: req.reason,
                scope: req.scope,
            },
        )
        .await?;

        Ok(Response::new(write_authz_tuple_response(&record)?))
    }

    async fn write_authz_tuples(
        &self,
        request: Request<WriteAuthzTuplesRequest>,
    ) -> Result<Response<WriteAuthzTuplesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if req.mutations.is_empty() {
            return Err(Status::invalid_argument(
                "mutations must contain at least one tuple",
            ));
        }
        if req.mutations.len() > 1000 {
            return Err(Status::invalid_argument(
                "mutations must contain no more than 1000 tuples",
            ));
        }
        for mutation in &req.mutations {
            validate_authz_tuple_mutation(&claims, mutation)?;
        }
        let scope = resolve_batch_scope(&claims, req.scope.as_ref(), &req.mutations)?;

        let mutations = req
            .mutations
            .into_iter()
            .map(|mutation| crate::persistence::AuthzTupleBatchMutation {
                namespace: encode_realm_namespace(&scope.authz_realm_id, &mutation.namespace),
                object_id: mutation.object_id,
                relation: mutation.relation,
                subject_id: encode_userset_subject_realm(
                    &scope.authz_realm_id,
                    &mutation.subject_kind,
                    &mutation.subject_id,
                ),
                subject_kind: mutation.subject_kind,
                caveat_hash: mutation.caveat_hash,
                operation: mutation.operation,
                reason: mutation.reason,
            })
            .collect();
        let records = self
            .persistence
            .write_authz_tuple_batch(claims.tenant_id, mutations, &claims.sub)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let latest_revision = records
            .iter()
            .map(|record| record.revision)
            .max()
            .unwrap_or(0);
        emit_authz_tuple_batch_side_effects(self, claims.tenant_id, &records).await?;

        Ok(Response::new(WriteAuthzTuplesResponse {
            results: records
                .iter()
                .map(write_authz_tuple_response)
                .collect::<Result<Vec<_>, _>>()?,
            revision: revision_to_u64(latest_revision)?,
            zookie: zookie(latest_revision),
        }))
    }

    async fn read_authz_tuples(
        &self,
        request: Request<ReadAuthzTuplesRequest>,
    ) -> Result<Response<ReadAuthzTuplesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_optional_tuple_component("namespace", &req.namespace)?;
        validate_optional_tuple_field("object_id", &req.object_id)?;
        validate_optional_tuple_component("relation", &req.relation)?;
        validate_optional_tuple_component("subject_kind", &req.subject_kind)?;
        validate_optional_tuple_field("subject_id", &req.subject_id)?;
        validate_caveat_hash(&req.caveat_hash)?;
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;

        let resource = authz_filter_resource(&req.namespace, &req.object_id, &req.relation);
        if !auth::is_authorized(AnvilAction::AuthzTupleRead, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let filter_hash = authz_page_filter_hash(
            "read_tuples",
            &[
                &scope.authz_realm_id,
                &req.namespace,
                &req.object_id,
                &req.relation,
                &req.subject_kind,
                &req.subject_id,
                &req.caveat_hash,
            ],
        );
        let page_token = parse_authz_page_token(
            &req.page_token,
            claims.tenant_id,
            &filter_hash,
            self.config.jwt_secret.as_bytes(),
        )?;
        let response_revision = match page_token.as_ref() {
            Some(token) => token.revision,
            None => {
                let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
                resolve_authz_response_revision(&self.storage, claims.tenant_id, consistency)
                    .await?
            }
        };
        let records = authz_journal::read_current_authz_tuples_at_revision(
            &self.storage,
            claims.tenant_id,
            authz_journal::AuthzTupleFilter {
                namespace: optional_filter_value(encode_optional_realm_namespace(
                    &scope.authz_realm_id,
                    &req.namespace,
                )),
                object_id: optional_filter_value(req.object_id),
                relation: optional_filter_value(req.relation),
                subject_kind: optional_filter_value(req.subject_kind),
                subject_id: optional_filter_value(req.subject_id),
                caveat_hash: optional_filter_value(req.caveat_hash),
            },
            response_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let records = filter_records_for_realm(records, &scope.authz_realm_id);
        let (records, next_page_token) = paginate_authz(
            records,
            req.page_size,
            page_token.as_ref().map(|token| token.offset).unwrap_or(0),
            claims.tenant_id,
            response_revision,
            &filter_hash,
            self.config.jwt_secret.as_bytes(),
        )?;

        Ok(Response::new(ReadAuthzTuplesResponse {
            tuples: records
                .into_iter()
                .map(|record| authz_tuple_response_for_realm(&record, &scope.authz_realm_id))
                .collect::<Result<Vec<_>, _>>()?,
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            next_page_token,
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
        Ok(Response::new(
            check_permission_response(self, &claims, req).await?,
        ))
    }

    async fn check_permissions(
        &self,
        request: Request<CheckPermissionsRequest>,
    ) -> Result<Response<CheckPermissionsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if req.checks.is_empty() {
            return Err(Status::invalid_argument(
                "checks must contain at least one request",
            ));
        }
        if req.checks.len() > 1000 {
            return Err(Status::invalid_argument(
                "checks must contain no more than 1000 requests",
            ));
        }

        let mut results = Vec::with_capacity(req.checks.len());
        let mut latest_revision = 0;
        for check in req.checks {
            let response = check_permission_response(self, &claims, check).await?;
            latest_revision = latest_revision.max(response.revision);
            results.push(response);
        }

        Ok(Response::new(CheckPermissionsResponse {
            results,
            revision: latest_revision,
            zookie: format!("authz:{latest_revision}"),
        }))
    }

    async fn list_authz_objects(
        &self,
        request: Request<ListAuthzObjectsRequest>,
    ) -> Result<Response<ListAuthzObjectsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_tuple_component("namespace", &req.namespace)?;
        validate_tuple_component("relation", &req.relation)?;
        validate_tuple_component("subject_kind", &req.subject_kind)?;
        validate_tuple_field("subject_id", &req.subject_id)?;
        validate_caveat_hash(&req.caveat_hash)?;
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        let resource = authz_filter_resource(&req.namespace, "", &req.relation);
        if !auth::is_authorized(AnvilAction::AuthzTupleRead, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let filter_hash = authz_page_filter_hash(
            "list_objects",
            &[
                &scope.authz_realm_id,
                &req.namespace,
                &req.relation,
                &req.subject_kind,
                &req.subject_id,
                &req.caveat_hash,
            ],
        );
        let page_token = parse_authz_page_token(
            &req.page_token,
            claims.tenant_id,
            &filter_hash,
            self.config.jwt_secret.as_bytes(),
        )?;
        let response_revision = match page_token.as_ref() {
            Some(token) => token.revision,
            None => {
                let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
                resolve_authz_response_revision(&self.storage, claims.tenant_id, consistency)
                    .await?
            }
        };
        let object_ids = authz_journal::list_current_authz_objects_at_revision(
            &self.storage,
            claims.tenant_id,
            &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
            &req.relation,
            &req.subject_kind,
            &req.subject_id,
            &req.caveat_hash,
            response_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let (object_ids, next_page_token) = paginate_authz(
            object_ids,
            req.page_size,
            page_token.as_ref().map(|token| token.offset).unwrap_or(0),
            claims.tenant_id,
            response_revision,
            &filter_hash,
            self.config.jwt_secret.as_bytes(),
        )?;

        Ok(Response::new(ListAuthzObjectsResponse {
            object_ids,
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            next_page_token,
        }))
    }

    async fn list_authz_subjects(
        &self,
        request: Request<ListAuthzSubjectsRequest>,
    ) -> Result<Response<ListAuthzSubjectsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_tuple_component("namespace", &req.namespace)?;
        validate_tuple_field("object_id", &req.object_id)?;
        validate_tuple_component("relation", &req.relation)?;
        validate_optional_tuple_component("subject_kind", &req.subject_kind)?;
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        let resource = authz_resource(&req.namespace, &req.object_id, &req.relation);
        if !auth::is_authorized(AnvilAction::AuthzTupleRead, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let filter_hash = authz_page_filter_hash(
            "list_subjects",
            &[
                &scope.authz_realm_id,
                &req.namespace,
                &req.object_id,
                &req.relation,
                &req.subject_kind,
            ],
        );
        let page_token = parse_authz_page_token(
            &req.page_token,
            claims.tenant_id,
            &filter_hash,
            self.config.jwt_secret.as_bytes(),
        )?;
        let response_revision = match page_token.as_ref() {
            Some(token) => token.revision,
            None => {
                let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
                resolve_authz_response_revision(&self.storage, claims.tenant_id, consistency)
                    .await?
            }
        };
        let subjects = authz_journal::list_current_authz_subjects_at_revision(
            &self.storage,
            claims.tenant_id,
            &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
            &req.object_id,
            &req.relation,
            optional_str(req.subject_kind.as_str()),
            response_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let (subjects, next_page_token) = paginate_authz(
            subjects,
            req.page_size,
            page_token.as_ref().map(|token| token.offset).unwrap_or(0),
            claims.tenant_id,
            response_revision,
            &filter_hash,
            self.config.jwt_secret.as_bytes(),
        )?;

        Ok(Response::new(ListAuthzSubjectsResponse {
            subjects: subjects
                .into_iter()
                .map(|subject| AuthzSubject {
                    subject_id: decode_userset_subject_realm(
                        &scope.authz_realm_id,
                        &subject.subject_kind,
                        &subject.subject_id,
                    ),
                    subject_kind: subject.subject_kind,
                    caveat_hash: subject.caveat_hash,
                })
                .collect(),
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            next_page_token,
        }))
    }

    async fn put_authz_schema(
        &self,
        request: Request<PutAuthzSchemaRequest>,
    ) -> Result<Response<PutAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_storage_tenant(&claims, &req.anvil_storage_tenant_id)?;
        validate_tuple_component("schema_id", &req.schema_id)?;
        if req.namespaces.is_empty() {
            return Err(Status::invalid_argument(
                "namespaces must contain at least one schema",
            ));
        }
        for namespace in &req.namespaces {
            validate_tuple_component("namespace", &namespace.namespace)?;
            if !auth::is_authorized(
                AnvilAction::AuthzSchemaWrite,
                &namespace.namespace,
                &claims.scopes,
            ) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }
        let authz_revision = authz_journal::latest_authz_revision(&self.storage, claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))
            .and_then(revision_to_u64)?
            .saturating_add(1);
        let record = authz_realm_schema::put_schema_revision(
            &self.storage,
            claims.tenant_id,
            &req.schema_id,
            req.namespaces,
            authz_revision,
            &claims.sub,
            &req.reason,
        )
        .await
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(PutAuthzSchemaResponse {
            schema_ref: Some(schema_ref_response(&record.schema_ref)),
            authz_revision,
            zookie: zookie(u64_to_i64(authz_revision)?),
        }))
    }

    async fn bind_authz_schema(
        &self,
        request: Request<BindAuthzSchemaRequest>,
    ) -> Result<Response<BindAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        let schema_ref = req
            .schema_ref
            .ok_or_else(|| Status::invalid_argument("schema_ref is required"))?;
        validate_tuple_component("schema_id", &schema_ref.schema_id)?;
        if !auth::is_authorized(
            AnvilAction::AuthzSchemaWrite,
            &scope.authz_realm_id,
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let authz_revision = authz_journal::latest_authz_revision(&self.storage, claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))
            .and_then(revision_to_u64)?
            .saturating_add(1);
        let binding = authz_realm_schema::bind_schema(
            &self.storage,
            claims.tenant_id,
            &scope.authz_realm_id,
            authz_realm_schema::StoredSchemaRef {
                schema_id: schema_ref.schema_id,
                schema_revision: schema_ref.schema_revision,
                schema_digest: schema_ref.schema_digest,
            },
            req.expected_binding_generation,
            authz_revision,
            &claims.sub,
            &req.reason,
        )
        .await
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(BindAuthzSchemaResponse {
            scope: Some(scope),
            schema_ref: Some(schema_ref_response(&binding.schema_ref)),
            binding_generation: binding.binding_generation,
            authz_revision,
            zookie: zookie(u64_to_i64(authz_revision)?),
        }))
    }

    async fn get_authz_schema_binding(
        &self,
        request: Request<GetAuthzSchemaBindingRequest>,
    ) -> Result<Response<GetAuthzSchemaBindingResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        if !auth::is_authorized(
            AnvilAction::AuthzSchemaRead,
            &scope.authz_realm_id,
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let binding = authz_realm_schema::read_schema_binding(
            &self.storage,
            claims.tenant_id,
            &scope.authz_realm_id,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("schema binding not found"))?;
        Ok(Response::new(GetAuthzSchemaBindingResponse {
            scope: Some(scope),
            schema_ref: Some(schema_ref_response(&binding.schema_ref)),
            binding_generation: binding.binding_generation,
        }))
    }

    async fn apply_authz_schema(
        &self,
        request: Request<ApplyAuthzSchemaRequest>,
    ) -> Result<Response<ApplyAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if req.namespaces.is_empty() {
            return Err(Status::invalid_argument(
                "namespaces must contain at least one schema",
            ));
        }
        if req.namespaces.len() > 1000 {
            return Err(Status::invalid_argument(
                "namespaces must contain no more than 1000 schemas",
            ));
        }

        let mut records = Vec::with_capacity(req.namespaces.len());
        let authz_revision = authz_journal::latest_authz_revision(&self.storage, claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))
            .and_then(revision_to_u64)?
            .max(1);
        for namespace in req.namespaces {
            validate_tuple_component("namespace", &namespace.namespace)?;
            if !auth::is_authorized(
                AnvilAction::AuthzSchemaWrite,
                &namespace.namespace,
                &claims.scopes,
            ) {
                return Err(Status::permission_denied("Permission denied"));
            }
            let record = authz_schema::write_authz_namespace_schema(
                &self.storage,
                claims.tenant_id,
                namespace,
                authz_revision,
                &claims.sub,
                &req.reason,
            )
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
            authz_namespace_watch::append_authz_namespace_watch_record(
                &self.storage,
                claims.tenant_id,
                u128::from(record.schema_version),
                mutation_id_from_record_hash(&record.record_hash),
                authz_namespace_watch::AuthzNamespaceWatchPayload {
                    namespace: record.namespace.clone(),
                    event_type: "schema_changed".to_string(),
                    authz_revision,
                    schema_hash: record.schema_hash.clone(),
                    invalidates_derived_usersets: true,
                    emitted_at: record.applied_at.clone(),
                },
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            records.push(record);
        }
        let schema_version = records
            .iter()
            .map(|record| record.schema_version)
            .max()
            .unwrap_or(0);
        Ok(Response::new(ApplyAuthzSchemaResponse {
            namespaces: records.iter().map(authz_schema::schema_response).collect(),
            schema_version,
        }))
    }

    async fn get_authz_schema(
        &self,
        request: Request<GetAuthzSchemaRequest>,
    ) -> Result<Response<GetAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !req.schema_id.is_empty() {
            validate_storage_tenant(&claims, &req.anvil_storage_tenant_id)?;
            validate_tuple_component("schema_id", &req.schema_id)?;
            if !auth::is_authorized(AnvilAction::AuthzSchemaRead, &req.schema_id, &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
            let record = authz_realm_schema::read_schema_revision(
                &self.storage,
                claims.tenant_id,
                &req.schema_id,
                req.schema_revision,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("schema not found"))?;
            return Ok(Response::new(GetAuthzSchemaResponse {
                namespaces: record.namespaces,
                schema_version: record.schema_ref.schema_revision,
                schema_ref: Some(schema_ref_response(&record.schema_ref)),
            }));
        }
        if !req.namespace.is_empty() {
            validate_tuple_component("namespace", &req.namespace)?;
        }
        let resource = if req.namespace.is_empty() {
            "*".to_string()
        } else {
            req.namespace.clone()
        };
        if !auth::is_authorized(AnvilAction::AuthzSchemaRead, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let records = if req.namespace.is_empty() {
            authz_schema::list_authz_namespace_schemas(&self.storage, claims.tenant_id).await
        } else {
            authz_schema::read_authz_namespace_schema(
                &self.storage,
                claims.tenant_id,
                &req.namespace,
            )
            .await
            .map(|record| record.into_iter().collect())
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        let schema_version = records
            .iter()
            .map(|record| record.schema_version)
            .max()
            .unwrap_or(0);
        Ok(Response::new(GetAuthzSchemaResponse {
            namespaces: records.iter().map(authz_schema::schema_response).collect(),
            schema_version,
            schema_ref: None,
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
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
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
            &encode_optional_realm_namespace(&scope.authz_realm_id, &req.namespace),
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
                    .send(Ok(authz_tuple_log_response_for_realm(
                        &record,
                        &scope.authz_realm_id,
                    )))
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
                            || !record_belongs_to_realm(&record, &scope.authz_realm_id)
                            || (!req.namespace.is_empty()
                                && record.namespace
                                    != encode_realm_namespace(
                                        &scope.authz_realm_id,
                                        &req.namespace,
                                    ))
                        {
                            continue;
                        }
                        last_revision = record.revision;
                        if tx
                            .send(Ok(authz_tuple_log_response_for_realm(
                                &record,
                                &scope.authz_realm_id,
                            )))
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

    async fn watch_authz_namespace(
        &self,
        request: Request<WatchAuthzNamespaceRequest>,
    ) -> Result<Response<Self::WatchAuthzNamespaceStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_watch_component("namespace", &req.namespace)?;
        if !auth::is_authorized(AnvilAction::AuthzWatch, &req.namespace, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let snapshot = authz_namespace_watch::list_authz_namespace_watch_events(
            &self.storage,
            claims.tenant_id,
            &req.namespace,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let storage = self.storage.clone();
        let namespace = req.namespace;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.cursor);
                if tx
                    .send(Ok(authz_namespace_watch_response(event)))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let events = match authz_namespace_watch::list_authz_namespace_watch_events(
                    &storage,
                    tenant_id,
                    &namespace,
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
                    if tx
                        .send(Ok(authz_namespace_watch_response(event)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchAuthzNamespaceStream
        ))
    }

    async fn watch_authz_derived_lag(
        &self,
        request: Request<WatchAuthzDerivedLagRequest>,
    ) -> Result<Response<Self::WatchAuthzDerivedLagStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_watch_component("derived_index_id", &req.derived_index_id)?;
        if !auth::is_authorized(
            AnvilAction::AuthzWatch,
            &req.derived_index_id,
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let snapshot = authz_derived_lag_watch::list_authz_derived_lag_watch_events(
            &self.storage,
            claims.tenant_id,
            &req.derived_index_id,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let storage = self.storage.clone();
        let derived_index_id = req.derived_index_id;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.cursor);
                if tx
                    .send(Ok(authz_derived_lag_watch_response(event)))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let events = match authz_derived_lag_watch::list_authz_derived_lag_watch_events(
                    &storage,
                    tenant_id,
                    &derived_index_id,
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
                    if tx
                        .send(Ok(authz_derived_lag_watch_response(event)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchAuthzDerivedLagStream
        ))
    }
}

mod helpers;
use helpers::*;

#[cfg(test)]
mod tests;
