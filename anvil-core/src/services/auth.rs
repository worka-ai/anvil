use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::*;
use crate::{
    AppState, auth, authz_derived_lag_watch, authz_journal, authz_namespace_watch,
    authz_realm_schema, authz_schema,
    authz_scope::{
        DEFAULT_AUTHZ_REALM_ID, decode_realm_namespace, decode_userset_subject_realm,
        encode_optional_realm_namespace, encode_realm_namespace, encode_userset_subject_realm,
    },
    authz_userset_index, crypto,
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

fn authz_resource(namespace: &str, object_id: &str, relation: &str) -> String {
    format!("{}/{}#{}", namespace, object_id, relation)
}

fn authz_filter_resource(namespace: &str, object_id: &str, relation: &str) -> String {
    match (
        namespace.is_empty(),
        object_id.is_empty(),
        relation.is_empty(),
    ) {
        (true, _, _) => "*".to_string(),
        (false, true, true) => namespace.to_string(),
        (false, true, false) => format!("{namespace}/*#{relation}"),
        (false, false, true) => format!("{namespace}/{object_id}#*"),
        (false, false, false) => authz_resource(namespace, object_id, relation),
    }
}

fn validate_storage_tenant(
    claims: &auth::Claims,
    anvil_storage_tenant_id: &str,
) -> Result<(), Status> {
    if anvil_storage_tenant_id.is_empty() || anvil_storage_tenant_id == claims.tenant_id.to_string()
    {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "authz scope storage tenant does not match authenticated tenant",
        ))
    }
}

fn resolve_authz_scope(
    claims: &auth::Claims,
    scope: Option<&AuthzScope>,
) -> Result<AuthzScope, Status> {
    let mut resolved = scope.cloned().unwrap_or_else(|| AuthzScope {
        anvil_storage_tenant_id: claims.tenant_id.to_string(),
        authz_realm_id: DEFAULT_AUTHZ_REALM_ID.to_string(),
    });
    if resolved.anvil_storage_tenant_id.is_empty() {
        resolved.anvil_storage_tenant_id = claims.tenant_id.to_string();
    }
    validate_storage_tenant(claims, &resolved.anvil_storage_tenant_id)?;
    validate_tuple_component("authz_realm_id", &resolved.authz_realm_id)?;
    Ok(resolved)
}

fn resolve_batch_scope(
    claims: &auth::Claims,
    request_scope: Option<&AuthzScope>,
    mutations: &[AuthzTupleMutation],
) -> Result<AuthzScope, Status> {
    let scope = resolve_authz_scope(
        claims,
        request_scope.or_else(|| {
            mutations
                .first()
                .and_then(|mutation| mutation.scope.as_ref())
        }),
    )?;
    for mutation in mutations {
        if let Some(mutation_scope) = mutation.scope.as_ref() {
            let mutation_scope = resolve_authz_scope(claims, Some(mutation_scope))?;
            if mutation_scope != scope {
                return Err(Status::invalid_argument(
                    "authz tuple batch must target one authz scope",
                ));
            }
        }
    }
    Ok(scope)
}

fn record_belongs_to_realm(record: &crate::persistence::AuthzTupleRecord, realm_id: &str) -> bool {
    decode_realm_namespace(realm_id, &record.namespace).is_some()
}

fn filter_records_for_realm(
    records: Vec<crate::persistence::AuthzTupleRecord>,
    realm_id: &str,
) -> Vec<crate::persistence::AuthzTupleRecord> {
    records
        .into_iter()
        .filter(|record| record_belongs_to_realm(record, realm_id))
        .collect()
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

fn validate_optional_tuple_field(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Ok(());
    }
    validate_tuple_field(name, value)
}

fn validate_tuple_component(name: &str, value: &str) -> Result<(), Status> {
    validate_tuple_field(name, value)?;
    if value == "." || value == ".." || value.contains('/') {
        return Err(Status::invalid_argument(format!(
            "{name} must be a safe authz component"
        )));
    }
    Ok(())
}

fn validate_optional_tuple_component(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Ok(());
    }
    validate_tuple_component(name, value)
}

fn validate_caveat_hash(value: &str) -> Result<(), Status> {
    authz_journal::validate_optional_caveat_hash(value)
        .map_err(|err| Status::invalid_argument(err.to_string()))
}

fn validate_watch_component(name: &str, value: &str) -> Result<(), Status> {
    validate_tuple_field(name, value)?;
    if value == "." || value == ".." || value.contains('/') {
        return Err(Status::invalid_argument(format!(
            "{name} must be a safe path component"
        )));
    }
    Ok(())
}

async fn write_authz_tuple_record(
    state: &AppState,
    claims: &auth::Claims,
    req: AuthzTupleMutation,
) -> Result<crate::persistence::AuthzTupleRecord, Status> {
    let operation = validate_authz_tuple_mutation(claims, &req)?;
    let scope = resolve_authz_scope(claims, req.scope.as_ref())?;
    let record = state
        .persistence
        .write_authz_tuple(
            claims.tenant_id,
            &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
            &req.object_id,
            &req.relation,
            &req.subject_kind,
            &encode_userset_subject_realm(
                &scope.authz_realm_id,
                &req.subject_kind,
                &req.subject_id,
            ),
            &req.caveat_hash,
            operation,
            &claims.sub,
            &req.reason,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    emit_authz_tuple_write_side_effects(state, claims.tenant_id, &record).await?;
    Ok(record)
}

fn validate_authz_tuple_mutation<'a>(
    claims: &auth::Claims,
    req: &'a AuthzTupleMutation,
) -> Result<&'a str, Status> {
    validate_tuple_component("namespace", &req.namespace)?;
    validate_tuple_field("object_id", &req.object_id)?;
    validate_tuple_component("relation", &req.relation)?;
    validate_tuple_component("subject_kind", &req.subject_kind)?;
    validate_tuple_field("subject_id", &req.subject_id)?;
    validate_caveat_hash(&req.caveat_hash)?;
    let operation = match req.operation.as_str() {
        "add" | "remove" => req.operation.as_str(),
        _ => return Err(Status::invalid_argument("operation must be add or remove")),
    };
    let resource = authz_resource(&req.namespace, &req.object_id, &req.relation);
    if !auth::is_authorized(AnvilAction::AuthzTupleWrite, &resource, &claims.scopes) {
        return Err(Status::permission_denied("Permission denied"));
    }
    Ok(operation)
}

async fn emit_authz_tuple_write_side_effects(
    state: &AppState,
    tenant_id: i64,
    record: &crate::persistence::AuthzTupleRecord,
) -> Result<(), Status> {
    emit_authz_tuple_batch_side_effects(state, tenant_id, std::slice::from_ref(record)).await
}

async fn emit_authz_tuple_batch_side_effects(
    state: &AppState,
    tenant_id: i64,
    records: &[crate::persistence::AuthzTupleRecord],
) -> Result<(), Status> {
    let Some(last_record) = records
        .iter()
        .max_by_key(|record| (record.revision, record.revision_ordinal))
    else {
        return Ok(());
    };
    for record in records {
        let _ = state.authz_watch_tx.send(record.clone());
    }
    let derived = authz_userset_index::advance_derived_userset_index_from_batch(
        &state.storage,
        tenant_id,
        authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID,
        records,
    )
    .await
    .map_err(|e| Status::internal(e.to_string()))?;
    let processed_revision = revision_to_u64(last_record.revision)?;
    authz_derived_lag_watch::append_authz_derived_lag_watch_record(
        &state.storage,
        tenant_id,
        u128::from(processed_revision),
        mutation_id_from_record_hash(&last_record.record_hash),
        authz_derived_lag_watch::AuthzDerivedLagWatchPayload {
            derived_index_id: authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID.to_string(),
            derived_index_kind: "userset".to_string(),
            processed_revision: derived.processed_revision,
            latest_revision: processed_revision,
            source_cursor: u128::from(processed_revision),
            source_manifest_hash: derived.source_records_hash,
            generation: derived.generation,
            emitted_at: chrono::Utc::now().to_rfc3339(),
        },
    )
    .await
    .map_err(|e| Status::internal(e.to_string()))?;
    Ok(())
}

async fn check_permission_response(
    state: &AppState,
    claims: &auth::Claims,
    req: CheckPermissionRequest,
) -> Result<CheckPermissionResponse, Status> {
    validate_tuple_component("namespace", &req.namespace)?;
    validate_tuple_field("object_id", &req.object_id)?;
    validate_tuple_component("relation", &req.relation)?;
    validate_tuple_component("subject_kind", &req.subject_kind)?;
    validate_tuple_field("subject_id", &req.subject_id)?;
    validate_caveat_hash(&req.caveat_hash)?;
    let scope = resolve_authz_scope(claims, req.scope.as_ref())?;
    let resource = authz_resource(&req.namespace, &req.object_id, &req.relation);
    if !auth::is_authorized(AnvilAction::AuthzCheck, &resource, &claims.scopes) {
        return Err(Status::permission_denied("Permission denied"));
    }
    let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
    let response_revision =
        resolve_authz_response_revision(&state.storage, claims.tenant_id, consistency).await?;
    let allowed = authz_journal::resolve_permission_at_revision(
        &state.storage,
        claims.tenant_id,
        &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
        &req.object_id,
        &req.relation,
        &req.subject_kind,
        &req.subject_id,
        &req.caveat_hash,
        response_revision,
    )
    .await
    .map_err(|e| Status::internal(e.to_string()))?;

    Ok(CheckPermissionResponse {
        allowed,
        revision: revision_to_u64(response_revision)?,
        zookie: zookie(response_revision),
        explanation_ref: if allowed {
            "tuple_or_userset_match".to_string()
        } else {
            "no_current_tuple_or_userset".to_string()
        },
    })
}

async fn resolve_authz_response_revision(
    storage: &crate::storage::Storage,
    tenant_id: i64,
    consistency: AuthzConsistency,
) -> Result<i64, Status> {
    let latest_revision = authz_journal::latest_authz_revision(storage, tenant_id)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    if let Some(required_revision) = consistency.required_revision()
        && latest_revision < required_revision
    {
        return Err(Status::failed_precondition("AuthzRevisionUnavailable"));
    }

    Ok(match consistency {
        AuthzConsistency::Exact(revision) => revision,
        AuthzConsistency::Latest | AuthzConsistency::AtLeast(_) => latest_revision,
    })
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

fn u64_to_i64(revision: u64) -> Result<i64, Status> {
    i64::try_from(revision).map_err(|_| Status::internal("Invalid authz revision"))
}

fn zookie(revision: i64) -> String {
    format!("authz:{}", revision.max(0))
}

fn schema_ref_response(record: &authz_realm_schema::StoredSchemaRef) -> AuthzSchemaRef {
    AuthzSchemaRef {
        schema_id: record.schema_id.clone(),
        schema_revision: record.schema_revision,
        schema_digest: record.schema_digest.clone(),
    }
}

fn write_authz_tuple_response(
    record: &crate::persistence::AuthzTupleRecord,
) -> Result<WriteAuthzTupleResponse, Status> {
    Ok(WriteAuthzTupleResponse {
        revision: revision_to_u64(record.revision)?,
        zookie: zookie(record.revision),
        record_hash: record.record_hash.clone(),
    })
}

fn authz_tuple_response_for_realm(
    record: &crate::persistence::AuthzTupleRecord,
    realm_id: &str,
) -> Result<AuthzTuple, Status> {
    let namespace = decode_realm_namespace(realm_id, &record.namespace)
        .ok_or_else(|| Status::internal("authz tuple namespace is outside requested realm"))?;
    Ok(AuthzTuple {
        namespace: namespace.to_string(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: decode_userset_subject_realm(
            realm_id,
            &record.subject_kind,
            &record.subject_id,
        ),
        caveat_hash: record.caveat_hash.clone(),
        revision: revision_to_u64(record.revision)?,
        zookie: zookie(record.revision),
    })
}

fn optional_filter_value(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn optional_str(value: &str) -> Option<&str> {
    if value.is_empty() { None } else { Some(value) }
}

fn paginate_authz<T>(
    values: Vec<T>,
    page_size: u32,
    offset: usize,
    tenant_id: i64,
    revision: i64,
    filter_hash: &str,
    signing_key: &[u8],
) -> Result<(Vec<T>, String), Status> {
    let limit = normalize_page_size(page_size);
    if offset >= values.len() {
        return Ok((Vec::new(), String::new()));
    }
    let next_offset = offset.saturating_add(limit);
    let next_page_token = if next_offset < values.len() {
        encode_authz_page_token(
            AuthzPageTokenClaims {
                tenant_id,
                revision,
                filter_hash,
                offset: next_offset,
            },
            signing_key,
        )?
    } else {
        String::new()
    };
    Ok((
        values.into_iter().skip(offset).take(limit).collect(),
        next_page_token,
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthzPageToken {
    version: u8,
    tenant_id: i64,
    revision: i64,
    filter_hash: String,
    offset: usize,
    signature: String,
}

#[derive(Debug, Clone, Copy)]
struct AuthzPageTokenClaims<'a> {
    tenant_id: i64,
    revision: i64,
    filter_hash: &'a str,
    offset: usize,
}

fn parse_authz_page_token(
    value: &str,
    expected_tenant_id: i64,
    expected_filter_hash: &str,
    signing_key: &[u8],
) -> Result<Option<AuthzPageToken>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| Status::invalid_argument("Invalid authz page token"))?;
    let token: AuthzPageToken = serde_json::from_slice(&bytes)
        .map_err(|_| Status::invalid_argument("Invalid authz page token"))?;
    if token.version != 1
        || token.tenant_id != expected_tenant_id
        || token.filter_hash != expected_filter_hash
    {
        return Err(Status::invalid_argument(
            "Authz page token does not match this request",
        ));
    }
    let expected = sign_authz_page_token(
        AuthzPageTokenClaims {
            tenant_id: token.tenant_id,
            revision: token.revision,
            filter_hash: &token.filter_hash,
            offset: token.offset,
        },
        signing_key,
    )?;
    if token.signature != expected {
        return Err(Status::invalid_argument("Invalid authz page token"));
    }
    Ok(Some(token))
}

fn encode_authz_page_token(
    claims: AuthzPageTokenClaims<'_>,
    signing_key: &[u8],
) -> Result<String, Status> {
    let token = AuthzPageToken {
        version: 1,
        tenant_id: claims.tenant_id,
        revision: claims.revision,
        filter_hash: claims.filter_hash.to_string(),
        offset: claims.offset,
        signature: sign_authz_page_token(claims, signing_key)?,
    };
    let bytes = serde_json::to_vec(&token)
        .map_err(|_| Status::internal("Failed to encode authz page token"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn sign_authz_page_token(
    claims: AuthzPageTokenClaims<'_>,
    signing_key: &[u8],
) -> Result<String, Status> {
    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key)
        .map_err(|_| Status::internal("Invalid authz page token signing key"))?;
    mac.update(b"authz-page-token-v1");
    mac.update(&claims.tenant_id.to_le_bytes());
    mac.update(&claims.revision.to_le_bytes());
    mac.update(&(claims.filter_hash.len() as u64).to_le_bytes());
    mac.update(claims.filter_hash.as_bytes());
    mac.update(&(claims.offset as u64).to_le_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn authz_page_filter_hash(kind: &str, values: &[&str]) -> String {
    let mut input = Vec::new();
    input.extend_from_slice(&(kind.len() as u64).to_le_bytes());
    input.extend_from_slice(kind.as_bytes());
    for value in values {
        input.extend_from_slice(&(value.len() as u64).to_le_bytes());
        input.extend_from_slice(value.as_bytes());
    }
    hex::encode(hash32(&input))
}

fn normalize_page_size(value: u32) -> usize {
    if value == 0 {
        1000
    } else {
        usize::try_from(value.min(1000)).unwrap_or(1000)
    }
}

fn authz_tuple_log_response(
    record: &crate::persistence::AuthzTupleRecord,
) -> WatchAuthzTupleLogResponse {
    let revision = revision_to_u64(record.revision).unwrap_or_default();
    let written_at = record.written_at.to_string();
    WatchAuthzTupleLogResponse {
        revision,
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
        written_at: written_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "authz_tuple_log",
            partition_family: "authz_tuple",
            partition_id: record.namespace.clone(),
            cursor: revision.into(),
            mutation_id: record.mutation_id.to_string(),
            record_kind: "authz_tuple".to_string(),
            object_ref: format!(
                "{}:{}#{}",
                record.namespace, record.object_id, record.relation
            ),
            authz_revision: revision,
            index_generation: 0,
            personaldb_log_index: 0,
            payload_hash: record.record_hash.clone(),
            emitted_at: written_at,
        })),
    }
}

fn authz_tuple_log_response_for_realm(
    record: &crate::persistence::AuthzTupleRecord,
    realm_id: &str,
) -> WatchAuthzTupleLogResponse {
    let mut response = authz_tuple_log_response(record);
    if let Some(namespace) = decode_realm_namespace(realm_id, &response.namespace) {
        response.namespace = namespace.to_string();
    }
    response.subject_id =
        decode_userset_subject_realm(realm_id, &response.subject_kind, &response.subject_id);
    response
}

fn authz_namespace_watch_response(
    event: authz_namespace_watch::AuthzNamespaceWatchEvent,
) -> WatchAuthzNamespaceResponse {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    let payload = event.payload;
    let emitted_at = payload.emitted_at.clone();
    let namespace = payload.namespace.clone();
    let payload_hash = watch_envelope::payload_hash(&payload);
    WatchAuthzNamespaceResponse {
        cursor_low,
        cursor_high,
        namespace: namespace.clone(),
        event_type: payload.event_type,
        authz_revision: event.authz_revision,
        schema_hash: payload.schema_hash,
        invalidates_derived_usersets: payload.invalidates_derived_usersets,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "authz_namespace",
            partition_family: "authz_namespace",
            partition_id: namespace.clone(),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "authz_namespace".to_string(),
            object_ref: namespace,
            authz_revision: event.authz_revision,
            index_generation: 0,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    }
}

fn authz_derived_lag_watch_response(
    event: authz_derived_lag_watch::AuthzDerivedLagWatchEvent,
) -> WatchAuthzDerivedLagResponse {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    let (source_cursor_low, source_cursor_high) = split_u128(event.payload.source_cursor);
    let revision_lag = event.payload.revision_lag();
    let payload = event.payload;
    let emitted_at = payload.emitted_at.clone();
    let derived_index_id = payload.derived_index_id.clone();
    let generation = payload.generation;
    let latest_revision = payload.latest_revision;
    let payload_hash = watch_envelope::payload_hash(&payload);
    WatchAuthzDerivedLagResponse {
        cursor_low,
        cursor_high,
        derived_index_id: derived_index_id.clone(),
        derived_index_kind: payload.derived_index_kind,
        processed_revision: payload.processed_revision,
        latest_revision,
        revision_lag,
        source_cursor_low,
        source_cursor_high,
        source_manifest_hash: payload.source_manifest_hash,
        generation,
        authz_revision: event.authz_revision,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "authz_derived_lag",
            partition_family: "authz_derived_lag",
            partition_id: derived_index_id.clone(),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "authz_derived_lag".to_string(),
            object_ref: derived_index_id,
            authz_revision: event.authz_revision,
            index_generation: generation,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    }
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}

fn mutation_id_from_record_hash(record_hash: &str) -> [u8; 16] {
    let mut mutation_id = [0; 16];
    if let Ok(bytes) = hex::decode(record_hash) {
        let len = bytes.len().min(mutation_id.len());
        mutation_id[..len].copy_from_slice(&bytes[..len]);
    }
    mutation_id
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

    #[test]
    fn authz_watch_cursor_split_round_trips() {
        let cursor = (u128::from(99_u64) << 64) | u128::from(42_u64);
        let (low, high) = split_u128(cursor);
        assert_eq!(join_u128(low, high), cursor);
    }
}
