use super::admin_cursor::{self, AdminCursorBinding};
use crate::admin_audit::{self, AdminAuditEvent, AuditEventFilter};
use crate::admin_auth::{self, AdminPrincipal, AnvilAdminCapability};
use crate::anvil_api::admin_service_server::AdminService;
use crate::anvil_api::*;
use crate::mesh_lifecycle::{
    self, CreateHostAliasDescriptor, CreateRegionDescriptor, LifecycleError,
    LifecycleState as CoreLifecycleState, NodeCapability as CoreNodeCapability,
    NodeDrainDescriptor, RegisterCellDescriptor, RegisterNodeDescriptor,
};
use crate::object_links;
use crate::persistence;
use crate::repair_finding::{RepairFinding, RepairSubjectRef};
use crate::routing::{
    self, HostAliasDescriptor as CoreHostAliasDescriptor, HostAliasState as CoreHostAliasState,
    RoutingConfig,
};
use crate::{
    AppState, auth, authz_repair, crypto, directory_repair, index_repair, mesh_control_stream,
    mesh_directory, persistence::Bucket, personaldb_repair,
};
use chrono::Utc;
use serde_json::json;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl AdminService for AppState {
    async fn create_tenant(
        &self,
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<TenantAdminResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageTenants)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let home_region = if req.home_region.trim().is_empty() {
            self.config.region.clone()
        } else {
            req.home_region.clone()
        };
        let tenant = self
            .persistence
            .create_tenant(&req.name, "admin-created")
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.tenant.create",
            &format!("tenant:{}", tenant.id),
            json!({
                "resource_kind": "tenant",
                "tenant_id": tenant.id,
                "tenant_name": &tenant.name,
                "home_region": &home_region,
            }),
        )
        .await?;
        Ok(Response::new(TenantAdminResponse {
            request_id: context.request_id.clone(),
            tenant: Some(TenantAdminDescriptor {
                tenant_id: tenant.id.to_string(),
                name: tenant.name,
                home_region,
            }),
            audit_event_id,
        }))
    }

    async fn create_application(
        &self,
        request: Request<CreateApplicationRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageApps)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let client_id = generated_client_id();
        let client_secret = generated_client_secret();
        let encrypted_secret = encrypt_admin_client_secret(self, &client_secret)?;
        let app = self
            .persistence
            .create_app(tenant_id, &req.app_name, &client_id, &encrypted_secret)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.app.create",
            &format!("app:{}", app.client_id),
            json!({
                "resource_kind": "application",
                "tenant_id": tenant_id,
                "app_id": app.id,
                "app_name": &app.name,
                "client_id": &app.client_id,
            }),
        )
        .await?;
        Ok(Response::new(ApplicationSecretResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            client_id: app.client_id,
            client_secret,
            audit_event_id,
        }))
    }

    async fn rotate_application_secret(
        &self,
        request: Request<RotateApplicationSecretRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageApps)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let app = self
            .persistence
            .list_apps_for_tenant(tenant_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .into_iter()
            .find(|app| app.name == req.app_name)
            .ok_or_else(|| Status::not_found("Application not found"))?;
        let client_secret = generated_client_secret();
        let encrypted_secret = encrypt_admin_client_secret(self, &client_secret)?;
        self.persistence
            .update_app_secret(app.id, &encrypted_secret)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.app.secret.rotate",
            &format!("app:{}", app.client_id),
            json!({
                "resource_kind": "application",
                "tenant_id": tenant_id,
                "app_id": app.id,
                "app_name": &app.name,
                "client_id": &app.client_id,
            }),
        )
        .await?;
        Ok(Response::new(ApplicationSecretResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            client_id: app.client_id,
            client_secret,
            audit_event_id,
        }))
    }

    async fn create_bucket_admin(
        &self,
        request: Request<CreateBucketAdminRequest>,
    ) -> Result<Response<BucketAdminResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageBuckets)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let bucket = self
            .persistence
            .create_bucket(tenant_id, &req.bucket_name, &req.region)
            .await?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.bucket.create",
            &bucket_resource_id(tenant_id, &bucket.name),
            json!({
                "resource_kind": "bucket",
                "tenant_id": tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": &bucket.name,
                "region": &bucket.region,
                "is_public_read": bucket.is_public_read,
            }),
        )
        .await?;
        Ok(Response::new(BucketAdminResponse {
            request_id: context.request_id.clone(),
            bucket: Some(bucket_to_proto(bucket)),
            audit_event_id,
        }))
    }

    async fn set_bucket_public_access_admin(
        &self,
        request: Request<SetBucketPublicAccessAdminRequest>,
    ) -> Result<Response<BucketAdminResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageBuckets)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        self.persistence
            .set_bucket_public_access(tenant_id, &req.bucket_name, req.allow_public_read)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let bucket = self
            .persistence
            .get_bucket_by_name(tenant_id, &req.bucket_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.bucket.public_access.set",
            &bucket_resource_id(tenant_id, &bucket.name),
            json!({
                "resource_kind": "bucket",
                "tenant_id": tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": &bucket.name,
                "region": &bucket.region,
                "allow_public_read": req.allow_public_read,
                "is_public_read": bucket.is_public_read,
            }),
        )
        .await?;
        Ok(Response::new(BucketAdminResponse {
            request_id: context.request_id.clone(),
            bucket: Some(bucket_to_proto(bucket)),
            audit_event_id,
        }))
    }

    async fn create_object_link(
        &self,
        request: Request<CreateObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageLinks)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let request_id = context.request_id.clone();
        let idempotency_key = context.idempotency_key.clone();
        let bucket = resolve_link_bucket(self, &req.tenant_id, &req.bucket_name).await?;
        let resolution = object_link_resolution_from_proto(req.resolution)?;
        let target_version = parse_optional_uuid("target_version", req.target_version)?;
        let mutation = self
            .persistence
            .put_object_link(object_links::PutObjectLinkRequest {
                tenant_id: bucket.tenant_id,
                bucket_id: bucket.id,
                link_key: req.link_key,
                target_key: req.target_key,
                target_version,
                resolution,
                expected_generation: None,
                create_only: true,
                allow_dangling: req.allow_dangling,
                idempotency_key,
                created_by: principal_label(&principal),
            })
            .await
            .map_err(object_link_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.object_link.create",
            &object_link_resource_id(
                bucket.tenant_id,
                &bucket.name,
                &mutation.descriptor.link_key,
            ),
            json!({
                "resource_kind": "object_link",
                "tenant_id": bucket.tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": &bucket.name,
                "link_key": &mutation.descriptor.link_key,
                "target_key": &mutation.descriptor.target_key,
                "target_version": &mutation.descriptor.target_version,
                "resolution": mutation.descriptor.resolution,
                "allow_dangling": req.allow_dangling,
                "generation": mutation.descriptor.generation,
                "created_by": &mutation.descriptor.created_by,
            }),
        )
        .await?;

        Ok(Response::new(ObjectLinkResponse {
            request_id,
            link: Some(object_link_descriptor_to_proto(mutation.descriptor)),
            audit_event_id,
        }))
    }

    async fn update_object_link(
        &self,
        request: Request<UpdateObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageLinks)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let request_id = context.request_id.clone();
        let idempotency_key = context.idempotency_key.clone();
        let expected_generation = context.expected_generation;
        let bucket = resolve_link_bucket(self, &req.tenant_id, &req.bucket_name).await?;
        let resolution = object_link_resolution_from_proto(req.resolution)?;
        let target_version = parse_optional_uuid("target_version", req.target_version)?;
        let mutation = self
            .persistence
            .put_object_link(object_links::PutObjectLinkRequest {
                tenant_id: bucket.tenant_id,
                bucket_id: bucket.id,
                link_key: req.link_key,
                target_key: req.target_key,
                target_version,
                resolution,
                expected_generation: Some(expected_generation),
                create_only: false,
                allow_dangling: req.allow_dangling,
                idempotency_key,
                created_by: principal_label(&principal),
            })
            .await
            .map_err(object_link_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.object_link.update",
            &object_link_resource_id(
                bucket.tenant_id,
                &bucket.name,
                &mutation.descriptor.link_key,
            ),
            json!({
                "resource_kind": "object_link",
                "tenant_id": bucket.tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": &bucket.name,
                "link_key": &mutation.descriptor.link_key,
                "target_key": &mutation.descriptor.target_key,
                "target_version": &mutation.descriptor.target_version,
                "resolution": mutation.descriptor.resolution,
                "allow_dangling": req.allow_dangling,
                "previous_generation": expected_generation,
                "generation": mutation.descriptor.generation,
                "created_by": &mutation.descriptor.created_by,
            }),
        )
        .await?;

        Ok(Response::new(ObjectLinkResponse {
            request_id,
            link: Some(object_link_descriptor_to_proto(mutation.descriptor)),
            audit_event_id,
        }))
    }

    async fn delete_object_link(
        &self,
        request: Request<DeleteObjectLinkRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageLinks)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let request_id = context.request_id.clone();
        let idempotency_key = context.idempotency_key.clone();
        let expected_generation = context.expected_generation;
        let bucket = resolve_link_bucket(self, &req.tenant_id, &req.bucket_name).await?;
        let deleted = self
            .persistence
            .delete_object_link(object_links::DeleteObjectLinkRequest {
                tenant_id: bucket.tenant_id,
                bucket_id: bucket.id,
                link_key: req.link_key,
                expected_generation,
                idempotency_key,
            })
            .await
            .map_err(object_link_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.object_link.delete",
            &object_link_resource_id(bucket.tenant_id, &bucket.name, &deleted.link_key),
            json!({
                "resource_kind": "object_link",
                "tenant_id": bucket.tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": &bucket.name,
                "link_key": &deleted.link_key,
                "previous_generation": expected_generation,
                "generation": deleted.generation,
            }),
        )
        .await?;

        Ok(Response::new(AdminMutationResponse {
            request_id,
            resource_id: deleted.link_key,
            generation: deleted.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn read_object_link(
        &self,
        request: Request<ReadObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        require_admin(&request, self, AnvilAdminCapability::ManageLinks)?;
        let req = request.into_inner();
        let bucket = resolve_link_bucket(self, &req.tenant_id, &req.bucket_name).await?;
        let descriptor = self
            .persistence
            .get_object_link(bucket.id, &req.link_key)
            .await
            .map_err(object_link_status)?
            .ok_or_else(|| Status::not_found("Object link not found"))?;

        Ok(Response::new(ObjectLinkResponse {
            request_id: req.request_id,
            link: Some(object_link_descriptor_to_proto(descriptor)),
            audit_event_id: String::new(),
        }))
    }

    async fn list_object_links(
        &self,
        request: Request<ListObjectLinksRequest>,
    ) -> Result<Response<ListObjectLinksResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageLinks)?;
        let req = request.into_inner();
        let bucket = resolve_link_bucket(self, &req.tenant_id, &req.bucket_name).await?;
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let links = self
            .persistence
            .list_object_links(bucket.id, none_if_empty(&req.prefix))
            .await
            .map_err(object_link_status)?;
        let revision = admin_cursor::collection_revision(
            links
                .iter()
                .map(|link| (link.link_key.as_str(), link.generation)),
        );
        let tenant_id_filter = bucket.tenant_id.to_string();
        let filters = [
            ("tenant_id", tenant_id_filter.as_str()),
            ("bucket_name", bucket.name.as_str()),
            ("prefix", req.prefix.as_str()),
        ];
        let binding = AdminCursorBinding {
            scope: "admin.list_object_links.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "link_key.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut links = links
            .into_iter()
            .filter(|link| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| link.link_key.as_str() > cursor)
            })
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = links.len() > limit;
        if has_more {
            links.truncate(limit);
        }
        let next_cursor = if has_more {
            links.last().map_or(Ok(String::new()), |link| {
                admin_cursor::encode_next_cursor(
                    &link.link_key,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };

        Ok(Response::new(ListObjectLinksResponse {
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            links: links
                .into_iter()
                .map(object_link_descriptor_to_proto)
                .collect(),
        }))
    }

    async fn create_host_alias(
        &self,
        request: Request<CreateHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageHostAliases)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let request_id = context.request_id.clone();
        let routing_config = routing_config_for_region(self, &req.region).await?;
        let host_alias = self
            .persistence
            .create_host_alias_descriptor(
                &routing_config,
                CreateHostAliasDescriptor {
                    hostname: req.hostname,
                    tenant_id: req.tenant_id,
                    bucket_name: req.bucket_name,
                    region: req.region,
                    prefix: req.prefix,
                },
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.create",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id,
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn activate_host_alias(
        &self,
        request: Request<ActivateHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageHostAliases)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &req.hostname,
                context.expected_generation,
                CoreHostAliasState::Active,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.activate",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id: context.request_id.clone(),
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn suspend_host_alias(
        &self,
        request: Request<SuspendHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageHostAliases)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &req.hostname,
                context.expected_generation,
                CoreHostAliasState::Suspended,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.suspend",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id: context.request_id.clone(),
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn delete_host_alias(
        &self,
        request: Request<DeleteHostAliasRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageHostAliases)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &req.hostname,
                context.expected_generation,
                CoreHostAliasState::Deleted,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.delete",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: host_alias.hostname,
            generation: host_alias.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn read_host_alias(
        &self,
        request: Request<ReadHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        require_admin(&request, self, AnvilAdminCapability::ManageHostAliases)?;
        let req = request.into_inner();
        let host_alias = self
            .persistence
            .get_host_alias_descriptor(&req.hostname)
            .await
            .map_err(lifecycle_status)?
            .ok_or_else(|| Status::not_found("Host alias not found"))?;

        Ok(Response::new(HostAliasResponse {
            request_id: req.request_id,
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id: String::new(),
        }))
    }

    async fn list_host_aliases(
        &self,
        request: Request<ListHostAliasesRequest>,
    ) -> Result<Response<ListHostAliasesResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageHostAliases)?;
        let req = request.into_inner();
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let host_aliases = self
            .persistence
            .list_host_alias_descriptors(none_if_empty(&req.region))
            .await
            .map_err(lifecycle_status)?;
        let revision = admin_cursor::collection_revision(
            host_aliases
                .iter()
                .map(|alias| (alias.hostname.as_str(), alias.generation)),
        );
        let filters = [("region", req.region.as_str())];
        let binding = AdminCursorBinding {
            scope: "admin.list_host_aliases.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "hostname.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut host_aliases = host_aliases
            .into_iter()
            .filter(|alias| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| alias.hostname.as_str() > cursor)
            })
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = host_aliases.len() > limit;
        if has_more {
            host_aliases.truncate(limit);
        }
        let next_cursor = if has_more {
            host_aliases.last().map_or(Ok(String::new()), |alias| {
                admin_cursor::encode_next_cursor(
                    &alias.hostname,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };

        Ok(Response::new(ListHostAliasesResponse {
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            host_aliases: host_aliases
                .into_iter()
                .map(host_alias_descriptor_to_proto)
                .collect(),
        }))
    }

    async fn create_region(
        &self,
        request: Request<CreateRegionRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let region = self
            .persistence
            .create_region_descriptor(CreateRegionDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                region: req.region,
                public_base_url: req.public_base_url,
                virtual_host_suffix: req.virtual_host_suffix,
                placement_weight: req.placement_weight,
                default_cell: empty_to_none(req.default_cell),
            })
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.create",
            &format!("region:{}", region.region),
            region_audit_details(&region),
        )
        .await?;
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id,
        }))
    }

    async fn activate_region(
        &self,
        request: Request<ActivateRegionRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let checkpoint =
            mesh_lifecycle::parse_activation_checkpoint_json(&req.activation_checkpoint_json)
                .map_err(lifecycle_status)?;
        let region = self
            .persistence
            .activate_region_descriptor(&req.region, context.expected_generation, &checkpoint)
            .await
            .map_err(lifecycle_status)?;
        let mut details = region_audit_details(&region);
        add_audit_detail(&mut details, "activation_checkpoint", json!(&checkpoint));
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.activate",
            &format!("region:{}", region.region),
            details,
        )
        .await?;
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id,
        }))
    }

    async fn set_region_read_only(
        &self,
        request: Request<SetRegionReadOnlyRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::ReadOnly,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.read_only.set",
            &format!("region:{}", region.region),
            region_audit_details(&region),
        )
        .await?;
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id,
        }))
    }

    async fn drain_region(
        &self,
        request: Request<DrainRegionRequest>,
    ) -> Result<Response<DrainOperationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let default_disposition =
            region_drain_disposition_from_proto(req.default_disposition, true)?;
        let bucket_overrides = req
            .bucket_overrides
            .iter()
            .map(bucket_drain_override_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::Draining,
            )
            .await
            .map_err(lifecycle_status)?;
        let drain_report = self
            .persistence
            .apply_region_drain_plan(&region.region, default_disposition, bucket_overrides)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        let mut details = region_audit_details(&region);
        add_audit_detail(
            &mut details,
            "default_disposition",
            json!(default_disposition.as_str()),
        );
        add_audit_detail(
            &mut details,
            "default_disposition_code",
            json!(req.default_disposition),
        );
        add_audit_detail(
            &mut details,
            "bucket_overrides",
            json!(bucket_drain_overrides_details(&req.bucket_overrides)),
        );
        add_audit_detail(
            &mut details,
            "bucket_disposition_decisions",
            region_drain_plan_details(&drain_report),
        );
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.drain",
            &format!("region:{}", region.region),
            details,
        )
        .await?;
        for decision in &drain_report.decisions {
            record_admin_audit_event_with_suffix(
                self,
                &principal,
                context,
                "admin.region.bucket_disposition",
                &format!(
                    "tenant:{}:bucket:{}:region:{}",
                    decision.tenant_id, decision.bucket_name, drain_report.region
                ),
                json!({
                    "region": &drain_report.region,
                    "tenant_id": &decision.tenant_id,
                    "bucket_name": &decision.bucket_name,
                    "disposition": decision.disposition.as_str(),
                    "reason": &decision.reason,
                    "expires_at": decision.expires_at.as_deref(),
                    "status_before": format!("{:?}", decision.status_before),
                    "status_after": format!("{:?}", decision.status_after),
                    "bucket_locator_generation_before": decision.bucket_locator_generation_before,
                    "bucket_locator_generation_after": decision.bucket_locator_generation_after,
                    "exception_written": decision.exception_written,
                    "locator_updated": decision.locator_updated,
                }),
                &format!(
                    "bucket-disposition-{}-{}",
                    decision.tenant_id, decision.bucket_name
                ),
            )
            .await?;
        }
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: region.region,
            state: lifecycle_state_to_proto(region.state),
            generation: region.generation,
            audit_event_id,
        }))
    }

    async fn remove_region(
        &self,
        request: Request<RemoveRegionRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::Removed,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.remove",
            &format!("region:{}", region.region),
            region_audit_details(&region),
        )
        .await?;
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: region.region,
            generation: region.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn list_regions(
        &self,
        request: Request<ListRegionsRequest>,
    ) -> Result<Response<ListRegionsResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let mut regions = self
            .persistence
            .list_region_descriptors()
            .await
            .map_err(lifecycle_status)?;
        regions.sort_by(|left, right| left.region.cmp(&right.region));
        let revision = admin_cursor::collection_revision(
            regions
                .iter()
                .map(|region| (region.region.as_str(), region.generation)),
        );
        let binding = AdminCursorBinding {
            scope: "admin.list_regions.v1",
            filters: &[],
            principal: &principal,
            limit,
            revision: &revision,
            sort: "region.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut regions = regions
            .into_iter()
            .filter(|region| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| region.region.as_str() > cursor)
            })
            .take(limit + 1)
            .map(region_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = regions.len() > limit;
        if has_more {
            regions.truncate(limit);
        }
        let next_cursor = if has_more {
            regions.last().map_or(Ok(String::new()), |region| {
                admin_cursor::encode_next_cursor(
                    &region.region,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };
        Ok(Response::new(ListRegionsResponse {
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            regions,
        }))
    }

    async fn register_cell(
        &self,
        request: Request<RegisterCellRequest>,
    ) -> Result<Response<CellResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let cell = self
            .persistence
            .register_cell_descriptor(RegisterCellDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                region: req.region,
                cell_id: req.cell_id,
                placement_weight: req.placement_weight,
            })
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.register",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(CellResponse {
            request_id: context.request_id.clone(),
            cell: Some(cell_descriptor_to_proto(cell)),
            audit_event_id,
        }))
    }

    async fn activate_cell(
        &self,
        request: Request<ActivateCellRequest>,
    ) -> Result<Response<CellResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let cell = self
            .persistence
            .transition_cell_descriptor(
                &req.region,
                &req.cell_id,
                context.expected_generation,
                CoreLifecycleState::Active,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.activate",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(CellResponse {
            request_id: context.request_id.clone(),
            cell: Some(cell_descriptor_to_proto(cell)),
            audit_event_id,
        }))
    }

    async fn drain_cell(
        &self,
        request: Request<DrainCellRequest>,
    ) -> Result<Response<DrainOperationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let cell = self
            .persistence
            .transition_cell_descriptor(
                &req.region,
                &req.cell_id,
                context.expected_generation,
                CoreLifecycleState::Draining,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.drain",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: cell.cell_id,
            state: lifecycle_state_to_proto(cell.state),
            generation: cell.generation,
            audit_event_id,
        }))
    }

    async fn remove_cell(
        &self,
        request: Request<RemoveCellRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let cell = self
            .persistence
            .transition_cell_descriptor(
                &req.region,
                &req.cell_id,
                context.expected_generation,
                CoreLifecycleState::Removed,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.remove",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: cell.cell_id,
            generation: cell.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn list_cells(
        &self,
        request: Request<ListCellsRequest>,
    ) -> Result<Response<ListCellsResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let region_filter = none_if_empty(&req.region);
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let mut cells = self
            .persistence
            .list_cell_descriptors(region_filter)
            .await
            .map_err(lifecycle_status)?;
        cells.sort_by(|left, right| {
            left.region
                .cmp(&right.region)
                .then(left.cell_id.cmp(&right.cell_id))
        });
        let revision_keys = cells
            .iter()
            .map(|cell| (format!("{}/{}", cell.region, cell.cell_id), cell.generation))
            .collect::<Vec<_>>();
        let revision = admin_cursor::collection_revision(
            revision_keys
                .iter()
                .map(|(key, generation)| (key.as_str(), *generation)),
        );
        let filters = [("region", req.region.as_str())];
        let binding = AdminCursorBinding {
            scope: "admin.list_cells.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "region_cell.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut cells = cells
            .into_iter()
            .filter(|cell| {
                cursor.as_deref().is_none_or(|cursor| {
                    format!("{}/{}", cell.region, cell.cell_id).as_str() > cursor
                })
            })
            .take(limit + 1)
            .map(cell_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = cells.len() > limit;
        if has_more {
            cells.truncate(limit);
        }
        let next_cursor = if has_more {
            cells.last().map_or(Ok(String::new()), |cell| {
                admin_cursor::encode_next_cursor(
                    &format!("{}/{}", cell.region, cell.cell_id),
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };
        Ok(Response::new(ListCellsResponse {
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            cells,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<NodeResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageNodes)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let capabilities = req
            .capabilities
            .into_iter()
            .map(node_capability_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let node = self
            .persistence
            .register_node_descriptor(RegisterNodeDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                node_id: req.node_id,
                region: req.region,
                cell_id: req.cell_id,
                libp2p_peer_id: req.libp2p_peer_id,
                public_api_addr: req.public_api_addr,
                public_cluster_addrs: req.public_cluster_addrs,
                capabilities,
            })
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.register",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id,
        }))
    }

    async fn activate_node(
        &self,
        request: Request<ActivateNodeRequest>,
    ) -> Result<Response<NodeResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageNodes)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Active,
                None,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.activate",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id,
        }))
    }

    async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainOperationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageNodes)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Draining,
                Some(NodeDrainDescriptor {
                    started_at: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    graceful_timeout_ms: req.graceful_timeout_ms,
                    force_after_timeout: req.force_after_timeout,
                }),
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.drain",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: node.node_id,
            state: lifecycle_state_to_proto(node.state),
            generation: node.generation,
            audit_event_id,
        }))
    }

    async fn force_offline_node(
        &self,
        request: Request<ForceOfflineNodeRequest>,
    ) -> Result<Response<NodeResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageNodes)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Offline,
                None,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.force_offline",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id,
        }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageNodes)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Removed,
                None,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.remove",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: node.node_id,
            generation: node.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn list_nodes(
        &self,
        request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageNodes)?;
        let req = request.into_inner();
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let mut nodes = self
            .persistence
            .list_node_descriptors(none_if_empty(&req.region), none_if_empty(&req.cell_id))
            .await
            .map_err(lifecycle_status)?;
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let revision = admin_cursor::collection_revision(
            nodes
                .iter()
                .map(|node| (node.node_id.as_str(), node.generation)),
        );
        let filters = [
            ("region", req.region.as_str()),
            ("cell_id", req.cell_id.as_str()),
        ];
        let binding = AdminCursorBinding {
            scope: "admin.list_nodes.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "node_id.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut nodes = nodes
            .into_iter()
            .filter(|node| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| node.node_id.as_str() > cursor)
            })
            .take(limit + 1)
            .map(node_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = nodes.len() > limit;
        if has_more {
            nodes.truncate(limit);
        }
        let next_cursor = if has_more {
            nodes.last().map_or(Ok(String::new()), |node| {
                admin_cursor::encode_next_cursor(
                    &node.node_id,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };
        Ok(Response::new(ListNodesResponse {
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            nodes,
        }))
    }

    async fn list_routing_records(
        &self,
        request: Request<ListRoutingRecordsRequest>,
    ) -> Result<Response<ListRoutingRecordsResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRouting)?;
        let req = request.into_inner();
        let family = routing_record_family_from_proto(req.family)?;
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let mut records = self
            .persistence
            .list_mesh_routing_records(family)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        records.sort_by(|left, right| left.descriptor_key.cmp(&right.descriptor_key));
        let revision = admin_cursor::collection_revision(
            records
                .iter()
                .map(|record| (record.descriptor_key.as_str(), record.generation)),
        );
        let family_filter = req.family.to_string();
        let filters = [("family", family_filter.as_str())];
        let binding = AdminCursorBinding {
            scope: "admin.list_routing_records.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "descriptor_key.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut records = records
            .into_iter()
            .filter(|record| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| record.descriptor_key.as_str() > cursor)
            })
            .take(limit + 1)
            .map(routing_record_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = records.len() > limit;
        if has_more {
            records.truncate(limit);
        }
        let next_cursor = if has_more {
            records.last().map_or(Ok(String::new()), |record| {
                admin_cursor::encode_next_cursor(
                    &record.descriptor_key,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };

        Ok(Response::new(ListRoutingRecordsResponse {
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            records,
        }))
    }

    async fn repair_routing_record(
        &self,
        request: Request<RepairRoutingRecordRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRouting)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let family = routing_record_family_from_proto(req.family)?
            .ok_or_else(|| Status::invalid_argument("routing record family is required"))?;
        let record = self
            .persistence
            .repair_mesh_routing_record(family, &req.record_key)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.routing_record.repair",
            &format!("routing_record:{}", record.descriptor_key),
            json!({
                "resource_kind": "routing_record",
                "family": record.family,
                "record_key": &record.record_key,
                "partition": &record.partition,
                "descriptor_key": &record.descriptor_key,
                "generation": record.generation,
                "payload_json": &record.payload_json,
            }),
        )
        .await?;

        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: record.descriptor_key,
            generation: record.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn run_repair(
        &self,
        request: Request<RunRepairRequest>,
    ) -> Result<Response<RepairTaskResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::RunRepair)?;
        let req = request.into_inner();
        let context = require_admin_action_context(req.context.as_ref())?;
        let request_id = context.request_id.clone();
        let audit_event_id = audit_event_id(&principal, context);

        let response = match req.repair_kind {
            1 => run_index_repair(self, &request_id, &audit_event_id, &req).await?,
            2 => run_directory_index_repair(self, &request_id, &audit_event_id, &req).await?,
            3 => run_authz_derived_index_repair(self, &request_id, &audit_event_id, &req).await?,
            4 => run_personaldb_log_chain_repair(self, &request_id, &audit_event_id, &req).await?,
            5 => run_mesh_routing_projection_repair(self, &request_id, &audit_event_id).await?,
            _ => {
                return Err(Status::invalid_argument(
                    "repair_kind must select a supported repair backend",
                ));
            }
        };
        record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.repair.run",
            &response.repair_task_id,
            json!({
                "repair_kind": req.repair_kind,
                "scope_kind": &response.scope_kind,
                "scope_id": &response.scope_id,
                "status": &response.status,
                "repair_task_details_json": &response.details_json,
            }),
        )
        .await?;

        Ok(Response::new(response))
    }

    async fn list_diagnostics(
        &self,
        request: Request<ListDiagnosticsRequest>,
    ) -> Result<Response<DiagnosticsResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ViewDiagnostics)?;
        let req = request.into_inner();
        let request_id = require_request_id(&req.request_id)?.to_string();
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let source = req.source.trim();

        if !req.severity.trim().is_empty() {
            validate_diagnostic_severity(&req.severity)?;
        }

        let mut diagnostics = Vec::new();

        if source.is_empty() || source == "index" || source == "index_diagnostic_journal" {
            if req.tenant_id.trim().is_empty() || req.bucket_name.trim().is_empty() {
                if source == "index" || source == "index_diagnostic_journal" {
                    return Err(Status::invalid_argument(
                        "tenant_id and bucket_name are required for index diagnostics",
                    ));
                }
            } else {
                let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
                let bucket = self
                    .persistence
                    .get_bucket_by_name(tenant_id, &req.bucket_name)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?
                    .ok_or_else(|| Status::not_found("Bucket not found"))?;
                diagnostics.extend(
                    self.persistence
                        .list_index_diagnostics(
                            tenant_id,
                            bucket.id,
                            &req.index_name,
                            &req.severity,
                            0,
                            i32::MAX,
                        )
                        .await
                        .map_err(|err| Status::internal(err.to_string()))?
                        .into_iter()
                        .map(index_diagnostic_to_admin_record)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
        }

        if source.is_empty() || source == "mesh" || source == "mesh_lifecycle" {
            diagnostics.extend(mesh_lifecycle_diagnostics(self).await?);
        }

        if source.is_empty() || source == "mesh" || source == "mesh_routing_projection" {
            diagnostics.extend(mesh_routing_projection_diagnostics(self).await?);
        }

        if !req.severity.trim().is_empty() {
            diagnostics.retain(|diagnostic| diagnostic.severity == req.severity);
        }
        diagnostics
            .sort_by(|left, right| diagnostic_position(left).cmp(&diagnostic_position(right)));

        let positions = diagnostics
            .iter()
            .map(|diagnostic| (diagnostic_position(diagnostic), diagnostic.cursor))
            .collect::<Vec<_>>();
        let revision = admin_cursor::collection_revision(
            positions
                .iter()
                .map(|(position, cursor)| (position.as_str(), *cursor)),
        );
        let filters = [
            ("source", source),
            ("tenant_id", req.tenant_id.trim()),
            ("bucket_name", req.bucket_name.trim()),
            ("index_name", req.index_name.trim()),
            ("severity", req.severity.trim()),
        ];
        let binding = AdminCursorBinding {
            scope: "admin.list_diagnostics.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "source.cursor.id.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut diagnostics = diagnostics
            .into_iter()
            .filter(|diagnostic| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| diagnostic_position(diagnostic).as_str() > cursor)
            })
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = diagnostics.len() > limit;
        if has_more {
            diagnostics.truncate(limit);
        }
        let next_cursor = if has_more {
            diagnostics.last().map_or(Ok(String::new()), |diagnostic| {
                admin_cursor::encode_next_cursor(
                    &diagnostic_position(diagnostic),
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };

        Ok(Response::new(DiagnosticsResponse {
            request_id,
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            diagnostics,
            data_source: if source.is_empty() {
                "combined".to_string()
            } else if source == "index" {
                "index_diagnostic_journal".to_string()
            } else if source == "mesh" {
                "mesh".to_string()
            } else {
                source.to_string()
            },
        }))
    }

    async fn list_audit_events(
        &self,
        request: Request<ListAuditEventsRequest>,
    ) -> Result<Response<AuditEventsResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ViewAuditLog)?;
        let req = request.into_inner();
        let request_id = require_request_id(&req.request_id)?.to_string();
        let page = req.page.as_ref();
        let limit = page_limit(page);
        let mut events = admin_audit::list_audit_events(
            &self.storage,
            AuditEventFilter {
                principal_id: none_if_empty(&req.principal_id),
                resource_id: none_if_empty(&req.resource_id),
                action: none_if_empty(&req.action),
            },
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let revision = admin_cursor::collection_revision(
            events
                .iter()
                .map(|event| (event.audit_event_id.as_str(), 1_u64)),
        );
        let filters = [
            ("principal_id", req.principal_id.as_str()),
            ("resource_id", req.resource_id.as_str()),
            ("action", req.action.as_str()),
        ];
        let binding = AdminCursorBinding {
            scope: "admin.list_audit_events.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "created_at.audit_event_id.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        events.retain(|event| {
            cursor
                .as_deref()
                .is_none_or(|cursor| audit_cursor_position(event).as_str() > cursor)
        });
        events.truncate(limit + 1);
        let has_more = events.len() > limit;
        if has_more {
            events.truncate(limit);
        }
        let next_cursor = if has_more {
            events.last().map_or(Ok(String::new()), |event| {
                admin_cursor::encode_next_cursor(
                    &audit_cursor_position(event),
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };

        Ok(Response::new(AuditEventsResponse {
            request_id,
            page: Some(PageResponse {
                next_cursor,
                has_more,
            }),
            events: events.into_iter().map(audit_event_to_proto).collect(),
            data_source: "admin_audit_log".to_string(),
        }))
    }
}

fn require_admin<T>(
    request: &Request<T>,
    state: &AppState,
    capability: AnvilAdminCapability,
) -> Result<AdminPrincipal, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .ok_or_else(|| Status::unauthenticated("Missing admin bearer token"))?;
    if !admin_auth::has_admin_capability(claims, capability, &state.config.mesh_id) {
        return Err(Status::permission_denied(format!(
            "Missing anvil_admin capability {}",
            capability.as_str()
        )));
    }
    Ok(AdminPrincipal::from(claims))
}

fn require_mutation_context(
    context: Option<&AdminRequestContext>,
    create: bool,
) -> Result<&AdminRequestContext, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("Missing admin context"))?;
    if context.request_id.trim().is_empty() {
        return Err(Status::invalid_argument("Admin request_id is required"));
    }
    if context.idempotency_key.trim().is_empty() {
        return Err(Status::invalid_argument(
            "Admin idempotency_key is required",
        ));
    }
    if context.audit_reason.trim().is_empty() {
        return Err(Status::invalid_argument("Admin audit_reason is required"));
    }
    if create && context.expected_generation != 0 {
        return Err(Status::invalid_argument(
            "Create requests must use expected_generation = 0",
        ));
    }
    if !create && context.expected_generation == 0 {
        return Err(Status::invalid_argument(
            "Update requests must include expected_generation",
        ));
    }
    Ok(context)
}

fn require_admin_action_context(
    context: Option<&AdminRequestContext>,
) -> Result<&AdminRequestContext, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("Missing admin context"))?;
    require_request_id(&context.request_id)?;
    if context.idempotency_key.trim().is_empty() {
        return Err(Status::invalid_argument(
            "Admin idempotency_key is required",
        ));
    }
    if context.audit_reason.trim().is_empty() {
        return Err(Status::invalid_argument("Admin audit_reason is required"));
    }
    Ok(context)
}

fn require_request_id(request_id: &str) -> Result<&str, Status> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(Status::invalid_argument("Admin request_id is required"));
    }
    Ok(request_id)
}

async fn run_index_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.bucket_name, "bucket_name")?;
    require_nonempty_admin_field(&req.index_name, "index_name")?;
    let bucket = state
        .persistence
        .get_bucket_by_name(tenant_id, &req.bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))?;
    let report = state
        .persistence
        .repair_index_from_base_journal(tenant_id, &bucket.name, &req.index_name, req.rebuild)
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let (source_cursor_low, source_cursor_high) = split_u128_admin(report.source_cursor);
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: index_repair::status_name(&report.status).to_string(),
        scope_kind: "index".to_string(),
        scope_id: format!(
            "tenant-{tenant_id}-bucket-{}-index-{}",
            bucket.id, report.index_name
        ),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "index",
            "bucket_name": report.bucket_name,
            "index_name": report.index_name,
            "index_storage_id": report.index_storage_id,
            "source_cursor_low": source_cursor_low,
            "source_cursor_high": source_cursor_high,
            "reason": index_repair::status_reason(&report.status),
            "rebuilt": report.build.is_some(),
        })
        .to_string(),
    })
}

async fn run_directory_index_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.bucket_name, "bucket_name")?;
    let bucket = state
        .persistence
        .get_bucket_by_name(tenant_id, &req.bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))?;
    let report = state
        .persistence
        .repair_directory_index(tenant_id, &bucket.name, req.rebuild)
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let (source_cursor_low, source_cursor_high) = split_u128_admin(report.source_cursor);
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();
    let actual = report.actual.as_ref();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: directory_repair::status_name(&report.status).to_string(),
        scope_kind: "bucket".to_string(),
        scope_id: format!("tenant-{tenant_id}-bucket-{}", bucket.id),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "directory_index",
            "bucket_name": report.bucket_name,
            "source_cursor_low": source_cursor_low,
            "source_cursor_high": source_cursor_high,
            "expected_entry_count": report.expected.entry_count,
            "actual_entry_count": actual.map(|snapshot| snapshot.entry_count).unwrap_or_default(),
            "expected_snapshot_hash": report.expected.snapshot_hash,
            "actual_snapshot_hash": actual.map(|snapshot| snapshot.snapshot_hash.clone()).unwrap_or_default(),
            "reason": directory_repair::status_reason(&report.status),
            "rebuilt_manifest_hash": report
                .rebuilt
                .as_ref()
                .map(|rebuilt| rebuilt.manifest_hash.clone())
                .unwrap_or_default(),
        })
        .to_string(),
    })
}

async fn run_authz_derived_index_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.derived_index_id, "derived_index_id")?;
    let report = state
        .persistence
        .repair_authz_derived_userset_index(tenant_id, &req.derived_index_id, req.rebuild)
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: authz_repair::status_name(&report.status).to_string(),
        scope_kind: "authz_derived_index".to_string(),
        scope_id: format!("tenant-{tenant_id}-authz-{}", report.derived_index_id),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "authz_derived_index",
            "derived_index_id": report.derived_index_id,
            "processed_revision": report.processed_revision,
            "latest_revision": report.latest_revision,
            "source_records_hash": report.source_records_hash,
            "reason": authz_repair::status_reason(&report.status),
            "rebuilt": report.rebuilt_index.is_some(),
        })
        .to_string(),
    })
}

async fn run_personaldb_log_chain_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.database_id, "database_id")?;
    let report = state
        .persistence
        .repair_personaldb_log_chain(tenant_id, &req.database_id)
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: personaldb_repair::status_name(&report.status).to_string(),
        scope_kind: "personaldb".to_string(),
        scope_id: format!("tenant-{tenant_id}-database-{}", report.database_id),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "personaldb_log_chain",
            "database_id": report.database_id,
            "committed_log_index": report.committed_log_index,
            "verified_log_index": report.verified_log_index,
            "committed_log_hash": report.committed_log_hash,
            "reason": personaldb_repair::status_reason(&report.status),
        })
        .to_string(),
    })
}

async fn run_mesh_routing_projection_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
) -> Result<RepairTaskResponse, Status> {
    let diagnostics = state
        .persistence
        .diagnose_mesh_routing_projection(None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut repaired_records = Vec::new();
    let mut skipped_records = Vec::new();

    for diagnostic in diagnostics {
        if !diagnostic.repair_safe
            || diagnostic.proposed_action != "repair_routing_record_from_control_stream"
            || diagnostic.record_key.trim().is_empty()
        {
            skipped_records.push(json!({
                "stream_family": diagnostic.stream_family,
                "partition": diagnostic.partition,
                "record_key": diagnostic.record_key,
                "code": diagnostic.code,
                "reason": "diagnostic is not safe for automatic routing projection repair",
            }));
            continue;
        }
        let Some(family) =
            mesh_directory::RoutingRecordFamily::from_stream_family(&diagnostic.stream_family)
        else {
            skipped_records.push(json!({
                "stream_family": diagnostic.stream_family,
                "partition": diagnostic.partition,
                "record_key": diagnostic.record_key,
                "code": diagnostic.code,
                "reason": "unknown routing record stream family",
            }));
            continue;
        };
        match state
            .persistence
            .repair_mesh_routing_record(family, &diagnostic.record_key)
            .await
        {
            Ok(record) => repaired_records.push(json!({
                "stream_family": diagnostic.stream_family,
                "partition": diagnostic.partition,
                "record_key": diagnostic.record_key,
                "descriptor_key": record.descriptor_key,
                "generation": record.generation,
            })),
            Err(err) => skipped_records.push(json!({
                "stream_family": diagnostic.stream_family,
                "partition": diagnostic.partition,
                "record_key": diagnostic.record_key,
                "code": diagnostic.code,
                "reason": err.to_string(),
            })),
        }
    }

    let status = if skipped_records.is_empty() {
        "completed"
    } else if repaired_records.is_empty() {
        "failed"
    } else {
        "completed_with_warnings"
    };

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id: format!("mesh-routing-projection-repair-{audit_event_id}"),
        status: status.to_string(),
        scope_kind: "mesh_routing_projection".to_string(),
        scope_id: state.config.mesh_id.clone(),
        findings: Vec::new(),
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "mesh_routing_projection",
            "repaired_count": repaired_records.len(),
            "skipped_count": skipped_records.len(),
            "repaired_records": repaired_records,
            "skipped_records": skipped_records,
        })
        .to_string(),
    })
}

fn require_nonempty_admin_field(value: &str, field: &'static str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    Ok(())
}

fn validate_diagnostic_severity(value: &str) -> Result<(), Status> {
    match value {
        "info" | "warning" | "error" => Ok(()),
        _ => Err(Status::invalid_argument("Invalid diagnostic severity")),
    }
}

fn index_diagnostic_to_admin_record(
    diagnostic: persistence::IndexDiagnostic,
) -> Result<DiagnosticRecord, Status> {
    let cursor =
        u64::try_from(diagnostic.id).map_err(|_| Status::internal("Invalid diagnostic cursor"))?;
    Ok(DiagnosticRecord {
        diagnostic_id: format!("index-diagnostic-{cursor}"),
        scope_kind: "index".to_string(),
        scope_id: diagnostic
            .index_id
            .map(|index_id| {
                format!(
                    "tenant-{}-bucket-{}-index-{}",
                    diagnostic.tenant_id, diagnostic.bucket_id, index_id
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "tenant-{}-bucket-{}-index-{}",
                    diagnostic.tenant_id, diagnostic.bucket_id, diagnostic.index_name
                )
            }),
        source: "index_diagnostic_journal".to_string(),
        severity: diagnostic.severity,
        code: diagnostic.code,
        message: diagnostic.message,
        object_key: diagnostic.object_key,
        version_id: diagnostic
            .version_id
            .map(|version_id| version_id.to_string())
            .unwrap_or_default(),
        details_json: diagnostic.details.to_string(),
        created_at_nanos: diagnostic
            .created_at
            .timestamp_nanos_opt()
            .ok_or_else(|| Status::internal("Invalid diagnostic timestamp"))?,
        cursor,
    })
}

async fn mesh_routing_projection_diagnostics(
    state: &AppState,
) -> Result<Vec<DiagnosticRecord>, Status> {
    state
        .persistence
        .diagnose_mesh_routing_projection(None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .into_iter()
        .enumerate()
        .map(|(index, diagnostic)| {
            mesh_routing_projection_diagnostic_to_admin_record(
                u64::try_from(index + 1)
                    .map_err(|_| Status::internal("Too many mesh diagnostics"))?,
                diagnostic,
            )
        })
        .collect()
}

fn mesh_routing_projection_diagnostic_to_admin_record(
    cursor: u64,
    diagnostic: mesh_control_stream::ControlProjectionDiagnostic,
) -> Result<DiagnosticRecord, Status> {
    let scope_id = format!(
        "{}/{}/{}",
        diagnostic.stream_family, diagnostic.partition, diagnostic.record_key
    );
    Ok(DiagnosticRecord {
        diagnostic_id: format!("mesh-routing-projection-{cursor}"),
        scope_kind: "routing_record".to_string(),
        scope_id,
        source: "mesh_routing_projection".to_string(),
        severity: diagnostic.severity.to_string(),
        code: diagnostic.code.to_string(),
        message: diagnostic.message,
        object_key: String::new(),
        version_id: String::new(),
        details_json: json!({
            "stream_family": diagnostic.stream_family,
            "partition": diagnostic.partition,
            "record_key": diagnostic.record_key,
            "stream_sequence": diagnostic.stream_sequence,
            "stream_generation": diagnostic.stream_generation,
            "stream_digest": diagnostic.stream_digest,
            "projection_generation": diagnostic.projection_generation,
            "projection_digest": diagnostic.projection_digest,
            "repair_safe": diagnostic.repair_safe,
            "proposed_action": diagnostic.proposed_action,
        })
        .to_string(),
        created_at_nanos: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        cursor,
    })
}

fn diagnostic_position(diagnostic: &DiagnosticRecord) -> String {
    format!(
        "{}:{:020}:{}",
        diagnostic.source, diagnostic.cursor, diagnostic.diagnostic_id
    )
}

async fn mesh_lifecycle_diagnostics(state: &AppState) -> Result<Vec<DiagnosticRecord>, Status> {
    let mut diagnostics = Vec::new();
    let mut cursor = 1_u64;
    let mut push = |scope_kind: &str,
                    scope_id: String,
                    severity: &str,
                    code: &str,
                    message: String,
                    details: serde_json::Value| {
        diagnostics.push(DiagnosticRecord {
            diagnostic_id: format!("mesh-diagnostic-{cursor}"),
            scope_kind: scope_kind.to_string(),
            scope_id,
            source: "mesh_lifecycle".to_string(),
            severity: severity.to_string(),
            code: code.to_string(),
            message,
            object_key: String::new(),
            version_id: String::new(),
            details_json: details.to_string(),
            created_at_nanos: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            cursor,
        });
        cursor = cursor.saturating_add(1);
    };

    for region in state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(lifecycle_status)?
    {
        if region.state != CoreLifecycleState::Active {
            push(
                "region",
                region.region.clone(),
                match region.state {
                    CoreLifecycleState::Draining
                    | CoreLifecycleState::Drained
                    | CoreLifecycleState::DrainedWithExceptions
                    | CoreLifecycleState::Offline
                    | CoreLifecycleState::Removed => "warning",
                    _ => "info",
                },
                "mesh_region_not_active",
                format!(
                    "region {} is {:?}; new writable placement is disabled",
                    region.region, region.state
                ),
                json!({
                    "generation": region.generation,
                    "state": format!("{:?}", region.state),
                    "default_cell": region.default_cell,
                }),
            );
        }
    }

    for cell in state
        .persistence
        .list_cell_descriptors(None)
        .await
        .map_err(lifecycle_status)?
    {
        if cell.state != CoreLifecycleState::Active {
            push(
                "cell",
                format!("{}/{}", cell.region, cell.cell_id),
                "info",
                "mesh_cell_not_active",
                format!(
                    "cell {}/{} is {:?}; node activation and placement are disabled",
                    cell.region, cell.cell_id, cell.state
                ),
                json!({
                    "generation": cell.generation,
                    "state": format!("{:?}", cell.state),
                    "placement_weight": cell.placement_weight,
                }),
            );
        }
    }

    for node in state
        .persistence
        .list_node_descriptors(None, None)
        .await
        .map_err(lifecycle_status)?
    {
        if node.state != CoreLifecycleState::Active {
            push(
                "node",
                node.node_id.clone(),
                match node.state {
                    CoreLifecycleState::Draining
                    | CoreLifecycleState::Drained
                    | CoreLifecycleState::Offline
                    | CoreLifecycleState::Removed => "warning",
                    _ => "info",
                },
                "mesh_node_not_active",
                format!(
                    "node {} is {:?}; new ownership should not be assigned",
                    node.node_id, node.state
                ),
                json!({
                    "generation": node.generation,
                    "state": format!("{:?}", node.state),
                    "region": node.region,
                    "cell_id": node.cell_id,
                    "drain": node.drain,
                }),
            );
        }
    }

    let routing_record_count = state
        .persistence
        .list_mesh_routing_records(None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .len();
    push(
        "mesh",
        state.config.mesh_id.clone(),
        "info",
        "mesh_routing_projection_summary",
        format!("mesh routing projection has {routing_record_count} records"),
        json!({ "routing_record_count": routing_record_count }),
    );

    Ok(diagnostics)
}

fn repair_finding_to_admin_proto(finding: &RepairFinding) -> Result<RepairFindingRecord, Status> {
    Ok(RepairFindingRecord {
        finding_id: finding.finding_id.clone(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        repair_task_id: finding.repair_task_id.clone(),
        lease_fence_token: finding.lease_fence_token,
        severity: format!("{:?}", finding.severity),
        status: format!("{:?}", finding.status),
        code: finding.code.clone(),
        message: finding.message.clone(),
        subjects: finding
            .subjects
            .iter()
            .map(repair_subject_to_admin_proto)
            .collect(),
        proposed_action: format!("{:?}", finding.proposed_action),
        evidence_json: serde_json::to_string(&finding.evidence).unwrap_or_default(),
        created_at_nanos: finding.created_at_nanos,
        finding_hash: finding.finding_hash.clone().unwrap_or_default(),
    })
}

fn repair_subject_to_admin_proto(subject: &RepairSubjectRef) -> RepairSubjectRecord {
    let (cursor_low, cursor_high) = subject.cursor.map(split_u128_admin).unwrap_or((0, 0));
    RepairSubjectRecord {
        subject_kind: subject.subject_kind.clone(),
        subject_id: subject.subject_id.clone(),
        generation: subject.generation.unwrap_or_default(),
        has_generation: subject.generation.is_some(),
        cursor_low,
        cursor_high,
        has_cursor: subject.cursor.is_some(),
        expected_hash: subject.expected_hash.clone().unwrap_or_default(),
        actual_hash: subject.actual_hash.clone().unwrap_or_default(),
    }
}

fn split_u128_admin(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn audit_event_id(principal: &AdminPrincipal, context: &AdminRequestContext) -> String {
    format!("audit:{}:{}", principal.principal_id, context.request_id)
}

fn audit_event_id_with_suffix(
    principal: &AdminPrincipal,
    context: &AdminRequestContext,
    suffix: &str,
) -> String {
    format!(
        "audit:{}:{}:{}",
        principal.principal_id,
        context.request_id,
        safe_audit_id_suffix(suffix)
    )
}

async fn record_admin_audit_event(
    state: &AppState,
    principal: &AdminPrincipal,
    context: &AdminRequestContext,
    action: &str,
    resource_id: &str,
    details: serde_json::Value,
) -> Result<String, Status> {
    let audit_event_id = audit_event_id(principal, context);
    let event = AdminAuditEvent {
        schema: admin_audit::ADMIN_AUDIT_EVENT_SCHEMA.to_string(),
        audit_event_id: audit_event_id.clone(),
        request_id: context.request_id.clone(),
        principal_id: principal.principal_id.clone(),
        resource_id: resource_id.to_string(),
        action: action.to_string(),
        audit_reason: context.audit_reason.clone(),
        created_at: Utc::now().to_rfc3339(),
        details_json: admin_audit_details_json(context, details)?,
    };
    admin_audit::append_audit_event(&state.storage, &event)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    Ok(audit_event_id)
}

async fn record_admin_audit_event_with_suffix(
    state: &AppState,
    principal: &AdminPrincipal,
    context: &AdminRequestContext,
    action: &str,
    resource_id: &str,
    details: serde_json::Value,
    suffix: &str,
) -> Result<String, Status> {
    let audit_event_id = audit_event_id_with_suffix(principal, context, suffix);
    let event = AdminAuditEvent {
        schema: admin_audit::ADMIN_AUDIT_EVENT_SCHEMA.to_string(),
        audit_event_id: audit_event_id.clone(),
        request_id: context.request_id.clone(),
        principal_id: principal.principal_id.clone(),
        resource_id: resource_id.to_string(),
        action: action.to_string(),
        audit_reason: context.audit_reason.clone(),
        created_at: Utc::now().to_rfc3339(),
        details_json: admin_audit_details_json(context, details)?,
    };
    admin_audit::append_audit_event(&state.storage, &event)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    Ok(audit_event_id)
}

fn safe_audit_id_suffix(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn admin_audit_details_json(
    context: &AdminRequestContext,
    details: serde_json::Value,
) -> Result<String, Status> {
    let mut details = match details {
        serde_json::Value::Object(map) => map,
        other => {
            let mut map = serde_json::Map::new();
            map.insert("details".to_string(), other);
            map
        }
    };
    details.insert(
        "idempotency_key".to_string(),
        json!(context.idempotency_key.clone()),
    );
    details.insert(
        "expected_generation".to_string(),
        json!(context.expected_generation),
    );
    serde_json::to_string(&serde_json::Value::Object(details))
        .map_err(|_| Status::internal("Failed to encode admin audit details"))
}

fn bucket_resource_id(tenant_id: i64, bucket_name: &str) -> String {
    format!("tenant:{tenant_id}:bucket:{bucket_name}")
}

fn object_link_resource_id(tenant_id: i64, bucket_name: &str, link_key: &str) -> String {
    format!(
        "{}:link:{link_key}",
        bucket_resource_id(tenant_id, bucket_name)
    )
}

fn cell_resource_id(region: &str, cell_id: &str) -> String {
    format!("region:{region}:cell:{cell_id}")
}

fn host_alias_audit_details(host_alias: &CoreHostAliasDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "host_alias",
        "hostname": &host_alias.hostname,
        "tenant_id": &host_alias.tenant_id,
        "bucket_name": &host_alias.bucket_name,
        "region": &host_alias.region,
        "prefix": &host_alias.prefix,
        "state": host_alias.state,
        "generation": host_alias.generation,
    })
}

fn region_audit_details(region: &mesh_lifecycle::RegionDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "region",
        "mesh_id": &region.mesh_id,
        "region": &region.region,
        "state": region.state,
        "public_base_url": &region.public_base_url,
        "virtual_host_suffix": &region.virtual_host_suffix,
        "placement_weight": region.placement_weight,
        "default_cell": &region.default_cell,
        "generation": region.generation,
    })
}

fn cell_audit_details(cell: &mesh_lifecycle::CellDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "cell",
        "mesh_id": &cell.mesh_id,
        "region": &cell.region,
        "cell_id": &cell.cell_id,
        "state": cell.state,
        "placement_weight": cell.placement_weight,
        "generation": cell.generation,
    })
}

fn node_audit_details(node: &mesh_lifecycle::NodeDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "node",
        "mesh_id": &node.mesh_id,
        "node_id": &node.node_id,
        "region": &node.region,
        "cell_id": &node.cell_id,
        "libp2p_peer_id": &node.libp2p_peer_id,
        "public_api_addr": &node.public_api_addr,
        "public_cluster_addrs": &node.public_cluster_addrs,
        "capabilities": &node.capabilities,
        "state": node.state,
        "drain": &node.drain,
        "generation": node.generation,
    })
}

fn add_audit_detail(details: &mut serde_json::Value, key: &str, value: serde_json::Value) {
    if let serde_json::Value::Object(map) = details {
        map.insert(key.to_string(), value);
    }
}

fn bucket_drain_overrides_details(overrides: &[BucketDrainOverride]) -> Vec<serde_json::Value> {
    overrides
        .iter()
        .map(|override_| {
            json!({
                "tenant_id": &override_.tenant_id,
                "bucket_name": &override_.bucket_name,
                "disposition": region_drain_disposition_name(override_.disposition),
                "disposition_code": override_.disposition,
                "reason": &override_.reason,
                "expires_at": none_if_empty(&override_.expires_at),
            })
        })
        .collect()
}

fn bucket_drain_override_from_proto(
    override_: &BucketDrainOverride,
) -> Result<persistence::RegionDrainBucketOverride, Status> {
    let disposition = region_drain_disposition_from_proto(override_.disposition, false)?;
    if override_.tenant_id.trim().is_empty() {
        return Err(Status::invalid_argument(
            "bucket drain override tenant_id is required",
        ));
    }
    if override_.bucket_name.trim().is_empty() {
        return Err(Status::invalid_argument(
            "bucket drain override bucket_name is required",
        ));
    }
    if override_.reason.trim().is_empty() && disposition.allows_drained_exception() {
        return Err(Status::invalid_argument(
            "bucket drain override reason is required for drain exceptions",
        ));
    }
    Ok(persistence::RegionDrainBucketOverride {
        tenant_id: override_.tenant_id.trim().to_string(),
        bucket_name: override_.bucket_name.trim().to_string(),
        disposition,
        reason: override_.reason.trim().to_string(),
        expires_at: none_if_empty(&override_.expires_at).map(str::to_string),
    })
}

fn region_drain_disposition_from_proto(
    value: i32,
    unspecified_defaults_to_block: bool,
) -> Result<mesh_lifecycle::BucketDrainDisposition, Status> {
    match value {
        0 if unspecified_defaults_to_block => {
            Ok(mesh_lifecycle::BucketDrainDisposition::BlockUntilEmpty)
        }
        1 => Ok(mesh_lifecycle::BucketDrainDisposition::BlockUntilEmpty),
        2 => Ok(mesh_lifecycle::BucketDrainDisposition::RemainProxyOnly),
        3 => Ok(mesh_lifecycle::BucketDrainDisposition::ReadOnlyUntilRemoved),
        4 => Ok(mesh_lifecycle::BucketDrainDisposition::DeleteAfterRetention),
        _ => Err(Status::invalid_argument(format!(
            "unsupported region drain disposition code {value}"
        ))),
    }
}

fn region_drain_plan_details(report: &persistence::RegionDrainPlanReport) -> serde_json::Value {
    json!({
        "region": &report.region,
        "decisions": report.decisions.iter().map(|decision| {
            json!({
                "tenant_id": &decision.tenant_id,
                "bucket_name": &decision.bucket_name,
                "disposition": decision.disposition.as_str(),
                "reason": &decision.reason,
                "expires_at": decision.expires_at.as_deref(),
                "status_before": format!("{:?}", decision.status_before),
                "status_after": format!("{:?}", decision.status_after),
                "bucket_locator_generation_before": decision.bucket_locator_generation_before,
                "bucket_locator_generation_after": decision.bucket_locator_generation_after,
                "exception_written": decision.exception_written,
                "locator_updated": decision.locator_updated,
            })
        }).collect::<Vec<_>>()
    })
}

fn region_drain_disposition_name(value: i32) -> &'static str {
    match value {
        1 => "block_until_empty",
        2 => "remain_proxy_only",
        3 => "read_only_until_removed",
        4 => "delete_after_retention",
        _ => "unspecified",
    }
}

fn audit_cursor_position(event: &AdminAuditEvent) -> String {
    format!("{}:{}", event.created_at, event.audit_event_id)
}

fn audit_event_to_proto(event: AdminAuditEvent) -> AuditEventRecord {
    AuditEventRecord {
        audit_event_id: event.audit_event_id,
        request_id: event.request_id,
        principal_id: event.principal_id,
        resource_id: event.resource_id,
        action: event.action,
        audit_reason: event.audit_reason,
        created_at: event.created_at,
        details_json: event.details_json,
    }
}

fn principal_label(principal: &AdminPrincipal) -> String {
    format!("principal:{}", principal.principal_id)
}

fn generated_client_id() -> String {
    format!("app_{}", uuid::Uuid::new_v4().simple())
}

fn generated_client_secret() -> String {
    format!("secret_{}", uuid::Uuid::new_v4().simple())
}

fn encrypt_admin_client_secret(state: &AppState, client_secret: &str) -> Result<Vec<u8>, Status> {
    let encryption_key = hex::decode(&state.config.anvil_secret_encryption_key)
        .map_err(|_| Status::internal("Invalid encryption key format"))?;
    crypto::encrypt(client_secret.as_bytes(), &encryption_key)
        .map_err(|err| Status::internal(err.to_string()))
}

fn bucket_to_proto(bucket: Bucket) -> crate::anvil_api::Bucket {
    crate::anvil_api::Bucket {
        name: bucket.name,
        creation_date: bucket
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        region: bucket.region,
        is_public_read: bucket.is_public_read,
        deleted: false,
        bucket_id: bucket.id,
    }
}

async fn resolve_link_bucket(
    state: &AppState,
    tenant_ref: &str,
    bucket_name: &str,
) -> Result<persistence::Bucket, Status> {
    let tenant_id = resolve_tenant_id(state, tenant_ref).await?;
    state
        .persistence
        .get_bucket_by_name(tenant_id, bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))
}

async fn resolve_tenant_id(state: &AppState, tenant_ref: &str) -> Result<i64, Status> {
    let tenant_ref = tenant_ref.trim();
    if tenant_ref.is_empty() {
        return Err(Status::invalid_argument("tenant_id is required"));
    }
    if let Ok(tenant_id) = tenant_ref.parse::<i64>() {
        return Ok(tenant_id);
    }
    state
        .persistence
        .get_tenant_by_name(tenant_ref)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .map(|tenant| tenant.id)
        .ok_or_else(|| Status::not_found("Tenant not found"))
}

async fn routing_config_for_region(
    state: &AppState,
    region_name: &str,
) -> Result<RoutingConfig, Status> {
    let region_name = region_name.trim();
    if region_name.is_empty() {
        return Err(Status::invalid_argument("region is required"));
    }
    let region = state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(lifecycle_status)?
        .into_iter()
        .find(|region| region.region == region_name)
        .ok_or_else(|| Status::not_found("Region not found"))?;
    let base_domain = base_domain_from_region_suffix(&region.region, &region.virtual_host_suffix)?;
    RoutingConfig::new(base_domain).map_err(|err| Status::invalid_argument(err.to_string()))
}

fn base_domain_from_region_suffix(
    region: &str,
    virtual_host_suffix: &str,
) -> Result<String, Status> {
    let suffix = routing::normalize_alias_hostname(virtual_host_suffix)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
    let region_prefix = format!(
        "{}.",
        region.trim().trim_end_matches('.').to_ascii_lowercase()
    );
    Ok(suffix
        .strip_prefix(&region_prefix)
        .unwrap_or(&suffix)
        .to_string())
}

fn parse_optional_uuid(
    field_name: &'static str,
    value: String,
) -> Result<Option<uuid::Uuid>, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<uuid::Uuid>()
        .map(Some)
        .map_err(|_| Status::invalid_argument(format!("Invalid {field_name}")))
}

fn page_limit(page: Option<&PageRequest>) -> usize {
    let requested = page.map(|page| page.limit).unwrap_or(100);
    if requested == 0 {
        100
    } else {
        requested.clamp(1, 1000) as usize
    }
}

fn object_link_status(err: object_links::ObjectLinkError) -> Status {
    match err {
        object_links::ObjectLinkError::InvalidLinkKey
        | object_links::ObjectLinkError::InvalidTargetKey
        | object_links::ObjectLinkError::MissingExpectedGeneration => {
            Status::invalid_argument(err.to_string())
        }
        object_links::ObjectLinkError::AlreadyExists => Status::already_exists(err.to_string()),
        object_links::ObjectLinkError::BucketNotFound | object_links::ObjectLinkError::NotFound => {
            Status::not_found(err.to_string())
        }
        object_links::ObjectLinkError::BucketTenantMismatch => {
            Status::not_found("Bucket not found")
        }
        object_links::ObjectLinkError::GenerationConflict { .. } => {
            Status::aborted(err.to_string())
        }
        object_links::ObjectLinkError::ExistingObjectIsNotLink
        | object_links::ObjectLinkError::DanglingObjectLink
        | object_links::ObjectLinkError::TargetNotBlob
        | object_links::ObjectLinkError::LinkLoop
        | object_links::ObjectLinkError::LinkDepthExceeded => {
            Status::failed_precondition(err.to_string())
        }
        object_links::ObjectLinkError::Internal(_) => Status::internal(err.to_string()),
    }
}

fn lifecycle_status(err: LifecycleError) -> Status {
    match err {
        LifecycleError::InvalidArgument(message) => Status::invalid_argument(message),
        LifecycleError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        LifecycleError::NotFound { .. } => Status::not_found(err.to_string()),
        LifecycleError::GenerationConflict { .. } => Status::aborted(err.to_string()),
        LifecycleError::LifecycleTransitionDenied { .. }
        | LifecycleError::ActivationCheckpointNotReached { .. } => {
            Status::failed_precondition(err.to_string())
        }
        LifecycleError::Io(_) | LifecycleError::Json(_) => Status::internal(err.to_string()),
    }
}

fn node_capability_from_proto(value: i32) -> Result<CoreNodeCapability, Status> {
    match value {
        1 => Ok(CoreNodeCapability::Object),
        2 => Ok(CoreNodeCapability::Index),
        3 => Ok(CoreNodeCapability::PersonalDb),
        4 => Ok(CoreNodeCapability::Gateway),
        5 => Ok(CoreNodeCapability::Admin),
        _ => Err(Status::invalid_argument("Invalid node capability")),
    }
}

fn node_capability_to_proto(value: CoreNodeCapability) -> i32 {
    match value {
        CoreNodeCapability::Object => 1,
        CoreNodeCapability::Index => 2,
        CoreNodeCapability::PersonalDb => 3,
        CoreNodeCapability::Gateway => 4,
        CoreNodeCapability::Admin => 5,
    }
}

fn lifecycle_state_to_proto(value: CoreLifecycleState) -> i32 {
    match value {
        CoreLifecycleState::Joining => 1,
        CoreLifecycleState::Active => 2,
        CoreLifecycleState::ReadOnly => 3,
        CoreLifecycleState::Draining => 4,
        CoreLifecycleState::Drained => 5,
        CoreLifecycleState::DrainedWithExceptions => 6,
        CoreLifecycleState::Offline => 7,
        CoreLifecycleState::Removed => 8,
    }
}

fn routing_record_family_from_proto(
    value: i32,
) -> Result<Option<mesh_directory::RoutingRecordFamily>, Status> {
    match value {
        0 => Ok(None),
        1 => Ok(Some(mesh_directory::RoutingRecordFamily::TenantName)),
        2 => Ok(Some(mesh_directory::RoutingRecordFamily::TenantLocator)),
        3 => Ok(Some(mesh_directory::RoutingRecordFamily::BucketLocator)),
        4 => Ok(Some(mesh_directory::RoutingRecordFamily::HostAlias)),
        _ => Err(Status::invalid_argument("Invalid routing record family")),
    }
}

fn routing_record_family_to_proto(value: mesh_directory::RoutingRecordFamily) -> i32 {
    match value {
        mesh_directory::RoutingRecordFamily::TenantName => 1,
        mesh_directory::RoutingRecordFamily::TenantLocator => 2,
        mesh_directory::RoutingRecordFamily::BucketLocator => 3,
        mesh_directory::RoutingRecordFamily::HostAlias => 4,
    }
}

fn routing_record_descriptor_to_proto(
    value: mesh_directory::RoutingRecordDescriptor,
) -> RoutingRecordDescriptor {
    RoutingRecordDescriptor {
        family: routing_record_family_to_proto(value.family),
        record_key: value.record_key,
        partition: value.partition,
        descriptor_key: value.descriptor_key,
        generation: value.generation,
        payload_json: value.payload_json,
    }
}

fn host_alias_state_to_proto(value: CoreHostAliasState) -> i32 {
    match value {
        CoreHostAliasState::PendingVerification => 1,
        CoreHostAliasState::Active => 2,
        CoreHostAliasState::Suspended => 3,
        CoreHostAliasState::Deleted => 4,
    }
}

fn object_link_resolution_from_proto(
    value: i32,
) -> Result<object_links::ObjectLinkResolution, Status> {
    match value {
        1 => Ok(object_links::ObjectLinkResolution::Follow),
        2 => Ok(object_links::ObjectLinkResolution::Redirect),
        _ => Err(Status::invalid_argument("Invalid object link resolution")),
    }
}

fn object_link_resolution_to_proto(value: object_links::ObjectLinkResolution) -> i32 {
    match value {
        object_links::ObjectLinkResolution::Follow => 1,
        object_links::ObjectLinkResolution::Redirect => 2,
    }
}

fn object_link_descriptor_to_proto(
    value: object_links::ObjectLinkDescriptor,
) -> crate::anvil_api::ObjectLinkDescriptor {
    crate::anvil_api::ObjectLinkDescriptor {
        schema: value.schema,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        link_key: value.link_key,
        target_key: value.target_key,
        target_version: value.target_version.unwrap_or_default(),
        resolution: object_link_resolution_to_proto(value.resolution),
        created_at: value
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        updated_at: value
            .updated_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        created_by: value.created_by,
        generation: value.generation,
    }
}

fn host_alias_descriptor_to_proto(
    value: CoreHostAliasDescriptor,
) -> crate::anvil_api::HostAliasDescriptor {
    crate::anvil_api::HostAliasDescriptor {
        schema: value.schema,
        hostname: value.hostname,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        region: value.region,
        prefix: value.prefix,
        state: host_alias_state_to_proto(value.state),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

fn node_descriptor_to_proto(value: mesh_lifecycle::NodeDescriptor) -> NodeDescriptor {
    NodeDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        node_id: value.node_id,
        region: value.region,
        cell_id: value.cell_id,
        libp2p_peer_id: value.libp2p_peer_id,
        public_api_addr: value.public_api_addr,
        public_cluster_addrs: value.public_cluster_addrs,
        capabilities: value
            .capabilities
            .into_iter()
            .map(node_capability_to_proto)
            .collect(),
        state: lifecycle_state_to_proto(value.state),
        drain: value.drain.map(node_drain_descriptor_to_proto),
        last_heartbeat_at: value.last_heartbeat_at.unwrap_or_default(),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

fn node_drain_descriptor_to_proto(
    value: NodeDrainDescriptor,
) -> crate::anvil_api::NodeDrainDescriptor {
    crate::anvil_api::NodeDrainDescriptor {
        started_at: value.started_at,
        graceful_timeout_ms: value.graceful_timeout_ms,
        force_after_timeout: value.force_after_timeout,
    }
}

fn region_descriptor_to_proto(value: mesh_lifecycle::RegionDescriptor) -> RegionDescriptor {
    RegionDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        region: value.region,
        state: lifecycle_state_to_proto(value.state),
        public_base_url: value.public_base_url,
        virtual_host_suffix: value.virtual_host_suffix,
        placement_weight: value.placement_weight,
        default_cell: value.default_cell.unwrap_or_default(),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

fn cell_descriptor_to_proto(value: mesh_lifecycle::CellDescriptor) -> CellDescriptor {
    CellDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        region: value.region,
        cell_id: value.cell_id,
        state: lifecycle_state_to_proto(value.state),
        placement_weight: value.placement_weight,
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn none_if_empty(value: &str) -> Option<&str> {
    if value.is_empty() { None } else { Some(value) }
}
