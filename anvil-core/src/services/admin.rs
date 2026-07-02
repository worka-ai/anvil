use crate::admin_auth::{self, AdminPrincipal, AnvilAdminCapability};
use crate::anvil_api::admin_service_server::AdminService;
use crate::anvil_api::*;
use crate::mesh_lifecycle::{
    self, CreateRegionDescriptor, LifecycleError, LifecycleState as CoreLifecycleState,
    NodeCapability as CoreNodeCapability, NodeDrainDescriptor, RegisterCellDescriptor,
    RegisterNodeDescriptor,
};
use crate::object_links;
use crate::persistence;
use crate::{AppState, auth};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl AdminService for AppState {
    async fn create_object_link(
        &self,
        request: Request<CreateObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageLinks)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let request_id = context.request_id.clone();
        let idempotency_key = context.idempotency_key.clone();
        let audit_event_id = audit_event_id(&principal, context);
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
        let audit_event_id = audit_event_id(&principal, context);
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
        let audit_event_id = audit_event_id(&principal, context);
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
        require_admin(&request, self, AnvilAdminCapability::ManageLinks)?;
        let req = request.into_inner();
        let bucket = resolve_link_bucket(self, &req.tenant_id, &req.bucket_name).await?;
        let page = req.page.as_ref();
        let cursor = page.map(|page| page.cursor.as_str()).unwrap_or_default();
        let limit = page_limit(page);
        let mut links = self
            .persistence
            .list_object_links(bucket.id, none_if_empty(&req.prefix))
            .await
            .map_err(object_link_status)?
            .into_iter()
            .filter(|link| cursor.is_empty() || link.link_key.as_str() > cursor)
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = links.len() > limit;
        if has_more {
            links.truncate(limit);
        }
        let next_cursor = if has_more {
            links
                .last()
                .map(|link| link.link_key.clone())
                .unwrap_or_default()
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
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id: audit_event_id(&principal, context),
        }))
    }

    async fn activate_region(
        &self,
        request: Request<ActivateRegionRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::Active,
            )
            .await
            .map_err(lifecycle_status)?;
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id: audit_event_id(&principal, context),
        }))
    }

    async fn drain_region(
        &self,
        request: Request<DrainRegionRequest>,
    ) -> Result<Response<DrainOperationResponse>, Status> {
        let principal = require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::Draining,
            )
            .await
            .map_err(lifecycle_status)?;
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: region.region,
            state: lifecycle_state_to_proto(region.state),
            generation: region.generation,
            audit_event_id: audit_event_id(&principal, context),
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
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: region.region,
            generation: region.generation,
            audit_event_id: audit_event_id(&principal, context),
            idempotent_replay: false,
        }))
    }

    async fn list_regions(
        &self,
        request: Request<ListRegionsRequest>,
    ) -> Result<Response<ListRegionsResponse>, Status> {
        require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let regions = self
            .persistence
            .list_region_descriptors()
            .await
            .map_err(lifecycle_status)?
            .into_iter()
            .map(region_descriptor_to_proto)
            .collect();
        Ok(Response::new(ListRegionsResponse {
            page: Some(PageResponse {
                next_cursor: String::new(),
                has_more: false,
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
        Ok(Response::new(CellResponse {
            request_id: context.request_id.clone(),
            cell: Some(cell_descriptor_to_proto(cell)),
            audit_event_id: audit_event_id(&principal, context),
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
        Ok(Response::new(CellResponse {
            request_id: context.request_id.clone(),
            cell: Some(cell_descriptor_to_proto(cell)),
            audit_event_id: audit_event_id(&principal, context),
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
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: cell.cell_id,
            state: lifecycle_state_to_proto(cell.state),
            generation: cell.generation,
            audit_event_id: audit_event_id(&principal, context),
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
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: cell.cell_id,
            generation: cell.generation,
            audit_event_id: audit_event_id(&principal, context),
            idempotent_replay: false,
        }))
    }

    async fn list_cells(
        &self,
        request: Request<ListCellsRequest>,
    ) -> Result<Response<ListCellsResponse>, Status> {
        require_admin(&request, self, AnvilAdminCapability::ManageRegions)?;
        let req = request.into_inner();
        let region_filter = none_if_empty(&req.region);
        let cells = self
            .persistence
            .list_cell_descriptors(region_filter)
            .await
            .map_err(lifecycle_status)?
            .into_iter()
            .map(cell_descriptor_to_proto)
            .collect();
        Ok(Response::new(ListCellsResponse {
            page: Some(PageResponse {
                next_cursor: String::new(),
                has_more: false,
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
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id: audit_event_id(&principal, context),
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
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id: audit_event_id(&principal, context),
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
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: node.node_id,
            state: lifecycle_state_to_proto(node.state),
            generation: node.generation,
            audit_event_id: audit_event_id(&principal, context),
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
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: node.node_id,
            generation: node.generation,
            audit_event_id: audit_event_id(&principal, context),
            idempotent_replay: false,
        }))
    }

    async fn list_nodes(
        &self,
        request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        require_admin(&request, self, AnvilAdminCapability::ManageNodes)?;
        let req = request.into_inner();
        let nodes = self
            .persistence
            .list_node_descriptors(none_if_empty(&req.region), none_if_empty(&req.cell_id))
            .await
            .map_err(lifecycle_status)?
            .into_iter()
            .map(node_descriptor_to_proto)
            .collect();
        Ok(Response::new(ListNodesResponse {
            page: Some(PageResponse {
                next_cursor: String::new(),
                has_more: false,
            }),
            nodes,
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

fn audit_event_id(principal: &AdminPrincipal, context: &AdminRequestContext) -> String {
    format!("audit:{}:{}", principal.principal_id, context.request_id)
}

fn principal_label(principal: &AdminPrincipal) -> String {
    format!("principal:{}", principal.principal_id)
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
        LifecycleError::LifecycleTransitionDenied { .. } => {
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
