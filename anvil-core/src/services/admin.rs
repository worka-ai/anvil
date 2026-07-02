use crate::admin_auth::{self, AdminPrincipal, AnvilAdminCapability};
use crate::anvil_api::admin_service_server::AdminService;
use crate::anvil_api::*;
use crate::mesh_lifecycle::{
    self, CreateRegionDescriptor, LifecycleError, LifecycleState as CoreLifecycleState,
    NodeCapability as CoreNodeCapability, NodeDrainDescriptor, RegisterCellDescriptor,
    RegisterNodeDescriptor,
};
use crate::{AppState, auth};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl AdminService for AppState {
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
