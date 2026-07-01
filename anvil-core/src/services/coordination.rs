use crate::anvil_api::coordination_service_server::CoordinationService;
use crate::anvil_api::*;
use crate::{AppState, auth, permissions::AnvilAction, task_lease};
use anyhow::{Result, anyhow};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl CoordinationService for AppState {
    async fn acquire_task_lease(
        &self,
        request: Request<AcquireTaskLeaseRequest>,
    ) -> Result<Response<TaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        if !auth::is_authorized(
            AnvilAction::CoordinationLeaseWrite,
            &resource,
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let lease = self
            .persistence
            .acquire_named_task_lease(task_lease::TaskLeaseAcquire {
                task_id: req.task_id,
                task_kind: req.task_kind,
                partition_family: req.partition_family,
                partition_id: req.partition_id,
                owner_node_id: req.owner_node_id,
                source_cursor: join_u128(req.source_cursor_low, req.source_cursor_high),
                now_nanos: current_time_nanos().map_err(|e| Status::internal(e.to_string()))?,
                ttl_nanos: req.ttl_nanos,
            })
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;

        Ok(Response::new(TaskLeaseResponse {
            lease: Some(task_lease_response(lease)),
        }))
    }

    async fn checkpoint_task_lease(
        &self,
        request: Request<CheckpointTaskLeaseRequest>,
    ) -> Result<Response<TaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        if !auth::is_authorized(
            AnvilAction::CoordinationLeaseWrite,
            &resource,
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let lease = self
            .persistence
            .checkpoint_named_task_lease(
                &req.task_id,
                &req.owner_node_id,
                req.fence_token,
                join_u128(req.checkpoint_cursor_low, req.checkpoint_cursor_high),
            )
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;

        Ok(Response::new(TaskLeaseResponse {
            lease: Some(task_lease_response(lease)),
        }))
    }

    async fn read_task_lease(
        &self,
        request: Request<ReadTaskLeaseRequest>,
    ) -> Result<Response<ReadTaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        if !auth::is_authorized(
            AnvilAction::CoordinationLeaseRead,
            &resource,
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let lease = self
            .persistence
            .read_named_task_lease(&req.task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ReadTaskLeaseResponse {
            found: lease.is_some(),
            lease: lease.map(task_lease_response),
        }))
    }
}

fn task_lease_response(lease: task_lease::TaskLease) -> TaskLease {
    let (source_cursor_low, source_cursor_high) = split_u128(lease.source_cursor);
    let (checkpoint_cursor_low, checkpoint_cursor_high) = split_u128(lease.checkpoint_cursor);
    TaskLease {
        task_id: lease.task_id,
        task_kind: lease.task_kind,
        partition_family: lease.partition_family,
        partition_id: lease.partition_id,
        owner_node_id: lease.owner_node_id,
        fence_token: lease.fence_token,
        source_cursor_low,
        source_cursor_high,
        checkpoint_cursor_low,
        checkpoint_cursor_high,
        lease_epoch: lease.lease_epoch,
        acquired_at_nanos: lease.acquired_at_nanos,
        expires_at_nanos: lease.expires_at_nanos,
        updated_at_nanos: lease.updated_at_nanos,
        lease_hash: lease.lease_hash.unwrap_or_default(),
        lease_signature: lease.lease_signature.unwrap_or_default(),
    }
}

fn task_lease_resource(task_id: &str) -> String {
    format!("task_lease/{task_id}")
}

fn validate_task_lease_id(value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument("task_id must not be empty"));
    }
    if value == "." || value == ".." || value.contains('/') || value.chars().any(char::is_control) {
        return Err(Status::invalid_argument(
            "task_id must be a safe path component",
        ));
    }
    Ok(())
}

fn current_time_nanos() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}
