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

        let now_nanos = current_time_nanos().map_err(|e| Status::internal(e.to_string()))?;
        let ttl_nanos =
            capped_public_ttl_nanos(req.requested_ttl_nanos, self.config.task_lease_ttl_secs)?;
        let owner = lease_owner_from_claims(&claims, &req.owner_label);
        let lease = self
            .persistence
            .acquire_named_task_lease(task_lease::TaskLeaseAcquire {
                task_id: req.task_id,
                task_kind: req.task_kind,
                partition_family: req.partition_family,
                partition_id: req.partition_id,
                owner,
                source_cursor: join_u128(req.source_cursor_low, req.source_cursor_high),
                now_nanos,
                ttl_nanos,
            })
            .await
            .map_err(lease_error_status)?;

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

        let owner = lease_owner_from_claims(&claims, "");
        let lease = self
            .persistence
            .checkpoint_named_task_lease(
                &req.task_id,
                &owner,
                req.fence_token,
                join_u128(req.checkpoint_cursor_low, req.checkpoint_cursor_high),
            )
            .await
            .map_err(lease_error_status)?;

        Ok(Response::new(TaskLeaseResponse {
            lease: Some(task_lease_response(lease)),
        }))
    }

    async fn commit_task_lease(
        &self,
        request: Request<CommitTaskLeaseRequest>,
    ) -> Result<Response<CommitTaskLeaseResponse>, Status> {
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

        let owner = lease_owner_from_claims(&claims, "");
        let lease = self
            .persistence
            .commit_named_task_lease(
                &req.task_id,
                &owner,
                req.fence_token,
                join_u128(req.committed_cursor_low, req.committed_cursor_high),
            )
            .await
            .map_err(lease_error_status)?;

        Ok(Response::new(CommitTaskLeaseResponse {
            committed: true,
            previous_lease: Some(task_lease_response(lease)),
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
            .read_named_task_lease(claims.tenant_id, &req.task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ReadTaskLeaseResponse {
            found: lease.is_some(),
            lease: lease.map(task_lease_response),
        }))
    }

    async fn force_release_task_lease(
        &self,
        request: Request<ForceReleaseTaskLeaseRequest>,
    ) -> Result<Response<ForceReleaseTaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        if !auth::is_authorized(
            AnvilAction::CoordinationLeaseAdmin,
            &resource,
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let released = self
            .persistence
            .force_release_named_task_lease(claims.tenant_id, &req.task_id)
            .await
            .map_err(lease_error_status)?;

        Ok(Response::new(ForceReleaseTaskLeaseResponse {
            released: released.is_some(),
            previous_lease: released.map(task_lease_response),
        }))
    }
}

fn task_lease_response(lease: task_lease::TaskLease) -> TaskLease {
    let (source_cursor_low, source_cursor_high) = split_u128(lease.source_cursor);
    let (checkpoint_cursor_low, checkpoint_cursor_high) = split_u128(lease.checkpoint_cursor);
    let owner = lease.owner;
    TaskLease {
        task_id: lease.task_id,
        task_kind: lease.task_kind,
        partition_family: lease.partition_family,
        partition_id: lease.partition_id,
        owner_label: owner.display_name,
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
        owner_tenant_id: owner.tenant_id,
        owner_principal_kind: owner.principal_kind,
        owner_principal_id: owner.principal_id,
        owner_actor_instance_id: owner.actor_instance_id,
    }
}

fn lease_owner_from_claims(claims: &auth::Claims, owner_label: &str) -> task_lease::TaskLeaseOwner {
    let actor_instance_id = claims.jti.clone().unwrap_or_else(|| claims.sub.clone());
    task_lease::TaskLeaseOwner {
        tenant_id: claims.tenant_id,
        principal_kind: "app".to_string(),
        principal_id: claims.sub.clone(),
        actor_instance_id,
        display_name: if owner_label.is_empty() {
            claims.sub.clone()
        } else {
            owner_label.to_string()
        },
    }
}

fn capped_public_ttl_nanos(requested_ttl_nanos: i64, max_ttl_secs: u64) -> Result<i64, Status> {
    let max_secs = if max_ttl_secs == 0 { 300 } else { max_ttl_secs };
    let max_nanos = i64::try_from(max_secs)
        .ok()
        .and_then(|secs| secs.checked_mul(1_000_000_000))
        .ok_or_else(|| Status::internal("task lease ttl cap exceeds supported range"))?;
    if requested_ttl_nanos <= 0 {
        return Ok(max_nanos);
    }
    Ok(requested_ttl_nanos.min(max_nanos))
}

fn lease_error_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains(task_lease::LEASE_HELD) {
        Status::failed_precondition(task_lease::LEASE_HELD)
    } else if message.contains(task_lease::LEASE_EXPIRED) {
        Status::failed_precondition(task_lease::LEASE_EXPIRED)
    } else if message.contains(task_lease::STALE_FENCE) {
        Status::failed_precondition(task_lease::STALE_FENCE)
    } else if message.contains(task_lease::LEASE_OWNER_MISMATCH) {
        Status::permission_denied(task_lease::LEASE_OWNER_MISMATCH)
    } else if message.contains(task_lease::LEASE_CAS_CONFLICT) {
        Status::aborted(task_lease::LEASE_CAS_CONFLICT)
    } else {
        Status::failed_precondition(message)
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
