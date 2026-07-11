use super::*;

pub(super) fn persistence_owner_node_id(config: &Config) -> String {
    if !config.node_id.is_empty() {
        return config.node_id.clone();
    }
    if !config.public_api_addr.is_empty() {
        return config.public_api_addr.clone();
    }
    if !config.api_listen_addr.is_empty() {
        return config.api_listen_addr.clone();
    }
    if !config.region.is_empty() {
        return config.region.clone();
    }
    "local-anvil-node".to_string()
}

pub(super) fn nonempty_or(value: &str, default_value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        default_value.to_string()
    } else {
        value.to_string()
    }
}

pub(super) fn mesh_directory_lifecycle_error(
    err: mesh_directory::MeshDirectoryError,
) -> crate::mesh_lifecycle::LifecycleError {
    match err {
        mesh_directory::MeshDirectoryError::InvalidTenantName(message)
        | mesh_directory::MeshDirectoryError::InvalidBucketName(message)
        | mesh_directory::MeshDirectoryError::NotFound(message) => {
            crate::mesh_lifecycle::LifecycleError::InvalidArgument(message)
        }
        mesh_directory::MeshDirectoryError::InvalidIdentifier { field, value } => {
            crate::mesh_lifecycle::LifecycleError::InvalidArgument(format!(
                "invalid {field}: {value}"
            ))
        }
        mesh_directory::MeshDirectoryError::DuplicateBucketLocator {
            tenant_id,
            bucket_name,
        } => crate::mesh_lifecycle::LifecycleError::AlreadyExists {
            resource_kind: "bucket locator",
            resource_id: format!("{tenant_id}/{bucket_name}"),
        },
        mesh_directory::MeshDirectoryError::TenantNameAlreadyExists { tenant_name } => {
            crate::mesh_lifecycle::LifecycleError::AlreadyExists {
                resource_kind: "tenant name",
                resource_id: tenant_name,
            }
        }
        mesh_directory::MeshDirectoryError::GenerationConflict {
            descriptor_key,
            expected,
            actual,
        } => crate::mesh_lifecycle::LifecycleError::GenerationConflict {
            resource_kind: "mesh directory record",
            resource_id: descriptor_key,
            expected,
            current: actual,
        },
        mesh_directory::MeshDirectoryError::InvalidState {
            descriptor_key,
            state,
        } => crate::mesh_lifecycle::LifecycleError::InvalidArgument(format!(
            "invalid mesh directory state for {descriptor_key}: {state}"
        )),
        mesh_directory::MeshDirectoryError::InvalidTimestamp { field, value } => {
            crate::mesh_lifecycle::LifecycleError::InvalidArgument(format!(
                "invalid RFC3339 timestamp in {field}: {value}"
            ))
        }
        mesh_directory::MeshDirectoryError::InvalidControlWritePermit {
            stream_family,
            partition,
            reason,
        } => crate::mesh_lifecycle::LifecycleError::InvalidArgument(format!(
            "invalid mesh control write permit for {stream_family}/{partition}: {reason}"
        )),
        mesh_directory::MeshDirectoryError::ControlFenceRejected {
            stream_family,
            partition,
            code,
            reason,
        } => crate::mesh_lifecycle::LifecycleError::InvalidArgument(format!(
            "mesh control write fence rejected for {stream_family}/{partition}: {code}: {reason}"
        )),
        mesh_directory::MeshDirectoryError::ControlStreamWrite {
            stream_family,
            partition,
            message,
        } => crate::mesh_lifecycle::LifecycleError::InvalidArgument(format!(
            "mesh control stream write failed for {stream_family}/{partition}: {message}"
        )),
        mesh_directory::MeshDirectoryError::Io(err) => {
            crate::mesh_lifecycle::LifecycleError::Io(err)
        }
        mesh_directory::MeshDirectoryError::Json(err) => {
            crate::mesh_lifecycle::LifecycleError::Json(err)
        }
        mesh_directory::MeshDirectoryError::Other(err) => {
            crate::mesh_lifecycle::LifecycleError::Other(err)
        }
    }
}

pub(super) fn current_time_nanos() -> Result<i64> {
    Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))
}

pub(super) fn task_lease_id(task_id: i64) -> Result<String> {
    if task_id <= 0 {
        return Err(anyhow!("task id must be positive"));
    }
    Ok(format!("task-{task_id}"))
}

pub(super) fn task_payload_i64(task: &TaskRecord, field: &'static str) -> Result<i64> {
    task.payload
        .get(field)
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| anyhow!("task {} payload must include integer {field}", task.id))
}

pub(super) fn task_payload_u128(task: &TaskRecord, field: &'static str) -> Result<u128> {
    task.payload
        .get(field)
        .and_then(JsonValue::as_u64)
        .map(u128::from)
        .ok_or_else(|| {
            anyhow!(
                "task {} payload must include unsigned integer {field}",
                task.id
            )
        })
}
