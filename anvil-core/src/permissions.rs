use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Defines a specific action that can be performed on a resource.
///
/// Permissions are structured as `<resource>:<action>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AnvilAction {
    // Bucket actions
    BucketCreate,
    BucketDelete,
    BucketRead,
    BucketWrite,
    BucketList,
    BucketWatch,

    // Object actions
    ObjectRead,
    ObjectWrite,
    ObjectDelete,
    ObjectList,

    // Hugging Face Key actions
    HfKeyCreate,
    HfKeyRead,
    HfKeyDelete,
    HfKeyList,

    // Hugging Face Ingestion actions
    HfIngestionCreate,
    HfIngestionRead,
    HfIngestionDelete,

    // Policy actions
    PolicyRead,
    PolicyGrant,
    PolicyRevoke,

    // Application credential actions
    AppCreate,
    AppRead,
    AppRotateSecret,
    AppDelete,

    // Relationship authorization actions
    AuthzTupleWrite,
    AuthzTupleRead,
    AuthzCheck,
    AuthzWatch,
    AuthzSchemaRead,
    AuthzSchemaWrite,

    // Index actions
    IndexCreate,
    IndexRead,
    IndexUpdate,
    IndexDelete,
    IndexWatch,

    // Append stream actions
    StreamCreate,
    StreamAppend,
    StreamRead,
    StreamSealSegment,

    // PersonalDB actions
    PersonalDbCreate,
    PersonalDbRead,
    PersonalDbCommit,
    PersonalDbWatch,
    PersonalDbInsert,
    PersonalDbUpdate,
    PersonalDbDelete,

    // Git source actions
    GitSourceRead,
    GitSourceWrite,
    GitSourceWatch,

    // Registry/package gateway actions
    RegistryBlobWrite,
    RegistryVersionWrite,
    RegistryRefWrite,
    RegistryRead,
    RegistryList,

    // Mesh lifecycle actions
    MeshManage,
    MeshRead,

    // Repair actions
    RepairRead,
    RepairRun,

    // Coordination actions
    CoordinationLeaseRead,
    CoordinationLeaseWrite,
    CoordinationLeaseAdmin,

    // Internal actions
    InternalProxyObject,
}

impl fmt::Display for AnvilAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            // Bucket actions
            AnvilAction::BucketCreate => "bucket:create",
            AnvilAction::BucketDelete => "bucket:delete",
            AnvilAction::BucketRead => "bucket:read",
            AnvilAction::BucketWrite => "bucket:write",
            AnvilAction::BucketList => "bucket:list",
            AnvilAction::BucketWatch => "bucket:watch",

            // Object actions
            AnvilAction::ObjectRead => "object:read",
            AnvilAction::ObjectWrite => "object:write",
            AnvilAction::ObjectDelete => "object:delete",
            AnvilAction::ObjectList => "object:list",

            // Hugging Face Key actions
            AnvilAction::HfKeyCreate => "hf_key:create",
            AnvilAction::HfKeyRead => "hf_key:read",
            AnvilAction::HfKeyDelete => "hf_key:delete",
            AnvilAction::HfKeyList => "hf_key:list",

            // Hugging Face Ingestion actions
            AnvilAction::HfIngestionCreate => "hf_ingestion:create",
            AnvilAction::HfIngestionRead => "hf_ingestion:read",
            AnvilAction::HfIngestionDelete => "hf_ingestion:delete",

            // Policy actions
            AnvilAction::PolicyRead => "policy:read",
            AnvilAction::PolicyGrant => "policy:grant",
            AnvilAction::PolicyRevoke => "policy:revoke",

            // Application credential actions
            AnvilAction::AppCreate => "app:create",
            AnvilAction::AppRead => "app:read",
            AnvilAction::AppRotateSecret => "app:rotate_secret",
            AnvilAction::AppDelete => "app:delete",

            // Relationship authorization actions
            AnvilAction::AuthzTupleWrite => "authz:tuple_write",
            AnvilAction::AuthzTupleRead => "authz:tuple_read",
            AnvilAction::AuthzCheck => "authz:check",
            AnvilAction::AuthzWatch => "authz:watch",
            AnvilAction::AuthzSchemaRead => "authz:schema_read",
            AnvilAction::AuthzSchemaWrite => "authz:schema_write",

            // Index actions
            AnvilAction::IndexCreate => "index:create",
            AnvilAction::IndexRead => "index:read",
            AnvilAction::IndexUpdate => "index:update",
            AnvilAction::IndexDelete => "index:delete",
            AnvilAction::IndexWatch => "index:watch",

            // Append stream actions
            AnvilAction::StreamCreate => "stream:create",
            AnvilAction::StreamAppend => "stream:append",
            AnvilAction::StreamRead => "stream:read",
            AnvilAction::StreamSealSegment => "stream:seal_segment",

            // PersonalDB actions
            AnvilAction::PersonalDbCreate => "personaldb:create",
            AnvilAction::PersonalDbRead => "personaldb:read",
            AnvilAction::PersonalDbCommit => "personaldb:commit",
            AnvilAction::PersonalDbWatch => "personaldb:watch",
            AnvilAction::PersonalDbInsert => "personaldb:insert",
            AnvilAction::PersonalDbUpdate => "personaldb:update",
            AnvilAction::PersonalDbDelete => "personaldb:delete",

            // Git source actions
            AnvilAction::GitSourceRead => "git_source:read",
            AnvilAction::GitSourceWrite => "git_source:write",
            AnvilAction::GitSourceWatch => "git_source:watch",

            // Registry/package gateway actions
            AnvilAction::RegistryBlobWrite => "registry:blob_write",
            AnvilAction::RegistryVersionWrite => "registry:version_write",
            AnvilAction::RegistryRefWrite => "registry:ref_write",
            AnvilAction::RegistryRead => "registry:read",
            AnvilAction::RegistryList => "registry:list",

            // Mesh lifecycle actions
            AnvilAction::MeshManage => "mesh:manage",
            AnvilAction::MeshRead => "mesh:read",

            // Repair actions
            AnvilAction::RepairRead => "repair:read",
            AnvilAction::RepairRun => "repair:run",

            // Coordination actions
            AnvilAction::CoordinationLeaseRead => "coordination:lease_read",
            AnvilAction::CoordinationLeaseWrite => "coordination:lease_write",
            AnvilAction::CoordinationLeaseAdmin => "coordination:lease_admin",

            // Internal actions
            AnvilAction::InternalProxyObject => "internal:proxy_object",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for AnvilAction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // Bucket actions
            "bucket:create" => Ok(AnvilAction::BucketCreate),
            "bucket:delete" => Ok(AnvilAction::BucketDelete),
            "bucket:read" => Ok(AnvilAction::BucketRead),
            "bucket:write" => Ok(AnvilAction::BucketWrite),
            "bucket:list" => Ok(AnvilAction::BucketList),
            "bucket:watch" => Ok(AnvilAction::BucketWatch),

            // Object actions
            "object:read" => Ok(AnvilAction::ObjectRead),
            "object:write" => Ok(AnvilAction::ObjectWrite),
            "object:delete" => Ok(AnvilAction::ObjectDelete),
            "object:list" => Ok(AnvilAction::ObjectList),

            // Hugging Face Key actions
            "hf_key:create" => Ok(AnvilAction::HfKeyCreate),
            "hf_key:read" => Ok(AnvilAction::HfKeyRead),
            "hf_key:delete" => Ok(AnvilAction::HfKeyDelete),
            "hf_key:list" => Ok(AnvilAction::HfKeyList),

            // Hugging Face Ingestion actions
            "hf_ingestion:create" => Ok(AnvilAction::HfIngestionCreate),
            "hf_ingestion:read" => Ok(AnvilAction::HfIngestionRead),
            "hf_ingestion:delete" => Ok(AnvilAction::HfIngestionDelete),

            // Policy actions
            "policy:read" => Ok(AnvilAction::PolicyRead),
            "policy:grant" => Ok(AnvilAction::PolicyGrant),
            "policy:revoke" => Ok(AnvilAction::PolicyRevoke),

            // Application credential actions
            "app:create" => Ok(AnvilAction::AppCreate),
            "app:read" => Ok(AnvilAction::AppRead),
            "app:rotate_secret" => Ok(AnvilAction::AppRotateSecret),
            "app:delete" => Ok(AnvilAction::AppDelete),

            // Relationship authorization actions
            "authz:tuple_write" => Ok(AnvilAction::AuthzTupleWrite),
            "authz:tuple_read" => Ok(AnvilAction::AuthzTupleRead),
            "authz:check" => Ok(AnvilAction::AuthzCheck),
            "authz:watch" => Ok(AnvilAction::AuthzWatch),
            "authz:schema_read" => Ok(AnvilAction::AuthzSchemaRead),
            "authz:schema_write" => Ok(AnvilAction::AuthzSchemaWrite),

            // Index actions
            "index:create" => Ok(AnvilAction::IndexCreate),
            "index:read" => Ok(AnvilAction::IndexRead),
            "index:update" => Ok(AnvilAction::IndexUpdate),
            "index:delete" => Ok(AnvilAction::IndexDelete),
            "index:watch" => Ok(AnvilAction::IndexWatch),

            // Append stream actions
            "stream:create" => Ok(AnvilAction::StreamCreate),
            "stream:append" => Ok(AnvilAction::StreamAppend),
            "stream:read" => Ok(AnvilAction::StreamRead),
            "stream:seal_segment" => Ok(AnvilAction::StreamSealSegment),

            // PersonalDB actions
            "personaldb:create" => Ok(AnvilAction::PersonalDbCreate),
            "personaldb:read" => Ok(AnvilAction::PersonalDbRead),
            "personaldb:commit" => Ok(AnvilAction::PersonalDbCommit),
            "personaldb:watch" => Ok(AnvilAction::PersonalDbWatch),
            "personaldb:insert" => Ok(AnvilAction::PersonalDbInsert),
            "personaldb:update" => Ok(AnvilAction::PersonalDbUpdate),
            "personaldb:delete" => Ok(AnvilAction::PersonalDbDelete),

            // Git source actions
            "git_source:read" => Ok(AnvilAction::GitSourceRead),
            "git_source:write" => Ok(AnvilAction::GitSourceWrite),
            "git_source:watch" => Ok(AnvilAction::GitSourceWatch),

            // Registry/package gateway actions
            "registry:blob_write" => Ok(AnvilAction::RegistryBlobWrite),
            "registry:version_write" => Ok(AnvilAction::RegistryVersionWrite),
            "registry:ref_write" => Ok(AnvilAction::RegistryRefWrite),
            "registry:read" => Ok(AnvilAction::RegistryRead),
            "registry:list" => Ok(AnvilAction::RegistryList),

            // Mesh lifecycle actions
            "mesh:manage" => Ok(AnvilAction::MeshManage),
            "mesh:read" => Ok(AnvilAction::MeshRead),

            // Repair actions
            "repair:read" => Ok(AnvilAction::RepairRead),
            "repair:run" => Ok(AnvilAction::RepairRun),

            // Coordination actions
            "coordination:lease_read" => Ok(AnvilAction::CoordinationLeaseRead),
            "coordination:lease_write" => Ok(AnvilAction::CoordinationLeaseWrite),
            "coordination:lease_admin" => Ok(AnvilAction::CoordinationLeaseAdmin),

            // Internal actions
            "internal:proxy_object" => Ok(AnvilAction::InternalProxyObject),

            _ => Err(anyhow::anyhow!("Unknown action: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_display_and_from_str() {
        let actions = vec![
            AnvilAction::BucketCreate,
            AnvilAction::BucketWatch,
            AnvilAction::ObjectWrite,
            AnvilAction::HfKeyList,
            AnvilAction::HfIngestionCreate,
            AnvilAction::PolicyGrant,
            AnvilAction::AuthzCheck,
            AnvilAction::AuthzSchemaRead,
            AnvilAction::AuthzSchemaWrite,
            AnvilAction::IndexCreate,
            AnvilAction::IndexWatch,
            AnvilAction::StreamCreate,
            AnvilAction::StreamAppend,
            AnvilAction::StreamRead,
            AnvilAction::StreamSealSegment,
            AnvilAction::PersonalDbCreate,
            AnvilAction::PersonalDbRead,
            AnvilAction::PersonalDbCommit,
            AnvilAction::PersonalDbWatch,
            AnvilAction::GitSourceRead,
            AnvilAction::GitSourceWrite,
            AnvilAction::GitSourceWatch,
            AnvilAction::RegistryBlobWrite,
            AnvilAction::RegistryVersionWrite,
            AnvilAction::RegistryRefWrite,
            AnvilAction::RegistryRead,
            AnvilAction::RegistryList,
            AnvilAction::MeshManage,
            AnvilAction::MeshRead,
            AnvilAction::RepairRead,
            AnvilAction::RepairRun,
            AnvilAction::CoordinationLeaseRead,
            AnvilAction::CoordinationLeaseWrite,
            AnvilAction::CoordinationLeaseAdmin,
            AnvilAction::InternalProxyObject,
        ];

        for action in actions {
            let action_str = action.to_string();
            let parsed_action: AnvilAction = action_str.parse().unwrap();
            assert_eq!(action, parsed_action);
        }

        let invalid_action = "foo:bar";
        assert!(invalid_action.parse::<AnvilAction>().is_err());
    }
}
