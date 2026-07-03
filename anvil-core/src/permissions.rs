use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Defines a specific action that can be performed on a resource.
///
/// Permissions are structured as `<resource>:<action>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AnvilAction {
    All, // Matches *:*

    // Wildcard actions
    BucketAll,       // Matches bucket:*
    ObjectAll,       // Matches object:*
    HfKeyAll,        // Matches hf_key:*
    HfIngestionAll,  // Matches hf_ingestion:*
    PolicyAll,       // Matches policy:*
    AuthzAll,        // Matches authz:*
    IndexAll,        // Matches index:*
    PersonalDbAll,   // Matches personaldb:*
    GitSourceAll,    // Matches git_source:*
    RepairAll,       // Matches repair:*
    CoordinationAll, // Matches coordination:*

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
    PolicyGrant,
    PolicyRevoke,

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

    // Repair actions
    RepairRead,
    RepairRun,

    // Coordination actions
    CoordinationLeaseRead,
    CoordinationLeaseWrite,
    CoordinationLeaseAdmin,

    // Internal actions
    InternalPutShard,
    InternalGetShard,
    InternalCommitShard,
    InternalDeleteShard,
    InternalProxyObject,
}

impl fmt::Display for AnvilAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AnvilAction::All => "*",

            // Wildcard actions
            AnvilAction::BucketAll => "bucket:*",
            AnvilAction::ObjectAll => "object:*",
            AnvilAction::HfKeyAll => "hf_key:*",
            AnvilAction::HfIngestionAll => "hf_ingestion:*",
            AnvilAction::PolicyAll => "policy:*",
            AnvilAction::AuthzAll => "authz:*",
            AnvilAction::IndexAll => "index:*",
            AnvilAction::PersonalDbAll => "personaldb:*",
            AnvilAction::GitSourceAll => "git_source:*",
            AnvilAction::RepairAll => "repair:*",
            AnvilAction::CoordinationAll => "coordination:*",

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
            AnvilAction::PolicyGrant => "policy:grant",
            AnvilAction::PolicyRevoke => "policy:revoke",

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

            // Repair actions
            AnvilAction::RepairRead => "repair:read",
            AnvilAction::RepairRun => "repair:run",

            // Coordination actions
            AnvilAction::CoordinationLeaseRead => "coordination:lease_read",
            AnvilAction::CoordinationLeaseWrite => "coordination:lease_write",
            AnvilAction::CoordinationLeaseAdmin => "coordination:lease_admin",

            // Internal actions
            AnvilAction::InternalPutShard => "internal:put_shard",
            AnvilAction::InternalGetShard => "internal:get_shard",
            AnvilAction::InternalCommitShard => "internal:commit_shard",
            AnvilAction::InternalDeleteShard => "internal:delete_shard",
            AnvilAction::InternalProxyObject => "internal:proxy_object",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for AnvilAction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => Ok(AnvilAction::All),

            // Wildcard actions
            "bucket:*" => Ok(AnvilAction::BucketAll),
            "object:*" => Ok(AnvilAction::ObjectAll),
            "hf_key:*" => Ok(AnvilAction::HfKeyAll),
            "hf_ingestion:*" => Ok(AnvilAction::HfIngestionAll),
            "policy:*" => Ok(AnvilAction::PolicyAll),
            "authz:*" => Ok(AnvilAction::AuthzAll),
            "index:*" => Ok(AnvilAction::IndexAll),
            "personaldb:*" => Ok(AnvilAction::PersonalDbAll),
            "git_source:*" => Ok(AnvilAction::GitSourceAll),
            "repair:*" => Ok(AnvilAction::RepairAll),
            "coordination:*" => Ok(AnvilAction::CoordinationAll),

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
            "policy:grant" => Ok(AnvilAction::PolicyGrant),
            "policy:revoke" => Ok(AnvilAction::PolicyRevoke),

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

            // Repair actions
            "repair:read" => Ok(AnvilAction::RepairRead),
            "repair:run" => Ok(AnvilAction::RepairRun),

            // Coordination actions
            "coordination:lease_read" => Ok(AnvilAction::CoordinationLeaseRead),
            "coordination:lease_write" => Ok(AnvilAction::CoordinationLeaseWrite),
            "coordination:lease_admin" => Ok(AnvilAction::CoordinationLeaseAdmin),

            // Internal actions
            "internal:put_shard" => Ok(AnvilAction::InternalPutShard),
            "internal:get_shard" => Ok(AnvilAction::InternalGetShard),
            "internal:commit_shard" => Ok(AnvilAction::InternalCommitShard),
            "internal:delete_shard" => Ok(AnvilAction::InternalDeleteShard),
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
            AnvilAction::All,
            AnvilAction::BucketAll,
            AnvilAction::ObjectAll,
            AnvilAction::HfKeyAll,
            AnvilAction::HfIngestionAll,
            AnvilAction::PolicyAll,
            AnvilAction::AuthzAll,
            AnvilAction::IndexAll,
            AnvilAction::PersonalDbAll,
            AnvilAction::GitSourceAll,
            AnvilAction::RepairAll,
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
            AnvilAction::PersonalDbCreate,
            AnvilAction::PersonalDbRead,
            AnvilAction::PersonalDbCommit,
            AnvilAction::PersonalDbWatch,
            AnvilAction::GitSourceRead,
            AnvilAction::GitSourceWrite,
            AnvilAction::GitSourceWatch,
            AnvilAction::RepairRead,
            AnvilAction::RepairRun,
            AnvilAction::CoordinationAll,
            AnvilAction::CoordinationLeaseRead,
            AnvilAction::CoordinationLeaseWrite,
            AnvilAction::CoordinationLeaseAdmin,
            AnvilAction::InternalPutShard,
            AnvilAction::InternalDeleteShard,
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
