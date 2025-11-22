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
    BucketAll,     // Matches bucket:*
    ObjectAll,     // Matches object:*
    HfKeyAll,      // Matches hf_key:*
    HfIngestionAll, // Matches hf_ingestion:*
    PolicyAll,     // Matches policy:*

    // Bucket actions
    BucketCreate,
    BucketDelete,
    BucketRead,
    BucketWrite,
    BucketList,

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

    // Internal actions
    InternalPutShard,
    InternalGetShard,
    InternalCommitShard,
    InternalDeleteShard,
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

            // Bucket actions
            AnvilAction::BucketCreate => "bucket:create",
            AnvilAction::BucketDelete => "bucket:delete",
            AnvilAction::BucketRead => "bucket:read",
            AnvilAction::BucketWrite => "bucket:write",
            AnvilAction::BucketList => "bucket:list",

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

            // Internal actions
            AnvilAction::InternalPutShard => "internal:put_shard",
            AnvilAction::InternalGetShard => "internal:get_shard",
            AnvilAction::InternalCommitShard => "internal:commit_shard",
            AnvilAction::InternalDeleteShard => "internal:delete_shard",
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

            // Bucket actions
            "bucket:create" => Ok(AnvilAction::BucketCreate),
            "bucket:delete" => Ok(AnvilAction::BucketDelete),
            "bucket:read" => Ok(AnvilAction::BucketRead),
            "bucket:write" => Ok(AnvilAction::BucketWrite),
            "bucket:list" => Ok(AnvilAction::BucketList),

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

            // Internal actions
            "internal:put_shard" => Ok(AnvilAction::InternalPutShard),
            "internal:get_shard" => Ok(AnvilAction::InternalGetShard),
            "internal:commit_shard" => Ok(AnvilAction::InternalCommitShard),
            "internal:delete_shard" => Ok(AnvilAction::InternalDeleteShard),

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
            AnvilAction::BucketCreate,
            AnvilAction::ObjectWrite,
            AnvilAction::HfKeyList,
            AnvilAction::HfIngestionCreate,
            AnvilAction::PolicyGrant,
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
