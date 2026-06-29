use crate::{authz_journal, formats::hash32, persistence::AuthzTupleRecord, storage::Storage};
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const DEFAULT_DERIVED_USERSET_INDEX_ID: &str = "derived-userset-primary";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzDerivedUsersetIndex {
    pub version: u16,
    pub tenant_id: i64,
    pub derived_index_id: String,
    pub processed_revision: u64,
    pub source_record_count: u64,
    pub source_records_hash: String,
    pub generation: u64,
    pub entries: Vec<AuthzDerivedUsersetEntry>,
    pub built_at: String,
    pub index_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzDerivedUsersetEntry {
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TupleViewKey {
    namespace: String,
    object_id: String,
    relation: String,
    subject_kind: String,
    subject_id: String,
    caveat_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UsersetRef {
    namespace: String,
    object_id: String,
    relation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SubjectRef {
    kind: String,
    id: String,
    caveat_hash: String,
}

impl From<&AuthzTupleRecord> for TupleViewKey {
    fn from(record: &AuthzTupleRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn lookup_derived_userset_index_at_revision(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: u64,
) -> Result<Option<bool>> {
    let Some(index) = read_derived_userset_index(storage, tenant_id, derived_index_id).await?
    else {
        return Ok(None);
    };
    if index.processed_revision != revision {
        return Ok(None);
    }
    let needle = AuthzDerivedUsersetEntry {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: caveat_hash.to_string(),
    };
    Ok(Some(index.entries.binary_search(&needle).is_ok()))
}

pub async fn rebuild_derived_userset_index(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<AuthzDerivedUsersetIndex> {
    let index = build_expected_derived_userset_index(storage, tenant_id, derived_index_id).await?;
    write_derived_userset_index(storage, &index).await?;
    Ok(index)
}

pub async fn build_expected_derived_userset_index(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<AuthzDerivedUsersetIndex> {
    let records = authz_journal::list_authz_tuple_log(storage, tenant_id, 0, "", 0).await?;
    build_derived_userset_index_from_records(tenant_id, derived_index_id, records)
}

pub async fn read_derived_userset_index(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<Option<AuthzDerivedUsersetIndex>> {
    let path = storage.authz_derived_userset_index_path(tenant_id, derived_index_id)?;
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let index: AuthzDerivedUsersetIndex =
        serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))?;
    validate_derived_userset_index(&index, tenant_id, derived_index_id)?;
    Ok(Some(index))
}

pub async fn write_derived_userset_index(
    storage: &Storage,
    index: &AuthzDerivedUsersetIndex,
) -> Result<()> {
    validate_derived_userset_index(index, index.tenant_id, &index.derived_index_id)?;
    let path =
        storage.authz_derived_userset_index_path(index.tenant_id, &index.derived_index_id)?;
    write_json_atomically(&path, index).await
}

fn build_derived_userset_index_from_records(
    tenant_id: i64,
    derived_index_id: &str,
    mut records: Vec<AuthzTupleRecord>,
) -> Result<AuthzDerivedUsersetIndex> {
    records.sort_by_key(|record| record.revision);
    let source_record_count = records.len() as u64;
    let processed_revision = records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0)
        .max(0) as u64;
    let source_records_hash = source_records_hash(&records)?;
    let mut current = BTreeMap::new();
    for record in &records {
        current.insert(TupleViewKey::from(record), record.clone());
    }

    let mut usersets = BTreeSet::new();
    for record in current.values() {
        usersets.insert(UsersetRef {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
        });
    }

    let mut entries = BTreeSet::new();
    for userset in usersets {
        let mut visited = BTreeSet::new();
        for subject in expand_userset_subjects(&current, &userset, &mut visited)? {
            entries.insert(AuthzDerivedUsersetEntry {
                namespace: userset.namespace.clone(),
                object_id: userset.object_id.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
            });
        }
    }

    let mut index = AuthzDerivedUsersetIndex {
        version: 1,
        tenant_id,
        derived_index_id: derived_index_id.to_string(),
        processed_revision,
        source_record_count,
        source_records_hash,
        generation: processed_revision,
        entries: entries.into_iter().collect(),
        built_at: Utc::now().to_rfc3339(),
        index_hash: String::new(),
    };
    index.index_hash = hash_derived_userset_index(&index)?;
    Ok(index)
}

fn expand_userset_subjects(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    userset: &UsersetRef,
    visited: &mut BTreeSet<UsersetRef>,
) -> Result<BTreeSet<SubjectRef>> {
    if !visited.insert(userset.clone()) {
        return Ok(BTreeSet::new());
    }

    let mut subjects = BTreeSet::new();
    for record in current.values() {
        if record.namespace != userset.namespace
            || record.object_id != userset.object_id
            || record.relation != userset.relation
            || record.operation != "add"
        {
            continue;
        }
        if record.subject_kind == "userset" {
            if !record.caveat_hash.is_empty() {
                continue;
            }
            let Some(next) = parse_userset_subject(&record.subject_id)? else {
                continue;
            };
            subjects.extend(expand_userset_subjects(current, &next, visited)?);
        } else {
            subjects.insert(SubjectRef {
                kind: record.subject_kind.clone(),
                id: record.subject_id.clone(),
                caveat_hash: record.caveat_hash.clone(),
            });
        }
    }

    visited.remove(userset);
    Ok(subjects)
}

fn parse_userset_subject(value: &str) -> Result<Option<UsersetRef>> {
    let Some((namespace, rest)) = value.split_once('/') else {
        return Ok(None);
    };
    let Some((object_id, relation)) = rest.rsplit_once('#') else {
        return Ok(None);
    };
    if namespace.is_empty()
        || object_id.is_empty()
        || relation.is_empty()
        || namespace.chars().any(char::is_control)
        || object_id.chars().any(char::is_control)
        || relation.chars().any(char::is_control)
    {
        return Err(anyhow!("invalid userset subject reference"));
    }
    Ok(Some(UsersetRef {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
    }))
}

fn validate_derived_userset_index(
    index: &AuthzDerivedUsersetIndex,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<()> {
    if index.version != 1 {
        return Err(anyhow!("unsupported authorization userset index version"));
    }
    if index.tenant_id != tenant_id || index.derived_index_id != derived_index_id {
        return Err(anyhow!("authorization userset index scope mismatch"));
    }
    if index.generation < index.processed_revision {
        return Err(anyhow!("authorization userset index generation is stale"));
    }
    let mut sorted = index.entries.clone();
    sorted.sort();
    sorted.dedup();
    if sorted != index.entries {
        return Err(anyhow!(
            "authorization userset index entries must be sorted and unique"
        ));
    }
    let expected = hash_derived_userset_index(index)?;
    if expected != index.index_hash {
        return Err(anyhow!("authorization userset index hash mismatch"));
    }
    Ok(())
}

fn hash_derived_userset_index(index: &AuthzDerivedUsersetIndex) -> Result<String> {
    let mut unsigned = index.clone();
    unsigned.index_hash.clear();
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

fn source_records_hash(records: &[AuthzTupleRecord]) -> Result<String> {
    Ok(hex::encode(hash32(&serde_json::to_vec(records)?)))
}

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| {
            format!(
                "write temporary authorization userset index {}",
                tmp.display()
            )
        })?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish authorization userset index {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        authz_journal::{AuthzTupleWrite, authz_partition_id, write_authz_tuple_with_permit},
        partition_fence::{
            PartitionRecoveryAcquire, PartitionWritePermit, acquire_partition_recovery,
            publish_partition_ready,
        },
        persistence::AuthzTupleRecord,
    };
    use chrono::Utc;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"authorization userset index test partition key";

    fn tuple(
        revision: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        operation: &str,
    ) -> AuthzTupleRecord {
        AuthzTupleRecord {
            revision,
            tenant_id: 42,
            namespace: namespace.to_string(),
            object_id: object_id.to_string(),
            relation: relation.to_string(),
            subject_kind: subject_kind.to_string(),
            subject_id: subject_id.to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            written_by: "tester".to_string(),
            reason: "test".to_string(),
            mutation_id: uuid::Uuid::new_v4(),
            record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
            written_at: Utc::now(),
        }
    }

    async fn ready_authz_permit(storage: &Storage, tenant_id: i64) -> PartitionWritePermit {
        let request = PartitionRecoveryAcquire {
            partition_family: "authz_tuple".to_string(),
            partition_id: hex::encode(authz_partition_id(tenant_id)),
            owner_node_id: "test-node".to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 100,
        };
        let recovering = acquire_partition_recovery(storage, request, PARTITION_OWNER_KEY)
            .await
            .unwrap();
        publish_partition_ready(
            storage,
            &recovering.partition_family,
            &recovering.partition_id,
            "test-node",
            recovering.fence_token,
            0,
            &hex::encode([3; 32]),
            200,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap()
        .write_permit()
        .unwrap()
    }

    async fn write_tuple(
        storage: &Storage,
        permit: &PartitionWritePermit,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        operation: &str,
    ) {
        write_authz_tuple_with_permit(
            storage,
            AuthzTupleWrite {
                tenant_id: 42,
                namespace,
                object_id,
                relation,
                subject_kind,
                subject_id,
                caveat_hash: "",
                operation,
                written_by: "tester",
                reason: "test",
            },
            permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[test]
    fn derived_userset_index_expands_current_tuple_view() {
        let index = build_derived_userset_index_from_records(
            42,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            vec![
                tuple(1, "group", "engineering", "member", "user", "alice", "add"),
                tuple(
                    2,
                    "folder",
                    "platform",
                    "viewer",
                    "userset",
                    "group/engineering#member",
                    "add",
                ),
                tuple(
                    3,
                    "document",
                    "alpha",
                    "viewer",
                    "userset",
                    "folder/platform#viewer",
                    "add",
                ),
                tuple(4, "group", "engineering", "member", "user", "bob", "remove"),
            ],
        )
        .unwrap();

        assert_eq!(index.processed_revision, 4);
        assert!(index.entries.contains(&AuthzDerivedUsersetEntry {
            namespace: "document".to_string(),
            object_id: "alpha".to_string(),
            relation: "viewer".to_string(),
            subject_kind: "user".to_string(),
            subject_id: "alice".to_string(),
            caveat_hash: String::new(),
        }));
        assert!(!index.entries.iter().any(|entry| entry.subject_id == "bob"));
        validate_derived_userset_index(&index, 42, DEFAULT_DERIVED_USERSET_INDEX_ID).unwrap();
    }

    #[tokio::test]
    async fn derived_userset_index_persists_and_serves_exact_processed_revision() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_authz_permit(&storage, 42).await;
        write_tuple(
            &storage,
            &permit,
            "group",
            "engineering",
            "member",
            "user",
            "alice",
            "add",
        )
        .await;
        write_tuple(
            &storage,
            &permit,
            "document",
            "alpha",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        )
        .await;

        let index = rebuild_derived_userset_index(&storage, 42, DEFAULT_DERIVED_USERSET_INDEX_ID)
            .await
            .unwrap();
        assert_eq!(index.processed_revision, 2);

        assert_eq!(
            lookup_derived_userset_index_at_revision(
                &storage,
                42,
                DEFAULT_DERIVED_USERSET_INDEX_ID,
                "document",
                "alpha",
                "viewer",
                "user",
                "alice",
                "",
                2,
            )
            .await
            .unwrap(),
            Some(true)
        );
        assert_eq!(
            lookup_derived_userset_index_at_revision(
                &storage,
                42,
                DEFAULT_DERIVED_USERSET_INDEX_ID,
                "document",
                "alpha",
                "viewer",
                "user",
                "alice",
                "",
                1,
            )
            .await
            .unwrap(),
            None
        );
    }
}
