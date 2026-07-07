use crate::{
    authz_journal::{self, AuthzTupleFilter},
    core_store::{CompareAndSwapRef, CoreObjectRef, CoreStore, GetBlob, PutBlob},
    formats::hash32,
    persistence::AuthzTupleRecord,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const AUTHZ_DERIVED_USERSET_INDEX_REF_PREFIX: &str = "authz_derived_userset_index:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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

pub async fn list_derived_userset_objects_at_revision(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
    namespace: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: u64,
) -> Result<Option<Vec<String>>> {
    let Some(index) = read_derived_userset_index(storage, tenant_id, derived_index_id).await?
    else {
        return Ok(None);
    };
    if index.processed_revision != revision {
        return Ok(None);
    }

    let objects = index
        .entries
        .iter()
        .filter(|entry| {
            entry.namespace == namespace
                && entry.relation == relation
                && entry.subject_kind == subject_kind
                && entry.subject_id == subject_id
                && entry.caveat_hash == caveat_hash
        })
        .map(|entry| entry.object_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(Some(objects))
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

pub async fn advance_derived_userset_index_from_batch(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
    batch_records: &[AuthzTupleRecord],
) -> Result<AuthzDerivedUsersetIndex> {
    let Some(target_revision) = batch_records
        .iter()
        .map(|record| {
            if record.tenant_id != tenant_id {
                return Err(anyhow!("authorization userset batch tenant mismatch"));
            }
            u64::try_from(record.revision)
                .map_err(|_| anyhow!("authorization userset revision must be nonnegative"))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
    else {
        return read_derived_userset_index(storage, tenant_id, derived_index_id)
            .await?
            .ok_or_else(|| anyhow!("authorization userset index does not exist"));
    };

    let all_records = authz_journal::list_authz_tuple_log(storage, tenant_id, 0, "", 0).await?;
    let existing = match read_derived_userset_index(storage, tenant_id, derived_index_id).await? {
        Some(existing) if existing.processed_revision + 1 >= target_revision => existing,
        _ => {
            let rebuilt =
                build_derived_userset_index_from_records(tenant_id, derived_index_id, all_records)?;
            write_derived_userset_index(storage, &rebuilt).await?;
            return Ok(rebuilt);
        }
    };

    if existing.processed_revision >= target_revision {
        return Ok(existing);
    }

    let current_records = authz_journal::read_current_authz_tuples_at_revision(
        storage,
        tenant_id,
        AuthzTupleFilter::default(),
        i64::try_from(target_revision)
            .map_err(|_| anyhow!("authorization userset revision exceeds supported range"))?,
    )
    .await?;
    let current = current_tuple_map(current_records);
    let impacted = impacted_usersets(&current, batch_records)?;
    if impacted.is_empty() {
        let mut advanced = existing;
        advanced.processed_revision = target_revision;
        advanced.source_record_count = all_records.len() as u64;
        advanced.source_records_hash = source_records_hash(&all_records)?;
        advanced.generation = target_revision;
        advanced.built_at = Utc::now().to_rfc3339();
        advanced.index_hash = hash_derived_userset_index(&advanced)?;
        write_derived_userset_index(storage, &advanced).await?;
        return Ok(advanced);
    }

    let mut entries = existing
        .entries
        .into_iter()
        .filter(|entry| {
            !impacted.contains(&UsersetRef {
                namespace: entry.namespace.clone(),
                object_id: entry.object_id.clone(),
                relation: entry.relation.clone(),
            })
        })
        .collect::<BTreeSet<_>>();
    for userset in &impacted {
        let mut visited = BTreeSet::new();
        for subject in expand_userset_subjects(&current, userset, &mut visited)? {
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

    let mut advanced = AuthzDerivedUsersetIndex {
        version: 1,
        tenant_id,
        derived_index_id: derived_index_id.to_string(),
        processed_revision: target_revision,
        source_record_count: all_records.len() as u64,
        source_records_hash: source_records_hash(&all_records)?,
        generation: target_revision,
        entries: entries.into_iter().collect(),
        built_at: Utc::now().to_rfc3339(),
        index_hash: String::new(),
    };
    advanced.index_hash = hash_derived_userset_index(&advanced)?;
    validate_derived_userset_index(&advanced, tenant_id, derived_index_id)?;
    write_derived_userset_index(storage, &advanced).await?;
    Ok(advanced)
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
    let ref_name = derived_userset_index_ref_name(tenant_id, derived_index_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let index: AuthzDerivedUsersetIndex = serde_json::from_slice(&bytes)?;
    validate_derived_userset_index(&index, tenant_id, derived_index_id)?;
    Ok(Some(index))
}

pub async fn write_derived_userset_index(
    storage: &Storage,
    index: &AuthzDerivedUsersetIndex,
) -> Result<()> {
    validate_derived_userset_index(index, index.tenant_id, &index.derived_index_id)?;
    let ref_name = derived_userset_index_ref_name(index.tenant_id, &index.derived_index_id)?;
    let bytes = serde_json::to_vec_pretty(index)?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!(
                "authz-derived-userset-index:{}:{}:{}",
                index.tenant_id, index.derived_index_id, index.generation
            ),
        })
        .await?;
    let new_target = encode_core_object_ref_target(&object_ref)?;
    let current = store.read_ref(&ref_name).await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name,
            expected_generation: current.as_ref().map(|value| value.generation),
            expected_target: current.map(|value| value.target),
            require_absent: false,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target,
            transaction_id: None,
        })
        .await?;
    Ok(())
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
    let current = current_tuple_map(records.clone());

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

fn current_tuple_map(records: Vec<AuthzTupleRecord>) -> BTreeMap<TupleViewKey, AuthzTupleRecord> {
    let mut current = BTreeMap::new();
    for record in records {
        current.insert(TupleViewKey::from(&record), record);
    }
    current
}

fn impacted_usersets(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    batch_records: &[AuthzTupleRecord],
) -> Result<BTreeSet<UsersetRef>> {
    let mut reverse_edges = BTreeMap::<UsersetRef, BTreeSet<UsersetRef>>::new();
    for record in current.values() {
        if record.operation != "add"
            || record.subject_kind != "userset"
            || !record.caveat_hash.is_empty()
        {
            continue;
        }
        let Some(child) = parse_userset_subject(&record.subject_id)? else {
            continue;
        };
        reverse_edges.entry(child).or_default().insert(UsersetRef {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
        });
    }

    let mut impacted = BTreeSet::new();
    let mut stack = Vec::new();
    for record in batch_records {
        let userset = UsersetRef {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
        };
        if impacted.insert(userset.clone()) {
            stack.push(userset);
        }
    }

    while let Some(userset) = stack.pop() {
        let Some(parents) = reverse_edges.get(&userset) else {
            continue;
        };
        for parent in parents {
            if impacted.insert(parent.clone()) {
                stack.push(parent.clone());
            }
        }
    }
    Ok(impacted)
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

fn derived_userset_index_ref_name(tenant_id: i64, derived_index_id: &str) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "authorization userset index tenant id must be nonnegative"
        ));
    }
    if derived_index_id.is_empty()
        || derived_index_id == "."
        || derived_index_id == ".."
        || derived_index_id.contains('/')
        || derived_index_id.contains('\\')
        || derived_index_id.contains(':')
        || derived_index_id.chars().any(char::is_control)
    {
        return Err(anyhow!("derived_index_id is not a safe component"));
    }
    Ok(format!(
        "{AUTHZ_DERIVED_USERSET_INDEX_REF_PREFIX}tenant:{tenant_id}:index:{derived_index_id}"
    ))
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
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
            revision_ordinal: 0,
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
    ) -> AuthzTupleRecord {
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
        .unwrap()
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
            list_derived_userset_objects_at_revision(
                &storage,
                42,
                DEFAULT_DERIVED_USERSET_INDEX_ID,
                "document",
                "viewer",
                "user",
                "alice",
                "",
                2,
            )
            .await
            .unwrap(),
            Some(vec!["alpha".to_string()])
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

    #[tokio::test]
    async fn derived_userset_index_advances_from_watch_batch_without_full_rebuild() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_authz_permit(&storage, 42).await;

        let group_member = write_tuple(
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
        let first = advance_derived_userset_index_from_batch(
            &storage,
            42,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            std::slice::from_ref(&group_member),
        )
        .await
        .unwrap();
        assert_eq!(first.processed_revision, 1);
        assert!(first.entries.iter().any(|entry| {
            entry.namespace == "group"
                && entry.object_id == "engineering"
                && entry.relation == "member"
                && entry.subject_id == "alice"
        }));

        let document_userset = write_tuple(
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
        let second = advance_derived_userset_index_from_batch(
            &storage,
            42,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            std::slice::from_ref(&document_userset),
        )
        .await
        .unwrap();
        assert_eq!(second.processed_revision, 2);
        assert!(second.entries.iter().any(|entry| {
            entry.namespace == "document"
                && entry.object_id == "alpha"
                && entry.relation == "viewer"
                && entry.subject_id == "alice"
        }));

        let unrelated = write_tuple(
            &storage, &permit, "document", "beta", "viewer", "user", "bob", "add",
        )
        .await;
        advance_derived_userset_index_from_batch(
            &storage,
            42,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            std::slice::from_ref(&unrelated),
        )
        .await
        .unwrap();

        let remove_member = write_tuple(
            &storage,
            &permit,
            "group",
            "engineering",
            "member",
            "user",
            "alice",
            "remove",
        )
        .await;
        let advanced = advance_derived_userset_index_from_batch(
            &storage,
            42,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            std::slice::from_ref(&remove_member),
        )
        .await
        .unwrap();
        assert_eq!(advanced.processed_revision, 4);
        assert!(!advanced.entries.iter().any(|entry| {
            entry.namespace == "document"
                && entry.object_id == "alpha"
                && entry.relation == "viewer"
                && entry.subject_id == "alice"
        }));
        assert!(advanced.entries.iter().any(|entry| {
            entry.namespace == "document"
                && entry.object_id == "beta"
                && entry.relation == "viewer"
                && entry.subject_id == "bob"
        }));

        let expected =
            build_expected_derived_userset_index(&storage, 42, DEFAULT_DERIVED_USERSET_INDEX_ID)
                .await
                .unwrap();
        assert_eq!(advanced.source_records_hash, expected.source_records_hash);
        assert_eq!(advanced.entries, expected.entries);
    }
}
