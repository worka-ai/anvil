#[cfg(test)]
use crate::core_store::CompareAndSwapRef;
use crate::{
    core_store::{
        CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreObjectRef,
        CoreStore, GetBlob, PutBlob,
    },
    formats::hash32,
    personaldb_control::PersonalDbGroupManifest,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
const PERSONALDB_HEAD_REF_PREFIX: &str = "personaldb_head:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbCommittedHead {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub log_index: u64,
    pub log_hash: String,
    pub segment_path: String,
    pub row_index_generation: u64,
    pub policy_epoch: u64,
    pub membership_epoch: u64,
    pub schema_hash: String,
    pub updated_at: String,
    pub updated_by_node: String,
    pub head_hash: Option<String>,
    pub head_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbSnapshotsHead {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub latest_snapshot_log_index: u64,
    pub latest_snapshot_log_hash: String,
    pub latest_snapshot_manifest_path: String,
    pub retained_snapshot_count: u32,
    pub updated_at: String,
    pub updated_by_node: String,
    pub head_hash: Option<String>,
    pub head_signature: Option<String>,
}

impl PersonalDbCommittedHead {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_committed_head_unsigned(&self)?;
        let hash = hash_committed_head(&self)?;
        let signature = sign_head_hash(
            signing_key,
            "personaldb_committed_head",
            &hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        self.head_hash = Some(hash);
        self.head_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_committed_head_unsigned(self)?;
        let expected_hash = hash_committed_head(self)?;
        if self.head_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb committed head hash mismatch"));
        }
        let expected_signature = sign_head_hash(
            signing_key,
            "personaldb_committed_head",
            &expected_hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        if self.head_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("personaldb committed head signature mismatch"));
        }
        Ok(())
    }
}

impl PersonalDbSnapshotsHead {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_snapshots_head_unsigned(&self)?;
        let hash = hash_snapshots_head(&self)?;
        let signature = sign_head_hash(
            signing_key,
            "personaldb_snapshots_head",
            &hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.latest_snapshot_log_index.to_string(),
            ],
        )?;
        self.head_hash = Some(hash);
        self.head_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_snapshots_head_unsigned(self)?;
        let expected_hash = hash_snapshots_head(self)?;
        if self.head_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb snapshots head hash mismatch"));
        }
        let expected_signature = sign_head_hash(
            signing_key,
            "personaldb_snapshots_head",
            &expected_hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.latest_snapshot_log_index.to_string(),
            ],
        )?;
        if self.head_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("personaldb snapshots head signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_committed_head(head: &PersonalDbCommittedHead) -> Result<String> {
    let mut unsigned = head.clone();
    unsigned.head_hash = None;
    unsigned.head_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub fn hash_snapshots_head(head: &PersonalDbSnapshotsHead) -> Result<String> {
    let mut unsigned = head.clone();
    unsigned.head_hash = None;
    unsigned.head_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn write_personaldb_group_manifest(
    storage: &Storage,
    tenant_id: i64,
    manifest: &PersonalDbGroupManifest,
    signing_key: &[u8],
) -> Result<()> {
    manifest.verify(signing_key)?;
    ensure_head_scope(
        tenant_id,
        &manifest.database_id,
        &manifest.tenant_id,
        &manifest.database_id,
    )?;
    write_json_ref(
        storage,
        &personaldb_ref_name(tenant_id, &manifest.database_id, "group_manifest")?,
        manifest,
    )
    .await
}

pub async fn read_personaldb_group_manifest(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbGroupManifest>> {
    let Some(manifest) = read_json_ref::<PersonalDbGroupManifest>(
        storage,
        &personaldb_ref_name(tenant_id, database_id, "group_manifest")?,
    )
    .await?
    else {
        return Ok(None);
    };
    manifest.verify(signing_key)?;
    ensure_head_scope(
        tenant_id,
        database_id,
        &manifest.tenant_id,
        &manifest.database_id,
    )?;
    Ok(Some(manifest))
}

pub async fn write_personaldb_committed_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
    signing_key: &[u8],
) -> Result<()> {
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    write_json_ref(
        storage,
        &personaldb_ref_name(tenant_id, database_id, "committed_head")?,
        head,
    )
    .await
}

pub async fn write_personaldb_committed_head_with_preconditions(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
    signing_key: &[u8],
    preconditions: Vec<CoreMutationPrecondition>,
) -> Result<()> {
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    write_json_ref_with_preconditions(
        storage,
        &personaldb_ref_name(tenant_id, database_id, "committed_head")?,
        head,
        preconditions,
    )
    .await
}

pub async fn read_personaldb_committed_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbCommittedHead>> {
    let Some(head) = read_json_ref::<PersonalDbCommittedHead>(
        storage,
        &personaldb_ref_name(tenant_id, database_id, "committed_head")?,
    )
    .await?
    else {
        return Ok(None);
    };
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    Ok(Some(head))
}

pub async fn write_personaldb_snapshots_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbSnapshotsHead,
    signing_key: &[u8],
) -> Result<()> {
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    write_json_ref(
        storage,
        &personaldb_ref_name(tenant_id, database_id, "snapshots_head")?,
        head,
    )
    .await
}

pub async fn read_personaldb_snapshots_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbSnapshotsHead>> {
    let Some(head) = read_json_ref::<PersonalDbSnapshotsHead>(
        storage,
        &personaldb_ref_name(tenant_id, database_id, "snapshots_head")?,
    )
    .await?
    else {
        return Ok(None);
    };
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    Ok(Some(head))
}

fn validate_committed_head_unsigned(head: &PersonalDbCommittedHead) -> Result<()> {
    if head.format_version != 1 {
        return Err(anyhow!("unsupported personaldb committed head version"));
    }
    validate_hex32(&head.log_hash, "log_hash")?;
    validate_hex32(&head.schema_hash, "schema_hash")?;
    require_nonempty(&head.tenant_id, "tenant_id")?;
    require_nonempty(&head.database_id, "database_id")?;
    if head.log_index == 0 {
        if !head.segment_path.is_empty() {
            return Err(anyhow!(
                "personaldb genesis committed head segment path must be empty"
            ));
        }
    } else {
        require_nonempty(&head.segment_path, "segment_path")?;
    }
    require_nonempty(&head.updated_at, "updated_at")?;
    require_nonempty(&head.updated_by_node, "updated_by_node")?;
    Ok(())
}

fn validate_snapshots_head_unsigned(head: &PersonalDbSnapshotsHead) -> Result<()> {
    if head.format_version != 1 {
        return Err(anyhow!("unsupported personaldb snapshots head version"));
    }
    validate_hex32(&head.latest_snapshot_log_hash, "latest_snapshot_log_hash")?;
    require_nonempty(&head.tenant_id, "tenant_id")?;
    require_nonempty(&head.database_id, "database_id")?;
    require_nonempty(
        &head.latest_snapshot_manifest_path,
        "latest_snapshot_manifest_path",
    )?;
    require_nonempty(&head.updated_at, "updated_at")?;
    require_nonempty(&head.updated_by_node, "updated_by_node")?;
    Ok(())
}

fn ensure_head_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    actual_tenant_id: &str,
    actual_database_id: &str,
) -> Result<()> {
    if actual_tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb head tenant scope mismatch"));
    }
    if actual_database_id != expected_database_id {
        return Err(anyhow!("personaldb head database scope mismatch"));
    }
    Ok(())
}

fn sign_head_hash(
    signing_key: &[u8],
    domain: &str,
    hash: &str,
    scope_parts: &[&str],
) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("personaldb head signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(domain.as_bytes());
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be 32 bytes encoded as hex"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

async fn write_json_ref<T: Serialize>(storage: &Storage, ref_name: &str, value: &T) -> Result<()> {
    write_json_ref_with_preconditions(storage, ref_name, value, Vec::new()).await
}

async fn write_json_ref_with_preconditions<T: Serialize>(
    storage: &Storage,
    ref_name: &str,
    value: &T,
    mut preconditions: Vec<CoreMutationPrecondition>,
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let current = store.read_ref(ref_name).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.to_string(),
            bytes: serde_json::to_vec_pretty(value)?,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!("personaldb-head:{}", uuid::Uuid::new_v4().simple()),
        })
        .await?;
    preconditions.push(CoreMutationPrecondition::Ref {
        ref_name: ref_name.to_string(),
        expected_generation: current.as_ref().map(|value| value.generation),
        expected_target: current.as_ref().map(|value| value.target.clone()),
        require_absent: current.is_none(),
        require_present: current.is_some(),
        fence: None,
        authz_revision: None,
        source_watch_cursor: None,
    });
    let partition_id = json_ref_partition_id(ref_name);
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "personaldb-json-ref:{}:{}",
                hex::encode(hash32(ref_name.as_bytes())),
                uuid::Uuid::new_v4().simple()
            ),
            scope_partition: partition_id.clone(),
            committed_by_principal: "personaldb-head-writer".to_string(),
            preconditions,
            operations: vec![CoreMutationOperation::RefUpdate {
                partition_id,
                ref_name: ref_name.to_string(),
                new_target: encode_core_object_ref_target(&object_ref)?,
            }],
        })
        .await?;
    Ok(())
}

fn json_ref_partition_id(ref_name: &str) -> String {
    format!("json-ref:{}", hex::encode(hash32(ref_name.as_bytes())))
}

async fn read_json_ref<T: for<'de> Deserialize<'de>>(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<T>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn personaldb_ref_name(tenant_id: i64, database_id: &str, kind: &str) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!("personaldb tenant id must be nonnegative"));
    }
    require_safe_component(database_id, "database_id")?;
    require_safe_component(kind, "personaldb ref kind")?;
    Ok(format!(
        "{PERSONALDB_HEAD_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:{kind}"
    ))
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
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
    use tempfile::tempdir;

    const KEY: &[u8] = b"personaldb head signing key";

    #[tokio::test]
    async fn committed_head_round_trips_via_core_store_ref() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let head = sample_committed_head().seal(KEY).unwrap();

        write_personaldb_committed_head(&storage, 7, "db-alpha", &head, KEY)
            .await
            .unwrap();

        let store = CoreStore::new(storage.clone()).await.unwrap();
        assert!(
            store
                .read_ref(&personaldb_ref_name(7, "db-alpha", "committed_head").unwrap())
                .await
                .unwrap()
                .is_some()
        );

        let read = read_personaldb_committed_head(&storage, 7, "db-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, head);
        read.verify(KEY).unwrap();
    }

    #[tokio::test]
    async fn snapshots_head_round_trips_via_core_store_ref() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let head = sample_snapshots_head().seal(KEY).unwrap();

        write_personaldb_snapshots_head(&storage, 7, "db-alpha", &head, KEY)
            .await
            .unwrap();

        let store = CoreStore::new(storage.clone()).await.unwrap();
        assert!(
            store
                .read_ref(&personaldb_ref_name(7, "db-alpha", "snapshots_head").unwrap())
                .await
                .unwrap()
                .is_some()
        );

        let read = read_personaldb_snapshots_head(&storage, 7, "db-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, head);
        read.verify(KEY).unwrap();
    }

    #[tokio::test]
    async fn group_manifest_round_trips_via_core_store_ref() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let manifest = sample_group_manifest().seal(KEY).unwrap();

        write_personaldb_group_manifest(&storage, 7, &manifest, KEY)
            .await
            .unwrap();

        let store = CoreStore::new(storage.clone()).await.unwrap();
        assert!(
            store
                .read_ref(&personaldb_ref_name(7, "db-alpha", "group_manifest").unwrap())
                .await
                .unwrap()
                .is_some()
        );

        let read = read_personaldb_group_manifest(&storage, 7, "db-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, manifest);
    }

    #[tokio::test]
    async fn missing_heads_return_none() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        assert!(
            read_personaldb_committed_head(&storage, 7, "db-alpha", KEY)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_personaldb_snapshots_head(&storage, 7, "db-alpha", KEY)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tampered_committed_head_is_rejected() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let head = sample_committed_head().seal(KEY).unwrap();
        write_personaldb_committed_head(&storage, 7, "db-alpha", &head, KEY)
            .await
            .unwrap();

        let store = CoreStore::new(storage.clone()).await.unwrap();
        let ref_value = store
            .read_ref(&personaldb_ref_name(7, "db-alpha", "committed_head").unwrap())
            .await
            .unwrap()
            .unwrap();
        let object_ref = decode_core_object_ref_target(&ref_value.target).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&store.get_blob(GetBlob { object_ref }).await.unwrap()).unwrap();
        value["log_index"] = serde_json::json!(head.log_index + 1);
        let tampered = store
            .put_blob(PutBlob {
                logical_name: "personaldb-head-tamper".to_string(),
                bytes: serde_json::to_vec_pretty(&value).unwrap(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "personaldb-head-tamper".to_string(),
            })
            .await
            .unwrap();
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: personaldb_ref_name(7, "db-alpha", "committed_head").unwrap(),
                expected_generation: Some(ref_value.generation),
                expected_target: Some(ref_value.target),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&tampered).unwrap(),
                transaction_id: None,
            })
            .await
            .unwrap();

        assert!(
            read_personaldb_committed_head(&storage, 7, "db-alpha", KEY)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_hashes_and_scope_are_rejected() {
        let invalid_hash = PersonalDbCommittedHead {
            log_hash: "not-hex".to_string(),
            ..sample_committed_head()
        };
        assert!(invalid_hash.seal(KEY).is_err());

        let wrong_scope = PersonalDbCommittedHead {
            database_id: "db-beta".to_string(),
            ..sample_committed_head()
        }
        .seal(KEY)
        .unwrap();
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            write_personaldb_committed_head(&storage, 7, "db-alpha", &wrong_scope, KEY)
                .await
                .is_err()
        );
        assert!(personaldb_ref_name(7, "../escape", "committed_head").is_err());
    }

    fn sample_committed_head() -> PersonalDbCommittedHead {
        PersonalDbCommittedHead {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 42,
            log_hash: hex::encode([1; 32]),
            segment_path: "_anvil/personaldb/tenants/tenant-7/groups/db-alpha/log/segments/00000000000000000001-00000000000000000042-segment.pdbseg".to_string(),
            row_index_generation: 3,
            policy_epoch: 5,
            membership_epoch: 8,
            schema_hash: hex::encode([2; 32]),
            updated_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            updated_by_node: "node-a".to_string(),
            head_hash: None,
            head_signature: None,
        }
    }

    fn sample_snapshots_head() -> PersonalDbSnapshotsHead {
        PersonalDbSnapshotsHead {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            latest_snapshot_log_index: 1024,
            latest_snapshot_log_hash: hex::encode([3; 32]),
            latest_snapshot_manifest_path: "_anvil/personaldb/tenants/tenant-7/groups/db-alpha/snapshots/manifests/00000000000000001024-state.json".to_string(),
            retained_snapshot_count: 2,
            updated_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            updated_by_node: "node-a".to_string(),
            head_hash: None,
            head_signature: None,
        }
    }

    fn sample_group_manifest() -> PersonalDbGroupManifest {
        PersonalDbGroupManifest {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            schema_hash: hex::encode([1; 32]),
            genesis_hash: hex::encode([2; 32]),
            created_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            created_by: "principal-a".to_string(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 8,
            active_policy_epoch: 5,
            current_row_index_generation: 3,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        }
    }
}
