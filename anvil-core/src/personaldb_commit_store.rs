use crate::{
    core_store::{
        CompareAndSwapRef, CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob,
        WriteLogicalFileRequest,
    },
    formats::{Hash32, hash32},
    personaldb_control::PersonalDbCommitCertificate,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

const PERSONALDB_CHANGESET_BY_INDEX_REF_PREFIX: &str = "personaldb_changeset_payload_by_index:";
const PERSONALDB_CHANGESET_BY_HASH_REF_PREFIX: &str = "personaldb_changeset_payload_by_hash:";
const PERSONALDB_COMMIT_CERTIFICATE_REF_PREFIX: &str = "personaldb_commit_certificate:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbChangesetPayloadRefs {
    pub by_index_ref: String,
    pub by_hash_ref: String,
}

pub async fn write_personaldb_changeset_payload(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    expected_payload_hash: Hash32,
    changeset_bytes: &[u8],
) -> Result<PersonalDbChangesetPayloadRefs> {
    let actual_hash = hash32(changeset_bytes);
    if actual_hash != expected_payload_hash {
        return Err(anyhow!("personaldb changeset payload hash mismatch"));
    }

    let payload_hash_hex = hex::encode(expected_payload_hash);
    let by_hash_ref =
        personaldb_changeset_payload_by_hash_ref_name(tenant_id, database_id, &payload_hash_hex)?;
    let by_index_ref = personaldb_changeset_payload_by_index_ref_name(
        tenant_id,
        database_id,
        log_index,
        &payload_hash_hex,
    )?;

    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "personaldb".to_string(),
            generation: log_index,
            logical_file_id: by_hash_ref.clone(),
            source: changeset_bytes.to_vec(),
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "personaldb-changeset-payload:{tenant_id}:{database_id}:{log_index}:{payload_hash_hex}"
            ),
            region_id: "local".to_string(),
        })
        .await?;
    put_immutable_ref_target(&store, &by_hash_ref, changeset_bytes, &object_ref).await?;
    put_immutable_ref_target(&store, &by_index_ref, changeset_bytes, &object_ref).await?;

    Ok(PersonalDbChangesetPayloadRefs {
        by_index_ref,
        by_hash_ref,
    })
}

pub async fn read_personaldb_changeset_payload_by_hash(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let ref_name = personaldb_changeset_payload_by_hash_ref_name(
        tenant_id,
        database_id,
        &hex::encode(payload_hash),
    )?;
    read_personaldb_changeset_payload_ref(storage, &ref_name, payload_hash).await
}

pub async fn read_personaldb_changeset_payload_by_index(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let ref_name = personaldb_changeset_payload_by_index_ref_name(
        tenant_id,
        database_id,
        log_index,
        &hex::encode(payload_hash),
    )?;
    read_personaldb_changeset_payload_ref(storage, &ref_name, payload_hash).await
}

pub async fn read_personaldb_changeset_payload_ref(
    storage: &Storage,
    ref_name: &str,
    expected_payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    if hash32(&bytes) != expected_payload_hash {
        return Err(anyhow!("personaldb changeset payload hash mismatch"));
    }
    Ok(Some(bytes))
}

pub async fn write_personaldb_commit_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    certificate: &PersonalDbCommitCertificate,
    signing_key: &[u8],
) -> Result<String> {
    certificate.verify(signing_key)?;
    ensure_scope(
        tenant_id,
        database_id,
        &certificate.tenant_id,
        &certificate.database_id,
    )?;
    let ref_name = personaldb_commit_certificate_ref_name(
        tenant_id,
        database_id,
        certificate.log_index,
        &certificate.entry_hash,
    )?;
    let bytes = serde_json::to_vec_pretty(certificate)?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "personaldb".to_string(),
            generation: certificate.log_index,
            logical_file_id: ref_name.clone(),
            source: bytes.clone(),
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "personaldb-commit-certificate:{tenant_id}:{database_id}:{}:{}",
                certificate.log_index, certificate.entry_hash
            ),
            region_id: "local".to_string(),
        })
        .await?;
    put_immutable_ref_target(&store, &ref_name, &bytes, &object_ref).await?;
    Ok(ref_name)
}

pub async fn read_personaldb_commit_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    entry_hash: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbCommitCertificate>> {
    let ref_name =
        personaldb_commit_certificate_ref_name(tenant_id, database_id, log_index, entry_hash)?;
    read_personaldb_commit_certificate_ref(storage, &ref_name, signing_key).await
}

pub async fn read_personaldb_commit_certificate_ref(
    storage: &Storage,
    ref_name: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbCommitCertificate>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let certificate: PersonalDbCommitCertificate = serde_json::from_slice(&bytes)?;
    certificate.verify(signing_key)?;
    Ok(Some(certificate))
}

pub fn personaldb_changeset_payload_by_index_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    payload_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(payload_hash, "personaldb changeset payload hash")?;
    Ok(format!(
        "{PERSONALDB_CHANGESET_BY_INDEX_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:hash:{payload_hash}"
    ))
}

pub fn personaldb_changeset_payload_by_hash_ref_name(
    tenant_id: i64,
    database_id: &str,
    payload_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(payload_hash, "personaldb changeset payload hash")?;
    Ok(format!(
        "{PERSONALDB_CHANGESET_BY_HASH_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:hash:{payload_hash}"
    ))
}

pub fn personaldb_commit_certificate_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    entry_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(entry_hash, "personaldb commit entry hash")?;
    Ok(format!(
        "{PERSONALDB_COMMIT_CERTIFICATE_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:entry:{entry_hash}"
    ))
}

fn ensure_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    actual_tenant_id: &str,
    actual_database_id: &str,
) -> Result<()> {
    if actual_tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb commit tenant scope mismatch"));
    }
    if actual_database_id != expected_database_id {
        return Err(anyhow!("personaldb commit database scope mismatch"));
    }
    Ok(())
}

fn validate_scope_component(tenant_id: i64, database_id: &str) -> Result<()> {
    if tenant_id < 0 {
        return Err(anyhow!("personaldb tenant id must be nonnegative"));
    }
    if database_id.is_empty()
        || database_id == "."
        || database_id == ".."
        || database_id.contains('/')
        || database_id.contains('\\')
        || database_id.contains(':')
        || database_id.chars().any(char::is_control)
    {
        return Err(anyhow!("database_id is not a safe component"));
    }
    Ok(())
}

fn decode_hex32(value: &str, field: &'static str) -> Result<Hash32> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("{field} must be hex32"))?)
}

async fn put_immutable_ref_target(
    store: &CoreStore,
    ref_name: &str,
    bytes: &[u8],
    object_ref: &CoreObjectRef,
) -> Result<()> {
    let target = encode_core_object_ref_target(object_ref)?;
    match store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.to_string(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: target,
            transaction_id: None,
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            let Some(existing) = store.read_ref(ref_name).await? else {
                return Err(err);
            };
            let existing_bytes = store
                .get_blob(GetBlob {
                    object_ref: decode_core_object_ref_target(&existing.target)?,
                })
                .await?;
            if existing_bytes == bytes {
                Ok(())
            } else {
                Err(anyhow!("personaldb immutable commit ref collision"))
            }
        }
    }
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
    use crate::core_store::PutBlob;
    use tempfile::tempdir;

    const KEY: &[u8] = b"personaldb commit signing key";

    #[tokio::test]
    async fn changeset_payload_is_written_by_hash_and_index() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload = b"sqlite changeset bytes";
        let payload_hash = hash32(payload);

        let refs =
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 42, payload_hash, payload)
                .await
                .unwrap();

        assert!(
            refs.by_index_ref
                .starts_with("personaldb_changeset_payload_by_index:tenant:9:database:db-alpha:")
        );
        assert!(
            refs.by_hash_ref
                .starts_with("personaldb_changeset_payload_by_hash:tenant:9:database:db-alpha:")
        );

        let by_hash =
            read_personaldb_changeset_payload_by_hash(&storage, 9, "db-alpha", payload_hash)
                .await
                .unwrap()
                .unwrap();
        let by_index =
            read_personaldb_changeset_payload_by_index(&storage, 9, "db-alpha", 42, payload_hash)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(by_hash, payload);
        assert_eq!(by_index, payload);
    }

    #[tokio::test]
    async fn changeset_payload_rejects_hash_mismatch_and_ref_collision() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload_hash = hash32(b"good");

        assert!(
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 1, payload_hash, b"bad")
                .await
                .is_err()
        );

        let refs =
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 1, payload_hash, b"good")
                .await
                .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let corrupt = store
            .put_blob(PutBlob {
                logical_name: "corrupt-changeset".to_string(),
                bytes: b"corrupt".to_vec(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "corrupt-changeset".to_string(),
            })
            .await
            .unwrap();
        let current = store
            .read_ref(&refs.by_hash_ref)
            .await
            .unwrap()
            .expect("changeset ref exists");
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: refs.by_hash_ref,
                expected_generation: Some(current.generation),
                expected_target: Some(current.target),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&corrupt).unwrap(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert!(
            read_personaldb_changeset_payload_by_hash(&storage, 9, "db-alpha", payload_hash)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn commit_certificate_round_trips_through_corestore_ref() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let certificate = sample_certificate().seal(KEY).unwrap();

        let ref_name =
            write_personaldb_commit_certificate(&storage, 9, "db-alpha", &certificate, KEY)
                .await
                .unwrap();

        assert!(ref_name.starts_with(
            "personaldb_commit_certificate:tenant:9:database:db-alpha:log:00000000000000000042:"
        ));

        let read = read_personaldb_commit_certificate(
            &storage,
            9,
            "db-alpha",
            42,
            &certificate.entry_hash,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read, certificate);
    }

    #[tokio::test]
    async fn commit_certificate_tamper_and_scope_mismatch_are_rejected() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let certificate = sample_certificate().seal(KEY).unwrap();
        let ref_name =
            write_personaldb_commit_certificate(&storage, 9, "db-alpha", &certificate, KEY)
                .await
                .unwrap();

        let store = CoreStore::new(storage.clone()).await.unwrap();
        let mut value = serde_json::to_value(&certificate).unwrap();
        value["authz_revision"] = serde_json::json!(99);
        let corrupt = store
            .put_blob(PutBlob {
                logical_name: "corrupt-certificate".to_string(),
                bytes: serde_json::to_vec_pretty(&value).unwrap(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "corrupt-certificate".to_string(),
            })
            .await
            .unwrap();
        let current = store
            .read_ref(&ref_name)
            .await
            .unwrap()
            .expect("certificate ref exists");
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name,
                expected_generation: Some(current.generation),
                expected_target: Some(current.target),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&corrupt).unwrap(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert!(
            read_personaldb_commit_certificate(
                &storage,
                9,
                "db-alpha",
                42,
                &certificate.entry_hash,
                KEY,
            )
            .await
            .is_err()
        );

        let wrong_scope = PersonalDbCommitCertificate {
            database_id: "db-beta".to_string(),
            ..sample_certificate()
        }
        .seal(KEY)
        .unwrap();
        assert!(
            write_personaldb_commit_certificate(&storage, 9, "db-alpha", &wrong_scope, KEY)
                .await
                .is_err()
        );
    }

    fn sample_certificate() -> PersonalDbCommitCertificate {
        PersonalDbCommitCertificate {
            format_version: 1,
            tenant_id: "9".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 42,
            previous_log_hash: hex::encode([0; 32]),
            entry_hash: hex::encode([1; 32]),
            changeset_payload_hash: hex::encode(hash32(b"sqlite changeset bytes")),
            verified_envelope_hash: hex::encode([3; 32]),
            client_log_epoch: 1,
            membership_epoch: 2,
            policy_epoch: 3,
            leader_replica_id: "leader-a".to_string(),
            voter_acks_hash: hex::encode([4; 32]),
            authz_revision: 5,
            witness_node_id: "node-a".to_string(),
            witnessed_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        }
    }
}
