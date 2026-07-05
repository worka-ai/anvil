use crate::{
    formats::personaldb::PersonalDbLogRecord,
    personaldb_commit_store::{
        read_personaldb_changeset_payload_by_index, read_personaldb_changeset_payload_ref,
        read_personaldb_commit_certificate, read_personaldb_commit_certificate_ref,
    },
    personaldb_control::PersonalDbCommitCertificate,
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead, read_personaldb_committed_head,
        read_personaldb_group_manifest, read_personaldb_snapshots_head,
    },
    personaldb_segment::{list_personaldb_log_segment_refs, read_personaldb_log_segment},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCatchUpRequest {
    pub tenant_id: i64,
    pub database_id: String,
    pub principal: String,
    pub replica_id: String,
    pub have_log_index: u64,
    pub have_log_hash: String,
    pub max_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbCatchUpResponse {
    Entries(PersonalDbCatchUpEntries),
    SnapshotRequired(PersonalDbSnapshotRestore),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCatchUpEntries {
    pub committed_head: PersonalDbCommittedHead,
    pub entries: Vec<PersonalDbCatchUpEntry>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCatchUpEntry {
    pub record: PersonalDbLogRecord,
    pub changeset_bytes: Vec<u8>,
    pub certificate: PersonalDbCommitCertificate,
    pub certificate_json: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotRestore {
    pub committed_head: Option<PersonalDbCommittedHead>,
    pub snapshots_head: Option<PersonalDbSnapshotsHead>,
    pub reason: PersonalDbSnapshotRestoreReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbSnapshotRestoreReason {
    MissingCommittedHead,
    DivergentReplica,
}

pub async fn personaldb_catch_up(
    storage: &Storage,
    request: PersonalDbCatchUpRequest,
    signing_key: &[u8],
) -> Result<PersonalDbCatchUpResponse> {
    validate_request(&request)?;
    let Some(committed_head) = read_personaldb_committed_head(
        storage,
        request.tenant_id,
        &request.database_id,
        signing_key,
    )
    .await?
    else {
        return Ok(snapshot_required(
            storage,
            &request,
            None,
            signing_key,
            PersonalDbSnapshotRestoreReason::MissingCommittedHead,
        )
        .await?);
    };

    let records = read_canonical_records(storage, &request.database_id, &committed_head).await?;
    ensure_head_matches_records(&committed_head, &records)?;
    if !is_replica_position_on_chain(storage, &request, signing_key, &records).await? {
        return Ok(snapshot_required(
            storage,
            &request,
            Some(committed_head),
            signing_key,
            PersonalDbSnapshotRestoreReason::DivergentReplica,
        )
        .await?);
    }

    let available = records
        .iter()
        .filter(|record| record.log_index > request.have_log_index)
        .cloned()
        .collect::<Vec<_>>();
    let selected_len = available.len().min(request.max_entries);
    let has_more = selected_len < available.len();
    let mut entries = Vec::with_capacity(selected_len);
    for record in available.into_iter().take(selected_len) {
        entries.push(
            load_catch_up_entry(
                storage,
                request.tenant_id,
                &request.database_id,
                record,
                signing_key,
            )
            .await?,
        );
    }

    Ok(PersonalDbCatchUpResponse::Entries(
        PersonalDbCatchUpEntries {
            committed_head,
            entries,
            has_more,
        },
    ))
}

async fn read_canonical_records(
    storage: &Storage,
    database_id: &str,
    committed_head: &PersonalDbCommittedHead,
) -> Result<Vec<PersonalDbLogRecord>> {
    if committed_head.log_index == 0 {
        return Ok(Vec::new());
    }
    let segment_refs = list_log_segment_refs(storage, committed_head, database_id).await?;
    let mut records = Vec::new();
    for segment_ref in segment_refs {
        let segment = read_personaldb_log_segment(storage, &segment_ref).await?;
        for record in segment.records {
            if record.log_index <= committed_head.log_index {
                records.push(record);
            }
        }
    }
    records.sort_by_key(|record| record.log_index);
    ensure_contiguous_chain(&records)?;
    Ok(records)
}

async fn list_log_segment_refs(
    storage: &Storage,
    committed_head: &PersonalDbCommittedHead,
    database_id: &str,
) -> Result<Vec<String>> {
    let tenant_id = committed_head
        .tenant_id
        .parse::<i64>()
        .context("personaldb committed head tenant id must be numeric")?;
    list_personaldb_log_segment_refs(storage, tenant_id, database_id).await
}

fn ensure_head_matches_records(
    committed_head: &PersonalDbCommittedHead,
    records: &[PersonalDbLogRecord],
) -> Result<()> {
    if committed_head.log_index == 0 {
        return Ok(());
    }
    let Some(last) = records.last() else {
        return Err(anyhow!(
            "personaldb committed head has no readable log records"
        ));
    };
    if last.log_index != committed_head.log_index {
        return Err(anyhow!(
            "personaldb committed head log index is not readable"
        ));
    }
    if hex::encode(last.entry_hash) != committed_head.log_hash {
        return Err(anyhow!(
            "personaldb committed head hash does not match log chain"
        ));
    }
    Ok(())
}

async fn is_replica_position_on_chain(
    storage: &Storage,
    request: &PersonalDbCatchUpRequest,
    signing_key: &[u8],
    records: &[PersonalDbLogRecord],
) -> Result<bool> {
    if request.have_log_index == 0 {
        let Some(manifest) = read_personaldb_group_manifest(
            storage,
            request.tenant_id,
            &request.database_id,
            signing_key,
        )
        .await?
        else {
            return Ok(false);
        };
        return Ok(request.have_log_hash == manifest.genesis_hash);
    }
    Ok(records.iter().any(|record| {
        record.log_index == request.have_log_index
            && hex::encode(record.entry_hash) == request.have_log_hash
    }))
}

async fn snapshot_required(
    storage: &Storage,
    request: &PersonalDbCatchUpRequest,
    committed_head: Option<PersonalDbCommittedHead>,
    signing_key: &[u8],
    reason: PersonalDbSnapshotRestoreReason,
) -> Result<PersonalDbCatchUpResponse> {
    let snapshots_head = read_personaldb_snapshots_head(
        storage,
        request.tenant_id,
        &request.database_id,
        signing_key,
    )
    .await?;
    Ok(PersonalDbCatchUpResponse::SnapshotRequired(
        PersonalDbSnapshotRestore {
            committed_head,
            snapshots_head,
            reason,
        },
    ))
}

async fn load_catch_up_entry(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: PersonalDbLogRecord,
    signing_key: &[u8],
) -> Result<PersonalDbCatchUpEntry> {
    let changeset_bytes = load_changeset_bytes(storage, tenant_id, database_id, &record).await?;
    let (certificate, certificate_json) =
        load_certificate(storage, tenant_id, database_id, &record, signing_key).await?;
    Ok(PersonalDbCatchUpEntry {
        record,
        changeset_bytes,
        certificate,
        certificate_json,
    })
}

async fn load_changeset_bytes(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
) -> Result<Vec<u8>> {
    if !record.payload_ref.is_empty() {
        let payload_ref = std::str::from_utf8(&record.payload_ref)?;
        return read_personaldb_changeset_payload_ref(
            storage,
            payload_ref,
            record.changeset_payload_hash,
        )
        .await?
        .ok_or_else(|| anyhow!("personaldb changeset payload is missing"));
    }
    read_personaldb_changeset_payload_by_index(
        storage,
        tenant_id,
        database_id,
        record.log_index,
        record.changeset_payload_hash,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb changeset payload is missing"))
}

async fn load_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
    signing_key: &[u8],
) -> Result<(PersonalDbCommitCertificate, Vec<u8>)> {
    let certificate_json = if !record.inline_certificate_json.is_empty() {
        record.inline_certificate_json.clone()
    } else if !record.certificate_ref.is_empty() {
        let certificate_ref = std::str::from_utf8(&record.certificate_ref)?;
        let certificate =
            read_personaldb_commit_certificate_ref(storage, certificate_ref, signing_key)
                .await?
                .ok_or_else(|| anyhow!("personaldb commit certificate is missing"))?;
        serde_json::to_vec(&certificate)?
    } else {
        let entry_hash = hex::encode(record.entry_hash);
        let certificate = read_personaldb_commit_certificate(
            storage,
            tenant_id,
            database_id,
            record.log_index,
            &entry_hash,
            signing_key,
        )
        .await?
        .ok_or_else(|| anyhow!("personaldb commit certificate is missing"))?;
        serde_json::to_vec(&certificate)?
    };
    let certificate: PersonalDbCommitCertificate = serde_json::from_slice(&certificate_json)?;
    certificate.verify(signing_key)?;
    let certificate_hash = certificate
        .certificate_hash
        .as_deref()
        .ok_or_else(|| anyhow!("personaldb commit certificate hash is missing"))?;
    if hex::decode(certificate_hash)?.as_slice() != record.certificate_hash {
        return Err(anyhow!(
            "personaldb commit certificate hash does not match log record"
        ));
    }
    if certificate.log_index != record.log_index {
        return Err(anyhow!("personaldb commit certificate log index mismatch"));
    }
    if certificate.entry_hash != hex::encode(record.entry_hash) {
        return Err(anyhow!("personaldb commit certificate entry hash mismatch"));
    }
    if hex::decode(&certificate.changeset_payload_hash)?.as_slice() != record.changeset_payload_hash
    {
        return Err(anyhow!(
            "personaldb commit certificate payload hash mismatch"
        ));
    }
    Ok((certificate, certificate_json))
}

fn ensure_contiguous_chain(records: &[PersonalDbLogRecord]) -> Result<()> {
    for pair in records.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.log_index != previous.log_index + 1 {
            return Err(anyhow!("personaldb log chain has a gap"));
        }
        if current.previous_log_hash != previous.entry_hash {
            return Err(anyhow!("personaldb log chain previous hash mismatch"));
        }
    }
    Ok(())
}

fn validate_request(request: &PersonalDbCatchUpRequest) -> Result<()> {
    if request.database_id.is_empty() {
        return Err(anyhow!("personaldb catch-up database id must not be empty"));
    }
    if request.principal.is_empty() {
        return Err(anyhow!("personaldb catch-up principal must not be empty"));
    }
    if request.replica_id.is_empty() {
        return Err(anyhow!("personaldb catch-up replica id must not be empty"));
    }
    if request.have_log_hash.len() != 64
        || !request
            .have_log_hash
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(anyhow!("personaldb catch-up log hash must be hex32"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        formats::{Hash32, hash32},
        personaldb_commit_store::{
            write_personaldb_changeset_payload, write_personaldb_commit_certificate,
        },
        personaldb_control::{PersonalDbCommitCertificate, PersonalDbGroupManifest},
        personaldb_heads::{
            PersonalDbCommittedHead, PersonalDbSnapshotsHead, write_personaldb_committed_head,
            write_personaldb_group_manifest, write_personaldb_snapshots_head,
        },
        personaldb_segment::{PersonalDbLogSegmentWrite, write_personaldb_log_segment},
    };
    use tempfile::{TempDir, tempdir};

    const KEY: &[u8] = b"personaldb catchup signing key";

    #[tokio::test]
    async fn catch_up_from_genesis_returns_entries_with_payloads_and_certificates() {
        let fixture = Fixture::create().await;
        let response = personaldb_catch_up(
            &fixture.storage,
            PersonalDbCatchUpRequest {
                tenant_id: 3,
                database_id: "db-alpha".to_string(),
                principal: "principal-a".to_string(),
                replica_id: "replica-b".to_string(),
                have_log_index: 0,
                have_log_hash: fixture.genesis_hash.clone(),
                max_entries: 2,
            },
            KEY,
        )
        .await
        .unwrap();

        let PersonalDbCatchUpResponse::Entries(entries) = response else {
            panic!("expected entries");
        };
        assert_eq!(entries.entries.len(), 2);
        assert!(entries.has_more);
        assert_eq!(entries.entries[0].changeset_bytes, b"change-1");
        assert_eq!(entries.entries[1].record.log_index, 2);
        assert!(entries.entries[0].certificate_json.starts_with(b"{"));
        entries.entries[0].certificate.verify(KEY).unwrap();
    }

    #[tokio::test]
    async fn catch_up_from_middle_returns_entries_after_position() {
        let fixture = Fixture::create().await;
        let response = personaldb_catch_up(
            &fixture.storage,
            PersonalDbCatchUpRequest {
                tenant_id: 3,
                database_id: "db-alpha".to_string(),
                principal: "principal-a".to_string(),
                replica_id: "replica-b".to_string(),
                have_log_index: 1,
                have_log_hash: hex::encode(fixture.records[0].entry_hash),
                max_entries: 10,
            },
            KEY,
        )
        .await
        .unwrap();

        let PersonalDbCatchUpResponse::Entries(entries) = response else {
            panic!("expected entries");
        };
        assert_eq!(
            entries
                .entries
                .iter()
                .map(|entry| entry.record.log_index)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(!entries.has_more);
    }

    #[tokio::test]
    async fn divergent_replica_gets_snapshot_restore_instruction() {
        let fixture = Fixture::create().await;
        let response = personaldb_catch_up(
            &fixture.storage,
            PersonalDbCatchUpRequest {
                tenant_id: 3,
                database_id: "db-alpha".to_string(),
                principal: "principal-a".to_string(),
                replica_id: "replica-b".to_string(),
                have_log_index: 2,
                have_log_hash: hex::encode([99; 32]),
                max_entries: 10,
            },
            KEY,
        )
        .await
        .unwrap();

        let PersonalDbCatchUpResponse::SnapshotRequired(snapshot) = response else {
            panic!("expected snapshot");
        };
        assert_eq!(
            snapshot.reason,
            PersonalDbSnapshotRestoreReason::DivergentReplica
        );
        assert_eq!(snapshot.committed_head.unwrap().log_index, 3);
        assert_eq!(
            snapshot.snapshots_head.unwrap().latest_snapshot_log_index,
            2
        );
    }

    #[tokio::test]
    async fn missing_head_gets_snapshot_restore_instruction() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let response = personaldb_catch_up(
            &storage,
            PersonalDbCatchUpRequest {
                tenant_id: 3,
                database_id: "db-alpha".to_string(),
                principal: "principal-a".to_string(),
                replica_id: "replica-b".to_string(),
                have_log_index: 0,
                have_log_hash: hex::encode([0; 32]),
                max_entries: 10,
            },
            KEY,
        )
        .await
        .unwrap();

        assert!(matches!(
            response,
            PersonalDbCatchUpResponse::SnapshotRequired(PersonalDbSnapshotRestore {
                reason: PersonalDbSnapshotRestoreReason::MissingCommittedHead,
                ..
            })
        ));
    }

    struct Fixture {
        _temp: TempDir,
        storage: Storage,
        genesis_hash: String,
        records: Vec<PersonalDbLogRecord>,
    }

    impl Fixture {
        async fn create() -> Self {
            let temp = tempdir().unwrap();
            let storage = Storage::new_at(temp.path()).await.unwrap();
            let genesis_hash = hex::encode([0; 32]);
            let manifest = PersonalDbGroupManifest {
                format_version: 1,
                tenant_id: "3".to_string(),
                database_id: "db-alpha".to_string(),
                schema_hash: hex::encode([7; 32]),
                genesis_hash: genesis_hash.clone(),
                created_at: now(),
                created_by: "principal-a".to_string(),
                consistency_policy: "StrictWitnessed".to_string(),
                object_layout_version: 1,
                active_membership_epoch: 1,
                active_policy_epoch: 1,
                current_row_index_generation: 0,
                current_projection_generation: 0,
                manifest_hash: None,
                manifest_signature: None,
            }
            .seal(KEY)
            .unwrap();
            write_personaldb_group_manifest(&storage, 3, &manifest, KEY)
                .await
                .unwrap();

            let mut previous = [0; 32];
            let mut records = Vec::new();
            for log_index in 1..=3 {
                let changeset = format!("change-{log_index}").into_bytes();
                let payload_hash = hash32(&changeset);
                let payload_paths = write_personaldb_changeset_payload(
                    &storage,
                    3,
                    "db-alpha",
                    log_index,
                    payload_hash,
                    &changeset,
                )
                .await
                .unwrap();

                let payload_ref = payload_paths.by_index_ref.clone().into_bytes();
                let provisional_record = PersonalDbLogRecord::new(
                    log_index,
                    1,
                    1,
                    1,
                    previous,
                    payload_hash,
                    [8; 32],
                    [0; 32],
                    payload_ref.clone(),
                    Vec::new(),
                    Vec::new(),
                );
                let certificate = PersonalDbCommitCertificate {
                    format_version: 1,
                    tenant_id: "3".to_string(),
                    database_id: "db-alpha".to_string(),
                    log_index,
                    previous_log_hash: hex::encode(previous),
                    entry_hash: hex::encode(provisional_record.entry_hash),
                    changeset_payload_hash: hex::encode(payload_hash),
                    verified_envelope_hash: hex::encode([8; 32]),
                    client_log_epoch: 1,
                    membership_epoch: 1,
                    policy_epoch: 1,
                    leader_replica_id: "leader-a".to_string(),
                    voter_acks_hash: hex::encode([9; 32]),
                    authz_revision: 1,
                    witness_node_id: "node-a".to_string(),
                    witnessed_at: now(),
                    certificate_hash: None,
                    witness_signature: None,
                }
                .seal(KEY)
                .unwrap();
                let certificate_ref =
                    write_personaldb_commit_certificate(&storage, 3, "db-alpha", &certificate, KEY)
                        .await
                        .unwrap();
                let certificate_hash =
                    hex_to_hash(certificate.certificate_hash.as_deref().unwrap());
                let record = PersonalDbLogRecord::new(
                    log_index,
                    1,
                    1,
                    1,
                    previous,
                    payload_hash,
                    [8; 32],
                    certificate_hash,
                    payload_ref,
                    certificate_ref.into_bytes(),
                    Vec::new(),
                );
                previous = record.entry_hash;
                records.push(record);
            }

            let segment_ref = write_personaldb_log_segment(
                &storage,
                PersonalDbLogSegmentWrite {
                    tenant_id: 3,
                    database_id: "db-alpha",
                    schema_hash: [7; 32],
                    source_fence_token: 11,
                    records: &records,
                },
            )
            .await
            .unwrap();
            let committed = PersonalDbCommittedHead {
                format_version: 1,
                tenant_id: "3".to_string(),
                database_id: "db-alpha".to_string(),
                log_index: 3,
                log_hash: hex::encode(records.last().unwrap().entry_hash),
                segment_path: segment_ref,
                row_index_generation: 0,
                policy_epoch: 1,
                membership_epoch: 1,
                schema_hash: hex::encode([7; 32]),
                updated_at: now(),
                updated_by_node: "node-a".to_string(),
                head_hash: None,
                head_signature: None,
            }
            .seal(KEY)
            .unwrap();
            write_personaldb_committed_head(&storage, 3, "db-alpha", &committed, KEY)
                .await
                .unwrap();

            let snapshots = PersonalDbSnapshotsHead {
                format_version: 1,
                tenant_id: "3".to_string(),
                database_id: "db-alpha".to_string(),
                latest_snapshot_log_index: 2,
                latest_snapshot_log_hash: hex::encode(records[1].entry_hash),
                latest_snapshot_manifest_path: "_anvil/personaldb/tenants/tenant-3/groups/db-alpha/snapshots/manifests/00000000000000000002-state.json".to_string(),
                retained_snapshot_count: 1,
                updated_at: now(),
                updated_by_node: "node-a".to_string(),
                head_hash: None,
                head_signature: None,
            }
            .seal(KEY)
            .unwrap();
            write_personaldb_snapshots_head(&storage, 3, "db-alpha", &snapshots, KEY)
                .await
                .unwrap();

            Self {
                _temp: temp,
                storage,
                genesis_hash,
                records,
            }
        }
    }

    fn now() -> String {
        "2026-06-27T00:00:00.000000000Z".to_string()
    }

    fn hex_to_hash(value: &str) -> Hash32 {
        hex::decode(value).unwrap().try_into().unwrap()
    }
}
