use crate::{
    anvil_personaldb_sqlite_changeset::apply_changeset_to_snapshot_builder,
    formats::{Hash32, hash32, personaldb::PersonalDbLogRecord},
    personaldb_commit_store::read_personaldb_changeset_payload_by_index,
    personaldb_control::PersonalDbSnapshotManifest,
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead, read_personaldb_committed_head,
        read_personaldb_group_manifest, read_personaldb_snapshots_head,
        write_personaldb_snapshots_head,
    },
    personaldb_segment::read_personaldb_log_segment,
    personaldb_snapshot_store::{read_personaldb_snapshot_object, write_personaldb_snapshot},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use rusqlite::Connection;
use std::{io::Cursor, path::PathBuf};
use tempfile::NamedTempFile;

pub const DEFAULT_SNAPSHOT_ENTRY_THRESHOLD: u64 = 1024;
pub const DEFAULT_SNAPSHOT_PAYLOAD_BYTES_THRESHOLD: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersonalDbSnapshotPolicy {
    pub entry_threshold: u64,
    pub payload_bytes_threshold: u64,
}

impl Default for PersonalDbSnapshotPolicy {
    fn default() -> Self {
        Self {
            entry_threshold: DEFAULT_SNAPSHOT_ENTRY_THRESHOLD,
            payload_bytes_threshold: DEFAULT_SNAPSHOT_PAYLOAD_BYTES_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersonalDbSnapshotBuildRequest<'a> {
    pub tenant_id: i64,
    pub database_id: &'a str,
    pub schema_sql: &'a str,
    pub created_by_node: &'a str,
    pub policy: PersonalDbSnapshotPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotBuildResult {
    pub manifest: PersonalDbSnapshotManifest,
    pub compressed_sqlite_bytes: Vec<u8>,
    pub uncompressed_state_hash: Hash32,
}

pub async fn maybe_build_personaldb_snapshot(
    storage: &Storage,
    request: PersonalDbSnapshotBuildRequest<'_>,
    signing_key: &[u8],
) -> Result<Option<PersonalDbSnapshotBuildResult>> {
    validate_request(&request)?;
    let manifest = read_personaldb_group_manifest(
        storage,
        request.tenant_id,
        request.database_id,
        signing_key,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb group manifest missing"))?;
    let committed_head = read_personaldb_committed_head(
        storage,
        request.tenant_id,
        request.database_id,
        signing_key,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb committed head missing"))?;
    if committed_head.log_index == 0 {
        return Ok(None);
    }
    if manifest.schema_hash != hex::encode(hash32(request.schema_sql.as_bytes())) {
        return Err(anyhow!("personaldb snapshot schema hash mismatch"));
    }

    let previous_snapshot = read_personaldb_snapshots_head(
        storage,
        request.tenant_id,
        request.database_id,
        signing_key,
    )
    .await?;
    let base_log_index = previous_snapshot
        .as_ref()
        .map(|head| head.latest_snapshot_log_index)
        .unwrap_or(0);
    if committed_head.log_index <= base_log_index {
        return Ok(None);
    }

    let records = read_canonical_records(storage, request.database_id, &committed_head).await?;
    ensure_head_matches_records(&committed_head, &records)?;
    let new_records = records
        .iter()
        .filter(|record| record.log_index > base_log_index)
        .cloned()
        .collect::<Vec<_>>();
    let payload_bytes = sum_changeset_payload_bytes(
        storage,
        request.tenant_id,
        request.database_id,
        &new_records,
    )
    .await?;
    if (new_records.len() as u64) < request.policy.entry_threshold
        && payload_bytes < request.policy.payload_bytes_threshold
    {
        return Ok(None);
    }

    let result = build_snapshot(
        storage,
        request,
        signing_key,
        previous_snapshot.as_ref(),
        &committed_head,
        &new_records,
    )
    .await?;
    publish_snapshots_head(storage, request, signing_key, &result.manifest).await?;
    Ok(Some(result))
}

async fn build_snapshot(
    storage: &Storage,
    request: PersonalDbSnapshotBuildRequest<'_>,
    signing_key: &[u8],
    previous_snapshot: Option<&PersonalDbSnapshotsHead>,
    committed_head: &PersonalDbCommittedHead,
    new_records: &[PersonalDbLogRecord],
) -> Result<PersonalDbSnapshotBuildResult> {
    let temp = NamedTempFile::new_in(storage.temp_dir_path())?;
    let temp_path = temp.path().to_path_buf();
    drop(temp);

    if let Some(snapshot_head) = previous_snapshot {
        restore_snapshot_database(storage, request, signing_key, snapshot_head, &temp_path).await?;
    }

    {
        let connection = Connection::open(&temp_path)?;
        if previous_snapshot.is_none() {
            connection.execute_batch(request.schema_sql)?;
        }
        for record in new_records {
            let changeset =
                load_changeset_bytes(storage, request.tenant_id, request.database_id, record)
                    .await?;
            apply_changeset_to_snapshot_builder(&connection, &changeset)?;
        }
        connection.execute_batch("PRAGMA optimize;")?;
    }

    let sqlite_bytes = tokio::fs::read(&temp_path)
        .await
        .with_context(|| format!("read personaldb snapshot builder {}", temp_path.display()))?;
    let uncompressed_state_hash = hash32(&sqlite_bytes);
    let compressed_sqlite_bytes = zstd::stream::encode_all(Cursor::new(&sqlite_bytes), 3)?;
    let snapshot_object_hash = hash32(&compressed_sqlite_bytes);
    let state_hash = hex::encode(uncompressed_state_hash);
    let object_path = storage.personaldb_snapshot_object_path(
        request.tenant_id,
        request.database_id,
        committed_head.log_index,
        &state_hash,
    )?;
    let manifest = PersonalDbSnapshotManifest {
        format_version: 1,
        tenant_id: request.tenant_id.to_string(),
        database_id: request.database_id.to_string(),
        log_index: committed_head.log_index,
        log_hash: committed_head.log_hash.clone(),
        state_hash,
        schema_hash: committed_head.schema_hash.clone(),
        snapshot_object_key: storage.relative_storage_path(&object_path)?,
        snapshot_object_hash: hex::encode(snapshot_object_hash),
        source_segment_start: new_records
            .first()
            .map(|record| record.log_index)
            .unwrap_or(0),
        source_segment_end: committed_head.log_index,
        row_index_generation: committed_head.row_index_generation,
        created_at: chrono::Utc::now().to_rfc3339(),
        created_by_node: request.created_by_node.to_string(),
        manifest_hash: None,
        manifest_signature: None,
    }
    .seal(signing_key)?;

    write_personaldb_snapshot(
        storage,
        request.tenant_id,
        request.database_id,
        &compressed_sqlite_bytes,
        &manifest,
        signing_key,
    )
    .await?;
    let _ = tokio::fs::remove_file(&temp_path).await;
    Ok(PersonalDbSnapshotBuildResult {
        manifest,
        compressed_sqlite_bytes,
        uncompressed_state_hash,
    })
}

async fn restore_snapshot_database(
    storage: &Storage,
    request: PersonalDbSnapshotBuildRequest<'_>,
    signing_key: &[u8],
    snapshot_head: &PersonalDbSnapshotsHead,
    target_path: &PathBuf,
) -> Result<()> {
    let manifest_path =
        storage.resolve_relative_storage_path(&snapshot_head.latest_snapshot_manifest_path)?;
    let manifest_bytes = tokio::fs::read(&manifest_path).await.with_context(|| {
        format!(
            "read personaldb snapshot manifest {}",
            manifest_path.display()
        )
    })?;
    let manifest: PersonalDbSnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
    manifest.verify(signing_key)?;
    if manifest.tenant_id != request.tenant_id.to_string()
        || manifest.database_id != request.database_id
        || manifest.log_index != snapshot_head.latest_snapshot_log_index
        || manifest.log_hash != snapshot_head.latest_snapshot_log_hash
    {
        return Err(anyhow!("personaldb snapshot head does not match manifest"));
    }
    let compressed = read_personaldb_snapshot_object(
        storage,
        request.tenant_id,
        request.database_id,
        &manifest,
        signing_key,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb snapshot object missing"))?;
    let sqlite_bytes = zstd::stream::decode_all(Cursor::new(compressed))?;
    tokio::fs::write(target_path, sqlite_bytes)
        .await
        .with_context(|| format!("restore personaldb snapshot {}", target_path.display()))?;
    Ok(())
}

async fn publish_snapshots_head(
    storage: &Storage,
    request: PersonalDbSnapshotBuildRequest<'_>,
    signing_key: &[u8],
    manifest: &PersonalDbSnapshotManifest,
) -> Result<()> {
    let manifest_path = storage.personaldb_snapshot_manifest_path(
        request.tenant_id,
        request.database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    let head = PersonalDbSnapshotsHead {
        format_version: 1,
        tenant_id: request.tenant_id.to_string(),
        database_id: request.database_id.to_string(),
        latest_snapshot_log_index: manifest.log_index,
        latest_snapshot_log_hash: manifest.log_hash.clone(),
        latest_snapshot_manifest_path: storage.relative_storage_path(&manifest_path)?,
        retained_snapshot_count: 1,
        updated_at: chrono::Utc::now().to_rfc3339(),
        updated_by_node: request.created_by_node.to_string(),
        head_hash: None,
        head_signature: None,
    }
    .seal(signing_key)?;
    write_personaldb_snapshots_head(
        storage,
        request.tenant_id,
        request.database_id,
        &head,
        signing_key,
    )
    .await
}

async fn read_canonical_records(
    storage: &Storage,
    database_id: &str,
    committed_head: &PersonalDbCommittedHead,
) -> Result<Vec<PersonalDbLogRecord>> {
    if committed_head.log_index == 0 {
        return Ok(Vec::new());
    }
    let tenant_id = committed_head
        .tenant_id
        .parse::<i64>()
        .context("personaldb committed head tenant id must be numeric")?;
    let dir = storage.personaldb_log_segment_dir(tenant_id, database_id)?;
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("read {}", dir.display())),
    };
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("pdbseg") {
            paths.push(path);
        }
    }
    paths.sort_by_key(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.split('-').next())
            .and_then(|start| start.parse::<u64>().ok())
            .unwrap_or(u64::MAX)
    });

    let mut records = Vec::new();
    for path in paths {
        let segment = read_personaldb_log_segment(path).await?;
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

async fn sum_changeset_payload_bytes(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    records: &[PersonalDbLogRecord],
) -> Result<u64> {
    let mut total = 0u64;
    for record in records {
        let len = load_changeset_bytes(storage, tenant_id, database_id, record)
            .await?
            .len();
        total = total
            .checked_add(len as u64)
            .ok_or_else(|| anyhow!("personaldb snapshot payload byte count overflow"))?;
    }
    Ok(total)
}

async fn load_changeset_bytes(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
) -> Result<Vec<u8>> {
    if !record.payload_ref.is_empty() {
        let relative = std::str::from_utf8(&record.payload_ref)?;
        let path = storage.resolve_relative_storage_path(relative)?;
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read personaldb changeset payload {}", path.display()))?;
        if hash32(&bytes) != record.changeset_payload_hash {
            return Err(anyhow!("personaldb changeset payload hash mismatch"));
        }
        return Ok(bytes);
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

fn ensure_head_matches_records(
    committed_head: &PersonalDbCommittedHead,
    records: &[PersonalDbLogRecord],
) -> Result<()> {
    let Some(last) = records.last() else {
        return Err(anyhow!(
            "personaldb committed head has no readable log records"
        ));
    };
    if last.log_index != committed_head.log_index
        || hex::encode(last.entry_hash) != committed_head.log_hash
    {
        return Err(anyhow!(
            "personaldb committed head does not match readable log chain"
        ));
    }
    Ok(())
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

fn validate_request(request: &PersonalDbSnapshotBuildRequest<'_>) -> Result<()> {
    if request.database_id.is_empty() {
        return Err(anyhow!("personaldb snapshot database id must not be empty"));
    }
    if request.schema_sql.trim().is_empty() {
        return Err(anyhow!("personaldb snapshot schema SQL must not be empty"));
    }
    if request.created_by_node.is_empty() {
        return Err(anyhow!("personaldb snapshot creator must not be empty"));
    }
    if request.policy.entry_threshold == 0 && request.policy.payload_bytes_threshold == 0 {
        return Err(anyhow!(
            "personaldb snapshot policy cannot disable both thresholds"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        personaldb_commit_store::{
            write_personaldb_changeset_payload, write_personaldb_commit_certificate,
        },
        personaldb_control::{PersonalDbCommitCertificate, PersonalDbGroupManifest},
        personaldb_heads::{
            PersonalDbCommittedHead, read_personaldb_snapshots_head,
            write_personaldb_committed_head, write_personaldb_group_manifest,
        },
        personaldb_segment::{PersonalDbLogSegmentWrite, write_personaldb_log_segment},
    };
    use rusqlite::{Connection, session::Session};
    use tempfile::{TempDir, tempdir};

    const KEY: &[u8] = b"personaldb snapshot builder signing key";
    const SCHEMA_SQL: &str = "CREATE TABLE items(
        id INTEGER PRIMARY KEY NOT NULL,
        name TEXT NOT NULL,
        payload BLOB
    );";

    #[tokio::test]
    async fn snapshot_builder_thresholds_writes_zstd_sqlite_snapshot_and_head() {
        let fixture = Fixture::create(2).await;

        let not_due = maybe_build_personaldb_snapshot(
            &fixture.storage,
            request(PersonalDbSnapshotPolicy {
                entry_threshold: 10,
                payload_bytes_threshold: 1024 * 1024,
            }),
            KEY,
        )
        .await
        .unwrap();
        assert!(not_due.is_none());

        let built = maybe_build_personaldb_snapshot(
            &fixture.storage,
            request(PersonalDbSnapshotPolicy {
                entry_threshold: 2,
                payload_bytes_threshold: 1024 * 1024,
            }),
            KEY,
        )
        .await
        .unwrap()
        .expect("snapshot should be due");

        assert_eq!(built.manifest.log_index, 2);
        assert_eq!(
            built.manifest.log_hash,
            hex::encode(fixture.records[1].entry_hash)
        );
        assert_eq!(built.manifest.source_segment_start, 1);
        assert_eq!(built.manifest.source_segment_end, 2);
        assert_eq!(built.manifest.schema_hash, fixture.schema_hash);
        assert!(built.manifest.snapshot_object_key.ends_with(".sqlite.zst"));
        assert_eq!(
            built.manifest.snapshot_object_hash,
            hex::encode(hash32(&built.compressed_sqlite_bytes))
        );

        let restored = zstd::stream::decode_all(Cursor::new(&built.compressed_sqlite_bytes))
            .expect("snapshot should decompress");
        assert_eq!(hash32(&restored), built.uncompressed_state_hash);
        let restored_path = fixture.temp.path().join("restored.sqlite");
        tokio::fs::write(&restored_path, restored).await.unwrap();
        let db = Connection::open(restored_path).unwrap();
        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
            .unwrap();
        let second_name: String = db
            .query_row("SELECT name FROM items WHERE id = 2", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(second_name, "item-2");

        let snapshots_head =
            read_personaldb_snapshots_head(&fixture.storage, 9, "db-snapshot", KEY)
                .await
                .unwrap()
                .expect("snapshots head should be published");
        assert_eq!(snapshots_head.latest_snapshot_log_index, 2);
        assert_eq!(
            snapshots_head.latest_snapshot_manifest_path,
            fixture
                .storage
                .relative_storage_path(
                    &fixture
                        .storage
                        .personaldb_snapshot_manifest_path(
                            9,
                            "db-snapshot",
                            2,
                            &built.manifest.state_hash,
                        )
                        .unwrap()
                )
                .unwrap()
        );
    }

    #[tokio::test]
    async fn snapshot_builder_rejects_schema_hash_mismatch() {
        let fixture = Fixture::create(1).await;
        let err = maybe_build_personaldb_snapshot(
            &fixture.storage,
            PersonalDbSnapshotBuildRequest {
                schema_sql: "CREATE TABLE different(id INTEGER PRIMARY KEY);",
                ..request(PersonalDbSnapshotPolicy {
                    entry_threshold: 1,
                    payload_bytes_threshold: 1024 * 1024,
                })
            },
            KEY,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("schema hash mismatch"));
    }

    struct Fixture {
        temp: TempDir,
        storage: Storage,
        schema_hash: String,
        records: Vec<PersonalDbLogRecord>,
    }

    impl Fixture {
        async fn create(record_count: u64) -> Self {
            let temp = tempdir().unwrap();
            let storage = Storage::new_at(temp.path()).await.unwrap();
            let schema_hash = hex::encode(hash32(SCHEMA_SQL.as_bytes()));
            let genesis_hash = hex::encode(hash32(b"snapshot-genesis"));
            let manifest = PersonalDbGroupManifest {
                format_version: 1,
                tenant_id: "9".to_string(),
                database_id: "db-snapshot".to_string(),
                schema_hash: schema_hash.clone(),
                genesis_hash,
                created_at: now(),
                created_by: "node-a".to_string(),
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
            write_personaldb_group_manifest(&storage, 9, &manifest, KEY)
                .await
                .unwrap();

            let mut previous = [0; 32];
            let mut records = Vec::new();
            for log_index in 1..=record_count {
                let changeset = make_insert_changeset(log_index);
                let payload_hash = hash32(&changeset);
                let payload_paths = write_personaldb_changeset_payload(
                    &storage,
                    9,
                    "db-snapshot",
                    log_index,
                    payload_hash,
                    &changeset,
                )
                .await
                .unwrap();
                let payload_ref = storage
                    .relative_storage_path(&payload_paths.by_index_path)
                    .unwrap()
                    .into_bytes();
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
                    tenant_id: "9".to_string(),
                    database_id: "db-snapshot".to_string(),
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
                let certificate_path = write_personaldb_commit_certificate(
                    &storage,
                    9,
                    "db-snapshot",
                    &certificate,
                    KEY,
                )
                .await
                .unwrap();
                let certificate_hash = hex_to_hash(certificate.certificate_hash.as_ref().unwrap());
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
                    storage
                        .relative_storage_path(&certificate_path)
                        .unwrap()
                        .into_bytes(),
                    Vec::new(),
                );
                previous = record.entry_hash;
                records.push(record);
            }

            let segment_path = write_personaldb_log_segment(
                &storage,
                PersonalDbLogSegmentWrite {
                    tenant_id: 9,
                    database_id: "db-snapshot",
                    schema_hash: hex_to_hash(&schema_hash),
                    source_fence_token: 1,
                    records: &records,
                },
            )
            .await
            .unwrap();
            let committed = PersonalDbCommittedHead {
                format_version: 1,
                tenant_id: "9".to_string(),
                database_id: "db-snapshot".to_string(),
                log_index: record_count,
                log_hash: hex::encode(records.last().unwrap().entry_hash),
                segment_path: storage.relative_storage_path(&segment_path).unwrap(),
                row_index_generation: record_count,
                policy_epoch: 1,
                membership_epoch: 1,
                schema_hash: schema_hash.clone(),
                updated_at: now(),
                updated_by_node: "node-a".to_string(),
                head_hash: None,
                head_signature: None,
            }
            .seal(KEY)
            .unwrap();
            write_personaldb_committed_head(&storage, 9, "db-snapshot", &committed, KEY)
                .await
                .unwrap();

            Self {
                temp,
                storage,
                schema_hash,
                records,
            }
        }
    }

    fn request(policy: PersonalDbSnapshotPolicy) -> PersonalDbSnapshotBuildRequest<'static> {
        PersonalDbSnapshotBuildRequest {
            tenant_id: 9,
            database_id: "db-snapshot",
            schema_sql: SCHEMA_SQL,
            created_by_node: "node-a",
            policy,
        }
    }

    fn make_insert_changeset(id: u64) -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(SCHEMA_SQL).unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        let name = format!("item-{id}");
        let payload = vec![id as u8, id.saturating_add(1) as u8];
        db.execute(
            "INSERT INTO items (id, name, payload) VALUES (?1, ?2, ?3)",
            rusqlite::params![id as i64, name, payload],
        )
        .unwrap();
        let mut output = Vec::new();
        session.changeset_strm(&mut output).unwrap();
        output
    }

    fn now() -> String {
        "2026-06-28T00:00:00.000000000Z".to_string()
    }

    fn hex_to_hash(value: &str) -> Hash32 {
        hex::decode(value).unwrap().try_into().unwrap()
    }
}
