use super::*;
use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreTransactionUpdate, ReadStream,
};

const CONTROL_STREAM_PAGE_MAX_ROWS: usize = 4_096;
const CONTROL_STREAM_PARTITION_PAGE_MAX_ROWS: usize = 4_095;
const CONTROL_STREAM_CURRENT_PAGE_MAX_ROWS: usize = 4_095;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamCurrentRecord {
    pub record_key: String,
    pub generation: u64,
    pub deleted: bool,
    pub frame: ControlStreamFrame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamCurrentPage {
    pub records: Vec<ControlStreamCurrentRecord>,
    pub next_stream_id: Option<String>,
}

pub async fn control_stream_append_cursor(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<ControlStreamAppendCursor> {
    let stream_id = control_stream_id(stream_family, partition)?;
    let store = CoreStore::new(storage.clone()).await?;
    let head_sequence = store.stream_head_sequence(&stream_id).await?;
    let byte_offset = if head_sequence == 0 {
        0
    } else {
        let record = read_exact_stream_record(&store, &stream_id, head_sequence).await?;
        let (frame, encoded_len) = decode_stored_frame(&stream_id, record, "mesh.control.frame")?;
        let header = decode_control_mutation_header(&frame.header_proto)?;
        header
            .byte_offset
            .checked_add(encoded_len as u64)
            .ok_or_else(|| anyhow!("CoreStore control stream {stream_id} byte offset overflow"))?
    };
    Ok(ControlStreamAppendCursor {
        sequence: ControlStreamSequence::new(
            head_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("CoreStore control stream {stream_id} sequence overflow"))?,
        )?,
        byte_offset,
    })
}

pub async fn read_control_stream_page(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    after_sequence: u64,
    limit: usize,
) -> AnyhowResult<ControlStreamLogPage> {
    if !(1..=CONTROL_STREAM_PAGE_MAX_ROWS).contains(&limit) {
        return Err(anyhow!(
            "control stream page size must be between 1 and {CONTROL_STREAM_PAGE_MAX_ROWS}"
        ));
    }
    let stream_id = control_stream_id(stream_family, partition)?;
    let store = CoreStore::new(storage.clone()).await?;
    let page = store
        .read_stream_page(ReadStream {
            stream_id: stream_id.clone(),
            after_sequence,
            limit,
        })
        .await?;
    let mut expected_offset = if page.records.is_empty() {
        0
    } else {
        offset_after_sequence(&store, &stream_id, after_sequence).await?
    };
    let mut records = Vec::with_capacity(page.records.len());
    for record in page.records {
        let sequence = record.sequence;
        let (frame, encoded_len) = decode_stored_frame(&stream_id, record, "mesh.control.frame")?;
        let metadata = frame.metadata()?;
        let header = decode_control_mutation_header(&frame.header_proto)?;
        if metadata.sequence.get() != sequence {
            return Err(anyhow!(
                "CoreStore control stream {stream_id} sequence mismatch: frame {}, stream {sequence}",
                metadata.sequence.get()
            ));
        }
        if header.stream_family != stream_family || header.partition != partition {
            return Err(anyhow!(
                "control stream header scope {}/{} does not match path {stream_family}/{partition}",
                header.stream_family,
                header.partition
            ));
        }
        if header.byte_offset != expected_offset {
            return Err(anyhow!(
                "CoreStore control stream {stream_id} sequence {sequence} declared byte offset {}, expected {expected_offset}",
                header.byte_offset
            ));
        }
        records.push(ControlStreamLogRecord {
            offset: expected_offset,
            encoded_len,
            metadata,
            frame,
        });
        expected_offset = expected_offset
            .checked_add(encoded_len as u64)
            .ok_or_else(|| anyhow!("CoreStore control stream {stream_id} byte offset overflow"))?;
    }
    Ok(ControlStreamLogPage {
        records,
        next_sequence: page.next_sequence,
        has_more: page.has_more,
    })
}

pub async fn append_control_stream_frame(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    frame: &ControlStreamFrame,
    precondition: Option<CoreMutationPrecondition>,
) -> AnyhowResult<ControlStreamAppend> {
    let stream_id = control_stream_id(stream_family, partition)?;
    let metadata = frame.metadata()?;
    let header = decode_control_mutation_header(&frame.header_proto)?;
    if header.stream_family != stream_family || header.partition != partition {
        return Err(anyhow!(
            "control stream header scope {}/{} does not match path {stream_family}/{partition}",
            header.stream_family,
            header.partition
        ));
    }
    if header.record_key.trim().is_empty() {
        return Err(anyhow!("control stream record key must not be empty"));
    }
    let cursor = control_stream_append_cursor(storage, stream_family, partition).await?;
    if metadata.sequence != cursor.sequence || header.byte_offset != cursor.byte_offset {
        return Err(anyhow!(
            "control stream append cursor changed: frame declares sequence {} offset {}, current sequence {} offset {}",
            metadata.sequence.get(),
            header.byte_offset,
            cursor.sequence.get(),
            cursor.byte_offset
        ));
    }

    let encoded = frame.encode()?;
    let store = CoreStore::new(storage.clone()).await?;
    let record_stream_id = control_record_stream_id(stream_family, partition, &header.record_key)?;
    let mut preconditions: Vec<_> = precondition.into_iter().collect();
    preconditions.push(store.stream_head_precondition(&stream_id).await?);
    preconditions.push(store.stream_head_precondition(&record_stream_id).await?);
    let idempotency_key = header.idempotency_key.as_deref();
    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "mesh-control:{}:{}:{}",
                stream_family,
                partition,
                metadata.sequence.get()
            ),
            scope_partition: partition.to_string(),
            committed_by_principal: format!(
                "partition-owner:mesh_control:{stream_family}:{partition}"
            ),
            preconditions,
            operations: vec![
                CoreMutationOperation::StreamAppend {
                    partition_id: partition.to_string(),
                    stream_id: stream_id.clone(),
                    record_kind: "mesh.control.frame".to_string(),
                    payload: encoded.clone(),
                    idempotency_key: idempotency_key
                        .map(|key| format!("mesh-control-partition:{key}")),
                },
                CoreMutationOperation::StreamAppend {
                    partition_id: partition.to_string(),
                    stream_id: record_stream_id.clone(),
                    record_kind: "mesh.control.record".to_string(),
                    payload: encoded.clone(),
                    idempotency_key: idempotency_key
                        .map(|key| format!("mesh-control-record:{key}")),
                },
            ],
        })
        .await
        .with_context(|| format!("append CoreStore control stream {stream_id}"))?;
    let visible_sequence = visible_stream_update(&receipt.visible_updates, &stream_id)?;
    let _record_sequence = visible_stream_update(&receipt.visible_updates, &record_stream_id)?;
    if visible_sequence != metadata.sequence.get() {
        return Err(anyhow!(
            "CoreStore control stream {stream_id} assigned sequence {visible_sequence}, but frame declared {}",
            metadata.sequence.get()
        ));
    }

    if std::env::var_os("ANVIL_MESH_SYNC_SEGMENTS").is_some() {
        crate::mesh_control_segment::write_mesh_control_segment(
            storage,
            crate::mesh_control_segment::MeshControlSegmentWrite {
                mesh_id: &header.mesh_id,
                stream_family,
                partition,
                generation: visible_sequence,
                event_kind: &header.operation,
                source_cursor: visible_sequence,
                placement_epoch: header.writer_fence,
                boundary_values: &[],
                records: &[crate::mesh_control_segment::MeshControlSegmentRecord {
                    key: header.record_key.as_bytes().to_vec(),
                    value: encoded.clone(),
                }],
            },
        )
        .await
        .with_context(|| format!("write CoreStore mesh-control segment for {stream_id}"))?;
    } else {
        crate::emit_test_timing(
            "mesh_control_stream.append_control_stream_frame deferred_writer_segment",
            std::time::Duration::ZERO,
        );
    }
    Ok(ControlStreamAppend {
        offset: cursor.byte_offset,
        encoded_len: encoded.len(),
        position: metadata.into(),
    })
}

pub async fn latest_projected_record_from_control_stream(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    record_key: &str,
) -> AnyhowResult<Option<ControlProjectionRecord>> {
    let stream_id = control_record_stream_id(stream_family, partition, record_key)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(current) =
        read_current_control_record(&store, stream_family, partition, record_key, &stream_id)
            .await?
    else {
        return Ok(None);
    };
    if current.deleted {
        return Ok(Some(ControlProjectionRecord::tombstone(
            record_key,
            current.generation,
        )));
    }
    Ok(Some(ControlProjectionRecord::new(
        record_key,
        current.generation,
        control_payload_operator_json(stream_family, record_key, &current.frame.payload_proto)?,
    )))
}

pub async fn list_current_control_stream_records_page(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    after_stream_id: Option<&str>,
    limit: usize,
) -> AnyhowResult<ControlStreamCurrentPage> {
    if !(1..=CONTROL_STREAM_CURRENT_PAGE_MAX_ROWS).contains(&limit) {
        return Err(anyhow!(
            "current control stream page size must be between 1 and {CONTROL_STREAM_CURRENT_PAGE_MAX_ROWS}"
        ));
    }
    let prefix = control_record_stream_prefix(stream_family, partition)?;
    let store = CoreStore::new(storage.clone()).await?;
    let stream_ids = store
        .list_stream_ids_page(&prefix, after_stream_id, limit + 1)
        .await?;
    let has_more = stream_ids.len() > limit;
    let visible = if has_more {
        &stream_ids[..limit]
    } else {
        &stream_ids[..]
    };
    let mut records = Vec::with_capacity(visible.len());
    for stream_id in visible {
        let head_sequence = store.stream_head_sequence(stream_id).await?;
        if head_sequence == 0 {
            return Err(anyhow!(
                "current control record stream {stream_id} has no records"
            ));
        }
        let record = read_exact_stream_record(&store, stream_id, head_sequence).await?;
        let (frame, _) = decode_stored_frame(stream_id, record, "mesh.control.record")?;
        let header = decode_control_mutation_header(&frame.header_proto)?;
        if header.stream_family != stream_family || header.partition != partition {
            return Err(anyhow!(
                "current control record stream payload does not match requested scope"
            ));
        }
        if control_record_stream_id(stream_family, partition, &header.record_key)? != *stream_id {
            return Err(anyhow!(
                "current control record stream payload does not match its physical stream"
            ));
        }
        records.push(ControlStreamCurrentRecord {
            record_key: header.record_key,
            generation: header.new_generation,
            deleted: is_delete_operation(&header.operation),
            frame,
        });
    }
    Ok(ControlStreamCurrentPage {
        records,
        next_stream_id: has_more.then(|| visible.last().cloned()).flatten(),
    })
}

pub async fn list_control_stream_partitions_page(
    storage: &Storage,
    stream_family: &str,
    after_stream_id: Option<&str>,
    limit: usize,
) -> AnyhowResult<ControlStreamPartitionPage> {
    if !(1..=CONTROL_STREAM_PARTITION_PAGE_MAX_ROWS).contains(&limit) {
        return Err(anyhow!(
            "control stream partition page size must be between 1 and {CONTROL_STREAM_PARTITION_PAGE_MAX_ROWS}"
        ));
    }
    validate_control_stream_scope(stream_family, "control stream family")?;
    let prefix = control_stream_prefix(stream_family);
    let store = CoreStore::new(storage.clone()).await?;
    let stream_ids = store
        .list_stream_ids_page(&prefix, after_stream_id, limit + 1)
        .await?;
    let has_more = stream_ids.len() > limit;
    let visible = if has_more {
        &stream_ids[..limit]
    } else {
        &stream_ids[..]
    };
    let mut partitions = Vec::with_capacity(visible.len());
    for stream_id in visible {
        let partition = stream_id
            .strip_prefix(&prefix)
            .ok_or_else(|| anyhow!("control stream id escaped requested family prefix"))?;
        validate_control_stream_partition(partition)?;
        partitions.push(partition.to_string());
    }
    Ok(ControlStreamPartitionPage {
        partitions,
        next_stream_id: has_more.then(|| visible.last().cloned()).flatten(),
    })
}

async fn offset_after_sequence(
    store: &CoreStore,
    stream_id: &str,
    sequence: u64,
) -> AnyhowResult<u64> {
    if sequence == 0 {
        return Ok(0);
    }
    let record = read_exact_stream_record(store, stream_id, sequence).await?;
    let (frame, encoded_len) = decode_stored_frame(stream_id, record, "mesh.control.frame")?;
    let header = decode_control_mutation_header(&frame.header_proto)?;
    header
        .byte_offset
        .checked_add(encoded_len as u64)
        .ok_or_else(|| anyhow!("CoreStore control stream {stream_id} byte offset overflow"))
}

async fn read_exact_stream_record(
    store: &CoreStore,
    stream_id: &str,
    sequence: u64,
) -> AnyhowResult<crate::core_store::StreamRecord> {
    let page = store
        .read_stream_page(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: sequence.saturating_sub(1),
            limit: 1,
        })
        .await?;
    page.records
        .into_iter()
        .find(|record| record.sequence == sequence)
        .ok_or_else(|| {
            anyhow!("CoreStore control stream {stream_id} record {sequence} is not readable")
        })
}

fn decode_stored_frame(
    stream_id: &str,
    record: crate::core_store::StreamRecord,
    expected_record_kind: &str,
) -> AnyhowResult<(ControlStreamFrame, usize)> {
    let sequence = record.sequence;
    if record.record_kind != expected_record_kind {
        return Err(anyhow!(
            "CoreStore control stream {stream_id} record {sequence} has kind {}, expected {expected_record_kind}",
            record.record_kind
        ));
    }
    let (frame, used) = ControlStreamFrame::decode(&record.payload)
        .map_err(|err| anyhow!("decode CoreStore control stream {stream_id}: {err}"))?;
    if used != record.payload.len() {
        return Err(anyhow!(
            "CoreStore control stream {stream_id} record {sequence} has trailing bytes"
        ));
    }
    Ok((frame, used))
}

fn visible_stream_update(updates: &[CoreTransactionUpdate], stream_id: &str) -> AnyhowResult<u64> {
    updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                stream_id: update_stream_id,
                visible_sequence,
                ..
            } if update_stream_id == stream_id => Some(*visible_sequence),
            _ => None,
        })
        .ok_or_else(|| anyhow!("CoreStore control stream batch did not append {stream_id}"))
}

fn control_record_stream_id(
    stream_family: &str,
    partition: &str,
    record_key: &str,
) -> AnyhowResult<String> {
    validate_control_stream_scope(stream_family, "control stream family")?;
    validate_control_stream_partition(partition)?;
    if record_key.trim().is_empty() {
        return Err(anyhow!("control stream record key must not be empty"));
    }
    Ok(format!(
        "mesh_control_record:{stream_family}:{partition}:{}",
        blake3::hash(record_key.as_bytes()).to_hex()
    ))
}

fn control_record_stream_prefix(stream_family: &str, partition: &str) -> AnyhowResult<String> {
    validate_control_stream_scope(stream_family, "control stream family")?;
    validate_control_stream_partition(partition)?;
    Ok(format!("mesh_control_record:{stream_family}:{partition}:"))
}

async fn read_current_control_record(
    store: &CoreStore,
    stream_family: &str,
    partition: &str,
    record_key: &str,
    stream_id: &str,
) -> AnyhowResult<Option<ControlStreamCurrentRecord>> {
    let head_sequence = store.stream_head_sequence(stream_id).await?;
    if head_sequence == 0 {
        return Ok(None);
    }
    let record = read_exact_stream_record(store, stream_id, head_sequence).await?;
    let (frame, _) = decode_stored_frame(stream_id, record, "mesh.control.record")?;
    let header = decode_control_mutation_header(&frame.header_proto)?;
    if header.stream_family != stream_family
        || header.partition != partition
        || header.record_key != record_key
    {
        return Err(anyhow!(
            "control record stream payload does not match requested scope"
        ));
    }
    Ok(Some(ControlStreamCurrentRecord {
        record_key: header.record_key,
        generation: header.new_generation,
        deleted: is_delete_operation(&header.operation),
        frame,
    }))
}
