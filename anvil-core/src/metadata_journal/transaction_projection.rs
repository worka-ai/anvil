use super::*;

pub(super) async fn materialize_object_metadata_projection(
    core_store: &CoreStore,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
) -> Result<()> {
    match mutation {
        ObjectJournalMutation::Put | ObjectJournalMutation::DeleteMarker => {
            core_store.put_object_metadata(bucket, object).await?;
        }
        ObjectJournalMutation::DeleteVersion => {
            core_store
                .record_object_metadata_mutation_id(bucket, object.id)
                .await?;
            core_store
                .delete_object_version_metadata(bucket, &object.key, object.version_id)
                .await?;
        }
    }
    Ok(())
}

pub async fn materialize_committed_object_metadata_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<Vec<CommittedObjectMetadataProjection>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut materialized = Vec::new();
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        let Some((tenant_id, bucket_id)) = parse_object_metadata_stream_id(stream_id) else {
            continue;
        };
        let Some(bucket) = bucket_journal::read_current_bucket_by_id(storage, bucket_id).await?
        else {
            return Err(anyhow!(
                "object metadata transaction {} references missing bucket {}",
                transaction.transaction_id,
                bucket_id
            ));
        };
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!(
                "object metadata transaction {} stream scope does not match bucket tenant",
                transaction.transaction_id
            ));
        }
        let records = core_store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence: visible_sequence.saturating_sub(1),
                limit: 1,
            })
            .await?;
        let Some(record) = records.into_iter().find(|record| {
            record.sequence == *visible_sequence && &record.event_hash == prepared_record_hash
        }) else {
            return Err(anyhow!(
                "object metadata transaction {} committed stream record {stream_id}:{visible_sequence} is not readable",
                transaction.transaction_id
            ));
        };
        let metadata_record = metadata_record_from_stream_record(record)?;
        if !metadata_record.record_kind.is_object_version_like() {
            continue;
        }
        let object = object_from_body(&metadata_record.body)?;
        let mutation = ObjectJournalMutation::from_event_name(&metadata_record.body.event)?;
        materialize_object_metadata_projection(&core_store, &bucket, &object, mutation).await?;
        materialized.push(CommittedObjectMetadataProjection {
            bucket,
            object,
            event_type: mutation.event_name(),
            is_delete_marker: mutation.is_delete_marker(),
        });
    }
    Ok(materialized)
}

fn parse_object_metadata_stream_id(stream_id: &str) -> Option<(i64, i64)> {
    let rest = stream_id.strip_prefix("object_metadata:tenant:")?;
    let (tenant, bucket_part) = rest.split_once(":bucket:")?;
    let tenant_id = tenant.parse().ok()?;
    let bucket_id = bucket_part.parse().ok()?;
    Some((tenant_id, bucket_id))
}

#[derive(Debug, Clone)]
pub struct CommittedObjectMetadataProjection {
    pub bucket: Bucket,
    pub object: Object,
    pub event_type: &'static str,
    pub is_delete_marker: bool,
}
