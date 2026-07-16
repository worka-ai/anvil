use anyhow::{Result, bail};
use prost::Message;

use super::types::StreamRecord;

#[derive(Clone, PartialEq, Message)]
struct StreamEventHashInputProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    stream_id: String,
    #[prost(string, tag = "3")]
    partition_id: String,
    #[prost(uint64, tag = "4")]
    sequence: u64,
    #[prost(string, tag = "5")]
    cursor: String,
    #[prost(string, tag = "6")]
    previous_event_hash: String,
    #[prost(string, tag = "7")]
    record_kind: String,
    #[prost(string, tag = "8")]
    payload_hash: String,
    #[prost(string, optional, tag = "9")]
    transaction_id: Option<String>,
    #[prost(string, optional, tag = "10")]
    idempotency_key_hash: Option<String>,
    #[prost(string, tag = "11")]
    created_at: String,
    #[prost(string, optional, tag = "12")]
    content_type: Option<String>,
    #[prost(string, tag = "13")]
    user_metadata_json: String,
}

pub(in crate::core_store) fn encode_stream_event_hash_input(
    record: &StreamRecord,
) -> Result<Vec<u8>> {
    let proto = StreamEventHashInputProto {
        schema: record.schema.clone(),
        stream_id: record.stream_id.clone(),
        partition_id: record.partition_id.clone(),
        sequence: record.sequence,
        cursor: record.cursor.clone(),
        previous_event_hash: record.previous_event_hash.clone(),
        record_kind: record.record_kind.clone(),
        payload_hash: record.payload_hash.clone(),
        transaction_id: record.transaction_id.clone(),
        idempotency_key_hash: record.idempotency_key_hash.clone(),
        created_at: record.created_at.clone(),
        content_type: record.content_type.clone(),
        user_metadata_json: record.user_metadata_json.clone(),
    };
    let bytes = proto.encode_to_vec();
    let decoded = StreamEventHashInputProto::decode(bytes.as_slice())?;
    if decoded.encode_to_vec() != bytes {
        bail!("CoreStore stream event hash input is not deterministic protobuf");
    }
    Ok(bytes)
}
