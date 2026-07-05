use crate::core_store::{CompareAndSwapRef, CoreObjectRef, CoreStore, GetBlob, PutBlob};
use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    Hash32, hash32,
    personaldb::{PersonalDbLogRecord, validate_personaldb_log_chain},
};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

const PERSONALDB_LOG_SEGMENT_REF_PREFIX: &str = "personaldb_log_segment:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbLogSegmentHeader {
    pub tenant_id: String,
    pub database_id: String,
    pub start_log_index: u64,
    pub end_log_index: u64,
    pub base_log_hash: String,
    pub policy_epoch: u64,
    pub membership_epoch: u64,
    pub schema_hash: String,
    pub source_fence_token: u64,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPersonalDbLogSegment {
    pub header: PersonalDbLogSegmentHeader,
    pub records: Vec<PersonalDbLogRecord>,
}

#[derive(Debug, Clone)]
pub struct PersonalDbLogSegmentWrite<'a> {
    pub tenant_id: i64,
    pub database_id: &'a str,
    pub schema_hash: Hash32,
    pub source_fence_token: u64,
    pub records: &'a [PersonalDbLogRecord],
}

pub async fn write_personaldb_log_segment(
    storage: &Storage,
    input: PersonalDbLogSegmentWrite<'_>,
) -> Result<String> {
    if input.source_fence_token == 0 {
        return Err(anyhow!(
            "personaldb log segment source fence token must be nonzero"
        ));
    }
    validate_log_segment_records(input.records)?;
    let start_log_index = input.records.first().expect("validated nonempty").log_index;
    let end_log_index = input.records.last().expect("validated nonempty").log_index;
    let base_log_hash = input
        .records
        .first()
        .expect("validated nonempty")
        .previous_log_hash;
    let policy_epoch = common_policy_epoch(input.records)?;
    let membership_epoch = common_membership_epoch(input.records)?;
    let body = encode_log_segment_body(input.records);
    let segment_hash = hash32(&body);
    let ref_name = personaldb_log_segment_ref_name(
        input.tenant_id,
        input.database_id,
        start_log_index,
        end_log_index,
        &hex::encode(segment_hash),
    )?;

    let header = PersonalDbLogSegmentHeader {
        tenant_id: input.tenant_id.to_string(),
        database_id: input.database_id.to_string(),
        start_log_index,
        end_log_index,
        base_log_hash: hex::encode(base_log_hash),
        policy_epoch,
        membership_epoch,
        schema_hash: hex::encode(input.schema_hash),
        source_fence_token: input.source_fence_token,
        codec: "none".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(FileFamily::PersonalDbLogSegment, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let (first_hash, last_hash) = record_hash_bounds(input.records);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        input.records.len() as u64,
        first_hash,
        last_hash,
    );

    let mut bytes = Vec::with_capacity(encoded_header.len() + body.len() + COMMON_FOOTER_LEN);
    bytes.extend_from_slice(&encoded_header);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes,
            region_id: "local".to_string(),
            mutation_id: format!(
                "personaldb-log-segment:{}:{}:{}:{}",
                input.tenant_id, input.database_id, start_log_index, end_log_index
            ),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.clone(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(ref_name)
}

pub async fn read_personaldb_log_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedPersonalDbLogSegment> {
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(segment_ref)
        .await?
        .ok_or_else(|| anyhow!("personaldb log segment ref is missing"))?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    decode_personaldb_log_segment(&bytes)
}

pub async fn list_personaldb_log_segment_refs(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<Vec<String>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut refs = store
        .list_ref_names(&personaldb_log_segment_ref_prefix(tenant_id, database_id)?)
        .await?;
    refs.sort();
    Ok(refs)
}

pub fn decode_personaldb_log_segment(bytes: &[u8]) -> Result<DecodedPersonalDbLogSegment> {
    let envelope = BinaryEnvelopeHeader::decode(bytes)?;
    if envelope.family != FileFamily::PersonalDbLogSegment {
        return Err(anyhow!("personaldb log segment file family mismatch"));
    }
    if bytes.len() < COMMON_FOOTER_LEN {
        return Err(anyhow!("personaldb log segment is shorter than footer"));
    }
    let header_end = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("personaldb log segment header length overflow"))?;
    let footer_start = bytes
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("personaldb log segment footer length overflow"))?;
    if footer_start < header_end {
        return Err(anyhow!("personaldb log segment body overlaps header"));
    }
    let encoded_header = &bytes[..header_end];
    let body = &bytes[header_end..footer_start];
    let footer = BinaryFileFooter::decode(&bytes[footer_start..])?;
    footer.verify(encoded_header, body)?;
    let header: PersonalDbLogSegmentHeader = serde_json::from_slice(&envelope.header_json)?;
    let records = decode_log_segment_body(body)?;
    validate_log_segment_records(&records)?;
    validate_header_matches_records(&header, &records)?;
    Ok(DecodedPersonalDbLogSegment { header, records })
}

fn encode_log_segment_body(records: &[PersonalDbLogRecord]) -> Vec<u8> {
    let len = records.iter().map(|record| record.encode().len()).sum();
    let mut out = Vec::with_capacity(len);
    for record in records {
        out.extend_from_slice(&record.encode());
    }
    out
}

fn decode_log_segment_body(mut input: &[u8]) -> Result<Vec<PersonalDbLogRecord>> {
    let mut records = Vec::new();
    while !input.is_empty() {
        let (record, used) = PersonalDbLogRecord::decode(input)?;
        records.push(record);
        input = &input[used..];
    }
    Ok(records)
}

fn validate_log_segment_records(records: &[PersonalDbLogRecord]) -> Result<()> {
    if records.is_empty() {
        return Err(anyhow!(
            "personaldb log segment must contain at least one record"
        ));
    }
    validate_personaldb_log_chain(records)?;
    let _ = common_policy_epoch(records)?;
    let _ = common_membership_epoch(records)?;
    Ok(())
}

fn personaldb_log_segment_ref_prefix(tenant_id: i64, database_id: &str) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "personaldb log segment tenant id must be nonnegative"
        ));
    }
    require_safe_component(database_id, "database_id")?;
    Ok(format!(
        "{PERSONALDB_LOG_SEGMENT_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:"
    ))
}

fn personaldb_log_segment_ref_name(
    tenant_id: i64,
    database_id: &str,
    start_log_index: u64,
    end_log_index: u64,
    segment_hash: &str,
) -> Result<String> {
    validate_hex32(segment_hash, "segment_hash")?;
    Ok(format!(
        "{}start:{start_log_index:020}:end:{end_log_index:020}:hash:{segment_hash}",
        personaldb_log_segment_ref_prefix(tenant_id, database_id)?
    ))
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty()
        || value == "."
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

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
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

fn validate_header_matches_records(
    header: &PersonalDbLogSegmentHeader,
    records: &[PersonalDbLogRecord],
) -> Result<()> {
    let first = records.first().expect("validated nonempty");
    let last = records.last().expect("validated nonempty");
    if header.start_log_index != first.log_index || header.end_log_index != last.log_index {
        return Err(anyhow!(
            "personaldb log segment header range does not match body"
        ));
    }
    if header.base_log_hash != hex::encode(first.previous_log_hash) {
        return Err(anyhow!(
            "personaldb log segment base hash does not match body"
        ));
    }
    if header.policy_epoch != common_policy_epoch(records)? {
        return Err(anyhow!(
            "personaldb log segment policy epoch does not match body"
        ));
    }
    if header.membership_epoch != common_membership_epoch(records)? {
        return Err(anyhow!(
            "personaldb log segment membership epoch does not match body"
        ));
    }
    Ok(())
}

fn common_policy_epoch(records: &[PersonalDbLogRecord]) -> Result<u64> {
    let expected = records.first().expect("validated nonempty").policy_epoch;
    if records.iter().all(|record| record.policy_epoch == expected) {
        Ok(expected)
    } else {
        Err(anyhow!(
            "personaldb log segment records span multiple policy epochs"
        ))
    }
}

fn common_membership_epoch(records: &[PersonalDbLogRecord]) -> Result<u64> {
    let expected = records
        .first()
        .expect("validated nonempty")
        .membership_epoch;
    if records
        .iter()
        .all(|record| record.membership_epoch == expected)
    {
        Ok(expected)
    } else {
        Err(anyhow!(
            "personaldb log segment records span multiple membership epochs"
        ))
    }
}

fn record_hash_bounds(records: &[PersonalDbLogRecord]) -> (Hash32, Hash32) {
    let first = records
        .first()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    let last = records
        .last()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn record(log_index: u64, previous_log_hash: Hash32) -> PersonalDbLogRecord {
        PersonalDbLogRecord::new(
            log_index,
            10,
            20,
            30,
            previous_log_hash,
            [1; 32],
            [2; 32],
            [3; 32],
            format!("log/payloads/by-index/{log_index:020}-payload.sqlite-changeset").into_bytes(),
            format!("log/certificates/{log_index:020}-cert.certificate.json").into_bytes(),
            format!(r#"{{"log_index":{log_index}}}"#).into_bytes(),
        )
    }

    #[tokio::test]
    async fn personaldb_log_segment_round_trips_with_common_envelope() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = record(1, [0; 32]);
        let second = record(2, first.entry_hash);
        let records = vec![first, second];

        let segment_ref = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 77,
                records: &records,
            },
        )
        .await
        .unwrap();
        assert!(segment_ref.starts_with("personaldb_log_segment:tenant:9:database:db-alpha:"));
        assert!(segment_ref.contains("start:00000000000000000001:end:00000000000000000002:"));

        let decoded = read_personaldb_log_segment(&storage, &segment_ref)
            .await
            .unwrap();
        assert_eq!(decoded.header.tenant_id, "9");
        assert_eq!(decoded.header.database_id, "db-alpha");
        assert_eq!(decoded.header.start_log_index, 1);
        assert_eq!(decoded.header.end_log_index, 2);
        assert_eq!(decoded.header.source_fence_token, 77);
        assert_eq!(decoded.records, records);
    }

    #[tokio::test]
    async fn personaldb_log_segment_rejects_non_contiguous_chain() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = record(1, [0; 32]);
        let second = record(3, first.entry_hash);
        let err = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 77,
                records: &[first, second],
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("log index is not contiguous"));
    }

    #[tokio::test]
    async fn personaldb_log_segment_rejects_zero_source_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let record = record(1, [0; 32]);

        let err = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 0,
                records: &[record],
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("source fence token must be nonzero")
        );
    }

    #[tokio::test]
    async fn personaldb_log_segment_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let record = record(1, [0; 32]);
        let segment_ref = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 77,
                records: &[record],
            },
        )
        .await
        .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let ref_value = store
            .read_ref(&segment_ref)
            .await
            .unwrap()
            .expect("segment ref exists");
        let mut bytes = store
            .get_blob(GetBlob {
                object_ref: decode_core_object_ref_target(&ref_value.target).unwrap(),
            })
            .await
            .unwrap();
        bytes[COMMON_HEADER_LEN + 1] ^= 1;
        assert!(decode_personaldb_log_segment(&bytes).is_err());
    }
}
