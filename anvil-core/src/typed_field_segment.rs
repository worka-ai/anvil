use crate::{
    core_store::{
        CompareAndSwapRef, CoreObjectRef, CoreStore, EncodedTypedValue, GetBlob, PutBlob, SourceId,
        TypedFieldValue,
    },
    formats::{
        BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
        Hash32, hash32,
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const TYPED_FIELD_SEGMENT_REF_PREFIX: &str = "typed_field_segment:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";
const TYPED_FIELD_BODY_MAGIC: &[u8; 8] = b"ANVTFRW1";
const TYPED_FIELD_BODY_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TypedFieldSegmentHeader {
    pub index_id: String,
    pub generation: u64,
    pub source_kind: String,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub definition_hash: String,
    pub row_count: u64,
    pub field_names: Vec<String>,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TypedFieldSegmentRow {
    pub object_key: String,
    pub object_version_id: String,
    pub source_identity: String,
    #[serde(default)]
    pub values: BTreeMap<String, JsonValue>,
    #[serde(default)]
    pub encoded_values: BTreeMap<String, Vec<u8>>,
    #[serde(default)]
    pub source_id_binary: Vec<u8>,
    #[serde(default)]
    pub value_flags: u32,
    pub authz_label_hash: String,
    pub authz_revision: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedTypedFieldSegment {
    pub header: TypedFieldSegmentHeader,
    pub rows: Vec<TypedFieldSegmentRow>,
}

#[derive(Debug, Clone)]
pub struct TypedFieldSegmentWrite<'a> {
    pub index_id: &'a str,
    pub generation: u64,
    pub source_kind: &'a str,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub definition_hash: &'a str,
    pub field_names: &'a [String],
    pub rows: &'a [TypedFieldSegmentRow],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct StoredFields {
    object_key: String,
    object_version_id: String,
    source_identity: String,
    values: BTreeMap<String, JsonValue>,
    authz_label_hash: String,
    authz_revision: u64,
}

pub async fn write_typed_field_segment(
    storage: &Storage,
    input: TypedFieldSegmentWrite<'_>,
) -> Result<String> {
    validate_hex32(input.definition_hash, "typed field definition hash")?;
    let mut rows = input.rows.to_vec();
    rows.sort_by(|left, right| left.source_identity.cmp(&right.source_identity));
    for row in &mut rows {
        if row.encoded_values.is_empty() {
            row.encoded_values = encode_row_values(&row.values)?;
        }
        if row.source_id_binary.is_empty() {
            row.source_id_binary = row.source_identity.as_bytes().to_vec();
        }
    }

    let body = encode_typed_field_body(input.field_names, &rows)?;
    let segment_hash = hash32(&body);
    let ref_name =
        typed_field_segment_ref_name(input.index_id, input.generation, &hex::encode(segment_hash))?;

    let header = TypedFieldSegmentHeader {
        index_id: input.index_id.to_string(),
        generation: input.generation,
        source_kind: input.source_kind.to_string(),
        source_cursor: input.source_cursor,
        authz_revision: input.authz_revision,
        definition_hash: input.definition_hash.to_string(),
        row_count: rows.len() as u64,
        field_names: input.field_names.to_vec(),
        codec: "typed-row-binary-v1".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(FileFamily::TypedFieldSegment, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let (first_hash, last_hash) = source_identity_hash_bounds(&rows);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        rows.len() as u64,
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
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!(
                "typed-field-segment:{}:{}",
                input.index_id, input.generation
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

pub async fn read_typed_field_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedTypedFieldSegment> {
    let bytes = read_typed_field_segment_bytes(storage, segment_ref).await?;
    decode_typed_field_segment(&bytes)
}

pub async fn read_typed_field_segment_bytes(
    storage: &Storage,
    segment_ref: &str,
) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(segment_ref)
        .await?
        .ok_or_else(|| anyhow!("typed field segment ref is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await
}

pub async fn read_latest_typed_field_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedTypedFieldSegment>> {
    let Some(segment_ref) = latest_typed_field_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_typed_field_segment(storage, &segment_ref).await?))
}

pub async fn latest_typed_field_segment_ref(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<String>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut refs = store
        .list_ref_names(&typed_field_segment_ref_prefix(index_id)?)
        .await?;
    refs.sort_by_key(|value| generation_from_ref(value).unwrap_or(0));
    Ok(refs.pop())
}

pub fn decode_typed_field_segment(bytes: &[u8]) -> Result<DecodedTypedFieldSegment> {
    let envelope = BinaryEnvelopeHeader::decode(bytes)?;
    if envelope.family != FileFamily::TypedFieldSegment {
        return Err(anyhow!("typed field segment file family mismatch"));
    }
    if bytes.len() < COMMON_FOOTER_LEN {
        return Err(anyhow!("typed field segment is shorter than footer"));
    }
    let header_end = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("typed field segment header length overflow"))?;
    let footer_start = bytes
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("typed field segment footer length overflow"))?;
    if footer_start < header_end {
        return Err(anyhow!("typed field segment body overlaps header"));
    }
    let encoded_header = &bytes[..header_end];
    let body = &bytes[header_end..footer_start];
    let footer = BinaryFileFooter::decode(&bytes[footer_start..])?;
    footer.verify(encoded_header, body)?;
    let header: TypedFieldSegmentHeader = serde_json::from_slice(&envelope.header_json)?;
    if header.codec != "typed-row-binary-v1" {
        return Err(anyhow!(
            "unsupported typed field segment codec {}",
            header.codec
        ));
    }
    let rows =
        decode_typed_field_body(&header.field_names, body).context("decode typed field rows")?;
    if rows.len() as u64 != header.row_count {
        return Err(anyhow!("typed field segment row count mismatch"));
    }
    Ok(DecodedTypedFieldSegment { header, rows })
}

pub fn encode_row_values(
    values: &BTreeMap<String, JsonValue>,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut encoded = BTreeMap::new();
    for (field, value) in values {
        let typed = json_value_to_typed_field_value(value)?;
        encoded.insert(
            field.clone(),
            EncodedTypedValue::for_ordered_value(&typed, false)?.bytes,
        );
    }
    Ok(encoded)
}

pub fn source_id_binary(source_id: &SourceId) -> Result<Vec<u8>> {
    source_id.encode_binary()
}

fn encode_typed_field_body(
    field_names: &[String],
    rows: &[TypedFieldSegmentRow],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(TYPED_FIELD_BODY_MAGIC);
    out.extend_from_slice(&TYPED_FIELD_BODY_VERSION.to_le_bytes());
    push_u64(&mut out, rows.len() as u64);
    for row in rows {
        encode_typed_field_row(&mut out, field_names, row)?;
    }
    Ok(out)
}

fn encode_typed_field_row(
    out: &mut Vec<u8>,
    field_names: &[String],
    row: &TypedFieldSegmentRow,
) -> Result<()> {
    let row_start = out.len();
    let key_count =
        u16::try_from(field_names.len()).map_err(|_| anyhow!("too many typed fields"))?;
    out.extend_from_slice(&key_count.to_le_bytes());
    for field in field_names {
        let encoded = row
            .encoded_values
            .get(field)
            .cloned()
            .unwrap_or_else(|| vec![0x01]);
        push_len_bytes(out, &encoded)?;
    }
    push_len_bytes(out, &row.source_id_binary)?;
    out.extend_from_slice(&row.value_flags.to_le_bytes());
    let stored = StoredFields {
        object_key: row.object_key.clone(),
        object_version_id: row.object_version_id.clone(),
        source_identity: row.source_identity.clone(),
        values: row.values.clone(),
        authz_label_hash: row.authz_label_hash.clone(),
        authz_revision: row.authz_revision,
    };
    let stored_json = serde_json::to_vec(&stored)?;
    push_len_bytes(out, &stored_json)?;
    let row_hash = Sha256::digest(&out[row_start..]);
    out.extend_from_slice(&row_hash);
    Ok(())
}

fn decode_typed_field_body(
    field_names: &[String],
    input: &[u8],
) -> Result<Vec<TypedFieldSegmentRow>> {
    let mut cursor = ByteCursor::new(input);
    if cursor.read_bytes(TYPED_FIELD_BODY_MAGIC.len())? != TYPED_FIELD_BODY_MAGIC {
        bail!("typed field body magic mismatch");
    }
    let version = cursor.read_u16()?;
    if version != TYPED_FIELD_BODY_VERSION {
        bail!("unsupported typed field body version {version}");
    }
    let row_count = cursor.read_u64()?;
    let mut rows = Vec::with_capacity(usize::try_from(row_count.min(1_000_000)).unwrap_or(1024));
    for _ in 0..row_count {
        rows.push(decode_typed_field_row(field_names, &mut cursor)?);
    }
    if !cursor.is_empty() {
        bail!("typed field body has trailing bytes");
    }
    Ok(rows)
}

fn decode_typed_field_row(
    field_names: &[String],
    cursor: &mut ByteCursor<'_>,
) -> Result<TypedFieldSegmentRow> {
    let row_start = cursor.position();
    let key_count = cursor.read_u16()? as usize;
    if key_count != field_names.len() {
        bail!("typed field row key count mismatch");
    }
    let mut encoded_values = BTreeMap::new();
    for field in field_names {
        encoded_values.insert(field.clone(), cursor.read_len_bytes()?.to_vec());
    }
    let source_id_binary = cursor.read_len_bytes()?.to_vec();
    let value_flags = cursor.read_u32()?;
    let stored_json = cursor.read_len_bytes()?.to_vec();
    let row_hash_offset = cursor.position();
    let expected_hash = cursor.read_bytes(32)?;
    let actual_hash = Sha256::digest(&cursor.input[row_start..row_hash_offset]);
    if expected_hash != &actual_hash[..] {
        bail!("typed field row hash mismatch");
    }
    let stored: StoredFields = serde_json::from_slice(&stored_json)?;
    Ok(TypedFieldSegmentRow {
        object_key: stored.object_key,
        object_version_id: stored.object_version_id,
        source_identity: stored.source_identity,
        values: stored.values,
        encoded_values,
        source_id_binary,
        value_flags,
        authz_label_hash: stored.authz_label_hash,
        authz_revision: stored.authz_revision,
    })
}

fn json_value_to_typed_field_value(value: &JsonValue) -> Result<TypedFieldValue> {
    match value {
        JsonValue::Null => Ok(TypedFieldValue::Null),
        JsonValue::Bool(value) => Ok(TypedFieldValue::Bool(*value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(TypedFieldValue::Int64(value))
            } else if let Some(value) = value.as_u64() {
                Ok(TypedFieldValue::Uint64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(TypedFieldValue::Float64(value))
            } else {
                bail!("unsupported JSON number for typed field encoding")
            }
        }
        JsonValue::String(value) => Ok(TypedFieldValue::String(value.clone())),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Ok(TypedFieldValue::String(value.to_string()))
        }
    }
}

struct ByteCursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn position(&self) -> usize {
        self.offset
    }

    fn is_empty(&self) -> bool {
        self.offset == self.input.len()
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_len_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_u32()? as usize;
        self.read_bytes(len)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| anyhow!("typed field segment offset overflow"))?;
        if end > self.input.len() {
            bail!("typed field segment truncated");
        }
        let bytes = &self.input[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_len_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| anyhow!("typed field value too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn typed_field_segment_ref_name(
    index_id: &str,
    generation: u64,
    segment_hash: &str,
) -> Result<String> {
    validate_hex32(segment_hash, "typed field segment hash")?;
    Ok(format!(
        "{}{}:{}:{}",
        TYPED_FIELD_SEGMENT_REF_PREFIX,
        URL_SAFE_NO_PAD.encode(index_id.as_bytes()),
        generation,
        segment_hash
    ))
}

fn typed_field_segment_ref_prefix(index_id: &str) -> Result<String> {
    Ok(format!(
        "{}{}:",
        TYPED_FIELD_SEGMENT_REF_PREFIX,
        URL_SAFE_NO_PAD.encode(index_id.as_bytes())
    ))
}

fn generation_from_ref(value: &str) -> Option<u64> {
    value.rsplit(':').nth(1)?.parse().ok()
}

fn source_identity_hash_bounds(rows: &[TypedFieldSegmentRow]) -> (Hash32, Hash32) {
    let first = rows
        .first()
        .map(|row| hash32(row.source_identity.as_bytes()))
        .unwrap_or([0u8; 32]);
    let last = rows
        .last()
        .map(|row| hash32(row.source_identity.as_bytes()))
        .unwrap_or([0u8; 32]);
    (first, last)
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    let bytes = serde_json::to_vec(object_ref)?;
    Ok(format!(
        "{}{}",
        CORE_OBJECT_REF_TARGET_PREFIX,
        URL_SAFE_NO_PAD.encode(bytes)
    ))
}

fn decode_core_object_ref_target(value: &str) -> Result<CoreObjectRef> {
    let encoded = value
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn validate_hex32(value: &str, label: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{label} must be 32 hex bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn typed_field_segment_round_trips_through_core_store() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let mut values = BTreeMap::new();
        values.insert(
            "status".to_string(),
            JsonValue::String("pending".to_string()),
        );
        values.insert("priority".to_string(), JsonValue::Number(10.into()));
        let row = TypedFieldSegmentRow {
            object_key: "queue/item-1.json".to_string(),
            object_version_id: uuid::Uuid::new_v4().to_string(),
            source_identity: "queue/item-1.json#1".to_string(),
            encoded_values: encode_row_values(&values).unwrap(),
            source_id_binary: b"source-id".to_vec(),
            value_flags: 0,
            values,
            authz_label_hash: hex::encode([7u8; 32]),
            authz_revision: 9,
        };
        let definition_hash = blake3::hash(b"definition").to_hex().to_string();
        let segment_ref = write_typed_field_segment(
            &storage,
            TypedFieldSegmentWrite {
                index_id: "tenant:bucket:index",
                generation: 1,
                source_kind: "object_current",
                source_cursor: 12,
                authz_revision: 9,
                definition_hash: &definition_hash,
                field_names: &["status".to_string(), "priority".to_string()],
                rows: &[row.clone()],
            },
        )
        .await
        .unwrap();

        let decoded = read_typed_field_segment(&storage, &segment_ref)
            .await
            .unwrap();
        assert_eq!(decoded.header.index_id, "tenant:bucket:index");
        assert_eq!(decoded.header.source_cursor, 12);
        assert_eq!(decoded.header.codec, "typed-row-binary-v1");
        assert_eq!(decoded.rows, vec![row]);
        assert_eq!(
            latest_typed_field_segment_ref(&storage, "tenant:bucket:index")
                .await
                .unwrap(),
            Some(segment_ref)
        );
    }
}
