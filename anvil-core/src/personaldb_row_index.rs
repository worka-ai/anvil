use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    Hash32, hash32, personaldb::RowIndexRecord,
};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbRowIndexHeader {
    pub tenant_id: String,
    pub database_id: String,
    pub generation: u64,
    pub source_hash: String,
    pub key_order: String,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPersonalDbRowIndex {
    pub header: PersonalDbRowIndexHeader,
    pub records: Vec<RowIndexRecord>,
}

#[derive(Debug, Clone)]
pub struct PersonalDbRowIndexWrite<'a> {
    pub tenant_id: i64,
    pub database_id: &'a str,
    pub generation: u64,
    pub source_hash: Hash32,
    pub records: &'a [RowIndexRecord],
}

pub async fn write_personaldb_row_index(
    storage: &Storage,
    input: PersonalDbRowIndexWrite<'_>,
) -> Result<PathBuf> {
    let mut records = input.records.to_vec();
    records.sort_by(compare_row_index_records);
    let body = encode_row_index_body(&records);
    let path = storage.personaldb_row_index_path(
        input.tenant_id,
        input.database_id,
        input.generation,
        &hex::encode(input.source_hash),
    )?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let header = PersonalDbRowIndexHeader {
        tenant_id: input.tenant_id.to_string(),
        database_id: input.database_id.to_string(),
        generation: input.generation,
        source_hash: hex::encode(input.source_hash),
        key_order: "database_id_table_hash_primary_key_hash".to_string(),
        codec: "none".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(FileFamily::PersonalDbRowIndex, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let (first_hash, last_hash) = record_hash_bounds(&records);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        records.len() as u64,
        first_hash,
        last_hash,
    );

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await
        .with_context(|| format!("create personaldb row index {}", path.display()))?;
    file.write_all(&encoded_header).await?;
    file.write_all(&body).await?;
    file.write_all(&footer.encode()).await?;
    file.sync_data().await?;
    Ok(path)
}

pub async fn read_personaldb_row_index(
    path: impl Into<PathBuf>,
) -> Result<DecodedPersonalDbRowIndex> {
    let path = path.into();
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read personaldb row index {}", path.display()))?;
    decode_personaldb_row_index(&bytes)
}

pub fn decode_personaldb_row_index(bytes: &[u8]) -> Result<DecodedPersonalDbRowIndex> {
    let envelope = BinaryEnvelopeHeader::decode(bytes)?;
    if envelope.family != FileFamily::PersonalDbRowIndex {
        return Err(anyhow!("personaldb row index file family mismatch"));
    }
    if bytes.len() < COMMON_FOOTER_LEN {
        return Err(anyhow!("personaldb row index is shorter than footer"));
    }
    let header_end = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("personaldb row index header length overflow"))?;
    let footer_start = bytes
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("personaldb row index footer length overflow"))?;
    if footer_start < header_end {
        return Err(anyhow!("personaldb row index body overlaps header"));
    }
    let encoded_header = &bytes[..header_end];
    let body = &bytes[header_end..footer_start];
    let footer = BinaryFileFooter::decode(&bytes[footer_start..])?;
    footer.verify(encoded_header, body)?;
    let header: PersonalDbRowIndexHeader = serde_json::from_slice(&envelope.header_json)?;
    let records = decode_row_index_body(body)?;
    ensure_sorted(&records)?;
    Ok(DecodedPersonalDbRowIndex { header, records })
}

fn encode_row_index_body(records: &[RowIndexRecord]) -> Vec<u8> {
    let len = records.iter().map(|record| record.encode().len()).sum();
    let mut out = Vec::with_capacity(len);
    for record in records {
        out.extend_from_slice(&record.encode());
    }
    out
}

fn decode_row_index_body(mut input: &[u8]) -> Result<Vec<RowIndexRecord>> {
    let mut records = Vec::new();
    while !input.is_empty() {
        let (record, used) = RowIndexRecord::decode(input)?;
        records.push(record);
        input = &input[used..];
    }
    Ok(records)
}

fn ensure_sorted(records: &[RowIndexRecord]) -> Result<()> {
    if records
        .windows(2)
        .all(|pair| compare_row_index_records(&pair[0], &pair[1]).is_le())
    {
        Ok(())
    } else {
        Err(anyhow!(
            "personaldb row index records are not sorted by database, table, and primary key"
        ))
    }
}

fn compare_row_index_records(left: &RowIndexRecord, right: &RowIndexRecord) -> std::cmp::Ordering {
    left.database_id
        .cmp(&right.database_id)
        .then_with(|| left.table_name_hash.cmp(&right.table_name_hash))
        .then_with(|| left.primary_key_hash.cmp(&right.primary_key_hash))
}

fn record_hash_bounds(records: &[RowIndexRecord]) -> (Hash32, Hash32) {
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

    fn row(primary_hash: u8) -> RowIndexRecord {
        RowIndexRecord::new(
            b"db-alpha".to_vec(),
            [3; 32],
            [primary_hash; 32],
            b"invoice".to_vec(),
            format!("invoice-{primary_hash}").into_bytes(),
            Vec::new(),
            b"user:alice".to_vec(),
            b"org:alpha".to_vec(),
            u64::from(primary_hash),
            9,
            [4; 32],
            1_717_000_000 + i64::from(primary_hash),
        )
    }

    #[tokio::test]
    async fn personaldb_row_index_round_trips_sorted_records() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let records = vec![row(9), row(1), row(5)];
        let path = write_personaldb_row_index(
            &storage,
            PersonalDbRowIndexWrite {
                tenant_id: 4,
                database_id: "db-alpha",
                generation: 12,
                source_hash: [7; 32],
                records: &records,
            },
        )
        .await
        .unwrap();
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".rowidx"))
        );

        let decoded = read_personaldb_row_index(path).await.unwrap();
        assert_eq!(decoded.header.tenant_id, "4");
        assert_eq!(decoded.header.database_id, "db-alpha");
        assert_eq!(decoded.header.generation, 12);
        assert_eq!(decoded.records.len(), 3);
        assert_eq!(decoded.records[0].primary_key_hash, [1; 32]);
        assert_eq!(decoded.records[2].primary_key_hash, [9; 32]);
    }

    #[tokio::test]
    async fn personaldb_row_index_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let path = write_personaldb_row_index(
            &storage,
            PersonalDbRowIndexWrite {
                tenant_id: 4,
                database_id: "db-alpha",
                generation: 12,
                source_hash: [7; 32],
                records: &[row(1)],
            },
        )
        .await
        .unwrap();
        let mut bytes = tokio::fs::read(path).await.unwrap();
        bytes[COMMON_HEADER_LEN + 1] ^= 1;
        assert!(decode_personaldb_row_index(&bytes).is_err());
    }

    #[test]
    fn personaldb_row_index_rejects_unsorted_body() {
        let records = vec![row(9), row(1)];
        let body = encode_row_index_body(&records);
        assert!(decode_row_index_body(&body).is_ok());
        assert!(ensure_sorted(&records).is_err());
    }
}
