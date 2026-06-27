use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

const SEGMENT_BODY_HEADER_LEN: usize = 20;
const DATA_BLOCK_HEADER_LEN: usize = 84;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockCodec {
    None = 0,
    Zstd = 1,
}

impl BlockCodec {
    fn from_u8(value: u8) -> Result<Self, FormatError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            other => Err(FormatError::UnsupportedCodec(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub value_hash: Hash32,
}

impl SegmentRecord {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let value_hash = hash32(&value);
        Self {
            key,
            value,
            value_hash,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.key.len() + 4 + self.value.len() + 32);
        out.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.key);
        out.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.value);
        out.extend_from_slice(&self.value_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < 4 {
            return Err(FormatError::TooShort {
                context: "segment record key length",
                needed: 4,
                actual: input.len(),
            });
        }
        let key_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let value_len_offset =
            4usize
                .checked_add(key_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "segment record key",
                })?;
        if input.len() < value_len_offset + 4 {
            return Err(FormatError::TooShort {
                context: "segment record value length",
                needed: value_len_offset + 4,
                actual: input.len(),
            });
        }
        let value_len = u32::from_le_bytes(
            input[value_len_offset..value_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let value_start = value_len_offset + 4;
        let value_end =
            value_start
                .checked_add(value_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "segment record value",
                })?;
        let record_end = value_end
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "segment record hash",
            })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "segment record",
                needed: record_end,
                actual: input.len(),
            });
        }
        let key = input[4..value_len_offset].to_vec();
        let value = input[value_start..value_end].to_vec();
        let value_hash: Hash32 = input[value_end..record_end].try_into().unwrap();
        if hash32(&value) != value_hash {
            return Err(FormatError::HashMismatch {
                context: "segment record value",
            });
        }
        Ok((
            Self {
                key,
                value,
                value_hash,
            },
            record_end,
        ))
    }

    pub fn encode_many(records: &[Self]) -> Result<Vec<u8>, FormatError> {
        ensure_sorted(records)?;
        let encoded_len = records.iter().map(|record| record.encode().len()).sum();
        let mut out = Vec::with_capacity(encoded_len);
        for record in records {
            out.extend_from_slice(&record.encode());
        }
        Ok(out)
    }

    pub fn decode_many(mut input: &[u8], expected_count: u32) -> Result<Vec<Self>, FormatError> {
        let mut records = Vec::with_capacity(expected_count as usize);
        for _ in 0..expected_count {
            let (record, used) = Self::decode(input)?;
            records.push(record);
            input = &input[used..];
        }
        if !input.is_empty() {
            return Err(FormatError::InvalidDeclaredLength {
                context: "segment record trailing bytes",
            });
        }
        ensure_sorted(&records)?;
        Ok(records)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBlock {
    pub compressed_len: u32,
    pub uncompressed_len: u32,
    pub record_count: u32,
    pub first_key_hash: Hash32,
    pub last_key_hash: Hash32,
    pub codec: BlockCodec,
    pub compressed_records: Vec<u8>,
    pub block_hash: Hash32,
}

impl DataBlock {
    pub fn from_uncompressed_records(records: &[SegmentRecord]) -> Result<Self, FormatError> {
        ensure_sorted(records)?;
        let uncompressed_records = SegmentRecord::encode_many(records)?;
        let first_key_hash = records
            .first()
            .map(|record| hash32(&record.key))
            .unwrap_or([0; 32]);
        let last_key_hash = records
            .last()
            .map(|record| hash32(&record.key))
            .unwrap_or([0; 32]);
        Ok(Self::new(
            BlockCodec::None,
            uncompressed_records.clone(),
            uncompressed_records.len() as u32,
            records.len() as u32,
            first_key_hash,
            last_key_hash,
        ))
    }

    pub fn new(
        codec: BlockCodec,
        compressed_records: Vec<u8>,
        uncompressed_len: u32,
        record_count: u32,
        first_key_hash: Hash32,
        last_key_hash: Hash32,
    ) -> Self {
        let compressed_len = compressed_records.len() as u32;
        let mut block = Self {
            compressed_len,
            uncompressed_len,
            record_count,
            first_key_hash,
            last_key_hash,
            codec,
            compressed_records,
            block_hash: [0; 32],
        };
        block.block_hash = hash32(&block.bytes_without_hash());
        block
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.bytes_without_hash();
        out.extend_from_slice(&self.block_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < DATA_BLOCK_HEADER_LEN + 32 {
            return Err(FormatError::TooShort {
                context: "data block",
                needed: DATA_BLOCK_HEADER_LEN + 32,
                actual: input.len(),
            });
        }
        let compressed_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let record_end = DATA_BLOCK_HEADER_LEN.checked_add(compressed_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "data block records",
            },
        )?;
        let block_end = record_end
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "data block hash",
            })?;
        if input.len() < block_end {
            return Err(FormatError::TooShort {
                context: "data block records",
                needed: block_end,
                actual: input.len(),
            });
        }
        let block_hash: Hash32 = input[record_end..block_end].try_into().unwrap();
        if hash32(&input[..record_end]) != block_hash {
            return Err(FormatError::HashMismatch {
                context: "data block",
            });
        }
        let data_block = Self {
            compressed_len: compressed_len as u32,
            uncompressed_len: u32::from_le_bytes(input[4..8].try_into().unwrap()),
            record_count: u32::from_le_bytes(input[8..12].try_into().unwrap()),
            first_key_hash: input[12..44].try_into().unwrap(),
            last_key_hash: input[44..76].try_into().unwrap(),
            codec: BlockCodec::from_u8(input[76])?,
            compressed_records: input[DATA_BLOCK_HEADER_LEN..record_end].to_vec(),
            block_hash,
        };
        if input[77..84] != [0; 7] {
            return Err(FormatError::InvalidDeclaredLength {
                context: "data block reserved bytes",
            });
        }
        Ok((data_block, block_end))
    }

    pub fn decode_uncompressed_records(&self) -> Result<Vec<SegmentRecord>, FormatError> {
        match self.codec {
            BlockCodec::None => {
                if self.uncompressed_len != self.compressed_records.len() as u32 {
                    return Err(FormatError::InvalidDeclaredLength {
                        context: "uncompressed data block length",
                    });
                }
                SegmentRecord::decode_many(&self.compressed_records, self.record_count)
            }
            BlockCodec::Zstd => Err(FormatError::UnsupportedCodec(self.codec as u8)),
        }
    }

    fn bytes_without_hash(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(DATA_BLOCK_HEADER_LEN + self.compressed_records.len());
        out.extend_from_slice(&self.compressed_len.to_le_bytes());
        out.extend_from_slice(&self.uncompressed_len.to_le_bytes());
        out.extend_from_slice(&self.record_count.to_le_bytes());
        out.extend_from_slice(&self.first_key_hash);
        out.extend_from_slice(&self.last_key_hash);
        out.push(self.codec as u8);
        out.extend_from_slice(&[0; 7]);
        out.extend_from_slice(&self.compressed_records);
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockIndexRecord {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub block_offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
    pub record_count: u32,
    pub block_hash: Hash32,
}

impl BlockIndexRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            2 + 2 + 8 + 4 + 4 + 4 + self.first_key.len() + self.last_key.len() + 32,
        );
        out.extend_from_slice(&(self.first_key.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.last_key.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.block_offset.to_le_bytes());
        out.extend_from_slice(&self.compressed_len.to_le_bytes());
        out.extend_from_slice(&self.uncompressed_len.to_le_bytes());
        out.extend_from_slice(&self.record_count.to_le_bytes());
        out.extend_from_slice(&self.first_key);
        out.extend_from_slice(&self.last_key);
        out.extend_from_slice(&self.block_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < 24 {
            return Err(FormatError::TooShort {
                context: "block index record",
                needed: 24,
                actual: input.len(),
            });
        }
        let first_key_len = u16::from_le_bytes(input[0..2].try_into().unwrap()) as usize;
        let last_key_len = u16::from_le_bytes(input[2..4].try_into().unwrap()) as usize;
        let first_key_start: usize = 24;
        let last_key_start = first_key_start.checked_add(first_key_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "block index first key",
            },
        )?;
        let hash_start =
            last_key_start
                .checked_add(last_key_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "block index last key",
                })?;
        let record_end = hash_start
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "block index hash",
            })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "block index record",
                needed: record_end,
                actual: input.len(),
            });
        }
        Ok((
            Self {
                first_key: input[first_key_start..last_key_start].to_vec(),
                last_key: input[last_key_start..hash_start].to_vec(),
                block_offset: u64::from_le_bytes(input[4..12].try_into().unwrap()),
                compressed_len: u32::from_le_bytes(input[12..16].try_into().unwrap()),
                uncompressed_len: u32::from_le_bytes(input[16..20].try_into().unwrap()),
                record_count: u32::from_le_bytes(input[20..24].try_into().unwrap()),
                block_hash: input[hash_start..record_end].try_into().unwrap(),
            },
            record_end,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentBody {
    pub block_index_offset: u64,
    pub bloom_offset: u64,
    pub data_blocks: Vec<DataBlock>,
    pub block_index: Vec<BlockIndexRecord>,
    pub bloom_blocks: Vec<u8>,
}

impl SegmentBody {
    pub fn new(
        data_blocks: Vec<DataBlock>,
        block_index: Vec<BlockIndexRecord>,
        bloom_blocks: Vec<u8>,
    ) -> Self {
        let data_len: usize = data_blocks.iter().map(|block| block.encode().len()).sum();
        let index_len: usize = block_index.iter().map(|record| record.encode().len()).sum();
        let block_index_offset = (SEGMENT_BODY_HEADER_LEN + data_len) as u64;
        let bloom_offset = (SEGMENT_BODY_HEADER_LEN + data_len + index_len) as u64;
        Self {
            block_index_offset,
            bloom_offset,
            data_blocks,
            block_index,
            bloom_blocks,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            SEGMENT_BODY_HEADER_LEN
                + self
                    .data_blocks
                    .iter()
                    .map(|block| block.encode().len())
                    .sum::<usize>()
                + self
                    .block_index
                    .iter()
                    .map(|record| record.encode().len())
                    .sum::<usize>()
                + self.bloom_blocks.len(),
        );
        out.extend_from_slice(&(self.data_blocks.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.block_index_offset.to_le_bytes());
        out.extend_from_slice(&self.bloom_offset.to_le_bytes());
        for block in &self.data_blocks {
            out.extend_from_slice(&block.encode());
        }
        for index_record in &self.block_index {
            out.extend_from_slice(&index_record.encode());
        }
        out.extend_from_slice(&self.bloom_blocks);
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < SEGMENT_BODY_HEADER_LEN {
            return Err(FormatError::TooShort {
                context: "segment body header",
                needed: SEGMENT_BODY_HEADER_LEN,
                actual: input.len(),
            });
        }
        let block_count = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let block_index_offset = u64::from_le_bytes(input[4..12].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(input[12..20].try_into().unwrap()) as usize;
        if block_index_offset < SEGMENT_BODY_HEADER_LEN
            || bloom_offset < block_index_offset
            || input.len() < bloom_offset
        {
            return Err(FormatError::InvalidDeclaredLength {
                context: "segment body offsets",
            });
        }

        let mut data_cursor = SEGMENT_BODY_HEADER_LEN;
        let mut data_blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            let (block, used) = DataBlock::decode(&input[data_cursor..block_index_offset])?;
            data_cursor += used;
            data_blocks.push(block);
        }
        if data_cursor != block_index_offset {
            return Err(FormatError::InvalidDeclaredLength {
                context: "segment body data blocks",
            });
        }

        let mut index_cursor = block_index_offset;
        let mut block_index = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            let (record, used) = BlockIndexRecord::decode(&input[index_cursor..bloom_offset])?;
            index_cursor += used;
            block_index.push(record);
        }
        if index_cursor != bloom_offset {
            return Err(FormatError::InvalidDeclaredLength {
                context: "segment body block index",
            });
        }

        Ok(Self {
            block_index_offset: block_index_offset as u64,
            bloom_offset: bloom_offset as u64,
            data_blocks,
            block_index,
            bloom_blocks: input[bloom_offset..].to_vec(),
        })
    }
}

fn ensure_sorted(records: &[SegmentRecord]) -> Result<(), FormatError> {
    if records.windows(2).any(|pair| pair[0].key > pair[1].key) {
        return Err(FormatError::RecordsNotSorted);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_record_round_trip_checks_hash_and_sort_order() {
        let first = SegmentRecord::new(b"a".to_vec(), br#"{"v":1}"#.to_vec());
        let second = SegmentRecord::new(b"b".to_vec(), br#"{"v":2}"#.to_vec());
        let encoded = SegmentRecord::encode_many(&[first.clone(), second.clone()]).unwrap();
        let decoded = SegmentRecord::decode_many(&encoded, 2).unwrap();
        assert_eq!(decoded, vec![first, second]);

        let unsorted = vec![decoded[1].clone(), decoded[0].clone()];
        assert_eq!(
            SegmentRecord::encode_many(&unsorted).unwrap_err(),
            FormatError::RecordsNotSorted
        );
    }

    #[test]
    fn data_block_round_trip_checks_block_hash() {
        let records = vec![
            SegmentRecord::new(b"bucket/a".to_vec(), b"first".to_vec()),
            SegmentRecord::new(b"bucket/b".to_vec(), b"second".to_vec()),
        ];
        let block = DataBlock::from_uncompressed_records(&records).unwrap();
        let encoded = block.encode();
        let (decoded, used) = DataBlock::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, block);
        assert_eq!(decoded.decode_uncompressed_records().unwrap(), records);

        let mut corrupted = encoded;
        corrupted[DATA_BLOCK_HEADER_LEN] ^= 1;
        assert_eq!(
            DataBlock::decode(&corrupted).unwrap_err(),
            FormatError::HashMismatch {
                context: "data block"
            }
        );
    }

    #[test]
    fn segment_body_round_trip_preserves_offsets() {
        let records = vec![
            SegmentRecord::new(b"k1".to_vec(), b"v1".to_vec()),
            SegmentRecord::new(b"k2".to_vec(), b"v2".to_vec()),
        ];
        let block = DataBlock::from_uncompressed_records(&records).unwrap();
        let index = BlockIndexRecord {
            first_key: records[0].key.clone(),
            last_key: records[1].key.clone(),
            block_offset: SEGMENT_BODY_HEADER_LEN as u64,
            compressed_len: block.compressed_len,
            uncompressed_len: block.uncompressed_len,
            record_count: block.record_count,
            block_hash: block.block_hash,
        };
        let body = SegmentBody::new(vec![block], vec![index], b"bloom".to_vec());
        let encoded = body.encode();
        let decoded = SegmentBody::decode(&encoded).unwrap();
        assert_eq!(decoded, body);
    }
}
