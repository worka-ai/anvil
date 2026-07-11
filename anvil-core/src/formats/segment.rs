use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

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
}
