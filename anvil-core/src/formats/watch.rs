use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

const WATCH_RECORD_HEADER_LEN: usize = 16 + 2 + 32 + 16 + 2 + 8 + 8 + 8 + 32 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRecord {
    pub cursor: u128,
    pub partition_family: u16,
    pub partition_id: Hash32,
    pub mutation_id: [u8; 16],
    pub record_kind: u16,
    pub authz_revision: u64,
    pub index_generation: u64,
    pub personaldb_log_index: u64,
    pub payload_hash: Hash32,
    pub payload: Vec<u8>,
    pub record_hash: Hash32,
}

impl WatchRecord {
    pub fn new(
        cursor: u128,
        partition_family: u16,
        partition_id: Hash32,
        mutation_id: [u8; 16],
        record_kind: u16,
        authz_revision: u64,
        index_generation: u64,
        personaldb_log_index: u64,
        payload: Vec<u8>,
    ) -> Self {
        let mut record = Self {
            cursor,
            partition_family,
            partition_id,
            mutation_id,
            record_kind,
            authz_revision,
            index_generation,
            personaldb_log_index,
            payload_hash: hash32(&payload),
            payload,
            record_hash: [0; 32],
        };
        record.record_hash = hash32(&record.bytes_without_record_hash());
        record
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.bytes_without_record_hash();
        out.extend_from_slice(&self.record_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < WATCH_RECORD_HEADER_LEN + 32 {
            return Err(FormatError::TooShort {
                context: "watch record",
                needed: WATCH_RECORD_HEADER_LEN + 32,
                actual: input.len(),
            });
        }
        let payload_len = u32::from_le_bytes(input[124..128].try_into().unwrap()) as usize;
        let payload_start = WATCH_RECORD_HEADER_LEN;
        let hash_start =
            payload_start
                .checked_add(payload_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "watch record payload",
                })?;
        let record_end = hash_start
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "watch record hash",
            })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "watch record payload bytes",
                needed: record_end,
                actual: input.len(),
            });
        }
        let payload_hash = input[92..124].try_into().unwrap();
        let payload = input[payload_start..hash_start].to_vec();
        if hash32(&payload) != payload_hash {
            return Err(FormatError::HashMismatch {
                context: "watch record payload",
            });
        }
        let record_hash = input[hash_start..record_end].try_into().unwrap();
        if hash32(&input[..hash_start]) != record_hash {
            return Err(FormatError::HashMismatch {
                context: "watch record",
            });
        }
        Ok((
            Self {
                cursor: u128::from_le_bytes(input[0..16].try_into().unwrap()),
                partition_family: u16::from_le_bytes(input[16..18].try_into().unwrap()),
                partition_id: input[18..50].try_into().unwrap(),
                mutation_id: input[50..66].try_into().unwrap(),
                record_kind: u16::from_le_bytes(input[66..68].try_into().unwrap()),
                authz_revision: u64::from_le_bytes(input[68..76].try_into().unwrap()),
                index_generation: u64::from_le_bytes(input[76..84].try_into().unwrap()),
                personaldb_log_index: u64::from_le_bytes(input[84..92].try_into().unwrap()),
                payload_hash,
                payload,
                record_hash,
            },
            record_end,
        ))
    }

    fn bytes_without_record_hash(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(WATCH_RECORD_HEADER_LEN + self.payload.len());
        out.extend_from_slice(&self.cursor.to_le_bytes());
        out.extend_from_slice(&self.partition_family.to_le_bytes());
        out.extend_from_slice(&self.partition_id);
        out.extend_from_slice(&self.mutation_id);
        out.extend_from_slice(&self.record_kind.to_le_bytes());
        out.extend_from_slice(&self.authz_revision.to_le_bytes());
        out.extend_from_slice(&self.index_generation.to_le_bytes());
        out.extend_from_slice(&self.personaldb_log_index.to_le_bytes());
        out.extend_from_slice(&self.payload_hash);
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_record_round_trip_checks_payload_and_record_hashes() {
        let record = WatchRecord::new(
            100,
            1,
            [2; 32],
            [3; 16],
            6,
            7,
            8,
            9,
            br#"{"event":"object_version"}"#.to_vec(),
        );
        let encoded = record.encode();
        let (decoded, used) = WatchRecord::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, record);

        let mut corrupted = encoded;
        corrupted[WATCH_RECORD_HEADER_LEN] ^= 1;
        assert_eq!(
            WatchRecord::decode(&corrupted).unwrap_err(),
            FormatError::HashMismatch {
                context: "watch record payload"
            }
        );
    }
}
