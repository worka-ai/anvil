use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

const PERSONALDB_LOG_FIXED_LEN: usize = 8 * 4 + 32 * 4 + 2 + 2 + 4;
const ROW_INDEX_FIXED_LEN: usize = 2 + 32 + 32 + 2 + 2 + 2 + 2 + 2 + 8 + 8 + 32 + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbLogRecord {
    pub log_index: u64,
    pub client_log_epoch: u64,
    pub membership_epoch: u64,
    pub policy_epoch: u64,
    pub previous_log_hash: Hash32,
    pub changeset_payload_hash: Hash32,
    pub verified_envelope_hash: Hash32,
    pub certificate_hash: Hash32,
    pub payload_ref: Vec<u8>,
    pub certificate_ref: Vec<u8>,
    pub inline_certificate_bytes: Vec<u8>,
    pub entry_hash: Hash32,
}

impl PersonalDbLogRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_index: u64,
        client_log_epoch: u64,
        membership_epoch: u64,
        policy_epoch: u64,
        previous_log_hash: Hash32,
        changeset_payload_hash: Hash32,
        verified_envelope_hash: Hash32,
        certificate_hash: Hash32,
        payload_ref: Vec<u8>,
        certificate_ref: Vec<u8>,
        inline_certificate_bytes: Vec<u8>,
    ) -> Self {
        let mut record = Self {
            log_index,
            client_log_epoch,
            membership_epoch,
            policy_epoch,
            previous_log_hash,
            changeset_payload_hash,
            verified_envelope_hash,
            certificate_hash,
            payload_ref,
            certificate_ref,
            inline_certificate_bytes,
            entry_hash: [0; 32],
        };
        record.entry_hash = hash32(&record.chain_hash_material());
        record
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.bytes_without_hash();
        out.extend_from_slice(&self.entry_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < PERSONALDB_LOG_FIXED_LEN + 32 {
            return Err(FormatError::TooShort {
                context: "personaldb log record",
                needed: PERSONALDB_LOG_FIXED_LEN + 32,
                actual: input.len(),
            });
        }
        let payload_ref_len = u16::from_le_bytes(input[160..162].try_into().unwrap()) as usize;
        let certificate_ref_len = u16::from_le_bytes(input[162..164].try_into().unwrap()) as usize;
        let inline_certificate_len =
            u32::from_le_bytes(input[164..168].try_into().unwrap()) as usize;
        let payload_ref_start = PERSONALDB_LOG_FIXED_LEN;
        let certificate_ref_start = payload_ref_start.checked_add(payload_ref_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "personaldb payload ref",
            },
        )?;
        let inline_certificate_start = certificate_ref_start
            .checked_add(certificate_ref_len)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "personaldb certificate ref",
            })?;
        let hash_start = inline_certificate_start
            .checked_add(inline_certificate_len)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "personaldb inline certificate",
            })?;
        let record_end = hash_start
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "personaldb entry hash",
            })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "personaldb log record bytes",
                needed: record_end,
                actual: input.len(),
            });
        }
        let entry_hash = input[hash_start..record_end].try_into().unwrap();
        let record = Self {
            log_index: u64::from_le_bytes(input[0..8].try_into().unwrap()),
            client_log_epoch: u64::from_le_bytes(input[8..16].try_into().unwrap()),
            membership_epoch: u64::from_le_bytes(input[16..24].try_into().unwrap()),
            policy_epoch: u64::from_le_bytes(input[24..32].try_into().unwrap()),
            previous_log_hash: input[32..64].try_into().unwrap(),
            changeset_payload_hash: input[64..96].try_into().unwrap(),
            verified_envelope_hash: input[96..128].try_into().unwrap(),
            certificate_hash: input[128..160].try_into().unwrap(),
            payload_ref: input[payload_ref_start..certificate_ref_start].to_vec(),
            certificate_ref: input[certificate_ref_start..inline_certificate_start].to_vec(),
            inline_certificate_bytes: input[inline_certificate_start..hash_start].to_vec(),
            entry_hash,
        };
        if hash32(&record.chain_hash_material()) != entry_hash {
            return Err(FormatError::HashMismatch {
                context: "personaldb log record",
            });
        }
        Ok((record, record_end))
    }

    fn bytes_without_hash(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            PERSONALDB_LOG_FIXED_LEN
                + self.payload_ref.len()
                + self.certificate_ref.len()
                + self.inline_certificate_bytes.len(),
        );
        out.extend_from_slice(&self.log_index.to_le_bytes());
        out.extend_from_slice(&self.client_log_epoch.to_le_bytes());
        out.extend_from_slice(&self.membership_epoch.to_le_bytes());
        out.extend_from_slice(&self.policy_epoch.to_le_bytes());
        out.extend_from_slice(&self.previous_log_hash);
        out.extend_from_slice(&self.changeset_payload_hash);
        out.extend_from_slice(&self.verified_envelope_hash);
        out.extend_from_slice(&self.certificate_hash);
        out.extend_from_slice(&(self.payload_ref.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.certificate_ref.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.inline_certificate_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload_ref);
        out.extend_from_slice(&self.certificate_ref);
        out.extend_from_slice(&self.inline_certificate_bytes);
        out
    }

    fn chain_hash_material(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 * 4 + 32 * 3 + 2 + self.payload_ref.len());
        out.extend_from_slice(&self.log_index.to_le_bytes());
        out.extend_from_slice(&self.client_log_epoch.to_le_bytes());
        out.extend_from_slice(&self.membership_epoch.to_le_bytes());
        out.extend_from_slice(&self.policy_epoch.to_le_bytes());
        out.extend_from_slice(&self.previous_log_hash);
        out.extend_from_slice(&self.changeset_payload_hash);
        out.extend_from_slice(&self.verified_envelope_hash);
        out.extend_from_slice(&(self.payload_ref.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.payload_ref);
        out
    }
}

pub fn validate_personaldb_log_chain(records: &[PersonalDbLogRecord]) -> Result<(), FormatError> {
    let mut last_index = None;
    let mut last_hash = [0u8; 32];
    for record in records {
        if let Some(previous_index) = last_index {
            if record.log_index != previous_index + 1 {
                return Err(FormatError::NonContiguousLogIndex);
            }
            if record.previous_log_hash != last_hash {
                return Err(FormatError::LogPreviousHashMismatch);
            }
        }
        last_index = Some(record.log_index);
        last_hash = record.entry_hash;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowIndexRecord {
    pub database_id: Vec<u8>,
    pub table_name_hash: Hash32,
    pub primary_key_hash: Hash32,
    pub resource_type: Vec<u8>,
    pub resource_id: Vec<u8>,
    pub parent_resource_id: Vec<u8>,
    pub creator: Vec<u8>,
    pub owner: Vec<u8>,
    pub row_version: u64,
    pub policy_epoch: u64,
    pub auth_attribute_hash: Hash32,
    pub updated_at_nanos: i64,
    pub record_hash: Hash32,
}

impl RowIndexRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: Vec<u8>,
        table_name_hash: Hash32,
        primary_key_hash: Hash32,
        resource_type: Vec<u8>,
        resource_id: Vec<u8>,
        parent_resource_id: Vec<u8>,
        creator: Vec<u8>,
        owner: Vec<u8>,
        row_version: u64,
        policy_epoch: u64,
        auth_attribute_hash: Hash32,
        updated_at_nanos: i64,
    ) -> Self {
        let mut record = Self {
            database_id,
            table_name_hash,
            primary_key_hash,
            resource_type,
            resource_id,
            parent_resource_id,
            creator,
            owner,
            row_version,
            policy_epoch,
            auth_attribute_hash,
            updated_at_nanos,
            record_hash: [0; 32],
        };
        record.record_hash = hash32(&record.bytes_without_hash());
        record
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.bytes_without_hash();
        out.extend_from_slice(&self.record_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < ROW_INDEX_FIXED_LEN + 32 {
            return Err(FormatError::TooShort {
                context: "personaldb row index record",
                needed: ROW_INDEX_FIXED_LEN + 32,
                actual: input.len(),
            });
        }
        let database_id_len = u16::from_le_bytes(input[0..2].try_into().unwrap()) as usize;
        let resource_type_len = u16::from_le_bytes(input[66..68].try_into().unwrap()) as usize;
        let resource_id_len = u16::from_le_bytes(input[68..70].try_into().unwrap()) as usize;
        let parent_resource_id_len = u16::from_le_bytes(input[70..72].try_into().unwrap()) as usize;
        let creator_len = u16::from_le_bytes(input[72..74].try_into().unwrap()) as usize;
        let owner_len = u16::from_le_bytes(input[74..76].try_into().unwrap()) as usize;
        let database_id_start = ROW_INDEX_FIXED_LEN;
        let resource_type_start = database_id_start.checked_add(database_id_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "row index database id",
            },
        )?;
        let resource_id_start = resource_type_start.checked_add(resource_type_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "row index resource type",
            },
        )?;
        let parent_resource_id_start = resource_id_start.checked_add(resource_id_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "row index resource id",
            },
        )?;
        let creator_start = parent_resource_id_start
            .checked_add(parent_resource_id_len)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "row index parent resource id",
            })?;
        let owner_start =
            creator_start
                .checked_add(creator_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "row index creator",
                })?;
        let hash_start =
            owner_start
                .checked_add(owner_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "row index owner",
                })?;
        let record_end = hash_start
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "row index record hash",
            })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "personaldb row index record bytes",
                needed: record_end,
                actual: input.len(),
            });
        }
        let record_hash = input[hash_start..record_end].try_into().unwrap();
        if hash32(&input[..hash_start]) != record_hash {
            return Err(FormatError::HashMismatch {
                context: "personaldb row index record",
            });
        }
        Ok((
            Self {
                database_id: input[database_id_start..resource_type_start].to_vec(),
                table_name_hash: input[2..34].try_into().unwrap(),
                primary_key_hash: input[34..66].try_into().unwrap(),
                resource_type: input[resource_type_start..resource_id_start].to_vec(),
                resource_id: input[resource_id_start..parent_resource_id_start].to_vec(),
                parent_resource_id: input[parent_resource_id_start..creator_start].to_vec(),
                creator: input[creator_start..owner_start].to_vec(),
                owner: input[owner_start..hash_start].to_vec(),
                row_version: u64::from_le_bytes(input[76..84].try_into().unwrap()),
                policy_epoch: u64::from_le_bytes(input[84..92].try_into().unwrap()),
                auth_attribute_hash: input[92..124].try_into().unwrap(),
                updated_at_nanos: i64::from_le_bytes(input[124..132].try_into().unwrap()),
                record_hash,
            },
            record_end,
        ))
    }

    fn bytes_without_hash(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            ROW_INDEX_FIXED_LEN
                + self.database_id.len()
                + self.resource_type.len()
                + self.resource_id.len()
                + self.parent_resource_id.len()
                + self.creator.len()
                + self.owner.len(),
        );
        out.extend_from_slice(&(self.database_id.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.table_name_hash);
        out.extend_from_slice(&self.primary_key_hash);
        out.extend_from_slice(&(self.resource_type.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.resource_id.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.parent_resource_id.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.creator.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.owner.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.row_version.to_le_bytes());
        out.extend_from_slice(&self.policy_epoch.to_le_bytes());
        out.extend_from_slice(&self.auth_attribute_hash);
        out.extend_from_slice(&self.updated_at_nanos.to_le_bytes());
        out.extend_from_slice(&self.database_id);
        out.extend_from_slice(&self.resource_type);
        out.extend_from_slice(&self.resource_id);
        out.extend_from_slice(&self.parent_resource_id);
        out.extend_from_slice(&self.creator);
        out.extend_from_slice(&self.owner);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn personaldb_log_record_round_trip_and_chain_validation() {
        let first = PersonalDbLogRecord::new(
            1,
            10,
            20,
            30,
            [0; 32],
            [1; 32],
            [2; 32],
            [3; 32],
            b"objects/changeset-1".to_vec(),
            b"objects/cert-1".to_vec(),
            br#"{"certificate":1}"#.to_vec(),
        );
        let second = PersonalDbLogRecord::new(
            2,
            10,
            20,
            30,
            first.entry_hash,
            [4; 32],
            [5; 32],
            [6; 32],
            b"objects/changeset-2".to_vec(),
            b"objects/cert-2".to_vec(),
            br#"{"certificate":2}"#.to_vec(),
        );
        let encoded = first.encode();
        let (decoded, used) = PersonalDbLogRecord::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, first);
        let with_different_certificate = PersonalDbLogRecord::new(
            1,
            10,
            20,
            30,
            [0; 32],
            [1; 32],
            [2; 32],
            [99; 32],
            b"objects/changeset-1".to_vec(),
            b"objects/cert-1b".to_vec(),
            br#"{"certificate":99}"#.to_vec(),
        );
        assert_eq!(with_different_certificate.entry_hash, first.entry_hash);
        validate_personaldb_log_chain(&[first.clone(), second.clone()]).unwrap();

        let invalid = PersonalDbLogRecord {
            previous_log_hash: [99; 32],
            ..second
        };
        assert_eq!(
            validate_personaldb_log_chain(&[first, invalid]).unwrap_err(),
            FormatError::LogPreviousHashMismatch
        );
    }

    #[test]
    fn row_index_record_round_trip_checks_hash() {
        let record = RowIndexRecord::new(
            b"db-1".to_vec(),
            [1; 32],
            [2; 32],
            b"invoice".to_vec(),
            b"invoice-1".to_vec(),
            Vec::new(),
            b"user:alice".to_vec(),
            b"org:example".to_vec(),
            3,
            4,
            [5; 32],
            1_717_000_001,
        );
        let encoded = record.encode();
        let (decoded, used) = RowIndexRecord::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, record);

        let mut corrupted = encoded;
        corrupted[ROW_INDEX_FIXED_LEN] ^= 1;
        assert_eq!(
            RowIndexRecord::decode(&corrupted).unwrap_err(),
            FormatError::HashMismatch {
                context: "personaldb row index record"
            }
        );
    }
}
