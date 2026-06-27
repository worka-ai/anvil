use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleKey {
    pub namespace: Vec<u8>,
    pub object_id: Vec<u8>,
    pub relation: Vec<u8>,
    pub subject_kind: u8,
    pub subject_id: Vec<u8>,
    pub caveat_hash: Hash32,
}

impl TupleKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            2 + 2
                + 2
                + 1
                + 2
                + 32
                + self.namespace.len()
                + self.object_id.len()
                + self.relation.len()
                + self.subject_id.len(),
        );
        out.extend_from_slice(&(self.namespace.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.object_id.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.relation.len() as u16).to_le_bytes());
        out.push(self.subject_kind);
        out.extend_from_slice(&(self.subject_id.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.caveat_hash);
        out.extend_from_slice(&self.namespace);
        out.extend_from_slice(&self.object_id);
        out.extend_from_slice(&self.relation);
        out.extend_from_slice(&self.subject_id);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < 41 {
            return Err(FormatError::TooShort {
                context: "authz tuple key",
                needed: 41,
                actual: input.len(),
            });
        }
        let namespace_len = u16::from_le_bytes(input[0..2].try_into().unwrap()) as usize;
        let object_id_len = u16::from_le_bytes(input[2..4].try_into().unwrap()) as usize;
        let relation_len = u16::from_le_bytes(input[4..6].try_into().unwrap()) as usize;
        let subject_kind = input[6];
        let subject_id_len = u16::from_le_bytes(input[7..9].try_into().unwrap()) as usize;
        let caveat_hash = input[9..41].try_into().unwrap();
        let namespace_start: usize = 41;
        let object_id_start = namespace_start.checked_add(namespace_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "authz tuple namespace",
            },
        )?;
        let relation_start = object_id_start.checked_add(object_id_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "authz tuple object id",
            },
        )?;
        let subject_id_start =
            relation_start
                .checked_add(relation_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "authz tuple relation",
                })?;
        let record_end = subject_id_start.checked_add(subject_id_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "authz tuple subject id",
            },
        )?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "authz tuple key bytes",
                needed: record_end,
                actual: input.len(),
            });
        }
        Ok((
            Self {
                namespace: input[namespace_start..object_id_start].to_vec(),
                object_id: input[object_id_start..relation_start].to_vec(),
                relation: input[relation_start..subject_id_start].to_vec(),
                subject_kind,
                subject_id: input[subject_id_start..record_end].to_vec(),
                caveat_hash,
            },
            record_end,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TupleOperation {
    Add = 1,
    Remove = 2,
}

impl TupleOperation {
    fn from_u8(value: u8) -> Result<Self, FormatError> {
        match value {
            1 => Ok(Self::Add),
            2 => Ok(Self::Remove),
            other => Err(FormatError::UnsupportedOperation(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleValue {
    pub operation: TupleOperation,
    pub revision: u64,
    pub written_at_nanos: i64,
    pub written_by: Vec<u8>,
    pub reason: Vec<u8>,
    pub record_hash: Hash32,
}

impl TupleValue {
    pub fn new(
        operation: TupleOperation,
        revision: u64,
        written_at_nanos: i64,
        written_by: Vec<u8>,
        reason: Vec<u8>,
    ) -> Self {
        let mut value = Self {
            operation,
            revision,
            written_at_nanos,
            written_by,
            reason,
            record_hash: [0; 32],
        };
        value.record_hash = hash32(&value.bytes_without_hash());
        value
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.bytes_without_hash();
        out.extend_from_slice(&self.record_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < 23 + 32 {
            return Err(FormatError::TooShort {
                context: "authz tuple value",
                needed: 23 + 32,
                actual: input.len(),
            });
        }
        let written_by_len = u16::from_le_bytes(input[17..19].try_into().unwrap()) as usize;
        let reason_len = u16::from_le_bytes(input[19..21].try_into().unwrap()) as usize;
        let written_by_start: usize = 21;
        let reason_start = written_by_start.checked_add(written_by_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "authz tuple written_by",
            },
        )?;
        let hash_start =
            reason_start
                .checked_add(reason_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "authz tuple reason",
                })?;
        let record_end = hash_start
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "authz tuple value hash",
            })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "authz tuple value bytes",
                needed: record_end,
                actual: input.len(),
            });
        }
        let record_hash = input[hash_start..record_end].try_into().unwrap();
        if hash32(&input[..hash_start]) != record_hash {
            return Err(FormatError::HashMismatch {
                context: "authz tuple value",
            });
        }
        Ok((
            Self {
                operation: TupleOperation::from_u8(input[0])?,
                revision: u64::from_le_bytes(input[1..9].try_into().unwrap()),
                written_at_nanos: i64::from_le_bytes(input[9..17].try_into().unwrap()),
                written_by: input[written_by_start..reason_start].to_vec(),
                reason: input[reason_start..hash_start].to_vec(),
                record_hash,
            },
            record_end,
        ))
    }

    fn bytes_without_hash(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(21 + self.written_by.len() + self.reason.len());
        out.push(self.operation as u8);
        out.extend_from_slice(&self.revision.to_le_bytes());
        out.extend_from_slice(&self.written_at_nanos.to_le_bytes());
        out.extend_from_slice(&(self.written_by.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.reason.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.written_by);
        out.extend_from_slice(&self.reason);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuple_key_round_trip_uses_exact_wire_layout() {
        let key = TupleKey {
            namespace: b"document".to_vec(),
            object_id: b"doc-1".to_vec(),
            relation: b"viewer".to_vec(),
            subject_kind: 1,
            subject_id: b"user:alice".to_vec(),
            caveat_hash: [7; 32],
        };
        let encoded = key.encode();
        let (decoded, used) = TupleKey::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, key);
    }

    #[test]
    fn tuple_value_round_trip_checks_hash_and_operation() {
        let value = TupleValue::new(
            TupleOperation::Add,
            42,
            1_717_000_000,
            b"node-1".to_vec(),
            b"grant".to_vec(),
        );
        let encoded = value.encode();
        let (decoded, used) = TupleValue::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, value);

        let mut corrupted = encoded;
        corrupted[23] ^= 1;
        assert_eq!(
            TupleValue::decode(&corrupted).unwrap_err(),
            FormatError::HashMismatch {
                context: "authz tuple value"
            }
        );
    }
}
