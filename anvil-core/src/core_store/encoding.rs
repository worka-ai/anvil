use super::types::{SourceId, SourceKind};
use anyhow::{Result, anyhow, bail};

const STRING_TAG: u8 = 0x30;
const BYTES_TAG: u8 = 0x31;
const DECIMAL_TAG: u8 = 0x32;
const SOURCE_ID_TAG: u8 = 0x60;

#[derive(Debug, Clone, PartialEq)]
pub enum TypedFieldValue {
    Null,
    Missing,
    Bool(bool),
    Int64(i64),
    Uint64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Decimal { coefficient: i128 },
    TimestampNanos(i64),
    Uuid([u8; 16]),
    SourceId(SourceId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedTypedValue {
    pub bytes: Vec<u8>,
}

impl EncodedTypedValue {
    pub fn for_ordered_value(value: &TypedFieldValue, descending: bool) -> Result<Self> {
        let mut bytes = encode_typed_value(value)?;
        if descending {
            for byte in &mut bytes {
                *byte = !*byte;
            }
        }
        Ok(Self { bytes })
    }
}

impl SourceKind {
    fn binary_code(self) -> u16 {
        match self {
            Self::ObjectCurrent => 1,
            Self::ObjectVersion => 2,
            Self::AppendRecord => 3,
            Self::AuthzResource => 4,
            Self::PackageRepository => 5,
            Self::PackageVersion => 6,
            Self::PackageFile => 7,
            Self::PackageTag => 8,
            Self::GitObject => 9,
            Self::PersonalDatabaseRecord => 10,
            Self::MeshControlRecord => 11,
        }
    }
}

impl SourceId {
    pub fn encode_binary(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&self.kind.binary_code().to_le_bytes());
        push_len_bytes(&mut out, self.mesh_id.as_bytes())?;
        push_len_bytes(&mut out, self.anvil_storage_tenant_id.as_bytes())?;
        push_len_bytes(&mut out, self.authz_scope.authz_realm_id.as_bytes())?;
        push_len_bytes(&mut out, self.resource_namespace.as_bytes())?;
        push_len_bytes(&mut out, self.resource_id.as_bytes())?;
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.push(u8::from(self.tombstone));
        let variant = serde_json::to_vec(&self.variant)?;
        push_len_bytes(&mut out, &variant)?;
        Ok(out)
    }

    pub fn sort_key(&self) -> Result<Vec<u8>> {
        // SourceId sort order follows the RFC tuple. Length-prefixing preserves tuple boundaries.
        self.encode_binary()
    }
}

fn encode_typed_value(value: &TypedFieldValue) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match value {
        TypedFieldValue::Null => out.push(0x00),
        TypedFieldValue::Missing => out.push(0x01),
        TypedFieldValue::Bool(false) => out.push(0x10),
        TypedFieldValue::Bool(true) => out.push(0x11),
        TypedFieldValue::Int64(value) => {
            out.push(0x20);
            let sortable = (*value as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&sortable.to_be_bytes());
        }
        TypedFieldValue::Uint64(value) => {
            out.push(0x21);
            out.extend_from_slice(&value.to_be_bytes());
        }
        TypedFieldValue::Float64(value) => {
            if value.is_nan() {
                bail!("NaN cannot be encoded as a CoreStore typed field value");
            }
            out.push(0x22);
            let bits = value.to_bits();
            let sortable = if bits & 0x8000_0000_0000_0000 != 0 {
                !bits
            } else {
                bits | 0x8000_0000_0000_0000
            };
            out.extend_from_slice(&sortable.to_be_bytes());
        }
        TypedFieldValue::String(value) => {
            out.push(STRING_TAG);
            push_escaped_terminated(&mut out, value.as_bytes());
        }
        TypedFieldValue::Bytes(value) => {
            out.push(BYTES_TAG);
            push_escaped_terminated(&mut out, value);
        }
        TypedFieldValue::Decimal { coefficient } => {
            out.push(DECIMAL_TAG);
            let sortable = (*coefficient as u128) ^ 0x8000_0000_0000_0000_0000_0000_0000_0000;
            out.extend_from_slice(&sortable.to_be_bytes());
        }
        TypedFieldValue::TimestampNanos(value) => {
            out.push(0x40);
            let sortable = (*value as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&sortable.to_be_bytes());
        }
        TypedFieldValue::Uuid(value) => {
            out.push(0x50);
            out.extend_from_slice(value);
        }
        TypedFieldValue::SourceId(value) => {
            out.push(SOURCE_ID_TAG);
            out.extend_from_slice(&value.encode_binary()?);
        }
    }
    Ok(out)
}

fn push_len_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| anyhow!("CoreStore value too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn push_escaped_terminated(out: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            out.push(0);
            out.push(0xff);
        } else {
            out.push(*byte);
        }
    }
    out.push(0);
    out.push(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_store::{AuthzScopeRef, SourceId};
    use std::collections::BTreeMap;

    #[test]
    fn typed_values_preserve_expected_order() {
        let neg =
            EncodedTypedValue::for_ordered_value(&TypedFieldValue::Int64(-10), false).unwrap();
        let zero = EncodedTypedValue::for_ordered_value(&TypedFieldValue::Int64(0), false).unwrap();
        let pos = EncodedTypedValue::for_ordered_value(&TypedFieldValue::Int64(10), false).unwrap();
        assert!(neg.bytes < zero.bytes);
        assert!(zero.bytes < pos.bytes);

        let a =
            EncodedTypedValue::for_ordered_value(&TypedFieldValue::Float64(-1.5), false).unwrap();
        let b =
            EncodedTypedValue::for_ordered_value(&TypedFieldValue::Float64(2.5), false).unwrap();
        assert!(a.bytes < b.bytes);
    }

    #[test]
    fn typed_string_and_bytes_escape_zero_with_double_zero_terminator() {
        let encoded = EncodedTypedValue::for_ordered_value(
            &TypedFieldValue::Bytes(vec![b'a', 0, b'b']),
            false,
        )
        .unwrap();
        assert_eq!(encoded.bytes, vec![0x31, b'a', 0, 0xff, b'b', 0, 0]);
    }

    #[test]
    fn descending_order_inverts_encoded_bytes() {
        let asc =
            EncodedTypedValue::for_ordered_value(&TypedFieldValue::Uint64(10), false).unwrap();
        let desc =
            EncodedTypedValue::for_ordered_value(&TypedFieldValue::Uint64(10), true).unwrap();
        assert_eq!(asc.bytes.len(), desc.bytes.len());
        for (a, d) in asc.bytes.iter().zip(desc.bytes.iter()) {
            assert_eq!(*d, !*a);
        }
    }

    #[test]
    fn source_id_binary_is_stable_and_includes_authz_scope() {
        let source = SourceId {
            schema: "anvil.query.source_id.v1".to_string(),
            mesh_id: "mesh".to_string(),
            anvil_storage_tenant_id: "storage-tenant".to_string(),
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "storage-tenant".to_string(),
                authz_realm_id: "realm".to_string(),
            },
            kind: SourceKind::ObjectCurrent,
            resource_namespace: "anvil_object".to_string(),
            resource_id: "tenant/bucket/key".to_string(),
            generation: 7,
            tombstone: false,
            variant: BTreeMap::from([("object_key".to_string(), "key".to_string())]),
        };
        let encoded = source.encode_binary().unwrap();
        assert!(encoded.windows("realm".len()).any(|w| w == b"realm"));
        assert_eq!(encoded, source.encode_binary().unwrap());
    }
}
