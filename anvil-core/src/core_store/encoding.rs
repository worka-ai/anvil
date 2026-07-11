use super::types::{AuthzScopeRef, SourceId, SourceKind};
use anyhow::{Result, anyhow, bail};
use std::collections::BTreeMap;

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
    fn from_binary_code(code: u16) -> Result<Self> {
        match code {
            1 => Ok(Self::ObjectCurrent),
            2 => Ok(Self::ObjectVersion),
            3 => Ok(Self::AppendRecord),
            4 => Ok(Self::AuthzResource),
            5 => Ok(Self::PackageRepository),
            6 => Ok(Self::PackageVersion),
            7 => Ok(Self::PackageFile),
            8 => Ok(Self::PackageTag),
            9 => Ok(Self::GitObject),
            10 => Ok(Self::PersonalDatabaseRecord),
            11 => Ok(Self::MeshControlRecord),
            other => Err(anyhow!("unknown SourceId kind code {other}")),
        }
    }

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
        let variant = encode_string_map(&self.variant)?;
        push_len_bytes(&mut out, &variant)?;
        Ok(out)
    }

    pub fn sort_key(&self) -> Result<Vec<u8>> {
        // SourceId sort order follows the RFC tuple. Length-prefixing preserves tuple boundaries.
        self.encode_binary()
    }

    pub fn decode_binary(input: &[u8]) -> Result<Self> {
        let mut cursor = BinaryCursor::new(input);
        let version = cursor.read_u16()?;
        if version != 1 {
            bail!("unsupported SourceId binary version {version}");
        }
        let kind = SourceKind::from_binary_code(cursor.read_u16()?)?;
        let mesh_id = cursor.read_string()?;
        let anvil_storage_tenant_id = cursor.read_string()?;
        let authz_realm_id = cursor.read_string()?;
        let resource_namespace = cursor.read_string()?;
        let resource_id = cursor.read_string()?;
        let generation = cursor.read_u64()?;
        let tombstone = cursor.read_bool()?;
        let variant_bytes = cursor.read_len_bytes()?.to_vec();
        cursor.finish()?;
        Ok(Self {
            schema: "anvil.query.source_id.v1".to_string(),
            mesh_id,
            anvil_storage_tenant_id: anvil_storage_tenant_id.clone(),
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id,
                authz_realm_id,
            },
            kind,
            resource_namespace,
            resource_id,
            generation,
            tombstone,
            variant: decode_string_map(&variant_bytes)?,
        })
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

fn encode_string_map(map: &BTreeMap<String, String>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let len = u32::try_from(map.len()).map_err(|_| anyhow!("CoreStore map too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    for (key, value) in map {
        push_len_bytes(&mut out, key.as_bytes())?;
        push_len_bytes(&mut out, value.as_bytes())?;
    }
    Ok(out)
}

fn decode_string_map(input: &[u8]) -> Result<BTreeMap<String, String>> {
    let mut cursor = BinaryCursor::new(input);
    let len = cursor.read_u32()? as usize;
    let mut out = BTreeMap::new();
    for _ in 0..len {
        let key = cursor.read_string()?;
        let value = cursor.read_string()?;
        out.insert(key, value);
    }
    cursor.finish()?;
    Ok(out)
}

struct BinaryCursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> BinaryCursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| anyhow!("SourceId cursor overflow"))?;
        if end > self.input.len() {
            bail!("SourceId binary is truncated");
        }
        let out = &self.input[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_exact(2)?.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_exact(4)?.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    fn read_bool(&mut self) -> Result<bool> {
        match self.read_exact(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(anyhow!("invalid SourceId bool byte {other}")),
        }
    }

    fn read_len_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }

    fn read_string(&mut self) -> Result<String> {
        let bytes = self.read_len_bytes()?;
        std::str::from_utf8(bytes)
            .map(|value| value.to_string())
            .map_err(|error| anyhow!("SourceId string is not UTF-8: {error}"))
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.input.len() {
            bail!("SourceId binary has trailing bytes");
        }
        Ok(())
    }
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
