use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use prost::Message;
use sha2::Sha256;

const OBJECT_PAGE_TOKEN_VERSION: u32 = 1;
const OBJECT_PAGE_TOKEN_TTL_SECONDS: i64 = 6 * 60 * 60;
const OBJECT_PAGE_TOKEN_DOMAIN: &[u8] = b"anvil-object-page-token-v1";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, PartialEq, Message)]
struct ObjectPageTokenProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    token_kind: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    principal_hash: String,
    #[prost(string, tag = "5")]
    bucket_name: String,
    #[prost(string, tag = "6")]
    prefix: String,
    #[prost(string, tag = "7")]
    delimiter: String,
    #[prost(uint32, tag = "8")]
    limit: u32,
    #[prost(string, tag = "9")]
    consistency_hash: String,
    #[prost(string, tag = "10")]
    last_key: String,
    #[prost(string, tag = "11")]
    last_version_id: String,
    #[prost(uint64, tag = "12")]
    last_sequence: u64,
    #[prost(string, tag = "13")]
    expires_at: String,
    #[prost(string, tag = "14")]
    signature: String,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectPageTokenBinding {
    pub token_kind: &'static str,
    pub tenant_id: i64,
    pub principal_hash: String,
    pub bucket_name: String,
    pub prefix: String,
    pub delimiter: String,
    pub limit: u32,
    pub consistency_hash: String,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectPageToken {
    token_kind: String,
    tenant_id: i64,
    principal_hash: String,
    bucket_name: String,
    prefix: String,
    delimiter: String,
    limit: u32,
    consistency_hash: String,
    pub last_key: String,
    pub last_version_id: String,
    pub last_sequence: u64,
    expires_at: String,
    signature: String,
}

impl ObjectPageTokenBinding {
    pub fn for_objects(
        claims: &auth::Claims,
        bucket_name: &str,
        prefix: &str,
        delimiter: &str,
        limit: u32,
        consistency: &ReadConsistency,
    ) -> Self {
        Self::new(
            "object-list",
            claims,
            bucket_name,
            prefix,
            delimiter,
            limit,
            consistency,
        )
    }

    pub fn for_versions(
        claims: &auth::Claims,
        bucket_name: &str,
        prefix: &str,
        limit: u32,
        consistency: &ReadConsistency,
    ) -> Self {
        Self::new(
            "object-version-list",
            claims,
            bucket_name,
            prefix,
            "",
            limit,
            consistency,
        )
    }

    pub fn for_stream(
        claims: &auth::Claims,
        bucket_name: &str,
        stream_key: &str,
        stream_id: &str,
        limit: u32,
        consistency: &ReadConsistency,
    ) -> Self {
        Self::new(
            "append-stream-read",
            claims,
            bucket_name,
            &format!("{stream_key}\0{stream_id}"),
            "",
            limit,
            consistency,
        )
    }

    fn new(
        token_kind: &'static str,
        claims: &auth::Claims,
        bucket_name: &str,
        prefix: &str,
        delimiter: &str,
        limit: u32,
        consistency: &ReadConsistency,
    ) -> Self {
        Self {
            token_kind,
            tenant_id: claims.tenant_id,
            principal_hash: principal_hash(claims),
            bucket_name: bucket_name.to_string(),
            prefix: prefix.to_string(),
            delimiter: delimiter.to_string(),
            limit,
            consistency_hash: consistency_hash(consistency),
        }
    }
}

impl ObjectPageToken {
    pub fn for_object_key(binding: &ObjectPageTokenBinding, last_key: String) -> Self {
        Self::new(binding, last_key, String::new(), 0)
    }

    pub fn for_version_marker(
        binding: &ObjectPageTokenBinding,
        last_key: String,
        last_version_id: String,
    ) -> Self {
        Self::new(binding, last_key, last_version_id, 0)
    }

    pub fn for_sequence(binding: &ObjectPageTokenBinding, last_sequence: u64) -> Self {
        Self::new(binding, String::new(), String::new(), last_sequence)
    }

    fn new(
        binding: &ObjectPageTokenBinding,
        last_key: String,
        last_version_id: String,
        last_sequence: u64,
    ) -> Self {
        Self {
            token_kind: binding.token_kind.to_string(),
            tenant_id: binding.tenant_id,
            principal_hash: binding.principal_hash.clone(),
            bucket_name: binding.bucket_name.clone(),
            prefix: binding.prefix.clone(),
            delimiter: binding.delimiter.clone(),
            limit: binding.limit,
            consistency_hash: binding.consistency_hash.clone(),
            last_key,
            last_version_id,
            last_sequence,
            expires_at: (chrono::Utc::now()
                + chrono::Duration::seconds(OBJECT_PAGE_TOKEN_TTL_SECONDS))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            signature: String::new(),
        }
    }

    pub fn decode(
        raw: &str,
        binding: &ObjectPageTokenBinding,
        signing_key: &[u8],
    ) -> Result<Option<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        let proto = crate::core_store::decode_deterministic_proto::<ObjectPageTokenProto>(
            &bytes,
            "object page token",
        )
        .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        if proto.version != OBJECT_PAGE_TOKEN_VERSION {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        let token = Self::from_proto(proto);
        if token.signature != token.sign(signing_key)? {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        token.validate(binding)?;
        Ok(Some(token))
    }

    pub fn encode(mut self, signing_key: &[u8]) -> Result<String, Status> {
        self.signature = self.sign(signing_key)?;
        Ok(
            URL_SAFE_NO_PAD.encode(crate::core_store::encode_deterministic_proto(
                &self.to_proto(),
            )),
        )
    }

    fn validate(&self, binding: &ObjectPageTokenBinding) -> Result<(), Status> {
        if self.token_kind != binding.token_kind
            || self.tenant_id != binding.tenant_id
            || self.principal_hash != binding.principal_hash
            || self.bucket_name != binding.bucket_name
            || self.prefix != binding.prefix
            || self.delimiter != binding.delimiter
            || self.limit != binding.limit
            || self.consistency_hash != binding.consistency_hash
        {
            return Err(Status::invalid_argument("PageTokenScopeMismatch"));
        }
        let expires_at = chrono::DateTime::parse_from_rfc3339(&self.expires_at)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?
            .with_timezone(&chrono::Utc);
        if expires_at <= chrono::Utc::now() {
            return Err(Status::invalid_argument("PageTokenGenerationExpired"));
        }
        Ok(())
    }

    fn sign(&self, signing_key: &[u8]) -> Result<String, Status> {
        let mut mac = HmacSha256::new_from_slice(signing_key)
            .map_err(|_| Status::internal("Invalid object page token signing key"))?;
        mac.update(OBJECT_PAGE_TOKEN_DOMAIN);
        mac.update(&OBJECT_PAGE_TOKEN_VERSION.to_le_bytes());
        update_mac_part(&mut mac, self.token_kind.as_bytes());
        mac.update(&self.tenant_id.to_le_bytes());
        update_mac_part(&mut mac, self.principal_hash.as_bytes());
        update_mac_part(&mut mac, self.bucket_name.as_bytes());
        update_mac_part(&mut mac, self.prefix.as_bytes());
        update_mac_part(&mut mac, self.delimiter.as_bytes());
        mac.update(&self.limit.to_le_bytes());
        update_mac_part(&mut mac, self.consistency_hash.as_bytes());
        update_mac_part(&mut mac, self.last_key.as_bytes());
        update_mac_part(&mut mac, self.last_version_id.as_bytes());
        mac.update(&self.last_sequence.to_le_bytes());
        update_mac_part(&mut mac, self.expires_at.as_bytes());
        Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }

    fn to_proto(&self) -> ObjectPageTokenProto {
        ObjectPageTokenProto {
            version: OBJECT_PAGE_TOKEN_VERSION,
            token_kind: self.token_kind.clone(),
            tenant_id: self.tenant_id,
            principal_hash: self.principal_hash.clone(),
            bucket_name: self.bucket_name.clone(),
            prefix: self.prefix.clone(),
            delimiter: self.delimiter.clone(),
            limit: self.limit,
            consistency_hash: self.consistency_hash.clone(),
            last_key: self.last_key.clone(),
            last_version_id: self.last_version_id.clone(),
            last_sequence: self.last_sequence,
            expires_at: self.expires_at.clone(),
            signature: self.signature.clone(),
        }
    }

    fn from_proto(proto: ObjectPageTokenProto) -> Self {
        Self {
            token_kind: proto.token_kind,
            tenant_id: proto.tenant_id,
            principal_hash: proto.principal_hash,
            bucket_name: proto.bucket_name,
            prefix: proto.prefix,
            delimiter: proto.delimiter,
            limit: proto.limit,
            consistency_hash: proto.consistency_hash,
            last_key: proto.last_key,
            last_version_id: proto.last_version_id,
            last_sequence: proto.last_sequence,
            expires_at: proto.expires_at,
            signature: proto.signature,
        }
    }
}

pub(super) fn default_read_consistency_proto() -> ReadConsistency {
    ReadConsistency {
        mode: Some(crate::anvil_api::read_consistency::Mode::Latest(true)),
    }
}

pub(super) fn effective_read_consistency(req: Option<&ReadConsistency>) -> ReadConsistency {
    req.filter(|value| value.mode.is_some())
        .cloned()
        .unwrap_or_else(default_read_consistency_proto)
}

fn consistency_hash(consistency: &ReadConsistency) -> String {
    let bytes = crate::core_store::encode_deterministic_proto(consistency);
    format!("sha256:{}", crate::core_store::sha256_hex(&bytes))
}

fn principal_hash(claims: &auth::Claims) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-object-page-token-principal-v2");
    update_blake_part(&mut hasher, claims.sub.as_bytes());
    update_blake_part(&mut hasher, claims.tenant_id.to_string().as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize().as_bytes()))
}

fn update_mac_part(mac: &mut HmacSha256, value: &[u8]) {
    let len = u64::try_from(value.len()).unwrap_or(u64::MAX);
    mac.update(&len.to_le_bytes());
    mac.update(value);
}

fn update_blake_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    let len = u64::try_from(value.len()).unwrap_or(u64::MAX);
    hasher.update(&len.to_le_bytes());
    hasher.update(value);
}
