use super::*;
use crate::core_store::{decode_deterministic_proto, encode_deterministic_proto};
use prost::Message;

pub(super) trait GatewayRecordCodec: Sized + Clone {
    fn encode_record(&self) -> Result<Vec<u8>>;
    fn decode_record(bytes: &[u8]) -> Result<Self>;
    fn clear_record_hash(&mut self);

    fn hash_bytes(&self) -> Result<Vec<u8>> {
        let mut unsigned = self.clone();
        unsigned.clear_record_hash();
        unsigned.encode_record()
    }
}

#[derive(Clone, PartialEq, Message)]
struct GatewayRepositoryRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    gateway: String,
    #[prost(string, tag = "4")]
    registry_instance_id: String,
    #[prost(string, tag = "5")]
    repository: String,
    #[prost(string, tag = "6")]
    created_at: String,
    #[prost(string, tag = "7")]
    created_by_principal: String,
    #[prost(string, tag = "8")]
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct GatewayBlobRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    gateway: String,
    #[prost(string, tag = "4")]
    registry_instance_id: String,
    #[prost(string, tag = "5")]
    repository: String,
    #[prost(string, tag = "6")]
    digest: String,
    #[prost(string, tag = "7")]
    media_type: String,
    #[prost(uint64, tag = "8")]
    size_bytes: u64,
    #[prost(string, tag = "9")]
    object_ref_target: String,
    #[prost(string, tag = "10")]
    created_at: String,
    #[prost(string, tag = "11")]
    created_by_principal: String,
    #[prost(string, tag = "12")]
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct GatewayTagRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    gateway: String,
    #[prost(string, tag = "4")]
    registry_instance_id: String,
    #[prost(string, tag = "5")]
    repository: String,
    #[prost(string, tag = "6")]
    tag: String,
    #[prost(string, tag = "7")]
    target_digest: String,
    #[prost(string, tag = "8")]
    updated_at: String,
    #[prost(string, tag = "9")]
    updated_by_principal: String,
    #[prost(string, tag = "10")]
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct GatewayUploadPartRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    session_id: String,
    #[prost(string, tag = "3")]
    part_id: String,
    #[prost(uint64, tag = "4")]
    offset: u64,
    #[prost(uint64, tag = "5")]
    length: u64,
    #[prost(string, tag = "6")]
    payload_hash: String,
    #[prost(string, tag = "7")]
    idempotency_key_hash: String,
    #[prost(string, tag = "8")]
    core_object_ref_target: String,
}

#[derive(Clone, PartialEq, Message)]
struct GatewayUploadSessionRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    gateway: String,
    #[prost(string, tag = "4")]
    registry_instance_id: String,
    #[prost(string, tag = "5")]
    repository: String,
    #[prost(string, tag = "6")]
    upload_id: String,
    #[prost(string, tag = "7")]
    idempotency_key_hash: String,
    #[prost(string, tag = "8")]
    state: String,
    #[prost(string, optional, tag = "9")]
    expected_digest: Option<String>,
    #[prost(uint64, tag = "10")]
    received_bytes: u64,
    #[prost(message, repeated, tag = "11")]
    staged_parts: Vec<GatewayUploadPartRecordProto>,
    #[prost(string, tag = "12")]
    started_at: String,
    #[prost(string, tag = "13")]
    expires_at: String,
    #[prost(string, optional, tag = "14")]
    completed_at: Option<String>,
    #[prost(string, tag = "15")]
    started_by_principal: String,
    #[prost(string, optional, tag = "16")]
    committed_digest: Option<String>,
    #[prost(string, tag = "17")]
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct GatewayCredentialRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    credential_id: String,
    #[prost(string, tag = "4")]
    gateway: String,
    #[prost(string, tag = "5")]
    subject_principal: String,
    #[prost(string, tag = "6")]
    secret_hash: String,
    #[prost(string, tag = "7")]
    created_at: String,
    #[prost(string, optional, tag = "8")]
    revoked_at: Option<String>,
    #[prost(string, tag = "9")]
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzScopeRefProto {
    #[prost(string, tag = "1")]
    anvil_storage_tenant_id: String,
    #[prost(string, tag = "2")]
    authz_realm_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct GatewayMountRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mount_id: String,
    #[prost(string, tag = "3")]
    gateway: String,
    #[prost(string, repeated, tag = "4")]
    hosts: Vec<String>,
    #[prost(string, repeated, tag = "5")]
    path_prefixes: Vec<String>,
    #[prost(string, tag = "6")]
    mesh_id: String,
    #[prost(string, tag = "7")]
    region: String,
    #[prost(string, tag = "8")]
    anvil_storage_tenant_id: String,
    #[prost(message, optional, tag = "9")]
    authz_scope: Option<AuthzScopeRefProto>,
    #[prost(string, tag = "10")]
    tenant_id: String,
    #[prost(string, tag = "11")]
    registry_instance_id: String,
    #[prost(string, tag = "12")]
    default_bucket: String,
    #[prost(string, tag = "13")]
    repository_prefix: String,
    #[prost(string, tag = "14")]
    state: String,
    #[prost(uint64, tag = "15")]
    generation: u64,
    #[prost(string, tag = "16")]
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct GatewayAuditRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    gateway: String,
    #[prost(string, tag = "4")]
    registry_instance_id: String,
    #[prost(string, tag = "5")]
    operation: String,
    #[prost(string, tag = "6")]
    repository: String,
    #[prost(string, optional, tag = "7")]
    package: Option<String>,
    #[prost(string, optional, tag = "8")]
    version_or_reference: Option<String>,
    #[prost(string, optional, tag = "9")]
    digest: Option<String>,
    #[prost(string, tag = "10")]
    subject_principal: String,
    #[prost(string, optional, tag = "11")]
    credential_id: Option<String>,
    #[prost(string, tag = "12")]
    request_id: String,
    #[prost(string, tag = "13")]
    result: String,
    #[prost(string, tag = "14")]
    created_at: String,
    #[prost(string, tag = "15")]
    record_hash: String,
}

impl GatewayRecordCodec for GatewayRepositoryRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        encode_proto(GatewayRepositoryRecordProto {
            schema: self.schema.clone(),
            tenant_id: self.tenant_id,
            gateway: self.gateway.clone(),
            registry_instance_id: self.registry_instance_id.clone(),
            repository: self.repository.clone(),
            created_at: self.created_at.clone(),
            created_by_principal: self.created_by_principal.clone(),
            record_hash: self.record_hash.clone(),
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        let proto = decode_proto::<GatewayRepositoryRecordProto>(bytes)?;
        Ok(Self {
            schema: proto.schema,
            tenant_id: proto.tenant_id,
            gateway: proto.gateway,
            registry_instance_id: proto.registry_instance_id,
            repository: proto.repository,
            created_at: proto.created_at,
            created_by_principal: proto.created_by_principal,
            record_hash: proto.record_hash,
        })
    }

    fn clear_record_hash(&mut self) {
        self.record_hash.clear();
    }
}

impl GatewayRecordCodec for GatewayBlobRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        encode_proto(GatewayBlobRecordProto {
            schema: self.schema.clone(),
            tenant_id: self.tenant_id,
            gateway: self.gateway.clone(),
            registry_instance_id: self.registry_instance_id.clone(),
            repository: self.repository.clone(),
            digest: self.digest.clone(),
            media_type: self.media_type.clone(),
            size_bytes: self.size_bytes,
            object_ref_target: encode_core_object_ref_target(&self.object_ref)?,
            created_at: self.created_at.clone(),
            created_by_principal: self.created_by_principal.clone(),
            record_hash: self.record_hash.clone(),
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        let proto = decode_proto::<GatewayBlobRecordProto>(bytes)?;
        Ok(Self {
            schema: proto.schema,
            tenant_id: proto.tenant_id,
            gateway: proto.gateway,
            registry_instance_id: proto.registry_instance_id,
            repository: proto.repository,
            digest: proto.digest,
            media_type: proto.media_type,
            size_bytes: proto.size_bytes,
            object_ref: decode_core_object_ref_target(&proto.object_ref_target)?,
            created_at: proto.created_at,
            created_by_principal: proto.created_by_principal,
            record_hash: proto.record_hash,
        })
    }

    fn clear_record_hash(&mut self) {
        self.record_hash.clear();
    }
}

impl GatewayRecordCodec for GatewayTagRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        encode_proto(GatewayTagRecordProto {
            schema: self.schema.clone(),
            tenant_id: self.tenant_id,
            gateway: self.gateway.clone(),
            registry_instance_id: self.registry_instance_id.clone(),
            repository: self.repository.clone(),
            tag: self.tag.clone(),
            target_digest: self.target_digest.clone(),
            updated_at: self.updated_at.clone(),
            updated_by_principal: self.updated_by_principal.clone(),
            record_hash: self.record_hash.clone(),
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        let proto = decode_proto::<GatewayTagRecordProto>(bytes)?;
        Ok(Self {
            schema: proto.schema,
            tenant_id: proto.tenant_id,
            gateway: proto.gateway,
            registry_instance_id: proto.registry_instance_id,
            repository: proto.repository,
            tag: proto.tag,
            target_digest: proto.target_digest,
            updated_at: proto.updated_at,
            updated_by_principal: proto.updated_by_principal,
            record_hash: proto.record_hash,
        })
    }

    fn clear_record_hash(&mut self) {
        self.record_hash.clear();
    }
}

impl GatewayRecordCodec for GatewayUploadSessionRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        let staged_parts = self
            .staged_parts
            .iter()
            .map(encode_upload_part_proto)
            .collect::<Result<Vec<_>>>()?;
        encode_proto(GatewayUploadSessionRecordProto {
            schema: self.schema.clone(),
            tenant_id: self.tenant_id,
            gateway: self.gateway.clone(),
            registry_instance_id: self.registry_instance_id.clone(),
            repository: self.repository.clone(),
            upload_id: self.upload_id.clone(),
            idempotency_key_hash: self.idempotency_key_hash.clone(),
            state: upload_state_name(self.state).to_string(),
            expected_digest: self.expected_digest.clone(),
            received_bytes: self.received_bytes,
            staged_parts,
            started_at: self.started_at.clone(),
            expires_at: self.expires_at.clone(),
            completed_at: self.completed_at.clone(),
            started_by_principal: self.started_by_principal.clone(),
            committed_digest: self.committed_digest.clone(),
            record_hash: self.record_hash.clone(),
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        let proto = decode_proto::<GatewayUploadSessionRecordProto>(bytes)?;
        Ok(Self {
            schema: proto.schema,
            tenant_id: proto.tenant_id,
            gateway: proto.gateway,
            registry_instance_id: proto.registry_instance_id,
            repository: proto.repository,
            upload_id: proto.upload_id,
            idempotency_key_hash: proto.idempotency_key_hash,
            state: parse_upload_state(&proto.state)?,
            expected_digest: proto.expected_digest,
            received_bytes: proto.received_bytes,
            staged_parts: proto
                .staged_parts
                .into_iter()
                .map(decode_upload_part_proto)
                .collect::<Result<Vec<_>>>()?,
            started_at: proto.started_at,
            expires_at: proto.expires_at,
            completed_at: proto.completed_at,
            started_by_principal: proto.started_by_principal,
            committed_digest: proto.committed_digest,
            record_hash: proto.record_hash,
        })
    }

    fn clear_record_hash(&mut self) {
        self.record_hash.clear();
    }
}

impl GatewayRecordCodec for GatewayCredentialRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        encode_proto(GatewayCredentialRecordProto {
            schema: self.schema.clone(),
            tenant_id: self.tenant_id,
            credential_id: self.credential_id.clone(),
            gateway: self.gateway.clone(),
            subject_principal: self.subject_principal.clone(),
            secret_hash: self.secret_hash.clone(),
            created_at: self.created_at.clone(),
            revoked_at: self.revoked_at.clone(),
            record_hash: self.record_hash.clone(),
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        let proto = decode_proto::<GatewayCredentialRecordProto>(bytes)?;
        Ok(Self {
            schema: proto.schema,
            tenant_id: proto.tenant_id,
            credential_id: proto.credential_id,
            gateway: proto.gateway,
            subject_principal: proto.subject_principal,
            secret_hash: proto.secret_hash,
            created_at: proto.created_at,
            revoked_at: proto.revoked_at,
            record_hash: proto.record_hash,
        })
    }

    fn clear_record_hash(&mut self) {
        self.record_hash.clear();
    }
}

impl GatewayRecordCodec for GatewayMountRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        encode_proto(GatewayMountRecordProto {
            schema: self.schema.clone(),
            mount_id: self.mount_id.clone(),
            gateway: self.gateway.clone(),
            hosts: self.hosts.clone(),
            path_prefixes: self.path_prefixes.clone(),
            mesh_id: self.mesh_id.clone(),
            region: self.region.clone(),
            anvil_storage_tenant_id: self.anvil_storage_tenant_id.clone(),
            authz_scope: Some(AuthzScopeRefProto {
                anvil_storage_tenant_id: self.authz_scope.anvil_storage_tenant_id.clone(),
                authz_realm_id: self.authz_scope.authz_realm_id.clone(),
            }),
            tenant_id: self.tenant_id.clone(),
            registry_instance_id: self.registry_instance_id.clone(),
            default_bucket: self.default_bucket.clone(),
            repository_prefix: self.repository_prefix.clone(),
            state: mount_state_name(self.state).to_string(),
            generation: self.generation,
            record_hash: self.record_hash.clone(),
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        let proto = decode_proto::<GatewayMountRecordProto>(bytes)?;
        let authz_scope = proto
            .authz_scope
            .ok_or_else(|| anyhow!("gateway mount record missing authz scope"))?;
        Ok(Self {
            schema: proto.schema,
            mount_id: proto.mount_id,
            gateway: proto.gateway,
            hosts: proto.hosts,
            path_prefixes: proto.path_prefixes,
            mesh_id: proto.mesh_id,
            region: proto.region,
            anvil_storage_tenant_id: proto.anvil_storage_tenant_id,
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: authz_scope.anvil_storage_tenant_id,
                authz_realm_id: authz_scope.authz_realm_id,
            },
            tenant_id: proto.tenant_id,
            registry_instance_id: proto.registry_instance_id,
            default_bucket: proto.default_bucket,
            repository_prefix: proto.repository_prefix,
            state: parse_mount_state(&proto.state)?,
            generation: proto.generation,
            record_hash: proto.record_hash,
        })
    }

    fn clear_record_hash(&mut self) {
        self.record_hash.clear();
    }
}

impl GatewayRecordCodec for GatewayAuditRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        encode_proto(GatewayAuditRecordProto {
            schema: self.schema.clone(),
            tenant_id: self.tenant_id,
            gateway: self.gateway.clone(),
            registry_instance_id: self.registry_instance_id.clone(),
            operation: self.operation.clone(),
            repository: self.repository.clone(),
            package: self.package.clone(),
            version_or_reference: self.version_or_reference.clone(),
            digest: self.digest.clone(),
            subject_principal: self.subject_principal.clone(),
            credential_id: self.credential_id.clone(),
            request_id: self.request_id.clone(),
            result: self.result.clone(),
            created_at: self.created_at.clone(),
            record_hash: self.record_hash.clone(),
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        let proto = decode_proto::<GatewayAuditRecordProto>(bytes)?;
        Ok(Self {
            schema: proto.schema,
            tenant_id: proto.tenant_id,
            gateway: proto.gateway,
            registry_instance_id: proto.registry_instance_id,
            operation: proto.operation,
            repository: proto.repository,
            package: proto.package,
            version_or_reference: proto.version_or_reference,
            digest: proto.digest,
            subject_principal: proto.subject_principal,
            credential_id: proto.credential_id,
            request_id: proto.request_id,
            result: proto.result,
            created_at: proto.created_at,
            record_hash: proto.record_hash,
        })
    }

    fn clear_record_hash(&mut self) {
        self.record_hash.clear();
    }
}

pub(super) fn encode_gateway_record<T: GatewayRecordCodec>(record: &T) -> Result<Vec<u8>> {
    record.encode_record()
}

pub(super) fn decode_gateway_record<T: GatewayRecordCodec>(bytes: &[u8]) -> Result<T> {
    T::decode_record(bytes)
}

pub(super) fn hash_gateway_record<T: GatewayRecordCodec>(record: &T) -> Result<String> {
    Ok(hex::encode(hash32(&record.hash_bytes()?)))
}

fn encode_upload_part_proto(
    part: &GatewayUploadPartRecord,
) -> Result<GatewayUploadPartRecordProto> {
    Ok(GatewayUploadPartRecordProto {
        schema: part.schema.clone(),
        session_id: part.session_id.clone(),
        part_id: part.part_id.clone(),
        offset: part.offset,
        length: part.length,
        payload_hash: part.payload_hash.clone(),
        idempotency_key_hash: part.idempotency_key_hash.clone(),
        core_object_ref_target: encode_core_object_ref_target(&part.core_object_ref)?,
    })
}

fn decode_upload_part_proto(
    proto: GatewayUploadPartRecordProto,
) -> Result<GatewayUploadPartRecord> {
    Ok(GatewayUploadPartRecord {
        schema: proto.schema,
        session_id: proto.session_id,
        part_id: proto.part_id,
        offset: proto.offset,
        length: proto.length,
        payload_hash: proto.payload_hash,
        idempotency_key_hash: proto.idempotency_key_hash,
        core_object_ref: decode_core_object_ref_target(&proto.core_object_ref_target)?,
    })
}

fn upload_state_name(state: GatewayUploadSessionState) -> &'static str {
    match state {
        GatewayUploadSessionState::Open => "open",
        GatewayUploadSessionState::Receiving => "receiving",
        GatewayUploadSessionState::Finalising => "finalising",
        GatewayUploadSessionState::Committed => "committed",
        GatewayUploadSessionState::Aborted => "aborted",
        GatewayUploadSessionState::Expired => "expired",
    }
}

fn parse_upload_state(value: &str) -> Result<GatewayUploadSessionState> {
    match value {
        "open" => Ok(GatewayUploadSessionState::Open),
        "receiving" => Ok(GatewayUploadSessionState::Receiving),
        "finalising" => Ok(GatewayUploadSessionState::Finalising),
        "committed" => Ok(GatewayUploadSessionState::Committed),
        "aborted" => Ok(GatewayUploadSessionState::Aborted),
        "expired" => Ok(GatewayUploadSessionState::Expired),
        other => bail!("unknown gateway upload session state {other}"),
    }
}

fn mount_state_name(state: GatewayMountState) -> &'static str {
    match state {
        GatewayMountState::Active => "active",
        GatewayMountState::Disabled => "disabled",
        GatewayMountState::Draining => "draining",
    }
}

fn parse_mount_state(value: &str) -> Result<GatewayMountState> {
    match value {
        "active" => Ok(GatewayMountState::Active),
        "disabled" => Ok(GatewayMountState::Disabled),
        "draining" => Ok(GatewayMountState::Draining),
        other => bail!("unknown gateway mount state {other}"),
    }
}

fn encode_proto(message: impl Message) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&message))
}

fn decode_proto<T>(bytes: &[u8]) -> Result<T>
where
    T: Message + Default,
{
    decode_deterministic_proto(bytes, "gateway record")
}
