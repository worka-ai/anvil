use crate::{
    core_store::{
        AppendStreamRecord, AuthzScopeRef, CompareAndSwapRef, CoreMutationBatch,
        CoreMutationOperation, CoreMutationPrecondition, CoreObjectRef, CorePipelinePolicy,
        CoreRefValue, CoreStore, CoreTraceContext, GetBlob, ReadStream, StreamAppendReceipt,
        StreamRecord, WriteLogicalFileRequest, core_object_ref_from_logical_file_manifest,
    },
    formats::hash32,
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";
const GATEWAY_REPOSITORY_SCHEMA: &str = "anvil.gateway.repository.v1";
const GATEWAY_BLOB_SCHEMA: &str = "anvil.gateway.blob.v1";
const GATEWAY_TAG_SCHEMA: &str = "anvil.gateway.tag.v1";
const GATEWAY_UPLOAD_SESSION_SCHEMA: &str = "anvil.gateway.upload_session.v1";
const GATEWAY_CREDENTIAL_SCHEMA: &str = "anvil.gateway.credential.v1";
const GATEWAY_MOUNT_SCHEMA: &str = "anvil.gateway.mount.v1";
const GATEWAY_AUDIT_SCHEMA: &str = "anvil.gateway.audit.v1";
const GATEWAY_ACCESS_TOKEN_KIND: &str = "anvil.gateway.access_token.v1";
pub const GATEWAY_CREDENTIAL_CACHE_TTL_SECONDS: i64 = 60;
pub const GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS: i64 = 900;
const REGIONAL_GATEWAY_SUFFIX: &str = ".anvil-storage.com";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayRepositoryRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub created_at: String,
    pub created_by_principal: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayBlobRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub digest: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub object_ref: CoreObjectRef,
    pub created_at: String,
    pub created_by_principal: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayTagRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub tag: String,
    pub target_digest: String,
    pub updated_at: String,
    pub updated_by_principal: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayUploadSessionState {
    Open,
    Receiving,
    Finalising,
    Committed,
    Aborted,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayUploadPartRecord {
    pub schema: String,
    pub session_id: String,
    pub part_id: String,
    pub offset: u64,
    pub length: u64,
    pub payload_hash: String,
    pub idempotency_key_hash: String,
    pub core_object_ref: CoreObjectRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayUploadSessionRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub upload_id: String,
    pub idempotency_key_hash: String,
    pub state: GatewayUploadSessionState,
    pub expected_digest: Option<String>,
    pub received_bytes: u64,
    pub staged_parts: Vec<GatewayUploadPartRecord>,
    pub started_at: String,
    pub expires_at: String,
    pub completed_at: Option<String>,
    pub started_by_principal: String,
    pub committed_digest: Option<String>,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayCredentialRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub credential_id: String,
    pub gateway: String,
    pub subject_principal: String,
    pub secret_hash: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
    pub record_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayMountState {
    Active,
    Disabled,
    Draining,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayMountRecord {
    pub schema: String,
    pub mount_id: String,
    pub gateway: String,
    pub hosts: Vec<String>,
    pub path_prefixes: Vec<String>,
    pub mesh_id: String,
    pub region: String,
    pub anvil_storage_tenant_id: String,
    pub authz_scope: AuthzScopeRef,
    pub tenant_id: String,
    pub registry_instance_id: String,
    pub default_bucket: String,
    pub repository_prefix: String,
    pub state: GatewayMountState,
    pub generation: u64,
    pub record_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayMountMatchKind {
    ExactHostAlias,
    VirtualHostRegional,
    PathStyleRegional,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayMountResolution {
    pub record: GatewayMountRecord,
    pub ref_generation: u64,
    pub matched_host: String,
    pub matched_path_prefix: String,
    pub match_kind: GatewayMountMatchKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTagUpdateReceipt {
    pub record: GatewayTagRecord,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayUploadSessionReceipt {
    pub record: GatewayUploadSessionRecord,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAuditRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub operation: String,
    pub repository: String,
    pub package: Option<String>,
    pub version_or_reference: Option<String>,
    pub digest: Option<String>,
    pub subject_principal: String,
    pub credential_id: Option<String>,
    pub request_id: String,
    pub result: String,
    pub created_at: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAuditAppendReceipt {
    pub record: GatewayAuditRecord,
    pub stream: StreamAppendReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAuditStreamRecord {
    pub audit: GatewayAuditRecord,
    pub stream: StreamRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAccessTokenClaims {
    pub token_kind: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub actions: Vec<String>,
    pub subject_principal: String,
    pub credential_id: String,
    pub credential_generation: u64,
    pub iat: i64,
    pub exp: usize,
    pub jti: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAccessToken {
    pub access_token: String,
    pub token_type: String,
    pub expires_in_seconds: i64,
    pub claims: GatewayAccessTokenClaims,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTokenRequirement {
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub action: String,
}

pub fn normalize_gateway_identifier(input: &str, label: &str) -> Result<String> {
    let value = input.nfc().collect::<String>();
    if value.is_empty() {
        bail!("gateway {label} must not be empty");
    }
    if value.len() > 255 {
        bail!("gateway {label} exceeds 255 bytes");
    }
    if value != input {
        bail!("gateway {label} must already be Unicode NFC normalised");
    }
    if value.contains('%') {
        bail!("gateway {label} must not contain percent-encoded bytes");
    }
    if value.contains('\\') || value.contains('\0') || value.chars().any(char::is_control) {
        bail!("gateway {label} contains an unsafe character");
    }
    for segment in value.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            bail!("gateway {label} contains an unsafe path segment");
        }
        let lower = segment.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "_anvil"
                | "_core"
                | "_system"
                | "_authz"
                | "_credentials"
                | "_gateway"
                | "_internal"
                | ".well-known"
        ) {
            bail!("gateway {label} uses a reserved name");
        }
    }
    Ok(value)
}

pub fn validate_gateway_digest(digest: &str) -> Result<()> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        bail!("gateway digest must use sha256:<hex>");
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("gateway digest must contain a 32 byte sha256 hex value");
    }
    Ok(())
}

pub async fn create_gateway_repository(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    created_by_principal: &str,
) -> Result<GatewayRepositoryRecord> {
    validate_tenant(tenant_id)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let created_by_principal = normalize_gateway_identifier(created_by_principal, "principal")?;
    let mut record = GatewayRepositoryRecord {
        schema: GATEWAY_REPOSITORY_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        created_at: now_rfc3339(),
        created_by_principal,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    put_record_ref(
        storage,
        &gateway_repository_ref_name(&record)?,
        &record,
        true,
        None,
    )
    .await?;
    Ok(record)
}

pub async fn read_gateway_repository(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
) -> Result<Option<GatewayRepositoryRecord>> {
    let key = GatewayRepositoryKey::new(tenant_id, gateway, registry_instance_id, repository)?;
    let Some(record) = read_record_ref::<GatewayRepositoryRecord>(storage, &key.ref_name()).await?
    else {
        return Ok(None);
    };
    validate_repository_record(&record, &key)?;
    Ok(Some(record))
}

#[allow(clippy::too_many_arguments)]
pub async fn put_gateway_blob(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
    media_type: &str,
    bytes: &[u8],
    created_by_principal: &str,
) -> Result<GatewayBlobRecord> {
    validate_tenant(tenant_id)?;
    validate_gateway_digest(digest)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let created_by_principal = normalize_gateway_identifier(created_by_principal, "principal")?;
    validate_media_type(media_type)?;
    let actual_digest = format!("sha256:{}", sha256_hex(bytes));
    if actual_digest != digest {
        bail!("gateway blob digest mismatch: expected {digest}, got {actual_digest}");
    }
    let ref_name = gateway_blob_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        digest,
    )?;
    if let Some(existing) = read_record_ref::<GatewayBlobRecord>(storage, &ref_name).await? {
        validate_blob_record(
            &existing,
            tenant_id,
            &gateway,
            &registry_instance_id,
            &repository,
            digest,
        )?;
        return Ok(existing);
    }

    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = write_gateway_logical_file(
        &store,
        "registry_blob",
        1,
        ref_name.clone(),
        bytes.to_vec(),
        format!("gateway-blob:{tenant_id}:{gateway}:{registry_instance_id}:{repository}:{digest}"),
    )
    .await?;
    let mut record = GatewayBlobRecord {
        schema: GATEWAY_BLOB_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        digest: digest.to_string(),
        media_type: media_type.to_string(),
        size_bytes: bytes.len() as u64,
        object_ref,
        created_at: now_rfc3339(),
        created_by_principal,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    put_record_ref(storage, &ref_name, &record, true, None).await?;
    Ok(record)
}

pub async fn read_gateway_blob(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
) -> Result<Option<(GatewayBlobRecord, Vec<u8>)>> {
    validate_gateway_digest(digest)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let ref_name = gateway_blob_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        digest,
    )?;
    let Some(record) = read_record_ref::<GatewayBlobRecord>(storage, &ref_name).await? else {
        return Ok(None);
    };
    validate_blob_record(
        &record,
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        digest,
    )?;
    let bytes = CoreStore::new(storage.clone())
        .await?
        .get_blob(GetBlob {
            object_ref: record.object_ref.clone(),
        })
        .await?;
    let actual_digest = format!("sha256:{}", sha256_hex(&bytes));
    if actual_digest != record.digest || bytes.len() as u64 != record.size_bytes {
        bail!("gateway blob payload does not match its record");
    }
    Ok(Some((record, bytes)))
}

#[allow(clippy::too_many_arguments)]
pub async fn update_gateway_tag(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
    target_digest: &str,
    updated_by_principal: &str,
    expected_generation: Option<u64>,
) -> Result<GatewayTagUpdateReceipt> {
    validate_gateway_digest(target_digest)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let tag = normalize_gateway_identifier(tag, "tag")?;
    let updated_by_principal = normalize_gateway_identifier(updated_by_principal, "principal")?;
    let mut record = GatewayTagRecord {
        schema: GATEWAY_TAG_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        tag,
        target_digest: target_digest.to_string(),
        updated_at: now_rfc3339(),
        updated_by_principal,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    let ref_name = gateway_tag_ref_name(&record)?;
    let receipt = put_record_ref(storage, &ref_name, &record, false, expected_generation).await?;
    Ok(GatewayTagUpdateReceipt {
        record,
        generation: receipt.generation,
    })
}

pub async fn read_gateway_tag(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
) -> Result<Option<(GatewayTagRecord, CoreRefValue)>> {
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let tag = normalize_gateway_identifier(tag, "tag")?;
    let ref_name = gateway_tag_ref_name_parts(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &tag,
    )?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let record: GatewayTagRecord = serde_json::from_slice(&bytes)?;
    validate_tag_record(
        &record,
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &tag,
    )?;
    Ok(Some((record, ref_value)))
}

pub async fn create_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    expected_digest: Option<&str>,
    started_by_principal: &str,
    idempotency_key: &str,
    ttl_seconds: i64,
) -> Result<GatewayUploadSessionRecord> {
    validate_tenant(tenant_id)?;
    if let Some(digest) = expected_digest {
        validate_gateway_digest(digest)?;
    }
    if ttl_seconds <= 0 {
        bail!("gateway upload session ttl must be positive");
    }
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let started_by_principal = normalize_gateway_identifier(started_by_principal, "principal")?;
    let idempotency_key_hash = idempotency_hash(idempotency_key)?;
    let idempotency_ref_name = gateway_upload_idempotency_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &idempotency_key_hash,
    )?;
    let store = CoreStore::new(storage.clone()).await?;
    if let Some(ref_value) = store.read_ref(&idempotency_ref_name).await? {
        let record = read_upload_session_from_ref_value(&store, &ref_value).await?;
        validate_upload_session_record(
            &record,
            tenant_id,
            &gateway,
            &registry_instance_id,
            &repository,
            &record.upload_id,
        )?;
        if record.expected_digest.as_deref() != expected_digest {
            bail!("gateway upload session idempotency target mismatch");
        }
        return Ok(record);
    }

    let upload_id = Uuid::new_v4().simple().to_string();
    let now = Utc::now();
    let mut record = GatewayUploadSessionRecord {
        schema: GATEWAY_UPLOAD_SESSION_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
        idempotency_key_hash,
        state: GatewayUploadSessionState::Open,
        expected_digest: expected_digest.map(str::to_string),
        received_bytes: 0,
        staged_parts: Vec::new(),
        started_at: now.to_rfc3339(),
        expires_at: (now + Duration::seconds(ttl_seconds)).to_rfc3339(),
        completed_at: None,
        started_by_principal,
        committed_digest: None,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    let session_ref_name = gateway_upload_ref_name(&record)?;
    let object_ref = write_gateway_logical_file(
        &store,
        "registry_upload_session",
        1,
        session_ref_name.clone(),
        serde_json::to_vec_pretty(&record)?,
        format!(
            "gateway-upload-start:{}:{}",
            session_ref_name,
            Uuid::new_v4().simple()
        ),
    )
    .await?;
    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "gateway-upload-start:{}:{}",
                session_ref_name,
                Uuid::new_v4().simple()
            ),
            scope_partition: gateway_partition_id(
                record.tenant_id,
                &record.gateway,
                &record.registry_instance_id,
                &record.repository,
            ),
            committed_by_principal: record.started_by_principal.clone(),
            preconditions: vec![
                CoreMutationPrecondition::Ref {
                    ref_name: session_ref_name.clone(),
                    expected_generation: None,
                    expected_target: None,
                    require_absent: true,
                    require_present: false,
                    fence: None,
                    authz_revision: None,
                    source_watch_cursor: None,
                },
                CoreMutationPrecondition::Ref {
                    ref_name: idempotency_ref_name.clone(),
                    expected_generation: None,
                    expected_target: None,
                    require_absent: true,
                    require_present: false,
                    fence: None,
                    authz_revision: None,
                    source_watch_cursor: None,
                },
            ],
            operations: vec![
                CoreMutationOperation::RefUpdate {
                    partition_id: gateway_partition_id(
                        record.tenant_id,
                        &record.gateway,
                        &record.registry_instance_id,
                        &record.repository,
                    ),
                    ref_name: session_ref_name,
                    new_target: encode_core_object_ref_target(&object_ref)?,
                },
                CoreMutationOperation::RefUpdate {
                    partition_id: gateway_partition_id(
                        record.tenant_id,
                        &record.gateway,
                        &record.registry_instance_id,
                        &record.repository,
                    ),
                    ref_name: idempotency_ref_name.clone(),
                    new_target: encode_core_object_ref_target(&object_ref)?,
                },
            ],
        })
        .await;
    if let Err(error) = receipt {
        if let Some(ref_value) = store.read_ref(&idempotency_ref_name).await? {
            let existing = read_upload_session_from_ref_value(&store, &ref_value).await?;
            if existing.expected_digest.as_deref() == expected_digest {
                return Ok(existing);
            }
        }
        return Err(error);
    }
    Ok(record)
}

#[allow(clippy::too_many_arguments)]
pub async fn read_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<Option<(GatewayUploadSessionRecord, CoreRefValue)>> {
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let upload_id = normalize_gateway_identifier(upload_id, "upload id")?;
    let ref_name = gateway_upload_ref_name_parts(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &upload_id,
    )?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let record: GatewayUploadSessionRecord = serde_json::from_slice(&bytes)?;
    validate_upload_session_record(
        &record,
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &upload_id,
    )?;
    Ok(Some((record, ref_value)))
}

#[allow(clippy::too_many_arguments)]
pub async fn abort_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<GatewayUploadSessionReceipt> {
    let Some((mut record, ref_value)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    match record.state {
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving => {
            record.state = GatewayUploadSessionState::Aborted;
            record.record_hash.clear();
            record.record_hash = hash_record(&record)?;
            let receipt = put_record_ref(
                storage,
                &gateway_upload_ref_name(&record)?,
                &record,
                false,
                Some(ref_value.generation),
            )
            .await?;
            Ok(GatewayUploadSessionReceipt {
                record,
                generation: receipt.generation,
            })
        }
        GatewayUploadSessionState::Aborted => Ok(GatewayUploadSessionReceipt {
            record,
            generation: ref_value.generation,
        }),
        GatewayUploadSessionState::Committed
        | GatewayUploadSessionState::Expired
        | GatewayUploadSessionState::Finalising => {
            bail!("gateway upload session is not abortable")
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn expire_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<GatewayUploadSessionReceipt> {
    let Some((mut record, ref_value)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    match record.state {
        GatewayUploadSessionState::Expired => Ok(GatewayUploadSessionReceipt {
            record,
            generation: ref_value.generation,
        }),
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving
            if is_upload_session_expired(&record)? =>
        {
            record.state = GatewayUploadSessionState::Expired;
            record.record_hash.clear();
            record.record_hash = hash_record(&record)?;
            let receipt = put_record_ref(
                storage,
                &gateway_upload_ref_name(&record)?,
                &record,
                false,
                Some(ref_value.generation),
            )
            .await?;
            Ok(GatewayUploadSessionReceipt {
                record,
                generation: receipt.generation,
            })
        }
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving => {
            bail!("gateway upload session has not expired")
        }
        GatewayUploadSessionState::Committed
        | GatewayUploadSessionState::Aborted
        | GatewayUploadSessionState::Finalising => {
            bail!("gateway upload session is not expirable")
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn append_gateway_upload_part(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
    part_id: &str,
    offset: u64,
    bytes: &[u8],
    idempotency_key: &str,
) -> Result<GatewayUploadSessionReceipt> {
    let Some((mut record, ref_value)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    if is_upload_session_expired(&record)? {
        record.state = GatewayUploadSessionState::Expired;
        record.record_hash.clear();
        record.record_hash = hash_record(&record)?;
        let receipt = put_record_ref(
            storage,
            &gateway_upload_ref_name(&record)?,
            &record,
            false,
            Some(ref_value.generation),
        )
        .await?;
        return Ok(GatewayUploadSessionReceipt {
            record,
            generation: receipt.generation,
        });
    }
    if !matches!(
        record.state,
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving
    ) {
        bail!("gateway upload session is not appendable");
    }
    let part_id = normalize_gateway_identifier(part_id, "part id")?;
    if bytes.is_empty() {
        bail!("gateway upload part must not be empty");
    }
    let idempotency_key_hash = format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
    let payload_hash = format!("sha256:{}", sha256_hex(bytes));
    if let Some(existing) = record
        .staged_parts
        .iter()
        .find(|part| part.idempotency_key_hash == idempotency_key_hash)
    {
        if existing.part_id == part_id
            && existing.offset == offset
            && existing.length == bytes.len() as u64
            && existing.payload_hash == payload_hash
        {
            return Ok(GatewayUploadSessionReceipt {
                record,
                generation: ref_value.generation,
            });
        }
        bail!("gateway upload part idempotency conflict");
    }
    if record
        .staged_parts
        .iter()
        .any(|part| part.part_id == part_id)
    {
        bail!("gateway upload part id already exists");
    }
    if offset != record.received_bytes {
        bail!(
            "gateway upload part offset must equal current received_bytes {}",
            record.received_bytes
        );
    }

    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = write_gateway_logical_file(
        &store,
        "registry_upload_part",
        record.staged_parts.len() as u64 + 1,
        format!(
            "gateway_upload_part:tenant:{tenant_id}:gateway:{}:registry:{}:repository:{}:upload:{}:part:{part_id}",
            record.gateway, record.registry_instance_id, record.repository, record.upload_id
        ),
        bytes.to_vec(),
        format!(
            "gateway-upload-part:{tenant_id}:{}:{}:{}:{}:{part_id}:{idempotency_key_hash}",
            record.gateway, record.registry_instance_id, record.repository, record.upload_id
        ),
    )
    .await?;
    record.staged_parts.push(GatewayUploadPartRecord {
        schema: "anvil.gateway.upload_part.v1".to_string(),
        session_id: record.upload_id.clone(),
        part_id,
        offset,
        length: bytes.len() as u64,
        payload_hash,
        idempotency_key_hash,
        core_object_ref: object_ref,
    });
    record.received_bytes = record
        .received_bytes
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| anyhow!("gateway upload received_bytes overflow"))?;
    record.state = GatewayUploadSessionState::Receiving;
    record.record_hash.clear();
    record.record_hash = hash_record(&record)?;
    let receipt = put_record_ref(
        storage,
        &gateway_upload_ref_name(&record)?,
        &record,
        false,
        Some(ref_value.generation),
    )
    .await?;
    Ok(GatewayUploadSessionReceipt {
        record,
        generation: receipt.generation,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn finalise_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
    expected_digest: Option<&str>,
    media_type: &str,
    committed_by_principal: &str,
) -> Result<GatewayBlobRecord> {
    let Some((session, session_ref)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    if is_upload_session_expired(&session)? {
        bail!("gateway upload session has expired");
    }
    if session.state == GatewayUploadSessionState::Committed {
        let Some(digest) = session.committed_digest.as_deref() else {
            bail!("committed gateway upload session is missing committed digest");
        };
        let Some((record, _bytes)) = read_gateway_blob(
            storage,
            tenant_id,
            &session.gateway,
            &session.registry_instance_id,
            &session.repository,
            digest,
        )
        .await?
        else {
            bail!("committed gateway upload blob is missing");
        };
        return Ok(record);
    }
    if !matches!(session.state, GatewayUploadSessionState::Receiving) {
        bail!("gateway upload session is not finalisable");
    }
    let target_digest = expected_digest
        .map(str::to_string)
        .or_else(|| session.expected_digest.clone())
        .ok_or_else(|| anyhow!("gateway upload finalisation requires an expected digest"))?;
    validate_gateway_digest(&target_digest)?;
    validate_media_type(media_type)?;
    let committed_by_principal = normalize_gateway_identifier(committed_by_principal, "principal")?;

    let store = CoreStore::new(storage.clone()).await?;
    let mut ordered_parts = session.staged_parts.clone();
    ordered_parts.sort_by_key(|part| part.offset);
    let mut payload = Vec::with_capacity(session.received_bytes as usize);
    let mut next_offset = 0_u64;
    for part in &ordered_parts {
        if part.offset != next_offset {
            bail!(
                "gateway upload finalisation found a gap before part {}",
                part.part_id
            );
        }
        let bytes = store
            .get_blob(GetBlob {
                object_ref: part.core_object_ref.clone(),
            })
            .await?;
        if bytes.len() as u64 != part.length
            || format!("sha256:{}", sha256_hex(&bytes)) != part.payload_hash
        {
            bail!("gateway upload part payload does not match its receipt");
        }
        next_offset = next_offset
            .checked_add(part.length)
            .ok_or_else(|| anyhow!("gateway upload finalisation length overflow"))?;
        payload.extend_from_slice(&bytes);
    }
    if next_offset != session.received_bytes {
        bail!("gateway upload finalisation length mismatch");
    }
    let actual_digest = format!("sha256:{}", sha256_hex(&payload));
    if actual_digest != target_digest {
        bail!("gateway upload digest mismatch: expected {target_digest}, got {actual_digest}");
    }

    let gateway = session.gateway.clone();
    let registry_instance_id = session.registry_instance_id.clone();
    let repository = session.repository.clone();
    let blob_ref_name = gateway_blob_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &target_digest,
    )?;
    if let Some(existing) = read_record_ref::<GatewayBlobRecord>(storage, &blob_ref_name).await? {
        validate_blob_record(
            &existing,
            tenant_id,
            &gateway,
            &registry_instance_id,
            &repository,
            &target_digest,
        )?;
        return commit_upload_session_record(store, session, session_ref, &target_digest, None)
            .await
            .map(|_| existing);
    }

    let payload_ref = write_gateway_logical_file(
        &store,
        "registry_blob",
        1,
        blob_ref_name.clone(),
        payload,
        format!(
            "gateway-upload-finalise:{tenant_id}:{gateway}:{registry_instance_id}:{repository}:{target_digest}"
        ),
    )
    .await?;
    let mut blob_record = GatewayBlobRecord {
        schema: GATEWAY_BLOB_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        digest: target_digest.clone(),
        media_type: media_type.to_string(),
        size_bytes: session.received_bytes,
        object_ref: payload_ref,
        created_at: now_rfc3339(),
        created_by_principal: committed_by_principal,
        record_hash: String::new(),
    };
    blob_record.record_hash = hash_record(&blob_record)?;
    let blob_record_ref = write_gateway_logical_file(
        &store,
        "registry_metadata",
        1,
        blob_ref_name.clone(),
        serde_json::to_vec_pretty(&blob_record)?,
        format!(
            "gateway-blob-record:{blob_ref_name}:{}",
            Uuid::new_v4().simple()
        ),
    )
    .await?;
    commit_upload_session_record(
        store,
        session,
        session_ref,
        &target_digest,
        Some((blob_ref_name, blob_record_ref)),
    )
    .await?;
    Ok(blob_record)
}

pub async fn put_gateway_credential_record(
    storage: &Storage,
    mut record: GatewayCredentialRecord,
    expected_generation: Option<u64>,
) -> Result<u64> {
    record.record_hash.clear();
    validate_credential_record_shape(&record)?;
    record.record_hash = hash_record(&record)?;
    let ref_name = gateway_credential_ref_name(&record)?;
    let receipt = put_record_ref(storage, &ref_name, &record, false, expected_generation).await?;
    Ok(receipt.generation)
}

pub async fn read_gateway_credential_record(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    credential_id: &str,
) -> Result<Option<(GatewayCredentialRecord, CoreRefValue)>> {
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let credential_id = normalize_gateway_identifier(credential_id, "credential id")?;
    let ref_name = gateway_credential_ref_name_parts(tenant_id, &gateway, &credential_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let record: GatewayCredentialRecord = serde_json::from_slice(&bytes)?;
    if record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.credential_id != credential_id
    {
        bail!("gateway credential record scope mismatch");
    }
    validate_credential_record_shape(&record)?;
    Ok(Some((record, ref_value)))
}

pub async fn revoke_gateway_credential_record(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    credential_id: &str,
    expected_generation: u64,
) -> Result<u64> {
    let Some((mut record, _ref_value)) =
        read_gateway_credential_record(storage, tenant_id, gateway, credential_id).await?
    else {
        bail!("gateway credential record not found");
    };
    if record.revoked_at.is_none() {
        record.revoked_at = Some(now_rfc3339());
    }
    put_gateway_credential_record(storage, record, Some(expected_generation)).await
}

pub fn hash_gateway_credential_secret(secret: &str) -> Result<String> {
    validate_gateway_secret(secret)?;
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|error| anyhow!("failed to hash gateway credential secret: {error}"))?;
    Ok(hash.to_string())
}

#[allow(clippy::too_many_arguments)]
pub async fn issue_gateway_access_token(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    credential_id: &str,
    presented_secret: &str,
    requested_actions: &[String],
    requested_ttl_seconds: i64,
    signing_secret: &str,
) -> Result<GatewayAccessToken> {
    validate_tenant(tenant_id)?;
    validate_gateway_secret(signing_secret)?;
    validate_gateway_secret(presented_secret)?;
    if requested_ttl_seconds <= 0 {
        bail!("gateway token ttl must be positive");
    }
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let credential_id = normalize_gateway_identifier(credential_id, "credential id")?;
    let actions = normalize_gateway_actions(requested_actions)?;
    if actions.is_empty() {
        bail!("gateway token requires at least one action");
    }
    let Some((credential, ref_value)) =
        read_gateway_credential_record(storage, tenant_id, &gateway, &credential_id).await?
    else {
        bail!("gateway credential not found");
    };
    if credential.revoked_at.is_some() {
        bail!("gateway credential has been revoked");
    }
    verify_gateway_credential_secret(&credential.secret_hash, presented_secret)?;

    let now = Utc::now().timestamp();
    let ttl = requested_ttl_seconds.min(GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS);
    let claims = GatewayAccessTokenClaims {
        token_kind: GATEWAY_ACCESS_TOKEN_KIND.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        actions,
        subject_principal: credential.subject_principal,
        credential_id,
        credential_generation: ref_value.generation,
        iat: now,
        exp: (now + ttl) as usize,
        jti: Uuid::new_v4().to_string(),
    };
    let access_token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(signing_secret.as_bytes()),
    )?;
    Ok(GatewayAccessToken {
        access_token,
        token_type: "Bearer".to_string(),
        expires_in_seconds: ttl,
        claims,
    })
}

pub async fn validate_gateway_access_token(
    storage: &Storage,
    token: &str,
    signing_secret: &str,
    requirement: Option<&GatewayTokenRequirement>,
) -> Result<GatewayAccessTokenClaims> {
    validate_gateway_secret(signing_secret)?;
    let claims = decode::<GatewayAccessTokenClaims>(
        token,
        &DecodingKey::from_secret(signing_secret.as_bytes()),
        &Validation::default(),
    )?
    .claims;
    if claims.token_kind != GATEWAY_ACCESS_TOKEN_KIND {
        bail!("gateway token kind mismatch");
    }
    validate_gateway_token_claim_shape(&claims)?;
    if let Some(requirement) = requirement {
        validate_gateway_token_requirement(requirement)?;
        if claims.tenant_id != requirement.tenant_id
            || claims.gateway != requirement.gateway
            || claims.registry_instance_id != requirement.registry_instance_id
            || claims.repository != requirement.repository
            || !claims
                .actions
                .iter()
                .any(|action| action == &requirement.action)
        {
            bail!("gateway token does not satisfy route requirement");
        }
    }

    let Some((credential, ref_value)) = read_gateway_credential_record(
        storage,
        claims.tenant_id,
        &claims.gateway,
        &claims.credential_id,
    )
    .await?
    else {
        bail!("gateway credential not found");
    };
    if ref_value.generation != claims.credential_generation
        || credential.subject_principal != claims.subject_principal
    {
        bail!("gateway credential changed after token issue");
    }
    if credential.revoked_at.is_some() {
        bail!("gateway credential has been revoked");
    }
    Ok(claims)
}

pub async fn put_gateway_mount_record(
    storage: &Storage,
    mut record: GatewayMountRecord,
    expected_generation: Option<u64>,
) -> Result<u64> {
    record.record_hash.clear();
    record.generation = expected_generation.unwrap_or(0).saturating_add(1);
    validate_mount_record_shape(&record)?;
    record.record_hash = hash_record(&record)?;
    let ref_name = gateway_mount_ref_name(&record)?;
    let receipt = put_record_ref(storage, &ref_name, &record, false, expected_generation).await?;
    Ok(receipt.generation)
}

pub async fn read_gateway_mount_record(
    storage: &Storage,
    mount_id: &str,
) -> Result<Option<(GatewayMountRecord, CoreRefValue)>> {
    let mount_id = normalize_gateway_identifier(mount_id, "mount id")?;
    let ref_name = gateway_mount_ref_name_parts(&mount_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let record: GatewayMountRecord = serde_json::from_slice(&bytes)?;
    if record.mount_id != mount_id {
        bail!("gateway mount record scope mismatch");
    }
    validate_mount_record_shape(&record)?;
    Ok(Some((record, ref_value)))
}

pub async fn resolve_gateway_mount(
    storage: &Storage,
    host: &str,
    path: &str,
) -> Result<Option<GatewayMountResolution>> {
    let host = normalize_gateway_host(host)?;
    let path = normalize_gateway_path(path)?;
    let mounts = list_gateway_mount_records(storage).await?;

    if let Some(resolution) =
        best_gateway_mount_match(&mounts, &host, &path, GatewayMountMatchKind::ExactHostAlias)
    {
        return Ok(Some(resolution));
    }
    if let Some(resolution) = best_gateway_mount_match(
        &mounts,
        &host,
        &path,
        GatewayMountMatchKind::VirtualHostRegional,
    ) {
        return Ok(Some(resolution));
    }
    Ok(best_gateway_mount_match(
        &mounts,
        &host,
        &path,
        GatewayMountMatchKind::PathStyleRegional,
    ))
}

#[allow(clippy::too_many_arguments)]
pub async fn append_gateway_audit_record(
    storage: &Storage,
    mut record: GatewayAuditRecord,
    idempotency_key: Option<&str>,
) -> Result<GatewayAuditAppendReceipt> {
    record.record_hash.clear();
    normalise_gateway_audit_record(&mut record)?;
    let stream_id = gateway_audit_stream_id(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
    )?;
    let partition_id = gateway_audit_partition_id(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
    )?;
    let store = CoreStore::new(storage.clone()).await?;
    if let Some(idempotency_key) = idempotency_key {
        let idempotency_key_hash = format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
        for stream in store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence: 0,
                limit: 0,
            })
            .await?
        {
            if stream.idempotency_key_hash.as_deref() != Some(idempotency_key_hash.as_str()) {
                continue;
            }
            let existing: GatewayAuditRecord = serde_json::from_slice(&stream.payload)?;
            validate_gateway_audit_record(&existing)?;
            if record.created_at.is_empty() {
                record.created_at = existing.created_at.clone();
            }
            record.record_hash = hash_record(&record)?;
            validate_gateway_audit_record(&record)?;
            if existing != record {
                bail!("gateway audit idempotency conflict");
            }
            return Ok(GatewayAuditAppendReceipt {
                record: existing,
                stream: StreamAppendReceipt {
                    stream_id: stream.stream_id,
                    sequence: stream.sequence,
                    cursor: stream.cursor,
                    event_hash: stream.event_hash,
                    idempotent_replay: true,
                },
            });
        }
    }
    if record.created_at.is_empty() {
        record.created_at = now_rfc3339();
    }
    record.record_hash = hash_record(&record)?;
    validate_gateway_audit_record(&record)?;

    let stream = store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id,
            record_kind: GATEWAY_AUDIT_SCHEMA.to_string(),
            payload: serde_json::to_vec(&record)?,
            fence: None,
            transaction_id: None,
            idempotency_key: idempotency_key.map(str::to_string),
        })
        .await?;
    Ok(GatewayAuditAppendReceipt { record, stream })
}

pub async fn read_gateway_audit_records(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    after_sequence: u64,
    limit: usize,
) -> Result<Vec<GatewayAuditStreamRecord>> {
    validate_tenant(tenant_id)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let records = CoreStore::new(storage.clone())
        .await?
        .read_stream(ReadStream {
            stream_id: gateway_audit_stream_id(tenant_id, &gateway, &registry_instance_id)?,
            after_sequence,
            limit,
        })
        .await?;
    let mut audited = Vec::with_capacity(records.len());
    for stream in records {
        if stream.record_kind != GATEWAY_AUDIT_SCHEMA {
            bail!("gateway audit stream contains unexpected record kind");
        }
        let audit: GatewayAuditRecord = serde_json::from_slice(&stream.payload)?;
        if audit.tenant_id != tenant_id
            || audit.gateway != gateway
            || audit.registry_instance_id != registry_instance_id
        {
            bail!("gateway audit record scope mismatch");
        }
        validate_gateway_audit_record(&audit)?;
        audited.push(GatewayAuditStreamRecord { audit, stream });
    }
    Ok(audited)
}

async fn list_gateway_mount_records(
    storage: &Storage,
) -> Result<Vec<(GatewayMountRecord, CoreRefValue)>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut mounts = Vec::new();
    for ref_name in store.list_ref_names("gateway_mount:mount:").await? {
        let Some(ref_value) = store.read_ref(&ref_name).await? else {
            continue;
        };
        let bytes = store
            .get_blob(GetBlob {
                object_ref: decode_core_object_ref_target(&ref_value.target)?,
            })
            .await?;
        let record: GatewayMountRecord = serde_json::from_slice(&bytes)?;
        validate_mount_record_shape(&record)?;
        mounts.push((record, ref_value));
    }
    Ok(mounts)
}

fn best_gateway_mount_match(
    mounts: &[(GatewayMountRecord, CoreRefValue)],
    host: &str,
    path: &str,
    match_kind: GatewayMountMatchKind,
) -> Option<GatewayMountResolution> {
    mounts
        .iter()
        .filter_map(|(record, ref_value)| {
            if record.state != GatewayMountState::Active {
                return None;
            }
            let matched_prefix = match match_kind {
                GatewayMountMatchKind::ExactHostAlias => {
                    if !record.hosts.iter().any(|candidate| candidate == host) {
                        return None;
                    }
                    best_configured_path_prefix(record, path)?
                }
                GatewayMountMatchKind::VirtualHostRegional => {
                    if host != virtual_host_regional_name(record) {
                        return None;
                    }
                    "/".to_string()
                }
                GatewayMountMatchKind::PathStyleRegional => {
                    if host != regional_gateway_host(record) {
                        return None;
                    }
                    let prefix = path_style_gateway_prefix(record);
                    if !path.starts_with(&prefix) {
                        return None;
                    }
                    prefix
                }
            };
            Some(GatewayMountResolution {
                record: record.clone(),
                ref_generation: ref_value.generation,
                matched_host: host.to_string(),
                matched_path_prefix: matched_prefix,
                match_kind,
            })
        })
        .max_by_key(|resolution| resolution.matched_path_prefix.len())
}

fn best_configured_path_prefix(record: &GatewayMountRecord, path: &str) -> Option<String> {
    record
        .path_prefixes
        .iter()
        .filter(|prefix| path.starts_with(prefix.as_str()))
        .max_by_key(|prefix| prefix.len())
        .cloned()
}

fn virtual_host_regional_name(record: &GatewayMountRecord) -> String {
    format!(
        "{}.{}.{}{}",
        record.registry_instance_id, record.tenant_id, record.region, REGIONAL_GATEWAY_SUFFIX
    )
}

fn regional_gateway_host(record: &GatewayMountRecord) -> String {
    format!("{}{}", record.region, REGIONAL_GATEWAY_SUFFIX)
}

fn path_style_gateway_prefix(record: &GatewayMountRecord) -> String {
    format!(
        "/{}/_gateway/{}/{}/",
        record.tenant_id, record.gateway, record.registry_instance_id
    )
}

async fn commit_upload_session_record(
    store: CoreStore,
    mut session: GatewayUploadSessionRecord,
    session_ref: CoreRefValue,
    committed_digest: &str,
    blob_record_ref: Option<(String, CoreObjectRef)>,
) -> Result<GatewayUploadSessionReceipt> {
    session.state = GatewayUploadSessionState::Committed;
    session.committed_digest = Some(committed_digest.to_string());
    session.completed_at = Some(now_rfc3339());
    session.record_hash.clear();
    session.record_hash = hash_record(&session)?;
    let session_ref_name = gateway_upload_ref_name(&session)?;
    let session_object_ref = write_gateway_logical_file(
        &store,
        "registry_upload_session",
        session_ref.generation + 1,
        session_ref_name.clone(),
        serde_json::to_vec_pretty(&session)?,
        format!(
            "gateway-upload-commit:{}:{}",
            session_ref_name,
            Uuid::new_v4().simple()
        ),
    )
    .await?;
    let mut preconditions = vec![CoreMutationPrecondition::Ref {
        ref_name: session_ref_name.clone(),
        expected_generation: Some(session_ref.generation),
        expected_target: Some(session_ref.target),
        require_absent: false,
        require_present: true,
        fence: None,
        authz_revision: None,
        source_watch_cursor: None,
    }];
    let mut operations = vec![CoreMutationOperation::RefUpdate {
        partition_id: gateway_partition_id(
            session.tenant_id,
            &session.gateway,
            &session.registry_instance_id,
            &session.repository,
        ),
        ref_name: session_ref_name.clone(),
        new_target: encode_core_object_ref_target(&session_object_ref)?,
    }];
    if let Some((blob_ref_name, object_ref)) = blob_record_ref {
        preconditions.push(CoreMutationPrecondition::Ref {
            ref_name: blob_ref_name.clone(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
        });
        operations.push(CoreMutationOperation::RefUpdate {
            partition_id: gateway_partition_id(
                session.tenant_id,
                &session.gateway,
                &session.registry_instance_id,
                &session.repository,
            ),
            ref_name: blob_ref_name,
            new_target: encode_core_object_ref_target(&object_ref)?,
        });
    }
    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "gateway-upload-commit:{}:{}",
                session.upload_id,
                Uuid::new_v4().simple()
            ),
            scope_partition: gateway_partition_id(
                session.tenant_id,
                &session.gateway,
                &session.registry_instance_id,
                &session.repository,
            ),
            committed_by_principal: session.started_by_principal.clone(),
            preconditions,
            operations,
        })
        .await?;
    let generation = receipt
        .visible_updates
        .iter()
        .find_map(|update| match update {
            crate::core_store::CoreTransactionUpdate::CoreRefUpdate {
                ref_name,
                new_generation,
            } if ref_name == &session_ref_name => Some(*new_generation),
            _ => None,
        })
        .ok_or_else(|| anyhow!("gateway upload commit did not update session ref"))?;
    Ok(GatewayUploadSessionReceipt {
        record: session,
        generation,
    })
}

async fn write_gateway_logical_file(
    store: &CoreStore,
    writer_family: &str,
    generation: u64,
    logical_file_id: String,
    source: Vec<u8>,
    mutation_id: String,
) -> Result<CoreObjectRef> {
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: writer_family.to_string(),
            generation,
            logical_file_id,
            source,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id,
            region_id: "local".to_string(),
        })
        .await?;
    Ok(core_object_ref_from_logical_file_manifest(&manifest))
}

async fn put_record_ref<T: Serialize>(
    storage: &Storage,
    ref_name: &str,
    record: &T,
    require_absent: bool,
    expected_generation: Option<u64>,
) -> Result<crate::core_store::CasRefReceipt> {
    let store = CoreStore::new(storage.clone()).await?;
    let current = store.read_ref(ref_name).await?;
    if require_absent && current.is_some() {
        bail!("CoreStore gateway ref {ref_name} already exists");
    }
    if let Some(expected_generation) = expected_generation {
        let actual = current.as_ref().map(|value| value.generation);
        if actual != Some(expected_generation) {
            bail!("CoreStore gateway ref {ref_name} generation mismatch");
        }
    }
    let object_ref = write_gateway_logical_file(
        &store,
        "registry_metadata",
        current
            .as_ref()
            .map(|value| value.generation + 1)
            .unwrap_or(1),
        ref_name.to_string(),
        serde_json::to_vec_pretty(record)?,
        format!("gateway-record:{ref_name}:{}", Uuid::new_v4().simple()),
    )
    .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.to_string(),
            expected_generation: current.as_ref().map(|value| value.generation),
            expected_target: current.as_ref().map(|value| value.target.clone()),
            require_absent: current.is_none(),
            require_present: current.is_some(),
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await
}

async fn read_record_ref<T: for<'de> Deserialize<'de>>(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<T>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

async fn read_upload_session_from_ref_value(
    store: &CoreStore,
    ref_value: &CoreRefValue,
) -> Result<GatewayUploadSessionRecord> {
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    Ok(serde_json::from_slice(&bytes)?)
}

struct GatewayRepositoryKey {
    tenant_id: i64,
    gateway: String,
    registry_instance_id: String,
    repository: String,
}

impl GatewayRepositoryKey {
    fn new(
        tenant_id: i64,
        gateway: &str,
        registry_instance_id: &str,
        repository: &str,
    ) -> Result<Self> {
        validate_tenant(tenant_id)?;
        Ok(Self {
            tenant_id,
            gateway: normalize_gateway_identifier(gateway, "gateway")?,
            registry_instance_id: normalize_gateway_identifier(registry_instance_id, "registry")?,
            repository: normalize_gateway_identifier(repository, "repository")?,
        })
    }

    fn ref_name(&self) -> String {
        format!(
            "gateway_repository:tenant:{}:gateway:{}:registry:{}:repository:{}",
            self.tenant_id, self.gateway, self.registry_instance_id, self.repository
        )
    }
}

fn gateway_repository_ref_name(record: &GatewayRepositoryRecord) -> Result<String> {
    Ok(GatewayRepositoryKey::new(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.repository,
    )?
    .ref_name())
}

fn gateway_blob_ref_name(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    validate_gateway_digest(digest)?;
    Ok(format!(
        "gateway_blob:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:digest:{digest}"
    ))
}

fn gateway_tag_ref_name(record: &GatewayTagRecord) -> Result<String> {
    gateway_tag_ref_name_parts(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.repository,
        &record.tag,
    )
}

fn gateway_tag_ref_name_parts(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_tag:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:tag:{tag}"
    ))
}

fn gateway_upload_ref_name(record: &GatewayUploadSessionRecord) -> Result<String> {
    validate_tenant(record.tenant_id)?;
    gateway_upload_ref_name_parts(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.repository,
        &record.upload_id,
    )
}

fn gateway_upload_ref_name_parts(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_upload_session:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:upload:{upload_id}"
    ))
}

fn gateway_upload_idempotency_ref_name(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    idempotency_key_hash: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    validate_gateway_digest(idempotency_key_hash)?;
    Ok(format!(
        "gateway_upload_idempotency:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:idempotency:{idempotency_key_hash}"
    ))
}

fn gateway_credential_ref_name(record: &GatewayCredentialRecord) -> Result<String> {
    validate_tenant(record.tenant_id)?;
    gateway_credential_ref_name_parts(record.tenant_id, &record.gateway, &record.credential_id)
}

fn gateway_credential_ref_name_parts(
    tenant_id: i64,
    gateway: &str,
    credential_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_credential:tenant:{tenant_id}:gateway:{gateway}:credential:{credential_id}"
    ))
}

fn gateway_mount_ref_name(record: &GatewayMountRecord) -> Result<String> {
    gateway_mount_ref_name_parts(&record.mount_id)
}

fn gateway_mount_ref_name_parts(mount_id: &str) -> Result<String> {
    let mount_id = normalize_gateway_identifier(mount_id, "mount id")?;
    Ok(format!("gateway_mount:mount:{mount_id}"))
}

fn gateway_partition_id(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
) -> String {
    format!(
        "gateway:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}"
    )
}

fn gateway_audit_partition_id(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_audit:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}"
    ))
}

fn gateway_audit_stream_id(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_audit:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}"
    ))
}

fn validate_repository_record(
    record: &GatewayRepositoryRecord,
    key: &GatewayRepositoryKey,
) -> Result<()> {
    if record.schema != GATEWAY_REPOSITORY_SCHEMA
        || record.tenant_id != key.tenant_id
        || record.gateway != key.gateway
        || record.registry_instance_id != key.registry_instance_id
        || record.repository != key.repository
    {
        bail!("gateway repository record scope mismatch");
    }
    validate_hash(record, &record.record_hash)
}

fn validate_blob_record(
    record: &GatewayBlobRecord,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
) -> Result<()> {
    if record.schema != GATEWAY_BLOB_SCHEMA
        || record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.registry_instance_id != registry_instance_id
        || record.repository != repository
        || record.digest != digest
    {
        bail!("gateway blob record scope mismatch");
    }
    validate_gateway_digest(&record.digest)?;
    validate_media_type(&record.media_type)?;
    validate_hash(record, &record.record_hash)
}

fn validate_tag_record(
    record: &GatewayTagRecord,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
) -> Result<()> {
    if record.schema != GATEWAY_TAG_SCHEMA
        || record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.registry_instance_id != registry_instance_id
        || record.repository != repository
        || record.tag != tag
    {
        bail!("gateway tag record scope mismatch");
    }
    validate_gateway_digest(&record.target_digest)?;
    validate_hash(record, &record.record_hash)
}

fn validate_upload_session_record(
    record: &GatewayUploadSessionRecord,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<()> {
    if record.schema != GATEWAY_UPLOAD_SESSION_SCHEMA
        || record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.registry_instance_id != registry_instance_id
        || record.repository != repository
        || record.upload_id != upload_id
    {
        bail!("gateway upload session record scope mismatch");
    }
    if let Some(digest) = record.expected_digest.as_deref() {
        validate_gateway_digest(digest)?;
    }
    if let Some(digest) = record.committed_digest.as_deref() {
        validate_gateway_digest(digest)?;
    }
    validate_gateway_digest(&record.idempotency_key_hash)?;
    let mut next_offset = 0_u64;
    for part in &record.staged_parts {
        if part.schema != "anvil.gateway.upload_part.v1" || part.session_id != record.upload_id {
            bail!("gateway upload part scope mismatch");
        }
        normalize_gateway_identifier(&part.part_id, "part id")?;
        validate_gateway_digest(&part.payload_hash)?;
        validate_gateway_digest(&part.idempotency_key_hash)?;
        if part.offset != next_offset {
            bail!("gateway upload part offsets must be contiguous");
        }
        next_offset = next_offset
            .checked_add(part.length)
            .ok_or_else(|| anyhow!("gateway upload part offset overflow"))?;
    }
    if next_offset != record.received_bytes {
        bail!("gateway upload session received_bytes mismatch");
    }
    DateTime::parse_from_rfc3339(&record.started_at)
        .map_err(|error| anyhow!("gateway upload session started_at is invalid: {error}"))?;
    DateTime::parse_from_rfc3339(&record.expires_at)
        .map_err(|error| anyhow!("gateway upload session expires_at is invalid: {error}"))?;
    if let Some(completed_at) = record.completed_at.as_deref() {
        DateTime::parse_from_rfc3339(completed_at)
            .map_err(|error| anyhow!("gateway upload session completed_at is invalid: {error}"))?;
    }
    validate_hash(record, &record.record_hash)
}

fn is_upload_session_expired(record: &GatewayUploadSessionRecord) -> Result<bool> {
    if matches!(
        record.state,
        GatewayUploadSessionState::Committed
            | GatewayUploadSessionState::Aborted
            | GatewayUploadSessionState::Expired
    ) {
        return Ok(false);
    }
    let expires_at = DateTime::parse_from_rfc3339(&record.expires_at)
        .map_err(|error| anyhow!("gateway upload session expires_at is invalid: {error}"))?
        .with_timezone(&Utc);
    Ok(expires_at <= Utc::now())
}

fn validate_credential_record_shape(record: &GatewayCredentialRecord) -> Result<()> {
    if record.schema != GATEWAY_CREDENTIAL_SCHEMA {
        bail!("gateway credential record schema mismatch");
    }
    validate_tenant(record.tenant_id)?;
    normalize_gateway_identifier(&record.credential_id, "credential id")?;
    normalize_gateway_identifier(&record.gateway, "gateway")?;
    normalize_gateway_identifier(&record.subject_principal, "principal")?;
    if record.secret_hash.is_empty() || record.secret_hash.contains(char::is_control) {
        bail!("gateway credential secret_hash must be non-empty and safe");
    }
    if !record.record_hash.is_empty() {
        validate_hash(record, &record.record_hash)?;
    }
    Ok(())
}

fn validate_mount_record_shape(record: &GatewayMountRecord) -> Result<()> {
    if record.schema != GATEWAY_MOUNT_SCHEMA {
        bail!("gateway mount record schema mismatch");
    }
    normalize_gateway_identifier(&record.mount_id, "mount id")?;
    normalize_gateway_identifier(&record.gateway, "gateway")?;
    validate_gateway_slug(&record.mesh_id, "mesh id")?;
    validate_gateway_slug(&record.region, "region")?;
    validate_gateway_slug(&record.anvil_storage_tenant_id, "anvil storage tenant id")?;
    validate_gateway_slug(
        &record.authz_scope.anvil_storage_tenant_id,
        "authz tenant id",
    )?;
    validate_gateway_slug(&record.authz_scope.authz_realm_id, "authz realm id")?;
    if record.authz_scope.anvil_storage_tenant_id != record.anvil_storage_tenant_id {
        bail!("gateway mount authz scope tenant mismatch");
    }
    validate_gateway_slug(&record.tenant_id, "tenant id")?;
    normalize_gateway_identifier(&record.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&record.default_bucket, "default bucket")?;
    validate_gateway_repository_prefix(&record.repository_prefix)?;
    if record.generation == 0 {
        bail!("gateway mount generation must be positive");
    }
    if record.hosts.is_empty() && record.path_prefixes.is_empty() {
        bail!("gateway mount must define at least one host or path prefix");
    }
    for host in &record.hosts {
        if normalize_gateway_host(host)? != *host {
            bail!("gateway mount host must be canonical");
        }
    }
    for prefix in &record.path_prefixes {
        validate_gateway_path_prefix(prefix)?;
    }
    if !record.record_hash.is_empty() {
        validate_hash(record, &record.record_hash)?;
    }
    Ok(())
}

fn normalise_gateway_audit_record(record: &mut GatewayAuditRecord) -> Result<()> {
    record.schema = GATEWAY_AUDIT_SCHEMA.to_string();
    validate_tenant(record.tenant_id)?;
    record.gateway = normalize_gateway_identifier(&record.gateway, "gateway")?;
    record.registry_instance_id =
        normalize_gateway_identifier(&record.registry_instance_id, "registry")?;
    record.operation = normalize_gateway_identifier(&record.operation, "operation")?;
    record.repository = normalize_gateway_identifier(&record.repository, "repository")?;
    record.package = record
        .package
        .as_deref()
        .map(|package| normalize_gateway_identifier(package, "package"))
        .transpose()?;
    record.version_or_reference = record
        .version_or_reference
        .as_deref()
        .map(|reference| normalize_gateway_identifier(reference, "version or reference"))
        .transpose()?;
    record.subject_principal =
        normalize_gateway_identifier(&record.subject_principal, "principal")?;
    record.credential_id = record
        .credential_id
        .as_deref()
        .map(|credential_id| normalize_gateway_identifier(credential_id, "credential id"))
        .transpose()?;
    record.request_id = normalize_gateway_identifier(&record.request_id, "request id")?;
    record.result = normalize_gateway_identifier(&record.result, "result")?;
    Ok(())
}

fn validate_gateway_audit_record(record: &GatewayAuditRecord) -> Result<()> {
    if record.schema != GATEWAY_AUDIT_SCHEMA {
        bail!("gateway audit record schema mismatch");
    }
    validate_tenant(record.tenant_id)?;
    normalize_gateway_identifier(&record.gateway, "gateway")?;
    normalize_gateway_identifier(&record.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&record.operation, "operation")?;
    normalize_gateway_identifier(&record.repository, "repository")?;
    if let Some(package) = record.package.as_deref() {
        normalize_gateway_identifier(package, "package")?;
    }
    if let Some(reference) = record.version_or_reference.as_deref() {
        normalize_gateway_identifier(reference, "version or reference")?;
    }
    if let Some(digest) = record.digest.as_deref() {
        validate_gateway_digest(digest)?;
    }
    normalize_gateway_identifier(&record.subject_principal, "principal")?;
    if let Some(credential_id) = record.credential_id.as_deref() {
        normalize_gateway_identifier(credential_id, "credential id")?;
    }
    normalize_gateway_identifier(&record.request_id, "request id")?;
    normalize_gateway_identifier(&record.result, "result")?;
    DateTime::parse_from_rfc3339(&record.created_at)
        .map_err(|error| anyhow!("gateway audit created_at is invalid: {error}"))?;
    validate_hash(record, &record.record_hash)
}

fn normalize_gateway_actions(actions: &[String]) -> Result<Vec<String>> {
    let mut normalized = Vec::with_capacity(actions.len());
    for action in actions {
        let action = normalize_gateway_identifier(action, "action")?;
        if !normalized.iter().any(|existing| existing == &action) {
            normalized.push(action);
        }
    }
    normalized.sort();
    Ok(normalized)
}

fn validate_gateway_token_claim_shape(claims: &GatewayAccessTokenClaims) -> Result<()> {
    validate_tenant(claims.tenant_id)?;
    normalize_gateway_identifier(&claims.gateway, "gateway")?;
    normalize_gateway_identifier(&claims.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&claims.repository, "repository")?;
    normalize_gateway_actions(&claims.actions)?;
    if claims.actions.is_empty() {
        bail!("gateway token must contain at least one action");
    }
    normalize_gateway_identifier(&claims.subject_principal, "principal")?;
    normalize_gateway_identifier(&claims.credential_id, "credential id")?;
    if claims.credential_generation == 0 {
        bail!("gateway token credential generation must be nonzero");
    }
    if claims.iat <= 0 || claims.exp == 0 {
        bail!("gateway token time fields are invalid");
    }
    normalize_gateway_identifier(&claims.jti, "token id")?;
    Ok(())
}

fn validate_gateway_token_requirement(requirement: &GatewayTokenRequirement) -> Result<()> {
    validate_tenant(requirement.tenant_id)?;
    normalize_gateway_identifier(&requirement.gateway, "gateway")?;
    normalize_gateway_identifier(&requirement.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&requirement.repository, "repository")?;
    normalize_gateway_identifier(&requirement.action, "action")?;
    Ok(())
}

fn validate_gateway_secret(secret: &str) -> Result<()> {
    if secret.is_empty() || secret.contains('\0') || secret.chars().any(char::is_control) {
        bail!("gateway secret is invalid");
    }
    Ok(())
}

fn verify_gateway_credential_secret(secret_hash: &str, presented_secret: &str) -> Result<()> {
    validate_gateway_secret(presented_secret)?;
    if secret_hash.starts_with("$argon2") {
        let parsed = PasswordHash::new(secret_hash)
            .map_err(|_| anyhow!("gateway credential secret hash is invalid"))?;
        Argon2::default()
            .verify_password(presented_secret.as_bytes(), &parsed)
            .map_err(|_| anyhow!("gateway credential secret mismatch"))?;
        return Ok(());
    }
    if let Some(expected) = secret_hash.strip_prefix("sha256:") {
        let actual = sha256_hex(presented_secret.as_bytes());
        if expected.len() == actual.len()
            && constant_time_eq::constant_time_eq(expected.as_bytes(), actual.as_bytes())
        {
            return Ok(());
        }
    }
    bail!("gateway credential secret mismatch")
}

fn validate_gateway_slug(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > 255 {
        bail!("gateway {label} length is invalid");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("gateway {label} contains an unsafe character");
    }
    Ok(())
}

fn validate_gateway_repository_prefix(value: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    normalize_gateway_identifier(value, "repository prefix")?;
    Ok(())
}

fn normalize_gateway_host(input: &str) -> Result<String> {
    let host = input.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 {
        bail!("gateway host length is invalid");
    }
    if host.contains("..") || host.starts_with('.') || host.ends_with('.') {
        bail!("gateway host has an empty label");
    }
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            bail!("gateway host label length is invalid");
        }
        if label.starts_with('-') || label.ends_with('-') {
            bail!("gateway host label hyphen placement is invalid");
        }
        if !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            bail!("gateway host contains an unsafe character");
        }
    }
    Ok(host)
}

fn normalize_gateway_path(input: &str) -> Result<String> {
    if !input.starts_with('/') {
        bail!("gateway path must start with /");
    }
    validate_gateway_path_prefix(input)?;
    Ok(input.to_string())
}

fn validate_gateway_path_prefix(value: &str) -> Result<()> {
    if !value.starts_with('/') {
        bail!("gateway path prefix must start with /");
    }
    if value.contains('\\') || value.contains('%') || value.contains(char::is_control) {
        bail!("gateway path prefix contains an unsafe character");
    }
    for segment in value.split('/') {
        if matches!(segment, "." | "..") {
            bail!("gateway path prefix contains an unsafe segment");
        }
    }
    Ok(())
}

fn validate_tenant(tenant_id: i64) -> Result<()> {
    if tenant_id <= 0 {
        bail!("gateway tenant id must be positive");
    }
    Ok(())
}

fn validate_media_type(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || value.contains(char::is_control)
        || value.contains('/') && value.split('/').any(str::is_empty)
    {
        bail!("gateway media type is invalid");
    }
    Ok(())
}

fn validate_hash<T: Serialize>(record: &T, actual: &str) -> Result<()> {
    let expected = hash_record(record)?;
    if expected != actual {
        bail!("gateway record hash mismatch");
    }
    Ok(())
}

fn hash_record<T: Serialize>(record: &T) -> Result<String> {
    let mut value = serde_json::to_value(record)?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "record_hash".to_string(),
            serde_json::Value::String(String::new()),
        );
    }
    Ok(hex::encode(hash32(&serde_json::to_vec(&value)?)))
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn idempotency_hash(value: &str) -> Result<String> {
    if value.is_empty() || value.contains(char::is_control) {
        bail!("gateway idempotency key is invalid");
    }
    Ok(format!("sha256:{}", sha256_hex(value.as_bytes())))
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    fn digest(bytes: &[u8]) -> String {
        format!("sha256:{}", sha256_hex(bytes))
    }

    #[tokio::test]
    async fn gateway_repository_blob_tag_and_upload_session_are_corestore_records() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let repository = create_gateway_repository(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            "service-account/deployer",
        )
        .await
        .unwrap();
        assert_eq!(repository.schema, GATEWAY_REPOSITORY_SCHEMA);
        assert_eq!(
            read_gateway_repository(&storage, 1, "docker", "registry-a", "containers/api")
                .await
                .unwrap()
                .unwrap(),
            repository
        );

        let payload = b"container layer bytes";
        let digest = digest(payload);
        let blob = put_gateway_blob(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &digest,
            "application/vnd.oci.image.layer.v1.tar+gzip",
            payload,
            "service-account/deployer",
        )
        .await
        .unwrap();
        assert_eq!(blob.schema, GATEWAY_BLOB_SCHEMA);
        let (read_blob, read_payload) = read_gateway_blob(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &digest,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read_blob, blob);
        assert_eq!(read_payload, payload);

        let first = update_gateway_tag(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            "latest",
            &digest,
            "service-account/deployer",
            None,
        )
        .await
        .unwrap();
        let (_tag, ref_value) = read_gateway_tag(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            "latest",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ref_value.generation, first.generation);
        let stale = update_gateway_tag(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            "latest",
            &digest,
            "service-account/deployer",
            Some(first.generation.saturating_sub(1)),
        )
        .await;
        assert!(stale.is_err(), "stale tag generation must be rejected");

        let upload = create_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            Some(&digest),
            "service-account/deployer",
            "start-upload-main",
            3600,
        )
        .await
        .unwrap();
        assert_eq!(upload.schema, GATEWAY_UPLOAD_SESSION_SCHEMA);
        assert_eq!(upload.expected_digest.as_deref(), Some(digest.as_str()));
        assert_eq!(upload.state, GatewayUploadSessionState::Open);
    }

    #[tokio::test]
    async fn gateway_upload_finalisation_is_digest_checked_and_commits_session_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload = b"first second";
        let expected_digest = digest(payload);
        let upload = create_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            Some(&expected_digest),
            "service-account/deployer",
            "start-upload-finalise",
            3600,
        )
        .await
        .unwrap();

        let first = append_gateway_upload_part(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &upload.upload_id,
            "part_000001",
            0,
            b"first ",
            "idem-first",
        )
        .await
        .unwrap();
        assert_eq!(first.record.state, GatewayUploadSessionState::Receiving);
        let second = append_gateway_upload_part(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &upload.upload_id,
            "part_000002",
            6,
            b"second",
            "idem-second",
        )
        .await
        .unwrap();
        assert_eq!(second.record.received_bytes, payload.len() as u64);

        let wrong_digest = digest(b"wrong");
        let wrong = finalise_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &upload.upload_id,
            Some(&wrong_digest),
            "application/vnd.oci.image.layer.v1.tar+gzip",
            "service-account/deployer",
        )
        .await;
        assert!(
            wrong.is_err(),
            "digest mismatch must reject upload finalisation"
        );

        let blob = finalise_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &upload.upload_id,
            None,
            "application/vnd.oci.image.layer.v1.tar+gzip",
            "service-account/deployer",
        )
        .await
        .unwrap();
        assert_eq!(blob.digest, expected_digest);
        let (_record, bytes) = read_gateway_blob(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &expected_digest,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(bytes, payload);
        let (session, _) = read_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &upload.upload_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(session.state, GatewayUploadSessionState::Committed);
        assert_eq!(
            session.committed_digest.as_deref(),
            Some(expected_digest.as_str())
        );
    }

    #[tokio::test]
    async fn gateway_identifiers_reject_reserved_and_traversal_forms() {
        for bad in [
            "../secret",
            "containers//api",
            "containers/%2e%2e/api",
            "containers\\api",
            "_anvil",
            "_system",
            "_authz",
            "_credentials",
            "containers/_gateway/api",
        ] {
            assert!(
                normalize_gateway_identifier(bad, "test").is_err(),
                "{bad} must be rejected"
            );
        }
        assert_eq!(
            normalize_gateway_identifier("containers/api", "repository").unwrap(),
            "containers/api"
        );
    }

    #[tokio::test]
    async fn gateway_upload_session_start_abort_and_expire_are_corestore_state_transitions() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload = b"idempotent upload";
        let expected_digest = digest(payload);

        let first = create_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            Some(&expected_digest),
            "service-account/deployer",
            "same-start-key",
            3600,
        )
        .await
        .unwrap();
        let retry = create_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            Some(&expected_digest),
            "service-account/deployer",
            "same-start-key",
            3600,
        )
        .await
        .unwrap();
        assert_eq!(retry.upload_id, first.upload_id);
        assert_eq!(retry.idempotency_key_hash, first.idempotency_key_hash);

        let conflict = create_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            Some(&digest(b"different")),
            "service-account/deployer",
            "same-start-key",
            3600,
        )
        .await;
        assert!(
            conflict.is_err(),
            "same start idempotency key cannot change target digest"
        );

        let aborted = abort_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &first.upload_id,
        )
        .await
        .unwrap();
        assert_eq!(aborted.record.state, GatewayUploadSessionState::Aborted);
        let append_after_abort = append_gateway_upload_part(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &first.upload_id,
            "part_000001",
            0,
            payload,
            "append-after-abort",
        )
        .await;
        assert!(append_after_abort.is_err());

        let expired = create_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            Some(&expected_digest),
            "service-account/deployer",
            "expired-start-key",
            1,
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let expired = expire_gateway_upload_session(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            &expired.upload_id,
        )
        .await
        .unwrap();
        assert_eq!(expired.record.state, GatewayUploadSessionState::Expired);
    }

    #[tokio::test]
    async fn gateway_audit_records_are_corestore_append_stream_records() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload = b"gateway audited payload";
        let expected_digest = digest(payload);
        let record = GatewayAuditRecord {
            schema: String::new(),
            tenant_id: 1,
            gateway: "docker".to_string(),
            registry_instance_id: "registry-a".to_string(),
            operation: "manifest_put".to_string(),
            repository: "containers/api".to_string(),
            package: None,
            version_or_reference: Some("latest".to_string()),
            digest: Some(expected_digest.clone()),
            subject_principal: "service-account/deployer".to_string(),
            credential_id: Some("cred-a".to_string()),
            request_id: "req-0001".to_string(),
            result: "success".to_string(),
            created_at: String::new(),
            record_hash: String::new(),
        };

        let first = append_gateway_audit_record(&storage, record.clone(), Some("request-req-0001"))
            .await
            .unwrap();
        assert_eq!(first.record.schema, GATEWAY_AUDIT_SCHEMA);
        assert_eq!(first.stream.sequence, 1);
        assert!(!first.stream.idempotent_replay);
        assert_eq!(
            first.record.digest.as_deref(),
            Some(expected_digest.as_str())
        );

        let replay =
            append_gateway_audit_record(&storage, record.clone(), Some("request-req-0001"))
                .await
                .unwrap();
        assert_eq!(replay.stream.sequence, first.stream.sequence);
        assert!(replay.stream.idempotent_replay);

        let read = read_gateway_audit_records(&storage, 1, "docker", "registry-a", 0, 100)
            .await
            .unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].audit, first.record);
        assert_eq!(read[0].stream.record_kind, GATEWAY_AUDIT_SCHEMA);
        assert_eq!(
            read[0].stream.payload,
            serde_json::to_vec(&first.record).unwrap()
        );
        assert_eq!(
            read[0].stream.payload_hash,
            format!("sha256:{}", sha256_hex(&read[0].stream.payload))
        );

        let bad_record = GatewayAuditRecord {
            repository: "_authz".to_string(),
            request_id: "req-0002".to_string(),
            ..record
        };
        assert!(
            append_gateway_audit_record(&storage, bad_record, None)
                .await
                .is_err(),
            "reserved gateway audit repository names must be rejected"
        );
    }

    #[tokio::test]
    async fn gateway_credential_record_is_corestore_backed_and_hash_checked() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let record = GatewayCredentialRecord {
            schema: GATEWAY_CREDENTIAL_SCHEMA.to_string(),
            tenant_id: 1,
            credential_id: "cred-a".to_string(),
            gateway: "docker".to_string(),
            subject_principal: "service-account/deployer".to_string(),
            secret_hash: hash_gateway_credential_secret("gateway-secret").unwrap(),
            created_at: now_rfc3339(),
            revoked_at: None,
            record_hash: String::new(),
        };
        let generation = put_gateway_credential_record(&storage, record, None)
            .await
            .unwrap();
        assert_eq!(generation, 1);
        let (credential, ref_value) =
            read_gateway_credential_record(&storage, 1, "docker", "cred-a")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(credential.subject_principal, "service-account/deployer");
        let revoked_generation =
            revoke_gateway_credential_record(&storage, 1, "docker", "cred-a", ref_value.generation)
                .await
                .unwrap();
        assert!(revoked_generation > generation);
        let (revoked, _) = read_gateway_credential_record(&storage, 1, "docker", "cred-a")
            .await
            .unwrap()
            .unwrap();
        assert!(revoked.revoked_at.is_some());

        let refs = CoreStore::new(storage.clone())
            .await
            .unwrap()
            .list_ref_names("gateway_credential:")
            .await
            .unwrap();
        assert_eq!(refs.len(), 1);
    }

    #[tokio::test]
    async fn gateway_token_challenge_maps_credential_to_principal_and_revocation() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let record = GatewayCredentialRecord {
            schema: GATEWAY_CREDENTIAL_SCHEMA.to_string(),
            tenant_id: 1,
            credential_id: "cred-token".to_string(),
            gateway: "docker".to_string(),
            subject_principal: "service-account/deployer".to_string(),
            secret_hash: hash_gateway_credential_secret("gateway-secret").unwrap(),
            created_at: now_rfc3339(),
            revoked_at: None,
            record_hash: String::new(),
        };
        put_gateway_credential_record(&storage, record, None)
            .await
            .unwrap();

        let requested_actions = vec!["pull".to_string(), "push".to_string()];
        let token = issue_gateway_access_token(
            &storage,
            1,
            "docker",
            "registry-a",
            "containers/api",
            "cred-token",
            "gateway-secret",
            &requested_actions,
            GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS + 30,
            "gateway-signing-secret",
        )
        .await
        .unwrap();
        assert_eq!(
            token.expires_in_seconds,
            GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS
        );
        assert_eq!(token.claims.subject_principal, "service-account/deployer");
        assert_eq!(token.claims.actions, vec!["pull", "push"]);

        let requirement = GatewayTokenRequirement {
            tenant_id: 1,
            gateway: "docker".to_string(),
            registry_instance_id: "registry-a".to_string(),
            repository: "containers/api".to_string(),
            action: "pull".to_string(),
        };
        let claims = validate_gateway_access_token(
            &storage,
            &token.access_token,
            "gateway-signing-secret",
            Some(&requirement),
        )
        .await
        .unwrap();
        assert_eq!(claims.credential_id, "cred-token");

        let wrong_action = GatewayTokenRequirement {
            action: "delete".to_string(),
            ..requirement.clone()
        };
        assert!(
            validate_gateway_access_token(
                &storage,
                &token.access_token,
                "gateway-signing-secret",
                Some(&wrong_action),
            )
            .await
            .is_err(),
            "gateway token scopes must not bypass route-level action checks"
        );

        let (_credential, ref_value) =
            read_gateway_credential_record(&storage, 1, "docker", "cred-token")
                .await
                .unwrap()
                .unwrap();
        revoke_gateway_credential_record(&storage, 1, "docker", "cred-token", ref_value.generation)
            .await
            .unwrap();
        assert!(
            validate_gateway_access_token(
                &storage,
                &token.access_token,
                "gateway-signing-secret",
                Some(&requirement),
            )
            .await
            .is_err(),
            "credential revocation must invalidate previously issued gateway tokens"
        );
    }

    #[tokio::test]
    async fn gateway_mount_resolution_uses_corestore_records_and_fixed_priority() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut exact = GatewayMountRecord {
            schema: GATEWAY_MOUNT_SCHEMA.to_string(),
            mount_id: "docker-primary".to_string(),
            gateway: "docker".to_string(),
            hosts: vec!["registry.example.test".to_string()],
            path_prefixes: vec!["/".to_string(), "/v2/".to_string()],
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            anvil_storage_tenant_id: "storage-tenant-a".to_string(),
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "storage-tenant-a".to_string(),
                authz_realm_id: "realm-a".to_string(),
            },
            tenant_id: "tenant-a".to_string(),
            registry_instance_id: "registry-a".to_string(),
            default_bucket: "packages".to_string(),
            repository_prefix: String::new(),
            state: GatewayMountState::Active,
            generation: 0,
            record_hash: String::new(),
        };
        let first_generation = put_gateway_mount_record(&storage, exact.clone(), None)
            .await
            .unwrap();
        assert_eq!(first_generation, 1);

        exact.default_bucket = "packages-v2".to_string();
        let stale =
            put_gateway_mount_record(&storage, exact.clone(), Some(first_generation + 10)).await;
        assert!(stale.is_err(), "stale gateway mount generation is rejected");
        let second_generation = put_gateway_mount_record(&storage, exact, Some(first_generation))
            .await
            .unwrap();
        assert_eq!(second_generation, 2);

        let exact_resolution =
            resolve_gateway_mount(&storage, "REGISTRY.EXAMPLE.TEST.", "/v2/containers/api")
                .await
                .unwrap()
                .expect("exact host alias mount");
        assert_eq!(
            exact_resolution.match_kind,
            GatewayMountMatchKind::ExactHostAlias
        );
        assert_eq!(exact_resolution.matched_path_prefix, "/v2/");
        assert_eq!(exact_resolution.ref_generation, 2);
        assert_eq!(
            exact_resolution.record.authz_scope.authz_realm_id,
            "realm-a"
        );
        assert_eq!(exact_resolution.record.default_bucket, "packages-v2");

        let virtual_mount = GatewayMountRecord {
            schema: GATEWAY_MOUNT_SCHEMA.to_string(),
            mount_id: "docker-virtual".to_string(),
            gateway: "docker".to_string(),
            hosts: vec![],
            path_prefixes: vec!["/".to_string()],
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            anvil_storage_tenant_id: "storage-tenant-b".to_string(),
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "storage-tenant-b".to_string(),
                authz_realm_id: "realm-b".to_string(),
            },
            tenant_id: "tenant-b".to_string(),
            registry_instance_id: "registry-b".to_string(),
            default_bucket: "packages".to_string(),
            repository_prefix: String::new(),
            state: GatewayMountState::Active,
            generation: 0,
            record_hash: String::new(),
        };
        put_gateway_mount_record(&storage, virtual_mount, None)
            .await
            .unwrap();
        let virtual_resolution = resolve_gateway_mount(
            &storage,
            "registry-b.tenant-b.eu-west-1.anvil-storage.com",
            "/v2/containers/api/manifests/latest",
        )
        .await
        .unwrap()
        .expect("virtual host regional mount");
        assert_eq!(
            virtual_resolution.match_kind,
            GatewayMountMatchKind::VirtualHostRegional
        );
        assert_eq!(
            virtual_resolution.record.authz_scope.authz_realm_id,
            "realm-b"
        );

        let path_style_resolution = resolve_gateway_mount(
            &storage,
            "eu-west-1.anvil-storage.com",
            "/tenant-b/_gateway/docker/registry-b/v2/containers/api/tags/list",
        )
        .await
        .unwrap()
        .expect("path-style regional mount");
        assert_eq!(
            path_style_resolution.match_kind,
            GatewayMountMatchKind::PathStyleRegional
        );
        assert_eq!(
            path_style_resolution.matched_path_prefix,
            "/tenant-b/_gateway/docker/registry-b/"
        );

        assert!(
            resolve_gateway_mount(
                &storage,
                "eu-west-1.anvil-storage.com",
                "/tenant-b/_gateway/npm/registry-b/package",
            )
            .await
            .unwrap()
            .is_none(),
            "route parsing must not infer another gateway from caller-controlled path text"
        );
    }
}
