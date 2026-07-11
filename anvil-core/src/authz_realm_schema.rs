use crate::anvil_api::AuthzNamespaceSchema;
use crate::authz_coremeta_payload::{decode_authz_payload_row, encode_authz_payload_row};
use crate::core_store::{
    CF_AUTHZ, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart,
    TABLE_AUTHZ_SCHEMA_ROW, commit_coremeta_batch_for_storage, core_meta_committed_row_common,
    core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
    encode_deterministic_proto,
};
use crate::formats::hash32;
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const AUTHZ_SCHEMA_REVISION_ROW_KIND: &str = "schema_revision";
const AUTHZ_SCHEMA_LATEST_ROW_KIND: &str = "schema_latest";
const AUTHZ_SCHEMA_BINDING_ROW_KIND: &str = "schema_binding";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredSchemaRef {
    pub schema_id: String,
    pub schema_revision: u64,
    pub schema_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAuthzSchemaRevision {
    pub schema_ref: StoredSchemaRef,
    pub namespaces: Vec<AuthzNamespaceSchema>,
    pub authz_revision: u64,
    pub written_by: String,
    pub reason: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAuthzSchemaBinding {
    pub realm_id: String,
    pub schema_ref: StoredSchemaRef,
    pub binding_generation: u64,
    pub authz_revision: u64,
    pub written_by: String,
    pub reason: String,
    pub updated_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct StoredSchemaRefProto {
    #[prost(string, tag = "1")]
    schema_id: String,
    #[prost(uint64, tag = "2")]
    schema_revision: u64,
    #[prost(string, tag = "3")]
    schema_digest: String,
}

#[derive(Clone, PartialEq, Message)]
struct StoredAuthzSchemaRevisionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    schema_ref: Option<StoredSchemaRefProto>,
    #[prost(message, repeated, tag = "3")]
    namespaces: Vec<AuthzNamespaceSchemaProto>,
    #[prost(uint64, tag = "4")]
    authz_revision: u64,
    #[prost(string, tag = "5")]
    written_by: String,
    #[prost(string, tag = "6")]
    reason: String,
    #[prost(string, tag = "7")]
    created_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct StoredAuthzSchemaBindingProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    realm_id: String,
    #[prost(message, optional, tag = "3")]
    schema_ref: Option<StoredSchemaRefProto>,
    #[prost(uint64, tag = "4")]
    binding_generation: u64,
    #[prost(uint64, tag = "5")]
    authz_revision: u64,
    #[prost(string, tag = "6")]
    written_by: String,
    #[prost(string, tag = "7")]
    reason: String,
    #[prost(string, tag = "8")]
    updated_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzNamespaceSchemaProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(message, repeated, tag = "2")]
    relations: Vec<AuthzRelationSchemaProto>,
    #[prost(string, tag = "3")]
    schema_json: String,
    #[prost(string, tag = "4")]
    schema_hash: String,
    #[prost(uint64, tag = "5")]
    schema_version: u64,
    #[prost(uint64, tag = "6")]
    authz_revision: u64,
    #[prost(string, tag = "7")]
    applied_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzNamespaceSetProto {
    #[prost(message, repeated, tag = "1")]
    namespaces: Vec<AuthzNamespaceSchemaProto>,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRelationSchemaProto {
    #[prost(string, tag = "1")]
    relation: String,
    #[prost(message, repeated, tag = "2")]
    rules: Vec<AuthzRelationRuleProto>,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRelationRuleProto {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(string, tag = "2")]
    relation: String,
    #[prost(string, tag = "3")]
    tuple_relation: String,
    #[prost(string, tag = "4")]
    target_relation: String,
}

pub async fn put_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    mut namespaces: Vec<AuthzNamespaceSchema>,
    authz_revision: u64,
    written_by: &str,
    reason: &str,
) -> Result<StoredAuthzSchemaRevision> {
    validate_schema_id(schema_id)?;
    if namespaces.is_empty() {
        return Err(anyhow!(
            "authorization schema must contain at least one namespace"
        ));
    }
    namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));
    let schema_digest = schema_digest(&namespaces)?;
    if let Some(existing) =
        find_schema_by_digest(storage, tenant_id, schema_id, &schema_digest).await?
    {
        return Ok(existing);
    }
    let latest = read_latest_schema_revision(storage, tenant_id, schema_id).await?;
    let next_revision = latest
        .map(|record| record.schema_ref.schema_revision.saturating_add(1))
        .unwrap_or(1);
    let record = StoredAuthzSchemaRevision {
        schema_ref: StoredSchemaRef {
            schema_id: schema_id.to_string(),
            schema_revision: next_revision,
            schema_digest,
        },
        namespaces,
        authz_revision,
        written_by: written_by.to_string(),
        reason: reason.to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    match write_schema_record(storage, tenant_id, &record).await {
        Ok(()) => Ok(record),
        Err(err) => {
            // Concurrent bootstrap/schema writers may race on the same deterministic
            // revision ref. If the winner wrote the identical schema digest, the
            // operation is idempotent; otherwise surface the conflict.
            if let Some(existing) = find_schema_by_digest(
                storage,
                tenant_id,
                schema_id,
                &record.schema_ref.schema_digest,
            )
            .await?
            {
                Ok(existing)
            } else {
                Err(err)
            }
        }
    }
}

pub async fn read_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    revision: Option<u64>,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    validate_schema_id(schema_id)?;
    match revision {
        Some(revision) => {
            read_proto_row(
                storage,
                tenant_id,
                schema_revision_tuple_key(tenant_id, schema_id, revision)?,
            )
            .await
        }
        None => read_latest_schema_revision(storage, tenant_id, schema_id).await,
    }
}

pub async fn bind_schema(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
    schema_ref: StoredSchemaRef,
    expected_generation: Option<u64>,
    authz_revision: u64,
    written_by: &str,
    reason: &str,
) -> Result<StoredAuthzSchemaBinding> {
    validate_realm_id(realm_id)?;
    if read_schema_revision(
        storage,
        tenant_id,
        &schema_ref.schema_id,
        Some(schema_ref.schema_revision),
    )
    .await?
    .is_none()
    {
        return Err(anyhow!("authorization schema revision not found"));
    }
    let current = read_proto_row::<StoredAuthzSchemaBinding>(
        storage,
        tenant_id,
        schema_binding_tuple_key(tenant_id, realm_id)?,
    )
    .await?;
    let actual = current.map(|binding| binding.binding_generation);
    match (expected_generation, actual) {
        (None, None) | (Some(0), None) => {}
        (Some(expected), Some(actual)) if expected == actual => {}
        _ => return Err(anyhow!("schema binding generation conflict")),
    }
    let binding = StoredAuthzSchemaBinding {
        realm_id: realm_id.to_string(),
        schema_ref,
        binding_generation: actual.map(|value| value.saturating_add(1)).unwrap_or(1),
        authz_revision,
        written_by: written_by.to_string(),
        reason: reason.to_string(),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    write_proto_row(
        storage,
        schema_binding_tuple_key(tenant_id, realm_id)?,
        &binding,
        false,
    )
    .await?;
    Ok(binding)
}

pub async fn read_schema_binding(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
) -> Result<Option<StoredAuthzSchemaBinding>> {
    validate_realm_id(realm_id)?;
    read_proto_row(
        storage,
        tenant_id,
        schema_binding_tuple_key(tenant_id, realm_id)?,
    )
    .await
}

pub async fn read_bound_namespace_schema(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
    namespace: &str,
) -> Result<Option<AuthzNamespaceSchema>> {
    validate_realm_id(realm_id)?;
    validate_component(namespace, "authorization namespace")?;
    let Some(binding) = read_schema_binding(storage, tenant_id, realm_id).await? else {
        return Ok(None);
    };
    let Some(revision) = read_schema_revision(
        storage,
        tenant_id,
        &binding.schema_ref.schema_id,
        Some(binding.schema_ref.schema_revision),
    )
    .await?
    else {
        return Err(anyhow!("bound authorization schema revision not found"));
    };
    Ok(revision
        .namespaces
        .into_iter()
        .find(|schema| schema.namespace == namespace))
}

async fn write_schema_record(
    storage: &Storage,
    tenant_id: i64,
    record: &StoredAuthzSchemaRevision,
) -> Result<()> {
    write_proto_row(
        storage,
        schema_revision_tuple_key(
            tenant_id,
            &record.schema_ref.schema_id,
            record.schema_ref.schema_revision,
        )?,
        record,
        true,
    )
    .await?;
    write_proto_row(
        storage,
        schema_latest_tuple_key(tenant_id, &record.schema_ref.schema_id)?,
        record,
        false,
    )
    .await
}

async fn read_latest_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    read_proto_row(
        storage,
        tenant_id,
        schema_latest_tuple_key(tenant_id, schema_id)?,
    )
    .await
}

async fn find_schema_by_digest(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    digest: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    for row in meta.scan_prefix(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &schema_revision_tuple_prefix(tenant_id, schema_id)?,
    )? {
        let record =
            decode_schema_record_row::<StoredAuthzSchemaRevision>(storage, tenant_id, &row.payload)
                .await?;
        if record.schema_ref.schema_digest == digest {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

async fn read_proto_row<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    tenant_id: i64,
    tuple_key: Vec<u8>,
) -> Result<Option<T>> {
    let Some(bytes) = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &tuple_key,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(
        decode_schema_record_row::<T>(storage, tenant_id, &bytes).await?,
    ))
}

async fn write_proto_row<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    tuple_key: Vec<u8>,
    value: &T,
    require_absent: bool,
) -> Result<()> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    if require_absent
        && meta
            .get(CF_AUTHZ, TABLE_AUTHZ_SCHEMA_ROW, &tuple_key)?
            .is_some()
    {
        return Err(anyhow!("authorization schema CoreMeta row already exists"));
    }
    let record_payload = value.encode_record()?;
    let payload = encode_authz_payload_row(
        storage,
        T::row_common(value),
        T::payload_kind(),
        &hex::encode(&tuple_key),
        T::payload_generation(value),
        &T::payload_transaction_id(value),
        record_payload,
    )
    .await?;
    let op = CoreMetaBatchOp {
        cf: CF_AUTHZ,
        table_id: TABLE_AUTHZ_SCHEMA_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!("authz-schema-row:{}", hex::encode(&tuple_key)),
        &[op],
    )
    .await?;
    Ok(())
}

trait AuthzSchemaRecordCodec: Sized {
    fn payload_kind() -> &'static str;
    fn payload_generation(&self) -> u64;
    fn payload_transaction_id(&self) -> String;
    fn row_common(&self) -> crate::core_store::CoreMetaRowCommonProto;
    fn encode_record(&self) -> Result<Vec<u8>>;
    fn decode_record(bytes: &[u8]) -> Result<Self>;
}

impl AuthzSchemaRecordCodec for StoredAuthzSchemaRevision {
    fn payload_kind() -> &'static str {
        AUTHZ_SCHEMA_REVISION_ROW_KIND
    }

    fn payload_generation(&self) -> u64 {
        self.schema_ref.schema_revision
    }

    fn payload_transaction_id(&self) -> String {
        self.schema_ref.schema_digest.clone()
    }

    fn row_common(&self) -> crate::core_store::CoreMetaRowCommonProto {
        schema_revision_common(self)
    }

    fn encode_record(&self) -> Result<Vec<u8>> {
        Ok(encode_deterministic_proto(&schema_revision_to_proto(self)))
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        schema_revision_from_proto(
            decode_deterministic_proto::<StoredAuthzSchemaRevisionProto>(
                bytes,
                "authorization schema revision",
            )?,
        )
    }
}

impl AuthzSchemaRecordCodec for StoredAuthzSchemaBinding {
    fn payload_kind() -> &'static str {
        AUTHZ_SCHEMA_BINDING_ROW_KIND
    }

    fn payload_generation(&self) -> u64 {
        self.binding_generation
    }

    fn payload_transaction_id(&self) -> String {
        self.schema_ref.schema_digest.clone()
    }

    fn row_common(&self) -> crate::core_store::CoreMetaRowCommonProto {
        schema_binding_common(self)
    }

    fn encode_record(&self) -> Result<Vec<u8>> {
        Ok(encode_deterministic_proto(&schema_binding_to_proto(self)))
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        schema_binding_from_proto(decode_deterministic_proto::<StoredAuthzSchemaBindingProto>(
            bytes,
            "authorization schema binding",
        )?)
    }
}

async fn decode_schema_record_row<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    tenant_id: i64,
    row_payload: &[u8],
) -> Result<T> {
    let record_payload =
        decode_authz_payload_row(storage, tenant_id, row_payload, T::payload_kind()).await?;
    T::decode_record(&record_payload)
}

fn schema_ref_to_proto(schema_ref: &StoredSchemaRef) -> StoredSchemaRefProto {
    StoredSchemaRefProto {
        schema_id: schema_ref.schema_id.clone(),
        schema_revision: schema_ref.schema_revision,
        schema_digest: schema_ref.schema_digest.clone(),
    }
}

fn schema_ref_from_proto(proto: StoredSchemaRefProto) -> StoredSchemaRef {
    StoredSchemaRef {
        schema_id: proto.schema_id,
        schema_revision: proto.schema_revision,
        schema_digest: proto.schema_digest,
    }
}

fn schema_revision_to_proto(record: &StoredAuthzSchemaRevision) -> StoredAuthzSchemaRevisionProto {
    StoredAuthzSchemaRevisionProto {
        common: Some(schema_revision_common(record)),
        schema_ref: Some(schema_ref_to_proto(&record.schema_ref)),
        namespaces: record.namespaces.iter().map(namespace_to_proto).collect(),
        authz_revision: record.authz_revision,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        created_at: record.created_at.clone(),
    }
}

fn schema_revision_common(
    record: &StoredAuthzSchemaRevision,
) -> crate::core_store::CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        "system",
        core_meta_root_key_hash(&format!(
            "authz-schema-revision/{}",
            record.schema_ref.schema_id
        )),
        record.schema_ref.schema_revision,
        record.schema_ref.schema_digest.clone(),
        record.authz_revision,
    )
}

fn schema_revision_from_proto(
    proto: StoredAuthzSchemaRevisionProto,
) -> Result<StoredAuthzSchemaRevision> {
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("authorization schema revision row missing CoreMeta common"))?;
    Ok(StoredAuthzSchemaRevision {
        schema_ref: schema_ref_from_proto(
            proto
                .schema_ref
                .ok_or_else(|| anyhow!("authorization schema revision missing schema_ref"))?,
        ),
        namespaces: proto
            .namespaces
            .into_iter()
            .map(namespace_from_proto)
            .collect(),
        authz_revision: proto.authz_revision,
        written_by: proto.written_by,
        reason: proto.reason,
        created_at: proto.created_at,
    })
}

fn schema_binding_to_proto(record: &StoredAuthzSchemaBinding) -> StoredAuthzSchemaBindingProto {
    StoredAuthzSchemaBindingProto {
        common: Some(schema_binding_common(record)),
        realm_id: record.realm_id.clone(),
        schema_ref: Some(schema_ref_to_proto(&record.schema_ref)),
        binding_generation: record.binding_generation,
        authz_revision: record.authz_revision,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        updated_at: record.updated_at.clone(),
    }
}

fn schema_binding_common(
    record: &StoredAuthzSchemaBinding,
) -> crate::core_store::CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        record.realm_id.clone(),
        core_meta_root_key_hash(&format!("authz-schema-binding/{}", record.realm_id)),
        record.binding_generation,
        record.schema_ref.schema_digest.clone(),
        record.authz_revision,
    )
}

fn schema_binding_from_proto(
    proto: StoredAuthzSchemaBindingProto,
) -> Result<StoredAuthzSchemaBinding> {
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("authorization schema binding row missing CoreMeta common"))?;
    Ok(StoredAuthzSchemaBinding {
        realm_id: proto.realm_id,
        schema_ref: schema_ref_from_proto(
            proto
                .schema_ref
                .ok_or_else(|| anyhow!("authorization schema binding missing schema_ref"))?,
        ),
        binding_generation: proto.binding_generation,
        authz_revision: proto.authz_revision,
        written_by: proto.written_by,
        reason: proto.reason,
        updated_at: proto.updated_at,
    })
}

fn namespace_to_proto(namespace: &AuthzNamespaceSchema) -> AuthzNamespaceSchemaProto {
    AuthzNamespaceSchemaProto {
        namespace: namespace.namespace.clone(),
        relations: namespace.relations.iter().map(relation_to_proto).collect(),
        schema_json: namespace.schema_json.clone(),
        schema_hash: namespace.schema_hash.clone(),
        schema_version: namespace.schema_version,
        authz_revision: namespace.authz_revision,
        applied_at: namespace.applied_at.clone(),
    }
}

fn namespace_from_proto(proto: AuthzNamespaceSchemaProto) -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: proto.namespace,
        relations: proto
            .relations
            .into_iter()
            .map(relation_from_proto)
            .collect(),
        schema_json: proto.schema_json,
        schema_hash: proto.schema_hash,
        schema_version: proto.schema_version,
        authz_revision: proto.authz_revision,
        applied_at: proto.applied_at,
    }
}

fn relation_to_proto(relation: &crate::anvil_api::AuthzRelationSchema) -> AuthzRelationSchemaProto {
    AuthzRelationSchemaProto {
        relation: relation.relation.clone(),
        rules: relation.rules.iter().map(rule_to_proto).collect(),
    }
}

fn relation_from_proto(proto: AuthzRelationSchemaProto) -> crate::anvil_api::AuthzRelationSchema {
    crate::anvil_api::AuthzRelationSchema {
        relation: proto.relation,
        rules: proto.rules.into_iter().map(rule_from_proto).collect(),
    }
}

fn rule_to_proto(rule: &crate::anvil_api::AuthzRelationRule) -> AuthzRelationRuleProto {
    AuthzRelationRuleProto {
        kind: rule.kind.clone(),
        relation: rule.relation.clone(),
        tuple_relation: rule.tuple_relation.clone(),
        target_relation: rule.target_relation.clone(),
    }
}

fn rule_from_proto(proto: AuthzRelationRuleProto) -> crate::anvil_api::AuthzRelationRule {
    crate::anvil_api::AuthzRelationRule {
        kind: proto.kind,
        relation: proto.relation,
        tuple_relation: proto.tuple_relation,
        target_relation: proto.target_relation,
    }
}

fn schema_digest(namespaces: &[AuthzNamespaceSchema]) -> Result<String> {
    let mut namespaces = namespaces.to_vec();
    namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));
    let bytes = encode_deterministic_proto(&AuthzNamespaceSetProto {
        namespaces: namespaces.iter().map(namespace_to_proto).collect(),
    });
    Ok(hex::encode(hash32(&bytes)))
}

fn validate_schema_id(value: &str) -> Result<()> {
    validate_component(value, "authorization schema id")
}

fn validate_realm_id(value: &str) -> Result<()> {
    if value == crate::system_realm::SYSTEM_REALM_ID {
        return Ok(());
    }
    validate_component(value, "authorization realm id")
}

fn validate_component(value: &str, name: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        Err(anyhow!("invalid {name}"))
    } else {
        Ok(())
    }
}

fn schema_revision_tuple_prefix(tenant_id: i64, schema_id: &str) -> Result<Vec<u8>> {
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_REVISION_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(schema_id),
    ])
}

fn schema_revision_tuple_key(tenant_id: i64, schema_id: &str, revision: u64) -> Result<Vec<u8>> {
    if revision == 0 {
        return Err(anyhow!("authorization schema revision must be nonzero"));
    }
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_REVISION_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(schema_id),
        CoreMetaTuplePart::U64(revision),
    ])
}

fn schema_latest_tuple_key(tenant_id: i64, schema_id: &str) -> Result<Vec<u8>> {
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_LATEST_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(schema_id),
    ])
}

fn schema_binding_tuple_key(tenant_id: i64, realm_id: &str) -> Result<Vec<u8>> {
    validate_storage_tenant(tenant_id)?;
    validate_realm_id(realm_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_BINDING_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(realm_id),
    ])
}

fn validate_storage_tenant(tenant_id: i64) -> Result<()> {
    if tenant_id < 0 {
        Err(anyhow!(
            "authorization storage tenant id must be nonnegative"
        ))
    } else {
        Ok(())
    }
}
