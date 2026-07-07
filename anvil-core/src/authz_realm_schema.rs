use crate::anvil_api::AuthzNamespaceSchema;
use crate::core_store::{
    CompareAndSwapRef, CoreObjectRef, CoreRefValue, CoreStore, GetBlob, PutBlob,
};
use crate::formats::hash32;
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

const AUTHZ_SCHEMA_REF_PREFIX: &str = "authz_schema:";
const AUTHZ_SCHEMA_BINDING_REF_PREFIX: &str = "authz_schema_binding:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
            read_json_ref(
                storage,
                &schema_revision_ref_name(tenant_id, schema_id, revision)?,
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
    let current_state = read_json_ref_state::<StoredAuthzSchemaBinding>(
        storage,
        &schema_binding_ref_name(tenant_id, realm_id)?,
    )
    .await?;
    let current = current_state.as_ref().map(|(_, binding)| binding);
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
    write_json_ref_with_expected(
        storage,
        &schema_binding_ref_name(tenant_id, realm_id)?,
        &binding,
        current_state.as_ref().map(|(ref_value, _)| ref_value),
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
    read_json_ref(storage, &schema_binding_ref_name(tenant_id, realm_id)?).await
}

async fn write_schema_record(
    storage: &Storage,
    tenant_id: i64,
    record: &StoredAuthzSchemaRevision,
) -> Result<()> {
    write_json_ref_with_expected(
        storage,
        &schema_revision_ref_name(
            tenant_id,
            &record.schema_ref.schema_id,
            record.schema_ref.schema_revision,
        )?,
        record,
        None,
        true,
    )
    .await?;
    write_json_ref_with_expected(
        storage,
        &schema_latest_ref_name(tenant_id, &record.schema_ref.schema_id)?,
        record,
        None,
        false,
    )
    .await
}

async fn read_latest_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    read_json_ref(storage, &schema_latest_ref_name(tenant_id, schema_id)?).await
}

async fn find_schema_by_digest(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    digest: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    let store = CoreStore::new(storage.clone()).await?;
    let prefix = schema_revision_ref_prefix(tenant_id, schema_id)?;
    for ref_name in store.list_ref_names(&prefix).await? {
        if let Some(record) = read_json_ref::<StoredAuthzSchemaRevision>(storage, &ref_name).await?
            && record.schema_ref.schema_digest == digest
        {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

async fn read_json_ref<T: for<'de> Deserialize<'de>>(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<T>> {
    Ok(read_json_ref_state(storage, ref_name)
        .await?
        .map(|(_, value)| value))
}

async fn read_json_ref_state<T: for<'de> Deserialize<'de>>(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<(CoreRefValue, T)>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    Ok(Some((ref_value, serde_json::from_slice(&bytes)?)))
}

async fn write_json_ref_with_expected<T: Serialize>(
    storage: &Storage,
    ref_name: &str,
    value: &T,
    expected_ref: Option<&CoreRefValue>,
    require_absent: bool,
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.to_string(),
            bytes: serde_json::to_vec_pretty(value)?,
            region_id: "local".to_string(),
            mutation_id: format!("authz-schema:{}", uuid::Uuid::new_v4().simple()),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.to_string(),
            expected_generation: expected_ref.map(|value| value.generation),
            expected_target: expected_ref.map(|value| value.target.clone()),
            require_absent,
            require_present: expected_ref.is_some(),
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(())
}

fn schema_digest(namespaces: &[AuthzNamespaceSchema]) -> Result<String> {
    let bytes = serde_json::to_vec(namespaces)?;
    Ok(hex::encode(hash32(&bytes)))
}

fn validate_schema_id(value: &str) -> Result<()> {
    validate_component(value, "authorization schema id")
}

fn validate_realm_id(value: &str) -> Result<()> {
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

fn schema_revision_ref_prefix(tenant_id: i64, schema_id: &str) -> Result<String> {
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    Ok(format!(
        "{AUTHZ_SCHEMA_REF_PREFIX}tenant:{tenant_id}:schema:{schema_id}:revision:"
    ))
}

fn schema_revision_ref_name(tenant_id: i64, schema_id: &str, revision: u64) -> Result<String> {
    if revision == 0 {
        return Err(anyhow!("authorization schema revision must be nonzero"));
    }
    Ok(format!(
        "{}{revision:020}",
        schema_revision_ref_prefix(tenant_id, schema_id)?
    ))
}

fn schema_latest_ref_name(tenant_id: i64, schema_id: &str) -> Result<String> {
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    Ok(format!(
        "{AUTHZ_SCHEMA_REF_PREFIX}tenant:{tenant_id}:schema:{schema_id}:latest"
    ))
}

fn schema_binding_ref_name(tenant_id: i64, realm_id: &str) -> Result<String> {
    validate_storage_tenant(tenant_id)?;
    validate_realm_id(realm_id)?;
    Ok(format!(
        "{AUTHZ_SCHEMA_BINDING_REF_PREFIX}tenant:{tenant_id}:realm:{realm_id}"
    ))
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
