use crate::anvil_api::AuthzNamespaceSchema;
use crate::formats::hash32;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    write_schema_record(storage, tenant_id, &record).await?;
    Ok(record)
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
            read_json(storage.authz_schema_revision_path(tenant_id, schema_id, revision)?).await
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
    let current = read_schema_binding(storage, tenant_id, realm_id).await?;
    let actual = current.as_ref().map(|binding| binding.binding_generation);
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
    let path = storage.authz_schema_binding_path(tenant_id, realm_id)?;
    write_json(path, &binding).await?;
    Ok(binding)
}

pub async fn read_schema_binding(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
) -> Result<Option<StoredAuthzSchemaBinding>> {
    validate_realm_id(realm_id)?;
    read_json(storage.authz_schema_binding_path(tenant_id, realm_id)?).await
}

async fn write_schema_record(
    storage: &Storage,
    tenant_id: i64,
    record: &StoredAuthzSchemaRevision,
) -> Result<()> {
    let revision_path = storage.authz_schema_revision_path(
        tenant_id,
        &record.schema_ref.schema_id,
        record.schema_ref.schema_revision,
    )?;
    write_json(revision_path, record).await?;
    let latest_path = storage.authz_schema_latest_path(tenant_id, &record.schema_ref.schema_id)?;
    write_json(latest_path, record).await
}

async fn read_latest_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    read_json(storage.authz_schema_latest_path(tenant_id, schema_id)?).await
}

async fn find_schema_by_digest(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    digest: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    let latest_path = storage.authz_schema_latest_path(tenant_id, schema_id)?;
    let Some(schema_dir) = latest_path.parent().map(|path| path.join("revisions")) else {
        return Ok(None);
    };
    if tokio::fs::metadata(&schema_dir).await.is_err() {
        return Ok(None);
    }
    let mut entries = tokio::fs::read_dir(&schema_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file()
            && let Some(record) = read_json::<StoredAuthzSchemaRevision>(entry.path()).await?
            && record.schema_ref.schema_digest == digest
        {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

async fn read_json<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<Option<T>> {
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))
}

async fn write_json<T: Serialize>(path: PathBuf, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?).await?;
    tokio::fs::rename(&tmp, &path)
        .await
        .with_context(|| format!("publish {}", path.display()))?;
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
        || value.chars().any(char::is_control)
    {
        Err(anyhow!("invalid {name}"))
    } else {
        Ok(())
    }
}
