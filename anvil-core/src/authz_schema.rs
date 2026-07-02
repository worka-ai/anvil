use crate::{
    anvil_api::{AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema},
    formats::hash32,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzNamespaceSchemaRecord {
    pub version: u16,
    pub tenant_id: i64,
    pub namespace: String,
    pub relations: Vec<AuthzRelationSchemaRecord>,
    pub schema_json: String,
    pub schema_hash: String,
    pub schema_version: u64,
    pub authz_revision: u64,
    pub applied_by: String,
    pub reason: String,
    pub applied_at: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzRelationSchemaRecord {
    pub relation: String,
    pub rules: Vec<AuthzRelationRuleRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzRelationRuleRecord {
    pub kind: String,
    pub relation: String,
    pub tuple_relation: String,
    pub target_relation: String,
}

pub async fn write_authz_namespace_schema(
    storage: &Storage,
    tenant_id: i64,
    mut schema: AuthzNamespaceSchema,
    authz_revision: u64,
    applied_by: &str,
    reason: &str,
) -> Result<AuthzNamespaceSchemaRecord> {
    validate_namespace_schema(&schema)?;
    let previous = read_authz_namespace_schema(storage, tenant_id, &schema.namespace).await?;
    let schema_version = previous
        .as_ref()
        .map(|record| record.schema_version.saturating_add(1))
        .unwrap_or(1);
    let applied_at = Utc::now().to_rfc3339();
    schema.schema_hash = schema_hash(&schema)?;
    schema.schema_version = schema_version;
    schema.authz_revision = authz_revision;
    schema.applied_at = applied_at.clone();
    let mut record = AuthzNamespaceSchemaRecord {
        version: 1,
        tenant_id,
        namespace: schema.namespace,
        relations: schema
            .relations
            .into_iter()
            .map(AuthzRelationSchemaRecord::from)
            .collect(),
        schema_json: schema.schema_json,
        schema_hash: schema.schema_hash,
        schema_version,
        authz_revision,
        applied_by: applied_by.to_string(),
        reason: reason.to_string(),
        applied_at,
        record_hash: String::new(),
    };
    record.record_hash = record_hash(&record)?;
    validate_record(&record, tenant_id, &record.namespace)?;
    let path = storage.authz_namespace_schema_path(tenant_id, &record.namespace)?;
    write_json_atomically(&path, &record).await?;
    Ok(record)
}

pub async fn read_authz_namespace_schema(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<AuthzNamespaceSchemaRecord>> {
    let path = storage.authz_namespace_schema_path(tenant_id, namespace)?;
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let record: AuthzNamespaceSchemaRecord =
        serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))?;
    validate_record(&record, tenant_id, namespace)?;
    Ok(Some(record))
}

pub async fn list_authz_namespace_schemas(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzNamespaceSchemaRecord>> {
    let dir = storage.authz_namespace_schema_dir(tenant_id);
    let mut records = Vec::new();
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(records),
        Err(err) => return Err(err).with_context(|| format!("list {}", dir.display())),
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        let record: AuthzNamespaceSchemaRecord =
            serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))?;
        validate_record(&record, tenant_id, &record.namespace)?;
        records.push(record);
    }
    records.sort_by(|left, right| left.namespace.cmp(&right.namespace));
    Ok(records)
}

pub fn schema_response(record: &AuthzNamespaceSchemaRecord) -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: record.namespace.clone(),
        relations: record
            .relations
            .iter()
            .map(AuthzRelationSchema::from)
            .collect(),
        schema_json: record.schema_json.clone(),
        schema_hash: record.schema_hash.clone(),
        schema_version: record.schema_version,
        authz_revision: record.authz_revision,
        applied_at: record.applied_at.clone(),
    }
}

fn validate_namespace_schema(schema: &AuthzNamespaceSchema) -> Result<()> {
    validate_component(&schema.namespace, "namespace")?;
    let mut seen_relations = BTreeMap::new();
    for relation in &schema.relations {
        validate_component(&relation.relation, "relation")?;
        if seen_relations
            .insert(relation.relation.clone(), ())
            .is_some()
        {
            return Err(anyhow!("duplicate authorization schema relation"));
        }
        for rule in &relation.rules {
            match rule.kind.as_str() {
                "inherit" => {
                    validate_component(&rule.relation, "inherited relation")?;
                    require_empty(&rule.tuple_relation, "tuple_relation")?;
                    require_empty(&rule.target_relation, "target_relation")?;
                }
                "computed" | "tuple_to_userset" => {
                    require_empty(&rule.relation, "relation")?;
                    validate_component(&rule.tuple_relation, "tuple relation")?;
                    validate_component(&rule.target_relation, "target relation")?;
                }
                _ => {
                    return Err(anyhow!(
                        "authorization schema rule kind must be inherit, computed, or tuple_to_userset"
                    ));
                }
            }
        }
    }
    if !schema.schema_json.is_empty() {
        serde_json::from_str::<serde_json::Value>(&schema.schema_json)
            .context("authorization schema_json must be valid JSON")?;
    }
    Ok(())
}

fn validate_record(
    record: &AuthzNamespaceSchemaRecord,
    tenant_id: i64,
    namespace: &str,
) -> Result<()> {
    if record.version != 1 {
        return Err(anyhow!(
            "unsupported authorization namespace schema version"
        ));
    }
    if record.tenant_id != tenant_id || record.namespace != namespace {
        return Err(anyhow!("authorization namespace schema scope mismatch"));
    }
    if record.schema_version == 0 {
        return Err(anyhow!(
            "authorization namespace schema version must be nonzero"
        ));
    }
    let expected_schema_hash = schema_hash(&schema_response(record))?;
    if expected_schema_hash != record.schema_hash {
        return Err(anyhow!("authorization namespace schema hash mismatch"));
    }
    let expected_record_hash = record_hash(record)?;
    if expected_record_hash != record.record_hash {
        return Err(anyhow!(
            "authorization namespace schema record hash mismatch"
        ));
    }
    Ok(())
}

fn validate_component(value: &str, name: &str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{name} must not be empty"));
    }
    if value == "." || value == ".." || value.contains('/') || value.chars().any(char::is_control) {
        return Err(anyhow!("{name} must be a safe component"));
    }
    Ok(())
}

fn require_empty(value: &str, name: &str) -> Result<()> {
    if value.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "{name} must be empty for this authorization rule kind"
        ))
    }
}

fn schema_hash(schema: &AuthzNamespaceSchema) -> Result<String> {
    let canonical = canonical_schema(schema);
    Ok(hex::encode(hash32(&serde_json::to_vec(&canonical)?)))
}

fn record_hash(record: &AuthzNamespaceSchemaRecord) -> Result<String> {
    let mut unsigned = record.clone();
    unsigned.record_hash.clear();
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

fn canonical_schema(schema: &AuthzNamespaceSchema) -> AuthzNamespaceSchema {
    let mut schema = schema.clone();
    schema.schema_hash.clear();
    schema.schema_version = 0;
    schema.authz_revision = 0;
    schema.applied_at.clear();
    schema
        .relations
        .sort_by(|left, right| left.relation.cmp(&right.relation));
    for relation in &mut schema.relations {
        relation.rules.sort_by(|left, right| {
            (
                &left.kind,
                &left.relation,
                &left.tuple_relation,
                &left.target_relation,
            )
                .cmp(&(
                    &right.kind,
                    &right.relation,
                    &right.tuple_relation,
                    &right.target_relation,
                ))
        });
    }
    schema
}

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| {
            format!(
                "write temporary authorization namespace schema {}",
                tmp.display()
            )
        })?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish authorization namespace schema {}", path.display()))?;
    Ok(())
}

impl From<AuthzRelationRule> for AuthzRelationRuleRecord {
    fn from(rule: AuthzRelationRule) -> Self {
        Self {
            kind: rule.kind,
            relation: rule.relation,
            tuple_relation: rule.tuple_relation,
            target_relation: rule.target_relation,
        }
    }
}

impl From<AuthzRelationSchema> for AuthzRelationSchemaRecord {
    fn from(schema: AuthzRelationSchema) -> Self {
        Self {
            relation: schema.relation,
            rules: schema
                .rules
                .into_iter()
                .map(AuthzRelationRuleRecord::from)
                .collect(),
        }
    }
}

impl From<&AuthzRelationRuleRecord> for AuthzRelationRule {
    fn from(rule: &AuthzRelationRuleRecord) -> Self {
        Self {
            kind: rule.kind.clone(),
            relation: rule.relation.clone(),
            tuple_relation: rule.tuple_relation.clone(),
            target_relation: rule.target_relation.clone(),
        }
    }
}

impl From<&AuthzRelationSchemaRecord> for AuthzRelationSchema {
    fn from(schema: &AuthzRelationSchemaRecord) -> Self {
        Self {
            relation: schema.relation.clone(),
            rules: schema.rules.iter().map(AuthzRelationRule::from).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn schema(namespace: &str) -> AuthzNamespaceSchema {
        AuthzNamespaceSchema {
            namespace: namespace.to_string(),
            relations: vec![AuthzRelationSchema {
                relation: "viewer".to_string(),
                rules: vec![AuthzRelationRule {
                    kind: "inherit".to_string(),
                    relation: "editor".to_string(),
                    tuple_relation: String::new(),
                    target_relation: String::new(),
                }],
            }],
            schema_json: r#"{"namespace":"document"}"#.to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        }
    }

    #[tokio::test]
    async fn namespace_schema_persists_versions_and_hashes() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first =
            write_authz_namespace_schema(&storage, 7, schema("document"), 10, "tester", "initial")
                .await
                .unwrap();
        assert_eq!(first.schema_version, 1);
        assert_eq!(first.authz_revision, 10);

        let second =
            write_authz_namespace_schema(&storage, 7, schema("document"), 11, "tester", "update")
                .await
                .unwrap();
        assert_eq!(second.schema_version, 2);

        let read = read_authz_namespace_schema(&storage, 7, "document")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.schema_version, 2);
        assert_eq!(
            list_authz_namespace_schemas(&storage, 7)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn namespace_schema_rejects_unsafe_names_and_bad_rules() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            write_authz_namespace_schema(&storage, 7, schema("../bad"), 1, "tester", "bad")
                .await
                .is_err()
        );

        let mut bad = schema("document");
        bad.relations[0].rules[0].kind = "made_up".to_string();
        assert!(
            write_authz_namespace_schema(&storage, 7, bad, 1, "tester", "bad")
                .await
                .is_err()
        );
    }
}
