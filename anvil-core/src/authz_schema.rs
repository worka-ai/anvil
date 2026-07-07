use crate::{
    anvil_api::{AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema},
    core_store::{
        CompareAndSwapRef, CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob,
        WriteLogicalFileRequest,
    },
    formats::hash32,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const AUTHZ_NAMESPACE_SCHEMA_REF_PREFIX: &str = "authz_namespace_schema:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
    write_namespace_schema_ref(storage, &record).await?;
    Ok(record)
}

pub async fn read_authz_namespace_schema(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<AuthzNamespaceSchemaRecord>> {
    let Some(record) =
        read_namespace_schema_ref(storage, &namespace_schema_ref_name(tenant_id, namespace)?)
            .await?
    else {
        return Ok(None);
    };
    validate_record(&record, tenant_id, namespace)?;
    Ok(Some(record))
}

pub async fn list_authz_namespace_schemas(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzNamespaceSchemaRecord>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut records = Vec::new();
    for ref_name in store
        .list_ref_names(&namespace_schema_ref_prefix(tenant_id)?)
        .await?
    {
        let Some(record) = read_namespace_schema_ref(storage, &ref_name).await? else {
            continue;
        };
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
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
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

async fn write_namespace_schema_ref(
    storage: &Storage,
    record: &AuthzNamespaceSchemaRecord,
) -> Result<()> {
    let ref_name = namespace_schema_ref_name(record.tenant_id, &record.namespace)?;
    let current = read_namespace_schema_ref_state(storage, &ref_name).await?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "authz".to_string(),
            generation: current
                .as_ref()
                .map(|(value, _)| value.generation + 1)
                .unwrap_or(1),
            logical_file_id: ref_name.clone(),
            source: serde_json::to_vec_pretty(record)?,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "authz-namespace-schema:{}:{}:{}",
                record.tenant_id, record.namespace, record.schema_version
            ),
            region_id: "local".to_string(),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name,
            expected_generation: current.as_ref().map(|(value, _)| value.generation),
            expected_target: current.as_ref().map(|(value, _)| value.target.clone()),
            require_absent: current.is_none(),
            require_present: current.is_some(),
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(())
}

async fn read_namespace_schema_ref(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<AuthzNamespaceSchemaRecord>> {
    Ok(read_namespace_schema_ref_state(storage, ref_name)
        .await?
        .map(|(_, record)| record))
}

async fn read_namespace_schema_ref_state(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<(crate::core_store::CoreRefValue, AuthzNamespaceSchemaRecord)>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    let record = serde_json::from_slice(&bytes).with_context(|| format!("decode {ref_name}"))?;
    Ok(Some((ref_value, record)))
}

fn namespace_schema_ref_prefix(tenant_id: i64) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "authorization schema tenant id must be nonnegative"
        ));
    }
    Ok(format!(
        "{AUTHZ_NAMESPACE_SCHEMA_REF_PREFIX}tenant:{tenant_id}:namespace:"
    ))
}

fn namespace_schema_ref_name(tenant_id: i64, namespace: &str) -> Result<String> {
    validate_component(namespace, "namespace")?;
    Ok(format!(
        "{}{}",
        namespace_schema_ref_prefix(tenant_id)?,
        namespace
    ))
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
