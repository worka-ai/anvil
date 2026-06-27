use crate::{formats::hash32, storage::Storage};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::ErrorKind;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionDefinition {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub projection_id: String,
    pub source_database_ids: Vec<String>,
    pub target_database_id: String,
    pub target_actor_or_scope: String,
    pub table_mappings: Vec<TableMapping>,
    pub column_mappings: Vec<ColumnMapping>,
    pub row_filters: Vec<RowFilter>,
    pub resource_bindings: Vec<ProjectionResourceBinding>,
    pub writeback_policy: WriteBackPolicy,
    pub definition_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableMapping {
    pub source_database_id: String,
    pub source_table: String,
    pub target_table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnMapping {
    pub source_table: String,
    pub source_column: String,
    pub target_table: String,
    pub target_column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowFilter {
    FieldEqualsLiteral {
        table: String,
        field: String,
        literal: String,
    },
    FieldInAuthorizedResourceSet {
        table: String,
        field: String,
        resource_set: String,
    },
    ResourceRelationAllows {
        table: String,
        resource_id_field: String,
        relation: String,
    },
    ParentRelationAllows {
        table: String,
        parent_resource_id_field: String,
        relation: String,
    },
    NotDeleted {
        table: String,
        deleted_field: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProjectionResourceBinding {
    pub source_table: String,
    pub primary_key_column: String,
    pub resource_type: String,
    pub resource_id_column: String,
    pub parent_resource_id_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WriteBackPolicy {
    Deny,
    AllowMappedColumns {
        protected_columns: Vec<String>,
        allowed_columns: Vec<String>,
    },
}

impl ProjectionDefinition {
    pub fn seal(mut self) -> Result<Self> {
        canonicalize_projection_definition(&mut self);
        validate_projection_definition_unsigned(&self)?;
        self.definition_hash = Some(hash_projection_definition(&self)?);
        Ok(self)
    }

    pub fn verify(&self) -> Result<()> {
        let mut canonical = self.clone();
        canonicalize_projection_definition(&mut canonical);
        validate_projection_definition_unsigned(&canonical)?;
        if canonical != *self {
            return Err(anyhow!("projection definition is not canonical"));
        }
        let expected = hash_projection_definition(self)?;
        if self.definition_hash.as_deref() != Some(expected.as_str()) {
            return Err(anyhow!("projection definition hash mismatch"));
        }
        Ok(())
    }
}

pub fn hash_projection_definition(definition: &ProjectionDefinition) -> Result<String> {
    let mut unsigned = definition.clone();
    unsigned.definition_hash = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn write_projection_definition(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    definition: &ProjectionDefinition,
) -> Result<()> {
    definition.verify()?;
    ensure_scope(tenant_id, database_id, definition)?;
    let path = storage.personaldb_projection_manifest_path(
        tenant_id,
        database_id,
        &definition.projection_id,
    )?;
    write_json_atomically(&path, definition).await
}

pub async fn read_projection_definition(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> Result<Option<ProjectionDefinition>> {
    let path =
        storage.personaldb_projection_manifest_path(tenant_id, database_id, projection_id)?;
    let Some(definition) = read_json_optional::<ProjectionDefinition>(&path).await? else {
        return Ok(None);
    };
    definition.verify()?;
    ensure_scope(tenant_id, database_id, &definition)?;
    if definition.projection_id != projection_id {
        return Err(anyhow!("projection definition path scope mismatch"));
    }
    Ok(Some(definition))
}

fn canonicalize_projection_definition(definition: &mut ProjectionDefinition) {
    definition.source_database_ids.sort();
    definition.table_mappings.sort();
    definition.column_mappings.sort();
    definition.row_filters.sort();
    definition.resource_bindings.sort();
    if let WriteBackPolicy::AllowMappedColumns {
        protected_columns,
        allowed_columns,
    } = &mut definition.writeback_policy
    {
        protected_columns.sort();
        allowed_columns.sort();
    }
}

fn validate_projection_definition_unsigned(definition: &ProjectionDefinition) -> Result<()> {
    if definition.format_version != 1 {
        return Err(anyhow!("unsupported projection definition version"));
    }
    require_nonempty(&definition.tenant_id, "tenant_id")?;
    require_nonempty(&definition.database_id, "database_id")?;
    require_nonempty(&definition.projection_id, "projection_id")?;
    require_nonempty(&definition.target_database_id, "target_database_id")?;
    require_nonempty(&definition.target_actor_or_scope, "target_actor_or_scope")?;
    validate_nonempty_unique_strings(&definition.source_database_ids, "source_database_ids")?;
    validate_unique(&definition.table_mappings, "table_mappings")?;
    validate_unique(&definition.column_mappings, "column_mappings")?;
    validate_unique(&definition.row_filters, "row_filters")?;
    validate_unique(&definition.resource_bindings, "resource_bindings")?;
    for mapping in &definition.table_mappings {
        validate_table_mapping(mapping)?;
        if !definition
            .source_database_ids
            .contains(&mapping.source_database_id)
        {
            return Err(anyhow!(
                "table mapping source database is not admitted by projection"
            ));
        }
    }
    for mapping in &definition.column_mappings {
        validate_column_mapping(mapping)?;
    }
    for filter in &definition.row_filters {
        validate_row_filter(filter)?;
    }
    for binding in &definition.resource_bindings {
        validate_resource_binding(binding)?;
    }
    validate_writeback_policy(&definition.writeback_policy)?;
    Ok(())
}

fn validate_table_mapping(mapping: &TableMapping) -> Result<()> {
    require_nonempty(&mapping.source_database_id, "source_database_id")?;
    require_nonempty(&mapping.source_table, "source_table")?;
    require_nonempty(&mapping.target_table, "target_table")?;
    Ok(())
}

fn validate_column_mapping(mapping: &ColumnMapping) -> Result<()> {
    require_nonempty(&mapping.source_table, "source_table")?;
    require_nonempty(&mapping.source_column, "source_column")?;
    require_nonempty(&mapping.target_table, "target_table")?;
    require_nonempty(&mapping.target_column, "target_column")?;
    Ok(())
}

fn validate_row_filter(filter: &RowFilter) -> Result<()> {
    match filter {
        RowFilter::FieldEqualsLiteral {
            table,
            field,
            literal,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(field, "row_filter.field")?;
            require_nonempty(literal, "row_filter.literal")?;
        }
        RowFilter::FieldInAuthorizedResourceSet {
            table,
            field,
            resource_set,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(field, "row_filter.field")?;
            require_nonempty(resource_set, "row_filter.resource_set")?;
        }
        RowFilter::ResourceRelationAllows {
            table,
            resource_id_field,
            relation,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(resource_id_field, "row_filter.resource_id_field")?;
            require_nonempty(relation, "row_filter.relation")?;
        }
        RowFilter::ParentRelationAllows {
            table,
            parent_resource_id_field,
            relation,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(
                parent_resource_id_field,
                "row_filter.parent_resource_id_field",
            )?;
            require_nonempty(relation, "row_filter.relation")?;
        }
        RowFilter::NotDeleted {
            table,
            deleted_field,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(deleted_field, "row_filter.deleted_field")?;
        }
    }
    Ok(())
}

fn validate_resource_binding(binding: &ProjectionResourceBinding) -> Result<()> {
    require_nonempty(&binding.source_table, "source_table")?;
    require_nonempty(&binding.primary_key_column, "primary_key_column")?;
    require_nonempty(&binding.resource_type, "resource_type")?;
    require_nonempty(&binding.resource_id_column, "resource_id_column")?;
    if binding
        .parent_resource_id_column
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(anyhow!("parent_resource_id_column must not be empty"));
    }
    Ok(())
}

fn validate_writeback_policy(policy: &WriteBackPolicy) -> Result<()> {
    match policy {
        WriteBackPolicy::Deny => Ok(()),
        WriteBackPolicy::AllowMappedColumns {
            protected_columns,
            allowed_columns,
        } => {
            validate_nonempty_unique_strings(allowed_columns, "allowed_columns")?;
            let mut protected = std::collections::BTreeSet::new();
            for column in protected_columns {
                require_nonempty(column, "protected_columns")?;
                if !protected.insert(column) {
                    return Err(anyhow!("protected_columns contains duplicates"));
                }
                if allowed_columns.contains(column) {
                    return Err(anyhow!(
                        "writeback column cannot be both protected and allowed"
                    ));
                }
            }
            Ok(())
        }
    }
}

fn validate_nonempty_unique_strings(values: &[String], field: &'static str) -> Result<()> {
    if values.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for value in values {
        require_nonempty(value, field)?;
        if !seen.insert(value) {
            return Err(anyhow!("{field} contains duplicates"));
        }
    }
    Ok(())
}

fn validate_unique<T>(values: &[T], field: &'static str) -> Result<()>
where
    T: Ord,
{
    if values.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    for pair in values.windows(2) {
        if pair[0] == pair[1] {
            return Err(anyhow!("{field} contains duplicates"));
        }
    }
    Ok(())
}

fn ensure_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    definition: &ProjectionDefinition,
) -> Result<()> {
    if definition.tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("projection definition tenant scope mismatch"));
    }
    if definition.database_id != expected_database_id {
        return Err(anyhow!("projection definition database scope mismatch"));
    }
    Ok(())
}

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| format!("write temporary projection definition {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish projection definition {}", path.display()))?;
    Ok(())
}

async fn read_json_optional<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn projection_definition_seals_verifies_and_canonicalizes() {
        let mut definition = sample_definition();
        definition.source_database_ids.reverse();
        definition.column_mappings.reverse();
        let definition = definition.seal().unwrap();
        definition.verify().unwrap();
        assert_eq!(definition.definition_hash.as_deref().unwrap().len(), 64);
        assert_eq!(definition.source_database_ids, vec!["db-alpha", "db-beta"]);
    }

    #[test]
    fn projection_definition_rejects_tampering_and_invalid_writeback() {
        let mut definition = sample_definition().seal().unwrap();
        definition.target_actor_or_scope = "scope-b".to_string();
        assert!(definition.verify().is_err());

        let mut invalid = sample_definition();
        invalid.writeback_policy = WriteBackPolicy::AllowMappedColumns {
            protected_columns: vec!["status".to_string()],
            allowed_columns: vec!["status".to_string()],
        };
        assert!(invalid.seal().is_err());
    }

    #[test]
    fn projection_definition_rejects_unadmitted_source_and_empty_filters() {
        let mut invalid_source = sample_definition();
        invalid_source.table_mappings[0].source_database_id = "db-missing".to_string();
        assert!(invalid_source.seal().is_err());

        let mut invalid_filter = sample_definition();
        invalid_filter.row_filters[0] = RowFilter::FieldEqualsLiteral {
            table: "invoice".to_string(),
            field: "status".to_string(),
            literal: String::new(),
        };
        assert!(invalid_filter.seal().is_err());
    }

    #[tokio::test]
    async fn projection_definition_round_trips_at_spec_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let definition = sample_definition().seal().unwrap();
        write_projection_definition(&storage, 7, "projection-db", &definition)
            .await
            .unwrap();
        let path = storage
            .personaldb_projection_manifest_path(7, "projection-db", "projection-a")
            .unwrap();
        assert!(path.ends_with("_anvil/personaldb/tenants/tenant-7/groups/projection-db/projections/projection-a/manifest.json"));

        let read = read_projection_definition(&storage, 7, "projection-db", "projection-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, definition);
    }

    #[tokio::test]
    async fn projection_definition_rejects_scope_and_unsafe_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut definition = sample_definition().seal().unwrap();
        definition.database_id = "other-db".to_string();
        definition = definition.seal().unwrap();
        assert!(
            write_projection_definition(&storage, 7, "projection-db", &definition)
                .await
                .is_err()
        );
        assert!(
            storage
                .personaldb_projection_manifest_path(7, "projection-db", "../escape")
                .is_err()
        );
    }

    fn sample_definition() -> ProjectionDefinition {
        ProjectionDefinition {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "projection-db".to_string(),
            projection_id: "projection-a".to_string(),
            source_database_ids: vec!["db-beta".to_string(), "db-alpha".to_string()],
            target_database_id: "projection-db".to_string(),
            target_actor_or_scope: "scope-account-a".to_string(),
            table_mappings: vec![TableMapping {
                source_database_id: "db-alpha".to_string(),
                source_table: "invoice".to_string(),
                target_table: "invoice_projection".to_string(),
            }],
            column_mappings: vec![
                ColumnMapping {
                    source_table: "invoice".to_string(),
                    source_column: "status".to_string(),
                    target_table: "invoice_projection".to_string(),
                    target_column: "status".to_string(),
                },
                ColumnMapping {
                    source_table: "invoice".to_string(),
                    source_column: "total".to_string(),
                    target_table: "invoice_projection".to_string(),
                    target_column: "total".to_string(),
                },
            ],
            row_filters: vec![
                RowFilter::FieldEqualsLiteral {
                    table: "invoice".to_string(),
                    field: "deleted".to_string(),
                    literal: "false".to_string(),
                },
                RowFilter::ResourceRelationAllows {
                    table: "invoice".to_string(),
                    resource_id_field: "resource_id".to_string(),
                    relation: "viewer".to_string(),
                },
                RowFilter::NotDeleted {
                    table: "invoice".to_string(),
                    deleted_field: "deleted".to_string(),
                },
            ],
            resource_bindings: vec![ProjectionResourceBinding {
                source_table: "invoice".to_string(),
                primary_key_column: "id".to_string(),
                resource_type: "invoice".to_string(),
                resource_id_column: "resource_id".to_string(),
                parent_resource_id_column: Some("account_id".to_string()),
            }],
            writeback_policy: WriteBackPolicy::AllowMappedColumns {
                protected_columns: vec!["id".to_string()],
                allowed_columns: vec!["status".to_string(), "total".to_string()],
            },
            definition_hash: None,
        }
    }
}
