use crate::{
    anvil_personaldb_sqlite_changeset::{
        DecodedSqliteChangesetChange, SqliteChangesetOperation, SqliteChangesetValue,
        iterate_changeset,
    },
    personaldb_projection::{ProjectionDefinition, RowFilter},
};
use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, params_from_iter, session::Session, types::Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct ProjectionBuildInput<'a> {
    pub source_database_id: &'a str,
    pub source_schema_sql: &'a str,
    pub target_schema_sql: &'a str,
    pub definition: &'a ProjectionDefinition,
    pub source_changeset_bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBuildResult {
    pub changeset_bytes: Vec<u8>,
    pub projected_operation_count: usize,
}

#[derive(Debug, Clone)]
struct TableSchema {
    columns: Vec<String>,
    primary_key_columns: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProjectedOperation {
    table: String,
    old_row: Option<BTreeMap<String, SqliteChangesetValue>>,
    new_row: Option<BTreeMap<String, SqliteChangesetValue>>,
}

pub fn build_projection_changeset(
    input: ProjectionBuildInput<'_>,
) -> Result<Option<ProjectionBuildResult>> {
    let source_changes = iterate_changeset(input.source_changeset_bytes)?;
    let source_schema = load_schema(input.source_schema_sql)?;
    let target_schema = load_schema(input.target_schema_sql)?;
    let operations = plan_projected_operations(input, &source_changes, &source_schema)?;
    if operations.is_empty() {
        return Ok(None);
    }
    let changeset_bytes =
        encode_projected_changeset(input.target_schema_sql, &target_schema, &operations)?;
    Ok(Some(ProjectionBuildResult {
        changeset_bytes,
        projected_operation_count: operations.len(),
    }))
}

fn plan_projected_operations(
    input: ProjectionBuildInput<'_>,
    source_changes: &[DecodedSqliteChangesetChange],
    source_schema: &BTreeMap<String, TableSchema>,
) -> Result<Vec<ProjectedOperation>> {
    let mut operations = Vec::new();
    for change in source_changes {
        let Some(source_table_schema) = source_schema.get(&change.table_name) else {
            continue;
        };
        for table_mapping in input.definition.table_mappings.iter().filter(|mapping| {
            mapping.source_database_id == input.source_database_id
                && mapping.source_table == change.table_name
        }) {
            let old_included = match change.operation {
                SqliteChangesetOperation::Insert => false,
                SqliteChangesetOperation::Update | SqliteChangesetOperation::Delete => {
                    row_matches_filters(input.definition, source_table_schema, change, false)?
                }
            };
            let new_included = match change.operation {
                SqliteChangesetOperation::Delete => false,
                SqliteChangesetOperation::Insert | SqliteChangesetOperation::Update => {
                    row_matches_filters(input.definition, source_table_schema, change, true)?
                }
            };

            match (old_included, new_included) {
                (false, false) => {}
                (false, true) => operations.push(ProjectedOperation {
                    table: table_mapping.target_table.clone(),
                    old_row: None,
                    new_row: Some(project_row(
                        input.definition,
                        source_table_schema,
                        change,
                        true,
                        &table_mapping.target_table,
                    )?),
                }),
                (true, false) => operations.push(ProjectedOperation {
                    table: table_mapping.target_table.clone(),
                    old_row: Some(project_row(
                        input.definition,
                        source_table_schema,
                        change,
                        false,
                        &table_mapping.target_table,
                    )?),
                    new_row: None,
                }),
                (true, true) => operations.push(ProjectedOperation {
                    table: table_mapping.target_table.clone(),
                    old_row: Some(project_row(
                        input.definition,
                        source_table_schema,
                        change,
                        false,
                        &table_mapping.target_table,
                    )?),
                    new_row: Some(project_row(
                        input.definition,
                        source_table_schema,
                        change,
                        true,
                        &table_mapping.target_table,
                    )?),
                }),
            }
        }
    }
    Ok(operations)
}

fn encode_projected_changeset(
    target_schema_sql: &str,
    target_schema: &BTreeMap<String, TableSchema>,
    operations: &[ProjectedOperation],
) -> Result<Vec<u8>> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(target_schema_sql)?;
    for operation in operations {
        if let Some(old_row) = &operation.old_row {
            insert_row(&db, &operation.table, old_row)?;
        }
    }

    let mut session = Session::new(&db)?;
    session.attach::<&str>(None)?;
    for operation in operations {
        let table_schema = target_schema
            .get(&operation.table)
            .ok_or_else(|| anyhow!("projection target table is absent from target schema"))?;
        match (&operation.old_row, &operation.new_row) {
            (None, Some(new_row)) => insert_row(&db, &operation.table, new_row)?,
            (Some(old_row), Some(new_row)) => {
                update_row(&db, &operation.table, table_schema, old_row, new_row)?
            }
            (Some(old_row), None) => delete_row(&db, &operation.table, table_schema, old_row)?,
            (None, None) => {}
        }
    }
    let mut output = Vec::new();
    session.changeset_strm(&mut output)?;
    if output.is_empty() {
        return Ok(Vec::new());
    }
    Ok(output)
}

fn row_matches_filters(
    definition: &ProjectionDefinition,
    source_schema: &TableSchema,
    change: &DecodedSqliteChangesetChange,
    use_new_values: bool,
) -> Result<bool> {
    for filter in definition
        .row_filters
        .iter()
        .filter(|filter| filter_table(filter) == change.table_name)
    {
        match filter {
            RowFilter::FieldEqualsLiteral { field, literal, .. } => {
                let value = value_for_field(source_schema, change, use_new_values, field)?;
                if value.as_ref().map(sqlite_value_literal) != Some(literal.clone()) {
                    return Ok(false);
                }
            }
            RowFilter::NotDeleted { deleted_field, .. } => {
                let value = value_for_field(source_schema, change, use_new_values, deleted_field)?;
                if !matches!(value, Some(SqliteChangesetValue::Null)) {
                    return Ok(false);
                }
            }
            RowFilter::FieldInAuthorizedResourceSet { .. }
            | RowFilter::ResourceRelationAllows { .. }
            | RowFilter::ParentRelationAllows { .. } => {
                return Err(anyhow!(
                    "authorization-backed projection filters require authorization index evaluation"
                ));
            }
        }
    }
    Ok(true)
}

fn project_row(
    definition: &ProjectionDefinition,
    source_schema: &TableSchema,
    change: &DecodedSqliteChangesetChange,
    use_new_values: bool,
    target_table: &str,
) -> Result<BTreeMap<String, SqliteChangesetValue>> {
    let mut row = BTreeMap::new();
    for mapping in definition.column_mappings.iter().filter(|mapping| {
        mapping.source_table == change.table_name && mapping.target_table == target_table
    }) {
        let value = value_for_field(
            source_schema,
            change,
            use_new_values,
            &mapping.source_column,
        )?
        .ok_or_else(|| anyhow!("projection source column value missing"))?;
        row.insert(mapping.target_column.clone(), value);
    }
    if row.is_empty() {
        return Err(anyhow!("projection row has no mapped columns"));
    }
    Ok(row)
}

fn value_for_field(
    schema: &TableSchema,
    change: &DecodedSqliteChangesetChange,
    use_new_values: bool,
    field: &str,
) -> Result<Option<SqliteChangesetValue>> {
    let index = schema
        .columns
        .iter()
        .position(|column| column == field)
        .ok_or_else(|| anyhow!("projection source field is absent from schema"))?;
    let values = if use_new_values {
        &change.new_values
    } else {
        &change.old_values
    };
    let value = values
        .get(index)
        .cloned()
        .ok_or_else(|| anyhow!("projection changeset column index is out of range"))?;
    if use_new_values && value.is_none() {
        return change
            .old_values
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow!("projection changeset old column index is out of range"));
    }
    Ok(value)
}

fn load_schema(schema_sql: &str) -> Result<BTreeMap<String, TableSchema>> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(schema_sql)?;
    let mut names = db.prepare(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )?;
    let mut rows = names.query([])?;
    let mut schema = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let table_name: String = row.get(0)?;
        schema.insert(table_name.clone(), read_table_schema(&db, &table_name)?);
    }
    Ok(schema)
}

fn read_table_schema(db: &Connection, table_name: &str) -> Result<TableSchema> {
    let mut stmt = db.prepare(&format!(
        "PRAGMA table_info({})",
        quote_identifier(table_name)
    ))?;
    let mut rows = stmt.query([])?;
    let mut columns = Vec::new();
    let mut primary_key_columns = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let primary_key_ordinal: i64 = row.get(5)?;
        if primary_key_ordinal > 0 {
            primary_key_columns.push(name.clone());
        }
        columns.push(name);
    }
    if columns.is_empty() {
        return Err(anyhow!("schema table has no columns"));
    }
    if primary_key_columns.is_empty() {
        return Err(anyhow!(
            "projection target table has no primary key columns"
        ));
    }
    Ok(TableSchema {
        columns,
        primary_key_columns,
    })
}

fn insert_row(
    db: &Connection,
    table: &str,
    row: &BTreeMap<String, SqliteChangesetValue>,
) -> Result<()> {
    let columns = row.keys().cloned().collect::<Vec<_>>();
    let placeholders = (0..columns.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({placeholders})",
        quote_identifier(table),
        columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let values = columns
        .iter()
        .map(|column| sqlite_value_to_rusqlite(row.get(column).expect("column exists")))
        .collect::<Result<Vec<_>>>()?;
    db.execute(&sql, params_from_iter(values))?;
    Ok(())
}

fn update_row(
    db: &Connection,
    table: &str,
    schema: &TableSchema,
    old_row: &BTreeMap<String, SqliteChangesetValue>,
    new_row: &BTreeMap<String, SqliteChangesetValue>,
) -> Result<()> {
    let set_columns = new_row
        .keys()
        .filter(|column| !schema.primary_key_columns.contains(column))
        .cloned()
        .collect::<Vec<_>>();
    if set_columns.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "UPDATE {} SET {} WHERE {}",
        quote_identifier(table),
        set_columns
            .iter()
            .map(|column| format!("{} = ?", quote_identifier(column)))
            .collect::<Vec<_>>()
            .join(", "),
        primary_key_predicate(schema)
    );
    let mut values = set_columns
        .iter()
        .map(|column| sqlite_value_to_rusqlite(new_row.get(column).expect("column exists")))
        .collect::<Result<Vec<_>>>()?;
    values.extend(primary_key_values(schema, old_row)?);
    db.execute(&sql, params_from_iter(values))?;
    Ok(())
}

fn delete_row(
    db: &Connection,
    table: &str,
    schema: &TableSchema,
    old_row: &BTreeMap<String, SqliteChangesetValue>,
) -> Result<()> {
    let sql = format!(
        "DELETE FROM {} WHERE {}",
        quote_identifier(table),
        primary_key_predicate(schema)
    );
    db.execute(&sql, params_from_iter(primary_key_values(schema, old_row)?))?;
    Ok(())
}

fn primary_key_predicate(schema: &TableSchema) -> String {
    schema
        .primary_key_columns
        .iter()
        .map(|column| format!("{} IS ?", quote_identifier(column)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn primary_key_values(
    schema: &TableSchema,
    row: &BTreeMap<String, SqliteChangesetValue>,
) -> Result<Vec<Value>> {
    schema
        .primary_key_columns
        .iter()
        .map(|column| {
            row.get(column)
                .ok_or_else(|| anyhow!("projection target primary key value missing"))
                .and_then(sqlite_value_to_rusqlite)
        })
        .collect()
}

fn sqlite_value_to_rusqlite(value: &SqliteChangesetValue) -> Result<Value> {
    match value {
        SqliteChangesetValue::Null => Ok(Value::Null),
        SqliteChangesetValue::Integer(value) => Ok(Value::Integer(*value)),
        SqliteChangesetValue::Real(value) => Ok(Value::Real(*value)),
        SqliteChangesetValue::Text(value) => Ok(Value::Text(
            String::from_utf8(value.clone()).context("projection text is not utf8")?,
        )),
        SqliteChangesetValue::Blob(value) => Ok(Value::Blob(value.clone())),
    }
}

fn sqlite_value_literal(value: &SqliteChangesetValue) -> String {
    match value {
        SqliteChangesetValue::Null => String::new(),
        SqliteChangesetValue::Integer(value) => value.to_string(),
        SqliteChangesetValue::Real(value) => value.to_string(),
        SqliteChangesetValue::Text(value) => String::from_utf8_lossy(value).into_owned(),
        SqliteChangesetValue::Blob(value) => hex::encode(value),
    }
}

fn filter_table(filter: &RowFilter) -> &str {
    match filter {
        RowFilter::FieldEqualsLiteral { table, .. }
        | RowFilter::FieldInAuthorizedResourceSet { table, .. }
        | RowFilter::ResourceRelationAllows { table, .. }
        | RowFilter::ParentRelationAllows { table, .. }
        | RowFilter::NotDeleted { table, .. } => table,
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        anvil_personaldb_sqlite_changeset::apply_changeset_to_snapshot_builder,
        personaldb_projection::{
            ColumnMapping, ProjectionResourceBinding, TableMapping, WriteBackPolicy,
        },
    };

    const SOURCE_SCHEMA: &str = "
        CREATE TABLE items(
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            deleted_at TEXT
        );
    ";

    const TARGET_SCHEMA: &str = "
        CREATE TABLE items_projection(
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        );
    ";

    #[test]
    fn projection_builder_maps_insert_changeset_to_target_changeset() {
        let source_changeset = source_insert_changeset();
        let definition = definition(vec![]);
        let built = build_projection_changeset(ProjectionBuildInput {
            source_database_id: "source-db",
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            source_changeset_bytes: &source_changeset,
        })
        .unwrap()
        .expect("projection changeset");

        assert_eq!(built.projected_operation_count, 1);
        let target = Connection::open_in_memory().unwrap();
        target.execute_batch(TARGET_SCHEMA).unwrap();
        apply_changeset_to_snapshot_builder(&target, &built.changeset_bytes).unwrap();
        let name: String = target
            .query_row(
                "SELECT name FROM items_projection WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "alpha");
    }

    #[test]
    fn projection_builder_maps_delete_changeset_to_target_delete() {
        let source_changeset = source_delete_changeset();
        let definition = definition(vec![RowFilter::NotDeleted {
            table: "items".to_string(),
            deleted_field: "deleted_at".to_string(),
        }]);
        let built = build_projection_changeset(ProjectionBuildInput {
            source_database_id: "source-db",
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            source_changeset_bytes: &source_changeset,
        })
        .unwrap()
        .expect("projection changeset");

        let target = Connection::open_in_memory().unwrap();
        target.execute_batch(TARGET_SCHEMA).unwrap();
        target
            .execute(
                "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
                [],
            )
            .unwrap();
        apply_changeset_to_snapshot_builder(&target, &built.changeset_bytes).unwrap();
        let count: i64 = target
            .query_row("SELECT COUNT(*) FROM items_projection", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn projection_builder_maps_update_changeset_to_target_update() {
        let source_changeset = source_update_changeset();
        let definition = definition(vec![RowFilter::FieldEqualsLiteral {
            table: "items".to_string(),
            field: "id".to_string(),
            literal: "1".to_string(),
        }]);
        let built = build_projection_changeset(ProjectionBuildInput {
            source_database_id: "source-db",
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            source_changeset_bytes: &source_changeset,
        })
        .unwrap()
        .expect("projection changeset");

        let target = Connection::open_in_memory().unwrap();
        target.execute_batch(TARGET_SCHEMA).unwrap();
        target
            .execute(
                "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
                [],
            )
            .unwrap();
        apply_changeset_to_snapshot_builder(&target, &built.changeset_bytes).unwrap();
        let name: String = target
            .query_row(
                "SELECT name FROM items_projection WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "beta");
    }

    #[test]
    fn projection_builder_rejects_authorization_backed_filters_until_authz_index_is_available() {
        let source_changeset = source_insert_changeset();
        let definition = definition(vec![RowFilter::ResourceRelationAllows {
            table: "items".to_string(),
            resource_id_field: "id".to_string(),
            relation: "viewer".to_string(),
        }]);
        let err = build_projection_changeset(ProjectionBuildInput {
            source_database_id: "source-db",
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            source_changeset_bytes: &source_changeset,
        })
        .unwrap_err();
        assert!(err.to_string().contains("authorization index evaluation"));
    }

    fn source_insert_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(SOURCE_SCHEMA).unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute(
            "INSERT INTO items (id, name, deleted_at) VALUES (1, 'alpha', NULL)",
            [],
        )
        .unwrap();
        session_changeset(session)
    }

    fn source_delete_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(SOURCE_SCHEMA).unwrap();
        db.execute(
            "INSERT INTO items (id, name, deleted_at) VALUES (1, 'alpha', NULL)",
            [],
        )
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute("DELETE FROM items WHERE id = 1", []).unwrap();
        session_changeset(session)
    }

    fn source_update_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(SOURCE_SCHEMA).unwrap();
        db.execute(
            "INSERT INTO items (id, name, deleted_at) VALUES (1, 'alpha', NULL)",
            [],
        )
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute("UPDATE items SET name = 'beta' WHERE id = 1", [])
            .unwrap();
        session_changeset(session)
    }

    fn session_changeset(mut session: Session<'_>) -> Vec<u8> {
        let mut output = Vec::new();
        session.changeset_strm(&mut output).unwrap();
        assert!(!output.is_empty());
        output
    }

    fn definition(mut row_filters: Vec<RowFilter>) -> ProjectionDefinition {
        if row_filters.is_empty() {
            row_filters.push(RowFilter::NotDeleted {
                table: "items".to_string(),
                deleted_field: "deleted_at".to_string(),
            });
        }
        ProjectionDefinition {
            format_version: 1,
            tenant_id: "1".to_string(),
            database_id: "projection-db".to_string(),
            projection_id: "projection-items".to_string(),
            source_database_ids: vec!["source-db".to_string()],
            target_database_id: "projection-db".to_string(),
            target_actor_or_scope: "scope-primary".to_string(),
            table_mappings: vec![TableMapping {
                source_database_id: "source-db".to_string(),
                source_table: "items".to_string(),
                target_table: "items_projection".to_string(),
            }],
            column_mappings: vec![
                ColumnMapping {
                    source_table: "items".to_string(),
                    source_column: "id".to_string(),
                    target_table: "items_projection".to_string(),
                    target_column: "id".to_string(),
                },
                ColumnMapping {
                    source_table: "items".to_string(),
                    source_column: "name".to_string(),
                    target_table: "items_projection".to_string(),
                    target_column: "name".to_string(),
                },
            ],
            row_filters,
            resource_bindings: vec![ProjectionResourceBinding {
                source_table: "items".to_string(),
                primary_key_column: "id".to_string(),
                resource_type: "items".to_string(),
                resource_id_column: "id".to_string(),
                parent_resource_id_column: None,
            }],
            writeback_policy: WriteBackPolicy::Deny,
            definition_hash: None,
        }
        .seal()
        .unwrap()
    }
}
