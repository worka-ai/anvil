use crate::{
    anvil_personaldb_sqlite_changeset::{
        DecodedSqliteChangesetChange, SqliteChangesetOperation, SqliteChangesetValue,
        iterate_changeset,
    },
    personaldb_projection::{ProjectionDefinition, WriteBackPolicy},
};
use anyhow::{Result, anyhow};
use rusqlite::{Connection, params_from_iter, session::Session, types::Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy)]
pub struct ProjectionWriteBackInput<'a> {
    pub source_schema_sql: &'a str,
    pub target_schema_sql: &'a str,
    pub definition: &'a ProjectionDefinition,
    pub projection_changeset_bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionWriteBackResult {
    pub source_database_id: String,
    pub changeset_bytes: Vec<u8>,
    pub operation_count: usize,
}

#[derive(Debug, Clone)]
struct TableSchema {
    columns: Vec<String>,
    primary_key_columns: Vec<String>,
}

#[derive(Debug, Clone)]
struct SourceOperation {
    table: String,
    old_row: Option<BTreeMap<String, SqliteChangesetValue>>,
    new_row: Option<BTreeMap<String, SqliteChangesetValue>>,
}

pub fn build_projection_writeback_changeset(
    input: ProjectionWriteBackInput<'_>,
) -> Result<ProjectionWriteBackResult> {
    let projection_changes = iterate_changeset(input.projection_changeset_bytes)?;
    let source_schema = load_schema(input.source_schema_sql)?;
    let target_schema = load_schema(input.target_schema_sql)?;
    let source_database_id = single_source_database_id(input.definition)?;
    let operations = plan_source_operations(input, &projection_changes, &target_schema)?;
    if operations.is_empty() {
        return Err(anyhow!("projection write-back has no source row binding"));
    }
    let changeset_bytes =
        encode_source_changeset(input.source_schema_sql, &source_schema, &operations)?;
    if changeset_bytes.is_empty() {
        return Err(anyhow!("projection write-back produced no source mutation"));
    }
    Ok(ProjectionWriteBackResult {
        source_database_id,
        changeset_bytes,
        operation_count: operations.len(),
    })
}

fn single_source_database_id(definition: &ProjectionDefinition) -> Result<String> {
    let mut sources = definition
        .table_mappings
        .iter()
        .map(|mapping| mapping.source_database_id.clone())
        .collect::<BTreeSet<_>>();
    if sources.len() != 1 {
        return Err(anyhow!(
            "projection write-back has ambiguous source database bindings"
        ));
    }
    Ok(sources.pop_first().expect("one source database"))
}

fn plan_source_operations(
    input: ProjectionWriteBackInput<'_>,
    projection_changes: &[DecodedSqliteChangesetChange],
    target_schema: &BTreeMap<String, TableSchema>,
) -> Result<Vec<SourceOperation>> {
    let mut operations = Vec::new();
    for change in projection_changes {
        let target_table_schema = target_schema
            .get(&change.table_name)
            .ok_or_else(|| anyhow!("projection write-back target table is absent from schema"))?;
        let table_mapping = input
            .definition
            .table_mappings
            .iter()
            .filter(|mapping| mapping.target_table == change.table_name)
            .collect::<Vec<_>>();
        if table_mapping.len() != 1 {
            return Err(anyhow!(
                "projection write-back target row does not resolve to one source table"
            ));
        }
        let table_mapping = table_mapping[0];
        let changed_columns = changed_target_columns(change, target_table_schema)?;
        enforce_writeback_policy(&input.definition.writeback_policy, &changed_columns)?;
        ensure_changed_columns_are_mapped(input.definition, &change.table_name, &changed_columns)?;
        let old_row = match change.operation {
            SqliteChangesetOperation::Insert => None,
            SqliteChangesetOperation::Update | SqliteChangesetOperation::Delete => {
                Some(map_projection_row_to_source(
                    input.definition,
                    target_table_schema,
                    change,
                    false,
                    &table_mapping.target_table,
                    &table_mapping.source_table,
                    false,
                )?)
            }
        };
        let new_row = match change.operation {
            SqliteChangesetOperation::Delete => None,
            SqliteChangesetOperation::Insert => Some(map_projection_row_to_source(
                input.definition,
                target_table_schema,
                change,
                true,
                &table_mapping.target_table,
                &table_mapping.source_table,
                false,
            )?),
            SqliteChangesetOperation::Update => Some(map_projection_row_to_source(
                input.definition,
                target_table_schema,
                change,
                true,
                &table_mapping.target_table,
                &table_mapping.source_table,
                true,
            )?),
        };
        operations.push(SourceOperation {
            table: table_mapping.source_table.clone(),
            old_row,
            new_row,
        });
    }
    Ok(operations)
}

fn changed_target_columns(
    change: &DecodedSqliteChangesetChange,
    target_schema: &TableSchema,
) -> Result<BTreeSet<String>> {
    let indexes = match change.operation {
        SqliteChangesetOperation::Insert => change
            .new_values
            .iter()
            .enumerate()
            .filter_map(|(index, value)| value.as_ref().map(|_| index))
            .collect::<Vec<_>>(),
        SqliteChangesetOperation::Update => change.changed_column_indexes.clone(),
        SqliteChangesetOperation::Delete => change
            .old_values
            .iter()
            .enumerate()
            .filter_map(|(index, value)| value.as_ref().map(|_| index))
            .collect::<Vec<_>>(),
    };
    let mut columns = BTreeSet::new();
    for index in indexes {
        let column = target_schema
            .columns
            .get(index)
            .ok_or_else(|| anyhow!("projection write-back column index is out of range"))?;
        columns.insert(column.clone());
    }
    Ok(columns)
}

fn enforce_writeback_policy(
    policy: &WriteBackPolicy,
    changed_columns: &BTreeSet<String>,
) -> Result<()> {
    match policy {
        WriteBackPolicy::Deny => Err(anyhow!(
            "projection write-back is denied by projection policy"
        )),
        WriteBackPolicy::AllowMappedColumns {
            protected_columns,
            allowed_columns,
        } => {
            let protected = protected_columns.iter().collect::<BTreeSet<_>>();
            let allowed = allowed_columns.iter().collect::<BTreeSet<_>>();
            for column in changed_columns {
                if protected.contains(column) {
                    return Err(anyhow!(
                        "projection write-back modifies a protected projection column"
                    ));
                }
                if !allowed.contains(column) {
                    return Err(anyhow!(
                        "projection write-back modifies a column outside the allowed policy"
                    ));
                }
            }
            Ok(())
        }
    }
}

fn ensure_changed_columns_are_mapped(
    definition: &ProjectionDefinition,
    target_table: &str,
    changed_columns: &BTreeSet<String>,
) -> Result<()> {
    for column in changed_columns {
        let count = definition
            .column_mappings
            .iter()
            .filter(|mapping| {
                mapping.target_table == target_table && mapping.target_column == *column
            })
            .count();
        if count != 1 {
            return Err(anyhow!(
                "projection write-back column does not resolve to one source column"
            ));
        }
    }
    Ok(())
}

fn map_projection_row_to_source(
    definition: &ProjectionDefinition,
    target_schema: &TableSchema,
    change: &DecodedSqliteChangesetChange,
    use_new_values: bool,
    target_table: &str,
    source_table: &str,
    changed_only: bool,
) -> Result<BTreeMap<String, SqliteChangesetValue>> {
    let mut row = BTreeMap::new();
    for mapping in definition.column_mappings.iter().filter(|mapping| {
        mapping.target_table == target_table && mapping.source_table == source_table
    }) {
        let Some(target_index) = target_schema
            .columns
            .iter()
            .position(|column| column == &mapping.target_column)
        else {
            return Err(anyhow!(
                "projection write-back target column is absent from schema"
            ));
        };
        if changed_only && !change.changed_column_indexes.contains(&target_index) {
            continue;
        }
        let value = value_at(change, use_new_values, target_index)?;
        if let Some(value) = value {
            row.insert(mapping.source_column.clone(), value);
        }
    }
    if row.is_empty() {
        return Err(anyhow!(
            "projection write-back row has no mapped source columns"
        ));
    }
    Ok(row)
}

fn value_at(
    change: &DecodedSqliteChangesetChange,
    use_new_values: bool,
    index: usize,
) -> Result<Option<SqliteChangesetValue>> {
    let values = if use_new_values {
        &change.new_values
    } else {
        &change.old_values
    };
    let value = values
        .get(index)
        .cloned()
        .ok_or_else(|| anyhow!("projection write-back value index is out of range"))?;
    if use_new_values && value.is_none() {
        return change
            .old_values
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow!("projection write-back old value index is out of range"));
    }
    Ok(value)
}

fn encode_source_changeset(
    source_schema_sql: &str,
    source_schema: &BTreeMap<String, TableSchema>,
    operations: &[SourceOperation],
) -> Result<Vec<u8>> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(source_schema_sql)?;
    for operation in operations {
        if let Some(old_row) = &operation.old_row {
            insert_row(&db, &operation.table, old_row)?;
        }
    }
    let mut session = Session::new(&db)?;
    session.attach::<&str>(None)?;
    for operation in operations {
        let table_schema = source_schema
            .get(&operation.table)
            .ok_or_else(|| anyhow!("projection write-back source table is absent from schema"))?;
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
    Ok(output)
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
        return Err(anyhow!("schema table has no primary key columns"));
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
                .ok_or_else(|| anyhow!("projection write-back primary key value missing"))
                .and_then(sqlite_value_to_rusqlite)
        })
        .collect()
}

fn sqlite_value_to_rusqlite(value: &SqliteChangesetValue) -> Result<Value> {
    match value {
        SqliteChangesetValue::Null => Ok(Value::Null),
        SqliteChangesetValue::Integer(value) => Ok(Value::Integer(*value)),
        SqliteChangesetValue::Real(value) => Ok(Value::Real(*value)),
        SqliteChangesetValue::Text(value) => Ok(Value::Text(String::from_utf8(value.clone())?)),
        SqliteChangesetValue::Blob(value) => Ok(Value::Blob(value.clone())),
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
            ColumnMapping, ProjectionResourceBinding, RowFilter, TableMapping,
        },
    };

    const SOURCE_SCHEMA: &str = "
        CREATE TABLE items(
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            payload BLOB
        );
    ";

    const TARGET_SCHEMA: &str = "
        CREATE TABLE items_projection(
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        );
    ";

    #[test]
    fn writeback_builder_maps_allowed_update_to_source_changeset() {
        let definition = allow_name_definition();
        let built = build_projection_writeback_changeset(ProjectionWriteBackInput {
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            projection_changeset_bytes: &projection_update_changeset(),
        })
        .unwrap();
        assert_eq!(built.source_database_id, "source-db");
        assert_eq!(built.operation_count, 1);

        let source = Connection::open_in_memory().unwrap();
        source.execute_batch(SOURCE_SCHEMA).unwrap();
        source
            .execute(
                "INSERT INTO items (id, name, payload) VALUES (1, 'alpha', NULL)",
                [],
            )
            .unwrap();
        apply_changeset_to_snapshot_builder(&source, &built.changeset_bytes).unwrap();
        let name: String = source
            .query_row("SELECT name FROM items WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(name, "beta");
    }

    #[test]
    fn writeback_builder_rejects_protected_columns() {
        let definition = allow_name_definition();
        let err = build_projection_writeback_changeset(ProjectionWriteBackInput {
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            projection_changeset_bytes: &projection_primary_key_update_changeset(),
        })
        .unwrap_err();
        assert!(err.to_string().contains("protected"));
    }

    #[test]
    fn writeback_builder_maps_insert_to_source_changeset() {
        let definition = allow_name_definition_with_id_insert_delete();
        let built = build_projection_writeback_changeset(ProjectionWriteBackInput {
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            projection_changeset_bytes: &projection_insert_changeset(),
        })
        .unwrap();
        assert_eq!(built.source_database_id, "source-db");
        assert_eq!(built.operation_count, 1);

        let source = Connection::open_in_memory().unwrap();
        source.execute_batch(SOURCE_SCHEMA).unwrap();
        apply_changeset_to_snapshot_builder(&source, &built.changeset_bytes).unwrap();
        let name: String = source
            .query_row("SELECT name FROM items WHERE id = 2", [], |row| row.get(0))
            .unwrap();
        assert_eq!(name, "beta");
    }

    #[test]
    fn writeback_builder_maps_delete_to_source_changeset() {
        let definition = allow_name_definition_with_id_insert_delete();
        let built = build_projection_writeback_changeset(ProjectionWriteBackInput {
            source_schema_sql: SOURCE_SCHEMA,
            target_schema_sql: TARGET_SCHEMA,
            definition: &definition,
            projection_changeset_bytes: &projection_delete_changeset(),
        })
        .unwrap();
        assert_eq!(built.source_database_id, "source-db");
        assert_eq!(built.operation_count, 1);

        let source = Connection::open_in_memory().unwrap();
        source.execute_batch(SOURCE_SCHEMA).unwrap();
        source
            .execute(
                "INSERT INTO items (id, name, payload) VALUES (1, 'alpha', NULL)",
                [],
            )
            .unwrap();
        apply_changeset_to_snapshot_builder(&source, &built.changeset_bytes).unwrap();
        let count: i64 = source
            .query_row("SELECT COUNT(*) FROM items WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    fn allow_name_definition() -> ProjectionDefinition {
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
            row_filters: vec![RowFilter::FieldEqualsLiteral {
                table: "items".to_string(),
                field: "id".to_string(),
                literal: "1".to_string(),
            }],
            resource_bindings: vec![ProjectionResourceBinding {
                source_table: "items".to_string(),
                primary_key_column: "id".to_string(),
                resource_type: "items".to_string(),
                resource_id_column: "id".to_string(),
                parent_resource_id_column: None,
            }],
            writeback_policy: WriteBackPolicy::AllowMappedColumns {
                protected_columns: vec!["id".to_string()],
                allowed_columns: vec!["name".to_string()],
            },
            definition_hash: None,
        }
        .seal()
        .unwrap()
    }

    fn allow_name_definition_with_id_insert_delete() -> ProjectionDefinition {
        let mut definition = allow_name_definition();
        definition.writeback_policy = WriteBackPolicy::AllowMappedColumns {
            protected_columns: Vec::new(),
            allowed_columns: vec!["id".to_string(), "name".to_string()],
        };
        definition.seal().unwrap()
    }

    fn projection_insert_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(TARGET_SCHEMA).unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute(
            "INSERT INTO items_projection (id, name) VALUES (2, 'beta')",
            [],
        )
        .unwrap();
        session_changeset(session)
    }

    fn projection_update_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(TARGET_SCHEMA).unwrap();
        db.execute(
            "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
            [],
        )
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute("UPDATE items_projection SET name = 'beta' WHERE id = 1", [])
            .unwrap();
        session_changeset(session)
    }

    fn projection_delete_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(TARGET_SCHEMA).unwrap();
        db.execute(
            "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
            [],
        )
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute("DELETE FROM items_projection WHERE id = 1", [])
            .unwrap();
        session_changeset(session)
    }

    fn projection_primary_key_update_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(TARGET_SCHEMA).unwrap();
        db.execute(
            "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
            [],
        )
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute("UPDATE items_projection SET id = 2 WHERE id = 1", [])
            .unwrap();
        session_changeset(session)
    }

    fn session_changeset(mut session: Session<'_>) -> Vec<u8> {
        let mut output = Vec::new();
        session.changeset_strm(&mut output).unwrap();
        assert!(!output.is_empty());
        output
    }
}
