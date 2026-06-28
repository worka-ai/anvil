use anyhow::{Context, Result, anyhow};
use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{
    Connection, Error as SqliteError,
    hooks::Action,
    session::{ChangesetIter, ConflictAction, ConflictType, invert_strm},
    types::ValueRef,
};
use std::io::{Cursor, Read};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteChangesetOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqliteChangesetValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedSqliteChangesetChange {
    pub table_name: String,
    pub operation: SqliteChangesetOperation,
    pub indirect: bool,
    pub primary_key_columns: Vec<bool>,
    pub old_values: Vec<Option<SqliteChangesetValue>>,
    pub new_values: Vec<Option<SqliteChangesetValue>>,
    pub changed_column_indexes: Vec<usize>,
}

pub fn iterate_changeset(changeset_bytes: &[u8]) -> Result<Vec<DecodedSqliteChangesetChange>> {
    if changeset_bytes.is_empty() {
        return Err(anyhow!("SQLite changeset must not be empty"));
    }
    let mut cursor = Cursor::new(changeset_bytes);
    let reader: &mut dyn Read = &mut cursor;
    let mut iter = ChangesetIter::start_strm(&reader).context("start SQLite changeset iterator")?;
    let mut changes = Vec::new();
    while let Some(item) = iter.next().context("advance SQLite changeset iterator")? {
        let op = item.op().context("read SQLite changeset operation")?;
        let operation = operation_kind(op.code())?;
        let column_count = usize::try_from(op.number_of_columns())
            .map_err(|_| anyhow!("SQLite changeset column count is negative"))?;
        let primary_key_columns = item
            .pk()
            .context("read SQLite changeset primary key columns")?
            .iter()
            .map(|value| *value != 0)
            .collect::<Vec<_>>();
        if primary_key_columns.len() != column_count {
            return Err(anyhow!(
                "SQLite changeset primary key column bitmap length mismatch"
            ));
        }

        let mut old_values = Vec::with_capacity(column_count);
        let mut new_values = Vec::with_capacity(column_count);
        let mut changed_column_indexes = Vec::new();
        for column in 0..column_count {
            let old_value = match operation {
                SqliteChangesetOperation::Insert => None,
                SqliteChangesetOperation::Update | SqliteChangesetOperation::Delete => {
                    optional_value(item.old_value(column), "old", column)?
                }
            };
            let new_value = match operation {
                SqliteChangesetOperation::Delete => None,
                SqliteChangesetOperation::Insert | SqliteChangesetOperation::Update => {
                    optional_value(item.new_value(column), "new", column)?
                }
            };
            let column_changed = match operation {
                SqliteChangesetOperation::Insert | SqliteChangesetOperation::Update => {
                    new_value.is_some()
                }
                SqliteChangesetOperation::Delete => old_value.is_some(),
            };
            if column_changed {
                changed_column_indexes.push(column);
            }
            old_values.push(old_value);
            new_values.push(new_value);
        }

        changes.push(DecodedSqliteChangesetChange {
            table_name: op.table_name().to_string(),
            operation,
            indirect: op.indirect(),
            primary_key_columns,
            old_values,
            new_values,
            changed_column_indexes,
        });
    }
    if changes.is_empty() {
        return Err(anyhow!("SQLite changeset contains no changes"));
    }
    Ok(changes)
}

pub fn read_table_name(change: &DecodedSqliteChangesetChange) -> &str {
    &change.table_name
}

pub fn read_operation_kind(change: &DecodedSqliteChangesetChange) -> SqliteChangesetOperation {
    change.operation
}

pub fn read_primary_key_columns(change: &DecodedSqliteChangesetChange) -> &[bool] {
    &change.primary_key_columns
}

pub fn read_old_values(change: &DecodedSqliteChangesetChange) -> &[Option<SqliteChangesetValue>] {
    &change.old_values
}

pub fn read_new_values(change: &DecodedSqliteChangesetChange) -> &[Option<SqliteChangesetValue>] {
    &change.new_values
}

pub fn read_changed_columns(change: &DecodedSqliteChangesetChange) -> &[usize] {
    &change.changed_column_indexes
}

pub fn apply_changeset_to_snapshot_builder(
    connection: &Connection,
    changeset_bytes: &[u8],
) -> Result<()> {
    let mut cursor = Cursor::new(changeset_bytes);
    let reader: &mut dyn Read = &mut cursor;
    connection.apply_strm(
        reader,
        None::<fn(&str) -> bool>,
        |_conflict_type: ConflictType, _item| ConflictAction::SQLITE_CHANGESET_ABORT,
    )?;
    Ok(())
}

pub fn invert_changeset_for_diagnostics(changeset_bytes: &[u8]) -> Result<Vec<u8>> {
    let mut input = Cursor::new(changeset_bytes);
    let mut output = Vec::new();
    invert_strm(&mut input, &mut output).context("invert SQLite changeset")?;
    Ok(output)
}

fn operation_kind(action: Action) -> Result<SqliteChangesetOperation> {
    match action {
        Action::SQLITE_INSERT => Ok(SqliteChangesetOperation::Insert),
        Action::SQLITE_UPDATE => Ok(SqliteChangesetOperation::Update),
        Action::SQLITE_DELETE => Ok(SqliteChangesetOperation::Delete),
        Action::UNKNOWN => Err(anyhow!("unsupported SQLite changeset operation")),
        _ => Err(anyhow!("unsupported SQLite changeset operation")),
    }
}

fn optional_value(
    value: rusqlite::Result<ValueRef<'_>>,
    value_kind: &'static str,
    column: usize,
) -> Result<Option<SqliteChangesetValue>> {
    match value {
        Ok(value) => Ok(Some(owned_value(value))),
        Err(SqliteError::InvalidColumnIndex(_)) => Ok(None),
        Err(err) => Err(err).with_context(|| {
            format!("read SQLite changeset {value_kind} value for column {column}")
        }),
    }
}

fn owned_value(value: ValueRef<'_>) -> SqliteChangesetValue {
    match value {
        ValueRef::Null => SqliteChangesetValue::Null,
        ValueRef::Integer(value) => SqliteChangesetValue::Integer(value),
        ValueRef::Real(value) => SqliteChangesetValue::Real(value),
        ValueRef::Text(value) => SqliteChangesetValue::Text(value.to_vec()),
        ValueRef::Blob(value) => SqliteChangesetValue::Blob(value.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::session::Session;

    #[test]
    fn iterates_insert_changeset() {
        let changeset = make_insert_changeset();
        let changes = iterate_changeset(&changeset).unwrap();
        assert_eq!(changes.len(), 1);
        let change = &changes[0];
        assert_eq!(read_table_name(change), "items");
        assert_eq!(
            read_operation_kind(change),
            SqliteChangesetOperation::Insert
        );
        assert_eq!(read_primary_key_columns(change), &[true, false, false]);
        assert!(read_old_values(change).iter().all(Option::is_none));
        assert_eq!(
            read_new_values(change),
            &[
                Some(SqliteChangesetValue::Integer(1)),
                Some(SqliteChangesetValue::Text(b"alpha".to_vec())),
                Some(SqliteChangesetValue::Blob(vec![1, 2, 3])),
            ]
        );
        assert_eq!(read_changed_columns(change), &[0, 1, 2]);
    }

    #[test]
    fn iterates_update_changeset_and_marks_changed_columns() {
        let changeset = make_update_changeset();
        let changes = iterate_changeset(&changeset).unwrap();
        assert_eq!(changes.len(), 1);
        let change = &changes[0];
        assert_eq!(change.operation, SqliteChangesetOperation::Update);
        assert_eq!(change.primary_key_columns, vec![true, false, false]);
        assert_eq!(change.old_values[0], Some(SqliteChangesetValue::Integer(1)));
        assert_eq!(
            change.new_values[1],
            Some(SqliteChangesetValue::Text(b"beta".to_vec()))
        );
        assert_eq!(change.new_values[2], None);
        assert_eq!(change.changed_column_indexes, vec![1]);
    }

    #[test]
    fn iterates_delete_changeset() {
        let changeset = make_delete_changeset();
        let changes = iterate_changeset(&changeset).unwrap();
        assert_eq!(changes.len(), 1);
        let change = &changes[0];
        assert_eq!(change.operation, SqliteChangesetOperation::Delete);
        assert_eq!(
            change.old_values[1],
            Some(SqliteChangesetValue::Text(b"alpha".to_vec()))
        );
        assert!(change.new_values.iter().all(Option::is_none));
        assert_eq!(change.changed_column_indexes, vec![0, 1, 2]);
    }

    #[test]
    fn applies_and_inverts_changeset() {
        let changeset = make_insert_changeset();
        let db = Connection::open_in_memory().unwrap();
        create_table(&db);
        apply_changeset_to_snapshot_builder(&db, &changeset).unwrap();
        let name: String = db
            .query_row("SELECT name FROM items WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(name, "alpha");

        let inverted = invert_changeset_for_diagnostics(&changeset).unwrap();
        let inverted_changes = iterate_changeset(&inverted).unwrap();
        assert_eq!(
            inverted_changes[0].operation,
            SqliteChangesetOperation::Delete
        );
    }

    #[test]
    fn rejects_empty_or_malformed_changeset() {
        assert!(iterate_changeset(&[]).is_err());
        assert!(iterate_changeset(b"not a sqlite changeset").is_err());
    }

    fn make_insert_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        create_table(&db);
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute(
            "INSERT INTO items (id, name, payload) VALUES (1, 'alpha', ?1)",
            [vec![1_u8, 2, 3]],
        )
        .unwrap();
        session_changeset(session)
    }

    fn make_update_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        create_table(&db);
        db.execute(
            "INSERT INTO items (id, name, payload) VALUES (1, 'alpha', ?1)",
            [vec![1_u8, 2, 3]],
        )
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute("UPDATE items SET name = 'beta' WHERE id = 1", [])
            .unwrap();
        session_changeset(session)
    }

    fn make_delete_changeset() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        create_table(&db);
        db.execute(
            "INSERT INTO items (id, name, payload) VALUES (1, 'alpha', ?1)",
            [vec![1_u8, 2, 3]],
        )
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute("DELETE FROM items WHERE id = 1", []).unwrap();
        session_changeset(session)
    }

    fn session_changeset(mut session: Session<'_>) -> Vec<u8> {
        let mut output = Vec::new();
        session.changeset_strm(&mut output).unwrap();
        assert!(!output.is_empty());
        output
    }

    fn create_table(db: &Connection) {
        db.execute_batch(
            "CREATE TABLE items(
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                payload BLOB
            );",
        )
        .unwrap();
    }
}
