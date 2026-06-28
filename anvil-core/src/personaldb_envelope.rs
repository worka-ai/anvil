use crate::{
    anvil_personaldb_sqlite_changeset::{
        DecodedSqliteChangesetChange, SqliteChangesetOperation, SqliteChangesetValue,
    },
    formats::{Hash32, hash32, personaldb::RowIndexRecord},
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifiedMutationEnvelope {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub base_log_index: u64,
    pub proposed_log_index: u64,
    pub changeset_payload_hash: String,
    pub schema_hash: String,
    pub policy_epoch: u64,
    pub authz_revision: u64,
    pub table_effects: Vec<TableEffect>,
    pub row_metadata_delta: RowMetadataDelta,
    pub envelope_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableEffect {
    pub table_name: String,
    pub primary_key_hash: String,
    pub operation: TableOperation,
    pub before_columns_hash: Option<String>,
    pub after_columns_hash: Option<String>,
    pub changed_columns: Vec<String>,
    pub source_resource_binding: ResourceBinding,
    pub required_permissions: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TableOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResourceBinding {
    pub resource_type: String,
    pub resource_id: String,
    pub parent_resource_id: Option<String>,
    pub creator_principal: String,
    pub owner_principal: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RowMetadataDelta {
    pub upserts: Vec<RowMetadata>,
    pub deletes: Vec<RowMetadataKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowMetadataKey {
    pub database_id: String,
    pub table_name_hash: String,
    pub primary_key_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowMetadata {
    pub source_database_id: String,
    pub source_table: String,
    pub table_name_hash: String,
    pub primary_key_hash: String,
    pub resource_type: String,
    pub resource_id: String,
    pub parent_resource_id: Option<String>,
    pub creator_principal: String,
    pub owner_principal: Option<String>,
    pub row_version: u64,
    pub policy_epoch: u64,
    pub auth_attribute_hash: String,
    pub updated_at_nanos: i64,
}

impl VerifiedMutationEnvelope {
    pub fn seal(mut self) -> Result<Self> {
        canonicalize_envelope(&mut self);
        validate_unsigned_envelope(&self)?;
        self.envelope_hash = Some(hash_verified_mutation_envelope(&self)?);
        Ok(self)
    }

    pub fn verify(&self) -> Result<()> {
        let mut canonical = self.clone();
        canonicalize_envelope(&mut canonical);
        validate_unsigned_envelope(&canonical)?;
        if canonical != *self {
            return Err(anyhow!("verified mutation envelope is not canonical"));
        }
        let expected = hash_verified_mutation_envelope(self)?;
        if self.envelope_hash.as_deref() != Some(expected.as_str()) {
            return Err(anyhow!("verified mutation envelope hash mismatch"));
        }
        Ok(())
    }

    pub fn envelope_hash32(&self) -> Result<Hash32> {
        self.verify()?;
        decode_hex32(
            self.envelope_hash
                .as_deref()
                .ok_or_else(|| anyhow!("verified mutation envelope hash is missing"))?,
            "envelope_hash",
        )
    }

    pub fn row_index_upserts(&self) -> Result<Vec<RowIndexRecord>> {
        self.verify()?;
        self.row_metadata_delta
            .upserts
            .iter()
            .map(RowMetadata::to_row_index_record)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct PersonalDbEnvelopeDerivationInput<'a> {
    pub tenant_id: i64,
    pub database_id: &'a str,
    pub principal: &'a str,
    pub base_log_index: u64,
    pub proposed_log_index: u64,
    pub changeset_payload_hash: Hash32,
    pub schema_hash: &'a str,
    pub policy_epoch: u64,
    pub authz_revision: u64,
    pub changes: &'a [DecodedSqliteChangesetChange],
    pub updated_at_nanos: i64,
}

pub fn derive_verified_mutation_envelope(
    input: PersonalDbEnvelopeDerivationInput<'_>,
) -> Result<VerifiedMutationEnvelope> {
    if input.changes.is_empty() {
        return Err(anyhow!(
            "verified mutation envelope requires at least one SQLite changeset effect"
        ));
    }
    let mut table_effects = Vec::with_capacity(input.changes.len());
    let mut row_metadata_delta = RowMetadataDelta::default();
    for change in input.changes {
        let table_name_hash = hex::encode(hash32(change.table_name.as_bytes()));
        let primary_key_hash = primary_key_hash(change)?;
        let operation = table_operation(change.operation);
        let before_columns_hash = columns_hash(&change.old_values);
        let after_columns_hash = columns_hash(&change.new_values);
        let resource_binding =
            derived_resource_binding(input.principal, &change.table_name, &primary_key_hash);
        table_effects.push(TableEffect {
            table_name: change.table_name.clone(),
            primary_key_hash: primary_key_hash.clone(),
            operation,
            before_columns_hash: before_columns_hash_for_operation(operation, before_columns_hash),
            after_columns_hash: after_columns_hash_for_operation(operation, after_columns_hash),
            changed_columns: changed_column_names(change),
            source_resource_binding: resource_binding.clone(),
            required_permissions: vec![required_permission(operation).to_string()],
        });
        match operation {
            TableOperation::Insert | TableOperation::Update => {
                row_metadata_delta.upserts.push(RowMetadata {
                    source_database_id: input.database_id.to_string(),
                    source_table: change.table_name.clone(),
                    table_name_hash: table_name_hash.clone(),
                    primary_key_hash: primary_key_hash.clone(),
                    resource_type: resource_binding.resource_type,
                    resource_id: resource_binding.resource_id,
                    parent_resource_id: resource_binding.parent_resource_id,
                    creator_principal: resource_binding.creator_principal,
                    owner_principal: resource_binding.owner_principal,
                    row_version: input.proposed_log_index,
                    policy_epoch: input.policy_epoch,
                    auth_attribute_hash: row_auth_attribute_hash(change, input.policy_epoch),
                    updated_at_nanos: input.updated_at_nanos,
                });
            }
            TableOperation::Delete => {
                row_metadata_delta.deletes.push(RowMetadataKey {
                    database_id: input.database_id.to_string(),
                    table_name_hash,
                    primary_key_hash,
                });
            }
        }
    }
    VerifiedMutationEnvelope {
        format_version: 1,
        tenant_id: input.tenant_id.to_string(),
        database_id: input.database_id.to_string(),
        base_log_index: input.base_log_index,
        proposed_log_index: input.proposed_log_index,
        changeset_payload_hash: hex::encode(input.changeset_payload_hash),
        schema_hash: input.schema_hash.to_string(),
        policy_epoch: input.policy_epoch,
        authz_revision: input.authz_revision,
        table_effects,
        row_metadata_delta,
        envelope_hash: None,
    }
    .seal()
}

impl RowMetadata {
    fn key(&self) -> RowMetadataKey {
        RowMetadataKey {
            database_id: self.source_database_id.clone(),
            table_name_hash: self.table_name_hash.clone(),
            primary_key_hash: self.primary_key_hash.clone(),
        }
    }

    fn to_row_index_record(&self) -> Result<RowIndexRecord> {
        Ok(RowIndexRecord::new(
            self.source_database_id.as_bytes().to_vec(),
            decode_hex32(&self.table_name_hash, "table_name_hash")?,
            decode_hex32(&self.primary_key_hash, "primary_key_hash")?,
            self.resource_type.as_bytes().to_vec(),
            self.resource_id.as_bytes().to_vec(),
            self.parent_resource_id
                .as_deref()
                .unwrap_or("")
                .as_bytes()
                .to_vec(),
            self.creator_principal.as_bytes().to_vec(),
            self.owner_principal
                .as_deref()
                .unwrap_or("")
                .as_bytes()
                .to_vec(),
            self.row_version,
            self.policy_epoch,
            decode_hex32(&self.auth_attribute_hash, "auth_attribute_hash")?,
            self.updated_at_nanos,
        ))
    }
}

pub fn hash_verified_mutation_envelope(envelope: &VerifiedMutationEnvelope) -> Result<String> {
    let mut unsigned = envelope.clone();
    unsigned.envelope_hash = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

fn canonicalize_envelope(envelope: &mut VerifiedMutationEnvelope) {
    envelope.table_effects.sort_by(compare_table_effects);
    for effect in &mut envelope.table_effects {
        effect.changed_columns.sort();
        effect.required_permissions.sort();
    }
    envelope.row_metadata_delta.upserts.sort();
    envelope.row_metadata_delta.deletes.sort();
}

fn compare_table_effects(left: &TableEffect, right: &TableEffect) -> std::cmp::Ordering {
    left.table_name
        .cmp(&right.table_name)
        .then_with(|| left.primary_key_hash.cmp(&right.primary_key_hash))
        .then_with(|| left.operation.cmp(&right.operation))
        .then_with(|| left.changed_columns.cmp(&right.changed_columns))
        .then_with(|| left.required_permissions.cmp(&right.required_permissions))
}

fn validate_unsigned_envelope(envelope: &VerifiedMutationEnvelope) -> Result<()> {
    if envelope.format_version != 1 {
        return Err(anyhow!("unsupported verified mutation envelope version"));
    }
    require_nonempty(&envelope.tenant_id, "tenant_id")?;
    require_nonempty(&envelope.database_id, "database_id")?;
    if envelope.proposed_log_index != envelope.base_log_index.saturating_add(1) {
        return Err(anyhow!(
            "verified mutation envelope log indexes are not contiguous"
        ));
    }
    validate_hex32(&envelope.changeset_payload_hash, "changeset_payload_hash")?;
    validate_hex32(&envelope.schema_hash, "schema_hash")?;
    if envelope.table_effects.is_empty() {
        return Err(anyhow!(
            "verified mutation envelope must include table effects"
        ));
    }
    validate_effects(&envelope.table_effects)?;
    validate_row_metadata_delta(&envelope.row_metadata_delta)?;
    Ok(())
}

fn validate_effects(effects: &[TableEffect]) -> Result<()> {
    let mut keys = BTreeSet::new();
    for effect in effects {
        require_nonempty(&effect.table_name, "table_name")?;
        validate_hex32(&effect.primary_key_hash, "primary_key_hash")?;
        if !keys.insert((
            effect.table_name.as_str(),
            effect.primary_key_hash.as_str(),
            effect.operation,
        )) {
            return Err(anyhow!("duplicate table effect"));
        }
        validate_operation_hashes(effect)?;
        validate_string_set(&effect.changed_columns, "changed_columns")?;
        validate_resource_binding(&effect.source_resource_binding)?;
        validate_string_set(&effect.required_permissions, "required_permissions")?;
    }
    Ok(())
}

fn validate_operation_hashes(effect: &TableEffect) -> Result<()> {
    match effect.operation {
        TableOperation::Insert => {
            if effect.before_columns_hash.is_some() || effect.after_columns_hash.is_none() {
                return Err(anyhow!("insert effect must have only after columns hash"));
            }
        }
        TableOperation::Update => {
            if effect.before_columns_hash.is_none() || effect.after_columns_hash.is_none() {
                return Err(anyhow!(
                    "update effect must have before and after columns hashes"
                ));
            }
        }
        TableOperation::Delete => {
            if effect.before_columns_hash.is_none() || effect.after_columns_hash.is_some() {
                return Err(anyhow!("delete effect must have only before columns hash"));
            }
        }
    }
    if let Some(hash) = &effect.before_columns_hash {
        validate_hex32(hash, "before_columns_hash")?;
    }
    if let Some(hash) = &effect.after_columns_hash {
        validate_hex32(hash, "after_columns_hash")?;
    }
    Ok(())
}

fn validate_resource_binding(binding: &ResourceBinding) -> Result<()> {
    require_nonempty(&binding.resource_type, "resource_type")?;
    require_nonempty(&binding.resource_id, "resource_id")?;
    require_nonempty(&binding.creator_principal, "creator_principal")?;
    if binding
        .parent_resource_id
        .as_deref()
        .is_some_and(str::is_empty)
        || binding
            .owner_principal
            .as_deref()
            .is_some_and(str::is_empty)
    {
        return Err(anyhow!(
            "resource binding optional fields must not be empty"
        ));
    }
    Ok(())
}

fn validate_row_metadata_delta(delta: &RowMetadataDelta) -> Result<()> {
    let mut upsert_keys = BTreeSet::new();
    for row in &delta.upserts {
        validate_row_metadata(row)?;
        if !upsert_keys.insert(row.key()) {
            return Err(anyhow!("duplicate row metadata upsert"));
        }
    }
    let mut delete_keys = BTreeSet::new();
    for key in &delta.deletes {
        validate_row_metadata_key(key)?;
        if !delete_keys.insert(key.clone()) {
            return Err(anyhow!("duplicate row metadata delete"));
        }
        if upsert_keys.contains(key) {
            return Err(anyhow!(
                "row metadata key cannot be both upserted and deleted"
            ));
        }
    }
    Ok(())
}

fn validate_row_metadata(row: &RowMetadata) -> Result<()> {
    require_nonempty(&row.source_database_id, "source_database_id")?;
    require_nonempty(&row.source_table, "source_table")?;
    validate_hex32(&row.table_name_hash, "table_name_hash")?;
    validate_hex32(&row.primary_key_hash, "primary_key_hash")?;
    require_nonempty(&row.resource_type, "resource_type")?;
    require_nonempty(&row.resource_id, "resource_id")?;
    require_nonempty(&row.creator_principal, "creator_principal")?;
    validate_hex32(&row.auth_attribute_hash, "auth_attribute_hash")?;
    if row.parent_resource_id.as_deref().is_some_and(str::is_empty)
        || row.owner_principal.as_deref().is_some_and(str::is_empty)
    {
        return Err(anyhow!("row metadata optional fields must not be empty"));
    }
    Ok(())
}

fn validate_row_metadata_key(key: &RowMetadataKey) -> Result<()> {
    require_nonempty(&key.database_id, "database_id")?;
    validate_hex32(&key.table_name_hash, "table_name_hash")?;
    validate_hex32(&key.primary_key_hash, "primary_key_hash")?;
    Ok(())
}

fn validate_string_set(values: &[String], field: &'static str) -> Result<()> {
    if values.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        require_nonempty(value, field)?;
        if !seen.insert(value) {
            return Err(anyhow!("{field} contains duplicates"));
        }
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn decode_hex32(value: &str, field: &'static str) -> Result<Hash32> {
    validate_hex32(value, field)?;
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("{field} must be hex32"))?)
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

fn table_operation(operation: SqliteChangesetOperation) -> TableOperation {
    match operation {
        SqliteChangesetOperation::Insert => TableOperation::Insert,
        SqliteChangesetOperation::Update => TableOperation::Update,
        SqliteChangesetOperation::Delete => TableOperation::Delete,
    }
}

fn primary_key_hash(change: &DecodedSqliteChangesetChange) -> Result<String> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(change.table_name.as_bytes());
    encoded.push(0);
    let mut saw_primary_key = false;
    for (column, is_primary_key) in change.primary_key_columns.iter().copied().enumerate() {
        if !is_primary_key {
            continue;
        }
        saw_primary_key = true;
        encoded.extend_from_slice(&(column as u32).to_le_bytes());
        let value = change.new_values[column]
            .as_ref()
            .or(change.old_values[column].as_ref())
            .ok_or_else(|| anyhow!("primary key column is absent from SQLite changeset"))?;
        encode_sqlite_value(&mut encoded, value);
    }
    if !saw_primary_key {
        return Err(anyhow!("SQLite changeset has no primary key columns"));
    }
    Ok(hex::encode(hash32(&encoded)))
}

fn columns_hash(values: &[Option<SqliteChangesetValue>]) -> Option<String> {
    if values.iter().all(Option::is_none) {
        return None;
    }
    let mut encoded = Vec::new();
    for (column, value) in values.iter().enumerate() {
        encoded.extend_from_slice(&(column as u32).to_le_bytes());
        match value {
            Some(value) => {
                encoded.push(1);
                encode_sqlite_value(&mut encoded, value);
            }
            None => encoded.push(0),
        }
    }
    Some(hex::encode(hash32(&encoded)))
}

fn encode_sqlite_value(out: &mut Vec<u8>, value: &SqliteChangesetValue) {
    match value {
        SqliteChangesetValue::Null => out.push(0),
        SqliteChangesetValue::Integer(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        SqliteChangesetValue::Real(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        SqliteChangesetValue::Text(value) => {
            out.push(3);
            out.extend_from_slice(&(value.len() as u64).to_le_bytes());
            out.extend_from_slice(value);
        }
        SqliteChangesetValue::Blob(value) => {
            out.push(4);
            out.extend_from_slice(&(value.len() as u64).to_le_bytes());
            out.extend_from_slice(value);
        }
    }
}

fn before_columns_hash_for_operation(
    operation: TableOperation,
    hash: Option<String>,
) -> Option<String> {
    match operation {
        TableOperation::Insert => None,
        TableOperation::Update | TableOperation::Delete => hash,
    }
}

fn after_columns_hash_for_operation(
    operation: TableOperation,
    hash: Option<String>,
) -> Option<String> {
    match operation {
        TableOperation::Insert | TableOperation::Update => hash,
        TableOperation::Delete => None,
    }
}

fn changed_column_names(change: &DecodedSqliteChangesetChange) -> Vec<String> {
    change
        .changed_column_indexes
        .iter()
        .map(|column| format!("column:{column}"))
        .collect()
}

fn derived_resource_binding(
    principal: &str,
    table_name: &str,
    primary_key_hash: &str,
) -> ResourceBinding {
    ResourceBinding {
        resource_type: table_name.to_string(),
        resource_id: format!("{table_name}:{primary_key_hash}"),
        parent_resource_id: None,
        creator_principal: principal.to_string(),
        owner_principal: Some(principal.to_string()),
    }
}

fn required_permission(operation: TableOperation) -> &'static str {
    match operation {
        TableOperation::Insert => "personaldb:insert",
        TableOperation::Update => "personaldb:update",
        TableOperation::Delete => "personaldb:delete",
    }
}

fn row_auth_attribute_hash(change: &DecodedSqliteChangesetChange, policy_epoch: u64) -> String {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(change.table_name.as_bytes());
    encoded.push(0);
    encoded.extend_from_slice(&policy_epoch.to_le_bytes());
    encoded.push(0);
    for value in change
        .new_values
        .iter()
        .chain(change.old_values.iter())
        .flatten()
    {
        encode_sqlite_value(&mut encoded, value);
    }
    hex::encode(hash32(&encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_seals_verifies_and_exports_hash32() {
        let envelope = sample_envelope().seal().unwrap();
        envelope.verify().unwrap();
        assert_eq!(envelope.envelope_hash.as_deref().unwrap().len(), 64);
        assert_eq!(envelope.envelope_hash32().unwrap().len(), 32);
    }

    #[test]
    fn envelope_hash_is_canonical_for_effect_and_permission_order() {
        let mut left = sample_envelope();
        let mut right = sample_envelope();
        right.table_effects.reverse();
        right.table_effects[0].changed_columns.reverse();
        right.table_effects[0].required_permissions.reverse();
        left.row_metadata_delta.upserts.reverse();
        right.row_metadata_delta.deletes.reverse();

        let left = left.seal().unwrap();
        let right = right.seal().unwrap();
        assert_eq!(left.envelope_hash, right.envelope_hash);
        assert_eq!(left, right);
    }

    #[test]
    fn envelope_rejects_tampering_and_noncanonical_values() {
        let mut envelope = sample_envelope().seal().unwrap();
        envelope.table_effects[0].required_permissions.reverse();
        assert!(envelope.verify().is_err());

        let mut tampered = sample_envelope().seal().unwrap();
        tampered.authz_revision += 1;
        assert!(tampered.verify().is_err());
    }

    #[test]
    fn operation_hash_shape_is_validated() {
        let mut invalid_insert = sample_envelope();
        invalid_insert.table_effects[0].before_columns_hash = Some(hex32(9));
        assert!(invalid_insert.seal().is_err());

        let mut invalid_delete = sample_envelope();
        invalid_delete.table_effects[0].operation = TableOperation::Delete;
        invalid_delete.table_effects[0].before_columns_hash = None;
        invalid_delete.table_effects[0].after_columns_hash = None;
        assert!(invalid_delete.seal().is_err());
    }

    #[test]
    fn row_metadata_delta_rejects_duplicate_and_conflicting_keys() {
        let mut duplicate = sample_envelope();
        duplicate
            .row_metadata_delta
            .upserts
            .push(duplicate.row_metadata_delta.upserts[0].clone());
        assert!(duplicate.seal().is_err());

        let mut conflict = sample_envelope();
        conflict
            .row_metadata_delta
            .deletes
            .push(conflict.row_metadata_delta.upserts[0].key());
        assert!(conflict.seal().is_err());
    }

    #[test]
    fn row_metadata_upserts_convert_to_row_index_records() {
        let envelope = sample_envelope().seal().unwrap();
        let rows = envelope.row_index_upserts().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].database_id, b"db-alpha".to_vec());
        assert_eq!(rows[0].table_name_hash, [4; 32]);
        assert_eq!(rows[0].primary_key_hash, [1; 32]);
        assert_eq!(rows[0].resource_id, b"invoice-1".to_vec());
    }

    #[test]
    fn derives_envelope_from_sqlite_changeset_effects_without_client_metadata() {
        let changes = vec![
            decoded_change(SqliteChangesetOperation::Insert),
            decoded_change(SqliteChangesetOperation::Delete),
        ];
        let envelope = derive_verified_mutation_envelope(PersonalDbEnvelopeDerivationInput {
            tenant_id: 7,
            database_id: "db-alpha",
            principal: "principal-a",
            base_log_index: 41,
            proposed_log_index: 42,
            changeset_payload_hash: [2; 32],
            schema_hash: &hex32(3),
            policy_epoch: 5,
            authz_revision: 9,
            changes: &changes,
            updated_at_nanos: 1_717_000_000,
        })
        .unwrap();

        envelope.verify().unwrap();
        assert_eq!(envelope.table_effects.len(), 2);
        assert_eq!(
            envelope
                .table_effects
                .iter()
                .map(|effect| effect.operation)
                .collect::<Vec<_>>(),
            vec![TableOperation::Insert, TableOperation::Delete]
        );
        assert_eq!(envelope.row_metadata_delta.upserts.len(), 1);
        assert_eq!(envelope.row_metadata_delta.deletes.len(), 1);
        assert_eq!(
            envelope.table_effects[0].required_permissions,
            vec!["personaldb:insert".to_string()]
        );
        assert!(
            envelope.table_effects[0]
                .changed_columns
                .contains(&"column:1".to_string())
        );
        assert!(envelope.table_effects[0].primary_key_hash.len() == 64);
    }

    #[test]
    fn envelope_derivation_rejects_missing_primary_key_values() {
        let mut change = decoded_change(SqliteChangesetOperation::Update);
        change.old_values[0] = None;
        change.new_values[0] = None;
        let err = derive_verified_mutation_envelope(PersonalDbEnvelopeDerivationInput {
            tenant_id: 7,
            database_id: "db-alpha",
            principal: "principal-a",
            base_log_index: 41,
            proposed_log_index: 42,
            changeset_payload_hash: [2; 32],
            schema_hash: &hex32(3),
            policy_epoch: 5,
            authz_revision: 9,
            changes: &[change],
            updated_at_nanos: 1_717_000_000,
        })
        .unwrap_err();
        assert!(err.to_string().contains("primary key column"));
    }

    fn sample_envelope() -> VerifiedMutationEnvelope {
        VerifiedMutationEnvelope {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            base_log_index: 41,
            proposed_log_index: 42,
            changeset_payload_hash: hex32(2),
            schema_hash: hex32(3),
            policy_epoch: 5,
            authz_revision: 9,
            table_effects: vec![
                TableEffect {
                    table_name: "invoice".to_string(),
                    primary_key_hash: hex32(1),
                    operation: TableOperation::Insert,
                    before_columns_hash: None,
                    after_columns_hash: Some(hex32(8)),
                    changed_columns: vec!["total".to_string(), "status".to_string()],
                    source_resource_binding: ResourceBinding {
                        resource_type: "invoice".to_string(),
                        resource_id: "invoice-1".to_string(),
                        parent_resource_id: Some("account-1".to_string()),
                        creator_principal: "user-a".to_string(),
                        owner_principal: Some("user-a".to_string()),
                    },
                    required_permissions: vec![
                        "invoice:write".to_string(),
                        "invoice:read".to_string(),
                    ],
                },
                TableEffect {
                    table_name: "line_item".to_string(),
                    primary_key_hash: hex32(6),
                    operation: TableOperation::Update,
                    before_columns_hash: Some(hex32(10)),
                    after_columns_hash: Some(hex32(11)),
                    changed_columns: vec!["name".to_string()],
                    source_resource_binding: ResourceBinding {
                        resource_type: "invoice".to_string(),
                        resource_id: "invoice-1".to_string(),
                        parent_resource_id: Some("account-1".to_string()),
                        creator_principal: "user-a".to_string(),
                        owner_principal: Some("user-a".to_string()),
                    },
                    required_permissions: vec!["invoice:write".to_string()],
                },
            ],
            row_metadata_delta: RowMetadataDelta {
                upserts: vec![RowMetadata {
                    source_database_id: "db-alpha".to_string(),
                    source_table: "invoice".to_string(),
                    table_name_hash: hex32(4),
                    primary_key_hash: hex32(1),
                    resource_type: "invoice".to_string(),
                    resource_id: "invoice-1".to_string(),
                    parent_resource_id: Some("account-1".to_string()),
                    creator_principal: "user-a".to_string(),
                    owner_principal: Some("user-a".to_string()),
                    row_version: 42,
                    policy_epoch: 5,
                    auth_attribute_hash: hex32(5),
                    updated_at_nanos: 1_717_000_000,
                }],
                deletes: vec![RowMetadataKey {
                    database_id: "db-alpha".to_string(),
                    table_name_hash: hex32(7),
                    primary_key_hash: hex32(12),
                }],
            },
            envelope_hash: None,
        }
    }

    fn hex32(seed: u8) -> String {
        hex::encode([seed; 32])
    }

    fn decoded_change(operation: SqliteChangesetOperation) -> DecodedSqliteChangesetChange {
        let old_values = match operation {
            SqliteChangesetOperation::Insert => vec![None, None, None],
            SqliteChangesetOperation::Update => vec![
                Some(SqliteChangesetValue::Integer(1)),
                Some(SqliteChangesetValue::Text(b"alpha".to_vec())),
                None,
            ],
            SqliteChangesetOperation::Delete => vec![
                Some(SqliteChangesetValue::Integer(2)),
                Some(SqliteChangesetValue::Text(b"alpha".to_vec())),
                None,
            ],
        };
        let new_values = match operation {
            SqliteChangesetOperation::Insert => vec![
                Some(SqliteChangesetValue::Integer(1)),
                Some(SqliteChangesetValue::Text(b"alpha".to_vec())),
                Some(SqliteChangesetValue::Blob(vec![1, 2, 3])),
            ],
            SqliteChangesetOperation::Update => vec![
                Some(SqliteChangesetValue::Integer(1)),
                Some(SqliteChangesetValue::Text(b"beta".to_vec())),
                None,
            ],
            SqliteChangesetOperation::Delete => vec![None, None, None],
        };
        DecodedSqliteChangesetChange {
            table_name: "items".to_string(),
            operation,
            indirect: false,
            primary_key_columns: vec![true, false, false],
            old_values,
            new_values,
            changed_column_indexes: match operation {
                SqliteChangesetOperation::Insert => vec![0, 1, 2],
                SqliteChangesetOperation::Update => vec![1],
                SqliteChangesetOperation::Delete => vec![0, 1],
            },
        }
    }
}
