use super::*;

pub(super) fn typed_json_value_index_lookups_for_predicate(
    predicate: &TypedPredicate,
) -> Result<Vec<typed_field_segment::TypedFieldValueIndexLookup>, Status> {
    let expected_values = predicate
        .values
        .iter()
        .map(encoded_typed_predicate_value)
        .collect::<Result<Vec<_>, _>>()?;
    let mut lookups = Vec::new();
    match predicate.op.as_str() {
        "eq" | "=" | "==" => {
            if let Some(expected) = expected_values.first() {
                lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                    field_name: predicate.field.clone(),
                    encoded_value: Some(expected.clone()),
                });
            }
        }
        "in" => {
            for expected in expected_values {
                lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                    field_name: predicate.field.clone(),
                    encoded_value: Some(expected),
                });
            }
        }
        "is_null" => {
            lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                field_name: predicate.field.clone(),
                encoded_value: Some(encoded_typed_predicate_value(&JsonValue::Null)?),
            });
            lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                field_name: predicate.field.clone(),
                encoded_value: Some(vec![0x01]),
            });
        }
        "lt" | "<" | "lte" | "<=" | "gt" | ">" | "gte" | ">=" | "exists" | "prefix" => {
            lookups.push(typed_field_segment::TypedFieldValueIndexLookup {
                field_name: predicate.field.clone(),
                encoded_value: None,
            });
        }
        _ => {
            return Err(Status::failed_precondition("IndexCapabilityMissing"));
        }
    }
    lookups.sort();
    lookups.dedup();
    Ok(lookups)
}

pub(super) fn typed_json_predicate_entries_from_entries(
    entries: &[typed_field_segment::TypedFieldValueIndexEntry],
    predicate: &TypedPredicate,
) -> Result<BTreeMap<usize, String>, Status> {
    let ordinals = typed_json_predicate_ordinals_from_entries(entries, predicate)?;
    Ok(entries
        .iter()
        .filter(|entry| ordinals.contains(&entry.row_ordinal))
        .map(|entry| (entry.row_ordinal, entry.source_identity.clone()))
        .collect())
}

pub(super) fn typed_json_predicate_ordinals_from_entries(
    entries: &[typed_field_segment::TypedFieldValueIndexEntry],
    predicate: &TypedPredicate,
) -> Result<BTreeSet<usize>, Status> {
    let null_value = encoded_typed_predicate_value(&JsonValue::Null)?;
    let missing_value = vec![0x01];
    let expected_values = predicate
        .values
        .iter()
        .map(encoded_typed_predicate_value)
        .collect::<Result<Vec<_>, _>>()?;
    let mut ordinals = BTreeSet::new();

    for entry in entries {
        if entry.field_name != predicate.field {
            continue;
        }
        let matched = match predicate.op.as_str() {
            "eq" | "=" | "==" => expected_values
                .first()
                .is_some_and(|expected| entry.encoded_value == *expected),
            "in" => expected_values
                .iter()
                .any(|expected| entry.encoded_value == *expected),
            "lt" | "<" | "lte" | "<=" | "gt" | ">" | "gte" | ">=" => {
                // Range predicates are final-checked against the row's JSON value. The
                // value index narrows to the field posting list without imposing a
                // second comparison model for mixed JSON types.
                !expected_values.is_empty()
            }
            "prefix" => encoded_string_prefix(&expected_values)
                .is_some_and(|prefix| entry.encoded_value.starts_with(&prefix)),
            "exists" => entry.encoded_value != null_value && entry.encoded_value != missing_value,
            "is_null" => entry.encoded_value == null_value || entry.encoded_value == missing_value,
            _ => false,
        };
        if matched {
            ordinals.insert(entry.row_ordinal);
        }
    }

    Ok(ordinals)
}

fn encoded_string_prefix(expected_values: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut prefix = expected_values.first()?.clone();
    if prefix.len() < 3 || prefix.first().copied() != Some(0x30) {
        return None;
    }
    if prefix.ends_with(&[0, 0]) {
        prefix.truncate(prefix.len().saturating_sub(2));
    }
    Some(prefix)
}

fn encoded_typed_predicate_value(value: &JsonValue) -> Result<Vec<u8>, Status> {
    typed_field_segment::encode_json_value_for_typed_index(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid typed predicate value: {e}")))
}
