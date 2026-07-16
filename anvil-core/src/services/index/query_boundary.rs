use super::*;

#[derive(Debug, Clone)]
pub(super) struct BoundaryPredicate {
    pub(super) field: String,
    pub(super) op: String,
    pub(super) values: Vec<JsonValue>,
}

impl BoundaryPredicate {
    pub(super) fn parse_list(raw: &str) -> Result<Vec<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(Vec::new());
        }
        let parsed: JsonValue = serde_json::from_str(raw).map_err(|e| {
            Status::invalid_argument(format!("Invalid boundary_predicates_json: {e}"))
        })?;
        let JsonValue::Array(items) = parsed else {
            return Err(Status::invalid_argument(
                "boundary_predicates_json must be an array",
            ));
        };
        let mut predicates = Vec::new();
        for item in &items {
            parse_boundary_predicate_item(item, &mut predicates)?;
        }
        Ok(predicates)
    }

    pub(super) fn matches_row(&self, row: &TypedIndexRow) -> bool {
        self.matches_value(row.values.get(&self.field).unwrap_or(&JsonValue::Null))
    }

    pub(super) fn matches_metadata(&self, metadata: &JsonValue) -> bool {
        self.matches_value(metadata_filter_value(metadata, &self.field).unwrap_or(&JsonValue::Null))
    }

    fn matches_value(&self, actual: &JsonValue) -> bool {
        match self.op.as_str() {
            "eq" | "=" | "==" => self
                .values
                .first()
                .is_some_and(|expected| actual == expected),
            "in" => self.values.iter().any(|expected| actual == expected),
            "prefix" => self.values.first().is_some_and(|expected| {
                actual
                    .as_str()
                    .zip(expected.as_str())
                    .is_some_and(|(actual, prefix)| actual.starts_with(prefix))
            }),
            "lt" | "<" => self
                .values
                .first()
                .is_some_and(|expected| compare_json_values(actual, expected).is_lt()),
            "lte" | "<=" => self
                .values
                .first()
                .is_some_and(|expected| !compare_json_values(actual, expected).is_gt()),
            "gt" | ">" => self
                .values
                .first()
                .is_some_and(|expected| compare_json_values(actual, expected).is_gt()),
            "gte" | ">=" => self
                .values
                .first()
                .is_some_and(|expected| !compare_json_values(actual, expected).is_lt()),
            "exists" => !actual.is_null(),
            "is_null" => actual.is_null(),
            _ => false,
        }
    }
}

fn parse_boundary_predicate_item(
    item: &JsonValue,
    predicates: &mut Vec<BoundaryPredicate>,
) -> Result<(), Status> {
    if let Some(eq) = item.get("eq") {
        predicates.push(boundary_predicate_from_node(eq, "eq")?);
        return Ok(());
    }
    if let Some(input) = item.get("in") {
        predicates.push(boundary_predicate_from_node(input, "in")?);
        return Ok(());
    }
    if let Some(prefix) = item.get("prefix") {
        predicates.push(boundary_predicate_from_node(prefix, "prefix")?);
        return Ok(());
    }
    if let Some(exists) = item.get("exists") {
        predicates.push(boundary_predicate_from_node(exists, "exists")?);
        return Ok(());
    }
    if let Some(range) = item.get("range") {
        boundary_predicates_from_range_node(range, predicates)?;
        return Ok(());
    }

    let field = boundary_predicate_field(item)
        .ok_or_else(|| Status::invalid_argument("boundary predicate requires field"))?
        .to_string();
    let op = json_optional_string_field(item, "op")
        .or_else(|| json_optional_string_field(item, "operator"))
        .unwrap_or_else(|| "eq".to_string())
        .to_ascii_lowercase();
    let values = if let Some(values) = item.get("values").and_then(JsonValue::as_array) {
        values.clone()
    } else if let Some(value) = item.get("value") {
        vec![value.clone()]
    } else {
        Vec::new()
    };
    predicates.push(BoundaryPredicate { field, op, values });
    Ok(())
}

fn boundary_predicate_from_node(node: &JsonValue, op: &str) -> Result<BoundaryPredicate, Status> {
    let field = node
        .as_str()
        .or_else(|| boundary_predicate_field(node))
        .ok_or_else(|| Status::invalid_argument("boundary predicate node requires field"))?
        .to_string();
    let values = if op == "exists" {
        Vec::new()
    } else if let Some(values) = node.get("values").and_then(JsonValue::as_array) {
        values.clone()
    } else if let Some(value) = node.get("value") {
        vec![value.clone()]
    } else {
        return Err(Status::invalid_argument(
            "boundary predicate node requires value or values",
        ));
    };
    Ok(BoundaryPredicate {
        field,
        op: op.to_string(),
        values,
    })
}

fn boundary_predicates_from_range_node(
    node: &JsonValue,
    predicates: &mut Vec<BoundaryPredicate>,
) -> Result<(), Status> {
    let field = boundary_predicate_field(node)
        .ok_or_else(|| Status::invalid_argument("boundary range predicate requires field"))?
        .to_string();
    for (key, op) in [("gt", "gt"), ("gte", "gte"), ("lt", "lt"), ("lte", "lte")] {
        if let Some(value) = node.get(key) {
            predicates.push(BoundaryPredicate {
                field: field.clone(),
                op: op.to_string(),
                values: vec![value.clone()],
            });
        }
    }
    Ok(())
}
