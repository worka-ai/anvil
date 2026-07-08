use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AnvilQuerySpec {
    pub(super) schema: String,
    pub(super) scope: AnvilQueryScope,
    #[serde(default = "default_query_source_kind")]
    pub(super) source_kind: String,
    #[serde(rename = "where", default)]
    pub(super) predicates: AnvilQueryWhere,
    #[serde(default)]
    pub(super) order_by: Vec<AnvilQueryOrder>,
    #[serde(default)]
    pub(super) limit: Option<u32>,
    #[serde(default)]
    pub(super) consistency: AnvilQueryConsistency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AnvilQueryScope {
    #[serde(default)]
    pub(super) mesh_id: Option<String>,
    #[serde(default)]
    pub(super) anvil_storage_tenant_id: Option<String>,
    #[serde(default)]
    pub(super) authz_scope: Option<JsonValue>,
    pub(super) bucket_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct AnvilQueryWhere {
    #[serde(default)]
    pub(super) all: Vec<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AnvilQueryOrder {
    pub(super) field: String,
    #[serde(default = "default_ascending")]
    pub(super) direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct AnvilQueryConsistency {
    #[serde(default)]
    pub(super) min_source_cursor: Option<JsonValue>,
    #[serde(default)]
    pub(super) min_authz_revision: Option<JsonValue>,
    #[serde(default)]
    pub(super) allow_stale_index: Option<bool>,
}

#[derive(Debug, Clone)]
pub(super) struct QuerySpecShape {
    pub(super) source_kind: String,
    pub(super) path_prefix: Option<String>,
    pub(super) typed_predicates: Vec<JsonValue>,
    pub(super) typed_order: Vec<TypedOrder>,
    pub(super) query_text: Option<String>,
    pub(super) query_vector: Option<Vec<f32>>,
    pub(super) phrase: bool,
    pub(super) can_relation: Option<String>,
    pub(super) min_source_cursor: Option<u64>,
    pub(super) min_authz_revision: Option<u64>,
    pub(super) limit: u32,
}

#[derive(Debug, Clone)]
pub(super) struct QuerySpecPlan {
    pub(super) index: crate::persistence::IndexDefinition,
    pub(super) typed_filter_index: Option<crate::persistence::IndexDefinition>,
    pub(super) canonical_query_hash: String,
    pub(super) plan_json: String,
    pub(super) diagnostics: Vec<String>,
    pub(super) query_text: String,
    pub(super) query_vector: Vec<f32>,
    pub(super) phrase: bool,
    pub(super) path_prefix: String,
    pub(super) typed_predicates: Vec<JsonValue>,
    pub(super) typed_order: Vec<TypedOrder>,
    pub(super) limit: u32,
    pub(super) require_caught_up_to_watch_cursor: String,
}

#[derive(Debug, Clone)]
pub(super) struct QuerySpecIndexSelection {
    pub(super) primary: crate::persistence::IndexDefinition,
    pub(super) typed_filter: Option<crate::persistence::IndexDefinition>,
}

impl QuerySpecPlan {
    pub(super) fn single_query_request(
        &self,
        bucket_name: &str,
        page_token: &str,
        lag_timeout_ms: u64,
    ) -> Result<QueryIndexRequest, Status> {
        Ok(QueryIndexRequest {
            bucket_name: bucket_name.to_string(),
            index_name: self.index.name.clone(),
            query_text: self.query_text.clone(),
            query_vector: self.query_vector.clone(),
            limit: self.limit,
            phrase: self.phrase,
            path_prefix: self.path_prefix.clone(),
            metadata_filters_json: String::new(),
            typed_predicates_json: serde_json::to_string(&self.typed_predicates)
                .map_err(|e| Status::internal(e.to_string()))?,
            typed_order_json: serde_json::to_string(&self.typed_order)
                .map_err(|e| Status::internal(e.to_string()))?,
            page_token: page_token.to_string(),
            require_caught_up_to_watch_cursor: self.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        })
    }
}

pub(super) fn default_query_source_kind() -> String {
    "object_current".to_string()
}

impl AnvilQuerySpec {
    pub(super) fn parse(raw: &str) -> Result<Self, Status> {
        let spec: Self = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid QuerySpec JSON: {e}")))?;
        if spec.schema != "anvil.query.spec.v1" {
            return Err(Status::invalid_argument("Invalid QuerySpec schema"));
        }
        if let Some(mesh_id) = spec.scope.mesh_id.as_deref()
            && mesh_id.trim().is_empty()
        {
            return Err(Status::invalid_argument("QuerySpec scope.mesh_id is empty"));
        }
        if let Some(authz_scope) = spec.scope.authz_scope.as_ref()
            && !authz_scope.is_object()
        {
            return Err(Status::invalid_argument(
                "QuerySpec scope.authz_scope must be an object",
            ));
        }
        Ok(spec)
    }

    pub(super) fn canonical_json(&self) -> Result<String, Status> {
        serde_json::to_string(
            &serde_json::to_value(self)
                .map_err(|e| Status::internal(format!("Serialize QuerySpec: {e}")))?,
        )
        .map_err(|e| Status::internal(format!("Canonicalize QuerySpec: {e}")))
    }

    pub(super) fn shape(&self) -> Result<QuerySpecShape, Status> {
        let mut path_prefix = None;
        let mut typed_predicates = Vec::new();
        let mut query_text = None;
        let mut query_vector = None;
        let mut phrase = false;
        let mut can_relation = None;

        for predicate in &self.predicates.all {
            if let Some(value) = predicate.get("path_prefix").and_then(JsonValue::as_str) {
                path_prefix = Some(value.to_string());
                continue;
            }
            if let Some(field) = predicate.get("field").and_then(JsonValue::as_str) {
                let op = predicate
                    .get("op")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        Status::invalid_argument("QuerySpec field predicate requires op")
                    })?;
                let value = predicate
                    .get("value")
                    .or_else(|| predicate.get("values"))
                    .cloned()
                    .unwrap_or(JsonValue::Null);
                let values = match value {
                    JsonValue::Array(values) if op.eq_ignore_ascii_case("in") => values,
                    other => vec![other],
                };
                typed_predicates.push(serde_json::json!({
                    "field": field,
                    "op": op,
                    "values": values,
                }));
                continue;
            }
            if let Some(full_text) = predicate.get("full_text") {
                let query = full_text
                    .get("query")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        Status::invalid_argument("QuerySpec full_text predicate requires query")
                    })?;
                query_text = Some(query.to_string());
                phrase = full_text
                    .get("phrase")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                continue;
            }
            if let Some(vector) = predicate.get("vector") {
                let near = vector
                    .get("near")
                    .and_then(JsonValue::as_array)
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "QuerySpec vector.near must be an inline numeric vector",
                        )
                    })?;
                let mut values = Vec::with_capacity(near.len());
                for value in near {
                    let Some(value) = value.as_f64() else {
                        return Err(Status::invalid_argument(
                            "QuerySpec vector.near values must be numeric",
                        ));
                    };
                    values.push(value as f32);
                }
                query_vector = Some(values);
                continue;
            }
            if let Some(can) = predicate.get("can") {
                let relation =
                    can.get("relation")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| {
                            Status::invalid_argument("QuerySpec can predicate requires relation")
                        })?;
                can_relation = Some(relation.to_string());
                continue;
            }
            return Err(Status::invalid_argument("Unsupported QuerySpec predicate"));
        }

        let typed_order = self
            .order_by
            .iter()
            .map(|order| {
                let direction = order.direction.to_ascii_lowercase();
                if direction != "asc" && direction != "desc" {
                    return Err(Status::invalid_argument(
                        "QuerySpec order_by direction must be asc or desc",
                    ));
                }
                Ok(TypedOrder {
                    field: order.field.clone(),
                    direction,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QuerySpecShape {
            source_kind: self.source_kind.clone(),
            path_prefix,
            typed_predicates,
            typed_order,
            query_text,
            query_vector,
            phrase,
            can_relation,
            min_source_cursor: parse_optional_u64_json(
                self.consistency.min_source_cursor.as_ref(),
                "min_source_cursor",
            )?,
            min_authz_revision: parse_optional_u64_json(
                self.consistency.min_authz_revision.as_ref(),
                "min_authz_revision",
            )?,
            limit: self.limit.unwrap_or(100),
        })
    }
}

pub(super) fn parse_optional_u64_json(
    value: Option<&JsonValue>,
    label: &str,
) -> Result<Option<u64>, Status> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(number)) => number
            .as_u64()
            .ok_or_else(|| Status::invalid_argument(format!("QuerySpec {label} must be u64")))
            .map(Some),
        Some(JsonValue::String(value)) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| Status::invalid_argument(format!("QuerySpec {label} must be u64"))),
        _ => Err(Status::invalid_argument(format!(
            "QuerySpec {label} must be a string or integer"
        ))),
    }
}

pub(super) fn select_query_spec_indexes(
    indexes: &[crate::persistence::IndexDefinition],
    shape: &QuerySpecShape,
    accept_degraded: bool,
) -> Result<QuerySpecIndexSelection, Status> {
    let needs_typed = !shape.typed_predicates.is_empty() || !shape.typed_order.is_empty();
    let needs_text = shape.query_text.is_some();
    let needs_vector = shape.query_vector.is_some();
    let needs_path_only =
        shape.path_prefix.is_some() && !needs_typed && !needs_text && !needs_vector;

    let typed_filter = if needs_typed {
        Some(
            indexes
                .iter()
                .filter(|index| index.enabled && index.kind == "typed_json")
                .find(|index| typed_json_index_covers(index, shape).unwrap_or(false))
                .cloned()
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "QuerySpec has no typed_json index covering predicates",
                    )
                })?,
        )
    } else {
        None
    };

    if needs_text && needs_vector {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "hybrid")
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("QuerySpec text+vector plan requires a hybrid index")
            })?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if let Some(typed_filter) = typed_filter.clone()
        && !needs_text
        && !needs_vector
    {
        return Ok(QuerySpecIndexSelection {
            primary: typed_filter,
            typed_filter: None,
        });
    }
    if needs_text {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "full_text")
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no full_text index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if needs_vector {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "vector")
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no vector index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if needs_path_only {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && matches!(index.kind.as_str(), "path" | "typed_json"))
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no path-capable index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter: None,
        });
    }
    if accept_degraded {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "path")
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("QuerySpec has no bounded primitive index")
            })?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter: None,
        });
    }
    Err(Status::failed_precondition(
        "QuerySpec requires at least one bounded primitive predicate",
    ))
}

pub(super) fn typed_json_index_covers(
    index: &crate::persistence::IndexDefinition,
    shape: &QuerySpecShape,
) -> Result<bool, Status> {
    let definition = TypedJsonIndexDefinition::from_index(index)?;
    if definition.source_kind != shape.source_kind {
        return Ok(false);
    }
    let fields = definition
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for predicate in &shape.typed_predicates {
        let Some(field) = predicate.get("field").and_then(JsonValue::as_str) else {
            return Ok(false);
        };
        if !fields.contains(field) {
            return Ok(false);
        }
    }
    for order in &shape.typed_order {
        if !fields.contains(order.field.as_str()) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn query_spec_overfetch_limit(requested: u32) -> u32 {
    requested.saturating_mul(10).clamp(100, 1000)
}

pub(super) fn composite_query_spec_index_name(
    primary: &crate::persistence::IndexDefinition,
    typed_filter: &crate::persistence::IndexDefinition,
) -> String {
    format!("{}+{}", primary.name, typed_filter.name)
}

pub(super) fn composite_index_definition_version(
    primary: &crate::persistence::IndexDefinition,
    typed_filter: &crate::persistence::IndexDefinition,
) -> u64 {
    let primary = u64::try_from(primary.version).unwrap_or(0);
    let typed_filter = u64::try_from(typed_filter.version).unwrap_or(0);
    primary.max(typed_filter)
}

pub(super) fn composite_query_spec_predicate_hash(
    plan: &QuerySpecPlan,
    primary_generation: u64,
    typed_generation: u64,
) -> String {
    let shape = serde_json::json!({
        "canonical_query_hash": plan.canonical_query_hash,
        "primary_index": plan.index.name,
        "primary_generation": primary_generation,
        "typed_filter_index": plan.typed_filter_index.as_ref().map(|index| index.name.as_str()),
        "typed_generation": typed_generation,
    });
    blake3::hash(shape.to_string().as_bytes())
        .to_hex()
        .to_string()
}

pub(super) fn composite_query_spec_order_hash(plan: &QuerySpecPlan) -> String {
    if plan.typed_order.is_empty() {
        score_order_hash()
    } else {
        stable_json_hash(&serde_json::to_string(&plan.typed_order).unwrap_or_default())
    }
}

pub(super) fn typed_values_from_query_hit(
    hit: &IndexQueryHit,
) -> Result<BTreeMap<String, JsonValue>, Status> {
    let metadata: JsonValue = serde_json::from_str(&hit.metadata_json)
        .map_err(|e| Status::internal(format!("Invalid query hit metadata_json: {e}")))?;
    let Some(values) = metadata.get("typed_values").and_then(JsonValue::as_object) else {
        return Err(Status::internal(
            "typed query hit metadata is missing typed_values",
        ));
    };
    Ok(values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect())
}

pub(super) fn merge_composite_metadata(
    primary_metadata_json: &str,
    typed_values: &BTreeMap<String, JsonValue>,
) -> Result<String, Status> {
    let primary = serde_json::from_str::<JsonValue>(primary_metadata_json)
        .unwrap_or_else(|_| JsonValue::String(primary_metadata_json.to_string()));
    serde_json::to_string(&serde_json::json!({
        "primary": primary,
        "typed_values": typed_values,
    }))
    .map_err(|e| Status::internal(format!("Serialize composite metadata: {e}")))
}

pub(super) fn compare_query_spec_hits_by_typed_order(
    left: &IndexQueryHit,
    right: &IndexQueryHit,
    plan: &QuerySpecPlan,
) -> std::cmp::Ordering {
    let left_values = typed_values_from_query_hit(left).unwrap_or_default();
    let right_values = typed_values_from_query_hit(right).unwrap_or_default();
    for term in &plan.typed_order {
        let ordering = compare_json_values(
            left_values.get(&term.field).unwrap_or(&JsonValue::Null),
            right_values.get(&term.field).unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.object_version_id.cmp(&right.object_version_id)
}

pub(super) fn query_spec_hit_sort_values(
    hit: &IndexQueryHit,
    plan: &QuerySpecPlan,
) -> Result<BTreeMap<String, JsonValue>, Status> {
    if plan.typed_order.is_empty() {
        Ok(score_sort_values(hit.score, &hit.object_version_id))
    } else {
        typed_values_from_query_hit(hit)
    }
}

pub(super) fn query_spec_hit_after_cursor(
    hit: &IndexQueryHit,
    token: &IndexPageToken,
    plan: &QuerySpecPlan,
) -> Result<bool, Status> {
    if plan.typed_order.is_empty() {
        return score_after_cursor(hit.score, &hit.object_version_id, token);
    }
    let values = typed_values_from_query_hit(hit)?;
    for term in &plan.typed_order {
        let ordering = compare_json_values(
            values.get(&term.field).unwrap_or(&JsonValue::Null),
            token
                .last_sort_values
                .get(&term.field)
                .unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return Ok(ordering.is_gt());
        }
    }
    Ok(hit.object_version_id > token.last_source_identity)
}

#[derive(Debug, Clone, Default)]
pub(super) struct QueryObjectRef {
    pub(super) object_version_id: String,
    pub(super) object_key: String,
    pub(super) user_meta: Option<JsonValue>,
    pub(super) created_at_nanos: i64,
    pub(super) authz_revision: i64,
}

impl QueryObjectRef {
    pub(super) fn from_typed_field_row(
        row: &typed_field_segment::TypedFieldSegmentRow,
    ) -> Result<Self, Status> {
        let user_meta = JsonValue::Object(
            row.values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        Ok(Self {
            object_version_id: row.object_version_id.clone(),
            object_key: row.object_key.clone(),
            user_meta: Some(user_meta),
            created_at_nanos: 0,
            authz_revision: i64::try_from(row.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct QueryPermissionFilter {
    pub(super) object_keys: BTreeSet<String>,
    pub(super) authorized_labels: BTreeSet<[u8; 32]>,
}

impl QueryPermissionFilter {
    pub(super) fn allows_object_key(&self, object_key: &str) -> bool {
        self.object_keys.contains(object_key)
    }
}

pub(super) fn collect_object_scope(
    scope: &str,
    bucket_name: &str,
    object_keys: &mut BTreeSet<String>,
    object_key_prefixes: &mut BTreeSet<String>,
    grants_bucket_read: &mut bool,
) {
    let Some((action, resource)) = scope.split_once('|') else {
        return;
    };
    if !matches!(action, "object:read" | "object:*" | "*") {
        return;
    }
    if resource == "*" {
        *grants_bucket_read = true;
        return;
    }
    let bucket_prefix = format!("{bucket_name}/");
    let Some(key_pattern) = resource.strip_prefix(&bucket_prefix) else {
        return;
    };
    if key_pattern.is_empty() {
        *grants_bucket_read = true;
        return;
    }
    if key_pattern.ends_with('*') {
        let prefix = key_pattern.trim_end_matches('*');
        if prefix.is_empty() {
            *grants_bucket_read = true;
        } else {
            object_key_prefixes.insert(prefix.to_string());
        }
        return;
    }
    object_keys.insert(key_pattern.to_string());
}

pub(super) fn query_object_authz_label_hash(
    bucket: &crate::persistence::Bucket,
    object: &crate::persistence::Object,
) -> [u8; 32] {
    hash32(
        format!(
            "tenant:{}:bucket:{}:object:{}:authz:{}",
            bucket.tenant_id, bucket.id, object.key, object.authz_revision
        )
        .as_bytes(),
    )
}

#[derive(Debug, Clone, Default)]
pub(super) struct QueryFilters {
    pub(super) path_prefix: Option<String>,
    pub(super) metadata: Vec<MetadataFilter>,
}

#[derive(Debug, Clone)]
pub(super) struct MetadataFilter {
    pub(super) field: String,
    pub(super) expected: JsonValue,
}

impl QueryFilters {
    pub(super) fn from_request(req: &QueryIndexRequest) -> Result<Self, Status> {
        let path_prefix = if req.path_prefix.trim().is_empty() {
            None
        } else {
            Some(req.path_prefix.clone())
        };
        let metadata = parse_metadata_filters(&req.metadata_filters_json)?;
        Ok(Self {
            path_prefix,
            metadata,
        })
    }

    pub(super) fn matches(&self, object_ref: &QueryObjectRef) -> Result<bool, Status> {
        if let Some(path_prefix) = &self.path_prefix
            && !object_ref.object_key.starts_with(path_prefix)
        {
            return Ok(false);
        }
        if self.metadata.is_empty() {
            return Ok(true);
        }
        let Some(metadata) = object_ref.user_meta.as_ref() else {
            return Ok(false);
        };
        for filter in &self.metadata {
            let Some(actual) = metadata_filter_value(metadata, &filter.field) else {
                return Ok(false);
            };
            if actual != &filter.expected {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub(super) struct TypedJsonIndexDefinition {
    pub(super) source_kind: String,
    pub(super) fields: Vec<TypedFieldDefinition>,
    pub(super) default_order: Vec<TypedOrder>,
}

#[derive(Debug, Clone)]
pub(super) struct TypedFieldDefinition {
    pub(super) name: String,
}

impl TypedJsonIndexDefinition {
    pub(super) fn from_index(index: &crate::persistence::IndexDefinition) -> Result<Self, Status> {
        let source_kind = json_optional_string_field(&index.build_policy, "source_kind")
            .or_else(|| json_optional_string_field(&index.build_policy, "source"))
            .unwrap_or_else(|| "object_current".to_string());
        let fields_json = index
            .build_policy
            .get("fields")
            .or_else(|| index.extractor.get("fields"))
            .ok_or_else(|| Status::invalid_argument("typed_json index requires fields"))?;
        let JsonValue::Array(field_values) = fields_json else {
            return Err(Status::invalid_argument(
                "typed_json fields must be an array",
            ));
        };
        let mut fields = Vec::with_capacity(field_values.len());
        for value in field_values {
            let name = json_optional_string_field(value, "name")
                .ok_or_else(|| Status::invalid_argument("typed_json field requires name"))?;
            let extractor = json_optional_string_field(value, "extractor")
                .or_else(|| json_optional_string_field(value, "json_pointer"))
                .ok_or_else(|| Status::invalid_argument("typed_json field requires extractor"))?;
            validate_typed_extractor(&source_kind, &extractor)?;
            fields.push(TypedFieldDefinition { name });
        }
        let default_order = index
            .build_policy
            .get("default_order")
            .map(TypedOrder::parse_json_array)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            source_kind,
            fields,
            default_order,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct TypedIndexRow {
    pub(super) object_key: String,
    pub(super) object_version_id: String,
    pub(super) source_identity: String,
    pub(super) values: BTreeMap<String, JsonValue>,
}

impl TypedIndexRow {
    pub(super) fn from_segment_row(row: typed_field_segment::TypedFieldSegmentRow) -> Self {
        Self {
            object_key: row.object_key,
            object_version_id: row.object_version_id,
            source_identity: row.source_identity,
            values: row.values,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct TypedPredicate {
    pub(super) field: String,
    pub(super) op: String,
    pub(super) values: Vec<JsonValue>,
}

impl TypedPredicate {
    pub(super) fn parse_list(raw: &str) -> Result<Vec<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(Vec::new());
        }
        let parsed: JsonValue = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid typed_predicates_json: {e}")))?;
        let JsonValue::Array(items) = parsed else {
            return Err(Status::invalid_argument(
                "typed_predicates_json must be an array",
            ));
        };
        items
            .iter()
            .map(|item| {
                let field = json_optional_string_field(item, "field")
                    .or_else(|| json_optional_string_field(item, "field_name"))
                    .ok_or_else(|| Status::invalid_argument("typed predicate requires field"))?;
                let op = json_optional_string_field(item, "op")
                    .or_else(|| json_optional_string_field(item, "operator"))
                    .ok_or_else(|| Status::invalid_argument("typed predicate requires op"))?
                    .to_ascii_lowercase();
                let values = if let Some(values) = item.get("values").and_then(JsonValue::as_array)
                {
                    values.clone()
                } else if let Some(value) = item.get("value") {
                    vec![value.clone()]
                } else {
                    Vec::new()
                };
                Ok(Self { field, op, values })
            })
            .collect()
    }

    pub(super) fn matches(&self, row: &TypedIndexRow) -> bool {
        let actual = row.values.get(&self.field).unwrap_or(&JsonValue::Null);
        match self.op.as_str() {
            "eq" | "=" | "==" => self
                .values
                .first()
                .is_some_and(|expected| actual == expected),
            "in" => self.values.iter().any(|expected| actual == expected),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct TypedOrder {
    pub(super) field: String,
    #[serde(default = "default_ascending")]
    pub(super) direction: String,
}

impl TypedOrder {
    pub(super) fn parse_list(raw: &str, default_order: &[TypedOrder]) -> Result<Vec<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(default_order.to_vec());
        }
        let parsed: JsonValue = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid typed_order_json: {e}")))?;
        Self::parse_json_array(&parsed)
    }

    pub(super) fn parse_json_array(value: &JsonValue) -> Result<Vec<Self>, Status> {
        let JsonValue::Array(items) = value else {
            return Err(Status::invalid_argument("typed order must be an array"));
        };
        items
            .iter()
            .map(|item| {
                let field = json_optional_string_field(item, "field")
                    .or_else(|| json_optional_string_field(item, "field_name"))
                    .ok_or_else(|| Status::invalid_argument("typed order requires field"))?;
                let direction = json_optional_string_field(item, "direction")
                    .unwrap_or_else(|| "asc".to_string())
                    .to_ascii_lowercase();
                if direction != "asc" && direction != "desc" {
                    return Err(Status::invalid_argument(
                        "typed order direction must be asc or desc",
                    ));
                }
                Ok(Self { field, direction })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct IndexPageToken {
    pub(super) version: u8,
    pub(super) token_kind: String,
    pub(super) mesh_id: String,
    pub(super) anvil_storage_tenant_id: String,
    pub(super) authz_realm_id: String,
    pub(super) tenant_id: i64,
    pub(super) bucket_name: String,
    pub(super) index_name: String,
    pub(super) index_generation: u64,
    pub(super) index_definition_version: u64,
    pub(super) index_inputs: Vec<IndexPageTokenInput>,
    pub(super) authz_revision: u64,
    pub(super) caller_principal_hash: String,
    pub(super) query_hash: String,
    pub(super) predicate_hash: String,
    pub(super) order_hash: String,
    pub(super) last_source_identity: String,
    #[serde(default)]
    pub(super) last_sort_values: BTreeMap<String, JsonValue>,
    pub(super) expires_at: String,
    pub(super) signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct IndexPageTokenInput {
    pub(super) index_id: String,
    pub(super) definition_hash: String,
    pub(super) generation: u64,
}

#[derive(Debug, Clone)]
pub(super) struct IndexPageTokenBinding {
    pub(super) token_kind: String,
    pub(super) mesh_id: String,
    pub(super) anvil_storage_tenant_id: String,
    pub(super) authz_realm_id: String,
    pub(super) tenant_id: i64,
    pub(super) bucket_name: String,
    pub(super) index_name: String,
    pub(super) index_generation: u64,
    pub(super) index_definition_version: u64,
    pub(super) index_inputs: Vec<IndexPageTokenInput>,
    pub(super) authz_revision: u64,
    pub(super) caller_principal_hash: String,
    pub(super) query_hash: String,
    pub(super) predicate_hash: String,
    pub(super) order_hash: String,
}

impl IndexPageToken {
    pub(super) fn for_cursor(
        binding: &IndexPageTokenBinding,
        last_source_identity: String,
        last_sort_values: BTreeMap<String, JsonValue>,
    ) -> Self {
        Self {
            version: INDEX_PAGE_TOKEN_VERSION,
            token_kind: binding.token_kind.clone(),
            mesh_id: binding.mesh_id.clone(),
            anvil_storage_tenant_id: binding.anvil_storage_tenant_id.clone(),
            authz_realm_id: binding.authz_realm_id.clone(),
            tenant_id: binding.tenant_id,
            bucket_name: binding.bucket_name.clone(),
            index_name: binding.index_name.clone(),
            index_generation: binding.index_generation,
            index_definition_version: binding.index_definition_version,
            index_inputs: binding.index_inputs.clone(),
            authz_revision: binding.authz_revision,
            caller_principal_hash: binding.caller_principal_hash.clone(),
            query_hash: binding.query_hash.clone(),
            predicate_hash: binding.predicate_hash.clone(),
            order_hash: binding.order_hash.clone(),
            last_source_identity,
            last_sort_values,
            expires_at: (chrono::Utc::now()
                + chrono::Duration::seconds(INDEX_PAGE_TOKEN_TTL_SECONDS))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            signature: String::new(),
        }
    }

    pub(super) fn decode(raw: &str, signing_key: &[u8]) -> Result<Option<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        let token: Self = serde_json::from_slice(&bytes)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        if token.version != INDEX_PAGE_TOKEN_VERSION {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        let expected = token.sign(signing_key)?;
        if !constant_time_eq::constant_time_eq(token.signature.as_bytes(), expected.as_bytes()) {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        Ok(Some(token))
    }

    pub(super) fn encode(mut self, signing_key: &[u8]) -> Result<String, Status> {
        self.signature = self.sign(signing_key)?;
        let bytes = serde_json::to_vec(&self)
            .map_err(|e| Status::internal(format!("Serialize page token: {e}")))?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    pub(super) fn validate(&self, binding: &IndexPageTokenBinding) -> Result<(), Status> {
        let expires_at = chrono::DateTime::parse_from_rfc3339(&self.expires_at)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?
            .with_timezone(&chrono::Utc);
        if expires_at <= chrono::Utc::now() {
            return Err(Status::invalid_argument("PageTokenExpired"));
        }
        if self.token_kind != binding.token_kind
            || self.mesh_id != binding.mesh_id
            || self.anvil_storage_tenant_id != binding.anvil_storage_tenant_id
            || self.authz_realm_id != binding.authz_realm_id
            || self.tenant_id != binding.tenant_id
            || self.bucket_name != binding.bucket_name
            || self.index_name != binding.index_name
            || self.index_generation != binding.index_generation
            || self.index_definition_version != binding.index_definition_version
            || self.index_inputs != binding.index_inputs
            || self.authz_revision != binding.authz_revision
            || self.caller_principal_hash != binding.caller_principal_hash
            || self.query_hash != binding.query_hash
            || self.predicate_hash != binding.predicate_hash
            || self.order_hash != binding.order_hash
        {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        Ok(())
    }

    pub(super) fn sign(&self, signing_key: &[u8]) -> Result<String, Status> {
        let mut mac = HmacSha256::new_from_slice(signing_key)
            .map_err(|_| Status::internal("Invalid index page token signing key"))?;
        mac.update(INDEX_PAGE_TOKEN_DOMAIN);
        mac.update(&[self.version]);
        update_mac_part(&mut mac, self.token_kind.as_bytes());
        update_mac_part(&mut mac, self.mesh_id.as_bytes());
        update_mac_part(&mut mac, self.anvil_storage_tenant_id.as_bytes());
        update_mac_part(&mut mac, self.authz_realm_id.as_bytes());
        mac.update(&self.tenant_id.to_le_bytes());
        update_mac_part(&mut mac, self.bucket_name.as_bytes());
        update_mac_part(&mut mac, self.index_name.as_bytes());
        mac.update(&self.index_generation.to_le_bytes());
        mac.update(&self.index_definition_version.to_le_bytes());
        let index_inputs = serde_json::to_vec(&self.index_inputs)
            .map_err(|_| Status::internal("Failed to encode index page token index inputs"))?;
        update_mac_part(&mut mac, &index_inputs);
        mac.update(&self.authz_revision.to_le_bytes());
        update_mac_part(&mut mac, self.caller_principal_hash.as_bytes());
        update_mac_part(&mut mac, self.query_hash.as_bytes());
        update_mac_part(&mut mac, self.predicate_hash.as_bytes());
        update_mac_part(&mut mac, self.order_hash.as_bytes());
        update_mac_part(&mut mac, self.last_source_identity.as_bytes());
        let sort_values = serde_json::to_vec(&self.last_sort_values)
            .map_err(|_| Status::internal("Failed to encode index page token sort values"))?;
        update_mac_part(&mut mac, &sort_values);
        update_mac_part(&mut mac, self.expires_at.as_bytes());
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }
}

impl IndexPageTokenBinding {
    pub(super) fn single_index(
        config: &Config,
        claims: &auth::Claims,
        token_kind: &str,
        bucket_name: &str,
        index_name: &str,
        index_generation: u64,
        index_definition_version: u64,
        authz_revision: u64,
        predicate_hash: String,
        order_hash: String,
    ) -> Self {
        let index_inputs = vec![IndexPageTokenInput {
            index_id: index_name.to_string(),
            definition_hash: stable_string_hash(&format!(
                "{index_name}:{index_definition_version}"
            )),
            generation: index_generation,
        }];
        Self::with_index_inputs(
            config,
            claims,
            token_kind,
            bucket_name,
            index_name,
            index_generation,
            index_definition_version,
            index_inputs,
            authz_revision,
            predicate_hash,
            order_hash,
        )
    }

    pub(super) fn with_index_inputs(
        config: &Config,
        claims: &auth::Claims,
        token_kind: &str,
        bucket_name: &str,
        index_name: &str,
        index_generation: u64,
        index_definition_version: u64,
        index_inputs: Vec<IndexPageTokenInput>,
        authz_revision: u64,
        predicate_hash: String,
        order_hash: String,
    ) -> Self {
        let anvil_storage_tenant_id = claims.tenant_id.to_string();
        let authz_realm_id = DEFAULT_AUTHZ_REALM_ID.to_string();
        let caller_principal_hash = stable_string_hash(&claims.sub);
        let query_hash = stable_string_hash(
            &serde_json::json!({
                "token_kind": token_kind,
                "mesh_id": config.mesh_id,
                "anvil_storage_tenant_id": anvil_storage_tenant_id,
                "authz_realm_id": authz_realm_id,
                "tenant_id": claims.tenant_id,
                "bucket_name": bucket_name,
                "index_name": index_name,
                "index_generation": index_generation,
                "index_definition_version": index_definition_version,
                "index_inputs": index_inputs,
                "authz_revision": authz_revision,
                "caller_principal_hash": caller_principal_hash,
                "predicate_hash": predicate_hash,
                "order_hash": order_hash,
            })
            .to_string(),
        );
        Self {
            token_kind: token_kind.to_string(),
            mesh_id: config.mesh_id.clone(),
            anvil_storage_tenant_id,
            authz_realm_id,
            tenant_id: claims.tenant_id,
            bucket_name: bucket_name.to_string(),
            index_name: index_name.to_string(),
            index_generation,
            index_definition_version,
            index_inputs,
            authz_revision,
            caller_principal_hash,
            query_hash,
            predicate_hash,
            order_hash,
        }
    }
}

pub(super) fn update_mac_part(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_le_bytes());
    mac.update(value);
}

pub(super) fn compare_typed_rows(
    left: &TypedIndexRow,
    right: &TypedIndexRow,
    order: &[TypedOrder],
) -> std::cmp::Ordering {
    for term in order {
        let ordering = compare_json_values(
            left.values.get(&term.field).unwrap_or(&JsonValue::Null),
            right.values.get(&term.field).unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.source_identity.cmp(&right.source_identity)
}

pub(super) fn compare_typed_row_to_cursor(
    row: &TypedIndexRow,
    cursor_values: &BTreeMap<String, JsonValue>,
    cursor_source_identity: &str,
    order: &[TypedOrder],
) -> std::cmp::Ordering {
    for term in order {
        let ordering = compare_json_values(
            row.values.get(&term.field).unwrap_or(&JsonValue::Null),
            cursor_values.get(&term.field).unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    row.source_identity.as_str().cmp(cursor_source_identity)
}

pub(super) fn compare_json_values(left: &JsonValue, right: &JsonValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(Ordering::Equal),
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Null, _) => Ordering::Less,
        (_, JsonValue::Null) => Ordering::Greater,
        _ => left.to_string().cmp(&right.to_string()),
    }
}

pub(super) fn json_optional_string_field(value: &JsonValue, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}

pub(super) fn default_ascending() -> String {
    "asc".to_string()
}

pub(super) fn validate_typed_extractor(source_kind: &str, extractor: &str) -> Result<(), Status> {
    let pointer_valid = |value: &str| value.starts_with('/');
    match (source_kind, extractor) {
        (_, "created_at") => Ok(()),
        ("object_current" | "object_version", "object_key" | "object_content_type") => Ok(()),
        ("object_current" | "object_version", value) if pointer_valid(value) => Ok(()),
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_body_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        (
            "append_record",
            "append_stream_key" | "append_record_sequence" | "append_content_type",
        ) => Ok(()),
        ("append_record", value) if pointer_valid(value) => Ok(()),
        ("append_record", value)
            if value
                .strip_prefix("append_payload_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("append_record", value)
            if value
                .strip_prefix("append_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        _ => Err(Status::invalid_argument(
            "Invalid typed_json field extractor",
        )),
    }
}

pub(super) fn stable_json_hash(raw: &str) -> String {
    let canonical = if raw.trim().is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_str(raw).unwrap_or(JsonValue::String(raw.to_string()))
    };
    blake3::hash(canonical.to_string().as_bytes())
        .to_hex()
        .to_string()
}

pub(super) fn stable_string_hash(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex().to_string()
}

pub(super) fn metadata_backed_predicate_hash(index_kind: &str, req: &QueryIndexRequest) -> String {
    let shape = serde_json::json!({
        "index_kind": index_kind,
        "path_prefix": req.path_prefix,
        "metadata_filters_hash": stable_json_hash(&req.metadata_filters_json),
    });
    blake3::hash(shape.to_string().as_bytes())
        .to_hex()
        .to_string()
}

pub(super) fn score_based_predicate_hash(index_kind: &str, req: &QueryIndexRequest) -> String {
    let query_vector_bits = req
        .query_vector
        .iter()
        .map(|value| value.to_bits())
        .collect::<Vec<_>>();
    let shape = serde_json::json!({
        "index_kind": index_kind,
        "query_text": req.query_text,
        "query_vector_bits": query_vector_bits,
        "phrase": req.phrase,
        "path_prefix": req.path_prefix,
        "metadata_filters_hash": stable_json_hash(&req.metadata_filters_json),
        "typed_predicates_hash": stable_json_hash(&req.typed_predicates_json),
    });
    blake3::hash(shape.to_string().as_bytes())
        .to_hex()
        .to_string()
}

pub(super) fn score_order_hash() -> String {
    stable_string_hash("score:desc,object_version_id:asc")
}

pub(super) fn score_sort_values(
    score: f32,
    object_version_id: &str,
) -> BTreeMap<String, JsonValue> {
    let mut values = BTreeMap::new();
    values.insert(
        "score".to_string(),
        serde_json::Number::from_f64(f64::from(score))
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
    );
    values.insert(
        "object_version_id".to_string(),
        JsonValue::String(object_version_id.to_string()),
    );
    values
}

pub(super) fn score_after_cursor(
    score: f32,
    object_version_id: &str,
    token: &IndexPageToken,
) -> Result<bool, Status> {
    let cursor_score = token
        .last_sort_values
        .get("score")
        .and_then(JsonValue::as_f64)
        .ok_or_else(|| Status::invalid_argument("InvalidPageToken"))? as f32;
    let cursor_object_version_id = token
        .last_sort_values
        .get("object_version_id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| Status::invalid_argument("InvalidPageToken"))?;
    Ok(score
        .partial_cmp(&cursor_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .is_lt()
        || (score == cursor_score && object_version_id > cursor_object_version_id))
}

pub(super) fn compare_score_hits(
    left_score: f32,
    left_object_version_id: &str,
    right_score: f32,
    right_object_version_id: &str,
) -> std::cmp::Ordering {
    right_score
        .partial_cmp(&left_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left_object_version_id.cmp(right_object_version_id))
}

pub(super) fn object_key_after_cursor(
    object_key: &str,
    source_identity: &str,
    cursor_values: &BTreeMap<String, JsonValue>,
    cursor_source_identity: &str,
) -> bool {
    let cursor_key = cursor_values
        .get("object_key")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    object_key
        .cmp(cursor_key)
        .then(source_identity.cmp(cursor_source_identity))
        .is_gt()
}

pub(super) fn parse_metadata_filters(value: &str) -> Result<Vec<MetadataFilter>, Status> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: JsonValue = serde_json::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid metadata_filters_json: {e}")))?;
    if parsed.is_null() {
        return Ok(Vec::new());
    }
    let JsonValue::Object(entries) = parsed else {
        return Err(Status::invalid_argument(
            "metadata_filters_json must be a JSON object",
        ));
    };
    let mut filters = Vec::with_capacity(entries.len());
    for (field, expected) in entries {
        if field.trim().is_empty() {
            return Err(Status::invalid_argument(
                "metadata_filters_json field names must not be empty",
            ));
        }
        filters.push(MetadataFilter { field, expected });
    }
    Ok(filters)
}

pub(super) fn metadata_filter_value<'a>(
    metadata: &'a JsonValue,
    field: &str,
) -> Option<&'a JsonValue> {
    if field.starts_with('/') {
        metadata.pointer(field)
    } else {
        metadata.get(field)
    }
}

#[derive(Debug, Clone)]
pub(super) struct HybridAccum {
    pub(super) object_version_id: [u8; 16],
    pub(super) text_score: f32,
    pub(super) vector_score: f32,
    pub(super) score: f32,
    pub(super) normalized_text_score: f32,
    pub(super) normalized_vector_score: f32,
    pub(super) freshness_score: f32,
    pub(super) document_id: u64,
    pub(super) field_id: u32,
    pub(super) vector_id: u64,
    pub(super) chunk_id: u32,
    pub(super) source_start: u64,
    pub(super) source_len: u32,
}

impl HybridAccum {
    pub(super) fn new(object_version_id: [u8; 16]) -> Self {
        Self {
            object_version_id,
            text_score: 0.0,
            vector_score: 0.0,
            score: 0.0,
            normalized_text_score: 0.0,
            normalized_vector_score: 0.0,
            freshness_score: 0.0,
            document_id: 0,
            field_id: 0,
            vector_id: 0,
            chunk_id: 0,
            source_start: 0,
            source_len: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct HybridCandidate {
    pub(super) item: HybridAccum,
    pub(super) object_ref: QueryObjectRef,
}

pub(super) fn score_hybrid_candidates(
    candidates: &mut [HybridCandidate],
    has_text: bool,
    has_vector: bool,
    text_weight: f32,
    vector_weight: f32,
    freshness_weight: f32,
) {
    let max_text_score = candidates
        .iter()
        .map(|candidate| candidate.item.text_score.max(0.0))
        .fold(0.0_f32, f32::max);
    let max_vector_score = candidates
        .iter()
        .map(|candidate| candidate.item.vector_score.max(0.0))
        .fold(0.0_f32, f32::max);
    let (min_created_at, max_created_at) =
        candidates
            .iter()
            .fold((i64::MAX, i64::MIN), |(min_seen, max_seen), candidate| {
                (
                    min_seen.min(candidate.object_ref.created_at_nanos),
                    max_seen.max(candidate.object_ref.created_at_nanos),
                )
            });
    let created_range = max_created_at.saturating_sub(min_created_at);

    for candidate in candidates {
        candidate.item.normalized_text_score = if has_text && max_text_score > f32::EPSILON {
            candidate.item.text_score.max(0.0) / max_text_score
        } else {
            0.0
        };
        candidate.item.normalized_vector_score = if has_vector && max_vector_score > f32::EPSILON {
            candidate.item.vector_score.max(0.0) / max_vector_score
        } else {
            0.0
        };
        candidate.item.freshness_score = if freshness_weight > 0.0 {
            if created_range <= 0 {
                1.0
            } else {
                candidate
                    .object_ref
                    .created_at_nanos
                    .saturating_sub(min_created_at) as f32
                    / created_range as f32
            }
        } else {
            0.0
        };
        candidate.item.score = candidate.item.normalized_text_score.mul_add(
            text_weight,
            candidate.item.normalized_vector_score * vector_weight,
        ) + candidate.item.freshness_score * freshness_weight;
    }
}
