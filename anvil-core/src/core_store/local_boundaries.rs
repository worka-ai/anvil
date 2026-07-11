use super::*;

pub(super) fn validate_boundary_schema(
    schema: &CoreBoundarySchema,
    current: Option<&CoreBoundarySchema>,
    expected_generation: Option<u64>,
) -> Result<()> {
    if schema.schema != CORE_BOUNDARY_SCHEMA_SCHEMA {
        bail!("CoreStore boundary schema has invalid schema");
    }
    validate_logical_id(&schema.bucket, "boundary schema bucket")?;
    if schema.dimensions.is_empty() {
        bail!("CoreStore boundary schema must include at least one dimension");
    }
    let mut names = BTreeSet::new();
    for dimension in &schema.dimensions {
        validate_boundary_dimension(dimension)?;
        if !names.insert(&dimension.name) {
            bail!(
                "CoreStore boundary schema dimension {} is duplicated",
                dimension.name
            );
        }
    }

    match current {
        Some(current) => {
            if current.bucket != schema.bucket {
                bail!("CoreStore boundary schema bucket mismatch");
            }
            if expected_generation != Some(current.generation) {
                bail!(
                    "{}: CoreStore boundary schema generation conflict",
                    AnvilErrorCode::BoundarySchemaGenerationConflict.as_str()
                );
            }
            if schema.generation != current.generation.saturating_add(1) {
                bail!(
                    "{}: CoreStore boundary schema generation must increment by one",
                    AnvilErrorCode::BoundarySchemaGenerationConflict.as_str()
                );
            }
            validate_boundary_schema_evolution(current, schema)?;
        }
        None => {
            if expected_generation.is_some() || schema.generation != 1 {
                bail!(
                    "{}: CoreStore boundary schema genesis generation must be 1",
                    AnvilErrorCode::BoundarySchemaGenerationConflict.as_str()
                );
            }
        }
    }
    Ok(())
}

pub(super) fn validate_boundary_dimension(dimension: &CoreBoundaryDimension) -> Result<()> {
    validate_logical_id(&dimension.name, "boundary dimension name")?;
    validate_boundary_value_type(&dimension.value_type)?;
    validate_boundary_source(&dimension.source, &dimension.value_type)?;
    if dimension.categories.is_empty() {
        bail!("CoreStore boundary dimension must include at least one category");
    }
    for category in &dimension.categories {
        validate_boundary_category(category)?;
    }
    validate_boundary_hint(
        &dimension.cardinality,
        &["low", "medium", "high", "extreme"],
        "cardinality",
    )?;
    validate_boundary_hint(
        &dimension.placement_affinity,
        &["none", "prefer_colocate", "prefer_spread"],
        "placement affinity",
    )?;
    validate_boundary_hint(
        &dimension.compaction_scope,
        &["none", "prefer_same_value", "require_same_value"],
        "compaction scope",
    )?;
    if dimension.max_values_per_block == 0 {
        bail!("CoreStore boundary max_values_per_block must be positive");
    }
    if dimension.shared_ranges_allowed && dimension.shared_record_kinds.is_empty() {
        bail!("CoreStore boundary shared ranges must list shared record kinds");
    }
    Ok(())
}

pub(super) fn validate_boundary_schema_evolution(
    current: &CoreBoundarySchema,
    next: &CoreBoundarySchema,
) -> Result<()> {
    let current_dimensions = current
        .dimensions
        .iter()
        .map(|dimension| (dimension.name.as_str(), dimension))
        .collect::<BTreeMap<_, _>>();
    for dimension in &next.dimensions {
        let Some(existing) = current_dimensions.get(dimension.name.as_str()) else {
            if dimension.required {
                bail!(
                    "{}: CoreStore boundary schema cannot add required dimension {}",
                    AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str(),
                    dimension.name
                );
            }
            continue;
        };
        if existing.value_type != dimension.value_type {
            bail!(
                "{}: CoreStore boundary schema cannot change value type for {}",
                AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str(),
                dimension.name
            );
        }
        if has_boundary_category(existing, "security_realm")
            != has_boundary_category(dimension, "security_realm")
        {
            bail!(
                "{}: CoreStore boundary schema cannot change security_realm category for {}",
                AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str(),
                dimension.name
            );
        }
    }
    Ok(())
}

pub(super) fn has_boundary_category(dimension: &CoreBoundaryDimension, category: &str) -> bool {
    dimension
        .categories
        .iter()
        .any(|candidate| candidate == category)
}

pub(super) fn validate_boundary_source(
    source: &CoreBoundarySource,
    value_type: &str,
) -> Result<()> {
    match source {
        CoreBoundarySource::UserMetadataJsonPointer { pointer }
        | CoreBoundarySource::BodyJsonPointer { pointer, .. } => {
            if !pointer.starts_with('/') {
                bail!(
                    "{}: CoreStore boundary JSON pointer must start with /",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                );
            }
        }
        CoreBoundarySource::SystemMetadataField { field } => {
            validate_boundary_system_metadata_field(field)?;
        }
        CoreBoundarySource::PathTemplate { template } => validate_boundary_path_template(template)?,
        CoreBoundarySource::WriterSuppliedBoundary {
            writer_family,
            field,
        } => {
            validate_writer_family(writer_family, "boundary writer supplied family")?;
            validate_logical_id(field, "boundary writer supplied field")?;
        }
    }
    validate_boundary_value_type(value_type)
}

fn validate_boundary_system_metadata_field(field: &str) -> Result<()> {
    validate_boundary_hint(
        field,
        &[
            "tenant_id",
            "bucket_name",
            "object_key",
            "content_type",
            "payload_length",
        ],
        "boundary system metadata field",
    )
}

pub(super) fn validate_boundary_path_template(template: &str) -> Result<()> {
    if !template.starts_with('/') {
        bail!("CoreStore boundary path template must start with /");
    }
    if template.contains("//") || template.contains("..") {
        bail!("CoreStore boundary path template contains an invalid path component");
    }
    Ok(())
}

pub(super) fn validate_boundary_value_type(value_type: &str) -> Result<()> {
    validate_boundary_hint(
        value_type,
        &["string", "uuid", "u64", "i64", "date", "timestamp"],
        "value type",
    )
}

pub(super) fn validate_boundary_category(category: &str) -> Result<()> {
    validate_boundary_hint(
        category,
        &[
            "security_realm",
            "storage_partition",
            "query_prune",
            "placement_affinity",
            "compaction_group",
            "retention_group",
            "observability_group",
        ],
        "category",
    )
}

pub(super) fn validate_boundary_hint(value: &str, allowed: &[&str], label: &str) -> Result<()> {
    if allowed.contains(&value) {
        Ok(())
    } else {
        bail!("CoreStore boundary {label} {value:?} is not supported")
    }
}

pub(super) fn validate_logical_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        bail!("CoreStore {label} must not be empty");
    }
    if value.contains('\0') || value.contains("..") {
        bail!("CoreStore {label} contains an invalid component");
    }
    Ok(())
}
