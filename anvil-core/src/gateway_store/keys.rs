use super::*;

pub(super) struct GatewayRepositoryKey {
    pub(super) tenant_id: i64,
    pub(super) gateway: String,
    pub(super) registry_instance_id: String,
    pub(super) repository: String,
}

impl GatewayRepositoryKey {
    pub(super) fn new(
        tenant_id: i64,
        gateway: &str,
        registry_instance_id: &str,
        repository: &str,
    ) -> Result<Self> {
        validate_tenant(tenant_id)?;
        Ok(Self {
            tenant_id,
            gateway: normalize_gateway_identifier(gateway, "gateway")?,
            registry_instance_id: normalize_gateway_identifier(registry_instance_id, "registry")?,
            repository: normalize_gateway_identifier(repository, "repository")?,
        })
    }

    pub(super) fn ref_name(&self) -> String {
        format!(
            "gateway_repository:tenant:{}:gateway:{}:registry:{}:repository:{}",
            self.tenant_id, self.gateway, self.registry_instance_id, self.repository
        )
    }
}

pub(super) fn gateway_repository_ref_name(record: &GatewayRepositoryRecord) -> Result<String> {
    Ok(GatewayRepositoryKey::new(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.repository,
    )?
    .ref_name())
}

pub(super) fn gateway_blob_ref_name(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    validate_gateway_digest(digest)?;
    Ok(format!(
        "gateway_blob:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:digest:{digest}"
    ))
}

pub(super) fn gateway_tag_ref_name(record: &GatewayTagRecord) -> Result<String> {
    gateway_tag_ref_name_parts(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.repository,
        &record.tag,
    )
}

pub(super) fn gateway_tag_ref_name_parts(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_tag:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:tag:{tag}"
    ))
}
