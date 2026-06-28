use crate::{auth, authz_journal, permissions::AnvilAction, storage::Storage};
use anyhow::Result;

pub const APP_SUBJECT_KIND: &str = "app";

#[allow(clippy::too_many_arguments)]
pub async fn scope_or_relationship_allows(
    storage: &Storage,
    claims: &auth::Claims,
    scope_action: AnvilAction,
    scope_resource: &str,
    namespace: &str,
    object_id: &str,
    relation: &str,
    authz_revision: Option<i64>,
) -> Result<bool> {
    if auth::is_authorized(scope_action, scope_resource, &claims.scopes) {
        return Ok(true);
    }
    relationship_allows(
        storage,
        claims,
        namespace,
        object_id,
        relation,
        authz_revision,
    )
    .await
}

pub async fn relationship_allows(
    storage: &Storage,
    claims: &auth::Claims,
    namespace: &str,
    object_id: &str,
    relation: &str,
    authz_revision: Option<i64>,
) -> Result<bool> {
    let revision = match authz_revision {
        Some(revision) => revision,
        None => authz_journal::latest_authz_revision(storage, claims.tenant_id).await?,
    };
    authz_journal::resolve_permission_at_revision(
        storage,
        claims.tenant_id,
        namespace,
        object_id,
        relation,
        APP_SUBJECT_KIND,
        &claims.sub,
        "",
        revision,
    )
    .await
}
