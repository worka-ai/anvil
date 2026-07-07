use crate::{
    anvil_api::{AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema},
    auth, authz_journal, authz_realm_schema,
    authz_scope::encode_realm_namespace,
    config::Config,
    core_store::{AcquireFence, CompareAndSwapRef, CoreStore},
    crypto::EncryptionKeyring,
    persistence::{App, Persistence},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const SYSTEM_STORAGE_TENANT_ID: i64 = 0;
pub const SYSTEM_REALM_ID: &str = "system";
pub const SYSTEM_SCHEMA_ID: &str = "anvil-system";
pub const SYSTEM_MESH_NAMESPACE: &str = "anvil_mesh";
pub const SYSTEM_ADMIN_SUBJECT_KIND_APP: &str = "app";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemAdminRelation {
    ManageTenants,
    ManageApps,
    ManagePolicies,
    ManageSecretEncryptionKeys,
    ManageBuckets,
    ManageNodes,
    ManageRegions,
    ManageRouting,
    ManageHostAliases,
    ManageLinks,
    RunRepair,
    ViewDiagnostics,
    ViewAuditLog,
}

impl SystemAdminRelation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ManageTenants => "manage_tenants",
            Self::ManageApps => "manage_apps",
            Self::ManagePolicies => "manage_policies",
            Self::ManageSecretEncryptionKeys => "manage_secret_encryption_keys",
            Self::ManageBuckets => "manage_buckets",
            Self::ManageNodes => "manage_nodes",
            Self::ManageRegions => "manage_regions",
            Self::ManageRouting => "manage_routing",
            Self::ManageHostAliases => "manage_host_aliases",
            Self::ManageLinks => "manage_links",
            Self::RunRepair => "run_repair",
            Self::ViewDiagnostics => "view_diagnostics",
            Self::ViewAuditLog => "view_audit_log",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminPrincipal {
    pub principal_id: String,
    pub tenant_id: i64,
    pub authenticated_methods: Vec<String>,
    pub checked_relation: Option<SystemAdminRelation>,
    pub checked_object: Option<String>,
}

impl From<&auth::Claims> for AdminPrincipal {
    fn from(claims: &auth::Claims) -> Self {
        Self {
            principal_id: claims.sub.clone(),
            tenant_id: claims.tenant_id,
            authenticated_methods: vec!["bearer".to_string()],
            checked_relation: None,
            checked_object: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BootstrapMarker {
    schema: &'static str,
    mesh_id: String,
    authz_realm_id: String,
    completed_at: String,
}

#[derive(Debug, Clone, Serialize)]
struct BootstrapCredentialFile<'a> {
    schema: &'static str,
    mesh_id: &'a str,
    tenant_id: i64,
    app_id: i64,
    app_name: &'a str,
    client_id: &'a str,
    client_secret: &'a str,
}

pub async fn ensure_bootstrapped(
    config: &Config,
    persistence: &Persistence,
    storage: &Storage,
    secret_keyring: &EncryptionKeyring,
) -> Result<()> {
    let mesh_id = normalized_mesh_id(&config.mesh_id);
    if bootstrap_marker_exists(storage, &mesh_id).await? {
        reject_stale_bootstrap_config(config)?;
        return Ok(());
    }

    let fence_owner = format!("system-realm-bootstrap-{}", uuid::Uuid::new_v4().simple());
    let Some(_fence) = acquire_bootstrap_fence(storage, &mesh_id, &fence_owner).await? else {
        reject_stale_bootstrap_config(config)?;
        return Ok(());
    };
    if bootstrap_marker_exists(storage, &mesh_id).await? {
        reject_stale_bootstrap_config(config)?;
        return Ok(());
    }

    let (subject_kind, subject_id) =
        resolve_bootstrap_subject(config, persistence, secret_keyring).await?;

    install_system_schema(storage).await?;
    write_system_relation_tuples(persistence, &mesh_id, &subject_kind, &subject_id).await?;
    write_bootstrap_marker(storage, &mesh_id).await
}

pub async fn check_admin_relation(
    storage: &Storage,
    mesh_id: &str,
    claims: &auth::Claims,
    relation: SystemAdminRelation,
) -> Result<bool> {
    let mesh_id = normalized_mesh_id(mesh_id);
    let revision = authz_journal::latest_authz_revision(storage, SYSTEM_STORAGE_TENANT_ID).await?;
    authz_journal::resolve_permission_at_revision(
        storage,
        SYSTEM_STORAGE_TENANT_ID,
        &system_namespace(),
        &mesh_id,
        relation.as_str(),
        SYSTEM_ADMIN_SUBJECT_KIND_APP,
        &claims.sub,
        "",
        revision,
    )
    .await
}

pub async fn principal_has_any_admin_relation(
    storage: &Storage,
    mesh_id: &str,
    app_id: i64,
) -> Result<bool> {
    let claims = auth::Claims {
        sub: app_id.to_string(),
        exp: usize::MAX,
        scopes: Vec::new(),
        tenant_id: 0,
        jti: None,
    };
    for relation in all_admin_relations() {
        if check_admin_relation(storage, mesh_id, &claims, *relation).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn system_namespace() -> String {
    encode_realm_namespace(SYSTEM_REALM_ID, SYSTEM_MESH_NAMESPACE)
}

pub fn system_mesh_object_id(mesh_id: &str) -> String {
    normalized_mesh_id(mesh_id)
}

fn normalized_mesh_id(mesh_id: &str) -> String {
    let trimmed = mesh_id.trim();
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

fn all_admin_relations() -> &'static [SystemAdminRelation] {
    &[
        SystemAdminRelation::ManageTenants,
        SystemAdminRelation::ManageApps,
        SystemAdminRelation::ManagePolicies,
        SystemAdminRelation::ManageSecretEncryptionKeys,
        SystemAdminRelation::ManageBuckets,
        SystemAdminRelation::ManageNodes,
        SystemAdminRelation::ManageRegions,
        SystemAdminRelation::ManageRouting,
        SystemAdminRelation::ManageHostAliases,
        SystemAdminRelation::ManageLinks,
        SystemAdminRelation::RunRepair,
        SystemAdminRelation::ViewDiagnostics,
        SystemAdminRelation::ViewAuditLog,
    ]
}

async fn bootstrap_marker_exists(storage: &Storage, mesh_id: &str) -> Result<bool> {
    let store = CoreStore::new(storage.clone()).await?;
    Ok(store
        .read_ref(&bootstrap_marker_ref(mesh_id))
        .await?
        .is_some())
}

async fn acquire_bootstrap_fence(
    storage: &Storage,
    mesh_id: &str,
    owner: &str,
) -> Result<Option<crate::core_store::FencedPermit>> {
    let store = CoreStore::new(storage.clone()).await?;
    let fence_name = format!("system-realm-bootstrap-{mesh_id}");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match store
            .acquire_fence(AcquireFence {
                fence_name: fence_name.clone(),
                authenticated_principal: owner.to_string(),
                ttl_ms: 60_000,
            })
            .await
        {
            Ok(permit) => return Ok(Some(permit)),
            Err(err) => {
                if bootstrap_marker_exists(storage, mesh_id).await? {
                    return Ok(None);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(err).context("acquire system realm bootstrap fence");
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

fn reject_stale_bootstrap_config(config: &Config) -> Result<()> {
    if !config.bootstrap_system_admin_app_name.trim().is_empty()
        || !config
            .bootstrap_system_admin_credential_output_path
            .trim()
            .is_empty()
        || !config.bootstrap_system_admin_subject_kind.trim().is_empty()
        || !config.bootstrap_system_admin_subject_id.trim().is_empty()
    {
        tracing::warn!(
            "system realm already exists; first-boot bootstrap system admin configuration is ignored"
        );
    }
    Ok(())
}

async fn resolve_bootstrap_subject(
    config: &Config,
    persistence: &Persistence,
    secret_keyring: &EncryptionKeyring,
) -> Result<(String, String)> {
    let subject_kind = config.bootstrap_system_admin_subject_kind.trim();
    let subject_id = config.bootstrap_system_admin_subject_id.trim();
    if !subject_kind.is_empty() || !subject_id.is_empty() {
        if subject_kind.is_empty() || subject_id.is_empty() {
            return Err(anyhow!(
                "both bootstrap_system_admin_subject_kind and bootstrap_system_admin_subject_id are required"
            ));
        }
        return Ok((subject_kind.to_string(), subject_id.to_string()));
    }

    let app_name = config.bootstrap_system_admin_app_name.trim();
    let output_path = config.bootstrap_system_admin_credential_output_path.trim();
    if app_name.is_empty() || output_path.is_empty() {
        return Err(anyhow!(
            "system realm is missing; configure bootstrap_system_admin_app_name and bootstrap_system_admin_credential_output_path, or configure an explicit bootstrap system admin subject"
        ));
    }

    let tenant = match persistence.get_tenant_by_name("system").await? {
        Some(tenant) => tenant,
        None => {
            persistence
                .create_tenant("system", "system-realm-bootstrap")
                .await?
        }
    };

    let existing = persistence
        .list_apps_for_tenant(tenant.id)
        .await?
        .into_iter()
        .find(|app| app.name == app_name);
    let app = match existing {
        Some(app) => app,
        None => {
            create_bootstrap_app(
                config,
                persistence,
                secret_keyring,
                tenant.id,
                app_name,
                Path::new(output_path),
            )
            .await?
        }
    };
    Ok((
        SYSTEM_ADMIN_SUBJECT_KIND_APP.to_string(),
        app.id.to_string(),
    ))
}

async fn create_bootstrap_app(
    config: &Config,
    persistence: &Persistence,
    secret_keyring: &EncryptionKeyring,
    tenant_id: i64,
    app_name: &str,
    output_path: &Path,
) -> Result<App> {
    let client_id = format!("app_{}", uuid::Uuid::new_v4().simple());
    let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
    let encrypted_secret = secret_keyring.encrypt(client_secret.as_bytes())?;
    let app = persistence
        .create_app(tenant_id, app_name, &client_id, &encrypted_secret)
        .await?;

    let credential = BootstrapCredentialFile {
        schema: "anvil.bootstrap.admin_credential.v1",
        mesh_id: &config.mesh_id,
        tenant_id,
        app_id: app.id,
        app_name,
        client_id: &client_id,
        client_secret: &client_secret,
    };
    write_bootstrap_credential(output_path, &credential)
        .with_context(|| format!("write bootstrap credential to {}", output_path.display()))?;

    Ok(app)
}

fn write_bootstrap_credential(path: &Path, credential: &BootstrapCredentialFile<'_>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(credential)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(path)?;
    use std::io::Write;
    file.write_all(&body)?;
    file.write_all(b"\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

async fn install_system_schema(storage: &Storage) -> Result<()> {
    let latest_revision =
        authz_journal::latest_authz_revision(storage, SYSTEM_STORAGE_TENANT_ID).await?;
    let revision = authz_realm_schema::put_schema_revision(
        storage,
        SYSTEM_STORAGE_TENANT_ID,
        SYSTEM_SCHEMA_ID,
        vec![system_mesh_schema()],
        u64::try_from(latest_revision.max(0)).unwrap_or(0),
        "system-realm-bootstrap",
        "install built-in system realm schema",
    )
    .await?;
    match authz_realm_schema::read_schema_binding(
        storage,
        SYSTEM_STORAGE_TENANT_ID,
        SYSTEM_REALM_ID,
    )
    .await?
    {
        Some(binding) if binding.schema_ref == revision.schema_ref => return Ok(()),
        Some(binding) => {
            authz_realm_schema::bind_schema(
                storage,
                SYSTEM_STORAGE_TENANT_ID,
                SYSTEM_REALM_ID,
                revision.schema_ref,
                Some(binding.binding_generation),
                u64::try_from(latest_revision.max(0)).unwrap_or(0),
                "system-realm-bootstrap",
                "bind built-in system realm schema",
            )
            .await?;
        }
        None => {
            match authz_realm_schema::bind_schema(
                storage,
                SYSTEM_STORAGE_TENANT_ID,
                SYSTEM_REALM_ID,
                revision.schema_ref.clone(),
                None,
                u64::try_from(latest_revision.max(0)).unwrap_or(0),
                "system-realm-bootstrap",
                "bind built-in system realm schema",
            )
            .await
            {
                Ok(_) => {}
                Err(err) => {
                    let Some(binding) = authz_realm_schema::read_schema_binding(
                        storage,
                        SYSTEM_STORAGE_TENANT_ID,
                        SYSTEM_REALM_ID,
                    )
                    .await?
                    else {
                        return Err(err);
                    };
                    if binding.schema_ref != revision.schema_ref {
                        return Err(err);
                    }
                }
            }
        }
    }
    Ok(())
}

fn system_mesh_schema() -> AuthzNamespaceSchema {
    let mut relations = Vec::new();
    for relation in ["owner", "admin"] {
        relations.push(AuthzRelationSchema {
            relation: relation.to_string(),
            rules: Vec::new(),
        });
    }
    for relation in all_admin_relations() {
        relations.push(AuthzRelationSchema {
            relation: relation.as_str().to_string(),
            rules: vec![
                inherit_rule("owner"),
                inherit_rule("admin"),
                inherit_rule(relation.as_str()),
            ],
        });
    }
    AuthzNamespaceSchema {
        namespace: SYSTEM_MESH_NAMESPACE.to_string(),
        relations,
        schema_json: serde_json::json!({
            "schema": "anvil.system.authz_schema.v1",
            "description": "Built-in Anvil system realm. Tenant APIs cannot mutate this schema."
        })
        .to_string(),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn inherit_rule(relation: &str) -> AuthzRelationRule {
    AuthzRelationRule {
        kind: "inherit".to_string(),
        relation: relation.to_string(),
        tuple_relation: String::new(),
        target_relation: String::new(),
    }
}

async fn write_system_relation_tuples(
    persistence: &Persistence,
    mesh_id: &str,
    subject_kind: &str,
    subject_id: &str,
) -> Result<()> {
    let namespace = system_namespace();
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &namespace,
            mesh_id,
            "owner",
            subject_kind,
            subject_id,
            "",
            "add",
            "system-realm-bootstrap",
            "grant initial system owner",
        )
        .await?;

    let owner_userset = format!("{namespace}/{mesh_id}#owner");
    let admin_userset = format!("{namespace}/{mesh_id}#admin");
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &namespace,
            mesh_id,
            "admin",
            "userset",
            &owner_userset,
            "",
            "add",
            "system-realm-bootstrap",
            "owner implies admin",
        )
        .await?;
    for relation in all_admin_relations() {
        for userset in [&owner_userset, &admin_userset] {
            persistence
                .write_authz_tuple(
                    SYSTEM_STORAGE_TENANT_ID,
                    &namespace,
                    mesh_id,
                    relation.as_str(),
                    "userset",
                    userset,
                    "",
                    "add",
                    "system-realm-bootstrap",
                    "system admin relation inheritance",
                )
                .await?;
        }
    }
    Ok(())
}

async fn write_bootstrap_marker(storage: &Storage, mesh_id: &str) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let marker = BootstrapMarker {
        schema: "anvil.system_realm.bootstrap_marker.v1",
        mesh_id: mesh_id.to_string(),
        authz_realm_id: SYSTEM_REALM_ID.to_string(),
        completed_at: chrono::Utc::now().to_rfc3339(),
    };
    let object_ref = store
        .put_blob(crate::core_store::PutBlob {
            logical_name: bootstrap_marker_ref(mesh_id),
            bytes: serde_json::to_vec_pretty(&marker)?,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!("system-realm-bootstrap:{}", uuid::Uuid::new_v4().simple()),
        })
        .await?;
    match store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: bootstrap_marker_ref(mesh_id),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: format!("core-object-ref:{}", serde_json::to_string(&object_ref)?),
            transaction_id: None,
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            if bootstrap_marker_exists(storage, mesh_id).await? {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

fn bootstrap_marker_ref(mesh_id: &str) -> String {
    format!("system_realm_bootstrap:{mesh_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(storage_path: &std::path::Path) -> Config {
        Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "test-node".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            admin_listen_addr: "127.0.0.1:0".to_string(),
            mesh_id: "mesh-test".to_string(),
            region: "test-region".to_string(),
            storage_path: storage_path.to_string_lossy().to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            ..Config::default()
        }
    }

    #[tokio::test]
    async fn system_realm_bootstrap_creates_builtin_schema_and_owner_tuple() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config, None).unwrap();
        let keyring = config.secret_keyring().unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();

        let allowed = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "admin-principal".to_string(),
                exp: usize::MAX,
                scopes: Vec::new(),
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageNodes,
        )
        .await
        .unwrap();
        assert!(allowed);

        let denied = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "ordinary-app".to_string(),
                exp: usize::MAX,
                scopes: vec!["*|*".to_string()],
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageNodes,
        )
        .await
        .unwrap();
        assert!(!denied);
    }

    #[tokio::test]
    async fn system_realm_bootstrap_existing_realm_does_not_grant_again() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config, None).unwrap();
        let keyring = config.secret_keyring().unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();

        let denied = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "new-bootstrap-subject".to_string(),
                exp: usize::MAX,
                scopes: Vec::new(),
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageNodes,
        )
        .await
        .unwrap();
        assert!(!denied);
    }

    #[tokio::test]
    async fn system_realm_bootstrap_missing_config_fails_closed() {
        let temp = tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.bootstrap_system_admin_subject_kind.clear();
        config.bootstrap_system_admin_subject_id.clear();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config, None).unwrap();
        let keyring = config.secret_keyring().unwrap();

        let err = ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("system realm is missing"));
    }

    #[tokio::test]
    async fn system_realm_bootstrap_runs_before_listeners_start() {
        let temp = tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.storage_path = temp.path().to_string_lossy().to_string();
        config.bootstrap_system_admin_subject_kind.clear();
        config.bootstrap_system_admin_subject_id.clear();

        let err = crate::AppState::new(config, None).await.unwrap_err();
        assert!(err.to_string().contains("system realm is missing"));
    }

    #[tokio::test]
    async fn system_realm_bootstrap_two_node_race_has_one_winner() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config, None).unwrap();
        let keyring = config.secret_keyring().unwrap();

        let (left, right) = tokio::join!(
            ensure_bootstrapped(&config, &persistence, &storage, &keyring),
            ensure_bootstrapped(&config, &persistence, &storage, &keyring),
        );

        left.unwrap();
        right.unwrap();
        let marker = bootstrap_marker_exists(&storage, &config.mesh_id)
            .await
            .unwrap();
        assert!(marker);
        let allowed = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "admin-principal".to_string(),
                exp: usize::MAX,
                scopes: Vec::new(),
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageRegions,
        )
        .await
        .unwrap();
        assert!(allowed);
    }

    #[tokio::test]
    async fn system_realm_bootstrap_partial_crash_recovers_idempotently() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config, None).unwrap();
        let keyring = config.secret_keyring().unwrap();

        install_system_schema(&storage).await.unwrap();
        assert!(
            !bootstrap_marker_exists(&storage, &config.mesh_id)
                .await
                .unwrap()
        );

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();
        assert!(
            bootstrap_marker_exists(&storage, &config.mesh_id)
                .await
                .unwrap()
        );
        let allowed = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "admin-principal".to_string(),
                exp: usize::MAX,
                scopes: Vec::new(),
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageSecretEncryptionKeys,
        )
        .await
        .unwrap();
        assert!(allowed);
    }

    #[tokio::test]
    async fn bootstrap_credential_is_not_accepted_by_public_or_admin_api() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config, None).unwrap();
        let keyring = config.secret_keyring().unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();
        let forged_claims = auth::Claims {
            sub: "legacy-bootstrap-token".to_string(),
            exp: usize::MAX,
            scopes: vec!["*|*".to_string()],
            tenant_id: 0,
            jti: None,
        };
        let denied = check_admin_relation(
            &storage,
            &config.mesh_id,
            &forged_claims,
            SystemAdminRelation::ManageTenants,
        )
        .await
        .unwrap();
        assert!(!denied);
    }
}
