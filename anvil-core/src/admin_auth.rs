use crate::auth::Claims;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnvilAdminCapability {
    ManageTenants,
    ManageApps,
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

impl AnvilAdminCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ManageTenants => "manage_tenants",
            Self::ManageApps => "manage_apps",
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

    pub fn scope_action(self) -> String {
        format!("anvil_admin:{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminPrincipal {
    pub principal_id: String,
    pub tenant_id: i64,
    pub authenticated_methods: Vec<String>,
}

impl From<&Claims> for AdminPrincipal {
    fn from(claims: &Claims) -> Self {
        Self {
            principal_id: claims.sub.clone(),
            tenant_id: claims.tenant_id,
            authenticated_methods: vec!["bearer".to_string()],
        }
    }
}

pub fn has_admin_capability(
    claims: &Claims,
    capability: AnvilAdminCapability,
    mesh_id: &str,
) -> bool {
    let action = capability.scope_action();
    let wildcard_action = "anvil_admin:*";
    let resource = format!("anvil_admin:cluster:{mesh_id}");

    claims.scopes.iter().any(|scope| {
        if scope == "*|*" {
            return true;
        }
        let Some((scope_action, scope_resource)) = scope.split_once('|') else {
            return false;
        };
        (scope_action == action || scope_action == wildcard_action)
            && (scope_resource == resource || scope_resource == "*")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(scopes: Vec<&str>) -> Claims {
        Claims {
            sub: "admin-a".to_string(),
            exp: usize::MAX,
            scopes: scopes.into_iter().map(str::to_string).collect(),
            tenant_id: 0,
            jti: None,
        }
    }

    #[test]
    fn admin_capabilities_are_typed_scope_checks() {
        assert!(has_admin_capability(
            &claims(vec!["anvil_admin:manage_nodes|anvil_admin:cluster:mesh-a"]),
            AnvilAdminCapability::ManageNodes,
            "mesh-a",
        ));
        assert!(!has_admin_capability(
            &claims(vec![
                "anvil_admin:manage_regions|anvil_admin:cluster:mesh-a"
            ]),
            AnvilAdminCapability::ManageNodes,
            "mesh-a",
        ));
        assert!(has_admin_capability(
            &claims(vec!["anvil_admin:*|anvil_admin:cluster:mesh-a"]),
            AnvilAdminCapability::ManageNodes,
            "mesh-a",
        ));
    }
}
