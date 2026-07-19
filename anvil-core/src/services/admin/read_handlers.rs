use super::*;

pub(super) async fn list_diagnostics(
    state: &AppState,
    request: Request<ListDiagnosticsRequest>,
) -> Result<Response<DiagnosticsResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ViewDiagnostics).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let page = req.page.as_ref();
    let limit = page_limit(page)?;
    let source = req.source.trim();

    if !req.severity.trim().is_empty() {
        validate_diagnostic_severity(&req.severity)?;
    }

    let mut diagnostics = Vec::new();

    if source.is_empty() || source == "index" || source == "index_diagnostic_journal" {
        if req.tenant_id.trim().is_empty() || req.bucket_name.trim().is_empty() {
            if source == "index" || source == "index_diagnostic_journal" {
                return Err(Status::invalid_argument(
                    "tenant_id and bucket_name are required for index diagnostics",
                ));
            }
        } else {
            let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
            let bucket = state
                .persistence
                .get_bucket_by_name(tenant_id, &req.bucket_name)
                .await
                .map_err(|err| Status::internal(err.to_string()))?
                .ok_or_else(|| Status::not_found("Bucket not found"))?;
            diagnostics.extend(
                state
                    .persistence
                    .list_index_diagnostics(
                        tenant_id,
                        bucket.id,
                        &req.index_name,
                        &req.severity,
                        0,
                        i32::MAX,
                    )
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?
                    .into_iter()
                    .map(index_diagnostic_to_admin_record)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }

    if source.is_empty() || source == "mesh" || source == "mesh_lifecycle" {
        diagnostics.extend(mesh_lifecycle_diagnostics(state).await?);
    }

    if source.is_empty() || source == "mesh" || source == "mesh_routing_projection" {
        diagnostics.extend(mesh_routing_projection_diagnostics(state).await?);
    }

    if !req.severity.trim().is_empty() {
        diagnostics.retain(|diagnostic| diagnostic.severity == req.severity);
    }
    diagnostics.sort_by(|left, right| diagnostic_position(left).cmp(&diagnostic_position(right)));

    let positions = diagnostics
        .iter()
        .map(|diagnostic| (diagnostic_position(diagnostic), diagnostic.cursor))
        .collect::<Vec<_>>();
    let revision = admin_cursor::collection_revision(
        positions
            .iter()
            .map(|(position, cursor)| (position.as_str(), *cursor)),
    );
    let filters = [
        ("source", source),
        ("tenant_id", req.tenant_id.trim()),
        ("bucket_name", req.bucket_name.trim()),
        ("index_name", req.index_name.trim()),
        ("severity", req.severity.trim()),
    ];
    let binding = AdminCursorBinding {
        scope: "admin.list_diagnostics.v1",
        filters: &filters,
        principal: &principal,
        limit,
        revision: &revision,
        sort: "source.cursor.id.asc",
    };
    let cursor =
        admin_cursor::decode_page_cursor(page, &binding, state.config.jwt_secret.as_bytes())?;
    let mut diagnostics = diagnostics
        .into_iter()
        .filter(|diagnostic| {
            cursor
                .as_deref()
                .is_none_or(|cursor| diagnostic_position(diagnostic).as_str() > cursor)
        })
        .take(limit + 1)
        .collect::<Vec<_>>();
    let has_more = diagnostics.len() > limit;
    if has_more {
        diagnostics.truncate(limit);
    }
    let next_cursor = if has_more {
        diagnostics.last().map_or(Ok(String::new()), |diagnostic| {
            admin_cursor::encode_next_cursor(
                &diagnostic_position(diagnostic),
                &binding,
                state.config.jwt_secret.as_bytes(),
            )
        })?
    } else {
        String::new()
    };

    Ok(Response::new(DiagnosticsResponse {
        request_id,
        page: Some(PageResponse {
            next_page_token: next_cursor,
        }),
        diagnostics,
        data_source: if source.is_empty() {
            "combined".to_string()
        } else if source == "index" {
            "index_diagnostic_journal".to_string()
        } else if source == "mesh" {
            "mesh".to_string()
        } else {
            source.to_string()
        },
    }))
}

pub(super) async fn list_audit_events(
    state: &AppState,
    request: Request<ListAuditEventsRequest>,
) -> Result<Response<AuditEventsResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ViewAuditLog).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let page = req.page.as_ref();
    let limit = page_limit(page)?;
    let mut events = admin_audit::list_audit_events(
        &state.storage,
        AuditEventFilter {
            principal_id: none_if_empty(&req.principal_id),
            resource_id: none_if_empty(&req.resource_id),
            action: none_if_empty(&req.action),
        },
    )
    .await
    .map_err(|err| Status::internal(err.to_string()))?;
    let revision = admin_cursor::collection_revision(
        events
            .iter()
            .map(|event| {
                (
                    audit_cursor_position(event),
                    admin_audit::audit_event_revision_generation(event),
                )
            })
            .collect::<Vec<_>>()
            .iter()
            .map(|(position, generation)| (position.as_str(), *generation)),
    );
    let filters = [
        ("principal_id", req.principal_id.as_str()),
        ("resource_id", req.resource_id.as_str()),
        ("action", req.action.as_str()),
    ];
    let binding = AdminCursorBinding {
        scope: "admin.list_audit_events.v1",
        filters: &filters,
        principal: &principal,
        limit,
        revision: &revision,
        sort: "created_at.audit_event_id.asc",
    };
    let cursor =
        admin_cursor::decode_page_cursor(page, &binding, state.config.jwt_secret.as_bytes())?;
    events.retain(|event| {
        cursor
            .as_deref()
            .is_none_or(|cursor| audit_cursor_position(event).as_str() > cursor)
    });
    events.truncate(limit + 1);
    let has_more = events.len() > limit;
    if has_more {
        events.truncate(limit);
    }
    let next_cursor = if has_more {
        events.last().map_or(Ok(String::new()), |event| {
            admin_cursor::encode_next_cursor(
                &audit_cursor_position(event),
                &binding,
                state.config.jwt_secret.as_bytes(),
            )
        })?
    } else {
        String::new()
    };

    Ok(Response::new(AuditEventsResponse {
        request_id,
        page: Some(PageResponse {
            next_page_token: next_cursor,
        }),
        events: events.into_iter().map(audit_event_to_proto).collect(),
        data_source: "admin_audit_log".to_string(),
    }))
}

pub(super) async fn list_storage_classes(
    state: &AppState,
    request: Request<ListStorageClassesRequest>,
) -> Result<Response<ListStorageClassesResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ViewSystem).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let catalog = state.core_store.storage_class_catalog();
    let storage_classes = state
        .core_store
        .list_storage_classes()
        .into_iter()
        .filter(|class| req.include_operator_only || class.tenant_selectable)
        .map(|class| storage_class_to_proto(&class, &catalog.default_class_id))
        .collect::<Vec<_>>();
    let include_operator_only = req.include_operator_only.to_string();
    let filters = [("include_operator_only", include_operator_only.as_str())];
    let principal_scope = format!(
        "admin:{}/tenant:{}",
        principal.principal_id, principal.tenant_id
    );
    let (storage_classes, page) = crate::services::collection_cursor::paginate(
        storage_classes,
        req.page.as_ref(),
        "anvil.AdminService/ListStorageClasses",
        &filters,
        &principal_scope,
        "class_id.asc",
        state.config.jwt_secret.as_bytes(),
        |class| class.class_id.as_str(),
        |class| {
            crate::services::collection_cursor::content_generation(&[
                class.description.as_bytes(),
                class.metadata_profile_id.as_bytes(),
                class.byte_profile_id.as_bytes(),
            ])
        },
    )?;
    Ok(Response::new(ListStorageClassesResponse {
        request_id,
        storage_classes,
        default_class_id: catalog.default_class_id.clone(),
        page: Some(page),
    }))
}

pub(super) async fn get_storage_class(
    state: &AppState,
    request: Request<GetStorageClassRequest>,
) -> Result<Response<StorageClassResponse>, Status> {
    let _principal = require_admin(&request, state, SystemAdminRelation::ViewSystem).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let class = state
        .core_store
        .get_storage_class(&req.class_id)
        .map_err(|err| Status::not_found(err.to_string()))?;
    Ok(Response::new(StorageClassResponse {
        request_id,
        storage_class: Some(storage_class_to_proto(
            &class,
            &state.core_store.storage_class_catalog().default_class_id,
        )),
    }))
}
