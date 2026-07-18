use super::*;

pub(super) async fn list_audit_events(
    state: &AppState,
    request: Request<ListAuditEventsRequest>,
) -> Result<Response<AuditEventsResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ViewAuditLog).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let page = req.page.as_ref();
    let limit = page_limit(page);
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
            next_cursor,
            has_more,
        }),
        events: events.into_iter().map(audit_event_to_proto).collect(),
        data_source: "admin_audit_log".to_string(),
    }))
}

pub(super) async fn list_storage_classes(
    state: &AppState,
    request: Request<ListStorageClassesRequest>,
) -> Result<Response<ListStorageClassesResponse>, Status> {
    let _principal = require_admin(&request, state, SystemAdminRelation::ViewSystem).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let catalog = state.core_store.storage_class_catalog();
    let storage_classes = state
        .core_store
        .list_storage_classes()
        .into_iter()
        .filter(|class| req.include_operator_only || class.tenant_selectable)
        .map(|class| storage_class_to_proto(&class, &catalog.default_class_id))
        .collect();
    Ok(Response::new(ListStorageClassesResponse {
        request_id,
        storage_classes,
        default_class_id: catalog.default_class_id.clone(),
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
