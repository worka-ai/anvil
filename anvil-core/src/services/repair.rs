use crate::anvil_api::repair_service_server::RepairService;
use crate::anvil_api::*;
use crate::{
    AppState, auth, authz_repair, directory_repair, index_repair,
    permissions::AnvilAction,
    personaldb_repair,
    repair_finding::{RepairFinding, RepairSubjectRef},
};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl RepairService for AppState {
    async fn repair_index(
        &self,
        request: Request<RepairIndexRequest>,
    ) -> Result<Response<RepairIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_component(&req.bucket_name, "bucket_name")?;
        validate_component(&req.index_name, "index_name")?;

        let resource = format!("{}/{}", req.bucket_name, req.index_name);
        if !auth::is_authorized(AnvilAction::RepairRun, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let report = self
            .persistence
            .repair_index_from_base_journal(
                claims.tenant_id,
                &req.bucket_name,
                &req.index_name,
                req.rebuild,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let (source_cursor_low, source_cursor_high) = split_u128(report.source_cursor);
        Ok(Response::new(RepairIndexResponse {
            status: index_repair::status_name(&report.status).to_string(),
            bucket_name: report.bucket_name,
            index_name: report.index_name,
            index_storage_id: report.index_storage_id,
            source_cursor_low,
            source_cursor_high,
            reason: index_repair::status_reason(&report.status),
            finding: report.finding.as_ref().map(repair_finding_record),
            build: report.build.as_ref().map(|build| {
                let (source_cursor_low, source_cursor_high) = split_u128(build.source_cursor);
                IndexBuildRecord {
                    index_kind: build.index_kind.clone(),
                    generation: build.generation,
                    item_count: build.item_count as u64,
                    source_cursor_low,
                    source_cursor_high,
                    segment_hashes: build.segment_hashes.clone(),
                }
            }),
        }))
    }

    async fn repair_directory_index(
        &self,
        request: Request<RepairDirectoryIndexRequest>,
    ) -> Result<Response<RepairDirectoryIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_component(&req.bucket_name, "bucket_name")?;

        if !auth::is_authorized(AnvilAction::RepairRun, &req.bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let report = self
            .persistence
            .repair_directory_index(claims.tenant_id, &req.bucket_name, req.rebuild)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let (source_cursor_low, source_cursor_high) = split_u128(report.source_cursor);
        Ok(Response::new(RepairDirectoryIndexResponse {
            status: directory_repair::status_name(&report.status).to_string(),
            bucket_name: report.bucket_name,
            source_cursor_low,
            source_cursor_high,
            expected_entry_count: report.expected.entry_count as u64,
            actual_entry_count: report
                .actual
                .as_ref()
                .map(|snapshot| snapshot.entry_count as u64)
                .unwrap_or_default(),
            expected_snapshot_hash: report.expected.snapshot_hash,
            actual_snapshot_hash: report
                .actual
                .as_ref()
                .map(|snapshot| snapshot.snapshot_hash.clone())
                .unwrap_or_default(),
            reason: directory_repair::status_reason(&report.status),
            finding: report.finding.as_ref().map(repair_finding_record),
            rebuilt_manifest_hash: report
                .rebuilt
                .as_ref()
                .map(|rebuilt| rebuilt.manifest_hash.clone())
                .unwrap_or_default(),
        }))
    }

    async fn list_repair_findings(
        &self,
        request: Request<ListRepairFindingsRequest>,
    ) -> Result<Response<ListRepairFindingsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_component(&req.scope_kind, "scope_kind")?;
        validate_component(&req.scope_id, "scope_id")?;

        if !auth::is_authorized(AnvilAction::RepairRead, &req.scope_id, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let findings = self
            .persistence
            .list_repair_findings(&req.scope_kind, &req.scope_id, req.limit as usize)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .iter()
            .map(repair_finding_record)
            .collect();
        Ok(Response::new(ListRepairFindingsResponse { findings }))
    }

    async fn repair_authz_derived_index(
        &self,
        request: Request<RepairAuthzDerivedIndexRequest>,
    ) -> Result<Response<RepairAuthzDerivedIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_component(&req.derived_index_id, "derived_index_id")?;

        let resource = format!("tenant-{}/authz/{}", claims.tenant_id, req.derived_index_id);
        if !auth::is_authorized(AnvilAction::RepairRun, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let report = self
            .persistence
            .repair_authz_derived_userset_index(
                claims.tenant_id,
                &req.derived_index_id,
                req.rebuild,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RepairAuthzDerivedIndexResponse {
            status: authz_repair::status_name(&report.status).to_string(),
            derived_index_id: report.derived_index_id,
            processed_revision: report.processed_revision,
            latest_revision: report.latest_revision,
            source_records_hash: report.source_records_hash,
            reason: authz_repair::status_reason(&report.status),
            finding: report.finding.as_ref().map(repair_finding_record),
        }))
    }

    async fn repair_personal_db_log_chain(
        &self,
        request: Request<RepairPersonalDbLogChainRequest>,
    ) -> Result<Response<RepairPersonalDbLogChainResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_component(&req.database_id, "database_id")?;

        let resource = format!("tenant-{}/{}", claims.tenant_id, req.database_id);
        if !auth::is_authorized(AnvilAction::RepairRun, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let report = self
            .persistence
            .repair_personaldb_log_chain(claims.tenant_id, &req.database_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RepairPersonalDbLogChainResponse {
            status: personaldb_repair::status_name(&report.status).to_string(),
            tenant_id: report.tenant_id,
            database_id: report.database_id,
            committed_log_index: report.committed_log_index,
            verified_log_index: report.verified_log_index,
            committed_log_hash: report.committed_log_hash,
            reason: personaldb_repair::status_reason(&report.status),
            finding: report.finding.as_ref().map(repair_finding_record),
        }))
    }
}

fn repair_finding_record(finding: &RepairFinding) -> RepairFindingRecord {
    RepairFindingRecord {
        finding_id: finding.finding_id.clone(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        repair_task_id: finding.repair_task_id.clone(),
        lease_fence_token: finding.lease_fence_token,
        severity: format!("{:?}", finding.severity),
        status: format!("{:?}", finding.status),
        code: finding.code.clone(),
        message: finding.message.clone(),
        subjects: finding.subjects.iter().map(repair_subject_record).collect(),
        proposed_action: format!("{:?}", finding.proposed_action),
        evidence_json: serde_json::to_string(&finding.evidence).unwrap_or_default(),
        created_at_nanos: finding.created_at_nanos,
        finding_hash: finding.finding_hash.clone().unwrap_or_default(),
    }
}

fn repair_subject_record(subject: &RepairSubjectRef) -> RepairSubjectRecord {
    let (cursor_low, cursor_high) = subject.cursor.map(split_u128).unwrap_or((0, 0));
    RepairSubjectRecord {
        subject_kind: subject.subject_kind.clone(),
        subject_id: subject.subject_id.clone(),
        generation: subject.generation.unwrap_or_default(),
        has_generation: subject.generation.is_some(),
        cursor_low,
        cursor_high,
        has_cursor: subject.cursor.is_some(),
        expected_hash: subject.expected_hash.clone().unwrap_or_default(),
        actual_hash: subject.actual_hash.clone().unwrap_or_default(),
    }
}

fn validate_component(value: &str, field: &'static str) -> Result<(), Status> {
    if value.trim().is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || value.starts_with('_')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(Status::invalid_argument(format!("{field} is invalid")));
    }
    Ok(())
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}
