use crate::anvil_api::personal_db_service_server::PersonalDbService;
use crate::anvil_api::*;
use crate::{
    AppState,
    anvil_personaldb_sqlite_changeset::iterate_changeset,
    auth, authz_journal,
    formats::{Hash32, personaldb::PersonalDbLogRecord as CorePersonalDbLogRecord},
    permissions::AnvilAction,
    personaldb_catchup::{
        PersonalDbCatchUpRequest as CoreCatchUpRequest,
        PersonalDbCatchUpResponse as CoreCatchUpResponse, PersonalDbSnapshotRestoreReason,
        personaldb_catch_up,
    },
    personaldb_commit_store::{
        write_personaldb_changeset_payload, write_personaldb_commit_certificate,
    },
    personaldb_control::{PersonalDbCommitCertificate, PersonalDbGroupManifest},
    personaldb_envelope::{
        PersonalDbEnvelopeDerivationInput, VerifiedMutationEnvelope,
        derive_verified_mutation_envelope,
    },
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead, read_personaldb_committed_head,
        read_personaldb_group_manifest, write_personaldb_committed_head,
        write_personaldb_group_manifest,
    },
    personaldb_row_index::{PersonalDbRowIndexWrite, write_personaldb_row_index},
    personaldb_segment::{PersonalDbLogSegmentWrite, write_personaldb_log_segment},
    personaldb_submit::{
        SubmitPersonalDbChangeset as CoreSubmitChangeset, default_max_changeset_size,
        validate_submit_personaldb_changeset,
    },
    personaldb_watch::{
        PersonalDbGroupWatchEvent, PersonalDbGroupWatchPayload,
        append_personaldb_group_watch_record, latest_personaldb_group_watch_cursor,
        list_personaldb_group_watch_events,
    },
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl PersonalDbService for AppState {
    type WatchPersonalDbGroupStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchPersonalDbGroupResponse, Status>> + Send>,
    >;

    async fn create_personal_db_group(
        &self,
        request: Request<CreatePersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroupResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_database_id(&req.database_id)?;
        validate_hex32(&req.schema_hash, "schema_hash")?;
        validate_hex32(&req.genesis_hash, "genesis_hash")?;

        let resource = personaldb_resource(claims.tenant_id, &req.database_id);
        if !auth::is_authorized(AnvilAction::PersonalDbCreate, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let signing_key = self.personaldb_signing_key();
        if read_personaldb_group_manifest(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            signing_key,
        )
        .await
        .map_err(internal_status)?
        .is_some()
        {
            return Err(Status::already_exists("PersonalDB group already exists"));
        }

        let now = now_rfc3339();
        let manifest = PersonalDbGroupManifest {
            format_version: 1,
            tenant_id: claims.tenant_id.to_string(),
            database_id: req.database_id.clone(),
            schema_hash: req.schema_hash.clone(),
            genesis_hash: req.genesis_hash.clone(),
            created_at: now.clone(),
            created_by: claims.sub.clone(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 1,
            active_policy_epoch: 1,
            current_row_index_generation: 0,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        }
        .seal(signing_key)
        .map_err(internal_status)?;
        write_personaldb_group_manifest(&self.storage, claims.tenant_id, &manifest, signing_key)
            .await
            .map_err(internal_status)?;

        let committed_head = PersonalDbCommittedHead {
            format_version: 1,
            tenant_id: claims.tenant_id.to_string(),
            database_id: req.database_id,
            log_index: 0,
            log_hash: manifest.genesis_hash.clone(),
            segment_path: String::new(),
            row_index_generation: 0,
            policy_epoch: manifest.active_policy_epoch,
            membership_epoch: manifest.active_membership_epoch,
            schema_hash: manifest.schema_hash.clone(),
            updated_at: now,
            updated_by_node: claims.sub.clone(),
            head_hash: None,
            head_signature: None,
        }
        .seal(signing_key)
        .map_err(internal_status)?;
        write_personaldb_committed_head(
            &self.storage,
            claims.tenant_id,
            &committed_head.database_id,
            &committed_head,
            signing_key,
        )
        .await
        .map_err(internal_status)?;

        Ok(Response::new(PersonalDbGroupResponse {
            manifest: Some(group_manifest_record(manifest)),
            committed_head: Some(committed_head_record(committed_head)),
        }))
    }

    async fn get_personal_db_group(
        &self,
        request: Request<GetPersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroupResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        let resource = personaldb_resource(claims.tenant_id, &req.database_id);
        if !auth::is_authorized(AnvilAction::PersonalDbRead, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let signing_key = self.personaldb_signing_key();
        let manifest = read_personaldb_group_manifest(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            signing_key,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB group not found"))?;
        let committed_head = read_personaldb_committed_head(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            signing_key,
        )
        .await
        .map_err(internal_status)?;

        Ok(Response::new(PersonalDbGroupResponse {
            manifest: Some(group_manifest_record(manifest)),
            committed_head: committed_head.map(committed_head_record),
        }))
    }

    async fn submit_personal_db_changeset(
        &self,
        request: Request<SubmitPersonalDbChangesetRequest>,
    ) -> Result<Response<SubmitPersonalDbChangesetResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let bearer_token = request_bearer_token(&request)?.to_string();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        let resource = personaldb_resource(claims.tenant_id, &req.database_id);
        if !auth::is_authorized(AnvilAction::PersonalDbCommit, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let validated = validate_submit_personaldb_changeset(
            core_submit_request(req)?,
            default_max_changeset_size(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        bind_personaldb_submit_session(&validated.request, &claims, &bearer_token)?;
        let signing_key = self.personaldb_signing_key();
        let manifest = read_personaldb_group_manifest(
            &self.storage,
            claims.tenant_id,
            &validated.request.database_id,
            signing_key,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB group not found"))?;
        let committed_head = read_personaldb_committed_head(
            &self.storage,
            claims.tenant_id,
            &validated.request.database_id,
            signing_key,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB committed head missing"))?;

        if committed_head.log_index != validated.request.base_log_index
            || committed_head.log_hash != validated.request.base_log_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB base log position does not match committed head",
            ));
        }
        if manifest.active_membership_epoch != validated.request.membership_epoch
            || manifest.active_policy_epoch != validated.request.policy_epoch
            || committed_head.schema_hash != manifest.schema_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB submit epochs or schema do not match the active group",
            ));
        }

        let changes = iterate_changeset(&validated.request.changeset_bytes)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let authz_revision = authz_journal::latest_authz_revision(&self.storage, claims.tenant_id)
            .await
            .map_err(internal_status)
            .and_then(|revision| {
                u64::try_from(revision)
                    .map_err(|_| Status::internal("Invalid authorization revision"))
            })?;
        let proposed_log_index = validated
            .request
            .base_log_index
            .checked_add(1)
            .ok_or_else(|| Status::failed_precondition("PersonalDB log index overflow"))?;
        let updated_at = chrono::Utc::now();
        let envelope = derive_verified_mutation_envelope(PersonalDbEnvelopeDerivationInput {
            tenant_id: claims.tenant_id,
            database_id: &validated.request.database_id,
            principal: &validated.request.principal,
            base_log_index: validated.request.base_log_index,
            proposed_log_index,
            changeset_payload_hash: validated.changeset_payload_hash,
            schema_hash: &manifest.schema_hash,
            policy_epoch: manifest.active_policy_epoch,
            authz_revision,
            changes: &changes,
            updated_at_nanos: updated_at
                .timestamp_nanos_opt()
                .ok_or_else(|| Status::internal("Invalid current timestamp"))?,
        })
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        authorize_personaldb_row_effects(&envelope, &claims)?;
        let envelope_hash = envelope.envelope_hash32().map_err(internal_status)?;
        let previous_log_hash = hex32_status(&committed_head.log_hash, "committed head log hash")?;
        let schema_hash = hex32_status(&manifest.schema_hash, "schema hash")?;
        let payload_paths = write_personaldb_changeset_payload(
            &self.storage,
            claims.tenant_id,
            &validated.request.database_id,
            proposed_log_index,
            validated.changeset_payload_hash,
            &validated.request.changeset_bytes,
        )
        .await
        .map_err(internal_status)?;
        let payload_ref = self
            .storage
            .relative_storage_path(&payload_paths.by_index_path)
            .map_err(internal_status)?
            .into_bytes();

        let provisional_record = CorePersonalDbLogRecord::new(
            proposed_log_index,
            validated.request.client_log_epoch,
            validated.request.membership_epoch,
            validated.request.policy_epoch,
            previous_log_hash,
            validated.changeset_payload_hash,
            envelope_hash,
            [0; 32],
            payload_ref.clone(),
            Vec::new(),
            Vec::new(),
        );
        let witnessed_at = now_rfc3339();
        let certificate = PersonalDbCommitCertificate {
            format_version: 1,
            tenant_id: claims.tenant_id.to_string(),
            database_id: validated.request.database_id.clone(),
            log_index: proposed_log_index,
            previous_log_hash: hex::encode(previous_log_hash),
            entry_hash: hex::encode(provisional_record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            verified_envelope_hash: hex::encode(envelope_hash),
            client_log_epoch: validated.request.client_log_epoch,
            membership_epoch: validated.request.membership_epoch,
            policy_epoch: validated.request.policy_epoch,
            leader_replica_id: validated.request.leader_replica_id.clone(),
            voter_acks_hash: hex::encode(validated.voter_acks_hash),
            authz_revision,
            witness_node_id: claims.sub.clone(),
            witnessed_at,
            certificate_hash: None,
            witness_signature: None,
        }
        .seal(signing_key)
        .map_err(internal_status)?;
        let certificate_path = write_personaldb_commit_certificate(
            &self.storage,
            claims.tenant_id,
            &validated.request.database_id,
            &certificate,
            signing_key,
        )
        .await
        .map_err(internal_status)?;
        let certificate_hash = hex32_status(
            certificate
                .certificate_hash
                .as_deref()
                .ok_or_else(|| Status::internal("PersonalDB certificate hash missing"))?,
            "certificate hash",
        )?;
        let certificate_ref = self
            .storage
            .relative_storage_path(&certificate_path)
            .map_err(internal_status)?
            .into_bytes();
        let record = CorePersonalDbLogRecord::new(
            proposed_log_index,
            validated.request.client_log_epoch,
            validated.request.membership_epoch,
            validated.request.policy_epoch,
            previous_log_hash,
            validated.changeset_payload_hash,
            envelope_hash,
            certificate_hash,
            payload_ref,
            certificate_ref,
            Vec::new(),
        );
        let segment_path = write_personaldb_log_segment(
            &self.storage,
            PersonalDbLogSegmentWrite {
                tenant_id: claims.tenant_id,
                database_id: &validated.request.database_id,
                schema_hash,
                source_fence_token: 0,
                records: std::slice::from_ref(&record),
            },
        )
        .await
        .map_err(internal_status)?;
        let mut row_index_generation = committed_head.row_index_generation;
        let row_index_records = envelope.row_index_upserts().map_err(internal_status)?;
        if !row_index_records.is_empty() {
            row_index_generation = row_index_generation
                .checked_add(1)
                .ok_or_else(|| Status::failed_precondition("PersonalDB row index overflow"))?;
            write_personaldb_row_index(
                &self.storage,
                PersonalDbRowIndexWrite {
                    tenant_id: claims.tenant_id,
                    database_id: &validated.request.database_id,
                    generation: row_index_generation,
                    source_hash: record.entry_hash,
                    records: &row_index_records,
                },
            )
            .await
            .map_err(internal_status)?;
        }
        let committed_head = PersonalDbCommittedHead {
            format_version: 1,
            tenant_id: claims.tenant_id.to_string(),
            database_id: validated.request.database_id.clone(),
            log_index: proposed_log_index,
            log_hash: hex::encode(record.entry_hash),
            segment_path: self
                .storage
                .relative_storage_path(&segment_path)
                .map_err(internal_status)?,
            row_index_generation,
            policy_epoch: manifest.active_policy_epoch,
            membership_epoch: manifest.active_membership_epoch,
            schema_hash: manifest.schema_hash.clone(),
            updated_at: now_rfc3339(),
            updated_by_node: claims.sub.clone(),
            head_hash: None,
            head_signature: None,
        }
        .seal(signing_key)
        .map_err(internal_status)?;
        write_personaldb_committed_head(
            &self.storage,
            claims.tenant_id,
            &validated.request.database_id,
            &committed_head,
            signing_key,
        )
        .await
        .map_err(internal_status)?;

        let watch_cursor = latest_personaldb_group_watch_cursor(
            &self.storage,
            claims.tenant_id,
            &validated.request.database_id,
        )
        .await
        .map_err(internal_status)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Status::internal("PersonalDB watch cursor overflow"))?;
        let watch_payload = PersonalDbGroupWatchPayload {
            database_id: validated.request.database_id.clone(),
            event_type: "commit".to_string(),
            log_index: proposed_log_index,
            log_hash: hex::encode(record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            certificate_hash: hex::encode(certificate_hash),
            committed_head_hash: committed_head.head_hash.clone().unwrap_or_default(),
            emitted_at: now_rfc3339(),
        };
        let mutation_id = *uuid::Uuid::new_v4().as_bytes();
        append_personaldb_group_watch_record(
            &self.storage,
            claims.tenant_id,
            &validated.request.database_id,
            watch_cursor,
            mutation_id,
            authz_revision,
            watch_payload.clone(),
        )
        .await
        .map_err(internal_status)?;
        let _ = self.personaldb_watch_tx.send(PersonalDbGroupWatchEvent {
            cursor: watch_cursor,
            mutation_id,
            authz_revision,
            payload: watch_payload,
        });
        let (watch_cursor_low, watch_cursor_high) = split_u128(watch_cursor);
        Ok(Response::new(SubmitPersonalDbChangesetResponse {
            log_index: proposed_log_index,
            log_hash: hex::encode(record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            verified_envelope_hash: hex::encode(envelope_hash),
            certificate_hash: hex::encode(certificate_hash),
            certificate: Some(certificate_record(certificate)),
            committed_head: Some(committed_head_record(committed_head)),
            watch_cursor_low,
            watch_cursor_high,
        }))
    }

    async fn catch_up_personal_db(
        &self,
        request: Request<PersonalDbCatchUpRequest>,
    ) -> Result<Response<PersonalDbCatchUpResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        let resource = personaldb_resource(claims.tenant_id, &req.database_id);
        if !auth::is_authorized(AnvilAction::PersonalDbRead, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let response = personaldb_catch_up(
            &self.storage,
            CoreCatchUpRequest {
                tenant_id: claims.tenant_id,
                database_id: req.database_id,
                principal: req.principal,
                replica_id: req.replica_id,
                have_log_index: req.have_log_index,
                have_log_hash: req.have_log_hash,
                max_entries: nonzero_limit(req.max_entries),
            },
            self.personaldb_signing_key(),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(catch_up_response(response)))
    }

    async fn watch_personal_db_group(
        &self,
        request: Request<WatchPersonalDbGroupRequest>,
    ) -> Result<Response<Self::WatchPersonalDbGroupStream>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        let resource = personaldb_resource(claims.tenant_id, &req.database_id);
        if !auth::is_authorized(AnvilAction::PersonalDbWatch, &resource, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let snapshot = list_personaldb_group_watch_events(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            after_cursor,
            1000,
        )
        .await
        .map_err(internal_status)?;
        let mut live = self.personaldb_watch_tx.subscribe();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.cursor);
                if tx.send(Ok(watch_response(event))).await.is_err() {
                    return;
                }
            }
            loop {
                match live.recv().await {
                    Ok(event) => {
                        if event.cursor <= last_cursor
                            || event.payload.database_id != req.database_id
                        {
                            continue;
                        }
                        last_cursor = event.cursor;
                        if tx.send(Ok(watch_response(event))).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = tx
                            .send(Err(Status::data_loss(
                                "PersonalDB watch fell behind retained live event window",
                            )))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchPersonalDbGroupStream
        ))
    }
}

impl AppState {
    fn personaldb_signing_key(&self) -> &[u8] {
        self.config.anvil_secret_encryption_key.as_bytes()
    }
}

fn request_claims<T>(request: &Request<T>) -> Result<&auth::Claims, Status> {
    request
        .extensions()
        .get::<auth::Claims>()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))
}

fn request_bearer_token<T>(request: &Request<T>) -> Result<&str, Status> {
    request
        .extensions()
        .get::<auth::AuthenticatedBearerToken>()
        .map(|token| token.0.as_str())
        .ok_or_else(|| Status::unauthenticated("Missing authenticated session token"))
}

fn bind_personaldb_submit_session(
    request: &CoreSubmitChangeset,
    claims: &auth::Claims,
    bearer_token: &str,
) -> Result<(), Status> {
    if request.session_token != bearer_token {
        return Err(Status::unauthenticated(
            "PersonalDB session token does not match authenticated bearer",
        ));
    }
    if request.principal != claims.sub {
        return Err(Status::permission_denied(
            "PersonalDB principal does not match authenticated session",
        ));
    }
    Ok(())
}

fn authorize_personaldb_row_effects(
    envelope: &VerifiedMutationEnvelope,
    claims: &auth::Claims,
) -> Result<(), Status> {
    for effect in &envelope.table_effects {
        let binding = &effect.source_resource_binding;
        let resource = format!(
            "tenant-{}/{}/{}/{}",
            claims.tenant_id, envelope.database_id, binding.resource_type, binding.resource_id
        );
        for permission in &effect.required_permissions {
            let action = permission
                .parse::<AnvilAction>()
                .map_err(|_| Status::internal("Invalid PersonalDB derived permission"))?;
            if !auth::is_authorized(action, &resource, &claims.scopes) {
                return Err(Status::permission_denied(
                    "PersonalDB row/resource mutation is not authorized",
                ));
            }
        }
    }
    Ok(())
}

fn core_submit_request(
    req: SubmitPersonalDbChangesetRequest,
) -> Result<CoreSubmitChangeset, Status> {
    let client_debug_metadata = if req.client_debug_metadata_json.trim().is_empty() {
        None
    } else {
        Some(
            serde_json::from_str(&req.client_debug_metadata_json)
                .map_err(|err| Status::invalid_argument(err.to_string()))?,
        )
    };
    Ok(CoreSubmitChangeset {
        tenant_id: req.tenant_id,
        database_id: req.database_id,
        principal: req.principal,
        session_token: req.session_token,
        request_id: req.request_id,
        idempotency_key: req.idempotency_key,
        base_log_index: req.base_log_index,
        base_log_hash: req.base_log_hash,
        client_log_epoch: req.client_log_epoch,
        membership_epoch: req.membership_epoch,
        policy_epoch: req.policy_epoch,
        leader_replica_id: req.leader_replica_id,
        voter_acks: req
            .voter_acks
            .into_iter()
            .map(|ack| crate::personaldb_submit::PersonalDbVoterAck {
                replica_id: ack.replica_id,
                log_index: ack.log_index,
                log_hash: ack.log_hash,
                signature: ack.signature,
            })
            .collect(),
        changeset_payload_hash: req.changeset_payload_hash,
        changeset_bytes: req.changeset_bytes,
        client_debug_metadata,
    })
}

fn group_manifest_record(manifest: PersonalDbGroupManifest) -> PersonalDbGroupManifestRecord {
    PersonalDbGroupManifestRecord {
        format_version: manifest.format_version.into(),
        tenant_id: manifest.tenant_id,
        database_id: manifest.database_id,
        schema_hash: manifest.schema_hash,
        genesis_hash: manifest.genesis_hash,
        created_at: manifest.created_at,
        created_by: manifest.created_by,
        consistency_policy: manifest.consistency_policy,
        object_layout_version: manifest.object_layout_version.into(),
        active_membership_epoch: manifest.active_membership_epoch,
        active_policy_epoch: manifest.active_policy_epoch,
        current_row_index_generation: manifest.current_row_index_generation,
        current_projection_generation: manifest.current_projection_generation,
        manifest_hash: manifest.manifest_hash.unwrap_or_default(),
        manifest_signature: manifest.manifest_signature.unwrap_or_default(),
    }
}

fn committed_head_record(head: PersonalDbCommittedHead) -> PersonalDbCommittedHeadRecord {
    PersonalDbCommittedHeadRecord {
        format_version: head.format_version.into(),
        tenant_id: head.tenant_id,
        database_id: head.database_id,
        log_index: head.log_index,
        log_hash: head.log_hash,
        segment_path: head.segment_path,
        row_index_generation: head.row_index_generation,
        policy_epoch: head.policy_epoch,
        membership_epoch: head.membership_epoch,
        schema_hash: head.schema_hash,
        updated_at: head.updated_at,
        updated_by_node: head.updated_by_node,
        head_hash: head.head_hash.unwrap_or_default(),
        head_signature: head.head_signature.unwrap_or_default(),
    }
}

fn snapshots_head_record(head: PersonalDbSnapshotsHead) -> PersonalDbSnapshotsHeadRecord {
    PersonalDbSnapshotsHeadRecord {
        format_version: head.format_version.into(),
        tenant_id: head.tenant_id,
        database_id: head.database_id,
        latest_snapshot_log_index: head.latest_snapshot_log_index,
        latest_snapshot_log_hash: head.latest_snapshot_log_hash,
        latest_snapshot_manifest_path: head.latest_snapshot_manifest_path,
        retained_snapshot_count: head.retained_snapshot_count,
        updated_at: head.updated_at,
        updated_by_node: head.updated_by_node,
        head_hash: head.head_hash.unwrap_or_default(),
        head_signature: head.head_signature.unwrap_or_default(),
    }
}

fn certificate_record(
    certificate: crate::personaldb_control::PersonalDbCommitCertificate,
) -> PersonalDbCommitCertificateRecord {
    PersonalDbCommitCertificateRecord {
        format_version: certificate.format_version.into(),
        tenant_id: certificate.tenant_id,
        database_id: certificate.database_id,
        log_index: certificate.log_index,
        previous_log_hash: certificate.previous_log_hash,
        entry_hash: certificate.entry_hash,
        changeset_payload_hash: certificate.changeset_payload_hash,
        verified_envelope_hash: certificate.verified_envelope_hash,
        client_log_epoch: certificate.client_log_epoch,
        membership_epoch: certificate.membership_epoch,
        policy_epoch: certificate.policy_epoch,
        leader_replica_id: certificate.leader_replica_id,
        voter_acks_hash: certificate.voter_acks_hash,
        authz_revision: certificate.authz_revision,
        witness_node_id: certificate.witness_node_id,
        witnessed_at: certificate.witnessed_at,
        certificate_hash: certificate.certificate_hash.unwrap_or_default(),
        witness_signature: certificate.witness_signature.unwrap_or_default(),
    }
}

fn log_record(record: crate::formats::personaldb::PersonalDbLogRecord) -> PersonalDbLogRecord {
    PersonalDbLogRecord {
        log_index: record.log_index,
        client_log_epoch: record.client_log_epoch,
        membership_epoch: record.membership_epoch,
        policy_epoch: record.policy_epoch,
        previous_log_hash: hex::encode(record.previous_log_hash),
        changeset_payload_hash: hex::encode(record.changeset_payload_hash),
        verified_envelope_hash: hex::encode(record.verified_envelope_hash),
        certificate_hash: hex::encode(record.certificate_hash),
        payload_ref: String::from_utf8_lossy(&record.payload_ref).into_owned(),
        certificate_ref: String::from_utf8_lossy(&record.certificate_ref).into_owned(),
        inline_certificate_json: record.inline_certificate_json,
        entry_hash: hex::encode(record.entry_hash),
    }
}

fn catch_up_response(response: CoreCatchUpResponse) -> PersonalDbCatchUpResponse {
    match response {
        CoreCatchUpResponse::Entries(entries) => PersonalDbCatchUpResponse {
            snapshot_required: false,
            snapshot_reason: String::new(),
            committed_head: Some(committed_head_record(entries.committed_head)),
            snapshots_head: None,
            entries: entries
                .entries
                .into_iter()
                .map(|entry| PersonalDbCatchUpEntry {
                    log_record: Some(log_record(entry.record)),
                    changeset_bytes: entry.changeset_bytes,
                    certificate: Some(certificate_record(entry.certificate)),
                    certificate_json: entry.certificate_json,
                })
                .collect(),
            has_more: entries.has_more,
        },
        CoreCatchUpResponse::SnapshotRequired(snapshot) => PersonalDbCatchUpResponse {
            snapshot_required: true,
            snapshot_reason: snapshot_reason(snapshot.reason).to_string(),
            committed_head: snapshot.committed_head.map(committed_head_record),
            snapshots_head: snapshot.snapshots_head.map(snapshots_head_record),
            entries: Vec::new(),
            has_more: false,
        },
    }
}

fn watch_response(event: PersonalDbGroupWatchEvent) -> WatchPersonalDbGroupResponse {
    let (low, high) = split_u128(event.cursor);
    WatchPersonalDbGroupResponse {
        cursor_low: low,
        cursor_high: high,
        database_id: event.payload.database_id,
        event_type: event.payload.event_type,
        log_index: event.payload.log_index,
        log_hash: event.payload.log_hash,
        changeset_payload_hash: event.payload.changeset_payload_hash,
        certificate_hash: event.payload.certificate_hash,
        committed_head_hash: event.payload.committed_head_hash,
        authz_revision: event.authz_revision,
        emitted_at: event.payload.emitted_at,
    }
}

fn snapshot_reason(reason: PersonalDbSnapshotRestoreReason) -> &'static str {
    match reason {
        PersonalDbSnapshotRestoreReason::MissingCommittedHead => "missing_committed_head",
        PersonalDbSnapshotRestoreReason::DivergentReplica => "divergent_replica",
    }
}

fn validate_claim_tenant(claim_tenant_id: i64, request_tenant_id: i64) -> Result<(), Status> {
    if request_tenant_id != claim_tenant_id {
        return Err(Status::permission_denied("Tenant scope mismatch"));
    }
    Ok(())
}

fn validate_database_id(database_id: &str) -> Result<(), Status> {
    if database_id.is_empty() {
        return Err(Status::invalid_argument("database_id must not be empty"));
    }
    if database_id.contains('/') || database_id.contains("..") {
        return Err(Status::invalid_argument(
            "database_id contains unsafe characters",
        ));
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<(), Status> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Status::invalid_argument(format!("{field} must be hex32")));
    }
    Ok(())
}

fn hex32_status(value: &str, field: &'static str) -> Result<Hash32, Status> {
    validate_hex32(value, field)?;
    hex::decode(value)
        .map_err(|_| Status::invalid_argument(format!("{field} must be hex32")))?
        .try_into()
        .map_err(|_| Status::invalid_argument(format!("{field} must be hex32")))
}

fn personaldb_resource(tenant_id: i64, database_id: &str) -> String {
    format!("tenant-{tenant_id}/{database_id}")
}

fn nonzero_limit(limit: u32) -> usize {
    if limit == 0 { 100 } else { limit as usize }
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn join_u128(low: u64, high: u64) -> u128 {
    (u128::from(high) << 64) | u128::from(low)
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn internal_status(err: impl std::fmt::Display) -> Status {
    Status::internal(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::hash32;

    #[test]
    fn watch_cursor_split_round_trips() {
        let value = (37u128 << 64) | 99;
        let (low, high) = split_u128(value);
        assert_eq!(join_u128(low, high), value);
    }

    #[test]
    fn database_id_validation_rejects_path_escape() {
        assert!(validate_database_id("db-alpha").is_ok());
        assert!(validate_database_id("../db").is_err());
        assert!(validate_database_id("tenant/db").is_err());
    }

    #[test]
    fn log_record_hex_encodes_hashes() {
        let record = crate::formats::personaldb::PersonalDbLogRecord::new(
            1,
            1,
            1,
            1,
            [1; 32],
            [2; 32],
            [3; 32],
            [4; 32],
            b"payload".to_vec(),
            b"certificate".to_vec(),
            Vec::new(),
        );
        let encoded = log_record(record);
        assert_eq!(encoded.previous_log_hash, hex::encode([1; 32]));
        assert_eq!(encoded.changeset_payload_hash, hex::encode([2; 32]));
    }

    #[test]
    fn genesis_hash_uses_blake3_hash_format() {
        assert!(validate_hex32(&hex::encode(hash32(b"genesis")), "genesis_hash").is_ok());
    }
}
