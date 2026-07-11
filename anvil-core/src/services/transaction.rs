use crate::anvil_api::transaction_service_server::TransactionService;
use crate::anvil_api::*;
use crate::core_store::{CoreBeginTransaction, CoreTransaction, CoreTransactionState};
use crate::{
    AppState, auth, index_journal, manifest_journal, mesh_lifecycle, metadata_journal, middleware,
    services::object::enforce_write_precondition,
};
use prost::Message;
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl TransactionService for AppState {
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let request_id = request_id(&request);
        let claims = transaction_claims(&request)?;
        let principal = transaction_principal_from_claims(&claims);
        let req = request.into_inner();
        let scope = req
            .scope
            .ok_or_else(|| Status::invalid_argument("transaction scope is required"))?;
        enforce_transaction_preconditions(self, &claims, &req.preconditions).await?;

        let transaction = self
            .core_store
            .begin_explicit_transaction(CoreBeginTransaction {
                idempotency_key: req.idempotency_key,
                root_anchor_key: scope.root_anchor_key.clone(),
                root_key_hash: scope.root_key_hash,
                scope_partition: scope.root_anchor_key,
                ttl_ms: req.ttl_ms,
                purpose: req.purpose,
                principal,
                preconditions_hash: transaction_preconditions_hash(
                    &req.preconditions,
                    &req.boundary_values,
                )?,
            })
            .await
            .map_err(core_store_status)?;

        Ok(Response::new(BeginTransactionResponse {
            request_id,
            transaction_id: transaction.transaction_id,
            expires_at_unix_nanos: transaction.expires_at_unix_nanos,
            state: transaction_state_name(transaction.state).to_string(),
        }))
    }

    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request_id = request_id(&request);
        let claims = transaction_claims(&request)?;
        let principal = transaction_principal_from_claims(&claims);
        let req = request.into_inner();
        if req.consistency != 0
            && req.consistency != ConsistencyMode::Committed as i32
            && req.consistency != ConsistencyMode::Finalised as i32
        {
            return Err(Status::invalid_argument(
                "explicit transactions support committed or finalised consistency",
            ));
        }
        let wait_for_finalization =
            req.wait_for_finalization || req.consistency == ConsistencyMode::Finalised as i32;
        enforce_transaction_preconditions(self, &claims, &req.final_preconditions).await?;

        let transaction = self
            .core_store
            .commit_explicit_transaction(&req.transaction_id, &principal)
            .await
            .map_err(core_store_status)?;
        let bucket_events =
            crate::bucket_journal::materialize_committed_bucket_metadata_transaction(
                &self.storage,
                &transaction,
            )
            .await
            .map_err(core_store_status)?;
        for event in bucket_events {
            if let Some(bucket) =
                crate::bucket_journal::read_current_bucket_by_id(&self.storage, event.bucket_id)
                    .await
                    .map_err(core_store_status)?
            {
                crate::access_control::grant_bucket_defaults(
                    &self.persistence,
                    &bucket,
                    &claims.sub,
                    &claims.sub,
                    "explicit transaction bucket materialisation",
                )
                .await
                .map_err(core_store_status)?;
                crate::access_control::write_bucket_public_read_tuple(
                    &self.persistence,
                    &bucket,
                    bucket.is_public_read,
                    &claims.sub,
                    "explicit transaction bucket public-read materialisation",
                )
                .await
                .map_err(core_store_status)?;
            }
            let _ = self.bucket_watch_tx.send(event);
        }
        let object_projections =
            metadata_journal::materialize_committed_object_metadata_transaction(
                &self.storage,
                &transaction,
            )
            .await
            .map_err(core_store_status)?;
        for projection in object_projections {
            crate::access_control::grant_object_defaults(
                &self.persistence,
                &projection.bucket,
                &projection.object.key,
                "explicit transaction object materialisation",
            )
            .await
            .map_err(core_store_status)?;
            self.object_manager
                .publish_object_watch_event(
                    projection.object.tenant_id,
                    &projection.bucket,
                    &projection.object,
                    projection.event_type,
                    projection.is_delete_marker,
                )
                .await?;
            self.persistence
                .enqueue_index_builds_for_bucket(&projection.bucket)
                .await
                .map_err(core_store_status)?;
        }
        manifest_journal::materialize_committed_manifest_cas_transaction(
            &self.storage,
            &transaction,
        )
        .await
        .map_err(core_store_status)?;
        let append_streams =
            crate::append_journal::materialize_committed_append_streams_transaction(
                &self.storage,
                &transaction,
            )
            .await
            .map_err(core_store_status)?;
        for stream in append_streams {
            let Some(bucket) =
                crate::bucket_journal::read_current_bucket_by_id(&self.storage, stream.bucket_id)
                    .await
                    .map_err(core_store_status)?
            else {
                continue;
            };
            crate::access_control::grant_stream_defaults(
                &self.persistence,
                &bucket,
                &stream.stream_key,
                &claims.sub,
                &claims.sub,
                "explicit transaction append stream materialisation",
            )
            .await
            .map_err(core_store_status)?;
        }
        let index_events = index_journal::materialize_committed_index_definition_transaction(
            &self.storage,
            &transaction,
        )
        .await
        .map_err(core_store_status)?;
        for event in index_events {
            let Some(bucket) =
                crate::bucket_journal::read_current_bucket_by_id(&self.storage, event.bucket_id)
                    .await
                    .map_err(core_store_status)?
            else {
                continue;
            };
            let index = crate::index_journal::index_definition_from_event_for_projection(&event)
                .map_err(core_store_status)?;
            crate::access_control::grant_index_defaults(
                &self.persistence,
                &bucket,
                &index.name,
                &claims.sub,
                &claims.sub,
                "explicit transaction index materialisation",
            )
            .await
            .map_err(core_store_status)?;
            let _ = self.index_watch_tx.send(event);
            self.persistence
                .enqueue_index_build_for_index(&bucket, &index)
                .await
                .map_err(core_store_status)?;
        }
        crate::gateway_store::materialize_committed_gateway_transaction(
            &self.storage,
            &transaction,
        )
        .await
        .map_err(core_store_status)?;
        for resource in mesh_lifecycle::committed_topology_resources_from_transaction(&transaction)
            .map_err(|err| Status::internal(err.to_string()))?
        {
            match resource {
                mesh_lifecycle::MeshLifecycleCommittedResource::Region { region } => {
                    crate::access_control::grant_region_defaults(
                        &self.persistence,
                        &region,
                        &claims.sub,
                        "explicit transaction mesh region materialisation",
                    )
                    .await
                    .map_err(core_store_status)?;
                }
                mesh_lifecycle::MeshLifecycleCommittedResource::Cell { region, cell_id } => {
                    crate::access_control::grant_cell_defaults(
                        &self.persistence,
                        &region,
                        &cell_id,
                        &claims.sub,
                        "explicit transaction mesh cell materialisation",
                    )
                    .await
                    .map_err(core_store_status)?;
                }
                mesh_lifecycle::MeshLifecycleCommittedResource::Node {
                    region,
                    cell_id,
                    node_id,
                } => {
                    crate::access_control::grant_node_defaults(
                        &self.persistence,
                        &region,
                        &cell_id,
                        &node_id,
                        &claims.sub,
                        "explicit transaction mesh node materialisation",
                    )
                    .await
                    .map_err(core_store_status)?;
                }
            }
        }
        let root_generation = if wait_for_finalization {
            Some(
                self.core_store
                    .verify_explicit_transaction_finalised(&req.transaction_id, &principal)
                    .await
                    .map_err(core_store_status)?,
            )
        } else {
            transaction.committed_root_generation
        };

        Ok(Response::new(WriteResponse {
            request_id,
            mutation_id: transaction.transaction_id.clone(),
            state: if wait_for_finalization {
                WriteState::Finalised as i32
            } else {
                WriteState::Committed as i32
            },
            root_generation,
            transaction_manifest_ref: None,
            idempotency_outcome: "accepted".to_string(),
            retry_after_hint: None,
            finalisation_error: transaction_error(&transaction),
        }))
    }

    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionResponse>, Status> {
        let request_id = request_id(&request);
        let principal = transaction_principal(&request)?;
        let req = request.into_inner();
        let transaction = self
            .core_store
            .rollback_explicit_transaction(&req.transaction_id, &principal, &req.reason)
            .await
            .map_err(core_store_status)?;

        Ok(Response::new(RollbackTransactionResponse {
            request_id,
            transaction_id: transaction.transaction_id,
            state: transaction_state_name(transaction.state).to_string(),
        }))
    }

    async fn get_transaction(
        &self,
        request: Request<GetTransactionRequest>,
    ) -> Result<Response<TransactionStatus>, Status> {
        let principal = transaction_principal(&request)?;
        let req = request.into_inner();
        let transaction = self
            .core_store
            .read_explicit_transaction_for_principal(&req.transaction_id, &principal)
            .await
            .map_err(core_store_status)?;

        Ok(Response::new(transaction_status(&transaction)))
    }
}

fn transaction_principal<T>(request: &Request<T>) -> Result<String, Status> {
    Ok(transaction_principal_from_claims(&transaction_claims(
        request,
    )?))
}

fn transaction_claims<T>(request: &Request<T>) -> Result<auth::Claims, Status> {
    request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))
}

fn transaction_principal_from_claims(claims: &auth::Claims) -> String {
    format!("tenant/{}/principal/{}", claims.tenant_id, claims.sub)
}

async fn enforce_transaction_preconditions(
    state: &AppState,
    claims: &auth::Claims,
    preconditions: &[WritePrecondition],
) -> Result<(), Status> {
    for precondition in preconditions {
        enforce_write_precondition(state, claims, Some(precondition)).await?;
    }
    Ok(())
}

#[derive(Clone, PartialEq, Message)]
struct TransactionPreconditionsHashProto {
    #[prost(message, repeated, tag = "1")]
    preconditions: Vec<WritePrecondition>,
    #[prost(message, repeated, tag = "2")]
    boundary_values: Vec<BoundaryValue>,
}

fn transaction_preconditions_hash(
    preconditions: &[WritePrecondition],
    boundary_values: &[BoundaryValue],
) -> Result<String, Status> {
    let input = TransactionPreconditionsHashProto {
        preconditions: preconditions.to_vec(),
        boundary_values: boundary_values.to_vec(),
    };
    let bytes = crate::core_store::encode_deterministic_proto(&input);
    let mut hasher = Sha256::new();
    hasher.update(b"anvil.transaction.preconditions.v1");
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(&bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn request_id<T>(request: &Request<T>) -> String {
    request
        .extensions()
        .get::<middleware::AnvilRequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string())
}

fn transaction_status(transaction: &CoreTransaction) -> TransactionStatus {
    TransactionStatus {
        transaction_id: transaction.transaction_id.clone(),
        state: transaction_state_name(transaction.state).to_string(),
        root_key_hash: transaction.root_key_hash.clone(),
        committed_root_generation: transaction.committed_root_generation,
        error: transaction_error(transaction),
    }
}

fn transaction_error(transaction: &CoreTransaction) -> Option<AnvilError> {
    transaction
        .finalisation_error
        .as_ref()
        .or(transaction.failure_evidence.as_ref())
        .map(|message| AnvilError {
            code: transaction_state_name(transaction.state).to_string(),
            message: message.clone(),
        })
}

fn transaction_state_name(state: CoreTransactionState) -> &'static str {
    match state {
        CoreTransactionState::Open => "open",
        CoreTransactionState::Prepared => "committing",
        CoreTransactionState::Committed => "committed",
        CoreTransactionState::FinalisationFailed | CoreTransactionState::Aborted => "failed",
        CoreTransactionState::RolledBack => "rolled_back",
        CoreTransactionState::Expired => "expired",
        CoreTransactionState::Failed => "failed",
    }
}

fn core_store_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("TransactionNotFound") {
        Status::not_found("TransactionNotFound")
    } else if message.contains("TransactionPrincipalMismatch") {
        Status::permission_denied("TransactionPrincipalMismatch")
    } else if message.contains("TransactionScopeMismatch") {
        Status::failed_precondition("TransactionScopeMismatch")
    } else if message.contains("TransactionExpired") {
        Status::failed_precondition("TransactionExpired")
    } else if message.contains("TransactionRolledBack") {
        Status::failed_precondition("TransactionRolledBack")
    } else if message.contains("TransactionAlreadyCommitted") {
        Status::failed_precondition("TransactionAlreadyCommitted")
    } else if message.contains("TransactionConflict") {
        Status::aborted("TransactionConflict")
    } else if message.contains("TransactionNotOpen") {
        Status::failed_precondition("TransactionNotOpen")
    } else if message.contains("TransactionNotCommittable") {
        Status::failed_precondition("TransactionNotCommittable")
    } else if message.contains("idempotency conflict") {
        Status::already_exists("TransactionConflict")
    } else if message.contains("must not be empty")
        || message.contains("must be a sha256 hash")
        || message.contains("root key hash mismatch")
        || message.contains("contains an invalid component")
    {
        Status::invalid_argument(message)
    } else {
        Status::internal(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::core_store::{
        CF_TRANSACTIONS, CoreMetaRowCommonProto, CoreMetaStore, CoreMetaTuplePart,
        CoreMetaVisibilityState, CoreMutationBatch, CoreMutationOperation, CoreStore, ReadStream,
        TABLE_EXPLICIT_TRANSACTION_ROW, core_meta_committed_row_common, core_meta_tuple_key,
    };
    use tempfile::TempDir;
    use tokio::time::{Duration, sleep};

    #[derive(Clone, PartialEq, Message)]
    struct ExplicitTransactionStateRowProto {
        #[prost(message, optional, tag = "1")]
        common: Option<CoreMetaRowCommonProto>,
        #[prost(string, tag = "2")]
        transaction_id: String,
        #[prost(string, tag = "3")]
        idempotency_key_hash: String,
        #[prost(string, tag = "4")]
        root_anchor_key: String,
        #[prost(string, tag = "5")]
        root_key_hash: String,
        #[prost(string, tag = "6")]
        state: String,
        #[prost(uint64, tag = "7")]
        opened_at_unix_nanos: u64,
        #[prost(uint64, tag = "8")]
        expires_at_unix_nanos: u64,
        #[prost(string, repeated, tag = "9")]
        staged_mutation_ids: Vec<String>,
        #[prost(string, repeated, tag = "10")]
        precondition_hashes: Vec<String>,
        #[prost(string, tag = "11")]
        terminal_error_code: String,
    }

    async fn test_state() -> (TempDir, AppState) {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            cluster_secret: Some("test-cluster-secret".to_string()),
            cluster_listen_addr: "/ip4/127.0.0.1/udp/0/quic-v1".to_string(),
            public_api_addr: "127.0.0.1:0".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "local".to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            bootstrap_addrs: Vec::new(),
            init_cluster: false,
            enable_mdns: false,
            storage_path: temp.path().join("storage").to_string_lossy().into_owned(),
            ..Config::default()
        };
        let state = AppState::new(config, None).await.unwrap();
        (temp, state)
    }

    fn claims_for(sub: &str) -> auth::Claims {
        auth::Claims {
            sub: sub.to_string(),
            exp: usize::MAX,
            tenant_id: 1,
            jti: Some("test-jti".to_string()),
        }
    }

    fn claims() -> auth::Claims {
        claims_for("test-app")
    }

    fn with_claims<T>(message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(claims());
        request
    }

    fn with_claims_for<T>(message: T, sub: &str) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(claims_for(sub));
        request
    }

    fn scope(root_anchor_key: &str) -> TransactionScope {
        TransactionScope {
            root_anchor_key: root_anchor_key.to_string(),
            root_key_hash: CoreStore::root_key_hash_for_anchor(root_anchor_key),
        }
    }

    fn explicit_transaction_tuple_key(transaction_id: &str) -> Vec<u8> {
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(transaction_id)]).unwrap()
    }

    fn explicit_transaction_state_payload(
        transaction_id: &str,
        root_anchor_key: &str,
        root_generation: u64,
        state: &str,
        expires_at_unix_nanos: u64,
    ) -> Vec<u8> {
        let root_key_hash = CoreStore::root_key_hash_for_anchor(root_anchor_key);
        crate::core_store::encode_deterministic_proto(&ExplicitTransactionStateRowProto {
            common: Some(core_meta_committed_row_common(
                "tenant/1",
                root_key_hash.clone(),
                root_generation,
                transaction_id,
                0,
            )),
            transaction_id: transaction_id.to_string(),
            idempotency_key_hash: hash_test_string("idempotency", transaction_id),
            root_anchor_key: root_anchor_key.to_string(),
            root_key_hash,
            state: state.to_string(),
            opened_at_unix_nanos: 0,
            expires_at_unix_nanos,
            staged_mutation_ids: Vec::new(),
            precondition_hashes: Vec::new(),
            terminal_error_code: String::new(),
        })
    }

    fn read_explicit_transaction_state_row(
        state: &AppState,
        tuple_key: &[u8],
    ) -> Option<ExplicitTransactionStateRowProto> {
        let payload = CoreMetaStore::open(state.storage.core_store_meta_path())
            .unwrap()
            .get(CF_TRANSACTIONS, TABLE_EXPLICIT_TRANSACTION_ROW, tuple_key)
            .unwrap()?;
        Some(
            crate::core_store::decode_deterministic_proto(
                &payload,
                "transaction service explicit transaction CoreMeta row",
            )
            .unwrap(),
        )
    }

    fn hash_test_string(domain: &str, value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(domain.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
        format!("sha256:{:x}", hasher.finalize())
    }

    #[test]
    fn transaction_precondition_hash_includes_boundary_values() {
        let precondition = WritePrecondition {
            object_versions: vec![ObjectVersionPrecondition {
                bucket_name: "docs".to_string(),
                object_key: "a.json".to_string(),
                expected_version_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
                must_not_exist: false,
            }],
            lease_fence: None,
        };
        let base = transaction_preconditions_hash(&[precondition.clone()], &[]).unwrap();
        let with_boundary = transaction_preconditions_hash(
            &[precondition],
            &[BoundaryValue {
                name: "customer".to_string(),
                value: "acme".to_string(),
            }],
        )
        .unwrap();
        assert_ne!(base, with_boundary);
    }

    #[tokio::test]
    async fn transaction_service_begin_get_rollback_and_reject_commit_after_rollback() {
        let (_temp, state) = test_state().await;
        let begin = state
            .begin_transaction(with_claims(BeginTransactionRequest {
                idempotency_key: "service-rollback".to_string(),
                scope: Some(scope("tenant/1/root/rollback")),
                preconditions: Vec::new(),
                boundary_values: Vec::new(),
                ttl_ms: 60_000,
                purpose: "service test rollback".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(begin.state, "open");

        let open = state
            .get_transaction(with_claims(GetTransactionRequest {
                transaction_id: begin.transaction_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(open.state, "open");

        let row_key = explicit_transaction_tuple_key(&begin.transaction_id);
        let row_payload = explicit_transaction_state_payload(
            &begin.transaction_id,
            "tenant/1/root/rollback",
            1,
            "rolled_back",
            begin.expires_at_unix_nanos,
        );
        state
            .core_store
            .stage_coremeta_put_in_transaction(
                &begin.transaction_id,
                &transaction_principal(&with_claims(())).unwrap(),
                CF_TRANSACTIONS,
                TABLE_EXPLICIT_TRANSACTION_ROW,
                row_key.clone(),
                row_payload,
                None,
                true,
                false,
            )
            .await
            .unwrap();
        assert!(read_explicit_transaction_state_row(&state, &row_key).is_none());

        let rolled_back = state
            .rollback_transaction(with_claims(RollbackTransactionRequest {
                transaction_id: begin.transaction_id.clone(),
                reason: "client cancelled".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rolled_back.state, "rolled_back");
        assert!(read_explicit_transaction_state_row(&state, &row_key).is_none());

        let rejected = state
            .commit_transaction(with_claims(CommitTransactionRequest {
                transaction_id: begin.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn transaction_service_corestore_stage_rejects_second_scope() {
        let (_temp, state) = test_state().await;
        let root = "tenant/1/root/scope-a";
        let begin = state
            .begin_transaction(with_claims(BeginTransactionRequest {
                idempotency_key: "service-scope-mismatch".to_string(),
                scope: Some(scope(root)),
                preconditions: Vec::new(),
                boundary_values: Vec::new(),
                ttl_ms: 60_000,
                purpose: "service test scope mismatch".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        let err = state
            .core_store
            .stage_explicit_transaction_batch(CoreMutationBatch {
                transaction_id: begin.transaction_id,
                scope_partition: root.to_string(),
                committed_by_principal: transaction_principal(&with_claims(())).unwrap(),
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: "tenant/1/root/scope-b".to_string(),
                    stream_id: "object_metadata:1:scope-mismatch".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"key":"wrong-scope"}"#.to_vec(),
                    idempotency_key: None,
                }],
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("TransactionScopeMismatch"));
    }

    #[tokio::test]
    async fn transaction_service_get_rejects_other_principal() {
        let (_temp, state) = test_state().await;
        let begin = state
            .begin_transaction(with_claims(BeginTransactionRequest {
                idempotency_key: "service-principal-scope".to_string(),
                scope: Some(scope("tenant/1/root/principal-scope")),
                preconditions: Vec::new(),
                boundary_values: Vec::new(),
                ttl_ms: 60_000,
                purpose: "service test principal scoping".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        let rejected = state
            .get_transaction(with_claims_for(
                GetTransactionRequest {
                    transaction_id: begin.transaction_id,
                },
                "other-app",
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn transaction_service_commit_publishes_staged_coremeta_rows() {
        let (_temp, state) = test_state().await;
        let root = "tenant/1/root/commit";
        let begin = state
            .begin_transaction(with_claims(BeginTransactionRequest {
                idempotency_key: "service-commit".to_string(),
                scope: Some(scope(root)),
                preconditions: Vec::new(),
                boundary_values: Vec::new(),
                ttl_ms: 60_000,
                purpose: "service test commit".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        let stream_id = "object_metadata:1:docs".to_string();
        let row_key = explicit_transaction_tuple_key(&begin.transaction_id);
        let row_payload = explicit_transaction_state_payload(
            &begin.transaction_id,
            root,
            1,
            "committed",
            begin.expires_at_unix_nanos,
        );

        state
            .core_store
            .stage_explicit_transaction_batch(CoreMutationBatch {
                transaction_id: begin.transaction_id.clone(),
                scope_partition: root.to_string(),
                committed_by_principal: transaction_principal(&with_claims(())).unwrap(),
                preconditions: Vec::new(),
                operations: vec![
                    CoreMutationOperation::CoreMetaPut {
                        partition_id: root.to_string(),
                        cf: CF_TRANSACTIONS.to_string(),
                        table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                        tuple_key: row_key.clone(),
                        payload: row_payload,
                    },
                    CoreMutationOperation::StreamAppend {
                        partition_id: root.to_string(),
                        stream_id: stream_id.clone(),
                        record_kind: "object.put".to_string(),
                        payload: br#"{"key":"a"}"#.to_vec(),
                        idempotency_key: Some("service-commit-stream".to_string()),
                    },
                ],
            })
            .await
            .unwrap();

        assert!(read_explicit_transaction_state_row(&state, &row_key).is_none());
        assert!(
            state
                .core_store
                .read_stream(ReadStream {
                    stream_id: stream_id.clone(),
                    after_sequence: 0,
                    limit: 10,
                })
                .await
                .unwrap()
                .is_empty()
        );

        let committed = state
            .commit_transaction(with_claims(CommitTransactionRequest {
                transaction_id: begin.transaction_id.clone(),
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(committed.state, WriteState::Committed as i32);

        let visible_row = read_explicit_transaction_state_row(&state, &row_key)
            .expect("committed transaction CoreMeta row");
        assert_eq!(visible_row.transaction_id, begin.transaction_id);
        assert_eq!(visible_row.state, "committed");
        let common = visible_row.common.expect("transaction row common");
        assert_eq!(
            common.visibility_state_enum(),
            CoreMetaVisibilityState::Committed
        );
        assert_eq!(
            state
                .core_store
                .read_stream(ReadStream {
                    stream_id,
                    after_sequence: 0,
                    limit: 10,
                })
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn transaction_service_expired_transaction_cannot_commit() {
        let (_temp, state) = test_state().await;
        let begin = state
            .begin_transaction(with_claims(BeginTransactionRequest {
                idempotency_key: "service-expiry".to_string(),
                scope: Some(scope("tenant/1/root/expiry")),
                preconditions: Vec::new(),
                boundary_values: Vec::new(),
                ttl_ms: 1,
                purpose: "service test expiry".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        sleep(Duration::from_millis(5)).await;

        let rejected = state
            .commit_transaction(with_claims(CommitTransactionRequest {
                transaction_id: begin.transaction_id.clone(),
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);

        let status = state
            .get_transaction(with_claims(GetTransactionRequest {
                transaction_id: begin.transaction_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.state, "expired");
        assert!(status.error.is_some());
    }
}
