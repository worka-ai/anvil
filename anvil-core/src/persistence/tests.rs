use super::*;
use serde_json::json;
use tempfile::tempdir;

fn test_config(storage_path: &std::path::Path) -> Config {
    Config {
        jwt_secret: "test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        public_api_addr: "test-node".to_string(),
        api_listen_addr: "127.0.0.1:0".to_string(),
        region: "test-region".to_string(),
        storage_path: storage_path.to_string_lossy().to_string(),
        ..Config::default()
    }
}

fn model_manifest() -> crate::anvil_api::ModelManifest {
    crate::anvil_api::ModelManifest {
        schema_version: "1".to_string(),
        artifact_id: "artifact-a".to_string(),
        name: "artifact-a".to_string(),
        format: "test".to_string(),
        components: Vec::new(),
        base_artifact_id: String::new(),
        delta_artifact_ids: Vec::new(),
        signatures: Vec::new(),
        merkle_root: "abc".to_string(),
        meta: std::collections::HashMap::new(),
    }
}

#[tokio::test]
async fn tenant_and_bucket_creation_materialise_mesh_directory_locators() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();

    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "eu-west-1")
        .await
        .unwrap();

    let tenant_name = persistence
        .get_mesh_tenant_name_locator("tenant-a")
        .await
        .unwrap()
        .expect("tenant-name locator");
    assert_eq!(tenant_name.tenant_id.as_str(), tenant.id.to_string());
    assert_eq!(tenant_name.status, mesh_directory::TenantNameStatus::Active);
    assert_eq!(tenant_name.idempotency_key.as_deref(), Some("unused"));
    assert_eq!(tenant_name.reservation_expires_at, None);
    assert_eq!(tenant_name.generation, 2);

    let bucket_locator = persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(bucket_locator.tenant_id.as_str(), tenant.id.to_string());
    assert_eq!(bucket_locator.bucket_name.as_str(), "docs");
    assert_eq!(bucket_locator.bucket_id.as_str(), bucket.id.to_string());
    assert_eq!(bucket_locator.home_region.as_str(), "eu-west-1");
    assert_eq!(
        bucket_locator.descriptor_key(),
        format!(
            "_anvil/control/v1/mesh/buckets/{}/{}/docs.pb",
            bucket_locator.partition(),
            tenant.id
        )
    );

    let tenant_name_fence = read_ownership_fence(
        &persistence.storage,
        0,
        &OwnershipResource {
            resource_kind: OwnershipResourceKind::ControlPartition,
            resource_id: format!(
                "{}/{}",
                mesh_directory::RoutingRecordFamily::TenantName.stream_family(),
                tenant_name.partition()
            ),
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap()
    .expect("tenant-name control partition ownership fence");
    assert_eq!(tenant_name_fence.owner, persistence.ownership_principal());

    let bucket_locator_fence = read_ownership_fence(
        &persistence.storage,
        0,
        &OwnershipResource {
            resource_kind: OwnershipResourceKind::ControlPartition,
            resource_id: format!(
                "{}/{}",
                mesh_directory::RoutingRecordFamily::BucketLocator.stream_family(),
                bucket_locator.partition()
            ),
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap()
    .expect("bucket-locator control partition ownership fence");
    assert_eq!(
        bucket_locator_fence.owner,
        persistence.ownership_principal()
    );
}

#[tokio::test]
async fn region_drain_blocks_bucket_creation_and_completion_with_active_locator() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();
    let (region, _, _) = register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let draining = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
        )
        .await
        .unwrap();
    let placement_err = persistence
        .create_bucket(tenant.id, "more-docs", "test-region")
        .await
        .unwrap_err();
    assert_eq!(placement_err.code(), tonic::Code::FailedPrecondition);
    assert!(
        placement_err
            .message()
            .contains("cannot accept new writable placement")
    );

    let completion_err = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Drained,
        )
        .await
        .unwrap_err();
    assert!(
        completion_err
            .to_string()
            .contains("still name the region as primary")
    );
}

#[tokio::test]
async fn region_drain_applies_read_only_exceptions_to_bucket_locators() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();
    let (region, _, _) = register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let draining = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
        )
        .await
        .unwrap();
    let report = persistence
        .apply_region_drain_plan(
            "test-region",
            crate::mesh_lifecycle::BucketDrainDisposition::BlockUntilEmpty,
            vec![RegionDrainBucketOverride {
                tenant_id: tenant.id.to_string(),
                bucket_name: "docs".to_string(),
                disposition: crate::mesh_lifecycle::BucketDrainDisposition::RemainProxyOnly,
                reason: "customer-approved delayed migration".to_string(),
                expires_at: Some("2026-08-02T00:00:00Z".to_string()),
            }],
        )
        .await
        .unwrap();

    assert_eq!(report.decisions.len(), 1);
    let decision = &report.decisions[0];
    assert_eq!(
        decision.status_before,
        mesh_directory::BucketLocatorStatus::Active
    );
    assert_eq!(
        decision.status_after,
        mesh_directory::BucketLocatorStatus::ReadOnly
    );
    assert!(decision.exception_written);
    assert!(decision.locator_updated);

    let locator = persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(
        locator.status,
        mesh_directory::BucketLocatorStatus::ReadOnly
    );
    assert_eq!(locator.generation, 2);

    let exceptions = crate::mesh_lifecycle::list_bucket_drain_exceptions(
        &persistence.storage,
        Some("test-region"),
    )
    .await
    .unwrap();
    assert_eq!(exceptions.len(), 1);
    assert_eq!(
        exceptions[0].disposition,
        crate::mesh_lifecycle::BucketDrainDisposition::RemainProxyOnly
    );

    let full_drain_err = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Drained,
        )
        .await
        .unwrap_err();
    assert!(
        full_drain_err
            .to_string()
            .contains("still name the region as primary")
    );

    let drained_with_exceptions = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::DrainedWithExceptions,
        )
        .await
        .unwrap();
    assert_eq!(
        drained_with_exceptions.state,
        crate::mesh_lifecycle::LifecycleState::DrainedWithExceptions
    );
}

#[tokio::test]
async fn region_drain_delete_after_retention_keeps_region_from_exception_completion() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();
    let (region, _, _) = register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let draining = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
        )
        .await
        .unwrap();
    let report = persistence
        .apply_region_drain_plan(
            "test-region",
            crate::mesh_lifecycle::BucketDrainDisposition::DeleteAfterRetention,
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        report.decisions[0].status_after,
        mesh_directory::BucketLocatorStatus::Draining
    );

    let completion_err = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::DrainedWithExceptions,
        )
        .await
        .unwrap_err();
    assert!(
        completion_err
            .to_string()
            .contains("do not have a valid read-only drain exception")
    );
}

#[tokio::test]
async fn node_drain_completion_requires_no_runtime_ownership_and_force_offline_expires_it() {
    let temp = tempdir().unwrap();
    let mut config = test_config(temp.path());
    config.public_api_addr = "admin-node".to_string();
    let persistence = Persistence::new(&config, None).unwrap();
    let now_nanos = current_time_nanos()
        .unwrap()
        .saturating_add(3_600_000_000_000);
    let ttl_nanos = i64::try_from(MAX_OWNERSHIP_LEASE_MS)
        .unwrap()
        .saturating_mul(1_000_000);

    let region = persistence
        .create_region_descriptor(crate::mesh_lifecycle::CreateRegionDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            public_base_url: "https://test-region.anvil-storage.test".to_string(),
            virtual_host_suffix: "test-region.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("default".to_string()),
        })
        .await
        .unwrap();
    let cell = persistence
        .register_cell_descriptor(crate::mesh_lifecycle::RegisterCellDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        })
        .await
        .unwrap();
    persistence
        .transition_cell_descriptor(
            "test-region",
            "default",
            cell.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    let worker = persistence
        .register_node_descriptor(crate::mesh_lifecycle::RegisterNodeDescriptor {
            mesh_id: "default".to_string(),
            node_id: "worker-node".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            libp2p_peer_id: "peer-worker-node".to_string(),
            receipt_signing_public_key_proto: libp2p::identity::Keypair::generate_ed25519()
                .public()
                .encode_protobuf(),
            public_api_addr: "worker-node".to_string(),
            public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7444/quic-v1".to_string()],
            capabilities: vec![crate::mesh_lifecycle::NodeCapability::Object],
            capacity_json: "{}".to_string(),
        })
        .await
        .unwrap();
    let worker = persistence
        .transition_node_descriptor(
            "worker-node",
            worker.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
            None,
        )
        .await
        .unwrap();

    let partition_owner = crate::partition_fence::acquire_partition_recovery(
        &persistence.storage,
        crate::partition_fence::PartitionRecoveryAcquire {
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([8; 32]),
            owner_node_id: "worker-node".to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos,
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();
    let partition_owner = crate::partition_fence::publish_partition_ready(
        &persistence.storage,
        &partition_owner.partition_family,
        &partition_owner.partition_id,
        "worker-node",
        partition_owner.fence_token,
        1,
        &hex::encode([1; 32]),
        now_nanos.saturating_add(1),
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();
    let stale_partition_permit = partition_owner.write_permit().unwrap();

    crate::partition_fence::acquire_ownership(
        &persistence.storage,
        crate::partition_fence::AcquireOwnership {
            request_id: "worker-control-acquire".to_string(),
            idempotency_key: "worker-control-acquire".to_string(),
            resource: crate::partition_fence::OwnershipResource {
                resource_kind: crate::partition_fence::OwnershipResourceKind::WatchPartition,
                resource_id: "watch/alpha".to_string(),
            },
            owner: crate::partition_fence::OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: "worker-node".to_string(),
                actor_instance_id: "worker-node".to_string(),
                display_name: "worker-node".to_string(),
                region: "test-region".to_string(),
                cell: "default".to_string(),
            },
            now_nanos,
            ttl_nanos,
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();

    let task_lease = crate::task_lease::acquire_task_lease(
        &persistence.storage,
        crate::task_lease::TaskLeaseAcquire {
            task_id: "worker-task".to_string(),
            task_kind: "index-build".to_string(),
            partition_family: "index_partition".to_string(),
            partition_id: hex::encode([9; 32]),
            owner: crate::task_lease::TaskLeaseOwner::node("worker-node"),
            source_cursor: 1,
            now_nanos,
            ttl_nanos,
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();

    let draining = persistence
        .transition_node_descriptor(
            "worker-node",
            worker.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
            Some(crate::mesh_lifecycle::NodeDrainDescriptor {
                started_at: "2026-07-02T00:00:00Z".to_string(),
                graceful_timeout_ms: 1000,
                force_after_timeout: false,
            }),
        )
        .await
        .unwrap();
    let blockers = persistence
        .node_runtime_ownership_blockers("worker-node")
        .await
        .unwrap();
    assert!(
        blockers
            .iter()
            .any(|blocker| blocker.starts_with("partition_owner:object_metadata/"))
    );
    assert!(
        blockers
            .iter()
            .any(|blocker| blocker.starts_with("ownership_fence:watch_partition/watch/alpha"))
    );
    assert!(
        blockers
            .iter()
            .any(|blocker| blocker == "task_lease:index-build:worker-task:fence=1")
    );

    let drained = persistence
        .transition_node_descriptor(
            "worker-node",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Drained,
            None,
        )
        .await
        .unwrap_err();
    assert!(drained.to_string().contains("drain cannot complete"));

    let offline = persistence
        .transition_node_descriptor(
            "worker-node",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Offline,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        offline.state,
        crate::mesh_lifecycle::LifecycleState::Offline
    );
    assert!(
        persistence
            .node_runtime_ownership_blockers("worker-node")
            .await
            .unwrap()
            .is_empty()
    );
    let stale_rejection = crate::partition_fence::validate_partition_write(
        &persistence.storage,
        &stale_partition_permit,
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap_err();
    assert_eq!(
        stale_rejection.code,
        crate::error_codes::AnvilErrorCode::PartitionNotOwned
    );
    assert!(
        crate::task_lease::checkpoint_task_lease(
            &persistence.storage,
            &task_lease.task_id,
            &task_lease.owner,
            task_lease.fence_token,
            task_lease.source_cursor,
            now_nanos.saturating_add(2),
            &persistence.partition_owner_signing_key,
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn mesh_routing_projection_diagnostics_detect_bucket_locator_mismatch() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();
    register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let clean = persistence
        .diagnose_mesh_routing_projection(Some(mesh_directory::RoutingRecordFamily::BucketLocator))
        .await
        .unwrap();
    assert!(clean.is_empty());

    let bucket_locator = persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(bucket_locator.bucket_id.as_str(), bucket.id.to_string());
    let mut projected: serde_json::Value = serde_json::to_value(&bucket_locator).unwrap();
    projected["home_region"] = json!("us-east-1");
    mesh_directory::rebuild_routing_record_projection_from_payload(
        &persistence.storage,
        mesh_directory::RoutingRecordFamily::BucketLocator,
        &format!("{}/docs", tenant.id),
        &serde_json::to_vec_pretty(&projected).unwrap(),
    )
    .await
    .unwrap();

    let diagnostics = persistence
        .diagnose_mesh_routing_projection(Some(mesh_directory::RoutingRecordFamily::BucketLocator))
        .await
        .unwrap();
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "mesh_control_projection_payload_mismatch"
            && diagnostic.record_key == format!("{}/docs", tenant.id)
            && diagnostic.repair_safe
            && diagnostic.proposed_action == "repair_routing_record_from_control_stream"
    }));

    let repaired = persistence
        .repair_mesh_routing_record(
            mesh_directory::RoutingRecordFamily::BucketLocator,
            &format!("{}/docs", tenant.id),
        )
        .await
        .unwrap();
    assert_eq!(repaired.record_key, format!("{}/docs", tenant.id));
    let repaired_payload: serde_json::Value = serde_json::from_str(&repaired.payload_json).unwrap();
    assert_eq!(repaired_payload["home_region"], "test-region");
    let clean = persistence
        .diagnose_mesh_routing_projection(Some(mesh_directory::RoutingRecordFamily::BucketLocator))
        .await
        .unwrap();
    assert!(clean.is_empty(), "{clean:#?}");
}

async fn register_active_mesh_placement(
    persistence: &Persistence,
) -> (
    crate::mesh_lifecycle::RegionDescriptor,
    crate::mesh_lifecycle::CellDescriptor,
    crate::mesh_lifecycle::NodeDescriptor,
) {
    let region = persistence
        .create_region_descriptor(crate::mesh_lifecycle::CreateRegionDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            public_base_url: "https://test-region.anvil-storage.test".to_string(),
            virtual_host_suffix: "test-region.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("default".to_string()),
        })
        .await
        .unwrap();
    let cell = persistence
        .register_cell_descriptor(crate::mesh_lifecycle::RegisterCellDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        })
        .await
        .unwrap();
    let cell = persistence
        .transition_cell_descriptor(
            "test-region",
            "default",
            cell.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    let region = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    let node = persistence
        .register_node_descriptor(crate::mesh_lifecycle::RegisterNodeDescriptor {
            mesh_id: "default".to_string(),
            node_id: "test-node".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            libp2p_peer_id: "peer-test-node".to_string(),
            receipt_signing_public_key_proto: libp2p::identity::Keypair::generate_ed25519()
                .public()
                .encode_protobuf(),
            public_api_addr: "test-node".to_string(),
            public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
            capabilities: vec![
                crate::mesh_lifecycle::NodeCapability::Object,
                crate::mesh_lifecycle::NodeCapability::Admin,
            ],
            capacity_json: "{}".to_string(),
        })
        .await
        .unwrap();
    let node = persistence
        .transition_node_descriptor(
            "test-node",
            node.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
            None,
        )
        .await
        .unwrap();
    (region, cell, node)
}

#[test]
fn persistence_replays_anvil_owned_state_after_fresh_instance() {
    std::thread::Builder::new()
        .name("persistence-replay-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .build()
                .unwrap()
                .block_on(persistence_replays_anvil_owned_state_after_fresh_instance_body())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn persistence_replays_anvil_owned_state_after_fresh_instance_body() {
    let temp = tempdir().unwrap();
    let first_config = test_config(temp.path());
    let persistence = Persistence::new(&first_config, None).unwrap();

    persistence.create_region("local").await.unwrap();
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    let app = persistence
        .create_app(tenant.id, "app-a", "client-a", b"encrypted-secret")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "local")
        .await
        .unwrap();
    let object = persistence
        .create_object(
            tenant.id,
            bucket.id,
            "project/a.txt",
            "payload-hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            Some(json!({"label": "alpha"})),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    persistence
        .create_object(
            tenant.id,
            bucket.id,
            "project/nested/b.txt",
            "payload-hash-b",
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let upload = persistence
        .create_multipart_upload(tenant.id, bucket.id, "uploads/large.bin")
        .await
        .unwrap()
        .upload;
    persistence
        .upsert_multipart_part(
            upload.id,
            1,
            payload_ref("part-hash-a", 4),
            4,
            "part-etag-a",
        )
        .await
        .unwrap();

    let append_stream = persistence
        .create_append_stream(tenant.id, bucket.id, &bucket.name, "events")
        .await
        .unwrap()
        .stream;
    persistence
        .append_stream_record(
            append_stream.id,
            payload_ref("event-payload-hash", 42),
            42,
            None,
            None,
        )
        .await
        .unwrap();

    let manifest = persistence
        .compare_and_swap_manifest(
            tenant.id,
            bucket.id,
            &bucket.name,
            "manifests/current.json",
            0,
            json!({"generation": 1}),
            "manifest-hash-a",
        )
        .await
        .unwrap()
        .unwrap();

    let index = persistence
        .create_index_definition(
            tenant.id,
            bucket.id,
            "body",
            "full_text",
            json!({"prefix": "project/"}),
            json!({"field": "body"}),
            "inherit",
            json!({"mode": "watch"}),
        )
        .await
        .unwrap();
    persistence
        .create_index_definition_event(tenant.id, bucket.id, &bucket.name, &index, "create")
        .await
        .unwrap();
    persistence
        .create_index_diagnostic(
            tenant.id,
            bucket.id,
            &bucket.name,
            Some(index.id),
            &index.name,
            &object.key,
            Some(object.version_id),
            "warning",
            "diagnostic-alpha",
            "synthetic diagnostic for replay coverage",
            json!({"source": "test"}),
        )
        .await
        .unwrap();

    let authz = persistence
        .write_authz_tuple(
            tenant.id,
            "document",
            &object.key,
            "reader",
            "user",
            "user-a",
            "",
            "add",
            "test",
            "grant reader",
        )
        .await
        .unwrap();
    persistence
        .enqueue_task(
            crate::tasks::TaskType::DeleteBucket,
            json!({"bucket_id": bucket.id}),
            5,
        )
        .await
        .unwrap();
    persistence
        .create_model_artifact("artifact-a", tenant.id, "models/a", &model_manifest())
        .await
        .unwrap();
    persistence
        .hf_create_key("primary", b"secret", Some("note"))
        .await
        .unwrap();

    drop(persistence);

    let replayed = Persistence::new(&first_config, None).unwrap();

    assert!(
        replayed
            .list_regions()
            .await
            .unwrap()
            .contains(&"local".to_string())
    );
    assert_eq!(
        replayed
            .get_tenant_by_name("tenant-a")
            .await
            .unwrap()
            .unwrap()
            .id,
        tenant.id
    );
    assert_eq!(
        replayed
            .get_app_by_client_id("client-a")
            .await
            .unwrap()
            .unwrap()
            .id,
        app.id
    );
    assert_eq!(
        replayed
            .get_bucket_by_name(tenant.id, "docs")
            .await
            .unwrap()
            .unwrap()
            .id,
        bucket.id
    );

    let replayed_object = replayed
        .get_object(bucket.id, "project/a.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed_object.version_id, object.version_id);
    assert_eq!(replayed_object.content_hash, object.content_hash);
    assert_eq!(replayed_object.user_meta.unwrap()["label"], "alpha");

    let (objects, common_prefixes) = replayed
        .list_objects(bucket.id, "project/", "", 100, "/")
        .await
        .unwrap();
    assert_eq!(
        objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["project/a.txt"]
    );
    assert_eq!(common_prefixes, vec!["project/nested/".to_string()]);
    assert_eq!(
        replayed
            .list_object_versions(bucket.id, "project/", "", None, 100)
            .await
            .unwrap()
            .versions
            .len(),
        2
    );

    assert_eq!(
        replayed
            .get_active_multipart_upload(
                tenant.id,
                bucket.id,
                "uploads/large.bin",
                upload.upload_id
            )
            .await
            .unwrap()
            .unwrap()
            .id,
        upload.id
    );
    assert_eq!(
        replayed
            .list_multipart_parts(upload.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .list_append_stream_records(append_stream.id)
            .await
            .unwrap()
            .len(),
        1
    );

    let second_manifest = replayed
        .compare_and_swap_manifest(
            tenant.id,
            bucket.id,
            &bucket.name,
            "manifests/current.json",
            manifest.revision,
            json!({"generation": 2}),
            "manifest-hash-b",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_manifest.revision, manifest.revision + 1);

    assert_eq!(
        replayed
            .list_index_definitions(tenant.id, bucket.id, false)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .list_index_definition_events(tenant.id, bucket.id, 0, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .list_index_diagnostics(tenant.id, bucket.id, &index.name, "", 0, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .check_authz_tuple(
                tenant.id,
                "document",
                &object.key,
                "reader",
                "user",
                "user-a",
                "",
            )
            .await
            .unwrap()
            .unwrap()
            .revision,
        authz.revision
    );
    assert_eq!(replayed.list_tasks().await.unwrap().len(), 1);
    assert!(
        replayed
            .get_model_artifact("artifact-a")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(replayed.hf_list_keys().await.unwrap().len(), 1);
}

#[tokio::test]
async fn persistence_compacts_object_metadata_and_restarts_from_manifest() {
    let temp = tempdir().unwrap();
    let first_config = test_config(temp.path());
    let persistence = Persistence::new(&first_config, None).unwrap();

    persistence.create_region("local").await.unwrap();
    let bucket = persistence
        .create_bucket(1, "compact-bucket", "local")
        .await
        .unwrap();
    let first = persistence
        .create_object(
            1,
            bucket.id,
            "docs/a.txt",
            "hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            Some(json!({"label": "a"})),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    persistence
        .create_object(
            1,
            bucket.id,
            "docs/nested/b.txt",
            "hash-b",
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let sealed = persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata journal should compact");
    assert_eq!(sealed.metadata_record_count, 2);
    assert_eq!(sealed.directory_record_count, 2);

    drop(persistence);
    let restarted = Persistence::new(&first_config, None).unwrap();

    let replayed = restarted
        .get_object(bucket.id, "docs/a.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed.version_id, first.version_id);
    assert_eq!(replayed.content_hash, first.content_hash);
    assert_eq!(replayed.user_meta.unwrap()["label"], "a");

    let (objects, common_prefixes) = restarted
        .list_objects(bucket.id, "docs/", "", 100, "/")
        .await
        .unwrap();
    assert_eq!(
        objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.txt"]
    );
    assert_eq!(common_prefixes, vec!["docs/nested/".to_string()]);
    assert_eq!(
        restarted
            .list_object_versions(bucket.id, "docs/", "", None, 100)
            .await
            .unwrap()
            .versions
            .len(),
        2
    );

    let replacement = restarted
        .create_object(
            1,
            bucket.id,
            "docs/a.txt",
            "hash-c",
            13,
            "etag-c",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let (objects_after_append, _) = restarted
        .list_objects(bucket.id, "docs/", "", 100, "/")
        .await
        .unwrap();
    assert_eq!(objects_after_append[0].version_id, replacement.version_id);
    assert_eq!(objects_after_append[0].content_hash, "hash-c");
    assert_eq!(
        restarted
            .list_object_versions(bucket.id, "docs/a.txt", "", None, 100)
            .await
            .unwrap()
            .versions
            .len(),
        2
    );
}

#[tokio::test]
async fn object_metadata_writes_require_rfc_ownership_fence() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();
    register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();
    let partition_id = hex::encode(metadata_journal::object_metadata_partition_id(
        tenant.id, bucket.id,
    ));
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::ObjectPartition,
        resource_id: format!(
            "tenant/{}/bucket/{}/object_metadata/{partition_id}",
            tenant.id, bucket.id
        ),
    };
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap();
    acquire_ownership(
        &persistence.storage,
        AcquireOwnership {
            request_id: "other-node-object-owner".to_string(),
            idempotency_key: "other-node-object-owner".to_string(),
            resource,
            owner: OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: "other-node".to_string(),
                actor_instance_id: "other-node".to_string(),
                display_name: "other-node".to_string(),
                region: "test-region".to_string(),
                cell: "default".to_string(),
            },
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap()
                .saturating_mul(1_000_000),
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();

    let err = persistence
        .create_object(
            tenant.id,
            bucket.id,
            "blocked.txt",
            "payload-hash",
            1,
            "etag",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("OwnershipHeld"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn persistence_schedules_deduplicated_object_metadata_compaction_tasks() {
    let temp = tempdir().unwrap();
    let config = Config {
        object_metadata_compaction_frame_threshold: 2,
        object_metadata_compaction_bytes_threshold: 0,
        ..test_config(temp.path())
    };
    let persistence = Persistence::new(&config, None).unwrap();

    persistence.create_region("local").await.unwrap();
    let bucket = persistence
        .create_bucket(1, "scheduled-compact-bucket", "local")
        .await
        .unwrap();
    persistence
        .create_object(
            1,
            bucket.id,
            "objects/a.txt",
            "hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let tasks = persistence.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].task_type,
        crate::tasks::TaskType::ObjectMetadataCompaction
    );
    assert_eq!(tasks[0].payload, json!({ "bucket_id": bucket.id }));

    persistence
        .create_object(
            1,
            bucket.id,
            "objects/b.txt",
            "hash-b",
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        persistence.list_tasks().await.unwrap().len(),
        1,
        "live compaction task should be deduplicated per bucket"
    );

    let claimed = persistence.claim_pending_tasks(1).await.unwrap();
    persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap();
    persistence
        .update_task_status(claimed[0].id, crate::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    persistence
        .create_object(
            1,
            bucket.id,
            "objects/c.txt",
            "hash-c",
            13,
            "etag-c",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        persistence.list_tasks().await.unwrap().len(),
        2,
        "new post-compaction journal frames should schedule a new task"
    );
}

#[tokio::test]
async fn persistence_task_execution_lease_targets_object_metadata_partition() {
    let temp = tempdir().unwrap();
    let config = test_config(temp.path());
    let persistence = Persistence::new(&config, None).unwrap();

    persistence.create_region("local").await.unwrap();
    let bucket = persistence
        .create_bucket(1, "lease-target-bucket", "local")
        .await
        .unwrap();
    persistence
        .create_object(
            1,
            bucket.id,
            "objects/a.txt",
            "hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let now = Utc::now();
    let task = TaskRecord {
        id: 77,
        task_type: crate::tasks::TaskType::ObjectMetadataCompaction,
        payload: json!({ "bucket_id": bucket.id }),
        priority: 0,
        status: crate::tasks::TaskStatus::Running,
        attempts: 1,
        last_error: None,
        scheduled_at: now,
        created_at: now,
        updated_at: now,
    };
    let lease = persistence
        .acquire_task_execution_lease(&task)
        .await
        .unwrap();
    assert_eq!(lease.task_id, "task-77");
    assert_eq!(lease.task_kind, "OBJECT_METADATA_COMPACTION");
    assert_eq!(lease.partition_family, "object_metadata");
    assert_eq!(
        lease.partition_id,
        hex::encode(metadata_journal::object_metadata_partition_id(1, bucket.id))
    );
    assert!(
        lease.source_cursor >= 2,
        "object PUT should create object-version and directory frames"
    );

    let read_back = persistence
        .read_task_execution_lease(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_back, lease);

    let competing_config = Config {
        public_api_addr: "other-worker-node".to_string(),
        ..config
    };
    let competing = Persistence::new(&competing_config, None).unwrap();
    let err = competing
        .acquire_task_execution_lease(&task)
        .await
        .unwrap_err();
    assert!(err.to_string().contains(task_lease::LEASE_HELD));

    let checkpointed = persistence
        .checkpoint_task_execution_lease(&lease, lease.source_cursor)
        .await
        .unwrap();
    assert_eq!(checkpointed.checkpoint_cursor, lease.source_cursor);
}

#[tokio::test]
async fn persistence_global_journal_writes_use_current_fence_tokens() {
    Box::pin(async {
        let temp = tempdir().unwrap();
        let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();

        persistence.create_region("local").await.unwrap();
        let bucket = persistence
            .create_bucket(1, "bucket-a", "local")
            .await
            .unwrap();
        let object = persistence
            .create_object(
                1,
                bucket.id,
                "objects/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        persistence
            .soft_delete_object(bucket.id, &object.key)
            .await
            .unwrap();
        let upload = persistence
            .create_multipart_upload(1, bucket.id, "objects/large.bin")
            .await
            .unwrap()
            .upload;
        persistence
            .upsert_multipart_part(upload.id, 1, payload_ref("part-hash", 12), 12, "part-etag")
            .await
            .unwrap();
        persistence
            .complete_multipart_upload(upload.id)
            .await
            .unwrap();
        let stream = persistence
            .create_append_stream(1, bucket.id, &bucket.name, "stream-a")
            .await
            .unwrap()
            .stream;
        persistence
            .append_stream_record(stream.id, payload_ref("payload-hash", 13), 13, None, None)
            .await
            .unwrap();
        persistence
            .seal_append_stream(stream.id, "segment-hash")
            .await
            .unwrap();
        persistence
            .compare_and_swap_manifest(
                1,
                bucket.id,
                &bucket.name,
                "manifest.json",
                0,
                json!({"version": 1}),
                "manifest-hash",
            )
            .await
            .unwrap();
        let index = persistence
            .create_index_definition(
                1,
                bucket.id,
                "body",
                "full_text",
                json!({"prefix": "objects/"}),
                json!({"field": "body"}),
                "inherit",
                json!({"mode": "sync"}),
            )
            .await
            .unwrap();
        persistence
            .create_index_definition_event(1, bucket.id, &bucket.name, &index, "create")
            .await
            .unwrap();
        persistence
            .create_index_diagnostic(
                1,
                bucket.id,
                &bucket.name,
                Some(index.id),
                &index.name,
                &object.key,
                Some(object.version_id),
                "warning",
                "test-warning",
                "diagnostic",
                json!({"source": "test"}),
            )
            .await
            .unwrap();
        persistence
            .write_authz_tuple(
                1,
                "object",
                &object.key,
                "reader",
                "user",
                "user-a",
                "",
                "add",
                "test",
                "test grant",
            )
            .await
            .unwrap();
        persistence
            .enqueue_task(
                crate::tasks::TaskType::DeleteBucket,
                json!({"bucket_id": 7}),
                1,
            )
            .await
            .unwrap();
        persistence
            .create_model_artifact("artifact-a", 1, "models/a", &model_manifest())
            .await
            .unwrap();
        persistence
            .hf_create_key("primary", b"secret", Some("note"))
            .await
            .unwrap();

        let control_fences =
            crate::control_journal::read_control_frame_fences_for_test(&persistence.storage)
                .await
                .unwrap();
        assert!(control_fences.iter().all(|fence| *fence > 0));
        let task_fences =
            crate::task_journal::read_task_frame_fences_for_test(&persistence.storage)
                .await
                .unwrap();
        assert!(task_fences.iter().all(|fence| *fence > 0));
        let model_fences =
            crate::model_journal::read_model_frame_fences_for_test(&persistence.storage)
                .await
                .unwrap();
        assert!(model_fences.iter().all(|fence| *fence > 0));
        let hf_fences = crate::hf_journal::read_hf_frame_fences_for_test(&persistence.storage)
            .await
            .expect("hf metadata journal fences");
        assert!(hf_fences.iter().all(|fence| *fence > 0));
        let (tenant_bucket_fences, global_bucket_fences) =
            crate::bucket_journal::read_bucket_frame_fences_for_test(&persistence.storage, 1)
                .await
                .unwrap();
        assert!(tenant_bucket_fences.iter().all(|fence| *fence > 0));
        assert!(global_bucket_fences.iter().all(|fence| *fence > 0));
        let object_fences = crate::metadata_journal::read_object_metadata_record_fences_for_test(
            &persistence.storage,
            &bucket,
        )
        .await
        .expect("object metadata journal fences");
        assert!(object_fences.iter().all(|fence| *fence > 0));
        let multipart_fences = crate::multipart_journal::read_multipart_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .expect("multipart journal fences");
        assert!(multipart_fences.iter().all(|fence| *fence > 0));
        let append_fences = crate::append_journal::read_append_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .unwrap();
        assert!(append_fences.iter().all(|fence| *fence > 0));
        let manifest_fences = crate::manifest_journal::read_manifest_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .unwrap();
        assert!(manifest_fences.iter().all(|fence| *fence > 0));
        let index_fences = crate::index_journal::read_index_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .unwrap();
        assert!(index_fences.iter().all(|fence| *fence > 0));
        let diagnostic_fences =
            crate::index_diagnostic_journal::read_index_diagnostic_frame_fences_for_test(
                &persistence.storage,
                1,
                bucket.id,
            )
            .await
            .unwrap();
        assert!(diagnostic_fences.iter().all(|fence| *fence > 0));
        let authz_fences =
            crate::authz_journal::read_authz_frame_fences_for_test(&persistence.storage, 1)
                .await
                .expect("authz tuple journal fences");
        assert!(authz_fences.iter().all(|fence| *fence > 0));
    })
    .await
}
fn payload_ref(label: &str, logical_size: u64) -> crate::core_store::CoreObjectRef {
    crate::core_store::CoreObjectRef::test_unlocated(
        format!(
            "sha256:{}",
            hex::encode(blake3::hash(label.as_bytes()).as_bytes())
        ),
        logical_size,
        format!("manifest:{label}"),
    )
}
