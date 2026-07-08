use super::*;
use serde::Serialize;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[test]
fn node_state_machine_rejects_invalid_transitions() {
    validate_node_transition(LifecycleState::Joining, LifecycleState::Active).unwrap();
    assert!(validate_node_transition(LifecycleState::Joining, LifecycleState::Draining).is_err());
    assert!(validate_node_transition(LifecycleState::Active, LifecycleState::Removed).is_err());
    assert!(validate_node_transition(LifecycleState::Removed, LifecycleState::Active).is_err());
}

#[test]
fn region_state_machine_rejects_invalid_transitions() {
    validate_region_transition(LifecycleState::Joining, LifecycleState::Active).unwrap();
    validate_region_transition(LifecycleState::Active, LifecycleState::ReadOnly).unwrap();
    assert!(validate_region_transition(LifecycleState::Joining, LifecycleState::Draining).is_err());
    assert!(validate_region_transition(LifecycleState::Active, LifecycleState::Removed).is_err());
}

#[tokio::test]
async fn activate_region_rejects_missing_activation_checkpoint_stream() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    let checkpoint = checkpoint_with_stream("bucket_locator", "0a7f", 1, digest_for(b"record"));

    let err = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LifecycleError::ActivationCheckpointNotReached { .. }
    ));
    assert_eq!(
        read_state(&storage)
            .await
            .unwrap()
            .regions
            .get("eu-west-1")
            .unwrap()
            .state,
        LifecycleState::Joining
    );
}

#[tokio::test]
async fn activate_region_rejects_mismatched_activation_checkpoint_digest() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    append_control_record(&storage, "bucket_locator", "0a7f", 1, digest_for(b"actual")).await;
    write_control_checkpoint_record(
        &storage,
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        1,
        digest_for(b"actual"),
    )
    .await;
    let checkpoint = checkpoint_with_stream("bucket_locator", "0a7f", 1, digest_for(b"expected"));

    let err = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LifecycleError::ActivationCheckpointNotReached { .. }
    ));
}

#[tokio::test]
async fn activate_region_rejects_checkpoint_that_omits_existing_control_stream() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    append_control_record(&storage, "bucket_locator", "0a7f", 1, digest_for(b"record")).await;
    let checkpoint = ActivationCheckpoint {
        schema: ACTIVATION_CHECKPOINT_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        required_streams: vec![],
    };

    let err = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LifecycleError::ActivationCheckpointNotReached {
            stream_family,
            partition,
            ..
        } if stream_family == "bucket_locator" && partition == "0a7f"
    ));
}

#[tokio::test]
async fn activate_region_accepts_reached_activation_checkpoint() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    let cell = register_test_cell(&storage).await;
    let _cell = transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let node = register_test_node(&storage).await;
    let _node = transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    let digest = digest_for(b"record");
    append_control_record(&storage, "bucket_locator", "0a7f", 1, digest.clone()).await;
    write_control_checkpoint_record(
        &storage,
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        1,
        digest.clone(),
    )
    .await;
    let checkpoint = checkpoint_with_stream("bucket_locator", "0a7f", 1, digest);

    let active = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap();
    assert_eq!(active.state, LifecycleState::Active);
}

#[tokio::test]
async fn region_activation_requires_active_cell_and_node() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let region = create_test_region(&storage).await;
    let checkpoint = ActivationCheckpoint {
        schema: ACTIVATION_CHECKPOINT_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        required_streams: vec![],
    };

    let missing_cell = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(
        matches!(missing_cell, LifecycleError::InvalidArgument(message) if message.contains("active cell"))
    );

    let cell = register_test_cell(&storage).await;
    transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let missing_node = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap_err();
    assert!(
        matches!(missing_node, LifecycleError::InvalidArgument(message) if message.contains("active node"))
    );

    let node = register_test_node(&storage).await;
    transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    let active = activate_region(&storage, "eu-west-1", region.generation, &checkpoint)
        .await
        .unwrap();
    assert_eq!(active.state, LifecycleState::Active);
}

#[tokio::test]
async fn writable_placement_rejects_non_active_region() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    assert!(
        ensure_region_accepts_new_writes(&storage, "legacy-region")
            .await
            .is_ok()
    );
    create_test_region(&storage).await;

    assert!(
        ensure_region_accepts_new_writes(&storage, "eu-west-1")
            .await
            .is_err()
    );
    assert!(
        ensure_region_accepts_new_writes(&storage, "legacy-region")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn writable_placement_rejects_stale_or_inactive_region_cell_and_node() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let (region, _cell, _node) = create_active_placement_model(&storage).await;

    ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
        .await
        .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "us-east-1", "cell-a", "node-a")
            .await
            .is_err()
    );
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-b", "node-a")
            .await
            .is_err()
    );
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-b")
            .await
            .is_err()
    );

    transition_region(
        &storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Draining,
    )
    .await
    .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
            .await
            .is_err()
    );

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let (_region, cell, _node) = create_active_placement_model(&storage).await;
    transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Draining,
    )
    .await
    .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
            .await
            .is_err()
    );

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let (_region, _cell, node) = create_active_placement_model(&storage).await;
    transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Draining,
        Some(NodeDrainDescriptor {
            started_at: timestamp_now(),
            graceful_timeout_ms: 1000,
            force_after_timeout: false,
        }),
    )
    .await
    .unwrap();
    assert!(
        ensure_new_writable_placement(&storage, "eu-west-1", "cell-a", "node-a")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lifecycle_store_persists_descriptors_and_enforces_transitions() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    let region = create_region(
        &storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("cell-a".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(region.state, LifecycleState::Joining);

    let cell = register_cell(
        &storage,
        RegisterCellDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            placement_weight: 100,
        },
    )
    .await
    .unwrap();
    let cell = transition_cell(
        &storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    assert_eq!(cell.state, LifecycleState::Active);

    let region = transition_region(
        &storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    assert_eq!(region.state, LifecycleState::Active);

    let node = register_node(
        &storage,
        RegisterNodeDescriptor {
            mesh_id: "mesh-a".to_string(),
            node_id: "node-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            libp2p_peer_id: "peer-a".to_string(),
            public_api_addr: "http://127.0.0.1:50051".to_string(),
            public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
            capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
        },
    )
    .await
    .unwrap();
    assert!(
        transition_node(
            &storage,
            "node-a",
            node.generation,
            LifecycleState::Draining,
            None,
        )
        .await
        .is_err()
    );

    let node = transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    assert!(
        transition_node(
            &storage,
            "node-a",
            node.generation,
            LifecycleState::Removed,
            None,
        )
        .await
        .is_err()
    );
    let node = transition_node(
        &storage,
        "node-a",
        node.generation,
        LifecycleState::Draining,
        Some(NodeDrainDescriptor {
            started_at: timestamp_now(),
            graceful_timeout_ms: 1000,
            force_after_timeout: false,
        }),
    )
    .await
    .unwrap();
    assert_eq!(node.state, LifecycleState::Draining);

    let replayed = read_state(&storage).await.unwrap();
    assert_eq!(replayed.nodes["node-a"].state, LifecycleState::Draining);
}

#[tokio::test]
async fn lifecycle_read_model_replays_control_streams_as_source_of_truth() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    let stale_region = RegionDescriptor {
        schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        state: LifecycleState::Joining,
        public_base_url: "https://stale.example.test".to_string(),
        virtual_host_suffix: "stale.example.test".to_string(),
        placement_weight: 1,
        default_cell: None,
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:00:00Z".to_string(),
        generation: 1,
    };
    write_state(
        &storage,
        &MeshLifecycleState {
            regions: BTreeMap::from([("eu-west-1".to_string(), stale_region)]),
            ..MeshLifecycleState::default()
        },
    )
    .await
    .unwrap();

    let region = RegionDescriptor {
        schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        state: LifecycleState::Active,
        public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
        virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
        placement_weight: 100,
        default_cell: Some("cell-a".to_string()),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:01:00Z".to_string(),
        generation: 2,
    };
    append_lifecycle_descriptor(
        &storage,
        REGION_DESCRIPTOR_STREAM_FAMILY,
        "eu-west-1",
        1,
        &region,
    )
    .await;

    let cell = CellDescriptor {
        schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        cell_id: "cell-a".to_string(),
        state: LifecycleState::Active,
        placement_weight: 100,
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:01:00Z".to_string(),
        generation: 2,
    };
    append_lifecycle_descriptor(
        &storage,
        CELL_DESCRIPTOR_STREAM_FAMILY,
        "eu-west-1/cell-a",
        1,
        &cell,
    )
    .await;

    let node = NodeDescriptor {
        schema: NODE_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        node_id: "node-a".to_string(),
        region: "eu-west-1".to_string(),
        cell_id: "cell-a".to_string(),
        libp2p_peer_id: "peer-a".to_string(),
        public_api_addr: "http://127.0.0.1:50051".to_string(),
        public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
        capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
        state: LifecycleState::Active,
        drain: None,
        last_heartbeat_at: Some("2026-07-02T00:01:00Z".to_string()),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        updated_at: "2026-07-02T00:01:00Z".to_string(),
        generation: 2,
    };
    append_lifecycle_descriptor(
        &storage,
        NODE_DESCRIPTOR_STREAM_FAMILY,
        "eu-west-1/cell-a/node-a",
        1,
        &node,
    )
    .await;

    let replayed = read_state(&storage).await.unwrap();
    assert_eq!(
        replayed.regions["eu-west-1"].public_base_url,
        "https://eu-west-1.anvil-storage.test"
    );
    assert_eq!(replayed.regions["eu-west-1"].state, LifecycleState::Active);
    assert_eq!(
        replayed.cells["eu-west-1/cell-a"].state,
        LifecycleState::Active
    );
    assert_eq!(replayed.nodes["node-a"].state, LifecycleState::Active);

    delete_lifecycle_state_projection(&storage).await.unwrap();
    let replayed_without_projection = read_state(&storage).await.unwrap();
    assert_eq!(
        replayed_without_projection.regions["eu-west-1"].generation,
        2
    );
    assert_eq!(
        replayed_without_projection.cells["eu-west-1/cell-a"].generation,
        2
    );
    assert_eq!(replayed_without_projection.nodes["node-a"].generation, 2);
}

async fn create_test_region(storage: &Storage) -> RegionDescriptor {
    create_region(
        storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: None,
        },
    )
    .await
    .unwrap()
}

async fn register_test_cell(storage: &Storage) -> CellDescriptor {
    register_cell(
        storage,
        RegisterCellDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            placement_weight: 100,
        },
    )
    .await
    .unwrap()
}

async fn register_test_node(storage: &Storage) -> NodeDescriptor {
    register_node(
        storage,
        RegisterNodeDescriptor {
            mesh_id: "mesh-a".to_string(),
            node_id: "node-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            libp2p_peer_id: "peer-a".to_string(),
            public_api_addr: "http://127.0.0.1:50051".to_string(),
            public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
            capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
        },
    )
    .await
    .unwrap()
}

async fn create_active_placement_model(
    storage: &Storage,
) -> (RegionDescriptor, CellDescriptor, NodeDescriptor) {
    let region = create_region(
        storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("cell-a".to_string()),
        },
    )
    .await
    .unwrap();
    let cell = register_cell(
        storage,
        RegisterCellDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            placement_weight: 100,
        },
    )
    .await
    .unwrap();
    let cell = transition_cell(
        storage,
        "eu-west-1",
        "cell-a",
        cell.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let region = transition_region(
        storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();
    let node = register_node(
        storage,
        RegisterNodeDescriptor {
            mesh_id: "mesh-a".to_string(),
            node_id: "node-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            libp2p_peer_id: "peer-a".to_string(),
            public_api_addr: "http://127.0.0.1:50051".to_string(),
            public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
            capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
        },
    )
    .await
    .unwrap();
    let node = transition_node(
        storage,
        "node-a",
        node.generation,
        LifecycleState::Active,
        None,
    )
    .await
    .unwrap();
    (region, cell, node)
}

fn checkpoint_with_stream(
    stream_family: &str,
    partition: &str,
    sequence: u64,
    digest: ControlRecordDigest,
) -> ActivationCheckpoint {
    ActivationCheckpoint {
        schema: ACTIVATION_CHECKPOINT_SCHEMA.to_string(),
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        created_at: "2026-07-02T00:00:00Z".to_string(),
        required_streams: vec![ActivationCheckpointStream {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            sequence: ControlStreamSequence::new(sequence).unwrap(),
            digest,
        }],
    }
}

fn digest_for(bytes: &[u8]) -> ControlRecordDigest {
    ControlRecordDigest::blake3(bytes)
}

async fn append_control_record(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    sequence: u64,
    digest: ControlRecordDigest,
) {
    let header_json = serde_json::json!({
        "schema": "anvil.mesh.control_mutation.v1",
        "mesh_id": "mesh-a",
        "stream_family": stream_family,
        "partition": partition,
        "sequence": sequence,
        "record_key": "tenant_acme/releases",
        "operation": "upsert",
        "expected_generation": 1,
        "new_generation": 2,
        "writer_node_id": "node-a",
        "writer_fence": 1,
        "idempotency_key": "idem-a",
        "record_digest": digest.as_str(),
        "created_at": "2026-07-02T00:00:00Z"
    })
    .to_string()
    .into_bytes();
    crate::mesh_control_stream::append_control_stream_frame(
        storage,
        stream_family,
        partition,
        &crate::mesh_control_stream::ControlStreamFrame::new(
            header_json,
            br#"{"ok":true}"#.to_vec(),
        ),
        None,
    )
    .await
    .unwrap();
}

async fn write_control_checkpoint_record(
    storage: &Storage,
    region: &str,
    stream_family: &str,
    partition: &str,
    sequence: u64,
    digest: ControlRecordDigest,
) {
    crate::mesh_control_stream::write_control_checkpoint(
        storage,
        &crate::mesh_control_stream::ControlCheckpointRecord::new(
            "mesh-a",
            region,
            stream_family,
            partition,
            ControlStreamSequence::new(sequence).unwrap(),
            digest,
            "2026-07-02T00:00:00Z",
        ),
    )
    .await
    .unwrap();
}

async fn append_lifecycle_descriptor<T: Serialize>(
    storage: &Storage,
    stream_family: &str,
    record_key: &str,
    sequence: u64,
    descriptor: &T,
) {
    let partition = lifecycle_control_partition(stream_family, record_key);
    let payload_json = serde_json::to_vec(descriptor).unwrap();
    let digest = ControlRecordDigest::blake3(&payload_json);
    let header_json = serde_json::json!({
        "schema": "anvil.mesh.control_mutation.v1",
        "mesh_id": "mesh-a",
        "stream_family": stream_family,
        "partition": partition,
        "sequence": sequence,
        "record_key": record_key,
        "operation": "upsert",
        "expected_generation": sequence.saturating_sub(1),
        "new_generation": sequence,
        "writer_node_id": "node-a",
        "writer_fence": 1,
        "idempotency_key": "idem-a",
        "record_digest": digest.as_str(),
        "created_at": "2026-07-02T00:00:00Z"
    })
    .to_string()
    .into_bytes();
    crate::mesh_control_stream::append_control_stream_frame(
        storage,
        stream_family,
        &partition,
        &crate::mesh_control_stream::ControlStreamFrame::new(header_json, payload_json),
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn host_aliases_are_generation_checked_and_region_bound() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let routing_config = RoutingConfig::new("anvil-storage.com").unwrap();

    let region = create_region(
        &storage,
        CreateRegionDescriptor {
            mesh_id: "mesh-a".to_string(),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.com".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.com".to_string(),
            placement_weight: 100,
            default_cell: None,
        },
    )
    .await
    .unwrap();
    transition_region(
        &storage,
        "eu-west-1",
        region.generation,
        LifecycleState::Active,
    )
    .await
    .unwrap();

    let alias = create_host_alias(
        &storage,
        &routing_config,
        CreateHostAliasDescriptor {
            hostname: "CDN.Example.Com.".to_string(),
            tenant_id: "tenant-acme".to_string(),
            bucket_name: "releases".to_string(),
            region: "eu-west-1".to_string(),
            prefix: "public/".to_string(),
        },
    )
    .await
    .unwrap();

    assert_eq!(alias.hostname, "cdn.example.com");
    assert_eq!(alias.state, HostAliasState::PendingVerification);
    let stale = transition_host_alias(&storage, "cdn.example.com", 99, HostAliasState::Active)
        .await
        .unwrap_err();
    assert!(matches!(stale, LifecycleError::GenerationConflict { .. }));

    let active = transition_host_alias(
        &storage,
        "cdn.example.com",
        alias.generation,
        HostAliasState::Active,
    )
    .await
    .unwrap();
    assert_eq!(active.state, HostAliasState::Active);
    assert_eq!(active.generation, 2);

    let aliases = list_host_aliases(&storage, Some("eu-west-1"))
        .await
        .unwrap();
    assert_eq!(aliases.len(), 1);
    assert_eq!(aliases[0].hostname, "cdn.example.com");
}
