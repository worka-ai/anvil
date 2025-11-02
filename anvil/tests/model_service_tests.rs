use anvil::anvil_api::model_service_client::ModelServiceClient;
use anvil::anvil_api::{
    ModelManifest, PutModelManifestRequest, TensorIndexRow,
};
use tonic::Request;
use anvil_test_utils::*;
use futures_util::StreamExt;

#[tokio::test]
async fn test_put_model_manifest_success() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(std::time::Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut model_client = ModelServiceClient::connect(grpc_addr.clone()).await.unwrap();

    // Create a bucket first
    let bucket_name = "test-model-bucket".to_string();
    cluster.create_bucket(&bucket_name, "test-region-1").await;

    let manifest = ModelManifest {
        schema_version: "1.0".to_string(),
        artifact_id: "test_artifact_123".to_string(),
        name: "test-model".to_string(),
        format: "safetensors".to_string(),
        components: vec![],
        base_artifact_id: "".to_string(),
        delta_artifact_ids: vec![],
        signatures: vec![],
        merkle_root: "".to_string(),
        meta: Default::default(),
    };

    let tensors = vec![TensorIndexRow {
        tensor_name: "layer.0.weight".to_string(),
        file_path: "model.safetensors".to_string(),
        file_offset: 1024,
        byte_length: 4096,
        dtype: 1, // F16
        shape: vec![128, 128],
        layout: "rowmajor".to_string(),
        block_bytes: 0,
        blocks: "{}".as_bytes().to_vec(),
    }];

    let mut req = Request::new(PutModelManifestRequest {
                scope: Some(anvil::anvil_api::TenantScope { tenant_id: "1".to_string(), region: "test-region-1".to_string() }),
        object: Some(anvil::anvil_api::ObjectRef {
            bucket: bucket_name.clone(),
            key: "model.safetensors".to_string(),
            version_id: "".to_string(),
        }),
        manifest: Some(manifest),
        index: tensors,
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );

    let response = model_client.put_model_manifest(req).await.unwrap();
    let inner = response.into_inner();

    assert_eq!(inner.artifact_id, "test_artifact_123");
}

#[tokio::test]
async fn test_list_tensors_success() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(std::time::Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut model_client = ModelServiceClient::connect(grpc_addr.clone()).await.unwrap();

    let bucket_name = "test-list-tensors-bucket".to_string();
    cluster.create_bucket(&bucket_name, "test-region-1").await;

    let manifest = ModelManifest {
        artifact_id: "test_artifact_456".to_string(),
        ..Default::default()
    };

    let tensors = vec![TensorIndexRow {
        tensor_name: "layer.0.weight".to_string(),
        file_path: "model.safetensors".to_string(),
        ..Default::default()
    }];

    let mut put_req = Request::new(PutModelManifestRequest {
        scope: Some(anvil::anvil_api::TenantScope { tenant_id: "1".to_string(), region: "test-region-1".to_string() }),
        object: Some(anvil::anvil_api::ObjectRef {
            bucket: bucket_name.clone(),
            key: "model.safetensors".to_string(),
            version_id: "".to_string(),
        }),
        manifest: Some(manifest),
        index: tensors.clone(),
    });
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    model_client.put_model_manifest(put_req).await.unwrap();

    let mut list_req = Request::new(anvil::anvil_api::ListTensorsRequest {
        scope: Some(anvil::anvil_api::TenantScope { tenant_id: "1".to_string(), region: "test-region-1".to_string() }),
        object: Some(anvil::anvil_api::ObjectRef {
            bucket: bucket_name.clone(),
            key: "model.safetensors".to_string(),
            version_id: "".to_string(),
        }),
        artifact_id: "test_artifact_456".to_string(),
        prefix: "".to_string(),
        limit: 0,
        page_token: "".to_string(),
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );

    let response = model_client.list_tensors(list_req).await.unwrap();
    let inner = response.into_inner();

    assert_eq!(inner.tensors.len(), 1);
    assert_eq!(inner.tensors[0].tensor_name, "layer.0.weight");
}

#[tokio::test]
async fn test_get_tensor_success() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(std::time::Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut model_client = ModelServiceClient::connect(grpc_addr.clone()).await.unwrap();

    let bucket_name = "test-get-tensor-bucket".to_string();
    cluster.create_bucket(&bucket_name, "test-region-1").await;

    // In a real scenario, we would upload an object first. For this test, we'll
    // just create the metadata and then manually place a file where the storage
    // layer expects it.
    let content = b"some tensor data";
    let content_hash = blake3::hash(content).to_hex().to_string();
    let storage = &cluster.states[0].storage;
    storage.commit_whole_object_from_bytes(content, &content_hash).await.unwrap();

    let manifest = ModelManifest {
        artifact_id: "test_artifact_789".to_string(),
        ..Default::default()
    };

    let tensors = vec![TensorIndexRow {
        tensor_name: "layer.1.bias".to_string(),
        file_path: content_hash.clone(), // In our test, the file_path is the content hash
        file_offset: 0,
        byte_length: content.len() as u64,
        ..Default::default()
    }];

    let mut put_req = Request::new(PutModelManifestRequest {
        scope: Some(anvil::anvil_api::TenantScope { tenant_id: "1".to_string(), region: "test-region-1".to_string() }),
        object: Some(anvil::anvil_api::ObjectRef {
            bucket: bucket_name.clone(),
            key: "model.safetensors".to_string(),
            version_id: "".to_string(),
        }),
        manifest: Some(manifest),
        index: tensors.clone(),
    });
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    model_client.put_model_manifest(put_req).await.unwrap();

    let mut get_req = Request::new(anvil::anvil_api::GetTensorRequest {
        artifact_id: "test_artifact_789".to_string(),
        scope: Some(anvil::anvil_api::TenantScope { tenant_id: "1".to_string(), region: "test-region-1".to_string() }),
        object: Some(anvil::anvil_api::ObjectRef {
            bucket: bucket_name.clone(),
            key: "model.safetensors".to_string(),
            version_id: "".to_string(),
        }),
        tensor_name: "layer.1.bias".to_string(),
        slice_begin: vec![],
        slice_extent: vec![],
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );

    let mut stream = model_client.get_tensor(get_req).await.unwrap().into_inner();

    let mut received_data = Vec::new();
    while let Some(chunk) = stream.next().await {
        received_data.extend_from_slice(&chunk.unwrap().data);
    }

    assert_eq!(received_data, content);
}

#[tokio::test]
async fn test_get_tensor_with_delta_manifest() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(std::time::Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut model_client = ModelServiceClient::connect(grpc_addr.clone()).await.unwrap();

    let bucket_name = "test-delta-bucket".to_string();
    cluster.create_bucket(&bucket_name, "test-region-1").await;

    // --- 1. Create and store the BASE model --- //
    let base_tensor_content = b"base_tensor_data";
    let base_tensor_hash = blake3::hash(base_tensor_content).to_hex().to_string();
    let storage = &cluster.states[0].storage;
    storage.commit_whole_object_from_bytes(base_tensor_content, &base_tensor_hash).await.unwrap();

    let base_manifest = ModelManifest {
        artifact_id: "base_model_v1".to_string(),
        ..Default::default()
    };
    let base_tensors = vec![TensorIndexRow {
        tensor_name: "base.layer.weight".to_string(),
        file_path: base_tensor_hash.clone(),
        file_offset: 0,
        byte_length: base_tensor_content.len() as u64,
        ..Default::default()
    }];

    let mut put_base_req = Request::new(PutModelManifestRequest {
        scope: Some(anvil::anvil_api::TenantScope { tenant_id: "1".to_string(), region: "test-region-1".to_string() }),
        object: Some(anvil::anvil_api::ObjectRef { bucket: bucket_name.clone(), key: "base_model".to_string(), ..Default::default() }),
        manifest: Some(base_manifest),
        index: base_tensors,
    });
    put_base_req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    model_client.put_model_manifest(put_base_req).await.unwrap();

    // --- 2. Create and store the DELTA model, referencing the base --- //
    let delta_manifest = ModelManifest {
        artifact_id: "delta_model_v1_ft".to_string(),
        base_artifact_id: "base_model_v1".to_string(), // Reference the base model
        ..Default::default()
    };

    let mut put_delta_req = Request::new(PutModelManifestRequest {
        scope: Some(anvil::anvil_api::TenantScope { tenant_id: "1".to_string(), region: "test-region-1".to_string() }),
        object: Some(anvil::anvil_api::ObjectRef { bucket: bucket_name.clone(), key: "delta_model".to_string(), ..Default::default() }),
        manifest: Some(delta_manifest),
        index: vec![], // The delta model contains no new tensors itself
    });
    put_delta_req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    model_client.put_model_manifest(put_delta_req).await.unwrap();

    // --- 3. Request a tensor that ONLY exists in the base model, but via the delta model's ID --- //
    let mut get_req = Request::new(anvil::anvil_api::GetTensorRequest {
        artifact_id: "delta_model_v1_ft".to_string(), // Requesting via the DELTA model
        tensor_name: "base.layer.weight".to_string(), // But asking for a BASE tensor
        ..Default::default()
    });
    get_req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());

    let mut stream = model_client.get_tensor(get_req).await.unwrap().into_inner();

    let mut received_data = Vec::new();
    while let Some(chunk) = stream.next().await {
        received_data.extend_from_slice(&chunk.unwrap().data);
    }

    assert_eq!(received_data, base_tensor_content);
}
