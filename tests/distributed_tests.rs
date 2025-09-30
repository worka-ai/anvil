use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{GetObjectRequest, ObjectMetadata, PutObjectRequest};
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::timeout;
use tonic::Request;

mod common;

#[tokio::test]
async fn test_distributed_reconstruction_on_node_failure() {
    common::with_test_dbs(|_global_db_url, _regional_db_url, _| async move {
        let num_nodes = 6;
        let mut cluster = common::TestCluster::new(num_nodes).await;

        let primary_addr = cluster.grpc_addrs[0].clone();
        //let bucket_client = BucketServiceClient::connect(primary_addr.clone()).await.unwrap();
        let mut object_client = ObjectServiceClient::connect(primary_addr.clone())
            .await
            .unwrap();

        let bucket_name = "reconstruction-bucket".to_string();
        common::create_test_bucket(&primary_addr, &bucket_name, &cluster.token).await;

        let object_key = "reconstruction-object".to_string();
        let content = (0..1024 * 256).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        // 1. Put an object, which will be distributed
        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: object_key.clone(),
        };
        let mut chunks = vec![PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        }];
        for chunk in content.chunks(1024 * 64) {
            chunks.push(PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    chunk.to_vec(),
                )),
            });
        }
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", cluster.token).parse().unwrap(),
        );
        object_client.put_object(put_req).await.unwrap();

        // 2. Stop one of the nodes
        cluster.nodes.remove(1).abort();
        // Wait a moment for the node to die and for gossip to propagate
        tokio::time::sleep(Duration::from_secs(5)).await;

        // 3. Connect to a *different*, live node
        let mut recovery_client = ObjectServiceClient::connect(cluster.grpc_addrs[2].clone())
            .await
            .unwrap();

        // 4. Get the object and verify its content
        let mut get_req = Request::new(GetObjectRequest {
            bucket_name: bucket_name.clone(),
            object_key: object_key.clone(),
            version_id: None,
        });
        get_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", cluster.token).parse().unwrap(),
        );

        let mut stream = timeout(Duration::from_secs(10), recovery_client.get_object(get_req))
            .await
            .expect("get_object timed out")
            .unwrap()
            .into_inner();

        let mut downloaded_data = Vec::new();
        if let Some(Ok(first_chunk)) = stream.next().await {
            if let Some(anvil::anvil_api::get_object_response::Data::Metadata(_)) = first_chunk.data
            {
                while let Some(Ok(chunk)) = stream.next().await {
                    if let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) =
                        chunk.data
                    {
                        downloaded_data.extend_from_slice(&bytes);
                    }
                }
            }
        }

        assert_eq!(
            downloaded_data, content,
            "Reconstructed data did not match original data"
        );
    })
    .await;
}
