use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::bucket_service_server::BucketServiceServer;
use anvil::anvil_api::internal_anvil_service_server::InternalAnvilServiceServer;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::object_service_server::ObjectServiceServer;
use anvil::anvil_api::{
    self, CreateBucketRequest, GetObjectRequest, ListObjectsRequest, ObjectMetadata,
    PutObjectRequest,
};
use anvil::cluster::run_gossip;
use futures_util::StreamExt;
use libp2p::swarm::SwarmEvent;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tonic::Code;

mod common;

#[tokio::test]
async fn test_distributed_put_and_get() {
    common::with_test_dbs(|global_db_url, east_db_url, _| async move {
        let num_nodes = 6;
        let base_grpc_port = 50100;

        let cluster_state = Arc::new(RwLock::new(HashMap::new()));

        let mut states = Vec::new();
        let mut swarms = Vec::new();
        for _ in 0..num_nodes {
            let (state, swarm) =
                common::prepare_node_state(&global_db_url, &east_db_url, "US_EAST_1")
                    .await
                    .unwrap();
            let mut new_state = state.clone();
            new_state.cluster = cluster_state.clone();
            states.push(new_state);
            swarms.push(swarm);
        }

        let mut listen_addrs = Vec::new();
        for i in 0..num_nodes {
            swarms[i]
                .listen_on(format!("/ip4/127.0.0.1/tcp/{}", 60100 + i).parse().unwrap())
                .unwrap();
            let addr = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if let SwarmEvent::NewListenAddr { address, .. } =
                        swarms[i].select_next_some().await
                    {
                        break address;
                    }
                }
            })
            .await
            .expect("timed out waiting for listen address");
            listen_addrs.push(addr);
        }

        for i in 1..num_nodes {
            swarms[i].dial(listen_addrs[0].clone()).unwrap();
        }

        for i in 0..num_nodes {
            let state = states.pop().unwrap();
            let swarm = swarms.pop().unwrap();
            let grpc_addr_str = format!("127.0.0.1:{}", base_grpc_port + i);
            let grpc_addr = grpc_addr_str.parse().unwrap();
            let http_grpc_addr_str = format!("http://{}", grpc_addr_str);
            tokio::spawn(async move {
                let server = Server::builder()
                    .add_service(ObjectServiceServer::new(state.clone()))
                    .add_service(BucketServiceServer::new(state.clone()))
                    .add_service(InternalAnvilServiceServer::new(state.clone()))
                    .serve(grpc_addr);
                let gossip = run_gossip(swarm, state.cluster, http_grpc_addr_str);
                let _ =
                    tokio::try_join!(async { server.await.map_err(anyhow::Error::from) }, async {
                        gossip.await.map_err(anyhow::Error::from)
                    });
            });
        }

        loop {
            let state = cluster_state.read().await;
            if state.len() >= num_nodes {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        let mut bucket_client =
            BucketServiceClient::connect(format!("http://127.0.0.1:{}", base_grpc_port))
                .await
                .unwrap();
        let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());
        bucket_client
            .create_bucket(CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "US_EAST_1".to_string(),
            })
            .await
            .unwrap();

        let mut object_client =
            ObjectServiceClient::connect(format!("http://127.0.0.1:{}", base_grpc_port))
                .await
                .unwrap();
        let object_key = "my-distributed-object".to_string();
        let data = (0..1024 * 128).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: object_key.clone(),
        };
        let mut chunks = vec![PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
        }];
        for chunk in data.chunks(1024 * 64) {
            chunks.push(PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Chunk(chunk.to_vec())),
            });
        }

        let request_stream = tokio_stream::iter(chunks);
        let response = object_client
            .put_object(request_stream)
            .await
            .unwrap()
            .into_inner();
        let object_hash = response.etag;

        let get_request = GetObjectRequest {
            bucket_name,
            object_key,
            version_id: response.version_id,
        };
        let mut response_stream = object_client
            .get_object(get_request)
            .await
            .unwrap()
            .into_inner();

        let mut downloaded_data = Vec::new();
        if let Some(Ok(first_chunk)) = response_stream.next().await {
            if let Some(anvil_api::get_object_response::Data::Metadata(_)) = first_chunk.data {
                while let Some(Ok(chunk)) = response_stream.next().await {
                    if let Some(anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.data {
                        downloaded_data.extend_from_slice(&bytes);
                    }
                }
            }
        }

        assert_eq!(downloaded_data, data);

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let mut shards_found = 0;
        for i in 0..num_nodes {
            let shard_path = format!("anvil-data/{}-{:02}", object_hash, i);
            if Path::new(&shard_path).exists() {
                shards_found += 1;
                fs::remove_file(shard_path).await.unwrap();
            }
        }
        assert!(shards_found > 0);
    })
    .await;
}

#[tokio::test]
async fn test_single_node_put() {
    common::with_test_dbs(|global_db_url, east_db_url, _| async move {
        let (state, mut swarm) =
            common::prepare_node_state(&global_db_url, &east_db_url, "US_EAST_1")
                .await
                .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let cluster_state_clone = state.cluster.clone();
        let grpc_addr = "127.0.0.1:50200".parse().unwrap();
        let http_grpc_addr = "http://127.0.0.1:50200".to_string();

        tokio::spawn(async move {
            let server = Server::builder()
                .add_service(ObjectServiceServer::new(state.clone()))
                .add_service(BucketServiceServer::new(state.clone()))
                .add_service(InternalAnvilServiceServer::new(state.clone()))
                .serve(grpc_addr);
            let gossip = run_gossip(swarm, state.cluster, http_grpc_addr);
            let _ = tokio::try_join!(async { server.await.map_err(anyhow::Error::from) }, async {
                gossip.await.map_err(anyhow::Error::from)
            });
        });

        loop {
            if cluster_state_clone.read().await.len() >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let mut bucket_client = BucketServiceClient::connect("http://127.0.0.1:50200")
            .await
            .unwrap();
        let bucket_name = "single-node-bucket".to_string();
        bucket_client
            .create_bucket(CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "US_EAST_1".to_string(),
            })
            .await
            .unwrap();

        let mut object_client = ObjectServiceClient::connect("http://127.0.0.1:50200")
            .await
            .unwrap();
        let object_key = "single-node-object".to_string();
        let data = b"hello world".to_vec();

        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: object_key.clone(),
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
            },
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Chunk(data.clone())),
            },
        ];

        let request_stream = tokio_stream::iter(chunks);

        let result = object_client.put_object(request_stream).await;
        if let Err(e) = &result {
            eprintln!("\n[Single Node Test] 'put_object' failed with: {:?}\n", e);
        }
        assert!(result.is_ok());
    })
    .await;
}

#[tokio::test]
async fn test_multi_region_list_and_isolation() {
    common::with_test_dbs(|global_db_url, east_db_url, west_db_url| async move {
        let (mut state_east, mut swarm_east) =
            common::prepare_node_state(&global_db_url, &east_db_url, "US_EAST_1")
                .await
                .unwrap();
        let (mut state_west, mut swarm_west) =
            common::prepare_node_state(&global_db_url, &west_db_url, "EU_WEST_1")
                .await
                .unwrap();

        swarm_east
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        swarm_west
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();

        let grpc_addr_east = "127.0.0.1:50201".parse().unwrap();
        let grpc_addr_west = "127.0.0.1:50202".parse().unwrap();

        tokio::spawn(async move {
            let cluster_state = Arc::new(RwLock::new(HashMap::new()));
            state_east.cluster = cluster_state.clone();
            let server = Server::builder()
                .add_service(ObjectServiceServer::new(state_east.clone()))
                .add_service(BucketServiceServer::new(state_east.clone()))
                .add_service(InternalAnvilServiceServer::new(state_east.clone()))
                .serve(grpc_addr_east);
            let gossip = run_gossip(
                swarm_east,
                cluster_state,
                "http://127.0.0.1:50201".to_string(),
            );
            let _ = tokio::try_join!(async { server.await.map_err(anyhow::Error::from) }, async {
                gossip.await.map_err(anyhow::Error::from)
            });
        });

        tokio::spawn(async move {
            let cluster_state = Arc::new(RwLock::new(HashMap::new()));
            state_west.cluster = cluster_state.clone();
            let server = Server::builder()
                .add_service(ObjectServiceServer::new(state_west.clone()))
                .add_service(BucketServiceServer::new(state_west.clone()))
                .add_service(InternalAnvilServiceServer::new(state_west.clone()))
                .serve(grpc_addr_west);
            let gossip = run_gossip(
                swarm_west,
                cluster_state,
                "http://127.0.0.1:50202".to_string(),
            );
            let _ = tokio::try_join!(async { server.await.map_err(anyhow::Error::from) }, async {
                gossip.await.map_err(anyhow::Error::from)
            });
        });

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let mut bucket_client_east = BucketServiceClient::connect("http://127.0.0.1:50201")
            .await
            .unwrap();
        let mut object_client_east = ObjectServiceClient::connect("http://127.0.0.1:50201")
            .await
            .unwrap();

        let bucket_name = "regional-bucket".to_string();
        bucket_client_east
            .create_bucket(CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "US_EAST_1".to_string(),
            })
            .await
            .unwrap();

        let object_key = "regional-object".to_string();
        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: object_key.clone(),
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
            },
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Chunk(
                    b"regional data".to_vec(),
                )),
            },
        ];
        object_client_east
            .put_object(tokio_stream::iter(chunks))
            .await
            .unwrap();

        // Add a small delay to allow the database transaction to become visible
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // 4. ListObjects (Success Case): List from us-east-1 and verify the object is there.
        let list_req_east = ListObjectsRequest {
            bucket_name: bucket_name.clone(),
            ..Default::default()
        };
        let list_resp_east = object_client_east
            .list_objects(list_req_east)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(list_resp_east.objects.len(), 1);
        assert_eq!(list_resp_east.objects[0].key, object_key);

        let mut object_client_west = ObjectServiceClient::connect("http://127.0.0.1:50202")
            .await
            .unwrap();
        let list_req_west = ListObjectsRequest {
            bucket_name: bucket_name.clone(),
            ..Default::default()
        };
        let list_resp_west = object_client_west.list_objects(list_req_west).await;

        assert!(list_resp_west.is_err());
        assert_eq!(list_resp_west.unwrap_err().code(), Code::NotFound);
    })
    .await;
}
