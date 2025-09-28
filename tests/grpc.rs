use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::bucket_service_server::BucketServiceServer;
use anvil::anvil_api::internal_anvil_service_server::InternalAnvilServiceServer;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::object_service_server::ObjectServiceServer;
use anvil::anvil_api::{
    self, CreateBucketRequest, GetObjectRequest, ObjectMetadata, PutObjectRequest,
};
use anvil::cluster::{create_swarm, run_gossip};
use anvil::{
    create_pool, run_migrations, AppState, migrations, regional_migrations
};
use futures_util::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::Swarm;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tonic::transport::Server;

async fn setup_node() -> (AppState, Swarm<anvil::cluster::ClusterBehaviour>) {
    dotenvy::dotenv().ok();

    let global_db_url = std::env::var("GLOBAL_DATABASE_URL").unwrap();
    let regional_db_url = std::env::var("DATABASE_URL_REGION_US_EAST_1").unwrap();

    let global_pool = create_pool(&global_db_url).unwrap();
    let regional_pool = create_pool(&regional_db_url).unwrap();

    run_migrations(&global_db_url, migrations::migrations::runner(), "refinery_schema_history_global").await.unwrap();
    run_migrations(&regional_db_url, regional_migrations::migrations::runner(), "refinery_schema_history_regional").await.unwrap();

    let client = global_pool.get().await.unwrap();
    client.execute("INSERT INTO tenants (id, name, api_key) VALUES (1, 'default', 'default-key') ON CONFLICT (id) DO NOTHING", &[]).await.unwrap();
    client.execute("INSERT INTO regions (name) VALUES ('US_EAST_1') ON CONFLICT (name) DO NOTHING", &[]).await.unwrap();

    let storage = anvil::storage::Storage::new().await.unwrap();
    let cluster_state = Arc::new(RwLock::new(HashMap::new()));
    let swarm = create_swarm().await.unwrap();

    let state = AppState {
        db: anvil::persistence::Persistence::new(global_pool, regional_pool),
        storage,
        cluster: cluster_state.clone(),
        sharder: anvil::sharding::ShardManager::new(),
        placer: anvil::placement::PlacementManager::default(),
    };

    (state, swarm)
}

#[tokio::test]
async fn test_distributed_put_and_get() {
    let num_nodes = 6;
    let base_grpc_port = 50100;

    let cluster_state: anvil::cluster::ClusterState = Arc::new(RwLock::new(HashMap::new()));

    let mut states = Vec::new();
    let mut swarms = Vec::new();
    for _ in 0..num_nodes {
        let (mut state, swarm) = setup_node().await;
        state.cluster = cluster_state.clone();
        states.push(state);
        swarms.push(swarm);
    }

    let mut listen_addrs = Vec::new();
    for i in 0..num_nodes {
        swarms[i].listen_on(format!("/ip4/127.0.0.1/tcp/{}", 60100 + i).parse().unwrap()).unwrap();
        let addr = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let SwarmEvent::NewListenAddr { address, .. } = swarms[i].select_next_some().await {
                    break address;
                }
            }
        }).await.expect("timed out waiting for listen address");
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
            let _ = tokio::try_join!(async { server.await.map_err(anyhow::Error::from) }, async { gossip.await.map_err(anyhow::Error::from) });
        });
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await; // Brief wait for listeners to bind

    // Wait for the cluster to form by checking the state
    loop {
        let state = cluster_state.read().await;
        if state.len() >= num_nodes {
            // Even if we see all nodes, gossip takes a moment to establish
            // A short sleep here is a pragmatic solution for the test
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    let mut bucket_client = BucketServiceClient::connect(format!("http://127.0.0.1:{}", base_grpc_port)).await.unwrap();
    let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());
    bucket_client.create_bucket(CreateBucketRequest { bucket_name: bucket_name.clone(), region: "US_EAST_1".to_string() }).await.unwrap();

    let mut object_client = ObjectServiceClient::connect(format!("http://127.0.0.1:{}", base_grpc_port)).await.unwrap();
    let object_key = "my-distributed-object".to_string();
    let data = (0..1024 * 128).map(|i| (i % 256) as u8).collect::<Vec<_>>();

    let metadata = ObjectMetadata { bucket_name: bucket_name.clone(), object_key: object_key.clone() };
    let mut chunks = vec![PutObjectRequest { data: Some(anvil_api::put_object_request::Data::Metadata(metadata)) }];
    for chunk in data.chunks(1024 * 64) {
        chunks.push(PutObjectRequest { data: Some(anvil_api::put_object_request::Data::Chunk(chunk.to_vec())) });
    }

    let request_stream = tokio_stream::iter(chunks);
    let response = object_client.put_object(request_stream).await.unwrap().into_inner();
    let object_hash = response.etag;

    let get_request = GetObjectRequest { bucket_name, object_key, version_id: response.version_id };
    let mut response_stream = object_client.get_object(get_request).await.unwrap().into_inner();

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
}
