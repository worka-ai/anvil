use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::bucket_service_server::BucketServiceServer;
use anvil::anvil_api::internal_anvil_service_server::InternalAnvilServiceServer;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::object_service_server::ObjectServiceServer;
use anvil::anvil_api::{
    self, CreateBucketRequest, ObjectMetadata, PutObjectRequest,
};
use anvil::cluster::{create_swarm, run_gossip};
use anvil::AppState;
use futures_util::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::Swarm;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::info;

async fn setup_node() -> (AppState, Swarm<anvil::cluster::ClusterBehaviour>) {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let pool = sqlx::PgPool::connect(&db_url).await.unwrap();
    sqlx::migrate!().run(&pool).await.unwrap();
    sqlx::query("INSERT INTO tenants (id, name, api_key) VALUES (1, 'default', 'default-key') ON CONFLICT (id) DO NOTHING").execute(&pool).await.unwrap();

    let storage = anvil::storage::Storage::new().await.unwrap();
    let cluster_state = Arc::new(RwLock::new(HashMap::new()));
    let swarm = create_swarm().await.unwrap();

    let state = AppState {
        db: anvil::persistence::Persistence::new(pool),
        storage,
        cluster: cluster_state.clone(),
        sharder: anvil::sharding::ShardManager::new(),
        placer: anvil::placement::PlacementManager::default(),
    };

    (state, swarm)
}

#[tokio::test]
async fn test_distributed_put_and_get() {
    println!("Running test_distributed_put_and_get");
    dotenvy::dotenv().ok();

    let num_nodes = 6;
    let base_grpc_port = 50080;
    println!("Running {} nodes", num_nodes);
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
        swarms[i]
            .listen_on(format!("/ip4/127.0.0.1/tcp/{}", 60090 + i).parse().unwrap())
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
        let grpc_addr = format!("127.0.0.1:{}", base_grpc_port + i).parse().unwrap();
        let grpc_addr_str = format!("http://127.0.0.1:{}", base_grpc_port + i);
        tokio::spawn(async move {
            let server = Server::builder()
                .add_service(ObjectServiceServer::new(state.clone()))
                .add_service(BucketServiceServer::new(state.clone()))
                .add_service(InternalAnvilServiceServer::new(state.clone()))
                .serve(grpc_addr);
            let gossip = run_gossip(swarm, state.cluster, grpc_addr_str);
            let _ = tokio::try_join!(async { server.await.map_err(anyhow::Error::from) }, async {
                gossip.await.map_err(anyhow::Error::from)
            },);
        });
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await; // Brief wait for listeners to bind

    // Wait for the cluster to form
    loop {
        let state = cluster_state.read().await;
        let len = state.len();
        println!("Num of nodes is {}", len);
        if len >= num_nodes {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    let mut bucket_client = BucketServiceClient::connect(format!("http://127.0.0.1:{}", base_grpc_port)).await.unwrap();
    let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());
    bucket_client
        .create_bucket(CreateBucketRequest {
            bucket_name: bucket_name.clone(),
        })
        .await
        .unwrap();

    let mut object_client =
        ObjectServiceClient::connect(format!("http://127.0.0.1:{}", base_grpc_port))
            .await
            .unwrap();
    let object_key = "my-final-object".to_string();
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

    tokio::time::sleep(std::time::Duration::from_secs(1)).await; // Give time for shards to be written

    let mut shards_found = 0;
    for i in 0..num_nodes {
        let shard_path = format!("anvil-data/{}-{:02}", object_hash, i);
        if Path::new(&shard_path).exists() {
            shards_found += 1;
            fs::remove_file(shard_path).await.unwrap();
        }
    }
    assert!(shards_found > 0, "Expected to find at least one shard file");
}
