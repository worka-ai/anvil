use anvil::persistence::Persistence;
use dotenvy::dotenv;
use std::env;

mod common;

async fn setup() -> Persistence {
    dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set for tests");
    let pool = common::create_pool(&db_url).unwrap();
    common::run_migrations(&pool).await.unwrap();

    let client = pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO regions (name) VALUES ('test-region') ON CONFLICT (name) DO NOTHING",
            &[],
        )
        .await
        .unwrap();

    Persistence::new(pool.clone(), pool)
}

#[tokio::test]
async fn test_create_tenant_and_bucket() {
    let p = setup().await;

    // Create a tenant
    let tenant_name = format!("test-tenant-grpc-{}", uuid::Uuid::new_v4());
    let tenant = p.create_tenant(&tenant_name, "test-key").await.unwrap();
    assert_eq!(tenant.name, tenant_name);

    // Create a bucket for that tenant
    let bucket_name = "test-bucket";
    let bucket = p
        .create_bucket(tenant.id, bucket_name, "test-region")
        .await
        .unwrap();
    assert_eq!(bucket.name, bucket_name);
    assert_eq!(bucket.tenant_id, tenant.id);
}
