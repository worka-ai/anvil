# anvil-storage

Rust gRPC client package for Anvil's native API.

The crate ships generated protocol bindings from `proto/anvil.proto`, a bearer-token interceptor, and typed service-client constructors.

```rust,no_run
use anvil_storage::{AnvilClient, proto::ListBucketsRequest};

# async fn example(endpoint: String, token: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;
let response = anvil.buckets().list_buckets(ListBucketsRequest {}).await?;
println!("{:?}", response.into_inner());
# Ok(())
# }
```
