# anvil-storage-client

Python gRPC client package for Anvil's native API.

The package ships `proto/anvil.proto` and generates Python protobuf/gRPC modules during package build.

```python
from anvil_storage_client import bearer_metadata, insecure_channel

channel = insecure_channel("localhost:50051")
metadata = bearer_metadata("<access-token>")
```

After installation, generated modules are available from the `anvil_storage_client` package.
