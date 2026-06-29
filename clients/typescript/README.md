# anvil-storage-client

TypeScript helper package for Anvil's native gRPC API.

The package ships `proto/anvil.proto` and a small dynamic loader built on `@grpc/grpc-js` and `@grpc/proto-loader`.

```ts
import { credentials, loadAnvilPackage, metadataWithBearer } from 'anvil-storage-client';

const anvil = loadAnvilPackage();
const channelCredentials = credentials({ endpoint: 'http://localhost:50051' });
const metadata = metadataWithBearer('<access-token>');
```

Generated, strongly typed service wrappers can be layered on top of the bundled proto without changing the package versioning model.
