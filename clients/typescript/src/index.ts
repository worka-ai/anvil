import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));

export interface AnvilClientOptions {
  endpoint: string;
  rootCertificate?: Buffer;
  privateKey?: Buffer;
  certChain?: Buffer;
}

export function protoPath(): string {
  return join(here, '..', 'proto', 'anvil.proto');
}

export function loadAnvilPackage(): grpc.GrpcObject {
  const definition = protoLoader.loadSync(protoPath(), {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(definition).anvil as grpc.GrpcObject;
}

export function credentials(options: AnvilClientOptions): grpc.ChannelCredentials {
  if (options.rootCertificate || options.privateKey || options.certChain) {
    return grpc.credentials.createSsl(options.rootCertificate, options.privateKey, options.certChain);
  }
  return grpc.credentials.createInsecure();
}

export function metadataWithBearer(token: string): grpc.Metadata {
  const metadata = new grpc.Metadata();
  metadata.set('authorization', `Bearer ${token}`);
  return metadata;
}
