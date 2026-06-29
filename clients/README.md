# Anvil Clients

This directory contains registry packaging for Anvil native API clients generated from `anvil-core/proto/anvil.proto`.

## TypeScript / npm

Package: `anvil-storage-client`

```sh
cd clients/typescript
npm install
npm run build
npm pack --dry-run
```

The package ships the canonical proto and a small `@grpc/grpc-js` dynamic loader. Strongly typed convenience wrappers can be added without changing the protobuf source of truth.

## Python / PyPI

Package: `anvil-storage-client`

```sh
cd clients/python
python3.12 -m pip wheel . --no-deps -w /tmp/anvil-python-wheel
```

The package ships the canonical proto and generates Python protobuf/gRPC modules during wheel build.

## Synchronizing proto files

After editing `anvil-core/proto/anvil.proto`, run:

```sh
scripts/sync-client-protos.sh
```

This copies the canonical proto into each registry package before release.
