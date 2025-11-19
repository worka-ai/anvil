---
slug: /anvil/user-guide/grpc-api
title: 'User Guide: Using the gRPC API'
description: A guide for developers on how to use Anvil's high-performance gRPC API for service-to-service communication.
tags: [user-guide, grpc, api, protobuf, performance]
---

# Chapter 5: Using the gRPC API

> **TL;DR:** For higher performance, use the gRPC API. Authenticate with your App credentials to get a JWT, then include it in the metadata of your gRPC calls. Ideal for service-to-service communication.

While the S3 gateway provides maximum compatibility, the native gRPC API offers the highest performance and gives you access to the full range of Anvil's features. It is the recommended way to interact with Anvil for backend services and performance-critical applications.

### 5.1. Getting Started with the Protobuf Definitions

The Anvil gRPC API is defined using Protocol Buffers (Protobuf). The first step in integrating with the API is to get the `.proto` file, which you can use with a Protobuf compiler (like `protoc`) and a gRPC plugin for your language of choice to generate client-side code.

Here is a snippet from the `ObjectService` definition in `anvil.proto`:

```proto
service ObjectService {
  rpc PutObject(stream PutObjectRequest) returns (PutObjectResponse);
  rpc GetObject(GetObjectRequest) returns (stream GetObjectResponse);
  rpc DeleteObject(DeleteObjectRequest) returns (DeleteObjectResponse);
  rpc HeadObject(HeadObjectRequest) returns (HeadObjectResponse);
}

message PutObjectRequest {
    oneof data {
        ObjectMetadata metadata = 1;
        bytes chunk = 2;
    }
}

message GetObjectRequest {
    string bucket_name = 1;
    string object_key = 2;
    optional string version_id = 3;
}
```

### 5.2. Authentication Flow: From API Key to JWT

> **Note on Administrative Tasks:** The gRPC API is used for data plane operations (like `PutObject`, `GetObject`). Administrative tasks such as creating tenants, managing apps, and granting policies are handled by the [`admin` tool](../operational-guide/admin-tool), not the gRPC API.

Unlike the S3 gateway which uses SigV4 on every request, the gRPC API uses a bearer token model with JSON Web Tokens (JWT).

The flow is as follows:

1.  **Get an Access Token:** Call the `AuthService.GetAccessToken` RPC method, providing your App's `client_id` and `client_secret`.
2.  **Receive a JWT:** Anvil will return a short-lived JWT.
3.  **Make Authenticated Calls:** For all subsequent gRPC calls to other services (like `ObjectService`), you must include this JWT in the request metadata with the key `authorization` and the value `Bearer <YOUR_TOKEN>`.

**Example: Getting a Token**

```go
// Example in Go
authClient := anvil.NewAuthServiceClient(conn)
tokenResponse, err := authClient.GetAccessToken(ctx, &anvil.GetAccessTokenRequest{
    ClientId:     "YOUR_CLIENT_ID",
    ClientSecret: "YOUR_CLIENT_SECRET",
    Scopes:       []string{"read:bucket:my-data-bucket/*"}, // Request specific permissions
})

accessToken := tokenResponse.AccessToken
```

**Example: Making an Authenticated Call**

```go
// Add the token to the context for the next call
md := metadata.New(map[string]string{"authorization": "Bearer " + accessToken})
ctxWithToken := metadata.NewOutgoingContext(context.Background(), md)

// Make the call
objectClient := anvil.NewObjectServiceClient(conn)
headResponse, err := objectClient.HeadObject(ctxWithToken, &anvil.HeadObjectRequest{
    BucketName: "my-data-bucket",
    ObjectKey:  "my-file.txt",
})
```

### 5.3. Streaming Data: `PutObject` and `GetObject`

The gRPC API is designed for high-performance streaming of large objects. Both the `PutObject` and `GetObject` RPCs use client-side and server-side streaming, respectively. This avoids the need to load an entire object into memory on either the client or the server.

#### Uploading an Object (`PutObject`)

`PutObject` is a **client-streaming** RPC. This means you send a series of messages to the server.

1.  The **first message** must contain the object's `ObjectMetadata` (bucket name and key).
2.  All **subsequent messages** must contain a `chunk` of the object's binary data.
3.  After sending all chunks, you close the stream, and the server returns a single `PutObjectResponse`.

#### Downloading an Object (`GetObject`)

`GetObject` is a **server-streaming** RPC. You send a single request, and the server responds with a stream of messages.

1.  You send a single `GetObjectRequest` with the bucket and key.
2.  The server's **first response message** will contain the `ObjectInfo` (metadata like content type and length).
3.  All **subsequent messages** will contain a `chunk` of the object's binary data.
4.  You read from the stream until it is closed by the server.

This streaming-first design is fundamental to Anvil's performance, enabling efficient transfer of large files with minimal memory overhead.
