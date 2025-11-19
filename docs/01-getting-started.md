---
slug: /anvil/getting-started
title: 'Getting Started: Anvil in 10 Minutes'
description: A hands-on guide to launching a single-node Anvil instance with Docker and interacting with it using the Anvil CLI.
tags: [getting-started, docker, cli]
---

# Chapter 1: Anvil in 10 Minutes

> **TL;DR:** Use our `docker-compose.yml` to launch a single-node Anvil instance and its Postgres database. Use the `anvil` to create a bucket and upload your first file.

This guide will walk you through the fastest way to get a fully functional, single-node Anvil instance running on your local machine. By the end, you will have created a bucket, uploaded a file, and downloaded it back.

### 1.1. Prerequisites

-   **Docker and Docker Compose:** Anvil is packaged as a Docker container for easy deployment. Ensure you have both [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.
-   **`anvil`:** The Anvil command-line interface is the primary tool for interacting with your Anvil cluster. It should be provided as part of your Anvil distribution.

### 1.2. Launching Anvil with Docker Compose

First, save the following content as `docker-compose.yml` in a new directory on your machine. This configuration will launch two services: one for the Anvil node and another for its PostgreSQL metadata database.

```yaml
version: "3.8"

networks:
  anvilnet:
    driver: bridge

services:
  postgres-global:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: worka
      POSTGRES_PASSWORD: "a-secure-password" # <-- Change this in production
      POSTGRES_DB: anvil_global
    ports:
      - "5433:5432"
    volumes:
      - postgres_global_data:/var/lib/postgresql/data
    networks: [anvilnet]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U worka -d anvil_global"]
      interval: 5s
      timeout: 5s
      retries: 5

  postgres-regional:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: worka
      POSTGRES_PASSWORD: "a-secure-password" # <-- Change this in production
      POSTGRES_DB: anvil_regional_europe
    volumes:
      - postgres_regional_data:/var/lib/postgresql/data
    networks: [anvilnet]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U worka -d anvil_regional_europe"]
      interval: 5s
      timeout: 5s
      retries: 5

  anvil1:
    image: ghcr.io/worka-ai/anvil:v2025.11.14-001012
    depends_on:
      postgres-global:
        condition: service_healthy
      postgres-regional:
        condition: service_healthy
    environment:
      RUST_LOG: "info"
      # Use a URL-encoded password if it contains special characters
      GLOBAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-global:5432/anvil_global"
      REGIONAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-regional:5432/anvil_regional_europe"
      REGION: "europe-west-1"
      # --- CRITICAL: SET THESE TO SECURE, RANDOMLY GENERATED VALUES ---
      JWT_SECRET: "must-be-a-long-and-random-secret-for-signing-jwts"
      ANVIL_SECRET_ENCRYPTION_KEY: "must-be-a-64-character-hex-string-generate-with-openssl-rand-hex-32"
      ANVIL_CLUSTER_SECRET: "must-be-a-long-and-random-secret-for-cluster-gossip"
      # --- Networking Configuration ---
      # For local testing, `localhost` is acceptable. In a real deployment, 
      # you would replace this with the node's public IP address.
      API_LISTEN_ADDR: "0.0.0.0:50051"
      CLUSTER_LISTEN_ADDR: "/ip4/0.0.0.0/udp/7443/quic-v1"
      PUBLIC_CLUSTER_ADDRS: "/ip4/127.0.0.1/udp/7443/quic-v1"
      PUBLIC_API_ADDR: "http://localhost:50051"
      ENABLE_MDNS: "false"
      BOOTSTRAP_ADDRS: "" # Empty for a single node
    command: ["anvil", "--init-cluster"]
    ports:
      - "50051:50051"    # Unified S3 & gRPC Port
      - "7443:7443/udp"  # QUIC P2P Port
    networks: [anvilnet]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:50051/ready"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  postgres_global_data:
  postgres_regional_data:
```

Now, open a terminal in the same directory and run:

```bash
docker-compose up -d
```

This command will download the necessary images and start the Anvil and Postgres containers in the background.

### 1.3. Creating Your First Tenant and API Key

Anvil is a multi-tenant system. Before you can create buckets, you need a **Tenant** and an **App** with an API key. You can create these using the `admin` tool, which we will run inside the running Docker container.

**Step 1: Create the Region and Tenant**

```bash
# Create the region (uses a positional argument)
docker compose exec anvil1 admin region create europe-west-1

# Create the tenant (uses a positional argument)
docker compose exec anvil1 admin tenant create my-first-tenant
```

**Step 2: Create an App**

Next, create an App for this tenant. This will generate the credentials needed to interact with the API.

```bash
# Create an app and get its credentials (uses named flags)
docker compose exec anvil1 admin app create --tenant-name my-first-tenant --app-name my-cli-app
```

This command will output a **Client ID** and a **Client Secret**. **Save these securely!** They are your API credentials.

### 1.4. Granting Permissions

By default, a new app has **no permissions**. You must explicitly grant it the rights to perform actions. For this guide, we will grant the app the specific permissions it needs to create a bucket and manage objects within that bucket.

> **IMPORTANT:** This is the critical step that allows your app to create buckets and upload objects.

```bash
# 1. Grant permission to create buckets
docker compose exec anvil1 admin policy grant \
    --app-name my-cli-app \
    --action "bucket:create" \
    --resource "*"

# 2. Grant permission to list buckets
docker compose exec anvil1 admin policy grant \
    --app-name my-cli-app \
    --action "bucket:read" \
    --resource "*"

# 3. Grant full object permissions for the bucket we are about to create
# Note that the bucket does not have to exist yet.
docker compose exec anvil1 admin policy grant \
    --app-name my-cli-app \
    --action "object:*" \
    --resource "my-first-anvil-bucket/*"
```

### 1.5. Using the `anvil` to Create a Bucket

Now you can configure the `anvil` to connect to your new Anvil instance.

**Step 1: Configure the CLI**

Run the `configure` command and provide the host and the credentials you saved.

```bash
# Replace YOUR_CLIENT_ID and YOUR_CLIENT_SECRET with the values from the previous step
anvil configure --host http://localhost:50051 --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET
```

**Step 2: Create a Bucket**

Now, create a bucket.

```bash
anvil bucket create --name my-first-anvil-bucket --region europe-west-1
```

### 1.5. Uploading and Downloading Your First Object

Create a sample file to upload:

```bash
echo "Hello, Anvil!" > hello.txt
```

Upload it to your new bucket using an S3-style path:

```bash
anvil object put --src hello.txt --dest s3://my-first-anvil-bucket/hello.txt
```

You can list the objects in your bucket to confirm the upload was successful:

```bash
anvil object ls --path s3://my-first-anvil-bucket/
```

Finally, download the file back to verify its contents:

```bash
anvil object get --src s3://my-first-anvil-bucket/hello.txt --dest downloaded_hello.txt

cat downloaded_hello.txt
# Expected output: Hello, Anvil!
```

Congratulations! You have successfully deployed Anvil, created a bucket, and performed basic object storage operations. You are now ready to explore the more advanced features covered in the rest of this guide.
