---
slug: /fundamentals/getting-started
title: 'Getting Started'
description: A hands-on guide to launching a single-node Anvil instance with Docker and interacting with it using the Anvil CLI.
tags: [getting-started, quickstart, tutorial, fundamentals]
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

### 1.3. Setup: Tenant, App, and Permissions

Anvil is a multi-tenant system where access is controlled by apps with fine-grained permissions. Before you can store files, an administrator must perform this one-time setup.

**Step 1: Create Region, Tenant, and App**

First, we use the `admin` tool inside the Docker container to create the necessary resources.

```bash
# 1. Create the region declared in your docker-compose.yml
docker compose exec anvil1 admin region create europe-west-1

# 2. Create a tenant to own your resources
docker compose exec anvil1 admin tenant create my-first-tenant

# 3. Create an app for your tenant. This generates your API credentials.
docker compose exec anvil1 admin app create --tenant-name my-first-tenant --app-name my-cli-app
```

The `app create` command will output a **Client ID** and a **Client Secret**. **Copy these securely!**

**Step 2: Grant Permissions**

By default, a new app has **zero permissions**. The administrator must explicitly grant it the rights to perform actions.

```bash
# 1. Grant permission to create buckets. The resource "*" allows creation in any region.
docker compose exec anvil1 admin policy grant \
    --app-name my-cli-app \
    --action "bucket:create" \
    --resource "*"

# 2. Grant the necessary object permissions for the bucket we are about to create.
# The bucket does not need to exist yet to set its permissions.
docker compose exec anvil1 admin policy grant \
    --app-name my-cli-app \
    --action "object:write" \
    --resource "my-first-anvil-bucket/*"

docker compose exec anvil1 admin policy grant \
    --app-name my-cli-app \
    --action "object:read" \
    --resource "my-first-anvil-bucket/*"

docker compose exec anvil1 admin policy grant \
    --app-name my-cli-app \
    --action "object:list" \
    --resource "my-first-anvil-bucket"
```

### 1.4. Using the Anvil CLI

Now, with the setup complete, you can use the `anvil` client CLI to manage your data.

**Step 1: Configure the CLI**

Use the `static-config` command to non-interactively create your configuration profile. Replace `YOUR_CLIENT_ID` and `YOUR_CLIENT_SECRET` with the credentials you saved.

```bash
anvil static-config \
    --name default \
    --host "http://localhost:50051" \
    --client-id YOUR_CLIENT_ID \
    --client-secret YOUR_CLIENT_SECRET \
    --default
```

**Step 2: Create a Bucket and Manage an Object**

Now you can create your bucket and perform file operations.

```bash
# 1. Create the bucket (this succeeds due to the "bucket:create" permission)
anvil bucket create my-first-anvil-bucket europe-west-1

# 2. Create a sample file to upload
echo "Hello, Anvil!" > hello.txt

# 3. Upload the file (succeeds due to "object:write")
anvil object put ./hello.txt s3://my-first-anvil-bucket/hello.txt

# 4. List objects to verify (succeeds due to "object:list")
anvil object ls s3://my-first-anvil-bucket/

# 5. Download the file to verify (succeeds due to "object:read")
anvil object get s3://my-first-anvil-bucket/hello.txt ./downloaded_hello.txt
cat ./downloaded_hello.txt
# Expected output: Hello, Anvil!
```

Congratulations! You have successfully deployed Anvil and performed a complete, permission-aware workflow. You are now ready to explore the scenarios and reference guides to learn about more advanced features.
