---
slug: /anvil/getting-started
title: 'Getting Started: Anvil in 10 Minutes'
description: A hands-on guide to launching a single-node Anvil instance with Docker and interacting with it using an S3 client.
tags: [getting-started, docker, s3]
---

# Chapter 1: Anvil in 10 Minutes

> **TL;DR:** Use our `docker-compose.yml` to launch a single-node Anvil instance and its Postgres database. Use the `anvil-cli` or any S3 client to create a bucket and upload your first file.

This guide will walk you through the fastest way to get a fully functional, single-node Anvil instance running on your local machine. By the end, you will have created a bucket, uploaded a file, and downloaded it back.

### 1.1. Prerequisites

-   **Docker and Docker Compose:** Anvil is packaged as a Docker container for easy deployment. Ensure you have both [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.
-   **An S3 Client:** You will need a client tool that can speak the S3 protocol. We recommend the [AWS Command Line Interface (CLI)](https://aws.amazon.com/cli/).

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
    image: ghcr.io/worka-ai/anvil:main
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

Anvil is a multi-tenant system. Before you can create buckets, you need a **Tenant** and an **App** with an API key. You can create these using the `admin` CLI, which we will run inside the running Docker container.

**Step 1: Create the Region and Tenant**

```bash
# Create the region (uses a positional argument)
docker compose exec anvil1 admin regions create europe-west-1

# Create the tenant (uses a positional argument)
docker compose exec anvil1 admin tenants create my-first-tenant
```

**Step 2: Create an App**

Next, create an App for this tenant. This will generate the credentials needed to interact with the S3 API.

```bash
# Create an app and get its credentials (uses named flags)
docker compose exec anvil1 admin apps create --tenant-name my-first-tenant --app-name my-s3-app
```

This command will output a **Client ID** and a **Client Secret**. **Save these securely!** They are your S3 access credentials.

**Step 3: Grant Permissions**

By default, a new app has **no permissions**. You must explicitly grant it the rights to perform actions. For this guide, we will grant it full admin-like permissions.

> **IMPORTANT:** This is the critical step that allows your app to create buckets and upload objects.

```bash
# Grant the app full permissions on all resources
docker compose exec anvil1 admin policies grant --app-name my-s3-app --action "*" --resource "*"
```

### 1.4. Using an S3 Client to Create a Bucket

Now you can configure your S3 client to connect to Anvil. For the AWS CLI, you can set the credentials and endpoint URL using environment variables.

Replace `YOUR_CLIENT_ID` and `YOUR_CLIENT_SECRET` with the values you saved in the previous step.

```bash
export AWS_ACCESS_KEY_ID=YOUR_CLIENT_ID
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=europe-west-1

# The Anvil S3 endpoint (note the port)
ANVIL_ENDPOINT="http://localhost:50051"
```

Now, create a bucket. Bucket names must be globally unique.

```bash
aws s3api create-bucket \
    --bucket my-first-anvil-bucket \
    --region europe-west-1 \
    --endpoint-url $ANVIL_ENDPOINT
```

### 1.5. Uploading and Downloading Your First Object

Create a sample file to upload:

```bash
echo "Hello, Anvil!" > hello.txt
```

Upload it to your new bucket:

```bash
aws s3 cp hello.txt s3://my-first-anvil-bucket/hello.txt --endpoint-url $ANVIL_ENDPOINT
```

You can list the objects in your bucket to confirm the upload was successful:

```bash
aws s3 ls s3://my-first-anvil-bucket/ --endpoint-url $ANVIL_ENDPOINT
```

Finally, download the file back to verify its contents:

```bash
aws s3 cp s3://my-first-anvil-bucket/hello.txt downloaded_hello.txt --endpoint-url $ANVIL_ENDPOINT

cat downloaded_hello.txt
# Expected output: Hello, Anvil!
```

Congratulations! You have successfully deployed Anvil, created a bucket, and performed basic object storage operations. You are now ready to explore the more advanced features covered in the rest of this guide.
