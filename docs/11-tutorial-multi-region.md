---
slug: /operational-guide/tutorial-multi-region
title: 'Tutorial: Deploying a Multi-Region Cluster'
description: A complete, step-by-step tutorial on setting up a geo-distributed, multi-region Anvil cluster.
tags: [operational-guide, tutorial, multi-region, cluster, scaling, production]
---

# Tutorial: Deploying a Multi-Region Anvil Cluster

This tutorial walks you through setting up a production-grade, geographically distributed Anvil cluster. This setup provides low-latency access for users across the globe by storing data in a region close to them.

### The Scenario

We will deploy a cluster with the following topology:

-   **3 Regions:** `us-east-1`, `europe-west-1`, and `ap-southeast-1`.
-   **3 Nodes per Region:** A total of 9 Anvil nodes.
-   **4 Databases:** 
    -   1 **Global** PostgreSQL database (e.g., hosted in `us-east-1`).
    -   3 **Regional** PostgreSQL databases, one in each region.

### Prerequisites

-   **9 Host Machines:** Three in each of your target geographical regions.
-   **4 PostgreSQL Databases:** One publicly accessible for global metadata, and one in each region (ideally with low latency to the Anvil nodes in that region).
-   **DNS Records:** You should have DNS records pointing to the public IP addresses of your hosts (e.g., `anvil-us-1.mycompany.com`, `anvil-eu-1.mycompany.com`, etc.).

### Step 1: Set Up and Secure Your Databases

1.  **Launch Databases:** Provision your four PostgreSQL servers.
2.  **Run Migrations:** Connect to each database and run the appropriate migrations.
    -   On the **Global** database, run the `migrations_global` scripts.
    -   On **each of the three Regional** databases, run the `migrations_regional` scripts.
3.  **Create a User:** Create a `worka` user with a strong, secure, and URL-encoded password (e.g., `Str0ng&P%40ss`).
4.  **Firewall Rules:** Ensure your Anvil hosts can connect to their respective databases on port `5432`.

### Step 2: Generate Secure Credentials

Before configuring the nodes, generate the secrets you will share across all of them.

```bash
# Generate a 64-character hex key for encryption
openssl rand -hex 32

# Generate a long, random string for JWTs
openssl rand -base64 48

# Generate a long, random string for the cluster secret
openssl rand -base64 48
```

Store these values securely.

### Step 3: Configure and Launch the First Node in Each Region

We will start by launching the *first* node in each of the three regions. These will act as the initial bootstrap points for their respective regions. The configuration is nearly identical, except for the `REGION` and `REGIONAL_DATABASE_URL`.

Here is a conceptual `docker-compose.yml` for the first node in `us-east-1` (`anvil-us-1` on host `203.0.113.1`):

```yaml
# docker-compose.us-east-1.yml
services:
  anvil-us-1:
    image: ghcr.io/worka-ai/anvil:main
    environment:
      RUST_LOG: "info"
      GLOBAL_DATABASE_URL: "postgres://worka:Str0ng&P%40ss@global-db.mycompany.com:5432/anvil_global"
      REGIONAL_DATABASE_URL: "postgres://worka:Str0ng&P%40ss@us-east-1-db.mycompany.com:5432/anvil_regional_us_east"
      REGION: "us-east-1"
      JWT_SECRET: "YOUR_SECURE_JWT_SECRET"
      ANVIL_SECRET_ENCRYPTION_KEY: "YOUR_64_CHAR_HEX_KEY"
      ANVIL_CLUSTER_SECRET: "YOUR_SECURE_CLUSTER_SECRET"
      API_LISTEN_ADDR: "0.0.0.0:50051"
      CLUSTER_LISTEN_ADDR: "/ip4/0.0.0.0/udp/7443/quic-v1"
      PUBLIC_CLUSTER_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
      PUBLIC_API_ADDR: "https://anvil.mycompany.com" # Your single public domain
    command: ["anvil", "--init-cluster"]
    ports: ["50051:50051", "7443:7443/udp"]
```

-   Launch this node on its host: `docker-compose -f docker-compose.us-east-1.yml up -d`.
-   Repeat this process for `anvil-eu-1` (in `europe-west-1`) and `anvil-ap-1` (in `ap-southeast-1`), making sure to change the `REGION`, `REGIONAL_DATABASE_URL`, and public IP addresses accordingly.

### Step 4: Configure and Launch the Remaining Nodes

Now, launch the other two nodes in each region. These nodes will point to the first node in their region to bootstrap.

Here is the conceptual `docker-compose.yml` for the *second* node in `us-east-1` (`anvil-us-2` on host `203.0.113.2`):

```yaml
# docker-compose.us-east-2.yml
services:
  anvil-us-2:
    image: ghcr.io/worka-ai/anvil:main
    environment:
      # ... Same DB URLs and Secrets as anvil-us-1 ...
      REGION: "us-east-1"
      # --- Networking for this specific node ---
      PUBLIC_CLUSTER_ADDRS: "/ip4/203.0.113.2/udp/7443/quic-v1"
      PUBLIC_API_ADDR: "https://anvil.mycompany.com"
      # --- BOOTSTRAP from the first node in this region ---
      BOOTSTRAP_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
    # NO --init-cluster command!
    ports: ["50051:50051", "7443:7443/udp"]
```

-   Launch this node on its host.
-   Repeat this process for the third node in `us-east-1`, and for all remaining nodes in `europe-west-1` and `ap-southeast-1`, always bootstrapping from the first node in their respective region.

### Step 5: Verify the Cluster

After a few moments, the gossip protocol will ensure all nodes are discovered. You can verify the cluster health by:

1.  **Checking Logs:** The logs for each node should show messages about discovered peers.

2.  **Create Admin Resources:** Use the `admin` CLI on any node to set up your tenant and an app for testing. Remember to save the Client ID and Secret.
    ```bash
    # Run on any Anvil host
    docker compose exec <anvil_service_name> admin tenants create my-company
    docker compose exec <anvil_service_name> admin apps create --tenant-name my-company --app-name ci-runner
    docker compose exec <anvil_service_name> admin policies grant --app-name ci-runner --action "*" --resource "*"
    ```

3.  **Configure S3 Client:** Configure your `aws-cli` with the credentials from the previous step and point it to the public S3 endpoint of one of your regions (e.g., `https://s3.mycompany.com`).

4.  **Create Buckets:** Use the S3 client to create buckets in each of your three regions. The `--region` parameter is critical.
    ```bash
    aws s3api create-bucket --bucket my-us-bucket --region us-east-1 --endpoint-url https://s3.mycompany.com
    aws s3api create-bucket --bucket my-eu-bucket --region europe-west-1 --endpoint-url https://s3.mycompany.com
    ```

5.  **Upload Data:** Connect to a node in a specific region (e.g., by pointing your S3 client to the IP of `anvil-eu-1`) and upload an object to a bucket in that same region (`my-eu-bucket`). The data will be sharded and stored only on the nodes within the `europe-west-1` region.

Congratulations! You now have a fully functional, geographically distributed storage and compute cloud.
