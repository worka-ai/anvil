---
slug: /admin/production-operations
title: "Administrator's Guide: Production Operations"
description: A guide to monitoring, maintenance, and networking for a production Anvil cluster.
tags: [admin, production, monitoring, networking, reverse-proxy, backup]
---

# Administrator's Guide: Production Operations

This guide covers essential topics for running a stable, observable, and secure Anvil cluster in a production environment.

## 1. Monitoring and Health

Running a distributed system in production requires robust monitoring and a clear understanding of its maintenance processes.

### Health Checks and Readiness Probes

Each Anvil node exposes two HTTP endpoints for health monitoring:

-   **`/` (Health Check):** Returns `200 OK` as long as the Anvil server process is running. This can be used for basic liveness probes.

-   **`/ready` (Readiness Check):** A more comprehensive check that returns `200 OK` only if the node is fully initialized, connected to its databases, and has established contact with the cluster. You should use this endpoint for readiness probes in orchestrators like Kubernetes to ensure traffic is only routed to healthy nodes.

### Key Metrics for Monitoring (Prometheus)

Anvil is designed to expose metrics in a Prometheus-compatible format. Key areas to monitor include:

-   **API Latency and Error Rates:** Latency and error rates for S3 and gRPC API calls.
-   **Cluster Membership:** Number of active peers and the rate of cluster churn (nodes joining/leaving).
-   **Storage Utilization:** Total storage capacity and usage per tenant and per bucket.
-   **Task Queue:** The number of pending background tasks (e.g., for garbage collection).

### Backup and Recovery Strategy

While Anvil's erasure coding provides high durability against node failure, a complete disaster recovery plan must include database backups.

-   **Database Backup:** Your primary responsibility is to regularly back up the **global** and all **regional** PostgreSQL databases using standard tools like `pg_dump` or continuous archiving.
-   **Data Recovery:** In a catastrophic scenario, you would restore the PostgreSQL databases from backup, restore the physical shard data from your own off-site backups (if you have them), and launch a new cluster connected to the restored databases.

### Background Tasks

Anvil uses a task queue in the global database to manage asynchronous operations like garbage collection. When an object is deleted via the API, it is only marked for deletion. A background worker process on each node picks up this task later to physically remove the data shards, ensuring the API call remains fast.

## 2. Networking and Reverse Proxy

For any production deployment, you should place a reverse proxy in front of your Anvil cluster. This allows you to use a custom domain, terminate TLS/SSL, and load balance across your nodes.

### The Multiplexed Port

Anvil serves both S3 (HTTP/1.1) and gRPC (HTTP/2) traffic from the **same network port** (`50051` by default). Your reverse proxy only needs to forward traffic to this single upstream port.

### Caddy Configuration (Recommended)

Caddy is a modern web server that automatically handles TLS certificates. Its configuration is extremely simple.

```caddy
# Caddyfile for anvil.mycompany.com
anvil.mycompany.com {
    # Reverse proxy all traffic to your Anvil nodes.
    # Caddy intelligently handles both HTTP/1.1 and HTTP/2.
    reverse_proxy h2c://<anvil-node-1-ip>:50051 \
                  h2c://<anvil-node-2-ip>:50051 \
                  h2c://<anvil-node-3-ip>:50051
}
```
**Note:** The `h2c://` scheme tells Caddy that the backend service speaks HTTP/2 over a cleartext (unencrypted) connection, which is required for gRPC.

### Nginx Configuration

Configuring Nginx requires a bit more work.

```nginx
# /etc/nginx/nginx.conf

# Upstream for the combined S3 and gRPC service
ups<ctrl62><ctrl61>tream anvil_backend {
    server <anvil-node-1-ip>:50051;
    server <anvil-node-2-ip>:50051;
    # Add all Anvil nodes here for load balancing
}

server {
    listen 443 ssl http2;
    server_name anvil.mycompany.com;

    # Your TLS certificate
    ssl_certificate /path/to/your/fullchain.pem;
    ssl_certificate_key /path/to/your/privkey.pem;

    # It is critical to allow large body sizes for S3 uploads
    client_max_body_size 0; # 0 means unlimited

    location / {
        proxy_pass http://anvil_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```