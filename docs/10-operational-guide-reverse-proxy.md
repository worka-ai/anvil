---
slug: /operational-guide/reverse-proxy
title: 'Operational Guide: Configuring a Reverse Proxy'
description: Learn how to configure a reverse proxy like Caddy or Nginx in front of your Anvil cluster for TLS termination and custom domains.
tags: [operational-guide, networking, reverse-proxy, tls, ssl, caddy, nginx]
---

# Bonus Chapter: Configuring a Reverse Proxy

For any production deployment of Anvil, you should not expose the S3 and gRPC ports directly to the internet. Instead, you should place a reverse proxy in front of your Anvil cluster. 

**Benefits of a Reverse Proxy:**

-   **TLS/SSL Termination:** The proxy can handle HTTPS and terminate the encrypted connection, passing plain HTTP traffic to Anvil on the internal network. This centralizes certificate management.
-   **Custom Domains:** You can serve Anvil under a custom domain name (e.g., `s3.mycompany.com`).
-   **Load Balancing:** The proxy can distribute traffic across multiple Anvil nodes.
-   **Security:** It provides an additional layer of security, hiding the internal topology of your cluster.

This guide provides example configurations for two popular reverse proxies: Caddy and Nginx.

## Anvil Services to Proxy

You need to proxy two main services:

1.  **The S3 Gateway:** A standard HTTP service, running on port `9000` by default.
2.  **The gRPC Service:** An HTTP/2-based service, running on port `50051` by default.

## Caddy Configuration

Caddy is a modern web server known for its simplicity and automatic HTTPS. The following `Caddyfile` configuration will proxy both S3 and gRPC traffic to a single Anvil node, with automatic TLS certificate acquisition from Let's Encrypt.

```caddy
# Caddyfile

s3.your-domain.com {
    # Proxy the S3 HTTP Gateway
    reverse_proxy http://<anvil-node-ip>:9000
}

grpc.your-domain.com {
    # Proxy the gRPC service, enabling HTTP/2
    reverse_proxy h2c://<anvil-node-ip>:50051 {
        transport http {
            versions h2c 2
        }
    }
}
```

**Key Points:**

-   Replace `<anvil-node-ip>` with the internal IP address of your Anvil node.
-   We use `h2c` (HTTP/2 over cleartext) for the gRPC backend because we are terminating TLS at the proxy.
-   Caddy automatically handles acquiring and renewing TLS certificates for `s3.your-domain.com` and `grpc.your-domain.com`.

## Nginx Configuration

Nginx is a powerful and widely-used reverse proxy. The configuration is more verbose but offers fine-grained control.

This example assumes you have already obtained a TLS certificate (e.g., using Certbot) and are running Nginx 1.13.10 or later for gRPC support.

```nginx
# /etc/nginx/nginx.conf

# Upstream for the S3 gateway
upstream anvil_s3 {
    server <anvil-node-ip>:9000;
    # Add more nodes here for load balancing
    # server <anvil-node-2-ip>:9000;
}

# Upstream for the gRPC service
upstream anvil_grpc {
    server <anvil-node-ip>:50051;
    # server <anvil-node-2-ip>:50051;
}

server {
    listen 443 ssl http2;
    server_name s3.your-domain.com;

    # Your TLS certificate
    ssl_certificate /path/to/your/fullchain.pem;
    ssl_certificate_key /path/to/your/privkey.pem;

    location / {
        proxy_pass http://anvil_s3;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

server {
    listen 443 ssl http2;
    server_name grpc.your-domain.com;

    ssl_certificate /path/to/your/fullchain.pem;
    ssl_certificate_key /path/to/your/privkey.pem;

    location / {
        # Use grpc_pass for gRPC services
        grpc_pass grpc://anvil_grpc;
        grpc_set_header Host $host;
    }
}
```

**Key Points:**

-   Replace `<anvil-node-ip>` with the internal IP of your Anvil node.
-   The `http2` flag on the `listen` directive is crucial for enabling gRPC.
-   We define `upstream` blocks, which makes it easy to add more Anvil nodes for load balancing later.
-   `grpc_pass` is used instead of `proxy_pass` for the gRPC service to ensure Nginx handles the protocol correctly.
