---
slug: /operational-guide/reverse-proxy
title: 'Operational Guide: Configuring a Reverse Proxy'
description: Learn how to configure a reverse proxy like Caddy or Nginx in front of your Anvil cluster for TLS termination and custom domains.
tags: [operational-guide, networking, reverse-proxy, tls, ssl, caddy, nginx]
---

# Bonus Chapter: Configuring a Reverse Proxy

For any production deployment of Anvil, you should place a reverse proxy in front of your cluster. This allows you to use a custom domain, terminate TLS/SSL, and add a layer of security.

### The Multiplexed Port

Anvil is designed for simplicity and performance. It serves both S3 (HTTP/1.1) and gRPC (HTTP/2) traffic from the **same network port** (port `50051` by default). This means you only need to configure your reverse proxy to forward traffic for your domain to this single upstream port.

This guide provides examples for a single domain: `anvil.mycompany.com`.

### Caddy Configuration (Recommended)

Caddy is a modern web server that automatically handles TLS certificates. Its configuration is extremely simple because it intelligently handles proxying both HTTP/1.1 and HTTP/2 traffic to the same backend.

This is the recommended approach.

```caddy
# Caddyfile
anvil.mycompany.com {
    # Reverse proxy all traffic (S3 and gRPC) to the single multiplexed port.
    # Caddy will automatically handle TLS and forward both HTTP/1.1 and HTTP/2.
    reverse_proxy h2c://<anvil-node-ip>:50051
}
```

**Key Points:**

-   Replace `<anvil-node-ip>` with the internal IP of your Anvil node (or `anvil1` if Caddy is in the same Docker network).
-   `h2c://` tells Caddy that the backend service speaks HTTP/2 over a cleartext (unencrypted) connection. This is crucial for gRPC to work.

### Nginx Configuration

Configuring Nginx to handle both gRPC and standard HTTP traffic on the same path is more complex than with Caddy. The following configuration provides a starting point.

```nginx
# /etc/nginx/nginx.conf

# Upstream for the combined S3 and gRPC service
upstream anvil_backend {
    server <anvil-node-ip>:50051;
    # For load balancing, add more Anvil nodes here
    # server <anvil-node-2-ip>:50051;
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
        # This will proxy both HTTP/1.1 and gRPC (HTTP/2) traffic.
        # Nginx will upgrade the connection to HTTP/2 if the client requests it.
        proxy_pass http://anvil_backend;
        proxy_http_version 1.1; # Required for keep-alives and headers
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

**Key Points:**

-   The `http2` flag on the `listen` directive is essential.
-   This configuration uses a single `proxy_pass`. While this works for many gRPC use cases, some advanced gRPC features or client libraries may behave better when using Nginx's dedicated `grpc_pass` directive, which would require a more complex configuration to split the traffic (e.g., using a `map` on the `$content_type`). For most S3 and gRPC client use cases, the above configuration is sufficient.
