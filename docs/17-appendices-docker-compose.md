---
slug: /anvil/appendices/docker-compose
title: 'Appendix A: Docker Compose Reference'
description: A reference copy of the `docker-compose.yml` file for deploying a multi-node Anvil cluster.
tags: [appendices, docker, configuration]
---

# Appendix A: Docker Compose Reference

This appendix contains the full `docker-compose.yml` file used for setting up a multi-node Anvil cluster for development and testing. It demonstrates how to configure multiple Anvil peers, connect them to shared databases, and set up the necessary networking and environment variables.

### Single-Node Development

For a simpler, single-node setup, you can refer to the version in the [Getting Started](/docs/anvil/getting-started) guide.

### Multi-Node Cluster (`docker-compose.yml`)

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
    build: .
    depends_on:
      postgres-global:
        condition: service_healthy
      postgres-regional:
        condition: service_healthy
    environment:
      RUST_LOG: "info"
      GLOBAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-global:5432/anvil_global"
      REGIONAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-regional:5432/anvil_regional_europe"
      REGION: "europe-west-1"
      # --- CRITICAL: SET THESE TO SECURE, RANDOMLY GENERATED VALUES ---
      JWT_SECRET: "must-be-a-long-and-random-secret-for-signing-jwts"
      ANVIL_SECRET_ENCRYPTION_KEY: "must-be-a-64-character-hex-string-generate-with-openssl-rand-hex-32"
      ANVIL_CLUSTER_SECRET: "must-be-a-long-and-random-secret-for-cluster-gossip"
      # --- Networking Configuration ---
      # These addresses MUST be reachable by other nodes and clients.
      # In a real deployment, replace 203.0.113.1 with the node's public IP address.
      API_LISTEN_ADDR: "0.0.0.0:50051"
      CLUSTER_LISTEN_ADDR: "/ip4/0.0.0.0/udp/7443/quic-v1"
      PUBLIC_CLUSTER_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
      PUBLIC_API_ADDR: "http://203.0.113.1:50051"
      ENABLE_MDNS: "false"
    command: ["anvil", "--init-cluster"]
    ports:
      - "50051:50051"
      - "7443:7443/udp"
    networks: [anvilnet]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:50051/ready"]
      interval: 5s
      timeout: 3s
      retries: 5

  anvil2:
    build: .
    depends_on:
      anvil1:
        condition: service_started
    environment:
      RUST_LOG: "info"
      GLOBAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-global:5432/anvil_global"
      REGIONAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-regional:5432/anvil_regional_europe"
      REGION: "europe-west-1"
      JWT_SECRET: "must-be-a-long-and-random-secret-for-signing-jwts"
      ANVIL_SECRET_ENCRYPTION_KEY: "must-be-a-64-character-hex-string-generate-with-openssl-rand-hex-32"
      ANVIL_CLUSTER_SECRET: "must-be-a-long-and-random-secret-for-cluster-gossip"
      API_LISTEN_ADDR: "0.0.0.0:50051"
      CLUSTER_LISTEN_ADDR: "/ip4/0.0.0.0/udp/7444/quic-v1"
      PUBLIC_CLUSTER_ADDRS: "/ip4/203.0.113.2/udp/7444/quic-v1"
      PUBLIC_API_ADDR: "http://203.0.113.2:50051"
      ENABLE_MDNS: "false"
      BOOTSTRAP_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
    ports:
      - "50052:50051"
      - "7444:7443/udp"
    networks: [anvilnet]

  anvil3:
    build: .
    depends_on:
      anvil1:
        condition: service_started
    environment:
      RUST_LOG: "info"
      GLOBAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-global:5432/anvil_global"
      REGIONAL_DATABASE_URL: "postgres://worka:a-secure-password@postgres-regional:5432/anvil_regional_europe"
      REGION: "europe-west-1"
      JWT_SECRET: "must-be-a-long-and-random-secret-for-signing-jwts"
      ANVIL_SECRET_ENCRYPTION_KEY: "must-be-a-64-character-hex-string-generate-with-openssl-rand-hex-32"
      ANVIL_CLUSTER_SECRET: "must-be-a-long-and-random-secret-for-cluster-gossip"
      API_LISTEN_ADDR: "0.0.0.0:50051"
      CLUSTER_LISTEN_ADDR: "/ip4/0.0.0.0/udp/7445/quic-v1"
      PUBLIC_CLUSTER_ADDRS: "/ip4/203.0.113.3/udp/7445/quic-v1"
      PUBLIC_API_ADDR: "http://203.0.113.3:50051"
      ENABLE_MDNS: "false"
      BOOTSTRAP_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
    ports:
      - "50053:50051"
      - "7445:7443/udp"
    networks: [anvilnet]

volumes:
  postgres_global_data:
  postgres_regional_data:
```
