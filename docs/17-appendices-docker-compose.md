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
      POSTGRES_PASSWORD: worka
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
      POSTGRES_PASSWORD: worka
      POSTGRES_DB: anvil_regional_docker
    volumes:
      - postgres_regional_data:/var/lib/postgresql/data
    networks: [anvilnet]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U worka -d anvil_regional_docker"]
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
      - RUST_LOG=info
      - GLOBAL_DATABASE_URL=postgres://worka:worka@postgres-global:5432/anvil_global
      - REGIONAL_DATABASE_URL=postgres://worka:worka@postgres-regional:5432/anvil_regional_docker
      - REGION=DOCKER_TEST
      - JWT_SECRET=docker-test-secret
      - ANVIL_SECRET_ENCRYPTION_KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      - ANVIL_CLUSTER_SECRET=docker-cluster-secret
      - HTTP_BIND_ADDR=0.0.0.0:9000
      - GRPC_BIND_ADDR=0.0.0.0:50051
      - QUIC_BIND_ADDR=/ip4/0.0.0.0/udp/7443/quic-v1
      - PUBLIC_ADDRS=/dns4/anvil1/udp/7443/quic-v1
      - PUBLIC_GRPC_ADDR=http://anvil1:50051
      - ENABLE_MDNS=false
    command: ["anvil", "--init-cluster"]
    ports:
      - "9000:9000"
      - "50051:50051"
      - "7443:7443/udp"
    networks: [anvilnet]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/ready"]
      interval: 5s
      timeout: 3s
      retries: 5

  anvil2:
    build: .
    depends_on:
      anvil1:
        condition: service_started
    environment:
      - RUST_LOG=info
      - GLOBAL_DATABASE_URL=postgres://worka:worka@postgres-global:5432/anvil_global
      - REGIONAL_DATABASE_URL=postgres://worka:worka@postgres-regional:5432/anvil_regional_docker
      - REGION=DOCKER_TEST
      - JWT_SECRET=docker-test-secret
      - ANVIL_SECRET_ENCRYPTION_KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      - ANVIL_CLUSTER_SECRET=docker-cluster-secret
      - HTTP_BIND_ADDR=0.0.0.0:9000
      - GRPC_BIND_ADDR=0.0.0.0:50052
      - QUIC_BIND_ADDR=/ip4/0.0.0.0/udp/7444/quic-v1
      - PUBLIC_ADDRS=/dns4/anvil2/udp/7444/quic-v1
      - PUBLIC_GRPC_ADDR=http://anvil2:50052
      - ENABLE_MDNS=false
      - BOOTSTRAP_ADDRS=/dns4/anvil1/udp/7443/quic-v1
    ports:
      - "9001:9000"
      - "50052:50052"
      - "7444:7443/udp"
    networks: [anvilnet]

  anvil3:
    build: .
    depends_on:
      anvil1:
        condition: service_started
    environment:
      - RUST_LOG=info
      - GLOBAL_DATABASE_URL=postgres://worka:worka@postgres-global:5432/anvil_global
      - REGIONAL_DATABASE_URL=postgres://worka:worka@postgres-regional:5432/anvil_regional_docker
      - REGION=DOCKER_TEST
      - JWT_SECRET=docker-test-secret
      - ANVIL_SECRET_ENCRYPTION_KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      - ANVIL_CLUSTER_SECRET=docker-cluster-secret
      - HTTP_BIND_ADDR=0.0.0.0:9000
      - GRPC_BIND_ADDR=0.0.0.0:50053
      - QUIC_BIND_ADDR=/ip4/0.0.0.0/udp/7445/quic-v1
      - PUBLIC_ADDRS=/dns4/anvil3/udp/7445/quic-v1
      - PUBLIC_GRPC_ADDR=http://anvil3:50053
      - ENABLE_MDNS=false
      - BOOTSTRAP_ADDRS=/dns4/anvil1/udp/7443/quic-v1
    ports:
      - "9002:9000"
      - "50053:50053"
      - "7445:7443/udp"
    networks: [anvilnet]

volumes:
  postgres_global_data:
  postgres_regional_data:
```
