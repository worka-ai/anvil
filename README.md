# Anvil: An Open-Source Object Store for AI/ML Research

[![Build Status](https://github.com/worka-ai/anvil-enterprise/actions/workflows/ci.yml/badge.svg)](https://github.com/worka-ai/anvil-enterprise/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![JOSS Submission](https://joss.theoj.org/papers/10.21105/joss.XXXXX/status.svg)](https://joss.theoj.org/papers/10.21105/joss.XXXXX)

**Anvil** is a high-performance, open-source distributed object store built in Rust. It is designed to address the data management and storage challenges inherent in modern computational research, particularly for large-scale Artificial Intelligence (AI) and Machine Learning (ML) workloads. By providing an S3-compatible interface, a native high-throughput gRPC API, and first-class support for content-addressing, Anvil serves as a foundational infrastructure layer for reproducible and efficient research.

---

## Key Features

-   **Content-Addressable Storage:** Automatically deduplicates identical data using BLAKE3 hashing, dramatically reducing storage costs for versioned models and datasets.
-   **High-Performance gRPC Streaming:** A native gRPC API with bidirectional streaming, ideal for high-throughput ML data loaders that feed GPUs directly from storage.
-   **S3-Compatible Gateway:** Provides drop-in compatibility with the vast ecosystem of existing research tools and SDKs that support the S3 API (Boto3, MLflow, Rclone, etc.).
-   **Built for the ML Ecosystem:** Includes features like the `anvil hf ingest` command to import model repositories directly from the Hugging Face Hub.
-   **Modern, Resilient Architecture:** Built in Rust for memory safety and high concurrency, with a SWIM-like gossip protocol over QUIC for clustering and failure detection.
-   **Multi-Tenant by Design:** Provides strong logical isolation between different users, teams, or projects.

---

## 🚀 Quick Start

The fastest way to get a single-node Anvil instance running is with Docker Compose.

1.  **Save the `docker-compose.yml`:**
    Save the example `docker-compose.yml` from the [Getting Started Guide](./docs/01-getting-started.md) to a local file.

2.  **Launch Anvil:**
    ```bash
    docker-compose up -d
    ```

3.  **Create Your First Tenant and App:**
    Use the `admin` tool to create a tenant and an app with API credentials.
    ```bash
    # Create a region and a tenant
    docker compose exec anvil1 admin region create europe-west-1
    docker compose exec anvil1 admin tenant create my-first-tenant

    # Create an app and save the credentials
    docker compose exec anvil1 admin app create --tenant-name my-first-tenant --app-name my-cli-app
    ```

4.  **Configure the Anvil CLI:**
    Use the credentials from the previous step to configure your local `anvil` CLI.
    ```bash
    anvil configure --host http://localhost:50051 --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET
    ```

---

## 📘 Documentation

For complete guides on deployment, architecture, and usage, please see the [**Full Documentation**](./docs/index.md).

-   [Getting Started](./docs/01-getting-started.md)
-   [Authentication & Permissions](./docs/03-user-guide-authentication.md)
-   [Using the S3 Gateway](./docs/04-user-guide-s3-gateway.md)
-   [Deployment Guide](./docs/06-operational-guide-deployment.md)

---

## 🤝 Contributing

We welcome contributions of all kinds! Please read our [**Contributing Guide**](./CONTRIBUTING.md) to get started. All participation in the Anvil community is governed by our [**Code of Conduct**](./CODE_OF_CONDUCT.md).

---

## 📜 Citing Anvil

If you use Anvil in your research, please cite it. Once published in JOSS, a BibTeX entry will be provided here.

```bibtex
@article{Anvil2025,
  doi = {10.21105/joss.XXXXX},
  url = {https://doi.org/10.21105/joss.XXXXX},
  year = {2025},
  publisher = {The Open Journal},
  volume = {X},
  number = {XX},
  pages = {XXXXX},
  author = {Your Name and Other Authors},
  title = {Anvil: An Open-Source Object Store for AI/ML Research},
  journal = {Journal of Open Source Software}
}
```

---

## License

Anvil is licensed under the [Apache 2.0 License](./LICENSE).
