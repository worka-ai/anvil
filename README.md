# Anvil: Open‑Source Object Storage in Rust

**Anvil** is an open‑source, S3‑compatible object storage server written in Rust. Built by the team behind Worka, Anvil is designed to host large files—such as open‑source model weights—with high performance and reliability. It exposes a familiar S3 HTTP gateway, a high‑performance gRPC API, multi‑tenant isolation, and the ability to scale from a single development node to a multi‑region cluster.

---

## 🔥 Why Anvil?

- **Written in Rust**: Modern, memory-safe, and highly concurrent.
- **S3-Compatible**: Works out of the box with AWS SDKs, CLI, and third-party tools.
- **gRPC API**: For low-latency, high-throughput access.
- **Multi-Tenant**: Serve different model groups or clients in isolation.
- **Clusterable**: Run standalone or as a horizontally-scalable distributed system.
- **Model Hosting Friendly**: Built to serve billions of tokens efficiently.

---

## 🚀 Quick Start (Standalone)

```bash
cargo install anvil
anvil server --root ./data --port 9000
```

Now test it:

```bash
aws --endpoint-url http://localhost:9000 s3 ls
```

---

## 🧪 Example: Upload and Fetch via S3

```bash
# Upload a file
aws --endpoint-url http://localhost:9000 s3 cp weights.gguf s3://mymodels/weights.gguf

# Fetch the file
curl http://localhost:9000/mymodels/weights.gguf
```

---

## 🏗️ Building From Source

Anvil uses [Rust](https://www.rust-lang.org/tools/install) and requires at least version 1.72.

```bash
git clone https://github.com/worka-ai/anvil
cd anvil
cargo build --release
```

---

## ⚙️ Running in Cluster Mode

Start multiple nodes with a shared cluster config (see [docs](https://worka.ai/docs/anvil/operational-guide/scaling)).

---

## 📡 gRPC API

See full [API reference](https://worka.ai/docs/anvil/user-guide/grpc-api). Example client use:

```bash
anvil grpc-client --list-buckets
```

---

## 🔐 Authentication

Supports API key-based tenant isolation. See [Auth docs](https://worka.ai/docs/anvil/user-guide/auth-permissions).

---

## 📘 Documentation

- [Getting Started](https://worka.ai/docs/anvil/getting-started)
- [Deployment](https://worka.ai/docs/anvil/operational-guide/deployment)
- [S3 Gateway](https://worka.ai/docs/anvil/user-guide/s3-gateway)
- [Cluster Scaling](https://worka.ai/docs/anvil/operational-guide/scaling)
- [Contributing](https://worka.ai/docs/anvil/developer-guide/contributing)

---

## 🤝 Contributing

We welcome PRs! Check out [CONTRIBUTING.md](https://worka.ai/docs/anvil/developer-guide/contributing) and start with [good first issues](https://github.com/worka-ai/anvil/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).

---

## 📣 Community

- [Discord](https://discord.gg/uCWVg5STGh) — Chat with the team
- [Product Hunt](https://www.producthunt.com/products/worka-anvil)

---

## License

Licensed under [Apache 2.0](LICENSE).
