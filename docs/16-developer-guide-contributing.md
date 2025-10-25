---
slug: /anvil/developer-guide/contributing
title: 'Developer Guide: Contributing to Anvil'
description: A guide for developers who want to contribute to the Anvil project, covering setup, testing, and contribution guidelines.
tags: [developer-guide, contributing, development, testing, github]
---

# Chapter 16: Contributing to Anvil

> **TL;DR:** Set up your development environment, run the test suites, and follow our contribution guidelines.

Anvil is an open-source project, and we welcome contributions from the community. This guide provides the information you need to get your development environment set up, run the tests, and prepare your first contribution.

### 16.1. Building from Source

**Prerequisites:**

-   **Rust:** Anvil is built with the Rust 2024 edition. We recommend installing Rust via [rustup](https://rustup.rs/).
-   **PostgreSQL:** You will need a running PostgreSQL server for the databases.
-   **Docker:** The integration tests use Docker to manage test environments.

**Steps to Build:**

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/worka-ai/anvil.git
    cd anvil
    ```

2.  **Set up Databases:**
    For development, you can use the provided `docker-compose.yml` to spin up the necessary PostgreSQL instances.

    ```bash
    docker-compose up -d postgres-global postgres-regional
    ```

3.  **Configure Environment:**
    Copy the `.env.example` file to `.env` and fill in the database URLs and other required secrets.

4.  **Build the Project:**

    ```bash
    cargo build
    ```

### 16.2. Running the Test Suite

Anvil has a comprehensive test suite that covers unit tests, integration tests, and end-to-end cluster tests.

**Unit Tests:**

These are fast tests that check individual components in isolation.

```bash
cargo test --lib
```

**Integration Tests:**

These tests spin up a test cluster, including isolated databases, to test the interaction between different components. They are located in the `tests/` directory.

```bash
cargo test --test '*'
```

> **Note:** The integration tests require Docker to be running, as they create isolated PostgreSQL instances for each test run to ensure a clean environment.

**End-to-End Docker Cluster Test:**

The `docker_cluster_test` is a full end-to-end test that uses Docker Compose to build and run a multi-node cluster, then interacts with it using both the gRPC and S3 APIs. It is the most comprehensive test of the system.

```bash
cargo test --test docker_cluster_test
```

### 16.3. Code Style and Contribution Guidelines

1.  **Code Formatting:** All code should be formatted with `rustfmt`. You can run this with `cargo fmt`.

2.  **Linting:** We use `clippy` for linting. Please run `cargo clippy --all-targets --all-features -- -D warnings` to check for any issues before submitting your code.

3.  **Commit Messages:** Please follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for your commit messages. This helps us maintain a clear and readable commit history.
    *   Example: `feat(storage): Add support for tiered storage`
    *   Example: `fix(s3): Correctly handle URL encoding in object keys`

4.  **Pull Requests:**
    *   Create your changes on a new branch.
    *   Ensure all tests pass before submitting.
    *   Provide a clear description of the changes in your pull request.
    *   If your change is user-facing, please include updates to the relevant documentation.

We look forward to your contributions!
