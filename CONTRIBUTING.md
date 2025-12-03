# Contributing to Anvil

First off, thank you for considering contributing to Anvil! We welcome any and all contributions, from bug reports and documentation improvements to new features. This document provides a
guide for making a contribution.

## Code of Conduct

This project and everyone participating in it is governed by the [Anvil Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report
unacceptable behavior.

## How Can I Contribute?

There are many ways to contribute to Anvil:

 *   **Reporting Bugs:** If you find a bug, please open an issue.
 *   **Suggesting Enhancements:** If you have an idea for a new feature or an improvement, open an issue to start a discussion.
 *   **Improving Documentation:** If you see a typo or find something unclear in the docs, you can open an issue or submit a pull request directly.
 *   **Writing Code:** You can pick up an existing issue or contribute a new feature you've discussed with the maintainers.

## Reporting Bugs

Before submitting a new issue, please check the [existing issues](https://github.com/worka-ai/anvil/issues) to see if your problem has already been reported.

When submitting a bug report, please include as much detail as possible:

 *   **Anvil Version:** The version of Anvil you are running.
 *   **Operating System:** The OS you are running Anvil on.
 *   **Steps to Reproduce:** A clear, step-by-step description of how to reproduce the bug.
 *   **Expected Behavior:** What you expected to happen.
 *   **Actual Behavior:** What actually happened, including any error messages and relevant logs.

## Suggesting Features

We welcome feature suggestions! To suggest a feature, please [open an issue](https://github.com/worka-ai/anvil/issues/new) and use the "Feature Request" template. This allows the
community and maintainers to discuss the feature's viability and design before any code is written.

Please include:
 *   **Problem Description:** A clear description of the problem the feature would solve.
 *   **Proposed Solution:** A detailed description of your proposed implementation.
 *   **Alternatives Considered:** Any alternative solutions or features you considered.

## Your First Code Contribution

Ready to contribute code? Here’s how to set up your development environment and submit a pull request.

### 1. Prerequisites

To work on Anvil, you will need the following tools installed:
 *   **Rust and Cargo:** Anvil is written in Rust. We recommend installing it via [rustup](https://rustup.rs/).
 *   **Docker and Docker Compose:** The development environment relies on Docker to run the required PostgreSQL databases.
 *   **`protoc`:** The Protocol Buffers compiler, used for the gRPC API.

### 2. Fork and Clone the Repository

First, [fork](https://github.com/worka-ai/anvil/fork) the repository to your own GitHub account. Then, clone your fork locally:

 ```bash
    git clone https://github.com/YOUR_USERNAME/anvil.git
    cd anvil
 ```

### 3. Set Up the Development Environment

 The Anvil development environment requires two PostgreSQL databases (one global, one regional) to be running. The easiest way to start them is with the `docker-compose.yml` file located in
the `anvil/` directory.

`docker-compose up -d`

This will start the two Postgres containers required to run the application and its tests.

### 4. Build and Run Tests

Before making any changes, ensure you can build the project and that all tests are passing.

From the root of the repository
The RUSTFLAGS will suppress warnings during the build
RUSTFLAGS="-A warnings" cargo build --quiet

Run the entire test suite

`cargo test --all-features`

### 5. Make Your Changes

1.  **Create a new branch:** Create a new branch for your feature or bugfix.
 `git checkout -b my-awesome-feature`
2.  **Write your code:** Make your changes to the codebase.
3.  **Format your code:** Anvil follows the standard Rust formatting guidelines. Before committing, please run `cargo fmt` to ensure your code is correctly formatted.
cargo fmt
4.  **Test your changes:** If you are adding a new feature, please add corresponding tests. Ensure that all existing tests still pass after your changes.
cargo test --all-features

 ### 6. Submit a Pull Request

 Once your changes are ready, push your branch to your fork and open a pull request against the `main` branch of the original Anvil repository.

 In your pull request description, please:
 *   **Link to the issue:** Reference the issue your PR addresses (e.g., `Fixes #123`).
 *   **Describe your changes:** Provide a clear summary of the changes you have made.
 *   **Explain your testing:** Describe how you have tested your changes.

 A maintainer will review your pull request, provide feedback, and guide you through the merge process. Thank you again for your contribution!
