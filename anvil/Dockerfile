# Stage 1: Build the binaries with the release toolchain used by CI.
FROM rust:1.96-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    libclang-dev \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/anvil

# Copy the entire project
COPY . .

# Build the anvil server and the admin CLI in release mode
RUN cargo build --release --bin anvil
RUN cargo build --release -p anvil-storage-cli --bin admin

# Stage 2: Create the final runtime image. Rust tooling is deliberately not
# present in the shipped server image.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --create-home --home-dir /var/lib/anvil --shell /usr/sbin/nologin anvil

# Copy the compiled binaries from the builder stage
COPY --from=builder /usr/src/anvil/target/release/anvil /usr/local/bin/anvil
COPY --from=builder /usr/src/anvil/target/release/admin /usr/local/bin/admin

RUN chown -R anvil:anvil /var/lib/anvil

ENV STORAGE_PATH=/var/lib/anvil
USER anvil
WORKDIR /var/lib/anvil

# Expose the default gRPC/S3 port and a potential swarm port
EXPOSE 50051
EXPOSE 50052
EXPOSE 7443

HEALTHCHECK --interval=30s --timeout=5s --retries=5 CMD curl -fsS http://127.0.0.1:50051/ready || exit 1

# Set the default command to run the anvil server
CMD ["anvil"]
