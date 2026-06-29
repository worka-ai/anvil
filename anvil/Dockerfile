# Stage 1: Build the binaries with the Rust version declared by the workspace.
FROM rust:1.89-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/anvil

# Copy the entire project
COPY . .

# Build the anvil server and the admin CLI in release mode
RUN cargo build --release --bin anvil --bin admin
RUN cargo build --release -p anvil-storage-cli

# Stage 2: Create the final runtime image. Rust tooling is deliberately not
# present in the shipped server image.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binaries from the builder stage
COPY --from=builder /usr/src/anvil/target/release/anvil /usr/local/bin/anvil
COPY --from=builder /usr/src/anvil/target/release/admin /usr/local/bin/admin
COPY --from=builder /usr/src/anvil/target/release/anvil-cli /usr/local/bin/anvil-cli

# Expose the default gRPC/S3 port and a potential swarm port
EXPOSE 50051
EXPOSE 7443

# Set the default command to run the anvil server
CMD ["anvil"]
