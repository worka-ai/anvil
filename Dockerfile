# Stage 1: Build the binaries using cache mounts
FROM rust:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y build-essential pkg-config libssl-dev protobuf-compiler

WORKDIR /usr/src/anvil

# Create a dummy project to cache dependencies.
# This layer is only invalidated if Cargo.toml or Cargo.lock changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
# Use a cache mount to persist the cargo registry and git dependencies.
RUN --mount=type=cache,target=/usr/local/cargo/registry \\
 --mount=type=cache,target=/usr/local/cargo/git \\
 cargo build --release

# Now, copy the actual source code and build the final binaries.
# This will be much faster as dependencies are already compiled.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \\
 --mount=type=cache,target=/usr/local/cargo/git \\
 --mount=type=cache,target=/usr/src/anvil/target,id=anvil-target \\
 cargo build --release --bin anvil --bin admin

# Stage 2: Create the final, minimal image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y libssl3 curl && rm -rf /var/lib/apt/lists/*

# Copy the compiled binaries from the builder stage's cached target directory
COPY --from=builder /usr/src/anvil/target/release/anvil /usr/local/bin/anvil
COPY --from=builder /usr/src/anvil/target/release/admin /usr/local/bin/admin

# Expose the default gRPC/S3 port and the QUIC P2P port
EXPOSE 50051
EXPOSE 7443/udp

# Set the default command to run the anvil server
CMD ["anvil"]
