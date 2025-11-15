# Stage 1: Build the binaries
FROM rust:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y build-essential pkg-config libssl-dev protobuf-compiler

WORKDIR /usr/src/anvil

# Copy the entire project
COPY . .

# Build the anvil server and the admin CLI in release mode
RUN cargo build --release --bin anvil --bin admin -p anvil-cli

# Stage 2: Create the final, minimal image
FROM rust:latest

# Remove build dependencies and clean up apt caches
RUN apt-get update && apt-get purge -y build-essential pkg-config libssl-dev protobuf-compiler && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the compiled binaries from the builder stage
COPY --from=builder /usr/src/anvil/target/release/anvil /usr/local/bin/anvil
COPY --from=builder /usr/src/anvil/target/release/admin /usr/local/bin/admin
COPY --from=builder /usr/src/anvil/target/release/anvil-cli /usr/local/bin/anvil-cli

# Expose the default gRPC/S3 port and a potential swarm port
EXPOSE 50051
EXPOSE 7443

# Set the default command to run the anvil server
CMD ["anvil"]