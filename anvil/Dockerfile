FROM debian:bookworm-slim

# Install runtime dependencies (e.g., OpenSSL)
RUN apt-get update && apt-get install -y libssl3 && rm -rf /var/lib/apt/lists/*

# Copy the pre-compiled binaries from the build context into the image.
# The `cargo build` command should be run on the host before building this image.
COPY ./target/release/anvil /usr/local/bin/anvil
COPY ./target/release/admin /usr/local/bin/admin

# Expose the default gRPC/S3 port and the QUIC P2P port
EXPOSE 50051
EXPOSE 7443/udp

# Set the default command to run the anvil server
CMD ["anvil"]
