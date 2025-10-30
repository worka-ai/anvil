# This Dockerfile is for the runtime image only.
# It expects that the binaries have already been compiled on the host.
# The path to the binaries can be passed in via the BINARY_PATH build argument.
ARG BINARY_PATH=./target/release

FROM debian:bookworm-slim

# Install runtime dependencies required by the host-built binaries and tests
# - libssl3: OpenSSL 3 (TLS)
# - libgcc-s1, libstdc++6: C++ runtime and GCC support libs used by glibc-linked Rust binaries
# - ca-certificates: TLS roots for outbound HTTPS in tests/clients
# - curl: used by the container healthcheck in docker-compose.test.yml
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libssl3 \
        libgcc-s1 \
        libstdc++6 \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the pre-compiled binaries from the build context into the image.
COPY ${BINARY_PATH}/anvil /usr/local/bin/anvil
COPY ${BINARY_PATH}/admin /usr/local/bin/admin

# Expose the default gRPC/S3 port and the QUIC P2P port
EXPOSE 50051
EXPOSE 7443/udp

# Set the default command to run the anvil server
CMD ["anvil"]
