# This Dockerfile is for the runtime image only.
# It expects that the binaries have already been compiled on the host.
# The path to the binaries can be passed in via the BINARY_PATH build argument.
FROM debian:bookworm-slim
ARG BINARY_PATH=./target/release

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

# Stash pre-compiled artifacts from the build context, then locate executables robustly.
COPY ${BINARY_PATH}/ /tmp/build/

# Find the actual executable files named 'anvil' and 'admin' within the copied tree
# and place them at fixed paths in the runtime image.
RUN set -eux; \
    anvil_src=$(find /tmp/build -type f -name anvil -perm -111 | head -n1); \
    admin_src=$(find /tmp/build -type f -name admin -perm -111 | head -n1); \
    test -n "$anvil_src" && test -n "$admin_src"; \
    install -m 0755 "$anvil_src" /usr/local/bin/anvil; \
    install -m 0755 "$admin_src" /usr/local/bin/admin; \
    rm -rf /tmp/build

# Expose the default gRPC/S3 port and the QUIC P2P port
EXPOSE 50051
EXPOSE 7443/udp

# Set the default command to run the anvil server
CMD ["anvil"]
