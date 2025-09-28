# Use the official Rust image as a builder
FROM rust:1.77 as builder

# Create a new empty shell project
WORKDIR /usr/src/anvil
COPY . .

# Build the project. This will also cache dependencies.
RUN cargo build --release

# The final, smaller image for the runtime
FROM debian:bullseye-slim

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/anvil/target/release/anvil /usr/local/bin/anvil

# Expose the gRPC port that the server will listen on
EXPOSE 50051

# Set the binary as the entrypoint
CMD ["anvil"]
