# Default Dockerfile for building and running Nebula Server wtih default configuration.
# This Dockerfile is intended for development and testing purposes only.
# For production deployments, please refer to the official documentation for best practices and security recommendations.

FROM rust:1.87-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --locked --release -p nebula-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates bash \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/nebula-server /usr/local/bin/nebula-server
EXPOSE 17171
USER 1000:1000
ENTRYPOINT ["nebula-server"]
