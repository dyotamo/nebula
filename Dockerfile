# Default Dockerfile for building and running Nebula Server wtih default configuration.
# This Dockerfile is intended for development and testing purposes only.
# For production deployments, please refer to the official documentation for best practices and security recommendations.

FROM rust:1.88-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY sdk/rust ./sdk/rust
COPY assets ./assets
RUN cargo build --locked --release -p nebula-server --bin nebulad

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates bash \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app \
    && chown 1000:1000 /app
WORKDIR /app
COPY --chown=1000:1000 nebulad.toml /app/nebulad.toml
COPY --from=builder /app/target/release/nebulad /usr/local/bin/nebulad
EXPOSE 17171 17172 17173
USER 1000:1000
ENTRYPOINT ["nebulad"]
CMD ["nebulad.toml"]
