FROM rust:1.83-bookworm AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY crates ./crates
RUN cargo build --release -p nebula-node

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates bash \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/nebula-node /usr/local/bin/nebula-node
EXPOSE 7001
ENTRYPOINT ["nebula-node"]
USER 1000:1000
CMD ["--listen", "0.0.0.0:7001"]
