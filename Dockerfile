FROM rust:1.83-bookworm AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY crates ./crates
RUN cargo build --release -p nebula-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates bash \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/nebula-server /usr/local/bin/nebula-server
COPY config/nebula-server.container.yml /etc/nebulacache/config.yml
EXPOSE 7001
ENTRYPOINT ["nebula-server"]
CMD ["/etc/nebulacache/config.yml"]
USER 1000:1000
