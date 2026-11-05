# NebulaCache

Distributed in-memory cache engine, built from scratch in Rust.

> Current status: Phase 1 — single-node engine and custom TCP protocol.
> The three Docker Compose containers are independent nodes, not yet a cluster.

## Requirements
- Rust stable (1.83+ recommended)
- Docker + Docker Compose (optional)

## Build and test
```bash
cargo fmt --all
cargo test --workspace
cargo build --release --workspace
```

## Run one node
```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001
```

## Run three isolated development nodes
```bash
docker compose up --build
```
Ports: 7001, 7002, 7003.

## Phase 1 scope
- Custom versioned binary protocol over TCP
- SET, GET, DELETE and PING
- Concurrent in-memory map
- Lazy TTL expiration
- Unit and integration tests

## Not implemented yet
Consistent hashing, gossip membership, replication, quorum, failure detection,
automatic failover, persistence, authentication and TLS.

See [docs/architecture.md](docs/architecture.md) and [docs/protocol.md](docs/protocol.md).
