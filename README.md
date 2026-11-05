# NebulaCache

NebulaCache is a Rust in-memory cache with a custom TCP protocol, a cache-node server, and a command-line client. The workspace is split into three crates:

- **`nebula-protocol`**: Defines the versioned binary request/response protocol and TCP framing shared by the node and client.
- **`nebula-node`**: Provides the asynchronous TCP server and concurrent in-memory store, including TTL expiration.
- **`nebulactl`**: Provides a reusable client library and the `nebulactl` command-line tool for interacting with a node.

> **Current status:** Phase 1 — single-node cache engine and custom TCP protocol. Docker Compose starts three independent nodes; they do not form a cluster and do not share data.

## Requirements

- Rust stable (1.83 or newer recommended)
- Docker and Docker Compose (optional, for running the development nodes)

## Build and test

From the workspace root:

```bash
cargo fmt --all
cargo test --workspace
cargo build --release --workspace
```

## Run a node

Start a node listening on `127.0.0.1:7001`:

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001
```

## Use the command-line client

In another terminal, `nebulactl` connects to `127.0.0.1:7001` by default. Set `NEBULA_ADDR` or pass `--address` / `-a` to select another node:

```bash
cargo run -p nebulactl -- ping
cargo run -p nebulactl -- set greeting hello
cargo run -p nebulactl -- get greeting
cargo run -p nebulactl -- set temporary value 30
cargo run -p nebulactl -- delete greeting
cargo run -p nebulactl -- --address 127.0.0.1:7001 ping
```

The optional `set` TTL is in seconds; omit it or use `0` for a key without expiration.

## Run three isolated development nodes

```bash
docker compose up --build
```

The nodes are available on host ports `7001`, `7002`, and `7003`. Each node has its own in-memory store. Stop the containers with `Ctrl-C`.

## Phase 1 capabilities

- Versioned binary protocol over TCP
- `SET`, `GET`, `DELETE`, and `PING` operations
- Concurrent in-memory key/value store
- Lazy TTL expiration
- Unit and integration tests

## Not implemented yet

Cluster membership, consistent hashing, replication, quorum, failure detection, automatic failover, persistence, authentication, and TLS.

See [docs/architecture.md](docs/architecture.md) and [docs/protocol.md](docs/protocol.md) for architecture and protocol details.
