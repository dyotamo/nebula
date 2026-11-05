# NebulaCache

NebulaCache is a Rust in-memory cache with a custom TCP protocol, an optional REST API, and a command-line client. The workspace is split into three crates:

- **`nebula-protocol`**: Defines the versioned binary request/response protocol and TCP framing shared by the node and client.
- **`nebula-node`**: Provides the asynchronous TCP server, optional HTTP REST API, and concurrent in-memory store with TTL expiration.
- **`nebulactl`**: Provides the command-line tool for interacting with a node.

> **Current status:** Phase 1 — single-node cache engine with TCP and optional REST interfaces. Docker Compose starts three independent nodes; they do not form a cluster and do not share data.

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

## Use the REST API

The REST API is disabled by default. Enable it with `--rest-api`; it listens on `0.0.0.0:7002` by default. Use `--rest-listen HOST:PORT` to choose another address. Bind to `127.0.0.1` when the API should only be reachable locally:

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001 --rest-api --rest-listen 127.0.0.1:7002
```

The API provides `GET /v1/ping` and these key operations:

- `PUT /v1/keys/{key}`: Store the raw request body as the value. Optionally set `ttl_secs` in the query string; `0` or an omitted TTL means no expiration.
- `GET /v1/keys/{key}`: Return the value as an `application/octet-stream` response, or `404` if the key is missing.
- `DELETE /v1/keys/{key}`: Delete the key, returning `204` when deleted or `404` when missing.

Example requests:

```bash
curl http://127.0.0.1:7002/v1/ping
curl -X PUT --data-binary 'hello' 'http://127.0.0.1:7002/v1/keys/greeting?ttl_secs=60'
curl http://127.0.0.1:7002/v1/keys/greeting
curl -X DELETE http://127.0.0.1:7002/v1/keys/greeting
```

The OpenAPI specification is in `./crates/nebula-node/openapi/openapi.yaml` and is also served at `/openapi.yaml` when the REST API is enabled. The `nebulactl` client currently uses the TCP protocol.

## Run three isolated development nodes

```bash
docker compose up --build
```

The nodes are available on host ports `7001`, `7002`, and `7003`. Each node has its own in-memory store. Stop the containers with `Ctrl-C`.

## Phase 1 capabilities

- Versioned binary protocol over TCP
- Optional REST API for health checks and key operations
- `SET`, `GET`, `DELETE`, and `PING` operations over TCP and HTTP
- Concurrent in-memory key/value store
- Lazy TTL expiration
- Unit and integration tests

## Not implemented yet

Cluster membership, consistent hashing, replication, quorum, failure detection, automatic failover, persistence, authentication, and TLS.

See [architecture](./docs/architecture.md) and [protocol](./docs/protocol.md) for architecture and TCP protocol details.
