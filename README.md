# NebulaCache

NebulaCache is a Rust in-memory cache with a custom TCP protocol, a command-line client, and an optional password-protected web admin console. The workspace is split into three crates:

- **`nebula-protocol`**: Defines the versioned binary request/response protocol and TCP framing shared by the node and client.
- **`nebula-node`**: Provides the asynchronous TCP server, concurrent in-memory store with TTL expiration, and optional password-protected web admin console.
- **`nebulactl`**: Provides the command-line tool for interacting with a node.

> **Current status:** Single-node cache engine with a TCP interface and an optional web admin console. Docker Compose starts three independent nodes; they do not form a cluster and do not share data.

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

The node also accepts a YAML configuration file with `--config PATH`. It can set `listen_addr`, `admin_listen_addr`, `max_key_len`, and `max_value_len`; the default limits are 1 KiB per key and 8 MiB per value. CLI options override the file for the TCP listen address and frame limits.

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

## Use the web admin console

The password-protected web console is disabled by default. Enable it with `--admin-console` and provide a non-empty password with `--admin-password`. It listens on `127.0.0.1:7002` by default; `--admin-console-port` changes the port while the bind address can be set with `admin_listen_addr` in the YAML configuration file.

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001 --admin-console --admin-password '<strong-password>'
```

Open `http://127.0.0.1:7002` to sign in. The console can list and search entries, paginate results, create or edit entries, delete individual entries, and clear the node's cache. It displays keys and values as text and shows each entry's remaining TTL; use the TCP clients for binary values.

The console uses password login and an expiring session cookie, but it does not provide TLS. Keep it bound to loopback unless access is protected by trusted network controls and HTTPS. The password is supplied as a command-line argument, so avoid exposing it through shell history or local process inspection. The TCP protocol itself does not provide authentication or TLS.

## Run three isolated development nodes

```bash
docker compose up --build
```

The nodes are available on host ports `7001`, `7002`, and `7003`. Each node has its own in-memory store. Stop the containers with `Ctrl-C`.

## Current capabilities

- Versioned binary protocol over TCP with `SET`, `GET`, `DELETE`, and `PING` operations
- Concurrent, volatile in-memory key/value store with TTL expiration
- `nebulactl` command-line client and Go, Java, JavaScript, and Python TCP SDKs
- Optional password-protected web admin console for listing, searching, paging, creating, editing, deleting, and clearing entries
- YAML node configuration and configurable protocol key/value size limits
- Unit and integration tests

## Not implemented

Cluster membership, consistent hashing, replication, quorum, failure detection, automatic failover, persistence, authentication for TCP clients, and TLS.

See [architecture](./docs/architecture.md) and [protocol](./docs/protocol.md) for architecture and TCP protocol details.
