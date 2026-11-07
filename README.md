# NebulaCache

NebulaCache is a Rust in-memory cache with a custom TCP protocol, a command-line client, and an optional password-protected web admin console. The workspace is split into four crates:

- **`nebula-protocol`**: Defines the versioned binary request/response protocol and TCP framing shared by the server and client.
- **`nebula-core`**: Provides the concurrent in-memory cache engine with TTL expiration.
- **`nebula-server`**: Provides the asynchronous TCP server and optional password-protected web admin console; it uses `nebula-core` for cache operations.
- **`nebula-client`**: Provides the command-line tool for interacting with a server.

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
cargo run -p nebula-server -- --listen 127.0.0.1:7001
```

The node also accepts a YAML configuration file by passing its path as the optional positional argument, for example `cargo run -p nebula-server -- server.yml`. The file can set `listen_addr`, admin console settings, TLS settings, shutdown timeout, and persistence settings. The TCP listener uses plaintext unless TLS is enabled in the configuration.

To enable TLS for the TCP protocol, add PEM certificate and private key paths to the YAML configuration. TLS applies to the TCP listener and all TCP SDKs must also be configured for TLS; the admin console remains separate and does not gain TLS from this setting.

```yaml
tls:
  enabled: true
  certificate: /etc/nebula/server-cert.pem
  private_key: /etc/nebula/server-key.pem
```

Both `tls.certificate` and `tls.private_key` are required when TLS is enabled. The default is disabled, preserving plaintext TCP connections.

## Use the command-line client

In another terminal, `nebula-client` connects to `127.0.0.1:7001` by default. Set `NEBULA_ADDR` or pass `--address` / `-a` to select another node:

```bash
cargo run -p nebula-client -- ping
cargo run -p nebula-client -- set greeting hello
cargo run -p nebula-client -- get greeting
cargo run -p nebula-client -- set temporary value 30
cargo run -p nebula-client -- delete greeting
cargo run -p nebula-client -- --address 127.0.0.1:7001 ping
```

The optional `set` TTL is in seconds; omit it or use `0` for a key without expiration.

## Use the web admin console

The password-protected web console is disabled by default. Enable it with `--admin-console` and provide a non-empty password with `--admin-password`. It listens on `127.0.0.1:7002` by default; `--admin-console-port` changes the port while the bind address can be set with `admin_listen_addr` in the YAML configuration file.

```bash
cargo run -p nebula-server -- --listen 127.0.0.1:7001 --admin-console --admin-password '<strong-password>'
```

Open `http://127.0.0.1:7002` to sign in. The console can list and search entries, paginate results, create or edit entries, delete individual entries, and clear the node's cache. It displays keys and values as text and shows each entry's remaining TTL; use the TCP clients for binary values.

The console uses password login and an expiring session cookie, but it does not provide TLS. Keep it bound to loopback unless access is protected by trusted network controls and HTTPS. The password is supplied as a command-line argument, so avoid exposing it through shell history or local process inspection. TCP client authentication is not provided. TCP TLS is optional and must be enabled separately as described above.

## Run three isolated development nodes

```bash
docker compose up --build
```

The nodes are available on host ports `7001`, `7002`, and `7003`. Each node has its own in-memory store. Stop the containers with `Ctrl-C`.

## Current capabilities

- Versioned binary protocol over TCP with `SET`, `GET`, `DELETE`, and `PING` operations
- Concurrent, volatile in-memory key/value store with TTL expiration
- `nebula-client` command-line client and Go, Java, JavaScript, and Python TCP SDKs
- Optional password-protected web admin console for listing, searching, paging, creating, editing, deleting, and clearing entries
- YAML node configuration and configurable protocol key/value size limits
- Unit and integration tests

## Not implemented

Cluster membership, consistent hashing, replication, quorum, failure detection, automatic failover, and authentication for TCP clients.

See [architecture](./docs/architecture.md) and [protocol](./docs/protocol.md) for architecture and TCP protocol details.
