# NebulaCache

NebulaCache is a single-node, in-memory key/value cache served over a versioned binary TCP protocol. It includes a Rust server and command-line client, TCP SDKs for Go, Java, JavaScript, and Python, optional TLS, periodic snapshot persistence, a Prometheus metrics endpoint, health monitoring, and an optional password-protected web admin console.

## Current system

Each server process owns an independent cache. The Docker Compose setup runs one development node; it does not create a cluster or share data between nodes. Snapshot mode can restore data after a restart, but it is not replication or a continuously written log. In volatile mode, a restart loses all entries.

The workspace contains these Rust crates:

- **`nebula-protocol`**: Versioned binary protocol and TCP framing.
- **`nebula-core`**: Concurrent in-memory cache with TTL expiration and snapshot support.
- **`nebula-health`**: Application lifecycle and component health state.
- **`nebula-watchdog`**: Worker heartbeat monitoring.
- **`nebula-server`**: TCP server, TLS, persistence, metrics exporter, and optional admin console.
- **`nebula-client`**: Command-line TCP client.

## Capabilities and limits

- `PING`, `SET`, `GET`, and `DELETE` operations over TCP.
- Binary keys and values up to 1 KiB and 8 MiB respectively. TTLs are in seconds; `0` means no expiration.
- Optional TLS for TCP connections. The server does not request client certificates, and TCP client authentication is not implemented.
- Optional snapshots with configurable interval and retention, restored at startup and written once more on graceful shutdown.
- Prometheus metrics at `/metrics` and a health-aware dashboard in the web admin console.
- Lifecycle management, worker health monitoring, graceful shutdown, and systemd readiness/watchdog notifications.

There is no cluster membership, node-to-node routing, replication, quorum, automatic failover, or TCP client authentication. Nodes do not share data.

## Requirements

- Rust stable (the Docker build currently uses Rust 1.83).
- Docker and Docker Compose are optional for the included development container.
- Go, Java/Maven, Node.js, and Python are only needed to build or test their respective SDKs.

## Build and test the Rust workspace

From the workspace root:

```bash
cargo fmt --all
cargo test --workspace
cargo build --release --workspace
```

## Run a server

Run a node on the default TCP address, `0.0.0.0:7001`:

```bash
cargo run -p nebula-server
```

The TCP listener is plaintext by default. Since the default bind address listens on all interfaces and the protocol has no client authentication, use a loopback or otherwise trusted network binding unless TLS and suitable network controls are configured.

The server accepts an optional YAML configuration path as its only command-line argument:

```bash
cargo run -p nebula-server -- ./server.yml
```

A configuration can use environment variables in string values as `${NAME}` or `${NAME:-default}`. For example, this enables the admin console with a password supplied through the environment, enables snapshot persistence, and leaves the metrics exporter enabled:

```yaml
listen_addr: "127.0.0.1:7001"
shutdown_timeout_secs: 30
log_level: info

admin_console:
  enabled: true
  password: ${NEBULA_ADMIN_PASSWORD}
  listen_addr: "127.0.0.1:7002"
  metrics_dashboard: true

tls:
  enabled: false

persistence:
  mode: snapshot
  snapshot_dir: ./data/snapshots
  snapshot_interval_secs: 60
  snapshot_retention: 3

metrics:
  enabled: true
  listen_addr: "127.0.0.1:7003"
```

Start it by setting the password in the environment:

```bash
NEBULA_ADMIN_PASSWORD='choose-a-secret' cargo run -p nebula-server -- ./server.yml
```

Configuration defaults are: TCP `0.0.0.0:7001`; admin console disabled on `127.0.0.1:7002`; metrics enabled on `127.0.0.1:7003`; volatile persistence; and a 30-second graceful-shutdown timeout. The admin console requires a non-empty password when enabled. Unknown YAML settings are rejected.

### Enable TCP TLS

Set certificate and private-key paths in the config. Both are required when TLS is enabled:

```yaml
tls:
  enabled: true
  certificate: /etc/nebula/server-cert.pem
  private_key: /etc/nebula/server-key.pem
```

TLS applies only to the TCP listener; it does not enable HTTPS for the admin console or metrics endpoint. Configure TCP clients to trust the server certificate. The Rust command-line client accepts a CA file through `--tls-ca` or `NEBULA_TLS_CA`; the SDKs accept their respective TLS configuration options. TLS is disabled by default.

## Use the command-line client

The client defaults to `127.0.0.1:7001`. Select another address with `--address` / `-a` or `NEBULA_ADDR`:

```bash
cargo run -p nebula-client -- ping
cargo run -p nebula-client -- set greeting hello
cargo run -p nebula-client -- get greeting
cargo run -p nebula-client -- set temporary value 30
cargo run -p nebula-client -- delete greeting
cargo run -p nebula-client -- --address 127.0.0.1:7001 ping
```

The CLI accepts text keys and values. Use an SDK or the binary protocol directly for arbitrary binary data.

## SDKs

Each SDK speaks the same TCP protocol and supports ping, set, get, delete, TTLs, and optional TLS:

- Go: `./sdk/go`
- Java: `./sdk/java`
- JavaScript: `./sdk/js`
- Python: `./sdk/python`

Each SDK directory contains its install, usage, TLS, and test instructions.

## Metrics and admin console

The Prometheus exporter is enabled by default and listens on `127.0.0.1:7003`. Scrape `http://127.0.0.1:7003/metrics`. It reports request counts and latency, active connections, cache size, application/component health, and snapshot/recovery metrics. Configure it with `metrics.enabled` and `metrics.listen_addr`.

The web admin console is disabled by default. Enable `admin_console.enabled` and set a non-empty `admin_console.password`; its default address is `127.0.0.1:7002`. Sign in at `http://127.0.0.1:7002`. The console can list, search, and page through entries, set or edit entries, delete entries, and clear the cache. It displays values as text, so use TCP clients for binary values. Its metrics dashboard can be toggled with `admin_console.metrics_dashboard`.

The console uses an expiring session cookie but does not provide TLS. Keep it on loopback or protect access with trusted network controls and HTTPS. Avoid putting passwords directly in command-line arguments; use environment substitution in the YAML configuration. The metrics endpoint also uses plaintext HTTP and should be kept on a trusted interface/network.

## Persistence

Persistence is disabled by default (`persistence.mode: volatile`). In `snapshot` mode, the server restores the newest valid snapshot at startup, writes snapshots at the configured interval, retains the configured number of snapshots, and writes a final snapshot during graceful shutdown. The default directory is `data/snapshots`, the interval is 60 seconds, and retention is three files. Snapshot mode requires a non-empty directory, a positive interval, and positive retention. Treat snapshots as recovery points: changes made after the last successful snapshot may be lost on an ungraceful stop.

## Docker Compose

```bash
docker compose up --build
```

This starts a single development container and publishes its TCP port on host port `7001`; it is not a three-node cluster. The container configuration enables TCP TLS and uses development credentials/configuration, so it is not suitable for production deployment. For a deployment, provide your own configuration, certificates, secrets, storage, and network policy.

## SDK checks

Run SDK checks from each SDK directory as applicable:

- Go: `go test ./...` and `go build ./...`
- Java: `mvn test` and `mvn package`
- JavaScript: `npm test`
- Python: `python -m unittest discover -s tests -v`

See `./docs/protocol.md` for the wire format and operation details.
