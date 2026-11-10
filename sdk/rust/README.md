# NebulaCache Rust SDK

An asynchronous Rust client for the NebulaCache v1 binary TCP protocol. It supports PING, SET, GET, and DELETE, binary values, TTLs, connection pooling, TLS, token authentication, and bounded GET retries.

## Before you start

Start a NebulaCache server using the repository's [server instructions](../../README.md#run-the-server). The default address is `127.0.0.1:7001`. This SDK provides cache operations over the binary TCP protocol; it does not provide user or token administration. Token authentication requires TLS and a trusted CA certificate.

## Use the SDK

This crate is included in the repository workspace. Add a path dependency to another crate in the repository:

```toml
[dependencies]
nebulacache = { path = "../sdk/rust" }
```

The SDK uses Tokio and the workspace's NebulaCache protocol crate. From the repository root, run its tests and build with:

```bash
cargo test -p nebulacache
cargo build -p nebulacache
```

## Quick start

```rust
use nebulacache::NebulaCache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = NebulaCache::new("127.0.0.1:7001")?;

    cache.ping().await?;
    cache.set_string_with_ttl("greeting", "hello", 60).await?;

    match cache.get("greeting").await? {
        Some(value) => println!("{}", String::from_utf8_lossy(&value)),
        None => println!("not found"),
    }

    println!("deleted: {}", cache.delete("greeting").await?);
    cache.close().await;
    Ok(())
}
```

`set` stores raw bytes without expiration; `set_string` stores UTF-8 text. Use `set_with_ttl` or `set_string_with_ttl` to specify a TTL in seconds. A TTL of `0` means no expiration. `get` returns `Some(Vec<u8>)` for a stored value, including an empty value, or `None` when the key is missing or expired.

## API and behavior

- `NebulaCache::new(address)`: create a client for a `host:port` address. `Config::default()` uses `127.0.0.1:7001`.
- `NebulaCache::with_config(config)`: configure connection limits, timeouts, retries, TLS, and an optional token.
- `ping().await`: check that the server responds with `PONG`.
- `set(key, value).await` / `set_string(key, value).await`: store binary or UTF-8 data without expiration.
- `set_with_ttl(key, value, ttl_secs).await` / `set_string_with_ttl(...)`: store data with a TTL in seconds.
- `get(key).await`: return raw bytes or `None` for a miss.
- `delete(key).await`: return whether an entry was removed.
- `close().await`: close idle connections, interrupt active requests, and reject subsequent operations.

Keys must be non-empty UTF-8 strings no longer than 1,024 bytes. Values may contain up to 8 MiB. TTLs are unsigned 64-bit integers in seconds. The server's configured cache capacity may additionally reject a SET.

## TLS and token authentication

Set `Config::tls_ca_path` to a PEM file containing trusted CA certificates to enable TLS. The server certificate and hostname are verified. For a server requiring token authentication, also set `Config::token`; tokens are only accepted when TLS is enabled.

```rust
use nebulacache::{Config, NebulaCache};
use std::path::PathBuf;

let mut config = Config::default();
config.address = "cache.example:7001".to_owned();
config.tls_ca_path = Some(PathBuf::from("./ca.pem"));
config.token = std::env::var("NEBULA_TOKEN").ok();
let cache = NebulaCache::with_config(config)?;
```

Do not disable certificate verification or commit tokens or private keys. Leave `tls_ca_path` unset for plaintext servers that do not require authentication.

## Pooling, timeouts, and retries

`Config` accepts `address`, `max_pool_size`, `connect_timeout`, `request_timeout`, `acquire_timeout`, `idle_timeout`, `max_lifetime`, `max_retries`, `tls_ca_path`, and `token`. Defaults are a pool maximum of 10 connections, five-second connect/request/acquisition timeouts, a 60-second idle timeout, a 300-second maximum connection lifetime, and two additional GET attempts.

Connections are opened on demand and reused; each connection handles one request at a time. Idle connections and connections past their maximum lifetime are retired. Only GET is retried after transient transport failures. SET and DELETE are not retried because a transport failure can happen after the server applies a mutation.

## Included example

The example CLI supports PING, SET, GET, and DELETE. It reads `NEBULA_ADDR` and `NEBULA_TLS_CA` when the corresponding command-line options are omitted. For token-authenticated connections, set `NEBULA_TOKEN` along with TLS options.

Run from the repository root:

```bash
cargo run -p nebulacache --example cache_cli -- ping
cargo run -p nebulacache --example cache_cli -- set greeting hello --ttl 60
cargo run -p nebulacache --example cache_cli -- get greeting
cargo run -p nebulacache --example cache_cli -- delete greeting
cargo run -p nebulacache --example cache_cli -- --address 127.0.0.1:7001 ping
cargo run -p nebulacache --example cache_cli -- --tls-ca ./server-ca.pem ping
NEBULA_TLS_CA=./server-ca.pem NEBULA_TOKEN="$NEBULA_TOKEN" cargo run -p nebulacache --example cache_cli -- ping
```

## Errors

- `NebulaCacheError::Connection`: transport, timeout, or pool acquisition failure.
- `NebulaCacheError::Protocol`: malformed or incomplete protocol response.
- `NebulaCacheError::Response`: unsuccessful server response; includes the status and message.
- `NebulaCacheError::Closed`: operation attempted after closing the client.
- `NebulaCacheError::InvalidInput`: invalid address, configuration, key, or value length.
