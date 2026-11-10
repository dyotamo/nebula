# NebulaCache Python SDK

A synchronous, dependency-free Python 3.10+ client for the NebulaCache v1 binary TCP protocol. The package uses only the standard library and supports cache operations, TTLs, TLS, token authentication, connection pooling, and bounded GET retries.

## Before you start

Start the server using [the repository's server setup instructions](./../../README.md#run-the-server). The default address is `127.0.0.1:17171`. This SDK provides PING, SET, GET, and DELETE over the binary TCP protocol; it does not provide user or token administration. When server token authentication is enabled, each new connection needs an API token; TLS is optional. For user and token management, enable the server's HTTP admin listener and access it locally or through a TLS-terminating reverse proxy.

## Install

From this directory, install the package in the active environment:

```bash
python -m pip install .
```

## Quick start

```python
from nebulacache import NebulaCache

cache = NebulaCache("127.0.0.1:17171")
try:
    cache.ping()
    cache.set("greeting", "hello", ttl_secs=60)

    value = cache.get("greeting")
    print(value.decode("utf-8") if value is not None else "missing")
    print("deleted:", cache.delete("greeting"))
finally:
    cache.close()
```

`set` accepts text and bytes-like values (`bytes`, `bytearray`, or `memoryview`). Text uses UTF-8. `get` always returns raw `bytes` or `None` for a missing/expired key. TTL `0` means the value does not expire.

## API and behavior

- `NebulaCache(address="127.0.0.1:17171", timeout=5.0, ...)`: create a client. `address` is `host:port`; bracket IPv6 literals as `[host]:port`.
- `ping() -> bool`: return `True` when the server replies with `PONG`.
- `set(key, value, ttl_secs=0) -> None`: store a non-empty string key and text or binary value.
- `get(key) -> bytes | None`: return raw bytes or `None` when missing/expired.
- `delete(key) -> bool`: return true if an entry was deleted, false if absent.
- `close()`: close the connection pool. Calls after close raise `CacheConnectionError`.

Keys must be non-empty UTF-8 strings up to 1,024 encoded bytes. Values may be up to 8 MiB. TTL is a non-negative unsigned 64-bit integer count of seconds. The server's cache memory limit can reject SET independently of the protocol's value limit.

## TLS and token authentication

Pass a standard-library `ssl.SSLContext` using `ssl_context` to encrypt TCP connections. For a private CA, create a verifying context with `ssl.create_default_context(cafile=...)`. If server authentication is enabled, also pass `token`; the SDK authenticates each newly opened connection before issuing cache requests. TLS is optional for token authentication, but plaintext connections expose tokens and cache traffic. Do not use an unverified context when TLS is enabled.

```python
import os
import ssl
from nebulacache import NebulaCache

cache = NebulaCache(
    "cache.example:17171",
    ssl_context=ssl.create_default_context(cafile="ca.pem"),
    token=os.environ["NEBULA_TOKEN"],
)
try:
    cache.ping()
finally:
    cache.close()
```

Omit `ssl_context` to use plaintext TCP. Authentication can still be enabled, but use plaintext only on a trusted network because tokens and cache traffic are unencrypted.

## Pooling, timeouts, and retries

Durations are expressed in seconds. Constructor options include `min_pool_size`, `max_pool_size`, `connect_timeout`, `request_timeout`, `acquire_timeout`, `idle_timeout`, `max_lifetime`, `max_retries`, `ssl_context`, and `token`. Defaults are pool size 0–10, five-second connect/request/acquire timeouts, 60-second idle timeout, 300-second maximum lifetime, and two retries. `timeout` is the legacy convenience setting for connect and request timeouts.

Connections are opened lazily, reused, and owned by the client until `close()`. One connection carries one request at a time; concurrent calls use separate pool connections. Idle, expired, and failed sockets are retired. Only GET is retried after transient transport failures, with bounded exponential full jitter and a shared retry budget. SET and DELETE are not retried because a timeout may happen after a mutation has been applied.

## Example CLI

The included CLI supports `ping`, `set`, `get`, and `delete`. It defaults to `NEBULA_ADDR` or `127.0.0.1:17171`. Use `--tls-ca` or `NEBULA_TLS_CA` to load a CA PEM and enable verified TLS:

```bash
python ./examples/cache_cli.py ping
python ./examples/cache_cli.py set greeting hello --ttl 60
python ./examples/cache_cli.py get greeting
python ./examples/cache_cli.py delete greeting
python ./examples/cache_cli.py --address 127.0.0.1:17171 ping
python ./examples/cache_cli.py --tls-ca /path/to/ca.pem ping
NEBULA_TLS_CA=/path/to/ca.pem python ./examples/cache_cli.py ping
```

The example CLI currently does not accept a token option. Use the `NebulaCache` API for authenticated connections.

## Errors

- `CacheConnectionError`: connection, acquisition, or request timeout failure.
- `CacheProtocolError`: malformed or incomplete response.
- `CacheResponseError`: unsuccessful server status; inspect its `status` attribute.
- `NebulaCacheError`: common base class for SDK errors.
- `TypeError` / `ValueError`: invalid arguments, key/value/TTL, options, or TLS/auth configuration.

## Test

Run the SDK unit tests from this directory:

```bash
python -m unittest discover -s tests -v
```
