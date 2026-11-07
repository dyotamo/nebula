# NebulaCache Python SDK

A synchronous, dependency-free Python client for managing entries through a NebulaCache node's binary TCP protocol. Requires Python 3.10 or newer and uses the Python standard library.

## Start a node

Start a node listening on the default TCP address:

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001
```

TCP client authentication is not provided. TLS is optional on the server and disabled by default; expose plaintext nodes only on trusted interfaces and apply suitable network controls. The optional password-protected web admin console is separate from this TCP SDK. See the [node setup and console instructions](../../README.md#use-the-web-admin-console).

## Install

From this directory, install the package into your environment:

```bash
python -m pip install .
```

## Quick start

```python
from nebulacache import NebulaCache

cache = NebulaCache(
    "127.0.0.1:7001",
    min_pool_size=1,
    max_pool_size=10,
    connect_timeout=5.0,
    request_timeout=5.0,
    acquire_timeout=5.0,
    idle_timeout=60.0,
    max_lifetime=300.0,
    max_retries=2,
)
try:
    cache.set("greeting", "hello", ttl_secs=60)
    value = cache.get("greeting")
    print(value.decode("utf-8") if value is not None else "missing")

    if cache.delete("greeting"):
        print("entry deleted")

    print(cache.ping())
finally:
    cache.close()
```

`set` accepts text or bytes-like values. Text is encoded as UTF-8; `get` always returns raw `bytes` so arbitrary binary values are preserved. A TTL of `0` (the default) means the entry does not expire.

## API

- `NebulaCache(address="127.0.0.1:7001", timeout=5.0, ...)`: create a client with a `host:port` node address and optional pool/timeout/retry settings in seconds. Pass `ssl_context` to enable TLS. IPv6 addresses use the `[host]:port` format.
- `close()`: close idle and active sockets; later operations raise `CacheConnectionError`.
- `ping() -> bool`: returns `True` if the node responds with `PONG`; otherwise raises an SDK exception.
- `set(key, value, ttl_secs=0) -> None`: store a non-empty string key and a text or bytes-like value.
- `get(key) -> bytes | None`: return the raw value bytes, or `None` if the key is missing or expired.
- `delete(key) -> bool`: return `True` if an entry was deleted, or `False` if it did not exist.

Keys are UTF-8 encoded and limited to 1 KiB. Values are limited to 8 MiB by the protocol. Each node has its own volatile cache; nodes do not share entries.

## TLS

When the server's TCP TLS listener is enabled, pass a standard-library `ssl.SSLContext` to the client. Use a context configured with your private CA when needed; hostname and certificate verification follow the context's settings.

```python
import ssl
from nebulacache import NebulaCache

cache = NebulaCache(
    "cache.example:7001",
    ssl_context=ssl.create_default_context(cafile="ca.pem"),
)
```

Omit `ssl_context` to use plaintext TCP.

## CLI example

The example CLI accepts `--tls-ca` or the `NEBULA_TLS_CA` environment variable to load a private CA certificate and enable TLS:

```bash
python examples/cache_cli.py --tls-ca "$(mkcert -CAROOT)/rootCA.pem" ping
NEBULA_TLS_CA="$(mkcert -CAROOT)/rootCA.pem" python examples/cache_cli.py set greeting hello --ttl 60
```

Without a CA option, the example uses plaintext TCP.

## Connection pooling and resilience

Connections are reused for sequential requests. Each socket carries one request at a time; concurrent calls use separate pooled sockets. The pool opens connections lazily, keeps at least `min_pool_size` once initialized, and is owned by the client until `close()`.

Options default to `min_pool_size=0`, `max_pool_size=10`, `connect_timeout=5.0`, `request_timeout=5.0`, `acquire_timeout=5.0`, `idle_timeout=60.0`, `max_lifetime=300.0`, and `max_retries=2`. The legacy `timeout` argument sets the connect and request timeouts; acquisition has its own default.

Only GET is retried, with bounded exponential full jitter and a shared per-client retry budget. SET and DELETE are never automatically replayed because a timeout after sending can leave their outcome ambiguous. Broken and expired connections are discarded and replaced as needed.

## Exceptions

- `CacheConnectionError`: the node could not be reached or the request timed out.
- `CacheProtocolError`: the node returned a malformed or incomplete protocol response.
- `CacheResponseError`: the node returned an unsuccessful protocol status; `status` contains its numeric status.
- `NebulaCacheError`: common base class for SDK errors.
- `TypeError` and `ValueError`: invalid key, value, TTL, or client configuration.

## Test

Run the SDK unit tests from this directory:

```bash
python -m unittest discover -s tests -v
```
