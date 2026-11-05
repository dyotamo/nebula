# NebulaCache Python SDK

A synchronous, dependency-free Python client for managing entries through a NebulaCache node's binary TCP protocol. Requires Python 3.10 or newer and uses the Python standard library.

## Start a node

Start a node listening on the default TCP address:

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001
```

The TCP protocol does not provide authentication or TLS. Expose the node only on trusted interfaces and apply suitable network controls. Nodes also include an optional password-protected web admin console for inspecting and managing entries; it is separate from this TCP SDK. See the [node setup and console instructions](../../README.md#use-the-web-admin-console).

## Install

From this directory, install the package into your environment:

```bash
python -m pip install .
```

## Quick start

```python
from nebulacache import NebulaCache

cache = NebulaCache("127.0.0.1:7001")

cache.set("greeting", "hello", ttl_secs=60)
value = cache.get("greeting")
print(value.decode("utf-8") if value is not None else "missing")

if cache.delete("greeting"):
    print("entry deleted")

print(cache.ping())
```

`set` accepts text or bytes-like values. Text is encoded as UTF-8; `get` always returns raw `bytes` so arbitrary binary values are preserved. A TTL of `0` (the default) means the entry does not expire.

## API

- `NebulaCache(address="127.0.0.1:7001", timeout=5.0)`: create a client with a `host:port` node address and per-operation timeout in seconds. IPv6 addresses use the `[host]:port` format.
- `ping() -> bool`: returns `True` if the node responds with `PONG`; otherwise raises an SDK exception.
- `set(key, value, ttl_secs=0) -> None`: store a non-empty string key and a text or bytes-like value.
- `get(key) -> bytes | None`: return the raw value bytes, or `None` if the key is missing or expired.
- `delete(key) -> bool`: return `True` if an entry was deleted, or `False` if it did not exist.

Keys are UTF-8 encoded and limited to 1 KiB. Values are limited to 8 MiB by the protocol. Each node has its own volatile cache; nodes do not share entries.

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
