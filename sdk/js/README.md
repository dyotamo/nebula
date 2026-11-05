# NebulaCache JavaScript SDK

A dependency-free Node.js client for the NebulaCache binary TCP protocol. Requires Node.js 18 or newer and uses the built-in `net` module.

## Start a node

Start a node listening on the default TCP address:

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001
```

The TCP protocol does not provide authentication or TLS. Expose the node only on trusted interfaces and apply suitable network controls. Nodes also include an optional password-protected web admin console for inspecting and managing entries; it is separate from this TCP SDK. See the [node setup and console instructions](../../README.md#use-the-web-admin-console).

## Install

Install the local SDK package into a Node.js project:

```bash
npm install /path/to/nebulacache/sdk/js
```

## Quick start

```js
const { NebulaCache } = require('nebulacache');

async function main() {
  const cache = new NebulaCache('127.0.0.1:7001');

  await cache.set('greeting', 'hello', 60);
  const value = await cache.get('greeting');
  console.log(value === null ? 'missing' : value.toString('utf8'));

  if (await cache.delete('greeting')) {
    console.log('entry deleted');
  }

  console.log(await cache.ping());
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```

`set` accepts strings, `Buffer` instances, or `Uint8Array` values. Strings and keys are encoded as UTF-8. `get` returns a `Buffer` so binary values are preserved, or `null` if the key is missing or expired. A TTL of `0` (the default) means the entry does not expire. TTLs can be provided as safe integer numbers or `BigInt` values up to the protocol's unsigned 64-bit maximum.

## API

- `new NebulaCache(address = '127.0.0.1:7001', timeout = 5000)`: create a client with a `host:port` address and per-operation timeout in milliseconds. IPv6 addresses use the `[host]:port` format.
- `await ping()`: returns `true` if the node responds with `PONG`; otherwise rejects with an SDK exception.
- `await set(key, value, ttlSecs = 0)`: store a non-empty string key and a string or binary value.
- `await get(key)`: return the raw value as a `Buffer`, or `null` if it does not exist.
- `await delete(key)`: return `true` if an entry was deleted, or `false` if it did not exist.

Keys are UTF-8 encoded and limited to 1 KiB. Values are limited to 8 MiB by the protocol. Each node has its own volatile cache; nodes do not share entries.

## CLI example

The example supports `ping`, `set`, `get`, and `delete` commands. It uses `NEBULA_ADDR` if set, or defaults to `127.0.0.1:7001`:

```bash
node examples/cache_cli.js ping
node examples/cache_cli.js set greeting hello --ttl 60
node examples/cache_cli.js get greeting
node examples/cache_cli.js delete greeting
node examples/cache_cli.js --address 127.0.0.1:7002 ping
```

## Exceptions

- `CacheConnectionError`: the node could not be reached or the request timed out.
- `CacheProtocolError`: the node returned a malformed or incomplete protocol response.
- `CacheResponseError`: the node returned an unsuccessful protocol status; `status` contains its numeric status.
- `NebulaCacheError`: common base class for SDK errors.
- `TypeError` and `RangeError`: invalid key, value, TTL, timeout, or client configuration.

## Test

Run the SDK tests from this directory:

```bash
npm test
```
