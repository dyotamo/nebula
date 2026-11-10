# NebulaCache JavaScript SDK

A dependency-free Node.js 18+ client for the NebulaCache v1 binary TCP protocol. The SDK uses built-in `net` and `tls` modules and provides async cache operations, TTLs, TLS, token authentication, a connection pool, and bounded GET retries. The package uses CommonJS.

## Before you start

Start a server using [the repository's server instructions](./../../README.md#run-the-server). The default address is `127.0.0.1:7001`. This SDK provides PING, SET, GET, and DELETE over the binary TCP protocol; it does not provide user or token administration. When server token authentication is enabled, each new connection needs an API token; TLS is optional. For user and token management, enable the server's HTTP admin listener and access it locally or through a TLS-terminating reverse proxy.

## Install

From a Node.js project, install the local package using its repository path:

```bash
npm install /path/to/nebulacache/sdk/js
```

No external runtime packages are required.

## Quick start

```js
const { NebulaCache } = require('nebulacache');

async function main() {
  const cache = new NebulaCache('127.0.0.1:7001');
  try {
    await cache.ping();
    await cache.set('greeting', 'hello', 60);

    const value = await cache.get('greeting');
    console.log(value === null ? 'missing' : value.toString('utf8'));
    console.log('deleted:', await cache.delete('greeting'));
  } finally {
    await cache.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```

`set` accepts strings, `Buffer`, or `Uint8Array`; strings and keys use UTF-8. `get` returns a `Buffer` or `null` for a missing/expired key. TTL `0` means no expiration. TTLs may be safe integer numbers or `BigInt` values up to the unsigned 64-bit maximum.

## API and behavior

- `new NebulaCache(address = '127.0.0.1:7001', timeoutOrOptions = 5000)`: create a client. A numeric second argument sets connect and request timeout milliseconds; an options object allows more control.
- `await ping()`: return true when the server responds with `PONG`.
- `await set(key, value, ttlSecs = 0)`: store text or binary data, optionally with a TTL in seconds.
- `await get(key)`: return raw `Buffer` bytes or `null` if absent/expired.
- `await delete(key)`: return true if an entry was deleted, otherwise false.
- `await close()`: stop pool activity and close active sockets. Requests after close reject.

Keys must be non-empty strings no longer than 1,024 UTF-8 bytes. Values may be up to 8 MiB. The server's memory limit may reject a SET independently of the protocol value limit.

The options object accepts `minPoolSize`, `maxPoolSize`, `connectTimeout`, `requestTimeout`, `acquireTimeout`, `idleTimeout`, `maxLifetime`, `maxRetries`, `tls`, and `token`. Timeout values are milliseconds; pool and retry counts are non-negative safe integers (the maximum pool size must be positive). Defaults are pool size 0–10, five-second connect/request/acquire timeouts, 60-second idle timeout, 300-second max lifetime, and two GET retries.

## TLS and token authentication

Pass normal Node.js TLS connection options in `tls` to encrypt TCP connections. Certificate verification is enabled unless explicitly disabled. To add a private CA, supply its PEM bytes as `ca`. When authentication is enabled on the server, also pass the API token in `token`; each new connection authenticates before use. TLS is optional for token authentication, but plaintext connections expose tokens and cache traffic.

```js
const fs = require('node:fs');
const { NebulaCache } = require('nebulacache');

const cache = new NebulaCache('cache.example:7001', {
  tls: { ca: fs.readFileSync('./ca.pem') },
  token: process.env.NEBULA_TOKEN,
});
```

Do not disable certificate validation or commit tokens/CA private keys. Omit `tls` to use plaintext TCP; only do so on a trusted network, since tokens and cache traffic will not be encrypted.

## Pooling and retries

Connections are created lazily, reused for requests, and closed by `close()`. Each socket handles one request at a time, so simultaneous operations require separate pool connections. Idle and old sockets are retired. The SDK retries only GET requests after transient transport errors, using bounded exponential full jitter and a per-client retry budget. SET and DELETE are not automatically repeated because their outcome can be ambiguous after a timeout.

## Example CLI

The included CLI supports `ping`, `set`, `get`, and `delete`. It uses `NEBULA_ADDR` or `127.0.0.1:7001`; `--tls-ca` or `NEBULA_TLS_CA` loads a private CA and enables TLS.

```bash
node ./examples/cache_cli.js ping
node ./examples/cache_cli.js set greeting hello --ttl 60
node ./examples/cache_cli.js get greeting
node ./examples/cache_cli.js delete greeting
node ./examples/cache_cli.js --address 127.0.0.1:7001 ping
node ./examples/cache_cli.js --tls-ca /path/to/ca.pem ping
NEBULA_TLS_CA=/path/to/ca.pem node ./examples/cache_cli.js ping
```

The example CLI does not currently accept a token option. Use the SDK client with its `token` option for an authenticated connection.

## Errors

- `CacheConnectionError`: connection, timeout, or transport failure.
- `CacheProtocolError`: malformed or incomplete protocol response.
- `CacheResponseError`: unsuccessful server status; the error's `status` property contains the numeric protocol status.
- `NebulaCacheError`: common base class for SDK-specific errors.
- `TypeError` / `RangeError`: invalid key, value, TTL, timeout, pool size, or options.

## Test

Run the unit tests from this directory:

```bash
npm test
```
