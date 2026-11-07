# NebulaCache Java SDK

A synchronous, dependency-free Java client for the NebulaCache binary TCP protocol. Requires Java 17 or newer. Maven is used to build and test the SDK.

## Start a node

Start a node listening on the default TCP address:

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001
```

The TCP protocol does not provide authentication or TLS. Expose the node only on trusted interfaces and apply suitable network controls. Nodes also include an optional password-protected web admin console for inspecting and managing entries; it is separate from this TCP SDK. See the [node setup and console instructions](../../README.md#use-the-web-admin-console).

## Build and install

Build and test the SDK from this directory:

```bash
mvn test
mvn package
```

Install it into your local Maven repository:

```bash
mvn install
```

Then add it to a Maven project:

```xml
<dependency>
  <groupId>io.nebulacache</groupId>
  <artifactId>nebulacache</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Quick start

```java
import io.nebulacache.NebulaCache;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

NebulaCache.Config config = new NebulaCache.Config(
        0, 10, Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(60), Duration.ofSeconds(300), 2);
try (NebulaCache cache = new NebulaCache("127.0.0.1:7001", config)) {
    cache.set("greeting", "hello", 60L);
    cache.get("greeting").ifPresentOrElse(
            value -> System.out.println(new String(value, StandardCharsets.UTF_8)),
            () -> System.out.println("missing"));

    if (cache.delete("greeting")) {
        System.out.println("entry deleted");
    }

    System.out.println(cache.ping());
}
```

`set` accepts `String` or `byte[]` values. Strings and keys are encoded as UTF-8. `get` returns an `Optional<byte[]>` containing the raw value bytes, or an empty `Optional` if the key is missing or expired. A TTL of `0` (the default) means the entry does not expire. TTLs can be supplied as a non-negative `long` or as a `BigInteger` from `0` through `2^64 - 1`.

## API

- `new NebulaCache()`: connect to `127.0.0.1:7001` with a five-second timeout.
- `new NebulaCache(address)`: connect to a `host:port` address with a five-second timeout. IPv6 addresses use the `[host]:port` format.
- `new NebulaCache(address, timeout)`: configure connect and request timeouts with a positive `java.time.Duration`.
- `new NebulaCache(address, config)`: configure connection pooling, timeouts, and GET retry limits.
- `close()`: close idle and active sockets; the client rejects operations after shutdown. Use try-with-resources.
- `ping() -> boolean`: returns `true` if the node responds with `PONG`; otherwise throws an SDK exception.
- `set(key, value[, ttlSecs])`: store a non-empty string key and a string or binary value.
- `get(key) -> Optional<byte[]>`: return the raw value, or an empty `Optional` if it does not exist.
- `delete(key) -> boolean`: return `true` if an entry was deleted, or `false` if it did not exist.

Keys are UTF-8 encoded and limited to 1 KiB. Values are limited to 8 MiB by the protocol. Each node has its own volatile cache; nodes do not share entries.

## Connection pooling and resilience

Connections are reused for sequential requests. Each socket carries one request at a time; concurrent calls use separate pooled sockets. The client lazily opens connections and owns them until `close()`.

`NebulaCache.Config` fields are `minPoolSize` (0), `maxPoolSize` (10), `connectTimeout` (5 seconds), `requestTimeout` (5 seconds), `acquireTimeout` (5 seconds), `idleTimeout` (60 seconds), `maxLifetime` (300 seconds), and `maxRetries` (2). Durations must be positive. The legacy timeout constructor sets connect and request timeouts; acquisition retains its own default.

Only GET is retried, with bounded exponential full jitter and a per-client retry budget. SET and DELETE are not automatically retried because a timeout after sending can leave their outcome ambiguous. Expired or failed sockets are discarded and replaced on later acquisition.

## Smoke test

The `NebulaCache` main method exercises ping, set, get, and delete. Build the SDK, then run it from this directory with the default node address or provide an address:

```bash
mvn package
java -cp target/classes io.nebulacache.NebulaCache
java -cp target/classes io.nebulacache.NebulaCache 127.0.0.1:7001
```

## Exceptions

- `CacheConnectionException`: the node could not be reached or the request timed out.
- `CacheProtocolException`: the node returned a malformed or incomplete protocol response.
- `CacheResponseException`: the node returned an unsuccessful protocol status; `getStatus()` returns its numeric status.
- `NebulaCacheException`: common unchecked base class for SDK errors.
- `NullPointerException` and `IllegalArgumentException`: null or invalid key, value, TTL, timeout, or client configuration.
