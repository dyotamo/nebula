# NebulaCache Java SDK

A synchronous Java 17+ client for the NebulaCache v1 binary TCP protocol. The SDK has no runtime dependencies and provides cache operations, TLS, token authentication, connection pooling, and bounded GET retries. Maven is used for building and tests.

## Before you start

Start a server using [the repository's server setup instructions](./../../README.md#run-the-server). The default address is `127.0.0.1:7001`. This SDK provides PING, SET, GET, and DELETE over the binary TCP protocol; it does not provide user or token administration. When server token authentication is enabled, each new connection needs an API token and TLS with certificate and hostname verification. For user and token management, enable the server's admin listener and use its HTTPS API or console.

## Build and install

Run tests and build a JAR from this directory:

```bash
mvn test
mvn package
```

Install the artifact into your local Maven repository:

```bash
mvn install
```

Then add the dependency to a Maven project:

```xml
<dependency>
  <groupId>io.nebulacache</groupId>
  <artifactId>nebulacache</artifactId>
  <version>0.1.0</version>
</dependency>
```

This project does not publish or configure a remote Maven repository; `mvn install` makes the artifact available locally.

## Quick start

```java
import io.nebulacache.NebulaCache;
import java.nio.charset.StandardCharsets;

public class Example {
    public static void main(String[] args) {
        try (NebulaCache cache = new NebulaCache("127.0.0.1:7001")) {
            if (cache.ping()) {
                System.out.println("PONG");
            }

            cache.set("greeting", "hello", 60L);
            cache.get("greeting").ifPresentOrElse(
                    value -> System.out.println(new String(value, StandardCharsets.UTF_8)),
                    () -> System.out.println("missing"));

            System.out.println("deleted: " + cache.delete("greeting"));
        }
    }
}
```

`set` accepts `String` or `byte[]` values. Keys and strings use UTF-8. `get` returns `Optional<byte[]>`; an empty optional means missing/expired, and the contained value remains raw bytes. TTL `0` means no expiration. TTLs can be a non-negative `long` or `BigInteger` in the unsigned 64-bit range.

## API and behavior

- `new NebulaCache()`: connect to `127.0.0.1:7001` with default settings.
- `new NebulaCache(address)`: connect to the given `host:port` using default settings. IPv6 addresses use `[host]:port`.
- `new NebulaCache(address, timeout)`: use one positive `Duration` for connect and request timeouts.
- `new NebulaCache(address, config)`: configure pool sizes, connect/request/acquire/idle/lifetime timeouts, retry count, TLS, and token.
- `ping() -> boolean`: return true when the server replies with `PONG`; otherwise throw an SDK exception.
- `set(key, value[, ttlSecs])`: store text or binary value, optionally with TTL seconds.
- `get(key) -> Optional<byte[]>`: return raw bytes or empty when missing/expired.
- `delete(key) -> boolean`: report whether an entry was deleted.
- `close()`: close active and idle sockets. Use try-with-resources; later requests are rejected.

Keys must be non-empty and no longer than 1,024 UTF-8 bytes; values may be up to 8 MiB. The server's configured cache capacity can additionally reject a SET if the entry cannot fit.

## TLS and token authentication

The TLS client uses the JVM's default trust store, validates the certificate and hostname, and negotiates TLS for the TCP connection. Import a private CA into a trust store used by the JVM when necessary. For token authentication, set both `tlsEnabled` and `token` in the `NebulaCache.Config`; the SDK sends AUTH over each new TLS connection. A token without TLS is rejected.

```java
import io.nebulacache.NebulaCache;
import java.time.Duration;

NebulaCache.Config config = new NebulaCache.Config(
        0, 10,
        Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(60), Duration.ofSeconds(300),
        2, true, System.getenv("NEBULA_TOKEN"));
try (NebulaCache cache = new NebulaCache("cache.example:7001", config)) {
    cache.ping();
}
```

`Config` fields in order are `minPoolSize`, `maxPoolSize`, `connectTimeout`, `requestTimeout`, `acquireTimeout`, `idleTimeout`, `maxLifetime`, `maxRetries`, `tlsEnabled`, and `token`. To use a private CA, add it to a JVM trust store and supply that store when starting the application:

```bash
keytool -importcert -noprompt -alias nebula-ca -file ./ca.pem \
  -keystore ./nebula-truststore.p12 -storetype PKCS12
java -Djavax.net.ssl.trustStore=./nebula-truststore.p12 \
  -Djavax.net.ssl.trustStorePassword="$TRUSTSTORE_PASSWORD" YourApplication
```

Keep trust-store passwords and API tokens out of source control. TLS is not enabled by default.

## Pooling, timeouts, and retries

`NebulaCache.Config()` defaults to min pool size 0, max pool size 10, five-second connect/request/acquire timeouts, 60-second idle timeout, 300-second maximum connection lifetime, two GET retries, TLS disabled, and no token. Pool sizes must satisfy `0 <= minPoolSize <= maxPoolSize`, with a positive maximum; durations must be positive.

Connections are opened lazily and reused for sequential calls. A socket carries one request at a time, while simultaneous calls obtain separate pool connections. The client owns its pool until `close()`. Only GET retries transient transport errors, using bounded exponential full jitter and a per-client retry budget. SET and DELETE are not retried because a timeout can happen after the mutation has reached the server.

## Smoke test

The SDK main method exercises ping, set, get, and delete. Build first, then run with the default address or supply a different one:

```bash
mvn package
java -cp ./target/classes io.nebulacache.NebulaCache
java -cp ./target/classes io.nebulacache.NebulaCache 127.0.0.1:7001
java -cp ./target/classes io.nebulacache.NebulaCache --tls 127.0.0.1:7001
```

For TLS with a private CA, start Java with the trust-store system properties shown in [TLS and token authentication](#tls-and-token-authentication). The smoke-test main currently supports the `--tls` flag but not a token argument; use the SDK `Config` API for authenticated calls.

## Exceptions

- `CacheConnectionException`: connection or timeout failure.
- `CacheProtocolException`: invalid or incomplete protocol response.
- `CacheResponseException`: the server returned an unsuccessful status; `getStatus()` returns its numeric status.
- `NebulaCacheException`: unchecked common base for SDK exceptions.
- `IllegalArgumentException` / `NullPointerException`: invalid or null arguments/configuration.

## Test

Run the unit tests from this directory:

```bash
mvn test
```
