# NebulaCache Go SDK

A dependency-free Go 1.22+ client for the NebulaCache v1 binary TCP protocol. The package uses only the Go standard library and supports cache operations, TTLs, TLS, token authentication, a connection pool, and bounded GET retries.

## Before you start

Start a NebulaCache server using the repository-root setup and configuration instructions in [the main README](./../../README.md#run-the-server). The default client address is `127.0.0.1:17171`. This SDK provides PING, SET, GET, and DELETE over the binary TCP protocol; it does not provide user or token administration. When server token authentication is enabled, each new connection needs an API token; TLS is optional. For user and token management, enable the server's HTTP admin listener and access it locally or through a TLS-terminating reverse proxy.

## Install

From a Go project, install the module:

```bash
go get github.com/dyotamo/nebula/sdk/go
```

The SDK has no third-party module dependencies.

## Quick start

```go
package main

import (
	"fmt"
	"log"

	nebulacache "github.com/dyotamo/nebula/sdk/go"
)

func main() {
	cache, err := nebulacache.New()
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close()

	if err := cache.Ping(); err != nil {
		log.Fatal(err)
	}
	if err := cache.SetStringWithTTL("greeting", "hello", 60); err != nil {
		log.Fatal(err)
	}

	value, found, err := cache.Get("greeting")
	if err != nil {
		log.Fatal(err)
	}
	if found {
		fmt.Println(string(value))
	}

	deleted, err := cache.Delete("greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("deleted:", deleted)
}
```

Keys and string values use UTF-8. `Set` accepts `[]byte`; `SetString` accepts text. Both default to TTL `0` (no expiration); use `SetWithTTL` or `SetStringWithTTL` for a TTL in seconds. `Get` returns raw bytes and a separate `found` flag, so an empty stored value is distinct from a missing/expired key.

## API and behavior

- `New()` and `New(address)`: create a client for `127.0.0.1:17171` or the supplied `host:port`, with a five-second default timeout.
- `NewWithTimeout(address, timeout)`: use a custom positive `time.Duration` for connect and request timeouts.
- `NewWithConfig(config)`: configure address, pool sizes, timeouts, retry count, optional TLS, and optional auth token.
- `Ping() error`: succeeds when the server replies with `PONG`.
- `Set(key, value []byte) error` / `SetString(key, value string) error`: store a value without expiration.
- `SetWithTTL(key, value []byte, ttlSecs uint64) error` / `SetStringWithTTL(...)`: store with a TTL in seconds.
- `Get(key) ([]byte, bool, error)`: return raw value, found status, and error.
- `Delete(key) (bool, error)`: report whether a key was removed.
- `Close() error`: close active and idle sockets. Operations after close return `ErrClosed`.

Keys must be non-empty UTF-8 strings no longer than 1,024 encoded bytes. Values may be arbitrary bytes up to 8 MiB. TTL is an unsigned 64-bit integer in seconds, where zero means no expiration. The server may also reject SET when configured cache capacity cannot accommodate it.

## TLS and token authentication

To encrypt TCP connections, provide a standard Go `tls.Config`. Add a private CA to the trusted root pool; do not disable certificate or hostname verification. Set `Config.Token` to an API token when server authentication is enabled. The SDK sends AUTH on each new pooled connection; authentication works with or without TLS. Use TLS when the network is not trusted because plaintext TCP exposes tokens and cache traffic.

```go
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	nebulacache "github.com/dyotamo/nebula/sdk/go"
)

func connect() (*nebulacache.NebulaCache, error) {
	pem, err := os.ReadFile("./ca.pem")
	if err != nil {
		return nil, err
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA file contains no certificates")
	}

	config := nebulacache.DefaultConfig()
	config.Address = "cache.example:17171"
	config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	config.Token = os.Getenv("NEBULA_TOKEN")
	return nebulacache.NewWithConfig(config)
}
```

Import `fmt` if using the error formatting shown. Leave `TLSConfig` nil to use plaintext TCP. TLS is optional for token authentication, but plaintext connections expose tokens and cache traffic.

## Pooling, timeouts, and retries

`NewWithConfig` accepts `Address`, `MinPoolSize`, `MaxPoolSize`, `ConnectTimeout`, `RequestTimeout`, `AcquireTimeout`, `IdleTimeout`, `MaxLifetime`, `MaxRetries`, `TLSConfig`, and `Token`. `DefaultConfig()` uses `127.0.0.1:17171`, min pool 0, max pool 10, five-second connect/request/acquire timeouts, 60-second idle timeout, 300-second maximum lifetime, and two retries.

Connections are opened lazily, reused, and owned by the client until `Close()`. A connection processes one request at a time; concurrent calls use separate pooled connections. Idle/old/broken connections are retired. Only GET is retried after transient transport failures, with bounded exponential full jitter and a shared retry budget. SET and DELETE are not replayed because a timeout may occur after the server has applied a mutation.

## Included example

From this directory, run the example against the default server address:

```bash
go run ./examples
```

Use another address or a private CA when needed:

```bash
go run ./examples --address 127.0.0.1:17171
go run ./examples --tls-ca /path/to/ca.pem
NEBULA_TLS_CA=/path/to/ca.pem go run ./examples
```

The example accepts `--address` and `--tls-ca`; the latter enables TLS and extends the system root set with the PEM CA certificate. It does not accept a token option; use `Config.Token` with `NewWithConfig` for authenticated connections.

## Errors

- `CacheConnectionError`: connection, timeout, or transport failure. Inspect its fields and wrapped cause for context.
- `CacheProtocolError`: malformed/incomplete protocol response.
- `CacheResponseError`: server returned a non-success status; the error's `Status` records its numeric protocol code.
- `ErrClosed`: operation attempted after closing the client.

SDK errors implement Go's `error` interface and may be inspected with `errors.As`.

## Test and build

Run from this directory:

```bash
go test ./...
go build ./...
```
