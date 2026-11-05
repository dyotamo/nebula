# NebulaCache Go SDK

A dependency-free Go client for the NebulaCache binary TCP protocol. The module targets Go 1.22 and newer and uses only the standard library.

## Start a node

Start a node listening on the default TCP address:

```bash
cargo run -p nebula-node -- --listen 127.0.0.1:7001
```

The TCP protocol does not provide authentication or TLS. Expose the node only on trusted interfaces and apply suitable network controls. Nodes also include an optional password-protected web admin console for inspecting and managing entries; it is separate from this TCP SDK. See the [node setup and console instructions](../../README.md#use-the-web-admin-console).

## Install

Add the SDK module to a Go project:

```bash
go get github.com/dyotamo/nebula/sdk/go
```

## Quick start

```go
package main

import (
	"fmt"
	"log"

	"github.com/dyotamo/nebula/sdk/go"
)

func main() {
	cache, err := nebulacache.New("127.0.0.1:7001")
	if err != nil {
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
	} else {
		fmt.Println("missing")
	}

	deleted, err := cache.Delete("greeting")
	if err != nil {
		log.Fatal(err)
	}
	if deleted {
		fmt.Println("entry deleted")
	}

	if err := cache.Ping(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("PONG")
}
```

`Set` accepts `[]byte`, while `SetString` accepts text; both use a TTL of zero (no expiration) by default. Use `SetWithTTL` or `SetStringWithTTL` to specify a TTL in seconds as a `uint64`. Keys and strings are encoded as UTF-8. `Get` returns the raw value, a `found` flag, and an error, so an empty value is distinct from a missing key.

## API

- `New()` / `New(address)`: create a client for `127.0.0.1:7001` or the provided `host:port` address, using a five-second timeout. IPv6 addresses use the `[host]:port` format.
- `NewWithTimeout(address, timeout)`: create a client with a custom positive `time.Duration` timeout for connect and read/write operations.
- `Ping() error`: returns `nil` if the node responds with `PONG`.
- `Set(key, value) error` and `SetString(key, value) error`: store a binary or text value without expiration.
- `SetWithTTL(key, value, ttlSecs) error` and `SetStringWithTTL(key, value, ttlSecs) error`: store a binary or text value with a TTL in seconds.
- `Get(key) ([]byte, bool, error)`: return the raw value, whether the key exists, and any error.
- `Delete(key) (bool, error)`: return whether an entry was deleted and any error.

Keys are limited to 1024 UTF-8 bytes and values to 8 MiB. Each node has its own volatile cache; nodes do not share entries.

## Usage example

Run the included example against the default address or supply another address:

```bash
go run ./examples
go run ./examples --address 127.0.0.1:7002
```

The example pings the node, stores and retrieves a short-lived value, then deletes it.

## Errors

- `CacheConnectionError`: the node could not be reached or the operation timed out.
- `CacheProtocolError`: the node returned a malformed or incomplete protocol response.
- `CacheResponseError`: the node returned an unsuccessful protocol status; its `Status` field contains the numeric status.

The SDK errors implement Go's `error` interface and can be inspected with `errors.As`.

## Test and build

Run the SDK tests and build from this directory:

```bash
go test ./...
go build ./...
```
