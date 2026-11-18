// Command main demonstrates a pooled client's PING, expiring SET, GET, and DELETE
// lifecycle, optionally verifying the server with a custom TLS certificate authority.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"time"

	nebulacache "github.com/dyotamo/nebula/sdk/go"
)

// main keeps process exit handling outside run so run's deferred pool cleanup
// completes before a failure terminates the example.
func main() {
	address := flag.String("address", nebulacache.DefaultAddress, "NebulaCache node address")
	// The environment supplies a default; an explicit flag takes precedence.
	tlsCA := flag.String("tls-ca", os.Getenv("NEBULA_TLS_CA"), "PEM CA file; enables TLS")
	flag.Parse()

	if err := run(*address, *tlsCA); err != nil {
		fmt.Fprintln(os.Stderr, "nebulacache-example:", err)
		os.Exit(1)
	}
}

// run shares one client across the lifecycle so sequential operations can reuse
// an authenticated transport. An empty tlsCA selects the default plaintext mode.
func run(address string, tlsCA string) error {
	// Starting from defaults preserves required positive timeouts and pool bounds.
	config := nebulacache.DefaultConfig()
	config.Address = address
	if tlsCA != "" {
		// Add private trust roots without discarding the platform's public authorities.
		roots, err := x509.SystemCertPool()
		if err != nil {
			return err
		}
		if roots == nil {
			roots = x509.NewCertPool()
		}
		certificate, err := os.ReadFile(tlsCA)
		if err != nil {
			return err
		}
		// Reject an empty or unusable PEM bundle instead of silently ignoring it.
		if !roots.AppendCertsFromPEM(certificate) {
			return fmt.Errorf("no certificates found in TLS CA file %q", tlsCA)
		}
		// Keep certificate and hostname verification enabled; the dialed host supplies
		// the server name when no explicit TLS ServerName is configured.
		config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
	// Construction only validates configuration; Ping below opens the first socket.
	cache, err := nebulacache.NewWithConfig(config)
	if err != nil {
		return err
	}
	defer cache.Close()

	if err := cache.Ping(); err != nil {
		return err
	}

	fmt.Println("PONG")

	key := fmt.Sprintf("go-sdk-example-%d", time.Now().UnixNano())
	if err := cache.SetStringWithTTL(key, "hello from Go", 60); err != nil {
		return err
	}

	value, found, err := cache.Get(key)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("key %q was not found", key)
	}

	fmt.Println(string(value))

	deleted, err := cache.Delete(key)
	if err != nil {
		return err
	}

	if deleted {
		fmt.Println("entry deleted")
	}

	return nil
}
