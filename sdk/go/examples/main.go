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

func main() {
	address := flag.String("address", nebulacache.DefaultAddress, "NebulaCache node address")
	tlsCA := flag.String("tls-ca", os.Getenv("NEBULA_TLS_CA"), "PEM CA file; enables TLS")
	flag.Parse()

	if err := run(*address, *tlsCA); err != nil {
		fmt.Fprintln(os.Stderr, "nebulacache-example:", err)
		os.Exit(1)
	}
}

func run(address string, tlsCA string) error {
	config := nebulacache.DefaultConfig()
	config.Address = address
	if tlsCA != "" {
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
		if !roots.AppendCertsFromPEM(certificate) {
			return fmt.Errorf("no certificates found in TLS CA file %q", tlsCA)
		}
		config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
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
