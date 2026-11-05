package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	nebulacache "github.com/dyotamo/nebula/sdk/go"
)

func main() {
	address := flag.String("address", nebulacache.DefaultAddress, "NebulaCache node address")
	flag.Parse()

	if err := run(*address); err != nil {
		fmt.Fprintln(os.Stderr, "nebulacache-example:", err)
		os.Exit(1)
	}
}

func run(address string) error {
	cache, err := nebulacache.New(address)
	if err != nil {
		return err
	}

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
