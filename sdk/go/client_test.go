package nebulacache

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type storedEntry struct {
	value   []byte
	ttlSecs uint64
}

type testFrame struct {
	opcode     byte
	key        string
	value      []byte
	ttlSecs    uint64
	connection int
	sequence   int
	attempt    int
}

type testServer struct {
	listener    net.Listener
	mu          sync.Mutex
	entries     map[string]storedEntry
	connections map[net.Conn]int
	attempts    map[string]int
	accepted    int
	peak        int
	requests    int
	stopped     bool
	hook        func(net.Conn, testFrame) bool
	changed     chan struct{}
	done        chan struct{}
	workers     sync.WaitGroup
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &testServer{
		listener: listener, entries: make(map[string]storedEntry),
		connections: make(map[net.Conn]int), attempts: make(map[string]int),
		changed: make(chan struct{}, 1), done: make(chan struct{}),
	}
	go server.serve()
	t.Cleanup(func() {
		server.mu.Lock()
		server.stopped = true
		close(server.done)
		listener.Close()
		for connection := range server.connections {
			connection.Close()
		}
		server.mu.Unlock()
		server.workers.Wait()
	})
	return server
}

func (server *testServer) address() string {
	return server.listener.Addr().String()
}

func (server *testServer) notify() {
	select {
	case server.changed <- struct{}{}:
	default:
	}
}

func (server *testServer) serve() {
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			return
		}
		server.mu.Lock()
		if server.stopped {
			server.mu.Unlock()
			connection.Close()
			return
		}
		server.accepted++
		id := server.accepted
		server.connections[connection] = id
		if len(server.connections) > server.peak {
			server.peak = len(server.connections)
		}
		server.workers.Add(1)
		server.mu.Unlock()
		server.notify()
		go server.handle(connection, id)
	}
}

func (server *testServer) handle(connection net.Conn, id int) {
	defer server.workers.Done()
	defer func() {
		connection.Close()
		server.mu.Lock()
		delete(server.connections, connection)
		server.mu.Unlock()
		server.notify()
	}()
	for {
		var header [requestHeader]byte
		if _, err := io.ReadFull(connection, header[:]); err != nil {
			return
		}
		if string(header[:4]) != string(protocolMagic[:]) || header[4] != protocolVersion {
			return
		}
		keyLength := binary.BigEndian.Uint32(header[6:10])
		valueLength := binary.BigEndian.Uint32(header[10:14])
		if keyLength > maxKeyLength || valueLength > maxValueLength {
			return
		}
		keyBytes := make([]byte, keyLength)
		value := make([]byte, valueLength)
		if _, err := io.ReadFull(connection, keyBytes); err != nil {
			return
		}
		if _, err := io.ReadFull(connection, value); err != nil {
			return
		}
		frame := testFrame{
			opcode: header[5], key: string(keyBytes), value: value,
			ttlSecs: binary.BigEndian.Uint64(header[14:22]), connection: id,
		}
		server.mu.Lock()
		server.requests++
		server.attempts[frame.key]++
		frame.sequence = server.requests
		frame.attempt = server.attempts[frame.key]
		hook := server.hook
		server.mu.Unlock()
		server.notify()
		if hook != nil {
			if !hook(connection, frame) {
				return
			}
			continue
		}
		var status byte
		var responseValue []byte
		server.mu.Lock()
		switch frame.opcode {
		case 1:
			server.entries[frame.key] = storedEntry{value: append([]byte(nil), value...), ttlSecs: frame.ttlSecs}
		case 2:
			if frame.key == "failure" {
				status = 3
				responseValue = []byte("forced failure")
			} else if entry, exists := server.entries[frame.key]; exists {
				responseValue = entry.value
			} else {
				status = 1
			}
		case 3:
			if _, exists := server.entries[frame.key]; !exists {
				status = 1
			} else {
				delete(server.entries, frame.key)
			}
		case 4:
			responseValue = []byte("PONG")
		default:
			status = 2
			responseValue = []byte("invalid request")
		}
		server.mu.Unlock()
		if !sendTestResponse(connection, status, responseValue) {
			return
		}
	}
}

func sendTestResponse(connection net.Conn, status byte, value []byte) bool {
	var header [responseHeader]byte
	copy(header[:4], protocolMagic[:])
	header[4] = protocolVersion
	header[5] = status
	binary.BigEndian.PutUint32(header[6:10], uint32(len(value)))
	return writeAll(connection, header[:]) == nil && writeAll(connection, value) == nil
}

func (server *testServer) setHook(hook func(net.Conn, testFrame) bool) {
	server.mu.Lock()
	server.hook = hook
	server.mu.Unlock()
}

func (server *testServer) counts() (accepted, active, peak, requests int) {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.accepted, len(server.connections), server.peak, server.requests
}

func (server *testServer) wait(t *testing.T, predicate func(int, int, int, int) bool) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		accepted, active, peak, requests := server.counts()
		if predicate(accepted, active, peak, requests) {
			return
		}
		select {
		case <-server.changed:
		case <-timer.C:
			t.Fatalf("server condition timed out: accepted=%d active=%d peak=%d requests=%d", accepted, active, peak, requests)
		}
	}
}

func (server *testServer) entry(key string) (storedEntry, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	entry, exists := server.entries[key]
	return entry, exists
}

func TestPingAndKeyLifecycle(t *testing.T) {
	server := newTestServer(t)
	cache, err := New(server.address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })

	if err := cache.Ping(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := cache.SetStringWithTTL("greeting", "hello", 30); err != nil {
		t.Fatalf("SetStringWithTTL() error = %v", err)
	}
	value, found, err := cache.Get("greeting")
	if err != nil || !found || string(value) != "hello" {
		t.Fatalf("Get() = (%q, %t, %v), want (%q, true, nil)", value, found, err, "hello")
	}
	entry, exists := server.entry("greeting")
	if !exists || entry.ttlSecs != 30 {
		t.Fatalf("stored TTL = (%d, %t), want (30, true)", entry.ttlSecs, exists)
	}

	deleted, err := cache.Delete("greeting")
	if err != nil || !deleted {
		t.Fatalf("Delete() = (%t, %v), want (true, nil)", deleted, err)
	}
	deleted, err = cache.Delete("greeting")
	if err != nil || deleted {
		t.Fatalf("Delete() for missing key = (%t, %v), want (false, nil)", deleted, err)
	}
	value, found, err = cache.Get("greeting")
	if err != nil || found || value != nil {
		t.Fatalf("Get() for missing key = (%q, %t, %v), want (nil, false, nil)", value, found, err)
	}
}

func TestBinaryAndEmptyValues(t *testing.T) {
	server := newTestServer(t)
	cache, err := New(server.address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })

	key := "space / café"
	want := []byte{0, 0xff}
	if err := cache.Set(key, want); err != nil {
		t.Fatal(err)
	}
	got, found, err := cache.Get(key)
	if err != nil || !found || string(got) != string(want) {
		t.Fatalf("Get() = (%v, %t, %v), want (%v, true, nil)", got, found, err, want)
	}

	if err := cache.SetString("empty", ""); err != nil {
		t.Fatal(err)
	}
	got, found, err = cache.Get("empty")
	if err != nil || !found || got == nil || len(got) != 0 {
		t.Fatalf("Get() empty value = (%v, %t, %v), want (empty bytes, true, nil)", got, found, err)
	}
}

func TestUnsignedTTLAndValidation(t *testing.T) {
	server := newTestServer(t)
	cache, err := New(server.address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	if err := cache.SetStringWithTTL("max-ttl", "value", ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	entry, exists := server.entry("max-ttl")
	if !exists || entry.ttlSecs != ^uint64(0) {
		t.Fatalf("stored maximum TTL = (%d, %t), want (%d, true)", entry.ttlSecs, exists, ^uint64(0))
	}

	if err := cache.SetString("", "value"); err == nil {
		t.Fatal("SetString() accepted an empty key")
	}
	if err := cache.SetString(string([]byte{0xff}), "value"); err == nil {
		t.Fatal("SetString() accepted a non-UTF-8 key")
	}
	if err := cache.SetString("invalid-value", string([]byte{0xff})); err == nil {
		t.Fatal("SetString() accepted a non-UTF-8 value")
	}
	if err := cache.SetString(strings.Repeat("é", 513), "value"); err == nil {
		t.Fatal("SetString() accepted a key longer than the protocol limit")
	}
	if err := cache.Set("large", make([]byte, maxValueLength+1)); err == nil {
		t.Fatal("Set() accepted a value larger than the protocol limit")
	}
}

func TestResponseAndConnectionErrors(t *testing.T) {
	server := newTestServer(t)
	cache, err := New(server.address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })

	_, _, err = cache.Get("failure")
	var responseErr *CacheResponseError
	if !errors.As(err, &responseErr) || responseErr.Status != 3 {
		t.Fatalf("Get() error = %v, want CacheResponseError with status 3", err)
	}

	unreachableListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachableAddress := unreachableListener.Addr().(*net.TCPAddr)
	unreachableListener.Close()
	unreachable, err := NewWithTimeout(net.JoinHostPort("127.0.0.1", strconv.Itoa(unreachableAddress.Port)), 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unreachable.Close() })
	var connectionErr *CacheConnectionError
	if err := unreachable.Ping(); !errors.As(err, &connectionErr) {
		t.Fatalf("Ping() error = %v, want CacheConnectionError", err)
	}
}

func TestProtocolError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		var request [requestHeader]byte
		if _, readErr := io.ReadFull(connection, request[:]); readErr != nil {
			return
		}
		var header [responseHeader]byte
		copy(header[:4], []byte("NOPE"))
		header[4] = protocolVersion
		writeAll(connection, header[:])
	}()

	cache, err := New(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var protocolErr *CacheProtocolError
	if err := cache.Ping(); !errors.As(err, &protocolErr) {
		t.Fatalf("Ping() error = %v, want CacheProtocolError", err)
	}
}

func TestAddressAndTimeoutValidation(t *testing.T) {
	if _, err := New("::1:17171"); err == nil {
		t.Fatal("New() accepted an unbracketed IPv6 address")
	}
	if _, err := New("localhost:70000"); err == nil {
		t.Fatal("New() accepted an out-of-range port")
	}
	if _, err := NewWithTimeout("localhost:17171", 0); err == nil {
		t.Fatal("NewWithTimeout() accepted a non-positive timeout")
	}
	if client, err := New(); err != nil || client == nil {
		t.Fatalf("New() = (%v, %v), want a default client", client, err)
	}
}

func newConfiguredTestClient(t *testing.T, server *testServer, configure func(*Config)) *NebulaCache {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Address = server.address()
	if configure != nil {
		configure(&cfg)
	}
	cache, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	return cache
}

func awaitTestValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for test event")
		var zero T
		return zero
	}
}

func requireTestTimeout(t *testing.T, err error) {
	t.Helper()
	var connectionErr *CacheConnectionError
	if !errors.As(err, &connectionErr) || !connectionErr.TimedOut {
		t.Fatalf("error = %v, want timed-out CacheConnectionError", err)
	}
}

func requireTestRequests(t *testing.T, server *testServer, want int) {
	t.Helper()
	_, _, _, got := server.counts()
	if got != want {
		t.Fatalf("request frames = %d, want %d", got, want)
	}
}

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Address != DefaultAddress || cfg.MinPoolSize != 0 || cfg.MaxPoolSize != 10 ||
		cfg.ConnectTimeout != 5*time.Second || cfg.RequestTimeout != 5*time.Second ||
		cfg.AcquireTimeout != 5*time.Second || cfg.IdleTimeout != 60*time.Second ||
		cfg.MaxLifetime != 300*time.Second || cfg.MaxRetries != 2 || cfg.TLSConfig != nil {
		t.Fatalf("DefaultConfig() = %+v, want documented pooling defaults", cfg)
	}
	cfg.MaxPoolSize = 1
	if DefaultConfig().MaxPoolSize != 10 {
		t.Fatal("modifying a config changed subsequent defaults")
	}
}

func TestPoolConfigValidation(t *testing.T) {
	invalid := []struct {
		name string
		set  func(*Config)
	}{
		{"empty address", func(c *Config) { c.Address = "" }},
		{"invalid address", func(c *Config) { c.Address = "localhost:70000" }},
		{"negative minimum", func(c *Config) { c.MinPoolSize = -1 }},
		{"zero maximum", func(c *Config) { c.MaxPoolSize = 0 }},
		{"negative maximum", func(c *Config) { c.MaxPoolSize = -1 }},
		{"minimum exceeds maximum", func(c *Config) { c.MinPoolSize = c.MaxPoolSize + 1 }},
		{"negative retries", func(c *Config) { c.MaxRetries = -1 }},
	}
	timeouts := []struct {
		name string
		set  func(*Config, time.Duration)
	}{
		{"connect", func(c *Config, d time.Duration) { c.ConnectTimeout = d }},
		{"request", func(c *Config, d time.Duration) { c.RequestTimeout = d }},
		{"acquire", func(c *Config, d time.Duration) { c.AcquireTimeout = d }},
		{"idle", func(c *Config, d time.Duration) { c.IdleTimeout = d }},
		{"lifetime", func(c *Config, d time.Duration) { c.MaxLifetime = d }},
	}
	for _, timeout := range timeouts {
		for _, duration := range []time.Duration{0, -time.Second} {
			invalid = append(invalid, struct {
				name string
				set  func(*Config)
			}{timeout.name + "/" + duration.String(), func(c *Config) { timeout.set(c, duration) }})
		}
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			test.set(&cfg)
			cache, err := NewWithConfig(cfg)
			if cache != nil {
				cache.Close()
			}
			if err == nil {
				t.Fatalf("NewWithConfig(%+v) accepted invalid configuration", cfg)
			}
		})
	}
	server := newTestServer(t)
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MinPoolSize, c.MaxPoolSize, c.MaxRetries = 1, 1, 0
	})
	if err := cache.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestTokenAuthenticationCanUsePlaintext(t *testing.T) {
	config := DefaultConfig()
	config.Token = "token"
	client, err := NewWithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPoolReusesSequentialConnections(t *testing.T) {
	for _, constructor := range []string{"New", "NewWithTimeout", "NewWithConfig"} {
		t.Run(constructor, func(t *testing.T) {
			server := newTestServer(t)
			var cache *NebulaCache
			var err error
			switch constructor {
			case "New":
				cache, err = New(server.address())
			case "NewWithTimeout":
				cache, err = NewWithTimeout(server.address(), time.Second)
			default:
				cache = newConfiguredTestClient(t, server, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { cache.Close() })
			for i := 0; i < 5; i++ {
				if err := cache.Ping(); err != nil {
					t.Fatal(err)
				}
				if err := cache.SetString("key", "value"); err != nil {
					t.Fatal(err)
				}
				value, found, err := cache.Get("key")
				if err != nil || !found || string(value) != "value" {
					t.Fatalf("Get() = (%q, %t, %v)", value, found, err)
				}
				if deleted, err := cache.Delete("key"); err != nil || !deleted {
					t.Fatalf("Delete() = (%t, %v)", deleted, err)
				}
			}
			accepted, _, _, requests := server.counts()
			if accepted != 1 || requests != 20 {
				t.Fatalf("accepted=%d requests=%d, want 1 and 20", accepted, requests)
			}
		})
	}
}

func TestPoolMinimumTargetOnFirstUse(t *testing.T) {
	server := newTestServer(t)
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MinPoolSize, c.MaxPoolSize = 3, 3
	})
	if err := cache.Ping(); err != nil {
		t.Fatal(err)
	}
	server.wait(t, func(accepted, active, peak, requests int) bool {
		return accepted >= 3 && active >= 3
	})
	for i := 0; i < 6; i++ {
		if err := cache.Ping(); err != nil {
			t.Fatal(err)
		}
	}
	accepted, active, _, _ := server.counts()
	if accepted != 3 || active != 3 {
		t.Fatalf("accepted=%d active=%d, want minimum target 3 without growth", accepted, active)
	}
}

func TestPoolConcurrentBound(t *testing.T) {
	server := newTestServer(t)
	entered := make(chan testFrame, 24)
	release := make(chan struct{})
	server.setHook(func(connection net.Conn, frame testFrame) bool {
		entered <- frame
		select {
		case <-release:
			return sendTestResponse(connection, 0, []byte(frame.key))
		case <-server.done:
			return false
		}
	})
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MaxPoolSize = 3
		c.MaxRetries = 0
	})
	start := make(chan struct{})
	results := make(chan error, 24)
	for i := 0; i < 24; i++ {
		go func(key string) {
			<-start
			value, found, err := cache.Get(key)
			if err == nil && (!found || string(value) != key) {
				err = errors.New("concurrent request received another request's response")
			}
			results <- err
		}(strconv.Itoa(i))
	}
	close(start)
	for i := 0; i < 3; i++ {
		awaitTestValue(t, entered)
	}
	select {
	case frame := <-entered:
		t.Fatalf("request exceeded pool bound while all leases were held: %+v", frame)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for i := 0; i < 24; i++ {
		if err := awaitTestValue(t, results); err != nil {
			t.Fatal(err)
		}
	}
	accepted, _, peak, requests := server.counts()
	if accepted != 3 || peak > 3 || requests != 24 {
		t.Fatalf("accepted=%d peak=%d requests=%d, want 3, <=3, 24", accepted, peak, requests)
	}
}

func TestPoolAcquisitionTimeout(t *testing.T) {
	server := newTestServer(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server.setHook(func(connection net.Conn, frame testFrame) bool {
		if frame.sequence == 1 {
			entered <- struct{}{}
			select {
			case <-release:
			case <-server.done:
				return false
			}
		}
		return sendTestResponse(connection, 0, []byte("PONG"))
	})
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MaxPoolSize = 1
		c.AcquireTimeout = 75 * time.Millisecond
		c.RequestTimeout = 2 * time.Second
	})
	first := make(chan error, 1)
	go func() { first <- cache.Ping() }()
	awaitTestValue(t, entered)
	second := make(chan error, 1)
	started := time.Now()
	go func() { _, _, err := cache.Get("waiting"); second <- err }()
	requireTestTimeout(t, awaitTestValue(t, second))
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond || elapsed > time.Second {
		t.Fatalf("acquisition took %v, want approximately 75ms independently of request timeout", elapsed)
	}
	requireTestRequests(t, server, 1)
	close(release)
	if err := awaitTestValue(t, first); err != nil {
		t.Fatal(err)
	}
	if err := cache.Ping(); err != nil {
		t.Fatalf("pool unusable after acquisition timeout: %v", err)
	}
	accepted, _, _, _ := server.counts()
	if accepted != 1 {
		t.Fatalf("accepted=%d, want original connection reused", accepted)
	}
}

func TestRequestTimeoutDiscardsAndReplacesConnection(t *testing.T) {
	server := newTestServer(t)
	entered := make(chan int, 1)
	closed := make(chan error, 1)
	server.setHook(func(connection net.Conn, frame testFrame) bool {
		if frame.sequence == 1 {
			entered <- frame.connection
			var extra [1]byte
			_, err := connection.Read(extra[:])
			closed <- err
			return false
		}
		return sendTestResponse(connection, 0, []byte("PONG"))
	})
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MaxPoolSize, c.MaxRetries = 1, 0
		c.RequestTimeout = 100 * time.Millisecond
		c.AcquireTimeout = 2 * time.Second
	})
	result := make(chan error, 1)
	go func() { result <- cache.Ping() }()
	awaitTestValue(t, entered)
	requireTestTimeout(t, awaitTestValue(t, result))
	if err := awaitTestValue(t, closed); err == nil {
		t.Fatal("timed-out socket was reused instead of closed")
	}
	server.wait(t, func(accepted, active, peak, requests int) bool { return active == 0 })
	for i := 0; i < 2; i++ {
		if err := cache.Ping(); err != nil {
			t.Fatalf("replacement request failed: %v", err)
		}
	}
	accepted, _, _, requests := server.counts()
	if accepted != 2 || requests != 3 {
		t.Fatalf("accepted=%d requests=%d, want 2 and 3", accepted, requests)
	}
}

func TestPoolConnectionExpiry(t *testing.T) {
	for _, expiry := range []string{"idle", "lifetime"} {
		t.Run(expiry, func(t *testing.T) {
			server := newTestServer(t)
			cache := newConfiguredTestClient(t, server, func(c *Config) {
				c.MaxPoolSize, c.MaxRetries = 1, 0
				if expiry == "idle" {
					c.IdleTimeout = 100 * time.Millisecond
				} else {
					c.MaxLifetime = 100 * time.Millisecond
				}
			})
			if err := cache.Ping(); err != nil {
				t.Fatal(err)
			}
			<-time.NewTimer(150 * time.Millisecond).C
			if err := cache.Ping(); err != nil {
				t.Fatal(err)
			}
			server.wait(t, func(accepted, active, peak, requests int) bool { return accepted >= 2 && active == 1 })
			accepted, _, _, _ := server.counts()
			if accepted != 2 {
				t.Fatalf("accepted=%d, want expired connection replaced", accepted)
			}
		})
	}
}

func TestPoolMaxLifetimeIsNotResetByUse(t *testing.T) {
	server := newTestServer(t)
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MaxPoolSize = 1
		c.MaxLifetime = 200 * time.Millisecond
		c.IdleTimeout = time.Second
	})
	if err := cache.Ping(); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ticker.C:
			if err := cache.Ping(); err != nil {
				t.Fatal(err)
			}
			accepted, _, _, _ := server.counts()
			if accepted >= 2 {
				return
			}
		case <-deadline.C:
			t.Fatal("repeated use reset maximum connection lifetime")
		}
	}
}

func failTestResponse(connection net.Conn, kind string) bool {
	var header [responseHeader]byte
	copy(header[:4], protocolMagic[:])
	header[4] = protocolVersion
	switch kind {
	case "truncated header":
		writeAll(connection, header[:5])
	case "truncated body":
		binary.BigEndian.PutUint32(header[6:10], 5)
		writeAll(connection, header[:])
		writeAll(connection, []byte("ab"))
	case "reset":
		if tcp, ok := connection.(*net.TCPConn); ok {
			tcp.SetLinger(0)
		}
	}
	return false
}

func TestGetRetriesTransientResponses(t *testing.T) {
	for _, kind := range []string{"EOF", "truncated header", "truncated body", "reset", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			server := newTestServer(t)
			server.setHook(func(connection net.Conn, frame testFrame) bool {
				if frame.sequence == 1 {
					if kind == "timeout" {
						var extra [1]byte
						connection.Read(extra[:])
						return false
					}
					return failTestResponse(connection, kind)
				}
				return sendTestResponse(connection, 0, []byte("recovered"))
			})
			cache := newConfiguredTestClient(t, server, func(c *Config) {
				c.MaxPoolSize = 1
				c.RequestTimeout = 150 * time.Millisecond
			})
			value, found, err := cache.Get("retry")
			if err != nil || !found || string(value) != "recovered" {
				t.Fatalf("Get() = (%q, %t, %v), want recovered value", value, found, err)
			}
			requireTestRequests(t, server, 2)
			accepted, _, _, _ := server.counts()
			if accepted != 2 {
				t.Fatalf("accepted=%d, want fresh connection for retry", accepted)
			}
		})
	}
}

func TestGetRetryLimit(t *testing.T) {
	for _, retries := range []int{0, 1, 2} {
		for _, kind := range []string{"EOF", "truncated header", "truncated body"} {
			t.Run(strconv.Itoa(retries)+"/"+kind, func(t *testing.T) {
				server := newTestServer(t)
				server.setHook(func(connection net.Conn, frame testFrame) bool {
					return failTestResponse(connection, kind)
				})
				cache := newConfiguredTestClient(t, server, func(c *Config) { c.MaxRetries = retries })
				if _, _, err := cache.Get("always fails"); err == nil {
					t.Fatal("Get() succeeded after every response failed")
				}
				requireTestRequests(t, server, retries+1)
			})
		}
	}
}

func TestNonGetRequestsAreNotRetriedAfterSend(t *testing.T) {
	operations := []struct {
		name string
		run  func(*NebulaCache) error
	}{
		{"SET", func(c *NebulaCache) error { return c.SetString("key", "value") }},
		{"DELETE", func(c *NebulaCache) error { _, err := c.Delete("key"); return err }},
		{"PING", func(c *NebulaCache) error { return c.Ping() }},
	}
	for _, operation := range operations {
		for _, kind := range []string{"EOF", "truncated header", "truncated body", "timeout"} {
			t.Run(operation.name+"/"+kind, func(t *testing.T) {
				server := newTestServer(t)
				server.setHook(func(connection net.Conn, frame testFrame) bool {
					if kind == "timeout" {
						var extra [1]byte
						connection.Read(extra[:])
						return false
					}
					return failTestResponse(connection, kind)
				})
				cache := newConfiguredTestClient(t, server, func(c *Config) {
					c.RequestTimeout = 100 * time.Millisecond
				})
				if err := operation.run(cache); err == nil {
					t.Fatal("request unexpectedly succeeded")
				}
				requireTestRequests(t, server, 1)
			})
		}
	}
}

func TestGetDoesNotRetryProtocolOrStatusErrors(t *testing.T) {
	for _, kind := range []string{"magic", "version", "unknown status", "oversized", "not found", "invalid argument", "internal error"} {
		t.Run(kind, func(t *testing.T) {
			server := newTestServer(t)
			server.setHook(func(connection net.Conn, frame testFrame) bool {
				var header [responseHeader]byte
				copy(header[:4], protocolMagic[:])
				header[4] = protocolVersion
				switch kind {
				case "magic":
					copy(header[:4], "NOPE")
				case "version":
					header[4] = protocolVersion + 1
				case "unknown status":
					header[5] = 6
				case "oversized":
					binary.BigEndian.PutUint32(header[6:10], maxValueLength+1)
				case "not found":
					header[5] = 1
				case "invalid argument":
					header[5] = 2
				case "internal error":
					header[5] = 3
				}
				return writeAll(connection, header[:]) == nil
			})
			cache := newConfiguredTestClient(t, server, nil)
			value, found, err := cache.Get("key")
			var protocolErr *CacheProtocolError
			var responseErr *CacheResponseError
			switch kind {
			case "not found":
				if err != nil || found || value != nil {
					t.Fatalf("Get() = (%v, %t, %v), want missing key", value, found, err)
				}
			case "invalid argument", "internal error":
				wantStatus := byte(2)
				if kind == "internal error" {
					wantStatus = 3
				}
				if !errors.As(err, &responseErr) || responseErr.Status != wantStatus {
					t.Fatalf("Get() error = %v, want status %d", err, wantStatus)
				}
			default:
				if !errors.As(err, &protocolErr) {
					t.Fatalf("Get() error = %v, want CacheProtocolError", err)
				}
			}
			requireTestRequests(t, server, 1)
		})
	}
}

func TestRetryBudgetSharedAndSuccessfulRetriesDoNotRefill(t *testing.T) {
	server := newTestServer(t)
	server.setHook(func(connection net.Conn, frame testFrame) bool {
		if frame.attempt == 1 {
			return false
		}
		return sendTestResponse(connection, 0, []byte("retried"))
	})
	cache := newConfiguredTestClient(t, server, func(c *Config) { c.MaxRetries = 1 })
	for i := 0; i < 10; i++ {
		value, found, err := cache.Get("budget/" + strconv.Itoa(i))
		if err != nil || !found || string(value) != "retried" {
			t.Fatalf("retry %d = (%q, %t, %v), want initial budget of ten", i, value, found, err)
		}
	}
	for i := 10; i < 21; i++ {
		if _, _, err := cache.Get("budget/" + strconv.Itoa(i)); err == nil {
			t.Fatalf("request %d retried despite exhausted shared budget", i)
		}
	}
	requireTestRequests(t, server, 31)
}

func TestRetryBudgetRefillsAtOneTokenPerSecond(t *testing.T) {
	server := newTestServer(t)
	server.setHook(func(connection net.Conn, frame testFrame) bool {
		if strings.HasPrefix(frame.key, "fail/") {
			return false
		}
		return sendTestResponse(connection, 0, []byte("ok"))
	})
	cache := newConfiguredTestClient(t, server, func(c *Config) { c.MaxRetries = 1 })
	fail := func(key string, wantFrames int) {
		t.Helper()
		_, _, _, before := server.counts()
		if _, _, err := cache.Get("fail/" + key); err == nil {
			t.Fatal("Get() unexpectedly succeeded")
		}
		_, _, _, after := server.counts()
		if after-before != wantFrames {
			t.Fatalf("%s sent %d frames, want %d", key, after-before, wantFrames)
		}
	}
	for i := 0; i < 10; i++ {
		fail("drain/"+strconv.Itoa(i), 2)
	}
	fail("empty", 1)
	time.Sleep(1100 * time.Millisecond)
	fail("refilled", 2)
	fail("spent", 1)
}

func TestCloseIdleConnectionsAndRejectRequests(t *testing.T) {
	server := newTestServer(t)
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MinPoolSize, c.MaxPoolSize = 2, 2
	})
	if err := cache.Ping(); err != nil {
		t.Fatal(err)
	}
	server.wait(t, func(accepted, active, peak, requests int) bool { return active == 2 })
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	server.wait(t, func(accepted, active, peak, requests int) bool { return active == 0 })
	if err := cache.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
	operations := []struct {
		name string
		run  func() error
	}{
		{"PING", cache.Ping},
		{"SET", func() error { return cache.Set("key", []byte("value")) }},
		{"SET TTL", func() error { return cache.SetWithTTL("key", nil, 1) }},
		{"SET string", func() error { return cache.SetString("key", "value") }},
		{"SET string TTL", func() error { return cache.SetStringWithTTL("key", "value", 1) }},
		{"GET", func() error { _, _, err := cache.Get("key"); return err }},
		{"DELETE", func() error { _, err := cache.Delete("key"); return err }},
	}
	for _, operation := range operations {
		if err := operation.run(); !errors.Is(err, ErrClosed) {
			t.Errorf("%s after Close() = %v, want ErrClosed", operation.name, err)
		}
	}
	requireTestRequests(t, server, 1)
	accepted, _, _, _ := server.counts()
	if accepted != 2 {
		t.Fatalf("closed pool opened new sockets: accepted=%d", accepted)
	}
}

func TestCloseInterruptsActiveRequestsAndWaiters(t *testing.T) {
	server := newTestServer(t)
	entered := make(chan struct{}, 1)
	peerClosed := make(chan error, 1)
	server.setHook(func(connection net.Conn, frame testFrame) bool {
		entered <- struct{}{}
		var extra [1]byte
		_, err := connection.Read(extra[:])
		peerClosed <- err
		return false
	})
	cache := newConfiguredTestClient(t, server, func(c *Config) {
		c.MaxPoolSize = 1
		c.RequestTimeout, c.AcquireTimeout = 10*time.Second, 10*time.Second
	})
	active := make(chan error, 1)
	go func() { _, _, err := cache.Get("active"); active <- err }()
	awaitTestValue(t, entered)
	started := make(chan struct{}, 8)
	waiters := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			started <- struct{}{}
			_, _, err := cache.Get("waiting")
			waiters <- err
		}()
	}
	for i := 0; i < 8; i++ {
		awaitTestValue(t, started)
	}
	select {
	case err := <-waiters:
		t.Fatalf("waiter returned before Close(): %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	closers := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { closers <- cache.Close() }()
	}
	for i := 0; i < 8; i++ {
		if err := awaitTestValue(t, closers); err != nil {
			t.Fatalf("concurrent Close() = %v", err)
		}
	}
	if err := awaitTestValue(t, active); !errors.Is(err, ErrClosed) {
		t.Fatalf("active request error = %v, want ErrClosed", err)
	}
	for i := 0; i < 8; i++ {
		if err := awaitTestValue(t, waiters); !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter error = %v, want ErrClosed", err)
		}
	}
	if err := awaitTestValue(t, peerClosed); err == nil {
		t.Fatal("active socket remained open after Close()")
	}
	server.wait(t, func(accepted, active, peak, requests int) bool { return active == 0 })
	requireTestRequests(t, server, 1)
	if err := cache.Ping(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ping() after Close() = %v, want ErrClosed", err)
	}
}

func TestCloseBeforeFirstUse(t *testing.T) {
	server := newTestServer(t)
	cache := newConfiguredTestClient(t, server, nil)
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Ping(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ping() = %v, want ErrClosed", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	accepted, _, _, requests := server.counts()
	if accepted != 0 || requests != 0 {
		t.Fatalf("unused closed pool contacted server: accepted=%d requests=%d", accepted, requests)
	}
}

func writeAll(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := writer.Write(value)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		value = value[written:]
	}
	return nil
}
