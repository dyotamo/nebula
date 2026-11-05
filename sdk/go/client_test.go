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

type testServer struct {
	listener net.Listener
	mu       sync.Mutex
	entries  map[string]storedEntry
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &testServer{listener: listener, entries: make(map[string]storedEntry)}
	go server.serve()
	t.Cleanup(func() {
		listener.Close()
	})
	return server
}

func (server *testServer) address() string {
	return server.listener.Addr().String()
}

func (server *testServer) serve() {
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			return
		}
		go server.handle(connection)
	}
}

func (server *testServer) handle(connection net.Conn) {
	defer connection.Close()
	var header [requestHeader]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return
	}
	if string(header[:4]) != string(protocolMagic[:]) || header[4] != protocolVersion {
		return
	}

	opcode := header[5]
	keyLength := binary.BigEndian.Uint32(header[6:10])
	valueLength := binary.BigEndian.Uint32(header[10:14])
	ttlSecs := binary.BigEndian.Uint64(header[14:22])
	keyBytes := make([]byte, keyLength)
	value := make([]byte, valueLength)
	if _, err := io.ReadFull(connection, keyBytes); err != nil {
		return
	}
	if _, err := io.ReadFull(connection, value); err != nil {
		return
	}

	key := string(keyBytes)
	var status byte
	var responseValue []byte
	server.mu.Lock()
	switch opcode {
	case 1:
		server.entries[key] = storedEntry{value: append([]byte(nil), value...), ttlSecs: ttlSecs}
	case 2:
		if key == "failure" {
			status = 3
			responseValue = []byte("forced failure")
		} else if entry, exists := server.entries[key]; exists {
			responseValue = entry.value
		} else {
			status = 1
		}
	case 3:
		if _, exists := server.entries[key]; !exists {
			status = 1
		} else {
			delete(server.entries, key)
		}
	case 4:
		responseValue = []byte("PONG")
	default:
		status = 2
		responseValue = []byte("invalid request")
	}
	server.mu.Unlock()

	var responseHeader [responseHeader]byte
	copy(responseHeader[:4], protocolMagic[:])
	responseHeader[4] = protocolVersion
	responseHeader[5] = status
	binary.BigEndian.PutUint32(responseHeader[6:10], uint32(len(responseValue)))
	writeAll(connection, responseHeader[:])
	writeAll(connection, responseValue)
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
	if _, err := New("::1:7001"); err == nil {
		t.Fatal("New() accepted an unbracketed IPv6 address")
	}
	if _, err := New("localhost:70000"); err == nil {
		t.Fatal("New() accepted an out-of-range port")
	}
	if _, err := NewWithTimeout("localhost:7001", 0); err == nil {
		t.Fatal("NewWithTimeout() accepted a non-positive timeout")
	}
	if client, err := New(); err != nil || client == nil {
		t.Fatalf("New() = (%v, %v), want a default client", client, err)
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
