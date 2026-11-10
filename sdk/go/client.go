package nebulacache

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultAddress is the local binary-protocol endpoint.
	DefaultAddress  = "127.0.0.1:7001"
	// DefaultTimeout is the default connect, request, and acquisition timeout.
	DefaultTimeout  = 5 * time.Second
	maxKeyLength    = 1024
	maxValueLength  = 8 * 1024 * 1024
	requestHeader   = 22
	responseHeader  = 10
	protocolVersion = 1
)

var protocolMagic = [4]byte{'N', 'E', 'B', 'U'}

// Config controls the connection pool, deadlines, and optional TLS authentication.
// Start with DefaultConfig; the zero value is not valid.
type Config struct {
	// Address is host:port, with brackets required around IPv6 hosts.
	Address        string
	// MinPoolSize is the pool size targeted lazily during acquisition.
	MinPoolSize    int
	// MaxPoolSize bounds established connections and in-progress dials together.
	MaxPoolSize    int
	// ConnectTimeout bounds each dial, including the TLS handshake when enabled.
	ConnectTimeout time.Duration
	// RequestTimeout bounds a request's writes and reads, and each AUTH exchange.
	RequestTimeout time.Duration
	// AcquireTimeout bounds pool acquisition, including waiting and dialing.
	// AUTH uses its own RequestTimeout and is not canceled by this deadline.
	AcquireTimeout time.Duration
	// IdleTimeout retires unused connections when the pool is next acquired.
	IdleTimeout    time.Duration
	// MaxLifetime retires aged connections during acquisition or release, not mid-request.
	MaxLifetime    time.Duration
	// MaxRetries limits additional GET attempts after transient transport failures.
	MaxRetries     int
	// TLSConfig enables TLS when non-nil and is cloned for each dial.
	TLSConfig      *tls.Config
	// Token enables per-connection AUTH when non-empty and requires verified TLS.
	Token          string
}

// DefaultConfig returns a valid configuration with a lazy pool of at most ten connections.
func DefaultConfig() Config {
	return Config{
		Address:        DefaultAddress,
		MaxPoolSize:    10,
		ConnectTimeout: DefaultTimeout,
		RequestTimeout: DefaultTimeout,
		AcquireTimeout: DefaultTimeout,
		IdleTimeout:    60 * time.Second,
		MaxLifetime:    300 * time.Second,
		MaxRetries:     2,
	}
}

type pooledConnection struct {
	net.Conn
	createdAt time.Time
	idleSince time.Time
}

// NebulaCache is a concurrency-safe client for the version 1 binary TCP protocol.
// Connections are opened lazily and carry at most one in-flight request each.
// Keys must be non-empty UTF-8 of at most 1024 bytes; values are limited to 8 MiB.
// Construct clients with New, NewWithTimeout, or NewWithConfig, then call Close.
type NebulaCache struct {
	address     string
	endpoint    string
	config      Config
	mu          sync.Mutex
	connections map[*pooledConnection]struct{}
	idle        []*pooledConnection
	dialing     int
	closed      bool
	changed     chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	retryTokens float64
	retryRefill time.Time
}

type response struct {
	status byte
	value  []byte
}

// New creates a client with defaults and an optional host:port address.
// It does not connect to the server until the first operation.
func New(address ...string) (*NebulaCache, error) {
	if len(address) > 1 {
		return nil, fmt.Errorf("New accepts at most one address")
	}

	target := DefaultAddress
	if len(address) == 1 {
		target = address[0]
	}

	return NewWithTimeout(target, DefaultTimeout)
}

// NewWithTimeout sets the connect and request timeouts; acquisition keeps its default.
func NewWithTimeout(address string, timeout time.Duration) (*NebulaCache, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be greater than zero")
	}

	config := DefaultConfig()
	config.Address = address
	config.ConnectTimeout = timeout
	config.RequestTimeout = timeout
	return NewWithConfig(config)
}

// NewWithConfig validates config and creates a client without opening connections.
func NewWithConfig(config Config) (*NebulaCache, error) {
	if config.MinPoolSize < 0 || config.MaxPoolSize <= 0 || config.MinPoolSize > config.MaxPoolSize {
		return nil, fmt.Errorf("pool sizes must satisfy 0 <= minPoolSize <= maxPoolSize and maxPoolSize > 0")
	}
	if config.ConnectTimeout <= 0 || config.RequestTimeout <= 0 || config.AcquireTimeout <= 0 || config.IdleTimeout <= 0 || config.MaxLifetime <= 0 {
		return nil, fmt.Errorf("timeouts and maxLifetime must be greater than zero")
	}
	if config.MaxRetries < 0 {
		return nil, fmt.Errorf("maxRetries must not be negative")
	}
	if config.Address == "" {
		return nil, fmt.Errorf("address must be a host:port string")
	}
	if config.Token != "" && (config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify) {
		return nil, fmt.Errorf("token authentication requires TLS with certificate verification enabled")
	}
	host, port, err := parseAddress(config.Address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &NebulaCache{
		address:     config.Address,
		endpoint:    net.JoinHostPort(host, strconv.Itoa(port)),
		config:      config,
		connections: make(map[*pooledConnection]struct{}),
		changed:     make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		retryTokens: 10,
		retryRefill: time.Now(),
	}, nil
}

// Close permanently closes the client, interrupts active connections, and wakes waiters.
// Repeated calls are safe and return nil.
func (c *NebulaCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	for connection := range c.connections {
		connection.Close()
		delete(c.connections, connection)
	}
	c.idle = nil
	c.notify()
	return nil
}

// notify broadcasts a pool state change; the caller must hold c.mu.
func (c *NebulaCache) notify() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *NebulaCache) acquire() (*pooledConnection, error) {
	ctx, cancel := context.WithTimeout(c.ctx, c.config.AcquireTimeout)
	defer cancel()
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return nil, connectionError(c.address, err)
		}
		now := time.Now()
		idle := c.idle[:0]
		for _, connection := range c.idle {
			if now.Sub(connection.idleSince) >= c.config.IdleTimeout || now.Sub(connection.createdAt) >= c.config.MaxLifetime {
				connection.Close()
				delete(c.connections, connection)
			} else {
				idle = append(idle, connection)
			}
		}
		c.idle = idle
		total := len(c.connections) + c.dialing
		if len(c.idle) > 0 && total >= c.config.MinPoolSize {
			last := len(c.idle) - 1
			connection := c.idle[last]
			c.idle[last] = nil
			c.idle = c.idle[:last]
			c.mu.Unlock()
			return connection, nil
		}
		if total < c.config.MaxPoolSize {
			count := c.config.MinPoolSize - total
			if count < 1 {
				count = 1
			}
			c.dialing += count
			c.mu.Unlock()
			if err := c.fill(ctx, count); err != nil {
				return nil, err
			}
			continue
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
	}
}

// fill consumes dial slots reserved under c.mu so concurrent acquisitions cannot exceed capacity.
func (c *NebulaCache) fill(ctx context.Context, count int) error {
	for remaining := count; remaining > 0; remaining-- {
		dialer := net.Dialer{Timeout: c.config.ConnectTimeout}
		var connection net.Conn
		var err error
		if c.config.TLSConfig == nil {
			connection, err = dialer.DialContext(ctx, "tcp", c.endpoint)
		} else {
			tlsDialer := tls.Dialer{NetDialer: &dialer, Config: c.config.TLSConfig.Clone()}
			connection, err = tlsDialer.DialContext(ctx, "tcp", c.endpoint)
		}
		if err == nil && c.config.Token != "" {
			err = authenticateConnection(connection, c.config.Token, c.config.RequestTimeout)
		}
		c.mu.Lock()
		c.dialing--
		if c.closed || err != nil {
			c.dialing -= remaining - 1
			if connection != nil {
				connection.Close()
			}
			closed := c.closed
			c.notify()
			c.mu.Unlock()
			if closed {
				return ErrClosed
			}
			return connectionError(c.address, err)
		}
		now := time.Now()
		pooled := &pooledConnection{Conn: connection, createdAt: now, idleSince: now}
		c.connections[pooled] = struct{}{}
		c.idle = append(c.idle, pooled)
		c.notify()
		c.mu.Unlock()
	}
	return nil
}

func (c *NebulaCache) release(connection *pooledConnection, healthy bool) {
	if healthy && connection.SetDeadline(time.Time{}) != nil {
		healthy = false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !healthy || time.Since(connection.createdAt) >= c.config.MaxLifetime {
		connection.Close()
		delete(c.connections, connection)
	} else {
		connection.idleSince = time.Now()
		c.idle = append(c.idle, connection)
	}
	c.notify()
}

// Ping checks that the server returns a successful PONG response.
func (c *NebulaCache) Ping() error {
	result, err := c.request(4, nil, nil, 0)
	if err != nil {
		return err
	}

	if result.status != 0 {
		return responseError(result)
	}

	if string(result.value) != "PONG" {
		return &CacheProtocolError{Message: "PING response did not contain PONG"}
	}

	return nil
}

// Set stores raw bytes without expiration. It is not automatically retried.
func (c *NebulaCache) Set(key string, value []byte) error {
	return c.SetWithTTL(key, value, 0)
}

// SetWithTTL stores raw bytes with a TTL in seconds; zero disables expiration.
// Writes are not retried because a transport failure may occur after the server applies them.
func (c *NebulaCache) SetWithTTL(key string, value []byte, ttlSecs uint64) error {
	encodedKey, err := encodeKey(key)
	if err != nil {
		return err
	}

	if len(value) > maxValueLength {
		return fmt.Errorf("value exceeds the %d-byte protocol limit", maxValueLength)
	}

	result, err := c.request(1, encodedKey, value, ttlSecs)
	if err != nil {
		return err
	}

	if result.status != 0 {
		return responseError(result)
	}

	return nil
}

// SetString stores a valid UTF-8 string without expiration.
func (c *NebulaCache) SetString(key, value string) error {
	return c.SetStringWithTTL(key, value, 0)
}

// SetStringWithTTL stores a valid UTF-8 string with a TTL in seconds; zero disables expiration.
func (c *NebulaCache) SetStringWithTTL(key, value string, ttlSecs uint64) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("value must be valid UTF-8")
	}

	return c.SetWithTTL(key, []byte(value), ttlSecs)
}

// Get returns raw bytes and whether the key exists; a miss returns (nil, false, nil).
// Only transient transport failures are eligible for bounded, jittered retries.
func (c *NebulaCache) Get(key string) ([]byte, bool, error) {
	encodedKey, err := encodeKey(key)
	if err != nil {
		return nil, false, err
	}

	result, err := c.request(2, encodedKey, nil, 0)
	if err != nil {
		return nil, false, err
	}

	if result.status == 1 {
		return nil, false, nil
	}

	if result.status != 0 {
		return nil, false, responseError(result)
	}

	return result.value, true, nil
}

// Delete returns whether a key was removed; a missing key returns (false, nil).
// Deletions are not automatically retried.
func (c *NebulaCache) Delete(key string) (bool, error) {
	encodedKey, err := encodeKey(key)
	if err != nil {
		return false, err
	}

	result, err := c.request(3, encodedKey, nil, 0)
	if err != nil {
		return false, err
	}

	if result.status == 1 {
		return false, nil
	}

	if result.status != 0 {
		return false, responseError(result)
	}

	return true, nil
}

// request retries only GET, using a shared ten-token budget refilled at one token per second.
// Jittered exponential backoff is canceled by Close; server and protocol errors are not retried.
func (c *NebulaCache) request(opcode byte, key, value []byte, ttlSecs uint64) (response, error) {
	for attempt := 0; ; attempt++ {
		result, err := c.requestOnce(opcode, key, value, ttlSecs)
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return response{}, ErrClosed
		}
		now := time.Now()
		c.retryTokens += now.Sub(c.retryRefill).Seconds()
		if c.retryTokens > 10 {
			c.retryTokens = 10
		}
		c.retryRefill = now
		if err == nil {
			c.mu.Unlock()
			return result, nil
		}
		if opcode != 2 || attempt >= c.config.MaxRetries || !transientTransportError(err) || c.retryTokens < 1 {
			c.mu.Unlock()
			return response{}, err
		}
		c.retryTokens--
		c.mu.Unlock()
		limit := time.Second
		if attempt < 5 {
			limit = 50 * time.Millisecond * time.Duration(1<<uint(attempt))
		}
		timer := time.NewTimer(time.Duration(rand.Int63n(int64(limit) + 1)))
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return response{}, ErrClosed
		case <-timer.C:
		}
	}
}

func transientTransportError(err error) bool {
	if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.Timeout() || dnsErr.Temporary()
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && (networkErr.Timeout() || networkErr.Temporary()) {
		return true
	}
	for _, cause := range []error{syscall.ECONNRESET, syscall.ECONNREFUSED, syscall.ECONNABORTED, syscall.EPIPE, syscall.ETIMEDOUT, syscall.ENETUNREACH, syscall.EHOSTUNREACH} {
		if errors.Is(err, cause) {
			return true
		}
	}
	return false
}

// requestOnce reuses a connection only after a complete, validated response.
// Server error statuses still leave framing intact and allow reuse.
func (c *NebulaCache) requestOnce(opcode byte, key, value []byte, ttlSecs uint64) (response, error) {
	connection, err := c.acquire()
	if err != nil {
		return response{}, err
	}
	healthy := false
	defer func() { c.release(connection, healthy) }()

	if err := connection.SetDeadline(time.Now().Add(c.config.RequestTimeout)); err != nil {
		return response{}, connectionError(c.address, err)
	}

	var header [requestHeader]byte
	copy(header[:4], protocolMagic[:])
	header[4] = protocolVersion
	header[5] = opcode
	binary.BigEndian.PutUint32(header[6:10], uint32(len(key)))
	binary.BigEndian.PutUint32(header[10:14], uint32(len(value)))
	binary.BigEndian.PutUint64(header[14:22], ttlSecs)

	for _, part := range [][]byte{header[:], key, value} {
		if err := writeFull(connection, part); err != nil {
			return response{}, connectionError(c.address, err)
		}
	}

	result, err := readResponse(connection)
	if err != nil {
		var protocolErr *CacheProtocolError
		if errors.As(err, &protocolErr) {
			return response{}, protocolErr
		}

		return response{}, connectionError(c.address, err)
	}

	if opcode == 4 && result.status == 0 && string(result.value) != "PONG" {
		return response{}, &CacheProtocolError{Message: "PING response did not contain PONG"}
	}
	healthy = true
	return result, nil
}

func authenticateConnection(connection net.Conn, token string, timeout time.Duration) error {
	if err := connection.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	var header [requestHeader]byte
	copy(header[:4], protocolMagic[:])
	header[4] = protocolVersion
	header[5] = 5
	binary.BigEndian.PutUint32(header[10:14], uint32(len(token)))
	for _, part := range [][]byte{header[:], []byte(token)} {
		if err := writeFull(connection, part); err != nil {
			return err
		}
	}
	result, err := readResponse(connection)
	if err != nil {
		return err
	}
	if result.status != 0 || string(result.value) != "AUTH_RESULT" {
		return errors.New("authentication failed")
	}
	return connection.SetDeadline(time.Time{})
}

// readResponse reads big-endian version 1 framing and validates lengths before allocating.
// Truncated frames remain transport errors, allowing GET to retry them.
func readResponse(reader io.Reader) (response, error) {
	var header [responseHeader]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return response{}, incompleteResponse(err)
	}

	if !bytes.Equal(header[:4], protocolMagic[:]) {
		return response{}, &CacheProtocolError{Message: "response has invalid magic bytes"}
	}

	if header[4] != protocolVersion {
		return response{}, &CacheProtocolError{Message: fmt.Sprintf("unsupported protocol version: %d", header[4])}
	}

	status := header[5]
	if status > 5 {
		return response{}, &CacheProtocolError{Message: fmt.Sprintf("unknown response status: %d", status)}
	}

	valueLength := binary.BigEndian.Uint32(header[6:10])
	if valueLength > maxValueLength {
		return response{}, &CacheProtocolError{Message: fmt.Sprintf("response exceeds the %d-byte protocol limit", maxValueLength)}
	}

	value := make([]byte, int(valueLength))
	if _, err := io.ReadFull(reader, value); err != nil {
		return response{}, incompleteResponse(err)
	}

	return response{status: status, value: value}, nil
}

func incompleteResponse(err error) error {
	return err
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func encodeKey(key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("key must not be empty")
	}

	if !utf8.ValidString(key) {
		return nil, fmt.Errorf("key must be valid UTF-8")
	}

	encodedKey := []byte(key)
	if len(encodedKey) > maxKeyLength {
		return nil, fmt.Errorf("key exceeds the %d-byte protocol limit", maxKeyLength)
	}

	return encodedKey, nil
}

func responseError(result response) error {
	statusName := map[byte]string{
		1: "NOT_FOUND",
		2: "INVALID_ARGUMENT",
		3: "INTERNAL_ERROR",
		4: "UNAUTHORIZED",
		5: "FORBIDDEN",
	}[result.status]
	message := strings.TrimRightFunc(strings.ToValidUTF8(string(result.value), "\uFFFD"), unicode.IsSpace)
	return &CacheResponseError{
		Status:  result.status,
		Message: fmt.Sprintf("%s: %s", statusName, message),
	}
}

func connectionError(address string, err error) error {
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return &CacheConnectionError{Address: address, Err: err, TimedOut: true}
	}

	return &CacheConnectionError{Address: address, Err: err}
}

func parseAddress(address string) (string, int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" || !decimal(portText) {
		return "", 0, fmt.Errorf("address must be a host:port string; bracket IPv6 addresses")
	}

	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("port must be between 1 and 65535")
	}

	return host, port, nil
}

func decimal(value string) bool {
	if value == "" {
		return false
	}

	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}

	return true
}
