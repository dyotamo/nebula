// Package nebulacache provides a pooled client for NebulaCache's version 1 binary
// TCP protocol, with optional TLS and per-connection token authentication.
// Clients connect lazily and must be closed when no longer needed.
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
	// DefaultAddress is the local binary-protocol endpoint used when New has no address.
	DefaultAddress = "127.0.0.1:17171"
	// DefaultTimeout is the default connect, request, and acquisition timeout.
	// These are separate phase limits, not a single end-to-end operation deadline.
	DefaultTimeout = 5 * time.Second
	// Length limits apply to encoded bytes, not Unicode code points.
	maxKeyLength   = 1024
	maxValueLength = 8 * 1024 * 1024
	// Requests contain magic, version, opcode, key/value lengths, and an unsigned TTL.
	requestHeader = 22
	// Responses contain magic, version, status, and a value length; no key is echoed.
	responseHeader  = 10
	protocolVersion = 1
)

// protocolMagic identifies binary frames in both directions; it is not a terminator.
var protocolMagic = [4]byte{'N', 'E', 'B', 'U'}

// Config controls connection pooling, phase deadlines, TLS, and token authentication.
// Start with DefaultConfig; the zero value is not valid, and zero durations do not
// disable timeouts. NewWithConfig copies the struct, but TLSConfig remains a pointer.
type Config struct {
	// Address is host:port, with brackets required around IPv6 hosts.
	// The host must be non-empty and the port must be numeric, between 1 and 65535.
	Address string
	// MinPoolSize is the pool size targeted lazily during acquisition.
	// Zero disables minimum-size filling; no background task maintains this target.
	MinPoolSize int
	// MaxPoolSize bounds established connections and in-progress dials together.
	// It must be positive and at least MinPoolSize; acquisitions wait when full.
	MaxPoolSize int
	// ConnectTimeout bounds each dial, including the TLS handshake when enabled.
	// The acquisition deadline may end dialing sooner; AUTH is a separate exchange.
	ConnectTimeout time.Duration
	// RequestTimeout bounds a request's writes and reads, and each AUTH exchange.
	// Each attempt gets a fresh deadline after acquisition, shared by writing and reading.
	RequestTimeout time.Duration
	// AcquireTimeout bounds pool acquisition, including waiting and dialing.
	// AUTH uses its own RequestTimeout and is not canceled by this deadline.
	AcquireTimeout time.Duration
	// IdleTimeout retires unused connections when the pool is next acquired.
	// Reuse restarts idle time; expiration is lazy rather than driven by a timer.
	IdleTimeout time.Duration
	// MaxLifetime retires aged connections during acquisition or release, not mid-request.
	// Unlike IdleTimeout, its age is not reset when a connection is reused.
	MaxLifetime time.Duration
	// MaxRetries limits additional GET attempts after transient transport failures.
	// Zero disables retries; a shared client-wide token budget can stop retries sooner.
	MaxRetries int
	// TLSConfig enables TLS when non-nil and is cloned for each dial.
	// Do not mutate the supplied configuration while the client is using it.
	TLSConfig *tls.Config
	// Token enables per-connection AUTH when non-empty.
	// It does not enable TLS: without TLSConfig, the token travels in plaintext.
	Token string
}

// DefaultConfig returns a valid configuration with a lazy pool of at most ten connections.
// It uses five-second phase timeouts, a one-minute idle timeout, a five-minute
// maximum lifetime, and up to two GET retries. TLS and authentication are disabled.
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

// pooledConnection tracks separate lifetime and idle clocks. Creation time is fixed
// after dialing and AUTH succeed; idle time is updated only when returned to the pool.
type pooledConnection struct {
	net.Conn
	createdAt time.Time
	idleSince time.Time
}

// NebulaCache is a concurrency-safe client for the version 1 binary TCP protocol.
// Connections are opened lazily and carry at most one in-flight request each.
// Keys must be non-empty UTF-8 of at most 1024 bytes; values are limited to 8 MiB.
// Construct clients with New, NewWithTimeout, or NewWithConfig, then call Close.
// The zero value is not usable, and a client must not be copied after first use.
// Only GET is automatically retried; mutation errors can leave the server outcome unknown.
type NebulaCache struct {
	// Keep the caller's address for diagnostics and a normalized endpoint for dialing.
	address  string
	endpoint string
	config   Config
	// mu protects pool membership, dial reservations, closure, and retry accounting.
	mu sync.Mutex
	// connections includes idle and leased sockets; idle is the available subset.
	connections map[*pooledConnection]struct{}
	idle        []*pooledConnection
	// Reservations count toward capacity before network work begins outside the lock.
	dialing int
	closed  bool
	// Each state change closes this generation, waking every waiter that captured it.
	changed chan struct{}
	// Client lifetime cancellation interrupts acquisition dials and retry backoff.
	ctx    context.Context
	cancel context.CancelFunc
	// Fractional tokens preserve elapsed-time refill across requests and goroutines.
	retryTokens float64
	retryRefill time.Time
}

// response holds a fully consumed frame. A nonzero status is a server outcome,
// not a framing error, so callers interpret it after the connection can be reused.
type response struct {
	status byte
	value  []byte
}

// New creates a client with defaults and an optional host:port address.
// With no argument it uses DefaultAddress; an empty or malformed address is an error.
// It does not connect to the server until the first operation, so construction
// validates configuration but does not establish reachability or authentication.
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
// The timeout must be positive and applies separately to each phase and attempt,
// not to the total duration including pool acquisition and retry backoff.
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
// Pool bounds, positive durations, a nonnegative retry count, and a numeric-port
// endpoint are required. TLS and token acceptance are checked only on first dial.
func NewWithConfig(config Config) (*NebulaCache, error) {
	// Reject inconsistent bounds up front so slot reservation cannot exceed capacity.
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
	host, port, err := parseAddress(config.Address)
	if err != nil {
		return nil, err
	}
	// A single lifetime context ties future dials and retry waits to Close without
	// starting any goroutines or performing network I/O during construction.
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
// Repeated and concurrent calls are safe and return nil; socket close errors are ignored.
// It cancels in-progress dials but does not wait for connection setup to finish.
// An AUTH exchange already underway retains its own request deadline.
func (c *NebulaCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	// Publish closure before cancellation so awakened operations report ErrClosed.
	c.closed = true
	c.cancel()
	// Membership includes leased sockets, allowing blocked I/O to be interrupted.
	for connection := range c.connections {
		connection.Close()
		delete(c.connections, connection)
	}
	c.idle = nil
	c.notify()
	return nil
}

// notify broadcasts a pool state change; the caller must hold c.mu.
// Replacing the closed channel lets current waiters wake without making future
// waiters spin on an already-closed notification.
func (c *NebulaCache) notify() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// acquire leases one socket exclusively, filling toward the minimum when needed.
// One acquisition context spans all waits and dials, rather than restarting its
// timeout after every notification or newly created connection.
func (c *NebulaCache) acquire() (*pooledConnection, error) {
	ctx, cancel := context.WithTimeout(c.ctx, c.config.AcquireTimeout)
	defer cancel()
	for {
		c.mu.Lock()
		// Closure takes precedence over the lifetime context's cancellation error.
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return nil, connectionError(c.address, err)
		}
		now := time.Now()
		// Prune only idle sockets: expiry must not interrupt an in-flight request.
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
		// Reserved dials count toward both capacity and the lazy minimum target.
		total := len(c.connections) + c.dialing
		if len(c.idle) > 0 && total >= c.config.MinPoolSize {
			// Prefer the most recently returned socket and remove it from availability
			// while retaining membership so Close can still interrupt the lease.
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
			// Reserve the entire fill before unlocking; concurrent callers cannot
			// race to claim the same capacity while network work is in progress.
			c.dialing += count
			c.mu.Unlock()
			if err := c.fill(ctx, count); err != nil {
				return nil, err
			}
			continue
		}
		// Capture the generation while locked to avoid missing a release or Close
		// between checking capacity and beginning to wait. Recheck on either wakeup.
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
	}
}

// fill consumes dial slots reserved under c.mu so concurrent acquisitions cannot exceed capacity.
// Successful connections become available individually; an error releases all
// remaining reservations without removing connections already added by this fill.
func (c *NebulaCache) fill(ctx context.Context, count int) error {
	for remaining := count; remaining > 0; remaining-- {
		dialer := net.Dialer{Timeout: c.config.ConnectTimeout}
		var connection net.Conn
		var err error
		if c.config.TLSConfig == nil {
			connection, err = dialer.DialContext(ctx, "tcp", c.endpoint)
		} else {
			// Clone per dial and let tls.Dialer apply the dial timeout to the handshake.
			tlsDialer := tls.Dialer{NetDialer: &dialer, Config: c.config.TLSConfig.Clone()}
			connection, err = tlsDialer.DialContext(ctx, "tcp", c.endpoint)
		}
		if err == nil && c.config.Token != "" {
			// Authenticate before publishing the socket. AUTH uses its own deadline,
			// independent of the acquisition context used for dialing.
			err = authenticateConnection(connection, c.config.Token, c.config.RequestTimeout)
		}
		c.mu.Lock()
		c.dialing--
		if c.closed || err != nil {
			// Abandon unstarted dials too, and never publish a socket after Close.
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
		// The socket is ready for any acquirer, not reserved for this fill's caller.
		now := time.Now()
		pooled := &pooledConnection{Conn: connection, createdAt: now, idleSince: now}
		c.connections[pooled] = struct{}{}
		c.idle = append(c.idle, pooled)
		c.notify()
		c.mu.Unlock()
	}
	return nil
}

// release returns a leased socket only if its response was fully validated and
// its transport is still usable. Every return or retirement wakes capacity waiters.
func (c *NebulaCache) release(connection *pooledConnection, healthy bool) {
	// A prior request's deadline must not leak into a later lease or idle period.
	if healthy && connection.SetDeadline(time.Time{}) != nil {
		healthy = false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !healthy || time.Since(connection.createdAt) >= c.config.MaxLifetime {
		// Lifetime can expire during a request; retire it here, not mid-exchange.
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
