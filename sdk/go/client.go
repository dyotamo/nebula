package nebulacache

import (
	"bytes"
	"context"
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
	DefaultAddress  = "127.0.0.1:7001"
	DefaultTimeout  = 5 * time.Second
	maxKeyLength    = 1024
	maxValueLength  = 8 * 1024 * 1024
	requestHeader   = 22
	responseHeader  = 10
	protocolVersion = 1
)

var protocolMagic = [4]byte{'N', 'E', 'B', 'U'}

type Config struct {
	Address        string
	MinPoolSize    int
	MaxPoolSize    int
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
	AcquireTimeout time.Duration
	IdleTimeout    time.Duration
	MaxLifetime    time.Duration
	MaxRetries     int
}

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

func (c *NebulaCache) fill(ctx context.Context, count int) error {
	for remaining := count; remaining > 0; remaining-- {
		dialer := net.Dialer{Timeout: c.config.ConnectTimeout}
		connection, err := dialer.DialContext(ctx, "tcp", c.endpoint)
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

func (c *NebulaCache) Set(key string, value []byte) error {
	return c.SetWithTTL(key, value, 0)
}

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

func (c *NebulaCache) SetString(key, value string) error {
	return c.SetStringWithTTL(key, value, 0)
}

func (c *NebulaCache) SetStringWithTTL(key, value string, ttlSecs uint64) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("value must be valid UTF-8")
	}

	return c.SetWithTTL(key, []byte(value), ttlSecs)
}

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
	if status > 3 {
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
