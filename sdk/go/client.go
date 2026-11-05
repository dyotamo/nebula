package nebulacache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
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

type NebulaCache struct {
	address  string
	endpoint string
	timeout  time.Duration
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

	host, port, err := parseAddress(address)
	if err != nil {
		return nil, err
	}

	return &NebulaCache{
		address:  address,
		endpoint: net.JoinHostPort(host, strconv.Itoa(port)),
		timeout:  timeout,
	}, nil
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
	connection, err := net.DialTimeout("tcp", c.endpoint, c.timeout)
	if err != nil {
		return response{}, connectionError(c.address, err)
	}

	defer connection.Close()

	if err := connection.SetDeadline(time.Now().Add(c.timeout)); err != nil {
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
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return &CacheProtocolError{Message: "connection closed before the response was complete", Err: err}
	}

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
