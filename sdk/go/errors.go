package nebulacache

import (
	"errors"
	"fmt"
)

// ErrClosed indicates that an operation was interrupted or rejected by Close.
var ErrClosed = errors.New("NebulaCache client is closed")

// CacheConnectionError wraps a transport or pool acquisition failure.
type CacheConnectionError struct {
	// Address is the configured server endpoint.
	Address  string
	// Err is the underlying cause, exposed by Unwrap.
	Err      error
	// TimedOut reports whether the cause implements net.Error with Timeout true.
	TimedOut bool
}

// Error describes the endpoint and underlying connection failure.
func (err *CacheConnectionError) Error() string {
	if err.TimedOut {
		return fmt.Sprintf("request to NebulaCache at %s timed out: %v", err.Address, err.Err)
	}
	return fmt.Sprintf("could not reach NebulaCache at %s: %v", err.Address, err.Err)
}

// Unwrap exposes the cause for errors.Is and errors.As.
func (err *CacheConnectionError) Unwrap() error {
	return err.Err
}

// CacheProtocolError indicates invalid response framing or an unexpected PING payload.
type CacheProtocolError struct {
	// Message describes the protocol violation.
	Message string
	// Err is an optional underlying cause.
	Err     error
}

// Error describes the protocol violation and any underlying cause.
func (err *CacheProtocolError) Error() string {
	if err.Err == nil {
		return err.Message
	}
	return fmt.Sprintf("%s: %v", err.Message, err.Err)
}

// Unwrap returns the optional underlying cause.
func (err *CacheProtocolError) Unwrap() error {
	return err.Err
}

// CacheResponseError represents a valid response with an unsuccessful server status.
// Get and Delete handle NOT_FOUND as a normal miss instead.
type CacheResponseError struct {
	// Status is the numeric protocol status.
	Status  byte
	// Message contains the status name and decoded server error text.
	Message string
}

// Error describes the server status and message.
func (err *CacheResponseError) Error() string {
	return fmt.Sprintf("NebulaCache returned status %d: %s", err.Status, err.Message)
}
