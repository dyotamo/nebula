package nebulacache

import (
	"errors"
	"fmt"
)

// ErrClosed indicates that an operation was interrupted or rejected by Close.
// Use errors.Is to distinguish permanent client shutdown from a transport failure.
// Public methods still validate their arguments before attempting a request.
var ErrClosed = errors.New("NebulaCache client is closed")

// CacheConnectionError wraps a transport or pool acquisition failure.
// It can also wrap an AUTH failure during connection setup. Use errors.As to
// inspect this category and errors.Is or errors.As on its cause for finer detail.
// A failed write or response read does not prove that a mutation was not applied.
type CacheConnectionError struct {
	// Address is the configured server endpoint, retained as supplied by the caller
	// rather than replaced with a resolved network address.
	Address string
	// Err is the underlying cause, exposed by Unwrap without losing its identity.
	Err error
	// TimedOut reports whether the cause implements net.Error with Timeout true.
	// It covers acquisition and I/O deadlines, not just TCP connection timeouts;
	// CacheConnectionError itself does not implement net.Error.
	TimedOut bool
}

// Error describes the endpoint and underlying connection failure.
// Timed-out causes use distinct wording, while Err remains available to callers
// that need to classify the failure without parsing this diagnostic text.
func (err *CacheConnectionError) Error() string {
	if err.TimedOut {
		return fmt.Sprintf("request to NebulaCache at %s timed out: %v", err.Address, err.Err)
	}
	return fmt.Sprintf("could not reach NebulaCache at %s: %v", err.Address, err.Err)
}

// Unwrap exposes the cause for errors.Is and errors.As, including wrapped network
// errors and acquisition context deadlines.
func (err *CacheConnectionError) Unwrap() error {
	return err.Err
}

// CacheProtocolError indicates invalid response framing or an unexpected PING payload.
// These failures discard the connection and are not automatically retried: an
// incompatible or malformed peer response is not treated as a transient outage.
// Truncated frames are transport errors instead, preserving GET retry eligibility.
type CacheProtocolError struct {
	// Message describes the violated framing or operation-specific invariant.
	Message string
	// Err is an optional underlying cause; it may be nil when validation alone
	// identifies the violation.
	Err error
}

// Error describes the protocol violation and any underlying cause.
// A validation-only failure returns Message without appending a nil cause.
func (err *CacheProtocolError) Error() string {
	if err.Err == nil {
		return err.Message
	}
	return fmt.Sprintf("%s: %v", err.Message, err.Err)
}

// Unwrap returns the optional underlying cause, or nil for a standalone validation
// failure, allowing standard error-chain inspection in either case.
func (err *CacheProtocolError) Unwrap() error {
	return err.Err
}

// CacheResponseError represents a valid response with an unsuccessful server status.
// Get and Delete handle NOT_FOUND as a normal miss instead. Server error statuses
// are not retried and do not by themselves prevent connection reuse, because the
// response frame has already been fully consumed and validated.
type CacheResponseError struct {
	// Status is the numeric protocol status: 1 NOT_FOUND, 2 INVALID_ARGUMENT,
	// 3 INTERNAL_ERROR, 4 UNAUTHORIZED, or 5 FORBIDDEN.
	Status byte
	// Message contains the status name and server error text, with invalid UTF-8
	// replaced and trailing Unicode whitespace removed.
	Message string
}

// Error describes the numeric server status and its named diagnostic message.
// Callers should inspect Status rather than depend on the formatting of this text.
func (err *CacheResponseError) Error() string {
	return fmt.Sprintf("NebulaCache returned status %d: %s", err.Status, err.Message)
}
