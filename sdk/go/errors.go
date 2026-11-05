package nebulacache

import "fmt"

type CacheConnectionError struct {
	Address  string
	Err      error
	TimedOut bool
}

func (err *CacheConnectionError) Error() string {
	if err.TimedOut {
		return fmt.Sprintf("request to NebulaCache at %s timed out: %v", err.Address, err.Err)
	}
	return fmt.Sprintf("could not reach NebulaCache at %s: %v", err.Address, err.Err)
}

func (err *CacheConnectionError) Unwrap() error {
	return err.Err
}

type CacheProtocolError struct {
	Message string
	Err     error
}

func (err *CacheProtocolError) Error() string {
	if err.Err == nil {
		return err.Message
	}
	return fmt.Sprintf("%s: %v", err.Message, err.Err)
}

func (err *CacheProtocolError) Unwrap() error {
	return err.Err
}

type CacheResponseError struct {
	Status  byte
	Message string
}

func (err *CacheResponseError) Error() string {
	return fmt.Sprintf("NebulaCache returned status %d: %s", err.Status, err.Message)
}
