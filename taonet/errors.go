package taonet

import (
	"errors"
	"fmt"
)

type Code string

const (
	CodeCanceled          Code = "canceled"
	CodeDeadlineExceeded  Code = "deadline_exceeded"
	CodeUnavailable       Code = "unavailable"
	CodeUnauthenticated   Code = "unauthenticated"
	CodePermissionDenied  Code = "permission_denied"
	CodeNotFound          Code = "not_found"
	CodeResourceExhausted Code = "resource_exhausted"
	CodeMessageTooLarge   Code = "message_too_large"
	CodeProtocolError     Code = "protocol_error"
	CodeInternal          Code = "internal"
	CodeApplication       Code = "application_error"
	CodeAbandoned         Code = "abandoned"
)

var (
	ErrClosed          = errors.New("taonet: session closed")
	ErrDraining        = errors.New("taonet: session draining")
	ErrProtocol        = errors.New("taonet: protocol error")
	ErrAlreadyResolved = errors.New("taonet: delivery already resolved")
	ErrOutOfOrder      = errors.New("taonet: resolve deliveries in receive order")
	ErrUnsupported     = errors.New("taonet: feature not negotiated")
)

type Outcome uint8

const (
	NotSent Outcome = iota
	Rejected
	Unknown
	Responded
)

type OpError struct {
	Code      Code
	Outcome   Outcome
	Remote    bool
	Operation uint64
	Message   string
	Cause     error
}

func (e *OpError) Error() string { return fmt.Sprintf("taonet: %s: %s", e.Code, e.Message) }
func (e *OpError) Unwrap() error { return e.Cause }

// StatusError lets an application return a stable code to the remote caller.
type StatusError struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

func (e *StatusError) Error() string { return string(e.Code) + ": " + e.Message }

func protocolError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}
