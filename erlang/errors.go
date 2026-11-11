package erlang

import (
	"errors"
	"fmt"

	"github.com/sllt/tao/erlang/etf"
)

// Outcome describes what is known about message delivery when an operation fails.
type Outcome uint8

const (
	// OutcomeUnknown means the request may have reached the wire or remote mailbox,
	// or failed via remote monitor/exit where non-execution cannot be proven.
	OutcomeUnknown Outcome = iota
	// OutcomeNotSent means the operation failed locally before any bytes were committed
	// to an authenticated connection's writer batch.
	OutcomeNotSent
	// OutcomeUnsupported means the requested operation or term is not supported by v1.
	OutcomeUnsupported
)

func (o Outcome) String() string {
	switch o {
	case OutcomeNotSent:
		return "not_sent"
	case OutcomeUnsupported:
		return "unsupported"
	default:
		return "unknown"
	}
}

// Stage identifies the protocol phase where an Error originated.
type Stage string

const (
	StageValidate  Stage = "validate"
	StageEncode    Stage = "encode"
	StageQuota     Stage = "quota"
	StageDiscovery Stage = "discovery"
	StageConnect   Stage = "connect"
	StageHandshake Stage = "handshake"
	StageSend      Stage = "send"
	StageWait      Stage = "wait"
	StageProcess   Stage = "process"
)

var (
	ErrNodeClosed        = errors.New("erlang: node closed")
	ErrClientClosed      = errors.New("erlang: client closed")
	ErrEndpointClosed    = errors.New("erlang: endpoint closed")
	ErrCapacity          = errors.New("erlang: resource quota exceeded")
	ErrReservedName      = errors.New("erlang: reserved registered name")
	ErrDuplicateExport   = errors.New("erlang: registered name already exported")
	ErrIdentityExhausted = errors.New("erlang: incarnation identity counter exhausted")
	ErrPeerNotAllowed    = errors.New("erlang: peer node not allowed")
)

// Error carries phase, delivery outcome, peer identity, and optional raw Erlang exit/down reason.
type Error struct {
	Stage   Stage
	Outcome Outcome
	Peer    string
	Reason  etf.Term
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	peerPart := ""
	if e.Peer != "" {
		peerPart = " peer=" + e.Peer
	}
	if e.Reason != nil && e.Err != nil {
		return fmt.Sprintf("erlang [%s/%s%s]: reason=%v: %v", e.Stage, e.Outcome, peerPart, e.Reason, e.Err)
	}
	if e.Reason != nil {
		return fmt.Sprintf("erlang [%s/%s%s]: reason=%v", e.Stage, e.Outcome, peerPart, e.Reason)
	}
	if e.Err != nil {
		return fmt.Sprintf("erlang [%s/%s%s]: %v", e.Stage, e.Outcome, peerPart, e.Err)
	}
	return fmt.Sprintf("erlang [%s/%s%s]", e.Stage, e.Outcome, peerPart)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// DeliveryOutcome extracts Outcome from err (defaulting to OutcomeUnknown for non-nil errors).
func DeliveryOutcome(err error) Outcome {
	if err == nil {
		return OutcomeUnknown
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Outcome
	}
	return OutcomeUnknown
}

// TaoReason builds the structured Erlang reason tuple {tao, CodeAtom, DetailBinary} (§11.2).
// Detail text is always encoded as a UTF-8 binary rather than dynamically creating atoms.
func TaoReason(code string, detail string) etf.Tuple {
	return etf.Tuple{
		etf.Atom("tao"),
		etf.Atom(code),
		etf.UTF8Binary(detail),
	}
}
