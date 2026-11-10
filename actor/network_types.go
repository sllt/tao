package actor

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sllt/tao/taonet"
)

type NetworkProfile string

const (
	IsolatedNetwork NetworkProfile = "isolated"
	CompactNetwork  NetworkProfile = "compact"
)

type NetworkOptions struct {
	ControlMessages       int
	TLS                   *tls.Config
	AllowInsecureLoopback bool
	Profile               NetworkProfile
	Namespace             string
	Peers                 map[string]string
	MaxMessageBytes       int
	SendBytes             int
	SendMessages          int
	SendTimeout           time.Duration
	MaxSessions           int
	ManagedBytes          int
}

func (o NetworkOptions) normalized() (NetworkOptions, error) {
	if o.ControlMessages == 0 {
		o.ControlMessages = 256
	}
	if o.ControlMessages < 1 {
		return NetworkOptions{}, fmt.Errorf("actor: invalid control capacity")
	}
	if o.Profile == "" {
		o.Profile = IsolatedNetwork
	}
	if o.Namespace == "" {
		o.Namespace = "hive.actor"
	}
	if o.MaxMessageBytes == 0 {
		o.MaxMessageBytes = 128 << 10
	}
	if o.SendBytes == 0 {
		o.SendBytes = 8 << 20
	}
	if o.SendMessages == 0 {
		o.SendMessages = 1024
	}
	if o.SendTimeout == 0 {
		o.SendTimeout = 5 * time.Second
	}
	if (o.Profile != IsolatedNetwork && o.Profile != CompactNetwork) || o.MaxMessageBytes < 1 || o.MaxMessageBytes > 3<<20 || o.SendBytes < 2*(o.MaxMessageBytes+4096) || o.SendMessages < 1 || o.SendTimeout < 0 {
		return NetworkOptions{}, fmt.Errorf("actor: invalid network options")
	}
	for peer := range o.Peers {
		if !validSegment(peer) {
			return NetworkOptions{}, fmt.Errorf("actor: invalid peer name")
		}
	}
	return o, nil
}

type DeliveryOutcome uint8

const (
	NotSent DeliveryOutcome = iota
	Rejected
	Unknown
	Responded
)

var ErrNodeUnreachable = errors.New("actor: node unreachable")
var ErrRemoteApplication = errors.New("actor: remote application error")
var ErrLinkConflict = errors.New("actor: link registration already in progress")

type RemoteError struct {
	Code    string
	Message string
	Outcome DeliveryOutcome
	Cause   error
}

func (e *RemoteError) Error() string        { return "actor remote: " + e.Code + ": " + e.Message }
func (e *RemoteError) Unwrap() error        { return e.Cause }
func (e *RemoteError) Is(target error) bool { return target == errorForCode(e.Code) }

var networkErrors = []struct {
	code string
	err  error
}{
	{"redirect", ErrRedirect}, {"directory_unavailable", ErrDirectoryUnavailable},
	{"migrating", ErrMigrating}, {"replaced", ErrReplaced},
	{"handed_off", ErrHandedOff},
	{"handoff_unsupported", ErrHandoffUnsupported}, {"directory_conflict", ErrDirectoryConflict}, {"move_committed", ErrMoveCommitted},
	{"node_closed", ErrClosed}, {"remote_unavailable", ErrRemoteUnavailable},
	{"link_conflict", ErrLinkConflict},
	{"mailbox_full", ErrMailboxFull}, {"capacity", ErrCapacity}, {"no_process", ErrNoProcess}, {"stale_pid", ErrStalePID}, {"starting", ErrStarting}, {"restarting", ErrRestarting}, {"stopped", ErrStopped}, {"unresponsive", ErrUnresponsive},
	{"no_reply", ErrNoReply}, {"process_crashed", ErrProcessCrashed}, {"unregistered_type", ErrUnregisteredType}, {"invalid_message", ErrInvalidMessage}, {"message_too_large", ErrMessageTooLarge}, {"canceled", context.Canceled}, {"deadline_exceeded", context.DeadlineExceeded}, {"unreachable", ErrNodeUnreachable},
	{"codec_mismatch", ErrCodecMismatch},
}

func codeForError(err error) string {
	for _, entry := range networkErrors {
		if errors.Is(err, entry.err) {
			return entry.code
		}
	}
	return "application_error"
}
func errorForCode(code string) error {
	for _, entry := range networkErrors {
		if entry.code == code {
			return entry.err
		}
	}
	return ErrRemoteApplication
}
func rejectCode(err error) taonet.Code { return taonet.Code("app.actor." + codeForError(err)) }
func deliveryError(err error, fallback DeliveryOutcome) error {
	var existing *RemoteError
	if errors.As(err, &existing) {
		return existing
	}
	code := codeForError(err)
	var wire *taonet.OpError
	if errors.As(err, &wire) {
		fallback = DeliveryOutcome(wire.Outcome)
		if strings.HasPrefix(string(wire.Code), "app.actor.") {
			code = strings.TrimPrefix(string(wire.Code), "app.actor.")
		} else if wire.Code == taonet.CodeResourceExhausted {
			code = "capacity"
		} else if code == "application_error" {
			code = "unreachable"
		}
	}
	if code == "application_error" {
		code = "unreachable"
	}
	return &RemoteError{Code: code, Message: err.Error(), Outcome: fallback, Cause: err}
}

type NetworkEvent struct {
	Peer      string
	Kind      string
	RequestID uint64
	Err       error
}
type NetworkStats struct {
	ControlReservations int
	ControlJobs         int
	QueuedBytes         int
	QueuedMessages      int
	OutgoingCalls       int
	IncomingCalls       int
	CancellationRecords int
	DroppedEvents       uint64
	Endpoint            taonet.EndpointStats
}

func remoteNode(addr Addr, local string) string {
	switch addr := addr.(type) {
	case routedRef:
		if addr.record.Owner != local {
			return addr.record.Owner
		}
	case PID:
		if addr.Node != local {
			return addr.Node
		}
	case Ref:
		if addr.Node != "" && addr.Node != local {
			return addr.Node
		}
	}
	return ""
}

type requestKey struct {
	Caller string   `json:"caller"`
	Boot   [16]byte `json:"boot"`
	ID     uint64   `json:"id"`
}
type wireScope struct {
	Session taonet.SessionID `json:"session"`
	Channel uint64           `json:"channel"`
}
type wireAddress struct {
	Route *RouteRecord `json:"route,omitempty"`
	Kind  string       `json:"kind"`
	PID   *PID         `json:"pid,omitempty"`
	Ref   *Ref         `json:"ref,omitempty"`
}

func encodeAddress(addr Addr) (wireAddress, error) {
	switch value := addr.(type) {
	case routedRef:
		return wireAddress{Kind: "global", Route: &value.record}, nil
	case PID:
		return wireAddress{Kind: "pid", PID: &value}, nil
	case Ref:
		return wireAddress{Kind: "ref", Ref: &value}, nil
	default:
		return wireAddress{}, ErrNoProcess
	}
}
func (addr wireAddress) localPID(local string) *PID {
	if addr.Kind == "pid" && addr.PID != nil && addr.Ref == nil && addr.Route == nil && addr.PID.Node == local {
		return addr.PID
	}
	return nil
}

func (addr wireAddress) decode(local string) (Addr, error) {
	switch {
	case addr.Kind == "global" && addr.Route != nil && addr.PID == nil && addr.Ref == nil && addr.Route.Owner == local && addr.Route.Epoch != 0:
		if err := addr.Route.Validate(); err != nil {
			return nil, ErrInvalidMessage
		}
		return routedRef{record: *addr.Route}, nil
	case addr.localPID(local) != nil:
		return *addr.PID, nil
	case addr.Kind == "ref" && addr.Ref != nil && addr.PID == nil && addr.Route == nil && addr.Ref.Node == local && validPath(addr.Ref.Path):
		return *addr.Ref, nil
	default:
		return nil, ErrInvalidMessage
	}
}

type wireActorError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type actorPacket struct {
	Move     MoveID          `json:"move,omitzero"`
	Sequence uint64          `json:"sequence,omitempty"`
	Monitor  *MonitorRef     `json:"monitor,omitempty"`
	From     *PID            `json:"from,omitempty"`
	Link     LinkID          `json:"link,omitzero"`
	Kind     string          `json:"kind"`
	Target   wireAddress     `json:"target,omitzero"`
	Sender   *PID            `json:"sender,omitempty"`
	Key      requestKey      `json:"key"`
	BudgetMS uint64          `json:"budget_ms,omitempty"`
	Message  EncodedMessage  `json:"message"`
	Error    *wireActorError `json:"error,omitempty"`
	Scope    wireScope       `json:"scope,omitzero"`
}

const (
	inboxRoute   = "hive.actor/inbox.v3"
	replyRoute   = "hive.actor/reply.v4"
	controlRoute = "hive.actor/control.v2"
)
