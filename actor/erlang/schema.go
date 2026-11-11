// Package erlangactor bridges Tao local Actors with Erlang/OTP distribution v6
// and gen_server processes without coupling ETF or BEAM protocols into actor core.
package erlangactor

import (
	"errors"
	"time"

	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
)

var (
	ErrBridgeClosed      = errors.New("erlangactor: bridge closed")
	ErrBindingClosed     = errors.New("erlangactor: binding closed")
	ErrOverloaded        = errors.New("erlangactor: target actor overloaded")
	ErrInboundTimeout    = errors.New("erlangactor: inbound call timeout exceeded")
	ErrUnsupportedSignal = errors.New("erlangactor: unsupported inbound signal")
)

// Schema performs deterministic, non-blocking value translation between Erlang ETF
// terms and Go business messages (§4.2). Implementations must not perform I/O or
// touch mutable Actor state, as methods may be invoked concurrently across peers.
type Schema interface {
	DecodeCall(term etf.Term) (any, error)
	DecodeCast(term etf.Term) (any, error)
	DecodeInfo(term etf.Term) (any, error)
	EncodeReply(value any, err error) (etf.Term, error)
}

// Funcs adapts plain functions into a concrete Schema without requiring a custom type (§4.2).
type Funcs struct {
	Call  func(term etf.Term) (any, error)
	Cast  func(term etf.Term) (any, error)
	Info  func(term etf.Term) (any, error)
	Reply func(value any, err error) (etf.Term, error)
}

func (f Funcs) DecodeCall(term etf.Term) (any, error) {
	if f.Call == nil {
		return nil, ErrUnsupportedSignal
	}
	return f.Call(term)
}

func (f Funcs) DecodeCast(term etf.Term) (any, error) {
	if f.Cast == nil {
		return nil, ErrUnsupportedSignal
	}
	return f.Cast(term)
}

func (f Funcs) DecodeInfo(term etf.Term) (any, error) {
	if f.Info == nil {
		return nil, ErrUnsupportedSignal
	}
	return f.Info(term)
}

func (f Funcs) EncodeReply(value any, err error) (etf.Term, error) {
	if f.Reply != nil {
		return f.Reply(value, err)
	}
	return DefaultEncodeReply(value, err)
}

// DefaultEncodeReply maps business errors to {error, {tao, Code, DetailBin}} and passes
// valid etf.Term values directly.
func DefaultEncodeReply(value any, err error) (etf.Term, error) {
	if err != nil {
		if errors.Is(err, ErrOverloaded) {
			return etf.Tuple{
				etf.Atom("error"),
				etf.Tuple{etf.Atom("tao"), etf.Atom("overloaded")},
			}, nil
		}
		return etf.Tuple{
			etf.Atom("error"),
			erlang.TaoReason("call_error", err.Error()),
		}, nil
	}
	if value == nil {
		return etf.Atom("ok"), nil
	}
	return value, nil
}

// PassthroughSchema returns a Schema that passes raw etf.Term values straight into
// the target Tao Actor's Receive and encodes etf.Term replies via DefaultEncodeReply.
func PassthroughSchema() Schema {
	id := func(term etf.Term) (any, error) { return term, nil }
	return Funcs{
		Call:  id,
		Cast:  id,
		Info:  id,
		Reply: DefaultEncodeReply,
	}
}

// Call is sent to Bridge.Outbound() via Actor.Request or actor.Call to perform
// a non-blocking Erlang '$gen_call' (§4.2).
type Call struct {
	Target  erlang.Target
	Request etf.Term
}

// Cast is sent to Bridge.Outbound() via Actor.Send, Actor.Request, or Node.Call
// to enqueue an Erlang '$gen_cast' (§4.2).
type Cast struct {
	Target  erlang.Target
	Request etf.Term
}

// Send is sent to Bridge.Outbound() via Actor.Send, Actor.Request, or Node.Call
// to enqueue a raw Erlang '!' message (§4.2).
type Send struct {
	Target  erlang.Target
	Message etf.Term
}

// Exit is the bridge DTO delivered to a local Tao Actor's Receive when an exported
// Binding enables WithTrapExit(true) and receives a trappable Erlang EXIT/EXIT2 signal (§10.2).
// It never impersonates actor.Exit or alters Tao's native supervision/Link graph.
type Exit struct {
	From     erlang.PID
	Reason   etf.Term
	FromLink bool
}

// Inbound optionally wraps a decoded business message with its Erlang sender metadata
// when WithSourceEnvelope(true) is configured on Export (§4.2).
type Inbound struct {
	Peer    string
	From    erlang.PID
	HasFrom bool
	Message any
}

type bridgeConfig struct {
	completionQueueSize int
	completionWorkers   int
	outboundPeerQueue   int
}

// Option configures Bridge-level parameters passed to Attach.
type Option func(*bridgeConfig)

// WithCompletionQueueSize sets the bounded completion queue capacity (minimum 64).
func WithCompletionQueueSize(size int) Option {
	return func(c *bridgeConfig) {
		if size >= 64 {
			c.completionQueueSize = size
		}
	}
}

// WithCompletionWorkers sets the number of completion encoding workers (default 4).
func WithCompletionWorkers(workers int) Option {
	return func(c *bridgeConfig) {
		if workers >= 1 && workers <= 64 {
			c.completionWorkers = workers
		}
	}
}

// WithOutboundPeerQueue sets the per-remote-peer submission queue depth on Bridge.Outbound().
func WithOutboundPeerQueue(size int) Option {
	return func(c *bridgeConfig) {
		if size >= 16 {
			c.outboundPeerQueue = size
		}
	}
}

type exportConfig struct {
	trapExit       bool
	inboundTimeout time.Duration
	allowedPeers   []string
	stopActor      bool
	sourceEnvelope bool
}

// ExportOption configures an individual Binding exported via Bridge.Export.
type ExportOption func(*exportConfig)

// WithTrapExit enables delivering trappable Erlang EXIT/EXIT2 signals to the target
// Tao Actor as erlangactor.Exit DTOs (§10.2). Explicit exit(Pid, kill) remains untrappable.
func WithTrapExit(enabled bool) ExportOption {
	return func(c *exportConfig) {
		c.trapExit = enabled
	}
}

// WithInboundCallTimeout enforces an explicit server-side execution deadline on inbound
// '$gen_call' requests (§8.3). Zero (the default) imposes no arbitrary timeout.
func WithInboundCallTimeout(d time.Duration) ExportOption {
	return func(c *exportConfig) {
		c.inboundTimeout = d
	}
}

// WithAllowedPeers restricts access to this Binding to the specified full Erlang node names.
func WithAllowedPeers(peers ...string) ExportOption {
	return func(c *exportConfig) {
		c.allowedPeers = append([]string(nil), peers...)
	}
}

// WithStopActor requests cooperative Tao Node.StopActor on the local target Actor when the
// Binding terminates due to an untrappable remote exit, cast overload, or explicit Close (§10.3).
func WithStopActor(enabled bool) ExportOption {
	return func(c *exportConfig) {
		c.stopActor = enabled
	}
}

// WithSourceEnvelope wraps decoded Call/Cast/Info messages in erlangactor.Inbound so the
// Actor can inspect the originating Erlang node and PID without forging Actor.Sender() (§4.2).
func WithSourceEnvelope(enabled bool) ExportOption {
	return func(c *exportConfig) {
		c.sourceEnvelope = enabled
	}
}

// Stats summarizes Bridge adapter state alongside the underlying erlang.Stats snapshot (§12.3).
type Stats struct {
	Bindings             int
	GuardianActors       int
	WatchedActors        int
	InboundInFlight      int64
	OutboundSubmitted    uint64
	OutboundErrors       uint64
	DecodeErrors         uint64
	OverloadReplies      uint64
	OverloadTerminations uint64
	Erlang               erlang.Stats
}
