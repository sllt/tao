package erlang

import (
	"crypto/tls"
	"time"
)

// Default operational parameters and resource quotas (§6.4, §8.2, §11.3).
const (
	DefaultTickTime            = 60 * time.Second
	DefaultCallTimeout         = 5 * time.Second
	DefaultMaxPeers            = 64
	DefaultMaxPayloadBytes     = 4 << 20  // 4 MiB
	DefaultMaxPeerSendBytes    = 16 << 20 // 16 MiB
	DefaultMaxPeerRecvBytes    = 16 << 20 // 16 MiB
	DefaultMaxPeerDecodedBytes = 32 << 20 // 32 MiB
	DefaultMaxNodePendingCalls = 4096
	DefaultMaxPeerPendingCalls = 1024
	DefaultMaxProcesses        = 65536
	DefaultMaxRelations        = 65536
	DefaultMaxProcessRelations = 256
	DefaultMaxControlQueue     = 2048
	DefaultMaxDiagnosticEvents = 256
)

// PeerCookie configures optional directional cookies for a specific remote node.
type PeerCookie struct {
	InCookie  string
	OutCookie string
}

// Route defines an explicit outbound static distribution endpoint bypassing EPMD lookup.
type Route struct {
	Addr string
	TLS  *tls.Config // nil inherits Config.TLS
}

// Limits configures bounded transport and process quotas for an Erlang distribution Node.
// Zero fields use the §11.3 defaults.
type Limits struct {
	MaxPeers            int
	MaxPayloadBytes     int
	MaxPeerSendBytes    int64
	MaxPeerRecvBytes    int64
	MaxPeerDecodedBytes int64
	MaxNodePendingCalls int
	MaxPeerPendingCalls int
	MaxProcesses        int
	MaxRelations        int
	MaxProcessRelations int
	MaxControlQueue     int
	MaxDiagnosticEvents int
}

func (l Limits) normalize() Limits {
	if l.MaxPeers <= 0 {
		l.MaxPeers = DefaultMaxPeers
	}
	if l.MaxPayloadBytes <= 0 {
		l.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if l.MaxPeerSendBytes <= 0 {
		l.MaxPeerSendBytes = DefaultMaxPeerSendBytes
	}
	if l.MaxPeerRecvBytes <= 0 {
		l.MaxPeerRecvBytes = DefaultMaxPeerRecvBytes
	}
	if l.MaxPeerDecodedBytes <= 0 {
		l.MaxPeerDecodedBytes = DefaultMaxPeerDecodedBytes
	}
	if l.MaxNodePendingCalls <= 0 {
		l.MaxNodePendingCalls = DefaultMaxNodePendingCalls
	}
	if l.MaxPeerPendingCalls <= 0 {
		l.MaxPeerPendingCalls = DefaultMaxPeerPendingCalls
	}
	if l.MaxProcesses <= 0 {
		l.MaxProcesses = DefaultMaxProcesses
	}
	if l.MaxRelations <= 0 {
		l.MaxRelations = DefaultMaxRelations
	}
	if l.MaxProcessRelations <= 0 {
		l.MaxProcessRelations = DefaultMaxProcessRelations
	}
	if l.MaxControlQueue <= 0 {
		l.MaxControlQueue = DefaultMaxControlQueue
	}
	if l.MaxDiagnosticEvents <= 0 {
		l.MaxDiagnosticEvents = DefaultMaxDiagnosticEvents
	}
	return l
}

// Config configures a standalone Erlang distribution Node.
type Config struct {
	// Name is the full ASCII Erlang node name ("alive@host"). Required.
	Name string
	// Listen is the TCP/TLS listener address (e.g. "127.0.0.1:0" or ":9100"). Required.
	Listen string
	// Cookie is the node authentication secret. Required; never implicitly read from $HOME.
	Cookie string
	// PeerCookies optionally overrides InCookie/OutCookie for specific remote node names.
	PeerCookies map[string]PeerCookie
	// TLS selects the inet_tls distribution profile (packet-4 handshake) when non-nil.
	// Nil selects the plain TCP distribution profile (packet-2 handshake).
	TLS *tls.Config
	// PeerTLS optionally overrides the outbound client tls.Config per remote node name.
	PeerTLS map[string]*tls.Config
	// AllowedPeers restricts inbound and outbound connections to the listed full node names
	// when non-empty.
	AllowedPeers []string
	// Published advertises DFLAG_PUBLISHED when true. False (the default) runs as a hidden node.
	Published bool
	// EPMDAddr is the host:port of the external EPMD daemon (defaults to "127.0.0.1:4369").
	EPMDAddr string
	// DisableEPMD disables EPMD ALIVE2 registration and generates a local Creation in [4, 2^32-1].
	DisableEPMD bool
	// StaticRoutes maps full remote node names to explicit host:port endpoints for outbound dials.
	StaticRoutes map[string]Route
	// TickTime is the net_ticktime peer liveness budget (defaults to 60s, intensity 4).
	TickTime time.Duration
	// DefaultCallTimeout applies to Client.Call when the caller context has no deadline (defaults to 5s).
	DefaultCallTimeout time.Duration
	// FragmentSize sets the outbound payload fragmentation chunk size when DFLAG_FRAGMENTS is negotiated
	// (defaults to 64 KiB).
	FragmentSize int
	// Limits overrides transport/process resource quotas.
	Limits Limits
}

// PeerStats summarizes an active peer session in Node.Stats().
type PeerStats struct {
	Name            string
	Generation      uint64
	Creation        uint32
	NegotiatedFlags uint64
	SendQueuedBytes int64
	ReassemblyBytes int64
	OutboundCalls   int
	InboundCalls    int
}

// Stats reports a point-in-time snapshot of node counters and active peer sessions (§12.3).
type Stats struct {
	NodeName          string
	Creation          uint32
	ActivePeers       int
	HandshakingPeers  int
	Processes         int
	ExportedNames     int
	ActiveAliases     int
	Relations         int
	OutboundPending   int
	InboundPending    int
	SendQueuedBytes   int64
	ReassemblyBytes   int64
	EncodedBytesTotal uint64
	DecodedBytesTotal uint64
	HandshakeFailures uint64
	ProtocolErrors    uint64
	LateReplies       uint64
	OverloadDrops     uint64
	UnknownOutcomes   uint64
	DroppedEvents     uint64
	EPMDLost          bool
	Peers             []PeerStats
}

// DiagnosticKind classifies low-frequency diagnostic events (§12.3).
// Cookie values, digests, and raw business terms are never recorded here.
type DiagnosticKind string

const (
	EventPeerUp          DiagnosticKind = "peer_up"
	EventPeerDown        DiagnosticKind = "peer_down"
	EventHandshakeFailed DiagnosticKind = "handshake_failed"
	EventProtocolError   DiagnosticKind = "protocol_error"
	EventEPMDLost        DiagnosticKind = "epmd_lost"
	EventOverload        DiagnosticKind = "overload"
)

// DiagnosticEvent is a bounded, non-blocking operational notification.
type DiagnosticEvent struct {
	Time       time.Time
	Kind       DiagnosticKind
	Peer       string
	Generation uint64
	Detail     string
}
