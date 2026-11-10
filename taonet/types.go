package taonet

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type PeerID string
type ID [16]byte
type InstanceID = ID
type SessionID = ID

func (id ID) String() string               { return hex.EncodeToString(id[:]) }
func (id ID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }
func (id *ID) UnmarshalText(text []byte) error {
	if len(text) != 32 {
		return fmt.Errorf("taonet: ID must contain 32 hex characters")
	}
	_, err := hex.Decode(id[:], text)
	return err
}
func newID() (ID, error) { var id ID; _, err := rand.Read(id[:]); return id, err }

var processInstance = sync.OnceValues(newID)

type Peer struct {
	ID        PeerID     `json:"id"`
	Instance  InstanceID `json:"instance"`
	Namespace string     `json:"namespace"`
}

type SessionInfo struct {
	ID        SessionID
	Local     Peer
	Remote    Peer
	Transport string
	Features  []string
	Class     TrafficClass
	Group     SessionID
}

type Message struct {
	Metadata map[string]string
	Body     []byte
}

type Request struct {
	Route   string
	Message Message
	session *Session
}

func (r *Request) Session() *Session { return r.session }

type RequestHandler func(context.Context, *Request) (Message, error)
type NotifyHandler func(context.Context, Message) error
type StreamHandler func(context.Context, *Stream) error
type ChannelHandler func(context.Context, *Channel) error

type EventKind string

const (
	NotifyFailed  EventKind = "notify_failed"
	SendFailed    EventKind = "send_failed"
	HandlerFailed EventKind = "handler_failed"
)

type Event struct {
	Kind      EventKind
	Operation uint64
	Route     string
	Err       error
}

type Role uint8

const (
	Initiator Role = iota
	Acceptor
)

// AuthInfo describes the transport already verified before peer authorization.
type AuthInfo struct {
	TLS   *tls.ConnectionState
	Local bool
}

type OperationInfo struct {
	ID    uint64
	Mode  string
	Route string
}

type Limits struct {
	MaxMessageSize        int `json:"max_message_size"`
	MaxMetadataBytes      int `json:"max_metadata_bytes"`
	MaxMetadataEntries    int `json:"max_metadata_entries"`
	MaxOperations         int `json:"max_operations"`
	MaxHandlers           int `json:"max_handlers"`
	ReceiveBytes          int `json:"receive_bytes"`
	SendBytes             int `json:"send_bytes"`
	ChannelMaxMessage     int `json:"channel_max_message"`
	ChannelWindowBytes    int `json:"channel_window_bytes"`
	ChannelWindowMessages int `json:"channel_window_messages"`
}

type TransportConfig struct {
	TLS                   *tls.Config
	AllowInsecureLoopback bool
	ExpectedPeer          PeerID
}

type SessionConfig struct {
	Local  Peer
	Mux    *Mux
	Limits Limits
	// StreamReceiveWindow bounds each native stream's buffered bytes. Default
	// 64KiB; 4KiB..1MiB in 4KiB steps, within the fixed 4MiB session budget.
	StreamReceiveWindow   int
	HandshakeTimeout      time.Duration
	DefaultRequestTimeout time.Duration
	DefaultWriteTimeout   time.Duration
	KeepAliveInterval     time.Duration
	IdleTimeout           time.Duration
	Authorize             func(AuthInfo, Peer) error
	AuthorizeOperation    func(context.Context, Peer, OperationInfo) error
	Transport             TransportConfig
	connection            connectionBinding
	acceptConnection      func(context.Context, Peer, SessionID, connectionBinding) (connectionBinding, error)
}
