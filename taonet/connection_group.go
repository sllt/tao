package taonet

import (
	"context"
	"encoding/json"
	"fmt"
)

type Profile uint8

const (
	Compact Profile = iota
	Isolated
)

type TrafficClass uint8

const (
	ClassControl TrafficClass = iota
	ClassInteractive
	ClassReply
	ClassBulk
)

func (class TrafficClass) String() string {
	switch class {
	case ClassControl:
		return "control"
	case ClassInteractive:
		return "interactive"
	case ClassReply:
		return "reply"
	case ClassBulk:
		return "bulk"
	default:
		return fmt.Sprintf("TrafficClass(%d)", class)
	}
}

type connectionBinding struct {
	Isolated       bool         `json:"isolated,omitempty"`
	Class          TrafficClass `json:"class,omitempty"`
	Group          SessionID    `json:"group"`
	RemoteInstance InstanceID   `json:"remote_instance"`
}
type connectionGroup struct {
	main   *Session
	remote Peer
}

func validateConnectionReply(local, remote hello) error {
	want, got := local.Connection, remote.Connection
	if !want.Isolated {
		if got != (connectionBinding{}) {
			return protocolError("unexpected isolated connection")
		}
		return nil
	}
	if !got.Isolated || got.Class != want.Class || got.Class > ClassBulk || got.RemoteInstance != local.Peer.Instance || want.RemoteInstance != (ID{}) && want.RemoteInstance != remote.Peer.Instance {
		return protocolError("connection group identity mismatch")
	}
	group := want.Group
	if group == (ID{}) && want.Class == ClassControl {
		group = remote.Session
	}
	if group == (ID{}) || got.Group != group {
		return protocolError("unexpected connection group")
	}
	return nil
}

func (e *Endpoint) acceptConnection(ctx context.Context, peer Peer, sessionID SessionID, binding connectionBinding) (connectionBinding, error) {
	if e.cfg.Profile == Compact {
		if binding != (connectionBinding{}) {
			return connectionBinding{}, protocolError("endpoint uses Compact")
		}
		return binding, nil
	}
	if !binding.Isolated || binding.Class > ClassBulk {
		return connectionBinding{}, protocolError("endpoint requires Isolated")
	}
	if binding.Class == ClassControl {
		if binding.Group != (ID{}) || binding.RemoteInstance != (ID{}) {
			return connectionBinding{}, protocolError("invalid primary connection binding")
		}
		return connectionBinding{Isolated: true, Class: ClassControl, Group: sessionID, RemoteInstance: peer.Instance}, nil
	}
	if binding.Group == (ID{}) || binding.RemoteInstance != e.cfg.Session.Local.Instance {
		return connectionBinding{}, protocolError("invalid auxiliary connection identity")
	}
	for {
		e.mu.Lock()
		group := e.groups[binding.Group]
		changed := e.changed
		closed := e.closed || e.draining
		if group != nil {
			valid := group.remote == peer && group.main.Err() == nil && e.instances[peer.ID] == peer.Instance
			e.mu.Unlock()
			if !valid {
				return connectionBinding{}, protocolError("connection group peer changed")
			}
			binding.RemoteInstance = peer.Instance
			return binding, nil
		}
		e.mu.Unlock()
		if closed {
			return connectionBinding{}, ErrClosed
		}
		// READY_ACK may arrive at the dialer just before the primary is registered
		// here. Wait within the existing handshake budget for that registration.
		select {
		case <-changed:
		case <-ctx.Done():
			return connectionBinding{}, ctx.Err()
		}
	}
}

const addConnectionRoute = "taonet.endpoint/connect"

type connectionRequest struct {
	Group SessionID    `json:"group"`
	Class TrafficClass `json:"class"`
}
type connectionResponse struct {
	Session SessionID `json:"session"`
}

// ConnectClass selects a physical connection inside the chosen primary group.
// Compact rejects non-control classes instead of silently removing isolation.
func (e *Endpoint) ConnectClass(ctx context.Context, target Target, class TrafficClass) (*Session, error) {
	if class == ClassControl {
		return e.Connect(ctx, target)
	}
	if class > ClassBulk || e.cfg.Profile != Isolated {
		return nil, fmt.Errorf("taonet: traffic class requires Isolated")
	}
	ctx, cancel := withDefaultTimeout(ctx, e.cfg.ConnectTimeout)
	defer cancel()
	main, err := e.Connect(ctx, target)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if existing := e.preferredLocked(target.Peer, class, main.info.Group); existing != nil {
		e.reused++
		e.mu.Unlock()
		return existing, nil
	}
	address := target.Address
	if address == "" {
		address = e.sessions[main].Address
	}
	e.mu.Unlock()
	if address != "" {
		return e.connect(ctx, dialTargetKey{Target: Target{Peer: target.Peer, Address: address}, Class: class, Group: main.info.Group})
	}
	payload, _ := json.Marshal(connectionRequest{main.info.Group, class})
	reply, err := main.Request(ctx, addConnectionRoute, Message{Body: payload})
	if err != nil {
		return nil, err
	}
	var response connectionResponse
	if err = json.Unmarshal(reply.Body, &response); err != nil || response.Session == (ID{}) {
		return nil, protocolError("invalid auxiliary connection response")
	}
	for {
		e.mu.Lock()
		for session := range e.sessions {
			if session.info.ID == response.Session && session.info.Group == main.info.Group && session.info.Class == class && session.info.Remote == main.info.Remote {
				e.mu.Unlock()
				return session, nil
			}
		}
		group := e.groups[main.info.Group]
		changed := e.changed
		e.mu.Unlock()
		if group == nil {
			return nil, ErrClosed
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (e *Endpoint) addConnection(ctx context.Context, request *Request) (Message, error) {
	var wanted connectionRequest
	if err := json.Unmarshal(request.Message.Body, &wanted); err != nil {
		return Message{}, protocolError("invalid connection request")
	}
	main := request.Session()
	if wanted.Class == ClassControl || wanted.Class > ClassBulk || wanted.Group != main.info.Group || main.info.Class != ClassControl {
		return Message{}, &StatusError{Code: CodePermissionDenied, Message: "connection request must use its primary session"}
	}
	var target Target
	for {
		e.mu.Lock()
		group := e.groups[wanted.Group]
		target = e.sessions[main]
		changed := e.changed
		closed := e.closed || e.draining
		e.mu.Unlock()
		if group != nil {
			if group.main != main || target.Address == "" {
				return Message{}, &StatusError{Code: CodeUnavailable, Message: "no outbound address for this connection group"}
			}
			break
		}
		if closed {
			return Message{}, ErrClosed
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return Message{}, ctx.Err()
		}
	}
	session, err := e.connect(ctx, dialTargetKey{Target: target, Class: wanted.Class, Group: wanted.Group})
	if err != nil {
		return Message{}, err
	}
	payload, _ := json.Marshal(connectionResponse{session.info.ID})
	return Message{Body: payload}, nil
}
