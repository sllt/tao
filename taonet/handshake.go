package taonet

import (
	"context"
	"fmt"
	"slices"
	"time"

	native "github.com/sllt/tao/taonet/internal/mux"
)

type hello struct {
	Major      int               `json:"major"`
	Minor      int               `json:"minor"`
	Peer       Peer              `json:"peer"`
	Session    SessionID         `json:"session"`
	Features   []string          `json:"features"`
	Required   []string          `json:"required"`
	Limits     Limits            `json:"limits"`
	Connection connectionBinding `json:"connection"`
}
type ready struct {
	Major   int       `json:"major"`
	Session SessionID `json:"session"`
}

const channelInlineFeature = "channel-inline-v1"
const channelUnconfirmedFeature = "channel-unconfirmed-v1"

var supportedFeatures = []string{"request", "notify", "stream", "channel", channelInlineFeature, channelUnconfirmedFeature}

func validateHello(h hello, cfg SessionConfig, auth AuthInfo) error {
	if h.Major != 1 || h.Minor < 0 || !slices.Contains(h.Features, "request") || len(h.Features) > 32 || len(h.Required) > 32 {
		return protocolError("incompatible protocol capabilities")
	}
	for _, feature := range h.Required {
		if !slices.Contains(supportedFeatures, feature) {
			return protocolError("unsupported required feature %q", feature)
		}
	}
	limits, err := h.Limits.normalized()
	if err != nil || limits != h.Limits {
		return protocolError("invalid peer limits")
	}
	if !validName(string(h.Peer.ID)) || h.Peer.Instance == (ID{}) || h.Peer.Namespace != cfg.Local.Namespace {
		return &StatusError{Code: CodeUnauthenticated, Message: "invalid peer identity or namespace"}
	}
	if cfg.Transport.ExpectedPeer != "" && cfg.Transport.ExpectedPeer != h.Peer.ID {
		return &StatusError{Code: CodeUnauthenticated, Message: "unexpected peer identity"}
	}
	if cfg.Authorize != nil {
		if err := cfg.Authorize(auth, h.Peer); err != nil {
			return fmt.Errorf("taonet peer authorization: %w", err)
		}
	} else if !auth.Local {
		if err := auth.TLS.PeerCertificates[0].VerifyHostname(string(h.Peer.ID)); err != nil {
			return &StatusError{Code: CodeUnauthenticated, Message: "peer ID does not match its certificate"}
		}
	}
	return nil
}

func negotiate(ctx context.Context, raw *native.Session, role Role, cfg SessionConfig, auth AuthInfo) (*native.Stream, SessionInfo, Limits, error) {
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	var control *native.Stream
	var err error
	if role == Initiator {
		control, err = raw.OpenControl(ctx)
	} else {
		control, err = raw.AcceptControl(ctx)
	}
	if err != nil {
		return nil, SessionInfo{}, Limits{}, err
	}
	deadline, _ := ctx.Deadline()
	_ = control.SetReadDeadline(deadline)
	_ = control.SetWriteDeadline(deadline)
	local := hello{Major: 1, Peer: cfg.Local, Features: supportedFeatures, Required: []string{"request"}, Limits: cfg.Limits, Connection: cfg.connection}
	var remote hello
	if role == Initiator {
		if err = writeJSON(control, recordHello, local); err == nil {
			err = readJSON(control, recordWelcome, &remote)
		}
		if err == nil {
			err = validateHello(remote, cfg, auth)
		}
		if err == nil && remote.Session == (ID{}) {
			err = protocolError("missing session ID")
		}
		if err == nil {
			err = validateConnectionReply(local, remote)
		}
		if err == nil {
			local.Session = remote.Session
			local.Connection = remote.Connection
			// Permit transport admission before READY; application handlers start
			// only after the peer's final acknowledgment below.
			raw.MarkReady()
			err = writeJSON(control, recordReady, ready{1, local.Session})
		}
		if err == nil {
			var ack ready
			err = readJSON(control, recordReadyAck, &ack)
			if err == nil && (ack.Major != 1 || ack.Session != local.Session) {
				err = protocolError("invalid READY_ACK")
			}
		}
	} else {
		if err = readJSON(control, recordHello, &remote); err == nil {
			err = validateHello(remote, cfg, auth)
		}
		if err == nil {
			local.Session, err = newID()
		}
		if err == nil {
			if cfg.acceptConnection != nil {
				local.Connection, err = cfg.acceptConnection(ctx, remote.Peer, local.Session, remote.Connection)
			} else if remote.Connection != (connectionBinding{}) {
				err = protocolError("connection profile requires an Endpoint")
			}
		}
		if err == nil {
			err = writeJSON(control, recordWelcome, local)
		}
		if err == nil {
			var confirmation ready
			err = readJSON(control, recordReady, &confirmation)
			if err == nil && (confirmation.Major != 1 || confirmation.Session != local.Session) {
				err = protocolError("invalid READY")
			}
		}
		if err == nil {
			raw.MarkReady()
			err = writeJSON(control, recordReadyAck, ready{1, local.Session})
		}
	}
	if err != nil {
		return nil, SessionInfo{}, Limits{}, err
	}
	if ctx.Err() != nil {
		return nil, SessionInfo{}, Limits{}, ctx.Err()
	}
	_ = control.SetReadDeadline(time.Time{})
	_ = control.SetWriteDeadline(time.Time{})
	transport := "tcp/tls"
	if auth.Local {
		transport = "tcp/loopback"
	}
	features := make([]string, 0, len(supportedFeatures))
	for _, feature := range supportedFeatures {
		if slices.Contains(remote.Features, feature) {
			features = append(features, feature)
		}
	}
	info := SessionInfo{ID: local.Session, Local: cfg.Local, Remote: remote.Peer, Transport: transport, Features: features, Class: local.Connection.Class, Group: local.Connection.Group}
	return control, info, remote.Limits, nil
}
