package taonet

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	native "github.com/sllt/tao/taonet/internal/mux"
)

const tlsALPN = "taonet/1"

func Dial(ctx context.Context, address string, cfg SessionConfig) (*Session, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	return dial(ctx, address, cfg)
}

func dial(ctx context.Context, address string, cfg SessionConfig) (*Session, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	if cfg.Transport.TLS != nil && cfg.Transport.TLS.ServerName == "" {
		cfg.Transport.TLS = cfg.Transport.TLS.Clone()
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		cfg.Transport.TLS.ServerName = host
		if cfg.Transport.ExpectedPeer != "" && cfg.Authorize == nil {
			cfg.Transport.TLS.ServerName = string(cfg.Transport.ExpectedPeer)
		}
	}
	return newSession(ctx, conn, Initiator, cfg)
}

func Accept(ctx context.Context, conn net.Conn, cfg SessionConfig) (*Session, error) {
	return NewSession(ctx, conn, Acceptor, cfg)
}

// NewSession takes ownership of an existing connection, including on failure.
// Its context controls connection establishment, not the returned lifetime.
func NewSession(ctx context.Context, conn net.Conn, role Role, cfg SessionConfig) (*Session, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
	defer cancel()
	return newSession(ctx, conn, role, cfg)
}

func newSession(ctx context.Context, conn net.Conn, role Role, cfg SessionConfig) (*Session, error) {
	if role != Initiator && role != Acceptor {
		_ = conn.Close()
		return nil, fmt.Errorf("taonet: invalid role")
	}
	secured, auth, err := secureConnection(ctx, conn, role, cfg.Transport)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	raw, err := native.New(ctx, secured, native.Role(role), native.Config{SendBudget: muxSendBudget, ReceiveWindow: uint32(cfg.StreamReceiveWindow)})
	if err != nil {
		return nil, err
	}
	control, info, peerLimits, err := negotiate(ctx, raw, role, cfg, auth)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	s := makeSession(raw, control, role, cfg, info, peerLimits)
	s.start()
	return s, nil
}

func secureConnection(ctx context.Context, conn net.Conn, role Role, cfg TransportConfig) (net.Conn, AuthInfo, error) {
	tc, alreadyTLS := conn.(*tls.Conn)
	if !alreadyTLS && cfg.TLS == nil {
		local, okLocal := conn.LocalAddr().(*net.TCPAddr)
		remote, okRemote := conn.RemoteAddr().(*net.TCPAddr)
		if cfg.AllowInsecureLoopback && okLocal && okRemote && local.IP.IsLoopback() && remote.IP.IsLoopback() {
			return conn, AuthInfo{Local: true}, nil
		}
		return nil, AuthInfo{}, &StatusError{Code: CodeUnauthenticated, Message: "TLS is required"}
	}
	if !alreadyTLS {
		c := cfg.TLS.Clone()
		c.MinVersion = max(c.MinVersion, tls.VersionTLS13)
		c.NextProtos = []string{tlsALPN}
		if role == Acceptor {
			c.ClientAuth = tls.RequireAndVerifyClientCert
			tc = tls.Server(conn, c)
		} else {
			tc = tls.Client(conn, c)
		}
	}
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, AuthInfo{}, fmt.Errorf("taonet TLS handshake: %w", err)
	}
	state := tc.ConnectionState()
	if state.Version < tls.VersionTLS13 || state.NegotiatedProtocol != tlsALPN || len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return nil, AuthInfo{}, &StatusError{Code: CodeUnauthenticated, Message: "verified mutual TLS and taonet/1 ALPN are required"}
	}
	return tc, AuthInfo{TLS: &state}, nil
}
