package dist

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/sllt/tao/erlang/internal/epmd"
)

const (
	PacketSizeTCPHandshake = 2
	PacketSizeTLSHandshake = 4
	maxHandshakePacketLen  = 4096
	defaultReadBufferSize  = 16 << 10
)

var (
	ErrHandshakeProtocol          = errors.New("dist: malformed handshake message")
	ErrLegacyHandshakeUnsupported = errors.New("dist: legacy v5 'n' handshake is unsupported")
	ErrUnexpectedPeerName         = errors.New("dist: peer node name mismatch")
	ErrCookieMismatch             = errors.New("dist: cookie authentication digest mismatch")
	ErrNotAllowed                 = errors.New("dist: connection not allowed")
	ErrSimultaneousReject         = errors.New("dist: simultaneous connect rejected (nok)")
	ErrAliveDeclined              = errors.New("dist: duplicate connection declined after alive status")
)

// InboundDecision represents the coordinator's decision during step 3 of an inbound handshake.
type InboundDecision uint8

const (
	DecisionOK InboundDecision = iota
	DecisionOKSimultaneous
	DecisionNOK
	DecisionNotAllowed
	DecisionAlive
)

// Conn wraps a net.Conn with a buffered reader so bytes received immediately after
// handshake completion remain available to the connected packet-4 frame reader.
type Conn struct {
	raw net.Conn
	rd  *bufio.Reader
}

// WrapConn creates a buffered dist Conn around raw.
func WrapConn(raw net.Conn) *Conn {
	return &Conn{
		raw: raw,
		rd:  bufio.NewReaderSize(raw, defaultReadBufferSize),
	}
}

func (c *Conn) RawConn() net.Conn                  { return c.raw }
func (c *Conn) Read(p []byte) (int, error)         { return c.rd.Read(p) }
func (c *Conn) ReadFull(p []byte) error            { _, err := io.ReadFull(c.rd, p); return err }
func (c *Conn) Write(p []byte) (int, error)        { return writeAll(c.raw, p) }
func (c *Conn) Close() error                       { return c.raw.Close() }
func (c *Conn) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }

// HandshakeResult reports the authenticated peer parameters after v6 handshake completion.
type HandshakeResult struct {
	Conn            *Conn
	PeerName        string
	PeerCreation    uint32
	PeerFlags       uint64
	NegotiatedFlags uint64
}

// ClientHandshakeConfig configures an outbound v6 ('N') handshake.
type ClientHandshakeConfig struct {
	LocalName        string
	LocalCreation    uint32
	LocalFlags       uint64
	ExpectedPeerName string
	InCookie         string
	OutCookie        string
	PacketSize       int // 2 for TCP, 4 for TLS
	// ConfirmAlive is consulted if the peer responds with "alive". Returning false sends "false" and aborts.
	ConfirmAlive func() bool
}

// ServerHandshakeConfig configures an inbound v6 ('N') handshake.
type ServerHandshakeConfig struct {
	LocalName     string
	LocalCreation uint32
	LocalFlags    uint64
	PacketSize    int // 2 for TCP, 4 for TLS
	AllowPeer     func(peerName string) bool
	CookiesFor    func(peerName string) (inCookie, outCookie string)
	// Arbitrate is invoked at step 3 BEFORE cookie authentication; it must NOT tear down
	// any existing authenticated connection yet.
	Arbitrate func(peerName string, peerCreation uint32, peerFlags uint64) InboundDecision
	// CommitAuthenticated is invoked at step 6 ONLY AFTER the peer's challenge_reply MD5
	// digest has been verified in constant time.
	CommitAuthenticated func(peerName string, peerCreation uint32, negotiated uint64, wasAlive bool) error
}

// ComputeDigest computes OTP's MD5(cookie || decimal_ascii(challenge)).
func ComputeDigest(cookie string, challenge uint32) [16]byte {
	var dec [16]byte
	ascii := strconv.AppendUint(dec[:0], uint64(challenge), 10)
	h := md5.New()
	_, _ = io.WriteString(h, cookie)
	_, _ = h.Write(ascii)
	var out [16]byte
	copy(out[:], h.Sum(nil))
	return out
}

// VerifyDigest compares two 16-byte challenge digests in constant time.
func VerifyDigest(got, want [16]byte) bool {
	return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}

func randomChallenge() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

// HandshakeClient executes the initiator side of the OTP v6 distribution handshake.
func HandshakeClient(ctx context.Context, raw net.Conn, cfg ClientHandshakeConfig) (*HandshakeResult, error) {
	pktSize := cfg.PacketSize
	if pktSize != PacketSizeTCPHandshake && pktSize != PacketSizeTLSHandshake {
		pktSize = PacketSizeTCPHandshake
	}
	conn := WrapConn(raw)
	cleanup := bindContextDeadline(ctx, conn)
	defer cleanup()

	// Step 2: send_name ('N' + Flags:8 + Creation:4 + Nlen:2 + Name)
	sendName := buildSendName(cfg.LocalFlags, cfg.LocalCreation, cfg.LocalName)
	if err := writeHandshakePacket(conn, pktSize, sendName); err != nil {
		return nil, wrapCtxErr(ctx, err)
	}

	// Step 3: recv_status ('s' + Status)
	statusPkt, err := readHandshakePacket(conn, pktSize)
	if err != nil {
		return nil, wrapCtxErr(ctx, err)
	}
	if len(statusPkt) < 2 || statusPkt[0] != 's' {
		return nil, fmt.Errorf("%w: invalid status tag", ErrHandshakeProtocol)
	}
	status := string(statusPkt[1:])
	switch status {
	case "ok", "ok_simultaneous":
	case "alive":
		cont := true
		if cfg.ConfirmAlive != nil {
			cont = cfg.ConfirmAlive()
		}
		reply := "sfalse"
		if cont {
			reply = "strue"
		}
		if err := writeHandshakePacket(conn, pktSize, []byte(reply)); err != nil {
			return nil, wrapCtxErr(ctx, err)
		}
		if !cont {
			return nil, ErrAliveDeclined
		}
	case "nok":
		return nil, ErrSimultaneousReject
	case "not_allowed":
		return nil, ErrNotAllowed
	default:
		return nil, fmt.Errorf("%w: unexpected status %q", ErrHandshakeProtocol, status)
	}

	// Step 4: recv_challenge ('N' + Flags:8 + Challenge:4 + Creation:4 + Nlen:2 + Name)
	chalPkt, err := readHandshakePacket(conn, pktSize)
	if err != nil {
		return nil, wrapCtxErr(ctx, err)
	}
	peerFlags, peerChal, peerCreation, peerName, err := parseChallenge(chalPkt)
	if err != nil {
		return nil, err
	}
	if peerName != cfg.ExpectedPeerName {
		return nil, fmt.Errorf("%w: got %q, expected %q", ErrUnexpectedPeerName, peerName, cfg.ExpectedPeerName)
	}
	negotiated, err := NegotiateFlags(cfg.LocalFlags, peerFlags)
	if err != nil {
		return nil, err
	}

	// Step 5: send_challenge_reply ('r' + OurChallenge:4 + Digest:16)
	ourChal, err := randomChallenge()
	if err != nil {
		return nil, err
	}
	outCookie := cfg.OutCookie
	inCookie := cfg.InCookie
	if inCookie == "" {
		inCookie = outCookie
	}
	ourDigest := ComputeDigest(outCookie, peerChal)
	var replyPkt [21]byte
	replyPkt[0] = 'r'
	binary.BigEndian.PutUint32(replyPkt[1:5], ourChal)
	copy(replyPkt[5:21], ourDigest[:])
	if err := writeHandshakePacket(conn, pktSize, replyPkt[:]); err != nil {
		return nil, wrapCtxErr(ctx, err)
	}

	// Step 6 & 7: recv_challenge_ack ('a' + PeerDigest:16)
	ackPkt, err := readHandshakePacket(conn, pktSize)
	if err != nil {
		return nil, wrapCtxErr(ctx, err)
	}
	if len(ackPkt) != 17 || ackPkt[0] != 'a' {
		return nil, fmt.Errorf("%w: invalid challenge_ack", ErrHandshakeProtocol)
	}
	var gotDigest [16]byte
	copy(gotDigest[:], ackPkt[1:17])
	wantDigest := ComputeDigest(inCookie, ourChal)
	if !VerifyDigest(gotDigest, wantDigest) {
		return nil, ErrCookieMismatch
	}

	_ = conn.SetDeadline(time.Time{})
	return &HandshakeResult{
		Conn:            conn,
		PeerName:        peerName,
		PeerCreation:    peerCreation,
		PeerFlags:       peerFlags,
		NegotiatedFlags: negotiated,
	}, nil
}

// HandshakeServer executes the acceptor side of the OTP v6 distribution handshake.
func HandshakeServer(ctx context.Context, raw net.Conn, cfg ServerHandshakeConfig) (*HandshakeResult, error) {
	pktSize := cfg.PacketSize
	if pktSize != PacketSizeTCPHandshake && pktSize != PacketSizeTLSHandshake {
		pktSize = PacketSizeTCPHandshake
	}
	conn := WrapConn(raw)
	cleanup := bindContextDeadline(ctx, conn)
	defer cleanup()

	// Step 2: recv_name
	namePkt, err := readHandshakePacket(conn, pktSize)
	if err != nil {
		return nil, wrapCtxErr(ctx, err)
	}
	peerFlags, peerCreation, peerName, err := parseSendName(namePkt)
	if err != nil {
		return nil, err
	}
	if _, _, err := epmd.SplitNodeName(peerName); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrHandshakeProtocol, err)
	}
	if peerName == cfg.LocalName {
		_ = writeHandshakePacket(conn, pktSize, []byte("snot_allowed"))
		return nil, ErrNotAllowed
	}
	if cfg.AllowPeer != nil && !cfg.AllowPeer(peerName) {
		_ = writeHandshakePacket(conn, pktSize, []byte("snot_allowed"))
		return nil, ErrNotAllowed
	}
	negotiated, err := NegotiateFlags(cfg.LocalFlags, peerFlags)
	if err != nil {
		return nil, err
	}

	// Step 3: send_status
	decision := DecisionOK
	if cfg.Arbitrate != nil {
		decision = cfg.Arbitrate(peerName, peerCreation, peerFlags)
	}
	wasAlive := false
	switch decision {
	case DecisionOK:
		if err := writeHandshakePacket(conn, pktSize, []byte("sok")); err != nil {
			return nil, wrapCtxErr(ctx, err)
		}
	case DecisionOKSimultaneous:
		if err := writeHandshakePacket(conn, pktSize, []byte("sok_simultaneous")); err != nil {
			return nil, wrapCtxErr(ctx, err)
		}
	case DecisionNOK:
		_ = writeHandshakePacket(conn, pktSize, []byte("snok"))
		return nil, ErrSimultaneousReject
	case DecisionNotAllowed:
		_ = writeHandshakePacket(conn, pktSize, []byte("snot_allowed"))
		return nil, ErrNotAllowed
	case DecisionAlive:
		wasAlive = true
		if err := writeHandshakePacket(conn, pktSize, []byte("salive")); err != nil {
			return nil, wrapCtxErr(ctx, err)
		}
		ans, err := readHandshakePacket(conn, pktSize)
		if err != nil {
			return nil, wrapCtxErr(ctx, err)
		}
		if len(ans) < 2 || ans[0] != 's' {
			return nil, fmt.Errorf("%w: invalid alive reply", ErrHandshakeProtocol)
		}
		switch string(ans[1:]) {
		case "true":
		case "false":
			return nil, ErrAliveDeclined
		default:
			return nil, fmt.Errorf("%w: unexpected alive reply %q", ErrHandshakeProtocol, string(ans[1:]))
		}
	default:
		return nil, fmt.Errorf("%w: unknown decision %d", ErrHandshakeProtocol, decision)
	}

	// Step 4: send_challenge
	ourChal, err := randomChallenge()
	if err != nil {
		return nil, err
	}
	chalPkt := buildChallenge(cfg.LocalFlags, ourChal, cfg.LocalCreation, cfg.LocalName)
	if err := writeHandshakePacket(conn, pktSize, chalPkt); err != nil {
		return nil, wrapCtxErr(ctx, err)
	}

	// Step 5: recv_challenge_reply ('r' + PeerChallenge:4 + PeerDigest:16)
	replyPkt, err := readHandshakePacket(conn, pktSize)
	if err != nil {
		return nil, wrapCtxErr(ctx, err)
	}
	if len(replyPkt) != 21 || replyPkt[0] != 'r' {
		return nil, fmt.Errorf("%w: invalid challenge_reply", ErrHandshakeProtocol)
	}
	peerChal := binary.BigEndian.Uint32(replyPkt[1:5])
	var gotDigest [16]byte
	copy(gotDigest[:], replyPkt[5:21])

	inCookie, outCookie := "", ""
	if cfg.CookiesFor != nil {
		inCookie, outCookie = cfg.CookiesFor(peerName)
	}
	if outCookie == "" {
		outCookie = inCookie
	}
	if !VerifyDigest(gotDigest, ComputeDigest(inCookie, ourChal)) {
		return nil, ErrCookieMismatch
	}

	if cfg.CommitAuthenticated != nil {
		if err := cfg.CommitAuthenticated(peerName, peerCreation, negotiated, wasAlive); err != nil {
			return nil, err
		}
	}

	// Step 6: send_challenge_ack ('a' + OurDigest:16)
	ackDigest := ComputeDigest(outCookie, peerChal)
	var ackPkt [17]byte
	ackPkt[0] = 'a'
	copy(ackPkt[1:17], ackDigest[:])
	if err := writeHandshakePacket(conn, pktSize, ackPkt[:]); err != nil {
		return nil, wrapCtxErr(ctx, err)
	}

	_ = conn.SetDeadline(time.Time{})
	return &HandshakeResult{
		Conn:            conn,
		PeerName:        peerName,
		PeerCreation:    peerCreation,
		PeerFlags:       peerFlags,
		NegotiatedFlags: negotiated,
	}, nil
}

func buildSendName(flags uint64, creation uint32, name string) []byte {
	b := make([]byte, 1+8+4+2+len(name))
	b[0] = 'N'
	binary.BigEndian.PutUint64(b[1:9], flags)
	binary.BigEndian.PutUint32(b[9:13], creation)
	binary.BigEndian.PutUint16(b[13:15], uint16(len(name)))
	copy(b[15:], name)
	return b
}

func parseSendName(pkt []byte) (flags uint64, creation uint32, name string, err error) {
	if len(pkt) == 0 {
		return 0, 0, "", ErrHandshakeProtocol
	}
	if pkt[0] == 'n' {
		return 0, 0, "", ErrLegacyHandshakeUnsupported
	}
	if pkt[0] != 'N' || len(pkt) < 15 {
		return 0, 0, "", fmt.Errorf("%w: expected v6 'N' send_name", ErrHandshakeProtocol)
	}
	flags = binary.BigEndian.Uint64(pkt[1:9])
	creation = binary.BigEndian.Uint32(pkt[9:13])
	nlen := int(binary.BigEndian.Uint16(pkt[13:15]))
	if len(pkt) < 15+nlen {
		return 0, 0, "", fmt.Errorf("%w: truncated send_name", ErrHandshakeProtocol)
	}
	// Extra data after Name is accepted and ignored per erl_dist_protocol.md.
	return flags, creation, string(pkt[15 : 15+nlen]), nil
}

func buildChallenge(flags uint64, challenge, creation uint32, name string) []byte {
	b := make([]byte, 1+8+4+4+2+len(name))
	b[0] = 'N'
	binary.BigEndian.PutUint64(b[1:9], flags)
	binary.BigEndian.PutUint32(b[9:13], challenge)
	binary.BigEndian.PutUint32(b[13:17], creation)
	binary.BigEndian.PutUint16(b[17:19], uint16(len(name)))
	copy(b[19:], name)
	return b
}

func parseChallenge(pkt []byte) (flags uint64, challenge, creation uint32, name string, err error) {
	if len(pkt) < 19 || pkt[0] != 'N' {
		return 0, 0, 0, "", fmt.Errorf("%w: expected v6 'N' challenge", ErrHandshakeProtocol)
	}
	flags = binary.BigEndian.Uint64(pkt[1:9])
	challenge = binary.BigEndian.Uint32(pkt[9:13])
	creation = binary.BigEndian.Uint32(pkt[13:17])
	nlen := int(binary.BigEndian.Uint16(pkt[17:19]))
	if len(pkt) < 19+nlen {
		return 0, 0, 0, "", fmt.Errorf("%w: truncated challenge name", ErrHandshakeProtocol)
	}
	return flags, challenge, creation, string(pkt[19 : 19+nlen]), nil
}

func readHandshakePacket(c *Conn, pktSize int) ([]byte, error) {
	var n int
	if pktSize == PacketSizeTLSHandshake {
		var hdr [4]byte
		if err := c.ReadFull(hdr[:]); err != nil {
			return nil, err
		}
		u := binary.BigEndian.Uint32(hdr[:])
		if u == 0 || u > maxHandshakePacketLen {
			return nil, fmt.Errorf("%w: handshake packet length %d out of [1,%d]", ErrHandshakeProtocol, u, maxHandshakePacketLen)
		}
		n = int(u)
	} else {
		var hdr [2]byte
		if err := c.ReadFull(hdr[:]); err != nil {
			return nil, err
		}
		u := binary.BigEndian.Uint16(hdr[:])
		if u == 0 || int(u) > maxHandshakePacketLen {
			return nil, fmt.Errorf("%w: handshake packet length %d out of [1,%d]", ErrHandshakeProtocol, u, maxHandshakePacketLen)
		}
		n = int(u)
	}
	buf := make([]byte, n)
	if err := c.ReadFull(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeHandshakePacket(c *Conn, pktSize int, body []byte) error {
	pkt := make([]byte, pktSize+len(body))
	if pktSize == PacketSizeTLSHandshake {
		binary.BigEndian.PutUint32(pkt[0:4], uint32(len(body)))
	} else {
		binary.BigEndian.PutUint16(pkt[0:2], uint16(len(body)))
	}
	copy(pkt[pktSize:], body)
	_, err := c.Write(pkt)
	return err
}

func bindContextDeadline(ctx context.Context, c *Conn) func() {
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	} else {
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	}
	stop := context.AfterFunc(ctx, func() {
		_ = c.SetDeadline(time.Unix(1, 0))
	})
	return func() { stop() }
}

func wrapCtxErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return err
}

func writeAll(w io.Writer, b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		n, err := w.Write(b)
		total += n
		b = b[n:]
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
