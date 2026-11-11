package epmd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultPort = 4369

	NodeTypeNormal byte = 77
	NodeTypeHidden byte = 72

	ProtocolTCP byte = 0

	DistVersion uint16 = 6

	reqAlive2      byte = 120
	reqPortPlease2 byte = 122
	reqNames       byte = 110

	respAlive2X byte = 118
	respPort2   byte = 119
	respAlive2  byte = 121
)

var (
	ErrInvalidNodeName  = errors.New("epmd: invalid node name")
	ErrDuplicateName    = errors.New("epmd: node name already registered")
	ErrNodeNotFound     = errors.New("epmd: node not registered")
	ErrVersionMismatch  = errors.New("epmd: incompatible distribution version")
	ErrProtocolResponse = errors.New("epmd: malformed response")
)

// SplitNodeName validates an explicit ASCII full Erlang node name ("alive@host")
// without rewriting hostnames, appending domains, or folding case.
func SplitNodeName(full string) (alive, host string, err error) {
	if len(full) == 0 || len(full) > 255 {
		return "", "", fmt.Errorf("%w: %q length out of [1,255]", ErrInvalidNodeName, full)
	}
	for i := 0; i < len(full); i++ {
		c := full[i]
		if c <= 0x20 || c >= 0x7f {
			return "", "", fmt.Errorf("%w: non-ASCII or whitespace byte in %q", ErrInvalidNodeName, full)
		}
	}
	at := strings.IndexByte(full, '@')
	if at <= 0 || at == len(full)-1 || strings.IndexByte(full[at+1:], '@') >= 0 {
		return "", "", fmt.Errorf("%w: expected alive@host in %q", ErrInvalidNodeName, full)
	}
	return full[:at], full[at+1:], nil
}

// PortInfo describes an EPMD PORT2_RESP record.
type PortInfo struct {
	Port           uint16
	NodeType       byte
	Protocol       byte
	HighestVersion uint16
	LowestVersion  uint16
	AliveName      string
	Extra          []byte
}

// Registration holds the live ALIVE2 TCP connection to EPMD for a node incarnation.
type Registration struct {
	conn      net.Conn
	creation  uint32
	closed    atomic.Bool
	lost      chan struct{}
	closeOnce sync.Once
}

// Creation returns the 32-bit incarnation identifier assigned by EPMD.
func (r *Registration) Creation() uint32 {
	return r.creation
}

// Lost closes if the EPMD daemon drops the registration socket while the node is running.
func (r *Registration) Lost() <-chan struct{} {
	return r.lost
}

// Close unregisters the node from EPMD by closing the registration TCP socket.
func (r *Registration) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		err = r.conn.Close()
	})
	return err
}

// Register opens a persistent TCP connection to epmdAddr and registers aliveName on listenPort.
func Register(ctx context.Context, epmdAddr, aliveName string, listenPort uint16, hidden bool, extra []byte) (*Registration, error) {
	if aliveName == "" || len(aliveName) > 255 {
		return nil, fmt.Errorf("%w: invalid alive name %q", ErrInvalidNodeName, aliveName)
	}
	if listenPort == 0 {
		return nil, errors.New("epmd: listenPort must be non-zero")
	}
	if len(extra) > 1024 {
		return nil, errors.New("epmd: extra exceeds 1024 bytes")
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", epmdAddr)
	if err != nil {
		return nil, err
	}

	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	}

	nodeType := NodeTypeHidden
	if !hidden {
		nodeType = NodeTypeNormal
	}

	bodyLen := 1 + 2 + 1 + 1 + 2 + 2 + 2 + len(aliveName) + 2 + len(extra)
	pkt := make([]byte, 2+bodyLen)
	binary.BigEndian.PutUint16(pkt[0:2], uint16(bodyLen))
	pkt[2] = reqAlive2
	binary.BigEndian.PutUint16(pkt[3:5], listenPort)
	pkt[5] = nodeType
	pkt[6] = ProtocolTCP
	binary.BigEndian.PutUint16(pkt[7:9], DistVersion)
	binary.BigEndian.PutUint16(pkt[9:11], DistVersion)
	binary.BigEndian.PutUint16(pkt[11:13], uint16(len(aliveName)))
	copy(pkt[13:13+len(aliveName)], aliveName)
	off := 13 + len(aliveName)
	binary.BigEndian.PutUint16(pkt[off:off+2], uint16(len(extra)))
	copy(pkt[off+2:], extra)

	if _, err := writeFull(conn, pkt); err != nil {
		stopCancel()
		_ = conn.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	var tag [2]byte
	if _, err := io.ReadFull(conn, tag[:]); err != nil {
		stopCancel()
		_ = conn.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %v", ErrProtocolResponse, err)
	}

	var creation uint32
	switch tag[0] {
	case respAlive2X:
		var creBuf [4]byte
		if _, err := io.ReadFull(conn, creBuf[:]); err != nil {
			stopCancel()
			_ = conn.Close()
			return nil, fmt.Errorf("%w: %v", ErrProtocolResponse, err)
		}
		if tag[1] != 0 {
			stopCancel()
			_ = conn.Close()
			return nil, fmt.Errorf("%w: result=%d", ErrDuplicateName, tag[1])
		}
		creation = binary.BigEndian.Uint32(creBuf[:])
	case respAlive2:
		var creBuf [2]byte
		if _, err := io.ReadFull(conn, creBuf[:]); err != nil {
			stopCancel()
			_ = conn.Close()
			return nil, fmt.Errorf("%w: %v", ErrProtocolResponse, err)
		}
		if tag[1] != 0 {
			stopCancel()
			_ = conn.Close()
			return nil, fmt.Errorf("%w: result=%d", ErrDuplicateName, tag[1])
		}
		creation = uint32(binary.BigEndian.Uint16(creBuf[:]))
	default:
		stopCancel()
		_ = conn.Close()
		return nil, fmt.Errorf("%w: unexpected tag %d", ErrProtocolResponse, tag[0])
	}

	if !stopCancel() {
		_ = conn.Close()
		return nil, ctx.Err()
	}
	// Clear deadline so the alive registration socket stays open indefinitely.
	_ = conn.SetDeadline(time.Time{})

	reg := &Registration{
		conn:     conn,
		creation: creation,
		lost:     make(chan struct{}),
	}
	go reg.watchEOF()
	return reg, nil
}

func (r *Registration) watchEOF() {
	var buf [1]byte
	for {
		_, err := r.conn.Read(buf[:])
		if err != nil {
			if !r.closed.Load() {
				_ = r.conn.Close()
				close(r.lost)
			}
			return
		}
	}
}

// LookupPort queries epmdAddr using PORT_PLEASE2_REQ (122) for aliveName.
func LookupPort(ctx context.Context, epmdAddr, aliveName string) (PortInfo, error) {
	if aliveName == "" || len(aliveName) > 255 {
		return PortInfo{}, fmt.Errorf("%w: invalid alive name %q", ErrInvalidNodeName, aliveName)
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", epmdAddr)
	if err != nil {
		return PortInfo{}, err
	}
	defer conn.Close()

	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	defer stopCancel()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	}

	bodyLen := 1 + len(aliveName)
	pkt := make([]byte, 2+bodyLen)
	binary.BigEndian.PutUint16(pkt[0:2], uint16(bodyLen))
	pkt[2] = reqPortPlease2
	copy(pkt[3:], aliveName)

	if _, err := writeFull(conn, pkt); err != nil {
		if ctx.Err() != nil {
			return PortInfo{}, ctx.Err()
		}
		return PortInfo{}, err
	}

	// Read message type and result first; error responses are strictly 2 bytes [119, Result].
	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		if ctx.Err() != nil {
			return PortInfo{}, ctx.Err()
		}
		return PortInfo{}, fmt.Errorf("%w: %v", ErrProtocolResponse, err)
	}
	if hdr[0] != respPort2 {
		return PortInfo{}, fmt.Errorf("%w: expected %d, got %d", ErrProtocolResponse, respPort2, hdr[0])
	}
	if hdr[1] != 0 {
		return PortInfo{}, fmt.Errorf("%w: %q (result=%d)", ErrNodeNotFound, aliveName, hdr[1])
	}

	var fixed [10]byte // Port(2) + NodeType(1) + Proto(1) + HiVer(2) + LoVer(2) + Nlen(2)
	if _, err := io.ReadFull(conn, fixed[:]); err != nil {
		return PortInfo{}, fmt.Errorf("%w: %v", ErrProtocolResponse, err)
	}
	info := PortInfo{
		Port:           binary.BigEndian.Uint16(fixed[0:2]),
		NodeType:       fixed[2],
		Protocol:       fixed[3],
		HighestVersion: binary.BigEndian.Uint16(fixed[4:6]),
		LowestVersion:  binary.BigEndian.Uint16(fixed[6:8]),
	}
	nlen := int(binary.BigEndian.Uint16(fixed[8:10]))
	if nlen > 255 {
		return PortInfo{}, fmt.Errorf("%w: alive name length %d > 255", ErrProtocolResponse, nlen)
	}
	nameBuf := make([]byte, nlen+2) // NodeName(nlen) + Elen(2)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		return PortInfo{}, fmt.Errorf("%w: %v", ErrProtocolResponse, err)
	}
	info.AliveName = string(nameBuf[:nlen])
	elen := int(binary.BigEndian.Uint16(nameBuf[nlen : nlen+2]))
	if elen > 4096 {
		return PortInfo{}, fmt.Errorf("%w: extra length %d > 4096", ErrProtocolResponse, elen)
	}
	if elen > 0 {
		info.Extra = make([]byte, elen)
		if _, err := io.ReadFull(conn, info.Extra); err != nil {
			return PortInfo{}, fmt.Errorf("%w: %v", ErrProtocolResponse, err)
		}
	}
	if info.LowestVersion > DistVersion || info.HighestVersion < DistVersion {
		return PortInfo{}, fmt.Errorf("%w: peer range [%d,%d]", ErrVersionMismatch, info.LowestVersion, info.HighestVersion)
	}
	if info.Port == 0 {
		return PortInfo{}, fmt.Errorf("%w: zero port", ErrProtocolResponse)
	}
	return info, nil
}

// Names sends NAMES_REQ (110) and returns the EPMD port and raw listing text.
func Names(ctx context.Context, epmdAddr string) (uint32, string, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", epmdAddr)
	if err != nil {
		return 0, "", err
	}
	defer conn.Close()

	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	defer stopCancel()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	}

	if _, err := writeFull(conn, []byte{0, 1, reqNames}); err != nil {
		return 0, "", err
	}
	var portBuf [4]byte
	if _, err := io.ReadFull(conn, portBuf[:]); err != nil {
		return 0, "", fmt.Errorf("%w: %v", ErrProtocolResponse, err)
	}
	raw, err := io.ReadAll(io.LimitReader(conn, 64<<10))
	if err != nil {
		return 0, "", err
	}
	return binary.BigEndian.Uint32(portBuf[:]), string(raw), nil
}

func writeFull(w io.Writer, b []byte) (int, error) {
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
