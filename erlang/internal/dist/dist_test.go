package dist

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sllt/tao/erlang/etf"
)

func TestAtomCacheAndFragmentBoundaries(t *testing.T) {
	flags := DefaultFlags(false)
	enc := NewFrameEncoder(FrameEncoderConfig{
		NegotiatedFlags: flags,
		FragmentSize:    DefaultFragmentSize,
	})
	rd := NewFrameReader(FrameReaderConfig{
		LocalNode:       "tao@127.0.0.1",
		RemoteNode:      "beam@127.0.0.1",
		NegotiatedFlags: flags,
	})

	from := etf.PID{Node: "beam@127.0.0.1", ID: 10, Serial: 0, Creation: 4}
	to := etf.PID{Node: "tao@127.0.0.1", ID: 1, Serial: 0, Creation: 4}
	longAtom := etf.Atom(strings.Repeat("界", 120)) // 360 UTF-8 bytes > 255 -> LongAtoms header bit

	sizes := []int{64, 1024, 63 << 10, 64 << 10, (64 << 10) + 1, (64 << 10) + 512, 1 << 20}
	for idx, sz := range sizes {
		bin := make(etf.Binary, sz)
		for i := range bin {
			bin[i] = byte((i + idx) & 0xff)
		}
		payload := etf.Tuple{etf.Atom("blob"), longAtom, int64(idx), bin}
		frames, err := enc.EncodeFrames(ControlMessage{
			Op:      OpSendSender,
			FromPID: from,
			HasFrom: true,
			ToPID:   to,
		}, payload)
		if err != nil {
			t.Fatalf("size %d EncodeFrames: %v", sz, err)
		}

		var got InboundMessage
		var delivered int
		now := time.Now()
		for fi, fp := range frames {
			raw := (*fp)[4:] // strip packet-4 length
			kind, msg, err := rd.ProcessFrame(raw, now)
			ReleaseBuffer(fp)
			if err != nil {
				t.Fatalf("size %d frame %d/%d ProcessFrame: %v", sz, fi+1, len(frames), err)
			}
			if fi < len(frames)-1 {
				if kind != PacketFragmentPending {
					t.Fatalf("size %d frame %d expected pending, got %v", sz, fi+1, kind)
				}
			} else {
				if kind != PacketMessage {
					t.Fatalf("size %d final frame expected message, got %v", sz, kind)
				}
				got = msg
				delivered++
			}
		}
		if delivered != 1 || !etf.TermEqual(got.Payload, payload) {
			t.Fatalf("size %d round-trip payload mismatch", sz)
		}
	}

	// Uninitialized atom cache slot must fail with ErrAtomCacheMiss:
	badFrame := []byte{
		etf.Version, DistHeaderNormal,
		1,    // 1 atom ref
		0x02, // New=0, Seg=2, LongAtoms=0
		0x77, // internal slot 0x77 (never populated)
		104, 0,
	}
	if _, _, err := rd.ProcessFrame(badFrame, time.Now()); !errors.Is(err, ErrAtomCacheMiss) {
		t.Fatalf("expected ErrAtomCacheMiss, got %v", err)
	}
}

func TestFragmentInterleavingAndFaults(t *testing.T) {
	flags := DefaultFlags(false)
	encA := NewFrameEncoder(FrameEncoderConfig{NegotiatedFlags: flags, FragmentSize: 512})
	rd := NewFrameReader(FrameReaderConfig{
		LocalNode:       "tao@127.0.0.1",
		RemoteNode:      "beam@127.0.0.1",
		NegotiatedFlags: flags,
		FragmentTimeout: 200 * time.Millisecond,
	})

	pid1 := etf.PID{Node: "beam@127.0.0.1", ID: 1, Creation: 4}
	pid2 := etf.PID{Node: "beam@127.0.0.1", ID: 2, Creation: 4}
	to := etf.PID{Node: "tao@127.0.0.1", ID: 9, Creation: 4}

	bigPayload := etf.Binary(bytes.Repeat([]byte{0x42}, 1500)) // 3 fragments at 512B
	frags1, err := encA.EncodeFrames(ControlMessage{Op: OpSendSender, FromPID: pid1, HasFrom: true, ToPID: to}, bigPayload)
	if err != nil || len(frags1) < 3 {
		t.Fatalf("expected >=3 fragments, got %d err=%v", len(frags1), err)
	}
	small2, err := encA.EncodeFrames(ControlMessage{Op: OpSendSender, FromPID: pid2, HasFrom: true, ToPID: to}, etf.Atom("interleaved_other_pid"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, fp := range frags1 {
			ReleaseBuffer(fp)
		}
		for _, fp := range small2 {
			ReleaseBuffer(fp)
		}
	}()

	now := time.Now()
	// Deliver first fragment from pid1
	if kind, _, err := rd.ProcessFrame((*frags1[0])[4:], now); err != nil || kind != PacketFragmentPending {
		t.Fatalf("frag 0 failed: kind=%v err=%v", kind, err)
	}
	// Different sender pid2 CAN interleave while pid1's sequence is open!
	if kind, msg, err := rd.ProcessFrame((*small2[0])[4:], now); err != nil || kind != PacketMessage || msg.Payload != etf.Atom("interleaved_other_pid") {
		t.Fatalf("different sender interleave failed: kind=%v msg=%+v err=%v", kind, msg, err)
	}

	// Same sender pid1 MUST NOT interleave a new message before finishing its sequence!
	illegalSameSender, err := encA.EncodeFrames(ControlMessage{Op: OpSendSender, FromPID: pid1, HasFrom: true, ToPID: to}, etf.Atom("illegal"))
	if err != nil {
		t.Fatal(err)
	}
	defer ReleaseBuffer(illegalSameSender[0])
	if _, _, err := rd.ProcessFrame((*illegalSameSender[0])[4:], now); !errors.Is(err, ErrInterleavedSenderFragment) {
		t.Fatalf("expected ErrInterleavedSenderFragment, got %v", err)
	}

	// Timeout of unfinished fragment sequence:
	if _, _, err := rd.ProcessFrame((*frags1[1])[4:], now.Add(500*time.Millisecond)); !errors.Is(err, ErrFragmentTimeout) {
		t.Fatalf("expected ErrFragmentTimeout, got %v", err)
	}
}

func TestExitPayloadAndSendSenderOneWayTransitions(t *testing.T) {
	flags := DefaultFlags(false)
	legacyOnlyFlags := flags &^ (DFlagExitPayload | DFlagSendSender)

	encLegacy := NewFrameEncoder(FrameEncoderConfig{NegotiatedFlags: legacyOnlyFlags})
	encModern := NewFrameEncoder(FrameEncoderConfig{NegotiatedFlags: flags})
	rd := NewFrameReader(FrameReaderConfig{
		LocalNode:       "tao@127.0.0.1",
		RemoteNode:      "beam@127.0.0.1",
		NegotiatedFlags: flags,
	})

	from := etf.PID{Node: "beam@127.0.0.1", ID: 1, Creation: 4}
	to := etf.PID{Node: "tao@127.0.0.1", ID: 2, Creation: 4}

	// 1. Pre-handshake hopeful legacy EXIT (op 3) is accepted BEFORE first PAYLOAD_EXIT (24..28)
	f1, err := encLegacy.EncodeFrames(ControlMessage{Op: OpExit, FromPID: from, HasFrom: true, ToPID: to, Reason: etf.Atom("early")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, msg, err := rd.ProcessFrame((*f1[0])[4:], time.Now()); err != nil || msg.Control.Reason != etf.Atom("early") {
		t.Fatalf("expected early legacy EXIT to succeed, got msg=%+v err=%v", msg, err)
	}
	ReleaseBuffer(f1[0])

	//Note: encLegacy and encModern have separate send caches, so create fresh reader for modern sequence:
	rd2 := NewFrameReader(FrameReaderConfig{
		LocalNode:       "tao@127.0.0.1",
		RemoteNode:      "beam@127.0.0.1",
		NegotiatedFlags: flags,
	})
	f2, err := encModern.EncodeFrames(ControlMessage{Op: OpPayloadExit, FromPID: from, HasFrom: true, ToPID: to, Reason: etf.Atom("modern")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, msg, err := rd2.ProcessFrame((*f2[0])[4:], time.Now()); err != nil || msg.Control.Reason != etf.Atom("modern") {
		t.Fatalf("modern PAYLOAD_EXIT failed: %v", err)
	}
	ReleaseBuffer(f2[0])

	// Now that PAYLOAD_EXIT has been seen on rd2, a fallback to legacy EXIT (op 3) MUST fail!
	var legacyRaw []byte
	legacyRaw = append(legacyRaw, etf.Version, DistHeaderNormal, 0)
	legacyTuple := etf.Tuple{int64(OpExit), from, to, etf.Atom("regression")}
	legacyRaw, _ = etf.AppendDistTerm(legacyRaw, legacyTuple, nil, false, etf.DefaultLimits())
	if _, _, err := rd2.ProcessFrame(legacyRaw, time.Now()); !errors.Is(err, ErrInvalidControl) {
		t.Fatalf("expected ErrInvalidControl on regression to legacy EXIT, got %v", err)
	}
}

func TestHandshakeTCPAndTLSFramingBufferedBytesAndAuth(t *testing.T) {
	for _, pktSize := range []int{PacketSizeTCPHandshake, PacketSizeTLSHandshake} {
		c1, c2 := net.Pipe()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

		errCh := make(chan error, 1)
		var srvRes *HandshakeResult
		committed := false
		go func() {
			var err error
			srvRes, err = HandshakeServer(ctx, c2, ServerHandshakeConfig{
				LocalName:     "server@127.0.0.1",
				LocalCreation: 4,
				LocalFlags:    DefaultFlags(false),
				PacketSize:    pktSize,
				CookiesFor:    func(string) (string, string) { return "secret", "secret" },
				Arbitrate:     func(string, uint32, uint64) InboundDecision { return DecisionAlive },
				CommitAuthenticated: func(_ string, _ uint32, _ uint64, wasAlive bool) error {
					if !wasAlive {
						return errors.New("expected wasAlive=true")
					}
					committed = true
					return nil
				},
			})
			errCh <- err
		}()

		cliRes, err := HandshakeClient(ctx, c1, ClientHandshakeConfig{
			LocalName:        "client@127.0.0.1",
			LocalCreation:    5,
			LocalFlags:       DefaultFlags(false),
			ExpectedPeerName: "server@127.0.0.1",
			OutCookie:        "secret",
			PacketSize:       pktSize,
			ConfirmAlive:     func() bool { return true },
		})
		if err != nil {
			cancel()
			t.Fatalf("pktSize=%d client handshake: %v", pktSize, err)
		}
		if srvErr := <-errCh; srvErr != nil {
			cancel()
			t.Fatalf("pktSize=%d server handshake: %v", pktSize, srvErr)
		}
		if !committed {
			t.Fatalf("pktSize=%d expected CommitAuthenticated to run", pktSize)
		}
		go func() {
			_, _ = cliRes.Conn.Write([]byte{0, 0, 0, 0})
		}()
		rd := NewFrameReader(FrameReaderConfig{
			LocalNode:       "server@127.0.0.1",
			RemoteNode:      "client@127.0.0.1",
			NegotiatedFlags: srvRes.NegotiatedFlags,
		})
		kind, _, err := rd.ReadPacket(srvRes.Conn)
		if err != nil || kind != PacketTick {
			t.Fatalf("pktSize=%d expected preserved tick packet, got kind=%v err=%v", pktSize, kind, err)
		}
		_ = cliRes.Conn.Close()
		_ = srvRes.Conn.Close()
		cancel()
	}

	// Verify unauthenticated "alive" client with WRONG cookie NEVER triggers CommitAuthenticated!
	c1, c2 := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	unauthCommitted := false
	srvErrCh := make(chan error, 1)
	go func() {
		_, err := HandshakeServer(ctx, c2, ServerHandshakeConfig{
			LocalName:     "server@127.0.0.1",
			LocalCreation: 4,
			LocalFlags:    DefaultFlags(false),
			PacketSize:    PacketSizeTCPHandshake,
			CookiesFor:    func(string) (string, string) { return "real-cookie", "real-cookie" },
			Arbitrate:     func(string, uint32, uint64) InboundDecision { return DecisionAlive },
			CommitAuthenticated: func(string, uint32, uint64, bool) error {
				unauthCommitted = true
				return nil
			},
		})
		_ = c2.Close()
		srvErrCh <- err
	}()
	_, cliErr := HandshakeClient(ctx, c1, ClientHandshakeConfig{
		LocalName:        "attacker@127.0.0.1",
		LocalCreation:    6,
		LocalFlags:       DefaultFlags(false),
		ExpectedPeerName: "server@127.0.0.1",
		OutCookie:        "wrong-cookie",
		PacketSize:       PacketSizeTCPHandshake,
		ConfirmAlive:     func() bool { return true },
	})
	_ = c1.Close()
	srvErr := <-srvErrCh
	if !errors.Is(srvErr, ErrCookieMismatch) || cliErr == nil || unauthCommitted {
		t.Fatalf("unauthenticated alive must fail before CommitAuthenticated: srvErr=%v cliErr=%v committed=%v", srvErr, cliErr, unauthCommitted)
	}
}
