package taonet

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	native "github.com/sllt/tao/taonet/internal/mux"
)

func TestChannelNegotiatesLegacyRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	accepted := make(chan *Channel, 1)
	m := NewMux()
	if err := m.HandleChannel("legacy", func(ctx context.Context, c *Channel) error {
		accepted <- c
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	a, err := localConfig("client", nil).normalized()
	if err != nil {
		t.Fatal(err)
	}
	b, err := localConfig("legacy", m).normalized()
	if err != nil {
		t.Fatal(err)
	}
	x, y := net.Pipe()
	t.Cleanup(func() { _ = x.Close(); _ = y.Close() })
	type result struct {
		s   *Session
		err error
	}
	serverResult := make(chan result, 1)
	go func() {
		raw, err := native.New(ctx, y, native.Acceptor, native.Config{})
		if err != nil {
			serverResult <- result{err: err}
			return
		}
		control, err := raw.AcceptControl(ctx)
		var remote hello
		if err == nil {
			err = readJSON(control, recordHello, &remote)
		}
		// An older peer advertises only the original Channel feature.
		features := []string{"request", "notify", "stream", "channel"}
		welcome := hello{Major: 1, Peer: b.Local, Session: ID{1}, Features: features, Required: []string{"request"}, Limits: b.Limits}
		if err == nil {
			err = writeJSON(control, recordWelcome, welcome)
		}
		if err == nil {
			var confirmation ready
			err = readJSON(control, recordReady, &confirmation)
		}
		if err == nil {
			raw.MarkReady()
			err = writeJSON(control, recordReadyAck, ready{1, welcome.Session})
		}
		if err != nil {
			_ = raw.Close()
			serverResult <- result{err: err}
			return
		}
		info := SessionInfo{ID: welcome.Session, Local: b.Local, Remote: remote.Peer, Features: features}
		serverResult <- result{s: makeSession(raw, control, Acceptor, b, info, remote.Limits)}
	}()
	raw, err := native.New(ctx, x, native.Initiator, native.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	control, info, limits, err := negotiate(ctx, raw, Initiator, a, AuthInfo{Local: true})
	if err != nil {
		t.Fatal(err)
	}
	resultValue := <-serverResult
	if resultValue.err != nil {
		t.Fatal(resultValue.err)
	}
	client, server := makeSession(raw, control, Initiator, a, info, limits), resultValue.s
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	server.start()
	client.start()
	if slices.Contains(client.Info().Features, channelInlineFeature) {
		t.Fatal("enabled an unadvertised feature")
	}
	channel, err := client.OpenChannel(ctx, "legacy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channel.SendOwnedUnconfirmed(ctx, Message{Body: []byte("unsupported")}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("legacy unconfirmed send: %v", err)
	}
	peer := <-accepted
	if channel.inline || peer.inline {
		t.Fatal("legacy channel selected inline records")
	}
	for _, direction := range [][2]*Channel{{channel, peer}, {peer, channel}} {
		receipt, err := direction[0].SendWithReceipt(ctx, Message{Body: []byte("legacy body")})
		if err != nil {
			t.Fatal(err)
		}
		delivery := receive(t, direction[1])
		if !bytes.Equal(delivery.Message().Body, []byte("legacy body")) {
			t.Fatal("legacy body changed")
		}
		if err := delivery.Accept(); err != nil {
			t.Fatal(err)
		}
		delivery.Release()
		if err := receipt.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Receiving the new record without negotiation must fail just this channel.
	// The peer can reject/reset before this deliberately invalid write returns.
	// Its protocol error below is the decisive result, not local I/O success.
	_ = writeRecord(channel.raw, recordInline, 2, []byte("unnegotiated"))
	waitSignal(t, peer.Done())
	if !errors.Is(peer.Err(), ErrProtocol) {
		t.Fatalf("unnegotiated inline: %v", peer.Err())
	}
}

func TestBinaryAcknowledgmentRejectsMalformedLengthsAndText(t *testing.T) {
	for _, body := range [][]byte{{0}, {0, 9, 'x'}, {0, 1, 0xff}} {
		if _, err := readBinaryAck(bytes.NewReader(body), len(body)); !errors.Is(err, ErrProtocol) {
			t.Fatalf("malformed ACK %x: %v", body, err)
		}
	}
}
