package taonet

import (
	"bytes"
	"context"
	"net"
	"testing"
)

func TestLargeChannelVectorsRemainDuplexAcrossChunksAndTLS(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "tcp"
		if secure {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			accepted := make(chan *Channel, 1)
			m := NewMux()
			if err := m.HandleChannel("large", func(ctx context.Context, c *Channel) error {
				accepted <- c
				select {
				case <-ctx.Done():
				case <-c.Done():
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			a, b := localConfig("client", nil), localConfig("server", m)
			if secure {
				clientTLS, serverTLS := tlsConfigs(t)
				a.Transport = TransportConfig{TLS: clientTLS, ExpectedPeer: "server"}
				b.Transport = TransportConfig{TLS: serverTLS, ExpectedPeer: "client"}
			}
			client, _ := pair(t, a, b)
			left, err := client.OpenChannel(t.Context(), "large", nil)
			if err != nil {
				t.Fatal(err)
			}
			right := <-accepted
			for _, size := range []int{17 << 10, 64 << 10, (64 << 10) + 256, 256 << 10} {
				inputs := [][]byte{bytes.Repeat([]byte{1}, size), bytes.Repeat([]byte{2}, size)}
				var receipts [2]*SendReceipt
				for i, c := range []*Channel{left, right} {
					receipts[i], err = c.SendOwnedWithReceipt(t.Context(), Message{Body: inputs[i], Metadata: map[string]string{"label": name}})
					if err != nil {
						t.Fatal(err)
					}
				}
				for i, c := range []*Channel{right, left} {
					d := receive(t, c)
					waitSignal(t, receipts[i].Released())
					clear(inputs[i])
					msg := d.Message()
					if !bytes.Equal(msg.Body, bytes.Repeat([]byte{byte(i + 1)}, size)) || msg.Metadata["label"] != name {
						t.Fatalf("size %d: chunk or input ownership corrupted", size)
					}
					if err := d.Accept(); err != nil {
						t.Fatal(err)
					}
					d.Release()
					if err := receipts[i].Wait(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
			}
			closeChannels(t, left, right)
		})
	}
}

func TestLargeChannelResetKeepsInputUntilPhysicalWriteEnds(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	left, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	paused := &pausedStreamWrite{Conn: right, entered: make(chan struct{}), release: make(chan struct{})}
	unblock := func() { paused.unblock.Do(func() { close(paused.release) }) }
	defer unblock()
	incoming := make(chan *Channel, 1)
	m := NewMux()
	if err := m.HandleChannel("paused", func(ctx context.Context, c *Channel) error {
		incoming <- c
		select {
		case <-ctx.Done():
		case <-c.Done():
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	type result struct {
		session *Session
		err     error
	}
	accepted := make(chan result, 1)
	go func() {
		s, err := NewSession(t.Context(), paused, Acceptor, localConfig("server", m))
		accepted <- result{s, err}
	}()
	client, err := NewSession(t.Context(), left, Initiator, localConfig("client", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	if server.err != nil {
		t.Fatal(server.err)
	}
	defer func() { unblock(); _ = server.session.Close() }()
	peer, err := client.OpenChannel(t.Context(), "paused", nil)
	if err != nil {
		t.Fatal(err)
	}
	channel := <-incoming
	paused.stream = channel.raw.ID()
	paused.armed.Store(true)
	input := bytes.Repeat([]byte("x"), 128<<10)
	receipt, err := channel.SendOwnedUnconfirmed(t.Context(), Message{Body: input})
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, paused.entered)
	_ = peer.Close()
	waitSignal(t, receipt.Done())
	if err := receipt.Wait(t.Context()); err == nil {
		t.Fatal("reset was reported as a successful write")
	}
	select {
	case <-receipt.Released():
		t.Fatal("large input released while physical I/O still owns it")
	default:
	}
	unblock()
	waitSignal(t, receipt.Released())
	clear(input)
}
