package erlang

import (
	"bytes"
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sllt/tao/erlang/etf"
	"github.com/sllt/tao/erlang/internal/dist"
)

func newTestPeerSession() *peerSession {
	n := &Node{
		limits: Limits{
			MaxPayloadBytes:  1 << 20,
			MaxPeerSendBytes: 1 << 20,
			MaxControlQueue:  64,
		},
		tickTime: time.Minute,
	}
	return &peerSession{
		node:       n,
		peerName:   "peer@host",
		gen:        1,
		wakeWriter: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
}

func TestOutboundAdmissionSnapshotsMutableTerms(t *testing.T) {
	s := newTestPeerSession()

	// 1. enqueueData (Send / Cast / SendReply)
	payload := etf.Binary("before")
	tup := etf.Tuple{payload, etf.Map{{Key: etf.Binary("k"), Value: etf.Binary("v")}}}
	wantData, err := etf.Encode(tup)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.enqueueData(dist.ControlMessage{Op: dist.OpSend}, tup); err != nil {
		t.Fatal(err)
	}
	copy(payload, "AFTER!")
	copy(tup[1].(etf.Map)[0].Key.(etf.Binary), "X")
	copy(tup[1].(etf.Map)[0].Value.(etf.Binary), "Y")
	tup[0] = etf.Atom("replaced")

	gotData, err := etf.Encode(s.queue[0].payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotData, wantData) {
		t.Fatalf("enqueueData retained caller mutable backing data:\ngot =%x\nwant=%x", gotData, wantData)
	}

	// 2. Endpoint.Terminate(reason)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv, err := Start(ctx, Config{
		Name:        "snap_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "snap-cookie",
		DisableEPMD: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	monSynced := make(chan struct{}, 1)
	ep, err := srv.Export(EndpointOptions{
		Name: "snap_ep",
		OnDeliver: func(_ *Endpoint, _ InboundDelivery) {
			select {
			case monSynced <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	cliNode, err := Start(ctx, Config{
		Name:        "snap_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "snap-cookie",
		DisableEPMD: true,
		StaticRoutes: map[string]Route{
			srv.Name(): {Addr: srv.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cliNode.Close()

	cli, err := cliNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	_, downCh, err := cli.Monitor(ctx, Process(ep.PID()))
	if err != nil {
		t.Fatal(err)
	}
	// Send on the same ordered stream so MONITOR_P is guaranteed installed before Terminate.
	if err := cli.Send(ctx, Process(ep.PID()), etf.Atom("sync")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-monSynced:
	case <-ctx.Done():
		t.Fatal("timed out syncing monitor installation")
	}

	reasonBin := etf.Binary("bye_clean")
	reasonTup := etf.Tuple{etf.Atom("custom"), reasonBin}
	ep.Terminate(reasonTup)
	copy(reasonBin, "CORRUPTED")
	reasonTup[0] = etf.Atom("tampered")

	select {
	case ev := <-downCh:
		wantReason := etf.Tuple{etf.Atom("custom"), etf.Binary("bye_clean")}
		if !etf.TermEqual(ev.Reason, wantReason) {
			t.Fatalf("DOWN reason = %#v, want %#v", ev.Reason, wantReason)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for DOWN")
	}
}

func TestLosingCancellationWaitsForPublishedResult(t *testing.T) {
	claimed := make(chan struct{})
	resume := make(chan struct{})
	pc := &pendingCall{
		done:          make(chan struct{}),
		bundle:        &callBundleState{},
		cancelTimeout: func() { close(claimed); <-resume },
	}
	pc.bundle.markCommitted()
	c := &Client{node: &Node{}, calls: map[Ref]*pendingCall{pc.ref: pc}}

	go pc.settle(etf.Atom("ok"), nil)
	<-claimed

	err := c.cancelAdmittedCall(pc, context.Canceled)
	if err != nil {
		t.Fatalf("losing cancelAdmittedCall returned error %v, want nil", err)
	}
	select {
	case <-pc.done:
	default:
		t.Fatal("cancelAdmittedCall returned nil before pc.done was closed")
	}
	if pc.reply != etf.Atom("ok") || pc.err != nil {
		t.Fatalf("result after losing cancel = (%#v, %v), want ('ok', nil)", pc.reply, pc.err)
	}
	close(resume)
}

func TestCallResultPublicationRace(t *testing.T) {
	for range 500 {
		pc := &pendingCall{done: make(chan struct{}), bundle: &callBundleState{}}
		pc.bundle.markCommitted()
		c := &Client{node: &Node{}, calls: map[Ref]*pendingCall{pc.ref: pc}}
		go pc.settle(etf.Atom("ok"), nil)
		for !pc.settled.Load() {
			runtime.Gosched()
		}
		if err := c.cancelAdmittedCall(pc, context.Canceled); err == nil {
			reply, callErr := pc.reply, pc.err
			if reply != etf.Atom("ok") || callErr != nil {
				t.Fatalf("losing cancel observed (%#v, %v), want ('ok', nil)", reply, callErr)
			}
		}
		<-pc.done
	}
}

type signalWriteConn struct {
	net.Conn
	writing chan struct{}
	once    sync.Once
}

func (c *signalWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	return c.Conn.Write(p)
}

func TestDisconnectReleasesWriterBatchAndQueuedBytes(t *testing.T) {
	s := newTestPeerSession()
	left, right := net.Pipe()
	defer right.Close()

	conn := &signalWriteConn{Conn: left, writing: make(chan struct{})}
	s.conn = dist.WrapConn(conn)
	ctrl := dist.ControlMessage{
		Op:    dist.OpSend,
		ToPID: etf.PID{Node: "peer@host", ID: 1, Creation: 4},
	}

	// First item will be popped by writeLoop and block inside net.Pipe Write.
	if err := s.enqueueData(ctrl, etf.Binary("in_flight_batch")); err != nil {
		t.Fatal(err)
	}
	s.wg.Add(1)
	go s.writeLoop()

	select {
	case <-conn.writing:
	case <-time.After(time.Second):
		t.Fatal("writer did not reach socket Write")
	}

	// Enqueue a second item that stays in s.queue while writeLoop is blocked in Write.
	if err := s.enqueueData(ctrl, etf.Binary("still_in_queue")); err != nil {
		t.Fatal(err)
	}

	s.closeWithError(context.Canceled)
	s.wg.Wait()

	if got := s.node.totalSendQueued.Load(); got != 0 {
		t.Fatalf("node totalSendQueued = %d after writer exit, want 0", got)
	}
	s.writeMu.Lock()
	sessionQueued := s.sendQueuedBytes
	s.writeMu.Unlock()
	if sessionQueued != 0 {
		t.Fatalf("session sendQueuedBytes = %d after writer exit, want 0", sessionQueued)
	}
}

func TestTrackInboundMonitorDuringCloseNoNilMapPanic(t *testing.T) {
	for range 200 {
		n, err := Start(context.Background(), Config{
			Name:        "inmon_close@127.0.0.1",
			Listen:      "127.0.0.1:0",
			Cookie:      "inmon-cookie",
			DisableEPMD: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		left, right := net.Pipe()
		s := &peerSession{
			node:       n,
			peerName:   "peer@host",
			gen:        1,
			conn:       dist.WrapConn(left),
			inMons:     make(map[inboundMonKey]inboundMonOwner),
			wakeWriter: make(chan struct{}, 1),
			done:       make(chan struct{}),
		}
		ep, err := n.Export(EndpointOptions{})
		if err != nil {
			_ = left.Close()
			_ = right.Close()
			_ = n.Close()
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.closeWithError(context.Canceled)
		}()
		go func() {
			defer wg.Done()
			key := inboundMonKey{
				peerGen: 1,
				watcher: PID{Node: "peer@host", ID: 1, Creation: 4},
			}
			_ = s.trackInboundMonitor(key, ep)
		}()
		wg.Wait()
		_ = right.Close()
		_ = n.Close()
	}
}
