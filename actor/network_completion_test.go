package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sllt/tao/taonet"
)

func TestRemoteCancellationCannotReplaceCommittedReply(t *testing.T) {
	n := networkNode(t, "caller")
	owner := PID{Node: "peer", ID: 1}
	for _, failure := range []error{nil, &RemoteError{Code: "application_error", Outcome: Responded, Cause: ErrRemoteApplication}} {
		ctx, cancel := context.WithCancel(t.Context())
		r, err := n.network.coreRequest(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		out := &outgoingCall{request: r, peer: "peer", issued: true}
		if err := n.reply(r, owner, "committed", failure, false); err != nil {
			t.Fatal(err)
		}
		cancel()
		// Both r.done and ctx.Done are now ready. Exercise the cancellation
		// branch directly, rather than depending on select's random choice.
		got, err := n.network.cancelCall(ctx, out, requestKey{})
		if failure == nil {
			if err != nil || got != "committed" {
				t.Fatalf("completed reply replaced: %v, %v", got, err)
			}
		} else if !errors.Is(err, ErrRemoteApplication) {
			t.Fatalf("business rejection replaced: %v", err)
		}
		if out.canceled || r.expired {
			t.Fatal("completed reply marked canceled")
		}
	}
	if n.Stats().PendingCalls != 0 {
		t.Fatal("completed requests retained")
	}
}

func TestInboxResetEndsWrittenCallWhileControlConnectionLives(t *testing.T) {
	start := func(name string) *Node {
		n, err := StartNode(name+"@127.0.0.1:0", NodeOptions{Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"paused", 1, pausedMessage{}}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Stop(context.Background()) })
		return n
	}
	a, b := start("caller"), start("server")
	connectNodes(t, a, b)
	// Keep admission blocked so a business error reply cannot mask a missing
	// inbox watcher. Closing the inbox leaves the control connection alive.
	barrier := &decodeBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	pausedDecoder.Store(barrier)
	defer pausedDecoder.Store(nil)
	defer close(barrier.release)
	pid := spawn(t, b, factory(func(*testBehavior, any) error { return nil }))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := a.Call[any](ctx, pid, pausedMessage{}); result <- err }()
	signal(t, barrier.entered)
	eventually(t, func() bool { return a.NetworkStats().QueuedMessages == 0 })
	a.network.mu.Lock()
	channel := a.network.pipes[pipeKey{peer: "server", class: taonet.ClassInteractive}].channel
	a.network.mu.Unlock()
	if err := channel.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Outcome != Unknown || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("reset result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Call kept waiting for its ten-second budget after inbox reset")
	}
	var controlAlive bool
	for _, session := range a.network.endpoint.Sessions("server") {
		if session.Info().Class == taonet.ClassControl && session.Err() == nil {
			controlAlive = true
		}
	}
	if !controlAlive {
		t.Fatal("test closed the peer instead of only the inbox")
	}
}

func TestAcceptanceSettlementDoesNotCancelBusinessCall(t *testing.T) {
	for _, sendTimeout := range []time.Duration{time.Second, 5 * time.Second} {
		t.Run(sendTimeout.String(), func(t *testing.T) {
			a, err := StartNode("caller@127.0.0.1:0", NodeOptions{
				Network:  NetworkOptions{AllowInsecureLoopback: true, SendTimeout: sendTimeout},
				Register: []MessageType{{"command", 1, remoteCommand{}}, {"count", 1, int64(0)}},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Stop(context.Background()) })
			b := networkNode(t, "server")
			connectNodes(t, a, b)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			pid := spawn(t, b, factory(func(actor *testBehavior, msg any) error {
				if _, ok := msg.(remoteCommand); ok {
					close(entered)
					<-release
					if err := actor.Context().Err(); err != nil {
						t.Errorf("acceptance canceled business context: %v", err)
					}
					_ = actor.Reply(int64(42))
				}
				return nil
			}))
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				value, err := a.Call[int64](ctx, pid, remoteCommand{})
				if err == nil && value != 42 {
					err = errors.New("wrong business result")
				}
				result <- err
			}()
			signal(t, entered)
			eventually(t, func() bool { return a.NetworkStats().QueuedMessages == 0 })
			unblock()
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}
