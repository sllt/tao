package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestIncomingReplyBackpressureReleasesOnCapacityCancelAndStop(t *testing.T) {
	for _, mode := range []string{"capacity", "deferred", "cancel", "deadline", "actor-stop", "node-stop"} {
		t.Run(mode, func(t *testing.T) {
			server := networkNode(t, "server")
			caller := networkNode(t, "caller")
			connectNodes(t, caller, server)
			replied := make(chan error, 1)
			pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
				if _, ok := message.(int64); !ok {
					return nil
				}
				if mode == "deferred" {
					reply := a.Defer()
					go func() { replied <- reply.Reply(int64(42)) }()
				} else {
					replied <- a.Reply(int64(42))
				}
				return nil
			}))
			// Leave room for one bounded error packet, but no payload encoder.
			charge := server.network.opts.SendBytes - 4096
			if err := server.network.reserve(charge); err != nil {
				t.Fatal(err)
			}
			release := sync.OnceFunc(func() { server.network.release(charge) })
			defer release()
			ctx, cancel := context.WithCancel(t.Context())
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 500*time.Millisecond)
			}
			defer cancel()
			called := make(chan error, 1)
			go func() {
				value, err := caller.Call[int64](ctx, pid, int64(1))
				if err == nil && value != 42 {
					err = errors.New("incorrect reply")
				}
				called <- err
			}()
			eventually(t, func() bool {
				server.network.mu.Lock()
				defer server.network.mu.Unlock()
				return server.network.sendChanged != nil && len(server.network.incoming) == 1
			})
			select {
			case err := <-replied:
				t.Fatalf("reply did not wait for admission: %v", err)
			default:
			}
			switch mode {
			case "capacity", "deferred":
				release()
			case "cancel":
				cancel()
			case "actor-stop", "node-stop":
				stopCtx, stopCancel := context.WithTimeout(t.Context(), time.Second)
				defer stopCancel()
				var err error
				if mode == "actor-stop" {
					err = server.StopActor(stopCtx, pid)
				} else {
					err = server.Stop(stopCtx)
				}
				if err != nil {
					t.Fatalf("shutdown blocked behind reply admission: %v", err)
				}
			}
			select {
			case err := <-replied:
				if err != nil {
					t.Fatalf("business completion was lost: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("reply admission remained blocked")
			}
			select {
			case err := <-called:
				if mode == "capacity" || mode == "deferred" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("interrupted reply unexpectedly succeeded")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("caller did not receive cancellation/failure")
			}
			release()
			eventually(t, func() bool {
				stats := server.NetworkStats()
				return stats.IncomingCalls == 0 && stats.QueuedBytes == 0 && stats.QueuedMessages == 0
			})
		})
	}
}

func TestSmallErrorReplyDoesNotReserveMaximumPayload(t *testing.T) {
	server := networkNode(t, "server")
	caller := networkNode(t, "caller")
	connectNodes(t, caller, server)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(int64); ok {
			return a.ReplyError(ErrCapacity)
		}
		return nil
	}))
	charge := server.network.opts.SendBytes - 4096
	if err := server.network.reserve(charge); err != nil {
		t.Fatal(err)
	}
	defer server.network.release(charge)
	_, err := caller.Call[int64](t.Context(), pid, int64(1))
	remoteError(t, err, ErrCapacity, Responded)
}

func TestWaitingDeferredReplyStillOccupiesIncomingCapacity(t *testing.T) {
	server, err := StartNode("server@127.0.0.1:0", NodeOptions{
		MaxPendingCalls: 1,
		Network:         NetworkOptions{AllowInsecureLoopback: true},
		Register:        []MessageType{{"count", 1, int64(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	caller := networkNode(t, "caller")
	connectNodes(t, caller, server)
	replied := make(chan error, 1)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(int64); ok {
			reply := a.Defer()
			go func() { replied <- reply.Reply(int64(42)) }()
		}
		return nil
	}))
	charge := server.network.opts.SendBytes - 4096
	if err := server.network.reserve(charge); err != nil {
		t.Fatal(err)
	}
	release := sync.OnceFunc(func() { server.network.release(charge) })
	defer release()
	first := make(chan error, 1)
	go func() {
		_, err := caller.Call[int64](t.Context(), pid, int64(1))
		first <- err
	}()
	eventually(t, func() bool {
		server.network.mu.Lock()
		defer server.network.mu.Unlock()
		return server.network.sendChanged != nil && len(server.network.incoming) == 1
	})
	if server.Stats().PendingCalls != 0 {
		t.Fatal("business request should already be completed")
	}
	_, err = caller.Call[int64](t.Context(), pid, int64(2))
	remoteError(t, err, ErrCapacity, Rejected)
	release()
	if err := <-replied; err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestIncomingReplyIndexRetiresOnSuccessAndEncodingFailure(t *testing.T) {
	server, caller := networkNode(t, "server"), networkNode(t, "caller")
	connectNodes(t, caller, server)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		switch message {
		case int64(1):
			return a.Reply(int64(42))
		case int64(2):
			return a.Reply(nil)
		case int64(3):
			return a.ReplyError(ErrCapacity)
		case int64(4):
			return a.Reply(struct{ Unregistered bool }{true})
		}
		return nil
	}))
	for range 10 {
		for value := int64(1); value <= 4; value++ {
			result, err := caller.Call[any](t.Context(), pid, value)
			switch value {
			case 1:
				if err != nil || result != int64(42) {
					t.Fatalf("value reply: %v %v", result, err)
				}
			case 2:
				if err != nil || result != nil {
					t.Fatalf("nil reply: %v %v", result, err)
				}
			case 3:
				remoteError(t, err, ErrCapacity, Responded)
			case 4:
				remoteError(t, err, ErrUnregisteredType, Responded)
			}
			eventually(t, func() bool {
				stats := server.NetworkStats()
				return stats.IncomingCalls == 0 && stats.QueuedMessages == 0 && stats.QueuedBytes == 0
			})
		}
	}
}
