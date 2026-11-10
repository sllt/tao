package actor

import (
	"context"
	"testing"

	"github.com/sllt/tao/taonet"
)

func TestFailedPeerPipeDoesNotRetainIdleWriter(t *testing.T) {
	n := networkNode(t, "caller")
	_, err := n.Call[int64](t.Context(), PID{Node: "missing", BootID: [16]byte{1}, ID: 1}, int64(1))
	if err == nil {
		t.Fatal("unconfigured peer unexpectedly accepted a Call")
	}
	eventually(t, func() bool {
		n.network.mu.Lock()
		defer n.network.mu.Unlock()
		return len(n.network.pipes) == 0
	})
}

func TestDisconnectedIdlePipeRetiresWithoutAnotherSend(t *testing.T) {
	caller, server := networkNode(t, "caller"), networkNode(t, "server")
	connectNodes(t, caller, server)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if value, ok := message.(int64); ok {
			return a.Reply(value)
		}
		return nil
	}))
	if _, err := caller.Call[int64](t.Context(), pid, int64(1)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return caller.NetworkStats().QueuedMessages == 0 })
	if err := server.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		caller.network.mu.Lock()
		defer caller.network.mu.Unlock()
		return caller.network.pipes[pipeKey{peer: "server", class: taonet.ClassInteractive}] == nil
	})
}

func TestDisconnectedIdleReplyPipeRetiresWithoutAnotherReply(t *testing.T) {
	caller, server := networkNode(t, "caller"), networkNode(t, "server")
	connectNodes(t, caller, server)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if value, ok := message.(int64); ok {
			return a.Reply(value)
		}
		return nil
	}))
	if _, err := caller.Call[int64](t.Context(), pid, int64(1)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return server.NetworkStats().QueuedMessages == 0 })
	if err := caller.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		server.network.mu.Lock()
		defer server.network.mu.Unlock()
		return server.network.pipes[pipeKey{peer: "caller", class: taonet.ClassReply}] == nil
	})
}
