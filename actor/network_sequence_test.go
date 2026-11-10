package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCancelFindsRegisteredCallBeforeMailboxAdmission(t *testing.T) {
	server, client := networkNode(t, "server"), networkNode(t, "client")
	connectNodes(t, client, server)
	var executions atomic.Int32
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if value, ok := message.(int64); ok {
			executions.Add(1)
			return a.Reply(value)
		}
		return nil
	}))
	p, _, err := server.resolve(pid)
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock() // stop after request registration, before mailbox admission
	unblock := sync.OnceFunc(p.mu.Unlock)
	defer unblock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := client.Call[int64](ctx, pid, int64(1))
		result <- err
	}()
	var incoming incomingCall
	eventually(t, func() bool {
		server.network.mu.Lock()
		defer server.network.mu.Unlock()
		for _, entry := range server.network.incoming {
			incoming = entry
			return true
		}
		return false
	})
	server.network.mu.Lock()
	progress := server.network.inboxProgress[incoming.scope]
	server.network.mu.Unlock()
	if progress != incoming.sequence {
		t.Fatal("cancellation watermark was not published with the incoming record")
	}
	cancel()
	remoteError(t, <-result, context.Canceled, Unknown)
	// The watermark already covers this sequence. The active record must still
	// win, so CANCEL cannot be mistaken for a late, already-settled operation.
	eventually(t, func() bool { return errors.Is(incoming.budget.Err(), context.Canceled) })
	unblock()
	eventually(t, func() bool {
		stats := server.NetworkStats()
		return stats.IncomingCalls == 0 && stats.CancellationRecords == 0
	})
	if executions.Load() != 0 {
		t.Fatal("canceled request reached Receive")
	}
	got, err := client.Call[int64](t.Context(), pid, int64(9))
	if err != nil || got != 9 || executions.Load() != 1 {
		t.Fatalf("next request lost sequence/credit: value=%d calls=%d err=%v", got, executions.Load(), err)
	}
}
