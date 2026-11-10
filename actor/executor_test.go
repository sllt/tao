package actor

import (
	"context"
	"sync"
	"testing"
)

func TestExecutorReusePreservesBlockingHandlersAndNestedCalls(t *testing.T) {
	t.Run("one-cache", func(t *testing.T) { testExecutorReuse(t, 2) })
	t.Run("sharded-cache", func(t *testing.T) { testExecutorReuse(t, 8) })
}

func testExecutorReuse(t *testing.T, maxIdle int) {
	n := testNode(t, NodeOptions{})
	n.executors.init(maxIdle, max(1, maxIdle/2))
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	for range cap(entered) {
		pid := spawn(t, n, factory(func(_ *testBehavior, msg any) error {
			if _, ok := msg.(int); ok {
				entered <- struct{}{}
				<-release
			}
			return nil
		}))
		if err := n.Send(pid, 1); err != nil {
			t.Fatal(err)
		}
	}
	for range cap(entered) {
		signal(t, entered)
	}
	callee := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if value, ok := msg.(int); ok {
			return a.Reply(value + 1)
		}
		return nil
	}))
	caller := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if value, ok := msg.(int); ok {
			result, err := a.Call(a.Context(), callee, value)
			if err != nil {
				return err
			}
			return a.Reply(result)
		}
		return nil
	}))
	for range 50 {
		if value, err := n.Call[int](t.Context(), caller, 41); err != nil || value != 42 {
			t.Fatalf("busy executors blocked a nested Call: %d %v", value, err)
		}
	}
	unblock()
	eventually(t, func() bool {
		idle := 0
		for i := range n.executors.shards {
			cache := &n.executors.shards[i]
			cache.mu.Lock()
			if len(cache.idle) > cache.maxIdle {
				t.Error("idle executor cache exceeded its bound")
			}
			idle += len(cache.idle)
			cache.mu.Unlock()
		}
		return idle > 0
	})
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := range n.executors.shards {
		cache := &n.executors.shards[i]
		cache.mu.Lock()
		if !cache.closed || len(cache.idle) != 0 {
			t.Error("stopped node retained idle executors")
		}
		cache.mu.Unlock()
	}
}
