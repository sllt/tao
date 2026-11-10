package actor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestCompletionHookHandlesLateRegistrationAndRaces(t *testing.T) {
	n := networkNode(t, "completion")
	owner := PID{Node: "remote", ID: 1}
	for _, incoming := range []bool{false, true} {
		for _, order := range []string{"before", "after", "race"} {
			for range 50 {
				var r *request
				if incoming {
					r, _ = scopeRequest(t, n, t.Context(), time.Second)
				} else {
					var err error
					r, err = n.network.coreRequest(t.Context(), owner)
					if err != nil {
						t.Fatal(err)
					}
				}
				var calls atomic.Int32
				registered := func() {
					fn := func(r *request) {
						_ = n.Stats() // Callback must run outside node.mu.
						if r.err != nil || r.value != "reply" {
							t.Errorf("completion value: %v %v", r.value, r.err)
						}
						calls.Add(1)
					}
					if r.bindCompletion(&requestCompletion{run: fn}) {
						fn(r)
					}
				}
				complete := func() {
					if err := n.reply(r, r.owner, "reply", nil, false); err != nil {
						t.Error(err)
					}
				}
				switch order {
				case "before":
					registered()
					complete()
				case "after":
					complete()
					registered()
				case "race":
					var group sync.WaitGroup
					group.Go(registered)
					group.Go(complete)
					group.Wait()
				}
				n.expireRequest(r, context.Canceled)
				if calls.Load() != 1 || n.Stats().PendingCalls != 0 {
					t.Fatalf("%s: completion count %d", order, calls.Load())
				}
			}
		}
	}
}

func TestSharedCompletionHookCompletesEachRequestOnce(t *testing.T) {
	n := networkNode(t, "shared-completion")
	hook := &requestCompletion{run: func(r *request) {
		r.value.(*atomic.Int32).Add(1)
	}}
	var results [100]atomic.Int32
	var group sync.WaitGroup
	for i := range results {
		r, _ := scopeRequest(t, n, t.Context(), time.Second)
		group.Go(func() {
			if r.bindCompletion(hook) {
				hook.run(r)
			}
		})
		group.Go(func() {
			if err := n.reply(r, r.owner, &results[i], nil, false); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	for i := range results {
		if got := results[i].Load(); got != 1 {
			t.Fatalf("request %d completed %d times", i, got)
		}
	}
}

func TestIncomingCompletionWaiterRacesReply(t *testing.T) {
	n := networkNode(t, "lazy-completion")
	for range 100 {
		r, _ := scopeRequest(t, n, t.Context(), time.Second)
		var group sync.WaitGroup
		group.Go(func() { signal(t, r.waitDone()) })
		group.Go(func() {
			if err := n.reply(r, r.owner, 42, nil, false); err != nil {
				t.Error(err)
			}
		})
		group.Wait()
		if r.value != 42 || r.err != nil {
			t.Fatalf("waiter missed completion: %v %v", r.value, r.err)
		}
	}
	r, _ := scopeRequest(t, n, t.Context(), time.Second)
	if err := n.reply(r, r.owner, 42, nil, false); err != nil {
		t.Fatal(err)
	}
	if r.done != nil {
		t.Fatal("unobserved incoming completion allocated a channel")
	}
	signal(t, r.waitDone())
}

func TestRetirementCompletionMaySpawnSibling(t *testing.T) {
	n := networkNode(t, "completion")
	pid := spawn(t, n, factory(func(*testBehavior, any) error { return nil }), WithRestart(Temporary))
	p, _, err := n.resolve(pid)
	if err != nil {
		t.Fatal(err)
	}
	r, err := n.network.coreRequest(t.Context(), pid)
	if err != nil {
		t.Fatal(err)
	}
	spawned := make(chan error, 1)
	consumed := false
	r.bindCompletion(&requestCompletion{run: func(*request) {
		go func() {
			_, err := n.Spawn(factory(func(*testBehavior, any) error { return nil }), WithRestart(Temporary))
			spawned <- err
		}()
		select {
		case err := <-spawned:
			consumed = true
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("completion could not reenter the parent child registry")
		}
	}})
	p.retire(nil, nil)
	p.cancel(ErrStopped)
	close(p.done)
	if !consumed {
		if err := <-spawned; err != nil {
			t.Fatal(err)
		}
	}
}
