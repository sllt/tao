package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRestartReleasesSavedCallThatExpiredBeforeReplay(t *testing.T) {
	n := networkNode(t, "restart-release")
	parentEntered, parentRelease := make(chan struct{}), make(chan struct{})
	childEntered, childRelease := make(chan struct{}), make(chan struct{})
	unblockParent := sync.OnceFunc(func() { close(parentRelease) })
	unblockChild := sync.OnceFunc(func() { close(childRelease) })
	defer unblockParent()
	defer unblockChild()
	parentPID := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "pause" {
			close(parentEntered)
			<-parentRelease
		}
		return nil
	}), WithName("parent"))
	parent, _, err := n.resolve(parentPID)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := n.spawn(parent, t.Context(), factory(func(_ *testBehavior, message any) error {
		if message == "crash" {
			close(childEntered)
			<-childRelease
			return errors.New("crash")
		}
		if message == "expired" {
			t.Error("expired saved Call executed")
		}
		return nil
	}), WithName("child"), WithRestartMailbox(PreserveRef))
	if err != nil {
		t.Fatal(err)
	}
	child, _, err := n.resolve(childPID)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Send(parentPID, "pause"); err != nil {
		t.Fatal(err)
	}
	signal(t, parentEntered)
	if err := n.Send(childPID, "crash"); err != nil {
		t.Fatal(err)
	}
	signal(t, childEntered)
	r, err := n.network.coreRequest(t.Context(), childPID)
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	if err := child.enqueue(envelope{message: "expired", request: r, toRef: true, release: func() {
		spawned := make(chan error, 1)
		go func() {
			_ = n.Stats()
			_, err := n.spawn(parent, t.Context(), factory(func(*testBehavior, any) error { return nil }), WithRestart(Temporary))
			spawned <- err
		}()
		select {
		case err := <-spawned:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("release callback could not reenter registry/parent locks")
		}
		close(released)
	}}); err != nil {
		t.Fatal(err)
	}
	unblockChild()
	eventually(t, func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.routes[child.path].err == ErrRestarting && len(child.spec.saved) == 1
	})
	n.expireRequest(r, context.Canceled)
	unblockParent()
	signal(t, released)
}
