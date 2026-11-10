package actor

import (
	"testing"

	"github.com/sllt/tao/taonet"
)

func TestClosedSessionCannotRestoreRetiredPeerIdentity(t *testing.T) {
	a, oldPeer := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, oldPeer)
	var oldControl *taonet.Session
	for _, session := range a.network.endpoint.Sessions("b") {
		if session.Info().Class == taonet.ClassControl {
			oldControl = session
			break
		}
	}
	if oldControl == nil {
		t.Fatal("missing old primary")
	}
	if err := oldPeer.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	signal(t, oldControl.Done())
	eventually(t, func() bool {
		a.network.mu.Lock()
		defer a.network.mu.Unlock()
		_, watched := a.network.watched[oldControl]
		return !watched
	})
	fresh := networkNode(t, "b")
	connectNodes(t, a, fresh)
	// A handler can retain a Session after endpoint cleanup. Re-observing
	// that closed object must not replace the newly connected boot identity.
	a.network.watch(oldControl)
	a.network.mu.Lock()
	current := a.network.currentBoot["b"]
	a.network.mu.Unlock()
	if current != fresh.BootID() {
		t.Fatalf("retired session restored old identity: got %x want %x", current, fresh.BootID())
	}
}
