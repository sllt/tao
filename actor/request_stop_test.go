package actor

import (
	"context"
	"errors"
	"testing"
)

func TestStopCancelsOwnedCallBeforeHandlerReturns(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "remote"
		}
		t.Run(name, func(t *testing.T) {
			server := networkNode(t, "server")
			caller := server
			if remote {
				caller = networkNode(t, "caller")
				connectNodes(t, caller, server)
			}
			entered, canceled := make(chan struct{}), make(chan struct{})
			pid := spawn(t, server, factory(func(a *testBehavior, msg any) error {
				if _, ok := msg.(remoteCommand); ok {
					close(entered)
					<-a.Context().Done()
					close(canceled)
				}
				return nil
			}))
			result := make(chan error, 1)
			go func() { _, err := caller.Call[any](t.Context(), pid, remoteCommand{}); result <- err }()
			signal(t, entered)
			if err := server.StopActor(t.Context(), pid); err != nil {
				t.Fatal(err)
			}
			signal(t, canceled)
			if err := <-result; !errors.Is(err, ErrStopped) {
				t.Fatalf("Call did not end with its owner: %v", err)
			}
			if err := caller.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
