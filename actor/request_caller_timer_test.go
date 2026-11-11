package actor

import (
	"context"
	"testing"
	"time"
)

func TestDefaultLocalReplyReleasesTimerBeforeHandlerReturns(t *testing.T) {
	for _, publicContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "lazy", true: "public-context"}[publicContext], func(t *testing.T) {
			n := testNode(t, NodeOptions{CallTimeout: time.Minute})
			proceed := make(chan struct{})
			defer close(proceed)
			scopes := make(chan *requestScope, 1)
			replied := make(chan struct{})
			pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
				if msg != "call" {
					return nil
				}
				scope := a.proc.current.request.ctx.(*requestScope)
				var ctx context.Context
				if publicContext {
					ctx = a.Context()
				}
				scopes <- scope
				if err := a.Reply(42); err != nil {
					t.Error(err)
				}
				if ctx != nil {
					select {
					case <-ctx.Done():
						if context.Cause(ctx) != context.Canceled {
							t.Errorf("reply cause = %v", context.Cause(ctx))
						}
					default:
						t.Error("Reply returned before canceling its public Context")
					}
				}
				close(replied)
				<-proceed
				return nil
			}))
			if value, err := n.Call[int](context.Background(), pid, "call"); err != nil || value != 42 {
				t.Fatalf("Call = %v, %v", value, err)
			}
			scope := <-scopes
			scope.mu.Lock()
			retained := scope.timer != nil
			scope.mu.Unlock()
			if retained {
				t.Error("completed caller retained its timer")
			}
			signal(t, replied)
		})
	}
}
