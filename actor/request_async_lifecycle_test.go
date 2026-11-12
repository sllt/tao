package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAsyncRequestStopAndRestartDiscardOldResultsAndReleaseQuota(t *testing.T) {
	for _, mode := range []string{"stop-running", "stop-queued", "restart"} {
		t.Run(mode, func(t *testing.T) {
			n := testNode(t, NodeOptions{MaxAsyncRequests: 1})
			held := make(chan asyncHeldCall, 2)
			callee := spawn(t, n, FromFunc(func(a *Actor, message any) error {
				if message == "defer" {
					held <- asyncHeldCall{"defer", a.Defer(), a.Context()}
				}
				return nil
			}))
			started := make(chan PID, 3)
			admissions := make(chan asyncAdmission, 2)
			results := make(chan asyncResultEvent, 2)
			holdEntered, holdRelease := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(holdRelease) })
			defer release()
			cancelled := make(chan bool, 1)
			behavior := FromFunc(func(a *Actor, message any) error {
				switch message := message.(type) {
				case Started:
					started <- a.PID()
				case string:
					switch message {
					case "start":
						id, err := a.Request(context.Background(), callee, "defer")
						admissions <- asyncAdmission{id, err}
					case "hold":
						close(holdEntered)
						<-holdRelease
					case "crash":
						return errors.New("restart async caller")
					}
				case RequestID:
					cancelled <- a.CancelRequest(message)
				case CallResult:
					results <- asyncResultEvent{a.PID(), message}
				}
				return nil
			})
			old := spawn(t, n, behavior, WithName("async-source"), WithRestart(Permanent))
			if first := <-started; first != old {
				t.Fatal("unexpected initial caller identity")
			}
			oldProcess, _ := n.resolvePID(old)
			if err := n.Send(old, "start"); err != nil {
				t.Fatal(err)
			}
			oldAdmission := nextAsyncAdmission(t, admissions)
			if oldAdmission.Err != nil {
				t.Fatal(oldAdmission.Err)
			}
			oldTicket := nextAsyncHeld(t, held)
			if mode == "stop-queued" {
				if err := n.Send(old, "hold"); err != nil {
					t.Fatal(err)
				}
				signal(t, holdEntered)
				if err := oldTicket.Reply.Reply(99); err != nil {
					t.Fatal(err)
				}
				waitAsyncQueued(t, n, old, oldAdmission.ID)
				stopped := make(chan error, 1)
				go func() { stopped <- n.StopActor(t.Context(), old) }()
				eventually(t, func() bool {
					oldProcess.mu.Lock()
					defer oldProcess.mu.Unlock()
					return oldProcess.stopRequested && len(oldProcess.asyncRequests) == 0
				})
				if n.Stats().AsyncRequests != 0 {
					t.Fatal("Stop retained a completed result while its handler was blocked")
				}
				release()
				nextResult(t, stopped)
			} else if mode == "restart" {
				if err := n.Send(old, "crash"); err != nil {
					t.Fatal(err)
				}
			} else if err := n.StopActor(t.Context(), old); err != nil {
				t.Fatal(err)
			}
			var fresh PID
			if mode == "restart" {
				select {
				case fresh = <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("async caller did not restart")
				}
				readyPID(t, n, fresh)
			} else {
				fresh = spawn(t, n, behavior, WithName("new-source"), WithRestart(Temporary))
				if got := <-started; got != fresh {
					t.Fatal("replacement caller bound the old identity")
				}
			}
			if fresh == old {
				t.Fatal("caller incarnation reused its old PID")
			}
			eventually(t, func() bool {
				oldProcess.mu.Lock()
				empty := len(oldProcess.asyncRequests) == 0
				oldProcess.mu.Unlock()
				return empty && n.Stats().AsyncRequests == 0 && n.Stats().PendingCalls == 0
			})
			if mode != "stop-queued" {
				select {
				case <-oldTicket.Context.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("Stop did not cancel the old worker's callee context")
				}
				if err := oldTicket.Reply.ReplyChecked(99); !errors.Is(err, ErrReplyExpired) {
					t.Fatalf("old incarnation still accepted a reply: %v", err)
				}
			}
			select {
			case event := <-results:
				t.Fatalf("old result reached a stopped or restarted caller: %+v", event)
			default:
			}
			if err := n.Send(fresh, oldAdmission.ID); err != nil {
				t.Fatal(err)
			}
			if nextAsyncBool(t, cancelled) {
				t.Fatal("new incarnation cancelled an old RequestID")
			}
			if err := n.Send(fresh, "start"); err != nil {
				t.Fatal(err)
			}
			admission := nextAsyncAdmission(t, admissions)
			if admission.Err != nil || admission.ID.Caller != fresh || admission.ID.ID == oldAdmission.ID.ID {
				t.Fatalf("old operation did not release bounded admission: %+v", admission)
			}
			ticket := nextAsyncHeld(t, held)
			if err := ticket.Reply.Reply(7); err != nil {
				t.Fatal(err)
			}
			if result := nextAsyncResult(t, results); result.Caller != fresh || result.Result.ID != admission.ID || result.Result.Value != 7 || result.Result.Err != nil {
				t.Fatalf("fresh incarnation result: %+v", result)
			}
			eventually(t, func() bool { return n.Stats().AsyncRequests == 0 && n.Stats().PendingCalls == 0 })
		})
	}
}

type asyncLifecycleHold struct {
	Entered chan struct{}
	Release chan struct{}
	Probe   bool
}

func TestAsyncRequestBlocksReplaceAndHandoffUntilResultConsumed(t *testing.T) {
	for _, operation := range []string{"replace", "handoff"} {
		t.Run(operation, func(t *testing.T) {
			directory := NewMemoryDirectory()
			var n *Node
			if operation == "handoff" {
				n = globalNetworkNode(t, "async-source", directory)
				target := globalNetworkNode(t, "async-target", directory, KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, nil)})
				connectNodes(t, n, target)
			} else {
				n = testNode(t, NodeOptions{Directory: directory})
			}
			held := make(chan asyncHeldCall, 1)
			callee := spawn(t, n, FromFunc(func(a *Actor, message any) error {
				if message == "defer" {
					held <- asyncHeldCall{"defer", a.Defer(), a.Context()}
				}
				return nil
			}))
			admissions := make(chan asyncAdmission, 2)
			results := make(chan asyncResultEvent, 1)
			hook := func(a *testBehavior, message any) error {
				switch message := message.(type) {
				case string:
					if message == "start" {
						id, err := a.Request(context.Background(), callee, "defer")
						admissions <- asyncAdmission{id, err}
					}
				case asyncLifecycleHold:
					close(message.Entered)
					<-message.Release
					if message.Probe {
						id, err := a.Request(context.Background(), callee, "defer")
						admissions <- asyncAdmission{id, err}
					}
				case CallResult:
					results <- asyncResultEvent{a.PID(), message}
				}
				return nil
			}
			pid := spawn(t, n, migratingCounter(10, hook), WithName("async-subject"), WithKind("counter", "1"), WithHandoff(true))
			p, _ := n.resolvePID(pid)
			change := func() error {
				if operation == "replace" {
					return n.Replace(t.Context(), pid, KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, nil)})
				}
				_, err := n.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "async-target", Version: "2"})
				return err
			}
			if err := n.Send(pid, "start"); err != nil {
				t.Fatal(err)
			}
			admission := nextAsyncAdmission(t, admissions)
			if admission.Err != nil {
				t.Fatal(admission.Err)
			}
			ticket := nextAsyncHeld(t, held)
			if err := change(); !errors.Is(err, ErrAsyncPending) {
				t.Fatalf("running Request did not reject %s: %v", operation, err)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			defer release()
			if err := n.Send(pid, asyncLifecycleHold{entered, resume, false}); err != nil {
				t.Fatal(err)
			}
			signal(t, entered)
			if err := ticket.Reply.Reply(3); err != nil {
				t.Fatal(err)
			}
			waitAsyncQueued(t, n, pid, admission.ID)
			if err := change(); !errors.Is(err, ErrAsyncPending) {
				t.Fatalf("queued CallResult did not reject %s: %v", operation, err)
			}
			release()
			if result := nextAsyncResult(t, results); result.Result.ID != admission.ID || result.Result.Value != 3 || result.Result.Err != nil {
				t.Fatalf("pre-migration result: %+v", result)
			}
			enteredFreeze, resumeFreeze := make(chan struct{}), make(chan struct{})
			releaseFreeze := sync.OnceFunc(func() { close(resumeFreeze) })
			defer releaseFreeze()
			if err := n.Send(pid, asyncLifecycleHold{enteredFreeze, resumeFreeze, true}); err != nil {
				t.Fatal(err)
			}
			signal(t, enteredFreeze)
			changed := make(chan error, 1)
			go func() { changed <- change() }()
			eventually(t, func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.change != nil || p.handoff != nil
			})
			releaseFreeze()
			if rejected := nextAsyncAdmission(t, admissions); !errors.Is(rejected.Err, ErrMigrating) || rejected.ID != (RequestID{}) {
				t.Fatalf("freeze admitted a new async operation: %+v", rejected)
			}
			nextResult(t, changed)
			if value, err := n.Call[int64](t.Context(), Ref{Path: "async-subject"}, remoteCommand{Kind: "get"}); err != nil || value != 10 {
				t.Fatalf("%s did not continue after async drain: %d, %v", operation, value, err)
			}
			if n.Stats().AsyncRequests != 0 {
				t.Fatal("migration retained or transferred the old async completion")
			}
		})
	}
}
