package actor

import (
	"errors"
	"strconv"
	"testing"
)

func TestMailboxWrapGrowthAndExactLimit(t *testing.T) {
	for _, limit := range []int{1, 3, 5, 17, 1024} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			m := mailbox{limit: limit}
			added, removed := 0, 0
			push := func() {
				if err := m.push(envelope{message: added}); err != nil {
					t.Fatal(err)
				}
				added++
			}
			pop := func() {
				if got := m.pop().message; got != removed {
					t.Fatalf("FIFO: got %v want %d", got, removed)
				}
				removed++
			}
			// Grow after wrapping, including the last growth to a non-power
			// of two. Admission must keep the exact configured bound.
			for target := 1; target <= limit; target++ {
				for m.size < target {
					push()
				}
				pop()
				push()
			}
			if err := m.push(envelope{message: "excess"}); !errors.Is(err, ErrMailboxFull) {
				t.Fatalf("capacity: %v", err)
			}
			for m.size > 0 {
				pop()
			}
			for _, slot := range m.buffer {
				if slot.message != nil {
					t.Fatal("drained mailbox retained a message")
				}
			}
		})
	}
}

func TestMailboxMixedMetadataSurvivesWrapAndGrowth(t *testing.T) {
	m := mailbox{limit: 5}
	owner := &request{}
	route := &RouteRecord{}
	sender := PID{Node: "sender", ID: 1}
	released := 0
	for i := range 2 {
		if err := m.push(envelope{message: i}); err != nil {
			t.Fatal(err)
		}
	}
	if m.metadata != nil {
		t.Fatal("ordinary sends allocated control storage")
	}
	_ = m.pop()
	for i := 2; i < 6; i++ {
		env := envelope{message: i}
		if i%2 == 0 {
			env.sender, env.request, env.global, env.toRef = sender, owner, route, true
			env.release = func() { released++ }
		}
		if err := m.push(env); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < 6; i++ {
		peek := m.at(0)
		env := m.pop()
		if peek.message != i || env.message != i {
			t.Fatalf("FIFO %d: peek=%v pop=%v", i, peek.message, env.message)
		}
		if i%2 == 0 {
			if env.request != owner || env.global != route || env.sender != sender || !env.toRef || env.release == nil {
				t.Fatal("wrapped metadata lost its request/lease/route")
			}
			env.release()
		} else if env.request != nil || env.global != nil || env.sender != nil || env.toRef || env.release != nil {
			t.Fatal("ordinary message inherited another message's metadata")
		}
	}
	if released != 2 {
		t.Fatalf("released %d leases", released)
	}
	for _, meta := range m.metadata {
		if meta.request != nil || meta.global != nil || meta.release != nil || meta.toRef {
			t.Fatal("drained metadata retained request/lease/route")
		}
	}
}
