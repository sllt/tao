package actor

import (
	"errors"
	"math/rand"
	"testing"
)

func TestSegmentedMailboxGrowthWrapAndDrain(t *testing.T) {
	const limit = 4105
	m := mailbox{limit: limit}
	added, removed := 0, 0
	push := func() {
		t.Helper()
		if err := m.push(envelope{message: added}); err != nil {
			t.Fatal(err)
		}
		added++
	}
	pop := func() {
		t.Helper()
		if value := m.pop().message; value != removed {
			t.Fatalf("got %v want %d", value, removed)
		}
		removed++
	}
	for round := range 3 {
		for m.size < limit {
			push()
		}
		if m.segments == nil || len(m.buffer) > mailboxSmallLimit {
			t.Fatal("backlog retained a growing message array")
		}
		if err := m.push(envelope{}); !errors.Is(err, ErrMailboxFull) {
			t.Fatal("lost exact capacity")
		}
		for range limit + round*271 {
			pop()
			push()
		}
		for index := range m.size {
			if value := m.at(index).message; value != removed+index {
				t.Fatalf("directory order at %d: %v", index, value)
			}
		}
		for m.size > 0 {
			pop()
		}
		if m.segments != nil {
			t.Fatal("empty mailbox retained the large directory")
		}
		for _, slot := range m.buffer {
			if slot.message != nil || slot.sender != nil {
				t.Fatal("small reserve retained a message")
			}
		}
	}
}

func TestSegmentedMailboxMixedBatchRestoreAndCredit(t *testing.T) {
	m := mailbox{limit: 1031}
	owner, route := &request{}, &RouteRecord{}
	sender := PID{Node: "sender", ID: 1}
	sequence := 0
	newMessage := func() envelope {
		id := sequence
		sequence++
		env := envelope{message: id, sender: sender}
		if id%97 == 0 {
			env.request, env.toRef = owner, true
		}
		if id%131 == 0 {
			env.global, env.release = route, func() {}
		}
		return env
	}
	check := func(got, want envelope) {
		t.Helper()
		if got.message != want.message || got.sender != want.sender || got.request != want.request || got.global != want.global || got.toRef != want.toRef || (got.release == nil) != (want.release == nil) {
			t.Fatalf("message/metadata mismatch: got %+v want %+v", got, want)
		}
	}
	var reference []envelope
	for range m.limit {
		env := newMessage()
		_ = m.push(env)
		reference = append(reference, env)
	}
	random := rand.New(rand.NewSource(1))
	var batch [16]mailboxMessage
	for range 10000 {
		switch random.Intn(3) {
		case 0:
			env := newMessage()
			err := m.push(env)
			if len(reference) == m.limit {
				if !errors.Is(err, ErrMailboxFull) {
					t.Fatal("full mailbox admitted a message")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				reference = append(reference, env)
			}
		case 1:
			if len(reference) > 0 {
				check(m.pop(), reference[0])
				reference = reference[1:]
			}
		case 2:
			n := m.takePure(batch[:])
			for i := range n {
				check(envelope{message: batch[i].message, sender: batch[i].sender}, reference[i])
			}
			claimed := n / 2
			reference = reference[claimed:]
			held := n - claimed
			for range 3 {
				env := newMessage()
				err := m.pushReserved(env, held)
				if len(reference) == m.limit {
					if !errors.Is(err, ErrMailboxFull) {
						t.Fatal("held suffix escaped capacity")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					reference = append(reference, env)
				}
			}
			m.prependPure(batch[claimed:n])
			clear(batch[:])
		}
		if m.size != len(reference) {
			t.Fatal("logical size mismatch")
		}
		if m.size > 0 {
			check(m.at(m.size-1), reference[len(reference)-1])
			check(m.at(0), reference[0])
		}
	}
	for _, env := range reference {
		check(m.pop(), env)
	}
	if m.segments != nil {
		t.Fatal("drained metadata retained a directory")
	}
}
