package actor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/actor/internal/directorytest"
)

func TestMemoryDirectoryContract(t *testing.T) {
	directorytest.Run(t, func(*testing.T) actor.Directory { return actor.NewMemoryDirectory() })
}

func TestMemoryDirectoryWatchOverflow(t *testing.T) {
	d := actor.NewMemoryDirectory()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := d.Watch(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	r, err := d.Claim(ctx, directorytest.Route("counter", "a", 1))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 80 {
		r, err = d.SwapInstance(ctx, r, directorytest.Route(r.Path, "a", uint64(i+2)))
		if err != nil {
			t.Fatal(err)
		}
	}
	failed := false
	for event := range events {
		if errors.Is(event.Err, actor.ErrWatchOverflow) {
			failed = true
		}
	}
	if !failed {
		t.Fatal("overflow silently lost updates")
	}
	resumed, err := d.Watch(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if event := <-resumed; !event.Snapshot || len(event.Routes) != 1 || event.Routes[0] != r {
		t.Fatalf("fresh snapshot: %+v", event)
	}
}
