package actor

import (
	"errors"
	"strings"
	"testing"
)

func TestParseRefCanonicalFormsAndLimits(t *testing.T) {
	maxNode := strings.Repeat("n", 128)
	maxPath := strings.Repeat(strings.Repeat("a", 127)+"/", 7) + strings.Repeat("b", 128)
	for _, test := range []struct {
		name    string
		address string
		want    Ref
	}{
		{"local", "counter", Ref{Path: "counter"}},
		{"nested", "app/v1.counter-worker_1", Ref{Path: "app/v1.counter-worker_1"}},
		{"qualified", "node://Node_1/app/counter", Ref{Node: "Node_1", Path: "app/counter"}},
		{"path-limit", maxPath, Ref{Path: maxPath}},
		{"node-and-path-limits", "node://" + maxNode + "/" + maxPath, Ref{Node: maxNode, Path: maxPath}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseRef(test.address)
			if err != nil || got != test.want || got.String() != test.address {
				t.Fatalf("ParseRef: got %#v, %v; want %#v", got, err, test.want)
			}
			if err := ValidateDirectoryPath(got.Path); err != nil {
				t.Fatalf("parsed path is not accepted by Directory: %v", err)
			}
		})
	}
}

func TestParseRefRejectsAmbiguousAndInvalidAddresses(t *testing.T) {
	longPath := strings.Repeat(strings.Repeat("a", 127)+"/", 7) + strings.Repeat("b", 127) + "/x"
	for _, test := range []struct {
		name    string
		address string
	}{
		{"empty", ""},
		{"leading-slash", "/app/counter"},
		{"trailing-slash", "app/counter/"},
		{"empty-segment", "app//counter"},
		{"dot-segment", "app/./counter"},
		{"parent-segment", "app/../counter"},
		{"whitespace", " counter "},
		{"control-character", "app/\ncounter"},
		{"unicode-path", "app/计数器"},
		{"long-segment", strings.Repeat("a", 129)},
		{"long-path", longPath},
		{"missing-node", "node:///counter"},
		{"missing-path", "node://worker"},
		{"empty-path", "node://worker/"},
		{"long-node", "node://" + strings.Repeat("n", 129) + "/counter"},
		{"dot-node", "node://./counter"},
		{"unicode-node", "node://节点/counter"},
		{"credentials", "node://user@worker/counter"},
		{"port", "node://worker:8080/counter"},
		{"query", "node://worker/counter?version=1"},
		{"empty-query", "node://worker/counter?"},
		{"fragment", "node://worker/counter#state"},
		{"escaped-segment", "node://worker/%63ounter"},
		{"escaped-slash", "node://worker/app%2Fcounter"},
		{"extra-slash", "node://worker//counter"},
		{"scheme-case", "NODE://worker/counter"},
		{"other-scheme", "http://worker/counter"},
		{"pid", "pid://worker/boot/1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ref, err := ParseRef(test.address)
			if !errors.Is(err, ErrInvalidRef) || ref != (Ref{}) {
				t.Fatalf("invalid address returned %#v, %v", ref, err)
			}
		})
	}
}

func TestParsedRefRoutesWithoutChangingNodeQualification(t *testing.T) {
	n := testNode(t, NodeOptions{})
	spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "read" {
			return a.Reply(42)
		}
		return nil
	}), WithName("counter"))
	for _, address := range []string{"counter", "node://" + n.Name() + "/counter"} {
		ref, err := ParseRef(address)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := n.Call[int](t.Context(), ref, "read"); err != nil || got != 42 {
			t.Fatalf("parsed address %q: got %d, %v", address, got, err)
		}
	}
}
