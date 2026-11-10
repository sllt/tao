package actor

import (
	"context"
	"fmt"
)

type KindSpec struct {
	Name    string
	Version string
	Factory Factory
}

func (kind KindSpec) validate() error {
	if !validSegment(kind.Name) || kind.Version == "" || len(kind.Version) > 128 || kind.Factory == nil {
		return fmt.Errorf("actor: invalid KindSpec")
	}
	return nil
}

type kindKey struct{ name, version string }

// SpawnKind constructs a behavior registered in NodeOptions.Kinds and records
// its stable kind/version for replacement and cross-node preparation.
func (n *Node) SpawnKind(name, version string, options ...SpawnOption) (PID, error) {
	return n.spawnKind(n.root, context.Background(), name, version, options...)
}

// SpawnKind creates a supervised child from the node's registered factory.
// Like Spawn, it may only be called from the actor's Receive call stack.
func (a *Actor) SpawnKind(name, version string, options ...SpawnOption) (PID, error) {
	a.proc.releaseNext()
	if err := a.proc.preparationError(); err != nil {
		return PID{}, err
	}
	return a.proc.node.spawnKind(a.proc, a.Context(), name, version, options...)
}

func (n *Node) spawnKind(parent *process, ctx context.Context, name, version string, options ...SpawnOption) (PID, error) {
	kind, ok := n.kinds[kindKey{name, version}]
	if !ok {
		return PID{}, fmt.Errorf("actor: unregistered kind %s@%s", name, version)
	}
	options = append(options, WithKind(name, version))
	return n.spawn(parent, ctx, kind.Factory, options...)
}

// WithKind labels an explicitly supplied Factory. The application must ensure
// it matches this version's behavior and snapshot schema.
func WithKind(name, version string) SpawnOption {
	return func(options *spawnOptions) error {
		if !validSegment(name) || version == "" || len(version) > 128 {
			return fmt.Errorf("actor: invalid kind name/version")
		}
		options.kind, options.version = name, version
		return nil
	}
}
