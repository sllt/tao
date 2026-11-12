package actor

import "time"

// ChildSpec declares a child of a Supervisor. Name, when non-empty, takes
// precedence over a WithName option. Factory must create a fresh Behavior.
type ChildSpec struct {
	Name    string
	Factory Factory
	Options []SpawnOption
}

// SupervisorSpec uses the ordinary supervision machinery. Zero MaxRestarts and
// Window retain the runtime defaults (5 restarts in 10 seconds).
type SupervisorSpec struct {
	Strategy    Strategy
	MaxRestarts int
	Window      time.Duration
	Children    []ChildSpec
}

// Supervisor builds a Factory whose Started handler configures supervision and
// starts children in declaration order. A child startup failure fails the parent
// startup and stops children already started. Stop visits children in reverse order.
// Configuration slices are copied; captured state inside Factory/Options is not.
func Supervisor(spec SupervisorSpec) Factory {
	spec.Children = append([]ChildSpec(nil), spec.Children...)
	for i := range spec.Children {
		spec.Children[i].Options = append([]SpawnOption(nil), spec.Children[i].Options...)
		if spec.Children[i].Name != "" {
			spec.Children[i].Options = append(spec.Children[i].Options, WithName(spec.Children[i].Name))
		}
	}
	return func() Behavior { return &supervisorBehavior{spec: spec} }
}

type supervisorBehavior struct {
	Actor
	spec SupervisorSpec
}

func (s *supervisorBehavior) Receive(message any) error {
	if _, ok := message.(Started); !ok {
		return nil
	}
	maxRestarts, window := s.spec.MaxRestarts, s.spec.Window
	if maxRestarts == 0 {
		maxRestarts = s.proc.maxRestarts
	}
	if window == 0 {
		window = s.proc.restartWindow
	}
	if err := s.SetStrategy(s.spec.Strategy, maxRestarts, window); err != nil {
		return err
	}
	for _, child := range s.spec.Children {
		if _, err := s.Spawn(child.Factory, child.Options...); err != nil {
			return err
		}
	}
	return nil
}
