package actor

// FromFunc adapts a stateless receive function into a Factory. Each spawn and
// restart gets a fresh Actor binding. Captured closure variables are shared;
// keep per-instance mutable state in a Behavior struct instead.
// A nil receive function produces a nil Factory, which Spawn rejects.
func FromFunc(receive func(*Actor, any) error) Factory {
	if receive == nil {
		return nil
	}
	return func() Behavior { return &functionBehavior{receive: receive} }
}

type functionBehavior struct {
	Actor
	receive func(*Actor, any) error
}

func (f *functionBehavior) Receive(message any) error {
	return f.receive(&f.Actor, message)
}
