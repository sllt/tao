package erlang

import (
	"fmt"

	"github.com/sllt/tao/erlang/etf"
	"github.com/sllt/tao/erlang/internal/epmd"
)

// Type aliases expose ETF process/port/reference identities directly without lossy conversion.
type (
	PID  = etf.PID
	Port = etf.Port
	Ref  = etf.Ref
)

type targetKind uint8

const (
	targetInvalid targetKind = iota
	targetByPID
	targetByRegistered
)

// Target explicitly identifies either a remote Erlang PID or a {Name, Node} registered process.
type Target struct {
	kind targetKind
	pid  PID
	name etf.Atom
	node etf.Atom
}

// Process addresses a specific Erlang PID.
func Process(pid PID) Target {
	return Target{
		kind: targetByPID,
		pid:  pid,
		node: pid.Node,
	}
}

// Registered addresses an Erlang registered process name on a full node ("alive@host").
func Registered(name, node string) Target {
	return Target{
		kind: targetByRegistered,
		name: etf.Atom(name),
		node: etf.Atom(node),
	}
}

// IsPID reports whether t addresses a concrete PID.
func (t Target) IsPID() bool { return t.kind == targetByPID }

// IsRegistered reports whether t addresses a {Name, Node} registered process.
func (t Target) IsRegistered() bool { return t.kind == targetByRegistered }

// PID returns the target PID when IsPID is true.
func (t Target) PID() PID { return t.pid }

// Name returns the registered process atom when IsRegistered is true.
func (t Target) Name() etf.Atom { return t.name }

// Node returns the target Erlang node name atom.
func (t Target) Node() etf.Atom { return t.node }

// Validate checks that t is a well-formed PID or Registered(Name, Node) target.
func (t Target) Validate() error {
	switch t.kind {
	case targetByPID:
		if err := t.pid.Validate(); err != nil {
			return err
		}
		if _, _, err := epmd.SplitNodeName(string(t.pid.Node)); err != nil {
			return err
		}
		return nil
	case targetByRegistered:
		if t.name == "" {
			return fmt.Errorf("%w: empty registered process name", etf.ErrInvalidTerm)
		}
		if err := etf.ValidateAtom(t.name); err != nil {
			return err
		}
		if _, _, err := epmd.SplitNodeName(string(t.node)); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("%w: uninitialized Target", etf.ErrInvalidTerm)
	}
}

// ReplyTarget captures the exact reply routing and original Tag of an inbound '$gen_call' or 'system' request.
// It follows OTP stdlib gen:reply/2 without mutating Ref ID bits (§2, §8.1).
type ReplyTarget struct {
	To              PID
	Tag             etf.Term
	Alias           Ref
	UseAlias        bool
	callerMonitored bool
}

func (rt ReplyTarget) callMonitorRef() (Ref, bool) {
	if rt.UseAlias {
		return rt.Alias, rt.Alias.Node != ""
	}
	if ref, ok := rt.Tag.(Ref); ok && ref.Node != "" {
		return ref, true
	}
	if lst, ok := rt.Tag.(etf.List); ok && len(lst.Elements) == 1 {
		if ref, ok := lst.Elements[0].(Ref); ok && ref.Node != "" {
			return ref, true
		}
	}
	return Ref{}, false
}

// PeerNode returns the destination Erlang node for delivering the reply.
func (rt ReplyTarget) PeerNode() etf.Atom {
	if rt.UseAlias {
		return rt.Alias.Node
	}
	return rt.To.Node
}

// ExtractReplyTarget parses an Erlang From tuple {ToPid, Tag} according to OTP gen:reply/2:
//  1. {To, [alias | Alias] = Tag} when is_reference(Alias)       -> send {Tag, Reply} to Alias
//  2. {To, [[alias | Alias] | _] = Tag} when is_reference(Alias) -> send {Tag, Reply} to Alias
//  3. {To, Tag} when is_pid(To)                                  -> send {Tag, Reply} to To
func ExtractReplyTarget(fromTerm etf.Term) (ReplyTarget, bool) {
	tup, ok := fromTerm.(etf.Tuple)
	if !ok || len(tup) != 2 {
		return ReplyTarget{}, false
	}
	toPID, ok := tup[0].(PID)
	if !ok || toPID.Node == "" {
		return ReplyTarget{}, false
	}
	tag := tup[1]
	if tag == nil {
		return ReplyTarget{}, false
	}
	if alias, ok := extractAliasFromTag(tag); ok {
		return ReplyTarget{
			To:       toPID,
			Tag:      tag,
			Alias:    alias,
			UseAlias: true,
		}, true
	}
	return ReplyTarget{
		To:  toPID,
		Tag: tag,
	}, true
}

func extractAliasFromTag(tag etf.Term) (Ref, bool) {
	lst, ok := tag.(etf.List)
	if !ok || len(lst.Elements) == 0 {
		return Ref{}, false
	}
	// Case 1: [alias | Alias]
	if len(lst.Elements) == 1 && lst.Elements[0] == etf.Atom("alias") {
		if alias, ok := lst.Tail.(Ref); ok && alias.Node != "" {
			return alias, true
		}
	}
	// Case 2: [[alias | Alias] | _]
	if inner, ok := lst.Elements[0].(etf.List); ok && len(inner.Elements) == 1 && inner.Elements[0] == etf.Atom("alias") {
		if alias, ok := inner.Tail.(Ref); ok && alias.Node != "" {
			return alias, true
		}
	}
	return Ref{}, false
}

// BuildAliasTag constructs the standard improper list [alias | Ref] used by modern OTP gen:do_call.
func BuildAliasTag(alias Ref) etf.List {
	return etf.List{
		Elements: []etf.Term{etf.Atom("alias")},
		Tail:     alias,
	}
}

// ParseGenCall checks whether msg has the outer shape {'$gen_call', {CallerPid, Tag}, Request}.
func ParseGenCall(msg etf.Term) (replyTo ReplyTarget, request etf.Term, ok bool) {
	tup, isTup := msg.(etf.Tuple)
	if !isTup || len(tup) != 3 || tup[0] != etf.Atom("$gen_call") {
		return ReplyTarget{}, nil, false
	}
	rt, validFrom := ExtractReplyTarget(tup[1])
	if !validFrom {
		return ReplyTarget{}, nil, false
	}
	return rt, tup[2], true
}

// ParseGenCast checks whether msg has the outer shape {'$gen_cast', Request}.
func ParseGenCast(msg etf.Term) (request etf.Term, ok bool) {
	tup, isTup := msg.(etf.Tuple)
	if !isTup || len(tup) != 2 || tup[0] != etf.Atom("$gen_cast") {
		return nil, false
	}
	return tup[1], true
}

// ParseSystemRequest checks whether msg has the OTP sys/proc_lib shape {'system', {FromPid, Tag}, Request}.
func ParseSystemRequest(msg etf.Term) (replyTo ReplyTarget, request etf.Term, ok bool) {
	tup, isTup := msg.(etf.Tuple)
	if !isTup || len(tup) != 3 || tup[0] != etf.Atom("system") {
		return ReplyTarget{}, nil, false
	}
	rt, validFrom := ExtractReplyTarget(tup[1])
	if !validFrom {
		return ReplyTarget{}, nil, false
	}
	return rt, tup[2], true
}
