package dist

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/sllt/tao/erlang/etf"
)

// OpCode represents a distribution control message number from OTP dist.h.
type OpCode uint8

const (
	OpLink                OpCode = 1
	OpSend                OpCode = 2
	OpExit                OpCode = 3
	OpUnlinkObsolete      OpCode = 4
	OpRegSend             OpCode = 6
	OpGroupLeader         OpCode = 7
	OpExit2               OpCode = 8
	OpSendTT              OpCode = 12
	OpExitTT              OpCode = 13
	OpRegSendTT           OpCode = 16
	OpExit2TT             OpCode = 18
	OpMonitorP            OpCode = 19
	OpDemonitorP          OpCode = 20
	OpMonitorPExit        OpCode = 21
	OpSendSender          OpCode = 22
	OpSendSenderTT        OpCode = 23
	OpPayloadExit         OpCode = 24
	OpPayloadExitTT       OpCode = 25
	OpPayloadExit2        OpCode = 26
	OpPayloadExit2TT      OpCode = 27
	OpPayloadMonitorPExit OpCode = 28
	OpSpawnRequest        OpCode = 29
	OpSpawnRequestTT      OpCode = 30
	OpSpawnReply          OpCode = 31
	OpSpawnReplyTT        OpCode = 32
	OpAliasSend           OpCode = 33
	OpAliasSendTT         OpCode = 34
	OpUnlinkID            OpCode = 35
	OpUnlinkIDAck         OpCode = 36
	OpAltActSigSend       OpCode = 37
)

var (
	ErrInvalidControl = errors.New("dist: invalid control message")
)

// ControlMessage is the parsed or outbound representation of an Erlang distribution control signal.
type ControlMessage struct {
	Op       OpCode
	FromPID  etf.PID
	HasFrom  bool
	ToPID    etf.PID
	ToName   etf.Atom
	ToProc   etf.Term // etf.PID or etf.Atom for MONITOR_P, DEMONITOR_P, and MONITOR_P_EXIT
	Ref      etf.Ref
	UnlinkID uint64
	Reason   etf.Term // exit/down reason (populated from control or payload)
	Token    etf.Term
	HasToken bool
}

// NeedsPayload reports whether op is followed by a separate payload term on the wire.
func NeedsPayload(op OpCode) bool {
	switch op {
	case OpSend, OpSendTT, OpRegSend, OpRegSendTT, OpSendSender, OpSendSenderTT,
		OpPayloadExit, OpPayloadExitTT, OpPayloadExit2, OpPayloadExit2TT, OpPayloadMonitorPExit,
		OpAliasSend, OpAliasSendTT:
		return true
	default:
		return false
	}
}

// IsPayloadReason reports whether op's wire payload represents an exit/monitor Reason term.
func IsPayloadReason(op OpCode) bool {
	switch op {
	case OpPayloadExit, OpPayloadExitTT, OpPayloadExit2, OpPayloadExit2TT, OpPayloadMonitorPExit:
		return true
	default:
		return false
	}
}

// ControlState tracks per-connection directional control invariants and node ownership.
type ControlState struct {
	LocalNode       etf.Atom
	RemoteNode      etf.Atom
	NegotiatedFlags uint64
	SeenSendSender  bool
	SeenExitPayload bool
}

// ParseInbound validates a decoded control tuple against negotiated flags,
// directional transition invariants, and OTP dist.c node-ownership rules.
func (s *ControlState) ParseInbound(term etf.Term) (ControlMessage, error) {
	tup, ok := term.(etf.Tuple)
	if !ok || len(tup) < 1 {
		return ControlMessage{}, fmt.Errorf("%w: control term is not a non-empty tuple", ErrInvalidControl)
	}
	opInt, ok := tup[0].(int64)
	if !ok || opInt < 1 || opInt > 255 {
		return ControlMessage{}, fmt.Errorf("%w: invalid opcode element %T (%v)", ErrInvalidControl, tup[0], tup[0])
	}
	op := OpCode(opInt)

	switch op {
	case OpLink:
		if len(tup) != 3 {
			return ControlMessage{}, fmt.Errorf("%w: LINK arity %d != 3", ErrInvalidControl, len(tup))
		}
		from, ok1 := tup[1].(etf.PID)
		to, ok2 := tup[2].(etf.PID)
		if !ok1 || !ok2 || from.Node != s.RemoteNode || to.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: LINK invalid PID ownership", ErrInvalidControl)
		}
		return ControlMessage{Op: op, FromPID: from, HasFrom: true, ToPID: to}, nil

	case OpSend, OpSendTT:
		if s.SeenSendSender {
			return ControlMessage{}, fmt.Errorf("%w: legacy SEND(%d) after SEND_SENDER transition", ErrInvalidControl, op)
		}
		wantArity := 3
		if op == OpSendTT {
			wantArity = 4
		}
		if len(tup) != wantArity {
			return ControlMessage{}, fmt.Errorf("%w: SEND arity %d != %d", ErrInvalidControl, len(tup), wantArity)
		}
		if unused, ok := tup[1].(etf.Atom); !ok || unused != "" {
			return ControlMessage{}, fmt.Errorf("%w: SEND unused cookie must be empty atom", ErrInvalidControl)
		}
		to, ok := tup[2].(etf.PID)
		if !ok || to.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: SEND invalid ToPid", ErrInvalidControl)
		}
		msg := ControlMessage{Op: op, ToPID: to}
		if op == OpSendTT {
			msg.Token = tup[3]
			msg.HasToken = true
		}
		return msg, nil

	case OpSendSender, OpSendSenderTT:
		if s.NegotiatedFlags&DFlagSendSender == 0 {
			return ControlMessage{}, fmt.Errorf("%w: SEND_SENDER received without DFLAG_SEND_SENDER", ErrInvalidControl)
		}
		wantArity := 3
		if op == OpSendSenderTT {
			wantArity = 4
		}
		if len(tup) != wantArity {
			return ControlMessage{}, fmt.Errorf("%w: SEND_SENDER arity %d != %d", ErrInvalidControl, len(tup), wantArity)
		}
		from, ok1 := tup[1].(etf.PID)
		to, ok2 := tup[2].(etf.PID)
		if !ok1 || !ok2 || to.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: SEND_SENDER invalid PIDs", ErrInvalidControl)
		}
		s.SeenSendSender = true
		msg := ControlMessage{Op: op, FromPID: from, HasFrom: true, ToPID: to}
		if op == OpSendSenderTT {
			msg.Token = tup[3]
			msg.HasToken = true
		}
		return msg, nil

	case OpRegSend, OpRegSendTT:
		wantArity := 4
		if op == OpRegSendTT {
			wantArity = 5
		}
		if len(tup) != wantArity {
			return ControlMessage{}, fmt.Errorf("%w: REG_SEND arity %d != %d", ErrInvalidControl, len(tup), wantArity)
		}
		from, ok1 := tup[1].(etf.PID)
		_, ok2 := tup[2].(etf.Atom)
		toName, ok3 := tup[3].(etf.Atom)
		if !ok1 || !ok2 || !ok3 {
			return ControlMessage{}, fmt.Errorf("%w: REG_SEND invalid field types", ErrInvalidControl)
		}
		msg := ControlMessage{Op: op, FromPID: from, HasFrom: true, ToName: toName}
		if op == OpRegSendTT {
			msg.Token = tup[4]
			msg.HasToken = true
		}
		return msg, nil

	case OpGroupLeader:
		if len(tup) != 3 {
			return ControlMessage{}, fmt.Errorf("%w: GROUP_LEADER arity %d != 3", ErrInvalidControl, len(tup))
		}
		from, ok1 := tup[1].(etf.PID)
		to, ok2 := tup[2].(etf.PID)
		if !ok1 || !ok2 || to.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: GROUP_LEADER invalid PIDs", ErrInvalidControl)
		}
		return ControlMessage{Op: op, FromPID: from, HasFrom: true, ToPID: to}, nil

	case OpExit, OpExitTT, OpExit2, OpExit2TT:
		if s.SeenExitPayload {
			return ControlMessage{}, fmt.Errorf("%w: non-payload exit(%d) after EXIT_PAYLOAD transition", ErrInvalidControl, op)
		}
		withToken := op == OpExitTT || op == OpExit2TT
		wantArity := 4
		if withToken {
			wantArity = 5
		}
		if len(tup) != wantArity {
			return ControlMessage{}, fmt.Errorf("%w: EXIT arity %d != %d", ErrInvalidControl, len(tup), wantArity)
		}
		from, ok1 := tup[1].(etf.PID)
		to, ok2 := tup[2].(etf.PID)
		if !ok1 || !ok2 || from.Node != s.RemoteNode || to.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: EXIT invalid PID ownership", ErrInvalidControl)
		}
		msg := ControlMessage{Op: op, FromPID: from, HasFrom: true, ToPID: to}
		if withToken {
			msg.Token = tup[3]
			msg.HasToken = true
			msg.Reason = tup[4]
		} else {
			msg.Reason = tup[3]
		}
		return msg, nil

	case OpPayloadExit, OpPayloadExitTT, OpPayloadExit2, OpPayloadExit2TT:
		if s.NegotiatedFlags&DFlagExitPayload == 0 {
			return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_EXIT(%d) without DFLAG_EXIT_PAYLOAD", ErrInvalidControl, op)
		}
		withToken := op == OpPayloadExitTT || op == OpPayloadExit2TT
		wantArity := 3
		if withToken {
			wantArity = 4
		}
		if len(tup) != wantArity {
			return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_EXIT arity %d != %d", ErrInvalidControl, len(tup), wantArity)
		}
		from, ok1 := tup[1].(etf.PID)
		to, ok2 := tup[2].(etf.PID)
		if !ok1 || !ok2 || from.Node != s.RemoteNode || to.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_EXIT invalid PID ownership", ErrInvalidControl)
		}
		s.SeenExitPayload = true
		msg := ControlMessage{Op: op, FromPID: from, HasFrom: true, ToPID: to}
		if withToken {
			msg.Token = tup[3]
			msg.HasToken = true
		}
		return msg, nil

	case OpMonitorP, OpDemonitorP:
		if s.NegotiatedFlags&DFlagDistMonitor == 0 {
			return ControlMessage{}, fmt.Errorf("%w: MONITOR(%d) without DFLAG_DIST_MONITOR", ErrInvalidControl, op)
		}
		if len(tup) != 4 {
			return ControlMessage{}, fmt.Errorf("%w: MONITOR(%d) arity %d != 4", ErrInvalidControl, op, len(tup))
		}
		watcher, ok1 := tup[1].(etf.PID)
		ref, ok2 := tup[3].(etf.Ref)
		if !ok1 || !ok2 || watcher.Node != s.RemoteNode {
			return ControlMessage{}, fmt.Errorf("%w: MONITOR(%d) invalid watcher or ref", ErrInvalidControl, op)
		}
		watched := tup[2]
		switch w := watched.(type) {
		case etf.PID:
			if w.Node != s.LocalNode {
				return ControlMessage{}, fmt.Errorf("%w: MONITOR(%d) watched PID node %q != %q", ErrInvalidControl, op, w.Node, s.LocalNode)
			}
		case etf.Atom:
			if s.NegotiatedFlags&DFlagDistMonitorName == 0 {
				return ControlMessage{}, fmt.Errorf("%w: named MONITOR(%d) without DFLAG_DIST_MONITOR_NAME", ErrInvalidControl, op)
			}
		default:
			return ControlMessage{}, fmt.Errorf("%w: MONITOR(%d) watched must be PID or Atom", ErrInvalidControl, op)
		}
		return ControlMessage{Op: op, FromPID: watcher, HasFrom: true, ToProc: watched, Ref: ref}, nil

	case OpMonitorPExit:
		if s.SeenExitPayload {
			return ControlMessage{}, fmt.Errorf("%w: MONITOR_P_EXIT(21) after EXIT_PAYLOAD transition", ErrInvalidControl)
		}
		if len(tup) != 5 {
			return ControlMessage{}, fmt.Errorf("%w: MONITOR_P_EXIT arity %d != 5", ErrInvalidControl, len(tup))
		}
		toWatcher, ok1 := tup[2].(etf.PID)
		ref, ok2 := tup[3].(etf.Ref)
		if !ok1 || !ok2 || toWatcher.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: MONITOR_P_EXIT invalid watcher or ref", ErrInvalidControl)
		}
		watched := tup[1]
		switch w := watched.(type) {
		case etf.PID:
			if w.Node != s.RemoteNode {
				return ControlMessage{}, fmt.Errorf("%w: MONITOR_P_EXIT watched PID node mismatch", ErrInvalidControl)
			}
		case etf.Atom:
		default:
			return ControlMessage{}, fmt.Errorf("%w: MONITOR_P_EXIT watched must be PID or Atom", ErrInvalidControl)
		}
		return ControlMessage{Op: op, ToProc: watched, ToPID: toWatcher, Ref: ref, Reason: tup[4]}, nil

	case OpPayloadMonitorPExit:
		if s.NegotiatedFlags&DFlagExitPayload == 0 {
			return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_MONITOR_P_EXIT without DFLAG_EXIT_PAYLOAD", ErrInvalidControl)
		}
		if len(tup) != 4 {
			return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_MONITOR_P_EXIT arity %d != 4", ErrInvalidControl, len(tup))
		}
		toWatcher, ok1 := tup[2].(etf.PID)
		ref, ok2 := tup[3].(etf.Ref)
		if !ok1 || !ok2 || toWatcher.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_MONITOR_P_EXIT invalid watcher or ref", ErrInvalidControl)
		}
		var fromPID etf.PID
		hasFrom := false
		watched := tup[1]
		switch w := watched.(type) {
		case etf.PID:
			if w.Node != s.RemoteNode {
				return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_MONITOR_P_EXIT watched PID node mismatch", ErrInvalidControl)
			}
			fromPID = w
			hasFrom = true
		case etf.Atom:
		default:
			return ControlMessage{}, fmt.Errorf("%w: PAYLOAD_MONITOR_P_EXIT watched must be PID or Atom", ErrInvalidControl)
		}
		s.SeenExitPayload = true
		return ControlMessage{Op: op, FromPID: fromPID, HasFrom: hasFrom, ToProc: watched, ToPID: toWatcher, Ref: ref}, nil

	case OpAliasSend, OpAliasSendTT:
		if s.NegotiatedFlags&DFlagAlias == 0 {
			return ControlMessage{}, fmt.Errorf("%w: ALIAS_SEND without DFLAG_ALIAS", ErrInvalidControl)
		}
		wantArity := 3
		if op == OpAliasSendTT {
			wantArity = 4
		}
		if len(tup) != wantArity {
			return ControlMessage{}, fmt.Errorf("%w: ALIAS_SEND arity %d != %d", ErrInvalidControl, len(tup), wantArity)
		}
		from, ok1 := tup[1].(etf.PID)
		alias, ok2 := tup[2].(etf.Ref)
		if !ok1 || !ok2 || alias.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: ALIAS_SEND invalid sender or alias ref", ErrInvalidControl)
		}
		msg := ControlMessage{Op: op, FromPID: from, HasFrom: true, Ref: alias}
		if op == OpAliasSendTT {
			msg.Token = tup[3]
			msg.HasToken = true
		}
		return msg, nil

	case OpUnlinkID, OpUnlinkIDAck:
		if len(tup) != 4 {
			return ControlMessage{}, fmt.Errorf("%w: UNLINK(%d) arity %d != 4", ErrInvalidControl, op, len(tup))
		}
		id, ok := termToUint64NonZero(tup[1])
		if !ok {
			return ControlMessage{}, fmt.Errorf("%w: UNLINK(%d) invalid unlink ID %v", ErrInvalidControl, op, tup[1])
		}
		from, ok1 := tup[2].(etf.PID)
		to, ok2 := tup[3].(etf.PID)
		if !ok1 || !ok2 || from.Node != s.RemoteNode || to.Node != s.LocalNode {
			return ControlMessage{}, fmt.Errorf("%w: UNLINK(%d) invalid PID ownership", ErrInvalidControl, op)
		}
		return ControlMessage{Op: op, UnlinkID: id, FromPID: from, HasFrom: true, ToPID: to}, nil

	default:
		return ControlMessage{}, fmt.Errorf("%w: unsupported or obsolete opcode %d", ErrInvalidControl, op)
	}
}

func termToUint64NonZero(t etf.Term) (uint64, bool) {
	switch v := t.(type) {
	case int64:
		if v <= 0 {
			return 0, false
		}
		return uint64(v), true
	case *big.Int:
		if v == nil || v.Sign() <= 0 || !v.IsUint64() {
			return 0, false
		}
		u := v.Uint64()
		return u, u > 0
	default:
		return 0, false
	}
}

func uint64Term(v uint64) etf.Term {
	if v <= math.MaxInt64 {
		return int64(v)
	}
	return new(big.Int).SetUint64(v)
}

// BuildOutbound constructs the wire control tuple and optional payload term according to
// negotiated flags (automatically selecting SEND_SENDER vs SEND and PAYLOAD_EXIT vs EXIT).
func BuildOutbound(msg ControlMessage, payload etf.Term, negotiated uint64) (ctrl etf.Tuple, wirePayload etf.Term, hasPayload bool, err error) {
	usePayloadExit := negotiated&DFlagExitPayload != 0
	useSendSender := negotiated&DFlagSendSender != 0

	switch msg.Op {
	case OpLink:
		return etf.Tuple{int64(OpLink), msg.FromPID, msg.ToPID}, nil, false, nil

	case OpSend, OpSendSender:
		if useSendSender && msg.HasFrom {
			if msg.HasToken {
				return etf.Tuple{int64(OpSendSenderTT), msg.FromPID, msg.ToPID, msg.Token}, payload, true, nil
			}
			return etf.Tuple{int64(OpSendSender), msg.FromPID, msg.ToPID}, payload, true, nil
		}
		if msg.HasToken {
			return etf.Tuple{int64(OpSendTT), etf.Atom(""), msg.ToPID, msg.Token}, payload, true, nil
		}
		return etf.Tuple{int64(OpSend), etf.Atom(""), msg.ToPID}, payload, true, nil

	case OpRegSend:
		if msg.HasToken {
			return etf.Tuple{int64(OpRegSendTT), msg.FromPID, etf.Atom(""), msg.ToName, msg.Token}, payload, true, nil
		}
		return etf.Tuple{int64(OpRegSend), msg.FromPID, etf.Atom(""), msg.ToName}, payload, true, nil

	case OpAliasSend:
		if negotiated&DFlagAlias == 0 {
			return nil, nil, false, fmt.Errorf("%w: peer did not negotiate DFLAG_ALIAS", ErrInvalidControl)
		}
		if msg.HasToken {
			return etf.Tuple{int64(OpAliasSendTT), msg.FromPID, msg.Ref, msg.Token}, payload, true, nil
		}
		return etf.Tuple{int64(OpAliasSend), msg.FromPID, msg.Ref}, payload, true, nil

	case OpMonitorP:
		return etf.Tuple{int64(OpMonitorP), msg.FromPID, msg.ToProc, msg.Ref}, nil, false, nil

	case OpDemonitorP:
		return etf.Tuple{int64(OpDemonitorP), msg.FromPID, msg.ToProc, msg.Ref}, nil, false, nil

	case OpMonitorPExit, OpPayloadMonitorPExit:
		if usePayloadExit {
			return etf.Tuple{int64(OpPayloadMonitorPExit), msg.ToProc, msg.ToPID, msg.Ref}, msg.Reason, true, nil
		}
		return etf.Tuple{int64(OpMonitorPExit), msg.ToProc, msg.ToPID, msg.Ref, msg.Reason}, nil, false, nil

	case OpExit, OpPayloadExit:
		if usePayloadExit {
			if msg.HasToken {
				return etf.Tuple{int64(OpPayloadExitTT), msg.FromPID, msg.ToPID, msg.Token}, msg.Reason, true, nil
			}
			return etf.Tuple{int64(OpPayloadExit), msg.FromPID, msg.ToPID}, msg.Reason, true, nil
		}
		if msg.HasToken {
			return etf.Tuple{int64(OpExitTT), msg.FromPID, msg.ToPID, msg.Token, msg.Reason}, nil, false, nil
		}
		return etf.Tuple{int64(OpExit), msg.FromPID, msg.ToPID, msg.Reason}, nil, false, nil

	case OpExit2, OpPayloadExit2:
		if usePayloadExit {
			if msg.HasToken {
				return etf.Tuple{int64(OpPayloadExit2TT), msg.FromPID, msg.ToPID, msg.Token}, msg.Reason, true, nil
			}
			return etf.Tuple{int64(OpPayloadExit2), msg.FromPID, msg.ToPID}, msg.Reason, true, nil
		}
		if msg.HasToken {
			return etf.Tuple{int64(OpExit2TT), msg.FromPID, msg.ToPID, msg.Token, msg.Reason}, nil, false, nil
		}
		return etf.Tuple{int64(OpExit2), msg.FromPID, msg.ToPID, msg.Reason}, nil, false, nil

	case OpUnlinkID:
		return etf.Tuple{int64(OpUnlinkID), uint64Term(msg.UnlinkID), msg.FromPID, msg.ToPID}, nil, false, nil

	case OpUnlinkIDAck:
		return etf.Tuple{int64(OpUnlinkIDAck), uint64Term(msg.UnlinkID), msg.FromPID, msg.ToPID}, nil, false, nil

	default:
		return nil, nil, false, fmt.Errorf("%w: unsupported outbound opcode %d", ErrInvalidControl, msg.Op)
	}
}
