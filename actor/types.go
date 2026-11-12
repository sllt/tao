// Package actor implements local actors with one Receive entry point.
package actor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Behavior interface {
	Receive(any) error
	bindProcess(*process) error
}
type Factory func() Behavior
type Addr interface {
	String() string
	isAddr()
}
type PID struct {
	Node   string
	BootID [16]byte
	ID     uint64
}

func (p PID) String() string {
	return fmt.Sprintf("pid://%s/%s/%d", p.Node, hex.EncodeToString(p.BootID[:]), p.ID)
}
func (PID) isAddr() {}

type Ref struct {
	Node string
	Path string
}

func (r Ref) String() string {
	if r.Node == "" {
		return r.Path
	}
	return "node://" + r.Node + "/" + r.Path
}
func (Ref) isAddr() {}

type Started struct{ Restoring bool }
type Stopped struct{ Reason error }
type MonitorRef struct {
	Observer PID
	ID       uint64
}
type Down struct {
	Ref    MonitorRef
	PID    PID
	Reason error
}
type Exit struct {
	PID    PID
	Reason error
}
type ExitError struct {
	PID   PID
	Cause error
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("actor: linked process %s exited: %v", e.PID, e.Cause)
}
func (e *ExitError) Unwrap() error { return e.Cause }

var (
	ErrClosed              = errors.New("actor: node closed")
	ErrNoProcess           = errors.New("actor: process does not exist")
	ErrStalePID            = errors.New("actor: stale node boot ID")
	ErrRemoteUnavailable   = errors.New("actor: remote node unavailable")
	ErrNameTaken           = errors.New("actor: name already registered")
	ErrStarting            = errors.New("actor: process starting")
	ErrRestarting          = errors.New("actor: process restarting")
	ErrStopped             = errors.New("actor: process stopped")
	ErrUnresponsive        = errors.New("actor: process did not stop before deadline")
	ErrMailboxFull         = errors.New("actor: mailbox full")
	ErrCapacity            = errors.New("actor: capacity exhausted")
	ErrSelfCall            = errors.New("actor: synchronous call to self")
	ErrNoReply             = errors.New("actor: handler returned without Reply or Defer")
	ErrNotCall             = errors.New("actor: current message is not a call")
	ErrAlreadyReplied      = errors.New("actor: reply already completed")
	ErrReplyExpired        = errors.New("actor: reply ticket expired")
	ErrReplyType           = errors.New("actor: unexpected reply type")
	ErrProcessCrashed      = errors.New("actor: process crashed")
	ErrMaxRestartsExceeded = errors.New("actor: restart intensity exceeded")
)

type CrashError struct {
	PID   PID
	Cause error
}

func (e *CrashError) Error() string {
	return fmt.Sprintf("%s: %v: %v", e.PID, ErrProcessCrashed, e.Cause)
}
func (e *CrashError) Unwrap() error        { return e.Cause }
func (e *CrashError) Is(target error) bool { return target == ErrProcessCrashed }

type NodeOptions struct {
	Kinds                   []KindSpec
	MaxSnapshotBytes        int
	Directory               Directory
	Register                []MessageType
	Codec                   MessageCodec // nil selects CBORCodec for network business messages
	Network                 NetworkOptions
	Name                    string
	Version                 string
	Logger                  *slog.Logger
	MaxActors               int
	MailboxSize             int
	MaxPendingCalls         int
	MaxAsyncRequests        int // Request operations plus results awaiting Receive; 0 uses MaxPendingCalls
	MaxObservations         int
	MaxObservationsPerActor int
	CallTimeout             time.Duration
	StartupTimeout          time.Duration
	ShutdownTimeout         time.Duration
}

func (o NodeOptions) normalized() (NodeOptions, error) {
	if o.MaxSnapshotBytes == 0 {
		o.MaxSnapshotBytes = 1 << 20
	}
	if o.MaxSnapshotBytes < 1 || o.MaxSnapshotBytes > 64<<20 {
		return NodeOptions{}, fmt.Errorf("actor: invalid snapshot limit")
	}
	if o.MaxObservations == 0 {
		o.MaxObservations = 65536
	}
	if o.MaxObservationsPerActor == 0 {
		o.MaxObservationsPerActor = 256
	}
	if o.MaxObservations < 1 || o.MaxObservationsPerActor < 1 {
		return NodeOptions{}, fmt.Errorf("actor: invalid observation limits")
	}
	if !validSegment(o.Name) {
		return NodeOptions{}, fmt.Errorf("actor: invalid node name")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxActors == 0 {
		o.MaxActors = 100000
	}
	if o.MailboxSize == 0 {
		o.MailboxSize = 1024
	}
	if o.MaxPendingCalls == 0 {
		o.MaxPendingCalls = 4096
	}
	if o.MaxAsyncRequests == 0 {
		o.MaxAsyncRequests = o.MaxPendingCalls
	}
	if o.CallTimeout == 0 {
		o.CallTimeout = 5 * time.Second
	}
	if o.StartupTimeout == 0 {
		o.StartupTimeout = 5 * time.Second
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = 5 * time.Second
	}
	if o.MaxActors < 1 || o.MailboxSize < 1 || o.MaxPendingCalls < 1 || o.MaxAsyncRequests < 1 || o.CallTimeout < 0 || o.StartupTimeout < 0 || o.ShutdownTimeout < 0 {
		return NodeOptions{}, fmt.Errorf("actor: invalid node limits")
	}
	return o, nil
}

type spawnOptions struct {
	handoff       bool
	kind, version string
	name          string
	mailbox       int
	shutdown      time.Duration
	restart       RestartPolicy
	mailPolicy    RestartMailbox
}

type Strategy uint8

const (
	OneForOne Strategy = iota
	OneForAll
	RestForOne
)

type RestartPolicy uint8

const (
	Permanent RestartPolicy = iota
	Transient
	Temporary
)

type RestartMailbox uint8

const (
	DropMailbox RestartMailbox = iota
	PreserveRef
)

func WithRestart(policy RestartPolicy) SpawnOption {
	return func(o *spawnOptions) error {
		if policy > Temporary {
			return fmt.Errorf("actor: invalid restart policy")
		}
		o.restart = policy
		return nil
	}
}
func WithRestartMailbox(policy RestartMailbox) SpawnOption {
	return func(o *spawnOptions) error {
		if policy > PreserveRef {
			return fmt.Errorf("actor: invalid restart mailbox policy")
		}
		o.mailPolicy = policy
		return nil
	}
}
func (a *Actor) SetStrategy(strategy Strategy, maxRestarts int, window time.Duration) error {
	if strategy > RestForOne || maxRestarts < 1 || window <= 0 {
		return fmt.Errorf("actor: invalid restart strategy")
	}
	a.proc.strategy = strategy
	a.proc.maxRestarts = maxRestarts
	a.proc.restartWindow = window
	return nil
}

type SpawnOption func(*spawnOptions) error

func WithHandoff(enabled bool) SpawnOption {
	return func(options *spawnOptions) error { options.handoff = enabled; return nil }
}

func WithName(name string) SpawnOption {
	return func(o *spawnOptions) error {
		if !validSegment(name) {
			return fmt.Errorf("actor: invalid child name")
		}
		o.name = name
		return nil
	}
}
func WithMailboxSize(size int) SpawnOption {
	return func(o *spawnOptions) error {
		if size < 1 {
			return fmt.Errorf("actor: invalid mailbox size")
		}
		o.mailbox = size
		return nil
	}
}
func WithShutdownTimeout(timeout time.Duration) SpawnOption {
	return func(o *spawnOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("actor: invalid shutdown timeout")
		}
		o.shutdown = timeout
		return nil
	}
}
func validSegment(s string) bool {
	if len(s) == 0 || len(s) > 128 || s == "." || s == ".." {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func validPath(path string) bool {
	if len(path) > 1024 {
		return false
	}
	for _, part := range strings.Split(path, "/") {
		if !validSegment(part) {
			return false
		}
	}
	return true
}

type Actor struct{ proc *process }

func (a *Actor) Monitor(target PID) (MonitorRef, error) {
	a.proc.releaseNext()
	if err := a.proc.preparationError(); err != nil {
		return MonitorRef{}, err
	}
	return a.proc.node.monitor(a.proc, target)
}
func (a *Actor) Demonitor(ref MonitorRef) { a.proc.releaseNext(); a.proc.node.demonitor(a.proc, ref) }
func (a *Actor) Link(ctx context.Context, target PID) error {
	a.proc.releaseNext()
	if err := a.proc.preparationError(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(a.proc.ctx, cancel)
	defer stop()
	return a.proc.node.link(ctx, a.proc, target)
}
func (a *Actor) Unlink(target PID) { a.proc.releaseNext(); a.proc.node.unlink(a.proc, target) }
func (a *Actor) SetTrapExit(enabled bool) {
	a.proc.mu.Lock()
	a.proc.trapExit = enabled
	a.proc.mu.Unlock()
}

func (a *Actor) bindProcess(p *process) error {
	if a.proc != nil {
		return fmt.Errorf("actor: factory reused a bound behavior")
	}
	a.proc = p
	return nil
}
func (a *Actor) PID() PID     { return a.proc.pid }
func (a *Actor) Name() string { return a.proc.path }
func (a *Actor) Parent() PID {
	if a.proc.parent.internal {
		return PID{}
	}
	return a.proc.parent.pid
}
func (a *Actor) Sender() Addr {
	return a.proc.current.sender
}
func (a *Actor) Context() context.Context {
	if a.proc.current.request != nil {
		return a.proc.current.request.context()
	}
	return a.proc.ctx
}
func (a *Actor) Log() *slog.Logger { return a.proc.logger() }
func (a *Actor) Send(to Addr, msg any) error {
	turn := a.proc.executor.Load()
	if turn != nil {
		turn.beginSend()
	}
	if err := a.proc.preparationError(); err != nil {
		return err
	}
	if pid, ok := to.(PID); ok && pid == a.PID() {
		return a.proc.enqueue(envelope{message: msg, sender: a.proc.selfAddr})
	}
	if pid, ok := to.(PID); ok && pid.Node == a.proc.node.Name() {
		var target *process
		if turn != nil && turn.target != nil && turn.target.pid == pid && turn.target.published.Load() && !a.proc.node.stopping.Load() {
			target = turn.target
		} else {
			var err error
			target, err = a.proc.node.resolvePID(pid)
			if err != nil {
				return err
			}
			if turn != nil && turn.multiSend {
				turn.target = target
			}
		}
		return target.enqueueFrom(envelope{message: msg, sender: a.proc.selfAddr}, nil, a.proc)
	}
	a.proc.releaseNext()
	return a.proc.node.send(to, msg, a.proc.selfAddr, nil)
}
func (a *Actor) call(ctx context.Context, to Addr, msg any) (any, error) {
	a.proc.releaseNext()
	if err := a.proc.preparationError(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(a.proc.ctx, cancel)
	defer stop()
	return a.proc.node.call(ctx, to, msg, a.proc)
}
func (a *Actor) Spawn(factory Factory, options ...SpawnOption) (PID, error) {
	a.proc.releaseNext()
	if err := a.proc.preparationError(); err != nil {
		return PID{}, err
	}
	return a.proc.node.spawn(a.proc, a.Context(), factory, options...)
}
func (a *Actor) Stop(reason error) { a.proc.requestStop(reason, false) }

// Reply submits a result, ignoring an expired caller. It does not confirm remote
// receipt. Use ReplyChecked to inspect whether the local reply ticket has expired.
func (a *Actor) Reply(value any) error {
	return ignoreExpiredReply(a.ReplyChecked(value))
}

// ReplyChecked reports ErrReplyExpired if the request can no longer be answered.
func (a *Actor) ReplyChecked(value any) error {
	a.proc.releaseNext()
	if a.proc.snapshot != nil {
		return a.proc.snapshotReply(value, nil)
	}
	if a.proc.current.request == nil {
		return ErrNotCall
	}
	return a.proc.node.reply(a.proc.current.request, a.PID(), value, nil, false)
}
func (a *Actor) ReplyError(err error) error {
	return ignoreExpiredReply(a.ReplyErrorChecked(err))
}

// ReplyErrorChecked is ReplyChecked for a business error.
func (a *Actor) ReplyErrorChecked(err error) error {
	a.proc.releaseNext()
	if a.proc.snapshot != nil {
		return a.proc.snapshotReply(nil, err)
	}
	if a.proc.current.request == nil {
		return ErrNotCall
	}
	return a.proc.node.reply(a.proc.current.request, a.PID(), nil, err, false)
}
func (a *Actor) Defer() DeferredReply {
	if a.proc.snapshot != nil {
		a.proc.snapshot.called, a.proc.snapshot.err = true, ErrRestoring
		return DeferredReply{}
	}
	if a.proc.current.request == nil {
		return DeferredReply{}
	}
	r := a.proc.current.request
	r.deferReply()
	return DeferredReply{request: r, owner: a.PID()}
}

type DeferredReply struct {
	request *request
	owner   PID
}

func (d DeferredReply) Reply(value any) error {
	return ignoreExpiredReply(d.ReplyChecked(value))
}

// ReplyChecked submits a deferred result and reports expired credentials.
func (d DeferredReply) ReplyChecked(value any) error {
	if d.request == nil {
		return ErrNotCall
	}
	return d.request.node.reply(d.request, d.owner, value, nil, false)
}
func (d DeferredReply) ReplyError(err error) error {
	return ignoreExpiredReply(d.ReplyErrorChecked(err))
}

// ReplyErrorChecked is ReplyChecked for a deferred business error.
func (d DeferredReply) ReplyErrorChecked(err error) error {
	if d.request == nil {
		return ErrNotCall
	}
	return d.request.node.reply(d.request, d.owner, nil, err, false)
}

func ignoreExpiredReply(err error) error {
	if err == ErrReplyExpired {
		return nil
	}
	return err
}
