package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/bits"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sllt/tao/actor"
)

const operationTimeout = 10 * time.Second
const sampleEvery = 10 * time.Second

var workloadPayload = strings.Repeat("hive", 32)

type work struct {
	Sequence uint64
	Value    uint64
	Payload  string
	Op       string
}

type reply struct {
	Sequence uint64
	Checksum uint64
	PID      actor.PID
	Path     string
}

func checksum(message work) uint64 {
	value := message.Sequence ^ message.Value
	for i := range len(message.Payload) {
		value = bits.RotateLeft64(value^uint64(message.Payload[i]), 5) * 0x9e3779b185ebca87
	}
	for i := range 16 {
		value = bits.RotateLeft64(value+uint64(i)*0x517cc1b727220a95, 13) ^ 0x94d049bb133111eb
	}
	return value
}

type announced struct{}
type flightGate struct {
	entered    chan struct{}
	replyError chan error
	executions atomic.Int32
}

type service struct {
	actor.Actor
	announce chan<- actor.PID
	flight   *flightGate
	lifetime context.Context
}

func (a *service) Receive(message any) error {
	switch message := message.(type) {
	case actor.Started:
		// A request's Context belongs to the caller. Keep the actor lifetime
		// separately so the shutdown probe cooperates with Node.Stop.
		a.lifetime = a.Context()
		if a.announce != nil {
			return a.Send(a.PID(), announced{})
		}
	case announced:
		a.announce <- a.PID() // This self message runs after PID publication.
	case work:
		switch message.Op {
		case "panic":
			panic("soak: planned local actor panic")
		case "hold":
			if a.flight == nil {
				return a.ReplyError(errors.New("hold requires the dedicated flight actor"))
			}
			a.flight.executions.Add(1)
			a.flight.entered <- struct{}{}
			<-a.lifetime.Done()
			// Stop may already have completed the caller. Keep Reply's own
			// result visible without turning expected expiration into a crash.
			a.flight.replyError <- a.ReplyError(actor.ErrStopped)
			return nil
		case "work":
			return a.Reply(reply{message.Sequence, checksum(message), a.PID(), a.Name()})
		default:
			return a.ReplyError(errors.New("unknown soak operation"))
		}
	}
	return nil
}

// forwardCall is local to the caller node; only its work crosses TCP.
type forwardCall struct {
	target  actor.Ref
	message work
}

type bridge struct {
	actor.Actor
	pending map[actor.RequestID]actor.DeferredReply // Receive owns this map
}

func (a *bridge) Receive(message any) error {
	switch message := message.(type) {
	case forwardCall:
		ticket := a.Defer()
		id, err := a.Request(a.Context(), message.target, message.message)
		if err != nil {
			replyErr := ticket.ReplyError(err)
			if errors.Is(replyErr, actor.ErrReplyExpired) {
				return nil
			}
			return replyErr
		}
		if a.pending == nil {
			a.pending = make(map[actor.RequestID]actor.DeferredReply)
		}
		a.pending[id] = ticket
	case actor.CallResult:
		ticket, ok := a.pending[message.ID]
		if !ok {
			return fmt.Errorf("bridge received unknown RequestID %d", message.ID.ID)
		}
		delete(a.pending, message.ID)
		var err error
		if message.Err != nil {
			err = ticket.ReplyError(message.Err)
		} else {
			err = ticket.Reply(message.Value)
		}
		if errors.Is(err, actor.ErrReplyExpired) {
			return nil
		}
		return err
	case actor.Stopped:
		clear(a.pending) // Stop owns cancellation and expires the original calls.
	}
	return nil
}

type localGate struct {
	entered chan struct{}
	release chan struct{}
	replied chan error
}
type tick struct{}
type readTicks struct{}
type probeActor struct {
	actor.Actor
	ticks     int64
	processed chan<- struct{}
}

func (a *probeActor) Receive(message any) error {
	switch message := message.(type) {
	case localGate:
		close(message.entered)
		<-message.release
		if message.replied != nil {
			message.replied <- a.Reply(int64(123))
		}
	case tick:
		a.ticks++
		a.processed <- struct{}{}
	case readTicks:
		return a.Reply(a.ticks)
	}
	return nil
}

// The fixed histogram rounds up to microseconds, then uses 16 linear buckets
// per power of two. Quantiles report the containing bucket's upper bound.
type histogram struct {
	buckets [64 * 16]uint64
	count   uint64
	maximum uint64
}

func (h *histogram) add(elapsed time.Duration) {
	ns := uint64(max(elapsed, 0))
	us := ns / 1000
	if ns%1000 != 0 {
		us++
	}
	us = max(uint64(1), us)
	exponent := bits.Len64(us) - 1
	base := uint64(1) << exponent
	index := exponent*16 + int((us-base)*16/base)
	h.buckets[index]++
	h.count++
	h.maximum = max(h.maximum, us)
}

func (h *histogram) merge(other *histogram) {
	for i, count := range other.buckets {
		h.buckets[i] += count
	}
	h.count += other.count
	h.maximum = max(h.maximum, other.maximum)
}

func (h *histogram) percentile(percent uint64) uint64 {
	if h.count == 0 {
		return 0
	}
	want, count := (h.count*percent+99)/100, uint64(0)
	for i, n := range h.buckets {
		count += n
		if count >= want {
			base := uint64(1) << (i / 16)
			return base + (uint64(i%16+1)*base+15)/16 - 1
		}
	}
	return h.maximum
}

type latency struct {
	Count uint64 `json:"count"`
	P50US uint64 `json:"p50_us_upper"`
	P95US uint64 `json:"p95_us_upper"`
	P99US uint64 `json:"p99_us_upper"`
	MaxUS uint64 `json:"max_us_rounded"`
}

func (h *histogram) report() latency {
	return latency{h.count, h.percentile(50), h.percentile(95), h.percentile(99), h.maximum}
}

type measurements struct {
	attempted      uint64
	completed      uint64
	asyncForwarded uint64
	errors         map[string]uint64
	routes         [4]uint64
	all            histogram
	local          histogram
	remote         histogram
}

func (m *measurements) merge(other *measurements) {
	m.attempted += other.attempted
	m.completed += other.completed
	m.asyncForwarded += other.asyncForwarded
	for key, count := range other.errors {
		if m.errors == nil {
			m.errors = make(map[string]uint64)
		}
		m.errors[key] += count
	}
	for i, count := range other.routes {
		m.routes[i] += count
	}
	m.all.merge(&other.all)
	m.local.merge(&other.local)
	m.remote.merge(&other.remote)
}

func (m *measurements) report() any {
	return struct {
		Attempted      uint64            `json:"attempted"`
		Completed      uint64            `json:"completed_validated"`
		AsyncForwarded uint64            `json:"async_forwarded"`
		Errors         map[string]uint64 `json:"errors"`
		Routes         map[string]uint64 `json:"routes"`
		All            latency           `json:"all"`
		Local          latency           `json:"local"`
		Remote         latency           `json:"remote"`
	}{m.attempted, m.completed, m.asyncForwarded, m.errors, map[string]uint64{
		"local_hot": m.routes[0], "local_spread": m.routes[1],
		"remote_hot": m.routes[2], "remote_spread": m.routes[3],
	}, m.all.report(), m.local.report(), m.remote.report()}
}

type observation struct {
	Name    string `json:"name"`
	Class   string `json:"class"`
	Outcome string `json:"delivery_outcome,omitempty"`
	Error   string `json:"error,omitempty"`
}
type probeResult struct {
	Name         string         `json:"name"`
	Expected     string         `json:"expected"`
	Passed       bool           `json:"passed"`
	Observations []observation  `json:"observations"`
	Details      map[string]any `json:"details,omitempty"`
	Failure      string         `json:"failure,omitempty"`
}

func outcome(err error) string {
	var remote *actor.RemoteError
	if !errors.As(err, &remote) {
		return ""
	}
	switch remote.Outcome {
	case actor.NotSent:
		return "not_sent"
	case actor.Rejected:
		return "rejected"
	case actor.Unknown:
		return "unknown"
	case actor.Responded:
		return "responded"
	}
	return "invalid"
}

func errorClass(err error) string {
	if err == nil {
		return "none"
	}
	kind := "other"
	for _, test := range []struct {
		err  error
		name string
	}{
		{context.Canceled, "canceled"}, {context.DeadlineExceeded, "deadline_exceeded"},
		{actor.ErrMailboxFull, "mailbox_full"}, {actor.ErrStalePID, "stale_pid"},
		{actor.ErrNoProcess, "no_process"}, {actor.ErrProcessCrashed, "process_crashed"},
		{actor.ErrNodeUnreachable, "node_unreachable"}, {actor.ErrRemoteUnavailable, "remote_unavailable"},
		{actor.ErrClosed, "closed"}, {actor.ErrStopped, "stopped"},
		{actor.ErrStarting, "starting"}, {actor.ErrRestarting, "restarting"},
		{actor.ErrCapacity, "capacity"}, {actor.ErrReplyExpired, "reply_expired"},
		{actor.ErrAlreadyReplied, "already_replied"},
	} {
		if errors.Is(err, test.err) {
			kind = test.name
			break
		}
	}
	if delivery := outcome(err); delivery != "" {
		return "remote_" + delivery + "/" + kind
	}
	return kind
}

func observe(name string, err error) observation {
	result := observation{Name: name, Class: errorClass(err), Outcome: outcome(err)}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

type config struct {
	duration time.Duration
	workers  int
	actors   int
}
type nodeSample struct {
	State   string             `json:"state"`
	Stats   actor.Stats        `json:"node"`
	Network actor.NetworkStats `json:"network"`
}
type runner struct {
	ctx           context.Context
	cfg           config
	encoder       *json.Encoder
	started       time.Time
	client        *actor.Node
	server        *actor.Node
	bridgePID     actor.PID
	closed        map[string]nodeSample
	remoteAddress string
	remotePID     actor.PID
	flightPID     actor.PID
	flight        *flightGate
	oldRemotePID  actor.PID
	oldExecutions int32
	sequence      atomic.Uint64
	total         measurements
	probes        []probeResult
}

func (r *runner) emit(kind string, data any) error {
	return r.encoder.Encode(struct {
		Type      string    `json:"type"`
		Time      time.Time `json:"time"`
		ElapsedMS int64     `json:"elapsed_ms"`
		Data      any       `json:"data"`
	}{kind, time.Now().UTC(), time.Since(r.started).Milliseconds(), data})
}

func (r *runner) sample(stage string, forcedGC bool, interval time.Duration) error {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	nodes := make(map[string]nodeSample)
	for key, node := range map[string]*actor.Node{"local": r.client, "remote": r.server} {
		if node != nil {
			nodes[key] = nodeSample{"live", node.Stats(), node.NetworkStats()}
		} else if last, ok := r.closed[key]; ok {
			nodes[key] = last
		}
	}
	return r.emit("resources", struct {
		Stage       string                `json:"stage"`
		ForcedGC    bool                  `json:"forced_gc"`
		IntervalMS  int64                 `json:"sample_interval_ms"`
		Goroutines  int                   `json:"goroutines"`
		HeapAlloc   uint64                `json:"heap_alloc_bytes"`
		HeapInuse   uint64                `json:"heap_inuse_bytes"`
		HeapObjects uint64                `json:"heap_objects"`
		StackInuse  uint64                `json:"stack_inuse_bytes"`
		TotalAlloc  uint64                `json:"total_alloc_bytes"`
		Mallocs     uint64                `json:"mallocs"`
		Frees       uint64                `json:"frees"`
		NumGC       uint32                `json:"num_gc"`
		Nodes       map[string]nodeSample `json:"nodes"`
	}{stage, forcedGC, interval.Milliseconds(), runtime.NumGoroutine(), memory.HeapAlloc, memory.HeapInuse,
		memory.HeapObjects, memory.StackInuse, memory.TotalAlloc, memory.Mallocs, memory.Frees, memory.NumGC, nodes})
}

func (r *runner) options(name string) actor.NodeOptions {
	return actor.NodeOptions{
		Name: name, MaxActors: r.cfg.actors + 32, MailboxSize: max(64, 2*r.cfg.workers),
		MaxPendingCalls: max(1024, 16*r.cfg.workers), CallTimeout: 5 * time.Second,
		Logger:  slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Network: actor.NetworkOptions{AllowInsecureLoopback: true},
		Register: []actor.MessageType{
			{Name: "soak.work", Version: 1, Sample: work{}},
			{Name: "soak.reply", Version: 1, Sample: reply{}},
		},
	}
}

func (r *runner) spawnPool(node *actor.Node) (actor.PID, error) {
	var first actor.PID
	for i := range r.cfg.actors {
		pid, err := node.Spawn(func() actor.Behavior { return &service{} },
			actor.WithName(fmt.Sprintf("worker-%d", i)), actor.WithRestart(actor.Temporary))
		if err != nil {
			return actor.PID{}, err
		}
		if i == 0 {
			first = pid
		}
	}
	return first, nil
}

func (r *runner) startRemote(address string) error {
	var err error
	r.server, err = actor.StartNode(address, r.options("soak-remote"))
	if err != nil {
		return err
	}
	r.remoteAddress = r.server.ListenAddr()
	r.remotePID, err = r.spawnPool(r.server)
	if err != nil {
		return err
	}
	gate := &flightGate{entered: make(chan struct{}, 1), replyError: make(chan error, 1)}
	r.flight = gate
	r.flightPID, err = r.server.Spawn(func() actor.Behavior { return &service{flight: gate} },
		actor.WithName("flight"), actor.WithRestart(actor.Temporary))
	return err
}

func (r *runner) message() work {
	sequence := r.sequence.Add(1)
	return work{sequence, sequence ^ 0xa0761d6478bd642f, workloadPayload, "work"}
}

func validate(message work, result reply, node string, boot [16]byte, path string) error {
	if result.Sequence != message.Sequence || result.Checksum != checksum(message) ||
		result.PID.Node != node || result.PID.BootID != boot || result.PID.ID == 0 || path != "" && result.Path != path {
		return fmt.Errorf("invalid response: sequence=%d got=%d checksum=%x pid=%s path=%s expected_node=%s expected_path=%s",
			message.Sequence, result.Sequence, result.Checksum, result.PID, result.Path, node, path)
	}
	return nil
}

func (r *runner) checked(ctx context.Context, target actor.Addr, node string, boot [16]byte) (reply, error) {
	message := r.message()
	result, err := r.client.Call[reply](ctx, target, message)
	if err == nil {
		path := ""
		if ref, ok := target.(actor.Ref); ok {
			path = ref.Path
		}
		err = validate(message, result, node, boot, path)
	}
	return result, err
}

func (r *runner) load(name string, duration time.Duration, useRemote bool, fault func() error) error {
	if err := r.sample(name+"/start", false, 0); err != nil {
		return err
	}
	localBoot := r.client.BootID()
	var remoteBoot [16]byte
	if r.server != nil {
		remoteBoot = r.server.BootID()
	}
	paths := make([]string, r.cfg.actors)
	for i := range paths {
		paths[i] = fmt.Sprintf("worker-%d", i)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	stopLoad := sync.OnceFunc(func() { close(stop) })
	failures := make(chan error, 1)
	workers := make([]measurements, r.cfg.workers)
	var group sync.WaitGroup
	started := time.Now()
	timer, ticker := time.NewTimer(duration), time.NewTicker(sampleEvery)
	defer timer.Stop()
	defer ticker.Stop()
	for index := range workers {
		group.Go(func() {
			m := &workers[index]
			state := uint64(index+1) * 0x9e3779b185ebca87
			for {
				select {
				case <-stop:
					return
				default:
				}
				state = state*6364136223846793005 + 1442695040888963407
				remote := useRemote && state>>32&1 != 0
				hot := (state>>40)%10 < 7
				targetIndex := 0
				if !hot {
					targetIndex = int((state >> 16) % uint64(r.cfg.actors))
				}
				node, boot, route := "soak-local", localBoot, 0
				if remote {
					node, boot, route = "soak-remote", remoteBoot, 2
				}
				if !hot {
					route++
				}
				message := r.message()
				target := actor.Ref{Node: node, Path: paths[targetIndex]}
				forward := m.attempted%4 == 0
				begin := time.Now()
				var result reply
				var err error
				if forward {
					result, err = r.client.Call[reply](r.ctx, r.bridgePID, forwardCall{target, message})
				} else {
					result, err = r.client.Call[reply](r.ctx, target, message)
				}
				elapsed := time.Since(begin)
				kind := errorClass(err)
				if err == nil {
					if err = validate(message, result, node, boot, paths[targetIndex]); err != nil {
						kind = "validation"
					}
				}
				m.attempted++
				m.routes[route]++
				m.all.add(elapsed)
				if remote {
					m.remote.add(elapsed)
				} else {
					m.local.add(elapsed)
				}
				if err != nil {
					m.errors = map[string]uint64{kind: 1}
					select {
					case failures <- fmt.Errorf("%s request %d: %w", name, message.Sequence, err):
					default:
					}
					stopLoad()
					return // Unknown and all other failed requests are never replayed.
				}
				m.completed++
				if forward {
					m.asyncForwarded++
				}
			}
		})
	}
	go func() { group.Wait(); close(done) }()
	var failure error
	if fault != nil {
		failure = fault() // The workers remain active while a bounded probe runs.
	}
	if failure == nil {
	loop:
		for {
			select {
			case failure = <-failures:
				break loop
			case <-r.ctx.Done():
				failure = r.ctx.Err()
				break loop
			case <-timer.C:
				break loop
			case <-ticker.C:
				if failure = r.sample(name+"/running", false, sampleEvery); failure != nil {
					break loop
				}
			}
		}
	}
	stopLoad()
	<-done // Stop offering new work; do not cancel an accepted phase-end Call.
	select {
	case err := <-failures:
		failure = errors.Join(failure, err)
	default:
	}
	var phase measurements
	for i := range workers {
		phase.merge(&workers[i])
	}
	r.total.merge(&phase)
	failure = errors.Join(failure, r.emit("phase", struct {
		Name       string `json:"name"`
		DurationMS int64  `json:"duration_ms"`
		Results    any    `json:"results"`
	}{name, time.Since(started).Milliseconds(), phase.report()}))
	return errors.Join(failure, r.sample(name+"/end", false, 0))
}

func await[T any](ctx context.Context, channel <-chan T) (T, error) {
	select {
	case value := <-channel:
		return value, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func waitUntil(ctx context.Context, condition func() bool) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (r *runner) probe(name, expected string, action func(*probeResult) error) error {
	result := probeResult{Name: name, Expected: expected}
	err := action(&result)
	result.Passed = err == nil
	if err != nil {
		result.Failure = err.Error()
	}
	r.probes = append(r.probes, result)
	return errors.Join(err, r.emit("probe", result))
}

func (r *runner) narrowProbes() error {
	processed := make(chan struct{}, 1)
	pid, err := r.client.Spawn(func() actor.Behavior { return &probeActor{processed: processed} },
		actor.WithMailboxSize(1), actor.WithRestart(actor.Temporary))
	if err != nil {
		return err
	}
	if err = r.probe("cancellation", "Canceled caller; late Reply returns ErrReplyExpired", func(p *probeResult) error {
		ctx, cancel := context.WithTimeout(r.ctx, operationTimeout)
		defer cancel()
		caller, cancelCall := context.WithCancel(ctx)
		defer cancelCall()
		gate := localGate{make(chan struct{}), make(chan struct{}), make(chan error, 1)}
		release := sync.OnceFunc(func() { close(gate.release) })
		defer release()
		completed := make(chan error, 1)
		go func() { _, err := r.client.Call[int64](caller, pid, gate); completed <- err }()
		if _, err := await(ctx, gate.entered); err != nil {
			return err
		}
		cancelCall()
		callErr, err := await(ctx, completed)
		if err != nil {
			return err
		}
		p.Observations = append(p.Observations, observe("canceled_call", callErr))
		if !errors.Is(callErr, context.Canceled) {
			return fmt.Errorf("canceled Call returned %v", callErr)
		}
		release()
		replyErr, err := await(ctx, gate.replied)
		if err != nil {
			return err
		}
		p.Observations = append(p.Observations, observe("late_reply", replyErr))
		if !errors.Is(replyErr, actor.ErrReplyExpired) {
			return fmt.Errorf("late Reply returned %v", replyErr)
		}
		return nil
	}); err != nil {
		return err
	}
	if err = r.probe("mailbox_full", "One queued tick accepted; excess tick rejected and never executed", func(p *probeResult) error {
		ctx, cancel := context.WithTimeout(r.ctx, operationTimeout)
		defer cancel()
		gate := localGate{entered: make(chan struct{}), release: make(chan struct{})}
		release := sync.OnceFunc(func() { close(gate.release) })
		defer release()
		if err := r.client.Send(pid, gate); err != nil {
			return err
		}
		if _, err := await(ctx, gate.entered); err != nil {
			return err
		}
		if err := r.client.Send(pid, tick{}); err != nil {
			return fmt.Errorf("first mailbox slot: %w", err)
		}
		err := r.client.Send(pid, tick{})
		p.Observations = append(p.Observations, observe("excess_send", err))
		if !errors.Is(err, actor.ErrMailboxFull) {
			return fmt.Errorf("excess mailbox Send returned %v", err)
		}
		release()
		if _, err := await(ctx, processed); err != nil {
			return err
		}
		count, err := r.client.Call[int64](ctx, pid, readTicks{})
		if err != nil || count != 1 {
			return fmt.Errorf("accepted mailbox work: count=%d error=%v", count, err)
		}
		return nil
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(r.ctx, operationTimeout)
	defer cancel()
	return r.client.StopActor(ctx, pid)
}

func (r *runner) panicProbe() error {
	return r.probe("local_panic_recovery", "ProcessCrashed; old PID revoked; the same Ref reaches a new PID", func(p *probeResult) error {
		ctx, cancel := context.WithTimeout(r.ctx, operationTimeout)
		defer cancel()
		ready := make(chan actor.PID, 2)
		old, err := r.client.Spawn(func() actor.Behavior { return &service{announce: ready} },
			actor.WithName("crash-probe"), actor.WithRestart(actor.Permanent))
		if err != nil {
			return err
		}
		if _, err := await(ctx, ready); err != nil {
			return err
		}
		message := r.message()
		message.Op = "panic"
		_, err = r.client.Call[reply](ctx, old, message)
		p.Observations = append(p.Observations, observe("panic_call", err))
		if !errors.Is(err, actor.ErrProcessCrashed) {
			return fmt.Errorf("planned panic returned %v", err)
		}
		fresh, err := await(ctx, ready)
		if err != nil {
			return err
		}
		if fresh == old {
			return errors.New("supervision reused the failed PID")
		}
		_, err = r.checked(ctx, old, r.client.Name(), r.client.BootID())
		p.Observations = append(p.Observations, observe("old_pid", err))
		if !errors.Is(err, actor.ErrNoProcess) {
			return fmt.Errorf("old local PID returned %v", err)
		}
		result, err := r.checked(ctx, actor.Ref{Path: "crash-probe"}, r.client.Name(), r.client.BootID())
		if err != nil {
			return err
		}
		if result.PID != fresh {
			return errors.New("Ref did not reach the supervised replacement")
		}
		p.Details = map[string]any{"old_pid": old.String(), "new_pid": fresh.String()}
		return r.sample("local_panic_recovered", false, 0)
	})
}

func (r *runner) stopNode(name string) error {
	node := r.server
	if name == "local" {
		node = r.client
	}
	if node == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	err := node.Stop(ctx)
	stats := node.Stats()
	r.closed[name] = nodeSample{"stopped_snapshot", stats, node.NetworkStats()}
	if name == "local" {
		r.client = nil
	} else {
		r.server = nil
	}
	if stats.Actors != 0 || stats.PendingCalls != 0 || stats.AsyncRequests != 0 || stats.QueuedMessages != 0 {
		err = errors.Join(err, fmt.Errorf("stopped %s node retains actors=%d pending=%d async=%d queued=%d", name, stats.Actors, stats.PendingCalls, stats.AsyncRequests, stats.QueuedMessages))
	}
	return err
}

func (r *runner) stopRemoteProbe() error {
	return r.probe("remote_stop_inflight", "An admitted unreplied Call fails as Responded/Stopped or Unknown; no replay", func(p *probeResult) error {
		ctx, cancel := context.WithTimeout(r.ctx, 2*operationTimeout)
		defer cancel()
		gate := r.flight
		message := r.message()
		message.Op = "hold"
		completed := make(chan error, 1)
		go func() { _, err := r.client.Call[reply](ctx, r.flightPID, message); completed <- err }()
		if _, err := await(ctx, gate.entered); err != nil {
			return err
		}
		r.oldRemotePID = r.remotePID
		if err := r.stopNode("remote"); err != nil {
			return err
		}
		callErr, err := await(ctx, completed)
		if err != nil {
			return err
		}
		p.Observations = append(p.Observations, observe("admitted_inflight_call", callErr))
		var remote *actor.RemoteError
		if callErr == nil || errors.Is(callErr, context.DeadlineExceeded) || !errors.As(callErr, &remote) ||
			(remote.Outcome != actor.Unknown && !(remote.Outcome == actor.Responded && errors.Is(callErr, actor.ErrStopped))) {
			return fmt.Errorf("inflight stop outcome: %v", callErr)
		}
		replyErr, err := await(ctx, gate.replyError)
		if err != nil {
			return err
		}
		p.Observations = append(p.Observations, observe("stop_reply_attempt", replyErr))
		if replyErr != nil && !errors.Is(replyErr, actor.ErrReplyExpired) {
			return fmt.Errorf("shutdown ReplyError returned %v", replyErr)
		}
		r.oldExecutions = gate.executions.Load()
		if r.oldExecutions != 1 {
			return fmt.Errorf("inflight Call executed %d times", r.oldExecutions)
		}
		// Stop does not synchronously deliver EOF to the peer. Wait on public
		// connection state; never retry a business Call to discover disconnect.
		if err := waitUntil(ctx, func() bool { return r.client.NetworkStats().Endpoint.Sessions == 0 }); err != nil {
			return err
		}
		_, err = r.checked(ctx, r.oldRemotePID, "soak-remote", r.oldRemotePID.BootID)
		p.Observations = append(p.Observations, observe("remote_down_call", err))
		if err == nil || outcome(err) != "not_sent" ||
			(!errors.Is(err, actor.ErrNodeUnreachable) && !errors.Is(err, actor.ErrRemoteUnavailable)) {
			return fmt.Errorf("stopped remote accepted or ambiguously delivered a new Call: %v", err)
		}
		p.Details = map[string]any{"executions": r.oldExecutions, "address": r.remoteAddress}
		return r.sample("remote_stopped", false, 0)
	})
}

func (r *runner) restartRemoteProbe() error {
	return r.probe("remote_same_address_restart", "New BootID, old PID NotSent/StalePID, same Ref recovers", func(p *probeResult) error {
		ctx, cancel := context.WithTimeout(r.ctx, operationTimeout)
		defer cancel()
		address := r.remoteAddress
		if err := r.startRemote(address); err != nil {
			return err
		}
		if r.server.BootID() == r.oldRemotePID.BootID || r.server.ListenAddr() != address {
			return errors.New("remote restart did not change BootID at the same address")
		}
		if err := r.client.Connect(ctx, "soak-remote", address); err != nil {
			return err
		}
		_, err := r.checked(ctx, r.oldRemotePID, "soak-remote", r.oldRemotePID.BootID)
		p.Observations = append(p.Observations, observe("old_boot_pid", err))
		if !errors.Is(err, actor.ErrStalePID) || outcome(err) != "not_sent" {
			return fmt.Errorf("old remote PID outcome: %v", err)
		}
		result, err := r.checked(ctx, actor.Ref{Node: "soak-remote", Path: "worker-0"}, "soak-remote", r.server.BootID())
		if err != nil {
			return err
		}
		if result.PID != r.remotePID || r.flight.executions.Load() != 0 {
			return errors.New("remote Ref generation mismatch or an inflight Call replayed")
		}
		p.Details = map[string]any{"old_pid": r.oldRemotePID.String(), "new_pid": result.PID.String(), "address": address}
		return r.sample("remote_reconnected", false, 0)
	})
}

func (r *runner) run() error {
	if err := r.emit("configuration", map[string]any{
		"duration": r.cfg.duration.String(), "workers": r.cfg.workers, "actors_per_node": r.cfg.actors,
		"transport":  "same-process loopback TCP; lifecycle/reclamation validation, not cross-machine capacity",
		"go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "sample_every": sampleEvery.String(),
		"latency_algorithm":  "round up to 1us; fixed 1024 buckets, 16 subdivisions per power of two; percentile upper bounds",
		"latency_population": "all completed normal-load attempts, including errors; fault probes are separate",
		"async_forwarding":   "one in four calls per worker uses one local Request/DeferredReply bridge; latency includes forwarding",
	}); err != nil {
		return err
	}
	if err := r.sample("before_nodes", false, 0); err != nil {
		return err
	}
	var err error
	r.client, err = actor.StartNode("127.0.0.1:0", r.options("soak-local"))
	if err != nil {
		return err
	}
	if _, err := r.spawnPool(r.client); err != nil {
		return err
	}
	r.bridgePID, err = r.client.Spawn(func() actor.Behavior { return &bridge{} },
		actor.WithName("async-bridge"), actor.WithRestart(actor.Temporary))
	if err != nil {
		return err
	}
	if err := r.startRemote("127.0.0.1:0"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(r.ctx, operationTimeout)
	err = r.client.Connect(ctx, "soak-remote", r.remoteAddress)
	cancel()
	if err != nil {
		return err
	}
	if err := r.narrowProbes(); err != nil {
		return err
	}
	part := r.cfg.duration / 4
	if err := r.load("mixed_before_faults", part, true, nil); err != nil {
		return err
	}
	if err := r.load("mixed_local_restart", part, true, r.panicProbe); err != nil {
		return err
	}
	if err := r.load("local_remote_stopped", part, false, r.stopRemoteProbe); err != nil {
		return err
	}
	if err := r.restartRemoteProbe(); err != nil {
		return err
	}
	if err := r.load("mixed_remote_recovered", r.cfg.duration-3*part, true, nil); err != nil {
		return err
	}
	if r.flight.executions.Load() != 0 {
		return errors.New("the admitted old-generation Call replayed after recovery")
	}
	return nil
}

func (r *runner) shutdown() error {
	err := errors.Join(r.stopNode("remote"), r.stopNode("local"))
	err = errors.Join(err, r.sample("nodes_closed", false, 0))
	// Node references have been dropped. Closed node fields below are value
	// snapshots; they do not keep executors, sessions or buffers alive.
	for i := range 3 {
		interval := time.Duration(0)
		if i != 0 {
			interval = time.Second
			<-time.After(interval)
		}
		runtime.GC()
		err = errors.Join(err, r.sample(fmt.Sprintf("after_gc_%d", i), true, interval))
	}
	return err
}

func main() {
	duration := flag.Duration("duration", 2*time.Minute, "sum of four load windows; setup/reconnect/shutdown add time")
	workers := flag.Int("workers", 8, "concurrent callers, 1..256")
	actors := flag.Int("actors", 8, "workload Actors per node, 1..256")
	flag.Parse()
	if *duration < 4*time.Second || *workers < 1 || *workers > 256 || *actors < 1 || *actors > 256 {
		fmt.Fprintln(os.Stderr, "duration must be >=4s; workers and actors must be in 1..256")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	r := &runner{ctx: ctx, cfg: config{*duration, *workers, *actors}, encoder: json.NewEncoder(os.Stdout), started: time.Now(), closed: make(map[string]nodeSample)}
	err := r.run()
	err = errors.Join(err, r.shutdown())
	status, failure := "passed", ""
	if err != nil {
		status, failure = "failed", err.Error()
	}
	err = errors.Join(err, r.emit("summary", map[string]any{
		"status": status, "failure": failure, "normal_load": r.total.report(), "probes": r.probes,
		"elapsed_ms": time.Since(r.started).Milliseconds(),
	}))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
