package erlangactor_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
	erlangactor "github.com/sllt/tao/actor/erlang"
	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
)

var (
	beamBuildOnce sync.Once
	beamOutDir    string
	beamBuildErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if beamOutDir != "" {
		_ = os.RemoveAll(beamOutDir)
	}
	os.Exit(code)
}

func ensureBEAMFixture(t *testing.T) string {
	t.Helper()
	for _, bin := range []string{"erl", "erlc", "epmd"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not found in PATH", bin)
		}
	}

	beamBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "tao-actor-erlang-fixture-*")
		if err != nil {
			beamBuildErr = err
			return
		}
		src := filepath.Join("..", "..", "erlang", "testdata", "beam", "tao_test_server.erl")
		cmd := exec.Command("erlc", "-Wall", "-o", dir, src)
		cmd.Env = append(os.Environ(), "ERL_CRASH_DUMP=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			beamBuildErr = fmt.Errorf("erlc failed: %w: %s", err, string(out))
			return
		}
		beamOutDir = dir
	})
	if beamBuildErr != nil {
		t.Fatalf("build BEAM fixture: %v", beamBuildErr)
	}
	return beamOutDir
}

type isolatedEPMD struct {
	addr string
	port int
	cmd  *exec.Cmd
}

func startIsolatedEPMD(t *testing.T) *isolatedEPMD {
	t.Helper()
	if _, err := exec.LookPath("epmd"); err != nil {
		t.Skip("epmd not found in PATH")
	}
	for attempt := 0; attempt < 16; attempt++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve epmd port: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		cmd := exec.Command("epmd", "-address", "127.0.0.1", "-port", strconv.Itoa(port))
		cmd.Env = append(os.Environ(), "ERL_CRASH_DUMP=/dev/null")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start epmd: %v", err)
		}

		ready := false
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				break
			}
			c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err == nil {
				_ = c.Close()
				ready = true
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		if ready {
			ep := &isolatedEPMD{addr: addr, port: port, cmd: cmd}
			t.Cleanup(func() { ep.Stop() })
			return ep
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	t.Fatal("failed to start isolated epmd")
	return nil
}

func (e *isolatedEPMD) Stop() {
	if e == nil || e.cmd == nil || e.cmd.Process == nil {
		return
	}
	_ = e.cmd.Process.Kill()
	_, _ = e.cmd.Process.Wait()
}

type beamServer struct {
	fullName string
	cmd      *exec.Cmd
	done     chan struct{}
}

func startBEAMServer(t *testing.T, aliveName, cookie string, epmdPort int) *beamServer {
	t.Helper()
	ebinDir := ensureBEAMFixture(t)
	fullName := aliveName + "@127.0.0.1"
	args := []string{
		"-noinput",
		"-name", fullName,
		"-setcookie", cookie,
		"-epmd_port", strconv.Itoa(epmdPort),
		"-pa", ebinDir,
		"-eval", `{ok, _} = tao_test_server:start_link(), io:format("TAO_BEAM_READY~n"), receive stop_vm -> halt(0) end.`,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "erl", args...)
	cmd.Env = append(os.Environ(),
		"ERL_CRASH_DUMP=/dev/null",
		"ERL_EPMD_PORT="+strconv.Itoa(epmdPort),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start erl (%s): %v", fullName, err)
	}

	readyCh := make(chan error, 1)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "TAO_BEAM_READY" {
				select {
				case readyCh <- nil:
				default:
				}
			}
		}
		waitErr := cmd.Wait()
		select {
		case readyCh <- fmt.Errorf("erl exited before ready (%v): %s", waitErr, stderrBuf.String()):
		default:
		}
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			cancel()
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatalf("timed out waiting for BEAM %s; stderr=%s", fullName, stderrBuf.String())
	}

	srv := &beamServer{fullName: fullName, cmd: cmd, done: doneCh}
	t.Cleanup(func() {
		cancel()
		if srv.cmd != nil && srv.cmd.Process != nil {
			_ = srv.cmd.Process.Kill()
			<-srv.done
		}
	})
	return srv
}

type counterCmd struct {
	op    string
	delta int64
	sleep time.Duration
}

type counterSnapshot struct {
	val          int64
	lastPeer     string
	lastHasFrom  bool
	senderWasNil bool
	infos        []string
}

func TestBridgeInboundOrderSchemaBudgetAndSourceEnvelope(t *testing.T) {
	epmdDaemon := startIsolatedEPMD(t)
	cookie := "bridge-order-cookie"
	beam := startBEAMServer(t, "beam_order", cookie, epmdDaemon.port)

	// Configure a short 40ms default Tao CallTimeout to prove inbound Erlang calls
	// via BeginCall are NOT truncated by NodeOptions.CallTimeout (§8.3, §13)!
	taoNode, err := actor.NewNode(actor.NodeOptions{
		Name:        "tao_order",
		CallTimeout: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	erlNode, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "tao_erl_order@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = erlNode.Close() }()

	bridge, err := erlangactor.Attach(taoNode, erlNode)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	var snap counterSnapshot
	counterPID, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch m := msg.(type) {
		case actor.Started, actor.Stopped:
			return nil
		case erlangactor.Inbound:
			snap.lastPeer = m.Peer
			snap.lastHasFrom = m.HasFrom
			snap.senderWasNil = a.Sender() == nil
			cmd, ok := m.Message.(counterCmd)
			if !ok {
				return nil
			}
			switch cmd.op {
			case "add":
				snap.val += cmd.delta
				return nil
			case "info":
				snap.infos = append(snap.infos, fmt.Sprintf("info:%d", cmd.delta))
				return nil
			case "get":
				return a.Reply(snap)
			case "sleep_get":
				time.Sleep(cmd.sleep)
				_ = a.Reply(snap)
				return nil
			case "noreply":
				return nil
			}
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	schema := erlangactor.Funcs{
		Call: func(term etf.Term) (any, error) {
			switch v := term.(type) {
			case etf.Atom:
				switch v {
				case "get":
					return counterCmd{op: "get"}, nil
				case "noreply":
					return counterCmd{op: "noreply"}, nil
				}
			case etf.Tuple:
				if len(v) == 2 && v[0] == etf.Atom("sleep_get") {
					ms, ok := v[1].(int64)
					if ok {
						return counterCmd{op: "sleep_get", sleep: time.Duration(ms) * time.Millisecond}, nil
					}
				}
			}
			return nil, errors.New("unsupported call shape")
		},
		Cast: func(term etf.Term) (any, error) {
			tup, ok := term.(etf.Tuple)
			if !ok || len(tup) != 2 || tup[0] != etf.Atom("add") {
				return nil, errors.New("unsupported cast shape")
			}
			d, ok := tup[1].(int64)
			if !ok {
				return nil, errors.New("bad delta")
			}
			return counterCmd{op: "add", delta: d}, nil
		},
		Info: func(term etf.Term) (any, error) {
			tup, ok := term.(etf.Tuple)
			if !ok || len(tup) != 2 || tup[0] != etf.Atom("note") {
				return nil, errors.New("unsupported info shape")
			}
			d, ok := tup[1].(int64)
			if !ok {
				return nil, errors.New("bad note")
			}
			return counterCmd{op: "info", delta: d}, nil
		},
		Reply: func(value any, err error) (etf.Term, error) {
			if err != nil {
				return erlangactor.DefaultEncodeReply(nil, err)
			}
			cs := value.(counterSnapshot)
			return etf.Tuple{
				etf.Atom("ok"),
				cs.val,
				etf.UTF8Binary(cs.lastPeer),
				etf.BoolAtom(cs.lastHasFrom),
				etf.BoolAtom(cs.senderWasNil),
				int64(len(cs.infos)),
			}, nil
		},
	}

	_, err = bridge.Export("counter", counterPID, schema, erlangactor.WithSourceEnvelope(true))
	if err != nil {
		t.Fatal(err)
	}
	_, err = bridge.Export("counter_short_budget", counterPID, schema,
		erlangactor.WithSourceEnvelope(true),
		erlangactor.WithInboundCallTimeout(40*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	cli, err := erlNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	beamSrv := erlang.Registered("tao_test_server", beam.fullName)
	erlCounter := etf.Tuple{etf.Atom("counter"), etf.Atom(erlNode.Name())}
	erlCounterShort := etf.Tuple{etf.Atom("counter_short_budget"), etf.Atom(erlNode.Name())}

	// 1. Plain '!' info followed by single-process cast(1)+cast(2)+cast(3)+call(get) (§9.1, §9.3).
	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("send_raw"),
		erlCounter,
		etf.Tuple{etf.Atom("note"), int64(9)},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("cast_then_call"),
		erlCounter,
		etf.ProperList(
			etf.Tuple{etf.Atom("add"), int64(10)},
			etf.Tuple{etf.Atom("add"), int64(20)},
			etf.Tuple{etf.Atom("add"), int64(12)},
		),
		etf.Atom("get"),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := etf.Tuple{
		etf.Atom("ok"),
		etf.Tuple{
			etf.Atom("ok"),
			int64(42),
			etf.UTF8Binary(beam.fullName),
			etf.Atom("true"),
			etf.Atom("true"),
			int64(1),
		},
	}
	if !etf.TermEqual(res, want) {
		t.Fatalf("cast_then_call result = %#v, want %#v", res, want)
	}

	// 2. Default no-5s/40ms truncation: sleep 120ms (> taoNode.CallTimeout=40ms) succeeds!
	resSleep, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlCounter,
		etf.Tuple{etf.Atom("sleep_get"), int64(120)},
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	tupSleep, ok := resSleep.(etf.Tuple)
	if !ok || len(tupSleep) != 2 || tupSleep[0] != etf.Atom("ok") {
		t.Fatalf("expected >CallTimeout inbound call to succeed, got %#v", resSleep)
	}

	// 3. Explicit WithInboundCallTimeout(40ms) cancels and replies {error, {tao, timeout, ...}}.
	resTimeout, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlCounterShort,
		etf.Tuple{etf.Atom("sleep_get"), int64(200)},
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isWrappedTaoErrorCode(resTimeout, "timeout") {
		t.Fatalf("WithInboundCallTimeout result = %#v, want {ok, {error, {tao, timeout, _}}}", resTimeout)
	}

	// 4. Handler returning without Reply/Defer -> {error, {tao, no_reply, ...}}.
	resNoReply, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlCounter,
		etf.Atom("noreply"),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isWrappedTaoErrorCode(resNoReply, "no_reply") {
		t.Fatalf("NoReply result = %#v, want {ok, {error, {tao, no_reply, _}}}", resNoReply)
	}

	// 5. Schema DecodeCall rejection -> {error, {tao, call_error, ...}} & DecodeErrors++.
	resBad, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlCounter,
		etf.Tuple{etf.Atom("unknown_shape"), int64(1)},
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isWrappedTaoErrorCode(resBad, "call_error") {
		t.Fatalf("DecodeCall error result = %#v, want {ok, {error, {tao, call_error, _}}}", resBad)
	}
	if bridge.Stats().DecodeErrors == 0 {
		t.Fatalf("expected DecodeErrors > 0, got %+v", bridge.Stats())
	}
}

func TestBridgeReplyBeforeDownDeferredStopAndBusinessReplyError(t *testing.T) {
	epmdDaemon := startIsolatedEPMD(t)
	cookie := "bridge-reply-down-cookie"
	beam := startBEAMServer(t, "beam_rdown", cookie, epmdDaemon.port)

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "tao_rdown"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	erlNode, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "tao_erl_rdown@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = erlNode.Close() }()

	bridge, err := erlangactor.Attach(taoNode, erlNode)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	cli, err := erlNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	beamSrv := erlang.Registered("tao_test_server", beam.fullName)

	// Part A: Reply-then-Stop in the same Receive turn (§9.3) + uncompleted Defer failed on Stop!
	deferredCh := make(chan actor.DeferredReply, 1)
	ephemeralPID, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch msg {
		case etf.Atom("defer_forever"):
			deferredCh <- a.Defer()
			return nil
		case etf.Atom("reply_and_stop"):
			_ = a.Reply(etf.Tuple{etf.Atom("final"), int64(777)})
			a.Stop(nil)
			return nil
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.Export("ephemeral", ephemeralPID, erlangactor.PassthroughSchema()); err != nil {
		t.Fatal(err)
	}
	erlEphemeral := etf.Tuple{etf.Atom("ephemeral"), etf.Atom(erlNode.Name())}

	// Install BEAM watcher on {ephemeral, Node}.
	watchRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("watch_target"), erlEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	watcherPID := watchRes.(etf.Tuple)[1].(erlang.PID)

	// Start an uncompleted deferred call concurrently from BEAM via send_request_target.
	deferErrCh := make(chan etf.Term, 1)
	go func() {
		r, _ := cli.Call(ctx, beamSrv, etf.Tuple{
			etf.Atom("send_request_target"),
			erlEphemeral,
			etf.Atom("defer_forever"),
			int64(5000),
		})
		deferErrCh <- r
	}()

	var orphanTicket actor.DeferredReply
	select {
	case orphanTicket = <-deferredCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for defer_forever to enter Actor")
	}

	// Trigger Reply + Stop in the same turn: caller MUST receive {ok, {final, 777}} before DOWN!
	callRes, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlEphemeral,
		etf.Atom("reply_and_stop"),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantFinal := etf.Tuple{etf.Atom("ok"), etf.Tuple{etf.Atom("final"), int64(777)}}
	if !etf.TermEqual(callRes, wantFinal) {
		t.Fatalf("Reply-before-DOWN call result = %#v, want %#v", callRes, wantFinal)
	}

	// Watcher MUST observe DOWN with preserved {ephemeral, Node} object and reason normal!
	downRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("await_down"), watcherPID, int64(5000)})
	if err != nil {
		t.Fatal(err)
	}
	downTup, ok := downRes.(etf.Tuple)
	if !ok || len(downTup) != 4 || downTup[0] != etf.Atom("down") || !etf.TermEqual(downTup[2], erlEphemeral) || downTup[3] != etf.Atom("normal") {
		t.Fatalf("watcher DOWN = %#v, want {down, _, %#v, normal}", downRes, erlEphemeral)
	}

	// Uncompleted deferred request MUST receive {down, normal}, NOT a business reply, and late ticket.Reply fails with ErrReplyExpired!
	deferredOutcome := <-deferErrCh
	wantDeferred := etf.Tuple{etf.Atom("down"), etf.Atom("normal")}
	if !etf.TermEqual(deferredOutcome, wantDeferred) {
		t.Fatalf("deferred request outcome = %#v, want %#v", deferredOutcome, wantDeferred)
	}
	if err := orphanTicket.ReplyChecked(etf.Atom("late")); !errors.Is(err, actor.ErrReplyExpired) {
		t.Fatalf("late DeferredReply.Reply err = %v, want ErrReplyExpired", err)
	}

	// Part B: Business ReplyError(actor.ErrStopped) MUST be encoded as Reply, NOT TargetExit/DOWN (§9.2, §13)!
	bizPID, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch msg {
		case etf.Atom("biz_err_stopped"):
			return a.ReplyError(actor.ErrStopped)
		case etf.Atom("still_alive"):
			return a.Reply(etf.Atom("alive"))
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.Export("biz_svc", bizPID, erlangactor.PassthroughSchema()); err != nil {
		t.Fatal(err)
	}
	erlBiz := etf.Tuple{etf.Atom("biz_svc"), etf.Atom(erlNode.Name())}

	bizRes, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlBiz,
		etf.Atom("biz_err_stopped"),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isWrappedTaoErrorCode(bizRes, "call_error") {
		t.Fatalf("ReplyError(ErrStopped) result = %#v, want business {ok, {error, {tao, call_error, _}}}", bizRes)
	}

	aliveRes, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlBiz,
		etf.Atom("still_alive"),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !etf.TermEqual(aliveRes, etf.Tuple{etf.Atom("ok"), etf.Atom("alive")}) {
		t.Fatalf("subsequent call after ReplyError(ErrStopped) = %#v, want {ok, alive}", aliveRes)
	}
}

type migCmd struct {
	Kind  string
	Delta int64
}

func makeMigratingCounter(initial int64) actor.Factory {
	return func() actor.Behavior {
		count := initial
		return actor.FromFunc(func(a *actor.Actor, msg any) error {
			switch m := msg.(type) {
			case actor.Started, actor.Stopped:
				return nil
			case actor.Snapshotting:
				return a.Reply(count)
			case actor.Upgraded:
				return m.Restore(&count)
			case string:
				if m == "self_pid" {
					return a.Reply(a.PID())
				}
			case migCmd:
				switch m.Kind {
				case "add":
					count += m.Delta
					return a.Reply(count)
				case "get":
					return a.Reply(count)
				}
			}
			return nil
		})()
	}
}

func migSchema() erlangactor.Schema {
	return erlangactor.Funcs{
		Call: func(term etf.Term) (any, error) {
			if term == etf.Atom("get") {
				return migCmd{Kind: "get"}, nil
			}
			if tup, ok := term.(etf.Tuple); ok && len(tup) == 2 && tup[0] == etf.Atom("add") {
				if d, ok := tup[1].(int64); ok {
					return migCmd{Kind: "add", Delta: d}, nil
				}
			}
			return nil, errors.New("bad migCmd")
		},
		Reply: func(value any, err error) (etf.Term, error) {
			if err != nil {
				return erlangactor.DefaultEncodeReply(nil, err)
			}
			return etf.Tuple{etf.Atom("ok"), value.(int64)}, nil
		},
	}
}

func TestBridgeReplaceAndHandoffBothPaths(t *testing.T) {
	epmdDaemon := startIsolatedEPMD(t)
	cookie := "bridge-mig-cookie"
	beam := startBEAMServer(t, "beam_mig", cookie, epmdDaemon.port)

	dir := actor.NewMemoryDirectory()
	kindV1 := actor.KindSpec{Name: "mig_counter", Version: "1", Factory: makeMigratingCounter(0)}
	kindV2 := actor.KindSpec{Name: "mig_counter", Version: "2", Factory: makeMigratingCounter(0)}

	sourceTao, err := actor.StartNode("127.0.0.1:0", actor.NodeOptions{
		Name:      "source",
		Directory: dir,
		Kinds:     []actor.KindSpec{kindV1, kindV2},
		Network:   actor.NetworkOptions{AllowInsecureLoopback: true},
		Register:  []actor.MessageType{{"mig_cmd", 1, migCmd{}}, {"count", 1, int64(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sourceTao.Stop(context.Background()) }()

	targetTao, err := actor.StartNode("127.0.0.1:0", actor.NodeOptions{
		Name:      "target",
		Directory: dir,
		Kinds:     []actor.KindSpec{kindV1, kindV2},
		Network:   actor.NetworkOptions{AllowInsecureLoopback: true},
		Register:  []actor.MessageType{{"mig_cmd", 1, migCmd{}}, {"count", 1, int64(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = targetTao.Stop(context.Background()) }()

	if err := sourceTao.Connect(context.Background(), targetTao.Name(), targetTao.ListenAddr()); err != nil {
		t.Fatal(err)
	}

	sourceErl, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "source_erl@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sourceErl.Close() }()

	targetErl, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "target_erl@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = targetErl.Close() }()

	sourceBridge, err := erlangactor.Attach(sourceTao, sourceErl)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sourceBridge.Close(context.Background()) }()

	targetBridge, err := erlangactor.Attach(targetTao, targetErl)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = targetBridge.Close(context.Background()) }()

	cli, err := sourceErl.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	beamSrv := erlang.Registered("tao_test_server", beam.fullName)

	// 1. Replace: old Binding terminates with {tao, replaced, _}, new PID re-exports with new Erlang PID (§10.4).
	repPIDv1, err := sourceTao.Spawn(makeMigratingCounter(10), actor.WithName("rep_local"), actor.WithKind("mig_counter", "1"))
	if err != nil {
		t.Fatal(err)
	}
	oldBind, err := sourceBridge.Export("rep_svc", repPIDv1, migSchema())
	if err != nil {
		t.Fatal(err)
	}
	erlRep := etf.Tuple{etf.Atom("rep_svc"), etf.Atom(sourceErl.Name())}

	wRepRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("watch_target"), erlRep})
	if err != nil {
		t.Fatal(err)
	}
	wRepPID := wRepRes.(etf.Tuple)[1].(erlang.PID)

	repRef := actor.Ref{Path: "rep_local"}
	if err := sourceTao.Replace(ctx, repRef, kindV2); err != nil {
		t.Fatal(err)
	}
	<-oldBind.Done()

	downRep, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("await_down"), wRepPID, int64(5000)})
	if err != nil {
		t.Fatal(err)
	}
	if !hasDownTaoCode(downRep, "replaced") {
		t.Fatalf("Replace watcher DOWN = %#v, want {tao, replaced, _}", downRep)
	}

	// Re-export upgraded PID under same Erlang name; new Erlang PID must differ from old!
	upgradedPID, err := sourceTao.Call[actor.PID](ctx, repRef, "self_pid")
	if err != nil {
		t.Fatal(err)
	}
	newBind, err := sourceBridge.Export("rep_svc", upgradedPID, migSchema())
	if err != nil {
		t.Fatal(err)
	}
	if etf.TermEqual(oldBind.PID(), newBind.PID()) {
		t.Fatalf("re-exported Erlang PID %v reused old PID", newBind.PID())
	}
	gotAfterRep, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("call_target"), erlRep, etf.Atom("get"), int64(5000)})
	if err != nil {
		t.Fatal(err)
	}
	if !etf.TermEqual(gotAfterRep, etf.Tuple{etf.Atom("ok"), etf.Tuple{etf.Atom("ok"), int64(10)}}) {
		t.Fatalf("state after Replace = %#v, want 10", gotAfterRep)
	}

	// 2. Controlled Handoff: binding.Close(ctx) -> Handoff -> target Export (§10.4).
	ho1PID, err := sourceTao.Spawn(makeMigratingCounter(50),
		actor.WithName("ho_controlled"),
		actor.WithKind("mig_counter", "1"),
		actor.WithHandoff(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	ho1Bind, err := sourceBridge.Export("ho_controlled_svc", ho1PID, migSchema())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		etf.Tuple{etf.Atom("ho_controlled_svc"), etf.Atom(sourceErl.Name())},
		etf.Tuple{etf.Atom("add"), int64(5)},
		int64(5000),
	}); err != nil {
		t.Fatal(err)
	}
	if err := ho1Bind.Close(ctx); err != nil {
		t.Fatal(err)
	}
	ho1Res, err := sourceTao.Handoff(ctx, actor.Ref{Path: "ho_controlled"}, actor.HandoffOptions{
		TargetNode: "target",
		Version:    "1",
	})
	if err != nil {
		t.Fatalf("controlled Handoff failed: %v", err)
	}
	if _, err := targetBridge.Export("ho_controlled_svc", ho1Res.Target.PID(), migSchema()); err != nil {
		t.Fatal(err)
	}
	gotTarget, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		etf.Tuple{etf.Atom("ho_controlled_svc"), etf.Atom(targetErl.Name())},
		etf.Atom("get"),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !etf.TermEqual(gotTarget, etf.Tuple{etf.Atom("ok"), etf.Tuple{etf.Atom("ok"), int64(55)}}) {
		t.Fatalf("controlled Handoff target state = %#v, want 55", gotTarget)
	}

	// 3. Direct Handoff WITHOUT prior binding.Close: guardian Monitor MUST NOT block Handoff,
	// and source Binding MUST terminate with {tao, handed_off, DetailBinary} (§10.4, §13)!
	ho2PID, err := sourceTao.Spawn(makeMigratingCounter(100),
		actor.WithName("ho_direct"),
		actor.WithKind("mig_counter", "1"),
		actor.WithHandoff(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	ho2Bind, err := sourceBridge.Export("ho_direct_svc", ho2PID, migSchema())
	if err != nil {
		t.Fatal(err)
	}
	erlDirect := etf.Tuple{etf.Atom("ho_direct_svc"), etf.Atom(sourceErl.Name())}
	wHoRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("watch_target"), erlDirect})
	if err != nil {
		t.Fatal(err)
	}
	wHoPID := wHoRes.(etf.Tuple)[1].(erlang.PID)

	if _, err := sourceTao.Handoff(ctx, actor.Ref{Path: "ho_direct"}, actor.HandoffOptions{
		TargetNode: "target",
		Version:    "1",
	}); err != nil {
		t.Fatalf("direct Handoff with active guardian Monitor failed: %v", err)
	}
	<-ho2Bind.Done()

	downHo, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("await_down"), wHoPID, int64(5000)})
	if err != nil {
		t.Fatal(err)
	}
	if !hasDownTaoCode(downHo, "handed_off") {
		t.Fatalf("direct Handoff watcher DOWN = %#v, want {tao, handed_off, _}", downHo)
	}
}

func TestBridgeAdaptiveGuardianPoolAndDeadPIDExport(t *testing.T) {
	// Subtest 1: Dead PID rejected before publishing Erlang name, plus shared watch & multi-group spillover (§9.3).
	t.Run("spillover_and_shared_watch", func(t *testing.T) {
		taoNode, err := actor.NewNode(actor.NodeOptions{
			Name:                    "tao_guard_spill",
			MaxObservationsPerActor: 64,
			MaxObservations:         256,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = taoNode.Stop(context.Background()) }()

		erlNode, err := erlang.Start(context.Background(), erlang.Config{
			Name:        "erl_guard_spill@127.0.0.1",
			Listen:      "127.0.0.1:0",
			Cookie:      "guard-cookie",
			DisableEPMD: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = erlNode.Close() }()

		bridge, err := erlangactor.Attach(taoNode, erlNode)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = bridge.Close(context.Background()) }()

		noop := actor.FromFunc(func(_ *actor.Actor, _ any) error { return nil })

		// Dead PID export must fail with ErrNoProcess and NOT leak the registered name.
		deadPID, err := taoNode.Spawn(noop)
		if err != nil {
			t.Fatal(err)
		}
		if err := taoNode.StopActor(context.Background(), deadPID); err != nil {
			t.Fatal(err)
		}
		if _, err := bridge.Export("reusable_name", deadPID, erlangactor.PassthroughSchema()); !errors.Is(err, actor.ErrNoProcess) {
			t.Fatalf("Export dead PID err = %v, want ErrNoProcess", err)
		}

		// Exporting 3 names onto the SAME live PID shares 1 watch.
		sharedPID, err := taoNode.Spawn(noop)
		if err != nil {
			t.Fatal(err)
		}
		for _, nm := range []string{"reusable_name", "alias_two", "alias_three"} {
			if _, err := bridge.Export(nm, sharedPID, erlangactor.PassthroughSchema()); err != nil {
				t.Fatalf("Export(%s) err = %v", nm, err)
			}
		}
		st := bridge.Stats()
		if st.WatchedActors != 1 || st.GuardianActors != 1 || st.Bindings != 3 {
			t.Fatalf("after shared exports: stats = %+v, want WatchedActors=1 GuardianActors=1 Bindings=3", st)
		}

		// Export 99 more distinct Actors (total 100 > MaxObservationsPerActor=64) -> 2 guardian groups!
		for i := 0; i < 99; i++ {
			pid, err := taoNode.Spawn(noop)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bridge.Export(fmt.Sprintf("svc_%d", i), pid, erlangactor.PassthroughSchema()); err != nil {
				t.Fatalf("Export svc_%d failed: %v", i, err)
			}
		}
		st = bridge.Stats()
		if st.WatchedActors != 100 || st.GuardianActors != 2 {
			t.Fatalf("after 100 distinct actors: stats = %+v, want WatchedActors=100 GuardianActors=2", st)
		}
	})

	// Subtest 2: Node total observation quota exhausted -> spawns at most 1 candidate, recycles it, returns ErrCapacity (§9.3).
	t.Run("node_quota_exhaustion_recycles_candidate", func(t *testing.T) {
		taoNode, err := actor.NewNode(actor.NodeOptions{
			Name:                    "tao_guard_cap",
			MaxObservationsPerActor: 8,
			MaxObservations:         8,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = taoNode.Stop(context.Background()) }()

		erlNode, err := erlang.Start(context.Background(), erlang.Config{
			Name:        "erl_guard_cap@127.0.0.1",
			Listen:      "127.0.0.1:0",
			Cookie:      "guard-cookie",
			DisableEPMD: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = erlNode.Close() }()

		bridge, err := erlangactor.Attach(taoNode, erlNode)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = bridge.Close(context.Background()) }()

		noop := actor.FromFunc(func(_ *actor.Actor, _ any) error { return nil })
		for i := 0; i < 8; i++ {
			pid, err := taoNode.Spawn(noop)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bridge.Export(fmt.Sprintf("cap_%d", i), pid, erlangactor.PassthroughSchema()); err != nil {
				t.Fatalf("Export cap_%d err = %v", i, err)
			}
		}

		extraPID, err := taoNode.Spawn(noop)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bridge.Export("cap_overflow", extraPID, erlangactor.PassthroughSchema()); !errors.Is(err, actor.ErrCapacity) {
			t.Fatalf("overflow Export err = %v, want ErrCapacity", err)
		}
		st := bridge.Stats()
		if st.GuardianActors != 1 || st.WatchedActors != 8 || st.Bindings != 8 {
			t.Fatalf("candidate leak check: stats = %+v, want GuardianActors=1 WatchedActors=8 Bindings=8", st)
		}
	})

	// Subtest 3: Empty first guardian failing on its very first Monitor -> immediate recycle & ErrCapacity (§9.3).
	t.Run("empty_first_guardian_fails_immediately", func(t *testing.T) {
		taoNode, err := actor.NewNode(actor.NodeOptions{
			Name:                    "tao_guard_zero",
			MaxObservationsPerActor: 1,
			MaxObservations:         1,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = taoNode.Stop(context.Background()) }()

		noop := actor.FromFunc(func(_ *actor.Actor, _ any) error { return nil })
		targetPID, err := taoNode.Spawn(noop)
		if err != nil {
			t.Fatal(err)
		}
		// Consume the single node-wide observation slot externally.
		ready := make(chan struct{})
		_, err = taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
			if _, ok := msg.(actor.Started); ok {
				_, _ = a.Monitor(targetPID)
				close(ready)
			}
			return nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		<-ready

		erlNode, err := erlang.Start(context.Background(), erlang.Config{
			Name:        "erl_guard_zero@127.0.0.1",
			Listen:      "127.0.0.1:0",
			Cookie:      "guard-cookie",
			DisableEPMD: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = erlNode.Close() }()

		bridge, err := erlangactor.Attach(taoNode, erlNode)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = bridge.Close(context.Background()) }()

		if _, err := bridge.Export("blocked_svc", targetPID, erlangactor.PassthroughSchema()); !errors.Is(err, actor.ErrCapacity) {
			t.Fatalf("empty guardian Export err = %v, want ErrCapacity", err)
		}
		if st := bridge.Stats(); st.GuardianActors != 0 || st.Bindings != 0 {
			t.Fatalf("empty guardian not recycled: stats = %+v", st)
		}
	})
}

func TestBridgeTrapExitKillAndStopActorPolicy(t *testing.T) {
	epmdDaemon := startIsolatedEPMD(t)
	cookie := "bridge-exit-cookie"
	beam := startBEAMServer(t, "beam_exit", cookie, epmdDaemon.port)

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "tao_exit"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	erlNode, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "tao_erl_exit@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = erlNode.Close() }()

	bridge, err := erlangactor.Attach(taoNode, erlNode)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	cli, err := erlNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	beamSrv := erlang.Registered("tao_test_server", beam.fullName)

	// 1. WithTrapExit(true) + default StopActor(false): receives Exit DTOs; exit(Pid, kill) kills Binding
	// with reason 'killed', cancels active BeginCall, but keeps local Tao Actor alive (§10.2–§10.3)!
	exitsCh := make(chan erlangactor.Exit, 4)
	callCanceledCh := make(chan error, 1)
	keepAlivePID, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch m := msg.(type) {
		case erlangactor.Exit:
			exitsCh <- m
			return nil
		case etf.Atom:
			if m == "wait_cancel" {
				ticket := a.Defer()
				reqCtx := a.Context()
				go func() {
					<-reqCtx.Done()
					callCanceledCh <- reqCtx.Err()
					_ = ticket.Reply(etf.Atom("unreachable"))
				}()
				return nil
			}
		case string:
			if m == "local_ping" {
				return a.Reply("still_running")
			}
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	bindKeep, err := bridge.Export("trap_keep", keepAlivePID, erlangactor.PassthroughSchema(),
		erlangactor.WithTrapExit(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Link a BEAM worker and exit it with {peer_boom, 1} -> delivered as erlangactor.Exit{FromLink: true}.
	wSpawn, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("spawn_worker"), etf.Atom("true")})
	if err != nil {
		t.Fatal(err)
	}
	workerPID := wSpawn.(etf.Tuple)[1].(erlang.PID)
	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("worker_op"),
		workerPID,
		etf.Tuple{etf.Atom("link_to"), bindKeep.PID()},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("worker_op"),
		workerPID,
		etf.Tuple{etf.Atom("exit_self"), etf.Tuple{etf.Atom("peer_boom"), int64(1)}},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case ex := <-exitsCh:
		if !ex.FromLink || !etf.TermEqual(ex.From, workerPID) || !etf.TermEqual(ex.Reason, etf.Tuple{etf.Atom("peer_boom"), int64(1)}) {
			t.Fatalf("trapped link exit = %+v", ex)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for trapped link exit")
	}

	// Explicit EXIT2 soft_reason -> delivered as erlangactor.Exit{FromLink: false}.
	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("send_exit2"),
		bindKeep.PID(),
		etf.Atom("soft_reason"),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case ex := <-exitsCh:
		if ex.FromLink || ex.Reason != etf.Atom("soft_reason") {
			t.Fatalf("trapped EXIT2 = %+v", ex)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for trapped EXIT2")
	}

	// Watch bindKeep.PID(), start an active deferred call, then send untrappable exit(Pid, kill)!
	wKillRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("watch_target"), bindKeep.PID()})
	if err != nil {
		t.Fatal(err)
	}
	wKillWatcher := wKillRes.(etf.Tuple)[1].(erlang.PID)

	go func() {
		_, _ = cli.Call(ctx, beamSrv, etf.Tuple{
			etf.Atom("send_request_target"),
			bindKeep.PID(),
			etf.Atom("wait_cancel"),
			int64(5000),
		})
	}()
	time.Sleep(40 * time.Millisecond)

	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("send_exit2"),
		bindKeep.PID(),
		etf.Atom("kill"),
	}); err != nil {
		t.Fatal(err)
	}
	<-bindKeep.Done()

	select {
	case cerr := <-callCanceledCh:
		if cerr == nil {
			t.Fatal("expected active BeginCall context to be canceled on kill")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for active BeginCall cancellation")
	}

	downKill, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("await_down"), wKillWatcher, int64(5000)})
	if err != nil {
		t.Fatal(err)
	}
	downKillTup := downKill.(etf.Tuple)
	if downKillTup[3] != etf.Atom("killed") {
		t.Fatalf("exit(Pid, kill) DOWN reason = %#v, want 'killed'", downKill)
	}

	// Local Tao Actor MUST still be alive!
	pong, err := taoNode.Call[string](ctx, keepAlivePID, "local_ping")
	if err != nil || pong != "still_running" {
		t.Fatalf("local Tao Actor after Binding kill: pong=%q err=%v", pong, err)
	}

	// 2. WithStopActor(true): untrappable exit(Pid, kill) cooperatively stops local Tao Actor (§10.3)!
	stoppedCh := make(chan struct{})
	sharedLifePID, err := taoNode.Spawn(actor.FromFunc(func(_ *actor.Actor, msg any) error {
		if _, ok := msg.(actor.Stopped); ok {
			close(stoppedCh)
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	bindShared, err := bridge.Export("trap_stop_actor", sharedLifePID, erlangactor.PassthroughSchema(),
		erlangactor.WithTrapExit(true),
		erlangactor.WithStopActor(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("send_exit2"),
		bindShared.PID(),
		etf.Atom("kill"),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stoppedCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for WithStopActor(true) local Actor to stop")
	}
}

func TestBridgeOverloadCallReplyAndCastTermination(t *testing.T) {
	epmdDaemon := startIsolatedEPMD(t)
	cookie := "bridge-overload-cookie"
	beam := startBEAMServer(t, "beam_ovl", cookie, epmdDaemon.port)

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "tao_ovl"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	erlNode, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "tao_erl_ovl@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = erlNode.Close() }()

	bridge, err := erlangactor.Attach(taoNode, erlNode)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	slowPID, err := taoNode.Spawn(actor.FromFunc(func(_ *actor.Actor, msg any) error {
		if msg == "block" {
			close(entered)
			<-release
		}
		return nil
	}), actor.WithMailboxSize(1))
	if err != nil {
		t.Fatal(err)
	}

	if err := taoNode.Send(slowPID, "block"); err != nil {
		t.Fatal(err)
	}
	<-entered
	// Fill the 1-message mailbox so subsequent admissions hit ErrMailboxFull.
	if err := taoNode.Send(slowPID, "queued"); err != nil {
		t.Fatal(err)
	}

	bind, err := bridge.Export("slow_svc", slowPID, erlangactor.PassthroughSchema())
	if err != nil {
		t.Fatal(err)
	}

	cli, err := erlNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	beamSrv := erlang.Registered("tao_test_server", beam.fullName)
	erlSlow := etf.Tuple{etf.Atom("slow_svc"), etf.Atom(erlNode.Name())}

	// Watch slow_svc before triggering cast overload.
	wRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("watch_target"), erlSlow})
	if err != nil {
		t.Fatal(err)
	}
	watcherPID := wRes.(etf.Tuple)[1].(erlang.PID)

	// 1. Inbound Call when mailbox is full -> returns {error, {tao, overloaded}}, keeping Binding alive (§11.1).
	callRes, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		erlSlow,
		etf.Atom("ping"),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantOvl := etf.Tuple{
		etf.Atom("ok"),
		etf.Tuple{
			etf.Atom("error"),
			etf.Tuple{etf.Atom("tao"), etf.Atom("overloaded")},
		},
	}
	if !etf.TermEqual(callRes, wantOvl) {
		t.Fatalf("overloaded Call result = %#v, want %#v", callRes, wantOvl)
	}

	// 2. Inbound Cast when mailbox is full -> terminates Binding with {tao, overloaded, DetailBin} (§11.1).
	if _, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("cast_target"),
		erlSlow,
		etf.Atom("cast_flood"),
	}); err != nil {
		t.Fatal(err)
	}
	<-bind.Done()

	downRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("await_down"), watcherPID, int64(5000)})
	if err != nil {
		t.Fatal(err)
	}
	if !hasDownTaoCode(downRes, "overloaded") {
		t.Fatalf("overloaded Cast DOWN = %#v, want {tao, overloaded, _}", downRes)
	}
	st := bridge.Stats()
	if st.OverloadReplies == 0 || st.OverloadTerminations == 0 {
		t.Fatalf("expected overload stats > 0, got %+v", st)
	}
}

func TestBridgeOutboundConnectorOrderAndReentrantCallback(t *testing.T) {
	epmdDaemon := startIsolatedEPMD(t)
	cookie := "bridge-outbound-cookie"
	beam := startBEAMServer(t, "beam_out", cookie, epmdDaemon.port)

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "tao_out"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	erlNode, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "tao_erl_out@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = erlNode.Close() }()

	bridge, err := erlangactor.Attach(taoNode, erlNode)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	beamTarget := erlang.Registered("tao_test_server", beam.fullName)

	// Export Actor A as "reentrant_a". On "run_flow", A sends a cold-connection Cast({add, 25})
	// followed immediately by Request(Call{get_state}), and then Request(Call{callback_target})
	// which synchronously calls BACK into "reentrant_a" from BEAM before replying (§4.2)!
	var pendingStateID, pendingCallbackID actor.RequestID
	var callerTicket actor.DeferredReply
	var stateVal int64

	actorA, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch m := msg.(type) {
		case string:
			if m == "run_flow" {
				callerTicket = a.Defer()
				if err := a.Send(bridge.Outbound(), erlangactor.Cast{
					Target:  beamTarget,
					Request: etf.Tuple{etf.Atom("add"), int64(25)},
				}); err != nil {
					return callerTicket.ReplyError(err)
				}
				reqID, err := a.Request(a.Context(), bridge.Outbound(), erlangactor.Call{
					Target:  beamTarget,
					Request: etf.Atom("get_state"),
				})
				if err != nil {
					return callerTicket.ReplyError(err)
				}
				pendingStateID = reqID
				return nil
			}
		case etf.Tuple:
			if len(m) == 2 && m[0] == etf.Atom("inner_ping") {
				return a.Reply(etf.Tuple{etf.Atom("inner_pong"), m[1], stateVal})
			}
		case actor.CallResult:
			if m.Err != nil {
				_ = callerTicket.ReplyError(m.Err)
				return nil
			}
			if m.ID == pendingStateID {
				tup := m.Value.(etf.Tuple)
				stateVal = tup[1].(int64)
				cbID, err := a.Request(a.Context(), bridge.Outbound(), erlangactor.Call{
					Target: beamTarget,
					Request: etf.Tuple{
						etf.Atom("callback_target"),
						etf.Tuple{etf.Atom("reentrant_a"), etf.Atom(erlNode.Name())},
						etf.Tuple{etf.Atom("inner_ping"), int64(314)},
						int64(5000),
					},
				})
				if err != nil {
					_ = callerTicket.ReplyError(err)
					return nil
				}
				pendingCallbackID = cbID
				return nil
			}
			if m.ID == pendingCallbackID {
				_ = callerTicket.Reply(m.Value)
				return nil
			}
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := bridge.Export("reentrant_a", actorA, erlangactor.PassthroughSchema()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	finalTerm, err := taoNode.Call[etf.Term](ctx, actorA, "run_flow")
	if err != nil {
		t.Fatalf("A -> BEAM -> A flow failed: %v", err)
	}
	want := etf.Tuple{
		etf.Atom("callback_ok"),
		etf.Tuple{etf.Atom("inner_pong"), int64(314), int64(25)},
	}
	if !etf.TermEqual(finalTerm, want) {
		t.Fatalf("A -> BEAM -> A result = %#v, want %#v", finalTerm, want)
	}
}

func TestBridgeOutOfOrderShutdownAndEPMDLoss(t *testing.T) {
	// 1. taoNode.Stop BEFORE bridge.Close -> Bridge auto-closes and emits shutdown DOWN to BEAM (§4.2, §13).
	t.Run("tao_stops_first", func(t *testing.T) {
		epmdDaemon := startIsolatedEPMD(t)
		cookie := "bridge-shut1-cookie"
		beam := startBEAMServer(t, "beam_shut1", cookie, epmdDaemon.port)

		taoNode, err := actor.NewNode(actor.NodeOptions{Name: "tao_shut1"})
		if err != nil {
			t.Fatal(err)
		}
		erlNode, err := erlang.Start(context.Background(), erlang.Config{
			Name:     "tao_erl_shut1@127.0.0.1",
			Listen:   "127.0.0.1:0",
			Cookie:   cookie,
			EPMDAddr: epmdDaemon.addr,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = erlNode.Close() }()

		bridge, err := erlangactor.Attach(taoNode, erlNode)
		if err != nil {
			t.Fatal(err)
		}

		pid, err := taoNode.Spawn(actor.FromFunc(func(_ *actor.Actor, _ any) error { return nil }))
		if err != nil {
			t.Fatal(err)
		}
		bind, err := bridge.Export("shut_svc", pid, erlangactor.PassthroughSchema())
		if err != nil {
			t.Fatal(err)
		}

		cli, err := erlNode.NewClient()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cli.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		beamSrv := erlang.Registered("tao_test_server", beam.fullName)
		erlTarget := etf.Tuple{etf.Atom("shut_svc"), etf.Atom(erlNode.Name())}

		wRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("watch_target"), erlTarget})
		if err != nil {
			t.Fatal(err)
		}
		watcherPID := wRes.(etf.Tuple)[1].(erlang.PID)

		// Stop Tao Node FIRST without calling bridge.Close!
		if err := taoNode.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-bridge.Done():
		case <-ctx.Done():
			t.Fatal("timed out waiting for Bridge to close after taoNode.Stop")
		}
		<-bind.Done()

		downRes, err := cli.Call(ctx, beamSrv, etf.Tuple{etf.Atom("await_down"), watcherPID, int64(5000)})
		if err != nil {
			t.Fatal(err)
		}
		downTup := downRes.(etf.Tuple)
		if downTup[3] != etf.Atom("shutdown") && downTup[3] != etf.Atom("normal") {
			t.Fatalf("unexpected DOWN reason after taoNode.Stop: %#v", downRes)
		}
	})

	// 2. EPMD registration loss kills erlangNode FIRST -> Bridge auto-closes while taoNode stays healthy (§6.1, §13).
	t.Run("epmd_loss_first", func(t *testing.T) {
		epmdDaemon := startIsolatedEPMD(t)
		taoNode, err := actor.NewNode(actor.NodeOptions{Name: "tao_shut2"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = taoNode.Stop(context.Background()) }()

		erlNode, err := erlang.Start(context.Background(), erlang.Config{
			Name:     "tao_erl_shut2@127.0.0.1",
			Listen:   "127.0.0.1:0",
			Cookie:   "bridge-shut2-cookie",
			EPMDAddr: epmdDaemon.addr,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = erlNode.Close() }()

		bridge, err := erlangactor.Attach(taoNode, erlNode)
		if err != nil {
			t.Fatal(err)
		}

		pid, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
			if msg == "ping" {
				return a.Reply("pong")
			}
			return nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bridge.Export("epmd_loss_svc", pid, erlangactor.PassthroughSchema()); err != nil {
			t.Fatal(err)
		}

		// Kill EPMD daemon!
		epmdDaemon.Stop()

		select {
		case <-bridge.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Bridge to close after EPMD registration loss")
		}

		// Local Tao Actor must remain healthy!
		got, err := taoNode.Call[string](context.Background(), pid, "ping")
		if err != nil || got != "pong" {
			t.Fatalf("taoNode Actor after EPMD loss: got=%q err=%v", got, err)
		}
	})
}

func isWrappedTaoErrorCode(term etf.Term, wantCode string) bool {
	outer, ok := term.(etf.Tuple)
	if !ok || len(outer) != 2 || outer[0] != etf.Atom("ok") {
		return false
	}
	errTup, ok := outer[1].(etf.Tuple)
	if !ok || len(errTup) != 2 || errTup[0] != etf.Atom("error") {
		return false
	}
	taoTup, ok := errTup[1].(etf.Tuple)
	if !ok || len(taoTup) < 2 || taoTup[0] != etf.Atom("tao") {
		return false
	}
	return taoTup[1] == etf.Atom(wantCode)
}

func hasDownTaoCode(term etf.Term, wantCode string) bool {
	downTup, ok := term.(etf.Tuple)
	if !ok || len(downTup) != 4 || downTup[0] != etf.Atom("down") {
		return false
	}
	taoTup, ok := downTup[3].(etf.Tuple)
	if !ok || len(taoTup) < 2 || taoTup[0] != etf.Atom("tao") {
		return false
	}
	return taoTup[1] == etf.Atom(wantCode)
}

func TestReplyQuotaFailureTerminatesBindingAndWakesCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "reply_quota_actor"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	server, err := erlang.Start(ctx, erlang.Config{
		Name:        "reply_quota_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "reply-quota-cookie",
		DisableEPMD: true,
		Limits:      erlang.Limits{MaxPeerSendBytes: 128},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	bridge, err := erlangactor.Attach(taoNode, server)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	replied := make(chan error, 1)
	pid, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		if msg == etf.Atom("request") {
			replied <- a.Reply(etf.Binary(make([]byte, 256)))
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	binding, err := bridge.Export("service", pid, erlangactor.PassthroughSchema())
	if err != nil {
		t.Fatal(err)
	}

	remote, err := erlang.Start(ctx, erlang.Config{
		Name:        "reply_quota_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "reply-quota-cookie",
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			server.Name(): {Addr: server.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = remote.Close() }()

	cli, err := remote.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
	defer callCancel()
	start := time.Now()
	_, callErr := cli.Call(callCtx, erlang.Registered("service", server.Name()), etf.Atom("request"))
	elapsed := time.Since(start)

	select {
	case rErr := <-replied:
		if rErr != nil {
			t.Fatalf("actor Reply failed: %v", rErr)
		}
	default:
		t.Fatal("request did not reach actor")
	}

	if callErr == nil || errors.Is(callErr, context.DeadlineExceeded) {
		t.Fatalf("Call err = %v (elapsed=%v), want immediate process DOWN error instead of timeout", callErr, elapsed)
	}
	var erlErr *erlang.Error
	if !errors.As(callErr, &erlErr) || erlErr.Stage != erlang.StageProcess || erlErr.Outcome != erlang.OutcomeUnknown {
		t.Fatalf("erlang.Error = %#v, want StageProcess + OutcomeUnknown", callErr)
	}

	select {
	case <-binding.Done():
	case <-time.After(time.Second):
		t.Fatal("expected Binding to terminate after reply send quota failure")
	}

	deadline := time.Now().Add(time.Second)
	for bridge.Stats().InboundInFlight != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	st := bridge.Stats()
	if st.InboundInFlight != 0 {
		t.Fatalf("InboundInFlight = %d, want 0", st.InboundInFlight)
	}
	if st.OverloadTerminations != 1 {
		t.Fatalf("OverloadTerminations = %d, want 1", st.OverloadTerminations)
	}
}

func TestReplyEncodeFailureSendsFallbackErrorWithoutKillingBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "reply_enc_actor"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	server, err := erlang.Start(ctx, erlang.Config{
		Name:        "reply_enc_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "reply-enc-cookie",
		DisableEPMD: true,
		Limits: erlang.Limits{
			MaxPayloadBytes:  512,
			MaxPeerSendBytes: 64 << 10,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	bridge, err := erlangactor.Attach(taoNode, server)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	pid, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch msg {
		case etf.Atom("oversized"):
			return a.Reply(etf.Binary(make([]byte, 1024)))
		case etf.Atom("small"):
			return a.Reply(etf.Atom("ok"))
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	binding, err := bridge.Export("enc_svc", pid, erlangactor.PassthroughSchema())
	if err != nil {
		t.Fatal(err)
	}

	remote, err := erlang.Start(ctx, erlang.Config{
		Name:        "reply_enc_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "reply-enc-cookie",
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			server.Name(): {Addr: server.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = remote.Close() }()

	cli, err := remote.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	// First call returns a 1024B payload (> MaxPayloadBytes 512) and must promptly receive
	// {error, {tao, encode_error, Detail}} without killing the Binding.
	rep1, err := cli.Call(ctx, erlang.Registered("enc_svc", server.Name()), etf.Atom("oversized"))
	if err != nil {
		t.Fatalf("oversized Call failed: %v", err)
	}
	if !isWrappedTaoErrorCode(etf.Tuple{etf.Atom("ok"), rep1}, "encode_error") {
		t.Fatalf("oversized Call reply = %#v, want {error, {tao, encode_error, ...}}", rep1)
	}
	select {
	case <-binding.Done():
		t.Fatal("Binding should remain alive after recoverable reply encode failure")
	default:
	}

	// Subsequent valid call on the same Binding succeeds cleanly.
	rep2, err := cli.Call(ctx, erlang.Registered("enc_svc", server.Name()), etf.Atom("small"))
	if err != nil || rep2 != etf.Atom("ok") {
		t.Fatalf("small Call = (%#v, %v), want ('ok', nil)", rep2, err)
	}
}

func TestUnmonitoredCallReplyQuotaFailureDisconnectsPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "unmon_quota_actor"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	server, err := erlang.Start(ctx, erlang.Config{
		Name:        "unmon_quota_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "unmon-quota-cookie",
		DisableEPMD: true,
		Limits:      erlang.Limits{MaxPeerSendBytes: 128},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	bridge, err := erlangactor.Attach(taoNode, server)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	pid, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		if msg == etf.Atom("request") {
			return a.Reply(etf.Binary(make([]byte, 256)))
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	binding, err := bridge.Export("unmon_svc", pid, erlangactor.PassthroughSchema())
	if err != nil {
		t.Fatal(err)
	}

	remote, err := erlang.Start(ctx, erlang.Config{
		Name:        "unmon_quota_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "unmon-quota-cookie",
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			server.Name(): {Addr: server.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = remote.Close() }()

	// Observe disconnect on remote via a separate probe endpoint watched by another Client.
	probeEP, err := server.Export(erlang.EndpointOptions{Name: "probe"})
	if err != nil {
		t.Fatal(err)
	}
	cli, err := remote.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	_, probeDown, err := cli.Monitor(ctx, erlang.Process(probeEP.PID()))
	if err != nil {
		t.Fatal(err)
	}

	// Send a raw '$gen_call' WITHOUT installing a monitor on binding.PID()!
	rawCall := etf.Tuple{
		etf.Atom("$gen_call"),
		etf.Tuple{cli.PID(), etf.Atom("custom_unmonitored_tag")},
		etf.Atom("request"),
	}
	if err := cli.Send(ctx, erlang.Registered("unmon_svc", server.Name()), rawCall); err != nil {
		t.Fatal(err)
	}

	<-binding.Done()

	// Because the caller had no monitor on unmon_svc, server must disconnect the peer session
	// so an unmonitored caller waiting with infinity observes noconnection instead of hanging.
	select {
	case ev := <-probeDown:
		if ev.Reason != etf.Atom("noconnection") {
			t.Fatalf("probe DOWN reason = %#v, want 'noconnection'", ev.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected peer disconnect for unmonitored call whose reply failed send quota")
	}
}

func TestBEAMInfinityCallReplyQuotaFailureDoesNotHang(t *testing.T) {
	epmdDaemon := startIsolatedEPMD(t)
	cookie := "beam-inf-quota-cookie"
	beam := startBEAMServer(t, "beam_inf_q", cookie, epmdDaemon.port)

	taoNode, err := actor.NewNode(actor.NodeOptions{Name: "tao_inf_q"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taoNode.Stop(context.Background()) }()

	// Give server enough send quota for handshake + small control/call requests (512B),
	// while a 1024B Actor reply exceeds MaxPeerSendBytes!
	erlNode, err := erlang.Start(context.Background(), erlang.Config{
		Name:     "tao_erl_inf_q@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdDaemon.addr,
		Limits:   erlang.Limits{MaxPeerSendBytes: 512},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = erlNode.Close() }()

	bridge, err := erlangactor.Attach(taoNode, erlNode)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bridge.Close(context.Background()) }()

	pid, err := taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		if msg == etf.Atom("big_reply") {
			return a.Reply(etf.Binary(make([]byte, 1024)))
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	binding, err := bridge.Export("inf_quota_svc", pid, erlangactor.PassthroughSchema())
	if err != nil {
		t.Fatal(err)
	}

	cli, err := erlNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	beamSrv := erlang.Registered("tao_test_server", beam.fullName)
	target := etf.Tuple{etf.Atom("inf_quota_svc"), etf.Atom(erlNode.Name())}

	// BEAM executes gen_server:call(Target, big_reply, infinity) — MUST exit promptly via DOWN
	// instead of hanging forever!
	res, err := cli.Call(ctx, beamSrv, etf.Tuple{
		etf.Atom("call_target"),
		target,
		etf.Atom("big_reply"),
		etf.Atom("infinity"),
	})
	if err != nil {
		t.Fatalf("BEAM call_target(infinity) hung or failed: %v", err)
	}
	resTup, ok := res.(etf.Tuple)
	if !ok || len(resTup) != 3 || resTup[0] != etf.Atom("caught") || resTup[1] != etf.Atom("exit") {
		t.Fatalf("BEAM gen_server:call(infinity) result = %#v, want {'caught', 'exit', ...}", res)
	}
	<-binding.Done()
}
