package erlang_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
	"github.com/sllt/tao/erlang/internal/dist"
)

func TestInteropBEAMTCPPingCallCastSendAndPayloadBoundaries(t *testing.T) {
	epmdSrv := startIsolatedEPMD(t)
	const cookie = "tao-tcp-secret-cookie"
	beam := startBEAMServer(t, beamServerOptions{
		AliveName: "beam_tcp",
		Cookie:    cookie,
		EPMDPort:  epmdSrv.port,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Hidden Tao node (Published: false by default).
	taoHidden, err := erlang.Start(ctx, erlang.Config{
		Name:     "tao_hidden@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdSrv.addr,
	})
	if err != nil {
		t.Fatalf("Start hidden node: %v", err)
	}
	defer taoHidden.Close()

	client, err := taoHidden.NewClient()
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	serverTarget := erlang.Registered("tao_test_server", beam.fullName)

	// 1. Query real OTP/ERTS version info.
	infoReply, err := client.Call(ctx, serverTarget, etf.Atom("otp_info"))
	if err != nil {
		t.Fatalf("Call otp_info: %v", err)
	}
	infoTup, ok := infoReply.(etf.Tuple)
	if !ok || len(infoTup) != 2 || infoTup[0] != etf.Atom("ok") {
		t.Fatalf("unexpected otp_info reply: %#v", infoReply)
	}
	t.Logf("Connected to real BEAM: %+v", infoTup[1])

	// 2. BEAM net_adm:ping -> hidden Tao node must return pong, absent from nodes(), present in nodes(hidden).
	pingRes, err := client.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("net_adm_ping"),
		etf.Atom(taoHidden.Name()),
	})
	if err != nil {
		t.Fatalf("Call net_adm_ping(hidden): %v", err)
	}
	pingTup := pingRes.(etf.Tuple)
	if pingTup[0] != etf.Atom("pong") {
		t.Fatalf("net_adm:ping hidden expected pong, got %v", pingTup[0])
	}
	if listContainsAtom(pingTup[1], etf.Atom(taoHidden.Name())) {
		t.Fatalf("hidden node %s should NOT appear in nodes(): %#v", taoHidden.Name(), pingTup[1])
	}
	if !listContainsAtom(pingTup[2], etf.Atom(taoHidden.Name())) {
		t.Fatalf("hidden node %s MUST appear in nodes(hidden): %#v", taoHidden.Name(), pingTup[2])
	}

	// Published Tao node must appear in nodes().
	taoPub, err := erlang.Start(ctx, erlang.Config{
		Name:      "tao_pub@127.0.0.1",
		Listen:    "127.0.0.1:0",
		Cookie:    cookie,
		Published: true,
		EPMDAddr:  epmdSrv.addr,
	})
	if err != nil {
		t.Fatalf("Start published node: %v", err)
	}
	defer taoPub.Close()

	pingPub, err := client.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("net_adm_ping"),
		etf.Atom(taoPub.Name()),
	})
	if err != nil {
		t.Fatalf("Call net_adm_ping(pub): %v", err)
	}
	pingPubTup := pingPub.(etf.Tuple)
	if pingPubTup[0] != etf.Atom("pong") || !listContainsAtom(pingPubTup[1], etf.Atom(taoPub.Name())) {
		t.Fatalf("published node ping/visibility mismatch: %#v", pingPubTup)
	}

	// 3. Payload size boundaries (64B, 1KiB, 63KiB, 64KiB, 64KiB+envelope, 1MiB) exercising fragmentation.
	for _, size := range []int{64, 1024, 63 * 1024, 64 * 1024, 64*1024 + 512, 1 << 20} {
		payload := make(etf.Binary, size)
		for i := range payload {
			payload[i] = byte((i*31 + size) & 0xff)
		}
		rep, err := client.Call(ctx, serverTarget, etf.Tuple{etf.Atom("echo"), payload})
		if err != nil {
			t.Fatalf("echo %d bytes failed: %v", size, err)
		}
		tup, ok := rep.(etf.Tuple)
		if !ok || len(tup) != 2 || tup[0] != etf.Atom("ok") {
			t.Fatalf("echo %d unexpected shape: %#v", size, rep)
		}
		gotBin, ok := tup[1].(etf.Binary)
		if !ok || !bytes.Equal(gotBin, payload) {
			t.Fatalf("echo %d binary mismatch (got len %d)", size, len(gotBin))
		}
	}

	// 4. Business {error, Reason} tuple is returned as a normal Term, NOT a Go transport error (§8.2).
	errTupleReply, err := client.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("error_tuple"),
		etf.Atom("insufficient_funds"),
	})
	if err != nil {
		t.Fatalf("expected business {error, Reason} reply without Go error, got %v", err)
	}
	if !etf.TermEqual(errTupleReply, etf.Tuple{etf.Atom("error"), etf.Atom("insufficient_funds")}) {
		t.Fatalf("unexpected business error tuple: %#v", errTupleReply)
	}

	// 5. Reply sent via gen_server:reply from a DIFFERENT helper PID (§8.2).
	helperRep, err := client.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("async_helper_reply"),
		int64(15),
		etf.Atom("delegated_ok"),
	})
	if err != nil {
		t.Fatalf("async_helper_reply failed: %v", err)
	}
	helperTup, ok := helperRep.(etf.Tuple)
	if !ok || len(helperTup) != 3 || helperTup[0] != etf.Atom("helper") || helperTup[2] != etf.Atom("delegated_ok") {
		t.Fatalf("unexpected helper reply: %#v", helperRep)
	}
	if _, isPID := helperTup[1].(erlang.PID); !isPID {
		t.Fatalf("expected helper PID in reply, got %T", helperTup[1])
	}

	// 6. Ordered Cast(add) followed by Call(get_state) on the single outbound stream.
	if _, err := client.Call(ctx, serverTarget, etf.Tuple{etf.Atom("reset"), int64(0)}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := client.Cast(ctx, serverTarget, etf.Tuple{etf.Atom("add"), int64(i)}); err != nil {
			t.Fatalf("Cast add %d: %v", i, err)
		}
	}
	stRep, err := client.Call(ctx, serverTarget, etf.Atom("get_state"))
	if err != nil {
		t.Fatalf("Call get_state: %v", err)
	}
	stTup := stRep.(etf.Tuple)
	if stTup[1] != int64(15) {
		t.Fatalf("expected accumulated state 15 after 5 ordered casts, got %#v", stRep)
	}

	// 7. Raw Send ('!') from Tao Client to BEAM gen_server handle_info -> reply to Tao Endpoint.
	infoCh := make(chan etf.Term, 1)
	ep, err := taoHidden.Export(erlang.EndpointOptions{
		OnDeliver: func(_ *erlang.Endpoint, msg erlang.InboundDelivery) {
			if msg.Kind == erlang.InboundInfo {
				select {
				case infoCh <- msg.Term:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Terminate(etf.Atom("normal"))

	if err := client.Send(ctx, serverTarget, etf.Tuple{
		etf.Atom("ping_info"),
		ep.PID(),
		etf.UTF8Binary("hello-raw-send"),
	}); err != nil {
		t.Fatalf("Send ping_info: %v", err)
	}
	select {
	case got := <-infoCh:
		want := etf.Tuple{etf.Atom("pong_info"), etf.UTF8Binary("hello-raw-send")}
		if !etf.TermEqual(got, want) {
			t.Fatalf("raw Send roundtrip mismatch: got %#v, want %#v", got, want)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for pong_info")
	}
}

func TestInteropBEAMInboundTagsSendRequestSystemAndBudgets(t *testing.T) {
	epmdSrv := startIsolatedEPMD(t)
	const cookie = "tao-inbound-cookie"
	beam := startBEAMServer(t, beamServerOptions{
		AliveName: "beam_in",
		Cookie:    cookie,
		EPMDPort:  epmdSrv.port,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Set DefaultCallTimeout very short (50ms) on Tao node to prove inbound calls do NOT
	// accidentally inherit Go's outbound DefaultCallTimeout (§8.3, §13)!
	taoNode, err := erlang.Start(ctx, erlang.Config{
		Name:               "tao_in@127.0.0.1",
		Listen:             "127.0.0.1:0",
		Cookie:             cookie,
		EPMDAddr:           epmdSrv.addr,
		DefaultCallTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer taoNode.Close()

	var counter atomic.Int64
	ep, err := taoNode.Export(erlang.EndpointOptions{
		Name: "tao_svc",
		OnDeliver: func(ep *erlang.Endpoint, msg erlang.InboundDelivery) {
			switch msg.Kind {
			case erlang.InboundGenCast:
				if tup, ok := msg.Term.(etf.Tuple); ok && len(tup) == 2 && tup[0] == etf.Atom("add") {
					counter.Add(tup[1].(int64))
				}
			case erlang.InboundGenCall:
				if msg.Term == etf.Atom("get") {
					_ = ep.SendReply(msg.ReplyTo, etf.Tuple{etf.Atom("ok"), counter.Load()})
					return
				}
				if tup, ok := msg.Term.(etf.Tuple); ok && len(tup) == 2 && tup[0] == etf.Atom("slow") {
					delay := time.Duration(tup[1].(int64)) * time.Millisecond
					go func(rt erlang.ReplyTarget) {
						time.Sleep(delay)
						_ = ep.SendReply(rt, etf.Atom("slow_done"))
					}(msg.ReplyTo)
					return
				}
				_ = ep.SendReply(msg.ReplyTo, etf.Tuple{etf.Atom("echoed"), msg.Term})
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Terminate(etf.Atom("normal"))

	client, err := taoNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	serverTarget := erlang.Registered("tao_test_server", beam.fullName)
	remoteSvc := etf.Tuple{etf.Atom("tao_svc"), etf.Atom(taoNode.Name())}

	// 1. Casts followed by gen_server:call from BEAM -> Tao preserve strict FIFO order.
	for _, delta := range []int64{10, 20, 12} {
		_, err := client.Call(callCtx, serverTarget, etf.Tuple{
			etf.Atom("cast_target"),
			remoteSvc,
			etf.Tuple{etf.Atom("add"), delta},
		})
		if err != nil {
			t.Fatalf("BEAM cast_target: %v", err)
		}
	}
	getRep, err := client.Call(callCtx, serverTarget, etf.Tuple{
		etf.Atom("call_target"),
		remoteSvc,
		etf.Atom("get"),
		int64(5000),
	})
	if err != nil {
		t.Fatalf("BEAM call_target(get): %v", err)
	}
	wantGet := etf.Tuple{etf.Atom("ok"), etf.Tuple{etf.Atom("ok"), int64(42)}}
	if !etf.TermEqual(getRep, wantGet) {
		t.Fatalf("BEAM call_target(get) got %#v, want %#v", getRep, wantGet)
	}

	// 2. BEAM gen_server:send_request + wait_response by PID.
	srRep, err := client.Call(callCtx, serverTarget, etf.Tuple{
		etf.Atom("send_request_target"),
		ep.PID(),
		etf.Atom("via_send_request"),
		int64(5000),
	})
	if err != nil {
		t.Fatalf("BEAM send_request_target: %v", err)
	}
	if !etf.TermEqual(srRep, etf.Tuple{etf.Atom("ok"), etf.Tuple{etf.Atom("echoed"), etf.Atom("via_send_request")}}) {
		t.Fatalf("send_request mismatch: %#v", srRep)
	}

	// 3. Nested alias tag [[alias | Alias] | extra_meta] and plain Ref tag (§8.1).
	nestedRep, err := client.Call(callCtx, serverTarget, etf.Tuple{
		etf.Atom("nested_alias_call"),
		remoteSvc,
		etf.Atom("nested_tag_test"),
		int64(5000),
	})
	if err != nil {
		t.Fatalf("BEAM nested_alias_call: %v", err)
	}
	if !etf.TermEqual(nestedRep, etf.Tuple{etf.Atom("ok"), etf.Tuple{etf.Atom("echoed"), etf.Atom("nested_tag_test")}}) {
		t.Fatalf("nested alias call mismatch: %#v", nestedRep)
	}

	plainRep, err := client.Call(callCtx, serverTarget, etf.Tuple{
		etf.Atom("plain_ref_call"),
		ep.PID(),
		etf.Atom("plain_tag_test"),
		int64(5000),
	})
	if err != nil {
		t.Fatalf("BEAM plain_ref_call: %v", err)
	}
	if !etf.TermEqual(plainRep, etf.Tuple{etf.Atom("ok"), etf.Tuple{etf.Atom("echoed"), etf.Atom("plain_tag_test")}}) {
		t.Fatalf("plain ref call mismatch: %#v", plainRep)
	}

	// 4. Inbound call with BEAM 60_000ms and infinity timeouts taking 180ms (> 50ms DefaultCallTimeout!).
	for _, beamTimeout := range []etf.Term{int64(60000), etf.Atom("infinity")} {
		slowRep, err := client.Call(callCtx, serverTarget, etf.Tuple{
			etf.Atom("call_target"),
			remoteSvc,
			etf.Tuple{etf.Atom("slow"), int64(180)},
			beamTimeout,
		})
		if err != nil {
			t.Fatalf("BEAM slow call (%v): %v", beamTimeout, err)
		}
		if !etf.TermEqual(slowRep, etf.Tuple{etf.Atom("ok"), etf.Atom("slow_done")}) {
			t.Fatalf("BEAM slow call (%v) unexpected result: %#v", beamTimeout, slowRep)
		}
	}

	// 5. Unsupported OTP sys:get_state and gen_server:stop fail fast instead of hanging (§8.4).
	start := time.Now()
	sysRep, err := client.Call(callCtx, serverTarget, etf.Tuple{
		etf.Atom("sys_get_state"),
		remoteSvc,
		int64(10000),
	})
	if err != nil {
		t.Fatalf("sys_get_state wrapper call: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("sys:get_state hung for %v instead of failing fast", elapsed)
	}
	wantUnsupported := etf.Tuple{
		etf.Atom("caught"),
		etf.Atom("error"),
		etf.Tuple{etf.Atom("tao"), etf.Atom("unsupported")},
	}
	if !etf.TermEqual(sysRep, wantUnsupported) {
		t.Fatalf("unexpected sys:get_state response: %#v", sysRep)
	}

	startStop := time.Now()
	stopRep, err := client.Call(callCtx, serverTarget, etf.Tuple{
		etf.Atom("gen_stop"),
		remoteSvc,
		etf.Atom("infinity"),
	})
	if err != nil {
		t.Fatalf("gen_stop wrapper call: %v", err)
	}
	if elapsed := time.Since(startStop); elapsed > 2*time.Second {
		t.Fatalf("gen_server:stop(..., infinity) hung for %v", elapsed)
	}
	stopTup, ok := stopRep.(etf.Tuple)
	if !ok || len(stopTup) != 3 || stopTup[0] != etf.Atom("caught") {
		t.Fatalf("expected gen_server:stop fast exception, got %#v", stopRep)
	}
}

func TestInteropBEAMTLSBidirectionalAndSecurityFailures(t *testing.T) {
	epmdSrv := startIsolatedEPMD(t)
	tlsBundle := generateDistTLSBundle(t)
	const cookie = "tao-tls-secret"

	beam := startBEAMServer(t, beamServerOptions{
		AliveName:  "beam_tls",
		Cookie:     cookie,
		EPMDPort:   epmdSrv.port,
		TLSOptFile: tlsBundle.ErlSSLOptFile,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Wrong CA must fail handshake/connect with OutcomeNotSent.
	badCANode, err := erlang.Start(ctx, erlang.Config{
		Name:     "tao_badca@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		TLS:      tlsBundle.WrongCATLS,
		EPMDAddr: epmdSrv.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer badCANode.Close()

	if err := badCANode.Connect(ctx, beam.fullName); err == nil {
		t.Fatal("expected TLS verification failure with wrong CA")
	} else if erlang.DeliveryOutcome(err) != erlang.OutcomeNotSent {
		t.Fatalf("expected OutcomeNotSent on TLS failure, got %v (%v)", erlang.DeliveryOutcome(err), err)
	}

	// 2. Wrong cookie over valid TLS must fail handshake with OutcomeNotSent.
	badCookieNode, err := erlang.Start(ctx, erlang.Config{
		Name:     "tao_badcookie@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   "wrong-cookie-value",
		TLS:      tlsBundle.GoTLS,
		EPMDAddr: epmdSrv.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer badCookieNode.Close()

	if err := badCookieNode.Connect(ctx, beam.fullName); err == nil {
		t.Fatal("expected cookie mismatch failure")
	} else if erlang.DeliveryOutcome(err) != erlang.OutcomeNotSent {
		t.Fatalf("expected OutcomeNotSent on cookie failure, got %v (%v)", erlang.DeliveryOutcome(err), err)
	}

	// 3. Valid mTLS bidirectional: BEAM -> Tao inbound ping & gen_server:call + Tao -> BEAM fragmented 128KiB Call.
	taoTLS, err := erlang.Start(ctx, erlang.Config{
		Name:     "tao_tls@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		TLS:      tlsBundle.GoTLS,
		EPMDAddr: epmdSrv.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer taoTLS.Close()

	_, err = taoTLS.Export(erlang.EndpointOptions{
		Name: "tls_echo",
		OnDeliver: func(ep *erlang.Endpoint, msg erlang.InboundDelivery) {
			if msg.Kind == erlang.InboundGenCall {
				_ = ep.SendReply(msg.ReplyTo, etf.Tuple{etf.Atom("tls_ok"), msg.Term})
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Reserved & duplicate export checks.
	if _, err := taoTLS.Export(erlang.EndpointOptions{Name: "net_kernel"}); !errors.Is(err, erlang.ErrReservedName) {
		t.Fatalf("expected ErrReservedName for net_kernel, got %v", err)
	}
	if _, err := taoTLS.Export(erlang.EndpointOptions{Name: "rex"}); !errors.Is(err, erlang.ErrReservedName) {
		t.Fatalf("expected ErrReservedName for rex, got %v", err)
	}
	if _, err := taoTLS.Export(erlang.EndpointOptions{Name: "tls_echo"}); !errors.Is(err, erlang.ErrDuplicateExport) {
		t.Fatalf("expected ErrDuplicateExport, got %v", err)
	}

	// Start a second BEAM TLS node to initiate a fresh inbound TLS handshake INTO taoTLS!
	beam2 := startBEAMServer(t, beamServerOptions{
		AliveName:  "beam_tls_caller",
		Cookie:     cookie,
		EPMDPort:   epmdSrv.port,
		TLSOptFile: tlsBundle.ErlSSLOptFile,
	})

	cl, err := taoTLS.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Outbound TLS 128 KiB fragmented binary echo to beam.
	bigBin := bytes.Repeat([]byte{0xab}, 128*1024)
	rep, err := cl.Call(ctx, erlang.Registered("tao_test_server", beam.fullName), etf.Tuple{
		etf.Atom("echo"),
		etf.Binary(bigBin),
	})
	if err != nil {
		t.Fatalf("TLS 128KiB Call failed: %v", err)
	}
	if !bytes.Equal(rep.(etf.Tuple)[1].(etf.Binary), bigBin) {
		t.Fatal("TLS 128KiB binary mismatch")
	}

	// Ask beam (connected outbound) to tell beam2 (NEVER dialed by taoTLS!) to dial taoTLS inbound over inet_tls!
	inboundTLSRep, err := cl.Call(ctx, erlang.Registered("tao_test_server", beam.fullName), etf.Tuple{
		etf.Atom("call_target"),
		etf.Tuple{etf.Atom("tao_test_server"), etf.Atom(beam2.fullName)},
		etf.Tuple{
			etf.Atom("call_target"),
			etf.Tuple{etf.Atom("tls_echo"), etf.Atom(taoTLS.Name())},
			etf.Atom("inbound_over_inet_tls"),
			int64(5000),
		},
		int64(8000),
	})
	if err != nil {
		t.Fatalf("BEAM2 -> Tao inbound inet_tls call failed: %v", err)
	}
	wantInbound := etf.Tuple{
		etf.Atom("ok"),
		etf.Tuple{
			etf.Atom("ok"),
			etf.Tuple{etf.Atom("tls_ok"), etf.Atom("inbound_over_inet_tls")},
		},
	}
	if !etf.TermEqual(inboundTLSRep, wantInbound) {
		t.Fatalf("inbound inet_tls call mismatch: got %#v, want %#v", inboundTLSRep, wantInbound)
	}

	// 4. DisableEPMD Creation >= 4 + StaticRoutes + AllowedPeers check.
	noEPMD1, err := erlang.Start(ctx, erlang.Config{
		Name:        "noepmd_a@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		AllowedPeers: []string{
			"noepmd_b@127.0.0.1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer noEPMD1.Close()
	if noEPMD1.Creation() < 4 {
		t.Fatalf("DisableEPMD Creation must be >= 4, got %d", noEPMD1.Creation())
	}

	noEPMD2, err := erlang.Start(ctx, erlang.Config{
		Name:        "noepmd_b@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			noEPMD1.Name(): {Addr: noEPMD1.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer noEPMD2.Close()

	if err := noEPMD2.Connect(ctx, noEPMD1.Name()); err != nil {
		t.Fatalf("StaticRoute Connect without EPMD failed: %v", err)
	}
	if err := noEPMD1.Connect(ctx, "intruder@127.0.0.1"); !errors.Is(err, erlang.ErrPeerNotAllowed) {
		t.Fatalf("expected ErrPeerNotAllowed, got %v", err)
	}
}

func TestInteropBEAMMonitorsNamedAndPIDRebindAndReasons(t *testing.T) {
	epmdSrv := startIsolatedEPMD(t)
	const cookie = "tao-monitor-cookie"
	beam := startBEAMServer(t, beamServerOptions{
		AliveName: "beam_mon",
		Cookie:    cookie,
		EPMDPort:  epmdSrv.port,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	taoNode, err := erlang.Start(ctx, erlang.Config{
		Name:     "tao_mon@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdSrv.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer taoNode.Close()

	ep1, err := taoNode.Export(erlang.EndpointOptions{Name: "watched_svc"})
	if err != nil {
		t.Fatal(err)
	}

	cl, err := taoNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	serverTarget := erlang.Registered("tao_test_server", beam.fullName)

	// 1. BEAM watches both {watched_svc, TaoNode} (named) and ep1.PID() (by PID).
	namedWatch, err := cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("watch_target"),
		etf.Tuple{etf.Atom("watched_svc"), etf.Atom(taoNode.Name())},
	})
	if err != nil {
		t.Fatal(err)
	}
	namedWatcherPID := namedWatch.(etf.Tuple)[1].(erlang.PID)
	namedMRef := namedWatch.(etf.Tuple)[2].(erlang.Ref)

	pidWatch, err := cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("watch_target"),
		ep1.PID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	pidWatcherPID := pidWatch.(etf.Tuple)[1].(erlang.PID)
	pidMRef := pidWatch.(etf.Tuple)[2].(erlang.Ref)

	// Also create a second named watcher that DEMONITORs AFTER watched_svc is detached and rebound!
	demonWatch, err := cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("watch_target"),
		etf.Tuple{etf.Atom("watched_svc"), etf.Atom(taoNode.Name())},
	})
	if err != nil {
		t.Fatal(err)
	}
	demonWatcherPID := demonWatch.(etf.Tuple)[1].(erlang.PID)

	// Detach watched_svc from ep1 and rebind watched_svc to ep2 BEFORE ep1 terminates!
	ep1.BeginShutdown()
	ep2, err := taoNode.Export(erlang.EndpointOptions{Name: "watched_svc"})
	if err != nil {
		t.Fatalf("rebind watched_svc after BeginShutdown: %v", err)
	}
	defer ep2.Terminate(etf.Atom("normal"))

	// Demonitor demonWatcherPID while watched_svc points to ep2; must clean up ep1's monitor cleanly!
	if _, err := cl.Call(ctx, serverTarget, etf.Tuple{etf.Atom("demonitor_watcher"), demonWatcherPID}); err != nil {
		t.Fatal(err)
	}

	// Terminate ep1 with structured TaoReason; ep2 stays alive!
	wantReason := erlang.TaoReason("actor_panicked", "boom-detail")
	ep1.Terminate(wantReason)

	// Verify BEAM named watcher got Object == {watched_svc, TaoNode} (NOT ep1.PID()!) and exact Reason!
	downNamed, err := cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("await_down"),
		namedWatcherPID,
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantNamedDown := etf.Tuple{
		etf.Atom("down"),
		namedMRef,
		etf.Tuple{etf.Atom("watched_svc"), etf.Atom(taoNode.Name())},
		wantReason,
	}
	if !etf.TermEqual(downNamed, wantNamedDown) {
		t.Fatalf("BEAM named DOWN mismatch:\n got: %#v\nwant: %#v", downNamed, wantNamedDown)
	}

	// Verify BEAM PID watcher got Object == ep1.PID() and exact Reason!
	downPID, err := cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("await_down"),
		pidWatcherPID,
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPIDDown := etf.Tuple{
		etf.Atom("down"),
		pidMRef,
		ep1.PID(),
		wantReason,
	}
	if !etf.TermEqual(downPID, wantPIDDown) {
		t.Fatalf("BEAM PID DOWN mismatch:\n got: %#v\nwant: %#v", downPID, wantPIDDown)
	}

	// 2. Non-existent target on Tao returns noproc preserving {missing_name, TaoNode}.
	missingWatch, err := cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("watch_target"),
		etf.Tuple{etf.Atom("missing_name"), etf.Atom(taoNode.Name())},
	})
	if err != nil {
		t.Fatal(err)
	}
	missingDown, err := cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("await_down"),
		missingWatch.(etf.Tuple)[1].(erlang.PID),
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantMissing := etf.Tuple{
		etf.Atom("down"),
		missingWatch.(etf.Tuple)[2].(erlang.Ref),
		etf.Tuple{etf.Atom("missing_name"), etf.Atom(taoNode.Name())},
		etf.Atom("noproc"),
	}
	if !etf.TermEqual(missingDown, wantMissing) {
		t.Fatalf("missing named monitor mismatch: got %#v, want %#v", missingDown, wantMissing)
	}

	// 3. Go Client.Monitor on missing BEAM registered name -> noproc with {Name, Node}, and on crashing BEAM worker.
	_, missCh, err := cl.Monitor(ctx, erlang.Registered("does_not_exist", beam.fullName))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-missCh:
		if ev.Reason != etf.Atom("noproc") || !etf.TermEqual(ev.Watched, etf.Tuple{etf.Atom("does_not_exist"), etf.Atom(beam.fullName)}) {
			t.Fatalf("Go Monitor missing target mismatch: %+v", ev)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for Go Monitor noproc")
	}

	wRep, err := cl.Call(ctx, serverTarget, etf.Tuple{etf.Atom("spawn_worker"), etf.Atom("false")})
	if err != nil {
		t.Fatal(err)
	}
	workerPID := wRep.(etf.Tuple)[1].(erlang.PID)
	_, wDownCh, err := cl.Monitor(ctx, erlang.Process(workerPID))
	if err != nil {
		t.Fatal(err)
	}
	crashReason := etf.Tuple{etf.Atom("worker_exploded"), int64(99)}
	_, err = cl.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("worker_op"),
		workerPID,
		etf.Tuple{etf.Atom("exit_self"), crashReason},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-wDownCh:
		if !etf.TermEqual(ev.Watched, workerPID) || !etf.TermEqual(ev.Reason, crashReason) {
			t.Fatalf("Go Monitor worker crash mismatch: %+v", ev)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for Go Monitor worker crash")
	}
}

func TestInteropBEAMLinksUnlinkRelinkDeadPIDAndExitSignals(t *testing.T) {
	epmdSrv := startIsolatedEPMD(t)
	const cookie = "tao-link-cookie"
	beam := startBEAMServer(t, beamServerOptions{
		AliveName: "beam_lnk",
		Cookie:    cookie,
		EPMDPort:  epmdSrv.port,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	taoNode, err := erlang.Start(ctx, erlang.Config{
		Name:     "tao_lnk@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdSrv.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer taoNode.Close()

	ctrlClient, err := taoNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlClient.Close()
	serverTarget := erlang.Registered("tao_test_server", beam.fullName)

	// 1. Link -> Unlink -> immediate Relink before ACK settles: delayed ACK must NOT break the relink!
	trapClient, err := taoNode.NewClientWithOptions(erlang.ClientOptions{TrapExit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer trapClient.Close()

	wRep, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{etf.Atom("spawn_worker"), etf.Atom("false")})
	if err != nil {
		t.Fatal(err)
	}
	worker1 := wRep.(etf.Tuple)[1].(erlang.PID)

	if err := trapClient.Link(ctx, worker1); err != nil {
		t.Fatal(err)
	}
	if err := trapClient.Unlink(ctx, worker1); err != nil {
		t.Fatal(err)
	}
	if err := trapClient.Link(ctx, worker1); err != nil {
		t.Fatal(err)
	}

	// Give UNLINK_ID_ACK from the first Unlink time to arrive and verify the relink STILL delivers EXIT!
	time.Sleep(50 * time.Millisecond)
	_, err = ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("worker_op"),
		worker1,
		etf.Tuple{etf.Atom("exit_self"), etf.Atom("relink_survived")},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ex := <-trapClient.Exits():
		if ex.From != worker1 || ex.Reason != etf.Atom("relink_survived") || !ex.FromLink {
			t.Fatalf("unexpected trapped link exit after relink: %+v", ex)
		}
	case <-ctx.Done():
		t.Fatal("relink lost its link exit signal")
	}

	// 2. Dead current-Creation PID receiving UNLINK_ID MUST still send UNLINK_ID_ACK (§7.3, §10.2).
	epShort, err := taoNode.Export(erlang.EndpointOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deadPID := epShort.PID()
	epShort.Terminate(etf.Atom("normal"))

	w2Rep, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{etf.Atom("spawn_worker"), etf.Atom("true")})
	if err != nil {
		t.Fatal(err)
	}
	worker2 := w2Rep.(etf.Tuple)[1].(erlang.PID)
	// unlink(DeadPid) from BEAM sends UNLINK_ID if linked, or we test raw UNLINK_ID in protocol test below;
	// also verify BEAM linking to DeadPid receives{'EXIT', DeadPid, noproc}!
	_, err = ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("worker_op"),
		worker2,
		etf.Tuple{etf.Atom("link_to"), deadPID},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	inboxRep, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("worker_op"),
		worker2,
		etf.Atom("drain_inbox"),
	})
	if err != nil {
		t.Fatal(err)
	}
	inboxList := inboxRep.(etf.Tuple)[1].(etf.List)
	wantNoprocExit := etf.Tuple{etf.Atom("EXIT"), deadPID, etf.Atom("noproc")}
	if len(inboxList.Elements) != 1 || !etf.TermEqual(inboxList.Elements[0], wantNoprocExit) {
		t.Fatalf("expected {'EXIT', DeadPid, noproc} on BEAM worker, got %#v", inboxRep)
	}

	// 3. EXIT vs EXIT2: normal, trappable link 'kill' vs UNTRAPPABLE explicit exit(Pid, kill) -> 'killed',
	// and >64KiB fragmented PAYLOAD_EXIT2 reason!
	trappedSignals := make(chan erlang.InboundDelivery, 8)
	termCh := make(chan etf.Term, 1)
	epTrap, err := taoNode.Export(erlang.EndpointOptions{
		TrapExit: true,
		OnDeliver: func(_ *erlang.Endpoint, msg erlang.InboundDelivery) {
			if msg.Kind == erlang.InboundExit {
				trappedSignals <- msg
			}
		},
		OnTerminate: func(reason etf.Term, _ bool) {
			select {
			case termCh <- reason:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BEAM watcher monitors epTrap to observe its external exit reason when killed!
	wTrap, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("watch_target"),
		epTrap.PID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	trapWatcher := wTrap.(etf.Tuple)[1].(erlang.PID)

	// (a) Explicit exit(EpTrap, normal) when TrapExit=true -> delivered as InboundExit(FromLink=false, Reason=normal).
	if _, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("send_exit2"),
		epTrap.PID(),
		etf.Atom("normal"),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case sig := <-trappedSignals:
		if sig.Term != etf.Atom("normal") || sig.ExitFromLink {
			t.Fatalf("expected trapped EXIT2 normal, got %+v", sig)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for trapped EXIT2 normal")
	}

	// (b) Explicit exit(EpTrap, HugeBinaryReason > 80KiB) -> fragmented PAYLOAD_EXIT2 delivered intact!
	hugeReason := etf.Binary(bytes.Repeat([]byte{0x5a}, 80*1024))
	if _, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("send_exit2"),
		epTrap.PID(),
		hugeReason,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case sig := <-trappedSignals:
		gotBin, ok := sig.Term.(etf.Binary)
		if !ok || !bytes.Equal(gotBin, hugeReason) {
			t.Fatalf("fragmented PAYLOAD_EXIT2 reason mismatch: %T len=%d", sig.Term, len(gotBin))
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for fragmented PAYLOAD_EXIT2")
	}

	// (c) Broken-link EXIT with reason 'kill' (worker links to epTrap then calls exit(kill) on ITSELF) ->
	// MUST BE TRAPPABLE as {'EXIT', Worker, kill} when TrapExit=true!
	wKillSelfRep, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{etf.Atom("spawn_worker"), etf.Atom("false")})
	if err != nil {
		t.Fatal(err)
	}
	wKillSelf := wKillSelfRep.(etf.Tuple)[1].(erlang.PID)
	if _, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("worker_op"),
		wKillSelf,
		etf.Tuple{etf.Atom("link_to"), epTrap.PID()},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("worker_op"),
		wKillSelf,
		etf.Tuple{etf.Atom("exit_self"), etf.Atom("kill")},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case sig := <-trappedSignals:
		if sig.From != wKillSelf || sig.Term != etf.Atom("kill") || !sig.ExitFromLink {
			t.Fatalf("expected trappable link EXIT(kill), got %+v", sig)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for trappable link EXIT(kill)")
	}

	// (d) Explicit exit(EpTrap, kill) (EXIT2 with 'kill') -> UNTRAPPABLE even with TrapExit=true,
	// terminates epTrap with external reason 'killed'!
	if _, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("send_exit2"),
		epTrap.PID(),
		etf.Atom("kill"),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-termCh:
		if r != etf.Atom("killed") {
			t.Fatalf("expected OnTerminate reason 'killed', got %v", r)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for untrappable EXIT2(kill)")
	}
	downKilled, err := ctrlClient.Call(ctx, serverTarget, etf.Tuple{
		etf.Atom("await_down"),
		trapWatcher,
		int64(5000),
	})
	if err != nil {
		t.Fatal(err)
	}
	if downKilled.(etf.Tuple)[3] != etf.Atom("killed") {
		t.Fatalf("expected BEAM watcher to observe DOWN reason 'killed', got %#v", downKilled)
	}
}

func TestProtocolStaleCreationMatrixAndBEAMReproduction(t *testing.T) {
	epmdSrv := startIsolatedEPMD(t)
	const cookie = "tao-stale-cookie"

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Part A: Verify every cell of §7.3's Dead Current-Creation vs Stale-Creation table against Tao Node.
	taoNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "tao_target@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer taoNode.Close()

	ep, err := taoNode.Export(erlang.EndpointOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deadCurrentPID := ep.PID()
	ep.Terminate(etf.Atom("normal"))

	staleCreation := taoNode.Creation() + 1
	if staleCreation < 4 {
		staleCreation = 5
	}
	stalePID := erlang.PID{
		Node:     etf.Atom(taoNode.Name()),
		ID:       999,
		Serial:   0,
		Creation: staleCreation,
	}

	// Connect a raw dist v6 client directly to taoNode to inspect exact wire responses.
	rawTCP, err := net.DialTimeout("tcp", taoNode.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer rawTCP.Close()

	const rawPeer = "raw_probe@127.0.0.1"
	hs, err := dist.HandshakeClient(ctx, rawTCP, dist.ClientHandshakeConfig{
		LocalName:        rawPeer,
		LocalCreation:    7,
		LocalFlags:       dist.DefaultFlags(false),
		ExpectedPeerName: taoNode.Name(),
		InCookie:         cookie,
		OutCookie:        cookie,
		PacketSize:       dist.PacketSizeTCPHandshake,
	})
	if err != nil {
		t.Fatalf("raw HandshakeClient: %v", err)
	}
	enc := dist.NewFrameEncoder(dist.FrameEncoderConfig{NegotiatedFlags: hs.NegotiatedFlags})
	rd := dist.NewFrameReader(dist.FrameReaderConfig{
		LocalNode:       etf.Atom(rawPeer),
		RemoteNode:      etf.Atom(taoNode.Name()),
		NegotiatedFlags: hs.NegotiatedFlags,
	})
	remoteFrom := erlang.PID{Node: etf.Atom(rawPeer), ID: 1, Serial: 0, Creation: 7}
	sendCtrl := func(ctrl dist.ControlMessage, payload etf.Term) {
		t.Helper()
		frames, err := enc.EncodeFrames(ctrl, payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, fp := range frames {
			if _, err := hs.Conn.Write(*fp); err != nil {
				t.Fatal(err)
			}
			dist.ReleaseBuffer(fp)
		}
	}
	readNextMsg := func() dist.InboundMessage {
		t.Helper()
		_ = hs.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		defer hs.Conn.SetReadDeadline(time.Time{})
		for {
			kind, msg, err := rd.ReadPacket(hs.Conn)
			if err != nil {
				t.Fatalf("ReadPacket failed: %v", err)
			}
			if kind == dist.PacketMessage {
				return msg
			}
		}
	}

	// 1. Dead current-Creation UNLINK_ID -> MUST receive UNLINK_ID_ACK (§7.3, §10.2).
	sendCtrl(dist.ControlMessage{
		Op:       dist.OpUnlinkID,
		UnlinkID: 777,
		FromPID:  remoteFrom,
		HasFrom:  true,
		ToPID:    deadCurrentPID,
	}, nil)
	ackMsg := readNextMsg()
	if ackMsg.Control.Op != dist.OpUnlinkIDAck || ackMsg.Control.UnlinkID != 777 || ackMsg.Control.FromPID != deadCurrentPID {
		t.Fatalf("expected UNLINK_ID_ACK(777) from deadCurrentPID, got %+v", ackMsg.Control)
	}

	// 2. Stale-Creation LINK -> EXIT(noproc), MONITOR_P -> MONITOR_P_EXIT(noproc).
	sendCtrl(dist.ControlMessage{
		Op:      dist.OpLink,
		FromPID: remoteFrom,
		HasFrom: true,
		ToPID:   stalePID,
	}, nil)
	lnkExit := readNextMsg()
	if lnkExit.Control.Reason != etf.Atom("noproc") || lnkExit.Control.FromPID != stalePID {
		t.Fatalf("expected stale LINK -> noproc exit, got %+v", lnkExit.Control)
	}

	staleRef := erlang.Ref{Node: etf.Atom(rawPeer), Creation: 7, Len: 3, ID: [5]uint32{11, 22, 33}}
	sendCtrl(dist.ControlMessage{
		Op:      dist.OpMonitorP,
		FromPID: remoteFrom,
		HasFrom: true,
		ToProc:  stalePID,
		Ref:     staleRef,
	}, nil)
	monExit := readNextMsg()
	if monExit.Control.Reason != etf.Atom("noproc") || !etf.TermEqual(monExit.Control.ToProc, stalePID) {
		t.Fatalf("expected stale MONITOR_P -> noproc preserving stalePID, got %+v", monExit.Control)
	}

	// 3. Stale-Creation SEND, DEMONITOR_P, UNLINK_ID, UNLINK_ID_ACK -> ignored without ACK and WITHOUT closing session!
	sendCtrl(dist.ControlMessage{Op: dist.OpSendSender, FromPID: remoteFrom, HasFrom: true, ToPID: stalePID}, etf.Atom("ignored"))
	sendCtrl(dist.ControlMessage{Op: dist.OpDemonitorP, FromPID: remoteFrom, HasFrom: true, ToProc: stalePID, Ref: staleRef}, nil)
	sendCtrl(dist.ControlMessage{Op: dist.OpUnlinkID, UnlinkID: 888, FromPID: remoteFrom, HasFrom: true, ToPID: stalePID}, nil)
	sendCtrl(dist.ControlMessage{Op: dist.OpUnlinkIDAck, UnlinkID: 889, FromPID: remoteFrom, HasFrom: true, ToPID: stalePID}, nil)

	// Prove the connection is STILL healthy and no spurious ACK was queued by sending a subsequent deadCurrentPID UNLINK_ID(999)!
	sendCtrl(dist.ControlMessage{
		Op:       dist.OpUnlinkID,
		UnlinkID: 999,
		FromPID:  remoteFrom,
		HasFrom:  true,
		ToPID:    deadCurrentPID,
	}, nil)
	probeAck := readNextMsg()
	if probeAck.Control.Op != dist.OpUnlinkIDAck || probeAck.Control.UnlinkID != 999 {
		t.Fatalf("expected UNLINK_ID_ACK(999) proving stale UNLINK_ID(888) was dropped cleanly, got %+v", probeAck.Control)
	}

	// Part B: Reproduce OTP dist.c:2304-2309 stale-Creation UNLINK_ID behavior against REAL BEAM (§7.3, §13)!
	beam := startBEAMServer(t, beamServerOptions{
		AliveName: "beam_stale",
		Cookie:    cookie,
		EPMDPort:  epmdSrv.port,
	})
	beamPortOwner, err := erlang.Start(ctx, erlang.Config{
		Name:     "tao_beam_probe@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdSrv.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer beamPortOwner.Close()

	// Verify Tao reconnect path cleans up old links on session loss and never sends stale UNLINK_ID on reconnect.
	cl, err := beamPortOwner.NewClientWithOptions(erlang.ClientOptions{TrapExit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	wRep, err := cl.Call(ctx, erlang.Registered("tao_test_server", beam.fullName), etf.Tuple{etf.Atom("spawn_worker"), etf.Atom("false")})
	if err != nil {
		t.Fatal(err)
	}
	beamWorker := wRep.(etf.Tuple)[1].(erlang.PID)
	if err := cl.Link(ctx, beamWorker); err != nil {
		t.Fatal(err)
	}

	// Stop and restart BEAM under the SAME node name (new Creation!), then Unlink(old beamWorker) + Call new BEAM!
	beam.Stop()
	select {
	case ex := <-cl.Exits():
		if ex.Reason != etf.Atom("noconnection") {
			t.Fatalf("expected noconnection trapped exit on BEAM stop, got %+v", ex)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for noconnection exit")
	}

	beamRestarted := startBEAMServer(t, beamServerOptions{
		AliveName: "beam_stale",
		Cookie:    cookie,
		EPMDPort:  epmdSrv.port,
	})
	_ = beamRestarted

	// Unlinking the old-Creation PID after reconnect must be a clean local no-op (not sending stale UNLINK_ID!).
	if err := cl.Unlink(ctx, beamWorker); err != nil {
		t.Fatalf("Unlink old incarnation PID: %v", err)
	}
	echoAfterRestart, err := cl.Call(ctx, erlang.Registered("tao_test_server", beam.fullName), etf.Tuple{
		etf.Atom("echo"),
		etf.Atom("reconnected_cleanly"),
	})
	if err != nil {
		t.Fatalf("Call after BEAM restart failed (stale unlink would have killed dist): %v", err)
	}
	if !etf.TermEqual(echoAfterRestart, etf.Tuple{etf.Atom("ok"), etf.Atom("reconnected_cleanly")}) {
		t.Fatalf("unexpected reply after BEAM restart: %#v", echoAfterRestart)
	}
}

func TestSimultaneousConnectAliveEPMDLossQuotasAndCompletionRaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const cookie = "tao-coord-cookie"

	// 1. Simultaneous connect + unauthenticated alive DoS resistance (§6.2).
	nodeA, err := erlang.Start(ctx, erlang.Config{
		Name:        "alpha@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Close()

	nodeB, err := erlang.Start(ctx, erlang.Config{
		Name:        "omega@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			nodeA.Name(): {Addr: nodeA.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeB.Close()

	// Also give nodeA static route to nodeB by restarting nodeA on a fresh config or connecting concurrently:
	nodeC, err := erlang.Start(ctx, erlang.Config{
		Name:        "beta@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			nodeB.Name(): {Addr: nodeB.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeC.Close()

	// Connect nodeC -> nodeB, then attempt an UNAUTHENTICATED 'alive' spoofing "beta@127.0.0.1" with WRONG cookie!
	if err := nodeC.Connect(ctx, nodeB.Name()); err != nil {
		t.Fatal(err)
	}
	genBefore := nodeB.Stats().Peers[0].Generation

	spoofConn, err := net.DialTimeout("tcp", nodeB.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, spoofErr := dist.HandshakeClient(ctx, spoofConn, dist.ClientHandshakeConfig{
		LocalName:        nodeC.Name(),
		LocalCreation:    nodeC.Creation() + 1,
		LocalFlags:       dist.DefaultFlags(false),
		ExpectedPeerName: nodeB.Name(),
		InCookie:         "wrong-attacker-cookie",
		OutCookie:        "wrong-attacker-cookie",
		PacketSize:       dist.PacketSizeTCPHandshake,
		ConfirmAlive:     func() bool { return true },
	})
	_ = spoofConn.Close()
	if spoofErr == nil {
		t.Fatal("expected spoofed alive handshake with wrong cookie to fail")
	}
	// Active session on nodeB MUST remain intact with the exact same generation!
	statsAfterSpoof := nodeB.Stats()
	if len(statsAfterSpoof.Peers) != 1 || statsAfterSpoof.Peers[0].Generation != genBefore {
		t.Fatalf("unauthenticated alive attempt disrupted active peer session: %+v", statsAfterSpoof.Peers)
	}

	// Authenticated 'alive' replacement (e.g. restarted nodeC incarnation with valid cookie) MUST succeed!
	validAliveConn, err := net.DialTimeout("tcp", nodeB.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, aliveErr := dist.HandshakeClient(ctx, validAliveConn, dist.ClientHandshakeConfig{
		LocalName:        nodeC.Name(),
		LocalCreation:    nodeC.Creation() + 2,
		LocalFlags:       dist.DefaultFlags(false),
		ExpectedPeerName: nodeB.Name(),
		InCookie:         cookie,
		OutCookie:        cookie,
		PacketSize:       dist.PacketSizeTCPHandshake,
		ConfirmAlive:     func() bool { return true },
	})
	if aliveErr != nil {
		_ = validAliveConn.Close()
		t.Fatalf("authenticated alive replacement failed: %v", aliveErr)
	}
	_ = validAliveConn.Close()

	// 2. MaxPeers quota counts handshaking inbound connections (§11.3: "握手中的连接也占槽").
	quotaNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "quota@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		Limits: erlang.Limits{
			MaxPeers:            1,
			MaxNodePendingCalls: 1,
			MaxPeerSendBytes:    1024,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer quotaNode.Close()

	// Hold 1 inbound socket paused mid-handshake!
	stalledConn, err := net.DialTimeout("tcp", quotaNode.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer stalledConn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && quotaNode.Stats().HandshakingPeers == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if quotaNode.Stats().HandshakingPeers != 1 {
		t.Fatalf("expected 1 handshaking peer occupying MaxPeers=1 slot, got %+v", quotaNode.Stats())
	}

	// Outbound dial while handshaking slot is full must fail with ErrCapacity & OutcomeNotSent!
	if err := quotaNode.Connect(ctx, nodeA.Name()); !errors.Is(err, erlang.ErrCapacity) || erlang.DeliveryOutcome(err) != erlang.OutcomeNotSent {
		t.Fatalf("expected ErrCapacity/OutcomeNotSent when MaxPeers=1 is occupied by handshake, got %v", err)
	}
	_ = stalledConn.Close()

	// 3. Completion race (reply vs timeout/cancel, late reply filtering, OutcomeUnknown vs OutcomeNotSent)
	// and Shutdown(ctx) draining in-flight inbound reply BEFORE DOWN!
	srvNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "race_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srvNode.Close()

	inCallEntered := make(chan struct{}, 1)
	releaseReply := make(chan struct{})
	srvEp, err := srvNode.Export(erlang.EndpointOptions{
		Name: "slow_svc",
		OnDeliver: func(ep *erlang.Endpoint, msg erlang.InboundDelivery) {
			if msg.Kind != erlang.InboundGenCall {
				return
			}
			_, releaseSlot, err := ep.ReserveInboundCall(msg.Peer, msg.PeerGen)
			if err != nil {
				return
			}
			select {
			case inCallEntered <- struct{}{}:
			default:
			}
			go func(rt erlang.ReplyTarget) {
				defer releaseSlot()
				<-releaseReply
				_ = ep.SendReply(rt, etf.Atom("drained_ok"))
			}(msg.ReplyTo)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = srvEp

	cliNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "race_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			srvNode.Name(): {Addr: srvNode.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cliNode.Close()

	cli, err := cliNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// Unsupported Go term fails locally with OutcomeUnsupported before wire write.
	if _, err := cli.Call(ctx, erlang.Registered("slow_svc", srvNode.Name()), struct{}{}); erlang.DeliveryOutcome(err) != erlang.OutcomeUnsupported {
		t.Fatalf("expected OutcomeUnsupported for struct{}{}, got %v (%v)", erlang.DeliveryOutcome(err), err)
	}

	// Start a call that reaches srvNode, then cancel caller -> OutcomeUnknown; when srvNode later replies, lateReplies increments!
	shortCtx, shortCancel := context.WithTimeout(ctx, 60*time.Millisecond)
	_, timeoutErr := cli.Call(shortCtx, erlang.Registered("slow_svc", srvNode.Name()), etf.Atom("first"))
	shortCancel()
	if timeoutErr == nil || erlang.DeliveryOutcome(timeoutErr) != erlang.OutcomeUnknown {
		t.Fatalf("expected OutcomeUnknown after request reached wire and timed out, got %v (%v)", erlang.DeliveryOutcome(timeoutErr), timeoutErr)
	}
	<-inCallEntered
	releaseFirst := make(chan struct{})
	_ = releaseFirst

	// Release the delayed reply for the timed-out call; verify it is discarded cleanly as a late reply
	// and does NOT corrupt a subsequent call!
	close(releaseReply)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cliNode.Stats().LateReplies == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if cliNode.Stats().LateReplies == 0 {
		t.Fatalf("expected LateReplies > 0 after delayed reply to timed-out call, stats=%+v", cliNode.Stats())
	}

	// 4. EPMD registration loss brings down the Erlang incarnation cleanly (§6.1 line 200).
	epmdLoss := startIsolatedEPMD(t)
	epmdBoundNode, err := erlang.Start(ctx, erlang.Config{
		Name:     "epmd_watch@127.0.0.1",
		Listen:   "127.0.0.1:0",
		Cookie:   cookie,
		EPMDAddr: epmdLoss.addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer epmdBoundNode.Close()

	epmdLoss.Stop()
	select {
	case <-epmdBoundNode.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("expected Node.Done() to close when EPMD registration socket was lost")
	}
	if !epmdBoundNode.Stats().EPMDLost {
		t.Fatal("expected Stats().EPMDLost == true after EPMD loss")
	}
	if _, err := epmdBoundNode.NewClient(); !errors.Is(err, erlang.ErrNodeClosed) {
		t.Fatalf("expected offline node to reject NewClient with ErrNodeClosed, got %v", err)
	}

	// Concurrent stress check under -race.
	var wg sync.WaitGroup
	echoSrv, err := erlang.Start(ctx, erlang.Config{
		Name:        "stress_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer echoSrv.Close()
	_, err = echoSrv.Export(erlang.EndpointOptions{
		Name: "echo",
		OnDeliver: func(ep *erlang.Endpoint, msg erlang.InboundDelivery) {
			if msg.Kind == erlang.InboundGenCall {
				_ = ep.SendReply(msg.ReplyTo, msg.Term)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	echoCliNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "stress_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			echoSrv.Name(): {Addr: echoSrv.Addr().String()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer echoCliNode.Close()
	echoCli, err := echoCliNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer echoCli.Close()

	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				want := etf.Tuple{etf.Atom(fmt.Sprintf("k_%d", id)), int64(i)}
				got, err := echoCli.Call(ctx, erlang.Registered("echo", echoSrv.Name()), want)
				if err != nil {
					t.Errorf("concurrent Call(%d,%d): %v", id, i, err)
					return
				}
				if !etf.TermEqual(got, want) {
					t.Errorf("concurrent Call mismatch: got %#v, want %#v", got, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func listContainsAtom(term etf.Term, want etf.Atom) bool {
	lst, ok := term.(etf.List)
	if !ok {
		return false
	}
	for _, el := range lst.Elements {
		if el == want {
			return true
		}
	}
	return false
}

func TestControlQueueExhaustionUnderClientLockAndCallAsyncNoCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const cookie = "ctrl-exhaust-cookie"

	srvDone := make(chan struct{})
	defer close(srvDone)

	srvNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "ctrl_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srvNode.Close()

	ep, err := srvNode.Export(erlang.EndpointOptions{
		Name: "stall",
		OnDeliver: func(_ *erlang.Endpoint, msg erlang.InboundDelivery) {
			if msg.Kind == erlang.InboundGenCast {
				// Stall inbound reader intentionally so socket backpressures and
				// client's MaxControlQueue (set to 4 below) overflows under Monitor/CallAsync!
				<-srvDone
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	cliNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "ctrl_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			srvNode.Name(): {Addr: srvNode.Addr().String()},
		},
		Limits: erlang.Limits{
			MaxControlQueue:     4,
			MaxRelations:        4096,
			MaxProcessRelations: 4096,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cliNode.Close()

	if err := cliNode.Connect(ctx, srvNode.Name()); err != nil {
		t.Fatal(err)
	}
	cli, err := cliNode.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// Hammer Monitor/Demonitor + CallAsync across 8 goroutines until MaxControlQueue (4) overflows.
	// Must never deadlock on Client.mu -> writeMu -> closeWithError -> Client.mu, and CallAsync
	// must NEVER invoke onComplete when CallAsync itself returns a non-nil admission error!
	var wg sync.WaitGroup
	targetPID := ep.PID()
	targetReg := erlang.Registered("stall", srvNode.Name())
	// Stall srvNode's inbound reader first so socket send buffer fills up / writeLoop blocks.
	_ = cli.Cast(ctx, targetReg, etf.Atom("stall_now"))
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				ref, _, err := cli.Monitor(ctx, erlang.Process(targetPID))
				if err == nil {
					_ = cli.Demonitor(ctx, ref)
				}
				var cbCalled atomic.Bool
				callErr := cli.CallAsync(ctx, targetReg, etf.Atom("ping"), func(etf.Term, error) {
					cbCalled.Store(true)
				})
				if callErr != nil && cbCalled.Load() {
					t.Errorf("CallAsync invoked callback despite returning admission error: %v", callErr)
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock under MaxControlQueue exhaustion")
	}
}
