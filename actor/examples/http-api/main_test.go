package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
)

func exampleNode(t *testing.T) *actor.Node {
	t.Helper()
	node, err := actor.NewNode(actor.NodeOptions{Name: "http-test", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := node.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	if _, err := node.Spawn(func() actor.Behavior { return &auditActor{} }, actor.WithName("audit")); err != nil {
		t.Fatal(err)
	}
	return node
}

func exampleGateway(t *testing.T) (*actor.Node, http.Handler) {
	t.Helper()
	node := exampleNode(t)
	if _, err := node.Spawn(func() actor.Behavior { return &accountSupervisor{} }, actor.WithName("accounts")); err != nil {
		t.Fatal(err)
	}
	return node, (&httpGateway{node: node, callTimeout: time.Minute}).routes()
}

func request(handler http.Handler, ctx context.Context, method, path, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
	handler.ServeHTTP(recorder, req)
	return recorder
}

func snapshotResponse(t *testing.T, recorder *httptest.ResponseRecorder) accountSnapshot {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", recorder.Code, recorder.Body.String())
	}
	var snapshot accountSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func errorResponse(t *testing.T, recorder *httptest.ResponseRecorder, status int, outcome string) {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != status || body["outcome"] != outcome || body["error"] == "" {
		t.Fatalf("HTTP %d %v, want status=%d outcome=%s", recorder.Code, body, status, outcome)
	}
}

func await(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("example operation did not make progress")
	}
}

func TestHTTPConcurrentDepositsAndRejectionsKeepState(t *testing.T) {
	node, handler := exampleGateway(t)
	results := make(chan *httptest.ResponseRecorder, 20)
	var group sync.WaitGroup
	for range cap(results) {
		group.Go(func() { results <- request(handler, t.Context(), "POST", "/accounts/alice/deposit", `{"amount":10}`) })
	}
	finished := make(chan struct{})
	go func() { group.Wait(); close(finished) }()
	await(t, finished)
	for range cap(results) {
		snapshotResponse(t, <-results)
	}
	before := snapshotResponse(t, request(handler, t.Context(), "GET", "/accounts/alice", ""))
	if before.Balance != 200 || before.TxCount != 20 {
		t.Fatalf("concurrent deposits: %+v", before)
	}
	errorResponse(t, request(handler, t.Context(), "POST", "/accounts/alice/withdraw", `{"amount":201}`), 409, "responded")
	if _, err := node.Call[accountSnapshot](t.Context(), actor.Ref{Path: "accounts/alice"}, deposit{Amount: -1}); !errors.Is(err, errInvalidAmount) {
		t.Fatalf("Actor input boundary: %v", err)
	}
	after := snapshotResponse(t, request(handler, t.Context(), "GET", "/accounts/alice", ""))
	if after != before {
		t.Fatalf("rejection changed balance, counter or PID: before=%+v after=%+v", before, after)
	}
	limit := snapshotResponse(t, request(handler, t.Context(), "GET", "/accounts/limit", ""))
	full := snapshotResponse(t, request(handler, t.Context(), "POST", "/accounts/limit/deposit", fmt.Sprintf(`{"amount":%d}`, int64(math.MaxInt64))))
	if full.Balance != math.MaxInt64 || full.TxCount != 1 || full.PID != limit.PID {
		t.Fatalf("int64 boundary: %+v", full)
	}
	errorResponse(t, request(handler, t.Context(), "POST", "/accounts/limit/deposit", `{"amount":1}`), 409, "responded")
	if got := snapshotResponse(t, request(handler, t.Context(), "GET", "/accounts/limit", "")); got != full {
		t.Fatalf("overflow rejection changed state: %+v", got)
	}
}

func TestHTTPInputBoundariesDoNotReachAccounts(t *testing.T) {
	node, handler := exampleGateway(t)
	for _, body := range []string{"", "null", `{}`, `{"amount":0}`, `{"amount":-1}`, `{"amount":1.5}`, `{"amount":9223372036854775808}`, `{"amount":1,"other":2}`, `{"amount":1}{"amount":2}`} {
		errorResponse(t, request(handler, t.Context(), "POST", "/accounts/alice/deposit", body), 400, "not_sent")
	}
	errorResponse(t, request(handler, t.Context(), "POST", "/accounts/alice/deposit", `{"amount":`+strings.Repeat("9", 2048)+`}`), 413, "not_sent")
	for _, id := range []string{"bad%2Fid", "bad:node", strings.Repeat("a", 65)} {
		errorResponse(t, request(handler, t.Context(), "GET", "/accounts/"+id, ""), 400, "not_sent")
	}
	if node.Stats().Actors != 2 {
		t.Fatal("invalid HTTP input created an account")
	}
	gateway := &httpGateway{node: node, callTimeout: time.Minute}
	for _, id := range []string{"", ".", "..", "a/b", "node://other/account", "中文"} {
		if _, err := gateway.callAccount(t.Context(), id, getAccount{}); !errors.Is(err, errInvalidAccountID) {
			t.Fatalf("account address %q: %v", id, err)
		}
	}
}

func TestAccountCreationCapacityDoesNotRestartSupervisor(t *testing.T) {
	node, err := actor.NewNode(actor.NodeOptions{Name: "capacity-test", MaxActors: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := node.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	pid, err := node.Spawn(func() actor.Behavior { return &accountSupervisor{} }, actor.WithName("accounts"))
	if err != nil {
		t.Fatal(err)
	}
	handler := (&httpGateway{node: node, callTimeout: time.Minute}).routes()
	errorResponse(t, request(handler, t.Context(), "GET", "/accounts/alice", ""), 503, "not_sent")
	view, err := node.ActorSnapshot(pid)
	if err != nil || !view.Published || view.Restarts != 0 || node.Stats().Actors != 1 {
		t.Fatalf("creation rejection restarted supervisor: %+v %v", view, err)
	}
}

type fixtureSupervisor struct {
	actor.Actor
	factory actor.Factory
	mailbox int
	child   chan actor.PID
	ensures *atomic.Int32
}

func (s *fixtureSupervisor) Receive(message any) error {
	switch message.(type) {
	case actor.Started:
		pid, err := s.Spawn(s.factory, actor.WithName("alice"), actor.WithMailboxSize(s.mailbox))
		if err != nil {
			return err
		}
		s.child <- pid
	case ensureAccount:
		s.ensures.Add(1)
		return s.Reply(actor.Ref{Path: "accounts/alice"})
	}
	return nil
}

func fixtureGateway(t *testing.T, factory actor.Factory, mailbox int) (*actor.Node, *httpGateway, actor.PID, *atomic.Int32) {
	t.Helper()
	node := exampleNode(t)
	child := make(chan actor.PID, 1)
	ensures := new(atomic.Int32)
	if _, err := node.Spawn(func() actor.Behavior {
		return &fixtureSupervisor{factory: factory, mailbox: mailbox, child: child, ensures: ensures}
	}, actor.WithName("accounts")); err != nil {
		t.Fatal(err)
	}
	return node, &httpGateway{node: node, callTimeout: time.Minute}, <-child, ensures
}

type gatedAccount struct {
	accountActor
	entered chan struct{}
	release <-chan struct{}
	blocked bool
}

func (a *gatedAccount) Receive(message any) error {
	switch message.(type) {
	case deposit, withdraw:
		if !a.blocked {
			a.blocked = true
			close(a.entered)
			<-a.release
		}
	}
	return a.accountActor.Receive(message)
}

func TestHTTPLateReplyKeepsAccountPIDAndCommittedState(t *testing.T) {
	for _, mode := range []string{"canceled", "deadline", "rejection-canceled"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			defer resume()
			var starts atomic.Int32
			_, gateway, pid, _ := fixtureGateway(t, func() actor.Behavior {
				starts.Add(1)
				return &gatedAccount{accountActor: accountActor{id: "alice", balance: 5}, entered: entered, release: release}
			}, 8)
			if mode == "deadline" {
				gateway.callTimeout = time.Second
			}
			handler := gateway.routes()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			path, body := "/accounts/alice/deposit", `{"amount":10}`
			if mode == "rejection-canceled" {
				path, body = "/accounts/alice/withdraw", `{"amount":100}`
			}
			returned := make(chan *httptest.ResponseRecorder, 1)
			go func() { returned <- request(handler, ctx, "POST", path, body) }()
			await(t, entered)
			if mode != "deadline" {
				cancel()
			}
			select {
			case response := <-returned:
				status := 408
				if mode == "deadline" {
					status = 504
				}
				errorResponse(t, response, status, "unknown")
			case <-time.After(5 * time.Second):
				t.Fatal("HTTP cancellation/deadline did not return")
			}
			resume()
			after := snapshotResponse(t, request(handler, t.Context(), "GET", "/accounts/alice", ""))
			balance, transactions := int64(15), int64(1)
			if mode == "rejection-canceled" {
				balance, transactions = 5, 0
			}
			if after.PID != pid.String() || after.Balance != balance || after.TxCount != transactions || starts.Load() != 1 {
				t.Fatalf("late reply reset or replayed account: %+v starts=%d", after, starts.Load())
			}
		})
	}
}

func TestHTTPMailboxFullIsRejectedWithoutRetry(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	node, gateway, pid, ensures := fixtureGateway(t, func() actor.Behavior {
		return &gatedAccount{accountActor: accountActor{id: "alice"}, entered: entered, release: release}
	}, 1)
	handler := gateway.routes()
	responses := make(chan *httptest.ResponseRecorder, 2)
	go func() { responses <- request(handler, t.Context(), "POST", "/accounts/alice/deposit", `{"amount":10}`) }()
	await(t, entered)
	go func() { responses <- request(handler, t.Context(), "GET", "/accounts/alice", "") }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for node.Stats().QueuedMessages != 1 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("second HTTP request did not fill the mailbox")
		}
	}
	errorResponse(t, request(handler, t.Context(), "GET", "/accounts/alice", ""), 503, "rejected")
	if node.Stats().PendingCalls != 2 || ensures.Load() != 0 {
		t.Fatal("rejected request changed or replayed outstanding calls")
	}
	resume()
	for range 2 {
		var response *httptest.ResponseRecorder
		select {
		case response = <-responses:
		case <-time.After(5 * time.Second):
			t.Fatal("admitted HTTP request did not finish")
		}
		snapshot := snapshotResponse(t, response)
		if snapshot.PID != pid.String() || snapshot.Balance != 10 || snapshot.TxCount != 1 {
			t.Fatalf("mailbox rejection changed state: %+v", snapshot)
		}
	}
}

type unknownAccount struct {
	actor.Actor
	calls *atomic.Int32
}

func (a *unknownAccount) Receive(message any) error {
	if _, ok := message.(getAccount); ok {
		a.calls.Add(1)
		return a.ReplyError(&actor.RemoteError{Code: "no_process", Message: "reply outcome is unknown", Outcome: actor.Unknown, Cause: actor.ErrNoProcess})
	}
	return nil
}

func TestHTTPUnknownDoesNotTriggerCreationOrReplay(t *testing.T) {
	var calls atomic.Int32
	_, gateway, _, ensures := fixtureGateway(t, func() actor.Behavior { return &unknownAccount{calls: &calls} }, 8)
	errorResponse(t, request(gateway.routes(), t.Context(), "GET", "/accounts/alice", ""), 502, "unknown")
	if calls.Load() != 1 || ensures.Load() != 0 {
		t.Fatalf("unknown operation replayed: calls=%d ensures=%d", calls.Load(), ensures.Load())
	}
}

func TestHTTPErrorMappings(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		status  int
		outcome string
	}{
		{"mailbox", actor.ErrMailboxFull, 503, "rejected"},
		{"capacity", actor.ErrCapacity, 503, "not_sent"},
		{"deadline", context.DeadlineExceeded, 504, "unknown"},
		{"cancel", context.Canceled, 408, "unknown"},
		{"stopped", actor.ErrStopped, 503, "unknown"},
		{"rejected-remote", &actor.RemoteError{Code: "mailbox_full", Outcome: actor.Rejected}, 503, "rejected"},
		{"unknown-deadline", &actor.RemoteError{Code: "deadline_exceeded", Outcome: actor.Unknown}, 504, "unknown"},
		{"unexpected", actor.ErrReplyType, 500, "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeJSON(recorder, nil, test.err)
			errorResponse(t, recorder, test.status, test.outcome)
		})
	}
}

func TestApplicationObserverAndShutdownCloseResources(t *testing.T) {
	app, err := startApplication("127.0.0.1:0", "127.0.0.1:0", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := app.shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	response, err := client.Get("http://" + app.api.listener.Addr().String() + "/accounts/alice")
	if err != nil {
		t.Fatal(err)
	}
	var account accountSnapshot
	err = json.NewDecoder(response.Body).Decode(&account)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("business endpoint: %+v %v", account, err)
	}
	response, err = client.Get("http://" + app.observer.listener.Addr().String() + "/api/nodes")
	if err != nil {
		t.Fatal(err)
	}
	var nodes []struct{ Name string }
	err = json.NewDecoder(response.Body).Decode(&nodes)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || len(nodes) != 1 || nodes[0].Name != "wallet-node" {
		t.Fatalf("Observer integration: nodes=%+v err=%v", nodes, err)
	}
	response, err = client.Get("http://" + app.observer.listener.Addr().String() + "/api/inspect?pid=" + url.QueryEscape(account.PID))
	if err != nil {
		t.Fatal(err)
	}
	var inspected struct{ Value accountSnapshot }
	err = json.NewDecoder(response.Body).Decode(&inspected)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || inspected.Value != account {
		t.Fatalf("Observer account inspection: %+v %v", inspected, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), shutdownTimeout)
	defer cancel()
	if err := app.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []*httpEndpoint{app.api, app.observer} {
		await(t, endpoint.done)
		connection, err := net.DialTimeout("tcp", endpoint.listener.Addr().String(), time.Second)
		if err == nil {
			connection.Close()
			t.Fatal("shutdown left a listening HTTP endpoint")
		}
	}
	await(t, app.node.Done())
}

func TestHTTPShutdownForcesCanceledConnections(t *testing.T) {
	entered, finished := make(chan struct{}), make(chan struct{})
	endpoint, err := listenHTTP("127.0.0.1:0", http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(finished)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = endpoint.shutdown(ctx)
	})
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	returned := make(chan struct{})
	go func() {
		response, _ := client.Get("http://" + endpoint.listener.Addr().String())
		if response != nil {
			response.Body.Close()
		}
		close(returned)
	}()
	await(t, entered)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := endpoint.shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("forced shutdown: %v", err)
	}
	await(t, finished)
	await(t, returned)
}

func TestApplicationStartupFailureClosesBusinessListener(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	reserved.Close()
	if _, err := startApplication(address, occupied.Addr().String(), time.Second); err == nil {
		t.Fatal("occupied Observer address unexpectedly started")
	}
	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("failed startup retained its business listener: %v", err)
	}
	rebound.Close()
	for _, address := range []string{":9912", "0.0.0.0:9912", "[::]:9912", "192.0.2.1:9912", "localhost:9912"} {
		if err := observerAddress(address); err == nil {
			t.Fatalf("Observer accepted non-literal loopback address %q", address)
		}
	}
}

func TestDemoChecksItsFinalStateAndExits(t *testing.T) {
	if err := run(t.Context(), "", "", 5*time.Second); err != nil {
		t.Fatal(err)
	}
}
