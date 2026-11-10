package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/actor/observer"
)

// --- 1. 消息定义 ---

type ensureAccount struct {
	ID string
}

type deposit struct {
	Amount int64 `json:"amount"`
}

type withdraw struct {
	Amount int64 `json:"amount"`
}

type getAccount struct{}

type accountSnapshot struct {
	ID      string `json:"id"`
	Balance int64  `json:"balance"`
	TxCount int64  `json:"tx_count"`
	PID     string `json:"pid"`
}

type auditLog struct {
	AccountID string
	Action    string
	Amount    int64
	Balance   int64
}

type getAuditCount struct{}

var (
	errInvalidAccountID  = errors.New("account id must contain 1 to 64 ASCII letters, digits, underscores or hyphens")
	errInvalidAmount     = errors.New("amount must be a positive int64 integer")
	errInsufficientFunds = errors.New("insufficient funds")
	errAccountLimit      = errors.New("account balance or transaction count limit reached")
)

// Caller cancellation invalidates a reply ticket, not the business instance.
// Other reply errors still expose programming or lifecycle errors to supervision.
func allowExpiredReply(err error) error {
	if errors.Is(err, actor.ErrReplyExpired) {
		return nil
	}
	return err
}

func validAccountID(id string) bool {
	if len(id) < 1 || len(id) > 64 {
		return false
	}
	for _, c := range []byte(id) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// --- 2. Actor 定义 ---

// accountSupervisor 负责按需创建并监督所有的单个账户子 Actor（路径为 accounts/<id>）。
type accountSupervisor struct {
	actor.Actor
	created map[string]struct{}
}

func (s *accountSupervisor) Receive(msg any) error {
	switch m := msg.(type) {
	case actor.Started:
		s.created = make(map[string]struct{})
	case ensureAccount:
		if !validAccountID(m.ID) {
			return allowExpiredReply(s.ReplyError(errInvalidAccountID))
		}
		if _, exists := s.created[m.ID]; !exists {
			_, err := s.Spawn(func() actor.Behavior {
				return &accountActor{id: m.ID}
			}, actor.WithName(m.ID), actor.WithKind("account", "1"))
			if err != nil && !errors.Is(err, actor.ErrNameTaken) {
				return allowExpiredReply(s.ReplyError(err))
			}
			s.created[m.ID] = struct{}{}
		}
		return allowExpiredReply(s.Reply(actor.Ref{Path: "accounts/" + m.ID}))
	}
	return nil
}

// accountActor 维护单个账户的状态。
// 因为单个 Actor 的邮箱天然串行执行，内部不需要任何 sync.Mutex。
type accountActor struct {
	actor.Actor
	id      string
	balance int64
	txCount int64
}

func (a *accountActor) Receive(msg any) error {
	switch m := msg.(type) {
	case deposit:
		if m.Amount <= 0 {
			return allowExpiredReply(a.ReplyError(errInvalidAmount))
		}
		if a.balance > math.MaxInt64-m.Amount || a.txCount == math.MaxInt64 {
			return allowExpiredReply(a.ReplyError(errAccountLimit))
		}
		a.balance += m.Amount
		a.txCount++
		a.audit("deposit", m.Amount)
		return allowExpiredReply(a.Reply(a.snapshot()))

	case withdraw:
		if m.Amount <= 0 {
			return allowExpiredReply(a.ReplyError(errInvalidAmount))
		}
		if a.balance < m.Amount {
			return allowExpiredReply(a.ReplyError(errInsufficientFunds))
		}
		if a.txCount == math.MaxInt64 {
			return allowExpiredReply(a.ReplyError(errAccountLimit))
		}
		a.balance -= m.Amount
		a.txCount++
		a.audit("withdraw", m.Amount)
		return allowExpiredReply(a.Reply(a.snapshot()))

	case getAccount:
		return allowExpiredReply(a.Reply(a.snapshot()))
	}
	return nil
}

func (a *accountActor) audit(action string, amount int64) {
	if err := a.Send(actor.Ref{Path: "audit"}, auditLog{
		AccountID: a.id,
		Action:    action,
		Amount:    amount,
		Balance:   a.balance,
	}); err != nil {
		// This demo's in-memory audit is best effort; a dropped event cannot
		// roll back a balance update that has already happened.
		a.Log().Warn("audit event dropped", "account", a.id, "action", action, "error", err)
	}
}

func (a *accountActor) Inspect(context.Context) (any, error) { return a.snapshot(), nil }

func (a *accountActor) snapshot() accountSnapshot {
	return accountSnapshot{
		ID:      a.id,
		Balance: a.balance,
		TxCount: a.txCount,
		PID:     a.PID().String(),
	}
}

// auditActor 异步接收所有账户的流水事件（Send / Tell 模式，不阻塞 HTTP 响应）。
type auditActor struct {
	actor.Actor
	totalEvents int64
}

func (a *auditActor) Receive(msg any) error {
	switch msg.(type) {
	case auditLog:
		a.totalEvents++
	case getAuditCount:
		return allowExpiredReply(a.Reply(a.totalEvents))
	}
	return nil
}

// --- 3. HTTP 网关层 ---

type httpGateway struct {
	node        *actor.Node
	callTimeout time.Duration
}

func (g *httpGateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /accounts/{id}", g.handleGet)
	mux.HandleFunc("POST /accounts/{id}/deposit", g.handleDeposit)
	mux.HandleFunc("POST /accounts/{id}/withdraw", g.handleWithdraw)
	return mux
}

// callAccount 先直接按逻辑路径 accounts/<id> 调用目标 Actor；
// 若账户首次访问尚不存在（ErrNoProcess）或正在启动中（ErrStarting），
// 则通过监督者串行确认就绪后再调用。
func (g *httpGateway) callAccount(ctx context.Context, id string, msg any) (accountSnapshot, error) {
	if !validAccountID(id) {
		return accountSnapshot{}, errInvalidAccountID
	}
	ctx, cancel := context.WithTimeout(ctx, g.callTimeout)
	defer cancel()
	target := g.node.Ref("accounts/" + id)
	snap, err := g.node.Call[accountSnapshot](ctx, target, msg)
	if !canCreateAccount(err) {
		return snap, err
	}
	if _, err := g.node.Call[actor.Ref](ctx, g.node.Ref("accounts"), ensureAccount{ID: id}); err != nil {
		return accountSnapshot{}, err
	}
	return g.node.Call[accountSnapshot](ctx, target, msg)
}

func canCreateAccount(err error) bool {
	if !errors.Is(err, actor.ErrNoProcess) && !errors.Is(err, actor.ErrStarting) {
		return false
	}
	var remote *actor.RemoteError
	return !errors.As(err, &remote) || remote.Outcome == actor.NotSent || remote.Outcome == actor.Rejected
}

func (g *httpGateway) handleGet(w http.ResponseWriter, r *http.Request) {
	snap, err := g.callAccount(r.Context(), r.PathValue("id"), getAccount{})
	writeJSON(w, snap, err)
}

func (g *httpGateway) handleDeposit(w http.ResponseWriter, r *http.Request) {
	var req deposit
	if err := decodeBody(w, r, &req); err != nil {
		writeInputError(w, err)
		return
	}
	if req.Amount <= 0 {
		writeInputError(w, errInvalidAmount)
		return
	}
	snap, err := g.callAccount(r.Context(), r.PathValue("id"), req)
	writeJSON(w, snap, err)
}

func (g *httpGateway) handleWithdraw(w http.ResponseWriter, r *http.Request) {
	var req withdraw
	if err := decodeBody(w, r, &req); err != nil {
		writeInputError(w, err)
		return
	}
	if req.Amount <= 0 {
		writeInputError(w, errInvalidAmount)
		return
	}
	snap, err := g.callAccount(r.Context(), r.PathValue("id"), req)
	writeJSON(w, snap, err)
}

func decodeBody(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeInputError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	var large *http.MaxBytesError
	if errors.As(err, &large) {
		status = http.StatusRequestEntityTooLarge
	}
	writeHTTPError(w, status, "not_sent", err)
}

func writeJSON(w http.ResponseWriter, data any, err error) {
	if err != nil {
		status, outcome := httpError(err)
		writeHTTPError(w, status, outcome, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(data)
}

func httpError(err error) (int, string) {
	status, outcome := http.StatusInternalServerError, "unknown"
	var remote *actor.RemoteError
	errors.As(err, &remote)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		status = http.StatusRequestTimeout
	case remote != nil && remote.Outcome == actor.Unknown:
		status = http.StatusBadGateway
	case errors.Is(err, errInvalidAccountID):
		status, outcome = http.StatusBadRequest, "not_sent"
	case errors.Is(err, errInvalidAmount):
		status, outcome = http.StatusBadRequest, "responded"
	case errors.Is(err, errInsufficientFunds), errors.Is(err, errAccountLimit):
		status, outcome = http.StatusConflict, "responded"
	case errors.Is(err, actor.ErrMailboxFull):
		status, outcome = http.StatusServiceUnavailable, "rejected"
	case errors.Is(err, actor.ErrCapacity):
		status, outcome = http.StatusServiceUnavailable, "not_sent"
	case errors.Is(err, actor.ErrNoProcess), errors.Is(err, actor.ErrStalePID):
		status, outcome = http.StatusNotFound, "not_sent"
	case errors.Is(err, actor.ErrStarting), errors.Is(err, actor.ErrRestarting):
		status, outcome = http.StatusServiceUnavailable, "rejected"
	case errors.Is(err, actor.ErrClosed), errors.Is(err, actor.ErrStopped), errors.Is(err, actor.ErrUnresponsive), errors.Is(err, actor.ErrNodeUnreachable):
		status = http.StatusServiceUnavailable
	}
	if remote != nil {
		switch remote.Outcome {
		case actor.NotSent:
			outcome = "not_sent"
		case actor.Rejected:
			outcome = "rejected"
		case actor.Responded:
			outcome = "responded"
		}
	}
	return status, outcome
}

func writeHTTPError(w http.ResponseWriter, status int, outcome string, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error(), "outcome": outcome})
}

// --- 4. 启动与自测演示 ---

const shutdownTimeout = 5 * time.Second

type httpEndpoint struct {
	server   *http.Server
	listener net.Listener
	done     chan struct{}
	err      error // published by closing done
}

func listenHTTP(address string, handler http.Handler) (*httpEndpoint, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	endpoint := &httpEndpoint{
		server: &http.Server{
			Handler: handler, ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second,
		},
		listener: listener,
		done:     make(chan struct{}),
	}
	go func() {
		err := endpoint.server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			endpoint.err = err
		}
		close(endpoint.done)
	}()
	return endpoint, nil
}

func (endpoint *httpEndpoint) shutdown(ctx context.Context) error {
	err := endpoint.server.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, endpoint.server.Close())
	}
	<-endpoint.done
	return errors.Join(err, endpoint.err)
}

type application struct {
	node     *actor.Node
	api      *httpEndpoint
	observer *httpEndpoint
}

func observerAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.IsLoopback() {
		return errors.New("observer must listen on an explicit loopback IP")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("invalid observer port: %w", err)
	}
	return nil
}

func startApplication(serveAddr, observerAddr string, callTimeout time.Duration) (result *application, err error) {
	if callTimeout <= 0 {
		return nil, errors.New("call timeout must be positive")
	}
	if observerAddr != "" {
		if err := observerAddress(observerAddr); err != nil {
			return nil, err
		}
	}
	node, err := actor.NewNode(actor.NodeOptions{Name: "wallet-node", CallTimeout: callTimeout})
	if err != nil {
		return nil, err
	}
	app := &application{node: node}
	defer func() {
		if result == nil {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			err = errors.Join(err, app.shutdown(ctx))
		}
	}()
	// Children stop in reverse creation order: accounts finish before audit.
	if _, err := node.Spawn(func() actor.Behavior { return &auditActor{} }, actor.WithName("audit"), actor.WithKind("audit", "1")); err != nil {
		return nil, err
	}
	if _, err := node.Spawn(func() actor.Behavior { return &accountSupervisor{} }, actor.WithName("accounts"), actor.WithKind("account-supervisor", "1")); err != nil {
		return nil, err
	}
	if serveAddr == "" {
		serveAddr = "127.0.0.1:0"
	}
	gateway := &httpGateway{node: node, callTimeout: callTimeout}
	app.api, err = listenHTTP(serveAddr, gateway.routes())
	if err != nil {
		return nil, err
	}
	if observerAddr != "" {
		view, err := observer.New(node)
		if err != nil {
			return nil, err
		}
		app.observer, err = listenHTTP(observerAddr, view)
		if err != nil {
			return nil, err
		}
	}
	return app, nil
}

func (app *application) shutdown(ctx context.Context) error {
	var err error
	for _, endpoint := range []*httpEndpoint{app.api, app.observer} {
		if endpoint != nil {
			err = errors.Join(err, endpoint.shutdown(ctx))
		}
	}
	return errors.Join(err, app.node.Stop(ctx))
}

func main() {
	serveAddr := flag.String("serve", "", "keep HTTP server listening at address (e.g. 127.0.0.1:8080)")
	observerAddr := flag.String("observer", "", "optional independent Observer loopback IP:port (e.g. 127.0.0.1:9912)")
	callTimeout := flag.Duration("call-timeout", 5*time.Second, "total timeout for one HTTP account operation")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *serveAddr, *observerAddr, *callTimeout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, serveAddr, observerAddr string, callTimeout time.Duration) (err error) {
	app, err := startApplication(serveAddr, observerAddr, callTimeout)
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		err = errors.Join(err, app.shutdown(shutdown))
	}()
	var observerDone <-chan struct{}
	if app.observer != nil {
		fmt.Printf("Observer http://%s\n", app.observer.listener.Addr())
		observerDone = app.observer.done
	}
	baseURL := "http://" + app.api.listener.Addr().String()
	if serveAddr == "" {
		return runDemoClient(ctx, baseURL, app.node)
	}
	fmt.Printf("HTTP + Actor server listening on %s\n", baseURL)
	select {
	case <-ctx.Done():
		return nil
	case <-app.api.done:
		return app.api.err
	case <-observerDone:
		return app.observer.err
	case <-app.node.Done():
		return app.node.Err()
	}
}

func runDemoClient(ctx context.Context, baseURL string, node *actor.Node) error {
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}

	// 1. 20 个并发 HTTP 请求同时向桃子 (taozi) 的账户存款 10 元
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for range 20 {
		wg.Go(func() {
			var snap accountSnapshot
			failures <- demoRequest(ctx, client, http.MethodPost, baseURL+"/accounts/taozi/deposit", deposit{Amount: 10}, &snap)
		})
	}
	wg.Wait()
	close(failures)
	var failure error
	for err := range failures {
		failure = errors.Join(failure, err)
	}
	if failure != nil {
		return failure
	}

	// 2. 扣款 50 元
	var snap accountSnapshot
	if err := demoRequest(ctx, client, http.MethodPost, baseURL+"/accounts/taozi/withdraw", withdraw{Amount: 50}, &snap); err != nil {
		return err
	}

	// 3. 查询账户最终状态
	if err := demoRequest(ctx, client, http.MethodGet, baseURL+"/accounts/taozi", nil, &snap); err != nil {
		return err
	}

	// 4. 查询异步审计 Actor 累计收到的流水条数
	auditCount, err := node.Call[int64](ctx, node.Ref("audit"), getAuditCount{})
	if err != nil {
		return err
	}
	if snap.ID != "taozi" || snap.Balance != 150 || snap.TxCount != 21 || auditCount != 21 {
		return fmt.Errorf("unexpected demo state: account=%+v audit_events=%d", snap, auditCount)
	}

	fmt.Printf("account=%s balance=%d tx_count=%d audit_events=%d pid=%s\n",
		snap.ID, snap.Balance, snap.TxCount, auditCount, snap.PID)
	return nil
}

func demoRequest(ctx context.Context, client *http.Client, method, url string, body, result any) error {
	var content io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		content = bytes.NewReader(raw)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, content)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("%s %s: HTTP %d: %s", method, url, response.StatusCode, raw)
	}
	return json.NewDecoder(response.Body).Decode(result)
}
