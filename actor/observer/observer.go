// Package observer serves an optional, read-only Web Observer. It is separate
// from the actor scheduler and uses only the standard library.
package observer

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/sllt/tao/actor"
)

//go:embed web/*
var assets embed.FS

type Observer struct {
	nodes   map[string]*actor.Node
	handler http.Handler
}
type HostSnapshot struct {
	GoVersion                      string
	Goroutines, CPUs               int
	HeapAlloc, HeapSys, TotalAlloc uint64
	GC                             uint32
}
type nodeSummary struct {
	Name, Version string
	Stats         actor.Stats
}

// New enables diagnostics for the supplied in-process nodes. Remote nodes can
// run their own Observer behind an SSH tunnel or the application's HTTP server.
func New(nodes ...*actor.Node) (*Observer, error) {
	if len(nodes) == 0 {
		return nil, errors.New("observer: at least one node is required")
	}
	o := &Observer{nodes: make(map[string]*actor.Node)}
	for _, node := range nodes {
		if node == nil {
			return nil, errors.New("observer: nil node")
		}
		if o.nodes[node.Name()] != nil {
			return nil, fmt.Errorf("observer: duplicate node %s", node.Name())
		}
		o.nodes[node.Name()] = node
	}
	for _, node := range nodes {
		node.EnableDiagnostics()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/nodes", o.listNodes)
	mux.HandleFunc("GET /api/snapshot", o.snapshot)
	mux.HandleFunc("GET /api/actor", o.detail)
	mux.HandleFunc("GET /api/inspect", o.inspect)
	web, _ := fs.Sub(assets, "web")
	mux.Handle("GET /", http.FileServer(http.FS(web)))
	o.handler = mux
	return o, nil
}
func (o *Observer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
	o.handler.ServeHTTP(w, r)
}
func sendJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, err error) {
	sendJSON(w, status, map[string]string{"error": err.Error()})
}
func (o *Observer) node(r *http.Request) (*actor.Node, error) {
	name := r.URL.Query().Get("node")
	if name == "" && len(o.nodes) == 1 {
		for _, node := range o.nodes {
			return node, nil
		}
	}
	if node := o.nodes[name]; node != nil {
		return node, nil
	}
	return nil, errors.New("observer: unknown node")
}
func (o *Observer) listNodes(w http.ResponseWriter, _ *http.Request) {
	var nodes []nodeSummary
	for _, node := range o.nodes {
		snapshot, _ := node.Snapshot(actor.SnapshotOptions{Limit: 1})
		nodes = append(nodes, nodeSummary{node.Name(), snapshot.Version, snapshot.Stats})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	sendJSON(w, http.StatusOK, nodes)
}
func (o *Observer) snapshot(w http.ResponseWriter, r *http.Request) {
	node, err := o.node(r)
	if err != nil {
		fail(w, 404, err)
		return
	}
	options := actor.SnapshotOptions{Filter: r.URL.Query().Get("filter")}
	if value := r.URL.Query().Get("after"); value != "" {
		options.After, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			fail(w, 400, err)
			return
		}
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		options.Limit, err = strconv.Atoi(value)
		if err != nil {
			fail(w, 400, err)
			return
		}
	}
	snapshot, err := node.Snapshot(options)
	if err != nil {
		fail(w, 400, err)
		return
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	sendJSON(w, 200, struct {
		Node actor.NodeSnapshot
		Host HostSnapshot
	}{snapshot, HostSnapshot{runtime.Version(), runtime.NumGoroutine(), runtime.NumCPU(), memory.HeapAlloc, memory.HeapSys, memory.TotalAlloc, memory.NumGC}})
}
func (o *Observer) actor(r *http.Request) (*actor.Node, actor.PID, error) {
	node, err := o.node(r)
	if err != nil {
		return nil, actor.PID{}, err
	}
	pid, err := actor.ParsePID(r.URL.Query().Get("pid"))
	return node, pid, err
}
func (o *Observer) detail(w http.ResponseWriter, r *http.Request) {
	node, pid, err := o.actor(r)
	if err != nil {
		fail(w, 400, err)
		return
	}
	detail, err := node.ActorSnapshot(pid)
	if err != nil {
		fail(w, 404, err)
		return
	}
	sendJSON(w, 200, detail)
}
func (o *Observer) inspect(w http.ResponseWriter, r *http.Request) {
	node, pid, err := o.actor(r)
	if err != nil {
		fail(w, 400, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	value, err := node.Inspect(ctx, pid)
	if err != nil {
		status := http.StatusConflict
		switch {
		case errors.Is(err, actor.ErrInspectUnsupported):
			status = 501
		case errors.Is(err, context.DeadlineExceeded):
			status = 504
		case errors.Is(err, actor.ErrNoProcess), errors.Is(err, actor.ErrStalePID):
			status = 404
		}
		fail(w, status, err)
		return
	}
	sendJSON(w, 200, struct {
		PID   string
		Value json.RawMessage
	}{pid.String(), value})
}

// Listen starts the Observer on loopback by default. Use Handler embedding or
// an explicit address when the application supplies its own access controls.
func (o *Observer) Listen(address string) (*Server, error) {
	if address == "" {
		address = "127.0.0.1:9911"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &Server{listener: listener, http: &http.Server{Handler: o, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		err := s.http.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			s.err = err
		}
	}()
	return s, nil
}

type Server struct {
	listener net.Listener
	http     *http.Server
	done     chan struct{}
	err      error
}

func (s *Server) Addr() net.Addr        { return s.listener.Addr() }
func (s *Server) Done() <-chan struct{} { return s.done }
func (s *Server) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }
