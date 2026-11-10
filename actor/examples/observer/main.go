package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/actor/observer"
)

type work struct{ Units int64 }
type worker struct {
	actor.Actor
	processed int64
	delay     time.Duration
	fragile   bool
}

func (w *worker) Receive(message any) error {
	if job, ok := message.(work); ok {
		if w.delay > 0 {
			select {
			case <-time.After(w.delay):
			case <-w.Context().Done():
				return nil
			}
		}
		w.processed += job.Units
		if w.fragile && w.processed%100 == 0 {
			return errors.New("demo: scheduled worker restart")
		}
		_ = w.Reply(w.processed)
	}
	return nil
}
func (w *worker) Inspect(context.Context) (any, error) {
	return map[string]any{"processed_units": w.processed, "delay_ms": w.delay.Milliseconds(), "role": w.Name()}, nil
}

type supervisor struct{ actor.Actor }

func (s *supervisor) Receive(message any) error {
	if _, ok := message.(actor.Started); !ok {
		return nil
	}
	for _, spec := range []struct {
		name    string
		delay   time.Duration
		fragile bool
	}{{"ingest", 0, false}, {"slow", 80 * time.Millisecond, false}, {"fragile", 0, true}} {
		if _, err := s.Spawn(func() actor.Behavior { return &worker{delay: spec.delay, fragile: spec.fragile} }, actor.WithName(spec.name), actor.WithMailboxSize(16), actor.WithKind("worker", "1")); err != nil {
			return err
		}
	}
	return nil
}

type gateway struct {
	actor.Actor
	calls, failed int64
}

func (g *gateway) Receive(message any) error {
	if job, ok := message.(work); ok {
		g.calls++
		_, err := g.Call(g.Context(), actor.Ref{Node: "worker-demo", Path: "remote-worker"}, job)
		if err != nil {
			g.failed++
		}
	}
	return nil
}
func (g *gateway) Inspect(context.Context) (any, error) {
	return map[string]int64{"calls": g.calls, "failed": g.failed}, nil
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	address := flag.String("listen", "127.0.0.1:9911", "Observer HTTP listen address")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	create := func(name string) (*actor.Node, error) {
		return actor.StartNode(name+"@127.0.0.1:0", actor.NodeOptions{Network: actor.NetworkOptions{AllowInsecureLoopback: true}, Register: []actor.MessageType{{Name: "work", Version: 1, Sample: work{}}, {Name: "count", Version: 1, Sample: int64(0)}}})
	}
	mainNode, err := create("hive-demo")
	if err != nil {
		return err
	}
	defer mainNode.Stop(context.Background())
	remote, err := create("worker-demo")
	if err != nil {
		return err
	}
	defer remote.Stop(context.Background())
	view, err := observer.New(mainNode, remote)
	if err != nil {
		return err
	}
	if err := mainNode.Connect(ctx, remote.Name(), remote.ListenAddr()); err != nil {
		return err
	}
	if _, err := remote.Spawn(func() actor.Behavior { return &worker{delay: 30 * time.Millisecond} }, actor.WithName("remote-worker"), actor.WithKind("worker", "1")); err != nil {
		return err
	}
	if _, err := mainNode.Spawn(func() actor.Behavior { return &supervisor{} }, actor.WithName("orders"), actor.WithKind("supervisor", "1")); err != nil {
		return err
	}
	if _, err := mainNode.Spawn(func() actor.Behavior { return &gateway{} }, actor.WithName("gateway"), actor.WithKind("gateway", "1")); err != nil {
		return err
	}
	server, err := view.Listen(*address)
	if err != nil {
		return err
	}
	fmt.Printf("Observer http://%s\n", server.Addr())
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			return server.Shutdown(shutdown)
		case <-server.Done():
			return server.Err()
		case <-ticker.C:
			for _, path := range []string{"orders/ingest", "orders/slow", "orders/fragile", "gateway"} {
				_ = mainNode.Send(actor.Ref{Path: path}, work{Units: 1})
			}
		}
	}
}
