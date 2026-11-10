package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/sllt/tao/actor"
)

type Inc struct{ Delta int64 }
type Get struct{}
type Counter struct {
	actor.Actor
	count int64
}

func (c *Counter) Receive(msg any) error {
	switch msg := msg.(type) {
	case Inc:
		c.count += msg.Delta
	case Get:
		if err := c.Reply(c.count); !errors.Is(err, actor.ErrReplyExpired) {
			return err
		}
	}
	return nil
}

type App struct{ actor.Actor }

func (a *App) Receive(msg any) error {
	if _, ok := msg.(actor.Started); ok {
		_, err := a.Spawn(func() actor.Behavior { return &Counter{} }, actor.WithName("counter"))
		return err
	}
	return nil
}

type ready struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

func options(name string) actor.NodeOptions {
	return actor.NodeOptions{Name: name, Network: actor.NetworkOptions{AllowInsecureLoopback: true}, Register: []actor.MessageType{{Name: "counter.inc", Version: 1, Sample: Inc{}}, {Name: "counter.get", Version: 1, Sample: Get{}}, {Name: "counter.value", Version: 1, Sample: int64(0)}}}
}
func stop(n *actor.Node) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Stop(ctx); err != nil {
		log.Print(err)
	}
}

func main() {
	mode := flag.String("mode", "demo", "demo, server, or client")
	listen := flag.String("listen", "127.0.0.1:0", "server listen address")
	peer := flag.String("peer", "", "server address for client mode")
	flag.Parse()
	var err error
	switch *mode {
	case "demo":
		err = demo()
	case "server":
		err = server(*listen)
	case "client":
		err = client(*peer)
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func server(address string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	n, err := actor.StartNode(address, options("server"))
	if err != nil {
		return err
	}
	defer stop(n)
	if _, err = n.Spawn(func() actor.Behavior { return &App{} }, actor.WithName("app")); err != nil {
		return err
	}
	if err = json.NewEncoder(os.Stdout).Encode(ready{"server", n.ListenAddr()}); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
func client(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, err := actor.StartNode("", options("client"))
	if err != nil {
		return err
	}
	defer stop(n)
	if err = n.Connect(ctx, "server", address); err != nil {
		return err
	}
	target := n.Ref("node://server/app/counter")
	for _, delta := range []int64{40, 2} {
		if err = n.SendConfirmed(ctx, target, Inc{delta}); err != nil {
			return err
		}
	}
	count, err := n.Call[int64](ctx, target, Get{})
	if err != nil {
		return err
	}
	fmt.Printf("remote counter=%d; physical connections=%d; client listens=%t\n", count, n.NetworkStats().Endpoint.Sessions, n.ListenAddr() != "")
	return nil
}
func demo() error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	process := exec.Command(executable, "-mode", "server", "-listen", "127.0.0.1:0")
	stdout, err := process.StdoutPipe()
	if err != nil {
		return err
	}
	process.Stderr = os.Stderr
	if err = process.Start(); err != nil {
		return err
	}
	defer process.Process.Kill()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		return fmt.Errorf("server did not report an address: %v", scanner.Err())
	}
	var listening ready
	if err = json.Unmarshal(scanner.Bytes(), &listening); err != nil {
		return err
	}
	if err = client(listening.Address); err != nil {
		return err
	}
	if err = process.Process.Signal(os.Interrupt); err != nil {
		return err
	}
	return process.Wait()
}
