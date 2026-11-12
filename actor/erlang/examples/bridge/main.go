package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/sllt/tao/actor"
	erlangactor "github.com/sllt/tao/actor/erlang"
	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
)

type addCmd int64
type getCmd struct{}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Server side: Tao Actor Node + Erlang Distribution Node + Bridge.
	srvTao, err := actor.NewNode(actor.NodeOptions{Name: "srv_tao"})
	if err != nil {
		return err
	}
	defer func() { _ = srvTao.Stop(context.Background()) }()

	srvErl, err := erlang.Start(ctx, erlang.Config{
		Name:        "tao_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "tao-bridge-demo",
		DisableEPMD: true,
	})
	if err != nil {
		return err
	}
	defer func() { _ = srvErl.Shutdown(context.Background()) }()

	srvBridge, err := erlangactor.Attach(srvTao, srvErl)
	if err != nil {
		return err
	}
	defer func() { _ = srvBridge.Close(context.Background()) }()

	var total int64
	counterPID, err := srvTao.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch m := msg.(type) {
		case addCmd:
			total += int64(m)
		case getCmd:
			return a.Reply(total)
		}
		return nil
	}))
	if err != nil {
		return err
	}

	counterSchema := erlangactor.Funcs{
		Cast: func(term etf.Term) (any, error) {
			tup, ok := term.(etf.Tuple)
			if !ok || len(tup) != 2 || tup[0] != etf.Atom("add") {
				return nil, errors.New("expected {add, Delta}")
			}
			d, ok := tup[1].(int64)
			if !ok {
				return nil, errors.New("expected integer delta")
			}
			return addCmd(d), nil
		},
		Call: func(term etf.Term) (any, error) {
			if term == etf.Atom("get") {
				return getCmd{}, nil
			}
			return nil, errors.New("expected get")
		},
		Reply: func(value any, err error) (etf.Term, error) {
			if err != nil {
				return erlangactor.DefaultEncodeReply(nil, err)
			}
			return etf.Tuple{etf.Atom("ok"), value.(int64)}, nil
		},
	}

	if _, err := srvBridge.Export("counter", counterPID, counterSchema); err != nil {
		return err
	}

	// 2. Caller side: Tao Actor calling Erlang target asynchronously via bridge.Outbound().
	cliTao, err := actor.NewNode(actor.NodeOptions{Name: "cli_tao"})
	if err != nil {
		return err
	}
	defer func() { _ = cliTao.Stop(context.Background()) }()

	cliErl, err := erlang.Start(ctx, erlang.Config{
		Name:        "tao_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      "tao-bridge-demo",
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			srvErl.Name(): {Addr: srvErl.Addr().String()},
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = cliErl.Shutdown(context.Background()) }()

	cliBridge, err := erlangactor.Attach(cliTao, cliErl)
	if err != nil {
		return err
	}
	defer func() { _ = cliBridge.Close(context.Background()) }()

	target := erlang.Registered("counter", srvErl.Name())

	var callerTicket actor.DeferredReply
	var pendingReq actor.RequestID
	callerPID, err := cliTao.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch m := msg.(type) {
		case string:
			if m == "run" {
				callerTicket = a.Defer()
				if err := a.Send(cliBridge.Outbound(), erlangactor.Cast{
					Target:  target,
					Request: etf.Tuple{etf.Atom("add"), int64(42)},
				}); err != nil {
					_ = callerTicket.ReplyError(err)
					return nil
				}
				id, err := a.Request(a.Context(), cliBridge.Outbound(), erlangactor.Call{
					Target:  target,
					Request: etf.Atom("get"),
				})
				if err != nil {
					_ = callerTicket.ReplyError(err)
					return nil
				}
				pendingReq = id
			}
		case actor.CallResult:
			if m.ID == pendingReq {
				if m.Err != nil {
					_ = callerTicket.ReplyError(m.Err)
					return nil
				}
				_ = callerTicket.Reply(m.Value)
			}
		}
		return nil
	}))
	if err != nil {
		return err
	}

	reply, err := cliTao.Call[etf.Term](ctx, callerPID, "run")
	if err != nil {
		return err
	}
	tup := reply.(etf.Tuple)
	fmt.Printf("bridged_counter=%v\n", tup[1])
	return nil
}
