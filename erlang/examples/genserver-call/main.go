package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
)

func main() {
	var (
		peer   = flag.String("peer", "", "optional remote BEAM node name (e.g. beam@127.0.0.1)")
		cookie = flag.String("cookie", "tao-demo-cookie", "Erlang distribution cookie")
	)
	flag.Parse()

	if err := run(*peer, *cookie); err != nil {
		log.Fatal(err)
	}
}

func run(externalPeer, cookie string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If -peer is provided, dial an external BEAM node via EPMD.
	// Otherwise run a self-contained loopback pair over Erlang distribution v6.
	if externalPeer != "" {
		node, err := erlang.Start(ctx, erlang.Config{
			Name:        "tao_client@127.0.0.1",
			Listen:      "127.0.0.1:0",
			Cookie:      cookie,
			DisableEPMD: true,
		})
		if err != nil {
			return err
		}
		defer func() { _ = node.Shutdown(context.Background()) }()

		cli, err := node.NewClient()
		if err != nil {
			return err
		}
		defer func() { _ = cli.Close() }()

		reply, err := cli.Call(ctx,
			erlang.Registered("net_kernel", externalPeer),
			etf.Tuple{etf.Atom("is_auth"), etf.Atom(node.Name())},
		)
		if err != nil {
			return err
		}
		fmt.Printf("peer=%s is_auth=%v\n", externalPeer, reply)
		return nil
	}

	srvNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "orders_srv@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
	})
	if err != nil {
		return err
	}
	defer func() { _ = srvNode.Shutdown(context.Background()) }()

	var stock atomic.Int64
	stock.Store(40)

	_, err = srvNode.Export(erlang.EndpointOptions{
		Name: "orders",
		OnDeliver: func(ep *erlang.Endpoint, msg erlang.InboundDelivery) {
			switch msg.Kind {
			case erlang.InboundGenCast:
				if tup, ok := msg.Term.(etf.Tuple); ok && len(tup) == 2 && tup[0] == etf.Atom("restock") {
					if delta, ok := tup[1].(int64); ok {
						stock.Add(delta)
					}
				}
			case erlang.InboundGenCall:
				_, release, err := ep.ReserveInboundCall(msg.Peer, msg.PeerGen)
				if err != nil {
					return
				}
				defer release()
				if tup, ok := msg.Term.(etf.Tuple); ok && len(tup) == 2 && tup[0] == etf.Atom("lookup") {
					_ = ep.SendReply(msg.ReplyTo, etf.Tuple{etf.Atom("ok"), tup[1], stock.Load()})
					return
				}
				_ = ep.SendReply(msg.ReplyTo, etf.Tuple{etf.Atom("error"), etf.Atom("badarg")})
			}
		},
	})
	if err != nil {
		return err
	}

	cliNode, err := erlang.Start(ctx, erlang.Config{
		Name:        "orders_cli@127.0.0.1",
		Listen:      "127.0.0.1:0",
		Cookie:      cookie,
		DisableEPMD: true,
		StaticRoutes: map[string]erlang.Route{
			srvNode.Name(): {Addr: srvNode.Addr().String()},
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = cliNode.Shutdown(context.Background()) }()

	client, err := cliNode.NewClient()
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	target := erlang.Registered("orders", srvNode.Name())
	if err := client.Cast(ctx, target, etf.Tuple{etf.Atom("restock"), int64(2)}); err != nil {
		return err
	}
	reply, err := client.Call(ctx, target, etf.Tuple{etf.Atom("lookup"), int64(1001)})
	if err != nil {
		return err
	}
	tup := reply.(etf.Tuple)
	fmt.Printf("order=%v stock=%v\n", tup[1], tup[2])
	return nil
}
