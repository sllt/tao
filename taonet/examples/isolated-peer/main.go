package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/sllt/tao/taonet"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mux := taonet.NewMux()
	if err := mux.HandleRequest("echo", func(_ context.Context, r *taonet.Request) (taonet.Message, error) { return r.Message, nil }); err != nil {
		return err
	}
	config := func(id taonet.PeerID, mux *taonet.Mux) taonet.EndpointConfig {
		return taonet.EndpointConfig{Profile: taonet.Isolated, Session: taonet.SessionConfig{Local: taonet.Peer{ID: id}, Mux: mux, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}}}
	}
	client, err := taonet.NewEndpoint(config("client", mux))
	if err != nil {
		return err
	}
	defer client.Close()
	server, err := taonet.NewEndpoint(config("server", nil))
	if err != nil {
		return err
	}
	defer server.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()
	if _, err = client.Connect(ctx, taonet.Target{Peer: "server", Address: listener.Addr().String()}); err != nil {
		return err
	}
	select {
	case <-server.Events():
	case <-ctx.Done():
		return ctx.Err()
	}
	var control, bulk *taonet.Session
	for _, class := range []taonet.TrafficClass{taonet.ClassControl, taonet.ClassInteractive, taonet.ClassReply, taonet.ClassBulk} {
		session, err := server.ConnectClass(ctx, taonet.Target{Peer: "client"}, class)
		if err != nil {
			return err
		}
		response, err := session.Request(ctx, "echo", taonet.Message{Body: []byte("outbound-only client")})
		if err != nil {
			return err
		}
		fmt.Printf("%s: %s\n", class, response.Body)
		if class == taonet.ClassControl {
			control = session
		}
		if class == taonet.ClassBulk {
			bulk = session
		}
	}
	if err = bulk.Close(); err != nil {
		return err
	}
	if _, err = control.Request(ctx, "echo", taonet.Message{}); err != nil {
		return err
	}
	fmt.Printf("control survived bulk close; client dials=%d, server dials=%d\n", client.Stats().DialAttempts, server.Stats().DialAttempts)
	if err = client.Shutdown(ctx); err != nil {
		return err
	}
	if err = server.Shutdown(ctx); err != nil {
		return err
	}
	<-served
	return nil
}
