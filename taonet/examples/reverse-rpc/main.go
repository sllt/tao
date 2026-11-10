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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientMux, serverMux := taonet.NewMux(), taonet.NewMux()
	if err := clientMux.HandleRequest("client.describe", func(context.Context, *taonet.Request) (taonet.Message, error) {
		return taonet.Message{Body: []byte("desktop client")}, nil
	}); err != nil {
		return err
	}
	if err := serverMux.HandleRequest("server.echo", func(ctx context.Context, req *taonet.Request) (taonet.Message, error) {
		description, err := req.Session().Request(ctx, "client.describe", taonet.Message{})
		if err != nil {
			return taonet.Message{}, err
		}
		return taonet.Message{Body: append(description.Body, req.Message.Body...)}, nil
	}); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		session, err := taonet.Accept(ctx, conn, taonet.SessionConfig{Local: taonet.Peer{ID: "server"}, Mux: serverMux, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}})
		if err == nil {
			<-session.Done()
			err = session.Close()
		}
		serverDone <- err
	}()
	client, err := taonet.Dial(ctx, listener.Addr().String(), taonet.SessionConfig{Local: taonet.Peer{ID: "client"}, Mux: clientMux, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}})
	if err != nil {
		return err
	}
	defer client.Close()
	response, err := client.Request(ctx, "server.echo", taonet.Message{Body: []byte(": hello over one connection")})
	if err != nil {
		return err
	}
	fmt.Println(string(response.Body))
	if err = client.Shutdown(ctx); err != nil {
		return err
	}
	return <-serverDone
}
