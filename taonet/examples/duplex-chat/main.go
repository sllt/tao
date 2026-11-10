package main

import (
	"context"
	"fmt"
	"io"
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
	mux := taonet.NewMux()
	if err := mux.HandleChannel("chat", func(ctx context.Context, c *taonet.Channel) error {
		for {
			delivery, err := c.Receive(ctx)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err = delivery.Accept(); err != nil {
				return err
			}
			reply := taonet.Message{Body: []byte("server: " + string(delivery.Message().Body))}
			delivery.Release()
			if err = c.Send(ctx, reply); err != nil {
				return err
			}
		}
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
		session, err := taonet.Accept(ctx, conn, taonet.SessionConfig{Local: taonet.Peer{ID: "server"}, Mux: mux, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}})
		if err == nil {
			<-session.Done()
			err = session.Close()
		}
		serverDone <- err
	}()
	session, err := taonet.Dial(ctx, listener.Addr().String(), taonet.SessionConfig{Local: taonet.Peer{ID: "client"}, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}})
	if err != nil {
		return err
	}
	defer session.Close()
	channel, err := session.OpenChannel(ctx, "chat", nil)
	if err != nil {
		return err
	}
	defer channel.Close()
	readDone := make(chan error, 1)
	go func() {
		for {
			delivery, err := channel.Receive(ctx)
			if err == io.EOF {
				readDone <- nil
				return
			}
			if err != nil {
				readDone <- err
				return
			}
			fmt.Println(string(delivery.Message().Body))
			err = delivery.Accept()
			delivery.Release()
			if err != nil {
				readDone <- err
				return
			}
		}
	}()
	var receipts []*taonet.SendReceipt
	for _, text := range []string{"hello", "both sides can send", "goodbye"} {
		receipt, err := channel.SendWithReceipt(ctx, taonet.Message{Body: []byte(text)})
		if err != nil {
			return err
		}
		receipts = append(receipts, receipt)
	}
	for _, receipt := range receipts {
		if err := receipt.Wait(ctx); err != nil {
			return err
		}
	}
	if err = channel.CloseWrite(); err != nil {
		return err
	}
	if err = <-readDone; err != nil {
		return err
	}
	select {
	case <-channel.Done():
	case <-ctx.Done():
		return ctx.Err()
	}
	if err = channel.Err(); err != nil {
		return err
	}
	if err = session.Shutdown(ctx); err != nil {
		return err
	}
	return <-serverDone
}
