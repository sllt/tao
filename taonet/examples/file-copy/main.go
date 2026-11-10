package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
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
	dir, err := os.MkdirTemp("", "taonet-copy-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	source, err := os.Create(filepath.Join(dir, "source.bin"))
	if err != nil {
		return err
	}
	defer source.Close()
	chunk := make([]byte, 32<<10)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	for range 128 {
		if _, err = source.Write(chunk); err != nil {
			return err
		}
	}
	if _, err = source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	mux := taonet.NewMux()
	if err = mux.HandleStream("file.upload", func(ctx context.Context, st *taonet.Stream) error {
		destination, err := os.Create(filepath.Join(dir, "received.bin"))
		if err != nil {
			return err
		}
		defer destination.Close()
		digest := sha256.New()
		n, err := io.CopyBuffer(io.MultiWriter(destination, digest), st, make([]byte, 32<<10))
		if err != nil {
			return err
		}
		if err = destination.Sync(); err != nil {
			return err
		}
		_, err = fmt.Fprintf(st, "stored %d bytes; sha256=%x", n, digest.Sum(nil))
		return err
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
		session, err := taonet.Accept(ctx, conn, taonet.SessionConfig{Local: taonet.Peer{ID: "receiver"}, Mux: mux, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}})
		if err == nil {
			<-session.Done()
			err = session.Close()
		}
		serverDone <- err
	}()
	session, err := taonet.Dial(ctx, listener.Addr().String(), taonet.SessionConfig{Local: taonet.Peer{ID: "sender"}, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}})
	if err != nil {
		return err
	}
	defer session.Close()
	stream, err := session.OpenStream(ctx, "file.upload", nil)
	if err != nil {
		return err
	}
	defer stream.Close()
	if _, err = io.CopyBuffer(stream, source, chunk); err != nil {
		return err
	}
	if err = stream.CloseWrite(); err != nil {
		return err
	}
	if _, err = io.Copy(os.Stdout, stream); err != nil {
		return err
	}
	fmt.Println()
	if err = session.Shutdown(ctx); err != nil {
		return err
	}
	return <-serverDone
}
