// mux-echo demonstrates native H0 streams on a loopback TCP connection.
// The higher-level HN/1 identity and Request protocol is not implemented here.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/sllt/tao/taonet/internal/mux"
)

type result struct {
	name, reply string
	err         error
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	type accepted struct {
		session *mux.Session
		err     error
	}
	serverResult := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverResult <- accepted{err: err}
			return
		}
		s, err := mux.New(ctx, conn, mux.Acceptor, mux.Config{})
		serverResult <- accepted{session: s, err: err}
	}()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		return err
	}
	client, err := mux.New(ctx, conn, mux.Initiator, mux.Config{})
	if err != nil {
		return err
	}
	defer client.Close()
	var server *mux.Session
	select {
	case r := <-serverResult:
		if r.err != nil {
			return r.err
		}
		server = r.session
	case <-ctx.Done():
		return ctx.Err()
	}
	defer server.Close()
	// H1 will perform its identity handshake before marking the mux ready.
	client.MarkReady()
	server.MarkReady()
	results := make(chan result, 4)
	for name, s := range map[string]*mux.Session{"client": client, "server": server} {
		go func() { results <- result{err: echo(ctx, s)} }()
		go func() {
			reply, err := exchange(ctx, s, name+" ping")
			results <- result{name: name, reply: reply, err: err}
		}()
	}
	replies := make(map[string]string)
	for range 4 {
		select {
		case r := <-results:
			if r.err != nil {
				return r.err
			}
			if r.name != "" {
				replies[r.name] = r.reply
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fmt.Printf("client received: %s\nserver received: %s\n", replies["client"], replies["server"])
	return nil
}

func echo(ctx context.Context, s *mux.Session) error {
	st, err := s.AcceptStream(ctx)
	if err != nil {
		return err
	}
	p, err := io.ReadAll(st)
	if err != nil {
		return err
	}
	if _, err = st.Write(append([]byte("pong: "), p...)); err != nil {
		return err
	}
	return st.CloseWrite()
}

func exchange(ctx context.Context, s *mux.Session, message string) (string, error) {
	st, err := s.OpenStream(ctx)
	if err != nil {
		return "", err
	}
	if _, err = st.Write([]byte(message)); err != nil {
		return "", err
	}
	if err = st.CloseWrite(); err != nil {
		return "", err
	}
	p, err := io.ReadAll(st)
	return string(p), err
}
