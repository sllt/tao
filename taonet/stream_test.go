package taonet

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

type repeatedByte byte

func (b repeatedByte) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}

func TestStreamUploadThenReadResult(t *testing.T) {
	m := NewMux()
	if err := m.HandleStream("upload", func(ctx context.Context, st *Stream) error {
		if st.Metadata["name"] != "file.bin" {
			return errors.New("missing metadata")
		}
		h := sha256.New()
		n, err := io.CopyBuffer(h, st, make([]byte, 32<<10))
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(st, "%d %x", n, h.Sum(nil))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	client, server := pair(t, localConfig("client", nil), localConfig("server", m))
	st, err := client.OpenStream(t.Context(), "upload", map[string]string{"name": "file.bin"})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const size = 3 << 20
	h := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(st, h), io.LimitReader(repeatedByte('x'), size), make([]byte, 32<<10))
	if err != nil || n != size {
		t.Fatalf("upload: %d %v", n, err)
	}
	if err = st.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	result, err := io.ReadAll(st)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != fmt.Sprintf("%d %x", size, h.Sum(nil)) {
		t.Fatalf("bad digest: %s", result)
	}
	waitSignal(t, st.Done())
	if st.Err() != nil {
		t.Fatal(st.Err())
	}
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestStreamPartialHeaderSurvivesReadDeadline(t *testing.T) {
	partial, resume := make(chan struct{}), make(chan struct{})
	defer close(resume)
	m := NewMux()
	if err := m.HandleStream("partial", func(ctx context.Context, st *Stream) error {
		// Deliberately write wire fragments to exercise a partial HN header.
		if _, err := io.Copy(io.Discard, st); err != nil {
			return err
		}
		var header [16]byte
		header[0] = recordData
		binary.BigEndian.PutUint32(header[4:8], 3)
		if _, err := st.raw.Write(header[:7]); err != nil {
			return err
		}
		close(partial)
		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
		if _, err := st.raw.Write(header[7:]); err != nil {
			return err
		}
		if _, err := st.raw.Write([]byte("abc")); err != nil {
			return err
		}
		if err := writeRecord(st.raw, recordEnd, 3, nil); err != nil {
			return err
		}
		return st.raw.CloseWrite()
	}); err != nil {
		t.Fatal(err)
	}
	client, _ := pair(t, localConfig("client", nil), localConfig("server", m))
	st, err := client.OpenStream(t.Context(), "partial", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err = st.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, partial)
	_ = st.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	var buf [10]byte
	if _, err = st.Read(buf[:]); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	if st.headerRead != 7 {
		t.Fatalf("partial header lost: %d", st.headerRead)
	}
	_ = st.SetReadDeadline(time.Time{})
	resume <- struct{}{}
	p, err := io.ReadAll(st)
	if err != nil || string(p) != "abc" {
		t.Fatalf("resume: %q %v", p, err)
	}
}

func TestStreamTruncationIsNotEOF(t *testing.T) {
	m := NewMux()
	if err := m.HandleStream("truncated", func(_ context.Context, st *Stream) error {
		if _, err := io.Copy(io.Discard, st); err != nil {
			return err
		}
		return st.raw.CloseWrite()
	}); err != nil {
		t.Fatal(err)
	}
	handle(t, m, "echo", func(_ context.Context, r *Request) (Message, error) { return r.Message, nil })
	client, _ := pair(t, localConfig("client", nil), localConfig("server", m))
	st, err := client.OpenStream(t.Context(), "truncated", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_ = st.CloseWrite()
	if _, err = io.ReadAll(st); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated stream: %v", err)
	}
	if _, err = client.Request(t.Context(), "echo", Message{}); err != nil {
		t.Fatal("truncation killed session:", err)
	}
}

func TestStreamCancellationWakesWriterAndHandler(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	m := NewMux()
	if err := m.HandleStream("blocked", func(ctx context.Context, _ *Stream) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	client, server := pair(t, localConfig("client", nil), localConfig("server", m))
	ctx, cancel := context.WithCancel(t.Context())
	st, err := client.OpenStream(ctx, "blocked", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	waitSignal(t, started)
	result := make(chan error, 1)
	go func() {
		_, err := io.CopyBuffer(st, io.LimitReader(repeatedByte('x'), 8<<20), make([]byte, 32<<10))
		result <- err
	}()
	cancel()
	select {
	case err = <-result:
		if err == nil {
			t.Fatal("canceled write succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not unblock")
	}
	waitSignal(t, canceled)
	waitIdle(t, client)
	waitIdle(t, server)
}
