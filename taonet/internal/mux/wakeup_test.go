package mux

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestAcceptNotificationsIgnoreEstablishedStreamIO(t *testing.T) {
	a, b := pipePair(t, Config{}, Config{})
	sender, receiver := openPair(t, a, b)
	b.mu.Lock()
	accept := b.acceptChangedLocked()
	b.mu.Unlock()
	if _, err := sender.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-accept:
		t.Fatal("DATA woke a new-stream waiter")
	default:
	}
	openPair(t, a, b)
	select {
	case <-accept:
	default:
		t.Fatal("OPEN did not wake a new-stream waiter")
	}
	b.mu.Lock()
	accept = b.acceptChangedLocked()
	b.mu.Unlock()
	_ = b.Close()
	select {
	case <-accept:
	default:
		t.Fatal("session close did not wake a new-stream waiter")
	}
}

func TestReadNotificationsIgnoreUnrelatedWritesAndStreams(t *testing.T) {
	a, b := pipePair(t, Config{}, Config{})
	_, first := openPair(t, a, b)
	_, second := openPair(t, a, b)
	b.mu.Lock()
	firstRead, secondRead := first.readWakeLocked(), second.readWakeLocked()
	b.mu.Unlock()
	if err := first.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan struct{}{firstRead, secondRead} {
		select {
		case <-ch:
			t.Fatal("write deadline woke a reader")
		default:
		}
	}
	read := make(chan error, 1)
	go func() { _, err := first.Read(make([]byte, 1)); read <- err }()
	if err := first.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := awaitError(t, testContext(t), read); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read deadline did not wake its reader: %v", err)
	}
	select {
	case <-secondRead:
		t.Fatal("one stream's read deadline woke another reader")
	default:
	}
	go func() { _, err := second.Read(make([]byte, 1)); read <- err }()
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := awaitError(t, testContext(t), read); err == nil {
		t.Fatal("reset did not wake reader with an error")
	}
}
