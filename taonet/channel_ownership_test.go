package taonet

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestOwnedInputReleaseIsIndependentOfAcceptance(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	input := []byte("owned input")
	receipt, err := a.SendOwnedWithReceipt(ctx, Message{Body: input})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-receipt.Released():
	case <-ctx.Done():
		t.Fatal("input release waited for application acceptance")
	}
	clear(input)
	select {
	case <-receipt.Done():
		t.Fatal("local write was reported as remote acceptance")
	default:
	}
	delivery := receive(t, b)
	if !bytes.Equal(delivery.Message().Body, []byte("owned input")) {
		t.Fatal("released input still affected the received message")
	}
	if err := delivery.Accept(); err != nil {
		t.Fatal(err)
	}
	delivery.Release()
	if err := receipt.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	closeChannels(t, a, b)
}

func TestChannelCloseReleasesUnsentOwnedInput(t *testing.T) {
	a, _, client, _ := channelPair(t, Limits{ChannelWindowMessages: 1})
	first, err := a.SendOwnedWithReceipt(t.Context(), Message{Body: []byte("fills window")})
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, first.Released())
	second, err := a.SendOwnedWithReceipt(t.Context(), Message{Body: []byte("still local")})
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	waitSignal(t, second.Released())
	requireOpError(t, second.Wait(t.Context()), CodeUnavailable, NotSent)
	waitIdle(t, client)
}
