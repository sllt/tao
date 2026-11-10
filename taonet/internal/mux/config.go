// Package mux implements the HN-MUX/1 transport used by taonet.
package mux

import (
	"errors"
	"fmt"
	"time"
)

const (
	controlWindow = 64 << 10
	controlBudget = 64 << 10
	windowDelay   = 10 * time.Millisecond
)

// Config is copied and normalized when a session starts.
type Config struct {
	MaxDataPayload   uint32
	MaxPeerStreams   uint32
	MaxOpenBacklog   uint32
	MaxGrantedWindow uint32
	ReceiveWindow    uint32
	ReceiveBudget    uint64
	MaxStreamsTotal  int
	SendBudget       int
	IOTimeout        time.Duration
	ResetTimeout     time.Duration
}

func (c Config) normalized() (Config, error) {
	if c.MaxDataPayload == 0 {
		c.MaxDataPayload = 32 << 10
	}
	if c.MaxPeerStreams == 0 {
		c.MaxPeerStreams = 256
	}
	if c.MaxOpenBacklog == 0 {
		c.MaxOpenBacklog = min(64, c.MaxPeerStreams)
	}
	if c.MaxGrantedWindow == 0 {
		c.MaxGrantedWindow = 1 << 20
	}
	if c.ReceiveWindow == 0 {
		c.ReceiveWindow = min(64<<10, c.MaxGrantedWindow)
	}
	if c.ReceiveBudget == 0 {
		c.ReceiveBudget = 4 << 20
	}
	if c.MaxStreamsTotal == 0 {
		c.MaxStreamsTotal = 256
	}
	if c.SendBudget == 0 {
		c.SendBudget = 1 << 20
	}
	if c.IOTimeout == 0 {
		c.IOTimeout = 5 * time.Second
	}
	if c.ResetTimeout == 0 {
		c.ResetTimeout = 5 * time.Second
	}
	if err := c.settings().validate(); err != nil {
		return Config{}, err
	}
	if !validWindow(uint64(c.ReceiveWindow), c.MaxGrantedWindow) {
		return Config{}, errors.New("mux: invalid receive window")
	}
	if c.ReceiveBudget < uint64(c.ReceiveWindow) || c.MaxStreamsTotal < 1 || c.MaxStreamsTotal > 8192 {
		return Config{}, errors.New("mux: invalid receive budget or total stream limit")
	}
	if c.SendBudget < headerSize+int(c.MaxDataPayload) || c.IOTimeout < 0 || c.ResetTimeout < 0 {
		return Config{}, errors.New("mux: invalid send budget or timeout")
	}
	return c, nil
}

func (c Config) settings() settings {
	return settings{c.MaxDataPayload, c.MaxPeerStreams, c.MaxOpenBacklog, c.MaxGrantedWindow, controlWindow, 0}
}

func validWindow(n uint64, maximum uint32) bool {
	return n >= 4096 && n <= uint64(maximum) && n%4096 == 0
}

var (
	ErrProtocol = errors.New("mux: protocol violation")
	ErrClosed   = errors.New("mux: session closed")
	ErrNotReady = errors.New("mux: session not ready")
	ErrDraining = errors.New("mux: session draining")
	ErrCapacity = errors.New("mux: capacity exhausted")
)

type Code uint32

const (
	Normal Code = iota
	ResourceExhausted
	Draining
	NotReady
	Canceled
	DeadlineExceeded
	Internal
	ProtocolError
)

// StreamError is an operation-level rejection or reset, not a connection fault.
type StreamError struct{ Code Code }

func (e *StreamError) Error() string { return fmt.Sprintf("mux: stream error (code %d)", e.Code) }

func protocolError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}
