package taonet

import (
	"fmt"
	"maps"
	"sync"
)

type Mux struct {
	mu       sync.Mutex
	frozen   bool
	requests map[string]RequestHandler
	notifies map[string]NotifyHandler
	streams  map[string]StreamHandler
	channels map[string]ChannelHandler
}

func NewMux() *Mux { return &Mux{requests: make(map[string]RequestHandler)} }

func validRoute(route string) bool {
	if len(route) == 0 || len(route) > 128 {
		return false
	}
	for _, b := range []byte(route) {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '/' || b == '.' || b == '_' || b == '-') {
			return false
		}
	}
	return true
}

func (m *Mux) HandleRequest(route string, h RequestHandler) error {
	if !validRoute(route) || h == nil {
		return fmt.Errorf("taonet: invalid route or handler")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return fmt.Errorf("taonet: mux is already in use")
	}
	if _, exists := m.requests[route]; exists {
		return fmt.Errorf("taonet: duplicate route %q", route)
	}
	if m.requests == nil {
		m.requests = make(map[string]RequestHandler)
	}
	m.requests[route] = h
	return nil
}

func (m *Mux) HandleNotify(route string, h NotifyHandler) error {
	if !validRoute(route) || h == nil {
		return fmt.Errorf("taonet: invalid route or handler")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return fmt.Errorf("taonet: mux is already in use")
	}
	if _, exists := m.notifies[route]; exists {
		return fmt.Errorf("taonet: duplicate notify route %q", route)
	}
	if m.notifies == nil {
		m.notifies = make(map[string]NotifyHandler)
	}
	m.notifies[route] = h
	return nil
}

type routeTable struct {
	requests map[string]RequestHandler
	notifies map[string]NotifyHandler
	streams  map[string]StreamHandler
	channels map[string]ChannelHandler
}

func (m *Mux) HandleChannel(route string, h ChannelHandler) error {
	if !validRoute(route) || h == nil {
		return fmt.Errorf("taonet: invalid route or handler")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return fmt.Errorf("taonet: mux is already in use")
	}
	if _, exists := m.channels[route]; exists {
		return fmt.Errorf("taonet: duplicate channel route %q", route)
	}
	if m.channels == nil {
		m.channels = make(map[string]ChannelHandler)
	}
	m.channels[route] = h
	return nil
}

func (m *Mux) HandleStream(route string, h StreamHandler) error {
	if !validRoute(route) || h == nil {
		return fmt.Errorf("taonet: invalid route or handler")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return fmt.Errorf("taonet: mux is already in use")
	}
	if _, exists := m.streams[route]; exists {
		return fmt.Errorf("taonet: duplicate stream route %q", route)
	}
	if m.streams == nil {
		m.streams = make(map[string]StreamHandler)
	}
	m.streams[route] = h
	return nil
}

func (m *Mux) snapshot() routeTable {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
	return routeTable{requests: maps.Clone(m.requests), notifies: maps.Clone(m.notifies), streams: maps.Clone(m.streams), channels: maps.Clone(m.channels)}
}
