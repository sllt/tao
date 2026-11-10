package actor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// DirectoryCache is a routing hint. Ownership changes and uncertain writes
// must use the underlying Directory directly, never a cached lookup.
type DirectoryCache struct {
	directory Directory
	prefix    string
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	mu        sync.RWMutex
	routes    map[string]RouteRecord
	ready     bool
	revision  int64
}

func NewDirectoryCache(ctx context.Context, directory Directory, prefix string) (*DirectoryCache, error) {
	if directory == nil {
		return nil, errors.New("actor directory: nil backend")
	}
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" {
		if err := ValidateDirectoryPath(prefix); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, err
	}
	cache := &DirectoryCache{directory: directory, prefix: prefix, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go cache.run(nil)
	return cache, nil
}

func (cache *DirectoryCache) Close() { cache.cancel(); <-cache.done }

// Lookup may return a stale route; receivers must validate its epoch. A miss
// falls back to a linearizable lookup, without letting that in-flight read
// overwrite a newer watch event or a tombstone.
func (cache *DirectoryCache) Lookup(ctx context.Context, path string) (RouteRecord, error) {
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, err
	}
	cache.mu.RLock()
	route, exists := cache.routes[path]
	ready := cache.ready
	cache.mu.RUnlock()
	if ready && exists {
		if route.State == RouteTombstone {
			return route, ErrDirectoryNotFound
		}
		return route, nil
	}
	return cache.directory.Lookup(ctx, path)
}

// Invalidate drops a rejected route hint. Resolve authoritative state before retrying.
func (cache *DirectoryCache) Invalidate(path string) {
	cache.mu.Lock()
	delete(cache.routes, path)
	cache.mu.Unlock()
}

func (cache *DirectoryCache) clear() {
	cache.mu.Lock()
	cache.ready = false
	cache.routes = nil
	cache.revision = 0
	cache.mu.Unlock()
}

func (cache *DirectoryCache) apply(event DirectoryEvent) bool {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if event.Err != nil {
		return false
	}
	if event.Snapshot {
		cache.routes = make(map[string]RouteRecord, len(event.Routes))
		for _, route := range event.Routes {
			cache.routes[route.Path] = route
		}
		cache.ready, cache.revision = true, event.Revision
		return true
	}
	if !cache.ready {
		return false
	}
	if event.Revision <= cache.revision {
		return true
	}
	for _, route := range event.Routes {
		cache.routes[route.Path] = route
	}
	cache.revision = event.Revision
	return true
}

func (cache *DirectoryCache) run(events <-chan DirectoryEvent) {
	defer close(cache.done)
	defer cache.clear()
	for {
		if events != nil {
			select {
			case <-cache.ctx.Done():
				return
			case event, ok := <-events:
				if ok && cache.apply(event) {
					continue
				}
				events = nil
				cache.clear()
			}
		}
		// A disconnected backend must not turn into a busy reconnect loop.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-cache.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		var err error
		events, err = cache.directory.Watch(cache.ctx, cache.prefix)
		if err != nil {
			events = nil
		}
	}
}
