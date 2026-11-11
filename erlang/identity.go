package erlang

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"github.com/sllt/tao/erlang/etf"
)

const (
	minGeneratedCreation = 4
	maxRefWord0Mask      = (1 << 18) - 1
)

var (
	activeCreationsMu sync.Mutex
	activeCreations   = make(map[nodeIncarnation]struct{})
)

type nodeIncarnation struct {
	name     string
	creation uint32
}

// acquireGeneratedCreation selects a random Creation in [4, 2^32-1] (OTP dist.c lower bound)
// that is not currently active in this OS process for the same node name.
func acquireGeneratedCreation(nodeName string) (uint32, func(), error) {
	activeCreationsMu.Lock()
	defer activeCreationsMu.Unlock()

	for attempts := 0; attempts < 128; attempts++ {
		var buf [4]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return 0, nil, err
		}
		v := binary.BigEndian.Uint32(buf[:])
		if v < minGeneratedCreation {
			v += minGeneratedCreation
		}
		key := nodeIncarnation{name: nodeName, creation: v}
		if _, taken := activeCreations[key]; taken {
			continue
		}
		activeCreations[key] = struct{}{}
		var once sync.Once
		release := func() {
			once.Do(func() {
				activeCreationsMu.Lock()
				delete(activeCreations, key)
				activeCreationsMu.Unlock()
			})
		}
		return v, release, nil
	}
	return 0, nil, ErrIdentityExhausted
}

// idAllocator issues strictly monotonic, non-wrapping PIDs, Refs, and UnlinkIDs
// within a single node incarnation (§4.3).
type idAllocator struct {
	node     etf.Atom
	creation uint32
	pidSeq   atomic.Uint64
	refSeq   atomic.Uint64
	unlnkSeq atomic.Uint64
}

func newIDAllocator(node etf.Atom, creation uint32) *idAllocator {
	return &idAllocator{
		node:     node,
		creation: creation,
	}
}

func (a *idAllocator) systemPID() PID {
	return PID{
		Node:     a.node,
		ID:       0,
		Serial:   0,
		Creation: a.creation,
	}
}

func (a *idAllocator) nextPID() (PID, error) {
	for {
		cur := a.pidSeq.Load()
		if cur == math.MaxUint64 {
			return PID{}, ErrIdentityExhausted
		}
		next := cur + 1
		if a.pidSeq.CompareAndSwap(cur, next) {
			return PID{
				Node:     a.node,
				ID:       uint32(next),
				Serial:   uint32(next >> 32),
				Creation: a.creation,
			}, nil
		}
	}
}

// nextRef allocates a 3-word NEWER_REFERENCE_EXT with an 18-bit word 0 (matching BEAM MAX_REFERENCE)
// and 64 bits across words 1..2, never wrapping within an incarnation.
func (a *idAllocator) nextRef() (Ref, error) {
	for {
		cur := a.refSeq.Load()
		if cur == math.MaxUint64 {
			return Ref{}, ErrIdentityExhausted
		}
		next := cur + 1
		if a.refSeq.CompareAndSwap(cur, next) {
			var r Ref
			r.Node = a.node
			r.Creation = a.creation
			r.Len = 3
			r.ID[0] = uint32(next) & maxRefWord0Mask
			r.ID[1] = uint32(next >> 18)
			r.ID[2] = uint32(next >> 50)
			return r, nil
		}
	}
}

func (a *idAllocator) nextUnlinkID() (uint64, error) {
	for {
		cur := a.unlnkSeq.Load()
		if cur == math.MaxUint64 {
			return 0, ErrIdentityExhausted
		}
		next := cur + 1
		if a.unlnkSeq.CompareAndSwap(cur, next) {
			return next, nil
		}
	}
}
