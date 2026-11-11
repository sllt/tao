package dist

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sllt/tao/erlang/etf"
)

const (
	AtomCacheSize       = 2048 // 8 segments * 256 entries
	MaxAtomRefsPerMsg   = 255
	PassThroughTag      = 112
	DistHeaderNormal    = 68
	DistHeaderFragStart = 69
	DistHeaderFragCont  = 70
)

var (
	ErrMalformedDistHeader = errors.New("dist: malformed distribution header")
	ErrAtomCacheMiss       = errors.New("dist: atom cache reference points to uninitialized slot")
)

// RecvAtomCache maintains the 2048-slot inbound atom cache owned exclusively by a single peer reader.
type RecvAtomCache struct {
	slots [AtomCacheSize]etf.Atom
	valid [AtomCacheSize]bool
}

// ParseRefs parses the NumberOfAtomCacheRefs, Flags, and AtomCacheRefs section of a
// normal (68) or starting-fragment (69) distribution header, commits updates to the
// connection's receive cache in wire order, and returns the message's atom reference table
// together with the remaining frame bytes.
func (c *RecvAtomCache) ParseRefs(data []byte) ([]etf.Atom, []byte, error) {
	if len(data) < 1 {
		return nil, nil, fmt.Errorf("%w: missing NumberOfAtomCacheRefs", ErrMalformedDistHeader)
	}
	numRefs := int(data[0])
	rest := data[1:]
	if numRefs == 0 {
		return emptyAtomSlice, rest, nil
	}

	flagBytesLen := numRefs/2 + 1
	if len(rest) < flagBytesLen {
		return nil, nil, fmt.Errorf("%w: truncated cache flags", ErrMalformedDistHeader)
	}
	flags := rest[:flagBytesLen]
	rest = rest[flagBytesLen:]

	hdrNibble := readNibble(flags, numRefs)
	longAtoms := (hdrNibble & 0x01) != 0

	refs := make([]etf.Atom, numRefs)
	for i := 0; i < numRefs; i++ {
		nib := readNibble(flags, i)
		isNew := (nib & 0x08) != 0
		seg := int(nib & 0x07)

		if len(rest) < 1 {
			return nil, nil, fmt.Errorf("%w: truncated cache entry %d", ErrMalformedDistHeader, i)
		}
		internalIdx := int(rest[0])
		rest = rest[1:]
		slot := (seg << 8) | internalIdx

		if isNew {
			var atomLen int
			if longAtoms {
				if len(rest) < 2 {
					return nil, nil, fmt.Errorf("%w: truncated long atom length", ErrMalformedDistHeader)
				}
				atomLen = int(binary.BigEndian.Uint16(rest[:2]))
				rest = rest[2:]
			} else {
				if len(rest) < 1 {
					return nil, nil, fmt.Errorf("%w: truncated short atom length", ErrMalformedDistHeader)
				}
				atomLen = int(rest[0])
				rest = rest[1:]
			}
			if len(rest) < atomLen {
				return nil, nil, fmt.Errorf("%w: truncated atom text (%d > %d)", ErrMalformedDistHeader, atomLen, len(rest))
			}
			atom := etf.Atom(string(rest[:atomLen]))
			if err := etf.ValidateAtom(atom); err != nil {
				return nil, nil, fmt.Errorf("%w: %v", ErrMalformedDistHeader, err)
			}
			rest = rest[atomLen:]
			c.slots[slot] = atom
			c.valid[slot] = true
			refs[i] = atom
		} else {
			if !c.valid[slot] {
				return nil, nil, fmt.Errorf("%w: slot %d (seg=%d idx=%d)", ErrAtomCacheMiss, slot, seg, internalIdx)
			}
			refs[i] = c.slots[slot]
		}
	}
	return refs, rest, nil
}

var emptyAtomSlice = []etf.Atom{}

func readNibble(flags []byte, k int) byte {
	return (flags[k/2] >> ((k & 1) * 4)) & 0x0f
}

func writeNibble(flags []byte, k int, nib byte) {
	shift := (k & 1) * 4
	flags[k/2] = (flags[k/2] &^ (0x0f << shift)) | ((nib & 0x0f) << shift)
}

type stagedAtomRef struct {
	atom  etf.Atom
	slot  uint16
	isNew bool
}

// SendAtomCache maintains the 2048-slot outbound atom cache owned exclusively by a single peer writer.
// Per-message updates are staged until CommitHeader succeeds so a failed term encode never leaves
// the connection cache out of sync with the wire.
type SendAtomCache struct {
	slots [AtomCacheSize]etf.Atom
	valid [AtomCacheSize]bool

	// Per-message staging state:
	msgIndex map[etf.Atom]uint8
	usedSlot [AtomCacheSize / 64]uint64
	staged   []stagedAtomRef
	longAtom bool
}

// NewSendAtomCache creates a new outbound atom cache.
func NewSendAtomCache() *SendAtomCache {
	return &SendAtomCache{
		msgIndex: make(map[etf.Atom]uint8, 32),
		staged:   make([]stagedAtomRef, 0, 32),
	}
}

// BeginMessage resets per-message staging without modifying committed cache slots.
func (c *SendAtomCache) BeginMessage() {
	clear(c.msgIndex)
	c.usedSlot = [AtomCacheSize / 64]uint64{}
	c.staged = c.staged[:0]
	c.longAtom = false
}

// AtomRef implements etf.AtomEncoder during single-message staging.
func (c *SendAtomCache) AtomRef(a etf.Atom) (uint8, bool) {
	if idx, ok := c.msgIndex[a]; ok {
		return idx, true
	}
	if len(c.staged) >= MaxAtomRefsPerMsg {
		return 0, false
	}
	slot := atomCacheSlot(a)
	word, bit := slot>>6, uint64(1)<<(slot&63)
	if c.usedSlot[word]&bit != 0 {
		// Another distinct atom in this same message already claimed this 11-bit slot;
		// encode this atom inline so we do not clobber the earlier ref in the same header.
		return 0, false
	}
	c.usedSlot[word] |= bit
	isNew := !c.valid[slot] || c.slots[slot] != a
	if isNew && len(a) > 255 {
		c.longAtom = true
	}
	idx := uint8(len(c.staged))
	c.msgIndex[a] = idx
	c.staged = append(c.staged, stagedAtomRef{
		atom:  a,
		slot:  slot,
		isNew: isNew,
	})
	return idx, true
}

// CommitRefs commits the staged per-message atom entries to the connection cache
// and appends the NumberOfAtomCacheRefs + Flags + AtomCacheRefs bytes to dst.
func (c *SendAtomCache) CommitRefs(dst []byte) []byte {
	numRefs := len(c.staged)
	if numRefs == 0 {
		return append(dst, 0)
	}
	dst = append(dst, byte(numRefs))
	flagBytesLen := numRefs/2 + 1
	flagStart := len(dst)
	dst = append(dst, make([]byte, flagBytesLen)...)
	flags := dst[flagStart : flagStart+flagBytesLen]

	for i, ref := range c.staged {
		seg := byte((ref.slot >> 8) & 0x07)
		nib := seg
		if ref.isNew {
			nib |= 0x08
		}
		writeNibble(flags, i, nib)
	}
	var hdrNibble byte
	if c.longAtom {
		hdrNibble = 0x01
	}
	writeNibble(flags, numRefs, hdrNibble)

	for _, ref := range c.staged {
		internalIdx := byte(ref.slot & 0xff)
		dst = append(dst, internalIdx)
		if ref.isNew {
			s := string(ref.atom)
			if c.longAtom {
				dst = append(dst, byte(len(s)>>8), byte(len(s)))
			} else {
				dst = append(dst, byte(len(s)))
			}
			dst = append(dst, s...)
			c.slots[ref.slot] = ref.atom
			c.valid[ref.slot] = true
		}
	}
	return dst
}

func atomCacheSlot(a etf.Atom) uint16 {
	var h uint32 = 2166136261
	for i := 0; i < len(a); i++ {
		h ^= uint32(a[i])
		h *= 16777619
	}
	h ^= h >> 16
	return uint16(h & (AtomCacheSize - 1))
}
