package etf

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/big"
)

type normInt struct {
	small int64
	big   *big.Int // non-nil only when value does not fit in int64
}

func normalizeInt(v Term) (normInt, bool) {
	switch x := v.(type) {
	case int:
		return normInt{small: int64(x)}, true
	case int8:
		return normInt{small: int64(x)}, true
	case int16:
		return normInt{small: int64(x)}, true
	case int32:
		return normInt{small: int64(x)}, true
	case int64:
		return normInt{small: x}, true
	case uint:
		if uint64(x) <= math.MaxInt64 {
			return normInt{small: int64(x)}, true
		}
		return normInt{big: new(big.Int).SetUint64(uint64(x))}, true
	case uint8:
		return normInt{small: int64(x)}, true
	case uint16:
		return normInt{small: int64(x)}, true
	case uint32:
		return normInt{small: int64(x)}, true
	case uint64:
		if x <= math.MaxInt64 {
			return normInt{small: int64(x)}, true
		}
		return normInt{big: new(big.Int).SetUint64(x)}, true
	case *big.Int:
		if x == nil {
			return normInt{}, false
		}
		if x.IsInt64() {
			return normInt{small: x.Int64()}, true
		}
		return normInt{big: x}, true
	default:
		return normInt{}, false
	}
}

// TermEqual reports whether a and b are identical under Erlang's exact equality (=:=).
//
// In particular:
//   - Integers of different Go integer types with the same mathematical value are equal.
//   - Integers and floats are never equal (1 =:= 1.0 is false).
//   - 0.0 and -0.0 are distinct (OTP 27+ exact float equality).
//   - Proper and nested Lists are compared as canonical cons sequences.
//   - Maps are compared independent of slice pair order.
//   - Refs compare Node, Creation, and 5-word canonical IDs.
//   - Funs ignore the creator PID, matching BEAM's NEW_FUN_EXT equality.
func TermEqual(a, b Term) bool {
	if a == nil || b == nil {
		return false
	}
	if ai, ok := normalizeInt(a); ok {
		bi, bok := normalizeInt(b)
		if !bok {
			return false
		}
		if ai.big == nil && bi.big == nil {
			return ai.small == bi.small
		}
		if ai.big != nil && bi.big != nil {
			return ai.big.Cmp(bi.big) == 0
		}
		return false
	}

	switch av := a.(type) {
	case Atom:
		bv, ok := b.(Atom)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		if !ok || math.IsNaN(av) || math.IsNaN(bv) || math.IsInf(av, 0) || math.IsInf(bv, 0) {
			return false
		}
		return math.Float64bits(av) == math.Float64bits(bv)
	case Binary:
		bv, ok := b.(Binary)
		return ok && bytes.Equal(av, bv)
	case BitBinary:
		bv, ok := b.(BitBinary)
		if !ok || av.LastBits != bv.LastBits || len(av.Bytes) != len(bv.Bytes) || len(av.Bytes) == 0 {
			return false
		}
		if av.LastBits < 1 || av.LastBits > 7 {
			return false
		}
		n := len(av.Bytes)
		if !bytes.Equal(av.Bytes[:n-1], bv.Bytes[:n-1]) {
			return false
		}
		mask := byte(0xFF) << (8 - av.LastBits)
		return (av.Bytes[n-1] & mask) == (bv.Bytes[n-1] & mask)
	case Nil:
		if _, ok := b.(Nil); ok {
			return true
		}
		if bl, ok := b.(List); ok {
			return listIsNil(bl)
		}
		return false
	case Tuple:
		bv, ok := b.(Tuple)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !TermEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case List:
		if _, ok := b.(Nil); ok {
			return listIsNil(av)
		}
		bv, ok := b.(List)
		if !ok {
			return false
		}
		return listEqual(av, bv)
	case Map:
		bv, ok := b.(Map)
		if !ok || len(av) != len(bv) {
			return false
		}
		return mapEqual(av, bv)
	case PID:
		bv, ok := b.(PID)
		return ok && av == bv
	case Port:
		bv, ok := b.(Port)
		return ok && av == bv
	case Ref:
		bv, ok := b.(Ref)
		return ok && av.Equal(bv)
	case Export:
		bv, ok := b.(Export)
		return ok && av == bv
	case Fun:
		bv, ok := b.(Fun)
		if !ok {
			return false
		}
		if av.Arity != bv.Arity ||
			av.Uniq != bv.Uniq ||
			av.Index != bv.Index ||
			av.Module != bv.Module ||
			av.OldIndex != bv.OldIndex ||
			av.OldUniq != bv.OldUniq ||
			len(av.FreeVars) != len(bv.FreeVars) {
			return false
		}
		for i := range av.FreeVars {
			if !TermEqual(av.FreeVars[i], bv.FreeVars[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func listIsNil(l List) bool {
	cur := Term(l)
	for {
		lv, ok := cur.(List)
		if !ok {
			_, isNil := cur.(Nil)
			return isNil
		}
		if len(lv.Elements) > 0 {
			return false
		}
		cur = lv.Tail
	}
}

type listCursor struct {
	elems []Term
	idx   int
	tail  Term
}

func newListCursor(t Term) listCursor {
	if l, ok := t.(List); ok {
		return listCursor{elems: l.Elements, tail: l.Tail}
	}
	return listCursor{tail: t}
}

func (c *listCursor) next() (head Term, hasHead bool, finalTail Term) {
	for {
		if c.idx < len(c.elems) {
			v := c.elems[c.idx]
			c.idx++
			return v, true, nil
		}
		nextList, ok := c.tail.(List)
		if !ok {
			return nil, false, c.tail
		}
		c.elems = nextList.Elements
		c.idx = 0
		c.tail = nextList.Tail
	}
}

func listEqual(a, b List) bool {
	ca := newListCursor(a)
	cb := newListCursor(b)
	for {
		ha, oka, ta := ca.next()
		hb, okb, tb := cb.next()
		if oka != okb {
			return false
		}
		if !oka {
			return TermEqual(ta, tb)
		}
		if !TermEqual(ha, hb) {
			return false
		}
	}
}

func mapEqual(a, b Map) bool {
	if len(a) == 0 {
		return true
	}
	if len(a) <= 8 {
		used := uint8(0)
		for i := range a {
			matched := false
			for j := range b {
				if used&(1<<j) != 0 {
					continue
				}
				if TermEqual(a[i].Key, b[j].Key) {
					if !TermEqual(a[i].Value, b[j].Value) {
						return false
					}
					used |= 1 << j
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		return true
	}

	buckets := make(map[uint64][]int, len(b))
	for j := range b {
		h := termHash(b[j].Key)
		buckets[h] = append(buckets[h], j)
	}
	used := make([]bool, len(b))
	for i := range a {
		h := termHash(a[i].Key)
		candidates := buckets[h]
		matched := false
		for _, j := range candidates {
			if used[j] {
				continue
			}
			if TermEqual(a[i].Key, b[j].Key) {
				if !TermEqual(a[i].Value, b[j].Value) {
					return false
				}
				used[j] = true
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func validateUniqueMapKeys(pairs []MapPair) error {
	if len(pairs) <= 1 {
		return nil
	}
	if len(pairs) <= 8 {
		for i := 1; i < len(pairs); i++ {
			for j := 0; j < i; j++ {
				if TermEqual(pairs[i].Key, pairs[j].Key) {
					return ErrDuplicateMapKey
				}
			}
		}
		return nil
	}
	buckets := make(map[uint64][]int, len(pairs))
	for i := range pairs {
		h := termHash(pairs[i].Key)
		for _, prev := range buckets[h] {
			if TermEqual(pairs[i].Key, pairs[prev].Key) {
				return ErrDuplicateMapKey
			}
		}
		buckets[h] = append(buckets[h], i)
	}
	return nil
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func mix64(h, k uint64) uint64 {
	h ^= k
	h *= fnvPrime64
	return h ^ (h >> 32)
}

func hashBytes(h uint64, b []byte) uint64 {
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

func hashString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

func termHash(t Term) uint64 {
	if ni, ok := normalizeInt(t); ok {
		if ni.big == nil {
			return mix64(fnvOffset64^0x01, uint64(ni.small))
		}
		h := mix64(fnvOffset64^0x02, uint64(ni.big.Sign()))
		for _, w := range ni.big.Bits() {
			h = mix64(h, uint64(w))
		}
		return h
	}

	switch v := t.(type) {
	case Atom:
		return hashString(mix64(fnvOffset64, 0x03), string(v))
	case float64:
		return mix64(fnvOffset64^0x04, math.Float64bits(v))
	case Binary:
		return hashBytes(mix64(fnvOffset64^0x05, uint64(len(v))), v)
	case BitBinary:
		h := mix64(fnvOffset64^0x06, uint64(v.LastBits))
		if len(v.Bytes) > 0 && v.LastBits >= 1 && v.LastBits <= 7 {
			h = hashBytes(h, v.Bytes[:len(v.Bytes)-1])
			mask := byte(0xFF) << (8 - v.LastBits)
			h = mix64(h, uint64(v.Bytes[len(v.Bytes)-1]&mask))
		}
		return h
	case Nil:
		return mix64(fnvOffset64, 0x07)
	case Tuple:
		h := mix64(fnvOffset64^0x08, uint64(len(v)))
		for i := range v {
			h = mix64(h, termHash(v[i]))
		}
		return h
	case List:
		if listIsNil(v) {
			return mix64(fnvOffset64, 0x07)
		}
		h := mix64(fnvOffset64, 0x09)
		cur := newListCursor(v)
		for {
			elem, ok, tail := cur.next()
			if !ok {
				return mix64(h, termHash(tail))
			}
			h = mix64(h, termHash(elem))
		}
	case Map:
		// Order-independent combination over key/value pairs.
		var sum, xor uint64
		for i := range v {
			ph := mix64(termHash(v[i].Key), termHash(v[i].Value))
			sum += ph
			xor ^= ph
		}
		return mix64(mix64(fnvOffset64^0x0A, uint64(len(v))), mix64(sum, xor))
	case PID:
		h := hashString(mix64(fnvOffset64, 0x0B), string(v.Node))
		h = mix64(h, (uint64(v.ID)<<32)|uint64(v.Serial))
		return mix64(h, uint64(v.Creation))
	case Port:
		h := hashString(mix64(fnvOffset64, 0x0C), string(v.Node))
		h = mix64(h, v.ID)
		return mix64(h, uint64(v.Creation))
	case Ref:
		h := hashString(mix64(fnvOffset64, 0x0D), string(v.Node))
		h = mix64(h, uint64(v.Creation))
		words := v.canonicalWords()
		var buf [20]byte
		for i, w := range words {
			binary.BigEndian.PutUint32(buf[i*4:], w)
		}
		return hashBytes(h, buf[:])
	case Export:
		h := hashString(mix64(fnvOffset64, 0x0E), string(v.Module))
		h = hashString(h, string(v.Function))
		return mix64(h, uint64(v.Arity))
	case Fun:
		h := hashString(mix64(fnvOffset64, 0x0F), string(v.Module))
		h = hashBytes(h, v.Uniq[:])
		h = mix64(h, (uint64(v.Arity)<<32)|uint64(v.Index))
		h = mix64(h, (uint64(uint32(v.OldIndex))<<32)|uint64(uint32(v.OldUniq)))
		for i := range v.FreeVars {
			h = mix64(h, termHash(v.FreeVars[i]))
		}
		return h
	default:
		return 0
	}
}
