package etf

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
)

const (
	termBaseChargeBytes = 24
	termSlotChargeBytes = 16
	mapPairChargeBytes  = 32
	maxOrdinaryRefWord0 = 1 << 18
)

// Decode parses a standalone ETF binary (prefixed with Version 131) using DefaultLimits.
// All returned slices are newly allocated and never alias data.
func Decode(data []byte) (Term, error) {
	return DecodeWithLimits(data, DefaultLimits())
}

// DecodeWithLimits parses a standalone ETF binary using explicit resource limits.
func DecodeWithLimits(data []byte, limits Limits) (Term, error) {
	limits = limits.normalize()
	if len(data) > limits.MaxFrameBytes {
		return nil, fmt.Errorf("%w: input %d bytes exceeds max %d", ErrLimitExceeded, len(data), limits.MaxFrameBytes)
	}
	if len(data) < 2 {
		return nil, fmt.Errorf("%w: truncated standalone ETF", ErrMalformedETF)
	}
	if data[0] != Version {
		return nil, fmt.Errorf("%w: expected version %d, got %d", ErrMalformedETF, Version, data[0])
	}

	d := DistDecoder{
		limits:      limits,
		allowLegacy: true,
	}
	if data[1] == tagCompressed {
		return d.decodeCompressed(data[2:])
	}
	term, rest, err := d.decodeTerm(data[1:], 0)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes after standalone term", ErrMalformedETF, len(rest))
	}
	return term, nil
}

// DistDecoder decodes one or more terms from a single distribution message
// (e.g. control tuple followed by optional payload) under shared resource limits.
type DistDecoder struct {
	atoms       []Atom
	passThrough bool
	allowLegacy bool
	limits      Limits
	terms       int
	usedBytes   int64
}

// NewDistDecoder creates a stateful decoder for a distribution message body.
// When passThrough is false, terms do not carry a leading 131 byte and may reference atoms.
// When passThrough is true, each term starts with 131 and atoms is ignored.
func NewDistDecoder(atoms []Atom, passThrough bool, limits Limits) DistDecoder {
	if passThrough {
		atoms = nil
	}
	return DistDecoder{
		atoms:       atoms,
		passThrough: passThrough,
		allowLegacy: false,
		limits:      limits.normalize(),
	}
}

// UsedBytes returns the cumulative Go heap charge attributed to terms decoded so far.
func (d *DistDecoder) UsedBytes() int64 {
	return d.usedBytes
}

// Terms returns the cumulative number of term nodes decoded so far.
func (d *DistDecoder) Terms() int {
	return d.terms
}

// DecodeNext decodes the next term from data and returns the unconsumed slice.
func (d *DistDecoder) DecodeNext(data []byte) (Term, []byte, error) {
	if len(data) > d.limits.MaxFrameBytes {
		return nil, nil, fmt.Errorf("%w: frame %d bytes exceeds max %d", ErrLimitExceeded, len(data), d.limits.MaxFrameBytes)
	}
	if d.passThrough {
		if len(data) < 2 {
			return nil, nil, fmt.Errorf("%w: truncated pass-through ETF", ErrMalformedETF)
		}
		if data[0] != Version {
			return nil, nil, fmt.Errorf("%w: expected pass-through version %d, got %d", ErrMalformedETF, Version, data[0])
		}
		data = data[1:]
	}
	return d.decodeTerm(data, 0)
}

func (d *DistDecoder) charge(terms int, bytes int64) error {
	if terms > d.limits.MaxTerms-d.terms {
		return fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, d.limits.MaxTerms)
	}
	if bytes > d.limits.MaxDecodedBytes-d.usedBytes {
		return fmt.Errorf("%w: decoded memory exceeds %d bytes", ErrLimitExceeded, d.limits.MaxDecodedBytes)
	}
	d.terms += terms
	d.usedBytes += bytes
	return nil
}

func (d *DistDecoder) decodeCompressed(data []byte) (Term, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("%w: truncated compressed header", ErrMalformedETF)
	}
	declared := binary.BigEndian.Uint32(data[:4])
	if declared == 0 {
		return nil, fmt.Errorf("%w: zero uncompressed size", ErrMalformedETF)
	}
	if uint64(declared) > uint64(d.limits.MaxFrameBytes) || int64(declared) > d.limits.MaxDecodedBytes {
		return nil, fmt.Errorf("%w: declared uncompressed size %d exceeds limits", ErrLimitExceeded, declared)
	}
	if err := d.charge(0, int64(declared)); err != nil {
		return nil, err
	}

	br := bytes.NewReader(data[4:])
	zr, err := zlib.NewReader(br)
	if err != nil {
		return nil, fmt.Errorf("%w: zlib header: %v", ErrMalformedETF, err)
	}
	expanded := make([]byte, int(declared)+1)
	n, readErr := io.ReadFull(zr, expanded)
	if readErr == nil {
		_ = zr.Close()
		return nil, fmt.Errorf("%w: compressed payload exceeds declared %d bytes", ErrLimitExceeded, declared)
	}
	if readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		_ = zr.Close()
		return nil, fmt.Errorf("%w: zlib read: %v", ErrMalformedETF, readErr)
	}
	if closeErr := zr.Close(); closeErr != nil {
		return nil, fmt.Errorf("%w: zlib close: %v", ErrMalformedETF, closeErr)
	}
	if n != int(declared) {
		return nil, fmt.Errorf("%w: uncompressed size mismatch: got %d, declared %d", ErrMalformedETF, n, declared)
	}
	if br.Len() != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes after zlib stream", ErrMalformedETF, br.Len())
	}

	raw := expanded[:n]
	if len(raw) > 0 && raw[0] == tagCompressed {
		return nil, fmt.Errorf("%w: nested compressed ETF", ErrMalformedETF)
	}
	term, rest, err := d.decodeTerm(raw, 0)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes inside compressed ETF", ErrMalformedETF, len(rest))
	}
	return term, nil
}

func (d *DistDecoder) decodeAtom(data []byte) (Atom, []byte, error) {
	if len(data) == 0 {
		return "", nil, fmt.Errorf("%w: truncated atom tag", ErrMalformedETF)
	}
	tag := data[0]
	data = data[1:]
	switch tag {
	case tagAtomCacheRef:
		if d.atoms == nil {
			return "", nil, fmt.Errorf("%w: unexpected ATOM_CACHE_REF without cache table", ErrMalformedETF)
		}
		if len(data) < 1 {
			return "", nil, fmt.Errorf("%w: truncated ATOM_CACHE_REF", ErrMalformedETF)
		}
		idx := int(data[0])
		if idx >= len(d.atoms) {
			return "", nil, fmt.Errorf("%w: atom cache ref %d >= %d", ErrMalformedETF, idx, len(d.atoms))
		}
		return d.atoms[idx], data[1:], nil

	case tagSmallAtomUTF8Ext:
		if len(data) < 1 {
			return "", nil, fmt.Errorf("%w: truncated SMALL_ATOM_UTF8_EXT", ErrMalformedETF)
		}
		n := int(data[0])
		data = data[1:]
		if len(data) < n {
			return "", nil, fmt.Errorf("%w: truncated SMALL_ATOM_UTF8_EXT body", ErrMalformedETF)
		}
		if err := d.charge(0, int64(n)); err != nil {
			return "", nil, err
		}
		a := Atom(string(data[:n]))
		if err := ValidateAtom(a); err != nil {
			return "", nil, fmt.Errorf("%w: %v", ErrMalformedETF, err)
		}
		return a, data[n:], nil

	case tagAtomUTF8Ext:
		if len(data) < 2 {
			return "", nil, fmt.Errorf("%w: truncated ATOM_UTF8_EXT", ErrMalformedETF)
		}
		n := int(binary.BigEndian.Uint16(data[:2]))
		data = data[2:]
		if n > MaxAtomUTF8Bytes {
			return "", nil, fmt.Errorf("%w: ATOM_UTF8_EXT length %d > %d", ErrMalformedETF, n, MaxAtomUTF8Bytes)
		}
		if len(data) < n {
			return "", nil, fmt.Errorf("%w: truncated ATOM_UTF8_EXT body", ErrMalformedETF)
		}
		if err := d.charge(0, int64(n)); err != nil {
			return "", nil, err
		}
		a := Atom(string(data[:n]))
		if err := ValidateAtom(a); err != nil {
			return "", nil, fmt.Errorf("%w: %v", ErrMalformedETF, err)
		}
		return a, data[n:], nil

	case tagAtomExt, tagSmallAtomExt:
		if !d.allowLegacy {
			return "", nil, fmt.Errorf("%w: legacy Latin-1 atom tag %d on v6 distribution", ErrUnsupportedTag, tag)
		}
		var n int
		if tag == tagSmallAtomExt {
			if len(data) < 1 {
				return "", nil, fmt.Errorf("%w: truncated SMALL_ATOM_EXT", ErrMalformedETF)
			}
			n = int(data[0])
			data = data[1:]
		} else {
			if len(data) < 2 {
				return "", nil, fmt.Errorf("%w: truncated ATOM_EXT", ErrMalformedETF)
			}
			n = int(binary.BigEndian.Uint16(data[:2]))
			data = data[2:]
			if n > MaxAtomChars {
				return "", nil, fmt.Errorf("%w: ATOM_EXT length %d > %d", ErrMalformedETF, n, MaxAtomChars)
			}
		}
		if len(data) < n {
			return "", nil, fmt.Errorf("%w: truncated Latin-1 atom body", ErrMalformedETF)
		}
		if err := d.charge(0, int64(n*2)); err != nil {
			return "", nil, err
		}
		a := latin1ToAtom(data[:n])
		return a, data[n:], nil

	default:
		return "", nil, fmt.Errorf("%w: expected atom tag, got %d", ErrMalformedETF, tag)
	}
}

func latin1ToAtom(raw []byte) Atom {
	ascii := true
	for _, b := range raw {
		if b >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return Atom(string(raw))
	}
	var sb strings.Builder
	sb.Grow(len(raw) * 2)
	for _, b := range raw {
		sb.WriteRune(rune(b))
	}
	return Atom(sb.String())
}

func (d *DistDecoder) decodePID(data []byte) (PID, []byte, error) {
	if len(data) == 0 {
		return PID{}, nil, fmt.Errorf("%w: truncated PID tag", ErrMalformedETF)
	}
	tag := data[0]
	switch tag {
	case tagNewPIDExt:
		node, rest, err := d.decodeAtom(data[1:])
		if err != nil {
			return PID{}, nil, err
		}
		if node == "" {
			return PID{}, nil, fmt.Errorf("%w: empty node in NEW_PID_EXT", ErrMalformedETF)
		}
		if len(rest) < 12 {
			return PID{}, nil, fmt.Errorf("%w: truncated NEW_PID_EXT fields", ErrMalformedETF)
		}
		p := PID{
			Node:     node,
			ID:       binary.BigEndian.Uint32(rest[0:4]),
			Serial:   binary.BigEndian.Uint32(rest[4:8]),
			Creation: binary.BigEndian.Uint32(rest[8:12]),
		}
		return p, rest[12:], nil

	case tagPIDExt:
		if !d.allowLegacy {
			return PID{}, nil, fmt.Errorf("%w: legacy PID_EXT on v6 distribution", ErrUnsupportedTag)
		}
		node, rest, err := d.decodeAtom(data[1:])
		if err != nil {
			return PID{}, nil, err
		}
		if node == "" {
			return PID{}, nil, fmt.Errorf("%w: empty node in PID_EXT", ErrMalformedETF)
		}
		if len(rest) < 9 {
			return PID{}, nil, fmt.Errorf("%w: truncated PID_EXT fields", ErrMalformedETF)
		}
		cre := rest[8]
		if cre > 3 {
			return PID{}, nil, fmt.Errorf("%w: PID_EXT creation %d > 3", ErrMalformedETF, cre)
		}
		p := PID{
			Node:     node,
			ID:       binary.BigEndian.Uint32(rest[0:4]),
			Serial:   binary.BigEndian.Uint32(rest[4:8]),
			Creation: uint32(cre),
		}
		return p, rest[9:], nil

	default:
		return PID{}, nil, fmt.Errorf("%w: expected PID tag, got %d", ErrMalformedETF, tag)
	}
}

func (d *DistDecoder) decodeSmallOrInt32(data []byte) (int32, []byte, error) {
	if len(data) == 0 {
		return 0, nil, fmt.Errorf("%w: truncated integer field", ErrMalformedETF)
	}
	switch data[0] {
	case tagSmallIntegerExt:
		if len(data) < 2 {
			return 0, nil, fmt.Errorf("%w: truncated SMALL_INTEGER_EXT", ErrMalformedETF)
		}
		return int32(data[1]), data[2:], nil
	case tagIntegerExt:
		if len(data) < 5 {
			return 0, nil, fmt.Errorf("%w: truncated INTEGER_EXT", ErrMalformedETF)
		}
		return int32(binary.BigEndian.Uint32(data[1:5])), data[5:], nil
	default:
		return 0, nil, fmt.Errorf("%w: expected integer tag, got %d", ErrMalformedETF, data[0])
	}
}

func (d *DistDecoder) decodeTerm(data []byte, depth int) (Term, []byte, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("%w: unexpected EOF", ErrMalformedETF)
	}
	if depth > d.limits.MaxDepth {
		return nil, nil, fmt.Errorf("%w: nesting depth exceeds %d", ErrLimitExceeded, d.limits.MaxDepth)
	}
	if err := d.charge(1, termBaseChargeBytes); err != nil {
		return nil, nil, err
	}

	tag := data[0]
	switch tag {
	case tagAtomCacheRef, tagSmallAtomUTF8Ext, tagAtomUTF8Ext, tagAtomExt, tagSmallAtomExt:
		return d.decodeAtom(data)

	case tagSmallIntegerExt:
		if len(data) < 2 {
			return nil, nil, fmt.Errorf("%w: truncated SMALL_INTEGER_EXT", ErrMalformedETF)
		}
		return int64(data[1]), data[2:], nil

	case tagIntegerExt:
		if len(data) < 5 {
			return nil, nil, fmt.Errorf("%w: truncated INTEGER_EXT", ErrMalformedETF)
		}
		v := int32(binary.BigEndian.Uint32(data[1:5]))
		return int64(v), data[5:], nil

	case tagSmallBigExt, tagLargeBigExt:
		var n int
		var rest []byte
		if tag == tagSmallBigExt {
			if len(data) < 3 {
				return nil, nil, fmt.Errorf("%w: truncated SMALL_BIG_EXT", ErrMalformedETF)
			}
			n = int(data[1])
			rest = data[2:]
		} else {
			if len(data) < 6 {
				return nil, nil, fmt.Errorf("%w: truncated LARGE_BIG_EXT", ErrMalformedETF)
			}
			u := binary.BigEndian.Uint32(data[1:5])
			if uint64(u) > uint64(d.limits.MaxBigIntBytes) {
				return nil, nil, fmt.Errorf("%w: LARGE_BIG_EXT %d bytes exceeds %d", ErrLimitExceeded, u, d.limits.MaxBigIntBytes)
			}
			n = int(u)
			rest = data[5:]
		}
		if n > d.limits.MaxBigIntBytes {
			return nil, nil, fmt.Errorf("%w: bignum %d bytes exceeds %d", ErrLimitExceeded, n, d.limits.MaxBigIntBytes)
		}
		if len(rest) < 1+n {
			return nil, nil, fmt.Errorf("%w: truncated bignum body", ErrMalformedETF)
		}
		sign := rest[0]
		if sign > 1 {
			return nil, nil, fmt.Errorf("%w: invalid bignum sign %d", ErrMalformedETF, sign)
		}
		digits := rest[1 : 1+n]
		after := rest[1+n:]
		// Strip trailing zero most-significant bytes (little-endian layout).
		for len(digits) > 0 && digits[len(digits)-1] == 0 {
			digits = digits[:len(digits)-1]
		}
		if len(digits) == 0 {
			return int64(0), after, nil
		}
		if len(digits) <= 8 {
			var mag uint64
			for i := len(digits) - 1; i >= 0; i-- {
				mag = (mag << 8) | uint64(digits[i])
			}
			if sign == 0 && mag <= math.MaxInt64 {
				return int64(mag), after, nil
			}
			if sign == 1 && mag <= uint64(math.MaxInt64)+1 {
				if mag == uint64(math.MaxInt64)+1 {
					return int64(math.MinInt64), after, nil
				}
				return -int64(mag), after, nil
			}
		}
		if err := d.charge(0, int64(len(digits))*2+32); err != nil {
			return nil, nil, err
		}
		be := make([]byte, len(digits))
		for i := range digits {
			be[len(digits)-1-i] = digits[i]
		}
		bi := new(big.Int).SetBytes(be)
		if sign == 1 {
			bi.Neg(bi)
		}
		return bi, after, nil

	case tagNewFloatExt:
		if len(data) < 9 {
			return nil, nil, fmt.Errorf("%w: truncated NEW_FLOAT_EXT", ErrMalformedETF)
		}
		bits := binary.BigEndian.Uint64(data[1:9])
		f := math.Float64frombits(bits)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, nil, fmt.Errorf("%w: non-finite NEW_FLOAT_EXT", ErrMalformedETF)
		}
		return f, data[9:], nil

	case tagFloatExt:
		if !d.allowLegacy {
			return nil, nil, fmt.Errorf("%w: legacy FLOAT_EXT on v6 distribution", ErrUnsupportedTag)
		}
		if len(data) < 32 {
			return nil, nil, fmt.Errorf("%w: truncated FLOAT_EXT", ErrMalformedETF)
		}
		raw := data[1:32]
		end := bytes.IndexByte(raw, 0)
		if end < 0 {
			end = len(raw)
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(string(raw[:end])), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, nil, fmt.Errorf("%w: invalid FLOAT_EXT", ErrMalformedETF)
		}
		return f, data[32:], nil

	case tagBinaryExt:
		if len(data) < 5 {
			return nil, nil, fmt.Errorf("%w: truncated BINARY_EXT", ErrMalformedETF)
		}
		u := binary.BigEndian.Uint32(data[1:5])
		rest := data[5:]
		if uint64(u) > uint64(d.limits.MaxBinaryBytes) {
			return nil, nil, fmt.Errorf("%w: binary %d bytes exceeds %d", ErrLimitExceeded, u, d.limits.MaxBinaryBytes)
		}
		if uint64(u) > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("%w: truncated BINARY_EXT body (%d > %d)", ErrMalformedETF, u, len(rest))
		}
		n := int(u)
		if err := d.charge(0, int64(n)); err != nil {
			return nil, nil, err
		}
		out := make(Binary, n)
		copy(out, rest[:n])
		return out, rest[n:], nil

	case tagBitBinaryExt:
		if len(data) < 6 {
			return nil, nil, fmt.Errorf("%w: truncated BIT_BINARY_EXT", ErrMalformedETF)
		}
		u := binary.BigEndian.Uint32(data[1:5])
		bits := data[5]
		rest := data[6:]
		if u == 0 || bits < 1 || bits > 8 {
			return nil, nil, fmt.Errorf("%w: invalid BIT_BINARY_EXT len=%d bits=%d", ErrMalformedETF, u, bits)
		}
		if uint64(u) > uint64(d.limits.MaxBinaryBytes) {
			return nil, nil, fmt.Errorf("%w: bitstring %d bytes exceeds %d", ErrLimitExceeded, u, d.limits.MaxBinaryBytes)
		}
		if uint64(u) > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("%w: truncated BIT_BINARY_EXT body", ErrMalformedETF)
		}
		n := int(u)
		if err := d.charge(0, int64(n)); err != nil {
			return nil, nil, err
		}
		buf := make([]byte, n)
		copy(buf, rest[:n])
		if bits == 8 {
			return Binary(buf), rest[n:], nil
		}
		buf[n-1] &= byte(0xFF) << (8 - bits)
		return BitBinary{Bytes: buf, LastBits: bits}, rest[n:], nil

	case tagNilExt:
		return Nil{}, data[1:], nil

	case tagStringExt:
		if len(data) < 3 {
			return nil, nil, fmt.Errorf("%w: truncated STRING_EXT", ErrMalformedETF)
		}
		n := int(binary.BigEndian.Uint16(data[1:3]))
		rest := data[3:]
		if len(rest) < n {
			return nil, nil, fmt.Errorf("%w: truncated STRING_EXT body", ErrMalformedETF)
		}
		if n == 0 {
			return Nil{}, rest, nil
		}
		if err := d.charge(n, int64(n)*termBaseChargeBytes); err != nil {
			return nil, nil, err
		}
		elems := make([]Term, n)
		for i := 0; i < n; i++ {
			elems[i] = int64(rest[i])
		}
		return List{Elements: elems, Tail: Nil{}}, rest[n:], nil

	case tagSmallTupleExt, tagLargeTupleExt:
		var arity uint32
		var rest []byte
		if tag == tagSmallTupleExt {
			if len(data) < 2 {
				return nil, nil, fmt.Errorf("%w: truncated SMALL_TUPLE_EXT", ErrMalformedETF)
			}
			arity = uint32(data[1])
			rest = data[2:]
		} else {
			if len(data) < 5 {
				return nil, nil, fmt.Errorf("%w: truncated LARGE_TUPLE_EXT", ErrMalformedETF)
			}
			arity = binary.BigEndian.Uint32(data[1:5])
			rest = data[5:]
		}
		if uint64(arity) > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("%w: tuple arity %d exceeds remaining %d bytes", ErrMalformedETF, arity, len(rest))
		}
		n := int(arity)
		if n > d.limits.MaxTerms-d.terms {
			return nil, nil, fmt.Errorf("%w: tuple arity %d exceeds term limit", ErrLimitExceeded, n)
		}
		if err := d.charge(0, int64(n)*termSlotChargeBytes); err != nil {
			return nil, nil, err
		}
		tup := make(Tuple, n)
		var err error
		for i := 0; i < n; i++ {
			tup[i], rest, err = d.decodeTerm(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
		}
		return tup, rest, nil

	case tagListExt:
		if len(data) < 5 {
			return nil, nil, fmt.Errorf("%w: truncated LIST_EXT", ErrMalformedETF)
		}
		length := binary.BigEndian.Uint32(data[1:5])
		rest := data[5:]
		// Need at least length element bytes + 1 tail byte.
		if uint64(length)+1 > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("%w: list length %d exceeds remaining %d bytes", ErrMalformedETF, length, len(rest))
		}
		n := int(length)
		if n == 0 {
			return d.decodeTerm(rest, depth+1)
		}
		if n > d.limits.MaxTerms-d.terms {
			return nil, nil, fmt.Errorf("%w: list length %d exceeds term limit", ErrLimitExceeded, n)
		}
		if err := d.charge(0, int64(n)*termSlotChargeBytes); err != nil {
			return nil, nil, err
		}
		elems := make([]Term, n)
		var err error
		for i := 0; i < n; i++ {
			elems[i], rest, err = d.decodeTerm(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
		}
		tail, rest, err := d.decodeTerm(rest, depth+1)
		if err != nil {
			return nil, nil, err
		}
		if tailList, ok := tail.(List); ok {
			merged := make([]Term, 0, len(elems)+len(tailList.Elements))
			if err := d.charge(0, int64(len(elems)+len(tailList.Elements))*termSlotChargeBytes); err != nil {
				return nil, nil, err
			}
			merged = append(merged, elems...)
			merged = append(merged, tailList.Elements...)
			return List{Elements: merged, Tail: tailList.Tail}, rest, nil
		}
		return List{Elements: elems, Tail: tail}, rest, nil

	case tagMapExt:
		if len(data) < 5 {
			return nil, nil, fmt.Errorf("%w: truncated MAP_EXT", ErrMalformedETF)
		}
		arity := binary.BigEndian.Uint32(data[1:5])
		rest := data[5:]
		if uint64(arity)*2 > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("%w: map arity %d exceeds remaining %d bytes", ErrMalformedETF, arity, len(rest))
		}
		n := int(arity)
		if n*2 > d.limits.MaxTerms-d.terms {
			return nil, nil, fmt.Errorf("%w: map arity %d exceeds term limit", ErrLimitExceeded, n)
		}
		if err := d.charge(0, int64(n)*mapPairChargeBytes); err != nil {
			return nil, nil, err
		}
		pairs := make(Map, n)
		var err error
		for i := 0; i < n; i++ {
			pairs[i].Key, rest, err = d.decodeTerm(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
			pairs[i].Value, rest, err = d.decodeTerm(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
		}
		if err := validateUniqueMapKeys(pairs); err != nil {
			return nil, nil, err
		}
		return pairs, rest, nil

	case tagNewPIDExt, tagPIDExt:
		return d.decodePID(data)

	case tagV4PortExt, tagNewPortExt, tagPortExt:
		if tag == tagPortExt && !d.allowLegacy {
			return nil, nil, fmt.Errorf("%w: legacy PORT_EXT on v6 distribution", ErrUnsupportedTag)
		}
		node, rest, err := d.decodeAtom(data[1:])
		if err != nil {
			return nil, nil, err
		}
		if node == "" {
			return nil, nil, fmt.Errorf("%w: empty node in Port", ErrMalformedETF)
		}
		switch tag {
		case tagV4PortExt:
			if len(rest) < 12 {
				return nil, nil, fmt.Errorf("%w: truncated V4_PORT_EXT", ErrMalformedETF)
			}
			return Port{
				Node:     node,
				ID:       binary.BigEndian.Uint64(rest[0:8]),
				Creation: binary.BigEndian.Uint32(rest[8:12]),
			}, rest[12:], nil
		case tagNewPortExt:
			if len(rest) < 8 {
				return nil, nil, fmt.Errorf("%w: truncated NEW_PORT_EXT", ErrMalformedETF)
			}
			return Port{
				Node:     node,
				ID:       uint64(binary.BigEndian.Uint32(rest[0:4])),
				Creation: binary.BigEndian.Uint32(rest[4:8]),
			}, rest[8:], nil
		default:
			if len(rest) < 5 {
				return nil, nil, fmt.Errorf("%w: truncated PORT_EXT", ErrMalformedETF)
			}
			cre := rest[4]
			if cre > 3 {
				return nil, nil, fmt.Errorf("%w: PORT_EXT creation %d > 3", ErrMalformedETF, cre)
			}
			return Port{
				Node:     node,
				ID:       uint64(binary.BigEndian.Uint32(rest[0:4])),
				Creation: uint32(cre),
			}, rest[5:], nil
		}

	case tagNewerReferenceExt, tagNewReferenceExt, tagReferenceExt:
		return d.decodeRef(data)

	case tagExportExt:
		mod, rest, err := d.decodeAtom(data[1:])
		if err != nil {
			return nil, nil, err
		}
		fn, rest, err := d.decodeAtom(rest)
		if err != nil {
			return nil, nil, err
		}
		if err := d.charge(1, 0); err != nil {
			return nil, nil, err
		}
		arity, rest, err := d.decodeSmallOrInt32(rest)
		if err != nil {
			return nil, nil, err
		}
		if arity < 0 || arity > 255 {
			return nil, nil, fmt.Errorf("%w: EXPORT_EXT arity %d out of [0,255]", ErrMalformedETF, arity)
		}
		return Export{Module: mod, Function: fn, Arity: uint8(arity)}, rest, nil

	case tagNewFunExt:
		// Tag(1) + Size(4) + Arity(1) + Uniq(16) + Index(4) + NumFree(4) = 30 bytes minimum
		if len(data) < 30 {
			return nil, nil, fmt.Errorf("%w: truncated NEW_FUN_EXT header", ErrMalformedETF)
		}
		declaredSize := binary.BigEndian.Uint32(data[1:5])
		if declaredSize < 29 {
			return nil, nil, fmt.Errorf("%w: invalid NEW_FUN_EXT size %d", ErrMalformedETF, declaredSize)
		}
		var fn Fun
		fn.Arity = data[5]
		copy(fn.Uniq[:], data[6:22])
		fn.Index = binary.BigEndian.Uint32(data[22:26])
		numFree := binary.BigEndian.Uint32(data[26:30])
		if uint64(numFree) > uint64(d.limits.MaxFunFreeVars) {
			return nil, nil, fmt.Errorf("%w: NEW_FUN_EXT numFree %d exceeds %d", ErrLimitExceeded, numFree, d.limits.MaxFunFreeVars)
		}
		rest := data[30:]
		if uint64(numFree) > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("%w: NEW_FUN_EXT numFree %d exceeds remaining %d bytes", ErrMalformedETF, numFree, len(rest))
		}
		var err error
		fn.Module, rest, err = d.decodeAtom(rest)
		if err != nil {
			return nil, nil, err
		}
		if err := d.charge(3, 0); err != nil {
			return nil, nil, err
		}
		fn.OldIndex, rest, err = d.decodeSmallOrInt32(rest)
		if err != nil {
			return nil, nil, err
		}
		if fn.OldIndex < 0 {
			return nil, nil, fmt.Errorf("%w: negative NEW_FUN_EXT OldIndex %d", ErrMalformedETF, fn.OldIndex)
		}
		fn.OldUniq, rest, err = d.decodeSmallOrInt32(rest)
		if err != nil {
			return nil, nil, err
		}
		if fn.OldUniq < 0 {
			return nil, nil, fmt.Errorf("%w: negative NEW_FUN_EXT OldUniq %d", ErrMalformedETF, fn.OldUniq)
		}
		fn.PID, rest, err = d.decodePID(rest)
		if err != nil {
			return nil, nil, err
		}
		nf := int(numFree)
		if nf > 0 {
			if err := d.charge(0, int64(nf)*termSlotChargeBytes); err != nil {
				return nil, nil, err
			}
			fn.FreeVars = make([]Term, nf)
			for i := 0; i < nf; i++ {
				fn.FreeVars[i], rest, err = d.decodeTerm(rest, depth+1)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		return fn, rest, nil

	case tagFunExt, tagRecordExt, tagLocalExt:
		return nil, nil, fmt.Errorf("%w: tag %d", ErrUnsupportedTag, tag)

	default:
		return nil, nil, fmt.Errorf("%w: unknown tag %d", ErrUnsupportedTag, tag)
	}
}

func (d *DistDecoder) decodeRef(data []byte) (Ref, []byte, error) {
	tag := data[0]
	switch tag {
	case tagNewerReferenceExt:
		if len(data) < 3 {
			return Ref{}, nil, fmt.Errorf("%w: truncated NEWER_REFERENCE_EXT", ErrMalformedETF)
		}
		words := int(binary.BigEndian.Uint16(data[1:3]))
		if words < 1 || words > MaxRefWords {
			return Ref{}, nil, fmt.Errorf("%w: NEWER_REFERENCE_EXT words=%d out of [1,%d]", ErrMalformedETF, words, MaxRefWords)
		}
		node, rest, err := d.decodeAtom(data[3:])
		if err != nil {
			return Ref{}, nil, err
		}
		if node == "" {
			return Ref{}, nil, fmt.Errorf("%w: empty node in NEWER_REFERENCE_EXT", ErrMalformedETF)
		}
		need := 4 + 4*words
		if len(rest) < need {
			return Ref{}, nil, fmt.Errorf("%w: truncated NEWER_REFERENCE_EXT body", ErrMalformedETF)
		}
		var r Ref
		r.Node = node
		r.Creation = binary.BigEndian.Uint32(rest[0:4])
		r.Len = uint8(words)
		for i := 0; i < words; i++ {
			r.ID[i] = binary.BigEndian.Uint32(rest[4+i*4 : 8+i*4])
		}
		return r, rest[need:], nil

	case tagNewReferenceExt:
		if !d.allowLegacy {
			return Ref{}, nil, fmt.Errorf("%w: legacy NEW_REFERENCE_EXT on v6 distribution", ErrUnsupportedTag)
		}
		if len(data) < 3 {
			return Ref{}, nil, fmt.Errorf("%w: truncated NEW_REFERENCE_EXT", ErrMalformedETF)
		}
		words := int(binary.BigEndian.Uint16(data[1:3]))
		if words < 1 || words > MaxRefWords {
			return Ref{}, nil, fmt.Errorf("%w: NEW_REFERENCE_EXT words=%d out of [1,%d]", ErrMalformedETF, words, MaxRefWords)
		}
		node, rest, err := d.decodeAtom(data[3:])
		if err != nil {
			return Ref{}, nil, err
		}
		if node == "" {
			return Ref{}, nil, fmt.Errorf("%w: empty node in NEW_REFERENCE_EXT", ErrMalformedETF)
		}
		need := 1 + 4*words
		if len(rest) < need {
			return Ref{}, nil, fmt.Errorf("%w: truncated NEW_REFERENCE_EXT body", ErrMalformedETF)
		}
		cre := rest[0]
		if cre > 3 {
			return Ref{}, nil, fmt.Errorf("%w: NEW_REFERENCE_EXT creation %d > 3", ErrMalformedETF, cre)
		}
		var r Ref
		r.Node = node
		r.Creation = uint32(cre)
		r.Len = uint8(words)
		for i := 0; i < words; i++ {
			r.ID[i] = binary.BigEndian.Uint32(rest[1+i*4 : 5+i*4])
		}
		if r.ID[0] >= maxOrdinaryRefWord0 {
			return Ref{}, nil, fmt.Errorf("%w: NEW_REFERENCE_EXT word0 exceeds 18 bits", ErrMalformedETF)
		}
		return r, rest[need:], nil

	default: // tagReferenceExt
		if !d.allowLegacy {
			return Ref{}, nil, fmt.Errorf("%w: legacy REFERENCE_EXT on v6 distribution", ErrUnsupportedTag)
		}
		node, rest, err := d.decodeAtom(data[1:])
		if err != nil {
			return Ref{}, nil, err
		}
		if node == "" {
			return Ref{}, nil, fmt.Errorf("%w: empty node in REFERENCE_EXT", ErrMalformedETF)
		}
		if len(rest) < 5 {
			return Ref{}, nil, fmt.Errorf("%w: truncated REFERENCE_EXT body", ErrMalformedETF)
		}
		r0 := binary.BigEndian.Uint32(rest[0:4])
		cre := rest[4]
		if cre > 3 || r0 >= maxOrdinaryRefWord0 {
			return Ref{}, nil, fmt.Errorf("%w: invalid REFERENCE_EXT", ErrMalformedETF)
		}
		var r Ref
		r.Node = node
		r.Creation = uint32(cre)
		r.Len = 1
		r.ID[0] = r0
		return r, rest[5:], nil
	}
}
