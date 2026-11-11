package etf

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
)

// Encode serializes term into standalone ETF bytes prefixed with Version (131).
func Encode(term Term) ([]byte, error) {
	return AppendEncodeWithLimits(nil, term, DefaultLimits())
}

// AppendEncode appends standalone ETF bytes (starting with Version 131) to dst.
func AppendEncode(dst []byte, term Term) ([]byte, error) {
	return AppendEncodeWithLimits(dst, term, DefaultLimits())
}

// AppendEncodeWithLimits appends standalone ETF bytes while enforcing limits.
func AppendEncodeWithLimits(dst []byte, term Term, limits Limits) ([]byte, error) {
	limits = limits.normalize()
	start := len(dst)
	dst = append(dst, Version)
	enc := encoder{
		limits: limits,
		start:  start,
	}
	out, err := enc.appendTerm(dst, term, 0)
	if err != nil {
		return dst[:start], err
	}
	if len(out)-start > limits.MaxFrameBytes {
		return dst[:start], fmt.Errorf("%w: encoded size %d exceeds max %d", ErrLimitExceeded, len(out)-start, limits.MaxFrameBytes)
	}
	return out, nil
}

// AppendDistTerm appends a distribution ETF term (without a leading 131 byte, unless
// passThrough is true) using optional atom cache references.
func AppendDistTerm(dst []byte, term Term, atoms AtomEncoder, passThrough bool, limits Limits) ([]byte, error) {
	limits = limits.normalize()
	start := len(dst)
	if passThrough {
		dst = append(dst, Version)
		atoms = nil
	}
	enc := encoder{
		atoms:  atoms,
		limits: limits,
		start:  start,
	}
	out, err := enc.appendTerm(dst, term, 0)
	if err != nil {
		return dst[:start], err
	}
	if len(out)-start > limits.MaxFrameBytes {
		return dst[:start], fmt.Errorf("%w: encoded size %d exceeds max %d", ErrLimitExceeded, len(out)-start, limits.MaxFrameBytes)
	}
	return out, nil
}

type encoder struct {
	atoms  AtomEncoder
	limits Limits
	start  int
	terms  int
}

func (e *encoder) step(dstLen int) error {
	e.terms++
	if e.terms > e.limits.MaxTerms {
		return fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, e.limits.MaxTerms)
	}
	if dstLen-e.start > e.limits.MaxFrameBytes {
		return fmt.Errorf("%w: encoded size exceeds %d", ErrLimitExceeded, e.limits.MaxFrameBytes)
	}
	return nil
}

func (e *encoder) appendAtom(dst []byte, a Atom) ([]byte, error) {
	if err := ValidateAtom(a); err != nil {
		return dst, err
	}
	if e.atoms != nil {
		if idx, ok := e.atoms.AtomRef(a); ok {
			return append(dst, tagAtomCacheRef, idx), nil
		}
	}
	if len(a) <= 255 {
		dst = append(dst, tagSmallAtomUTF8Ext, byte(len(a)))
		return append(dst, a...), nil
	}
	dst = append(dst, tagAtomUTF8Ext, byte(len(a)>>8), byte(len(a)))
	return append(dst, a...), nil
}

func (e *encoder) appendSmallOrInt32(dst []byte, v int32) []byte {
	if v >= 0 && v <= 255 {
		return append(dst, tagSmallIntegerExt, byte(v))
	}
	var b [5]byte
	b[0] = tagIntegerExt
	binary.BigEndian.PutUint32(b[1:], uint32(v))
	return append(dst, b[:]...)
}

func (e *encoder) appendInt64(dst []byte, v int64) ([]byte, error) {
	if v >= 0 && v <= 255 {
		return append(dst, tagSmallIntegerExt, byte(v)), nil
	}
	if v >= math.MinInt32 && v <= math.MaxInt32 {
		var b [5]byte
		b[0] = tagIntegerExt
		binary.BigEndian.PutUint32(b[1:], uint32(int32(v)))
		return append(dst, b[:]...), nil
	}
	sign := byte(0)
	var mag uint64
	if v < 0 {
		sign = 1
		mag = uint64(^v) + 1
	} else {
		mag = uint64(v)
	}
	return e.appendUint64Big(dst, sign, mag)
}

func (e *encoder) appendUint64(dst []byte, v uint64) ([]byte, error) {
	if v <= 255 {
		return append(dst, tagSmallIntegerExt, byte(v)), nil
	}
	if v <= math.MaxInt32 {
		var b [5]byte
		b[0] = tagIntegerExt
		binary.BigEndian.PutUint32(b[1:], uint32(v))
		return append(dst, b[:]...), nil
	}
	return e.appendUint64Big(dst, 0, v)
}

func (e *encoder) appendUint64Big(dst []byte, sign byte, mag uint64) ([]byte, error) {
	var le [8]byte
	n := 0
	for mag != 0 {
		le[n] = byte(mag)
		mag >>= 8
		n++
	}
	if n > e.limits.MaxBigIntBytes {
		return dst, fmt.Errorf("%w: bignum %d bytes exceeds %d", ErrLimitExceeded, n, e.limits.MaxBigIntBytes)
	}
	dst = append(dst, tagSmallBigExt, byte(n), sign)
	return append(dst, le[:n]...), nil
}

func (e *encoder) appendBigInt(dst []byte, v *big.Int) ([]byte, error) {
	if v == nil {
		return dst, fmt.Errorf("%w: nil *big.Int", ErrInvalidTerm)
	}
	if v.IsInt64() {
		return e.appendInt64(dst, v.Int64())
	}
	if v.Sign() > 0 && v.IsUint64() {
		return e.appendUint64(dst, v.Uint64())
	}
	be := v.Bytes()
	n := len(be)
	if n > e.limits.MaxBigIntBytes {
		return dst, fmt.Errorf("%w: bignum %d bytes exceeds %d", ErrLimitExceeded, n, e.limits.MaxBigIntBytes)
	}
	sign := byte(0)
	if v.Sign() < 0 {
		sign = 1
	}
	if n <= 255 {
		dst = append(dst, tagSmallBigExt, byte(n), sign)
	} else {
		var hdr [6]byte
		hdr[0] = tagLargeBigExt
		binary.BigEndian.PutUint32(hdr[1:5], uint32(n))
		hdr[5] = sign
		dst = append(dst, hdr[:]...)
	}
	// ETF bignum stores least-significant byte first.
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, be[i])
	}
	return dst, nil
}

func (e *encoder) appendPID(dst []byte, p PID) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return dst, err
	}
	dst = append(dst, tagNewPIDExt)
	var err error
	dst, err = e.appendAtom(dst, p.Node)
	if err != nil {
		return dst, err
	}
	var tail [12]byte
	binary.BigEndian.PutUint32(tail[0:4], p.ID)
	binary.BigEndian.PutUint32(tail[4:8], p.Serial)
	binary.BigEndian.PutUint32(tail[8:12], p.Creation)
	return append(dst, tail[:]...), nil
}

func (e *encoder) appendTerm(dst []byte, term Term, depth int) ([]byte, error) {
	if term == nil {
		return dst, fmt.Errorf("%w: Go nil is not an ETF term (use etf.Nil{})", ErrInvalidTerm)
	}
	if depth > e.limits.MaxDepth {
		return dst, fmt.Errorf("%w: nesting depth exceeds %d", ErrLimitExceeded, e.limits.MaxDepth)
	}
	if err := e.step(len(dst)); err != nil {
		return dst, err
	}

	switch v := term.(type) {
	case Atom:
		return e.appendAtom(dst, v)
	case int:
		return e.appendInt64(dst, int64(v))
	case int8:
		return e.appendInt64(dst, int64(v))
	case int16:
		return e.appendInt64(dst, int64(v))
	case int32:
		return e.appendSmallOrInt32(dst, v), nil
	case int64:
		return e.appendInt64(dst, v)
	case uint:
		return e.appendUint64(dst, uint64(v))
	case uint8:
		return append(dst, tagSmallIntegerExt, v), nil
	case uint16:
		return e.appendUint64(dst, uint64(v))
	case uint32:
		return e.appendUint64(dst, uint64(v))
	case uint64:
		return e.appendUint64(dst, v)
	case *big.Int:
		return e.appendBigInt(dst, v)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return dst, fmt.Errorf("%w: non-finite float %v", ErrInvalidTerm, v)
		}
		var b [9]byte
		b[0] = tagNewFloatExt
		binary.BigEndian.PutUint64(b[1:], math.Float64bits(v))
		return append(dst, b[:]...), nil
	case Binary:
		if len(v) > e.limits.MaxBinaryBytes {
			return dst, fmt.Errorf("%w: binary %d bytes exceeds %d", ErrLimitExceeded, len(v), e.limits.MaxBinaryBytes)
		}
		if len(v)+5 > e.limits.MaxFrameBytes-(len(dst)-e.start) {
			return dst, fmt.Errorf("%w: binary exceeds frame budget", ErrLimitExceeded)
		}
		var hdr [5]byte
		hdr[0] = tagBinaryExt
		binary.BigEndian.PutUint32(hdr[1:], uint32(len(v)))
		dst = append(dst, hdr[:]...)
		return append(dst, v...), nil
	case BitBinary:
		if err := v.Validate(); err != nil {
			return dst, err
		}
		if len(v.Bytes) > e.limits.MaxBinaryBytes {
			return dst, fmt.Errorf("%w: bitstring %d bytes exceeds %d", ErrLimitExceeded, len(v.Bytes), e.limits.MaxBinaryBytes)
		}
		if len(v.Bytes)+6 > e.limits.MaxFrameBytes-(len(dst)-e.start) {
			return dst, fmt.Errorf("%w: bitstring exceeds frame budget", ErrLimitExceeded)
		}
		var hdr [6]byte
		hdr[0] = tagBitBinaryExt
		binary.BigEndian.PutUint32(hdr[1:5], uint32(len(v.Bytes)))
		hdr[5] = v.LastBits
		dst = append(dst, hdr[:]...)
		return append(dst, v.Bytes...), nil
	case Nil:
		return append(dst, tagNilExt), nil
	case Tuple:
		if uint64(len(v)) > math.MaxUint32 {
			return dst, fmt.Errorf("%w: tuple arity overflows uint32", ErrLimitExceeded)
		}
		if len(v) <= 255 {
			dst = append(dst, tagSmallTupleExt, byte(len(v)))
		} else {
			var hdr [5]byte
			hdr[0] = tagLargeTupleExt
			binary.BigEndian.PutUint32(hdr[1:], uint32(len(v)))
			dst = append(dst, hdr[:]...)
		}
		var err error
		for i := range v {
			dst, err = e.appendTerm(dst, v[i], depth+1)
			if err != nil {
				return dst, err
			}
		}
		return dst, nil
	case List:
		return e.appendList(dst, v, depth)
	case Map:
		if uint64(len(v)) > math.MaxUint32 {
			return dst, fmt.Errorf("%w: map arity overflows uint32", ErrLimitExceeded)
		}
		if err := validateUniqueMapKeys(v); err != nil {
			return dst, err
		}
		var hdr [5]byte
		hdr[0] = tagMapExt
		binary.BigEndian.PutUint32(hdr[1:], uint32(len(v)))
		dst = append(dst, hdr[:]...)
		var err error
		for i := range v {
			dst, err = e.appendTerm(dst, v[i].Key, depth+1)
			if err != nil {
				return dst, err
			}
			dst, err = e.appendTerm(dst, v[i].Value, depth+1)
			if err != nil {
				return dst, err
			}
		}
		return dst, nil
	case PID:
		return e.appendPID(dst, v)
	case Port:
		if err := v.Validate(); err != nil {
			return dst, err
		}
		dst = append(dst, tagV4PortExt)
		var err error
		dst, err = e.appendAtom(dst, v.Node)
		if err != nil {
			return dst, err
		}
		var tail [12]byte
		binary.BigEndian.PutUint64(tail[0:8], v.ID)
		binary.BigEndian.PutUint32(tail[8:12], v.Creation)
		return append(dst, tail[:]...), nil
	case Ref:
		if err := v.Validate(); err != nil {
			return dst, err
		}
		dst = append(dst, tagNewerReferenceExt, 0, v.Len)
		var err error
		dst, err = e.appendAtom(dst, v.Node)
		if err != nil {
			return dst, err
		}
		var cre [4]byte
		binary.BigEndian.PutUint32(cre[:], v.Creation)
		dst = append(dst, cre[:]...)
		for i := 0; i < int(v.Len); i++ {
			var word [4]byte
			binary.BigEndian.PutUint32(word[:], v.ID[i])
			dst = append(dst, word[:]...)
		}
		return dst, nil
	case Export:
		if err := v.Validate(); err != nil {
			return dst, err
		}
		dst = append(dst, tagExportExt)
		var err error
		dst, err = e.appendAtom(dst, v.Module)
		if err != nil {
			return dst, err
		}
		dst, err = e.appendAtom(dst, v.Function)
		if err != nil {
			return dst, err
		}
		return append(dst, tagSmallIntegerExt, v.Arity), nil
	case Fun:
		if err := v.Validate(); err != nil {
			return dst, err
		}
		if len(v.FreeVars) > e.limits.MaxFunFreeVars {
			return dst, fmt.Errorf("%w: fun free vars %d exceeds %d", ErrLimitExceeded, len(v.FreeVars), e.limits.MaxFunFreeVars)
		}
		dst = append(dst, tagNewFunExt)
		sizePos := len(dst)
		var fixed [29]byte // Size(4) + Arity(1) + Uniq(16) + Index(4) + NumFree(4)
		fixed[4] = v.Arity
		copy(fixed[5:21], v.Uniq[:])
		binary.BigEndian.PutUint32(fixed[21:25], v.Index)
		binary.BigEndian.PutUint32(fixed[25:29], uint32(len(v.FreeVars)))
		dst = append(dst, fixed[:]...)
		var err error
		dst, err = e.appendAtom(dst, v.Module)
		if err != nil {
			return dst, err
		}
		dst = e.appendSmallOrInt32(dst, v.OldIndex)
		dst = e.appendSmallOrInt32(dst, v.OldUniq)
		dst, err = e.appendPID(dst, v.PID)
		if err != nil {
			return dst, err
		}
		for i := range v.FreeVars {
			dst, err = e.appendTerm(dst, v.FreeVars[i], depth+1)
			if err != nil {
				return dst, err
			}
		}
		totalSize := len(dst) - sizePos
		if uint64(totalSize) > math.MaxUint32 {
			return dst, fmt.Errorf("%w: fun size overflows uint32", ErrLimitExceeded)
		}
		binary.BigEndian.PutUint32(dst[sizePos:sizePos+4], uint32(totalSize))
		return dst, nil
	default:
		return dst, fmt.Errorf("%w: %w: %T", ErrInvalidTerm, ErrUnsupportedTerm, term)
	}
}

func (e *encoder) appendList(dst []byte, l List, depth int) ([]byte, error) {
	if l.Tail == nil {
		return dst, fmt.Errorf("%w: List has nil Tail (use etf.Nil{})", ErrInvalidTerm)
	}
	if len(l.Elements) == 0 {
		if _, ok := l.Tail.(Nil); ok {
			return append(dst, tagNilExt), nil
		}
		return dst, fmt.Errorf("%w: List with empty Elements must have Nil{} Tail", ErrInvalidTerm)
	}

	// Count canonical cons length across any nested List tails and check if it is a byte charlist.
	total := 0
	allBytes := true
	cur := Term(l)
	var finalTail Term
	steps := 0
	for {
		lv, ok := cur.(List)
		if !ok {
			finalTail = cur
			break
		}
		steps++
		if steps > e.limits.MaxDepth && len(lv.Elements) == 0 {
			return dst, fmt.Errorf("%w: empty-element list tail chain exceeds %d", ErrLimitExceeded, e.limits.MaxDepth)
		}
		if lv.Tail == nil {
			return dst, fmt.Errorf("%w: nested List has nil Tail", ErrInvalidTerm)
		}
		for _, elem := range lv.Elements {
			total++
			if total > e.limits.MaxTerms {
				return dst, fmt.Errorf("%w: list length exceeds %d", ErrLimitExceeded, e.limits.MaxTerms)
			}
			if allBytes {
				if ni, ok := normalizeInt(elem); !ok || ni.big != nil || ni.small < 0 || ni.small > 255 {
					allBytes = false
				}
			}
		}
		cur = lv.Tail
	}
	if total == 0 {
		if _, ok := finalTail.(Nil); ok {
			return append(dst, tagNilExt), nil
		}
		return dst, fmt.Errorf("%w: List with empty Elements must have Nil{} Tail", ErrInvalidTerm)
	}
	if uint64(total) > math.MaxUint32 {
		return dst, fmt.Errorf("%w: list length overflows uint32", ErrLimitExceeded)
	}

	if _, isProper := finalTail.(Nil); isProper && allBytes && total <= 65535 {
		if err := e.chargeEncodeTerms(total); err != nil {
			return dst, err
		}
		if total+3 > e.limits.MaxFrameBytes-(len(dst)-e.start) {
			return dst, fmt.Errorf("%w: string list exceeds frame budget", ErrLimitExceeded)
		}
		dst = append(dst, tagStringExt, byte(total>>8), byte(total))
		iter := newListCursor(l)
		for {
			elem, ok, _ := iter.next()
			if !ok {
				break
			}
			ni, _ := normalizeInt(elem)
			dst = append(dst, byte(ni.small))
		}
		return dst, nil
	}

	var hdr [5]byte
	hdr[0] = tagListExt
	binary.BigEndian.PutUint32(hdr[1:], uint32(total))
	dst = append(dst, hdr[:]...)
	iter := newListCursor(l)
	var err error
	for {
		elem, ok, tail := iter.next()
		if !ok {
			return e.appendTerm(dst, tail, depth+1)
		}
		dst, err = e.appendTerm(dst, elem, depth+1)
		if err != nil {
			return dst, err
		}
	}
}

func (e *encoder) chargeEncodeTerms(n int) error {
	e.terms += n
	if e.terms > e.limits.MaxTerms {
		return fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, e.limits.MaxTerms)
	}
	return nil
}

// EstimateSizeWithLimits validates term against limits and computes its standalone ETF encoded
// byte size without allocating buffers or copying binary payloads.
func EstimateSizeWithLimits(term Term, limits Limits) (int64, error) {
	limits = limits.normalize()
	est := sizeEstimator{limits: limits, size: 1} // leading Version (131)
	if _, err := est.visit(term, 0); err != nil {
		return 0, err
	}
	if est.size > int64(limits.MaxFrameBytes) {
		return 0, fmt.Errorf("%w: encoded size %d exceeds max %d", ErrLimitExceeded, est.size, limits.MaxFrameBytes)
	}
	return est.size, nil
}

// SnapshotWithLimits validates term against limits, computes its standalone ETF encoded byte
// size, and returns an owned Term detached from any caller-mutable slices or *big.Int values.
func SnapshotWithLimits(term Term, limits Limits) (Term, int64, error) {
	limits = limits.normalize()
	est := sizeEstimator{limits: limits, size: 1, clone: true} // leading Version (131)
	owned, err := est.visit(term, 0)
	if err != nil {
		return nil, 0, err
	}
	if est.size > int64(limits.MaxFrameBytes) {
		return nil, 0, fmt.Errorf("%w: encoded size %d exceeds max %d", ErrLimitExceeded, est.size, limits.MaxFrameBytes)
	}
	return owned, est.size, nil
}

type sizeEstimator struct {
	limits Limits
	size   int64
	terms  int
	clone  bool
}

func (s *sizeEstimator) add(bytes int64) error {
	s.size += bytes
	if s.size > int64(s.limits.MaxFrameBytes) {
		return fmt.Errorf("%w: encoded size %d exceeds %d", ErrLimitExceeded, s.size, s.limits.MaxFrameBytes)
	}
	return nil
}

func (s *sizeEstimator) step() error {
	s.terms++
	if s.terms > s.limits.MaxTerms {
		return fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, s.limits.MaxTerms)
	}
	return nil
}

func (s *sizeEstimator) addAtom(a Atom) error {
	if err := ValidateAtom(a); err != nil {
		return err
	}
	if len(a) <= 255 {
		return s.add(int64(2 + len(a)))
	}
	return s.add(int64(3 + len(a)))
}

func (s *sizeEstimator) addSmallOrInt32(v int32) error {
	if v >= 0 && v <= 255 {
		return s.add(2)
	}
	return s.add(5)
}

func (s *sizeEstimator) addInt64(v int64) error {
	if v >= 0 && v <= 255 {
		return s.add(2)
	}
	if v >= math.MinInt32 && v <= math.MaxInt32 {
		return s.add(5)
	}
	return s.add(11) // tagSmallBigExt + len(1) + sign(1) + up to 8 magnitude bytes
}

func (s *sizeEstimator) addUint64(v uint64) error {
	if v <= 255 {
		return s.add(2)
	}
	if v <= math.MaxInt32 {
		return s.add(5)
	}
	return s.add(11)
}

func (s *sizeEstimator) addPID(p PID) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if err := s.add(13); err != nil {
		return err
	}
	return s.addAtom(p.Node)
}

func (s *sizeEstimator) visit(term Term, depth int) (Term, error) {
	if term == nil {
		return nil, fmt.Errorf("%w: Go nil is not an ETF term (use etf.Nil{})", ErrInvalidTerm)
	}
	if depth > s.limits.MaxDepth {
		return nil, fmt.Errorf("%w: nesting depth exceeds %d", ErrLimitExceeded, s.limits.MaxDepth)
	}
	if err := s.step(); err != nil {
		return nil, err
	}

	switch v := term.(type) {
	case Atom:
		return v, s.addAtom(v)
	case int:
		return v, s.addInt64(int64(v))
	case int8:
		return v, s.addInt64(int64(v))
	case int16:
		return v, s.addInt64(int64(v))
	case int32:
		return v, s.addSmallOrInt32(v)
	case int64:
		return v, s.addInt64(v)
	case uint:
		return v, s.addUint64(uint64(v))
	case uint8:
		return v, s.add(2)
	case uint16:
		return v, s.addUint64(uint64(v))
	case uint32:
		return v, s.addUint64(uint64(v))
	case uint64:
		return v, s.addUint64(v)
	case *big.Int:
		if v == nil {
			return nil, fmt.Errorf("%w: nil *big.Int", ErrInvalidTerm)
		}
		var err error
		if v.IsInt64() {
			err = s.addInt64(v.Int64())
		} else {
			magBytes := (v.BitLen() + 7) / 8
			if magBytes > s.limits.MaxBigIntBytes {
				return nil, fmt.Errorf("%w: bigint %d bytes exceeds %d", ErrLimitExceeded, magBytes, s.limits.MaxBigIntBytes)
			}
			if magBytes <= 255 {
				err = s.add(int64(3 + magBytes))
			} else {
				err = s.add(int64(6 + magBytes))
			}
		}
		if err != nil {
			return nil, err
		}
		if !s.clone {
			return v, nil
		}
		return new(big.Int).Set(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("%w: non-finite float %v", ErrInvalidTerm, v)
		}
		return v, s.add(9)
	case Binary:
		if len(v) > s.limits.MaxBinaryBytes {
			return nil, fmt.Errorf("%w: binary %d bytes exceeds %d", ErrLimitExceeded, len(v), s.limits.MaxBinaryBytes)
		}
		if err := s.add(int64(5 + len(v))); err != nil {
			return nil, err
		}
		if !s.clone {
			return v, nil
		}
		if len(v) == 0 {
			return Binary{}, nil
		}
		cp := make(Binary, len(v))
		copy(cp, v)
		return cp, nil
	case BitBinary:
		if err := v.Validate(); err != nil {
			return nil, err
		}
		if len(v.Bytes) > s.limits.MaxBinaryBytes {
			return nil, fmt.Errorf("%w: bitstring %d bytes exceeds %d", ErrLimitExceeded, len(v.Bytes), s.limits.MaxBinaryBytes)
		}
		if err := s.add(int64(6 + len(v.Bytes))); err != nil {
			return nil, err
		}
		if !s.clone {
			return v, nil
		}
		cp := make([]byte, len(v.Bytes))
		copy(cp, v.Bytes)
		return BitBinary{Bytes: cp, LastBits: v.LastBits}, nil
	case Nil:
		return v, s.add(1)
	case Tuple:
		if uint64(len(v)) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: tuple arity overflows uint32", ErrLimitExceeded)
		}
		if len(v) > s.limits.MaxTerms-s.terms {
			return nil, fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, s.limits.MaxTerms)
		}
		if len(v) <= 255 {
			if err := s.add(2); err != nil {
				return nil, err
			}
		} else {
			if err := s.add(5); err != nil {
				return nil, err
			}
		}
		if !s.clone {
			for i := range v {
				if _, err := s.visit(v[i], depth+1); err != nil {
					return nil, err
				}
			}
			return v, nil
		}
		if len(v) == 0 {
			return Tuple{}, nil
		}
		cp := make(Tuple, len(v))
		for i := range v {
			elem, err := s.visit(v[i], depth+1)
			if err != nil {
				return nil, err
			}
			cp[i] = elem
		}
		return cp, nil
	case List:
		return s.visitList(v, depth)
	case Map:
		if uint64(len(v)) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: map arity overflows uint32", ErrLimitExceeded)
		}
		if len(v) > (s.limits.MaxTerms-s.terms)/2 {
			return nil, fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, s.limits.MaxTerms)
		}
		if err := s.add(5); err != nil {
			return nil, err
		}
		if !s.clone {
			if err := validateUniqueMapKeys(v); err != nil {
				return nil, err
			}
			for i := range v {
				if _, err := s.visit(v[i].Key, depth+1); err != nil {
					return nil, err
				}
				if _, err := s.visit(v[i].Value, depth+1); err != nil {
					return nil, err
				}
			}
			return v, nil
		}
		if len(v) == 0 {
			return Map{}, nil
		}
		cp := make(Map, len(v))
		for i := range v {
			k, err := s.visit(v[i].Key, depth+1)
			if err != nil {
				return nil, err
			}
			val, err := s.visit(v[i].Value, depth+1)
			if err != nil {
				return nil, err
			}
			cp[i] = MapPair{Key: k, Value: val}
		}
		if err := validateUniqueMapKeys(cp); err != nil {
			return nil, err
		}
		return cp, nil
	case PID:
		return v, s.addPID(v)
	case Port:
		if err := v.Validate(); err != nil {
			return nil, err
		}
		if err := s.add(13); err != nil {
			return nil, err
		}
		return v, s.addAtom(v.Node)
	case Ref:
		if err := v.Validate(); err != nil {
			return nil, err
		}
		if err := s.add(int64(7 + 4*int(v.Len))); err != nil {
			return nil, err
		}
		return v, s.addAtom(v.Node)
	case Export:
		if err := v.Validate(); err != nil {
			return nil, err
		}
		if err := s.add(3); err != nil {
			return nil, err
		}
		if err := s.addAtom(v.Module); err != nil {
			return nil, err
		}
		return v, s.addAtom(v.Function)
	case Fun:
		if err := v.Validate(); err != nil {
			return nil, err
		}
		if len(v.FreeVars) > s.limits.MaxFunFreeVars {
			return nil, fmt.Errorf("%w: fun free vars %d exceeds %d", ErrLimitExceeded, len(v.FreeVars), s.limits.MaxFunFreeVars)
		}
		if len(v.FreeVars) > s.limits.MaxTerms-s.terms {
			return nil, fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, s.limits.MaxTerms)
		}
		if err := s.add(30); err != nil {
			return nil, err
		}
		if err := s.addAtom(v.Module); err != nil {
			return nil, err
		}
		if err := s.addSmallOrInt32(v.OldIndex); err != nil {
			return nil, err
		}
		if err := s.addSmallOrInt32(v.OldUniq); err != nil {
			return nil, err
		}
		if err := s.addPID(v.PID); err != nil {
			return nil, err
		}
		if !s.clone || len(v.FreeVars) == 0 {
			for i := range v.FreeVars {
				if _, err := s.visit(v.FreeVars[i], depth+1); err != nil {
					return nil, err
				}
			}
			if s.clone {
				out := v
				out.FreeVars = nil
				return out, nil
			}
			return v, nil
		}
		cpVars := make([]Term, len(v.FreeVars))
		for i := range v.FreeVars {
			fv, err := s.visit(v.FreeVars[i], depth+1)
			if err != nil {
				return nil, err
			}
			cpVars[i] = fv
		}
		out := v
		out.FreeVars = cpVars
		return out, nil
	default:
		return nil, fmt.Errorf("%w: %w: %T", ErrInvalidTerm, ErrUnsupportedTerm, term)
	}
}

func (s *sizeEstimator) visitList(l List, depth int) (Term, error) {
	if l.Tail == nil {
		return nil, fmt.Errorf("%w: List has nil Tail (use etf.Nil{})", ErrInvalidTerm)
	}

	total := 0
	allBytes := true
	cur := Term(l)
	var finalTail Term
	steps := 0
	for {
		lv, ok := cur.(List)
		if !ok {
			finalTail = cur
			break
		}
		steps++
		if steps > s.limits.MaxDepth && len(lv.Elements) == 0 {
			return nil, fmt.Errorf("%w: empty-element list tail chain exceeds %d", ErrLimitExceeded, s.limits.MaxDepth)
		}
		if lv.Tail == nil {
			return nil, fmt.Errorf("%w: nested List has nil Tail", ErrInvalidTerm)
		}
		for _, elem := range lv.Elements {
			total++
			if total > s.limits.MaxTerms {
				return nil, fmt.Errorf("%w: list length exceeds %d", ErrLimitExceeded, s.limits.MaxTerms)
			}
			if allBytes {
				if ni, ok := normalizeInt(elem); !ok || ni.big != nil || ni.small < 0 || ni.small > 255 {
					allBytes = false
				}
			}
		}
		cur = lv.Tail
	}
	if total == 0 {
		if _, ok := finalTail.(Nil); ok {
			return Nil{}, s.add(1)
		}
		return nil, fmt.Errorf("%w: List with empty Elements must have Nil{} Tail", ErrInvalidTerm)
	}
	if uint64(total) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: list length overflows uint32", ErrLimitExceeded)
	}

	if _, isProper := finalTail.(Nil); isProper && allBytes && total <= 65535 {
		s.terms += total
		if s.terms > s.limits.MaxTerms {
			return nil, fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, s.limits.MaxTerms)
		}
		if err := s.add(int64(3 + total)); err != nil {
			return nil, err
		}
		if !s.clone {
			return l, nil
		}
		cpElems := make([]Term, 0, total)
		iter := newListCursor(l)
		for {
			elem, ok, _ := iter.next()
			if !ok {
				break
			}
			ni, _ := normalizeInt(elem)
			cpElems = append(cpElems, uint8(ni.small))
		}
		return List{Elements: cpElems, Tail: Nil{}}, nil
	}

	if total > s.limits.MaxTerms-s.terms {
		return nil, fmt.Errorf("%w: term count exceeds %d", ErrLimitExceeded, s.limits.MaxTerms)
	}
	if err := s.add(5); err != nil {
		return nil, err
	}
	if !s.clone {
		iter := newListCursor(l)
		for {
			elem, ok, tail := iter.next()
			if !ok {
				_, err := s.visit(tail, depth+1)
				return l, err
			}
			if _, err := s.visit(elem, depth+1); err != nil {
				return nil, err
			}
		}
	}
	cpElems := make([]Term, 0, total)
	iter := newListCursor(l)
	for {
		elem, ok, tail := iter.next()
		if !ok {
			ct, err := s.visit(tail, depth+1)
			if err != nil {
				return nil, err
			}
			return List{Elements: cpElems, Tail: ct}, nil
		}
		ce, err := s.visit(elem, depth+1)
		if err != nil {
			return nil, err
		}
		cpElems = append(cpElems, ce)
	}
}
