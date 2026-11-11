package etf

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// Version is the standalone External Term Format magic byte.
const Version byte = 131

const (
	MaxAtomChars     = 255
	MaxAtomUTF8Bytes = MaxAtomChars * utf8.UTFMax
	MaxRefWords      = 5
)

// Wire tags from OTP erts/emulator/beam/external.h (OTP 29.1.2).
const (
	tagRecordExt         byte = 67
	tagDistHeader        byte = 68
	tagDistFragHeader    byte = 69
	tagDistFragCont      byte = 70
	tagNewFloatExt       byte = 70
	tagBitBinaryExt      byte = 77
	tagCompressed        byte = 80
	tagAtomCacheRef      byte = 82
	tagNewPIDExt         byte = 88
	tagNewPortExt        byte = 89
	tagNewerReferenceExt byte = 90
	tagSmallIntegerExt   byte = 97
	tagIntegerExt        byte = 98
	tagFloatExt          byte = 99
	tagAtomExt           byte = 100
	tagReferenceExt      byte = 101
	tagPortExt           byte = 102
	tagPIDExt            byte = 103
	tagSmallTupleExt     byte = 104
	tagLargeTupleExt     byte = 105
	tagNilExt            byte = 106
	tagStringExt         byte = 107
	tagListExt           byte = 108
	tagBinaryExt         byte = 109
	tagSmallBigExt       byte = 110
	tagLargeBigExt       byte = 111
	tagNewFunExt         byte = 112
	tagExportExt         byte = 113
	tagNewReferenceExt   byte = 114
	tagSmallAtomExt      byte = 115
	tagMapExt            byte = 116
	tagFunExt            byte = 117
	tagAtomUTF8Ext       byte = 118
	tagSmallAtomUTF8Ext  byte = 119
	tagV4PortExt         byte = 120
	tagLocalExt          byte = 121
)

var (
	// ErrInvalidTerm reports a Go value that is not a valid ETF term for encoding.
	ErrInvalidTerm = errors.New("etf: invalid term")
	// ErrUnsupportedTerm reports a Go value whose type is outside the supported ETF Term set.
	ErrUnsupportedTerm = errors.New("etf: unsupported Go type")
	// ErrMalformedETF reports truncated or structurally invalid ETF input.
	ErrMalformedETF = errors.New("etf: malformed external term")
	// ErrUnsupportedTag reports an unknown or unsupported ETF tag.
	ErrUnsupportedTag = errors.New("etf: unsupported tag")
	// ErrLimitExceeded reports an input or term exceeding configured decode/encode limits.
	ErrLimitExceeded = errors.New("etf: limit exceeded")
	// ErrDuplicateMapKey reports duplicate keys (under Erlang =:=) in a map.
	ErrDuplicateMapKey = errors.New("etf: duplicate map key")
)

// Term represents an Erlang term value in Go.
//
// Supported concrete types:
//   - Atom
//   - int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, *big.Int (decodes to int64 or *big.Int)
//   - float64 (finite IEEE-754)
//   - Binary
//   - BitBinary
//   - Nil
//   - Tuple
//   - List
//   - Map
//   - PID
//   - Port
//   - Ref
//   - Export
//   - Fun
type Term any

// Atom is a UTF-8 Erlang atom containing at most 255 Unicode code points.
type Atom string

// ValidateAtom checks that an atom is valid UTF-8 and has at most 255 characters.
func ValidateAtom(a Atom) error {
	s := string(a)
	if len(s) > MaxAtomUTF8Bytes {
		return fmt.Errorf("%w: atom exceeds %d UTF-8 bytes", ErrInvalidTerm, MaxAtomUTF8Bytes)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: atom contains invalid UTF-8", ErrInvalidTerm)
	}
	if utf8.RuneCountInString(s) > MaxAtomChars {
		return fmt.Errorf("%w: atom exceeds %d characters", ErrInvalidTerm, MaxAtomChars)
	}
	return nil
}

// Valid reports whether a is a valid UTF-8 Erlang atom.
func (a Atom) Valid() bool {
	return ValidateAtom(a) == nil
}

// BoolAtom returns the Erlang atom "true" or "false".
func BoolAtom(v bool) Atom {
	if v {
		return "true"
	}
	return "false"
}

// Binary is an owned byte-aligned Erlang binary.
type Binary []byte

// UTF8Binary copies s into an owned Erlang binary.
func UTF8Binary(s string) Binary {
	b := make(Binary, len(s))
	copy(b, s)
	return b
}

// BitBinary is an owned non-byte-aligned Erlang bitstring.
// LastBits must be in [1, 7], and the unused low (8 - LastBits) bits of the
// final byte must be zero.
type BitBinary struct {
	Bytes    []byte
	LastBits uint8
}

// Validate checks whether b is a canonical non-byte-aligned bitstring.
func (b BitBinary) Validate() error {
	if len(b.Bytes) == 0 {
		return fmt.Errorf("%w: empty BitBinary", ErrInvalidTerm)
	}
	if b.LastBits < 1 || b.LastBits > 7 {
		return fmt.Errorf("%w: BitBinary LastBits=%d out of [1,7]", ErrInvalidTerm, b.LastBits)
	}
	mask := byte((1 << (8 - b.LastBits)) - 1)
	if b.Bytes[len(b.Bytes)-1]&mask != 0 {
		return fmt.Errorf("%w: BitBinary has non-zero unused trailing bits", ErrInvalidTerm)
	}
	return nil
}

// Nil is the empty Erlang list [].
type Nil struct{}

// Tuple is a fixed-arity Erlang tuple.
type Tuple []Term

// List is a non-empty Erlang list whose final cdr is Tail.
// Proper lists use Nil{} as Tail; improper lists use a non-Nil, non-List term.
type List struct {
	Elements []Term
	Tail     Term
}

// ProperList constructs Nil{} for zero elements or a proper List ending in Nil{}.
func ProperList(elems ...Term) Term {
	if len(elems) == 0 {
		return Nil{}
	}
	out := make([]Term, len(elems))
	copy(out, elems)
	return List{Elements: out, Tail: Nil{}}
}

// ImproperList constructs a list with at least one element and an explicit tail.
func ImproperList(elems []Term, tail Term) (List, error) {
	if len(elems) == 0 || tail == nil {
		return List{}, fmt.Errorf("%w: improper list requires non-empty elements and non-nil tail", ErrInvalidTerm)
	}
	out := make([]Term, len(elems))
	copy(out, elems)
	return List{Elements: out, Tail: tail}, nil
}

// Charlist converts a Go string into an Erlang proper list of Unicode code point integers.
func Charlist(s string) Term {
	if s == "" {
		return Nil{}
	}
	elems := make([]Term, 0, utf8.RuneCountInString(s))
	for _, r := range s {
		elems = append(elems, int64(r))
	}
	return List{Elements: elems, Tail: Nil{}}
}

// IsProper reports whether l is non-empty and terminates in Nil{}.
func (l List) IsProper() bool {
	if len(l.Elements) == 0 {
		return false
	}
	_, ok := l.Tail.(Nil)
	return ok
}

// MapPair is a single key/value association in an Erlang map.
type MapPair struct {
	Key   Term
	Value Term
}

// Map is an Erlang map supporting arbitrary ETF terms as keys.
type Map []MapPair

// Get looks up key using exact Erlang term equality (=:=).
func (m Map) Get(key Term) (Term, bool) {
	for i := range m {
		if TermEqual(m[i].Key, key) {
			return m[i].Value, true
		}
	}
	return nil, false
}

// PID is an Erlang process identifier (NEW_PID_EXT).
type PID struct {
	Node     Atom
	ID       uint32
	Serial   uint32
	Creation uint32
}

// Validate checks that p has a valid node atom.
func (p PID) Validate() error {
	if p.Node == "" {
		return fmt.Errorf("%w: PID has empty node", ErrInvalidTerm)
	}
	return ValidateAtom(p.Node)
}

// Port is an Erlang port identifier (V4_PORT_EXT / NEW_PORT_EXT).
type Port struct {
	Node     Atom
	ID       uint64
	Creation uint32
}

// Validate checks that p has a valid node atom.
func (p Port) Validate() error {
	if p.Node == "" {
		return fmt.Errorf("%w: Port has empty node", ErrInvalidTerm)
	}
	return ValidateAtom(p.Node)
}

// Ref is an Erlang reference (NEWER_REFERENCE_EXT) preserving 1..5 32-bit ID words.
type Ref struct {
	Node     Atom
	Creation uint32
	Len      uint8
	ID       [MaxRefWords]uint32
}

// NewRef constructs a Ref from 1..5 32-bit ID words.
func NewRef(node Atom, creation uint32, words ...uint32) (Ref, error) {
	if node == "" {
		return Ref{}, fmt.Errorf("%w: Ref has empty node", ErrInvalidTerm)
	}
	if err := ValidateAtom(node); err != nil {
		return Ref{}, err
	}
	if len(words) < 1 || len(words) > MaxRefWords {
		return Ref{}, fmt.Errorf("%w: Ref requires 1..%d words, got %d", ErrInvalidTerm, MaxRefWords, len(words))
	}
	var r Ref
	r.Node = node
	r.Creation = creation
	r.Len = uint8(len(words))
	copy(r.ID[:], words)
	return r, nil
}

// Words returns a copy-free view of the active ID words.
func (r *Ref) Words() []uint32 {
	if r.Len > MaxRefWords {
		return r.ID[:]
	}
	return r.ID[:r.Len]
}

// Validate checks that r has a valid node atom and 1..5 words with zeroed tail words.
func (r Ref) Validate() error {
	if r.Node == "" {
		return fmt.Errorf("%w: Ref has empty node", ErrInvalidTerm)
	}
	if err := ValidateAtom(r.Node); err != nil {
		return err
	}
	if r.Len < 1 || r.Len > MaxRefWords {
		return fmt.Errorf("%w: Ref Len=%d out of [1,%d]", ErrInvalidTerm, r.Len, MaxRefWords)
	}
	for i := int(r.Len); i < MaxRefWords; i++ {
		if r.ID[i] != 0 {
			return fmt.Errorf("%w: Ref has non-zero word at index %d >= Len %d", ErrInvalidTerm, i, r.Len)
		}
	}
	return nil
}

func (r Ref) canonicalWords() [MaxRefWords]uint32 {
	var out [MaxRefWords]uint32
	limit := int(r.Len)
	if limit > MaxRefWords {
		limit = MaxRefWords
	}
	for i := 0; i < limit; i++ {
		out[i] = r.ID[i]
	}
	return out
}

// Equal reports whether r and other are identical under Erlang =:= semantics
// (same node, creation, and 32-bit ID words with missing trailing words as zero).
func (r Ref) Equal(other Ref) bool {
	return r.Node == other.Node &&
		r.Creation == other.Creation &&
		r.canonicalWords() == other.canonicalWords()
}

// Export represents an external fun M:F/A (EXPORT_EXT).
type Export struct {
	Module   Atom
	Function Atom
	Arity    uint8
}

// Validate checks that Module and Function are valid atoms.
func (e Export) Validate() error {
	if err := ValidateAtom(e.Module); err != nil {
		return err
	}
	return ValidateAtom(e.Function)
}

// Fun represents an anonymous/local Erlang function value (NEW_FUN_EXT).
// It is carried purely as structured data and is never executed.
type Fun struct {
	Arity    uint8
	Uniq     [16]byte
	Index    uint32
	Module   Atom
	OldIndex int32
	OldUniq  int32
	PID      PID
	FreeVars []Term
}

// Validate checks structural fields of f (excluding recursive FreeVars, which
// are validated during encoding).
func (f Fun) Validate() error {
	if err := ValidateAtom(f.Module); err != nil {
		return err
	}
	return f.PID.Validate()
}
