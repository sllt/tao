package etf

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os/exec"
	"strings"
	"testing"
)

type sliceAtomCache struct {
	index map[Atom]uint8
	list  []Atom
}

func newSliceAtomCache() *sliceAtomCache {
	return &sliceAtomCache{index: make(map[Atom]uint8)}
}

func (c *sliceAtomCache) AtomRef(a Atom) (uint8, bool) {
	if idx, ok := c.index[a]; ok {
		return idx, true
	}
	if len(c.list) >= 255 {
		return 0, false
	}
	idx := uint8(len(c.list))
	c.index[a] = idx
	c.list = append(c.list, a)
	return idx, true
}

func TestAtomBoundariesAndLatin1(t *testing.T) {
	a255ASCII := Atom(strings.Repeat("a", 255))
	encASCII, err := Encode(a255ASCII)
	if err != nil {
		t.Fatalf("encode 255 ASCII atom: %v", err)
	}
	if encASCII[1] != tagSmallAtomUTF8Ext {
		t.Fatalf("expected SMALL_ATOM_UTF8_EXT (119), got %d", encASCII[1])
	}
	decASCII, err := Decode(encASCII)
	if err != nil || !TermEqual(decASCII, a255ASCII) {
		t.Fatalf("decode 255 ASCII atom: %v, err=%v", decASCII, err)
	}

	// 255 4-byte runes = 1020 UTF-8 bytes -> must use ATOM_UTF8_EXT (118)
	a255Emoji := Atom(strings.Repeat("🚀", 255))
	encEmoji, err := Encode(a255Emoji)
	if err != nil {
		t.Fatalf("encode 255 emoji atom: %v", err)
	}
	if encEmoji[1] != tagAtomUTF8Ext {
		t.Fatalf("expected ATOM_UTF8_EXT (118), got %d", encEmoji[1])
	}
	decEmoji, err := Decode(encEmoji)
	if err != nil || !TermEqual(decEmoji, a255Emoji) {
		t.Fatalf("decode 255 emoji atom: %v, err=%v", decEmoji, err)
	}

	// 256 ASCII runes -> rejected
	if _, err := Encode(Atom(strings.Repeat("x", 256))); !errors.Is(err, ErrInvalidTerm) {
		t.Fatalf("expected ErrInvalidTerm for 256-char atom, got %v", err)
	}
	// Invalid UTF-8 -> rejected
	if _, err := Encode(Atom(string([]byte{0xff, 0xfe}))); !errors.Is(err, ErrInvalidTerm) {
		t.Fatalf("expected ErrInvalidTerm for invalid UTF-8 atom, got %v", err)
	}

	// Read-only Latin-1 ATOM_EXT (100) and SMALL_ATOM_EXT (115) in standalone mode:
	// 0x63 0x61 0x66 0xE9 = "café" in ISO-8859-1 (invalid UTF-8 raw byte 0xE9!).
	latin1Small := []byte{Version, tagSmallAtomExt, 4, 'c', 'a', 'f', 0xe9}
	gotSmall, err := Decode(latin1Small)
	if err != nil || gotSmall != Atom("café") {
		t.Fatalf("SMALL_ATOM_EXT Latin-1 decode: got %#v, err=%v", gotSmall, err)
	}
	latin1Full := []byte{Version, tagAtomExt, 0, 4, 'c', 'a', 'f', 0xe9}
	gotFull, err := Decode(latin1Full)
	if err != nil || gotFull != Atom("café") {
		t.Fatalf("ATOM_EXT Latin-1 decode: got %#v, err=%v", gotFull, err)
	}

	// Rejected on v6 distribution body decoder:
	dd := NewDistDecoder(nil, false, DefaultLimits())
	if _, _, err := dd.DecodeNext(latin1Small[1:]); !errors.Is(err, ErrUnsupportedTag) {
		t.Fatalf("expected ErrUnsupportedTag for SMALL_ATOM_EXT in DistDecoder, got %v", err)
	}
}

func TestIntegerAndFloatBoundaries(t *testing.T) {
	hugePos := new(big.Int).Lsh(big.NewInt(1), 2200) // > 255 bytes -> LARGE_BIG_EXT
	hugePos.Add(hugePos, big.NewInt(123456789))
	hugeNeg := new(big.Int).Neg(hugePos)

	cases := []struct {
		in   Term
		want Term
	}{
		{int(0), int64(0)},
		{uint8(255), int64(255)},
		{uint16(256), int64(256)},
		{int8(-1), int64(-1)},
		{int32(math.MinInt32), int64(math.MinInt32)},
		{int32(math.MaxInt32), int64(math.MaxInt32)},
		{int64(math.MinInt32) - 1, int64(math.MinInt32) - 1},
		{int64(math.MaxInt32) + 1, int64(math.MaxInt32) + 1},
		{int64(math.MinInt64), int64(math.MinInt64)},
		{int64(math.MaxInt64), int64(math.MaxInt64)},
		{uint64(math.MaxUint64), new(big.Int).SetUint64(math.MaxUint64)},
		{hugePos, hugePos},
		{hugeNeg, hugeNeg},
		{float64(0.0), float64(0.0)},
		{math.Copysign(0.0, -1), math.Copysign(0.0, -1)},
		{float64(-3.141592653589793), float64(-3.141592653589793)},
	}

	for i, tc := range cases {
		raw, err := Encode(tc.in)
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		got, err := Decode(raw)
		if err != nil {
			t.Fatalf("case %d decode: %v", i, err)
		}
		if !TermEqual(got, tc.want) {
			t.Fatalf("case %d mismatch: got %#v (%T), want %#v (%T)", i, got, got, tc.want, tc.want)
		}
	}

	// Non-finite floats rejected
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := Encode(bad); !errors.Is(err, ErrInvalidTerm) {
			t.Fatalf("expected ErrInvalidTerm for %v, got %v", bad, err)
		}
		var wire [10]byte
		wire[0] = Version
		wire[1] = tagNewFloatExt
		binary.BigEndian.PutUint64(wire[2:], math.Float64bits(bad))
		if _, err := Decode(wire[:]); !errors.Is(err, ErrMalformedETF) {
			t.Fatalf("expected ErrMalformedETF for wire %v, got %v", bad, err)
		}
	}
}

func TestMapExactKeyEqualityAndDuplicates(t *testing.T) {
	negZero := math.Copysign(0.0, -1)
	posZero := float64(0.0)
	if TermEqual(posZero, negZero) {
		t.Fatalf("0.0 and -0.0 must not be =:= in OTP 27+")
	}
	if TermEqual(int64(1), float64(1.0)) {
		t.Fatalf("1 and 1.0 must not be =:=")
	}

	// Distinct keys: 1, 1.0, 0.0, -0.0, tuple key, improper list key, nested map key
	refA, err := NewRef("n@host", 4, 1, 2, 3, 4, 5)
	if err != nil {
		t.Fatal(err)
	}
	aliasTag, err := ImproperList([]Term{Atom("alias")}, refA)
	if err != nil {
		t.Fatal(err)
	}
	validMap := Map{
		{Key: int64(1), Value: Atom("int_one")},
		{Key: float64(1.0), Value: Atom("float_one")},
		{Key: posZero, Value: Atom("pos_zero")},
		{Key: negZero, Value: Atom("neg_zero")},
		{Key: Tuple{Atom("k"), Binary{1, 2}}, Value: int64(99)},
		{Key: aliasTag, Value: Atom("alias_val")},
	}
	raw, err := Encode(validMap)
	if err != nil {
		t.Fatalf("encode validMap: %v", err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode validMap: %v", err)
	}
	dm := decoded.(Map)
	if v, ok := dm.Get(negZero); !ok || v != Atom("neg_zero") {
		t.Fatalf("lookup -0.0 failed: %v, %v", v, ok)
	}
	if v, ok := dm.Get(posZero); !ok || v != Atom("pos_zero") {
		t.Fatalf("lookup +0.0 failed: %v, %v", v, ok)
	}

	// Duplicate keys rejected on encode:
	refShort, _ := NewRef("n@host", 4, 7, 8)
	refPadded, _ := NewRef("n@host", 4, 7, 8, 0)
	dupCases := []Map{
		{{Key: int(5), Value: Atom("a")}, {Key: int64(5), Value: Atom("b")}},
		{{Key: negZero, Value: Atom("a")}, {Key: math.Copysign(0.0, -1), Value: Atom("b")}},
		{{Key: refShort, Value: Atom("a")}, {Key: refPadded, Value: Atom("b")}},
		{
			{Key: Map{{Key: Atom("x"), Value: int64(1)}, {Key: Atom("y"), Value: int64(2)}}, Value: Atom("a")},
			{Key: Map{{Key: Atom("y"), Value: int64(2)}, {Key: Atom("x"), Value: int64(1)}}, Value: Atom("b")},
		},
	}
	for i, dm := range dupCases {
		if _, err := Encode(dm); !errors.Is(err, ErrDuplicateMapKey) {
			t.Fatalf("dup case %d: expected ErrDuplicateMapKey on encode, got %v", i, err)
		}
	}

	// Duplicate keys on raw wire rejected on decode:
	dupWire := []byte{
		Version, tagMapExt, 0, 0, 0, 2,
		tagSmallIntegerExt, 42, tagSmallAtomUTF8Ext, 1, 'a',
		tagIntegerExt, 0, 0, 0, 42, tagSmallAtomUTF8Ext, 1, 'b',
	}
	if _, err := Decode(dupWire); !errors.Is(err, ErrDuplicateMapKey) {
		t.Fatalf("expected ErrDuplicateMapKey on wire decode, got %v", err)
	}
}

func TestOwnedBuffersAndFunCacheRewrite(t *testing.T) {
	origPayload := []byte("owned-payload-bytes")
	ref5, _ := NewRef("origin@host", 9, 0x3ffff, 0xdeadbeef, 0xcafebabe, 4, 5)
	fn := Fun{
		Arity:    2,
		Uniq:     [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Index:    7,
		Module:   "my_mod",
		OldIndex: 3,
		OldUniq:  1234567,
		PID:      PID{Node: "origin@host", ID: 42, Serial: 1, Creation: 9},
		FreeVars: []Term{
			Atom("captured_atom"),
			Binary(origPayload),
			BitBinary{Bytes: []byte{0b10100000}, LastBits: 3},
			Port{Node: "origin@host", ID: (uint64(1) << 40) | 99, Creation: 9},
			ref5,
			Export{Module: "lists", Function: "reverse", Arity: 1},
		},
	}

	// Encode with connection A's atom cache, decode, then re-encode with connection B's atom cache.
	cacheA := newSliceAtomCache()
	wireA, err := AppendDistTerm(nil, fn, cacheA, false, DefaultLimits())
	if err != nil {
		t.Fatalf("AppendDistTerm A: %v", err)
	}
	// Mutate source slice to prove encoder didn't retain anything weird, then mutate wireA after decode.
	decA := NewDistDecoder(cacheA.list, false, DefaultLimits())
	gotA, rest, err := decA.DecodeNext(wireA)
	if err != nil || len(rest) != 0 {
		t.Fatalf("DecodeNext A: err=%v rest=%d", err, len(rest))
	}
	for i := range wireA {
		wireA[i] = 0xFF
	}

	// Pre-populate connection B's cache in a different order so atom indices shift!
	cacheB := newSliceAtomCache()
	cacheB.AtomRef("unrelated_first")
	cacheB.AtomRef("captured_atom")
	wireB, err := AppendDistTerm(nil, gotA, cacheB, false, DefaultLimits())
	if err != nil {
		t.Fatalf("AppendDistTerm B: %v", err)
	}
	decB := NewDistDecoder(cacheB.list, false, DefaultLimits())
	gotB, rest, err := decB.DecodeNext(wireB)
	if err != nil || len(rest) != 0 {
		t.Fatalf("DecodeNext B: err=%v rest=%d", err, len(rest))
	}
	if !TermEqual(gotB, fn) {
		t.Fatalf("Fun cross-cache round-trip mismatch:\ngot:  %#v\nwant: %#v", gotB, fn)
	}
}

func TestCompressedAndBoundsEnforcement(t *testing.T) {
	term := Tuple{Atom("compressed_ok"), ProperList(int64(1), int64(2), int64(300)), UTF8Binary("hello")}
	plain, err := Encode(term)
	if err != nil {
		t.Fatal(err)
	}
	rawBody := plain[1:]
	var zbuf bytes.Buffer
	zw := zlib.NewWriter(&zbuf)
	if _, err := zw.Write(rawBody); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	comp := make([]byte, 6+zbuf.Len())
	comp[0] = Version
	comp[1] = tagCompressed
	binary.BigEndian.PutUint32(comp[2:6], uint32(len(rawBody)))
	copy(comp[6:], zbuf.Bytes())

	got, err := Decode(comp)
	if err != nil || !TermEqual(got, term) {
		t.Fatalf("compressed decode failed: got=%#v err=%v", got, err)
	}

	// Compression bomb: declared size exceeds limit
	bombDeclared := append([]byte(nil), comp...)
	binary.BigEndian.PutUint32(bombDeclared[2:6], 64<<20)
	if _, err := DecodeWithLimits(bombDeclared, Limits{MaxFrameBytes: 1 << 20, MaxDecodedBytes: 1 << 20}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expected ErrLimitExceeded for declared bomb, got %v", err)
	}

	// Compression mismatch: declared smaller than actual zlib stream
	bombSmallDeclared := append([]byte(nil), comp...)
	binary.BigEndian.PutUint32(bombSmallDeclared[2:6], uint32(len(rawBody)-1))
	if _, err := Decode(bombSmallDeclared); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expected ErrLimitExceeded when zlib expands past declared size, got %v", err)
	}

	// Huge arity in tiny frame must fail without allocating huge slice
	fakeHugeTuple := []byte{Version, tagLargeTupleExt, 0xff, 0xff, 0xff, 0xff, tagNilExt}
	if _, err := Decode(fakeHugeTuple); !errors.Is(err, ErrMalformedETF) {
		t.Fatalf("expected ErrMalformedETF on huge tuple arity, got %v", err)
	}

	// Deep nesting > MaxDepth rejected
	var deep []byte
	deep = append(deep, Version)
	for i := 0; i < 70; i++ {
		deep = append(deep, tagSmallTupleExt, 1)
	}
	deep = append(deep, tagNilExt)
	if _, err := DecodeWithLimits(deep, Limits{MaxDepth: 64}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expected ErrLimitExceeded on deep tuple, got %v", err)
	}

	// STRING_EXT expansion bomb: 50,000 bytes wire -> > 1.2 MiB Go heap charge
	strBomb := make([]byte, 1+3+50000)
	strBomb[0] = Version
	strBomb[1] = tagStringExt
	binary.BigEndian.PutUint16(strBomb[2:4], 50000)
	if _, err := DecodeWithLimits(strBomb, Limits{MaxDecodedBytes: 256 << 10}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expected ErrLimitExceeded on STRING_EXT expansion bomb, got %v", err)
	}
}

func TestBEAMBinaryToTermAndTermToBinaryRoundTrip(t *testing.T) {
	erlPath, err := exec.LookPath("erl")
	if err != nil {
		t.Skip("erl not installed in PATH")
	}

	huge := new(big.Int).Lsh(big.NewInt(7), 300)
	huge.Sub(huge, big.NewInt(1))
	ref5, _ := NewRef("tao@127.0.0.1", 42, 17, 0x80000001, 0xffffffff, 4, 5)
	aliasImproper, _ := ImproperList([]Term{Atom("alias")}, ref5)

	goTerm := Tuple{
		Atom("ok"),
		Atom(""),
		Atom(strings.Repeat("中", 200)), // 600 bytes > 255 -> ATOM_UTF8_EXT
		int64(0),
		int64(-1),
		int64(math.MinInt64),
		int64(math.MaxInt64),
		huge,
		float64(0.0),
		math.Copysign(0.0, -1),
		UTF8Binary("hello beam"),
		BitBinary{Bytes: []byte{0xAA, 0b11000000}, LastBits: 2},
		Nil{},
		Charlist("ascii"),
		Charlist("你好"),
		aliasImproper,
		PID{Node: "tao@127.0.0.1", ID: 100, Serial: 2, Creation: 42},
		Port{Node: "tao@127.0.0.1", ID: (uint64(1) << 45) | 7, Creation: 42},
		ref5,
		Export{Module: "erlang", Function: "self", Arity: 0},
		Map{
			{Key: int64(1), Value: Atom("int")},
			{Key: float64(1.0), Value: Atom("flt")},
			{Key: float64(0.0), Value: Atom("pz")},
			{Key: math.Copysign(0.0, -1), Value: Atom("nz")},
			{Key: aliasImproper, Value: Tuple{Atom("nested"), int64(9)}},
		},
	}

	wire, err := Encode(goTerm)
	if err != nil {
		t.Fatalf("Encode goTerm: %v", err)
	}
	hexIn := hex.EncodeToString(wire)

	// Ask real BEAM to decode our ETF, verify map_size == 5, construct a real anonymous fun with free vars,
	// and re-encode both back as hex for Go to decode and re-verify!
	eval := fmt.Sprintf(`
BinIn = binary:decode_hex(<<"%s">>),
Term = binary_to_term(BinIn),
Map = element(21, Term),
5 = map_size(Map),
int = maps:get(1, Map),
flt = maps:get(1.0, Map),
pz = maps:get(0.0, Map),
nz = maps:get(-0.0, Map),
Captured = {captured, self(), make_ref(), <<5:3>>},
Fun = fun(X) -> {X, Captured} end,
OutBin = term_to_binary({Term, Fun}),
CompBin = term_to_binary(Term, [compressed]),
io:format("~s|~s~n", [binary:encode_hex(OutBin), binary:encode_hex(CompBin)]),
halt(0).
`, hexIn)

	cmd := exec.Command(erlPath, "-noshell", "-eval", eval)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("erl failed: %v\noutput: %s", err, string(out))
	}
	parts := strings.Split(strings.TrimSpace(string(out)), "|")
	if len(parts) != 2 {
		t.Fatalf("unexpected erl output: %q", string(out))
	}
	beamBytes, err := hex.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decode OutBin hex: %v", err)
	}
	compBytes, err := hex.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode CompBin hex: %v", err)
	}

	decodedPair, err := Decode(beamBytes)
	if err != nil {
		t.Fatalf("Decode BEAM OutBin: %v", err)
	}
	pairTup, ok := decodedPair.(Tuple)
	if !ok || len(pairTup) != 2 {
		t.Fatalf("expected 2-tuple from BEAM, got %#v", decodedPair)
	}
	if !TermEqual(pairTup[0], goTerm) {
		t.Fatalf("BEAM round-trip term mismatch!\ngot:  %#v\nwant: %#v", pairTup[0], goTerm)
	}
	if _, ok := pairTup[1].(Fun); !ok {
		t.Fatalf("expected Fun in element 2, got %T", pairTup[1])
	}

	// Re-encode the pair (including BEAM's real Fun!) in Go and verify BEAM's binary_to_term =:= original!
	reEncoded, err := Encode(decodedPair)
	if err != nil {
		t.Fatalf("re-encode BEAM Fun pair: %v", err)
	}
	verifyEval := fmt.Sprintf(`
Orig = binary_to_term(binary:decode_hex(<<"%s">>)),
Re = binary_to_term(binary:decode_hex(<<"%s">>)),
true = (Orig =:= Re),
io:format("EXACT_MATCH~n"),
halt(0).
`, parts[0], hex.EncodeToString(reEncoded))
	out2, err := exec.Command(erlPath, "-noshell", "-eval", verifyEval).CombinedOutput()
	if err != nil || !strings.Contains(string(out2), "EXACT_MATCH") {
		t.Fatalf("BEAM exact match check failed: err=%v out=%s", err, string(out2))
	}

	// Also decode BEAM's zlib-compressed term_to_binary(Term, [compressed])
	decodedComp, err := Decode(compBytes)
	if err != nil {
		t.Fatalf("Decode BEAM compressed: %v", err)
	}
	if !TermEqual(decodedComp, goTerm) {
		t.Fatalf("BEAM compressed term mismatch")
	}
}

func FuzzDecode(f *testing.F) {
	seeds := [][]byte{
		{Version, tagNilExt},
		{Version, tagSmallIntegerExt, 42},
		{Version, tagIntegerExt, 0xff, 0xff, 0xff, 0xff},
		{Version, tagSmallAtomUTF8Ext, 2, 'o', 'k'},
		{Version, tagSmallAtomExt, 4, 'c', 'a', 'f', 0xe9},
		{Version, tagBitBinaryExt, 0, 0, 0, 1, 3, 0xe0},
		{Version, tagMapExt, 0, 0, 0, 2, tagSmallIntegerExt, 1, tagNilExt, tagSmallIntegerExt, 1, tagNilExt},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	limits := Limits{
		MaxFrameBytes:   64 << 10,
		MaxDecodedBytes: 256 << 10,
		MaxTerms:        4096,
		MaxDepth:        32,
		MaxBinaryBytes:  32 << 10,
		MaxBigIntBytes:  4 << 10,
		MaxFunFreeVars:  64,
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		term, err := DecodeWithLimits(data, limits)
		if err != nil {
			return
		}
		// Any successfully decoded standalone term (even from legacy read-only tags)
		// must cleanly re-encode to modern UTF-8 ETF and round-trip with TermEqual!
		re, err := AppendEncodeWithLimits(nil, term, Limits{
			MaxFrameBytes:   256 << 10,
			MaxDecodedBytes: 512 << 10,
			MaxTerms:        8192,
			MaxDepth:        64,
			MaxBinaryBytes:  64 << 10,
			MaxBigIntBytes:  8 << 10,
			MaxFunFreeVars:  64,
		})
		if err != nil {
			t.Fatalf("re-encode failed for %#v: %v", term, err)
		}
		term2, err := Decode(re)
		if err != nil {
			t.Fatalf("decode(re-encode) failed: %v", err)
		}
		if !TermEqual(term, term2) {
			t.Fatalf("round-trip inequality:\nterm1=%#v\nterm2=%#v", term, term2)
		}
	})
}

func TestSnapshotWithLimitsOwnsMutableData(t *testing.T) {
	bin := Binary("before")
	bitBytes := []byte{0xe0}
	bigVal := new(big.Int).Lsh(big.NewInt(1), 80)
	tup := Tuple{bin, BitBinary{Bytes: bitBytes, LastBits: 3}, bigVal}
	charList := List{Elements: []Term{uint8('a'), uint8('b')}, Tail: Nil{}}
	genList := List{Elements: []Term{Binary("item")}, Tail: Binary("tail")}
	mp := Map{{Key: Binary("k1"), Value: tup}, {Key: Atom("clist"), Value: charList}}
	fn := Fun{
		Arity:    1,
		Module:   "mod",
		PID:      PID{Node: "n@h", ID: 1, Creation: 4},
		FreeVars: []Term{genList, mp},
	}

	wantEncoded, err := Encode(fn)
	if err != nil {
		t.Fatal(err)
	}

	snap, est, err := SnapshotWithLimits(fn, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if est != int64(len(wantEncoded)) {
		t.Fatalf("estimated size = %d, want %d", est, len(wantEncoded))
	}

	// Mutate every caller-owned backing slice and *big.Int in place.
	copy(bin, "AFTER!")
	bitBytes[0] = 0x20
	bigVal.SetInt64(7)
	tup[0] = Atom("mutated")
	charList.Elements[0] = uint8('Z')
	copy(genList.Elements[0].(Binary), "XXXX")
	copy(genList.Tail.(Binary), "YYYY")
	copy(mp[0].Key.(Binary), "k2")
	mp[0].Value = Atom("changed")
	fn.FreeVars[0] = Atom("wiped")

	gotEncoded, err := Encode(snap)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotEncoded, wantEncoded) {
		t.Fatalf("snapshot mutated by caller changes:\ngot =%x\nwant=%x", gotEncoded, wantEncoded)
	}
}
