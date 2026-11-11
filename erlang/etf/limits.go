package etf

// Default decode/encode limits aligned with §11.3 of the interop design.
const (
	DefaultMaxFrameBytes   = 4 << 20  // 4 MiB wire payload limit
	DefaultMaxDecodedBytes = 32 << 20 // 32 MiB Go heap charge limit
	DefaultMaxTerms        = 1 << 20  // 1,048,576 term nodes
	DefaultMaxDepth        = 64       // maximum container nesting depth
	DefaultMaxBinaryBytes  = 4 << 20  // 4 MiB single binary/bitstring
	DefaultMaxBigIntBytes  = 64 << 10 // 64 KiB bignum magnitude
	DefaultMaxFunFreeVars  = 255      // BEAM MAX_ARG
)

// Limits bounds wire size, structural complexity, and decoded Go memory charge.
// Zero fields fall back to package defaults.
type Limits struct {
	MaxFrameBytes   int
	MaxDecodedBytes int64
	MaxTerms        int
	MaxDepth        int
	MaxBinaryBytes  int
	MaxBigIntBytes  int
	MaxFunFreeVars  int
}

// DefaultLimits returns the standard bounded ETF limits.
func DefaultLimits() Limits {
	return Limits{
		MaxFrameBytes:   DefaultMaxFrameBytes,
		MaxDecodedBytes: DefaultMaxDecodedBytes,
		MaxTerms:        DefaultMaxTerms,
		MaxDepth:        DefaultMaxDepth,
		MaxBinaryBytes:  DefaultMaxBinaryBytes,
		MaxBigIntBytes:  DefaultMaxBigIntBytes,
		MaxFunFreeVars:  DefaultMaxFunFreeVars,
	}
}

func (l Limits) normalize() Limits {
	if l.MaxFrameBytes <= 0 {
		l.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if l.MaxDecodedBytes <= 0 {
		l.MaxDecodedBytes = DefaultMaxDecodedBytes
	}
	if l.MaxTerms <= 0 {
		l.MaxTerms = DefaultMaxTerms
	}
	if l.MaxDepth <= 0 {
		l.MaxDepth = DefaultMaxDepth
	}
	if l.MaxBinaryBytes <= 0 {
		l.MaxBinaryBytes = DefaultMaxBinaryBytes
	}
	if l.MaxBigIntBytes <= 0 {
		l.MaxBigIntBytes = DefaultMaxBigIntBytes
	}
	if l.MaxFunFreeVars <= 0 {
		l.MaxFunFreeVars = DefaultMaxFunFreeVars
	}
	return l
}

// AtomEncoder maps an Atom to a per-message distribution header cache reference index.
// Returning ok=false encodes the atom inline using UTF-8 atom tags.
type AtomEncoder interface {
	AtomRef(a Atom) (index uint8, ok bool)
}
