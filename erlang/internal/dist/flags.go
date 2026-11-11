package dist

import (
	"errors"
	"fmt"
)

// Capability flags from OTP erts/emulator/beam/dist.h (OTP 29.1.2, commit d199a49a01a2).
const (
	DFlagPublished          uint64 = 0x01
	DFlagAtomCache          uint64 = 0x02
	DFlagExtendedReferences uint64 = 0x04
	DFlagDistMonitor        uint64 = 0x08
	DFlagFunTags            uint64 = 0x10
	DFlagDistMonitorName    uint64 = 0x20
	DFlagHiddenAtomCache    uint64 = 0x40
	DFlagNewFunTags         uint64 = 0x80
	DFlagExtendedPIDsPorts  uint64 = 0x100
	DFlagExportPtrTag       uint64 = 0x200
	DFlagBitBinaries        uint64 = 0x400
	DFlagNewFloats          uint64 = 0x800
	DFlagUnicodeIO          uint64 = 0x1000
	DFlagDistHdrAtomCache   uint64 = 0x2000
	DFlagSmallAtomTags      uint64 = 0x4000
	DFlagETSCompressed      uint64 = 0x8000 // internal only
	DFlagUTF8Atoms          uint64 = 0x10000
	DFlagMapTag             uint64 = 0x20000
	DFlagBigCreation        uint64 = 0x40000
	DFlagSendSender         uint64 = 0x80000
	DFlagBigSeqtraceLabels  uint64 = 0x100000
	DFlagPendingConnect     uint64 = 0x200000 // internal only
	DFlagExitPayload        uint64 = 0x400000
	DFlagFragments          uint64 = 0x800000
	DFlagHandshake23        uint64 = 0x1000000
	DFlagUnlinkID           uint64 = 0x2000000
	DFlagMandatory25Digest  uint64 = 0x4000000 // dist.h:56 (note online doc typo says 1<<36)

	DFlagSpawn         uint64 = uint64(0x01) << 32
	DFlagNameMe        uint64 = uint64(0x02) << 32
	DFlagV4NC          uint64 = uint64(0x04) << 32
	DFlagAlias         uint64 = uint64(0x08) << 32
	DFlagLocalExt      uint64 = uint64(0x10) << 32 // internal only
	DFlagAltActSig     uint64 = uint64(0x20) << 32
	DFlagNativeRecords uint64 = uint64(0x40) << 32

	DFlagMandatory25 uint64 = DFlagExtendedReferences |
		DFlagFunTags |
		DFlagExtendedPIDsPorts |
		DFlagUTF8Atoms |
		DFlagNewFunTags |
		DFlagBigCreation |
		DFlagNewFloats |
		DFlagMapTag |
		DFlagExportPtrTag |
		DFlagBitBinaries |
		DFlagHandshake23

	DFlagMandatory26 uint64 = DFlagV4NC | DFlagUnlinkID

	MandatoryFlags uint64 = DFlagMandatory25 | DFlagMandatory26

	// SupportedOptionalFlags lists optional capabilities implemented in v1.
	// SPAWN, NAME_ME, ALTACT_SIG, NATIVE_RECORDS and internal-only flags are intentionally excluded.
	SupportedOptionalFlags uint64 = DFlagPublished |
		DFlagDistMonitor |
		DFlagDistMonitorName |
		DFlagUnicodeIO |
		DFlagDistHdrAtomCache |
		DFlagSmallAtomTags |
		DFlagSendSender |
		DFlagBigSeqtraceLabels |
		DFlagExitPayload |
		DFlagFragments |
		DFlagAlias |
		DFlagMandatory25Digest
)

var (
	ErrMissingMandatoryFlags  = errors.New("dist: missing mandatory capability flags")
	ErrDynamicNameUnsupported = errors.New("dist: dynamic node naming (DFLAG_NAME_ME) is unsupported")
)

// ExpandFlags expands DFLAG_MANDATORY_25_DIGEST into the explicit OTP 25 mandatory flag bits.
func ExpandFlags(flags uint64) uint64 {
	if flags&DFlagMandatory25Digest != 0 {
		flags |= DFlagMandatory25
	}
	return flags
}

// DefaultFlags returns the capability bitmask advertised by a Tao Erlang node.
// Nodes default to hidden (published == false).
func DefaultFlags(published bool) uint64 {
	f := MandatoryFlags |
		DFlagMandatory25Digest |
		DFlagDistMonitor |
		DFlagDistMonitorName |
		DFlagUnicodeIO |
		DFlagDistHdrAtomCache |
		DFlagSmallAtomTags |
		DFlagSendSender |
		DFlagBigSeqtraceLabels |
		DFlagExitPayload |
		DFlagFragments |
		DFlagAlias
	if published {
		f |= DFlagPublished
	}
	return f
}

// NegotiateFlags validates remote capabilities and computes the negotiated intersection.
// Unknown optional flags are never enabled.
func NegotiateFlags(local, remote uint64) (uint64, error) {
	if remote&DFlagNameMe != 0 {
		return 0, ErrDynamicNameUnsupported
	}
	expLocal := ExpandFlags(local) & (MandatoryFlags | SupportedOptionalFlags)
	expRemote := ExpandFlags(remote)
	if expLocal&MandatoryFlags != MandatoryFlags {
		return 0, fmt.Errorf("%w: local missing 0x%x", ErrMissingMandatoryFlags, MandatoryFlags&^expLocal)
	}
	if expRemote&MandatoryFlags != MandatoryFlags {
		return 0, fmt.Errorf("%w: peer missing 0x%x", ErrMissingMandatoryFlags, MandatoryFlags&^expRemote)
	}
	return expLocal & expRemote, nil
}
