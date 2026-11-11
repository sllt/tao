package dist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/sllt/tao/erlang/etf"
)

const (
	DefaultFragmentSize       = 64 << 10 // 64 KiB (matches OTP ERTS_DIST_FRAGMENT_SIZE)
	DefaultMaxPayloadBytes    = 4 << 20  // 4 MiB single message encoded size
	DefaultMaxControlOverhead = 8 << 10  // bounded header + control overhead
	DefaultMaxReassemblyBytes = 16 << 20 // 16 MiB per-peer reassembly budget
	DefaultMaxActiveFragments = 64       // max concurrent fragment sequences per peer
	DefaultFragmentTimeout    = 30 * time.Second
)

var (
	ErrFrameTooLarge             = errors.New("dist: packet exceeds frame size limit")
	ErrFragmentProtocol          = errors.New("dist: invalid fragment sequence")
	ErrFragmentBudgetExceeded    = errors.New("dist: fragment reassembly budget exceeded")
	ErrFragmentTimeout           = errors.New("dist: incomplete fragment sequence timed out")
	ErrInterleavedSenderFragment = errors.New("dist: sender interleaved message before finishing fragment sequence")
)

// PacketKind classifies the outcome of reading one packet-4 frame from a connected peer.
type PacketKind uint8

const (
	PacketTick PacketKind = iota
	PacketFragmentPending
	PacketMessage
)

// InboundMessage is a complete reassembled distribution control + optional payload message.
type InboundMessage struct {
	Control      ControlMessage
	Payload      etf.Term
	WireBytes    int
	DecodedBytes int64
}

type fragSequence struct {
	seqID        uint64
	expectedFrag uint64
	atoms        []etf.Atom
	control      ControlMessage
	decoder      etf.DistDecoder
	payload      []byte
	started      time.Time
}

// FrameReaderConfig configures bounded packet-4 reading, atom cache, and fragment reassembly.
type FrameReaderConfig struct {
	LocalNode          etf.Atom
	RemoteNode         etf.Atom
	NegotiatedFlags    uint64
	MaxPayloadBytes    int
	MaxReassemblyBytes int64
	MaxActiveFragments int
	FragmentTimeout    time.Duration
	DecodeLimits       etf.Limits
}

// FrameReader is owned exclusively by a single peer reader goroutine.
type FrameReader struct {
	cfg             FrameReaderConfig
	recvCache       RecvAtomCache
	ctrlState       ControlState
	seqs            map[uint64]*fragSequence
	activeSenders   map[etf.PID]uint64
	reassemblyBytes int64
}

// NewFrameReader creates a connected packet-4 reader for one peer generation.
func NewFrameReader(cfg FrameReaderConfig) *FrameReader {
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if cfg.MaxReassemblyBytes <= 0 {
		cfg.MaxReassemblyBytes = DefaultMaxReassemblyBytes
	}
	if cfg.MaxActiveFragments <= 0 {
		cfg.MaxActiveFragments = DefaultMaxActiveFragments
	}
	if cfg.FragmentTimeout <= 0 {
		cfg.FragmentTimeout = DefaultFragmentTimeout
	}
	if cfg.DecodeLimits.MaxFrameBytes <= 0 {
		cfg.DecodeLimits.MaxFrameBytes = cfg.MaxPayloadBytes + DefaultMaxControlOverhead
	}
	return &FrameReader{
		cfg: cfg,
		ctrlState: ControlState{
			LocalNode:       cfg.LocalNode,
			RemoteNode:      cfg.RemoteNode,
			NegotiatedFlags: cfg.NegotiatedFlags,
		},
		seqs:          make(map[uint64]*fragSequence),
		activeSenders: make(map[etf.PID]uint64),
	}
}

// ReassemblyBytes returns the current bytes held in unfinished fragment sequences.
func (r *FrameReader) ReassemblyBytes() int64 {
	return r.reassemblyBytes
}

// ActiveSequences returns the number of incomplete fragment sequences currently buffered.
func (r *FrameReader) ActiveSequences() int {
	return len(r.seqs)
}

// CheckTimeouts fails if any buffered fragment sequence has exceeded FragmentTimeout.
func (r *FrameReader) CheckTimeouts(now time.Time) error {
	for _, seq := range r.seqs {
		if now.Sub(seq.started) > r.cfg.FragmentTimeout {
			return fmt.Errorf("%w: seq=%d", ErrFragmentTimeout, seq.seqID)
		}
	}
	return nil
}

// ReadPacket reads and processes a single packet-4 frame from conn.
func (r *FrameReader) ReadPacket(conn *Conn) (PacketKind, InboundMessage, error) {
	var lenBuf [4]byte
	if err := conn.ReadFull(lenBuf[:]); err != nil {
		return PacketTick, InboundMessage{}, err
	}
	now := time.Now()
	if err := r.CheckTimeouts(now); err != nil {
		return PacketTick, InboundMessage{}, err
	}
	pktLen := binary.BigEndian.Uint32(lenBuf[:])
	if pktLen == 0 {
		return PacketTick, InboundMessage{}, nil
	}
	maxFrame := uint64(r.cfg.MaxPayloadBytes + DefaultMaxControlOverhead)
	if uint64(pktLen) > maxFrame {
		return PacketTick, InboundMessage{}, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, pktLen, maxFrame)
	}

	bp := AcquireBuffer(int(pktLen))
	defer ReleaseBuffer(bp)
	buf := (*bp)[:int(pktLen)]
	if err := conn.ReadFull(buf); err != nil {
		return PacketTick, InboundMessage{}, err
	}
	return r.ProcessFrame(buf, now)
}

// ProcessFrame parses a single non-zero packet-4 payload (exposed for deterministic unit/fuzz testing).
func (r *FrameReader) ProcessFrame(frame []byte, now time.Time) (PacketKind, InboundMessage, error) {
	if err := r.CheckTimeouts(now); err != nil {
		return PacketTick, InboundMessage{}, err
	}
	maxFrame := r.cfg.MaxPayloadBytes + DefaultMaxControlOverhead
	if len(frame) > maxFrame {
		return PacketTick, InboundMessage{}, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, len(frame), maxFrame)
	}
	if len(frame) == 0 {
		return PacketTick, InboundMessage{}, nil
	}

	if r.cfg.NegotiatedFlags&DFlagDistHdrAtomCache == 0 {
		// Pass-through mode: [112][131 + Control][optional 131 + Payload]
		if frame[0] != PassThroughTag {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: expected pass-through 112, got %d", ErrMalformedDistHeader, frame[0])
		}
		dec := etf.NewDistDecoder(nil, true, r.cfg.DecodeLimits)
		ctrlTerm, rest, err := dec.DecodeNext(frame[1:])
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		ctrl, err := r.ctrlState.ParseInbound(ctrlTerm)
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		var payload etf.Term
		if NeedsPayload(ctrl.Op) {
			payload, rest, err = dec.DecodeNext(rest)
			if err != nil {
				return PacketTick, InboundMessage{}, err
			}
			if IsPayloadReason(ctrl.Op) {
				ctrl.Reason = payload
				payload = nil
			}
		}
		if len(rest) != 0 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: %d trailing bytes in pass-through frame", ErrMalformedDistHeader, len(rest))
		}
		return PacketMessage, InboundMessage{
			Control:      ctrl,
			Payload:      payload,
			WireBytes:    len(frame),
			DecodedBytes: dec.UsedBytes(),
		}, nil
	}

	if len(frame) < 2 || frame[0] != etf.Version {
		return PacketTick, InboundMessage{}, fmt.Errorf("%w: expected dist header 131, got %v", ErrMalformedDistHeader, frame[0])
	}

	switch frame[1] {
	case DistHeaderNormal:
		atoms, rest, err := r.recvCache.ParseRefs(frame[2:])
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		dec := etf.NewDistDecoder(atoms, false, r.cfg.DecodeLimits)
		ctrlTerm, rest, err := dec.DecodeNext(rest)
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		ctrl, err := r.ctrlState.ParseInbound(ctrlTerm)
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		if ctrl.HasFrom {
			if _, busy := r.activeSenders[ctrl.FromPID]; busy {
				return PacketTick, InboundMessage{}, ErrInterleavedSenderFragment
			}
		}
		var payload etf.Term
		if NeedsPayload(ctrl.Op) {
			payload, rest, err = dec.DecodeNext(rest)
			if err != nil {
				return PacketTick, InboundMessage{}, err
			}
			if IsPayloadReason(ctrl.Op) {
				ctrl.Reason = payload
				payload = nil
			}
		}
		if len(rest) != 0 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: %d trailing bytes in normal dist frame", ErrMalformedDistHeader, len(rest))
		}
		return PacketMessage, InboundMessage{
			Control:      ctrl,
			Payload:      payload,
			WireBytes:    len(frame),
			DecodedBytes: dec.UsedBytes(),
		}, nil

	case DistHeaderFragStart:
		if r.cfg.NegotiatedFlags&DFlagFragments == 0 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: fragment start without DFLAG_FRAGMENTS", ErrFragmentProtocol)
		}
		if len(frame) < 18 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: truncated fragment start header", ErrFragmentProtocol)
		}
		seqID := binary.BigEndian.Uint64(frame[2:10])
		fragID := binary.BigEndian.Uint64(frame[10:18])
		if fragID == 0 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: zero FragmentId", ErrFragmentProtocol)
		}
		if _, exists := r.seqs[seqID]; exists {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: duplicate active SequenceId %d", ErrFragmentProtocol, seqID)
		}
		atoms, rest, err := r.recvCache.ParseRefs(frame[18:])
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		dec := etf.NewDistDecoder(atoms, false, r.cfg.DecodeLimits)
		// Entire control message MUST be decodable in the starting fragment.
		ctrlTerm, rest, err := dec.DecodeNext(rest)
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		ctrl, err := r.ctrlState.ParseInbound(ctrlTerm)
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		if !NeedsPayload(ctrl.Op) {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: control op %d has no fragmentable payload", ErrFragmentProtocol, ctrl.Op)
		}
		if ctrl.HasFrom {
			if _, busy := r.activeSenders[ctrl.FromPID]; busy {
				return PacketTick, InboundMessage{}, ErrInterleavedSenderFragment
			}
		}
		if fragID == 1 {
			payload, rem, err := dec.DecodeNext(rest)
			if err != nil {
				return PacketTick, InboundMessage{}, err
			}
			if len(rem) != 0 {
				return PacketTick, InboundMessage{}, fmt.Errorf("%w: trailing bytes in single-fragment message", ErrFragmentProtocol)
			}
			if IsPayloadReason(ctrl.Op) {
				ctrl.Reason = payload
				payload = nil
			}
			return PacketMessage, InboundMessage{
				Control:      ctrl,
				Payload:      payload,
				WireBytes:    len(frame),
				DecodedBytes: dec.UsedBytes(),
			}, nil
		}
		if len(r.seqs) >= r.cfg.MaxActiveFragments {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: active sequences >= %d", ErrFragmentBudgetExceeded, r.cfg.MaxActiveFragments)
		}
		if len(rest) > r.cfg.MaxPayloadBytes {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: fragment payload exceeds %d", ErrFrameTooLarge, r.cfg.MaxPayloadBytes)
		}
		if int64(len(rest)) > r.cfg.MaxReassemblyBytes-r.reassemblyBytes {
			return PacketTick, InboundMessage{}, ErrFragmentBudgetExceeded
		}
		buf := make([]byte, len(rest))
		copy(buf, rest)
		r.reassemblyBytes += int64(len(buf))
		r.seqs[seqID] = &fragSequence{
			seqID:        seqID,
			expectedFrag: fragID - 1,
			atoms:        atoms,
			control:      ctrl,
			decoder:      dec,
			payload:      buf,
			started:      now,
		}
		if ctrl.HasFrom {
			r.activeSenders[ctrl.FromPID] = seqID
		}
		return PacketFragmentPending, InboundMessage{}, nil

	case DistHeaderFragCont:
		if r.cfg.NegotiatedFlags&DFlagFragments == 0 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: fragment continuation without DFLAG_FRAGMENTS", ErrFragmentProtocol)
		}
		if len(frame) < 18 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: truncated fragment continuation header", ErrFragmentProtocol)
		}
		seqID := binary.BigEndian.Uint64(frame[2:10])
		fragID := binary.BigEndian.Uint64(frame[10:18])
		seq, ok := r.seqs[seqID]
		if !ok {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: unknown SequenceId %d", ErrFragmentProtocol, seqID)
		}
		if fragID == 0 || fragID != seq.expectedFrag {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: SequenceId %d got FragmentId %d, expected %d", ErrFragmentProtocol, seqID, fragID, seq.expectedFrag)
		}
		chunk := frame[18:]
		if len(seq.payload)+len(chunk) > r.cfg.MaxPayloadBytes {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: reassembled payload exceeds %d", ErrFrameTooLarge, r.cfg.MaxPayloadBytes)
		}
		if int64(len(chunk)) > r.cfg.MaxReassemblyBytes-r.reassemblyBytes {
			return PacketTick, InboundMessage{}, ErrFragmentBudgetExceeded
		}
		seq.payload = append(seq.payload, chunk...)
		r.reassemblyBytes += int64(len(chunk))
		if fragID > 1 {
			seq.expectedFrag = fragID - 1
			return PacketFragmentPending, InboundMessage{}, nil
		}

		// Final fragment (fragID == 1): detach sequence, release reassembly bytes, decode payload.
		delete(r.seqs, seqID)
		if seq.control.HasFrom {
			delete(r.activeSenders, seq.control.FromPID)
		}
		r.reassemblyBytes -= int64(len(seq.payload))

		payload, rem, err := seq.decoder.DecodeNext(seq.payload)
		if err != nil {
			return PacketTick, InboundMessage{}, err
		}
		if len(rem) != 0 {
			return PacketTick, InboundMessage{}, fmt.Errorf("%w: %d trailing bytes in reassembled payload", ErrFragmentProtocol, len(rem))
		}
		ctrl := seq.control
		if IsPayloadReason(ctrl.Op) {
			ctrl.Reason = payload
			payload = nil
		}
		return PacketMessage, InboundMessage{
			Control:      ctrl,
			Payload:      payload,
			WireBytes:    len(seq.payload),
			DecodedBytes: seq.decoder.UsedBytes(),
		}, nil

	default:
		return PacketTick, InboundMessage{}, fmt.Errorf("%w: unsupported dist header tag %d", ErrMalformedDistHeader, frame[1])
	}
}

// FrameEncoderConfig configures outbound packet-4 frame encoding for a single peer writer.
type FrameEncoderConfig struct {
	NegotiatedFlags uint64
	FragmentSize    int
	MaxPayloadBytes int
	EncodeLimits    etf.Limits
}

// FrameEncoder is owned exclusively by a single peer writer goroutine.
type FrameEncoder struct {
	cfg       FrameEncoderConfig
	sendCache *SendAtomCache
	nextSeqID uint64
}

// NewFrameEncoder creates an outbound distribution frame encoder.
func NewFrameEncoder(cfg FrameEncoderConfig) *FrameEncoder {
	if cfg.FragmentSize <= 0 {
		cfg.FragmentSize = DefaultFragmentSize
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if cfg.EncodeLimits.MaxFrameBytes <= 0 {
		cfg.EncodeLimits.MaxFrameBytes = cfg.MaxPayloadBytes + DefaultMaxControlOverhead
	}
	return &FrameEncoder{
		cfg:       cfg,
		sendCache: NewSendAtomCache(),
	}
}

var tickPacket = [4]byte{0, 0, 0, 0}

// AppendTick appends the 4-byte zero-length tick packet.
func AppendTick(dst []byte) []byte {
	return append(dst, tickPacket[:]...)
}

// EncodeFrames encodes control + optional payload into one or more length-prefixed packet-4 frames
// appended to pooled buffers. On error, sendCache is left unmodified.
func (e *FrameEncoder) EncodeFrames(msg ControlMessage, payload etf.Term) ([]*[]byte, error) {
	ctrlTuple, wirePayload, hasPayload, err := BuildOutbound(msg, payload, e.cfg.NegotiatedFlags)
	if err != nil {
		return nil, err
	}

	if e.cfg.NegotiatedFlags&DFlagDistHdrAtomCache == 0 {
		// Pass-through: [len:4][112][131 + ctrl][optional 131 + payload]
		bp := AcquireBuffer(256)
		*bp = append(*bp, 0, 0, 0, 0, PassThroughTag)
		*bp, err = etf.AppendDistTerm(*bp, ctrlTuple, nil, true, e.cfg.EncodeLimits)
		if err != nil {
			ReleaseBuffer(bp)
			return nil, err
		}
		if hasPayload {
			*bp, err = etf.AppendDistTerm(*bp, wirePayload, nil, true, e.cfg.EncodeLimits)
			if err != nil {
				ReleaseBuffer(bp)
				return nil, err
			}
		}
		frameLen := len(*bp) - 4
		if frameLen > e.cfg.MaxPayloadBytes+DefaultMaxControlOverhead {
			ReleaseBuffer(bp)
			return nil, ErrFrameTooLarge
		}
		binary.BigEndian.PutUint32((*bp)[0:4], uint32(frameLen))
		return []*[]byte{bp}, nil
	}

	// Stage atom cache refs while encoding control and payload into temporary body buffers.
	e.sendCache.BeginMessage()
	ctrlBufPtr := AcquireBuffer(128)
	*ctrlBufPtr, err = etf.AppendDistTerm(*ctrlBufPtr, ctrlTuple, e.sendCache, false, e.cfg.EncodeLimits)
	if err != nil {
		ReleaseBuffer(ctrlBufPtr)
		return nil, err
	}

	var payBufPtr *[]byte
	if hasPayload {
		payBufPtr = AcquireBuffer(1024)
		*payBufPtr, err = etf.AppendDistTerm(*payBufPtr, wirePayload, e.sendCache, false, e.cfg.EncodeLimits)
		if err != nil {
			ReleaseBuffer(ctrlBufPtr)
			ReleaseBuffer(payBufPtr)
			return nil, err
		}
		if len(*payBufPtr) > e.cfg.MaxPayloadBytes {
			ReleaseBuffer(ctrlBufPtr)
			ReleaseBuffer(payBufPtr)
			return nil, fmt.Errorf("%w: payload %d > %d", ErrFrameTooLarge, len(*payBufPtr), e.cfg.MaxPayloadBytes)
		}
	}
	defer func() {
		ReleaseBuffer(ctrlBufPtr)
		if payBufPtr != nil {
			ReleaseBuffer(payBufPtr)
		}
	}()

	ctrlBytes := *ctrlBufPtr
	var payBytes []byte
	if payBufPtr != nil {
		payBytes = *payBufPtr
	}

	useFrag := hasPayload &&
		e.cfg.NegotiatedFlags&DFlagFragments != 0 &&
		len(payBytes) > e.cfg.FragmentSize

	if !useFrag {
		outPtr := AcquireBuffer(4 + 64 + len(ctrlBytes) + len(payBytes))
		*outPtr = append(*outPtr, 0, 0, 0, 0, etf.Version, DistHeaderNormal)
		*outPtr = e.sendCache.CommitRefs(*outPtr)
		*outPtr = append(*outPtr, ctrlBytes...)
		*outPtr = append(*outPtr, payBytes...)
		binary.BigEndian.PutUint32((*outPtr)[0:4], uint32(len(*outPtr)-4))
		return []*[]byte{outPtr}, nil
	}

	e.nextSeqID++
	seqID := e.nextSeqID
	fragSize := e.cfg.FragmentSize
	totalFrags := (len(payBytes) + fragSize - 1) / fragSize
	frames := make([]*[]byte, 0, totalFrags)

	firstChunk := fragSize
	if firstChunk > len(payBytes) {
		firstChunk = len(payBytes)
	}
	firstPtr := AcquireBuffer(4 + 18 + 64 + len(ctrlBytes) + firstChunk)
	var startHdr [22]byte // len(4) + 131 + 69 + seqID(8) + fragID(8)
	startHdr[4] = etf.Version
	startHdr[5] = DistHeaderFragStart
	binary.BigEndian.PutUint64(startHdr[6:14], seqID)
	binary.BigEndian.PutUint64(startHdr[14:22], uint64(totalFrags))
	*firstPtr = append(*firstPtr, startHdr[:]...)
	*firstPtr = e.sendCache.CommitRefs(*firstPtr)
	*firstPtr = append(*firstPtr, ctrlBytes...)
	*firstPtr = append(*firstPtr, payBytes[:firstChunk]...)
	binary.BigEndian.PutUint32((*firstPtr)[0:4], uint32(len(*firstPtr)-4))
	frames = append(frames, firstPtr)

	offset := firstChunk
	for fragID := totalFrags - 1; fragID >= 1; fragID-- {
		end := offset + fragSize
		if end > len(payBytes) {
			end = len(payBytes)
		}
		chunk := payBytes[offset:end]
		offset = end

		contPtr := AcquireBuffer(4 + 18 + len(chunk))
		var contHdr [22]byte // len(4) + 131 + 70 + seqID(8) + fragID(8)
		contHdr[4] = etf.Version
		contHdr[5] = DistHeaderFragCont
		binary.BigEndian.PutUint64(contHdr[6:14], seqID)
		binary.BigEndian.PutUint64(contHdr[14:22], uint64(fragID))
		*contPtr = append(*contPtr, contHdr[:]...)
		*contPtr = append(*contPtr, chunk...)
		binary.BigEndian.PutUint32((*contPtr)[0:4], uint32(len(*contPtr)-4))
		frames = append(frames, contPtr)
	}
	return frames, nil
}
