package actor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/sllt/tao/taonet"
)

func TestDirectPacketEncodingMatchesExistingWireAndOwnsBody(t *testing.T) {
	for _, codec := range []MessageCodec{CBORCodec{}, JSONCodec{}, struct{ MessageCodec }{CBORCodec{}}} {
		r, err := newRegistry([]MessageType{{"blob", 1, []byte(nil)}}, 128<<10, codec)
		if err != nil {
			t.Fatal(err)
		}
		for _, size := range []int{0, 64, 1024, 4096, 64 << 10} {
			value := bytes.Repeat([]byte{0, 1, 255, 42}, size/4)
			packet := fullPacket()
			packet.Message, err = r.Encode(value)
			if err != nil {
				t.Fatal(err)
			}
			want, err := appendPacket([]byte("prefix"), packet)
			if err != nil {
				t.Fatal(err)
			}
			got, encoded, err := r.encodePacket(append(make([]byte, 0, 4096), "prefix"...), packet, value)
			if err != nil || !bytes.Equal(got, want) || !reflect.DeepEqual(encoded, packet.Message) {
				t.Fatalf("%s size=%d: direct encoding changed wire: %v", codec.Name(), size, err)
			}
			decoded, err := r.Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			clear(got)
			if !bytes.Equal(decoded.([]byte), value) {
				t.Fatal("decoded body retained the reused packet buffer")
			}
			if !bytes.Equal(encoded.Data, make([]byte, len(encoded.Data))) {
				t.Fatal("encoded body must alias the final packet, even after growth")
			}
		}
		data, encoded, err := r.encodePacket(nil, actorPacket{Kind: "reply"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		packet, err := unmarshalPacket(data)
		if err != nil || len(encoded.Data) != 0 || packet.Message.Type != "" {
			t.Fatalf("nil message: %+v %v", packet, err)
		}
		if _, _, err := r.encodePacket(nil, actorPacket{Kind: "send"}, "unregistered"); !errors.Is(err, ErrUnregisteredType) {
			t.Fatal(err)
		}
		if _, _, err := r.encodePacket(nil, actorPacket{Kind: "send"}, make([]byte, 128<<10+1)); !errors.Is(err, ErrMessageTooLarge) {
			t.Fatal(err)
		}
	}
}

type failingAppendCodec struct {
	CBORCodec
	crash bool
}

func (c failingAppendCodec) MarshalAppend(dst []byte, _ any) ([]byte, error) {
	dst = append(dst, 0)
	if c.crash {
		panic("codec failed after writing a prefix")
	}
	return dst, errors.New("codec failed after writing a prefix")
}

func TestAppendFailureReturnsEncodingReservation(t *testing.T) {
	for _, crash := range []bool{false, true} {
		r, err := newRegistry([]MessageType{{"text", 1, ""}}, 128, failingAppendCodec{crash: crash})
		if err != nil {
			t.Fatal(err)
		}
		w := &network{registry: r, opts: NetworkOptions{SendBytes: 16384, SendMessages: 1, MaxMessageBytes: 128}, packetBuffers: make(chan *packetBuffer, 1)}
		if err := w.queueMessage("peer", taonet.ClassInteractive, actorPacket{Kind: "send"}, "body", PID{}, nil, nil); !errors.Is(err, ErrInvalidMessage) {
			t.Fatal(err)
		}
		if w.messages != 0 || w.bytes != 0 || len(w.packetBuffers) != 1 {
			t.Fatalf("failed encoding leaked reservation: messages=%d bytes=%d cache=%d", w.messages, w.bytes, len(w.packetBuffers))
		}
	}
}

func BenchmarkPacketEncoding(b *testing.B) {
	r, err := NewRegistry([]MessageType{{"text", 1, ""}}, 128<<10)
	if err != nil {
		b.Fatal(err)
	}
	packet := actorPacket{Kind: "call", Target: wireAddress{Kind: "pid", PID: &PID{Node: "server", BootID: [16]byte{1}, ID: 1}}, Key: requestKey{"client", [16]byte{2}, 1}, Sequence: 1, BudgetMS: 5000}
	for _, size := range []int{64, 1024, 65536} {
		value := strings.Repeat("x", size)
		b.Run(fmt.Sprintf("%dB/separate", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				message, err := r.Encode(value)
				if err != nil {
					b.Fatal(err)
				}
				packet.Message = message
				if _, err := marshalPacket(packet); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("%dB/shared", size), func(b *testing.B) {
			w := &network{opts: NetworkOptions{SendBytes: 8 << 20}, packetBuffers: make(chan *packetBuffer, 1)}
			b.ReportAllocs()
			for b.Loop() {
				buffer := w.takePacketBuffer()
				data, _, err := r.encodePacket(buffer.data, packet, value)
				if err != nil {
					b.Fatal(err)
				}
				buffer.data = data
				w.putPacketBuffer(buffer)
			}
		})
	}
}

func TestPacketCacheDropsLargeBackingArraysAndJobAliases(t *testing.T) {
	w := &network{opts: NetworkOptions{SendBytes: 8 << 20}, packetBuffers: make(chan *packetBuffer, 1)}
	buffer := w.takePacketBuffer()
	buffer.data = make([]byte, 1<<20)
	job := &networkJob{buffer: buffer, raw: buffer.data, packet: actorPacket{Message: EncodedMessage{Data: buffer.data[10:]}}}
	w.releaseJobBuffer(job)
	if job.buffer != nil || job.raw != nil || job.packet.Message.Data != nil {
		t.Fatal("finished job retained the reusable buffer")
	}
	reused := w.takePacketBuffer()
	if reused != buffer || len(reused.data) != 0 || cap(reused.data) != 4096 {
		t.Fatal("cache retained a large array or lost its original storage")
	}
	w.putPacketBuffer(reused)
	w.putPacketBuffer(&packetBuffer{})
	if len(w.packetBuffers) != 1 {
		t.Fatal("cache exceeded its bound")
	}
}

func TestNetworkBudgetChargesOwnedBuffersAndRedirectCopies(t *testing.T) {
	registry, err := NewRegistry([]MessageType{{"text", 1, ""}}, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"send", "reply", "global"} {
		for _, size := range []int{32, 64 << 10} {
			t.Run(fmt.Sprintf("%s/%d", mode, size), func(t *testing.T) {
				class := taonet.ClassInteractive
				packet := actorPacket{Kind: "send", Target: wireAddress{Kind: "pid", PID: &PID{Node: "peer", BootID: [16]byte{1}, ID: 1}}}
				if mode == "reply" {
					class = taonet.ClassReply
					packet = actorPacket{Kind: "reply", Key: requestKey{Caller: "peer", Boot: [16]byte{1}, ID: 1}}
				} else if mode == "global" {
					packet.Target = wireAddress{Kind: "global", Route: &RouteRecord{Path: "worker", Owner: "peer", BootID: [16]byte{1}, ProcessID: 1, Epoch: 1, Revision: 1, State: RouteActive}}
				}
				key := pipeKey{peer: "peer", class: class}
				pipe := &actorPipe{key: key, wake: make(chan struct{}, 1)}
				w := &network{registry: registry, opts: NetworkOptions{MaxMessageBytes: 128 << 10, SendBytes: 1 << 20, SendMessages: 1}, pipes: map[pipeKey]*actorPipe{key: pipe}, packetBuffers: make(chan *packetBuffer, 1)}
				// An existing pipe keeps the encoded job queued until the test
				// releases it, so no network scheduling affects the ledger.
				if err := w.queueMessage("peer", class, packet, strings.Repeat("x", size), PID{}, nil, nil); err != nil {
					t.Fatal(err)
				}
				job := pipe.jobAt(0)
				want := cap(job.raw)
				if job.buffer != nil && &job.raw[0] != &job.buffer.storage[0] {
					want += len(job.buffer.storage)
				}
				if mode == "global" {
					want += len(job.raw)
				}
				if w.bytes != want || w.messages != 1 {
					t.Fatalf("buffer reservation: bytes=%d want=%d messages=%d", w.bytes, want, w.messages)
				}
				pipe.dropJobs(1)
				w.finishJob(job, nil)
				if w.bytes != 0 || w.messages != 0 {
					t.Fatal("completed write retained its reservation")
				}
			})
		}
	}
}

type heldPacketReceipt struct {
	done, released chan struct{}
}

func (r heldPacketReceipt) Done() <-chan struct{} { return r.done }
func (r heldPacketReceipt) IsDone() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}
func (r heldPacketReceipt) Released() <-chan struct{}  { return r.released }
func (r heldPacketReceipt) Wait(context.Context) error { return context.Canceled }

func TestLargePacketReturnsToCacheOnlyAfterInputRelease(t *testing.T) {
	w := &network{opts: NetworkOptions{SendBytes: 8 << 20, SendMessages: 1}, packetBuffers: make(chan *packetBuffer, 1)}
	buffer := w.takePacketBuffer()
	buffer.data = bytes.Repeat([]byte{42}, 64<<10)
	original := buffer.data
	job := &networkJob{buffer: buffer, raw: original, charge: len(buffer.storage) + cap(original)}
	if err := w.reserve(job.charge); err != nil {
		t.Fatal(err)
	}
	settled := make(chan struct{})
	job.onComplete = func(error) { close(settled) }
	receipt := heldPacketReceipt{make(chan struct{}), make(chan struct{})}
	release := sync.OnceFunc(func() { close(receipt.released) })
	defer release()
	close(receipt.done)
	finished := make(chan struct{})
	go func() {
		w.finishPending(&pendingWrite{job: job, ctx: t.Context(), receipt: receipt})
		close(finished)
	}()
	signal(t, settled)
	if len(w.packetBuffers) != 0 {
		t.Fatal("completion returned an input still in use")
	}
	other := w.takePacketBuffer()
	if other == buffer {
		t.Fatal("in-flight backing array reused")
	}
	clear(other.data[:cap(other.data)])
	if original[0] != 42 {
		t.Fatal("in-flight body overwritten")
	}
	release()
	signal(t, finished)
	reused := w.takePacketBuffer()
	if reused != buffer || len(reused.data) != 0 || &reused.data[:1][0] != &original[0] {
		t.Fatal("released large encoding was not retained")
	}
	if w.packetCacheBytes.Load() != 0 || w.bytes != 0 || w.messages != 0 {
		t.Fatal("released encoding left cache or send accounting behind")
	}
}

func TestPacketCacheBoundsConcurrentGrowthAndSmallErrors(t *testing.T) {
	w := &network{opts: NetworkOptions{SendBytes: 8 << 20}, packetBuffers: make(chan *packetBuffer, networkCacheEntries)}
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			for range 20 {
				buffer := w.takePacketBuffer()
				if cap(buffer.data) < 64<<10 {
					buffer.data = make([]byte, 64<<10)
				}
				buffer.data = buffer.data[:64<<10]
				buffer.data[0]++
				w.putPacketBuffer(buffer)
			}
		})
	}
	group.Wait()
	expectedBytes := w.packetCacheBytes.Load()
	var bytes int
	for len(w.packetBuffers) > 0 {
		buffer := w.takePacketBuffer()
		bytes += buffer.retainedBytes()
	}
	if int64(bytes) != expectedBytes || bytes > packetCacheBudget(w.opts.SendBytes) || w.packetCacheBytes.Load() != 0 {
		t.Fatalf("cache accounting: bytes=%d charged=%d", bytes, expectedBytes)
	}
	registry, err := NewRegistry(nil, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	w.registry = registry
	w.opts.MaxMessageBytes, w.opts.SendBytes, w.opts.SendMessages = 128<<10, 1<<20, 1
	key := pipeKey{"peer", taonet.ClassReply}
	pipe := &actorPipe{key: key, wake: make(chan struct{}, 1)}
	w.pipes = map[pipeKey]*actorPipe{key: pipe}
	// Use a smaller cache so a growth array fits the configured idle budget.
	w.packetBuffers = make(chan *packetBuffer, 1)
	buffer := w.takePacketBuffer()
	buffer.data = make([]byte, 64<<10)
	w.putPacketBuffer(buffer)
	packet := actorPacket{Kind: "reject", Key: requestKey{Caller: "peer", Boot: [16]byte{1}, ID: 1}, Error: &wireActorError{"capacity", "full"}}
	if err := w.queueMessage("peer", taonet.ClassReply, packet, nil, PID{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	job := pipe.jobAt(0)
	if w.bytes != packetInlineBytes || cap(job.raw) != packetInlineBytes {
		t.Fatal("small rejection retained a large send reservation")
	}
	pipe.dropJobs(1)
	w.finishJob(job, nil)
	full := &packetBuffer{data: make([]byte, 64<<10)}
	w.putPacketBuffer(full)
	if len(w.packetBuffers) != 1 || w.packetCacheBytes.Load() != packetInlineBytes {
		t.Fatal("full cache retained an unqueued reservation")
	}
}

func TestPacketCacheRetainsOverlappingLargeEncodersWithinByteBudget(t *testing.T) {
	w := &network{opts: NetworkOptions{SendBytes: 8 << 20}, packetBuffers: make(chan *packetBuffer, networkCacheEntries)}
	// All twenty encoders fit in the 2MiB idle budget, including their embedded
	// 4KiB arrays. Unused slots must not consume another 1MiB of that budget.
	var buffers [20]*packetBuffer
	for i := range buffers {
		buffers[i] = w.takePacketBuffer()
		buffers[i].data = make([]byte, 64<<10)
	}
	for _, buffer := range buffers {
		w.putPacketBuffer(buffer)
	}
	var retained int
	for range buffers {
		buffer := w.takePacketBuffer()
		retained += buffer.retainedBytes()
		if cap(buffer.data) < 64<<10 {
			t.Error("growth array discarded while actual idle storage fits the budget")
		}
	}
	if retained > packetCacheBudget(w.opts.SendBytes) {
		t.Fatalf("idle cache exceeded budget: %d", retained)
	}
}
