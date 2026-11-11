package dist

import "sync"

// Size-bucketed byte buffer pool (§12.2) covering small control frames,
// 63 KiB / 64 KiB payload boundaries, and 64 KiB + envelope headers without
// repeatedly growing/discarding 4 KiB buffers.
var bucketCaps = [...]int{
	1024,        // 1 KiB: control & small replies
	8 << 10,     // 8 KiB
	65 << 10,    // 65 KiB: covers 63 KiB, 64 KiB, and 64 KiB + dist/packet-4 headers!
	256 << 10,   // 256 KiB
	1 << 20,     // 1 MiB
	4<<20 + 512, // 4 MiB + header slack
}

var bucketPools = [...]sync.Pool{
	{New: func() any { b := make([]byte, 0, bucketCaps[0]); return &b }},
	{New: func() any { b := make([]byte, 0, bucketCaps[1]); return &b }},
	{New: func() any { b := make([]byte, 0, bucketCaps[2]); return &b }},
	{New: func() any { b := make([]byte, 0, bucketCaps[3]); return &b }},
	{New: func() any { b := make([]byte, 0, bucketCaps[4]); return &b }},
	{New: func() any { b := make([]byte, 0, bucketCaps[5]); return &b }},
}

// AcquireBuffer returns an empty byte slice with capacity >= hint when within pooled tiers.
func AcquireBuffer(hint int) *[]byte {
	for i, c := range bucketCaps {
		if hint <= c {
			bp := bucketPools[i].Get().(*[]byte)
			*bp = (*bp)[:0]
			return bp
		}
	}
	b := make([]byte, 0, hint)
	return &b
}

// ReleaseBuffer returns bp to the matching size-tier pool if its capacity fits a bounded tier.
func ReleaseBuffer(bp *[]byte) {
	if bp == nil {
		return
	}
	c := cap(*bp)
	for i, tierCap := range bucketCaps {
		if c == tierCap {
			*bp = (*bp)[:0]
			bucketPools[i].Put(bp)
			return
		}
	}
}
