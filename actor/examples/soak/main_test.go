package main

import (
	"testing"
	"time"
)

func TestHistogramQuantileUpperBoundsAndMerge(t *testing.T) {
	var whole histogram
	var parts [2]histogram
	for i := range 100 {
		elapsed := 10 * time.Microsecond
		if i >= 95 {
			elapsed = time.Millisecond
		}
		if i == 99 {
			elapsed = 100 * time.Millisecond
		}
		whole.add(elapsed)
		parts[i%2].add(elapsed)
	}
	got := whole.report()
	if got.Count != 100 || got.P50US != 10 || got.P95US != 10 || got.P99US != 1023 || got.MaxUS != 100000 {
		t.Fatalf("quantile upper bounds: %+v", got)
	}
	parts[0].merge(&parts[1])
	if merged := parts[0].report(); merged != got {
		t.Fatalf("merged histogram: %+v, want %+v", merged, got)
	}
	var rounded histogram
	rounded.add(time.Nanosecond)
	if got := rounded.report(); got.P50US != 1 || got.MaxUS != 1 {
		t.Fatalf("microsecond round-up: %+v", got)
	}
}
