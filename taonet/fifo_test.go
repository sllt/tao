package taonet

import "testing"

func TestFIFOOrderAcrossWrapGrowthAndIdleRetention(t *testing.T) {
	var q fifo[*int]
	next, want := 0, 0
	push := func(n int) {
		for range n {
			value := next
			q.push(&value)
			next++
		}
	}
	pop := func(n int) {
		for range n {
			if *q.peek() != want || *q.pop() != want {
				t.Fatalf("lost FIFO ordering at %d", want)
			}
			want++
		}
	}
	push(16)
	pop(11)
	push(27)
	pop(19)
	push(80)
	pop(q.len())
	if q.buffer != nil {
		t.Fatal("large idle queue kept its backing array")
	}
	for range 100 {
		push(8)
		pop(3)
		push(3)
		pop(8)
	}
	for _, value := range q.buffer {
		if value != nil {
			t.Fatal("idle queue retained a delivery")
		}
	}
}
