package engine

import "math/bits"

// Req is one request. It holds no pointers, so queues of them cost the garbage collector nothing.
type Req struct {
	ID        uint64
	Submitted int64 // ns since the run started
	Tokens    int32
	Model     uint16
	Lane      uint16 // index into the owning shard's lanes
	Attempts  uint8
}

type outcome uint8

const (
	ok outcome = iota
	transient
	permanent
)

type entry struct {
	req     Req
	outcome outcome
}

// fifo is a growable ring buffer.
type fifo[T any] struct {
	buf     []T
	head, n int
}

func (q *fifo[T]) len() int { return q.n }

func (q *fifo[T]) push(v T) {
	if q.n == len(q.buf) {
		grown := make([]T, max(16, 2*len(q.buf)))
		for i := range q.n {
			grown[i] = q.buf[(q.head+i)%len(q.buf)]
		}
		q.buf, q.head = grown, 0
	}
	q.buf[(q.head+q.n)%len(q.buf)] = v
	q.n++
}

func (q *fifo[T]) peek() *T { return &q.buf[q.head] }

func (q *fifo[T]) pop() T {
	v := q.buf[q.head]
	q.head = (q.head + 1) % len(q.buf)
	q.n--
	return v
}

// wheel is a timing wheel: a ring of 1 ms slots, like a clock face. Adding an entry is an append
// to the slot for its due time; advancing walks the slots that have come due. That's O(1) per
// entry, where a heap of ~100k in-flight requests would cost ~17 comparisons each way.
type wheel struct {
	tickNs int64
	slots  [][]entry
	last   int64 // last tick processed
	count  int
}

func newWheel(tickNs, span, now int64) *wheel {
	return &wheel{tickNs: tickNs, slots: make([][]entry, span/tickNs+1), last: now / tickNs}
}

func (w *wheel) add(due int64, e entry) {
	t := (due + w.tickNs - 1) / w.tickNs                    // round up: an entry may come out late, never early
	t = min(max(t, w.last+1), w.last+int64(len(w.slots))-1) // never in the past, never past one turn
	i := t % int64(len(w.slots))
	w.slots[i] = append(w.slots[i], e)
	w.count++
}

// advance hands every entry due by now to fn. fn must not add to this wheel.
func (w *wheel) advance(now int64, fn func(entry)) int {
	cur := now / w.tickNs
	end := min(cur, w.last+int64(len(w.slots))) // one full turn visits every slot
	n := 0
	for t := w.last + 1; t <= end; t++ {
		s := &w.slots[t%int64(len(w.slots))]
		for _, e := range *s {
			fn(e)
		}
		n += len(*s)
		*s = (*s)[:0]
	}
	if cur > w.last {
		w.last = cur
	}
	w.count -= n
	return n
}

// hist is a log-linear latency histogram: 64 sub-buckets per power of two (about 1.5% precision),
// no floating point on the hot path.
type hist struct{ counts [64 * 58]int64 }

func histIndex(v uint64) int {
	if v < 64 {
		return int(v)
	}
	e := bits.Len64(v) - 7 // keep the top 7 bits: 1 leading bit + 6 sub-bucket bits
	return 64*(e+1) + int((v>>e)&63)
}

func histValue(i int) uint64 {
	if i < 64 {
		return uint64(i)
	}
	e := i/64 - 1
	return uint64(64+i%64) << e
}

func (h *hist) add(ns int64) { h.counts[histIndex(uint64(max(ns, 0)))]++ }

func (h *hist) merge(o *hist) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
}

func (h *hist) quantile(q float64) int64 {
	var total int64
	for _, c := range h.counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	rank := int64(q*float64(total-1)) + 1
	var seen int64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			return int64(histValue(i))
		}
	}
	return int64(histValue(len(h.counts) - 1))
}
