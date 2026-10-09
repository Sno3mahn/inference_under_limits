// Package limiter keeps one lane within its provider limits.
//
// The rule is the same as the Python gateway's (docs/ARCHITECTURE.md §4.1): at every send t,
// the requests and tokens sent in (t-60s, t] must be within the caps. Only the bookkeeping
// differs. Python keeps one entry per send, which at 1M sends/s would be 60M entries. Here
// sends are counted in fixed-size buckets (10 ms by default), and a bucket leaves the window
// only once its *end* is more than 60 s old. Treating every send as if it happened at the end
// of its bucket can only over-count, so the rule still holds. The cost is at most one bucket
// of window capacity.
//
// All times are integer nanoseconds on a monotonic clock supplied by the caller.
package limiter

import "math"

// WindowNs is the provider's limit window.
const WindowNs int64 = 60_000_000_000

// Window counts requests and tokens over the trailing 60 s in fixed-size buckets.
//
// Bucket j covers [j*d, (j+1)*d). It is expired once (j+1)*d <= now-60s, so at any moment the
// live buckets are cur-n .. cur (n+1 of them, where n = 60s/d), and the ring holds n+1 slots.
type Window struct {
	bucketNs   int64
	size       int64 // ring slots: n+1
	reqs, toks []int64
	newest     int64 // absolute index of the newest bucket the ring has been advanced to
	sumR, sumT int64
}

func NewWindow(bucketNs, now int64) *Window {
	size := WindowNs/bucketNs + 1
	return &Window{bucketNs: bucketNs, size: size, reqs: make([]int64, size), toks: make([]int64, size),
		newest: now / bucketNs}
}

// advance clears every slot whose bucket has expired by now.
func (w *Window) advance(now int64) {
	cur := now / w.bucketNs
	if cur <= w.newest {
		return
	}
	if cur-w.newest >= w.size { // idle for a whole window: everything expired
		clear(w.reqs)
		clear(w.toks)
		w.sumR, w.sumT = 0, 0
	} else {
		// Slot j%size last held bucket j-size, which is expired once cur >= j.
		for j := w.newest + 1; j <= cur; j++ {
			i := j % w.size
			w.sumR -= w.reqs[i]
			w.sumT -= w.toks[i]
			w.reqs[i], w.toks[i] = 0, 0
		}
	}
	w.newest = cur
}

// Usage returns the requests and tokens still inside the window at now.
func (w *Window) Usage(now int64) (int64, int64) {
	w.advance(now)
	return w.sumR, w.sumT
}

// Record counts one send of tokens at now.
func (w *Window) Record(now, tokens int64) {
	w.advance(now)
	i := (now / w.bucketNs) % w.size
	w.reqs[i]++
	w.toks[i] += tokens
	w.sumR++
	w.sumT += tokens
}

// UntilNextBucket is how long until the next bucket boundary, the earliest moment anything can expire.
func (w *Window) UntilNextBucket(now int64) int64 {
	return (now/w.bucketNs+1)*w.bucketNs - now
}

// Pacer spreads sends evenly: a token bucket refilled at limit/60 per second that holds at most
// burstS seconds of refill. Without it, a whole minute's budget could go out in the first second.
// A request costing more than the bucket can hold is let through when the bucket is full and
// drives the level negative (debt), so it can't be starved forever.
type Pacer struct {
	ratePerNs float64
	capacity  float64
	level     float64
	last      int64
}

func NewPacer(perMinute, burstS float64, now int64) *Pacer {
	p := &Pacer{last: now}
	p.SetRate(perMinute, burstS, now)
	p.level = p.capacity
	return p
}

// SetRate refills at the old rate up to now, then switches to the new one.
func (p *Pacer) SetRate(perMinute, burstS float64, now int64) {
	p.refill(now)
	p.ratePerNs = perMinute / 60 / 1e9
	p.capacity = math.Max(perMinute/60*burstS, 1)
	p.level = math.Min(p.level, p.capacity)
}

func (p *Pacer) refill(now int64) {
	if now > p.last {
		p.level = math.Min(p.capacity, p.level+float64(now-p.last)*p.ratePerNs)
		p.last = now
	}
}

// WaitNs returns 0 if cost can be taken now, otherwise how long until it can (-1: never).
func (p *Pacer) WaitNs(now int64, cost float64) int64 {
	p.refill(now)
	need := math.Min(cost, p.capacity)
	if p.level >= need {
		return 0
	}
	if p.ratePerNs <= 0 {
		return -1
	}
	return max(int64(math.Ceil((need-p.level)/p.ratePerNs)), 1)
}

func (p *Pacer) Take(cost float64) { p.level -= cost }

// Caps applies the utilization target: floor(limit * utilization), never rounded below 1.
func Caps(rpm, tpm int64, utilization float64) (int64, int64) {
	f := func(limit int64) int64 {
		if limit <= 0 {
			return 0
		}
		return max(int64(float64(limit)*utilization), 1)
	}
	return f(rpm), f(tpm)
}

// Limiter is the window plus two pacers (requests, tokens) for one lane.
type Limiter struct {
	CapR, CapT  int64
	utilization float64
	burstS      float64
	win         *Window
	reqs, toks  *Pacer
}

func New(rpm, tpm int64, utilization, burstS float64, bucketNs, now int64) *Limiter {
	l := &Limiter{utilization: utilization, burstS: burstS, win: NewWindow(bucketNs, now)}
	l.CapR, l.CapT = Caps(rpm, tpm, utilization)
	l.reqs = NewPacer(float64(l.CapR), burstS, now)
	l.toks = NewPacer(float64(l.CapT), burstS, now)
	return l
}

// SetLimits changes the limits live. Sends already in the window keep counting, so after a drop
// the lane waits until enough of them expire.
func (l *Limiter) SetLimits(rpm, tpm, now int64) {
	l.CapR, l.CapT = Caps(rpm, tpm, l.utilization)
	l.reqs.SetRate(float64(l.CapR), l.burstS, now)
	l.toks.SetRate(float64(l.CapT), l.burstS, now)
}

// TryAcquire records one send of tokens at now if it fits and returns 0. Otherwise it returns
// how long to wait before trying again, or -1 if the request can never fit.
func (l *Limiter) TryAcquire(now, tokens int64) int64 {
	if l.CapR == 0 || tokens > l.CapT {
		return -1
	}
	r, t := l.win.Usage(now)
	if r+1 > l.CapR || t+tokens > l.CapT {
		return l.win.UntilNextBucket(now)
	}
	wr, wt := l.reqs.WaitNs(now, 1), l.toks.WaitNs(now, float64(tokens))
	if wr < 0 || wt < 0 {
		return -1
	}
	if w := max(wr, wt); w > 0 {
		return w
	}
	l.reqs.Take(1)
	l.toks.Take(float64(tokens))
	l.win.Record(now, tokens)
	return 0
}

// Usage returns the requests and tokens in the trailing window.
func (l *Limiter) Usage(now int64) (int64, int64) { return l.win.Usage(now) }
