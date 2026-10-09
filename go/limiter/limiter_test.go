package limiter

import (
	"math/rand/v2"
	"testing"
)

const (
	ms = int64(1_000_000)
	s  = 1000 * ms
)

type send struct{ t, tokens int64 }

type change struct {
	at       int64
	rpm, tpm int64
}

// saturate offers the limiter far more than it allows: every millisecond it sends until refused.
func saturate(l *Limiter, until int64, tokens func() int64, changes []change) []send {
	var out []send
	next := tokens()
	for now := int64(0); now < until; now += ms {
		for len(changes) > 0 && changes[0].at <= now {
			l.SetLimits(changes[0].rpm, changes[0].tpm, now)
			changes = changes[1:]
		}
		for l.TryAcquire(now, next) == 0 {
			out = append(out, send{now, next})
			next = tokens()
		}
	}
	return out
}

// violations checks the exact rule on the real send times, independently of the buckets:
// at every send t, sends in (t-60s, t] must be within the caps in effect at t.
func violations(sends []send, caps func(t int64) (int64, int64)) int {
	bad, lo := 0, 0
	var tokens int64
	for hi, sd := range sends {
		tokens += sd.tokens
		for sends[lo].t <= sd.t-WindowNs {
			tokens -= sends[lo].tokens
			lo++
		}
		r, t := caps(sd.t)
		if int64(hi-lo+1) > r || tokens > t {
			bad++
		}
	}
	return bad
}

func between(sends []send, from, to int64) int {
	n := 0
	for _, sd := range sends {
		if sd.t >= from && sd.t < to {
			n++
		}
	}
	return n
}

func TestSaturatedLaneStaysWithinTheWindowAndUsesIt(t *testing.T) {
	l := New(30_000, 1<<40, 0.97, 1, 10*ms, 0)
	sends := saturate(l, 180*s, func() int64 { return 1 }, nil)
	if v := violations(sends, func(int64) (int64, int64) { return l.CapR, l.CapT }); v != 0 {
		t.Fatalf("%d window violations", v)
	}
	// The pacer spreads the 29,100 cap over the minute: every full minute should be close to it.
	for m := int64(0); m < 3; m++ {
		if n := between(sends, m*60*s, (m+1)*60*s); n < 28_900 || n > 29_100 {
			t.Fatalf("minute %d: %d sends, want ~29,100", m, n)
		}
	}
}

func TestLimitDropPausesUntilOldSendsExpire(t *testing.T) {
	l := New(30_000, 1<<40, 0.97, 1, 10*ms, 0)
	changes := []change{{120 * s, 5_000, 1 << 40}, {270 * s, 30_000, 1 << 40}}
	sends := saturate(l, 400*s, func() int64 { return 1 }, changes)
	capsAt := func(at int64) (int64, int64) {
		rpm := int64(30_000)
		if at >= 120*s && at < 270*s {
			rpm = 5_000
		}
		return Caps(rpm, 1<<40, 0.97)
	}
	if v := violations(sends, capsAt); v != 0 {
		t.Fatalf("%d window violations", v)
	}
	// About 29k sends from the last minute must age out below 4,850 before anything goes out.
	if n := between(sends, 120*s, 165*s); n != 0 {
		t.Fatalf("%d sends during the drain pause", n)
	}
	if n := between(sends, 200*s, 260*s); n < 4_700 {
		t.Fatalf("only %d sends in a minute at the new limit (cap 4,850)", n)
	}
	if n := between(sends, 300*s, 360*s); n < 28_900 {
		t.Fatalf("only %d sends in a minute after the raise", n)
	}
}

func TestTokenLimitBindsWithMixedSizes(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	l := New(1_000_000, 2_000_000, 0.97, 1, 10*ms, 0)
	sends := saturate(l, 150*s, func() int64 { return 100 + rng.Int64N(4_000) }, nil)
	if v := violations(sends, func(int64) (int64, int64) { return l.CapR, l.CapT }); v != 0 {
		t.Fatalf("%d window violations", v)
	}
	var tokens int64
	for _, sd := range sends {
		if sd.t >= 60*s && sd.t < 120*s {
			tokens += sd.tokens
		}
	}
	if tokens < l.CapT*98/100 {
		t.Fatalf("minute 2 used %d tokens of a %d cap", tokens, l.CapT)
	}
}

func TestBucketingNeverAllowsABurstAcrossABoundary(t *testing.T) {
	// No pacer effect (huge burst): the window alone has to stop a second minute's worth of sends
	// right after the first, however the sends fall across bucket edges.
	l := New(600, 1<<40, 1, 60, 10*ms, 0)
	var sends []send
	for now := int64(0); now < 200*s; now += 7 * ms {
		for l.TryAcquire(now, 1) == 0 {
			sends = append(sends, send{now, 1})
		}
	}
	if v := violations(sends, func(int64) (int64, int64) { return 600, 1 << 40 }); v != 0 {
		t.Fatalf("%d window violations", v)
	}
}

func TestRequestsThatCanNeverFitAreRefused(t *testing.T) {
	l := New(100, 1_000, 0.97, 1, 10*ms, 0)
	if w := l.TryAcquire(0, 971); w != -1 {
		t.Fatalf("token count above the cap: wait %d, want -1", w)
	}
	z := New(0, 1_000, 0.97, 1, 10*ms, 0)
	if w := z.TryAcquire(0, 1); w != -1 {
		t.Fatalf("zero RPM: wait %d, want -1", w)
	}
}

func TestCapsRoundDownButNeverToZero(t *testing.T) {
	if r, tk := Caps(50_000, 100_000_000, 0.97); r != 48_500 || tk != 97_000_000 {
		t.Fatalf("caps %d %d", r, tk)
	}
	if r, _ := Caps(1, 1, 0.97); r != 1 {
		t.Fatalf("cap of a 1 RPM limit is %d", r)
	}
}
