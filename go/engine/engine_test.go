package engine

import (
	"testing"
	"time"
)

func short(layout string, models int) Config {
	cfg := DefaultConfig()
	cfg.Target, cfg.Models, cfg.Layout, cfg.Shards = 20_000, models, layout, 4
	cfg.Duration, cfg.Warmup = 3*time.Second, time.Second
	cfg.P50, cfg.P99 = 5*time.Millisecond, 20*time.Millisecond
	return cfg
}

func check(t *testing.T, r *Result) {
	t.Helper()
	if !r.AccountingOK {
		t.Fatalf("accounting: generated %d accepted %d rejected %d dropped %d final %d pending %v",
			r.Generated, r.Accepted, r.Rejected, r.InboxDropped, r.Succeeded+r.Failed, r.AtStop)
	}
	if !r.ProviderPassed {
		t.Fatalf("provider saw a window over the limit: %+v", r.Provider)
	}
	if r.Succeeded == 0 || r.Failed == 0 || r.Retries == 0 {
		t.Fatalf("expected successes, failures and retries: %d / %d / %d", r.Succeeded, r.Failed, r.Retries)
	}
}

func TestPinnedLayoutAccountsForEveryRequest(t *testing.T) {
	r, err := Run(short("pinned", 8))
	if err != nil {
		t.Fatal(err)
	}
	check(t, r)
}

// With 3x the capacity offered, the queues stay full and the limiter is what decides the rate.
// Each lane's pacer starts with one second of credit, so over a short run the provider may see
// up to cap x (duration + 1 s). Less than ~90% of cap x duration would mean the limiter starves.
func TestSaturatedLimiterHoldsTheCapAndUsesIt(t *testing.T) {
	cfg := short("pinned", 8)
	cfg.Offer, cfg.Duration = 3, 4*time.Second
	r, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	check(t, r)
	capPerS := r.Limits.TotalCapPerS
	lo, hi := capPerS*cfg.Duration.Seconds()*0.9, capPerS*(cfg.Duration.Seconds()+cfg.BurstS)*1.02
	if c := float64(r.Calls); c < lo || c > hi {
		t.Fatalf("provider calls %d, want between %.0f and %.0f (caps sum to %.0f/s)", r.Calls, lo, hi, capPerS)
	}
}

func TestSpreadLayoutSplitsOneModelsLimitAcrossShards(t *testing.T) {
	r, err := Run(short("spread", 1))
	if err != nil {
		t.Fatal(err)
	}
	check(t, r)
	if r.Limits.Lanes != 4 {
		t.Fatalf("one model over 4 shards should be 4 lanes, got %d", r.Limits.Lanes)
	}
}

func TestOverloadIsRejectedNotQueuedForever(t *testing.T) {
	cfg := short("pinned", 4)
	cfg.Offer, cfg.QueueS = 3, 0.1
	r, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	check(t, r)
	if r.Rejected == 0 {
		t.Fatal("3x the capacity with a short queue should reject some requests")
	}
}

func TestHistogramQuantilesAreWithinTwoPercent(t *testing.T) {
	var h hist
	for v := int64(1); v <= 100_000; v++ {
		h.add(v * 1_000)
	}
	for _, q := range []float64{0.5, 0.99} {
		want := q * 100_000 * 1_000
		if got := float64(h.quantile(q)); got < want*0.98 || got > want*1.02 {
			t.Fatalf("q%.2f = %.0f, want ~%.0f", q, got, want)
		}
	}
}

func TestWheelDeliversEverythingInOrderOfTicks(t *testing.T) {
	w := newWheel(1_000_000, 100_000_000, 0)
	for i := range int64(500) {
		w.add((i%90)*1_000_000+1, entry{req: Req{ID: uint64(i)}})
	}
	var seen int
	last := int64(-1)
	for now := int64(0); now <= 200_000_000; now += 3_000_000 {
		w.advance(now, func(e entry) {
			due := (int64(e.req.ID)%90)*1_000_000 + 1
			if due > now {
				t.Fatalf("entry due at %d delivered at %d", due, now)
			}
			last = max(last, due)
			seen++
		})
	}
	if seen != 500 || w.count != 0 {
		t.Fatalf("delivered %d of 500, %d left", seen, w.count)
	}
}
