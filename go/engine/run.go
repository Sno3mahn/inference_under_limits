// Package engine is the Go hot path: per-model lanes on shards (queues, limiter, dispatch,
// retries, state accounting) driven by an open-loop generator against an in-process provider.
// No HTTP and no persistence: Python covers those. This measures how many requests per second
// the core design can push through while staying inside the limits.
package engine

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/Sno3mahn/inference-under-limits/go/limiter"
)

type Config struct {
	Target      float64       `json:"target_per_s"` // completions/s the limits are sized for
	Headroom    float64       `json:"headroom"`     // 97% caps sum to Target*Headroom (covers retries)
	Offer       float64       `json:"offer"`        // offered load = Target*Offer
	Ceiling     bool          `json:"ceiling"`      // limits out of the way: measure the CPU ceiling
	Models      int           `json:"models"`
	Layout      string        `json:"layout"` // "pinned": a model lives on one shard; "spread": on all of them
	Shards      int           `json:"shards"`
	Generators  int           `json:"generators"`
	Duration    time.Duration `json:"duration_ns"`
	Warmup      time.Duration `json:"warmup_ns"`
	P50         time.Duration `json:"p50_ns"`
	P99         time.Duration `json:"p99_ns"`
	Transient   float64       `json:"transient"`
	Permanent   float64       `json:"permanent"`
	Tokens      int           `json:"tokens"`
	TokenJitter float64       `json:"token_jitter"`
	QueueS      float64       `json:"queue_s"` // per-lane queue bound, in seconds of the lane's rate
	Utilization float64       `json:"utilization"`
	BurstS      float64       `json:"burst_s"`
	MaxAttempts int           `json:"max_attempts"`
	BackoffBase time.Duration `json:"backoff_base_ns"`
	BackoffCap  time.Duration `json:"backoff_cap_ns"`
	BucketNs    int64         `json:"bucket_ns"`
	Seed        uint64        `json:"seed"`
}

func DefaultConfig() Config {
	return Config{
		Target: 300_000, Headroom: 1.03, Offer: 1.10, Models: 64, Layout: "pinned",
		Shards: runtime.GOMAXPROCS(0), Generators: 2, Duration: 75 * time.Second, Warmup: 15 * time.Second,
		P50: 150 * time.Millisecond, P99: 600 * time.Millisecond, Transient: 0.02, Permanent: 0.01,
		Tokens: 1000, TokenJitter: 0.25, QueueS: 2, Utilization: 0.97, BurstS: 1, MaxAttempts: 5,
		BackoffBase: 200 * time.Millisecond, BackoffCap: 10 * time.Second, BucketNs: 10_000_000, Seed: 42,
	}
}

type Limits struct {
	Lanes         int          `json:"lanes"`
	LaneCapPerMin int64        `json:"lane_cap_per_min"`
	TotalCapPerS  float64      `json:"total_cap_per_s"` // sum of the 97% caps
	Models        []ModelLimit `json:"models"`
	OfferedPerS   float64      `json:"offered_per_s"`
}

type Result struct {
	Config      Config         `json:"config"`
	Limits      Limits         `json:"limits"`
	Environment map[string]any `json:"environment"`

	Generated    int64            `json:"generated"`
	InboxDropped int64            `json:"inbox_dropped"` // generator couldn't hand off (should be 0)
	Accepted     int64            `json:"accepted"`
	Rejected     int64            `json:"rejected"` // lane queue full: a 429 in the HTTP version
	Calls        int64            `json:"provider_calls"`
	Retries      int64            `json:"retries"`
	Succeeded    int64            `json:"succeeded"`
	Failed       int64            `json:"failed"`
	NeverFits    int64            `json:"never_fits"`
	AtStop       map[string]int64 `json:"pending_at_stop"`
	AccountingOK bool             `json:"accounting_ok"`

	CompletedPerS      float64            `json:"completed_per_s"` // over [warmup, duration)
	OfferedPerS        float64            `json:"offered_per_s_achieved"`
	CompletedPerSecond []int64            `json:"completed_per_second"`
	GeneratedPerSecond []int64            `json:"generated_per_second"`
	LatencyMs          map[string]float64 `json:"latency_ms"`

	Provider        []ModelCheck `json:"provider_check"`
	ProviderPassed  bool         `json:"provider_passed"`
	MaxPeakFraction float64      `json:"max_peak_rpm_fraction"`

	CPUSeconds float64 `json:"cpu_seconds"`
	CoresUsed  float64 `json:"cores_used"`
	GCCount    uint32  `json:"gc_count"`
	GCPauseMs  float64 `json:"gc_pause_total_ms"`
}

// sizeLimits turns Target into per-lane caps. Every lane gets the same cap, and each lane's limit
// is the smallest one whose 97% cap reaches it, so the caps add up to at least Target*Headroom.
func sizeLimits(cfg *Config) (lanesPerModel int, laneRPM, laneTPM int64, l Limits) {
	lanesPerModel = 1
	if cfg.Layout == "spread" {
		lanesPerModel = cfg.Shards
	}
	l.Lanes = cfg.Models * lanesPerModel
	capPerMin := int64(math.Ceil(cfg.Target * cfg.Headroom * 60 / float64(l.Lanes)))
	laneRPM = int64(math.Ceil(float64(capPerMin)/cfg.Utilization)) + 1
	if cfg.Ceiling {
		laneRPM = 1 << 40
	}
	laneTPM = laneRPM * int64(cfg.Tokens) * 2 // RPM binds; TPM is checked but has room
	l.LaneCapPerMin, _ = limiter.Caps(laneRPM, laneTPM, cfg.Utilization)
	l.TotalCapPerS = float64(l.LaneCapPerMin) * float64(l.Lanes) / 60
	for range cfg.Models {
		l.Models = append(l.Models, ModelLimit{RPM: laneRPM * int64(lanesPerModel), TPM: laneTPM * int64(lanesPerModel)})
	}
	l.OfferedPerS = cfg.Target * cfg.Offer
	return
}

func Run(cfg Config) (*Result, error) {
	if cfg.Layout != "pinned" && cfg.Layout != "spread" {
		return nil, fmt.Errorf("layout must be pinned or spread, got %q", cfg.Layout)
	}
	if cfg.Models < 1 || cfg.Models > math.MaxUint16 || cfg.Shards < 1 || cfg.Generators < 1 {
		return nil, fmt.Errorf("need 1..65535 models, >= 1 shard and >= 1 generator")
	}
	lanesPerModel, laneRPM, laneTPM, limits := sizeLimits(&cfg)
	shardOf := func(model int) int { return model % cfg.Shards }
	serves := func(model, shard int) bool { return lanesPerModel > 1 || shardOf(model) == shard }

	start := time.Now()
	clock := func() int64 { return int64(time.Since(start)) }
	provider := newProvider(&cfg, start, limits.Models, cfg.Shards, serves)

	laneRatePerS := float64(limits.LaneCapPerMin) / 60
	if cfg.Ceiling {
		laneRatePerS = limits.OfferedPerS / float64(limits.Lanes)
	}
	shards := make([]*Shard, cfg.Shards)
	for i := range shards {
		s := &Shard{
			id: i, cfg: &cfg, clock: clock, inbox: make(chan []Req, 8192), provider: provider,
			done:    newWheel(time.Millisecond.Nanoseconds(), 10*cfg.P99.Nanoseconds()+time.Second.Nanoseconds(), 0),
			retries: newWheel(time.Millisecond.Nanoseconds(), cfg.BackoffCap.Nanoseconds()+time.Second.Nanoseconds(), 0),
			rng:     rand.New(rand.NewPCG(cfg.Seed, uint64(i))),
			laneOf:  make([]int, cfg.Models),
		}
		for m := range cfg.Models {
			s.laneOf[m] = -1
			if !serves(m, i) {
				continue
			}
			s.laneOf[m] = len(s.lanes)
			s.lanes = append(s.lanes, &lane{
				model:       uint16(m),
				lim:         limiter.New(laneRPM, laneTPM, cfg.Utilization, cfg.BurstS, cfg.BucketNs, 0),
				queueCap:    max(1000, int(cfg.QueueS*laneRatePerS)),
				maxInFlight: int(laneRatePerS*cfg.P99.Seconds()*4) + 100, // Little's law with room for the tail
			})
		}
		shards[i] = s
	}

	var before syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	var gcBefore runtime.MemStats
	runtime.ReadMemStats(&gcBefore)

	stopShards, stopGen := make(chan struct{}), make(chan struct{})
	var shardWG, genWG sync.WaitGroup
	for _, s := range shards {
		shardWG.Add(1)
		go func() { defer shardWG.Done(); s.run(stopShards) }()
	}
	gens := make([]*generator, cfg.Generators)
	for g := range gens {
		gens[g] = &generator{id: g, cfg: &cfg, rate: limits.OfferedPerS / float64(cfg.Generators),
			clock: clock, shards: shards, shardOf: shardOf, spread: lanesPerModel > 1}
		genWG.Add(1)
		go func() { defer genWG.Done(); gens[g].run(stopGen) }()
	}

	time.Sleep(cfg.Duration)
	close(stopGen)
	genWG.Wait()
	close(stopShards)
	shardWG.Wait()
	elapsed := time.Since(start)

	var after syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	var gcAfter runtime.MemStats
	runtime.ReadMemStats(&gcAfter)

	r := &Result{Config: cfg, Limits: limits, AtStop: map[string]int64{}}
	var lat hist
	for _, g := range gens {
		r.Generated += g.made
		r.InboxDropped += g.dropped
		r.GeneratedPerSecond = addSeries(r.GeneratedPerSecond, g.perSecond)
	}
	for _, s := range shards {
		st := &s.stats
		r.Accepted += st.Accepted
		r.Rejected += st.Rejected
		r.Calls += st.Calls
		r.Retries += st.Retries
		r.Succeeded += st.Succeeded
		r.Failed += st.Failed
		r.NeverFits += st.NeverFits
		r.CompletedPerSecond = addSeries(r.CompletedPerSecond, st.CompletedPerSecond)
		lat.merge(&st.latency)
		q, rw, inf := s.pending()
		r.AtStop["queued"] += q
		r.AtStop["retry_wait"] += rw
		r.AtStop["in_flight"] += inf
		for len(s.inbox) > 0 { // handed off but never admitted
			r.AtStop["in_inbox"] += int64(len(<-s.inbox))
		}
	}
	final := r.Succeeded + r.Failed
	r.AccountingOK = r.Generated == r.Accepted+r.Rejected+r.InboxDropped+r.AtStop["in_inbox"] &&
		r.Accepted == final+r.AtStop["queued"]+r.AtStop["retry_wait"]+r.AtStop["in_flight"]

	from, to := int(cfg.Warmup.Seconds()), int(cfg.Duration.Seconds())
	r.CompletedPerS = meanOver(r.CompletedPerSecond, from, to)
	r.OfferedPerS = meanOver(r.GeneratedPerSecond, from, to)
	r.LatencyMs = map[string]float64{}
	for name, q := range map[string]float64{"p50": 0.5, "p95": 0.95, "p99": 0.99, "p999": 0.999} {
		r.LatencyMs[name] = float64(lat.quantile(q)) / 1e6
	}

	r.Provider = provider.Check()
	r.ProviderPassed = true
	for _, c := range r.Provider {
		r.ProviderPassed = r.ProviderPassed && c.Violations == 0
		r.MaxPeakFraction = max(r.MaxPeakFraction, c.PeakFraction)
	}

	cpu := func(u syscall.Rusage) float64 {
		return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
	}
	r.CPUSeconds = cpu(after) - cpu(before)
	r.CoresUsed = r.CPUSeconds / elapsed.Seconds()
	r.GCCount = gcAfter.NumGC - gcBefore.NumGC
	r.GCPauseMs = float64(gcAfter.PauseTotalNs-gcBefore.PauseTotalNs) / 1e6
	r.Environment = map[string]any{
		"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH,
		"cpus": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0),
	}
	return r, nil
}

func addSeries(dst, src []int64) []int64 {
	for len(dst) < len(src) {
		dst = append(dst, 0)
	}
	for i, v := range src {
		dst[i] += v
	}
	return dst
}

func meanOver(series []int64, from, to int) float64 {
	if to <= from {
		return 0
	}
	var sum int64
	for i := from; i < to && i < len(series); i++ {
		sum += series[i]
	}
	return float64(sum) / float64(to-from)
}

// generator offers load open loop: it works out how many requests are due from elapsed time,
// not from how fast the shards are going, so a slow system can't slow the offered load down
// (no coordinated omission).
type generator struct {
	id        int
	cfg       *Config
	rate      float64
	clock     func() int64
	shards    []*Shard
	shardOf   func(int) int
	spread    bool
	made      int64
	dropped   int64
	perSecond []int64
}

func (g *generator) run(stop <-chan struct{}) {
	rng := rand.New(rand.NewPCG(g.cfg.Seed, 1_000+uint64(g.id)))
	batches := make([][]Req, len(g.shards))
	rr := g.id
	idBase := uint64(g.id) << 48
	jitter := int(float64(g.cfg.Tokens) * g.cfg.TokenJitter)
	t0 := g.clock()
	for {
		select {
		case <-stop:
			return
		default:
		}
		now := g.clock()
		due := int64(g.rate*float64(now-t0)/1e9) - g.made
		for i := range due {
			m := rng.IntN(g.cfg.Models)
			sh := g.shardOf(m)
			if g.spread {
				sh = rr % len(g.shards)
				rr++
			}
			tokens := g.cfg.Tokens
			if jitter > 0 {
				tokens += rng.IntN(2*jitter+1) - jitter
			}
			batches[sh] = append(batches[sh], Req{ID: idBase + uint64(g.made+i), Submitted: now,
				Tokens: int32(max(tokens, 1)), Model: uint16(m)})
		}
		for sh, b := range batches {
			if len(b) == 0 {
				continue
			}
			select {
			case g.shards[sh].inbox <- b:
			default:
				g.dropped += int64(len(b))
			}
			batches[sh] = nil
		}
		if due > 0 {
			g.made += due
			sec := int(now / time.Second.Nanoseconds())
			for len(g.perSecond) <= sec {
				g.perSecond = append(g.perSecond, 0)
			}
			g.perSecond[sec] += due
		}
		time.Sleep(500 * time.Microsecond)
	}
}
