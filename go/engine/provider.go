package engine

import (
	"math"
	"math/rand/v2"
	"time"
)

// Provider is the in-process simulator. It decides each call's outcome and latency, and it
// records every call it receives against its own clock, so the run can be checked afterwards
// without trusting the gateway's numbers.
//
// Calls are recorded per (model, shard) "stripe", so shards never write to the same memory.
// The check sums the stripes, so a model split across shards is checked against its one global limit.
type Provider struct {
	start     time.Time
	bucketNs  int64
	nBuckets  int
	limits    []ModelLimit
	stripes   [][]*stripe // [model][shard], nil where that shard doesn't serve the model
	mu, sigma float64
	maxLatNs  float64
	transient float64
	permanent float64
}

type ModelLimit struct {
	RPM int64 `json:"rpm"`
	TPM int64 `json:"tpm"`
}

type stripe struct{ reqs, toks []int64 }

func newProvider(cfg *Config, start time.Time, limits []ModelLimit, shards int, serves func(model, shard int) bool) *Provider {
	p := &Provider{
		start:     start,
		bucketNs:  cfg.BucketNs,
		nBuckets:  int((cfg.Duration+10*time.Second).Nanoseconds()/cfg.BucketNs) + 1,
		limits:    limits,
		mu:        math.Log(float64(cfg.P50.Nanoseconds())),
		sigma:     (math.Log(float64(cfg.P99.Nanoseconds())) - math.Log(float64(cfg.P50.Nanoseconds()))) / 2.3263,
		maxLatNs:  10 * float64(cfg.P99.Nanoseconds()),
		transient: cfg.Transient,
		permanent: cfg.Permanent,
	}
	p.stripes = make([][]*stripe, len(limits))
	for m := range limits {
		p.stripes[m] = make([]*stripe, shards)
		for s := range shards {
			if serves(m, s) {
				p.stripes[m][s] = &stripe{reqs: make([]int64, p.nBuckets), toks: make([]int64, p.nBuckets)}
			}
		}
	}
	return p
}

// Call is one provider request from a shard. The shard's own RNG keeps it lock-free.
func (p *Provider) Call(model, shard int, tokens int32, rng *rand.Rand) (outcome, int64) {
	b := min(int(int64(time.Since(p.start))/p.bucketNs), p.nBuckets-1)
	st := p.stripes[model][shard]
	st.reqs[b]++
	st.toks[b] += int64(tokens)

	lat := int64(math.Min(math.Exp(p.mu+p.sigma*rng.NormFloat64()), p.maxLatNs))
	switch u := rng.Float64(); {
	case u < p.permanent:
		return permanent, lat
	case u < p.permanent+p.transient:
		return transient, lat
	default:
		return ok, lat
	}
}

// ModelCheck is the provider's view of one model: the busiest trailing-60 s window it saw.
type ModelCheck struct {
	Model        int     `json:"model"`
	RPM          int64   `json:"rpm"`
	TPM          int64   `json:"tpm"`
	Calls        int64   `json:"calls"`
	PeakRequests int64   `json:"peak_requests_60s"`
	PeakTokens   int64   `json:"peak_tokens_60s"`
	PeakFraction float64 `json:"peak_rpm_fraction"`
	Violations   int     `json:"violating_windows"`
}

// Check slides a window of n+1 buckets (n = 60 s / bucket) over the recorded calls. Any real
// window (t-60s, t] fits inside n+1 consecutive buckets, so a pass here means the real rule held.
// Note that it's checked against the provider's full limit, not the gateway's 97% target.
func (p *Provider) Check() []ModelCheck {
	span := int(60*time.Second.Nanoseconds()/p.bucketNs) + 1
	out := make([]ModelCheck, len(p.limits))
	for m, lim := range p.limits {
		reqs, toks := make([]int64, p.nBuckets), make([]int64, p.nBuckets)
		for _, st := range p.stripes[m] {
			if st == nil {
				continue
			}
			for b := range p.nBuckets {
				reqs[b] += st.reqs[b]
				toks[b] += st.toks[b]
			}
		}
		c := ModelCheck{Model: m, RPM: lim.RPM, TPM: lim.TPM}
		var r, t int64
		for b := range p.nBuckets {
			c.Calls += reqs[b]
			r, t = r+reqs[b], t+toks[b]
			if b >= span {
				r, t = r-reqs[b-span], t-toks[b-span]
			}
			c.PeakRequests, c.PeakTokens = max(c.PeakRequests, r), max(c.PeakTokens, t)
			if r > lim.RPM || t > lim.TPM {
				c.Violations++
			}
		}
		c.PeakFraction = float64(c.PeakRequests) / float64(lim.RPM)
		out[m] = c
	}
	return out
}
