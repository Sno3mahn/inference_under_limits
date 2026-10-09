package engine

import (
	"math/rand/v2"
	"runtime"
	"time"

	"github.com/Sno3mahn/inference-under-limits/go/limiter"
)

// lane is one model's queue and limiter on one shard. In the pinned layout a model has one lane
// with its whole limit; in the spread layout it has a lane on every shard with 1/shards of it.
type lane struct {
	model       uint16
	lim         *limiter.Limiter
	fresh       fifo[Req] // new requests, bounded by queueCap
	retry       fifo[Req] // retries whose backoff has passed; served before fresh work
	queueCap    int
	inFlight    int
	maxInFlight int
	nextTry     int64 // the limiter said not before this
}

// shardStats are written only by the shard's goroutine and read after it stops.
type shardStats struct {
	Accepted, Rejected int64
	Calls, Retries     int64
	Succeeded, Failed  int64
	NeverFits          int64
	CompletedPerSecond []int64
	latency            hist
}

// Shard is one goroutine that owns a set of lanes. Nothing in it is shared, so it takes no locks.
// Requests arrive in batches on inbox; everything else happens inside run.
type Shard struct {
	id        int
	cfg       *Config
	clock     func() int64
	inbox     chan []Req
	lanes     []*lane
	laneOf    []int // model -> local lane index, -1 if this shard doesn't serve it
	done      *wheel
	retries   *wheel
	provider  *Provider
	rng       *rand.Rand
	now       int64
	inFlight  int
	retryWait int
	stats     shardStats
}

func (s *Shard) run(stop <-chan struct{}) {
	tick := time.Millisecond.Nanoseconds()
	for {
		select {
		case <-stop:
			return
		default:
		}
		s.now = s.clock()
		work := false

	inbox:
		for range 64 {
			select {
			case batch := <-s.inbox:
				s.admit(batch)
				work = true
			default:
				break inbox
			}
		}
		if s.done.advance(s.now, s.complete) > 0 {
			work = true
		}
		s.retries.advance(s.now, s.retryDue)

		wake := s.now + tick
		for _, l := range s.lanes {
			sent, next := s.dispatch(l)
			work = work || sent > 0
			wake = min(wake, next)
		}
		if !work {
			if d := wake - s.clock(); d > 20_000 {
				time.Sleep(time.Duration(d))
			} else {
				runtime.Gosched()
			}
		}
	}
}

func (s *Shard) admit(batch []Req) {
	for _, r := range batch {
		li := s.laneOf[r.Model]
		l := s.lanes[li]
		if l.fresh.len() >= l.queueCap {
			s.stats.Rejected++ // the HTTP gateway would answer 429 + Retry-After here
			continue
		}
		r.Lane = uint16(li)
		l.fresh.push(r)
		s.stats.Accepted++
	}
}

// dispatch sends from one lane until the limiter or the in-flight cap says stop. It returns
// how many it sent and when the lane next wants attention.
func (s *Shard) dispatch(l *lane) (int, int64) {
	if s.now < l.nextTry {
		return 0, l.nextTry
	}
	sent := 0
	for l.inFlight < l.maxInFlight {
		q := &l.retry
		if q.len() == 0 {
			q = &l.fresh
		}
		if q.len() == 0 {
			return sent, s.now + time.Millisecond.Nanoseconds()
		}
		w := l.lim.TryAcquire(s.now, int64(q.peek().Tokens))
		if w < 0 { // larger than the lane's whole cap: fail it rather than block the lane
			s.finish(q.pop(), false)
			s.stats.NeverFits++
			continue
		}
		if w > 0 {
			l.nextTry = s.now + w
			return sent, l.nextTry
		}
		s.send(l, q.pop())
		sent++
	}
	return sent, s.now + time.Millisecond.Nanoseconds() // in-flight cap: completions free slots
}

func (s *Shard) send(l *lane, r Req) {
	r.Attempts++
	o, lat := s.provider.Call(int(l.model), s.id, r.Tokens, s.rng)
	s.stats.Calls++
	l.inFlight++
	s.inFlight++
	s.done.add(s.now+lat, entry{req: r, outcome: o})
}

func (s *Shard) complete(e entry) {
	l := s.lanes[e.req.Lane]
	l.inFlight--
	s.inFlight--
	switch {
	case e.outcome == ok:
		s.finish(e.req, true)
	case e.outcome == transient && int(e.req.Attempts) < s.cfg.MaxAttempts:
		// Exponential backoff with full jitter, as in Python.
		ceiling := min(s.cfg.BackoffCap.Nanoseconds(), s.cfg.BackoffBase.Nanoseconds()<<(e.req.Attempts-1))
		s.retries.add(s.now+1+s.rng.Int64N(ceiling), e)
		s.retryWait++
		s.stats.Retries++
	default: // permanent, or transient with no attempts left
		s.finish(e.req, false)
	}
}

func (s *Shard) retryDue(e entry) {
	s.retryWait--
	s.lanes[e.req.Lane].retry.push(e.req)
}

// finish is the one place a request becomes final, so each request is counted exactly once.
func (s *Shard) finish(r Req, succeeded bool) {
	if succeeded {
		s.stats.Succeeded++
	} else {
		s.stats.Failed++
	}
	sec := int(s.now / time.Second.Nanoseconds())
	for len(s.stats.CompletedPerSecond) <= sec {
		s.stats.CompletedPerSecond = append(s.stats.CompletedPerSecond, 0)
	}
	s.stats.CompletedPerSecond[sec]++
	s.stats.latency.add(s.now - r.Submitted)
}

// pending counts what isn't final yet, for the accounting check after the shard stops.
func (s *Shard) pending() (queued, retryWait, inFlight int64) {
	for _, l := range s.lanes {
		queued += int64(l.fresh.len())
		retryWait += int64(l.retry.len())
	}
	return queued, retryWait + int64(s.retryWait), int64(s.inFlight)
}
