# Findings log

Things measured or discovered while building. These feed the benchmark report.
Machine: Apple Silicon, 12 cores, 36 GB, Python 3.14, single process each for the gateway and the simulator.

## 2026-10-07: simulator capacity
- One simulator process (aiohttp, access log off) handles about **9,800 calls/s** with 2,000 concurrent
  clients and 150 ms latency. Scenario 1 needs ~833/s, so the simulator is not the bottleneck.
- The first measurement read 581/s. That was wrong: `timeout` outcomes hang for 30 s and kept the
  run open after the 8 s of load. Lesson: report rate as completed calls ÷ active load time.

## 2026-10-07: gateway core against the real simulator, 50k RPM / 100M TPM, 1k-token requests
- Offered ~1,200 req/s for 75 s (above the 833/s capacity).
- The gateway's trailing-60 s count settles at exactly **48,500** (= 97% target) and holds there.
- Achieved about **818 req/s** with about 127 calls in flight (Little's law: 818/s × 0.155 s mean latency ≈ 127).
- **The provider's own trailing-window peak was 48,567**: above our 48,500 target, below the 50,000 limit.
  The gateway stamps a send when it leaves and the provider stamps it when it arrives. Network
  jitter moves some arrivals closer together, so the provider sees slightly more in a window.
  This is what the 3% utilisation margin is for. With a 100% target we would have produced provider 429s.

## 2026-10-07: scenario 3 dry run (real processes, before the load generator existed)
- 10,000 × 1k-token requests split across model-a (30k RPM) and model-b (20k RPM); 3% transient 503s,
  0.2% timeouts, 1% permanent failures; the receiver answers 503 to the first two callbacks.
- Ack: **202 in 68 ms**. The handler validates 10k items, reserves queue room, and commits 10k rows in one
  SQLite transaction before replying.
- Batch complete at ~19.8 s (5k requests per model at ~485/s and ~323/s, plus timeouts held for the
  10 s gateway timeout before retrying). Callback attempts at +19.9 s (503), +20.9 s (503), +22.8 s (200).
- The callback body was byte-identical on all three attempts. Its counts matched `GET /v1/batches/{id}` exactly
  (9,886 succeeded / 114 failed / 0 expired). Results: 10,000 rows, 10,000 unique ids.
- SQLite: 10,006 writes in 3,692 commits. Group commit already merges about 3 operations per commit at this rate.

## 2026-10-07: bug found, outcome keyed on request_id
- The simulator first derived each request's random outcome from `request_id`. Request ids are only
  unique *within a batch*, so with a load generator reusing `r0..r119` per batch there were only
  120 distinct outcomes, and no transient failures appeared at all.
- Fix: key on the idempotency key (`batch_id:request_id`), which the gateway makes globally unique.
  Covered by `test_same_request_id_in_different_batches_gets_independent_outcomes`.
- General rule: never key anything global on a client-supplied id that is only scoped per batch.

## 2026-10-07: scenario 1 (runs/20261007-202624-scenario1): PASS
- Offered 1,250 req/s (1.5× capacity) of 1k-token requests for 360 s against 50k RPM / 100M TPM.
- Sent: a flat ~48,480 per trailing minute (the 97% target) from t=60 s to the end. The provider counted
  341,250 calls (including retries), with a peak trailing-60 s window of **48,668 (97.3% of the limit)**
  and **zero violations** and **zero provider 429s**. The peak is 168 above our 48,500 target: requests
  arriving at the provider bunch together slightly because of network jitter. This is why we don't target 100%.
- Completed: every full minute after warm-up completed ≥ 95.9% of the RPM limit (pass needs 90%).
- Overload: 112,162 of 450,000 submissions (25%) got `429` once the 50,000-slot queue was full,
  about the excess over capacity. None expired, and all 337,838 accepted requests reached a final state
  and were persisted.
- Ack latency: p50 0.27 ms, p99 57 ms. End-to-end p50 **62.7 s**. A full one-minute queue means
  about a minute of waiting. That's the cost of `queue_minutes: 1`: a shorter queue means lower
  latency but more rejections. Either way, throughput stays the same.
- TPM ran at 48.7%: with 1k-token requests, RPM is the limit that binds.

## 2026-10-07: scenario 2 (runs/20261007-204418-scenario2): PASS
- A at 30k RPM and B at 20k RPM, offered 600/s and 400/s (both above their limits) for 420 s.
  A drops to 5k at t=120 s and goes back to 30k at t=270 s. Limits were changed in the safe order:
  on the drop, gateway first then provider; on the raise, provider first then gateway.
- Zero window violations on either model (peaks 97.4% / 97.5% of the limit in effect), zero 429s from the provider.
- After the drop, A **sends nothing for about 50 s**: the window still holds about 29k sends from the last minute,
  well over the new 4,850 cap. Sending resumes as those sends age out, and A settles at 4,849/min (97%).
  After the raise it ramps straight back to 29,100/min. No restart.
- A's queue during the drop: it held about 14.6k when the limit fell. The queue bound shrinks with the
  limit (1 min × 5k), so new arrivals get `429` while the backlog drains at 83/s. At about t=222 s the requests
  waiting since before the drop reach their 120 s deadline and expire. That accounts for **8,843 EXPIRED**,
  which is the deadline doing its job: they couldn't be served in time at the new rate.
- B was unaffected: ≥ 96.0% of its limit every minute after warm-up, including while A was constrained
  (separate queue, separate limiter).
- Accounting: 420,000 = 323,985 accepted + 96,015 rejected; accepted = 315,142 completed + 8,843 expired.

## 2026-10-09: Go hot path
- **Timing wheel delivered up to 1 ms early:** due times were rounded down to their slot. Now rounded up
  (late by < 1 ms, never early).
- **A weak throughput test:** it offered only 10% over the caps, and the pacer's 1 s of starting credit
  absorbed that, so a broken limiter would have passed. It now offers 3× capacity and bounds total calls
  between 90% of cap × duration and cap × (duration + 1 s). Making the pacer 50% too fast fails it.
- **300k required / 1M stretch:** 302.8k/s and 1.009M/s, 3/3 runs each, 97.0% peak window, zero violations.
  The spread layout (one model split over 12 shards) also reaches 302.8k/s against one global limit.
- **CPU in limited runs is mostly idle churn:** the profile shows ~97% in the Go scheduler (sleep/wake),
  with ~0.1 core per 1M req/s of real work. Not fixed. Documented in BENCHMARKS.md.
- **Once-a-minute dip and spike:** the pacer's 1 s starting burst uses up part of the 60 s window early, and the
  window blocks briefly about 60 s later. No violation, but not flat. Fix would be a smaller burst. Not applied.
- **Ceiling:** 11M/s completed with limits disabled, on 2.3 cores. At 44M/s offered, memory ran out first
  (88M in flight, swap), because ceiling mode sizes the in-flight cap from the offered load.
