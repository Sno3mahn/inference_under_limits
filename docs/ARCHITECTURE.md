# Architecture

This document describes how the system is put together and, more importantly, *why*.
Each section calls out the gotchas it is designed around.

## 1. The problem in one paragraph

An inference provider gives us capacity per model, as **requests per minute (RPM)** and
**tokens per minute (TPM)**. Today we send requests one at a time and use about 1% of it.
We need a gateway that accepts work (single requests and async batches), sends it to the
provider as fast as the limits allow **and never faster**, survives failures, tells clients
when batches finish, and can prove all of that with measurements.

## 2. The pieces

There are three roles. Only the gateway is "the product"; the other two are the test harness.

| Role | What it does | Lives in |
|---|---|---|
| **Load generator** | Plays the customer. Generates requests at a chosen rate, duration, model mix, batch size and token size. Runs a small **callback receiver** that can be told to fail the first N deliveries. | `python/loadgen/` |
| **Gateway** | Our system. Queues, rate limiting, dispatch, retries, state, batches, callbacks. | `python/gateway/` (full), `go/` (hot path only) |
| **Provider simulator** | Plays the LLM provider. Has its own per-model limits, latency distribution and outcome mix. Logs every call it receives. | `python/simulator/`, in-process in `go/` |

Plus a **verifier** that reads the simulator's call log after a run, checks it against the
limits, and writes the report.

```
Load generator ──▶ Gateway API ──▶ per-model queue ──▶ per-model scheduler ──▶ dispatcher ──▶ Provider simulator
      ▲                                                                            │                 │
      │                                                                            ▼                 ▼
Callback receiver ◀── callback sender ◀── batch tracker ◀────────────────── state store       call log ──▶ verifier/report
```

## 3. Life of a request

### 3.1 States

Every request is in exactly one state. A request can enter a **final** state only once.
That single rule is what gives us "every request is accounted for" and "every id appears
exactly once".

```
               ┌──────────── deadline passed ───────────▶ EXPIRED   (final)
               │
 submit ──▶ QUEUED ──▶ IN_FLIGHT ──▶ success ──────────────▶ SUCCEEDED (final)
   │           ▲           │
   │           │           ├──▶ permanent error ───────────▶ FAILED    (final)
   │           │           │
   │           └─ RETRY_WAIT ◀── transient error (attempts left)
   │                       │
   │                       └──▶ transient error, no attempts left ──▶ FAILED (final, reason=retries_exhausted)
   │
   └──▶ invalid / queue full ──────────────────────────────▶ REJECTED  (final, never queued)
```

`RETRY_WAIT` can also expire if the deadline passes while waiting.

**Final states:** `SUCCEEDED`, `FAILED`, `EXPIRED`, `REJECTED`.
**Non-final:** `QUEUED`, `IN_FLIGHT`, `RETRY_WAIT`. Scenario 1's "waiting" maps to these.

### 3.2 Step by step

1. **API validates and enqueues.** It checks the schema, stores the request as `QUEUED`, puts it on
   its model's queue and returns immediately. It never waits for the provider.
2. **The per-model queue holds it.** Each model has its own queue so a throttled model cannot
   block others (no head-of-line blocking across models).
3. **The scheduler releases it** when the model's RPM *and* TPM windows both have room (§4).
4. **The dispatcher calls the provider** and classifies the response (§6).
5. **The state store records the final outcome.** The batch tracker increments the batch's count.
6. **When a batch's last request is final**, the callback sender delivers the summary (§7).

## 4. Rate limiting (the core of the design)

### 4.1 What "within the limit" means

The brief says "no observed 60-second period exceeds either configured limit". We read that
as **any** 60-second window, not calendar minutes.

> **Gotcha: calendar-minute counters are not enough.** 50k requests at 12:00:59 plus 50k at
> 12:01:00 keeps each calendar minute within the limit, yet the 60 seconds around the boundary
> saw 100k.

> **Gotcha: a standard token bucket is not enough either.** A bucket that holds a full minute
> of credit and refills at `limit/60` per second can release *bucket + refill* = about 2× the limit in
> one window. To be safe with a bucket alone you must have `burst + 60 × rate ≤ limit`.

**Precise rule we enforce and verify:** at the moment of every send `t`,

```
count(sends in (t − 60s, t])           ≤ RPM(t)
sum(tokens of sends in (t − 60s, t])   ≤ TPM(t)
```

where `RPM(t)` / `TPM(t)` are the limits in effect at time `t`, and the counts include the send
itself. Every send must satisfy this. A window that contains no new send is never "violated"
by old traffic. This matters when limits drop (§4.4).

### 4.2 Two layers: a hard guarantee and pacing

| Layer | Job | Mechanism |
|---|---|---|
| **Sliding window** (hard guarantee) | Never exceed the rule above | Python: a deque of `(send_time, tokens)` for the last 60s, with running totals. Go: a ring of small time slots (e.g. 100 ms) |
| **Pacing** (smoothness) | Spread sends evenly instead of 50k in second 1 then silence | A token bucket refilling at `target_rate / 60` per second with a small burst (≈ 1 s worth) |

A request is released only when **both** layers allow it. The sliding window alone would be
correct but bursty. A real provider may also enforce shorter windows, and bursts make
latency worse. The pacer alone is not provably correct. Together they give a hard limit with smooth traffic.

### 4.3 Target utilisation and safety margin

The scheduler aims at `utilisation_target × limit`, default **0.97**, configurable.
The margin absorbs clock differences between the gateway and the provider, plus token
estimate error (§4.6). The brief needs ≥ 90% after warm-up, so 97% leaves headroom on both sides.

### 4.4 Changing limits while running

Limits live in a per-model config object that the scheduler reads on every decision.
`PUT /v1/models/{model}/limits` updates it with no restart.

- **Decrease (e.g. Model A 30k → 5k RPM).** The trailing window may already hold ~25k sends,
  more than the new limit. By the rule in §4.1 we simply **send nothing** until enough old sends
  age out, then continue at 5k/min. The pacer is also reset to the new rate.
  Expect a pause of up to ~60 s for Model A. That is correct behaviour, and the report shows it.
- **Increase (5k → 30k).** The pacer's rate rises immediately but its burst is small, so
  throughput ramps up smoothly instead of spiking.
- **Model B is untouched.** It has its own queue, window and pacer.

> **Gotcha: ordering of gateway vs provider updates.** In the scenario script, a decrease is
> applied to the gateway first and the provider second. An increase is applied to the provider
> first and the gateway second. Otherwise there is a short moment where we send more than the
> provider allows. If the provider changes limits without telling us, we see 429s and back off (§6).

### 4.5 How many requests in flight

By Little's law: `in_flight ≈ rate × latency`. At 50k RPM (833/s) and 300 ms p99 latency that's
about 250. Each model gets a concurrency cap of `rate × p99_latency × 2`. That stops a slow
provider from causing an unbounded pile of open connections.

### 4.6 Estimated vs actual tokens

Requests only carry `estimated_tokens`. We **reserve the estimate** in the TPM window at send
time. The simulator returns `actual_tokens`. By default this equals the estimate, and it can be
configured to vary. If actual > estimated, the difference is added to the window when the
response arrives. This can make a single window slightly over-full; the safety margin (§4.3)
covers it, and the report states the assumption.

### 4.7 Order within a model

Strict **FIFO per model**, with one exception: retries whose backoff has elapsed are served
before fresh work. The reason is that a retry is part of a batch someone is waiting on. Retries are
capped (§6), so this cannot turn into a retry storm.

We do not let small requests overtake a large one waiting for TPM room. That is simpler and
avoids starving large requests. A request whose `estimated_tokens` exceeds the model's TPM could
never be sent, so it is **rejected at the API** instead of blocking the queue forever.

## 5. When demand exceeds capacity

| Mechanism | Default | Result |
|---|---|---|
| Bounded queue per model | max = 1 minute of work at the current RPM | New requests are `REJECTED` with `429` + `Retry-After` |
| Queue deadline per request | 120 s (overridable per request) | Request becomes `EXPIRED` instead of waiting forever |
| Batch submissions | Whole batch is rejected if it does not fit | No half-accepted batches; the client retries the whole thing |

Every submitted request ends up counted in exactly one of: completed (succeeded/failed),
waiting (queued/in-flight/retry-wait), expired, or rejected. That is Scenario 1's accounting rule.

## 6. Failures and retries

| Class | Examples from the simulator | Action |
|---|---|---|
| **Transient** | `429` rate limited, `503` overloaded, `500`/`502`/`504`, timeout, connection reset | Retry with exponential backoff + jitter (base 200 ms, ×2, cap 10 s), max 5 attempts. Honour `Retry-After`. Every retry goes **back through the scheduler**, because it spends RPM/TPM like any request |
| **Permanent** | `400` bad payload, `404` unknown model, `413` context too long, policy rejection | `FAILED` immediately |
| **Exhausted** | Transient error with no attempts left | `FAILED`, `reason=retries_exhausted` |

- A `429` from the provider means our picture of capacity is wrong. The model's scheduler pauses
  briefly (`Retry-After`, or 1 s). The report counts provider 429s; in a correct run it should be ~0.
- **Timeouts are ambiguous:** the provider may have done the work. We send `request_id` as an
  idempotency key so the simulator returns the original result for a repeat, and because a
  final state can only be entered once, a late duplicate response is ignored.

## 7. Batches and callbacks

### 7.1 Submitting

`POST /v1/batches` returns **`202 Accepted`** within 1 s:

```json
{ "batch_id": "b_01H...", "status": "accepted", "total_requests": 10000,
  "status_url": "/v1/batches/b_01H...", "results_url": "/v1/batches/b_01H.../results" }
```

The handler validates the batch, writes all requests in **one** SQLite transaction (10k rows
takes tens of milliseconds), enqueues them and returns.

Validation rules:
- At least 1 request. The maximum is configurable (default 50k).
- `request_id`s must be unique within the batch, or the whole batch gets a `400` listing the duplicates.
  Request ids are scoped to their batch, so internally the key is `(batch_id, request_id)`.
- Unknown model, or `estimated_tokens` ≤ 0 or > model TPM, gets a `400`.
- `callback_url` must be a valid http(s) URL.
- An optional `Idempotency-Key` header: resubmitting with the same key returns the original `batch_id`.

### 7.2 Completing

The batch tracker keeps `succeeded`, `failed` and `expired` counters per batch. When
`succeeded + failed + expired == total`, the batch becomes final:

- `completed`: every request succeeded
- `completed_with_failures`: at least one did not

A callback job is then written to SQLite **in the same transaction** that marks the batch final.
So the callback cannot be lost, and it cannot be sent before the batch is final.

### 7.3 Delivering the callback

```json
{ "event_id": "b_01H...:completed", "batch_id": "b_01H...", "status": "completed_with_failures",
  "total": 10000, "succeeded": 9412, "failed": 571, "expired": 17,
  "completed_at": "2026-10-08T10:15:02Z", "results_url": "/v1/batches/b_01H.../results" }
```

- The payload is built from the **same row** the status API reads, so they always match, and it is
  identical on every attempt.
- **At-least-once delivery.** The receiver dedupes on `event_id`. An `X-Callback-Attempt` header says which attempt this is.
- `2xx` means delivered. `408`, `429`, `5xx`, timeout or connection error mean retry with backoff
  (1 s, 2 s, 4 s … cap 60 s, max 10 attempts, all configurable). Any other `4xx` means give up and set
  `callback_status=failed`. The batch is still fully queryable.
- Callback jobs live in SQLite, so a gateway restart resumes pending deliveries.
- Callbacks run on their own worker pool, so a slow receiver never slows inference.
- Results are **not inline**. 10k results is a large body for a webhook. The client pages through
  `GET /v1/batches/{id}/results?cursor=...&limit=1000`, and each `request_id` appears exactly once.

## 8. Persistence (Python)

SQLite in WAL mode. Tables: `batches`, `requests`, `callbacks`.

> **Gotcha: SQLite has one writer.** All writes go through a single writer **thread** (off the
> event loop) that commits in groups: it collects ops for up to 5 ms or 2,000 ops, then does one
> transaction. Requests are written only twice: at submit and at final state. Intermediate states
> (in flight, retry wait) live only in memory. At ~830 req/s that's ~1.7k writes/s; the scenario 3
> batch of 10,000 took 10,006 writes in 3,692 commits. If a group commit fails, each op in it is
> retried on its own, so one bad row can't lose its neighbours. The hot path (scheduler, window
> counters) never reads SQLite; it is the durable record only.

Ordering rules that matter:
- A batch is acked (`202`) only after its rows are committed, so an acked batch survives a crash.
  If the save fails, its requests are expired with `not_persisted` and the client gets an error.
- The callback sender waits on a store barrier before its first attempt, so a callback never
  reports a batch the database doesn't yet show as complete.

On restart: requests that aren't final on disk (queued, or in flight at crash time, which on disk
still look queued) are re-queued. That's safe because of the idempotency key (§6): the provider
replays work it already did. Batches whose requests are all final but have no callback row get
completed; pending callbacks resume. Pending callbacks are read *before* completing batches, so
a callback is never scheduled twice.

## 9. API summary

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/requests` | Single request → `202` with `request_id` |
| `GET` | `/v1/requests/{id}` | Status and result of a single request |
| `POST` | `/v1/batches` | Async batch → `202` ack |
| `GET` | `/v1/batches/{id}` | Batch summary (same numbers as the callback) |
| `GET` | `/v1/batches/{id}/results` | Paginated per-request results |
| `GET` | `/v1/models` | Configured limits plus observed RPM/TPM per model |
| `PUT` | `/v1/models/{model}/limits` | Change RPM/TPM live |
| `GET` | `/v1/metrics` | Counters and latency summary for the load generator |

## 10. Provider simulator

Configured from `config/models.yaml`, and changeable at runtime through its admin API:

```yaml
models:
  model-a:
    rpm: 30000
    tpm: 60000000
    latency: { p50_ms: 150, p99_ms: 600 }       # log-normal fit
    transient: { "503": 0.02, "500": 0.005, "timeout": 0.001 }
    permanent: { "400": 0.005, "413": 0.005 }
seed: 42
hang_s: 30          # a "timeout" outcome hangs this long (the work is still done)
```

- It **enforces limits independently** of the gateway, using the same rule as §4.1. If the gateway
  sends too fast, it returns 429s. That gives a second, independent check.
- It appends every accepted call to a JSONL **call log** (`time, model, request_id, tokens, outcome`).
  The verifier works from this log, not from the gateway's own numbers.
- All randomness is seeded, so a run is repeatable.

## 11. Verifier and report

After a run the verifier:
1. Replays the call log per model and checks the §4.1 rule at every send against the limit
   timeline (limit changes are logged too).
2. Reconciles the counts: submitted = succeeded + failed + expired + rejected + still-waiting.
3. Computes p50/p95/p99 end-to-end latency (submit → final), achieved RPM/TPM per model per
   second and per sliding minute, and batch and callback timings.
4. Writes `report.json`, `report.md` and charts (configured limit vs observed throughput over time).

Measured numbers and projections are kept in separate sections, and the simulation assumptions are listed.

## 12. Python vs Go

Same boxes, different implementation.

| | Python, scenarios 1–3 | Go, 300k/s and 1M/s benchmarks |
|---|---|---|
| API | FastAPI + uvicorn | None. Requests are generated in-process |
| Queues | `asyncio.Queue` per model | Channels, sharded per CPU core |
| Sliding window | Exact deque of timestamps | Ring of 100 ms slots, O(1) per decision |
| Provider | HTTP to the simulator server | Function call, latency modeled with a timer |
| State | SQLite + in-memory | In-memory counters only |
| Callbacks | Yes | No (stated as out of scope) |

The Go benchmark measures **completions**: the simulated provider call has reached a final state.
It does not count acks. It has no network hop, so its numbers are an upper bound for the gateway logic and
not an end-to-end figure. The report says so.

## 13. Towards 10 billion requests per minute (projection)

10B/min ≈ 167M/s. No single machine does that. Outline (to be expanded with measured per-core numbers):
- **Shard by model and tenant** across many gateway nodes. Each node owns a slice of the traffic.
- **Hand out capacity in leases.** A central budget service gives each node "you may use X RPM
  for the next N seconds" and rebalances based on demand. The nodes rate limit locally, with no
  per-request coordination.
- **Use a durable log (e.g. Kafka) for ingest** instead of a database per request. Batch and results state
  goes in a horizontally scaled store.
- **Capacity maths:** if one Go core sustains *X* completions/s, then 167M/s needs about 167M / X cores,
  plus overhead for leases and replication.

## 14. Proposed repo layout

```
python/
  gateway/     api.py  scheduler.py  limiter.py  dispatcher.py  store.py  batches.py  callbacks.py  config.py
  simulator/   server.py  models.py
  loadgen/     run.py  receiver.py
  verify/      verify.py  report.py
  tests/
go/
  cmd/bench/   internal/{limiter,scheduler,sim}
config/        models.yaml  scenario1.yaml  scenario2.yaml  scenario3.yaml
scenarios/     scenario1.sh  scenario2.sh  scenario3.sh
docs/          ARCHITECTURE.md  BENCHMARKS.md
Makefile
```

## 15. Assumptions (also listed in the README)

- The provider counts **estimated** tokens at admission. Actual tokens are reconciled afterwards (§4.6).
- "60-second period" means any trailing window, evaluated at every send (§4.1).
- Request ids are unique within a batch, not globally.
- Latency and failure distributions are our choice, are seeded, and are not taken from a real provider.
- Benchmark limits for 300k/s and 1M/s are raised far above the 50k RPM baseline, as the brief allows.
