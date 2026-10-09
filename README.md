# Inference under provider limits

A gateway that accepts single and batch inference requests and sends them to a provider without
breaking the provider's per-model **RPM** (requests per minute) and **TPM** (tokens per minute)
limits, even when those limits change at runtime. It ships with a provider simulator that enforces
the same limits independently, a load generator, and a verifier that checks every run against the
provider's own call log.

- **Python** (`python/`) is the complete reference: HTTP API, scheduler, retries, SQLite persistence,
  async batches with callbacks. It covers the 50k RPM baseline and validation scenarios 1–3.
- **Go** (`go/`, in progress) is the hot path only (queues, limiter, dispatch, state accounting,
  in-process simulator), used for the 300k req/s and 1M req/s benchmarks.

Design: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Measured results and the bugs they surfaced:
[`docs/FINDINGS.md`](docs/FINDINGS.md).

## Quick start

Requires Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/).

```bash
make setup        # install dependencies
make test         # unit, property and end-to-end tests
make scenario3    # ~1 min: async batch of 10,000 with a flaky callback receiver
```

Every scenario run writes to `runs/<timestamp>-<scenario>/`: `report.md` (verdict, evidence and charts),
`report.json`, the provider call log, the gateway database, and the logs for each process.

## Running each component

Run each one from the repo root, in its own terminal:

```bash
make simulator    # provider simulator on :9000   (python -m simulator --config ../config/models.yaml --port 9000)
make gateway      # gateway on :8000 -> simulator  (python -m gateway --provider-url http://127.0.0.1:9000)
cd python && uv run python -m loadgen --gateway http://127.0.0.1:8000 --rate 500 --duration 30 --drain 120
```

| Component | Module | Useful flags |
|---|---|---|
| Gateway | `python -m gateway` | `--config`, `--provider-url`, `--db`, `--port`, `--utilization` (default 0.97), `--deadline-s` (default 120) |
| Simulator | `python -m simulator` | `--config`, `--port`, `--call-log` (JSONL record of every call, used by the verifier) |
| Load generator | `python -m loadgen` | `--rate`, `--duration`, `--models a:0.6,b:0.4`, `--batch-size`, `--tokens`, `--callback-port`, `--fail-first-callbacks`, `--drain` |
| Scenario runner | `python -m scenarios <yaml>` | `--duration`, `--rate` (overrides), `--runs-dir` |
| Verifier | `python -m verify <run_dir>` | re-checks a finished run and rewrites its report |

### Try the API

```bash
# single request: 202 straight away, or wait up to 5 s for the result
curl -s -XPOST 'localhost:8000/v1/requests?wait=5' -H 'content-type: application/json' \
  -d '{"request_id":"r1","model":"model-a","estimated_tokens":800,"payload":{"prompt":"hi"}}'
curl -s localhost:8000/v1/requests/r1

# async batch with a callback
curl -s -XPOST localhost:8000/v1/batches -H 'content-type: application/json' -H 'Idempotency-Key: b-1' \
  -d '{"callback_url":"http://127.0.0.1:9100/callback",
       "requests":[{"request_id":"1","model":"model-a","estimated_tokens":500,"payload":{}},
                   {"request_id":"2","model":"model-b","estimated_tokens":900,"payload":{}}]}'
curl -s localhost:8000/v1/batches/<batch_id>
curl -s 'localhost:8000/v1/batches/<batch_id>/results?limit=1000'

# change a model's limits live (also change the simulator's: see "Changing limits" below)
curl -s -XPUT localhost:8000/v1/models/model-a/limits -H 'content-type: application/json' \
  -d '{"rpm":5000,"tpm":10000000}'
curl -s localhost:8000/v1/models
curl -s localhost:8000/v1/metrics
```

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/requests[?wait=s]` | Submit one request. `202` queued, `200` if it finished within `wait`, `429` + `Retry-After` when full |
| `GET` | `/v1/requests/{id}` | State, attempts, result or error |
| `POST` | `/v1/batches` | Submit a batch (all or nothing). `202` with `batch_id`; same `Idempotency-Key` → `200` with the same batch |
| `GET` | `/v1/batches/{id}` | Batch summary: the same body the callback carries |
| `GET` | `/v1/batches/{id}/results` | Per-request results, paginated with `cursor` |
| `GET` / `PUT` | `/v1/models`, `/v1/models/{m}/limits` | Read limits and live usage; change limits without a restart |
| `GET` | `/v1/metrics` | Counters, queue depths, latency, limit history |

## Configuring models and limits

`config/models.yaml` is read by both the gateway (only `rpm` / `tpm`) and the simulator (everything):

```yaml
models:
  model-a:
    rpm: 50000
    tpm: 100000000
    latency: { p50_ms: 150, p99_ms: 600 }                     # simulator: log-normal fit
    transient: { "503": 0.02, "500": 0.005, "timeout": 0.001 } # simulator: retryable failures
    permanent: { "400": 0.005, "413": 0.005 }                  # simulator: final failures
seed: 42
hang_s: 30     # a simulated timeout hangs this long; the gateway gives up after 10 s and retries
```

### Changing limits at runtime

Both sides have to change, in a safe order (the scenario runner does this for you):

- **Lowering a limit:** change the gateway first (`PUT /v1/models/{m}/limits`), then the simulator
  (`PUT /admin/models/{m}` on :9000). If you did it the other way round, the gateway would briefly
  send at the old, higher rate against the new, lower limit.
- **Raising a limit:** change the simulator first, then the gateway.

After a drop, the gateway pauses that model until the sends from the last 60 s fit under the new limit
(about 50 s for a drop from 30k to 5k). Other models are not affected.

## Validation scenarios

| | Command | Duration | What it proves |
|---|---|---|---|
| 1 | `make scenario1` | ~9 min | 50k RPM / 100M TPM with demand above the limit: sustained ≥ 90% of the limit every minute after warm-up, zero window violations |
| 2 | `make scenario2` | ~10 min | Two models; A drops 30k → 5k at t=120 s and goes back at t=270 s. A adapts without a restart, B stays at ≥ 90% throughout, no violations |
| 3 | `make scenario3` | ~1 min | One batch of 10,000 with transient and permanent failures and a receiver that fails twice. Ack < 1 s, callback only after every request is final, retried until delivered, matches the API, every id exactly once |

Scenario files live in `config/scenarios/`. `make scenario1 ARGS="--duration 120"` shortens a run.
A scenario passes only if all of its checks pass. The runner exits non-zero otherwise.

Every scenario also checks:
- **limits:** at each call the provider counted, the calls and tokens in the trailing 60 s are within
  the limit in effect at that moment. This uses the simulator's log, not the gateway's own numbers.
- **accounting:** submitted = accepted + rejected (with no invalid submissions or transport errors), and
  every accepted request reached exactly one final state, both in the API and in the database.

## Key design decisions

Full reasoning in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). In short:

1. **The limit is a sliding 60 s window, enforced exactly.** At every send, requests and tokens in the
   last 60 s must fit under the caps. A plain token bucket can let about 2× the limit through in one
   window, and calendar-minute counters allow a burst on both sides of a minute boundary. A second
   token-bucket **pacer** at limit/60 per second spreads sends evenly, so a minute's budget doesn't go
   out in the first second.
2. **Target 97% of the limit.** Network jitter means the provider's view of "when" can differ slightly
   from ours. At 50k RPM the provider's peak window reached 48,567 against our 48,500 target, so we
   need some margin.
3. **One queue per model.** A slow or throttled model never blocks the others (scenario 2).
4. **Bounded queues, then reject.** Each model queues about one minute of its limit. Beyond that,
   submissions get `429` + `Retry-After` (estimated from the queue's drain time) instead of piling up
   unbounded latency. Every request also has a deadline, and a request still queued past it becomes
   `EXPIRED`. Batches are admitted all or nothing.
5. **Retries:** 408/429/5xx/timeouts are retried up to 5 attempts with exponential backoff and full
   jitter. Retries go ahead of fresh work, and a provider 429 pauses the whole model briefly. Any other
   4xx is final. Every call carries an `Idempotency-Key`, so a retry after a timeout cannot do the work twice.
6. **Callbacks are at least once.** Every attempt carries the same `X-Event-Id` and a byte-identical
   body, so the receiver can deduplicate. Backoff runs 1, 2, 4 … 60 s for up to 10 attempts, and a
   permanent 4xx from the receiver stops it. The batch status endpoint works independently of the callback.
7. **Persist at submit and at final state only.** Writes go to SQLite (WAL) through one writer thread
   that groups commits. A batch is acked only after it is saved, and a restart re-queues unfinished work.
8. **Integer nanoseconds on a monotonic clock** inside the gateway, so wall-clock jumps can't open or close
   the window. A fake clock makes the limiter tests deterministic. Property tests (Hypothesis) try random
   limits, request sizes and limit changes, and fail on any window violation.

### Assumptions and limits

- The gateway runs as **one process**: the limiter state lives in memory. Scaling out needs the limit
  split across instances or a shared limiter (see the 10B/min plan in `docs/BENCHMARKS.md`).
- Limits are charged on **estimated** tokens. When the actual count comes back higher, the difference
  is charged afterwards. A request larger than a whole minute's TPM is rejected as invalid.
- Request ids are unique within a batch; single requests share one namespace.
- Rejected submissions are counted, not stored.

## Repository layout

```
config/            models.yaml, scenarios/*.yaml
docs/              ARCHITECTURE.md, FINDINGS.md, BENCHMARKS.md
python/gateway/    limiter, scheduler, tracker, store, batches, callbacks, FastAPI app
python/simulator/  provider simulator (aiohttp) with independent limit enforcement and a call log
python/loadgen/    open-loop load generator + callback receiver
python/verify/     checks + report/charts from a run directory
python/scenarios/  end-to-end scenario runner
python/tests/      unit, property, and end-to-end tests
go/                hot-path benchmark (300k / 1M req/s)
```
