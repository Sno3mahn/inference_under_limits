"""HTTP front-end for the provider simulator.

POST /v1/infer                  the inference call the gateway makes
GET  /admin/models              current config of every model
PUT  /admin/models/{name}       change a model while running (limits, latency, failure mix)
GET  /admin/stats               per-model window usage and outcome counts
GET  /healthz
"""

import argparse
import asyncio
import contextlib
import logging

from aiohttp import web

from gateway.clock import NS_PER_SECOND, MonotonicClock
from simulator.calllog import CallLog
from simulator.config import load_config
from simulator.engine import Outcome, ProviderSim

SIM_KEY = web.AppKey("sim", ProviderSim)
LOG_KEY = web.AppKey("call_log", CallLog)


def _error(outcome: Outcome) -> web.Response:
    headers = {}
    if outcome.retry_after_s is not None:
        headers["Retry-After"] = str(outcome.retry_after_s)
    return web.json_response({"error": {"type": outcome.kind}}, status=outcome.status, headers=headers)


async def infer(request: web.Request) -> web.Response:
    try:
        body = await request.json()
        model, request_id = body["model"], str(body["request_id"])
        tokens = body["estimated_tokens"]
    except (ValueError, KeyError, TypeError):
        return web.json_response({"error": {"type": "invalid"}}, status=400)
    attempt = int(request.headers.get("X-Attempt", "1"))

    outcome = request.app[SIM_KEY].handle(
        model, request_id, tokens, attempt, request.headers.get("Idempotency-Key")
    )
    # The decision (including the rate-limit charge) happens on arrival, like a real
    # provider; the response is then held back for the simulated latency.
    if outcome.status != 429:
        await asyncio.sleep(outcome.latency_ns / NS_PER_SECOND)
    if outcome.status != 200:
        return _error(outcome)
    return web.json_response({
        "request_id": request_id,
        "model": model,
        "output": f"simulated completion for {request_id}",
        "usage": {"estimated_tokens": tokens, "actual_tokens": outcome.actual_tokens},
        "replay": outcome.kind == "replay",
    })


def _model_json(cfg) -> dict:
    return {
        "rpm": cfg.rpm,
        "tpm": cfg.tpm,
        "latency": {"p50_ms": cfg.latency.p50_ms, "p99_ms": cfg.latency.p99_ms},
        "transient": cfg.transient,
        "permanent": cfg.permanent,
        "actual_tokens_jitter": cfg.actual_tokens_jitter,
    }


async def list_models(request: web.Request) -> web.Response:
    models = request.app[SIM_KEY].models()
    return web.json_response({name: _model_json(cfg) for name, cfg in models.items()})


async def update_model(request: web.Request) -> web.Response:
    name = request.match_info["name"]
    try:
        cfg = request.app[SIM_KEY].update_model(name, await request.json())
    except (ValueError, TypeError) as exc:
        return web.json_response({"error": {"type": "invalid", "message": str(exc)}}, status=400)
    logging.info("model %s updated: rpm=%s tpm=%s", name, cfg.rpm, cfg.tpm)
    return web.json_response({name: _model_json(cfg)})


async def stats(request: web.Request) -> web.Response:
    return web.json_response(request.app[SIM_KEY].stats())


async def healthz(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _flush_periodically(app: web.Application):
    async def loop():
        while True:
            await asyncio.sleep(0.5)
            app[LOG_KEY].flush()

    task = asyncio.create_task(loop())
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    app[LOG_KEY].close()


def make_app(sim: ProviderSim, call_log: CallLog) -> web.Application:
    app = web.Application()
    app[SIM_KEY] = sim
    app[LOG_KEY] = call_log
    app.add_routes([
        web.post("/v1/infer", infer),
        web.get("/admin/models", list_models),
        web.put("/admin/models/{name}", update_model),
        web.get("/admin/stats", stats),
        web.get("/healthz", healthz),
    ])
    app.cleanup_ctx.append(_flush_periodically)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Provider simulator")
    parser.add_argument("--config", default="../config/models.yaml")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--call-log", default="../runs/provider-calls.jsonl")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s simulator %(message)s")
    call_log = CallLog(args.call_log)
    sim = ProviderSim(load_config(args.config), MonotonicClock(), call_log)
    # access_log=None: logging every request costs more than handling it at these rates.
    web.run_app(make_app(sim, call_log), host=args.host, port=args.port, access_log=None)


if __name__ == "__main__":
    main()
