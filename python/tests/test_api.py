"""End-to-end through the HTTP API: real service, SQLite, callback sender, and a real
callback receiver. The provider is scripted and in-process to keep the tests fast."""

import asyncio
import json
import time

import httpx
from aiohttp import web
from aiohttp.test_utils import TestServer
from fakes import BAD_REQUEST, OK, UNAVAILABLE, ScriptedProvider, async_test, wait_until

from gateway.api import create_app
from gateway.callbacks import CallbackSettings
from gateway.core import GatewaySettings
from gateway.limiter import Limits
from gateway.scheduler import SchedulerSettings
from gateway.service import Service, ServiceConfig

FAST = Limits(rpm=6_000_000, tpm=10**10)


class CallbackReceiver:
    """Records every delivery; answers `fail_first` deliveries with `fail_status`."""

    def __init__(self, fail_first: int = 0, fail_status: int = 503) -> None:
        self.fail_first = fail_first
        self.fail_status = fail_status
        self.deliveries: list[dict] = []  # {"t", "attempt", "event_id", "body", "answered"}

    async def handle(self, request: web.Request) -> web.Response:
        answered = self.fail_status if len(self.deliveries) < self.fail_first else 200
        self.deliveries.append({
            "t": time.monotonic(),
            "attempt": int(request.headers["X-Callback-Attempt"]),
            "event_id": request.headers["X-Event-Id"],
            "body": await request.read(),
            "answered": answered,
        })
        return web.json_response({}, status=answered)

    async def __aenter__(self):
        app = web.Application()
        app.router.add_post("/hook", self.handle)
        self.server = TestServer(app)
        await self.server.start_server()
        self.url = str(self.server.make_url("/hook"))
        return self

    async def __aexit__(self, *exc):
        await self.server.close()


def make_service(tmp_path, provider, models=None, **gateway_overrides) -> Service:
    scheduler = SchedulerSettings(backoff_base_s=0.01, backoff_cap_s=0.05, sweep_interval_s=0.05)
    gateway = GatewaySettings(scheduler=scheduler, **gateway_overrides)
    config = ServiceConfig(
        models=models or {"model-a": FAST, "model-b": FAST},
        db_path=str(tmp_path / "gateway.db"),
        gateway=gateway,
        callbacks=CallbackSettings(backoff_base_s=0.05, backoff_cap_s=0.2),
    )
    return Service(config, provider=provider)


class Api:
    """Runs a service and gives an httpx client wired straight to its ASGI app."""

    def __init__(self, service: Service) -> None:
        self.service = service

    async def __aenter__(self) -> httpx.AsyncClient:
        await self.service.start()
        transport = httpx.ASGITransport(app=create_app(self.service))
        self.client = httpx.AsyncClient(transport=transport, base_url="http://gateway")
        return self.client

    async def __aexit__(self, *exc):
        await self.client.aclose()
        await self.service.stop()


def batch_body(n: int, callback_url: str | None = None, models=("model-a", "model-b")) -> dict:
    body = {"requests": [
        {"request_id": f"req-{i:05d}", "model": models[i % len(models)], "estimated_tokens": 100,
         "payload": {"prompt": f"hello {i}"}}
        for i in range(n)
    ]}
    if callback_url:
        body["callback_url"] = callback_url
    return body


async def all_results(client: httpx.AsyncClient, batch_id: str, page: int = 300) -> list[dict]:
    results, cursor = [], None
    while True:
        params = {"limit": page} | ({"cursor": cursor} if cursor else {})
        data = (await client.get(f"/v1/batches/{batch_id}/results", params=params)).json()
        results += data["results"]
        cursor = data["next_cursor"]
        if cursor is None:
            return results


# --- Scenario 3 in miniature ---------------------------------------------------------


@async_test
async def test_batch_with_failures_and_flaky_callback(tmp_path):
    def outcome(request):
        n = int(request.request_id.split("-")[1])
        if n % 10 == 0:
            return BAD_REQUEST  # permanent
        if n % 7 == 0 and request.attempts == 1:
            return UNAVAILABLE  # transient, succeeds on retry
        return OK

    service = make_service(tmp_path, ScriptedProvider(outcome))
    async with CallbackReceiver(fail_first=2) as receiver, Api(service) as client:
        started = time.monotonic()
        ack = await client.post("/v1/batches", json=batch_body(1_000, receiver.url))
        ack_s = time.monotonic() - started
        assert ack.status_code == 202 and ack_s < 1.0
        batch_id = ack.json()["batch_id"]

        batch = service.batches.batches[batch_id]
        await wait_until(lambda: (batch.callback or {}).get("status") == "delivered", timeout_s=10)

        # Delivered on the third attempt, same event id and identical body each time.
        assert [(d["attempt"], d["answered"]) for d in receiver.deliveries] == [(1, 503), (2, 503), (3, 200)]
        assert len({d["event_id"] for d in receiver.deliveries}) == 1
        assert len({d["body"] for d in receiver.deliveries}) == 1

        # The callback summary matches the status API.
        callback = json.loads(receiver.deliveries[-1]["body"])
        status = (await client.get(f"/v1/batches/{batch_id}")).json()
        for field in ("batch_id", "status", "total", "succeeded", "failed", "expired", "results_url"):
            assert callback[field] == status[field], field
        assert callback["status"] == "completed_with_failures"
        assert callback["total"] == 1_000 and callback["failed"] == 100
        assert callback["succeeded"] + callback["failed"] + callback["expired"] == 1_000
        assert status["callback"]["status"] == "delivered" and status["callback"]["attempts"] == 3

        # Every request id exactly once in the results, all final, matching the counts.
        results = await all_results(client, batch_id)
        ids = [r["request_id"] for r in results]
        assert len(ids) == 1_000 and len(set(ids)) == 1_000
        assert sum(r["status"] == "failed" for r in results) == 100
        assert all(r["status"] in ("succeeded", "failed", "expired") for r in results)


@async_test
async def test_callback_is_only_sent_after_every_request_is_final(tmp_path):
    provider = ScriptedProvider(latency_s=0.02)
    service = make_service(tmp_path, provider)
    async with CallbackReceiver() as receiver, Api(service) as client:
        ack = (await client.post("/v1/batches", json=batch_body(200, receiver.url))).json()
        await wait_until(lambda: receiver.deliveries, timeout_s=10)
        sent_at = receiver.deliveries[0]["t"]
        results = await all_results(client, ack["batch_id"])
        assert len(results) == 200 and all(r["finished_at"] is not None for r in results)
        last_call = max(t for t, *_ in provider.calls)
        assert sent_at > last_call  # nothing was still being processed


@async_test
async def test_callback_gives_up_on_a_permanent_4xx(tmp_path):
    async with CallbackReceiver(fail_first=99, fail_status=404) as receiver, \
            Api(make_service(tmp_path, ScriptedProvider())) as client:
        batch_id = (await client.post("/v1/batches", json=batch_body(5, receiver.url))).json()["batch_id"]
        await wait_until(lambda: receiver.deliveries)
        await asyncio.sleep(0.5)
        assert len(receiver.deliveries) == 1  # not retried
        cb = (await client.get(f"/v1/batches/{batch_id}")).json()["callback"]
        assert cb["status"] == "failed" and cb["last_error"] == "http_404"


# --- batch submission rules ----------------------------------------------------------


@async_test
async def test_batch_idempotency_key_returns_the_original_batch(tmp_path):
    async with Api(make_service(tmp_path, ScriptedProvider())) as client:
        headers = {"Idempotency-Key": "client-key-1"}
        first = await client.post("/v1/batches", json=batch_body(10), headers=headers)
        again = await client.post("/v1/batches", json=batch_body(10), headers=headers)
        assert first.status_code == 202 and again.status_code == 200
        assert first.json()["batch_id"] == again.json()["batch_id"]
        assert again.json()["created"] is False


@async_test
async def test_invalid_and_over_capacity_batches(tmp_path):
    slow = {"model-a": Limits(rpm=60, tpm=10**9)}
    service = make_service(tmp_path, ScriptedProvider(), models=slow)
    service.config.gateway.scheduler.min_queue = 50
    service.config.gateway.scheduler.queue_minutes = 0
    async with Api(service) as client:
        dup = batch_body(3, models=("model-a",))
        dup["requests"][1]["request_id"] = dup["requests"][0]["request_id"]
        bad = await client.post("/v1/batches", json=dup)
        assert bad.status_code == 400 and "duplicate" in bad.json()["details"][0]

        unknown = await client.post("/v1/batches", json=batch_body(2, models=("model-x",)))
        assert unknown.status_code == 400

        empty = await client.post("/v1/batches", json={"requests": []})
        assert empty.status_code == 400

        assert (await client.post("/v1/batches", json=batch_body(50, models=("model-a",)))).status_code == 202
        full = await client.post("/v1/batches", json=batch_body(10, models=("model-a",)))
        assert full.status_code == 429 and int(full.headers["Retry-After"]) >= 1


# --- single requests, limits, metrics ------------------------------------------------


@async_test
async def test_single_request_with_and_without_wait(tmp_path):
    async with Api(make_service(tmp_path, ScriptedProvider(latency_s=0.05))) as client:
        body = {"request_id": "one", "model": "model-a", "estimated_tokens": 10}
        waited = await client.post("/v1/requests", params={"wait": 5}, json=body)
        assert waited.status_code == 200 and waited.json()["status"] == "succeeded"

        quick = await client.post("/v1/requests", json=body | {"request_id": "two"})
        assert quick.status_code == 202 and quick.json()["status"] == "queued"
        await asyncio.sleep(0.2)
        assert (await client.get("/v1/requests/two")).json()["status"] == "succeeded"
        assert (await client.get("/v1/requests/nope")).status_code == 404

        resubmit = await client.post("/v1/requests", json=body)
        assert resubmit.json()["created"] is False


@async_test
async def test_limits_can_be_changed_through_the_api(tmp_path):
    async with Api(make_service(tmp_path, ScriptedProvider())) as client:
        resp = await client.put("/v1/models/model-a/limits", json={"rpm": 5_000, "tpm": 60_000_000})
        assert resp.json()["limits"] == {"rpm": 5_000, "tpm": 60_000_000}
        assert resp.json()["targets"]["rpm"] == 4_850
        assert (await client.put("/v1/models/nope/limits", json={"rpm": 1, "tpm": 1})).status_code == 404
        assert (await client.put("/v1/models/model-a/limits", json={"rpm": -1, "tpm": 1})).status_code == 422
        history = (await client.get("/v1/metrics")).json()["limit_history"]
        assert history[-1]["model"] == "model-a" and history[-1]["rpm"] == 5_000


# --- restart recovery ----------------------------------------------------------------


@async_test
async def test_unfinished_batch_and_its_callback_survive_a_restart(tmp_path):
    slow = {"model-a": Limits(rpm=600, tpm=10**9)}  # ~10/s, so 40 requests take a few seconds
    async with CallbackReceiver() as receiver:
        first = make_service(tmp_path, ScriptedProvider(), models=slow)
        async with Api(first) as client:
            ack = (await client.post("/v1/batches", json=batch_body(40, receiver.url, models=("model-a",)))).json()
            await asyncio.sleep(0.5)
        done_before = first.batches.batches[ack["batch_id"]].finished
        assert 0 < done_before < 40

        second = make_service(tmp_path, ScriptedProvider(), models={"model-a": FAST})
        async with Api(second) as client:
            await wait_until(lambda: receiver.deliveries, timeout_s=10)
            status = (await client.get(f"/v1/batches/{ack['batch_id']}")).json()
            assert status["total"] == 40 and status["pending"] == 0
            results = await all_results(client, ack["batch_id"])
            assert len({r["request_id"] for r in results}) == 40
