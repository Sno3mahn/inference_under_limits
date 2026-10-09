"""HTTP API. Thin on purpose: parse, call the service, map errors to status codes.

POST /v1/requests[?wait=s]          single request -> 202 (or 200 if final within `wait`)
GET  /v1/requests/{request_id}
POST /v1/batches                    async batch -> 202 ack with batch_id
GET  /v1/batches/{batch_id}         summary (same numbers as the callback)
GET  /v1/batches/{batch_id}/results?cursor=&limit=
GET  /v1/models
PUT  /v1/models/{model}/limits      change RPM/TPM live
GET  /v1/metrics
GET  /healthz
Interactive docs at /docs.
"""

import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import AnyHttpUrl, BaseModel, Field, StrictInt

from gateway.core import OverCapacity, RequestItem, SubmissionError
from gateway.limiter import Limits
from gateway.service import Service


class SingleRequestIn(BaseModel):
    request_id: str | None = Field(None, min_length=1, max_length=256)
    model: str
    estimated_tokens: StrictInt
    payload: Any = None
    deadline_s: float | None = None


class BatchItemIn(BaseModel):
    request_id: str = Field(min_length=1, max_length=256)
    model: str
    estimated_tokens: StrictInt
    payload: Any = None
    deadline_s: float | None = None


class BatchIn(BaseModel):
    callback_url: AnyHttpUrl | None = None
    requests: list[BatchItemIn]


class LimitsIn(BaseModel):
    rpm: StrictInt = Field(ge=0)
    tpm: StrictInt = Field(ge=0)


def _item(r: SingleRequestIn | BatchItemIn, request_id: str) -> RequestItem:
    return RequestItem(request_id, r.model, r.estimated_tokens, r.payload, r.deadline_s)


def create_app(service: Service) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await service.start()
        yield
        await service.stop()

    app = FastAPI(title="Inference gateway", lifespan=lifespan)

    @app.exception_handler(SubmissionError)
    async def submission_error(_: Request, exc: SubmissionError):
        headers = {"Retry-After": str(exc.retry_after_s)} if isinstance(exc, OverCapacity) else None
        return JSONResponse(
            {"error": str(exc), "details": exc.details}, status_code=exc.status, headers=headers
        )

    # --- single requests ---------------------------------------------------------

    @app.post("/v1/requests", status_code=202)
    async def submit_request(body: SingleRequestIn, wait: float = Query(0, ge=0, le=300)):
        request_id = body.request_id or f"req_{uuid.uuid4().hex}"
        request, created = service.gateway.submit_single(_item(body, request_id))
        if wait > 0:
            await service.gateway.wait_final(request, wait)
        out = request.summary() | {"created": created, "status_url": f"/v1/requests/{request_id}"}
        return JSONResponse(out, status_code=200 if request.state.is_final else 202)

    @app.get("/v1/requests/{request_id}")
    async def get_request(request_id: str):
        found = await service.find_single(request_id)
        if found is None:
            raise HTTPException(404, "unknown request_id")
        return found

    # --- batches -----------------------------------------------------------------

    @app.post("/v1/batches", status_code=202)
    async def submit_batch(body: BatchIn, idempotency_key: str | None = Header(None)):
        items = [_item(r, r.request_id) for r in body.requests]
        callback = str(body.callback_url) if body.callback_url else None
        batch, created = await service.batches.submit(items, callback, idempotency_key)
        return JSONResponse(
            {
                "batch_id": batch.batch_id,
                "status": "accepted" if created else batch.status,
                "created": created,
                "total_requests": batch.total,
                "status_url": f"/v1/batches/{batch.batch_id}",
                "results_url": f"/v1/batches/{batch.batch_id}/results",
            },
            status_code=202 if created else 200,
        )

    @app.get("/v1/batches/{batch_id}")
    async def get_batch(batch_id: str):
        batch = await service.batches.get(batch_id)
        if batch is None:
            raise HTTPException(404, "unknown batch_id")
        return service.batches.status(batch)

    @app.get("/v1/batches/{batch_id}/results")
    async def get_results(batch_id: str, cursor: str | None = None, limit: int = Query(1000, ge=1, le=10_000)):
        if await service.batches.get(batch_id) is None:
            raise HTTPException(404, "unknown batch_id")
        return await service.batches.results(batch_id, cursor, limit)

    # --- models, limits, metrics -------------------------------------------------

    @app.get("/v1/models")
    async def list_models():
        return {model: service.gateway.model_status(model) for model in service.gateway.schedulers}

    @app.put("/v1/models/{model}/limits")
    async def set_limits(model: str, body: LimitsIn):
        if model not in service.gateway.schedulers:
            raise HTTPException(404, "unknown model")
        service.gateway.set_limits(model, Limits(rpm=body.rpm, tpm=body.tpm))
        return service.gateway.model_status(model)

    @app.get("/v1/metrics")
    async def metrics():
        return service.metrics()

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app
