"""Calling the provider, and deciding what a response means.

The gateway only ever sees a ProviderResult with one of three verdicts:

  success    -> SUCCEEDED
  transient  -> retry (429, 408, 5xx, timeout, connection error)
  permanent  -> FAILED (any other 4xx: bad request, unknown model, too long ...)
"""

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

import aiohttp

from gateway.tracker import InferenceRequest

TRANSIENT_STATUSES = {408, 429, 500, 502, 503, 504}


class Verdict(StrEnum):
    SUCCESS = "success"
    TRANSIENT = "transient"
    PERMANENT = "permanent"


@dataclass(frozen=True)
class ProviderResult:
    verdict: Verdict
    status: int | None  # None when there was no HTTP response (timeout, connection error)
    reason: str  # short machine-readable cause, e.g. "http_503", "timeout"
    body: Any = None
    actual_tokens: int | None = None
    retry_after_s: float | None = None


def classify(status: int) -> Verdict:
    if 200 <= status < 300:
        return Verdict.SUCCESS
    if status in TRANSIENT_STATUSES:
        return Verdict.TRANSIENT
    return Verdict.PERMANENT


def _retry_after(headers) -> float | None:
    try:
        return float(headers["Retry-After"])
    except (KeyError, ValueError):
        return None


class Provider(Protocol):
    async def call(self, request: InferenceRequest) -> ProviderResult: ...


class HttpProvider:
    """Calls the provider simulator over HTTP. Never raises; every outcome is a ProviderResult."""

    def __init__(self, base_url: str, timeout_s: float = 10.0, max_connections: int = 2_000) -> None:
        self._url = base_url.rstrip("/") + "/v1/infer"
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_connections = max_connections
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        # One shared session = one connection pool, reused across calls. Opening a new
        # connection per request would cost a TCP handshake each time.
        connector = aiohttp.TCPConnector(limit=self._max_connections)
        self._session = aiohttp.ClientSession(connector=connector, timeout=self._timeout)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()

    async def call(self, request: InferenceRequest) -> ProviderResult:
        assert self._session is not None, "call start() first"
        body = {
            "request_id": request.request_id,
            "model": request.model,
            "estimated_tokens": request.estimated_tokens,
            "payload": request.payload,
        }
        headers = {
            # Same key on every attempt, so a provider that already did the work (e.g. we
            # timed out waiting) returns the original result instead of doing it again.
            "Idempotency-Key": request.key,
            "X-Attempt": str(request.attempts),
        }
        try:
            async with self._session.post(self._url, json=body, headers=headers) as resp:
                verdict = classify(resp.status)
                data = await resp.json(content_type=None) if verdict == Verdict.SUCCESS else None
                actual = None
                if data is not None:
                    actual = (data.get("usage") or {}).get("actual_tokens")
                return ProviderResult(
                    verdict=verdict,
                    status=resp.status,
                    reason=f"http_{resp.status}",
                    body=data,
                    actual_tokens=actual,
                    retry_after_s=_retry_after(resp.headers),
                )
        except TimeoutError:
            return ProviderResult(Verdict.TRANSIENT, None, "timeout")
        except (aiohttp.ClientError, asyncio.IncompleteReadError, ValueError) as exc:
            return ProviderResult(Verdict.TRANSIENT, None, f"connection_error:{type(exc).__name__}")
