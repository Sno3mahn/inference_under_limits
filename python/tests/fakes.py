"""Test doubles shared by the gateway tests."""

import asyncio
import functools
import time
from collections.abc import Callable

from gateway.provider import ProviderResult, Verdict

OK = ProviderResult(Verdict.SUCCESS, 200, "http_200", body={"output": "hi"}, actual_tokens=10)
UNAVAILABLE = ProviderResult(Verdict.TRANSIENT, 503, "http_503")
BAD_REQUEST = ProviderResult(Verdict.PERMANENT, 400, "http_400")
THROTTLED = ProviderResult(Verdict.TRANSIENT, 429, "http_429", retry_after_s=0.3)


class ScriptedProvider:
    """outcome(request) decides each attempt's result. Records every call it receives."""

    def __init__(self, outcome: Callable = lambda r: OK, latency_s: float = 0.005) -> None:
        self.outcome = outcome
        self.latency_s = latency_s
        self.calls: list[tuple[float, str, str, int]] = []  # (time, model, key, attempt)
        self.concurrent = 0
        self.max_concurrent = 0

    async def call(self, request):
        self.calls.append((time.monotonic(), request.model, request.key, request.attempts))
        self.concurrent += 1
        self.max_concurrent = max(self.max_concurrent, self.concurrent)
        try:
            await asyncio.sleep(self.latency_s)
            return self.outcome(request)
        finally:
            self.concurrent -= 1


def async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


async def wait_until(predicate, timeout_s: float = 5.0) -> None:
    end = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > end:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.01)
