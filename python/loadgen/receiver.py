"""A callback receiver for testing: records every delivery, and can be told to fail the
first N with a given status (Scenario 3: reject the first two)."""

import json
import time
from pathlib import Path

from aiohttp import web


class CallbackReceiver:
    def __init__(self, host: str, port: int, fail_first: int = 0, fail_status: int = 503,
                 log_path: Path | None = None) -> None:
        self.host, self.port = host, port
        self.fail_first = fail_first
        self.fail_status = fail_status
        self.deliveries: list[dict] = []
        self._log_path = log_path
        self._runner: web.AppRunner | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/callback"

    async def _handle(self, request: web.Request) -> web.Response:
        body = await request.read()
        status = self.fail_status if len(self.deliveries) < self.fail_first else 200
        delivery = {
            "t_wall": time.time(),
            "attempt": int(request.headers.get("X-Callback-Attempt", "0")),
            "event_id": request.headers.get("X-Event-Id"),
            "answered": status,
            "body": json.loads(body),
        }
        self.deliveries.append(delivery)
        if self._log_path is not None:
            with open(self._log_path, "a") as out:
                out.write(json.dumps(delivery) + "\n")
        return web.json_response({"ok": status == 200}, status=status)

    async def start(self) -> None:
        app = web.Application()
        app.router.add_post("/callback", self._handle)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        await web.TCPSite(self._runner, self.host, self.port).start()

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    def delivered(self, batch_id: str | None = None) -> list[dict]:
        return [d for d in self.deliveries
                if d["answered"] == 200 and (batch_id is None or d["body"].get("batch_id") == batch_id)]
