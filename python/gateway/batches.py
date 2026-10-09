"""Async batches: accept fast, track completion, then hand off to the callback sender.

A batch is complete when succeeded + failed + expired == total. Each counter only moves
when a request reaches a final state, and the state machine allows that once per request,
so a batch can't complete early or count a request twice.

The batch summary is built in one place (Batch.summary) and used both by the status API
and as the callback body, so the two can't disagree.
"""

import json
import logging
import uuid
from dataclasses import dataclass

from gateway.callbacks import CallbackJob, CallbackSender, encode
from gateway.clock import Clock
from gateway.core import Gateway, RequestItem
from gateway.store import Store
from gateway.tracker import InferenceRequest, State

log = logging.getLogger("gateway.batches")


@dataclass
class Batch:
    batch_id: str
    total: int
    created_ns: int
    callback_url: str | None = None
    idempotency_key: str | None = None
    succeeded: int = 0
    failed: int = 0
    expired: int = 0
    completed_ns: int | None = None
    callback: dict | None = None  # delivery status, mirrors the callbacks table

    @property
    def finished(self) -> int:
        return self.succeeded + self.failed + self.expired

    @property
    def status(self) -> str:
        if self.completed_ns is None:
            return "processing"
        return "completed" if self.succeeded == self.total else "completed_with_failures"

    def summary(self, store: Store) -> dict:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "expired": self.expired,
            "pending": self.total - self.finished,
            "created_at": store.wall(self.created_ns),
            "completed_at": store.wall(self.completed_ns),
            "results_url": f"/v1/batches/{self.batch_id}/results",
        }


class BatchManager:
    def __init__(self, gateway: Gateway, store: Store, sender: CallbackSender, clock: Clock) -> None:
        self._gateway = gateway
        self._store = store
        self._sender = sender
        self._clock = clock
        self.batches: dict[str, Batch] = {}
        self._by_idempotency_key: dict[str, str] = {}
        gateway.tracker.listeners.append(self._on_transition)

    # --- submit --------------------------------------------------------------------

    async def submit(
        self,
        items: list[RequestItem],
        callback_url: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[Batch, bool]:
        """Returns (batch, created). Same Idempotency-Key -> the original batch."""
        if idempotency_key is not None:
            existing = self._by_idempotency_key.get(idempotency_key) or self._key_from_store(idempotency_key)
            if existing is not None:
                return await self.get(existing), False

        batch_id = f"b_{uuid.uuid4().hex[:20]}"
        # Accept (validate + reserve queue room) but don't queue yet: the rows must be
        # saved first, so "202 Accepted" really means "we won't lose this".
        requests = self._gateway.submit_batch(batch_id, items, enqueue=False)
        batch = Batch(batch_id, len(requests), self._clock.now_ns(), callback_url, idempotency_key)
        self.batches[batch_id] = batch
        if idempotency_key is not None:
            self._by_idempotency_key[idempotency_key] = batch_id
        try:
            await self._store.insert_batch(self._batch_row(batch), requests)
        except Exception:
            log.exception("could not persist batch %s", batch_id)
            # Keep the accounting whole: these requests were accepted, so they end as
            # EXPIRED rather than vanishing.
            now = self._clock.now_ns()
            for request in requests:
                self._gateway.tracker.transition(request, State.EXPIRED, now, error="not_persisted")
            raise
        self._gateway.enqueue(requests)
        return batch, True

    def _batch_row(self, batch: Batch) -> dict:
        return {
            "batch_id": batch.batch_id,
            "idempotency_key": batch.idempotency_key,
            "callback_url": batch.callback_url,
            "total": batch.total,
            "created_at": self._store.wall(batch.created_ns),
            "status": batch.status,
            "succeeded": batch.succeeded,
            "failed": batch.failed,
            "expired": batch.expired,
            "completed_at": self._store.wall(batch.completed_ns),
        }

    # --- completion ----------------------------------------------------------------

    def _on_transition(self, request: InferenceRequest, old: State | None) -> None:
        if not request.state.is_final or request.batch_id is None:
            return
        batch = self.batches.get(request.batch_id)
        if batch is None:
            return
        if request.state is State.SUCCEEDED:
            batch.succeeded += 1
        elif request.state is State.FAILED:
            batch.failed += 1
        else:
            batch.expired += 1
        if batch.finished == batch.total:
            self._complete(batch)

    def _complete(self, batch: Batch) -> None:
        batch.completed_ns = self._clock.now_ns()
        job = None
        callback_row = None
        if batch.callback_url:
            payload = batch.summary(self._store) | {"event": "batch.completed"}
            job = CallbackJob(batch.batch_id, batch.callback_url, encode(payload), f"{batch.batch_id}:completed")
            callback_row = {
                "batch_id": batch.batch_id,
                "url": batch.callback_url,
                "payload": job.body.decode(),
                "next_attempt_at": self._store.wall(batch.completed_ns),
            }
            batch.callback = {"status": "pending", "attempts": 0, "last_error": None}
        # Batch row + callback job in one transaction: the callback can't be lost, and
        # it can't exist before the batch is complete.
        self._store.finish_batch(self._batch_row(batch), callback_row)
        if job is not None:
            self._sender.schedule(job)
        log.info("batch %s %s (%d ok, %d failed, %d expired)",
                 batch.batch_id, batch.status, batch.succeeded, batch.failed, batch.expired)

    def on_callback_status(self, batch_id: str, fields: dict) -> None:
        batch = self.batches.get(batch_id)
        if batch is not None and batch.callback is not None:
            batch.callback.update(fields)

    # --- reading -------------------------------------------------------------------

    async def get(self, batch_id: str) -> Batch | None:
        if batch_id in self.batches:
            return self.batches[batch_id]
        rows = await self._store.read("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        return self._batch_from_row(rows[0]) if rows else None

    def status(self, batch: Batch) -> dict:
        out = batch.summary(self._store)
        if batch.callback_url:
            out["callback"] = {"url": batch.callback_url, **(batch.callback or {"status": "not_sent_yet"})}
        return out

    async def results(self, batch_id: str, cursor: str | None, limit: int) -> dict:
        """One page of per-request results, ordered by request_id. Each request_id is the
        primary key's suffix, so it appears exactly once across all pages."""
        rows = await self._store.read(
            "SELECT request_id, model, state, attempts, error, result, actual_tokens,"
            " submitted_at, finished_at FROM requests WHERE batch_id=? AND request_id > ?"
            " ORDER BY request_id LIMIT ?",
            (batch_id, cursor or "", limit + 1),
        )
        more = len(rows) > limit
        rows = rows[:limit]
        for row in rows:
            row["status"] = row.pop("state")
            if row["finished_at"] is not None:
                row["latency_ms"] = round((row["finished_at"] - row["submitted_at"]) * 1000, 3)
        return {"results": rows, "next_cursor": rows[-1]["request_id"] if more else None}

    def _key_from_store(self, idempotency_key: str) -> str | None:
        rows = self._store.read_sync("SELECT batch_id FROM batches WHERE idempotency_key=?", (idempotency_key,))
        return rows[0]["batch_id"] if rows else None

    def _batch_from_row(self, row: dict) -> Batch:
        batch = Batch(
            batch_id=row["batch_id"],
            total=row["total"],
            created_ns=self._store.mono_ns(row["created_at"]),
            callback_url=row["callback_url"],
            idempotency_key=row["idempotency_key"],
            succeeded=row["succeeded"],
            failed=row["failed"],
            expired=row["expired"],
            completed_ns=None if row["completed_at"] is None else self._store.mono_ns(row["completed_at"]),
        )
        if batch.callback_url:
            cb = self._store.read_sync(
                "SELECT status, attempts, last_error, delivered_at FROM callbacks WHERE batch_id=?",
                (batch.batch_id,),
            )
            batch.callback = cb[0] if cb else None
        return batch

    # --- restart recovery ----------------------------------------------------------

    def restore(self) -> dict[str, int]:
        """Reload unfinished work after a restart:
        - unfinished batches, with counters rebuilt from their already-final requests;
        - every non-final request goes back to the queue (deadline kept, attempts reset);
        - pending callbacks are rescheduled with their attempt count."""
        stats = {"batches": 0, "requests": 0, "callbacks": 0}
        for row in self._store.read_sync("SELECT * FROM batches WHERE completed_at IS NULL"):
            batch = self._batch_from_row(row)
            counts = self._store.read_sync(
                "SELECT state, COUNT(*) AS n FROM requests WHERE batch_id=? GROUP BY state", (batch.batch_id,)
            )
            by_state = {c["state"]: c["n"] for c in counts}
            batch.succeeded = by_state.get("succeeded", 0)
            batch.failed = by_state.get("failed", 0)
            batch.expired = by_state.get("expired", 0)
            self.batches[batch.batch_id] = batch
            if batch.idempotency_key:
                self._by_idempotency_key[batch.idempotency_key] = batch.batch_id
            stats["batches"] += 1

        for row in self._store.read_sync("SELECT * FROM requests WHERE state='queued'"):
            request = InferenceRequest(
                key=row["key"],
                request_id=row["request_id"],
                batch_id=row["batch_id"],
                model=row["model"],
                estimated_tokens=row["estimated_tokens"],
                payload=None if row["payload"] is None else json.loads(row["payload"]),
                submitted_ns=self._store.mono_ns(row["submitted_at"]),
                deadline_ns=self._store.mono_ns(row["deadline_at"]),
            )
            self._gateway.restore(request)
            stats["requests"] += 1

        # Read pending callbacks before completing anything below: completing schedules a
        # new callback, and reading after it was committed would schedule it twice.
        for row in self._store.read_sync("SELECT * FROM callbacks WHERE status='pending'"):
            job = CallbackJob(row["batch_id"], row["url"], row["payload"].encode(),
                              f"{row['batch_id']}:completed", attempts=row["attempts"])
            due = self._store.mono_ns(row["next_attempt_at"]) if row["next_attempt_at"] else None
            self._sender.schedule(job, due)
            if row["batch_id"] not in self.batches:
                batch = self._batch_from_row(
                    self._store.read_sync("SELECT * FROM batches WHERE batch_id=?", (row["batch_id"],))[0]
                )
                self.batches[batch.batch_id] = batch
            stats["callbacks"] += 1
        # A batch whose last requests finished just before the crash, but whose
        # completion wasn't written, completes now.
        for batch in list(self.batches.values()):
            if batch.completed_ns is None and batch.finished == batch.total:
                self._complete(batch)

        return stats

