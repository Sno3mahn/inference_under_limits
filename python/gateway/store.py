"""SQLite persistence: the durable record of batches, requests and callbacks.

What gets written (see docs/ARCHITECTURE.md §8):
- a request row when it's accepted (state 'queued'), updated once when it's final;
- a batch row when it's accepted, updated when the batch is complete;
- a callback row created in the same transaction that completes the batch, so a callback
  can be neither lost nor sent early, then updated after each delivery attempt.
In-flight / retry states are not written. After a restart those requests go back to the
queue, which is safe because the provider dedupes on the idempotency key.

Gotcha: SQLite allows one writer at a time. So all writes go through one background thread
that commits in groups (every few ms, or up to 2,000 operations). The event loop only
puts operations on a queue, which costs microseconds, and never waits on disk unless it
asks to (a batch ack waits for its rows to be committed, so "202 Accepted" means "saved").

Times are stored as wall-clock epoch seconds, since the monotonic clock restarts at boot.
"""

import asyncio
import json
import logging
import queue
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from gateway.clock import NS_PER_SECOND, Clock
from gateway.tracker import InferenceRequest

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    callback_url TEXT,
    total INTEGER NOT NULL,
    created_at REAL NOT NULL,
    status TEXT NOT NULL,
    succeeded INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    expired INTEGER NOT NULL DEFAULT 0,
    completed_at REAL
);
CREATE TABLE IF NOT EXISTS requests (
    key TEXT PRIMARY KEY,
    batch_id TEXT,
    request_id TEXT NOT NULL,
    model TEXT NOT NULL,
    estimated_tokens INTEGER NOT NULL,
    payload TEXT,
    submitted_at REAL NOT NULL,
    deadline_at REAL NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    result TEXT,
    actual_tokens INTEGER,
    finished_at REAL
);
CREATE INDEX IF NOT EXISTS requests_by_batch ON requests (batch_id, request_id);
CREATE TABLE IF NOT EXISTS callbacks (
    batch_id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,          -- pending | delivered | failed
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL,
    last_error TEXT,
    delivered_at REAL
);
"""

_STOP = object()
log = logging.getLogger("gateway.store")


class Store:
    def __init__(self, path: str | Path, clock: Clock, max_group: int = 2_000) -> None:
        self._path = str(path)  # a file: the reader and writer need separate connections to one db
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        # Wall time = monotonic time + this offset, fixed at startup.
        self._wall_offset_s = time.time() - clock.now_ns() / NS_PER_SECOND
        self._max_group = max_group
        self._ops: queue.SimpleQueue = queue.SimpleQueue()
        self._reader = self._connect()
        self._reader_lock = threading.Lock()
        self._reader.executescript(SCHEMA)
        self._thread: threading.Thread | None = None
        self.writes = 0  # operations committed, for metrics
        self.commits = 0

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
        conn.execute("PRAGMA synchronous=NORMAL")  # survives an app crash; an OS crash may lose the last ms
        conn.row_factory = sqlite3.Row
        return conn

    def wall(self, mono_ns: int | None) -> float | None:
        return None if mono_ns is None else mono_ns / NS_PER_SECOND + self._wall_offset_s

    def mono_ns(self, wall_s: float) -> int:
        return round((wall_s - self._wall_offset_s) * NS_PER_SECOND)

    # --- writer thread -----------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(target=self._writer, name="sqlite-writer", daemon=True)
        self._thread.start()

    def close(self) -> None:
        if self._thread is not None:
            self._ops.put(_STOP)
            self._thread.join()
            self._thread = None
        self._reader.close()

    def _writer(self) -> None:
        conn = self._connect()
        stop = False
        while not stop:
            group = [self._ops.get()]  # block until there's work
            deadline = time.monotonic() + 0.005
            while len(group) < self._max_group and time.monotonic() < deadline:
                try:
                    group.append(self._ops.get(timeout=0.001))
                except queue.Empty:
                    break
            if _STOP in group:
                stop = True
                group = [op for op in group if op is not _STOP]
            if not group:
                continue
            try:
                errors = self._commit(conn, group)
            except Exception:
                # One bad operation must not throw away everyone else's writes in the
                # group: retry them one per transaction so only the bad one fails.
                log.exception("group commit failed; retrying operations one by one")
                errors = [self._commit(conn, [op])[0] for op in group]
            for (fn, _, waiter), error in zip(group, errors, strict=True):
                if error is not None:
                    log.error("write %s failed: %r", fn.__name__, error)
                if waiter is not None:
                    loop, future = waiter
                    loop.call_soon_threadsafe(_resolve, future, error)
        conn.close()

    def _commit(self, conn: sqlite3.Connection, ops: list) -> list[Exception | None]:
        """Run ops in one transaction. Raises if a group of several fails; for a single op
        returns its error instead, so the caller can report it."""
        try:
            conn.execute("BEGIN")
            for fn, args, _ in ops:
                fn(conn, *args)
            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            if len(ops) > 1:
                raise
            return [exc]
        self.writes += len(ops)
        self.commits += 1
        return [None] * len(ops)

    def _submit(self, fn: Callable, *args) -> None:
        self._ops.put((fn, args, None))

    async def _submit_and_wait(self, fn: Callable, *args) -> None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._ops.put((fn, args, (loop, future)))
        await future

    # --- write operations (called from the event loop) ---------------------------

    def _request_row(self, r: InferenceRequest) -> tuple:
        return (
            r.key, r.batch_id, r.request_id, r.model, r.estimated_tokens,
            json.dumps(r.payload), self.wall(r.submitted_ns), self.wall(r.deadline_ns), r.state.value,
        )

    async def insert_batch(self, batch_row: dict, requests: list[InferenceRequest]) -> None:
        rows = [self._request_row(r) for r in requests]
        await self._submit_and_wait(_insert_batch, batch_row, rows)

    def insert_request(self, request: InferenceRequest) -> None:
        self._submit(_insert_requests, [self._request_row(request)])

    def finish_request(self, r: InferenceRequest) -> None:
        self._submit(_finish_request, (
            r.state.value, r.attempts, r.error, json.dumps(r.result) if r.result is not None else None,
            r.actual_tokens, self.wall(r.finished_ns), r.key,
        ))

    def finish_batch(self, batch_row: dict, callback_row: dict | None) -> None:
        self._submit(_finish_batch, batch_row, callback_row)

    def update_callback(self, batch_id: str, **fields: Any) -> None:
        self._submit(_update_callback, batch_id, fields)

    async def barrier(self) -> None:
        """Wait until everything queued so far is committed."""
        await self._submit_and_wait(lambda conn: None)

    # --- reads (run in a worker thread so the event loop isn't blocked) ----------

    async def read(self, sql: str, params: tuple = ()) -> list[dict]:
        def run() -> list[dict]:
            with self._reader_lock:
                return [dict(row) for row in self._reader.execute(sql, params)]

        return await asyncio.to_thread(run)

    def read_sync(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._reader_lock:
            return [dict(row) for row in self._reader.execute(sql, params)]


def _resolve(future: asyncio.Future, error: BaseException | None) -> None:
    if future.done():
        return
    if error is None:
        future.set_result(None)
    else:
        future.set_exception(error)


def _insert_requests(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    conn.executemany(
        # OR IGNORE: re-adding a request on restart recovery must not fail on its existing row.
        "INSERT OR IGNORE INTO requests (key, batch_id, request_id, model, estimated_tokens, payload,"
        " submitted_at, deadline_at, state) VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )


def _insert_batch(conn: sqlite3.Connection, batch: dict, rows: list[tuple]) -> None:
    conn.execute(
        "INSERT INTO batches (batch_id, idempotency_key, callback_url, total, created_at, status)"
        " VALUES (:batch_id, :idempotency_key, :callback_url, :total, :created_at, :status)",
        batch,
    )
    _insert_requests(conn, rows)


def _finish_request(conn: sqlite3.Connection, row: tuple) -> None:
    conn.execute(
        "UPDATE requests SET state=?, attempts=?, error=?, result=?, actual_tokens=?, finished_at=?"
        " WHERE key=?",
        row,
    )


def _finish_batch(conn: sqlite3.Connection, batch: dict, callback: dict | None) -> None:
    conn.execute(
        "UPDATE batches SET status=:status, succeeded=:succeeded, failed=:failed, expired=:expired,"
        " completed_at=:completed_at WHERE batch_id=:batch_id",
        batch,
    )
    if callback is not None:
        conn.execute(
            "INSERT INTO callbacks (batch_id, url, payload, status, next_attempt_at)"
            " VALUES (:batch_id, :url, :payload, 'pending', :next_attempt_at)",
            callback,
        )


def _update_callback(conn: sqlite3.Connection, batch_id: str, fields: dict) -> None:
    assignments = ", ".join(f"{name}=?" for name in fields)
    conn.execute(f"UPDATE callbacks SET {assignments} WHERE batch_id=?", (*fields.values(), batch_id))
