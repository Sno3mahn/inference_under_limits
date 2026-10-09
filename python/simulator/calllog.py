"""Append-only JSONL log of everything the provider saw.

This is the evidence the verifier uses. It is written by the provider, not the gateway,
so a bug in the gateway's own accounting can't hide a limit violation.

Line types:
  {"type": "start", "wall_time": "...", "t_ns": 0}
  {"type": "limits", "t_ns": ..., "model": ..., "rpm": ..., "tpm": ...}
  {"type": "call", "t_ns": ..., "model": ..., "request_id": ..., "attempt": ...,
   "tokens": ..., "status": ..., "kind": ..., "counted": true}

t_ns is the simulator's monotonic clock. "counted" says whether the call used up rate-limit
capacity: rejected (429 from the limiter), replayed or invalid calls don't.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO


class CallLog:
    def __init__(self, path: str | Path | None) -> None:
        self._file: TextIO | None = None
        self._buffer: list[str] = []
        if path is not None:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._file = open(path, "w")

    def start(self, t_ns: int) -> None:
        self.write({"type": "start", "wall_time": datetime.now(UTC).isoformat(), "t_ns": t_ns})

    def write(self, record: dict) -> None:
        if self._file is not None:
            self._buffer.append(json.dumps(record, separators=(",", ":")))

    def flush(self) -> None:
        if self._file is not None and self._buffer:
            self._file.write("\n".join(self._buffer) + "\n")
            self._file.flush()
            self._buffer.clear()

    def close(self) -> None:
        self.flush()
        if self._file is not None:
            self._file.close()
            self._file = None
