"""Cross-process ledger for one original owned container qualification budget."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import stat
import time


def require(ok, message):
    if not ok:
        raise ValueError(message)


class BudgetLedger:
    def __init__(self, path, nonce):
        self.path, self.nonce = Path(path), nonce
        require(self.path.is_absolute() and self.path.resolve() == self.path,
                "original canonical budget path required")

    @classmethod
    def create(cls, path, nonce, *, calls, written, maximum_calls, maximum_bytes, deadline_ns):
        value = cls(path, nonce)
        state = {"format": "recall-container-shared-budget/v1", "nonce": nonce, "calls": calls,
                 "written": written, "held": {}, "maximum_calls": maximum_calls,
                 "maximum_bytes": maximum_bytes, "deadline_ns": deadline_ns}
        value.validate(state)
        fd = os.open(value.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            raw = json.dumps(state, sort_keys=True, separators=(",", ":")).encode()
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(fd)
        finally:
            os.close(fd)
        return value

    def validate(self, state):
        require(type(state) is dict and set(state) == {"format", "nonce", "calls", "written", "held",
                "maximum_calls", "maximum_bytes", "deadline_ns"}
                and state["format"] == "recall-container-shared-budget/v1" and state["nonce"] == self.nonce,
                "original budget identity or shape changed")
        require(all(type(state[key]) is int and state[key] >= 0 for key in
                    ("calls", "written", "maximum_calls", "maximum_bytes", "deadline_ns"))
                and 0 < state["maximum_calls"] <= 1000 and 0 < state["maximum_bytes"] <= 96 * 1024**2,
                "original combined resource ceiling changed")
        require(type(state["held"]) is dict and set(state["held"]) <= {"provider"}
                and all(type(value) is int and value >= 0 for value in state["held"].values()),
                "invalid original provider reservation")
        require(state["calls"] <= state["maximum_calls"]
                and state["written"] + sum(state["held"].values()) <= state["maximum_bytes"],
                "original shared budget exhausted")

    def change(self, *, calls=0, written=0, held=None, allow_expired=False):
        require(type(calls) is int and calls >= 0 and type(written) is int and written >= 0,
                "budget cannot discard prior observations")
        fd = os.open(self.path, os.O_RDWR | os.O_NOFOLLOW)
        try:
            require(stat.S_ISREG(os.fstat(fd).st_mode) and os.fstat(fd).st_size <= 4096,
                    "bounded original regular budget file required")
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.read(fd, 4097)
            def unique(pairs):
                result = {}
                for key, value in pairs:
                    require(key not in result, "duplicate budget field")
                    result[key] = value
                return result
            state = json.loads(raw, object_pairs_hook=unique)
            self.validate(state)
            require(allow_expired or time.monotonic_ns() < state["deadline_ns"], "original allocation deadline expired")
            state["calls"] += calls
            state["written"] += written
            if held is not None:
                expected, next_value = held
                require(type(expected) is int and type(next_value) is int and next_value >= 0
                        and state["held"].get("provider", 0) == expected,
                        "provider reservation cannot adopt a different original state")
                state["held"]["provider"] = next_value
            self.validate(state)
            if calls or written or held is not None:
                encoded = json.dumps(state, sort_keys=True, separators=(",", ":")).encode()
                os.lseek(fd, 0, os.SEEK_SET)
                with os.fdopen(fd, "wb", closefd=False) as stream:
                    stream.write(encoded)
                    stream.truncate()
                    stream.flush()
                    os.fsync(fd)
            return state
        finally:
            os.close(fd)
