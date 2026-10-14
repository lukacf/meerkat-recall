"""Durable, nonrefundable physical-dispatch reservations. No provider calls."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import sqlite3
import threading
import time

from wire import Denied, Request, digest, encode, require

OPERATIONS = {
    # Document/query share Recall's active profile and credential. This combined
    # authenticated quota does NOT enforce separate 28/48 operation allowances.
    "embedding": 76, "triage": 0,
    "extraction": 112, "support_verification": 56, "summary": 8,
    "promotion_and_identity_disambiguation": 4, "truth_maintenance": 4,
    "wiki_planner_writer_verifier": 12,
}
BACKENDS = ("surreal-db", "elephant-db")
COUNT_CAP = 432
MONEY_CAP = 75_000_000_000  # integer nano-USD; never binary floating point
TIME_CAP = 5400
CONCURRENCY_CAP = 2
# This is a GATEWAY transport/capture bound, not Recall's public search cap.
# It is an explicit qualification proposal delta requiring independent review.
RESPONSE_CAP = 8 * 1024 * 1024


def sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def clock() -> tuple[float, float, str]:
    return time.time(), time.monotonic(), Path("/proc/sys/kernel/random/boot_id").read_text().strip()


class Ledger:
    """One host supervisor holds an exclusive lock for the run's lifetime.

    Creating a run and resuming one are intentionally different commands. An
    incomplete initialization or any ambiguous dispatch requires operator review;
    deleting the ledger never authorizes a fresh budget for an existing anchor.
    """

    def __init__(self, directory: Path, manifest_hash: str, *, create: bool,
                 now=clock):
        self.directory = directory
        accounting_policy = getattr(self, "ACCOUNTING_POLICY", "nonrefundable-v1")
        require(accounting_policy in ("nonrefundable-v1", "full-context-terminal-v2", "separate-exclusive-workload-embedding-reservation/v1",
                                      "separate-exclusive-semantic140-embedding-reservation/v1"), "unknown_accounting_policy")
        self.now = now
        self.mutex = threading.RLock()
        self.broken = False
        if create:
            # Independent sibling anchor survives accidental ledger-directory
            # deletion. Never remove/reinitialize this file to retry a run.
            anchor = directory.parent / (".recall-gateway-" + manifest_hash + ".anchor")
            anchor_fd = os.open(anchor, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(anchor_fd, "wb") as output:
                output.write(encode({"manifest_sha256": manifest_hash, "directory": str(directory.absolute())}))
                output.flush()
                os.fsync(output.fileno())
            sync_directory(directory.parent)
            directory.mkdir(mode=0o700, parents=False, exist_ok=False)
            sync_directory(directory.parent)
            self.lock_fd = os.open(directory / "owner.lock", os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        else:
            anchor = directory.parent / (".recall-gateway-" + manifest_hash + ".anchor")
            require(anchor.is_file() and not anchor.is_symlink(), "missing_run_anchor")
            require(anchor.read_bytes() == encode({"manifest_sha256": manifest_hash, "directory": str(directory.absolute())}), "run_anchor_mismatch")
            require(directory.is_dir() and not directory.is_symlink(), "missing_run_anchor")
            require((directory / "ledger.sqlite3").is_file(), "missing_run_ledger")
            self.lock_fd = os.open(directory / "owner.lock", os.O_RDWR | os.O_NOFOLLOW)
        fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = directory / "ledger.sqlite3"
        if create:
            fd = os.open(db, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        require(not db.is_symlink(), "unsafe_ledger_path")
        self.db = sqlite3.connect(f"file:{db}?mode=rw", uri=True, isolation_level=None,
                                  check_same_thread=False, timeout=5)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        require(self.db.execute("PRAGMA quick_check").fetchone() == ("ok",), "corrupt_ledger")
        if create:
            wall, mono, boot = self.now()
            with self.transaction():
                if accounting_policy != "nonrefundable-v1":
                    self.db.execute("CREATE TABLE accounting_mode (policy TEXT NOT NULL)")
                    self.db.execute("INSERT INTO accounting_mode VALUES (?)", (accounting_policy,))
                self.db.execute("CREATE TABLE run (manifest TEXT, wall_start REAL, mono_start REAL, boot TEXT, last_wall REAL, halted TEXT)")
                self.db.execute("INSERT INTO run VALUES (?,?,?,?,?,NULL)", (manifest_hash, wall, mono, boot, wall))
                self.db.execute("""CREATE TABLE dispatch (
                    id INTEGER PRIMARY KEY, backend TEXT, space TEXT, operation TEXT,
                    model TEXT, input_bound INTEGER, output_cap INTEGER, reserved INTEGER,
                    request_hash TEXT, forwarded_hash TEXT, transforms TEXT,
                    started REAL, finished REAL, outcome TEXT, status INTEGER,
                    response_hash TEXT, response_bytes INTEGER, usage_input INTEGER,
                    usage_output INTEGER, requested_output_cap INTEGER)""")
            sync_directory(directory)
        else:
            # No mode conversion or fallback: old genuine v1 ledgers have no
            # marker, while v2 must carry its marker from initial creation.
            tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if accounting_policy == "nonrefundable-v1":
                if "accounting_mode" in tables or "settlement_policy" in tables:
                    self.close()
                    raise Denied("accounting_mode_mismatch")
            elif "accounting_mode" not in tables or self.db.execute("SELECT policy FROM accounting_mode").fetchall() != [(accounting_policy,)]:
                self.close()
                raise Denied("accounting_mode_mismatch")
            with self.transaction():
                require(self.db.execute("SELECT manifest FROM run").fetchone() == (manifest_hash,), "run_manifest_mismatch")
                if self.db.execute("SELECT 1 FROM dispatch WHERE outcome IS NULL LIMIT 1").fetchone():
                    self.db.execute("UPDATE run SET halted='ambiguous_dispatch_after_restart'")
            # Preserve the halt on disk even when initialization fails.
            with self.transaction():
                self.check_time()

    @contextmanager
    def transaction(self):
        with self.mutex:
            require(not self.broken, "ledger_unavailable")
            try:
                self.db.execute("BEGIN IMMEDIATE")
                yield
                self.db.execute("COMMIT")
            except Denied:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                raise
            except BaseException:
                self.broken = True
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                raise

    def check_time(self) -> float:
        wall, mono, boot = self.now()
        row = self.db.execute("SELECT wall_start,mono_start,boot,last_wall,halted FROM run").fetchone()
        require(row is not None and row[4] is None, "run_halted")
        require(boot == row[2], "boot_changed")
        require(wall >= row[3] and mono >= row[1], "clock_rollback")
        remaining = min(TIME_CAP - (wall - row[0]), TIME_CAP - (mono - row[1]))
        require(remaining > 0, "run_time_limit")
        self.db.execute("UPDATE run SET last_wall=?", (wall,))
        return remaining

    def reserve(self, request: Request, authorize) -> tuple[int, float]:
        """The returned ID exists in a FULL-synchronous commit before any send."""
        with self.transaction():
            remaining = self.check_time()
            authorize()
            scope = request.scope
            require(scope.backend in BACKENDS and scope.operation in OPERATIONS, "unknown_scope")
            total, money, active = self.db.execute(
                "SELECT COUNT(*),COALESCE(SUM(reserved),0),COALESCE(SUM(outcome IS NULL),0) FROM dispatch").fetchone()
            require(total < COUNT_CAP, "dispatch_count_limit")
            require(money + request.reservation <= MONEY_CAP, "money_limit")
            require(active < CONCURRENCY_CAP, "concurrency_limit")
            used = self.db.execute("SELECT COUNT(*) FROM dispatch WHERE backend=? AND operation=?",
                                   (scope.backend, scope.operation)).fetchone()[0]
            require(used < OPERATIONS[scope.operation], "operation_count_limit")
            cursor = self.db.execute("""INSERT INTO dispatch
                (backend,space,operation,model,input_bound,output_cap,reserved,request_hash,
                 forwarded_hash,transforms,started,requested_output_cap) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (scope.backend, scope.space, scope.operation, scope.model, request.input_bound,
                 request.output_cap, request.reservation, request.original_hash,
                 request.forwarded_hash, ",".join(request.transforms), self.now()[0], request.requested_output_cap))
            identifier = cursor.lastrowid
        return identifier, min(120.0, remaining)

    def before_forward(self, authorize) -> float:
        with self.transaction():
            remaining = self.check_time()
            authorize()
            return min(120.0, remaining)

    def finish(self, identifier: int, *, outcome: str, status: int | None,
               response_hash: str, response_bytes: int, usage: tuple[int, int] | None,
               halt: bool) -> None:
        require(outcome in ("complete", "upstream_error", "incomplete", "usage_bound_violation"), "invalid_outcome")
        with self.transaction():
            row = self.db.execute("SELECT outcome,input_bound,output_cap FROM dispatch WHERE id=?", (identifier,)).fetchone()
            require(row is not None and row[0] is None, "invalid_dispatch_completion")
            if usage and (usage[0] > row[1] or usage[1] > row[2]):
                outcome, halt = "usage_bound_violation", True
            self.db.execute("""UPDATE dispatch SET finished=?,outcome=?,status=?,response_hash=?,
                response_bytes=?,usage_input=?,usage_output=? WHERE id=?""",
                (self.now()[0], outcome, status, response_hash, response_bytes,
                 usage[0] if usage else None, usage[1] if usage else None, identifier))
            if halt:
                self.db.execute("UPDATE run SET halted=COALESCE(halted,?)", (outcome,))
        # No successful/error/timeout path decreases reserved dollars or count.

    def receipt(self) -> dict:
        with self.mutex:
            rows = self.db.execute("SELECT id,backend,space,operation,model,input_bound,output_cap,reserved,request_hash,forwarded_hash,transforms,started,finished,outcome,status,response_hash,response_bytes,usage_input,usage_output,requested_output_cap FROM dispatch ORDER BY id")
            names = [column[0] for column in rows.description]
            entries = [dict(zip(names, row)) for row in rows]
            return {"format": "recall-gateway-receipt/v1", "manifest_sha256": self.db.execute("SELECT manifest FROM run").fetchone()[0],
                    "halted": self.db.execute("SELECT halted FROM run").fetchone()[0],
                    "reserved_nano_usd": sum(row["reserved"] for row in entries), "dispatches": entries,
                    "raw_capture": "private-synthetic-bodies-v1", "provider_quality_acceptance": False}

    def close(self) -> None:
        self.db.close()
        os.close(self.lock_fd)
