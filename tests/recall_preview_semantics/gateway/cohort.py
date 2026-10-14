"""Separate D28 embedding allocation: exclusive cohorts, no refunds or sends."""
from __future__ import annotations

import os
from pathlib import Path
import stat

from cohort_profile import SEMANTIC140, profile_for_id
from ledger import Ledger, BACKENDS, RESPONSE_CAP
from wire import Denied, EMBEDDING_RESERVATION_NANO_USD, MAX_BODY_BYTES, digest, encode, require, strict_json, validate

POLICY = "separate-exclusive-workload-embedding-reservation/v1"
COUNT_CAP = 2100
MONEY_CAP = 3_000_000_000
TIME_CAP = 7800
CAPTURE_CAP = 20 * 1024**3
CAPTURE_SLOT = 2 * MAX_BODY_BYTES + RESPONSE_CAP + 32768
SPACES = ("qualification-household", "qualification-business")
PHASES = tuple((backend, phase) for backend in ("elephant-db", "surreal-db") for phase in ("preparation", "measurement"))
PHASE_LIMITS = {"preparation": (450, 225, 2, 3600), "measurement": (600, 300, 50, 120)}


def allocation_contract(profile_id="d28-v1") -> dict:
    """Exact finite proposal, not an execution authorization."""
    profile = profile_for_id(profile_id)
    result = {"format": "recall-exclusive-embedding-allocation/v1", "accounting_policy": profile.accounting_policy,
            "physical_dispatch_cap": profile.count_cap, "money_cap_nano_usd": profile.money_cap,
            "reservation_per_dispatch_nano_usd": EMBEDDING_RESERVATION_NANO_USD,
            "time_cap_seconds": profile.time_cap, "gateway_capture_cap_bytes": profile.capture_cap,
            "request_body_cap_bytes": MAX_BODY_BYTES, "response_body_cap_bytes": RESPONSE_CAP,
            "embedding_input_bound": 8192, "embedding_batch_items": 64,
            "public_sources_per_backend": 50, "public_queries_per_backend": profile.public_queries,
            "harness_query_retries": 0, "generation_dispatch_cap": 0,
            "spaces": list(SPACES), "phases": [
                {"backend": backend, "phase": phase, "physical_dispatch_cap": profile.phase_limits(phase)[0],
                 "per_space_cap": profile.phase_limits(phase)[1], "concurrency_cap": profile.phase_limits(phase)[2],
                 "time_cap_seconds": profile.phase_limits(phase)[3]} for backend, phase in PHASES]}

    if profile == SEMANTIC140:
        result["format"] = "recall-semantic140-embedding-allocation/v1"
        result["profile_id"] = profile.profile_id
    return result


def read_capture(path: Path, cap: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and not info.st_mode & 0o077 and info.st_size <= cap, "unsafe_cohort_capture")
        with os.fdopen(fd, "rb", closefd=False) as source:
            value = source.read(cap + 1)
        require(len(value) == info.st_size, "cohort_capture_changed")
        return value
    finally:
        os.close(fd)


class CohortLedger(Ledger):
    ACCOUNTING_POLICY = POLICY
    capture_original_transport = True

    def __init__(self, directory: Path, manifest_hash: str, *, create: bool,
                 identity: dict, scopes: dict, now=None, profile_id="d28-v1"):
        self.profile = profile_for_id(profile_id)
        self.ACCOUNTING_POLICY = self.profile.accounting_policy
        self.identity = strict_json(encode(identity))
        self.scopes = dict(scopes)
        require(set(identity) == {"account_id", "credential_generation", "configuration_sha256",
                                  "bindings_sha256", "allocation_sha256", "contract_sha256", "corpus_sha256",
                                  "preparation_contract_sha256", "typed_delta_sha256"},
                "cohort_identity_fields")
        require(scopes and all(scope.operation == "embedding" and scope.backend in BACKENDS
                              and scope.space in SPACES and scope.binding_id == name
                              and scope.credential_generation == identity["credential_generation"]
                              for name, scope in scopes.items()), "cohort_scope_identity")
        require(len(scopes) == 4 and len({(scope.backend, scope.space) for scope in scopes.values()}) == 4,
                "cohort_scope_coverage")
        if self.profile == SEMANTIC140:
            require(all(identity[key] == expected for key, expected in (
                ("allocation_sha256", self.profile.allocation_sha256),
                ("contract_sha256", self.profile.contract_sha256),
                ("corpus_sha256", self.profile.corpus_sha256),
                ("preparation_contract_sha256", self.profile.preparation_sha256),
                ("typed_delta_sha256", self.profile.typed_delta_sha256))), "cohort_profile_identity")
        else:
            require(identity["allocation_sha256"] != SEMANTIC140.allocation_sha256,
                    "cohort_profile_required")
        kwargs = {"now": now} if now is not None else {}
        super().__init__(directory, manifest_hash, create=create, **kwargs)
        expected = encode({"identity": identity, "scopes": {name: scope.__dict__ for name, scope in scopes.items()},
                           "limits": allocation_contract(self.profile.profile_id)}).decode()
        with self.transaction():
            if create:
                self.db.execute("CREATE TABLE cohort_policy (value TEXT NOT NULL)")
                self.db.execute("INSERT INTO cohort_policy VALUES (?)", (expected,))
                self.db.execute("""CREATE TABLE cohort (
                    id INTEGER PRIMARY KEY, backend TEXT, phase TEXT, caller TEXT UNIQUE,
                    state TEXT, started_wall REAL, started_mono REAL, deadline REAL,
                    baseline TEXT, baseline_hash TEXT, receipt TEXT, observation TEXT)""")
                self.db.execute("""CREATE TABLE cohort_dispatch (
                    dispatch_id INTEGER PRIMARY KEY REFERENCES dispatch(id), cohort_id INTEGER,
                    binding TEXT, send_intent INTEGER NOT NULL DEFAULT 0)""")
            else:
                require(self.db.execute("SELECT value FROM cohort_policy").fetchall() == [(expected,)],
                        "cohort_policy_changed")
                # A resumed live cohort cannot mint a replacement interval.
                if self.db.execute("SELECT 1 FROM cohort WHERE state != 'closed'").fetchone():
                    self.db.execute("UPDATE run SET halted='ambiguous_cohort_after_restart'")
        if not create and self.db.execute("SELECT halted FROM run").fetchone() != (None,):
            self.close()
            raise Denied("run_halted")

    def check_time(self) -> float:
        wall, mono, boot = self.now()
        row = self.db.execute("SELECT wall_start,mono_start,boot,last_wall,halted FROM run").fetchone()
        require(row is not None and row[4] is None, "run_halted")
        require(boot == row[2], "boot_changed")
        require(wall >= row[3] and mono >= row[1], "clock_rollback")
        remaining = min(self.profile.time_cap - (wall - row[0]), self.profile.time_cap - (mono - row[1]))
        require(remaining > 0, "run_time_limit")
        self.db.execute("UPDATE run SET last_wall=?", (wall,))
        return remaining

    def begin(self, backend: str, phase: str, caller: str, authorize) -> dict:
        require(type(caller) is str and len(caller) == 64, "cohort_caller_identity")
        with self.transaction():
            remaining = self.check_time()
            authorize()
            require(not self.db.execute("SELECT 1 FROM cohort WHERE state != 'closed'").fetchone(), "cohort_not_drained")
            require(not self.db.execute("SELECT 1 FROM dispatch WHERE outcome IS NULL").fetchone(), "dispatch_not_drained")
            count = self.db.execute("SELECT COUNT(*) FROM cohort").fetchone()[0]
            require(count < len(PHASES) and PHASES[count] == (backend, phase), "cohort_order_or_replay")
            require(not self.db.execute("SELECT 1 FROM cohort WHERE caller=?", (caller,)).fetchone(), "cohort_caller_replay")
            baseline = encode(super().receipt()).decode()
            baseline_hash = digest(baseline.encode())
            wall, mono, _ = self.now()
            deadline = mono + min(remaining, self.profile.phase_limits(phase)[3])
            self.db.execute("INSERT INTO cohort VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL)",
                            (count + 1, backend, phase, caller, "open", wall, mono, deadline, baseline, baseline_hash))
            result = {"format": "recall-exclusive-cohort-begin/v1", "cohort_id": count + 1,
                      "backend": backend, "phase": phase, "baseline_sha256": baseline_hash,
                      "allocation_sha256": self.identity["allocation_sha256"],
                      "authority": "exclusive_gateway_cohort"}
        return result

    def _open(self):
        row = self.db.execute("SELECT id,backend,phase,deadline FROM cohort WHERE state='open'").fetchone()
        require(row is not None, "no_open_cohort")
        require(self.now()[1] < row[3], "cohort_deadline")
        return row

    def binding(self, scope) -> dict:
        require(self.scopes.get(scope.binding_id) == scope, "cohort_binding_mismatch")
        return {"identity": self.identity, "scope": scope.__dict__}

    def reserve(self, request, authorize) -> tuple[int, float]:
        with self.transaction():
            remaining = self.check_time()
            authorize()
            cohort, backend, phase, deadline = self._open()
            scope = request.scope
            binding = encode(self.binding(scope)).decode()
            checked = validate(request.body, request.path, scope)
            require(checked.body == request.body and checked.forwarded_hash == request.forwarded_hash
                    and checked.input_bound == request.input_bound and checked.output_cap == request.output_cap
                    and checked.reservation == request.reservation, "cohort_wire_mismatch")
            require(scope.backend == backend and request.path == "/v1/embeddings"
                    and request.reservation == EMBEDDING_RESERVATION_NANO_USD
                    and 0 < request.input_bound <= 8192 and request.output_cap == 0
                    and not request.streaming and not request.transforms, "cohort_request_scope")
            count, money = self.db.execute("SELECT COUNT(*),COALESCE(SUM(reserved),0) FROM dispatch").fetchone()
            require(count < self.profile.count_cap, "dispatch_count_limit")
            require(money + request.reservation <= self.profile.money_cap, "money_limit")
            require((count + 1) * CAPTURE_SLOT <= self.profile.capture_cap, "total_capture_limit")
            used, active = self.db.execute("SELECT COUNT(*),COALESCE(SUM(d.outcome IS NULL),0) FROM dispatch d JOIN cohort_dispatch c ON c.dispatch_id=d.id WHERE c.cohort_id=?", (cohort,)).fetchone()
            in_space = self.db.execute("SELECT COUNT(*) FROM dispatch d JOIN cohort_dispatch c ON c.dispatch_id=d.id WHERE c.cohort_id=? AND d.space=?", (cohort, scope.space)).fetchone()[0]
            total_cap, space_cap, concurrency, _ = self.profile.phase_limits(phase)
            require(used < total_cap and in_space < space_cap, "cohort_count_limit")
            require(active < concurrency, "cohort_concurrency_limit")
            cursor = self.db.execute("""INSERT INTO dispatch
                (backend,space,operation,model,input_bound,output_cap,reserved,request_hash,
                 forwarded_hash,transforms,started,requested_output_cap) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (scope.backend, scope.space, scope.operation, scope.model, request.input_bound, 0,
                 request.reservation, request.original_hash, request.forwarded_hash, "", self.now()[0], 0))
            identifier = cursor.lastrowid
            self.db.execute("INSERT INTO cohort_dispatch(dispatch_id,cohort_id,binding) VALUES (?,?,?)",
                            (identifier, cohort, binding))
        return identifier, min(120.0, remaining, deadline - self.now()[1])

    def before_forward(self, authorize) -> float:
        with self.transaction():
            remaining = self.check_time()
            authorize()
            _, _, _, deadline = self._open()
            return min(120.0, remaining, deadline - self.now()[1])

    def original_transport_deadline(self, identifier):
        """The original cohort deadline; observing it never extends an interval."""
        with self.mutex:
            row = self.db.execute(
                "SELECT c.deadline FROM cohort c JOIN cohort_dispatch d ON d.cohort_id=c.id WHERE d.dispatch_id=?",
                (identifier,)).fetchone()
            require(row is not None, "original_transport_cohort_missing")
            return row[0]

    def before_dispatch(self, identifier: int, request) -> None:
        # Exact reservation identity, never matching duplicated inputs by hash.
        with self.transaction():
            self.check_time()
            cohort, _, _, _ = self._open()
            row = self.db.execute("SELECT cohort_id,binding,send_intent FROM cohort_dispatch WHERE dispatch_id=?", (identifier,)).fetchone()
            require(row == (cohort, encode(self.binding(request.scope)).decode(), 0), "cohort_send_identity")
            self.db.execute("UPDATE cohort_dispatch SET send_intent=1 WHERE dispatch_id=?", (identifier,))

    def _capture_inventory(self, cohort: int) -> tuple[list, int]:
        rows = self.db.execute("""SELECT d.id,d.request_hash,d.forwarded_hash,d.response_hash,d.response_bytes,
            d.status,d.outcome,c.binding,c.send_intent FROM dispatch d JOIN cohort_dispatch c ON c.dispatch_id=d.id
            WHERE c.cohort_id=? ORDER BY d.id""", (cohort,)).fetchall()
        inventory = []
        for identifier, original_hash, forwarded_hash, response_hash, response_bytes, status, outcome, binding, sent in rows:
            require(outcome is not None, "cohort_dispatch_unsettled")
            folder = self.directory / f"dispatch-{identifier:06d}"
            require(folder.is_dir() and not folder.is_symlink(), "missing_cohort_capture")
            hashes = {}
            bodies = {}
            for name, cap in (("request.original.json", MAX_BODY_BYTES), ("request.forwarded.json", MAX_BODY_BYTES),
                              ("intent.json", 16384), ("response.body", RESPONSE_CAP), ("capture.json", 16384)):
                bodies[name] = read_capture(folder / name, cap)
                hashes[name] = digest(bodies[name])
            intent = strict_json(bodies["intent.json"])
            capture = strict_json(bodies["capture.json"])
            require(intent.get("dispatch_id") == identifier and intent.get("binding") == strict_json(binding.encode())
                    and intent.get("original_sha256") == original_hash == hashes["request.original.json"]
                    and intent.get("forwarded_sha256") == forwarded_hash == hashes["request.forwarded.json"]
                    and capture.get("response_sha256") == response_hash == hashes["response.body"]
                    and capture.get("response_bytes") == response_bytes == len(bodies["response.body"])
                    and capture.get("status") == status, "cohort_capture_mismatch")
            require(sent == 1 and capture.get("complete") is True and outcome in ("complete", "upstream_error"),
                    "cohort_capture_incomplete")
            inventory.append({"dispatch_id": identifier, "files": hashes})
        return inventory, len(rows)

    def close_cohort(self, backend: str, phase: str, caller: str, observation_hash: str) -> dict:
        require(type(observation_hash) is str and len(observation_hash) == 64, "cohort_observation_identity")
        with self.transaction():
            row = self.db.execute("SELECT id,state,baseline_hash,receipt,observation FROM cohort WHERE backend=? AND phase=? AND caller=?",
                                  (backend, phase, caller)).fetchone()
            require(row is not None, "original_cohort_missing")
            cohort, state, baseline_hash, receipt, previous_observation = row
            if state == "closed":
                require(previous_observation == observation_hash, "cohort_close_replay_mismatch")
                return strict_json(receipt.encode())
            if previous_observation is not None:
                require(previous_observation == observation_hash, "cohort_close_replay_mismatch")
            # Durable admission closure precedes capture reads and every error.
            self.db.execute("UPDATE cohort SET state='closing',observation=? WHERE id=?", (observation_hash, cohort))
        with self.mutex:
            active = self.db.execute("SELECT COUNT(*) FROM dispatch d JOIN cohort_dispatch c ON c.dispatch_id=d.id WHERE c.cohort_id=? AND d.outcome IS NULL", (cohort,)).fetchone()[0]
            if active:
                return {"verdict": "unmet", "cohort_id": cohort, "unsettled_dispatches": active,
                        "per_request_association": "unmet", "admission_closed": True}
            try:
                inventory, count = self._capture_inventory(cohort)
                require(count > 0, "empty_cohort")
                require(self.db.execute("SELECT halted FROM run").fetchone() == (None,), "run_halted")
                after = super().receipt()
                require(len(encode(after)) <= 4 * 1024**2, "cohort_receipt_size_limit")
                from capture import write_new
                sealed = {"before_sha256": baseline_hash, "after": after, "capture_inventory": inventory,
                          "observation_sha256": observation_hash, "identity": self.identity}
                encoded = encode(sealed)
                path = self.directory / f"cohort-{cohort:02d}.json"
                if path.exists():
                    require(read_capture(path, 8 * 1024**2) == encoded, "cohort_seal_changed")
                else:
                    write_new(path, encoded)
                    from ledger import sync_directory
                    sync_directory(self.directory)
                reserved = self.db.execute("SELECT SUM(d.reserved) FROM dispatch d JOIN cohort_dispatch c ON c.dispatch_id=d.id WHERE c.cohort_id=?", (cohort,)).fetchone()[0]
                result = {"format": "recall-exclusive-cohort-accounting/v1", "verdict": "passed", "cohort_id": cohort,
                          "before_sha256": baseline_hash, "after_sha256": digest(encode(after)),
                          "capture_inventory_sha256": digest(encode(inventory)), "sealed_receipt_sha256": digest(encoded),
                          "physical_dispatch_reservations": count, "reserved_nano_usd": reserved,
                          "unsettled_dispatches": 0, "per_request_association": "unmet", "admission_closed": True}
                with self.transaction():
                    self.db.execute("UPDATE cohort SET state='closed',receipt=? WHERE id=?", (encode(result).decode(), cohort))
                return result
            except BaseException:
                with self.transaction():
                    self.db.execute("UPDATE run SET halted='cohort_capture_or_seal_failure'")
                raise

    def receipt(self) -> dict:
        with self.mutex:
            result = super().receipt()
            result["format"] = "recall-exclusive-embedding-receipt/v1"
            result["identity"] = self.identity
            result["cohorts"] = [dict(zip(("id", "backend", "phase", "state", "baseline_sha256"), row))
                                  for row in self.db.execute("SELECT id,backend,phase,state,baseline_hash FROM cohort ORDER BY id")]
            result["per_request_association"] = "unmet"
            return result

    def close_with_evidence(self, backend, phase, caller, observation_hash):
        """Explicit v2 envelope; the original v1 close record is unchanged."""
        close = self.close_cohort(backend, phase, caller, observation_hash)
        index = None
        if close.get("verdict") == "passed":
            from accounting_export import closed_export
            index = closed_export(self, backend, phase, caller, close)
        return {"format": "recall-exclusive-cohort-close-envelope/v2", "close": close, "export": index}
