"""Versioned full-context reservations with durable strict terminal settlement."""
from __future__ import annotations

import os
import json
from dataclasses import asdict
from pathlib import Path
import stat
import time
import math

from ledger import (Ledger, BACKENDS, OPERATIONS, COUNT_CAP, MONEY_CAP, CONCURRENCY_CAP,
                    RESPONSE_CAP, clock)
from terminal_usage import (POLICY, RATE_VERSION, FULL_CONTEXT, MAX_OUTPUT, INPUT_RATE,
                            OUTPUT_RATE, TerminalUsage, validate_terminal, strict_value, SDK_SOURCE_SHA256, SDK_PROJECTION_VERSION)
from wire import (Denied, Request, Scope, EMBEDDING_RESERVATION_NANO_USD, MAX_BODY_BYTES,
                  digest, encode, fields, integer, require)


def capture_binding(manifest_hash: str, context_identity: dict, scope: Scope) -> dict:
    return {"policy": POLICY, "rate_version": RATE_VERSION, "manifest_sha256": manifest_hash,
            "context_identity_sha256": digest(encode(context_identity)),
            "scope_sha256": scope.context_key}


def private_bytes(path: Path, limit: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and not info.st_mode & 0o077 and info.st_size <= limit, "settlement_capture_file")
        chunks, count = [], 0
        while True:
            part = os.read(fd, min(65536, limit + 1 - count))
            if not part:
                break
            chunks.append(part)
            count += len(part)
            require(count <= limit, "settlement_capture_limit")
        return b"".join(chunks)
    finally:
        os.close(fd)


class SettlementLedger(Ledger):
    """The v1 financial policy remains separate; mode guards prevent reopening."""
    ACCOUNTING_POLICY = POLICY

    def __init__(self, directory: Path, manifest_hash: str, *, create: bool,
                 context_identity: dict, now=clock):
        context_fields = {"account_id", "credential_generation", "configuration_sha256", "bindings_sha256",
                          "reasoning_sdk_sha256", "reasoning_projection_version"}
        fields(context_identity, context_fields, context_fields)
        require(context_identity["reasoning_sdk_sha256"] == SDK_SOURCE_SHA256
                and context_identity["reasoning_projection_version"] == SDK_PROJECTION_VERSION,
                "settlement_sdk_projection_mismatch")
        require(type(context_identity["account_id"]) is str and bool(context_identity["account_id"]),
                "settlement_account_required")
        integer(context_identity["credential_generation"], 2**63 - 1, "settlement_credential_generation")
        for key in ("configuration_sha256", "bindings_sha256"):
            value = context_identity[key]
            require(type(value) is str and len(value) == 64
                    and all(c in "0123456789abcdef" for c in value), "settlement_context_digest")
        self.context_identity = strict_value(encode(context_identity))
        self.manifest_hash = manifest_hash
        super().__init__(directory, manifest_hash, create=create, now=now)
        expected = encode({"policy": POLICY, "rate_version": RATE_VERSION,
                           "manifest_sha256": manifest_hash, "context": self.context_identity}).decode()
        if create:
            with self.transaction():
                self.db.execute("CREATE TABLE settlement_policy (identity TEXT NOT NULL)")
                self.db.execute("INSERT INTO settlement_policy VALUES (?)", (expected,))
                self.db.execute("""CREATE TABLE settlement (
                    dispatch_id INTEGER PRIMARY KEY REFERENCES dispatch(id), state TEXT NOT NULL,
                    reserved INTEGER NOT NULL, binding TEXT NOT NULL, scope_hash TEXT NOT NULL, scope_identity TEXT NOT NULL,
                    evidence_hash TEXT, response_id TEXT UNIQUE, spent INTEGER,
                    decision TEXT, replay_hashes TEXT, transport_started REAL,
                    transport_deadline REAL, read_completed REAL, capture_completed REAL)""")
        else:
            try:
                require(self.db.execute("SELECT identity FROM settlement_policy").fetchall() == [(expected,)],
                        "settlement_policy_mismatch")
                self._balance()
                for identifier, in self.db.execute("SELECT dispatch_id FROM settlement WHERE state='settled'").fetchall():
                    # Reopening cannot turn an unverified historical amount into
                    # available money. Exact durable evidence is checked again.
                    self.settle(identifier)
                if self.db.execute("SELECT 1 FROM settlement WHERE state='reserved' LIMIT 1").fetchone():
                    self._halt("unresolved_settlement_after_restart")
                    raise Denied("unresolved_settlement_after_restart")
            except Exception:
                self._halt("settlement_resume_integrity")
                raise

    def binding(self, scope: Scope) -> dict:
        return capture_binding(self.manifest_hash, self.context_identity, scope)

    def _halt(self, code: str) -> None:
        with self.transaction():
            self.db.execute("UPDATE run SET halted=COALESCE(halted,?)", (code,))

    def _balance(self) -> tuple[int, int]:
        rows = self.db.execute("""SELECT d.reserved,s.reserved,s.state,s.spent,s.decision
            FROM dispatch d LEFT JOIN settlement s ON d.id=s.dispatch_id""").fetchall()
        require(len(rows) == self.db.execute("SELECT COUNT(*) FROM settlement").fetchone()[0],
                "settlement_row_integrity")
        spent, liability = 0, 0
        for original, reserved, state, charge, decision in rows:
            require(type(original) is int and original == reserved and original > 0
                    and state in ("reserved", "settled", "nonrefundable"), "settlement_row_integrity")
            if state == "settled":
                require(type(charge) is int and 0 <= charge <= reserved and decision is not None,
                        "settlement_amount_integrity")
                spent += charge
            else:
                require(charge is None and decision is None, "settlement_row_integrity")
                liability += reserved
        require(spent + liability <= MONEY_CAP, "settlement_money_integrity")
        return spent, liability

    def reserve(self, request: Request, authorize) -> tuple[int, float]:
        with self.transaction():
            remaining = self.check_time()
            authorize()
            scope = request.scope
            require(scope.backend in BACKENDS and scope.operation in OPERATIONS, "unknown_scope")
            require(all(type(value) is str and value for value in (scope.binding_id, scope.connection_id, scope.profile_id))
                    and scope.credential_generation == self.context_identity["credential_generation"],
                    "settlement_exact_binding_required")
            require(digest(request.body) == request.forwarded_hash, "settlement_request_hash")
            body = strict_value(request.body)
            if scope.operation == "embedding":
                require(request.path == "/v1/embeddings" and request.output_cap == 0
                        and request.reservation == EMBEDDING_RESERVATION_NANO_USD, "settlement_embedding_reserve")
                state = "nonrefundable"
            else:
                cap = integer(request.output_cap, MAX_OUTPUT, "settlement_output_cap")
                require(request.path == "/v1/responses" and scope.model == "gpt-6.1-sol"
                        and request.input_bound == FULL_CONTEXT
                        and request.reservation == FULL_CONTEXT * INPUT_RATE + cap * OUTPUT_RATE,
                        "settlement_full_reserve_required")
                require(body.get("model") == scope.model and type(body.get("max_output_tokens")) is int
                        and body["max_output_tokens"] == cap, "settlement_request_identity")
                self._authorize_history(body, scope)
                state = "reserved"
            spent, liability = self._balance()
            count, active = self.db.execute(
                "SELECT COUNT(*),COALESCE(SUM(outcome IS NULL),0) FROM dispatch").fetchone()
            require(count < COUNT_CAP, "dispatch_count_limit")
            require(spent + liability + request.reservation <= MONEY_CAP, "money_limit")
            require(active < CONCURRENCY_CAP, "concurrency_limit")
            used = self.db.execute("SELECT COUNT(*) FROM dispatch WHERE backend=? AND operation=?",
                                   (scope.backend, scope.operation)).fetchone()[0]
            require(used < OPERATIONS[scope.operation], "operation_count_limit")
            cursor = self.db.execute("""INSERT INTO dispatch
                (backend,space,operation,model,input_bound,output_cap,reserved,request_hash,
                 forwarded_hash,transforms,started,requested_output_cap) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (scope.backend, scope.space, scope.operation, scope.model, request.input_bound,
                 request.output_cap, request.reservation, request.original_hash, request.forwarded_hash,
                 ",".join(request.transforms), self.now()[0], request.requested_output_cap))
            identifier = cursor.lastrowid
            self.db.execute("""INSERT INTO settlement
                (dispatch_id,state,reserved,binding,scope_hash,scope_identity) VALUES (?,?,?,?,?,?)""",
                (identifier, state, request.reservation, encode(self.binding(scope)).decode(), scope.context_key, encode(asdict(scope)).decode()))
        return identifier, min(120.0, remaining)

    def _authorize_history(self, body: dict, scope: Scope) -> None:
        inputs = body.get("input")
        if type(inputs) is list:
            for item in inputs:
                if type(item) is dict and item.get("type") == "reasoning":
                    self.authorize_reasoning(item, scope)

    def bind_deadline(self, identifier: int, deadline: float) -> None:
        now = time.monotonic()
        require(type(deadline) is float and math.isfinite(deadline) and now < deadline <= now + 120.0,
                "settlement_transport_deadline")
        with self.transaction():
            row = self.db.execute("SELECT transport_deadline FROM settlement WHERE dispatch_id=?", (identifier,)).fetchone()
            require(row == (None,), "settlement_deadline_already_bound")
            self.db.execute("UPDATE settlement SET transport_started=?,transport_deadline=? WHERE dispatch_id=?",
                            (now, deadline, identifier))

    def record_transport(self, identifier: int, read_completed: float, capture_completed: float) -> bool:
        # Called only after Capture.finish has returned from all file/directory
        # fsyncs. This durable row records that observation, not a pre-fsync guess.
        with self.transaction():
            row = self.db.execute("SELECT transport_started,transport_deadline,read_completed,capture_completed FROM settlement WHERE dispatch_id=?", (identifier,)).fetchone()
            require(row is not None and row[0] is not None and row[1] is not None
                    and row[2:] == (None, None), "settlement_transport_state")
            require(all(type(value) is float and math.isfinite(value) for value in (read_completed, capture_completed))
                    and row[0] <= read_completed <= capture_completed, "settlement_transport_clock")
            self.db.execute("UPDATE settlement SET read_completed=?,capture_completed=? WHERE dispatch_id=?",
                            (read_completed, capture_completed, identifier))
            timely = capture_completed <= row[1] and time.monotonic() <= row[1]
            if not timely:
                self.db.execute("UPDATE run SET halted=COALESCE(halted,'late_capture')")
        return timely

    def before_request(self, request: Request) -> None:
        # Called again after TLS connection and directly before physical send.
        with self.mutex:
            self._authorize_history(strict_value(request.body), request.scope)

    @staticmethod
    def validate_response_headers(response) -> None:
        # Stdlib accepts some ambiguous framing. v2 cannot use that as evidence
        # for releasing liability; the complete physical body must be bounded.
        headers = response.headers
        length = headers.get_all("Content-Length", [])
        transfer = headers.get_all("Transfer-Encoding", [])
        require(len(length) <= 1 and len(transfer) <= 1 and not (length and transfer),
                "settlement_http_ambiguous_framing")
        if length:
            require(length[0].isascii() and length[0].isdecimal()
                    and int(length[0]) <= RESPONSE_CAP, "settlement_http_length")
        if transfer:
            require(transfer[0].lower().strip() == "chunked", "settlement_http_transfer")
        for name in ("Content-Type", "Content-Encoding"):
            require(len(headers.get_all(name, [])) <= 1, "settlement_http_ambiguous_header")

    def finish(self, identifier: int, *, outcome: str, status: int | None,
               response_hash: str, response_bytes: int, usage, halt: bool) -> None:
        with self.mutex:
            # Advisory usage is intentionally never passed to the authoritative path.
            super().finish(identifier, outcome=outcome, status=status, response_hash=response_hash,
                           response_bytes=response_bytes, usage=None, halt=halt)
            row = self.db.execute("SELECT state FROM settlement WHERE dispatch_id=?", (identifier,)).fetchone()
            require(row is not None, "settlement_row_integrity")
            if row[0] == "reserved" and outcome == "complete":
                try:
                    self.settle(identifier)
                except (Denied, OSError, ValueError, TypeError, KeyError):
                    # Retain the full reservation and private response capture.
                    # The delivery barrier withholds an unsettled successful response.
                    self._halt("settlement_refused")

    def authorize_response_delivery(self, identifier: int) -> None:
        """Called after durable finish, before any successful Responses bytes.

        A failed settlement cannot create client history that is ineligible for
        replay. This hook never settles, retries, authorizes history or refunds.
        """
        with self.transaction():
            row = self.db.execute("SELECT state,transport_deadline FROM settlement WHERE dispatch_id=?",
                                  (identifier,)).fetchone()
            require(row is not None and row[0] == "settled", "settlement_delivery_unsettled")
            self.check_time()
            require(type(row[1]) is float, "settlement_transport_deadline")
            timely = time.monotonic() <= row[1]
            if not timely:
                self.db.execute("UPDATE run SET halted=COALESCE(halted,'late_delivery')")
        require(timely, "settlement_late_delivery")

    def _evidence(self, identifier: int):
        cursor = self.db.execute("SELECT * FROM dispatch WHERE id=?", (identifier,))
        names = [column[0] for column in cursor.description]
        values = cursor.fetchone()
        require(values is not None, "settlement_unknown_dispatch")
        row = dict(zip(names, values))
        require(row["operation"] != "embedding" and row["outcome"] == "complete"
                and type(row["status"]) is int and 200 <= row["status"] < 300, "settlement_transport_incomplete")
        state = self.db.execute("SELECT binding,scope_identity,transport_started,transport_deadline,read_completed,capture_completed FROM settlement WHERE dispatch_id=?", (identifier,)).fetchone()
        require(state is not None, "settlement_row_integrity")
        scope = Scope(**strict_value(state[1].encode()))
        require((scope.backend, scope.space, scope.operation, scope.model)
                == (row["backend"], row["space"], row["operation"], row["model"]), "settlement_scope_mismatch")
        require(state[0] == encode(self.binding(scope)).decode(), "settlement_binding_mismatch")
        directory = self.directory / f"dispatch-{identifier:06d}"
        require(directory.is_dir() and not directory.is_symlink(), "settlement_capture_directory")
        files = {name: private_bytes(directory / name, limit) for name, limit in (
            ("intent.json", MAX_BODY_BYTES), ("capture.json", MAX_BODY_BYTES),
            ("request.original.json", MAX_BODY_BYTES), ("request.forwarded.json", MAX_BODY_BYTES),
            ("response.body", RESPONSE_CAP))}
        intent = strict_value(files["intent.json"])
        capture = strict_value(files["capture.json"])
        require(intent.get("dispatch_id") == identifier and intent.get("binding") == self.binding(scope),
                "settlement_capture_binding")
        require(digest(files["request.original.json"]) == row["request_hash"] == intent.get("original_sha256")
                and digest(files["request.forwarded.json"]) == row["forwarded_hash"] == intent.get("forwarded_sha256"),
                "settlement_capture_request")
        require(digest(files["response.body"]) == row["response_hash"] == capture.get("response_sha256")
                and len(files["response.body"]) == row["response_bytes"] == capture.get("response_bytes"),
                "settlement_capture_response")
        require(capture.get("complete") is True and capture.get("status") == row["status"],
                "settlement_capture_incomplete")
        require(all(type(value) is float and math.isfinite(value) for value in state[2:])
                and state[2] <= state[4] <= state[5] <= state[3], "settlement_capture_deadline")
        require(capture.get("transport") == {"deadline_mono": state[3], "read_completed_mono": state[4]},
                "settlement_capture_timing_mismatch")
        forwarded = strict_value(files["request.forwarded.json"])
        require(forwarded.get("model") == row["model"]
                and type(forwarded.get("max_output_tokens")) is int
                and forwarded["max_output_tokens"] == row["output_cap"], "settlement_request_identity")
        streaming = forwarded.get("stream", False)
        require(type(streaming) is bool, "settlement_stream_type")
        content_type = capture.get("content_type")
        require(type(content_type) is str and content_type.split(";", 1)[0].strip().lower()
                == ("text/event-stream" if streaming else "application/json"), "settlement_content_type")
        usage = validate_terminal(files["response.body"], streaming=streaming,
                                  model=row["model"], output_cap=row["output_cap"])
        require(row["input_bound"] == FULL_CONTEXT
                and row["reserved"] == FULL_CONTEXT * INPUT_RATE + row["output_cap"] * OUTPUT_RATE
                and usage.charge_nano_usd <= row["reserved"], "settlement_reservation_mismatch")
        evidence_hash = digest(encode({name: digest(body) for name, body in files.items()}))
        decision = {"dispatch_id": identifier, "policy": POLICY, "rate_version": RATE_VERSION,
                    "binding": self.binding(scope), "evidence_sha256": evidence_hash,
                    "response_id": usage.response_id, "original_reserve_nano_usd": row["reserved"],
                    "spent_upper_nano_usd": usage.charge_nano_usd,
                    "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
                    "cached_tokens": usage.cached_tokens, "cache_write_tokens": usage.cache_write_tokens,
                    "reasoning_tokens": usage.reasoning_tokens,
                    "transport_deadline_mono": state[3], "read_completed_mono": state[4],
                    "capture_completed_mono": state[5],
                    "reasoning_projection_version": SDK_PROJECTION_VERSION,
                    "reasoning_sdk_sha256": SDK_SOURCE_SHA256,
                    "replay_hashes": [digest(item) for item in usage.replay_items]}
        return decision, usage

    def settle(self, identifier: int) -> dict:
        with self.mutex:
            try:
                decision, usage = self._evidence(identifier)
                encoded = encode(decision).decode()
                with self.transaction():
                    current = self.db.execute("SELECT state,decision FROM settlement WHERE dispatch_id=?", (identifier,)).fetchone()
                    require(current is not None, "settlement_row_integrity")
                    if current[0] == "settled":
                        require(current[1] == encoded, "settlement_conflicting_replay")
                    else:
                        require(current == ("reserved", None), "settlement_state")
                        require(time.monotonic() <= decision["transport_deadline_mono"], "settlement_late_decision")
                        require(self.db.execute("SELECT 1 FROM settlement WHERE response_id=?", (usage.response_id,)).fetchone() is None,
                                "settlement_response_reused")
                        self.db.execute("""UPDATE settlement SET state='settled',evidence_hash=?,response_id=?,
                            spent=?,decision=?,replay_hashes=? WHERE dispatch_id=? AND state='reserved'""",
                            (decision["evidence_sha256"], usage.response_id, usage.charge_nano_usd,
                             encoded, encode(decision["replay_hashes"]).decode(), identifier))
                    self._balance()
                return decision
            except (Denied, OSError, ValueError, TypeError, KeyError):
                self._halt("settlement_refused")
                raise

    def authorize_reasoning(self, item: dict, scope: Scope) -> None:
        # Input must already be the exact reviewed SDK projection, not an ID-only
        # item or an alternative caller-selected summary normalization.
        fields(item, {"type", "id", "summary", "encrypted_content"},
               {"type", "id", "summary", "encrypted_content"})
        item_hash = digest(encode(item))
        with self.mutex:
            rows = self.db.execute("SELECT dispatch_id,replay_hashes FROM settlement WHERE state='settled' AND scope_hash=?",
                                   (scope.context_key,)).fetchall()
            for identifier, hashes in rows:
                # JSON list is emitted locally, never used as authority alone.
                if item_hash not in json.loads(hashes):
                    continue
                decision, usage = self._evidence(identifier)
                stored = self.db.execute("SELECT decision FROM settlement WHERE dispatch_id=?", (identifier,)).fetchone()[0]
                require(stored == encode(decision).decode(), "reasoning_capture_changed")
                if encode(item) in usage.replay_items:
                    return
        raise Denied("reasoning_provenance_required")

    def receipt(self) -> dict:
        with self.mutex:
            result = super().receipt()
            spent, liability = self._balance()
            result.update({"format": "recall-gateway-receipt/v2", "accounting_policy": POLICY,
                           "rate_version": RATE_VERSION, "spent_upper_nano_usd": spent,
                           "unresolved_nano_usd": liability, "current_liability_nano_usd": spent + liability,
                           "original_reserved_nano_usd": result.pop("reserved_nano_usd"),
                           "settlements": [{"dispatch_id": identifier, "state": state,
                                            "decision": strict_value(decision.encode()) if decision else None}
                                           for identifier, state, decision in self.db.execute(
                                               "SELECT dispatch_id,state,decision FROM settlement ORDER BY dispatch_id")]})
            return result
