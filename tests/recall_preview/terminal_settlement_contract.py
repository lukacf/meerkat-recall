#!/usr/bin/env python3
"""D independent v2 settlement oracles; no sockets, listeners, keys or providers.

Use only against a coherent G checkpoint. Real temporary SQLite transactions,
private capture files and the stdlib HTTP parser exercise the actual gateway.
Fixtures below freeze expectations independently of G's tests and constants.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import replace
import hashlib
import http.client
import importlib
import json
from pathlib import Path
import sqlite3
import ssl
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gateway_contract import ByteSocket, DormantTimer, FakeConnection

GATEWAY = None
MODULES = ("wire", "ledger", "capture", "transport", "policy", "settlement", "terminal_usage")
FULL_RESERVE = 6_210_000_000  # 1,050,000 input*5000 + 64,000 output*15000.
ACTUAL_UPPER = 9_800_000  # 1009 input*5000 + 317 total output*15000.


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def terminal(identifier="resp_independent_D", **updates):
    value = {"object": "response", "id": identifier, "status": "completed",
             "model": "gpt-6.1-sol", "max_output_tokens": 64000, "service_tier": "default",
             "error": None, "incomplete_details": None, "output": [],
             "usage": {"input_tokens": 1009, "output_tokens": 317, "total_tokens": 1326,
                       "input_tokens_details": {"cached_tokens": 777, "cache_write_tokens": 101},
                       "output_tokens_details": {"reasoning_tokens": 311}}}
    value.update(updates)
    return value


def reasoning(parts=None):
    return {"type": "reasoning", "id": "rs_original_D", "status": "completed",
            "encrypted_content": "public-synthetic-opaque-D-not-a-real-provider-item",
            "summary": [{"type": "summary_text", "text": text} for text in
                        (["", "First", "", "Second", ""] if parts is None else parts)]}


def event(value):
    return b"data: " + encoded(value) + b"\n\n"


def completed_sse(value):
    return event({"type": "response.completed", "sequence_number": 20, "response": value})


class CommitFault:
    """Fault only the real transaction that changed a settlement to settled."""
    def __init__(self, connection, after):
        self.connection, self.after, self.fired = connection, after, False

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def execute(self, sql, *args):
        if sql == "COMMIT" and not self.fired and self.connection.execute(
                "SELECT 1 FROM settlement WHERE state='settled'").fetchone():
            self.fired = True
            if self.after:
                self.connection.execute(sql, *args)
            raise sqlite3.OperationalError("D synthetic settlement commit acknowledgement fault")
        return self.connection.execute(sql, *args)


class TerminalSettlementContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="recall-D-terminal-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledgers = []
        self.addCleanup(self.close_ledgers)
        self.time = [1_800_000_000.0, 1000.0, "D-synthetic-boot"]
        self.scope = GATEWAY.wire.Scope("surreal-db", "semantics-household", "extraction", "gpt-6.1-sol",
            binding_id="D-original-extraction", connection_id="D-original-connection",
            profile_id="D-original-profile", credential_generation=7)
        self.context = {"account_id": "D-synthetic-account", "credential_generation": 7,
                        "configuration_sha256": "a" * 64, "bindings_sha256": "b" * 64,
                        "reasoning_sdk_sha256": "105229842f1a221277513653d5396c8e36a116384dde406182f2e173cdd0f8c3",
                        "reasoning_projection_version": "sdk-0.8.51-reasoning-projection-v1"}
        self.serial = 0

    def close_ledgers(self):
        for ledger in self.ledgers:
            ledger.close()

    def close_one(self, ledger):
        self.ledgers.remove(ledger)
        ledger.close()

    def ledger(self, directory=None, *, create=True):
        if directory is None:
            self.serial += 1
            parent = self.root / str(self.serial)
            parent.mkdir()
            directory = parent / "run"
        result = GATEWAY.settlement.SettlementLedger(directory, "d" * 64, create=create,
            context_identity=self.context, now=lambda: tuple(self.time))
        self.ledgers.append(result)
        return result

    def request(self, ledger, *, streaming=False, scope=None, inputs=None):
        raw = encoded({"model": "gpt-6.1-sol", "max_output_tokens": 64000,
                       "input": "Unchanged D synthetic request." if inputs is None else inputs,
                       "reasoning": {"effort": "medium"}, "stream": streaming, "store": False})
        request = GATEWAY.wire.validate(raw, "/v1/responses", scope or self.scope,
            output_cap_mode="preserve", context_policy="provider-full-context-v1",
            authorize_reasoning=ledger.authorize_reasoning)
        return raw, request

    def dispatch(self, ledger, value=None, *, raw_response=None, streaming=False,
                 headers=None, status=200, capture_factory=None, on_send=lambda: None,
                 authorize=lambda: None, on_connect=lambda: None, request_input=None):
        raw, request = self.request(ledger, streaming=streaming, inputs=request_input)
        body = encoded(terminal() if value is None else value)
        if streaming:
            body = completed_sse(terminal() if value is None else value)
        if raw_response is None:
            headers = headers or [("Content-Length", str(len(body))),
                ("Content-Type", "text/event-stream" if streaming else "application/json")]
            raw_response = (f"HTTP/1.1 {status} Fixture\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers)
                            + "\r\n").encode() + body
        response = http.client.HTTPResponse(ByteSocket(raw_response), method="POST")
        response.begin()
        def reserved_before_send():
            self.assertGreaterEqual(ledger.receipt()["unresolved_nano_usd"], FULL_RESERVE)
            on_send()
        connection = FakeConnection(response, on_connect, reserved_before_send)
        delivered, failure = bytearray(), None
        factory = capture_factory or (lambda identifier, req, original: GATEWAY.capture.Capture(
            ledger.directory, identifier, req, original, binding=ledger.binding(req.scope)))
        forwarder = GATEWAY.transport.Forwarder(ledger, authorize, lambda *_: connection,
            "PUBLIC-D-SYNTHETIC-NOT-CREDENTIAL", factory)
        with patch.object(GATEWAY.transport.threading, "Timer", DormantTimer):
            try:
                forwarder.dispatch(request, raw, lambda *_: None, delivered.extend)
            except Exception as error:
                failure = error
        return request, connection, bytes(delivered), failure

    def check(self, body, *, streaming=False):
        return GATEWAY.settlement.validate_terminal(body, streaming=streaming,
            model="gpt-6.1-sol", output_cap=64000)

    def test_full_reserve_and_strict_usage_charge_once_without_cache_or_reasoning_discount(self):
        ledger = self.ledger()
        request, connection, output, failure = self.dispatch(ledger)
        self.assertIsNone(failure)
        self.assertEqual((request.input_bound, request.output_cap, request.reservation),
                         (1_050_000, 64000, FULL_RESERVE))
        self.assertEqual(output, encoded(terminal()))
        self.assertEqual(len(connection.sent), 1)
        receipt = ledger.receipt()
        self.assertEqual(receipt["spent_upper_nano_usd"], ACTUAL_UPPER)
        self.assertEqual(receipt["unresolved_nano_usd"], 0)
        self.assertEqual(receipt["original_reserved_nano_usd"], FULL_RESERVE)
        ledger.settle(1)
        self.assertEqual(ledger.receipt(), receipt)
        directory = ledger.directory
        self.close_one(ledger)
        reopened = self.ledger(directory, create=False)
        self.assertEqual(reopened.receipt(), receipt)

    def test_maximum_documented_usage_spends_the_full_original_reservation(self):
        value = terminal()
        value["usage"].update(input_tokens=1_050_000, output_tokens=64000, total_tokens=1_114_000)
        result = self.check(encoded(value))
        self.assertEqual(result.charge_nano_usd, FULL_RESERVE)
        self.assertEqual(self.check(completed_sse(value), streaming=True).charge_nano_usd, FULL_RESERVE)

    def test_usage_unknown_missing_wrong_type_or_contradictory_cannot_release(self):
        changes = [("input_tokens", True), ("output_tokens", 317.0), ("input_tokens", -1),
                   ("total_tokens", 1325), ("input_tokens", 1_050_001), ("output_tokens", 64001),
                   ("input_tokens_details", {"cached_tokens": 777}),
                   ("input_tokens_details", {"cached_tokens": 1009, "cache_write_tokens": 1}),
                   ("output_tokens_details", {"reasoning_tokens": 318}),
                   ("output_tokens_details", {"reasoning_tokens": 311, "other_billed": 0})]
        for field in terminal()["usage"]:
            value = terminal()
            del value["usage"][field]
            with self.subTest(missing=field), self.assertRaises(GATEWAY.wire.Denied):
                self.check(encoded(value))
        for field, replacement in changes:
            value = terminal()
            value["usage"][field] = replacement
            with self.subTest(field=field, replacement=replacement), self.assertRaises(GATEWAY.wire.Denied):
                self.check(encoded(value))
        for raw in (encoded(terminal()).replace(b'"input_tokens":1009', b'"input_tokens":1009,"input_tokens":0'),
                    encoded(terminal()).replace(b'"input_tokens":1009', b'"input_tokens":1e999')):
            with self.assertRaises(GATEWAY.wire.Denied):
                self.check(raw)
        bad = terminal()
        bad["usage"]["input_tokens_details"].pop("cache_write_tokens")
        ledger = self.ledger()
        self.dispatch(ledger, bad)
        self.assertEqual(ledger.receipt()["unresolved_nano_usd"], FULL_RESERVE)
        self.assertEqual(ledger.receipt()["spent_upper_nano_usd"], 0)

    def test_completed_refusal_and_unreviewed_model_tier_keep_full_liability(self):
        for value in (terminal(output=[{"type": "message", "status": "completed", "content":
                    [{"type": "refusal", "refusal": "Synthetic refusal"}]}]),
                terminal(model="gpt-6.1-sol-unreviewed-snapshot"), terminal(service_tier="priority"),
                terminal(max_output_tokens=16384), terminal(error={"code": "fixture"})):
            with self.subTest(value=value):
                ledger = self.ledger()
                self.dispatch(ledger, value)
                self.assertEqual(ledger.receipt()["unresolved_nano_usd"], FULL_RESERVE)
                self.assertEqual(ledger.receipt()["spent_upper_nano_usd"], 0)

    def test_sse_unique_terminal_entire_suffix_and_streamed_reasoning_must_agree(self):
        good = completed_sse(terminal())
        wrong = reasoning(["streamed"])
        inconsistent = event({"type": "response.output_item.done", "sequence_number": 1,
                             "output_index": 0, "item": wrong}) + completed_sse(terminal(output=[reasoning(["terminal"])]))
        cases = [good + good, good + b"data: {", good[:-1],
                 good + event({"type": "response.output_text.delta", "sequence_number": 21, "delta": "late"}),
                 good + b"data: [DONE]\n\ndata: [DONE]\n\n", inconsistent]
        for body in cases:
            with self.subTest(body=body), self.assertRaises(GATEWAY.wire.Denied):
                self.check(body, streaming=True)
        self.assertEqual(self.check(good + b"data: [DONE]\n\n", streaming=True).charge_nano_usd, ACTUAL_UPPER)

    def test_actual_http_parser_rejects_short_ambiguous_or_unterminated_chunk_framing(self):
        body = encoded(terminal())
        frames = [
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body) + 1).encode() + b"\r\n\r\n" + body,
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n" + format(len(body), "x").encode() + b"\r\n" + body + b"\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body,
        ]
        for frame in frames:
            with self.subTest(frame=frame[:100]):
                ledger = self.ledger()
                self.dispatch(ledger, raw_response=frame)
                self.assertEqual(ledger.receipt()["spent_upper_nano_usd"], 0)
                self.assertEqual(ledger.receipt()["unresolved_nano_usd"], FULL_RESERVE)

    def test_capture_fsync_failure_cannot_release_money(self):
        ledger = self.ledger()
        def capture(identifier, request, original):
            result = GATEWAY.capture.Capture(ledger.directory, identifier, request, original,
                                            binding=ledger.binding(request.scope))
            result.finish = lambda **_: (_ for _ in ()).throw(OSError("D synthetic fsync failure"))
            return result
        _, _, _, failure = self.dispatch(ledger, capture_factory=capture)
        self.assertIsNotNone(failure)
        self.assertEqual(ledger.receipt()["unresolved_nano_usd"], FULL_RESERVE)
        self.assertEqual(ledger.receipt()["spent_upper_nano_usd"], 0)

    def test_capture_that_finishes_after_original_deadline_cannot_settle(self):
        ledger = self.ledger()
        ticks = [1000.0]
        def capture(identifier, request, original):
            result = GATEWAY.capture.Capture(ledger.directory, identifier, request, original,
                                            binding=ledger.binding(request.scope))
            original_finish = result.finish
            def late(**kwargs):
                original_finish(**kwargs)
                ticks[0] = 1121.0
            result.finish = late
            return result
        with patch.object(GATEWAY.transport.time, "monotonic", side_effect=lambda: ticks[0]):
            self.dispatch(ledger, capture_factory=capture)
        self.assertEqual(ledger.receipt()["unresolved_nano_usd"], FULL_RESERVE)
        self.assertEqual(ledger.receipt()["spent_upper_nano_usd"], 0)

    def test_commit_fault_before_and_after_publication_retains_exact_original_decision(self):
        for after_commit in (False, True):
            with self.subTest(after_commit=after_commit):
                ledger = self.ledger()
                directory = ledger.directory
                fault = CommitFault(ledger.db, after_commit)
                ledger.db = fault
                self.dispatch(ledger)
                self.assertTrue(fault.fired)
                self.close_one(ledger)
                with sqlite3.connect(directory / "ledger.sqlite3") as db:
                    rows = db.execute("SELECT reserved,state,spent FROM settlement").fetchall()
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM dispatch").fetchone()[0], 1)
                self.assertEqual(rows, [(FULL_RESERVE, "settled" if after_commit else "reserved",
                                         ACTUAL_UPPER if after_commit else None)])
                if after_commit:
                    recovered = self.ledger(directory, create=False)
                    first = recovered.receipt()
                    recovered.settle(1)
                    self.assertEqual(recovered.receipt(), first)
                    self.assertEqual(first["spent_upper_nano_usd"], ACTUAL_UPPER)
                else:
                    with self.assertRaises(GATEWAY.wire.Denied):
                        self.ledger(directory, create=False)

    def test_changed_captured_bytes_and_duplicate_response_id_cannot_release_twice(self):
        ledger = self.ledger()
        self.dispatch(ledger)
        self.dispatch(ledger)
        receipt = ledger.receipt()
        self.assertEqual(receipt["spent_upper_nano_usd"], ACTUAL_UPPER)
        self.assertEqual(receipt["unresolved_nano_usd"], FULL_RESERVE)
        self.assertEqual(len(receipt["dispatches"]), 2)
        first = self.ledger()
        self.dispatch(first)
        path = first.directory / "dispatch-000001" / "response.body"
        path.write_bytes(encoded(terminal(identifier="resp_changed_D")))
        with self.assertRaises(GATEWAY.wire.Denied):
            first.settle(1)
        self.assertEqual(first.receipt()["spent_upper_nano_usd"], ACTUAL_UPPER)
        with self.assertRaises(GATEWAY.wire.Denied):
            first.reserve(self.request(first)[1], lambda: None)

    def test_unknown_attempts_and_simultaneous_last_budget_do_not_reset_original_quota(self):
        ledger = self.ledger()
        _, request = self.request(ledger)
        for _ in range(11):
            identifier, _ = ledger.reserve(request, lambda: None)
            ledger.finish(identifier, outcome="upstream_error", status=503,
                response_hash=hashlib.sha256(b"{}").hexdigest(), response_bytes=2, usage=None, halt=False)
        barrier, outcomes = threading.Barrier(2), []
        def attempt():
            barrier.wait(timeout=3)
            try:
                outcomes.append((True, ledger.reserve(request, lambda: None)[0]))
            except GATEWAY.wire.Denied:
                outcomes.append((False, None))
        workers = [threading.Thread(target=attempt) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(sorted(success for success, _ in outcomes), [False, True])
        receipt = ledger.receipt()
        self.assertEqual(len(receipt["dispatches"]), 12)
        self.assertEqual(receipt["unresolved_nano_usd"], 12 * FULL_RESERVE)
        self.assertLessEqual(receipt["current_liability_nano_usd"], 75_000_000_000)

    def test_settled_attempts_still_exhaust_original_per_role_quota(self):
        ledger = self.ledger()
        for index in range(56):
            _, _, _, error = self.dispatch(ledger, terminal(identifier=f"resp_quota_D_{index}"))
            self.assertIsNone(error)
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.reserve(self.request(ledger)[1], lambda: None)
        receipt = ledger.receipt()
        self.assertEqual(len(receipt["dispatches"]), 56)
        self.assertEqual(receipt["spent_upper_nano_usd"], 56 * ACTUAL_UPPER)
        self.assertEqual(receipt["original_reserved_nano_usd"], 56 * FULL_RESERVE)

    def test_reasoning_uses_exact_sdk_empty_part_projection_and_original_scope(self):
        ledger = self.ledger()
        original = reasoning()
        self.dispatch(ledger, terminal(output=[original]))
        replay = {"type": "reasoning", "id": original["id"], "encrypted_content": original["encrypted_content"],
                  "summary": [{"type": "summary_text", "text": "First\n\nSecond\n"}]}
        ledger.authorize_reasoning(replay, self.scope)
        for item in (dict(replay, id="rs_foreign"), dict(replay, encrypted_content="changed"),
                     dict(replay, summary=[{"type": "summary_text", "text": "First\nSecond"}]),
                     dict(replay, phase="analysis")):
            with self.subTest(item=item), self.assertRaises(GATEWAY.wire.Denied):
                ledger.authorize_reasoning(item, self.scope)
        for scope in (replace(self.scope, space="semantics-business"), replace(self.scope, backend="elephant-db"),
                      replace(self.scope, operation="summary"), replace(self.scope, credential_generation=8),
                      replace(self.scope, profile_id="new-profile")):
            with self.subTest(scope=scope), self.assertRaises(GATEWAY.wire.Denied):
                ledger.authorize_reasoning(replay, scope)
        path = ledger.directory / "dispatch-000001" / "response.body"
        path.write_bytes(encoded(terminal(output=[reasoning(["changed original capture"])])))
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.authorize_reasoning(replay, self.scope)

    def test_current_revocation_before_physical_send_holds_reserved_attempt_without_send(self):
        ledger = self.ledger()
        allowed = [True]
        def authorize():
            if not allowed[0]:
                raise GATEWAY.wire.Denied("D original authority revoked")
        _, connection, _, failure = self.dispatch(ledger, authorize=authorize,
            on_connect=lambda: allowed.__setitem__(0, False))
        self.assertIsNotNone(failure)
        self.assertEqual(connection.sent, [])
        self.assertEqual(ledger.receipt()["unresolved_nano_usd"], FULL_RESERVE)
        self.assertEqual(len(ledger.receipt()["dispatches"]), 1)

    def test_v1_and_v2_ledgers_cannot_be_reopened_as_each_other(self):
        v2 = self.ledger()
        self.dispatch(v2)
        directory = v2.directory
        self.close_one(v2)
        with self.assertRaises(GATEWAY.wire.Denied):
            GATEWAY.ledger.Ledger(directory, "d" * 64, create=False, now=lambda: tuple(self.time))
        parent = self.root / "v1-separate"
        parent.mkdir()
        v1 = GATEWAY.ledger.Ledger(parent / "run", "e" * 64, create=True, now=lambda: tuple(self.time))
        v1.close()
        with self.assertRaises((GATEWAY.wire.Denied, sqlite3.DatabaseError)):
            GATEWAY.settlement.SettlementLedger(parent / "run", "e" * 64, create=False,
                context_identity=self.context, now=lambda: tuple(self.time))


def source_hashes(directory):
    return {name + ".py": hashlib.sha256((directory / (name + ".py")).read_bytes()).hexdigest()
            for name in MODULES}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-source", type=Path, required=True)
    args = parser.parse_args()
    directory = args.gateway_source.resolve(strict=True)
    before = source_hashes(directory)
    sys.path.insert(0, str(directory))
    global GATEWAY
    with ExitStack() as stack:
        for name in ("socket.socket", "socket.create_connection"):
            stack.enter_context(patch(name, side_effect=AssertionError("D offline settlement forbids networking")))
        GATEWAY = SimpleNamespace(**{name: importlib.import_module(name) for name in MODULES})
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(TerminalSettlementContract))
    stable = before == source_hashes(directory)
    print(json.dumps({"format": "recall-d-terminal-settlement-offline/v1", "source_sha256": before,
        "source_stable_during_test": stable, "tests_run": result.testsRun,
        "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
        "network_permitted": False, "provider_billing_verified": False,
        "sdk_live_replay_qualified": False, "installed_artifact_qualified": False,
        "live_spending_authorized": False}, sort_keys=True))
    return 0 if result.wasSuccessful() and not result.skipped and stable else 1


if __name__ == "__main__":
    sys.exit(main())
