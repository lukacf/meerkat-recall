"""Offline settlement oracles: real SQLite/private captures, fake transport only."""
from __future__ import annotations

from email.message import Message
from dataclasses import replace
import io
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from capture import Capture
from ledger import Ledger, MONEY_CAP
from settlement import SettlementLedger, FULL_CONTEXT, RATE_VERSION, validate_terminal
from terminal_usage import reasoning_projection, SDK_SOURCE_SHA256, SDK_PROJECTION_VERSION
from transport import Forwarder
from wire import Denied, Scope, digest, encode, validate

SCOPE = Scope("surreal-db", "semantics-household", "extraction", "gpt-6.1-sol",
              binding_id="household_extraction", connection_id="generate", profile_id="generation", credential_generation=3)
CONTEXT = {"account_id": "synthetic-account", "credential_generation": 3,
           "configuration_sha256": "a" * 64, "bindings_sha256": "b" * 64,
           "reasoning_sdk_sha256": SDK_SOURCE_SHA256, "reasoning_projection_version": SDK_PROJECTION_VERSION}
REASONING = {"type": "reasoning", "id": "rs_fixture", "encrypted_content": "synthetic-opaque-payload",
             "summary": [{"type": "summary_text", "text": "First."},
                         {"type": "summary_text", "text": "Second."}]}


def request_body(**updates):
    body = {"model": "gpt-6.1-sol", "input": "Synthetic recall fixture", "max_output_tokens": 64000,
            "reasoning": {"effort": "medium"}, "stream": False, "store": False}
    body.update(updates)
    return encode(body)


def terminal(**updates):
    body = {"object": "response", "id": "resp_fixture", "model": "gpt-6.1-sol",
            "status": "completed", "service_tier": "default", "max_output_tokens": 64000,
            "error": None, "incomplete_details": None, "output": [],
            "usage": {"input_tokens": 1000, "output_tokens": 200, "total_tokens": 1200,
                      "input_tokens_details": {"cached_tokens": 400, "cache_write_tokens": 100},
                      "output_tokens_details": {"reasoning_tokens": 150}}}
    body.update(updates)
    return body


def sse(body):
    return b"event: response.completed\ndata: " + encode({"type": "response.completed", "sequence_number": 2,
                                                           "response": body}) + b"\n\n"


class Response:
    def __init__(self, body, streaming=False, status=200):
        self.body, self.status, self.position = body, status, 0
        self.headers = Message()
        self.headers["Content-Type"] = "text/event-stream" if streaming else "application/json"
        self.headers["Content-Length"] = str(len(body))
        self.length = len(body)
        self.chunked = False

    def getheader(self, key, default=None):
        return self.headers.get(key, default)

    def read1(self, size):
        part = self.body[self.position:self.position + min(size, 37)]
        self.position += len(part)
        self.length -= len(part)
        return part


class Connection:
    def __init__(self, response, on_send):
        self.response, self.on_send, self.sock = response, on_send, self
    def connect(self): pass
    def settimeout(self, timeout): pass
    def shutdown(self, how): pass
    def close(self): pass
    def getresponse(self): return self.response
    def request(self, method, path, body, headers): self.on_send(body)


class TerminalTests(unittest.TestCase):
    def check(self, body, streaming=False):
        return validate_terminal(body, streaming=streaming, model="gpt-6.1-sol", output_cap=64000)

    def test_json_and_sse_exact_conservative_arithmetic(self):
        for body, streaming in ((encode(terminal()), False), (sse(terminal()), True)):
            usage = self.check(body, streaming)
            self.assertEqual(usage.charge_nano_usd, 8_000_000)
            self.assertEqual((usage.cached_tokens, usage.cache_write_tokens, usage.reasoning_tokens), (400, 100, 150))

    def test_usage_missing_wrong_type_totals_subsets_and_unknown_categories(self):
        changes = [
            lambda u: u.pop("output_tokens"),
            lambda u: u.update(input_tokens=True),
            lambda u: u.update(input_tokens=1.0),
            lambda u: u.update(input_tokens=-1),
            lambda u: u.update(total_tokens=1199),
            lambda u: u.update(input_tokens=1_050_001, total_tokens=1_050_201),
            lambda u: u.update(output_tokens=64001, total_tokens=65001),
            lambda u: u["input_tokens_details"].update(cached_tokens=950),
            lambda u: u["input_tokens_details"].pop("cache_write_tokens"),
            lambda u: u["output_tokens_details"].update(reasoning_tokens=201),
            lambda u: u.update(prompt_tokens=1000),
            lambda u: u["output_tokens_details"].update(other_billable=0),
        ]
        for change in changes:
            body = terminal()
            change(body["usage"])
            with self.subTest(body=body), self.assertRaises(Denied):
                self.check(encode(body))

    def test_duplicate_keys_nonfinite_and_wrong_response_identity(self):
        for raw in (encode(terminal()).replace(b'"input_tokens":1000', b'"input_tokens":1000,"input_tokens":2'),
                    encode(terminal()).replace(b'"input_tokens":1000', b'"input_tokens":NaN')):
            with self.assertRaises(Denied): self.check(raw)
        for field, value in (("model", "gpt-6.1-sol-unreviewed"), ("service_tier", "priority"),
                             ("max_output_tokens", True), ("max_output_tokens", 16384),
                             ("id", ""), ("error", {}), ("status", "incomplete"),
                             ("background", True), ("tools", [{"type": "web_search"}])):
            with self.subTest(field=field), self.assertRaises(Denied):
                self.check(encode(terminal(**{field: value})))

    def test_sse_entire_stream_and_unique_terminal(self):
        valid = sse(terminal())
        delta = encode({"type": "response.output_text.delta", "sequence_number": 3, "delta": "later"})
        for bad in (valid + valid, valid + b"data: " + delta + b"\n\n", valid + b"data: {",
                    valid[:-1], valid + b"data: [DONE]\n\ndata: [DONE]\n\n",
                    valid.replace(b'response.completed', b'response.failed')):
            with self.subTest(body=bad), self.assertRaises(Denied): self.check(bad, True)
        self.check(valid + b"data: [DONE]\n\n", True)
        first = b"data: " + encode({"type": "response.created", "sequence_number": 0,
            "response": {"id": "resp_foreign", "status": "in_progress"}}) + b"\n\n"
        with self.assertRaises(Denied): self.check(first + valid, True)

    def test_streamed_done_item_preserves_identity_and_terminal_ciphertext(self):
        item = dict(REASONING, status="completed")
        done = b"data: " + encode({"type": "response.output_item.done", "sequence_number": 1, "item": item}) + b"\n\n"
        self.check(done + sse(terminal(output=[item])), True)
        changed_cipher = dict(item, encrypted_content="changed")
        usage = self.check(done + sse(terminal(output=[changed_cipher])), True)
        self.assertEqual(usage.replay_items, (reasoning_projection(changed_cipher),))
        for changed in (dict(item, summary=[]), dict(item, id="foreign")):
            with self.subTest(changed=changed), self.assertRaises(Denied):
                self.check(done + sse(terminal(output=[changed])), True)
        with self.assertRaises(Denied): self.check(done + sse(terminal(output=[])), True)

    def test_sdk_reasoning_projection_is_exact_and_phase_unsupported(self):
        projected = json.loads(reasoning_projection(REASONING))
        self.assertEqual(projected["summary"], [{"type": "summary_text", "text": "First.\nSecond."}])
        self.assertEqual(projected["encrypted_content"], REASONING["encrypted_content"])
        usage = self.check(encode(terminal(output=[REASONING])))
        self.assertEqual(usage.replay_items, (encode(projected),))
        with self.assertRaises(Denied): reasoning_projection(dict(REASONING, phase="analysis"))


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name) / "run"
        self.clock = lambda: (1000.0, 500.0, "fixture-boot")
        self.ledger = SettlementLedger(self.directory, "c" * 64, create=True,
                                       context_identity=CONTEXT, now=self.clock)
        self.network = patch("socket.create_connection", side_effect=AssertionError("network forbidden"))
        self.network.start()

    def tearDown(self):
        self.network.stop()
        self.ledger.close()
        self.temp.cleanup()

    def request(self, raw=None, scope=SCOPE):
        raw = request_body() if raw is None else raw
        return validate(raw, "/v1/responses", scope, output_cap_mode="preserve",
                        context_policy="provider-full-context-v1", authorize_reasoning=self.ledger.authorize_reasoning)

    def forward(self, body=None, *, raw=None, response=None, status=200, capture=None):
        raw = request_body() if raw is None else raw
        request = self.request(raw)
        body = encode(terminal()) if body is None else body
        response = response or Response(body, request.streaming, status)
        delivered, sends = [], []
        def sent(value):
            # A durable full reservation must already exist at the physical send.
            receipt = self.ledger.receipt()
            self.assertGreaterEqual(receipt["unresolved_nano_usd"], 6_210_000_000)
            sends.append(value)
        connection = Connection(response, sent)
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "synthetic-unused",
            capture or (lambda identifier, req, original: Capture(self.directory, identifier, req, original,
                                                                  binding=self.ledger.binding(req.scope))))
        forwarder.dispatch(request, raw, lambda status, content: None, delivered.append)
        return request, b"".join(delivered), sends

    def test_full_reserve_preserves_sdk_output_and_settles_once(self):
        req, body, sends = self.forward()
        self.assertEqual(req.input_bound, 1_050_000)
        self.assertEqual(req.output_cap, 64000)
        self.assertEqual(req.reservation, 6_210_000_000)
        self.assertEqual(sends, [req.body])
        self.assertEqual(body, encode(terminal()))
        first = self.ledger.receipt()
        self.assertEqual(first["spent_upper_nano_usd"], 8_000_000)
        self.assertEqual(first["unresolved_nano_usd"], 0)
        self.assertEqual(first["original_reserved_nano_usd"], 6_210_000_000)
        self.ledger.settle(1)
        self.assertEqual(self.ledger.receipt(), first)
        self.assertEqual(len(first["dispatches"]), 1)

    def test_sse_settlement_preserves_exact_bytes(self):
        raw, output = request_body(stream=True), sse(terminal())
        _, delivered, _ = self.forward(output, raw=raw)
        self.assertEqual(delivered, output)
        self.assertEqual(self.ledger.receipt()["spent_upper_nano_usd"], 8_000_000)

    def test_incomplete_and_bad_usage_keep_full_liability(self):
        value = terminal()
        value["usage"].pop("total_tokens")
        with self.assertRaisesRegex(Denied, "settlement_delivery_unsettled"):
            self.forward(encode(value))
        receipt = self.ledger.receipt()
        self.assertEqual(receipt["unresolved_nano_usd"], 6_210_000_000)
        self.assertEqual(receipt["spent_upper_nano_usd"], 0)
        self.assertIsNotNone(receipt["halted"])
        with self.assertRaises(Denied): self.ledger.reserve(self.request(), lambda: None)

    def test_errors_are_nonrefundable_without_advisory_usage(self):
        self.forward(encode({"error": "synthetic"}), status=429)
        self.assertEqual(self.ledger.receipt()["unresolved_nano_usd"], 6_210_000_000)
        self.assertEqual(self.ledger.receipt()["spent_upper_nano_usd"], 0)

    def test_foreign_binding_or_missing_capture_cannot_settle(self):
        req = self.request()
        identifier, _ = self.ledger.reserve(req, lambda: None)
        Ledger.finish(self.ledger, identifier, outcome="complete", status=200, response_hash="d"*64,
                      response_bytes=1, usage=None, halt=False)
        with self.assertRaises((Denied, OSError)): self.ledger.settle(identifier)
        self.assertEqual(self.ledger.receipt()["unresolved_nano_usd"], req.reservation)

    def test_reasoning_same_scope_and_original_capture_required(self):
        self.forward(encode(terminal(output=[REASONING])))
        projected = json.loads(reasoning_projection(REASONING))
        replay = request_body(input=[projected, {"role": "user", "content": "Continue."}])
        req = self.request(replay)
        self.assertEqual(json.loads(req.body)["input"][0], projected)
        for change in (dict(projected, encrypted_content="changed"), dict(projected, id="other"),
                       dict(projected, summary=[])):
            with self.assertRaises(Denied): self.request(request_body(input=[change]))
        foreign = replace(SCOPE, space="semantics-business")
        with self.assertRaises(Denied): self.request(replay, foreign)
        path = self.directory / "dispatch-000001" / "response.body"
        path.write_bytes(b"{}"); path.chmod(0o600)
        with self.assertRaises(Denied): self.request(replay)

    def test_foreign_profile_cannot_reuse_completed_reasoning(self):
        self.forward(encode(terminal(output=[REASONING])))
        projected = json.loads(reasoning_projection(REASONING))
        raw = request_body(input=[projected])
        for field, value in (("profile_id", "other"), ("connection_id", "other"),
                             ("binding_id", "other"), ("credential_generation", 4)):
            with self.subTest(field=field), self.assertRaises(Denied):
                self.request(raw, replace(SCOPE, **{field: value}))

    def test_completed_refusal_keeps_full_liability(self):
        output = [{"type": "message", "status": "completed", "content": [{"type": "refusal", "refusal": "Synthetic refusal."}]}]
        with self.assertRaisesRegex(Denied, "settlement_delivery_unsettled"):
            self.forward(encode(terminal(output=output)))
        self.assertEqual(self.ledger.receipt()["unresolved_nano_usd"], 6_210_000_000)
        self.assertEqual(self.ledger.receipt()["spent_upper_nano_usd"], 0)

    def test_duplicate_response_id_cannot_release_second_reservation(self):
        self.forward()
        with self.assertRaisesRegex(Denied, "settlement_delivery_unsettled"):
            self.forward()
        receipt = self.ledger.receipt()
        self.assertEqual(receipt["spent_upper_nano_usd"], 8_000_000)
        self.assertEqual(receipt["unresolved_nano_usd"], 6_210_000_000)
        self.assertIsNotNone(receipt["halted"])

    def test_last_budget_admission_is_atomic_and_attempts_never_refund(self):
        request = self.request()
        # Eleven complete HTTP errors retain 68.31 USD, leaving one reservation.
        for _ in range(11):
            identifier, _ = self.ledger.reserve(request, lambda: None)
            self.ledger.finish(identifier, outcome="upstream_error", status=429,
                               response_hash="d"*64, response_bytes=0, usage=None, halt=False)
        barrier, results = threading.Barrier(2), []
        def reserve():
            barrier.wait()
            try:
                results.append(self.ledger.reserve(request, lambda: None)[0])
            except Denied:
                results.append(None)
        threads = [threading.Thread(target=reserve) for _ in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(sum(value is not None for value in results), 1)
        receipt = self.ledger.receipt()
        self.assertEqual(len(receipt["dispatches"]), 12)
        self.assertLessEqual(receipt["current_liability_nano_usd"], MONEY_CAP)

    def test_settled_reopen_revalidates_capture_without_releasing_twice(self):
        self.forward()
        before = self.ledger.receipt()
        self.ledger.close()
        self.ledger = SettlementLedger(self.directory, "c"*64, create=False,
                                       context_identity=CONTEXT, now=self.clock)
        self.assertEqual(before, self.ledger.receipt())

    def test_short_content_length_and_missing_chunk_terminator_never_settle(self):
        # Separate temporary ledgers: either fault permanently halts that run.
        for index, framed in enumerate((b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 99999\r\n\r\n{}",
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n")):
            if index:
                self.ledger.close()
                self.directory = Path(self.temp.name) / "run-two"
                self.ledger = SettlementLedger(self.directory, "e"*64, create=True,
                                               context_identity=CONTEXT, now=self.clock)
            class Socket:
                def makefile(self, *args, **kwargs): return io.BytesIO(framed)
            response = http.client.HTTPResponse(Socket())
            response.begin()
            with self.assertRaises((Denied, http.client.IncompleteRead)):
                self.forward(response=response)
            self.assertEqual(self.ledger.receipt()["spent_upper_nano_usd"], 0)
            self.assertEqual(self.ledger.receipt()["unresolved_nano_usd"], 6_210_000_000)

    def test_ambiguous_http_header_and_capture_failure_never_release(self):
        response = Response(encode(terminal()))
        response.headers["Content-Length"] = "1"
        with self.assertRaises(Denied): self.forward(response=response)
        self.assertEqual(self.ledger.receipt()["unresolved_nano_usd"], 6_210_000_000)

    def test_v1_cannot_open_v2_and_v2_cannot_open_v1(self):
        self.forward()
        before = self.ledger.receipt()
        self.ledger.close()
        with self.assertRaisesRegex(Denied, "accounting_mode_mismatch"):
            Ledger(self.directory, "c"*64, create=False, now=self.clock)
        self.ledger = SettlementLedger(self.directory, "c"*64, create=False,
                                       context_identity=CONTEXT, now=self.clock)
        self.assertEqual(self.ledger.receipt(), before)
        legacy_path = Path(self.temp.name) / "legacy"
        old = Ledger(legacy_path, "f"*64, create=True, now=self.clock)
        old.close()
        with self.assertRaisesRegex(Denied, "accounting_mode_mismatch"):
            SettlementLedger(legacy_path, "f"*64, create=False, context_identity=CONTEXT, now=self.clock)
        old = Ledger(legacy_path, "f"*64, create=False, now=self.clock)
        self.assertEqual(old.receipt()["reserved_nano_usd"], 0)
        old.close()

    def test_capture_completing_after_original_deadline_retains_full_reserve(self):
        tick = [1000.0]
        class LateCapture(Capture):
            def finish(self, **kwargs):
                super().finish(**kwargs)
                tick[0] = 1121.0  # models a fsync that returned after the deadline
        factory = lambda identifier, req, original: LateCapture(self.directory, identifier, req, original,
                                                                binding=self.ledger.binding(req.scope))
        with patch("time.monotonic", side_effect=lambda: tick[0]):
            with self.assertRaisesRegex(Denied, "settlement_delivery_unsettled"):
                self.forward(capture=factory)
        receipt = self.ledger.receipt()
        self.assertEqual(receipt["spent_upper_nano_usd"], 0)
        self.assertEqual(receipt["unresolved_nano_usd"], 6_210_000_000)
        self.assertIsNotNone(receipt["halted"])
        row = self.ledger.db.execute("SELECT transport_deadline,read_completed,capture_completed FROM settlement").fetchone()
        self.assertEqual(row, (1120.0, 1000.0, 1121.0))
        capture = json.loads((self.directory / "dispatch-000001" / "capture.json").read_bytes())
        self.assertEqual(capture["transport"], {"deadline_mono": 1120.0, "read_completed_mono": 1000.0})

    def test_sdk_projection_identity_cannot_change_for_same_run(self):
        for field, value in (("reasoning_sdk_sha256", "0"*64), ("reasoning_projection_version", "unreviewed")):
            context = dict(CONTEXT, **{field: value})
            with self.assertRaisesRegex(Denied, "settlement_sdk_projection_mismatch"):
                SettlementLedger(Path(self.temp.name)/"other", "d"*64, create=True,
                                 context_identity=context, now=self.clock)

    def test_existing_v1_wire_and_output_cap_remain_strict(self):
        with self.assertRaises(Denied): validate(request_body(), "/v1/responses", SCOPE)
        with self.assertRaises(Denied): self.request(request_body(previous_response_id="resp_fixture"))
        with self.assertRaises(Denied): self.request(request_body(truncation="auto"))
        self.assertEqual(RATE_VERSION, "sol-standard-long-max-2026-10-06")


if __name__ == "__main__":
    unittest.main()
