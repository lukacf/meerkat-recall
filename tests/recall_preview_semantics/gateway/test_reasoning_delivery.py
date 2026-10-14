"""Synthetic transport spies: terminal replay custody, never model relevance."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from capture import Capture
from ledger import Ledger
from settlement import SettlementLedger
from terminal_usage import validate_terminal
from transport import Forwarder
from wire import Denied, digest, encode, validate
from test_settlement import CONTEXT, SCOPE, Connection, Response, request_body, terminal


class TerminalReasoningDeliveryTests(unittest.TestCase):
    def test_terminal_ciphertext_and_durable_delivery_barrier(self):
        # Independent expected projection: do not derive the oracle with the
        # projection implementation under test. All payload strings are spies.
        observed = {"id": "rs_synthetic", "type": "reasoning", "content": [],
                    "summary": [{"type": "summary_text", "text": "First."},
                                {"type": "summary_text", "text": "Second."}],
                    "encrypted_content": "provisional-cipher-spy-only"}
        final = dict(observed, encrypted_content="terminal-cipher-spy-only")
        projected = {"id": "rs_synthetic", "type": "reasoning",
                     "summary": [{"type": "summary_text", "text": "First.\nSecond."}],
                     "encrypted_content": "terminal-cipher-spy-only"}

        def stream(item, output):
            events = [
                {"type": "response.output_item.done", "sequence_number": 1, "item": item},
                {"type": "response.completed", "sequence_number": 2, "response": terminal(output=output)},
            ]
            return b"".join(b"data: " + encode(event) + b"\n\n" for event in events)

        body = stream(observed, [final])
        usage = validate_terminal(body, streaming=True, model="gpt-6.1-sol", output_cap=64000)
        self.assertEqual(usage.replay_items, (encode(projected),))
        self.assertEqual(usage.charge_nano_usd, 8_000_000)
        self.assertEqual((usage.input_tokens, usage.output_tokens, usage.reasoning_tokens), (1000, 200, 150))

        # Changing ciphertext alone is permitted only for complete reasoning;
        # identity, content, summary, event ordering and function calls stay strict.
        invalid = [
            stream(observed, []),
            stream(observed, [dict(final, id="rs_other")]),
            stream(observed, [dict(final, summary=[])]),
            stream(observed, [dict(final, content=[{"type": "reasoning_text", "text": "opaque"}])]),
            stream(observed, [dict(final, status="in_progress")]),
            stream(observed, [final, final]),
            body + b'data: {"type":"response.output_text.delta","sequence_number":3,"delta":"later"}\n\n',
        ]
        tool = {"type": "function_call", "id": "fc_synthetic", "call_id": "call_synthetic",
                "name": "synthetic", "arguments": "{}"}
        invalid.append(stream(tool, [dict(tool, arguments='{"changed":true}')]))
        for index, bad in enumerate(invalid):
            with self.subTest(rejected_shape=index), self.assertRaises(Denied):
                validate_terminal(bad, streaming=True, model="gpt-6.1-sol", output_cap=64000)

        # Real private Capture and SQLite ledger; the transport below is an
        # explicitly synthetic connection and socket creation is prohibited.
        for scenario in ("success", "invalid_terminal", "capture_failure", "downstream_disconnect"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "run"
                ledger = SettlementLedger(directory, "c" * 64, create=True, context_identity=CONTEXT,
                                          now=lambda: (1000.0, 500.0, "fixture-boot"))
                raw = request_body(stream=True)
                def wire(raw_body, scope=SCOPE):
                    return validate(raw_body, "/v1/responses", scope, output_cap_mode="preserve",
                                    context_policy="provider-full-context-v1",
                                    authorize_reasoning=ledger.authorize_reasoning)
                request = wire(raw)
                self.assertEqual(json.loads(request.body)["reasoning"]["effort"], "medium")
                self.assertEqual(request.output_cap, 64000)
                self.assertEqual(request.reservation, 6_210_000_000)
                delivered, headers, sends, stages = [], [], [], []
                payload = invalid[2] if scenario == "invalid_terminal" else body
                outer = self

                class ResponseSpy(Response):
                    def read1(self, size):
                        outer.assertEqual(headers, [])
                        outer.assertEqual(delivered, [])
                        return super().read1(size)

                class CaptureSpy(Capture):
                    def finish(self, **kwargs):
                        outer.assertEqual(headers, [])
                        outer.assertEqual(delivered, [])
                        if scenario == "capture_failure" and kwargs.get("complete"):
                            raise OSError("synthetic-capture-fsync-failure")
                        super().finish(**kwargs)
                        stages.append("capture-fsynced")

                def sent(value):
                    self.assertEqual(ledger.receipt()["unresolved_nano_usd"], request.reservation)
                    sends.append(value)

                def started(status, content_type):
                    self.assertIn("capture-fsynced", stages)
                    self.assertEqual(ledger.receipt()["settlements"][0]["state"], "settled")
                    self.assertEqual((directory / "dispatch-000001" / "response.body").read_bytes(), payload)
                    self.assertEqual(ledger.receipt()["dispatches"][0]["response_hash"], digest(payload))
                    # The first visible response byte can immediately be replayed
                    # without waiting or weakening any provenance requirement.
                    replay = wire(request_body(input=[projected]))
                    self.assertEqual(json.loads(replay.body)["input"], [projected])
                    headers.append((status, content_type))

                def wrote(value):
                    self.assertEqual(len(headers), 1)
                    if scenario == "downstream_disconnect":
                        raise BrokenPipeError("synthetic-downstream-disconnect")
                    delivered.append(value)

                connection = Connection(ResponseSpy(payload, streaming=True), sent)
                forwarder = Forwarder(ledger, lambda: None, lambda _: connection, "synthetic-unused",
                    lambda identifier, req, original: CaptureSpy(directory, identifier, req, original,
                                                                binding=ledger.binding(req.scope)))
                try:
                    with patch("socket.create_connection", side_effect=AssertionError("network forbidden")):
                        if scenario == "invalid_terminal":
                            with self.assertRaisesRegex(Denied, "settlement_delivery_unsettled"):
                                forwarder.dispatch(request, raw, started, wrote)
                        elif scenario == "capture_failure":
                            with self.assertRaisesRegex(OSError, "synthetic-capture-fsync-failure"):
                                forwarder.dispatch(request, raw, started, wrote)
                        elif scenario == "downstream_disconnect":
                            with self.assertRaisesRegex(BrokenPipeError, "synthetic-downstream-disconnect"):
                                forwarder.dispatch(request, raw, started, wrote)
                        else:
                            forwarder.dispatch(request, raw, started, wrote)
                    self.assertEqual(sends, [request.body])
                    receipt = ledger.receipt()
                    self.assertEqual(len(receipt["dispatches"]), 1)
                    if scenario in ("success", "downstream_disconnect"):
                        self.assertEqual(receipt["spent_upper_nano_usd"], 8_000_000)
                        self.assertEqual(receipt["unresolved_nano_usd"], 0)
                        self.assertEqual(receipt["dispatches"][0]["outcome"], "complete")
                        self.assertIsNone(receipt["halted"])
                        self.assertEqual(b"".join(delivered), body if scenario == "success" else b"")
                        for wrong in (dict(projected, encrypted_content=observed["encrypted_content"]),
                                      dict(projected, encrypted_content="unseen-spy-cipher"),
                                      dict(projected, id="rs_other"), dict(projected, summary=[])):
                            with self.assertRaises(Denied): wire(request_body(input=[wrong]))
                        for field, value in (("backend", "elephant-db"), ("space", "semantics-business"),
                                             ("profile_id", "other"), ("binding_id", "other"),
                                             ("connection_id", "other"), ("credential_generation", 4)):
                            with self.subTest(foreign=field), self.assertRaises(Denied):
                                wire(request_body(input=[projected]), replace(SCOPE, **{field: value}))
                        self.assertEqual(len(sends), 1)  # unauthorized histories never reach transport
                        # Exercise first-cause preservation when another admitted
                        # request aborts after the original strict-settlement halt.
                        pending, _ = ledger.reserve(wire(request_body()), lambda: None)
                        ledger._halt("settlement_refused")
                        Ledger.finish(ledger, pending, outcome="incomplete", status=None,
                                      response_hash=digest(b""), response_bytes=0, usage=None, halt=True)
                        self.assertEqual(ledger.receipt()["halted"], "settlement_refused")
                    else:
                        self.assertEqual(headers, [])
                        self.assertEqual(delivered, [])
                        self.assertEqual(receipt["spent_upper_nano_usd"], 0)
                        self.assertEqual(receipt["unresolved_nano_usd"], request.reservation)
                        self.assertEqual(receipt["halted"],
                                         "settlement_refused" if scenario == "invalid_terminal" else "incomplete")
                        with self.assertRaises(Denied): wire(request_body(input=[projected]))
                        self.assertEqual(len(sends), 1)
                finally:
                    ledger.close()


if __name__ == "__main__":
    unittest.main()
