"""Offline gateway controls: in-memory handlers and transport spies, no providers."""
from email.message import Message
import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import server
from wire import BOUND_VERSION, Denied, Scope, encode


class ForwarderSpy:
    def __init__(self):
        self.calls = []
        self.ledger = SimpleNamespace()

    def dispatch(self, request, raw, start, write):
        self.calls.append((request, raw))
        start(200, "application/json")
        write(b"{}")


def invoke(capture, body, *, auth_error=None, path="/v1/responses"):
    raw = encode(body)
    def authenticate(_capability):
        if auth_error is not None:
            raise Denied(auth_error)
        return Scope("surreal-db", "semantics-household", "extraction", "gpt-6.1-sol")
    forwarder = ForwarderSpy()
    policy = SimpleNamespace(authenticate=authenticate, output_cap_mode="reject", context_policy=BOUND_VERSION)
    handler = server.Handler.__new__(server.Handler)
    handler.server = SimpleNamespace(policy=policy, forwarder=forwarder, capture_denial=capture.record)
    handler.request_version = "HTTP/1.1"
    handler.path = path
    handler.headers = Message()
    for key, value in {"Host": "127.0.0.1:43129", "Authorization": "Bearer SYNTHETIC_SECRET",
                       "Content-Type": "application/json", "Content-Length": str(len(raw))}.items():
        handler.headers[key] = value
    handler.rfile = io.BytesIO(raw)
    handler.wfile = io.BytesIO()
    statuses = []
    handler.send_response = statuses.append
    handler.send_header = lambda *_args: None
    handler.end_headers = lambda: None
    handler.do_POST()
    return statuses, handler.wfile.getvalue(), forwarder.calls


def valid_body():
    return {"model": "gpt-6.1-sol", "input": "original bounded synthetic text",
            "max_output_tokens": 64, "reasoning": {"effort": "medium"}}


class GatewayDenialControls(unittest.TestCase):
    def test_actual_wire_rejection_has_no_forwarding_and_valid_request_is_unchanged(self):
        with TemporaryDirectory() as directory:
            capture = server.DenialCapture(directory)
            invalid = dict(valid_body(), unsupported="SYNTHETIC_SECRET https://private.invalid")
            statuses, body, calls = invoke(capture, invalid)
            self.assertEqual(statuses, [403])
            self.assertEqual(json.loads(body), {"error": {"code": "unsupported_fields"}})
            self.assertEqual(calls, [])
            statuses, body, calls = invoke(capture, valid_body())
            self.assertEqual(statuses, [200])
            self.assertEqual(body, b"{}")
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][1], encode(valid_body()))
            summary = capture.finish()
            records = [json.loads(line) for line in capture.path.read_bytes().splitlines()]
            self.assertEqual(len(records), 1)
            self.assertEqual({k: records[0][k] for k in ["status", "stage", "route", "code"]},
                             {"status": 403, "stage": "wire_validation", "route": "responses", "code": "unsupported_fields"})
            self.assertTrue(summary["complete"])
            self.assertFalse(summary["dispatch_or_usage_evidence"])
            self.assertNotIn(b"SYNTHETIC_SECRET", capture.path.read_bytes())
            self.assertEqual(summary["file"]["sha256"], hashlib.sha256(capture.path.read_bytes()).hexdigest())

    def test_unknown_auth_reason_and_private_url_never_enter_diagnostics(self):
        with TemporaryDirectory() as directory:
            capture = server.DenialCapture(directory)
            secret = "SYNTHETIC_SECRET https://private.invalid/source?token=hidden"
            statuses, _body, calls = invoke(capture, valid_body(), auth_error=secret,
                                           path="/private/source?token=hidden")
            self.assertEqual(statuses, [403])
            self.assertEqual(calls, [])
            summary = capture.finish()
            record = json.loads(capture.path.read_bytes())
            self.assertEqual(record["code"], "unclassified_denial")
            self.assertEqual(record["stage"], "authentication")
            self.assertEqual(record["route"], "other")
            self.assertEqual(set(record), {"sequence", "elapsed_ns", "status", "stage", "route", "code"})
            for p in [capture.path, capture.receipt_path]:
                raw = p.read_bytes()
                for forbidden in [b"SYNTHETIC_SECRET", b"private.invalid", b"token=hidden", b"Bearer"]:
                    self.assertNotIn(forbidden, raw)
            self.assertTrue(summary["complete"])

    def test_bound_is_explicit_incomplete_and_retains_exact_file_identity(self):
        with TemporaryDirectory() as directory:
            capture = server.DenialCapture(directory)
            actual_codes = ["dispatch_count_limit", "operation_count_limit", "run_time_limit", "clock_rollback"]
            for number in range(capture.MAX_EVENTS + 7):
                capture.record(403, actual_codes[number % len(actual_codes)], "forwarding", "/v1/responses")
            summary = capture.finish()
            raw = capture.path.read_bytes()
            self.assertEqual(len(raw.splitlines()), 128)
            self.assertEqual({json.loads(line)["code"] for line in raw.splitlines()}, set(actual_codes))
            self.assertLessEqual(len(raw), capture.MAX_EVENTS * capture.MAX_EVENT_BYTES)
            self.assertEqual(summary["recorded_events"], 128)
            self.assertEqual(summary["omitted_events"], 7)
            self.assertFalse(summary["complete"])
            self.assertFalse(summary["write_failed"])
            self.assertEqual(summary["file"]["bytes"], len(raw))
            self.assertEqual(summary["file"]["sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(json.loads(capture.receipt_path.read_bytes()), summary)
            self.assertEqual(capture.path.stat().st_mode & 0o777, 0o600)

    def test_recording_failure_preserves_original_denial_and_marks_incomplete(self):
        with TemporaryDirectory() as directory:
            capture = server.DenialCapture(directory)
            invalid = dict(valid_body(), unsupported="synthetic")
            with patch.object(server.os, "write", side_effect=OSError("SYNTHETIC_SECRET")):
                statuses, body, calls = invoke(capture, invalid)
            self.assertEqual(statuses, [403])
            self.assertEqual(json.loads(body), {"error": {"code": "unsupported_fields"}})
            self.assertEqual(calls, [])
            summary = capture.finish()
            self.assertTrue(summary["write_failed"])
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["omitted_events"], 1)
            self.assertEqual(summary["recorded_events"], 0)
            self.assertNotIn(b"SYNTHETIC_SECRET", capture.receipt_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
