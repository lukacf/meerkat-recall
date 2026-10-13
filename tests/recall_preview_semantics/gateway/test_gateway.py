"""Offline qualification gateway tests: no sockets, credentials or providers."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from capture import Capture
from bridge import worker_environment
from ledger import Ledger, MONEY_CAP, OPERATIONS, RESPONSE_CAP
from policy import Policy
from transport import Forwarder, stream_complete, usage_from_response
from wire import Denied, Scope, digest, encode, validate


GEN = Scope("surreal-db", "semantics-household", "extraction", "gpt-6.1-sol")
EMBED = Scope("surreal-db", "semantics-household", "embedding", "text-embedding-3-small", 1536)


def generation(**changes):
    value = {"model": "gpt-6.1-sol", "input": "Synthetic Alice joined Example Co.",
             "max_output_tokens": 16384, "reasoning": {"effort": "medium"}, "stream": False}
    value.update(changes)
    return encode(value)


def request(**changes):
    return validate(generation(**changes), "/v1/responses", GEN)


class WireTests(unittest.TestCase):
    def test_actual_body_including_tool_schema_counts_and_tier_is_recorded(self):
        simple = request()
        tool = {"type": "function", "name": "extract", "parameters": {"type": "object", "description": "x" * 4096}}
        extended = request(tools=[tool])
        self.assertGreaterEqual(extended.input_bound - simple.input_bound, 4096)
        self.assertEqual(json.loads(simple.body)["service_tier"], "default")
        self.assertEqual(json.loads(simple.body)["store"], False)
        self.assertIn("explicit_service_tier", simple.transforms)
        self.assertNotEqual(simple.original_hash, simple.forwarded_hash)
        self.assertEqual(simple.reservation, 245760000)

    def test_unicode_bytes_not_characters_and_full_serialized_limit(self):
        self.assertGreater(request(input="🌍" * 100).input_bound, request(input="a" * 100).input_bound + 299)
        with self.assertRaises(Denied):
            request(input="🌍" * 8192)
        with self.assertRaises(Denied):
            request(tools=[{"type": "function", "name": "x", "parameters": {"description": "x" * 32768}}])

    def test_state_and_nontext_references_refused(self):
        cases = [
            {"previous_response_id": "resp_observed_or_forged"},
            {"conversation": "conversation_id"},
            {"input": [{"type": "reasoning", "id": "r", "summary": [], "encrypted_content": "small_cipher"}]},
            {"input": [{"role": "assistant", "id": "observed", "content": "complete text"}]},
            {"input": [{"role": "user", "content": [{"type": "input_image", "image_url": "https://example.invalid/x"}]}]},
            {"input": [{"type": "item_reference", "id": "file_opaque"}]},
            {"tools": [{"type": "web_search"}]}, {"tools": [{"type": "code_interpreter"}]},
            {"tools": [{"type": "function", "name": "x", "parameters": {"$ref": "https://example.invalid/schema"}}]},
        ]
        for values in cases:
            with self.subTest(values=values), self.assertRaises(Denied):
                request(**values)

    def test_plaintext_function_history_is_wholly_present(self):
        raw = generation(input=[{"role": "system", "content": "Synthetic only"},
                                {"type": "function_call", "call_id": "a", "name": "extract", "arguments": "{}"},
                                {"type": "function_call_output", "call_id": "a", "output": "Literal source text"}])
        result = validate(raw, "/v1/responses", GEN)
        self.assertIn(b"Literal source text", result.body)

    def test_exact_model_reasoning_output_and_service(self):
        for values in ({"model": "gpt-6-astra"}, {"reasoning": {"effort": "high"}},
                       {"max_output_tokens": 16385}, {"max_output_tokens": True},
                       {"service_tier": "auto"}, {"service_tier": "priority"}, {"store": True}):
            with self.subTest(values=values), self.assertRaises(Denied):
                request(**values)

    def test_output_transform_is_explicit_and_original_cap_is_retained(self):
        raw = generation(max_output_tokens=64000)
        with self.assertRaises(Denied):
            validate(raw, "/v1/responses", GEN)
        result = validate(raw, "/v1/responses", GEN, output_cap_mode="cap_at_16384")
        self.assertEqual((result.requested_output_cap, result.output_cap), (64000, 16384))
        self.assertEqual(json.loads(raw)["max_output_tokens"], 64000)
        self.assertEqual(json.loads(result.body)["max_output_tokens"], 16384)
        self.assertIn("approved_output_cap:64000->16384", result.transforms)
        self.assertEqual(result.reservation, 245760000)
        for invalid in (True, 0, -1, "64000", 128001):
            with self.assertRaises(Denied):
                validate(generation(max_output_tokens=invalid), "/v1/responses", GEN, output_cap_mode="cap_at_16384")
        with self.assertRaises(Denied):
            validate(raw, "/v1/responses", GEN, output_cap_mode="silent_auto")
        small = validate(generation(max_output_tokens=123), "/v1/responses", GEN, output_cap_mode="cap_at_16384")
        self.assertEqual(small.output_cap, 123)
        self.assertFalse(any(value.startswith("approved_output_cap:") for value in small.transforms))

    def test_strict_json_and_paths(self):
        for raw in (b'{"model":"x","model":"gpt-6.1-sol"}', b'{"input":NaN}', b'{"input":"\\ud800"}'):
            with self.assertRaises(Denied):
                validate(raw, "/v1/responses", GEN)
        for path in ("/v1/responses?x=1", "https://api.openai.com/v1/responses", "/v1/chat/completions"):
            with self.assertRaises(Denied):
                validate(generation(), path, GEN)

    def test_embedding_cap_is_total_batch_not_per_item(self):
        raw = encode({"model": EMBED.model, "input": ["a" * 4100, "b" * 4100]})
        with self.assertRaises(Denied):
            validate(raw, "/v1/embeddings", EMBED)
        result = validate(encode({"model": EMBED.model, "input": ["one", "two"], "dimensions": 1536}), "/v1/embeddings", EMBED)
        self.assertEqual(result.reservation, 1064960)
        with self.assertRaises(Denied):
            validate(encode({"model": EMBED.model, "input": [1, 2, 3]}), "/v1/embeddings", EMBED)


class LedgerFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name) / "run"
        self.tick = [100000.0, 1000.0, "fake-boot"]
        self.ledger = Ledger(self.directory, "a" * 64, create=True, now=lambda: tuple(self.tick))

    def tearDown(self):
        if self.ledger is not None:
            self.ledger.close()
        self.temp.cleanup()

    def finish(self, identifier, **changes):
        values = dict(outcome="complete", status=200, response_hash="b" * 64, response_bytes=2, usage=(1, 1), halt=False)
        values.update(changes)
        self.ledger.finish(identifier, **values)


class LedgerTests(LedgerFixture):
    def test_full_proposal_arithmetic_independent_integer_calculation(self):
        generation_count = sum(v for k, v in OPERATIONS.items() if k != "embedding") * 2
        embedding_count = (28 + 48) * 2
        self.assertEqual(generation_count + embedding_count, 432)
        self.assertEqual(generation_count * 245760000 + embedding_count * 1064960, 68974673920)
        self.assertLess(68974673920, MONEY_CAP)

    def test_nonrefundable_errors_and_unknown_usage(self):
        identifier, _ = self.ledger.reserve(request(), lambda: None)
        self.finish(identifier, outcome="upstream_error", status=429, usage=None)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 245760000)
        second, _ = self.ledger.reserve(request(), lambda: None)
        self.finish(second, outcome="incomplete", usage=None, halt=True)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 491520000)
        with self.assertRaises(Denied):
            self.ledger.reserve(request(), lambda: None)

    def test_last_money_allowance_two_callers_only_one_reserves(self):
        with self.ledger.transaction():
            self.ledger.db.execute("INSERT INTO dispatch (reserved,outcome) VALUES (?, 'complete')", (MONEY_CAP - request().reservation,))
        barrier = threading.Barrier(3)
        accepted, denied = [], []
        def attempt():
            barrier.wait()
            try:
                accepted.append(self.ledger.reserve(request(), lambda: None)[0])
            except Denied as error:
                denied.append(str(error))
        threads = [threading.Thread(target=attempt) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        self.assertEqual(len(accepted), 1)
        self.assertEqual(denied, ["money_limit"])
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], MONEY_CAP)

    def test_operation_caps_across_spaces_and_concurrency(self):
        first, _ = self.ledger.reserve(request(), lambda: None)
        second, _ = self.ledger.reserve(replace(request(), scope=replace(GEN, space="semantics-business")), lambda: None)
        with self.assertRaisesRegex(Denied, "concurrency_limit"):
            self.ledger.reserve(request(), lambda: None)
        self.finish(first)
        self.finish(second)
        for _ in range(54):
            identifier, _ = self.ledger.reserve(request(), lambda: None)
            self.finish(identifier)
        with self.assertRaisesRegex(Denied, "operation_count_limit"):
            self.ledger.reserve(replace(request(), scope=replace(GEN, space="semantics-business")), lambda: None)
        self.ledger.reserve(replace(request(), scope=replace(GEN, backend="elephant-db")), lambda: None)

    def test_crash_after_reservation_fails_resume(self):
        self.ledger.reserve(request(), lambda: None)
        self.ledger.close()
        self.ledger = None
        with self.assertRaisesRegex(Denied, "run_halted"):
            Ledger(self.directory, "a" * 64, create=False, now=lambda: tuple(self.tick))
        # A create attempt cannot silently reset the existing run anchor.
        with self.assertRaises(FileExistsError):
            Ledger(self.directory, "a" * 64, create=True)

    def test_resume_deadline_clock_rollback_and_missing_ledger(self):
        self.tick[0] -= 1
        with self.assertRaisesRegex(Denied, "clock_rollback"):
            self.ledger.reserve(request(), lambda: None)
        self.tick[0] += 1
        self.tick[1] += 5401
        with self.assertRaisesRegex(Denied, "run_time_limit"):
            self.ledger.reserve(request(), lambda: None)
        self.ledger.close()
        self.ledger = None
        (self.directory / "ledger.sqlite3").unlink()
        with self.assertRaisesRegex(Denied, "missing_run_ledger"):
            Ledger(self.directory, "a" * 64, create=False)

    def test_current_authorization_checked_twice_and_revocation_denied(self):
        calls = []
        self.ledger.reserve(request(), lambda: calls.append("reserve"))
        def revoked():
            raise Denied("approval_revoked")
        with self.assertRaisesRegex(Denied, "approval_revoked"):
            self.ledger.before_forward(revoked)
        self.assertEqual(calls, ["reserve"])

    def test_usage_exceeding_bound_halts_and_retains_full_charge(self):
        identifier, _ = self.ledger.reserve(request(), lambda: None)
        self.finish(identifier, usage=(1000000, 1))
        receipt = self.ledger.receipt()
        self.assertEqual(receipt["halted"], "usage_bound_violation")
        self.assertEqual(receipt["reserved_nano_usd"], 245760000)


class FakeResponse:
    def __init__(self, body, status=200, fail_after=None):
        from email.message import Message
        self.body, self.status, self.position, self.fail_after = body, status, 0, fail_after
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"
        self.headers["Content-Length"] = str(len(body))
        self.length, self.chunked = len(body), False

    def getheader(self, name, default=None):
        return self.headers.get(name, default)

    def isclosed(self):
        return self.position == len(self.body)

    def read1(self, size):
        if self.fail_after is not None and self.position >= self.fail_after:
            raise OSError("synthetic transport interruption")
        value = self.body[self.position:self.position + min(size, 97)]
        self.position += len(value)
        self.length -= len(value)
        return value


class FakeConnection:
    def __init__(self, response):
        self.response, self.sock, self.sent, self.connected = response, self, [], False

    def connect(self): self.connected = True
    def settimeout(self, value): pass
    def shutdown(self, how): pass
    def request(self, method, path, body, headers): self.sent.append((method, path, body))
    def getresponse(self): return self.response
    def close(self): pass


class ForwardingTests(LedgerFixture):
    def forward(self, body, *, status=200, stream=False, fail_after=None, capture=None):
        raw = generation(stream=stream)
        req = validate(raw, "/v1/responses", GEN)
        connection = FakeConnection(FakeResponse(body, status, fail_after))
        def factory(timeout):
            self.assertEqual(len(self.ledger.receipt()["dispatches"]), 1)
            self.assertTrue((self.directory / "dispatch-000001/request.forwarded.json").is_file())
            return connection
        sink, statuses = bytearray(), []
        capture_factory = capture or (lambda identifier, request, original: Capture(self.directory, identifier, request, original))
        forwarder = Forwarder(self.ledger, lambda: None, factory, "synthetic-not-a-secret", capture_factory)
        forwarder.dispatch(req, raw, lambda status, kind: statuses.append(status), sink.extend)
        return bytes(sink), statuses, connection

    def test_exact_json_error_and_stream_bytes_retained_without_headers(self):
        body = b'{"error": {"message": "synthetic rate limit", "code": "rate_limit"}}'
        received, statuses, connection = self.forward(body, status=429)
        self.assertEqual((received, statuses), (body, [429]))
        self.assertEqual(len(connection.sent), 1)
        directory = self.directory / "dispatch-000001"
        self.assertEqual((directory / "response.body").read_bytes(), body)
        self.assertTrue(json.loads((directory / "capture.json").read_bytes())["complete"])
        for path in directory.iterdir():
            self.assertNotIn(b"synthetic-not-a-secret", path.read_bytes())

    def test_sse_preserved_and_full_usage_keeps_reservation(self):
        body = b'event: response.output_text.delta\r\ndata: {"type":"response.output_text.delta","delta":"hello"}\r\n\r\ndata: {"type":"response.completed","response":{"usage":{"input_tokens":12,"output_tokens":3}}}\n\n'
        received, _, _ = self.forward(body, stream=True)
        self.assertEqual(received, body)
        row = self.ledger.receipt()["dispatches"][0]
        self.assertEqual((row["usage_input"], row["usage_output"], row["outcome"]), (12, 3, "complete"))
        self.assertEqual(row["reserved"], 245760000)

    def test_stream_without_terminal_complete_is_incomplete(self):
        self.forward(b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n', stream=True)
        self.assertEqual(self.ledger.receipt()["halted"], "incomplete")

    def test_provider_reports_output_limit_incomplete_without_rewriting_body(self):
        body = b'{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}'
        received, statuses, _ = self.forward(body)
        self.assertEqual((received, statuses), (body, [200]))
        self.assertEqual(self.ledger.receipt()["halted"], "incomplete")

    def test_partial_network_failure_keeps_exact_prefix_and_halts(self):
        body = b"synthetic response " * 50
        with self.assertRaises(OSError):
            self.forward(body, fail_after=97)
        captured = self.directory / "dispatch-000001"
        self.assertEqual((captured / "response.body").read_bytes(), body[:97])
        self.assertFalse(json.loads((captured / "capture.json").read_bytes())["complete"])
        self.assertEqual(self.ledger.receipt()["halted"], "incomplete")

    def test_capture_failure_never_sends_and_does_not_refund(self):
        def fail_capture(*_):
            raise OSError("synthetic disk failure")
        with self.assertRaises(OSError):
            self.forward(b"{}", capture=fail_capture)
        receipt = self.ledger.receipt()
        self.assertEqual(receipt["reserved_nano_usd"], 245760000)
        self.assertEqual(receipt["halted"], "incomplete")

    def test_oversized_transport_marks_incomplete_without_raising_cap(self):
        with patch("transport.RESPONSE_CAP", 100):
            with self.assertRaisesRegex(Denied, "response_byte_limit"):
                self.forward(b"x" * 101)
        row = self.ledger.receipt()["dispatches"][0]
        self.assertEqual(row["outcome"], "incomplete")
        self.assertLessEqual(row["response_bytes"], 100)

    def test_redirect_is_returned_once_never_followed(self):
        received, statuses, connection = self.forward(b"upstream moved", status=307)
        self.assertEqual(statuses, [307])
        self.assertEqual(received, b"upstream moved")
        self.assertEqual(len(connection.sent), 1)

    def test_real_http_parser_refuses_truncated_content_length(self):
        terminal = b'data: {"type":"response.completed","response":{}}\n\n'
        class MemorySocket:
            def makefile(self, *args, **kwargs):
                return io.BytesIO(b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n" + terminal)
        response = http.client.HTTPResponse(MemorySocket())
        response.begin()
        connection = FakeConnection(response)
        raw = generation(stream=True)
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "synthetic-only",
                              lambda identifier, request, original: Capture(self.directory, identifier, request, original))
        with self.assertRaisesRegex(Denied, "incomplete_http_framing"):
            forwarder.dispatch(validate(raw, "/v1/responses", GEN), raw, lambda *_: None, lambda _: None)
        self.assertEqual(self.ledger.receipt()["halted"], "incomplete")

    def test_watchdog_eof_cannot_mark_close_delimited_response_complete(self):
        raw = generation()
        response = FakeResponse(b"")
        tick = [10.0]
        def expired_eof(_size):
            tick[0] = 131.0
            return b""
        response.read1 = expired_eof
        connection = FakeConnection(response)
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "synthetic-only",
                              lambda identifier, request, original: Capture(self.directory, identifier, request, original))
        with patch("transport.time.monotonic", side_effect=lambda: tick[0]):
            with self.assertRaisesRegex(Denied, "dispatch_deadline"):
                forwarder.dispatch(validate(raw, "/v1/responses", GEN), raw, lambda *_: None, lambda _: None)
        self.assertEqual(self.ledger.receipt()["halted"], "incomplete")


class PolicyTests(unittest.TestCase):
    def test_output_transform_requires_separate_current_review(self):
        # Synthetic file fixtures only; fake digests cannot pass launcher artifact
        # checks, and this test never constructs a transport or reads a real key.
        manifest = json.loads((Path(__file__).parent / "manifest.example.json").read_bytes())
        manifest.update(live_ready=True, run_id="offline-fixture", account_id="offline-synthetic",
                        upstream_ips=["8.8.8.8"], output_cap_mode="cap_at_16384",
                        bindings=[{"id": "role", "backend": GEN.backend, "space": GEN.space,
                                   "operation": GEN.operation, "model": GEN.model,
                                   "connection_id": "offline-connection", "profile_id": "offline-profile"}])
        for key in ("proposal_sha256", "artifact_sha256", "configuration_sha256", "runtime_sha256", "gateway_sha256"):
            manifest[key] = "1" * 64
        raw = encode(manifest)
        approval = {"format": "recall-gateway-approval/v1", "manifest_sha256": digest(raw),
                    "authorized": True, "expires_unix": int(time.time()) + 60,
                    "user_approval_id": "offline-fixture-not-spending-authorization",
                    "bound_review_sha256": "1" * 64, "isolation_review_sha256": "1" * 64,
                    "binding_review_sha256": "1" * 64, "capture_scope_review_sha256": "1" * 64}
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, approval_path = Path(directory) / "manifest", Path(directory) / "approval"
            manifest_path.write_bytes(raw)
            manifest_path.chmod(0o600)
            approval_path.write_bytes(encode(approval))
            approval_path.chmod(0o600)
            with self.assertRaises(Denied):
                Policy(manifest_path, approval_path)
            approval["output_transform_review_sha256"] = "2" * 64
            approval_path.write_bytes(encode(approval))
            policy = Policy(manifest_path, approval_path)
            self.assertEqual(policy.output_cap_mode, "cap_at_16384")
            self.assertEqual(policy.authenticate("Bearer " + policy.tokens["role"]), GEN)
            approval["authorized"] = False
            approval_path.write_bytes(encode(approval))
            with self.assertRaisesRegex(Denied, "approval_changed_or_revoked"):
                policy.check()

    def test_authenticated_child_gets_only_fresh_synthetic_jwt_key(self):
        base = {"RECALL_GATEWAY_role": "proxy-only", "JWT_AUDIENCE": "qualification"}
        first = worker_environment(base)
        second = worker_environment(base)
        self.assertGreaterEqual(len(first["JWT_SECRET"]), 32)
        self.assertNotEqual(first["JWT_SECRET"], second["JWT_SECRET"])
        self.assertNotIn("JWT_SECRET", base)
        self.assertEqual(first["JWT_ALGORITHM"], "HS256")
        self.assertEqual(first["RECALL_GATEWAY_role"], "proxy-only")
        for injected in ({"SKIP_AUTH": "true"}, {"JWT_SECRET": "synthetic-host-key"}):
            with self.assertRaises(RuntimeError):
                worker_environment(injected)

    def test_skills_directory_must_be_explicit_readonly_runtime_content(self):
        from launcher import reviewed_runtime_directory
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "opt/recall/share/skills").mkdir(parents=True)
            reviewed_runtime_directory(root, "/opt/recall/share/skills")
            for value in ("/home/skills", "relative/skills", "/opt/recall/../secrets", "/opt/recall//share/skills", "/opt/recall/missing"):
                with self.subTest(path=value), self.assertRaises(Denied):
                    reviewed_runtime_directory(root, value)
            (root / "opt/recall/alias").symlink_to(root / "opt/recall/share/skills")
            with self.assertRaises(Denied):
                reviewed_runtime_directory(root, "/opt/recall/alias")
            (root / "opt/recall/parent-alias").symlink_to(root / "opt/recall/share")
            with self.assertRaises(Denied):
                reviewed_runtime_directory(root, "/opt/recall/parent-alias/skills")

    def test_checked_in_proposal_cannot_start_and_no_secret_is_read(self):
        source = Path(__file__).parent / "manifest.example.json"
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_bytes(source.read_bytes())
            manifest.chmod(0o600)
            with self.assertRaisesRegex(Denied, "live_not_ready"):
                Policy(manifest, Path(directory) / "no_approval_or_secret_exists")


if __name__ == "__main__":
    unittest.main()
