#!/usr/bin/env python3
"""Independent, offline qualification gateway contracts.

Run explicitly against a coherent gateway source checkpoint. No listener, real
socket, credential, provider request or installed-artifact acceptance is used.
The source hashes in the result identify what was actually tested. These cases
cannot establish the provider's undocumented token/protocol framing bound.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import http.client
import importlib
import io
import json
from pathlib import Path
import socket
import ssl  # Import before replacing socket construction with a refusal.
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


GATEWAY = None
MODULES = ("wire", "ledger", "capture", "transport", "policy")


class ByteSocket:
    """Only the file interface used by the actual stdlib HTTP parser."""

    def __init__(self, raw):
        self.raw = raw
        self.closed = False

    def makefile(self, *_args, **_kwargs):
        return io.BytesIO(self.raw)

    def settimeout(self, _seconds):
        pass

    def shutdown(self, _how):
        self.closed = True


class DormantTimer:
    """No wall-clock waits; the tested transport still computes its deadline."""

    def __init__(self, *_args, **_kwargs):
        self.daemon = False

    def start(self):
        pass

    def cancel(self):
        pass


class FakeConnection:
    def __init__(self, response, on_connect=lambda: None, on_send=lambda: None):
        self.response = response
        self.sock = ByteSocket(b"")
        self.on_connect = on_connect
        self.on_send = on_send
        self.connected = False
        self.closed = False
        self.sent = []

    def connect(self):
        self.on_connect()
        self.connected = True

    def request(self, method, path, *, body, headers):
        self.on_send()
        self.sent.append((method, path, body, headers))

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


class GatewayContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="recall-d-gateway-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = [1_800_000_000.0, 1000.0, "synthetic-boot"]
        self.ledgers = []
        self.addCleanup(self.close_ledgers)
        self.scope = GATEWAY.wire.Scope(
            "surreal-db", "semantics-household", "extraction", "gpt-6.1-sol"
        )

    def close_ledgers(self):
        for ledger in self.ledgers:
            ledger.close()

    def ledger(self):
        # Separate synthetic run roots also give each subcase its own independent
        # sibling anchor. Reusing a real approved manifest anchor is forbidden.
        parent = self.root / f"fixture-{len(self.ledgers)}"
        parent.mkdir()
        directory = parent / "run"
        result = GATEWAY.ledger.Ledger(
            directory, "d" * 64, create=True, now=lambda: tuple(self.now)
        )
        self.ledgers.append(result)
        return result

    def close_one(self, ledger):
        self.ledgers.remove(ledger)
        ledger.close()

    def request(self, changes=None, *, scope=None):
        value = {
            "model": "gpt-6.1-sol",
            "input": [{"role": "user", "content": "Synthetic Oriole fixture."}],
            "max_output_tokens": 128,
            "reasoning": {"effort": "medium", "summary": "auto"},
        }
        value.update(changes or {})
        raw = json.dumps(value, ensure_ascii=False).encode()
        return raw, GATEWAY.wire.validate(raw, "/v1/responses", scope or self.scope)

    def http_response(self, body, *, status=200, declared_length=None, streaming=False):
        length = len(body) if declared_length is None else declared_length
        content_type = "text/event-stream" if streaming else "application/json"
        raw = (
            f"HTTP/1.1 {status} Synthetic\r\nContent-Length: {length}\r\n"
            f"Content-Type: {content_type}\r\nConnection: close\r\n\r\n"
        ).encode() + body
        response = http.client.HTTPResponse(ByteSocket(raw), method="POST")
        response.begin()
        return response

    def dispatch(self, ledger, raw, request, response, *, authorize=lambda: None,
                 on_connect=lambda: None, on_send=lambda: None, capture=None):
        conn = FakeConnection(response, on_connect, on_send)
        forwarded = bytearray()
        headers = []
        factory = lambda *_args: conn
        capture = capture or (
            lambda identifier, req, original: GATEWAY.capture.Capture(
                ledger.directory, identifier, req, original
            )
        )
        forwarder = GATEWAY.transport.Forwarder(
            ledger, authorize, factory, "PUBLIC-SYNTHETIC-KEY", capture
        )
        failure = None
        with patch.object(GATEWAY.transport.threading, "Timer", DormantTimer):
            try:
                forwarder.dispatch(
                    request, raw, lambda *values: headers.append(values), forwarded.extend
                )
            except Exception as error:
                failure = error
        return conn, bytes(forwarded), headers, failure

    def test_stateless_positive_has_only_disclosed_transforms(self):
        raw, request = self.request()
        forwarded = json.loads(request.body)
        original = json.loads(raw)
        for key in original:
            self.assertEqual(forwarded[key], original[key])
        self.assertEqual(set(forwarded) - set(original), {"service_tier", "store"})
        self.assertEqual(forwarded["service_tier"], "default")
        self.assertIs(forwarded["store"], False)
        self.assertEqual(request.original_hash, hashlib.sha256(raw).hexdigest())
        self.assertEqual(request.forwarded_hash, hashlib.sha256(request.body).hexdigest())
        self.assertGreater(request.reservation, 0)

    def test_opaque_context_paid_tools_and_unapproved_models_are_refused(self):
        cases = [
            {"previous_response_id": "resp_synthetic"},
            {"input": [{"type": "reasoning", "encrypted_content": "synthetic"}]},
            {"input": [{"role": "assistant", "content": "x", "id": "msg_synthetic"}]},
            {"input": [{"role": "user", "content": [{"type": "input_file", "file_id": "x"}]}]},
            {"input": [{"type": "item_reference", "id": "x"}]},
            {"tools": [{"type": "web_search"}]},
            {"tools": [{"type": "function", "name": "x", "parameters": {"$ref": "https://fixture.invalid/x"}}]},
            {"service_tier": "priority"},
            {"model": "gpt-6-astra"},
            {"reasoning": {"effort": "high"}},
            {"max_output_tokens": 16385},
            {"max_output_tokens": True},
        ]
        for change in cases:
            with self.subTest(change=change):
                with self.assertRaises(GATEWAY.wire.Denied):
                    self.request(change)

    def test_embedding_batch_limit_counts_all_inputs(self):
        scope = GATEWAY.wire.Scope(
            "elephant-db", "semantics-business", "embedding",
            "text-embedding-3-large", 3072,
        )
        body = {"model": scope.model, "input": ["short fixture"], "dimensions": 3072}
        good = GATEWAY.wire.validate(json.dumps(body).encode(), "/v1/embeddings", scope)
        self.assertGreater(good.reservation, 0)
        # Each item is individually smaller than the proposal limit. The batch
        # exceeds it and must not receive the one-batch reservation regardless.
        body["input"] = ["é" * 2200, "é" * 2200]
        with self.assertRaises(GATEWAY.wire.Denied):
            GATEWAY.wire.validate(json.dumps(body).encode(), "/v1/embeddings", scope)

    def test_output_cap_transform_is_explicit_and_actual_forwarding_is_captured(self):
        raw, _request = self.request()
        original = json.loads(raw)
        original["max_output_tokens"] = 64000  # Actual active extractor request.
        raw = json.dumps(original).encode()
        with self.assertRaises(GATEWAY.wire.Denied):
            GATEWAY.wire.validate(raw, "/v1/responses", self.scope)
        request = GATEWAY.wire.validate(
            raw, "/v1/responses", self.scope, output_cap_mode="cap_at_16384"
        )
        self.assertEqual((request.requested_output_cap, request.output_cap), (64000, 16384))
        self.assertEqual(json.loads(request.body)["max_output_tokens"], 16384)
        self.assertEqual(json.loads(request.body)["input"], original["input"])
        self.assertEqual(json.loads(request.body)["reasoning"], original["reasoning"])
        self.assertIn("approved_output_cap:64000->16384", request.transforms)
        ledger = self.ledger()
        body = b'{"usage":{"input_tokens":4,"output_tokens":2},"output":[]}'
        conn, forwarded, _headers, failure = self.dispatch(
            ledger, raw, request, self.http_response(body)
        )
        self.assertIsNone(failure)
        self.assertEqual(forwarded, body)
        self.assertEqual(conn.sent[0][2], request.body)
        entry = ledger.receipt()["dispatches"][0]
        self.assertEqual((entry["requested_output_cap"], entry["output_cap"]), (64000, 16384))
        self.assertEqual(entry["reserved"], 245760000)
        capture = ledger.directory / "dispatch-000001"
        self.assertEqual(json.loads((capture / "request.original.json").read_bytes()), original)
        self.assertEqual((capture / "request.forwarded.json").read_bytes(), request.body)

    def test_output_cap_mode_never_repairs_invalid_limits_or_increases_small_ones(self):
        raw, _request = self.request()
        original = json.loads(raw)
        for limit in (False, 0, -1, 128001, 16384.5, "64000", None):
            original["max_output_tokens"] = limit
            with self.subTest(limit=limit), self.assertRaises(GATEWAY.wire.Denied):
                GATEWAY.wire.validate(json.dumps(original).encode(), "/v1/responses",
                                      self.scope, output_cap_mode="cap_at_16384")
        original["max_output_tokens"] = 123
        raw = json.dumps(original).encode()
        request = GATEWAY.wire.validate(raw, "/v1/responses", self.scope,
                                        output_cap_mode="cap_at_16384")
        self.assertEqual(request.output_cap, 123)
        self.assertEqual(json.loads(request.body)["max_output_tokens"], 123)
        self.assertFalse(any(value.startswith("approved_output_cap:") for value in request.transforms))
        with self.assertRaises(GATEWAY.wire.Denied):
            GATEWAY.wire.validate(raw, "/v1/responses", self.scope, output_cap_mode="implicit")

    def test_transform_requires_separate_approval_reference_and_immutable_manifest(self):
        # Synthetic trusted-host records only; socket construction remains denied.
        manifest = {
            "format": "recall-gateway-manifest/v1", "run_id": "D-offline-policy-only",
            "live_ready": True, "synthetic_corpus_only": True, "account_id": "synthetic-no-account",
            "endpoint": "https://api.openai.com/v1", "gateway_endpoint": "http://127.0.0.1:43129/v1",
            "bound_version": GATEWAY.wire.BOUND_VERSION, "transport_response_byte_cap": 8388608,
            "upstream_ips": ["1.1.1.1"], "run_directory": str(self.root / "unused-run"),
            "catalog_sha256": "9b40bf9b99a93b3f6d1ba7cf0b402fd46b01b4aac42205c9681c8e894a03e2ce",
            "corpus_sha256": "fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b",
            "output_cap_mode": "cap_at_16384",
            "bindings": [{"id": "fixture", "backend": "surreal-db", "space": "semantics-household",
                          "operation": "extraction", "model": "gpt-6.1-sol",
                          "connection_id": "fixture-connection", "profile_id": "fixture-profile"}],
        }
        for name in ("proposal_sha256", "artifact_sha256", "configuration_sha256", "runtime_sha256", "gateway_sha256"):
            manifest[name] = "d" * 64
        manifest_path, approval_path = self.root / "manifest.json", self.root / "approval.json"
        manifest_raw = json.dumps(manifest).encode()
        manifest_path.write_bytes(manifest_raw)
        manifest_path.chmod(0o600)
        approval = {"format": "recall-gateway-approval/v1", "authorized": True,
                    "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
                    "expires_unix": 4102444800, "user_approval_id": "SYNTHETIC-NOT-A-LIVE-APPROVAL"}
        for name in ("bound_review_sha256", "isolation_review_sha256", "binding_review_sha256", "capture_scope_review_sha256"):
            approval[name] = "e" * 64
        approval_path.write_text(json.dumps(approval))
        approval_path.chmod(0o600)
        with self.assertRaises(GATEWAY.wire.Denied):
            GATEWAY.policy.Policy(manifest_path, approval_path)
        approval["output_transform_review_sha256"] = "f" * 64
        approval_path.write_text(json.dumps(approval))
        policy = GATEWAY.policy.Policy(manifest_path, approval_path)
        self.assertEqual(policy.output_cap_mode, "cap_at_16384")
        manifest["output_cap_mode"] = "reject"
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaises(GATEWAY.wire.Denied):
            policy.check()

    def test_actual_sse_bytes_and_both_request_bodies_are_captured(self):
        ledger = self.ledger()
        raw, request = self.request({"stream": True})
        body = (
            b': synthetic keepalive\r\n\r\nevent: response.completed\r\n'
            b'data: {"type":"response.completed","response":{"usage":'
            b'{"input_tokens":12,"output_tokens":4}}}\r\n\r\ndata: [DONE]\r\n\r\n'
        )
        conn, forwarded, headers, failure = self.dispatch(
            ledger, raw, request, self.http_response(body, streaming=True)
        )
        self.assertIsNone(failure)
        self.assertEqual(forwarded, body)
        self.assertEqual(headers, [(200, "text/event-stream")])
        self.assertEqual(len(conn.sent), 1)
        self.assertTrue(conn.closed)
        capture = ledger.directory / "dispatch-000001"
        self.assertEqual((capture / "request.original.json").read_bytes(), raw)
        self.assertEqual((capture / "request.forwarded.json").read_bytes(), request.body)
        self.assertEqual((capture / "response.body").read_bytes(), body)
        receipt = json.loads((capture / "capture.json").read_bytes())
        self.assertIs(receipt["complete"], True)
        self.assertEqual(receipt["response_sha256"], hashlib.sha256(body).hexdigest())
        for path in capture.iterdir():
            self.assertEqual(path.stat().st_mode & 0o077, 0)
            self.assertNotIn(b"PUBLIC-SYNTHETIC-KEY", path.read_bytes())
        self.assertIsNone(ledger.receipt()["halted"])
        self.assertEqual(ledger.receipt()["reserved_nano_usd"], request.reservation)

    def test_short_content_length_is_incomplete_even_after_terminal_event(self):
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                ledger = self.ledger()
                raw, request = self.request({"stream": streaming})
                body = (b'data: {"type":"response.completed"}\n\n' if streaming
                        else b'{"output":[],"usage":{"input_tokens":1,"output_tokens":0}}')
                response = self.http_response(
                    body, declared_length=len(body) + 19, streaming=streaming
                )
                _conn, forwarded, _headers, _failure = self.dispatch(
                    ledger, raw, request, response
                )
                self.assertEqual(forwarded, body)
                receipt = ledger.receipt()
                self.assertIsNotNone(receipt["halted"], "short HTTP body was acknowledged complete")
                self.assertEqual(receipt["dispatches"][0]["outcome"], "incomplete")
                self.assertEqual(receipt["reserved_nano_usd"], request.reservation)
                capture = ledger.directory / "dispatch-000001" / "capture.json"
                self.assertIs(json.loads(capture.read_bytes())["complete"], False)

    def test_stream_without_terminal_event_halts_and_retains_reservation(self):
        ledger = self.ledger()
        raw, request = self.request({"stream": True})
        body = b'data: {"type":"response.output_text.delta","delta":"synthetic"}\n\n'
        self.dispatch(ledger, raw, request, self.http_response(body, streaming=True))
        receipt = ledger.receipt()
        self.assertIsNotNone(receipt["halted"])
        self.assertEqual(receipt["dispatches"][0]["outcome"], "incomplete")
        self.assertEqual(receipt["reserved_nano_usd"], request.reservation)
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.reserve(request, lambda: None)

    def test_reservation_and_original_capture_precede_connect_and_send(self):
        ledger = self.ledger()
        raw, request = self.request()
        observed = []

        def inspect():
            row = ledger.receipt()["dispatches"][0]
            self.assertEqual(row["reserved"], request.reservation)
            self.assertIsNone(row["outcome"])
            path = ledger.directory / "dispatch-000001" / "request.original.json"
            self.assertEqual(path.read_bytes(), raw)
            observed.append(True)

        body = b'{"usage":{"input_tokens":1,"output_tokens":1}}'
        _conn, _bytes, _headers, failure = self.dispatch(
            ledger, raw, request, self.http_response(body), on_connect=inspect, on_send=inspect
        )
        self.assertIsNone(failure)
        self.assertEqual(observed, [True, True])

    def test_revocation_after_connect_prevents_send_and_halts(self):
        ledger = self.ledger()
        raw, request = self.request()
        revoked = [False]

        def authorize():
            if revoked[0]:
                raise GATEWAY.wire.Denied("approval_revoked")

        conn, _bytes, _headers, failure = self.dispatch(
            ledger, raw, request, self.http_response(b"{}"), authorize=authorize,
            on_connect=lambda: revoked.__setitem__(0, True),
        )
        self.assertIsNotNone(failure)
        self.assertTrue(conn.connected)
        self.assertEqual(conn.sent, [])
        self.assertTrue(conn.closed)
        self.assertIsNotNone(ledger.receipt()["halted"])
        self.assertEqual(ledger.receipt()["reserved_nano_usd"], request.reservation)

    def test_ambiguous_restart_does_not_refund_or_resume(self):
        ledger = self.ledger()
        _raw, request = self.request()
        ledger.reserve(request, lambda: None)
        directory = ledger.directory
        self.close_one(ledger)
        with self.assertRaises(GATEWAY.wire.Denied):
            GATEWAY.ledger.Ledger(directory, "d" * 64, create=False, now=lambda: tuple(self.now))
        # Read the durable evidence through sqlite only; no implementation reset.
        import sqlite3
        with sqlite3.connect(directory / "ledger.sqlite3") as connection:
            self.assertEqual(connection.execute("SELECT SUM(reserved) FROM dispatch").fetchone()[0], request.reservation)
            self.assertIsNotNone(connection.execute("SELECT halted FROM run").fetchone()[0])

    def test_deleted_ledger_never_recreates_existing_run(self):
        ledger = self.ledger()
        directory = ledger.directory
        self.close_one(ledger)
        (directory / "ledger.sqlite3").unlink()
        with self.assertRaises(GATEWAY.wire.Denied):
            GATEWAY.ledger.Ledger(directory, "d" * 64, create=False, now=lambda: tuple(self.now))
        with self.assertRaises(FileExistsError):
            GATEWAY.ledger.Ledger(directory, "d" * 64, create=True, now=lambda: tuple(self.now))
        self.assertFalse((directory / "ledger.sqlite3").exists())

    def test_role_quota_is_shared_across_spaces_and_retries(self):
        ledger = self.ledger()
        for index in range(56):
            space = "semantics-household" if index % 2 else "semantics-business"
            scope = GATEWAY.wire.Scope("surreal-db", space, "extraction", "gpt-6.1-sol")
            _raw, request = self.request(scope=scope)
            identifier, _ = ledger.reserve(request, lambda: None)
            ledger.finish(identifier, outcome="upstream_error", status=429,
                          response_hash=hashlib.sha256(b"{}").hexdigest(), response_bytes=2,
                          usage=None, halt=False)
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.reserve(request, lambda: None)
        other = GATEWAY.wire.Scope("elephant-db", "semantics-household", "extraction", "gpt-6.1-sol")
        _raw, request = self.request(scope=other)
        ledger.reserve(request, lambda: None)
        self.assertEqual(len(ledger.receipt()["dispatches"]), 57)
        self.assertEqual(ledger.receipt()["reserved_nano_usd"], 57 * request.reservation)

    def test_shared_embedding_profile_has_one_combined_quota(self):
        ledger = self.ledger()
        for index in range(76):
            business = bool(index % 2)
            scope = GATEWAY.wire.Scope(
                "surreal-db", "semantics-business" if business else "semantics-household",
                "embedding", "text-embedding-3-large" if business else "text-embedding-3-small",
                3072 if business else 1536,
            )
            body = {"model": scope.model, "dimensions": scope.dimensions,
                    "input": "Unchanged original synthetic embedding input."}
            request = GATEWAY.wire.validate(json.dumps(body).encode(), "/v1/embeddings", scope)
            identifier, _ = ledger.reserve(request, lambda: None)
            ledger.finish(identifier, outcome="upstream_error", status=429,
                          response_hash=hashlib.sha256(b"{}").hexdigest(), response_bytes=2,
                          usage=None, halt=False)
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.reserve(request, lambda: None)
        self.assertEqual(len(ledger.receipt()["dispatches"]), 76)
        self.assertEqual(ledger.receipt()["reserved_nano_usd"], 76 * request.reservation)

    def test_global_concurrency_and_original_clock_deadline(self):
        ledger = self.ledger()
        _raw, request = self.request()
        ledger.reserve(request, lambda: None)
        other = GATEWAY.wire.Scope("elephant-db", "semantics-business", "extraction", "gpt-6.1-sol")
        _raw, second = self.request(scope=other)
        ledger.reserve(second, lambda: None)
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.reserve(request, lambda: None)
        self.now[0] -= 1
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.before_forward(lambda: None)
        self.now[0] += 5402
        self.now[1] += 5401
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.before_forward(lambda: None)
        self.assertEqual(len(ledger.receipt()["dispatches"]), 2)

    def test_observed_usage_above_reservation_halts_without_refund(self):
        ledger = self.ledger()
        _raw, request = self.request()
        identifier, _ = ledger.reserve(request, lambda: None)
        ledger.finish(identifier, outcome="complete", status=200,
                      response_hash="a" * 64, response_bytes=20,
                      usage=(request.input_bound + 1, 0), halt=False)
        receipt = ledger.receipt()
        self.assertEqual(receipt["halted"], "usage_bound_violation")
        self.assertEqual(receipt["reserved_nano_usd"], request.reservation)
        with self.assertRaises(GATEWAY.wire.Denied):
            ledger.reserve(request, lambda: None)


def source_hashes(directory):
    return {f"{name}.py": hashlib.sha256((directory / f"{name}.py").read_bytes()).hexdigest()
            for name in MODULES}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-source", type=Path, required=True)
    args = parser.parse_args()
    directory = args.gateway_source.resolve(strict=True)
    before = source_hashes(directory)
    sys.path.insert(0, str(directory))
    # This test process cannot accidentally contact an upstream or start a socket.
    global GATEWAY
    with ExitStack() as stack:
        for target in ("socket.socket", "socket.create_connection"):
            stack.enter_context(patch(target, side_effect=AssertionError("D offline oracle forbids networking")))
        GATEWAY = SimpleNamespace(**{name: importlib.import_module(name) for name in MODULES})
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(GatewayContract)
        )
    stable = before == source_hashes(directory)
    print(json.dumps({
        "format": "recall-d-gateway-offline-result/v1", "source_sha256": before,
        "source_stable_during_test": stable, "tests_run": result.testsRun,
        "failures": len(result.failures), "errors": len(result.errors),
        "network_permitted": False, "token_framing_bound_qualified": False,
        "installed_artifact_qualified": False, "live_spending_authorized": False,
    }, sort_keys=True))
    return 0 if result.wasSuccessful() and stable else 1


if __name__ == "__main__":
    sys.exit(main())
