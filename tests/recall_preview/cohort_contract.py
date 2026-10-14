#!/usr/bin/env python3
"""Independent D28 accounting oracles against an explicitly supplied G source.

Real temporary SQLite/capture files; mocked transport with the actual stdlib HTTP
parser. No listener, socket, subprocess, provider credential or live allocation.
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
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gateway_contract import ByteSocket, DormantTimer, FakeConnection

G = None
MODULES = ("wire", "ledger", "capture", "transport", "policy", "cohort", "cohort_policy", "cohort_bridge")
RESERVE = 1_064_960
D_ROOT = Path(__file__).resolve().parent


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


class CohortContract(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="recall-D-cohort-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.ledgers = []
        self.addCleanup(self.close_all)
        self.time = [1_800_000_000.0, 1000.0, "D-fixed-boot"]
        self.scopes = {}
        for backend in ("elephant-db", "surreal-db"):
            for suffix, model, dims in (("household", "text-embedding-3-small", 1536),
                                        ("business", "text-embedding-3-large", 3072)):
                key = backend.replace("-", "_") + "_" + suffix
                self.scopes[key] = G.wire.Scope(backend, "qualification-" + suffix, "embedding", model,
                    dims, key, key + "_connection", key + "_profile", 7)
        self.scope = self.scopes["elephant_db_household"]
        self.identity = {"account_id": "D-synthetic-account", "credential_generation": 7,
            **{key: "a" * 64 for key in ("configuration_sha256", "bindings_sha256", "allocation_sha256",
                "contract_sha256", "corpus_sha256", "preparation_contract_sha256", "typed_delta_sha256")}}
        self.ledger = self.open_ledger()

    def close_all(self):
        for ledger in self.ledgers:
            ledger.close()

    def open_ledger(self, create=True):
        ledger = G.cohort.CohortLedger(self.root / "run", "b" * 64, create=create,
            identity=self.identity, scopes=self.scopes, now=lambda: tuple(self.time))
        self.ledgers.append(ledger)
        return ledger

    def reopen(self):
        self.ledgers.remove(self.ledger)
        self.ledger.close()
        self.ledger = self.open_ledger(create=False)

    def begin(self, backend="elephant-db", phase="preparation", caller="c" * 64):
        return self.ledger.begin(backend, phase, caller, lambda: None)

    def close(self, backend="elephant-db", phase="preparation", caller="c" * 64, observation="d" * 64):
        return self.ledger.close_cohort(backend, phase, caller, observation)

    def request(self, scope=None, inputs=None):
        scope = scope or self.scope
        raw = encode({"model": scope.model, "dimensions": scope.dimensions,
                      "input": ["Unchanged original D synthetic paragraph."] if inputs is None else inputs})
        return raw, G.wire.validate(raw, "/v1/embeddings", scope)

    def forward(self, *, status=200, scope=None, on_connect=lambda: None, on_send=lambda: None,
                raw_response=None):
        raw, request = self.request(scope)
        body = encode({"model": request.scope.model, "data": [],
                       "usage": {"prompt_tokens": 7, "total_tokens": 7}})
        if raw_response is None:
            raw_response = (f"HTTP/1.1 {status} Fixture\r\nContent-Length: {len(body)}\r\n"
                            "Content-Type: application/json\r\n\r\n").encode() + body
        response = http.client.HTTPResponse(ByteSocket(raw_response), method="POST")
        response.begin()
        def sent():
            # Independent read-only database connection sees the actual durable
            # original reservation/send intent before the mocked physical send.
            with sqlite3.connect(f"file:{self.root / 'run/ledger.sqlite3'}?mode=ro", uri=True) as db:
                row = db.execute("SELECT d.reserved,c.send_intent FROM dispatch d JOIN cohort_dispatch c "
                                 "ON c.dispatch_id=d.id ORDER BY d.id DESC LIMIT 1").fetchone()
                self.assertEqual(row, (RESERVE, 1))
            on_send()
        connection = FakeConnection(response, on_connect, sent)
        forwarder = G.transport.Forwarder(self.ledger, lambda: None, lambda _seconds: connection,
            "D-synthetic-not-a-provider-key", lambda identifier, req, original:
                G.capture.Capture(self.root / "run", identifier, req, original,
                                  binding=self.ledger.binding(req.scope)))
        with patch.object(G.transport.threading, "Timer", DormantTimer):
            forwarder.dispatch(request, raw, lambda *_args: None, lambda _body: None)
        self.assertEqual(len(connection.sent), 1)
        return connection

    def test_exact_frozen_numeric_caps_and_four_phase_order(self):
        allocation = G.cohort.allocation_contract()
        self.assertEqual((allocation["physical_dispatch_cap"], allocation["money_cap_nano_usd"],
                          allocation["time_cap_seconds"], allocation["reservation_per_dispatch_nano_usd"]),
                         (2100, 3_000_000_000, 7800, RESERVE))
        self.assertEqual(2100 * (2 * 131072 + 8388608 + 32768), 18_235_392_000)
        self.assertLess(2100 * RESERVE, 3_000_000_000)
        phases = [("elephant-db", "preparation", 450, 225, 2, 3600),
                  ("elephant-db", "measurement", 600, 300, 50, 120),
                  ("surreal-db", "preparation", 450, 225, 2, 3600),
                  ("surreal-db", "measurement", 600, 300, 50, 120)]
        self.assertEqual([(p["backend"], p["phase"], p["physical_dispatch_cap"], p["per_space_cap"],
                           p["concurrency_cap"], p["time_cap_seconds"]) for p in allocation["phases"]], phases)
        with self.assertRaises(G.wire.Denied):
            self.begin(phase="measurement")
        with self.assertRaises(G.wire.Denied):
            self.begin(backend="surreal-db")

    def test_original_ids_capture_and_baseline_survive_closed_reopen(self):
        first = self.begin()
        self.forward(status=503)
        self.forward()
        closed = self.close()
        receipt = self.ledger.receipt()
        self.assertEqual([row["id"] for row in receipt["dispatches"]], [1, 2])
        self.assertEqual(receipt["dispatches"][0]["request_hash"], receipt["dispatches"][1]["request_hash"])
        self.assertEqual(closed["reserved_nano_usd"], 2 * RESERVE)
        self.assertEqual(closed["physical_dispatch_reservations"], 2)
        self.assertEqual(closed["before_sha256"], first["baseline_sha256"])
        self.assertEqual(closed["per_request_association"], "unmet")
        self.assertTrue(closed["admission_closed"])
        self.assertEqual(closed["unsettled_dispatches"], 0)
        seal = (self.root / "run/cohort-01.json").read_bytes()
        self.assertEqual(digest(seal), closed["sealed_receipt_sha256"])
        self.reopen()
        self.assertEqual(self.close(), closed)
        self.assertEqual((self.root / "run/cohort-01.json").read_bytes(), seal)
        with self.assertRaises(G.wire.Denied):
            self.close(observation="e" * 64)
        with self.assertRaises(G.wire.Denied):
            self.begin()

    def test_close_during_original_send_blocks_new_work_and_drains_same_id(self):
        self.begin()
        def close_active():
            pending = self.close()
            self.assertEqual(pending["verdict"], "unmet")
            self.assertEqual(pending["unsettled_dispatches"], 1)
            self.assertTrue(pending["admission_closed"])
            _, request = self.request()
            with self.assertRaises(G.wire.Denied):
                self.ledger.reserve(request, lambda: None)
            with self.assertRaises(G.wire.Denied):
                self.begin(phase="measurement", caller="e" * 64)
        self.forward(on_send=close_active)
        result = self.close()
        self.assertEqual(result["verdict"], "passed")
        self.assertEqual(result["reserved_nano_usd"], RESERVE)
        self.assertEqual(result["physical_dispatch_reservations"], 1)
        self.assertEqual(self.close(), result)

    def test_close_before_send_cannot_reopen_or_refund_original_reservation(self):
        self.begin()
        def close_before_send():
            self.assertEqual(self.close()["unsettled_dispatches"], 1)
        with self.assertRaises(G.wire.Denied):
            self.forward(on_connect=close_before_send)
        receipt = self.ledger.receipt()
        self.assertEqual(receipt["reserved_nano_usd"], RESERVE)
        self.assertEqual(len(receipt["dispatches"]), 1)
        self.assertNotEqual(receipt["dispatches"][0]["outcome"], "complete")
        with self.assertRaises(G.wire.Denied):
            self.close()

    def test_changed_profile_generation_or_backend_cannot_reserve(self):
        self.begin()
        for scope in (replace(self.scope, profile_id="wrong"), replace(self.scope, credential_generation=8),
                      self.scopes["surreal_db_household"]):
            with self.subTest(scope=scope), self.assertRaises(G.wire.Denied):
                _, request = self.request(scope)
                self.ledger.reserve(request, lambda: None)
        self.assertEqual(self.ledger.receipt()["dispatches"], [])

    def test_full_batch_bound_and_forged_computed_bound_refused(self):
        self.begin()
        with self.assertRaises(G.wire.Denied):
            self.request(inputs=["a"] * 65)
        with self.assertRaises(G.wire.Denied):
            self.request(inputs=["🌍" * 2048])
        _, request = self.request()
        with self.assertRaises(G.wire.Denied):
            self.ledger.reserve(replace(request, input_bound=1), lambda: None)
        with self.assertRaises(G.wire.Denied):
            self.ledger.reserve(replace(request, path="/v1/responses", output_cap=100), lambda: None)
        self.assertEqual(self.ledger.receipt()["dispatches"], [])

    def test_measurement_fifty_keeps_preparation_reservations_and_blocks_next_phase(self):
        self.begin()
        self.forward()
        self.close()
        self.begin(phase="measurement", caller="e" * 64)
        _, request = self.request()
        for _ in range(50):
            self.ledger.reserve(request, lambda: None)
        with self.assertRaises(G.wire.Denied):
            self.ledger.reserve(request, lambda: None)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 51 * RESERVE)
        pending = self.close(phase="measurement", caller="e" * 64)
        self.assertEqual(pending["verdict"], "unmet")
        self.assertEqual(pending["unsettled_dispatches"], 50)
        with self.assertRaises(G.wire.Denied):
            self.begin(backend="surreal-db", caller="f" * 64)

    def test_original_measurement_deadline_is_not_reset(self):
        self.begin()
        self.forward()
        self.close()
        self.begin(phase="measurement", caller="e" * 64)
        self.time[0] += 121
        self.time[1] += 121
        _, request = self.request()
        with self.assertRaises(G.wire.Denied):
            self.ledger.reserve(request, lambda: None)
        with self.assertRaises(G.wire.Denied):
            self.begin(phase="measurement", caller="f" * 64)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], RESERVE)

    def test_corrupted_original_capture_halts_without_seal_or_refund(self):
        self.begin()
        self.forward()
        path = self.root / "run/dispatch-000001/request.original.json"
        path.write_bytes(b'{"different":"D explicit corruption"}')
        with self.assertRaises(G.wire.Denied):
            self.close()
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], RESERVE)
        self.assertIsNotNone(self.ledger.receipt()["halted"])
        self.assertFalse((self.root / "run/cohort-01.json").exists())

    def test_real_http_parser_short_response_cannot_seal(self):
        self.begin()
        with self.assertRaises(G.wire.Denied):
            self.forward(raw_response=b'HTTP/1.1 200 Fixture\r\nContent-Length: 90\r\n\r\n{}')
        with self.assertRaises(G.wire.Denied):
            self.close()
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], RESERVE)
        self.assertFalse((self.root / "run/cohort-01.json").exists())

    def test_typed_envelope_and_original_approval_pins_are_rechecked(self):
        files = {}
        for name, filename in (("allocation", "workload-allocation-proposal.json"),
                               ("preparation_contract", "workload-preparation-contract.json"),
                               ("typed_delta", "workload-allocation-typed-delta.json")):
            path = self.root / filename
            path.write_bytes((D_ROOT / filename).read_bytes())
            path.chmod(0o600)
            files[name] = path
        allocation = json.loads(files["allocation"].read_bytes())
        bindings = [{"id": scope.binding_id, "backend": scope.backend, "space": scope.space,
                     "operation": scope.operation, "model": scope.model, "dimensions": scope.dimensions,
                     "connection_id": scope.connection_id, "profile_id": scope.profile_id}
                    for scope in self.scopes.values()]
        manifest = {"format": "recall-exclusive-embedding-manifest/v1", "live_ready": True,
            "run_id": "D-offline-constructor-only", "run_directory": str(self.root / "unused-live-run"),
            "accounting_policy": "separate-exclusive-workload-embedding-reservation/v1",
            "account_id": "D-synthetic-account", "credential_generation": 7,
            "artifact_sha256": "a" * 64, "configuration_sha256": "b" * 64,
            "runtime_sha256": "c" * 64, "gateway_sha256": "d" * 64,
            "contract_sha256": allocation["custody"]["contract_sha256"],
            "corpus_sha256": allocation["custody"]["corpus_sha256"],
            "bindings": bindings, "endpoint": "https://api.openai.com/v1",
            "gateway_endpoint": "http://127.0.0.1:43129/v1", "upstream_ips": ["8.8.8.8"],
            "synthetic_corpus_only": True, "transport_response_byte_cap": 8388608,
            "bound_version": "utf8-wire-plus-protocol-v1"}
        # A public IP is parser-only input. Every socket/process constructor is
        # forbidden around this suite; no DNS, connect or launcher is invoked.
        for name, path in files.items():
            manifest[name + "_path"] = str(path)
            manifest[name + "_sha256"] = digest(path.read_bytes())
        manifest_path = self.root / "policy.json"
        manifest_path.write_bytes(encode(manifest))
        manifest_path.chmod(0o600)
        approval = {"format": "recall-exclusive-embedding-approval/v1", "authorized": True,
            "manifest_sha256": digest(manifest_path.read_bytes()), "expires_unix": 1_800_001_000,
            "user_approval_id": "D-CONSTRUCTOR-ONLY-NOT-REAL-AUTHORIZATION",
            **{key: "e" * 64 for key in ("bound_review_sha256", "isolation_review_sha256",
                "binding_review_sha256", "capture_scope_review_sha256", "cohort_review_sha256",
                "exclusive_run_review_sha256")}}
        approval_path = self.root / "approval.json"
        approval_path.write_bytes(encode(approval))
        approval_path.chmod(0o600)
        with patch.object(G.cohort_policy.time, "time", return_value=1_800_000_000):
            policy = G.cohort_policy.CohortPolicy(manifest_path, approval_path)
            policy.check()
            original = files["typed_delta"].read_bytes()
            files["typed_delta"].write_bytes(original + b" ")
            with self.assertRaises(G.wire.Denied):
                policy.check()
            files["typed_delta"].write_bytes(original)
            approval["authorized"] = False
            approval_path.write_bytes(encode(approval))
            with self.assertRaises(G.wire.Denied):
                policy.check()
        self.assertFalse((self.root / "unused-live-run").exists())

    def test_empty_cohort_does_not_claim_complete_provider_accounting(self):
        self.begin()
        with self.assertRaises(G.wire.Denied):
            self.close()
        self.assertEqual(self.ledger.receipt()["dispatches"], [])
        self.assertFalse((self.root / "run/cohort-01.json").exists())


def hashes(directory):
    return {name + ".py": digest((directory / (name + ".py")).read_bytes()) for name in MODULES}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-source", type=Path, required=True)
    args = parser.parse_args()
    directory = args.gateway_source.resolve(strict=True)
    before = hashes(directory)
    sys.path.insert(0, str(directory))
    global G
    with ExitStack() as stack:
        for name in ("socket.socket", "socket.create_connection", "subprocess.Popen"):
            stack.enter_context(patch(name, side_effect=AssertionError("D offline cohort forbids network/processes")))
        G = SimpleNamespace(**{name: importlib.import_module(name) for name in MODULES})
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(CohortContract))
    stable = before == hashes(directory)
    print(json.dumps({"format": "recall-d-cohort-offline/v1", "source_sha256": before,
        "source_stable_during_test": stable, "tests_run": result.testsRun,
        "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
        "network_permitted": False, "provider_billing_verified": False,
        "installed_artifact_qualified": False, "live_spending_authorized": False}, sort_keys=True))
    return 0 if result.wasSuccessful() and not result.skipped and stable else 1


if __name__ == "__main__":
    sys.exit(main())
