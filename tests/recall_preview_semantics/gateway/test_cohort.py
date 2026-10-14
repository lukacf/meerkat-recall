"""Offline exclusive allocation controls: real SQLite/fsync, mocked transport."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from capture import Capture
from cohort import CohortLedger, COUNT_CAP, CAPTURE_CAP, CAPTURE_SLOT, PHASES, allocation_contract
from cohort_policy import CohortPolicy
from ledger import Ledger
from transport import Forwarder
from wire import Denied, Scope, encode, validate
from test_gateway import FakeConnection, FakeResponse


def scopes():
    result = {}
    for backend in ("elephant-db", "surreal-db"):
        for name, model, dimensions in (("household", "text-embedding-3-small", 1536), ("business", "text-embedding-3-large", 3072)):
            key = backend.replace("-", "_") + "_" + name
            result[key] = Scope(backend, "qualification-" + name, "embedding", model, dimensions,
                                key, key + "_connection", key + "_profile", 1)
    return result


class CohortTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name) / "run"
        self.wall, self.mono = time.time(), time.monotonic()
        self.scopes = scopes()
        self.identity = {"account_id": "offline-synthetic", "credential_generation": 1,
                         **{name: "a" * 64 for name in ("configuration_sha256", "bindings_sha256",
                                                        "allocation_sha256", "contract_sha256", "corpus_sha256",
                                                        "preparation_contract_sha256", "typed_delta_sha256")}}
        self.ledger = CohortLedger(self.directory, "a" * 64, create=True, identity=self.identity,
                                  scopes=self.scopes, now=lambda: (self.wall, self.mono, "offline-boot"))
        self.scope = self.scopes["elephant_db_household"]
        self.caller = "b" * 64
        self.no_sockets = patch("socket.create_connection", side_effect=AssertionError("offline tests cannot open sockets"))
        self.no_sockets.start()

    def tearDown(self):
        self.no_sockets.stop()
        if self.ledger is not None:
            self.ledger.close()
        self.temp.cleanup()

    def begin(self, phase="preparation", backend="elephant-db", caller=None):
        return self.ledger.begin(backend, phase, caller or self.caller, lambda: None)

    def request(self, scope=None):
        scope = scope or self.scope
        raw = encode({"model": scope.model, "input": ["Synthetic source paragraph."], "dimensions": scope.dimensions})
        return raw, validate(raw, "/v1/embeddings", scope)

    def forward(self, *, scope=None, status=200, fail_after=None, on_send=None):
        raw, request = self.request(scope)
        connection = FakeConnection(FakeResponse(encode({"model": request.scope.model, "data": [],
                                                        "usage": {"prompt_tokens": 3, "total_tokens": 3}}), status, fail_after))
        original_send = connection.request
        def send(*args, **kwargs):
            entry = self.ledger.db.execute("SELECT send_intent FROM cohort_dispatch ORDER BY dispatch_id DESC LIMIT 1").fetchone()
            self.assertEqual(entry, (1,), "send authority must be durably recorded first")
            if on_send:
                on_send()
            original_send(*args, **kwargs)
        connection.request = send
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "offline-not-provider-credential",
                              lambda identifier, req, original: Capture(self.directory, identifier, req, original,
                                                                       binding=self.ledger.binding(req.scope)))
        forwarder.dispatch(request, raw, lambda *_: None, lambda _: None)
        self.assertEqual(len(connection.sent), 1)

    def close(self, phase="preparation", backend="elephant-db", caller=None, observation="c" * 64):
        return self.ledger.close_cohort(backend, phase, caller or self.caller, observation)

    def test_no_send_without_original_cohort(self):
        with self.assertRaisesRegex(Denied, "no_open_cohort"):
            self.forward()
        self.assertEqual(self.ledger.receipt()["dispatches"], [])

    def test_durable_forward_capture_and_idempotent_seal(self):
        begin = self.begin()
        self.forward()
        result = self.close()
        self.assertEqual(result["verdict"], "passed")
        self.assertEqual(result["before_sha256"], begin["baseline_sha256"])
        self.assertEqual(result["physical_dispatch_reservations"], 1)
        self.assertEqual(result["reserved_nano_usd"], 1064960)
        self.assertEqual(result["per_request_association"], "unmet")
        self.assertTrue((self.directory / "cohort-01.json").is_file())
        self.assertEqual(self.close(), result)
        with self.assertRaisesRegex(Denied, "replay_mismatch"):
            self.close(observation="d" * 64)

    def test_four_single_use_cohorts_and_backend_order(self):
        with self.assertRaisesRegex(Denied, "cohort_order"):
            self.begin(backend="surreal-db")
        for index, (backend, phase) in enumerate(PHASES):
            caller = str(index + 1) * 64
            self.begin(backend=backend, phase=phase, caller=caller)
            scope = next(s for s in self.scopes.values() if s.backend == backend)
            self.forward(scope=scope)
            self.assertEqual(self.close(backend=backend, phase=phase, caller=caller)["verdict"], "passed")
        with self.assertRaisesRegex(Denied, "cohort_order"):
            self.begin()
        self.assertEqual(len(self.ledger.receipt()["dispatches"]), 4)

    def test_wrong_backend_binding_and_generation_are_denied(self):
        self.begin()
        for scope in (self.scopes["surreal_db_household"], replace(self.scope, profile_id="other"),
                      replace(self.scope, credential_generation=2)):
            with self.assertRaises(Denied):
                self.forward(scope=scope)
        self.assertEqual(self.ledger.receipt()["dispatches"], [])

    def test_generation_cannot_reserve_even_with_forged_request(self):
        self.begin()
        _, request = self.request()
        with self.assertRaises(Denied):
            self.ledger.reserve(replace(request, path="/v1/responses", output_cap=100), lambda: None)
        self.assertEqual(self.ledger.receipt()["dispatches"], [])

    def test_preparation_concurrency_two_and_no_refund(self):
        self.begin()
        _, request = self.request()
        for _ in range(2):
            self.ledger.reserve(request, lambda: None)
        with self.assertRaisesRegex(Denied, "concurrency_limit"):
            self.ledger.reserve(request, lambda: None)
        result = self.close()
        self.assertEqual(result["unsettled_dispatches"], 2)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 2 * 1064960)
        with self.assertRaisesRegex(Denied, "no_open_cohort"):
            self.ledger.reserve(request, lambda: None)
        with self.assertRaisesRegex(Denied, "not_drained"):
            self.begin(phase="measurement")

    def test_measurement_concurrency_fifty_is_explicit(self):
        self.begin()
        self.forward()
        self.close()
        self.begin(phase="measurement", caller="d" * 64)
        _, request = self.request()
        for _ in range(50):
            self.ledger.reserve(request, lambda: None)
        with self.assertRaisesRegex(Denied, "concurrency_limit"):
            self.ledger.reserve(request, lambda: None)

    def test_same_body_two_physical_attempts_are_never_merged(self):
        self.begin()
        self.forward(status=503)
        self.forward()
        result = self.close()
        self.assertEqual(result["physical_dispatch_reservations"], 2)
        self.assertEqual(result["reserved_nano_usd"], 2 * 1064960)

    def test_tampered_capture_halts_and_retains_full_reservation(self):
        self.begin()
        self.forward()
        (self.directory / "dispatch-000001/response.body").write_bytes(b'{}')
        with self.assertRaisesRegex(Denied, "capture_mismatch"):
            self.close()
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 1064960)
        self.assertEqual(self.ledger.receipt()["halted"], "cohort_capture_or_seal_failure")

    def test_incomplete_transport_cannot_be_sealed_as_complete(self):
        self.begin()
        with self.assertRaises(OSError):
            self.forward(fail_after=0)
        with self.assertRaisesRegex(Denied, "capture_incomplete"):
            self.close()
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 1064960)

    def test_original_deadline_is_not_reset_by_another_begin(self):
        self.begin()
        self.wall += 3600
        self.mono += 3600
        with self.assertRaisesRegex(Denied, "cohort_deadline"):
            self.forward()
        with self.assertRaisesRegex(Denied, "not_drained"):
            self.begin()

    def test_v1_cannot_resume_cohort_ledger(self):
        self.ledger.close()
        self.ledger = None
        with self.assertRaisesRegex(Denied, "accounting_mode_mismatch"):
            Ledger(self.directory, "a" * 64, create=False)

    def test_forged_wire_bound_is_rejected_at_reservation(self):
        self.begin()
        _, request = self.request()
        with self.assertRaisesRegex(Denied, "wire_mismatch"):
            self.ledger.reserve(replace(request, input_bound=1), lambda: None)
        with self.assertRaises(Denied):
            self.ledger.reserve(replace(request, body=encode({"model": request.scope.model, "input": "x" * 8192})), lambda: None)
        self.assertEqual(self.ledger.receipt()["dispatches"], [])

    def test_resuming_open_empty_cohort_halts_without_fresh_interval(self):
        self.begin()
        self.ledger.close()
        self.ledger = None
        with self.assertRaisesRegex(Denied, "run_halted"):
            CohortLedger(self.directory, "a" * 64, create=False, identity=self.identity,
                         scopes=self.scopes, now=lambda: (self.wall, self.mono, "offline-boot"))

    def test_per_space_count_guard_survives_many_completed_attempts(self):
        self.begin()
        _, request = self.request()
        # Allocator unit inputs only; absence of actual capture files prevents
        # these synthetic outcomes from ever sealing a qualified cohort.
        for _ in range(225):
            identifier, _ = self.ledger.reserve(request, lambda: None)
            self.ledger.finish(identifier, outcome="upstream_error", status=503,
                               response_hash="a" * 64, response_bytes=0, usage=None, halt=False)
        with self.assertRaisesRegex(Denied, "cohort_count_limit"):
            self.ledger.reserve(request, lambda: None)
        _, business = self.request(self.scopes["elephant_db_business"])
        self.ledger.reserve(business, lambda: None)
        self.assertEqual(len(self.ledger.receipt()["dispatches"]), 226)

    def test_finite_capture_and_money_arithmetic(self):
        self.assertEqual(COUNT_CAP * CAPTURE_SLOT, 18235392000)
        self.assertLess(COUNT_CAP * CAPTURE_SLOT + 64 * 1024**2, CAPTURE_CAP)
        self.assertEqual(COUNT_CAP * 1064960, 2236416000)
        self.assertEqual(allocation_contract()["generation_dispatch_cap"], 0)

    def test_inactive_example_refuses_before_approval_read(self):
        path = Path(self.temp.name) / "manifest.json"
        path.write_bytes((Path(__file__).parent / "manifest.cohort.example.json").read_bytes())
        path.chmod(0o600)
        with self.assertRaisesRegex(Denied, "live_not_ready"):
            CohortPolicy(path, Path(self.temp.name) / "no-approval-or-key")


if __name__ == "__main__":
    unittest.main()
