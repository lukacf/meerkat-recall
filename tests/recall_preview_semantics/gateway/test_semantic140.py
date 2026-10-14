"""Closed successor controls: SQLite/captures only, no providers or processes.

Allocator-history fixtures cannot establish a qualified cohort: they deliberately
lack provider captures. Positive closure cases use the real capture/seal path.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import socketserver
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import accounting_channel
import bridge
import launch_context
from capture import Capture
from cohort import CohortLedger, allocation_contract
from cohort_bridge import _payload
from cohort_policy import CohortPolicy, Semantic140Policy
from cohort_profile import D28, SEMANTIC140, profile_for_id
from ledger import Ledger
from server import GatewayServer
from test_cohort import scopes
from test_gateway import FakeConnection, FakeResponse
from test_launch_context import anchor
from transport import Forwarder
from wire import Denied, encode, digest, validate


def successor_identity():
    return {"account_id": "offline-synthetic", "credential_generation": 1,
            "configuration_sha256": "a" * 64, "bindings_sha256": "b" * 64,
            "allocation_sha256": SEMANTIC140.allocation_sha256,
            "contract_sha256": SEMANTIC140.contract_sha256,
            "corpus_sha256": SEMANTIC140.corpus_sha256,
            "preparation_contract_sha256": SEMANTIC140.preparation_sha256,
            "typed_delta_sha256": SEMANTIC140.typed_delta_sha256}


def successor_anchor():
    value = anchor()
    value.update(format="recall-gateway-launch-anchor/v2", profile_id="semantic140-v1",
                 allocation_sha256=SEMANTIC140.allocation_sha256,
                 proposal_sha256=SEMANTIC140.allocation_sha256,
                 corpus_sha256=SEMANTIC140.corpus_sha256)
    value["clock"].update(time_cap_seconds=7800, deadline_wall=8800.0, deadline_mono=8300.0)
    value["caps"].update(physical_dispatches=4260, maximum_nano_usd=5_000_000_000)
    return value


class NoExternalEffects(unittest.TestCase):
    def setUp(self):
        for name in ("socket.socket", "socket.create_connection", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("offline controls cannot start network/processes"))
            guard.start()
            self.addCleanup(guard.stop)


class Semantic140ProfileTests(NoExternalEffects):
    def test_original_primary_and_d28_limits_remain_exact(self):
        legacy = allocation_contract()
        self.assertNotIn("profile_id", legacy)
        self.assertEqual(legacy["format"], "recall-exclusive-embedding-allocation/v1")
        self.assertEqual((legacy["physical_dispatch_cap"], legacy["money_cap_nano_usd"],
                          legacy["gateway_capture_cap_bytes"], legacy["public_queries_per_backend"]),
                         (2100, 3_000_000_000, 21474836480, 200))
        self.assertEqual([(p["physical_dispatch_cap"], p["per_space_cap"], p["concurrency_cap"],
                           p["time_cap_seconds"]) for p in legacy["phases"]],
                         [(450, 225, 2, 3600), (600, 300, 50, 120)] * 2)
        self.assertEqual(launch_context.connection_limits(anchor()), (8, 5400))
        old = anchor()
        old["allocation_sha256"] = D28.allocation_sha256
        old["clock"].update(time_cap_seconds=7800, deadline_wall=8800.0, deadline_mono=8300.0)
        old["caps"].update(physical_dispatches=2100, maximum_nano_usd=3_000_000_000)
        self.assertEqual(launch_context.connection_limits(old), (64, 7800))

    def test_profile_is_closed_and_export_bounds_do_not_expand(self):
        new = allocation_contract("semantic140-v1")
        self.assertEqual((new["physical_dispatch_cap"], new["money_cap_nano_usd"],
                          new["gateway_capture_cap_bytes"], new["public_queries_per_backend"]),
                         (4260, 5_000_000_000, 42949672960, 560))
        self.assertEqual(4260 * 1064960, 4536729600)
        self.assertEqual(4260 * (2 * 131072 + 8388608 + 32768), 36991795200)
        self.assertEqual((accounting_channel.MAX_TOTAL, accounting_channel.MAX_FILES,
                          accounting_channel.MAX_RECORD, accounting_channel.MAX_CACHE),
                         (21474836480, 10532, 8388608, 268435456))
        for unknown in (None, "", "semantic141-v1", {"count_cap": 5000}):
            with self.subTest(profile=unknown), self.assertRaises(ValueError):
                profile_for_id(unknown)
        with self.assertRaises(AttributeError):
            SEMANTIC140.count_cap = 4261

    def test_new_anchor_requires_exact_identity_caps_and_original_clock(self):
        self.assertEqual(launch_context.connection_limits(successor_anchor()), (160, 7800))
        mutations = [lambda v: v.pop("profile_id"), lambda v: v.update(profile_id="d28-v1"),
                     lambda v: v.update(format=launch_context.ANCHOR_FORMAT),
                     lambda v: v.update(allocation_sha256=D28.allocation_sha256),
                     lambda v: v.update(corpus_sha256=D28.corpus_sha256),
                     lambda v: v.update(profile_helper_sha256="a" * 64),
                     lambda v: v["caps"].update(physical_dispatches=4261),
                     lambda v: v["caps"].update(maximum_nano_usd=3_000_000_000),
                     lambda v: v["clock"].update(deadline_mono=8301.0)]
        for mutate in mutations:
            value = successor_anchor()
            mutate(value)
            with self.subTest(anchor=value), self.assertRaises(ValueError):
                launch_context.validate_anchor(value)
        downgraded = successor_anchor()
        downgraded.pop("profile_id")
        downgraded["format"] = launch_context.ANCHOR_FORMAT
        downgraded["caps"].update(physical_dispatches=2100, maximum_nano_usd=3_000_000_000)
        with self.assertRaisesRegex(ValueError, "profile_required"):
            launch_context.validate_anchor(downgraded)

    def test_host_transport_capacity_is_closed_for_each_profile(self):
        for policy, expected, backlog in ((SimpleNamespace(cohort=False), 8, 4),
                                          (SimpleNamespace(cohort=True, profile=D28), 64, 64),
                                          (SimpleNamespace(cohort=True, profile=SEMANTIC140), 160, 160)):
            with self.subTest(capacity=expected), patch.object(socketserver.ThreadingUnixStreamServer,
                                                              "__init__", return_value=None):
                server = GatewayServer("/offline-unused", policy, None)
                self.assertEqual(server.request_queue_size, backlog)
                for _ in range(expected):
                    self.assertTrue(server.slots.acquire(blocking=False))
                self.assertFalse(server.slots.acquire(blocking=False))
                for _ in range(expected):
                    server.slots.release()

    def test_bridge_rejects_environment_capacity_or_deadline_overrides(self):
        value = successor_anchor()
        self.assertEqual(bridge.runtime_limits(launch_context, value, "160", "7800"), (160, 160))
        for connections, duration in (("161", "7800"), ("64", "7800"), ("160", "7801"),
                                      ("0160", "7800"), ("160", "07800")):
            with self.subTest(connections=connections, duration=duration), self.assertRaises(RuntimeError):
                bridge.runtime_limits(launch_context, value, connections, duration)
        with self.assertRaises(RuntimeError):
            bridge.runtime_limits(launch_context, anchor(), "160", "7800")


class Semantic140LedgerTests(NoExternalEffects):
    def setUp(self):
        super().setUp()
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name) / "run"
        self.wall, self.mono = 1000.0, 500.0
        self.scopes = scopes()
        self.identity = successor_identity()
        self.ledger = CohortLedger(self.directory, "a" * 64, create=True, identity=self.identity,
                                  scopes=self.scopes, profile_id="semantic140-v1",
                                  now=lambda: (self.wall, self.mono, "offline-boot"))

    def tearDown(self):
        if self.ledger is not None:
            self.ledger.close()
        self.temp.cleanup()

    def request(self, business=False):
        scope = self.scopes["elephant_db_business" if business else "elephant_db_household"]
        raw = encode({"model": scope.model, "input": ["Synthetic original query."], "dimensions": scope.dimensions})
        return raw, validate(raw, "/v1/embeddings", scope)

    def forward(self):
        raw, request = self.request()
        connection = FakeConnection(FakeResponse(encode({"data": [], "usage": {"prompt_tokens": 3}})))
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "offline-only",
                              lambda identifier, req, original: Capture(self.directory, identifier, req, original,
                                                                       binding=self.ledger.binding(req.scope)))
        # The ledger's injected clock and the original transport deadlines
        # must share one epoch. Keep every real deadline check, reservation,
        # capture and close; only the synthetic transport clock is controlled.
        with patch("transport.time.monotonic", side_effect=lambda: self.mono):
            forwarder.dispatch(request, raw, lambda *_: None, lambda _: None)
        self.assertEqual(len(connection.sent), 1)

    def measurement(self):
        self.ledger.begin("elephant-db", "preparation", "b" * 64, lambda: None)
        self.forward()
        self.assertEqual(self.ledger.close_cohort("elephant-db", "preparation", "b" * 64,
                                                 "c" * 64)["verdict"], "passed")
        return self.ledger.begin("elephant-db", "measurement", "d" * 64, lambda: None)

    def finish_allocator_fixture(self, identifier):
        self.ledger.finish(identifier, outcome="upstream_error", status=503, response_hash="a" * 64,
                           response_bytes=0, usage=None, halt=False)

    def test_preparation_stays_at_two_unsettled_dispatches(self):
        self.ledger.begin("elephant-db", "preparation", "b" * 64, lambda: None)
        _, request = self.request()
        for _ in range(2):
            self.ledger.reserve(request, lambda: None)
        with self.assertRaisesRegex(Denied, "cohort_concurrency_limit"):
            self.ledger.reserve(request, lambda: None)

    def test_140_unsettled_sends_are_not_refunded_by_public_timeout(self):
        self.measurement()
        _, request = self.request()
        identifiers = [self.ledger.reserve(request, lambda: None)[0] for _ in range(140)]
        self.wall += 8
        self.mono += 8
        with self.assertRaisesRegex(Denied, "cohort_concurrency_limit"):
            self.ledger.reserve(request, lambda: None)
        self.finish_allocator_fixture(identifiers[0])
        self.ledger.reserve(request, lambda: None)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 142 * 1064960)

    def test_per_space_840_is_a_hard_reservation_cap_including_errors(self):
        self.measurement()
        _, household = self.request()
        for _ in range(840):
            identifier, _ = self.ledger.reserve(household, lambda: None)
            self.finish_allocator_fixture(identifier)
        with self.assertRaisesRegex(Denied, "cohort_count_limit"):
            self.ledger.reserve(household, lambda: None)
        _, business = self.request(True)
        self.ledger.reserve(business, lambda: None)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 842 * 1064960)

    def test_global_count_and_money_guards_read_durable_history(self):
        self.measurement()
        _, request = self.request()
        # Explicit allocator-history fixture, not fabricated provider evidence.
        with self.ledger.transaction():
            self.ledger.db.executemany("INSERT INTO dispatch (backend,space,reserved,outcome) VALUES (?,?,?,?)",
                                       [("surreal-db", "qualification-business", 1064960, "upstream_error")] * 4258)
        self.ledger.reserve(request, lambda: None)
        with self.assertRaisesRegex(Denied, "dispatch_count_limit"):
            self.ledger.reserve(request, lambda: None)
        self.assertEqual(self.ledger.db.execute("SELECT COUNT(*) FROM dispatch").fetchone()[0], 4260)
        # A persisted excessive reservation must also refuse; no refund/repair.
        with self.ledger.transaction():
            self.ledger.db.execute("DELETE FROM dispatch WHERE id=2")
            self.ledger.db.execute("UPDATE dispatch SET reserved=5000000000 WHERE id=1")
        with self.assertRaisesRegex(Denied, "money_limit"):
            self.ledger.reserve(request, lambda: None)

    def test_admission_closes_before_unsettled_capture_and_cannot_change_observation(self):
        self.measurement()
        _, request = self.request()
        self.ledger.reserve(request, lambda: None)
        close = self.ledger.close_cohort("elephant-db", "measurement", "d" * 64, "e" * 64)
        self.assertEqual((close["verdict"], close["unsettled_dispatches"], close["admission_closed"]),
                         ("unmet", 1, True))
        with self.assertRaisesRegex(Denied, "no_open_cohort"):
            self.ledger.reserve(request, lambda: None)
        with self.assertRaisesRegex(Denied, "cohort_close_replay_mismatch"):
            self.ledger.close_cohort("elephant-db", "measurement", "d" * 64, "f" * 64)
        with self.assertRaisesRegex(Denied, "cohort_not_drained"):
            self.ledger.begin("surreal-db", "preparation", "f" * 64, lambda: None)

    def test_real_successful_capture_exports_original_unchanged_channel(self):
        self.measurement()
        self.forward()
        envelope = self.ledger.close_with_evidence("elephant-db", "measurement", "d" * 64, "e" * 64)
        self.assertEqual(envelope["close"]["verdict"], "passed")
        self.assertEqual(envelope["close"]["physical_dispatch_reservations"], 1)
        self.assertEqual(envelope["close"]["per_request_association"], "unmet")
        self.assertEqual(envelope, self.ledger.close_with_evidence("elephant-db", "measurement",
                                                                 "d" * 64, "e" * 64))
        index = json.loads((self.directory / "accounting-export" / envelope["export"]["path"]).read_bytes())
        self.assertEqual(len(index["files"]), 13)
        self.assertEqual(self.ledger.receipt()["reserved_nano_usd"], 2 * 1064960)

    def test_phase_and_whole_deadlines_never_restart(self):
        self.measurement()
        _, request = self.request()
        self.wall += 120
        self.mono += 120
        with self.assertRaisesRegex(Denied, "cohort_deadline"):
            self.ledger.reserve(request, lambda: None)
        self.wall, self.mono = 8800.0, 8300.0
        with self.ledger.transaction(), self.assertRaisesRegex(Denied, "run_time_limit"):
            self.ledger.check_time()

    def test_new_identity_cannot_default_to_old_ledger_or_resume_with_fresh_phase(self):
        with self.assertRaisesRegex(Denied, "cohort_profile_required"):
            CohortLedger(Path(self.temp.name) / "wrong", "c" * 64, create=True,
                         identity=self.identity, scopes=self.scopes)
        self.ledger.begin("elephant-db", "preparation", "b" * 64, lambda: None)
        self.ledger.close()
        self.ledger = None
        with self.assertRaisesRegex(Denied, "run_halted"):
            CohortLedger(self.directory, "a" * 64, create=False, identity=self.identity,
                         scopes=self.scopes, profile_id="semantic140-v1",
                         now=lambda: (self.wall, self.mono, "offline-boot"))
        with self.assertRaisesRegex(Denied, "accounting_mode_mismatch"):
            Ledger(self.directory, "a" * 64, create=False)


class Semantic140PolicyTests(NoExternalEffects):
    def test_inactive_example_refuses_before_missing_approval_or_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_bytes((Path(__file__).parent / "manifest.semantic140.example.json").read_bytes())
            path.chmod(0o600)
            with self.assertRaisesRegex(Denied, "live_not_ready"):
                Semantic140Policy(path, Path(directory) / "no-approval-or-key")

    def fixture(self, root, profile):
        prefix = "semantic140" if profile == SEMANTIC140 else "workload"
        source = Path(__file__).resolve().parents[2] / "recall_preview"
        manifest = json.loads((Path(__file__).parent / "manifest.cohort.example.json").read_bytes())
        manifest.update(format=profile.manifest_format, live_ready=True,
                        run_id="offline-synthetic-run", run_directory=str(root / "run"), account_id="offline",
                        accounting_policy=profile.accounting_policy,
                        allocation_sha256=profile.allocation_sha256, contract_sha256=profile.contract_sha256,
                        corpus_sha256=profile.corpus_sha256, upstream_ips=["1.1.1.1"])
        if profile == SEMANTIC140:
            manifest["profile_id"] = "semantic140-v1"
        for name in ("artifact_sha256", "configuration_sha256", "runtime_sha256", "gateway_sha256"):
            manifest[name] = "a" * 64
        for name, filename in (("allocation", f"{prefix}-allocation-proposal.json"),
                               ("preparation_contract", f"{prefix}-preparation-contract.json"),
                               ("typed_delta", f"{prefix}-allocation-typed-delta.json")):
            target = root / filename
            target.write_bytes((source / filename).read_bytes())
            target.chmod(0o600)
            manifest[name + "_path"] = str(target)
            manifest[name + "_sha256"] = digest(target.read_bytes())
        manifest["bindings"] = [{"id": scope.binding_id, "backend": scope.backend, "space": scope.space,
                                 "operation": "embedding", "model": scope.model, "dimensions": scope.dimensions,
                                 "connection_id": scope.connection_id, "profile_id": scope.profile_id}
                                for scope in scopes().values()]
        approval = {"format": "recall-exclusive-embedding-approval/v1", "authorized": True,
                    "expires_unix": int(time.time()) + 3600, "user_approval_id": "offline-no-spending"}
        for name in ("bound", "isolation", "binding", "capture_scope", "cohort", "exclusive_run"):
            approval[name + "_review_sha256"] = "b" * 64
        manifest_path, approval_path = root / "manifest.json", root / "approval.json"
        manifest_path.write_bytes(encode(manifest))
        approval["manifest_sha256"] = digest(manifest_path.read_bytes())
        approval_path.write_bytes(encode(approval))
        manifest_path.chmod(0o600)
        approval_path.chmod(0o600)
        return manifest_path, approval_path, manifest

    def test_exact_original_and_successor_files_select_only_their_policy(self):
        for profile, policy_type, wrong in ((D28, CohortPolicy, Semantic140Policy),
                                            (SEMANTIC140, Semantic140Policy, CohortPolicy)):
            with tempfile.TemporaryDirectory() as directory:
                path, approval, _ = self.fixture(Path(directory), profile)
                policy = policy_type(path, approval)
                self.assertEqual(policy.profile, profile)
                self.assertEqual(policy.worker_environment()["RECALL_GATEWAY_MAX_CONNECTIONS"],
                                 str(profile.connection_slots))
                with self.assertRaises(Denied):
                    wrong(path, approval)

    def test_unknown_profile_quota_override_and_mixed_allocation_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            path, approval, original = self.fixture(Path(directory), SEMANTIC140)
            for mutate in (lambda m: m.pop("profile_id"), lambda m: m.update(profile_id="d28-v1"),
                           lambda m: m.update(physical_dispatch_cap=4261),
                           lambda m: m.update(allocation_sha256=D28.allocation_sha256),
                           lambda m: m.update(corpus_sha256=D28.corpus_sha256)):
                value = copy.deepcopy(original)
                mutate(value)
                path.write_bytes(encode(value))
                with self.subTest(manifest=value), self.assertRaises(Denied):
                    Semantic140Policy(path, approval)

    def test_control_keeps_expired_closure_available_but_requires_exact_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            path, approval, _ = self.fixture(Path(directory), SEMANTIC140)
            policy = Semantic140Policy(path, approval)
            phase = {"execution_authorized": True, "provider_dispatch_authorized": True,
                     "profile_id": "semantic140-v1", "backend": "elephant-db", "binary_sha256": "a" * 64,
                     "provider_allocation_sha256": SEMANTIC140.allocation_sha256, **successor_identity()}
            with patch.dict("os.environ", {"RECALL_GATEWAY_MANIFEST_SHA256": policy.manifest_hash}, clear=True):
                payload = _payload(phase, {}, "close")
            payload["observation_sha256"] = "c" * 64
            closed = []
            ledger = SimpleNamespace(close_cohort=lambda *args: closed.append(args) or {"verdict": "unmet"})
            # Original approval bytes are now revoked. Closure still settles the
            # original reservation without reopening admission or a new clock.
            approval.write_bytes(b'{}')
            self.assertEqual(policy.control(payload, ledger), {"verdict": "unmet"})
            self.assertEqual(len(closed), 1)
            for value in (None, "d28-v1"):
                wrong = dict(payload)
                if value is None:
                    wrong.pop("profile_id")
                else:
                    wrong["profile_id"] = value
                with self.assertRaises(Denied):
                    policy.control(wrong, ledger)
            self.assertEqual(len(closed), 1)


if __name__ == "__main__":
    unittest.main()
