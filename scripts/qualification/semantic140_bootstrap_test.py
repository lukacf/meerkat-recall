"""Closed successor startup controls using the original no-execution fixture."""
import asyncio
from types import SimpleNamespace
import unittest

import d28_runtime_bootstrap as adapter
import d28_runtime_bootstrap_test as fixtures
import runtime_package_bootstrap as bootstrap
import runtime_package_bootstrap_contract as contract


class Semantic140StartupControls(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.Controls("test_input_expectations_are_snapshotted")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.expected = {"allocation_sha256": contract.SEMANTIC140_ALLOCATION_SHA256,
                         "profile_id": "semantic140-v1", "format": "recall-gateway-launch-anchor/v2"}

    def test_new_profile_requires_exact_allocation_and_anchor_before_startup(self):
        phase = adapter.phase_from_original(self.fixture.reference, self.expected)
        self.assertEqual(phase["review"]["expected_launch"], self.expected)
        for updates in ({"profile_id": "d28-v1"}, {"format": "recall-gateway-launch-anchor/v1"},
                        {"allocation_sha256": contract.D28_ALLOCATION_SHA256},
                        {"allocation_sha256": "0" * 64}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                adapter.phase_from_original(self.fixture.reference, dict(self.expected, **updates))
        candidate = dict(self.expected)
        del candidate["profile_id"]
        with self.assertRaises(ValueError):
            adapter.phase_from_original(self.fixture.reference, candidate)
        self.fixture.constructor.assert_not_called()

    def test_successor_preserves_original_clock_and_positive_owned_cleanup(self):
        async def collect():
            async with adapter.reviewed_d28_startup(self.fixture.reference, self.expected) as live:
                allocation = live.instance.allocation
                self.assertEqual(allocation.started_ns, 1000 * 10**9)
                self.assertEqual(allocation.deadline_ns, 8800 * 10**9)
                self.assertEqual(allocation.whole_deadline_ns, 8800 * 10**9)
                self.assertEqual(allocation.maximum_seconds, 7800)
                self.assertFalse(live.receipt["qualified"])
            self.assertTrue(live.closed)
            self.assertIsNotNone(live.cleanup_reference)
        asyncio.run(collect())
        self.assertEqual(self.fixture.context_call.call_args.kwargs,
                         {"allocation_sha256": contract.SEMANTIC140_ALLOCATION_SHA256})
        self.assertEqual(self.fixture.events[-1], "cleanup")
        self.assertTrue(self.fixture.terminal()["cleanup_complete"])

    def test_caller_failure_still_closes_original_startup_without_second_context(self):
        async def collect():
            with self.assertRaisesRegex(RuntimeError, "collector failed"):
                async with adapter.reviewed_d28_startup(self.fixture.reference, self.expected):
                    raise RuntimeError("collector failed")
        asyncio.run(collect())
        self.assertEqual(self.fixture.events[-1], "cleanup")
        self.assertEqual(len(self.fixture.instances), 1)
        self.assertTrue(self.fixture.terminal()["cleanup_complete"])
        self.assertEqual(self.fixture.terminal()["status"], "incomplete")

    def test_launch_expectations_refuse_old_caps_and_unreviewed_extra_fields(self):
        gateway = SimpleNamespace(HASH_FIELDS={"manifest_sha256", "configuration_sha256", "artifact_sha256"},
                                  ANCHOR_FORMAT="recall-gateway-launch-anchor/v1")
        expected = {**self.expected, "artifact_sha256": "a" * 64, "source_commit": "b" * 40,
                    "run_nonce": "original-allocation-1234",
                    "caps": {"physical_dispatches": 4260, "maximum_nano_usd": 5_000_000_000}}
        self.assertEqual(bootstrap.launch_expectations(gateway, expected, contract.SEMANTIC140_ALLOCATION_SHA256), expected)
        candidates = [dict(expected, caps={"physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000}),
                      dict(expected, caps={"physical_dispatches": 4261, "maximum_nano_usd": 5_000_000_000}),
                      dict(expected, profile_helper_sha256="c" * 64),
                      dict(expected, profile_id="d28-v1")]
        for candidate in candidates:
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                bootstrap.launch_expectations(gateway, candidate, contract.SEMANTIC140_ALLOCATION_SHA256)
        with self.assertRaises(ValueError):
            bootstrap.launch_expectations(gateway, expected, contract.D28_ALLOCATION_SHA256)


if __name__ == "__main__":
    unittest.main()
