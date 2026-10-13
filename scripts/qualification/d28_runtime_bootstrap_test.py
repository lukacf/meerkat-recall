"""Closed-world D28 startup controls. No real process, service or socket."""
from __future__ import annotations

import asyncio
from contextlib import ExitStack
import copy
from pathlib import Path
import unittest
from unittest import mock
from types import SimpleNamespace

import d28_runtime_bootstrap as adapter
import runtime_package_bootstrap_contract as contract
import runtime_package_bootstrap_test as fixtures


class Controls(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for target in ("subprocess.Popen", "socket.create_connection", "asyncio.open_connection"):
            self.stack.enter_context(mock.patch(target, side_effect=AssertionError("execution forbidden")))
        self.plan = fixtures.plan()
        self.plan["dependencies"][adapter.SELF] = "1" * 64
        self.plan["dependencies"][adapter.EVIDENCE_SELF] = "2" * 64
        self.plan_raw = contract.encode(self.plan)
        self.reference = {"path": "/inputs/bootstrap-plan.json", "sha256": contract.sha(self.plan_raw)}
        self.expected = {"allocation_sha256": contract.D28_ALLOCATION_SHA256}
        self.stack.enter_context(mock.patch.object(adapter, "static_input", return_value=self.plan_raw))
        self.context = {"anchor": {"clock": {"mono_start": 1000.0, "deadline_mono": 8800.0,
            "time_cap_seconds": 7800, "deadline_wall": 2524608000.0}}}
        self.context_reference = {"path": "/work/context.json", "sha256": contract.sha(contract.encode(self.context))}
        self.gateway = SimpleNamespace(read_context=mock.Mock(return_value=self.context))
        self.context_call = self.stack.enter_context(mock.patch.object(adapter, "actual_context",
            return_value=(self.gateway, self.context, contract.encode(self.context), self.context_reference)))
        self.stack.enter_context(mock.patch.object(adapter, "load_helpers", return_value=object()))
        self.stack.enter_context(mock.patch.object(adapter, "ensure_ports_absent"))
        self.stack.enter_context(mock.patch.object(adapter, "verify_isolation"))
        self.custody_capture = self.stack.enter_context(mock.patch.object(adapter, "capture_live_startup",
            return_value={"raw_evidence": {}, "references": {}}))
        self.stack.enter_context(mock.patch.object(adapter, "read_bound", return_value=b"{}"))
        self.stack.enter_context(mock.patch("time.monotonic_ns", return_value=2000 * 10**9))
        self.saved = {}
        def private_new(path, raw):
            key = str(path)
            if key in self.saved:
                raise FileExistsError("original evidence must not be rewritten")
            self.saved[key] = raw
        self.stack.enter_context(mock.patch.object(adapter, "private_new", side_effect=private_new))
        self.events, self.instances = [], []
        self.failure_stage = None
        self.failures = []
        self.identity_changed = False
        def instance(plan, plan_raw, context, context_raw, allocation, wire, gateway):
            result = fixtures.receipt(plan)
            result["original_deadline_monotonic_ns"] = allocation.deadline_ns
            result["whole_deadline_monotonic_ns"] = allocation.whole_deadline_ns
            result["allocation_started_monotonic_ns"] = allocation.started_ns
            root = Path(plan["run_directory"])
            async def stage(name):
                self.events.append(name)
                if self.failure_stage == name:
                    raise RuntimeError("mocked startup failure")
            async def cleanup():
                self.events.append("cleanup")
                return list(self.failures)
            def identity(*_):
                value = dict(result["processes"]["recall"])
                if self.identity_changed:
                    value["start_ticks"] += 1
                return value
            obj = SimpleNamespace(root=root, context=context, allocation=allocation, recall=object(), paired=None,
                credentials=lambda: stage("credentials"), services=lambda: stage("services"),
                spaces=lambda: stage("spaces"), receipt=lambda _: copy.deepcopy(result),
                processes=SimpleNamespace(cleanup=cleanup, identity=identity, records=[]))
            self.instances.append(obj)
            return obj
        self.constructor = self.stack.enter_context(mock.patch.object(adapter, "Bootstrap", side_effect=instance))

    def terminal(self):
        return contract.strict_json(self.saved[self.plan["run_directory"] + "/cleanup.json"])

    def test_original_plan_hash_and_explicit_allocation_refuse_before_start(self):
        for reference, expected in (({**self.reference, "sha256": "f" * 64}, self.expected),
                                    (self.reference, {"allocation_sha256": None}),
                                    ({**self.reference, "approved": True}, self.expected)):
            with self.subTest(reference=reference, expected=expected), self.assertRaises(ValueError):
                adapter.phase_from_original(reference, expected)
        self.constructor.assert_not_called()
        self.context_call.assert_not_called()

    def test_adapter_must_be_in_original_transitive_helper_inventory(self):
        plan = copy.deepcopy(self.plan)
        del plan["dependencies"][adapter.SELF]
        raw = contract.encode(plan)
        with mock.patch.object(adapter, "static_input", return_value=raw), self.assertRaises(ValueError):
            adapter.phase_from_original({**self.reference, "sha256": contract.sha(raw)}, self.expected)

    def test_input_expectations_are_snapshotted(self):
        expected = {**self.expected, "nested": {"source": "original"}}
        phase = adapter.phase_from_original(self.reference, expected)
        expected["nested"]["source"] = "changed"
        self.assertEqual(phase["review"]["expected_launch"]["nested"]["source"], "original")

    def test_live_handoff_preserves_original_clock_receipt_and_cleanup(self):
        async def run():
            async with adapter.reviewed_d28_startup(self.reference, self.expected) as live:
                self.events.append("consumer")
                self.assertEqual(self.events, ["credentials", "services", "spaces", "consumer"])
                allocation = live.instance.allocation
                self.assertEqual((allocation.started_ns, allocation.deadline_ns, allocation.whole_deadline_ns),
                                 (1000 * 10**9, 8800 * 10**9, 8800 * 10**9))
                self.assertEqual(allocation.maximum_seconds, 7800)
                self.assertFalse(allocation.bootstrap)
                self.assertFalse(live.receipt["qualified"])
                self.assertFalse(live.receipt["product_acceptance"])
                live.receipt["qualified"] = True
                self.assertFalse(live.receipt["qualified"])
                self.assertIsNone(live.cleanup_reference)
                self.assertEqual(live.custody_sequence, 1)
                self.assertEqual(live.custody, {"raw_evidence": {}, "references": {}})
                original = live.receipt_raw
            self.assertTrue(live.closed)
            self.assertEqual(self.saved[live.receipt_reference["path"]], original)
            self.assertEqual(contract.sha(self.saved[live.cleanup_reference["path"]]), live.cleanup_reference["sha256"])
            with self.assertRaises(ValueError):
                live.revalidate_custody()
        asyncio.run(run())
        self.assertEqual(self.events[-1], "cleanup")
        self.assertEqual(self.terminal()["status"], "complete")
        self.assertIsNone(self.terminal()["collector_process_index"])
        self.assertEqual(self.context_call.call_args.kwargs, {"allocation_sha256": contract.D28_ALLOCATION_SHA256})
        for call in self.gateway.read_context.call_args_list:
            self.assertTrue(call.kwargs["immediate_parent"])

    def test_startup_failure_never_yields_and_cleans_original_handles(self):
        self.failure_stage = "services"
        async def run():
            async with adapter.reviewed_d28_startup(self.reference, self.expected):
                self.fail("failed startup yielded")
        with self.assertRaises(RuntimeError):
            asyncio.run(run())
        self.assertEqual(self.events, ["credentials", "services", "cleanup"])
        self.assertEqual(self.terminal()["status"], "incomplete")
        self.assertTrue(self.terminal()["cleanup_complete"])
        self.assertNotIn(self.plan["run_directory"] + "/runtime-bootstrap.json", self.saved)

    def test_consumer_failure_keeps_startup_receipt_immutable_and_blocks_next_phase(self):
        async def run():
            async with adapter.reviewed_d28_startup(self.reference, self.expected) as live:
                original = live.receipt_raw
                self.assertEqual(self.saved[live.receipt_reference["path"]], original)
                raise RuntimeError("consumer failed")
            self.fail("next backend began after failed consumer")
        with self.assertRaises(RuntimeError):
            asyncio.run(run())
        self.assertEqual(self.terminal()["error_type"], "RuntimeError")
        self.assertEqual(self.terminal()["status"], "incomplete")
        self.assertEqual(self.events.count("cleanup"), 1)

    def test_cancelled_consumer_still_cleans(self):
        async def run():
            async with adapter.reviewed_d28_startup(self.reference, self.expected):
                raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(run())
        self.assertEqual(self.terminal()["error_type"], "CancelledError")
        self.assertEqual(self.events.count("cleanup"), 1)

    def test_cleanup_failure_records_incomplete_and_prevents_next_backend(self):
        self.failures = [{"index": 1, "stage": "owned-final-reap", "error_type": "UnreapedOwnedChild"}]
        async def run():
            async with adapter.reviewed_d28_startup(self.reference, self.expected):
                pass
            self.fail("next backend began despite incomplete cleanup")
        with self.assertRaises(ValueError):
            asyncio.run(run())
        self.assertFalse(self.terminal()["cleanup_complete"])
        self.assertEqual(self.terminal()["cleanup_failures"], self.failures)
        self.assertEqual(self.terminal()["status"], "incomplete")

    def test_changed_service_identity_after_consumer_refuses_and_cleans(self):
        async def run():
            async with adapter.reviewed_d28_startup(self.reference, self.expected):
                self.identity_changed = True
        with self.assertRaises(ValueError):
            asyncio.run(run())
        self.assertEqual(self.events[-1], "cleanup")
        self.assertEqual(self.terminal()["error_type"], "ValueError")

    def test_expired_original_clock_refuses_without_startup(self):
        with mock.patch("time.monotonic_ns", return_value=9000 * 10**9):
            async def run():
                async with adapter.reviewed_d28_startup(self.reference, self.expected):
                    self.fail("expired original allocation yielded")
            with self.assertRaises(ValueError):
                asyncio.run(run())
        self.constructor.assert_not_called()

    def test_original_byte_loader_preserves_whitespace_and_never_reads_tokens(self):
        async def run():
            async with adapter.reviewed_d28_startup(self.reference, self.expected) as live:
                original = b'{ "original" : true }\n'
                paths = [live.receipt_reference, {"path": "/work/original.capture", "sha256": contract.sha(original)}]
                live.original_references = paths
                source = {live.receipt_reference["path"]: live.receipt_raw, "/work/original.capture": original}
                with mock.patch.object(adapter, "read_bound", side_effect=lambda path, *_args, **_kwargs: source[str(path)]) as read:
                    self.assertEqual(live.original_bytes(), source)
                self.assertEqual([call.args[0] for call in read.call_args_list], [row["path"] for row in paths])
                source["/work/original.capture"] = b'{"original":true}'
                with mock.patch.object(adapter, "read_bound", side_effect=lambda path, *_args, **_kwargs: source[str(path)]), \
                     self.assertRaises(ValueError):
                    live.original_bytes()
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
