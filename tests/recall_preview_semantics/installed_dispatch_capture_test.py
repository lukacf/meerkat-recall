"""Pure dispatch custody regressions: real journal files, no child or network."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

SOURCE_ROOT = Path(__file__).resolve().parents[2]


class DispatchCapture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "dispatch_capture_subject", SOURCE_ROOT / "scripts/qualification/installed_linux_dispatch.py")
        cls.subject = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.subject)
        cls.core = cls.subject.core
        cls.producer = cls.core.process_capture

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ("captures", "service"):
            (self.root / name).mkdir()
        for target, name in ((self.core.subprocess, "Popen"), (self.core.os, "killpg"),
                             (self.core.os, "waitid"), (self.core.socket, "socket"),
                             (self.core.threading, "Thread")):
            guard = patch.object(target, name, side_effect=AssertionError("unexpected process/network operation"))
            guard.start()
            self.addCleanup(guard.stop)
        self.inventory = self.subject.plan()
        self.manifest = {"format": "recall-installed-linux-dispatch-run/v1",
                         "expires_at": "2999-01-01T00:00:00Z"}
        review = self.root / "review.json"
        review.write_bytes(b'{"reviewed":"original-dispatch"}\n')
        self.manifest.update(expectation_review=str(review),
                             expectation_review_sha256=self.core.file_digest(review))
        self.original = json.dumps(self.manifest, indent=3).encode() + b"\n"
        value = self.subject.DispatchDriver.__new__(self.subject.DispatchDriver)
        value.root, value.manifest, value.inventory = self.root, self.manifest, self.inventory
        value.original_manifest_raw = self.original
        value.cases = self.inventory["expectations"]
        value.process_journal = None
        value.allocation_deadline_ns = time.monotonic_ns() + 60 * 10**9
        value.allocation_clock = {"fixture": "no physical process"}
        value.started = time.monotonic()
        value.serial = value.requests = value.capture_bytes = 0
        value.child, value.owned_commands, value.case_proofs = None, [], []
        value.capture_budget_lock = threading.RLock()
        value.report = {"cases": [], "product_acceptance": False}
        value.stop = Mock()
        self.value = value

        def controller(journal, deadline_ns, *, allocation_clock):
            self.assertEqual(deadline_ns, value.allocation_deadline_ns)
            self.assertEqual(allocation_clock, value.allocation_clock)
            return journal.save("captures/controller-fixture.json", b'{"fixture":true}', "test-controller")

        guard = patch.object(self.producer, "capture_controller", side_effect=controller)
        self.controller = guard.start()
        self.addCleanup(guard.stop)

    def read(self, name):
        return json.loads((self.root / "captures" / name).read_bytes())

    def test_dispatch_run_persists_exact_manifest_cases_and_cleanup(self):
        self.value.execute = Mock()
        self.assertEqual(self.value.run(), 0)
        self.value.execute.assert_called_once_with()
        self.assertEqual((self.root / "captures/original-manifest.json").read_bytes(), self.original)
        self.assertEqual((self.root / "captures/original-cases.json").read_bytes(), self.subject.CASES.read_bytes())
        cleanup = self.read("process-cleanup.json")
        self.assertIs(cleanup["capture_complete"], True)
        self.assertIsNone(cleanup["remaining_child_pid"])
        self.assertEqual(cleanup["remaining_command_pids"], [])
        self.assertEqual(cleanup["original_deadline_ns"], self.value.allocation_deadline_ns)
        self.assertEqual(cleanup["original_expires_at"], self.manifest["expires_at"])
        self.assertEqual(self.read("process-journal.json")["attempted_processes"], 0)
        evidence = self.read("process-evidence.json")
        self.assertIsNotNone(evidence["reviewed_plan"])
        self.assertIsNotNone(evidence["cleanup"])
        self.assertFalse(self.value.report["product_acceptance"])

    def test_missing_original_manifest_stops_before_dispatch(self):
        self.value.original_manifest_raw = None
        self.value.execute = Mock()
        self.assertEqual(self.value.run(), 1)
        self.value.execute.assert_not_called()
        self.assertEqual(self.value.report["subset_observation"], "failed")
        self.assertIn("original validated manifest bytes", self.value.report["failure"])
        self.assertFalse((self.root / "captures/original-manifest.json").exists())

    def test_changed_original_manifest_is_rejected(self):
        changed = dict(self.manifest, expires_at="2998-01-01T00:00:00Z")
        self.value.original_manifest_raw = json.dumps(changed).encode()
        with self.assertRaisesRegex(ValueError, "original validated manifest bytes"):
            self.value.begin_process_capture()

    def test_basic_case_file_cannot_stand_in_for_dispatch(self):
        self.value.process_capture_cases = self.core.CASES
        with self.assertRaisesRegex(ValueError, "original installed literal inputs changed"):
            self.value.begin_process_capture()
        self.assertFalse((self.root / "captures/original-cases.json").exists())

    def test_unreaped_original_retains_failed_capture_and_run(self):
        def unfinished():
            self.value.serial = 1
            proof = Mock()
            proof.snapshot.return_value = {"index": 1, "reaped": False, "errors": [], "logs": []}
            self.value.process_journal.processes.append(proof)

        self.value.execute = unfinished
        self.assertEqual(self.value.run(), 1)
        self.assertIs(self.read("process-cleanup.json")["capture_complete"], False)
        self.assertEqual(self.read("process-cleanup.json")["reaped_indices"], [])
        self.assertEqual(self.value.report["subset_observation"], "failed")
        self.assertEqual(self.value.report["process_capture_error_type"], "ValueError")

    def test_shared_initializer_preserves_basic_and_darwin_scope(self):
        self.assertEqual(self.core.Driver.process_capture_manifest_format, "recall-installed-linux-run/v1")
        self.assertEqual(self.core.Driver.process_capture_cases, self.core.CASES)
        self.value.expected_target = "aarch64-apple-darwin"
        self.value.begin_process_capture()
        self.controller.assert_not_called()
        self.assertIsNone(self.value.process_journal)

    def test_dispatch_constructor_forwards_original_bytes(self):
        observed = {}

        def initialize(instance, manifest, inventory, isolation, *, original_manifest_raw=None):
            observed["raw"] = original_manifest_raw
            instance.report = {}

        with patch.object(self.core.Driver, "__init__", initialize), \
             patch.object(self.subject, "TransportSpy", return_value=object()):
            self.subject.DispatchDriver(self.manifest, self.inventory, {}, original_manifest_raw=self.original)
        self.assertIs(observed["raw"], self.original)

    def test_entrypoint_forwards_the_validated_manifest_file_bytes(self):
        original_path = self.root / "allocated-manifest.json"
        original_path.write_bytes(self.original)
        argv = ["dispatch", "run", "--manifest", str(original_path), "--execute-reviewed",
                "--parent-network-namespace", "net:[fixture]"]
        driver = Mock()
        driver.run.return_value = 0
        with patch.object(sys, "argv", argv), \
             patch.object(self.subject.sys, "version_info", (3, 12)), \
             patch.object(self.subject, "validate_manifest", return_value=(self.manifest, self.inventory)), \
             patch.object(self.core, "offline_namespace", return_value={"fixture": True}), \
             patch.object(self.core, "enable_loopback"), \
             patch.object(self.subject, "DispatchDriver", return_value=driver) as construct:
            self.assertEqual(self.subject.main(), 0)
        construct.assert_called_once_with(self.manifest, self.inventory, {"fixture": True},
                                          original_manifest_raw=self.original)
        driver.run.assert_called_once_with()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    options, remaining = parser.parse_known_args()
    SOURCE_ROOT = options.source_root.resolve(strict=True)
    unittest.main(argv=[sys.argv[0], *remaining])
