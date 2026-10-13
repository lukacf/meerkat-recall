#!/usr/bin/env python3
"""Offline mocked controls. Never execute a candidate or open a socket."""
import ctypes
import datetime as dt
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import sys
import tarfile
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("darwin_test_driver", ROOT / "scripts/qualification/darwin_offline_native.py")
driver = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = driver
spec.loader.exec_module(driver)


class DarwinOfflineControls(unittest.TestCase):
    def setUp(self):
        self.no_process = patch.object(driver.subprocess, "Popen", side_effect=AssertionError("candidate execution forbidden"))
        self.no_socket = patch.object(driver.socket, "socket", side_effect=AssertionError("socket forbidden"))
        self.no_process.start()
        self.no_socket.start()
        self.addCleanup(self.no_process.stop)
        self.addCleanup(self.no_socket.stop)

    def test_inert_plan_reuses_exact_case_inventory_and_transitive_digests(self):
        value = driver.plan()
        self.assertEqual(value["expectations"], driver.core.plan()["expectations"])
        self.assertFalse(value["product_acceptance"])
        self.assertEqual(set(value["dependency_sha256"]), set(driver.DEPENDENCIES))
        self.assertIs(driver.DarwinDriver.credentials, driver.core.Driver.credentials)
        self.assertIs(driver.DarwinDriver.http, driver.core.Driver.http)
        self.assertEqual(driver.DarwinDriver.expected_target, "aarch64-apple-darwin")

    def test_non_mac_host_is_unmet_before_candidate_or_socket(self):
        with patch.object(driver.sys, "platform", "linux"):
            with self.assertRaises(driver.UnmetPrerequisite):
                driver.host_prerequisites({})

    def test_version_comparison_and_malformed_evidence(self):
        self.assertEqual(driver.version("26.5"), (26, 5, 0))
        self.assertLessEqual(driver.version("26.5"), driver.version("26.5.2"))
        for value in ("26", "26.5beta", "", "26.5.2.0", 26):
            with self.subTest(value=value), self.assertRaises(ValueError):
                driver.version(value)

    def test_receiving_os_and_each_actual_link_minimum_bind_to_trusted_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            interpreter = root / "reviewed-python"
            interpreter.write_bytes(b"synthetic interpreter identity; never executable")
            receipt_path = root / "compatibility.json"
            receipt = {"format": "recall-darwin-compatibility/v1", "source_commit": "a" * 40,
                "archive_sha256": "b" * 64, "binary_sha256": "c" * 64, "architecture": "arm64",
                "receiver_os_version": "26.5.2", "binary_min_os": "26.5", "onnx_min_os": "14.0",
                "binary_load_commands_sha256": "d" * 64, "onnx_archive_sha256": "e" * 64,
                "onnx_load_commands_sha256": "f" * 64}
            manifest = {name: receipt[name] for name in ("source_commit", "archive_sha256", "binary_sha256")}
            manifest.update(python=str(interpreter), python_sha256=driver.core.file_digest(interpreter),
                            compatibility=str(receipt_path), host_os_version="26.5.2")
            is_file = Path.is_file
            with patch.object(driver.sys, "platform", "darwin"), \
                 patch.object(driver.sys, "executable", str(interpreter)), \
                 patch.object(driver.platform, "machine", return_value="arm64"), \
                 patch.object(driver.platform, "mac_ver", return_value=("26.5.2", (), "")), \
                 patch.object(driver.select, "kqueue", Mock(), create=True), \
                 patch.object(Path, "is_file", lambda path: str(path) == "/usr/bin/sandbox-exec" or is_file(path)):
                for field, value in ((None, None), ("binary_min_os", "26.5.1"), ("onnx_min_os", "27.0"),
                                     ("binary_sha256", "0" * 64), ("receiver_os_version", "26.4")):
                    selected = receipt if field is None else {**receipt, field: value}
                    receipt_path.write_text(json.dumps(selected))
                    manifest["compatibility_sha256"] = driver.core.file_digest(receipt_path)
                    if field is None:
                        self.assertEqual(driver.host_prerequisites(manifest)["macos"], "26.5.2")
                    else:
                        with self.subTest(field=field), self.assertRaises(ValueError):
                            driver.host_prerequisites(manifest)

    def test_profile_exact_endpoint_and_owned_write_roots_without_ambient_home(self):
        result = driver.sandbox_profile(Path("/task/run"), 18181, Path("/task/run/installed/meerkat-recall"))
        self.assertIn("(deny default)", result)
        self.assertIn('(local ip "localhost:18181")', result)
        self.assertNotIn("network-outbound", result)
        self.assertNotIn("process-fork", result)
        self.assertNotIn("mach-lookup", result)
        self.assertNotIn("/Users", result)
        self.assertNotIn("/home", result)
        writes = next(line for line in result.splitlines() if line.startswith("(allow file-write"))
        for value in ("private", "service", "backups", "media", "recovery", "tmp"):
            self.assertIn('"/task/run/' + value + '"', writes)
        for value in ("installed", "captures", "candidate.sb", "denied-control"):
            self.assertNotIn(value, writes)
        self.assertNotIn('(subpath "/task/run")', result)

    def test_profile_paths_cannot_inject_rules_and_port_is_not_configurable_text(self):
        value = driver.sandbox_profile(Path('/task/quote"x'), 1, Path('/task/bin"x'))
        self.assertIn('bin\\"x', value)
        for port in (0, 65536, "1234", True):
            with self.subTest(port=port), self.assertRaises(ValueError):
                driver.sandbox_profile(Path("/task"), port, Path("/bin/recall"))
        with self.assertRaises(ValueError):
            driver.sandbox_profile(Path("/task\n(allow default)"), 1, Path("/bin/recall"))

    def allocation(self, root):
        inventory = driver.plan()
        review = {"format": "recall-darwin-native-review/v1", "reviewer": "qualification", "approved": True}
        for key in ("platform_expectations_sha256", "driver_sha256", "dependency_sha256"):
            review[key] = inventory[key]
        review_path = root / "review.json"
        review_path.write_text(json.dumps(review))
        manifest = {"format": "recall-darwin-native-run/v1", "execution_authorized": True,
            "expires_at": "2026-10-07T03:00:00Z", "backend": "elephant-db", "source_commit": "a" * 40,
            "archive_sha256": "b" * 64, "binary_sha256": "c" * 64, "python_sha256": "d" * 64,
            "compatibility_sha256": "e" * 64, "review_sha256": driver.core.file_digest(review_path),
            "review": str(review_path), "archive": str(root / "artifact.tgz"), "run_directory": str(root / "new"),
            "python": str(root / "python"), "compatibility": str(root / "compatibility.json")}
        return manifest, review, review_path

    def test_allocation_review_changes_expiry_and_missing_authority_refuse(self):
        now = dt.datetime(2026, 10, 7, 2, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, review, review_path = self.allocation(root)
            path = root / "run.json"
            path.write_text(json.dumps(manifest))
            checked, _ = driver.validate_allocation(path, now)
            self.assertEqual(checked, manifest)
            for key, value in (("execution_authorized", False), ("expires_at", "2026-10-07T01:00:00Z"),
                               ("archive_sha256", "bad"), ("backend", "remote"), ("review", "relative")):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    path.write_text(json.dumps({**manifest, key: value}))
                    driver.validate_allocation(path, now)
            path.write_text(json.dumps(manifest))
            review["dependency_sha256"]["scripts/qualification/installed_linux_offline.py"] = "f" * 64
            review_path.write_text(json.dumps(review))
            manifest["review_sha256"] = driver.core.file_digest(review_path)
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "dependency_sha256"):
                driver.validate_allocation(path, now)

    def test_archive_is_bounded_before_shared_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "fixture.tgz"
            with tarfile.open(archive, "w:gz") as stream:
                item = tarfile.TarInfo("synthetic")
                item.size = 32
                stream.addfile(item, io.BytesIO(b"x" * 32))
            allocation = {"archive": str(archive), "archive_sha256": driver.core.file_digest(archive)}
            limits = {"archive_bytes": 1024, "archive_expanded_bytes": 64, "filesystem_entries": 10}
            self.assertEqual(driver.archive_bounds(allocation, limits), {"archive_entries": 1, "declared_expanded_bytes": 32})
            for key, value in (("archive_bytes", 1), ("archive_expanded_bytes", 31), ("filesystem_entries", 0)):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    driver.archive_bounds(allocation, {**limits, key: value})
            self.assertEqual(list(Path(temporary).iterdir()), [archive])

    def process_stub(self):
        child = object.__new__(driver.DarwinProcess)
        child.pid = 901
        child.group_lock = threading.Lock()
        child.reaped = False
        child.exited = False
        child.failed = threading.Event()
        child.threads = []
        child.process = Mock()
        child.process.poll.side_effect = AssertionError("Popen.poll reaps too soon")
        child.process.returncode = None
        child.events = Mock()
        return child

    def test_nonreaping_poll_uses_only_registered_exit_event(self):
        child = self.process_stub()
        child.events.control.return_value = []
        with patch.object(driver.select, "KQ_EV_ERROR", 1, create=True), \
             patch.object(driver.select, "KQ_NOTE_EXIT", 2, create=True):
            self.assertIsNone(child.poll())
            child.events.control.return_value = [SimpleNamespace(ident=901, flags=0, fflags=2)]
            self.assertEqual(child.poll(), 0)
            self.assertEqual(child.poll(), 0)
        child.process.wait.assert_not_called()
        child.process.poll.assert_not_called()

    def test_final_group_cleanup_precedes_reap_and_no_signal_after_reap(self):
        child = self.process_stub()
        child.exited = True
        calls = []
        child.process.wait.side_effect = lambda **_kwargs: calls.append("reap") or 7
        with patch.object(driver.os, "killpg", side_effect=lambda pid, sig: calls.append((pid, sig))):
            self.assertEqual(child.wait(1), 7)
            child.signal(signal.SIGKILL)
        self.assertEqual(calls, [(901, signal.SIGKILL), "reap"])
        self.assertTrue(child.reaped)
        child.events.close.assert_called_once()

    def test_registration_happens_before_waiting_child_release(self):
        calls = []
        descriptors = []
        real_pipe = os.pipe
        def pipe():
            pair = real_pipe()
            descriptors.extend(pair)
            return pair
        def release(_fd, data):
            # Parent cannot retain a reader: early child exit must produce
            # EPIPE rather than a successful release with no future exit event.
            with self.assertRaises(OSError):
                os.fstat(descriptors[0])
            calls.append("released")
            return len(data)
        events = Mock()
        events.control.side_effect = lambda *_args: calls.append("registered") or []
        process = Mock(pid=1001)
        process.stdout = Mock()
        process.stderr = Mock()
        thread = Mock()
        with tempfile.TemporaryDirectory() as temporary, \
             patch.object(driver.select, "kqueue", return_value=events, create=True), \
             patch.object(driver.select, "kevent", return_value=object(), create=True), \
             patch.object(driver.select, "KQ_FILTER_PROC", 1, create=True), \
             patch.object(driver.select, "KQ_EV_ADD", 2, create=True), \
             patch.object(driver.select, "KQ_EV_ENABLE", 4, create=True), \
             patch.object(driver.select, "KQ_EV_ONESHOT", 8, create=True), \
             patch.object(driver.select, "KQ_NOTE_EXIT", 16, create=True), \
             patch.object(driver.subprocess, "Popen", return_value=process) as popen, \
             patch.object(driver.threading, "Thread", return_value=thread), \
             patch.object(driver.os, "pipe", side_effect=pipe), \
             patch.object(driver.os, "write", side_effect=release):
            root = Path(temporary)
            child = driver.DarwinProcess(["/usr/bin/sandbox-exec", "-f", "profile", "candidate"],
                root, {"HOME": str(root)}, [root / "stdout", root / "stderr"], 64, Path("/reviewed/python"),
                initialization_deadline=driver.time.monotonic() + 1)
            self.assertEqual(calls, ["registered", "released"])
            argv = popen.call_args.args[0]
            self.assertEqual(argv[:4], ["/reviewed/python", "-I", "-S", str(driver.CHILD)])
            self.assertEqual(argv[4], "--await-parent")
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            self.assertEqual(popen.call_args.kwargs["env"], {"HOME": str(root)})
            for log in child.logs:
                log.close()

    def test_resource_monitor_abi_and_disk_limits(self):
        self.assertEqual(ctypes.sizeof(driver.TaskInfo), 96)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "owned").write_bytes(b"x" * 20)
            monitor = object.__new__(driver.ResourceMonitor)
            monitor.driver = SimpleNamespace(root=root, started=driver.time.monotonic(),
                cases={"limits": {"total_seconds": 300}}, manifest={"expires_at": "2099-01-01T00:00:00Z"})
            monitor.limits = {"minimum_free_bytes": 1, "run_bytes": 1, "filesystem_entries": 10, "resident_bytes": 100}
            monitor.peak_bytes, monitor.peak_rss, monitor.minimum_free, monitor.samples = 0, 0, None, 0
            monitor.lock, monitor.children = threading.Lock(), []
            with self.assertRaisesRegex(ValueError, "disk allocation"):
                monitor.sample()
            monitor.limits["run_bytes"] = 16384
            monitor.sample()
            self.assertGreaterEqual(monitor.peak_bytes, 20)
            self.assertEqual(monitor.samples, 1)
            monitor.limits["minimum_free_bytes"] = 2**70
            with self.assertRaisesRegex(ValueError, "free-space"):
                monitor.sample()

    def test_resource_failure_registers_then_kills_new_child_and_never_passes(self):
        monitor = object.__new__(driver.ResourceMonitor)
        monitor.lock = threading.Lock()
        monitor.children = []
        monitor.failure = "allocated RSS exceeded"
        child = Mock()
        monitor.register(child)
        child.signal.assert_called_once_with(signal.SIGKILL)
        self.assertEqual(monitor.children, [child])
        with self.assertRaisesRegex(ValueError, "RSS"):
            monitor.check()

    def test_shared_suite_unmet_is_never_success_and_cleanup_covers_all_children(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = object.__new__(driver.DarwinDriver)
            value.report = {"product_acceptance": False}
            value.root, value.started, value.requests, value.serial = root, driver.time.monotonic(), 0, 0
            first, second = Mock(reaped=False), Mock(reaped=False)
            first.wait.side_effect = ValueError("first capture failed")
            value.monitor = Mock(children=[first, second])
            value.monitor.receipt.return_value = {"samples": 1}
            value.execute = Mock(side_effect=driver.UnmetPrerequisite())
            value.stop = Mock()
            value.cleanup_remaining = Mock(return_value=0)
            self.assertEqual(value.run(), 1)
            first.signal.assert_called_once_with(signal.SIGKILL)
            second.signal.assert_called_once_with(signal.SIGKILL)
            second.wait.assert_called_once_with(0)
            value.monitor.close.assert_called_once()
            observation = json.loads((root / "observation.json").read_text())
            self.assertFalse(observation["product_acceptance"])
            self.assertEqual(observation["subset_observation"], "failed")

    def test_failed_thread_start_uses_shared_attached_custody_and_original_deadline(self):
        for failed_signal in (False, True):
            with self.subTest(failed_signal=failed_signal), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                candidate = Mock(pid=1001, stdout=io.BytesIO(), stderr=io.BytesIO())
                candidate.wait.return_value = -9
                thread = Mock(ident=None)
                thread.start.side_effect = RuntimeError("controlled thread-start failure")
                thread.is_alive.return_value = False
                event = Mock()
                names = {"KQ_FILTER_PROC": 1, "KQ_EV_ADD": 2, "KQ_EV_ENABLE": 4,
                         "KQ_EV_ONESHOT": 8, "KQ_NOTE_EXIT": 16}
                with patch.multiple(driver.select, create=True, **names), \
                     patch.object(driver.select, "kqueue", return_value=event, create=True), \
                     patch.object(driver.select, "kevent", return_value=object(), create=True), \
                     patch.object(driver.subprocess, "Popen", return_value=candidate), \
                     patch.object(driver.threading, "Thread", return_value=thread), \
                     patch.object(driver.os, "killpg", side_effect=PermissionError("controlled") if failed_signal else None), \
                     patch.object(driver.time, "monotonic", side_effect=[10, 12]), \
                     self.assertRaisesRegex(RuntimeError, "thread-start") as caught:
                    driver.DarwinProcess(["/never-executed"], root, {}, [root / "out", root / "err"],
                                         64, Path("/reviewed/python"), initialization_deadline=11)
                child = caught.exception.owned_process
                self.assertIs(child.process, candidate)
                self.assertEqual(child.reaped, not failed_signal)
                self.assertTrue(all(log.closed for log in child.logs))
                self.assertTrue(candidate.stdout.closed and candidate.stderr.closed)
                thread.join.assert_not_called()
                if failed_signal:
                    candidate.wait.assert_not_called()
                    event.close.assert_not_called()
                    self.assertTrue(caught.exception.owned_cleanup_errors)
                    event.close()  # Close only the mocked resource after assertions.
                else:
                    candidate.wait.assert_called_once_with(timeout=0)
                    event.close.assert_called_once()

    def test_direct_preflight_constructor_failure_is_registered_without_resetting_clock(self):
        value = object.__new__(driver.DarwinDriver)
        value.root, value.environment, value.interpreter = Path("/never-written"), {}, Path("/reviewed/python")
        value.cases = {"limits": {"process_log_bytes": 64}}
        value.cleanup_remaining = Mock(return_value=2)
        value.monitor = Mock()
        child = Mock(reaped=False)
        error = RuntimeError("controlled initialization failure")
        error.owned_process = child
        value.monitor.register.side_effect = PermissionError("controlled subsequent monitor failure")
        with patch.object(driver.time, "monotonic", return_value=10), \
             patch.object(driver, "DarwinProcess", side_effect=error) as factory, \
             self.assertRaisesRegex(RuntimeError, "initialization failure"):
            value.spawn(["/never-executed"], [])
        self.assertEqual(factory.call_args.kwargs["initialization_deadline"], 12)
        value.monitor.register.assert_called_once_with(child)


if __name__ == "__main__":
    unittest.main()
