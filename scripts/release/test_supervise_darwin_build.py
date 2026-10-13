"""No builds, processes, network, libproc loads or Mac lane allocations."""
import copy
import errno
import io
import json
import os
from pathlib import Path
import signal
import socket
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import container_artifact
import darwin_build_process as process
import supervise_darwin_build as supervisor

GIB = container_artifact.GIB


def allocation():
    return {"format": supervisor.FORMAT, "target": "aarch64-apple-darwin", "cargo_jobs": 4,
        "source_identity": {"source_commit": "1" * 40, "source_tree": "2" * 40,
                            "source_tar_sha256": "3" * 64, "cargo_lock_sha256": "4" * 64},
        "maximum_seconds": 43200, "minimum_free_bytes": 250 * GIB, "allocation_bytes": 150 * GIB,
        "maximum_group_resident_bytes": 96 * GIB,
        "started_at": (supervisor.dt.datetime.now(supervisor.dt.timezone.utc) - supervisor.dt.timedelta(seconds=60)).isoformat(),
        "expires_at": (supervisor.dt.datetime.now(supervisor.dt.timezone.utc) + supervisor.dt.timedelta(seconds=43000)).isoformat(),
        "lane": {"coordinator": "codex-security-adr-mac", "state": "START", "grant_id": "original-grant"}}


class Offline(unittest.TestCase):
    def setUp(self):
        for target in ("subprocess.Popen", "subprocess.run", "subprocess.check_output", "socket.socket", "ctypes.CDLL"):
            block = patch(target, side_effect=AssertionError("forbidden process/network/platform execution"))
            block.start()
            self.addCleanup(block.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_fixed_allocation_cannot_expand_or_invent_lane(self):
        original = allocation()
        self.assertEqual(supervisor.validate(original), original["source_identity"])
        for key, value in (("cargo_jobs", 5), ("minimum_free_bytes", 249 * GIB),
            ("allocation_bytes", 151 * GIB), ("maximum_seconds", 43201),
            ("maximum_group_resident_bytes", 97 * GIB), ("target", "x86_64-unknown-linux-gnu"),
            ("source_identity", {"source_commit": "1" * 40}),
            ("lane", {"coordinator": "other", "state": "START", "grant_id": "wrong"}),
            ("expires_at", "2025-01-01T00:00:00Z")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                supervisor.validate({**original, key: value})

    def test_private_inputs_reject_missing_public_symlink_and_duplicate_keys(self):
        file = self.root / "receipt.json"
        with self.assertRaises(FileNotFoundError):
            supervisor.private_file(file)
        file.write_text('{}')
        file.chmod(0o644)
        with self.assertRaises(ValueError):
            supervisor.private_file(file)
        file.chmod(0o600)
        self.assertEqual(supervisor.private_file(file), file)
        link = self.root / "link.json"
        link.symlink_to(file)
        with self.assertRaises(ValueError):
            supervisor.private_file(link)
        file.write_text('{"original":1,"original":2}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            supervisor.read_json(file)

    def test_retry_retains_original_grant_elapsed_ceiling(self):
        original = supervisor.dt.datetime(2026, 10, 7, tzinfo=supervisor.dt.timezone.utc)
        a = allocation()
        a.update(started_at=original.isoformat(), expires_at=(original + supervisor.dt.timedelta(hours=12)).isoformat())
        with self.assertRaisesRegex(ValueError, "original elapsed"):
            supervisor.validate({**a, "expires_at": (original + supervisor.dt.timedelta(hours=13)).isoformat()})
        p = {"ledger": self.root / "unused-ledger", "monitored": (self.root,)}
        deadlines = []
        for hours, monotonic in ((6, 10), (7, 3610)):
            with patch.object(supervisor.dt, "datetime", wraps=supervisor.dt.datetime) as date, \
                 patch.object(supervisor.time, "monotonic", return_value=monotonic):
                date.now.return_value = original + supervisor.dt.timedelta(hours=hours)
                instance = supervisor.Supervisor(a, p, Mock())
                deadlines.append(instance.deadline)
        self.assertEqual(deadlines, [21610, 21610])

    def test_exact_receipt_is_private_and_cannot_overwrite(self):
        path = self.root / "out.json"
        with patch.object(supervisor.os, "fsync", wraps=os.fsync) as synchronize:
            supervisor.write_receipt(path, {"original": True})
        self.assertEqual(synchronize.call_count, 2)  # File and containing directory.
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            supervisor.write_receipt(path, {"original": False})
        self.assertEqual(json.loads(path.read_text()), {"original": True})

    def budget(self):
        receipt = self.root / "budget.json"
        disk = types.SimpleNamespace(total=1000 * GIB, used=100 * GIB, free=900 * GIB)
        with patch.object(container_artifact.shutil, "disk_usage", return_value=disk):
            container_artifact.ResourceBudget(receipt, (self.root,)).check()
        p = {"ledger": receipt, "monitored": (self.root,), "source": self.root,
             "paths": {"receipt_directory": self.root}, "environment": {}, "python": Path('/pinned/python')}
        return supervisor.Supervisor(allocation(), p, Mock()), disk

    def test_existing_ledger_consumption_and_missing_ledger_never_reset(self):
        instance, disk = self.budget()
        disk.used, disk.free = 240 * GIB, 760 * GIB
        with patch.object(container_artifact.shutil, "disk_usage", return_value=disk):
            instance.check()
        self.assertEqual(instance.last_budget["consumed_bytes"], 140 * GIB)
        disk.used, disk.free = 230 * GIB, 770 * GIB
        with patch.object(container_artifact.shutil, "disk_usage", return_value=disk):
            instance.check()
        self.assertEqual(instance.last_budget["consumed_bytes"], 140 * GIB)
        instance.prepared["ledger"].unlink()
        with self.assertRaises(FileNotFoundError):
            instance.check()
        self.assertFalse(instance.prepared["ledger"].exists())

    def test_floor_deadline_cancel_and_rss_stop_before_more_work(self):
        instance, disk = self.budget()
        disk.free = 249 * GIB
        with patch.object(container_artifact.shutil, "disk_usage", return_value=disk), self.assertRaises(RuntimeError):
            instance.check()
        instance.cancelled = True
        with self.assertRaisesRegex(ValueError, "cancelled"):
            instance.check()
        instance.cancelled = False
        instance.deadline = supervisor.time.monotonic() - 1
        with self.assertRaisesRegex(ValueError, "elapsed"):
            instance.check()

    def test_resource_lock_contention_fails_promptly_without_ledger_replacement(self):
        instance, _ = self.budget()
        before = instance.prepared["ledger"].read_bytes()
        with patch.object(container_artifact.fcntl, "flock", side_effect=BlockingIOError) as lock:
            with self.assertRaises(BlockingIOError):
                instance.check()
        self.assertTrue(lock.call_args.args[1] & container_artifact.fcntl.LOCK_NB)
        self.assertEqual(instance.prepared["ledger"].read_bytes(), before)

    def test_rss_or_incomplete_observation_refused(self):
        instance, disk = self.budget()
        child = types.SimpleNamespace(process=types.SimpleNamespace(pid=301))
        for sample in ({"uncertain_pids": [301], "resident_bytes": 0},
                       {"uncertain_pids": [], "resident_bytes": 96 * GIB}):
            instance.observer.sample.return_value = sample
            with patch.object(container_artifact.shutil, "disk_usage", return_value=disk), self.assertRaises(ValueError):
                instance.check(child)

    def test_launch_failure_retains_registered_handle_and_cleans(self):
        instance, _ = self.budget()
        child = Mock()
        child.launch.side_effect = OSError("release handshake failed")
        child.cleanup.return_value = -9
        child.custody.return_value = {"child_pid": 301, "expected_pgid": 301,
                                     "group_cleanup_confirmed": True}
        child.group_empty, child.reaped, child.bytes = True, True, 0
        with patch.object(instance, "check"), patch.object(supervisor, "BuildProcess", return_value=child):
            with self.assertRaises(OSError):
                instance.command("failure", ["/exact/build-native"], 2)
        self.assertEqual(instance.children, [child])
        child.cleanup.assert_called_once()
        self.assertEqual(instance.commands[0]["returncode"], -9)
        self.assertLessEqual(child.cleanup.call_args.kwargs["deadline"], instance.deadline)
        self.assertEqual(child.cleanup.call_args.kwargs["deadline"], child.cleanup_deadline)

    def test_launch_registers_before_release_and_closes_parent_reader(self):
        observer = Mock()
        child = process.BuildProcess(observer)
        fake = Mock(pid=333, stdout=io.BytesIO(b""))
        events = Mock()
        order = []
        real_write = os.write
        def popen(*args, **kwargs):
            self.assertTrue(kwargs["start_new_session"])
            self.assertEqual(kwargs["pass_fds"], (71,))
            order.append("launch")
            return fake
        def close(fd):
            order.append("close-" + str(fd))
        def control(*args):
            self.assertIn("close-71", order)
            order.append("register")
        def registered(handle):
            self.assertIs(handle.process, fake)
            self.assertNotIn("release", order)
            order.append("custody")
        def write(fd, data):
            if fd == 72:
                self.assertEqual(data, b"G")
                self.assertIn("register", order)
                order.append("release")
                return 1
            return real_write(fd, data)
        events.control.side_effect = control
        names = {"KQ_FILTER_PROC": -5, "KQ_EV_ADD": 1, "KQ_EV_ENABLE": 4,
                 "KQ_EV_ONESHOT": 16, "KQ_NOTE_EXIT": 0x80000000}
        with patch.multiple(process.select, create=True, **names), \
             patch.object(process.select, "kqueue", return_value=events, create=True), \
             patch.object(process.select, "kevent", return_value=Mock(), create=True), \
             patch.object(process.os, "pipe", return_value=(71,72)), \
             patch.object(process.os, "close", side_effect=close), \
             patch.object(process.os, "write", side_effect=write), \
             patch.object(process.subprocess, "Popen", side_effect=popen), \
             patch.object(process.threading, "Thread", return_value=Mock()):
            child.launch(["/exact/build-native"], self.root, {}, self.root / "log", '/pinned/python', 99,
                         registered=registered)
        child.log.close()
        self.assertEqual(order, ["launch", "custody", "close-71", "register", "release", "close-72"])
        fake.poll.assert_not_called()
        fake.wait.assert_not_called()

    def test_exit_observation_never_reaps(self):
        child = process.BuildProcess(Mock())
        child.process = Mock(pid=301)
        child.events = Mock()
        child.events.control.return_value = [types.SimpleNamespace(flags=0, ident=301, fflags=8)]
        with patch.multiple(process.select, create=True, KQ_EV_ERROR=4, KQ_NOTE_EXIT=8):
            self.assertTrue(child.done())
        child.process.wait.assert_not_called()
        child.process.poll.assert_not_called()

    def test_unstarted_log_thread_cleanup_closes_pipe_and_log_before_lane_release(self):
        observer = Mock()
        observer.sample.return_value = {"live_pids": [], "uncertain_pids": []}
        child = process.BuildProcess(observer)
        child.process = Mock(pid=301, stdout=io.BytesIO(), returncode=-9)
        child.process.wait.return_value = -9
        child.events = Mock()
        child.log = (self.root / "unstarted.log").open("wb")
        child.thread = Mock(ident=None)
        child.thread.join.side_effect = RuntimeError("cannot join thread before it is started")
        with patch.object(process.os, "killpg") as kill:
            self.assertEqual(child.cleanup(deadline=process.time.monotonic() + 15), -9)
            self.assertEqual(child.cleanup(deadline=process.time.monotonic() + 15), -9)
        child.thread.join.assert_not_called()
        self.assertTrue(child.process.stdout.closed and child.log.closed)
        self.assertTrue(child.group_empty and child.reaped)
        child.process.wait.assert_called_once()
        kill.assert_not_called()

    def test_launch_thread_start_failure_keeps_child_and_closes_original_capture(self):
        observer = Mock()
        observer.sample.return_value = {"live_pids": [], "uncertain_pids": []}
        child = process.BuildProcess(observer)
        fake = Mock(pid=333, stdout=io.BytesIO(), returncode=-9)
        fake.wait.return_value = -9
        thread = Mock(ident=None)
        thread.start.side_effect = RuntimeError("controlled log-thread start failure")
        names = {"KQ_FILTER_PROC": -5, "KQ_EV_ADD": 1, "KQ_EV_ENABLE": 4,
                 "KQ_EV_ONESHOT": 16, "KQ_NOTE_EXIT": 0x80000000}
        with patch.multiple(process.select, create=True, **names), \
             patch.object(process.select, "kqueue", return_value=Mock(), create=True), \
             patch.object(process.select, "kevent", return_value=Mock(), create=True), \
             patch.object(process.subprocess, "Popen", return_value=fake), \
             patch.object(process.threading, "Thread", return_value=thread), \
             patch.object(process.os, "killpg") as kill:
            with self.assertRaisesRegex(RuntimeError, "thread start failure"):
                child.launch(["/exact/build-native"], self.root, {}, self.root / "failed.log", '/pinned/python', 99)
            self.assertIs(child.process, fake)
            self.assertEqual(child.cleanup(deadline=process.time.monotonic() + 15), -9)
        self.assertTrue(child.reaped and child.group_empty and child.log.closed and fake.stdout.closed)
        fake.wait.assert_called_once()
        kill.assert_not_called()
        thread.join.assert_not_called()

    def test_reaped_child_cleanup_rechecks_unfinished_capture(self):
        child = process.BuildProcess(Mock())
        child.process = Mock(pid=301, stdout=io.BytesIO(), returncode=-9)
        child.log = (self.root / "late.log").open("wb")
        child.thread = Mock(ident=401)
        child.thread.is_alive.side_effect = [True, False]
        child.reaped = True
        with patch.object(process.os, "killpg") as kill:
            with self.assertRaisesRegex(RuntimeError, "retain Mac lane"):
                child.cleanup(deadline=process.time.monotonic())
            self.assertFalse(child.group_empty)
            self.assertEqual(child.cleanup(deadline=process.time.monotonic()), -9)
        self.assertTrue(child.group_empty and child.process.stdout.closed and child.log.closed)
        self.assertTrue(all(call.kwargs["timeout"] == 0 for call in child.thread.join.call_args_list))
        child.process.wait.assert_not_called()
        kill.assert_not_called()

    def test_cleanup_signals_only_original_group_before_reaping(self):
        observer = Mock()
        observer.sample.side_effect = [{"live_pids": [302], "uncertain_pids": []},
                                       {"live_pids": [], "uncertain_pids": []}]
        child = process.BuildProcess(observer)
        child.process = Mock(pid=301)
        child.events = Mock()
        order = []
        child.process.wait.side_effect = lambda **kwargs: order.append("wait") or -9
        with patch.object(process.os, "killpg", side_effect=lambda pid,sig: order.append((pid,sig))), \
             patch.object(process.time, "sleep"):
            self.assertEqual(child.cleanup(deadline=process.time.monotonic() + 15), -9)
            child.kill_owned()
        self.assertTrue(child.group_empty and child.reaped)
        self.assertEqual(order, [(301, signal.SIGKILL), "wait"])
        self.assertEqual(child.custody()["first_cleanup_sample"], {"live_pids": [302], "uncertain_pids": []})
        self.assertEqual(child.custody()["successful_group_signal"],
                         {"pgid": 301, "signal": int(signal.SIGKILL), "leader_reaped": False})
        self.assertEqual(child.custody()["last_group_sample"], {"live_pids": [], "uncertain_pids": []})

    def test_exited_zombie_only_group_is_observed_before_reap_without_signal(self):
        observer = Mock()
        observer.sample.return_value = {"live_pids": [], "uncertain_pids": [], "resident_bytes": 0}
        child = process.BuildProcess(observer)
        child.process = Mock(pid=301, returncode=0)
        child.process.wait.return_value = 0
        child.events = Mock()
        child.exited = True
        with patch.object(process.os, "killpg", side_effect=PermissionError(errno.EPERM, "zombie group")) as kill:
            self.assertEqual(child.cleanup(deadline=process.time.monotonic() + 15), 0)
        kill.assert_not_called()
        observer.sample.assert_called_once_with(301)
        self.assertTrue(child.group_empty and child.reaped)
        self.assertEqual(child.custody()["last_group_sample"], observer.sample.return_value)

    def test_eperm_race_requires_second_complete_empty_sample(self):
        observer = Mock()
        observer.sample.side_effect = [{"live_pids": [301], "uncertain_pids": []},
                                       {"live_pids": [], "uncertain_pids": []}]
        child = process.BuildProcess(observer)
        child.process = Mock(pid=301, returncode=0)
        child.process.wait.return_value = 0
        child.events = Mock()
        with patch.object(process.os, "killpg", side_effect=PermissionError(errno.EPERM, "exited race")) as kill:
            self.assertEqual(child.cleanup(deadline=process.time.monotonic() + 15), 0)
            child.kill_owned()
        kill.assert_called_once_with(301, signal.SIGKILL)
        self.assertEqual(observer.sample.call_count, 2)
        self.assertEqual(child.custody()["signal_error"]["errno"], errno.EPERM)
        self.assertTrue(child.group_empty and child.reaped)

    def test_eperm_live_or_uncertain_recheck_never_reaps_or_releases(self):
        for sample in ({"live_pids": [302], "uncertain_pids": []},
                       {"live_pids": [], "uncertain_pids": [302]}):
            with self.subTest(sample=sample):
                observer = Mock()
                observer.sample.side_effect = [{"live_pids": [301], "uncertain_pids": []}, sample]
                child = process.BuildProcess(observer)
                child.process = Mock(pid=301)
                child.events = Mock()
                with patch.object(process.os, "killpg", side_effect=PermissionError(errno.EPERM, "denied")), \
                     self.assertRaises(PermissionError):
                    child.cleanup(deadline=process.time.monotonic() + 15)
                child.process.wait.assert_not_called()
                self.assertFalse(child.group_empty or child.reaped)
                self.assertEqual(child.custody()["last_group_sample"], sample)

    def test_command_cleanup_failure_retains_exact_custody_and_errno(self):
        instance, _ = self.budget()
        child = process.BuildProcess(Mock())
        child.process = Mock(pid=301)
        child.bytes = 17
        child.last_sample = {"live_pids": [301], "uncertain_pids": []}
        child.stage = "cleanup-signal-recheck"
        child.launch = Mock()
        child.done = Mock(return_value=True)
        child.cleanup = Mock(side_effect=PermissionError(errno.EPERM, "controlled denial"))
        with patch.object(instance, "check"), patch.object(supervisor, "BuildProcess", return_value=child), \
             self.assertRaises(PermissionError):
            instance.command("failed-cleanup", ["/exact/tool"], 30)
        record = instance.commands[0]
        self.assertEqual((record["child_pid"], record["expected_pgid"]), (301, 301))
        self.assertEqual(record["cleanup_failure"]["errno"], errno.EPERM)
        self.assertEqual(record["cleanup_failure"]["custody"]["stage"], "cleanup-signal-recheck")
        self.assertEqual(record["last_group_sample"], child.last_sample)
        self.assertEqual(record["captured_bytes"], 17)
        self.assertFalse(record["group_cleanup_confirmed"])
        child.stage = "closed"
        record.update(child.custody())
        self.assertEqual(record["cleanup_failure"]["custody"]["stage"], "cleanup-signal-recheck")

    def test_command_registration_durably_records_child_before_release(self):
        instance, _ = self.budget()
        child = process.BuildProcess(Mock())
        child.process = Mock(pid=301)
        child.done = Mock(return_value=True)
        child.cleanup = Mock(return_value=0)
        def launch(*args, registered):
            registered(child)
            proof = json.loads((self.root / "identity.custody.json").read_text())
            self.assertEqual((proof["child_pid"], proof["expected_pgid"]), (301, 301))
            self.assertFalse(proof["child_released"])
            self.assertEqual(proof["grant_id"], instance.allocation["lane"]["grant_id"])
        child.launch = Mock(side_effect=launch)
        with patch.object(instance, "check"), patch.object(supervisor, "BuildProcess", return_value=child):
            instance.command("identity", ["/exact/tool"], 30)
        self.assertEqual((self.root / "identity.custody.json").stat().st_mode & 0o777, 0o600)

    def test_custody_write_failure_never_releases_original_child(self):
        child = process.BuildProcess(Mock())
        fake = Mock(pid=333, stdout=io.BytesIO())
        def fail_custody(handle):
            self.assertIs(handle.process, fake)
            raise OSError(errno.ENOSPC, "controlled custody fsync failure")
        with patch.object(process.select, "kqueue", return_value=Mock(), create=True), \
             patch.object(process.subprocess, "Popen", return_value=fake), \
             patch.object(process.os, "write", wraps=os.write) as write, \
             patch.object(process.threading, "Thread") as thread, \
             self.assertRaisesRegex(OSError, "custody fsync"):
            child.launch(["/must/not/execute"], self.root, {}, self.root / "custody-failure.log",
                         '/pinned/python', 99, registered=fail_custody)
        write.assert_not_called()
        thread.assert_not_called()
        self.assertIs(child.process, fake)
        fake.wait.assert_not_called()
        self.assertFalse(child.reaped)
        child.log.close()

    def test_final_cleanup_retry_refreshes_original_command_custody(self):
        lane = self.root / "lane.lock"
        lane.touch()
        a = {**allocation(), "execution_authorized": True, "tool_sha256": {}, "python_sha256": "2"*64}
        p = {"lane_file": lane, "python": Path('/pinned/python'), "identity": a["source_identity"],
             "environment": {}, "original_ledger_sha256": "1"*64, "paths": {"receipt_directory": self.root}}
        child = process.BuildProcess(Mock())
        child.process = Mock(pid=301)
        child.cleanup_deadline = process.time.monotonic() + 15
        child.command_record = {"name": "original", **child.custody()}
        child.command_record["cleanup_failure"] = {"errno": errno.EPERM,
            "custody": {"stage": "cleanup-signal-recheck", "last_group_sample": {"live_pids": [301], "uncertain_pids": []}}}
        def close_child(**kwargs):
            child.group_empty = child.reaped = True
            child.stage = "closed"
            child.last_sample = {"live_pids": [], "uncertain_pids": []}
            return 0
        child.cleanup = Mock(side_effect=close_child)
        instance = Mock(started=supervisor.time.monotonic(), commands=[child.command_record],
                        samples=1, peak_rss=0, minimum_free=900*GIB, last_budget={}, children=[child])
        instance.run.side_effect = PermissionError(errno.EPERM, "initial cleanup race")
        instance.cleanup_deadline.return_value = child.cleanup_deadline
        with patch.object(supervisor.platform, "system", return_value="Darwin"), \
             patch.object(supervisor.platform, "machine", return_value="arm64"), \
             patch.object(supervisor.sys, "version_info", (3,12,12)), \
             patch.object(supervisor.sys, "executable", '/pinned/python'), \
             patch.object(supervisor, "GroupObserver", return_value=Mock()), \
             patch.object(supervisor, "Supervisor", return_value=instance), self.assertRaises(ValueError):
            supervisor.execute(a, p, "3"*64)
        report = json.loads((self.root / "supervision.json").read_text())
        self.assertTrue(report["lane_release_ready"])
        self.assertFalse(report["build_succeeded"])
        self.assertTrue(report["commands"][0]["group_cleanup_confirmed"])
        self.assertEqual(report["commands"][0]["stage"], "closed")
        self.assertEqual(report["commands"][0]["returncode"], 0)
        self.assertEqual(report["commands"][0]["cleanup_failure"]["custody"]["stage"],
                         "cleanup-signal-recheck")
        self.assertEqual(report["commands"][0]["last_group_sample"], child.last_sample)
        self.assertEqual(lane.read_bytes(), b'')

    def test_incomplete_cleanup_never_reaps_or_reports_lane_ready(self):
        observer = Mock()
        observer.sample.return_value = {"live_pids": [], "uncertain_pids": [301]}
        child = process.BuildProcess(observer)
        child.process = Mock(pid=301)
        child.events = Mock()
        with patch.object(process.os, "killpg"), self.assertRaisesRegex(RuntimeError, "retain Mac lane"):
            child.cleanup(deadline=process.time.monotonic())
        child.process.wait.assert_not_called()
        self.assertFalse(child.group_empty or child.reaped)

    def test_bounded_log_retains_prefix_and_records_overflow(self):
        child = process.BuildProcess(Mock())
        child.process = Mock(stdout=io.BytesIO(b"known-build-output"))
        path = self.root / "bounded.log"
        child.log = path.open('wb')
        child._drain(5)
        self.assertEqual(path.read_bytes(), b"known")
        self.assertTrue(child.failure.is_set())
        self.assertEqual(child.bytes, 18)

    def prepared_tree(self):
        a = allocation()
        task = self.root / "task"
        task.mkdir(mode=0o700)
        source = task / "source"
        (source / ".git").mkdir(parents=True)
        home = self.root / "home"
        home.mkdir()
        key = "source-" + supervisor.hashlib.sha1(str(source / ".git").encode()).hexdigest()[:10]
        base = home / "Library/Caches/rust-workspaces" / key
        locations = {"cargo_home": base / "cargo-home",
            "cargo_target": base / "targets/recall-darwin-111111111111",
            "ort_cache": task / "ort-cache", "rustup_home": task / "rustup",
            "tmp_directory": task / "tmp", "output_directory": task / "output",
            "receipt_directory": task / "attempt", "developer_directory": self.root / "CLT",
            "sdk_root": self.root / "CLT/SDKs/MacOSX.sdk",
            "libclang_directory": self.root / "CLT/usr/lib"}
        for path in locations.values():
            path.mkdir(parents=True, exist_ok=True)
        (task / "cargo-bootstrap").mkdir()
        (locations["libclang_directory"] / "libclang.dylib").write_bytes(b"reviewed libclang")
        python = task / "tools/python/bin/python3.12"
        tools = {"rustc": task / "rustup/toolchains/1.94.0-aarch64-apple-darwin/bin/rustc",
                 "cargo": task / "rustup/toolchains/1.94.0-aarch64-apple-darwin/bin/cargo",
                 "clang": self.root / "CLT/usr/bin/clang", "clang++": self.root / "CLT/usr/bin/clang++"}
        for name,path in {**tools, "python": python}.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(("reviewed " + name).encode())
        tools["clang++"].unlink()
        tools["clang++"].symlink_to(tools["clang"])
        ledger = task / "budget.json"
        usage = types.SimpleNamespace(total=1000*GIB, used=100*GIB, free=900*GIB)
        with patch.object(container_artifact.shutil, "disk_usage", return_value=usage):
            container_artifact.ResourceBudget(ledger, (task,)).check()
        lane = task / "mac-lane.lock"
        lane.touch(mode=0o600)
        a.update({name:str(path) for name,path in locations.items()})
        a.update(task_directory=str(task), source_directory=str(source), python=str(python),
                 python_sha256=supervisor.artifact.digest(python),
                 tool_sha256={name:supervisor.artifact.digest(path) for name,path in tools.items()},
                 clang=str(tools["clang"]), clangxx=str(tools["clang++"]),
                 budget_receipt=str(ledger), lane_lock=str(lane), monitored_paths=[str(task)])
        return a, home, task

    def test_prepare_binds_real_cache_pinned_tools_and_task_local_environment(self):
        a, home, task = self.prepared_tree()
        with patch.object(supervisor.pwd, "getpwuid", return_value=types.SimpleNamespace(pw_dir=str(home))), \
             patch.dict(os.environ, {"AWS_SECRET_ACCESS_KEY":"must-not-copy", "RUSTFLAGS":"unsafe-override"}):
            prepared = supervisor.prepare(a)
        environment = prepared["environment"]
        self.assertEqual(environment["HOME"], str(home))
        self.assertEqual(environment["CARGO_BUILD_JOBS"], "4")
        self.assertEqual(environment["CXX"], a["clangxx"])
        self.assertEqual(environment["ORT_CACHE_DIR"], str(task / "ort-cache"))
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
        self.assertNotIn("RUSTFLAGS", environment)
        self.assertEqual(prepared["original_ledger_sha256"], supervisor.artifact.digest(Path(a["budget_receipt"])))
        (Path(a["cargo_home"]) / "config.toml").write_text('[build]\nrustflags=["foreign"]\n')
        with patch.object(supervisor.pwd, "getpwuid", return_value=types.SimpleNamespace(pw_dir=str(home))), \
             self.assertRaisesRegex(ValueError, "ambient configuration"):
            supervisor.prepare(a)

    def test_execute_clears_marker_only_after_cleanup_receipt(self):
        self.execution_marker_case(False, False)

    def test_execute_retains_marker_when_any_cleanup_is_uncertain(self):
        self.execution_marker_case(True, False)

    def test_execute_never_clears_an_existing_unreconciled_marker(self):
        self.execution_marker_case(False, True)

    def test_execute_expired_original_clock_cleans_all_children_without_new_allowance(self):
        instance, _ = self.budget()
        instance.deadline = supervisor.time.monotonic() - 1
        instance.wall_deadline = supervisor.dt.datetime.now(supervisor.dt.timezone.utc) - supervisor.dt.timedelta(seconds=1)
        first, second = Mock(group_empty=False, reaped=False), Mock(group_empty=True, reaped=True)
        for child in (first, second):
            child.cleanup_deadline = instance.deadline
        first.cleanup.side_effect = RuntimeError("original group still live")
        instance.children = [second, first]
        instance.run = Mock(side_effect=ValueError("original deadline exhausted"))
        lane = self.root / "expired-lane.lock"
        lane.touch()
        p = {"lane_file": lane, "python": Path('/pinned/python'), "identity": allocation()["source_identity"],
             "environment": {}, "original_ledger_sha256": "1"*64, "paths": {"receipt_directory": self.root}}
        a = {**allocation(), "execution_authorized": True, "tool_sha256": {}, "python_sha256":"2"*64}
        with patch.object(supervisor.platform, "system", return_value="Darwin"), \
             patch.object(supervisor.platform, "machine", return_value="arm64"), \
             patch.object(supervisor.sys, "version_info", (3,12,12)), \
             patch.object(supervisor.sys, "executable", "/pinned/python"), \
             patch.object(supervisor, "GroupObserver", return_value=Mock()), \
             patch.object(supervisor, "Supervisor", return_value=instance), \
             self.assertRaises(ValueError):
            supervisor.execute(a, p, "3"*64)
        for child in (first, second):
            child.cleanup.assert_called_once_with(deadline=instance.deadline)
        self.assertTrue(lane.read_text())
        self.assertFalse(json.loads((self.root / "supervision.json").read_text())["lane_release_ready"])

    def test_expired_cleanup_uses_only_zero_time_reap_and_capture(self):
        observer = Mock()
        observer.sample.return_value = {"live_pids": [], "uncertain_pids": []}
        child = process.BuildProcess(observer)
        child.process = Mock(pid=301, returncode=0)
        child.process.wait.return_value = 0
        child.events = Mock()
        child.thread = Mock(ident=1)
        child.thread.is_alive.return_value = False
        with patch.object(process.os, "killpg"), patch.object(process.time, "monotonic", return_value=10):
            self.assertEqual(child.cleanup(deadline=9), 0)
        child.process.wait.assert_called_once_with(timeout=0)
        child.thread.join.assert_called_once_with(timeout=0)

    def execution_marker_case(self, cleanup_failure, original_marker):
        lane = self.root / "lane.lock"
        lane.write_text("prior-owner" if original_marker else "")
        p = {"lane_file": lane, "python": Path('/pinned/python'), "identity": allocation()["source_identity"],
             "environment": {}, "original_ledger_sha256": "1"*64, "paths": {"receipt_directory": self.root}}
        a = {**allocation(), "execution_authorized": True, "tool_sha256": {}, "python_sha256":"2"*64}
        instance = Mock(started=supervisor.time.monotonic(), commands=[], samples=1, peak_rss=0,
                        minimum_free=900*GIB, last_budget={})
        child = Mock(group_empty=not cleanup_failure, reaped=not cleanup_failure)
        if cleanup_failure:
            child.cleanup.side_effect = RuntimeError("original group still live")
        instance.children = [child]
        with patch.object(supervisor.platform, "system", return_value="Darwin"), \
             patch.object(supervisor.platform, "machine", return_value="arm64"), \
             patch.object(supervisor.sys, "version_info", (3,12,12)), \
             patch.object(supervisor.sys, "executable", "/pinned/python"), \
             patch.object(supervisor, "GroupObserver", return_value=Mock()), \
             patch.object(supervisor, "Supervisor", return_value=instance):
            if cleanup_failure or original_marker:
                with self.assertRaises(ValueError):
                    supervisor.execute(a, p, "3"*64)
            else:
                supervisor.execute(a, p, "3"*64)
        report = json.loads((self.root / "supervision.json").read_text())
        self.assertEqual(report["lane_release_ready"], not (cleanup_failure or original_marker))
        self.assertEqual(bool(lane.read_text()), cleanup_failure or original_marker)
        if original_marker:
            instance.run.assert_not_called()
            self.assertEqual(lane.read_text(), "prior-owner")

    def test_c_abi_layout_matches_apple_header(self):
        import ctypes
        self.assertEqual(ctypes.sizeof(process.ShortInfo), 64)
        self.assertEqual(ctypes.sizeof(process.TaskInfo), 96)


if __name__ == "__main__":
    unittest.main()
