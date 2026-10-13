"""Offline harness controls only: no candidate, socket, subprocess, or provider.

These tests judge capture/custody mechanics, never installed product behavior.
Run explicitly with unittest's installed_linux_test.py discovery pattern.
"""
from contextlib import nullcontext
import copy
import datetime as dt
import http.client
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import ANY, Mock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("installed_linux_driver", ROOT / "scripts/qualification/installed_linux_offline.py")
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class MemorySocket:
    def __init__(self, wire):
        self.wire = wire

    def makefile(self, *_args):
        return io.BytesIO(self.wire)


class KeptBytes(io.BytesIO):
    def close(self):
        self.saved = self.getvalue()
        super().close()


class InstalledLinuxControlTests(unittest.TestCase):
    def test_inert_plan_binds_actual_inputs_and_does_not_claim_acceptance(self):
        with patch.object(driver.subprocess, "Popen", side_effect=AssertionError("no child")), \
             patch.object(driver.socket, "socket", side_effect=AssertionError("no socket")), \
             patch.object(driver.artifact, "verify", side_effect=AssertionError("no install")):
            plan = driver.plan()
        self.assertFalse(plan["product_acceptance"])
        self.assertEqual(plan["corpus_sha256"], driver.fixtures.corpus_sha256())
        self.assertEqual(plan["expectations"]["spaces"], [row["id"] for row in driver.fixtures.corpus()["spaces"]])
        self.assertIn("physical erasure and old-backup journal replay", plan["expectations"]["unqualified_gates"])

    def manifest_packet(self, root):
        plan = driver.plan()
        review = {key: plan[key] for key in ("expectations_sha256", "corpus_sha256", "driver_sha256", "collector_sha256", "installer_sha256", "process_capture_sha256")}
        review.update(format="recall-installed-linux-review/v1", approved=True, reviewer="qualification")
        review_path = root / "review.json"
        review_path.write_bytes(driver.encoded(review))
        manifest = {"format": "recall-installed-linux-run/v1", "execution_authorized": True,
                    "backend": "elephant-db", "source_commit": "a" * 40, "archive_sha256": "b" * 64,
                    "binary_sha256": "c" * 64, "archive": str(root / "absent.tar.gz"),
                    "run_directory": str(root / "new"), "expectation_review": str(review_path),
                    "expectation_review_sha256": driver.file_digest(review_path),
                    "expires_at": "2026-10-07T23:00:00Z"}
        return manifest, review

    def test_manifest_requires_independent_review_exact_hashes_and_allocation(self):
        now = dt.datetime(2026, 10, 7, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, review = self.manifest_packet(root)
            path = root / "run.json"
            path.write_bytes(driver.encoded(manifest))
            self.assertEqual(driver.validate_manifest(path, now)[0], manifest)
            mutations = {"execution_authorized": False, "backend": "remote", "source_commit": "branch-name",
                         "expires_at": "2026-10-06T23:00:00Z", "run_directory": "relative",
                         "expectation_review_sha256": "0" * 64}
            for key, value in mutations.items():
                with self.subTest(key=key):
                    changed = dict(manifest, **{key: value})
                    path.write_bytes(driver.encoded(changed))
                    with self.assertRaises(ValueError):
                        driver.validate_manifest(path, now)

            for key in ("expectations_sha256", "corpus_sha256", "driver_sha256", "collector_sha256", "installer_sha256", "process_capture_sha256", "reviewer", "approved"):
                with self.subTest(review_field=key):
                    changed = dict(review, **{key: False if key == "approved" else "different"})
                    Path(manifest["expectation_review"]).write_bytes(driver.encoded(changed))
                    packet = dict(manifest, expectation_review_sha256=driver.file_digest(Path(manifest["expectation_review"])))
                    path.write_bytes(driver.encoded(packet))
                    with self.assertRaises(ValueError):
                        driver.validate_manifest(path, now)

    def test_cli_bootstrap_operator_has_only_management_scopes_and_no_data_clearance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "private").mkdir()
            value = self.stub(root)
            issued = {}

            def process(arguments):
                if arguments[:2] == ["service-token", "bootstrap"]:
                    driver.private_new(Path(arguments[arguments.index("--secret-file") + 1]), b"synthetic-signing-material-only-0001")
                    return
                self.assertEqual(arguments[:2], ["service-token", "issue"])
                subject = arguments[arguments.index("--subject") + 1]
                issued[subject] = arguments
                driver.private_new(Path(arguments[arguments.index("--output") + 1]), b"synthetic.signed.control")

            value.process = Mock(side_effect=process)
            with patch.object(driver.subprocess, "Popen", side_effect=AssertionError("no candidate")), \
                 patch.object(driver.socket, "socket", side_effect=AssertionError("no socket")):
                value.credentials()
            self.assertEqual(value.process.call_count, 6)
            operator = issued["installed-operator"]
            scopes = [operator[index + 1] for index, arg in enumerate(operator) if arg == "--scope"]
            self.assertEqual(set(scopes), {"manage:grant", "manage:provider", "read:space_config", "write:space_config",
                                          "read:status", "read:system", "write:system"})
            self.assertIn("--global-admin", operator)
            self.assertNotIn("--clearances-file", operator)
            self.assertNotIn("--space", operator)
            self.assertNotIn("--bootstrap-space", operator)
            for role, count in (("service", 2), ("household", 1)):
                arguments = issued["installed-" + role]
                self.assertNotIn("--global-admin", arguments)
                self.assertNotIn("--bootstrap-space", arguments)
                self.assertEqual([arguments[index + 1] for index, arg in enumerate(arguments) if arg == "--space"],
                                 value.cases["spaces"][:count])
                self.assertIn("--clearances-file", arguments)
                self.assertEqual([arguments[index + 1] for index, arg in enumerate(arguments) if arg == "--scope"], value.data_scopes)
                self.assertIn("write:source", value.data_scopes)
                self.assertIn("read:doc:raw", value.data_scopes)
            for space in driver.fixtures.corpus()["spaces"]:
                role = value.bootstrap_role(space["id"])
                arguments = issued["installed-" + role]
                self.assertNotIn("--global-admin", arguments)
                self.assertNotIn("--space", arguments)
                self.assertNotIn("--read-only", arguments)
                self.assertEqual(arguments[arguments.index("--bootstrap-space") + 1], space["id"])
                self.assertEqual(arguments[arguments.index("--expires-in") + 1], "900")
                scopes = [arguments[index + 1] for index, arg in enumerate(arguments) if arg == "--scope"]
                self.assertEqual(set(scopes), {"read:space_config", "write:space_config", "manage:grant", "manage:provider"})
                clearance_path = Path(arguments[arguments.index("--clearances-file") + 1])
                self.assertEqual(driver.read_json(clearance_path), [{"policy_id": space["policy_id"], "max_level": space["level"],
                    "labels": [], "handling_allow": []}])
                self.assertEqual(clearance_path.stat().st_mode & 0o077, 0)

    def test_bootstrap_missing_or_wrong_space_never_falls_back_to_global_operator(self):
        value = self.stub(Path("/never-written"))
        value.tokens["operator"] = "synthetic-global-management"
        home, business = value.cases["spaces"]
        value.tokens["bootstrap-" + home] = "synthetic-household-bootstrap"
        self.assertEqual(value.bootstrap_role(home), "bootstrap-" + home)
        for space in (business, "default", "unallocated-space"):
            with self.subTest(space=space), self.assertRaises(ValueError):
                value.bootstrap_role(space)

    def test_mcp_requires_complete_unambiguous_exact_identity(self):
        response = {"jsonrpc": "2.0", "id": 7, "result": {"tools": []}}
        raw = driver.encoded(response)
        self.assertEqual(driver.mcp_value(raw, 7), response)
        self.assertEqual(driver.mcp_value(b"event: message\ndata: " + raw + b"\n\n", 7), response)
        invalid = [raw.replace(b'"id": 7', b'"id": 8'), raw.replace(b'"id": 7', b'"id": true'),
                   b'data: ' + raw + b'\n', b'data: ' + raw + b'\n\ndata: ' + raw + b'\n\n',
                   b'{"jsonrpc":"2.0","id":7,"id":7,"result":{}}',
                   b'{"jsonrpc":"2.0","id":7,"result":{"x":1e999}}']
        for value in invalid:
            with self.subTest(raw=value), self.assertRaises(ValueError):
                driver.mcp_value(value, 7)

    def stub(self, root):
        result = driver.Driver.__new__(driver.Driver)
        result.root = root
        result.cases = copy.deepcopy(driver.plan()["expectations"])
        result.manifest = {"expires_at": "2999-01-01T00:00:00Z"}
        result.started = time.monotonic()
        result.serial = result.requests = result.capture_bytes = 0
        result.tokens = {"service": "synthetic-token-not-for-receipts"}
        result.port = 45678
        result.child = None
        result.owned_commands = []
        result.binary = root / "never-run"
        result.environment = {"HOME": str(root)}
        result.report = {"cases": [], "product_acceptance": False}
        return result

    def request_memory(self, wire, *, limit=262144):
        response = http.client.HTTPResponse(MemorySocket(wire))
        response.begin()
        connection = Mock()
        connection.getresponse.return_value = response
        result = self.stub(Path("/never-written"))
        result.cases["limits"]["response_bytes"] = limit
        saved = []
        result.save = lambda name, raw: saved.append(json.loads(raw))
        with patch.object(driver.http.client, "HTTPConnection", return_value=connection), \
             patch.object(driver.capture, "total_deadline", return_value=nullcontext()):
            try:
                value = result.http("POST", "/v1/example", "service", "qualification-household", {"query": "original"})
            except Exception as error:
                value = error
        connection.close.assert_called_once()
        return value, saved

    def test_http_complete_capture_keeps_original_body_without_bearer(self):
        value, saved = self.request_memory(b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{"ok":true}')
        self.assertEqual(value, {"status": 200, "body": {"ok": True}})
        self.assertTrue(saved[0]["raw_body_complete"])
        self.assertEqual(saved[0]["credential_role"], "service")
        self.assertNotIn("synthetic-token", json.dumps(saved))
        self.assertEqual(saved[0]["body_sha256"], driver.digest(b'{"ok":true}'))

    def test_http_rejects_truncated_ambiguous_and_oversized_frames(self):
        frames = [(b'HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n{"ok":true}', 100),
                  (b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Length: 11\r\n\r\n{"ok":true}', 100),
                  (b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 11\r\n\r\nb\r\n{"ok":true}\r\n0\r\n\r\n', 100),
                  (b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nb\r\n{"ok":true}\r\n', 100),
                  (b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{"ok":true}', 5)]
        for frame, limit in frames:
            with self.subTest(frame=frame, limit=limit):
                value, saved = self.request_memory(frame, limit=limit)
                self.assertIsInstance(value, ValueError)
                self.assertFalse(saved[0]["raw_body_complete"])
                self.assertIn("capture_error", saved[0])

    def test_missing_chunk_terminator_keeps_full_prefix_without_completeness(self):
        value, saved = self.request_memory(b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nb\r\n{"ok":true}\r\n')
        self.assertIsInstance(value, ValueError)
        self.assertFalse(saved[0]["raw_body_complete"])
        self.assertEqual(driver.base64.b64decode(saved[0]["raw_body_base64"]), b'{"ok":true}')

    def test_http_preserves_framed_invalid_json_but_rejects_it(self):
        body = b'{"x":1e999}'
        value, saved = self.request_memory(b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
        self.assertIsInstance(value, ValueError)
        self.assertTrue(saved[0]["raw_body_complete"], "framing completeness must not imply JSON validity")

    def test_request_and_process_caps_fail_before_outbound_or_child(self):
        result = self.stub(Path("/never-written"))
        result.requests = result.cases["limits"]["http_calls"]
        result.serial = result.cases["limits"]["processes"]
        with patch.object(driver.http.client, "HTTPConnection", side_effect=AssertionError("outbound")), \
             patch.object(driver, "OwnedProcess", side_effect=AssertionError("child")):
            with self.assertRaisesRegex(ValueError, "HTTP attempt"):
                result.http("GET", "/v1/health")
            with self.assertRaisesRegex(ValueError, "process count"):
                result.process(["--version"])

    def test_expiry_and_elapsed_time_are_independent_limits(self):
        result = self.stub(Path("/never-written"))
        result.manifest["expires_at"] = "2000-01-01T00:00:00Z"
        with self.assertRaisesRegex(ValueError, "allocation expired"):
            result.remaining()
        result.manifest["expires_at"] = "2999-01-01T00:00:00Z"
        result.started -= result.cases["limits"]["total_seconds"] + 1
        with self.assertRaisesRegex(ValueError, "run deadline"):
            result.remaining()

    def test_owned_directory_and_environment_are_fresh(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake_socket = Mock()
            fake_socket.__enter__ = Mock(return_value=fake_socket)
            fake_socket.__exit__ = Mock(return_value=False)
            fake_socket.getsockname.return_value = ("127.0.0.1", 45678)
            with patch.object(driver.socket, "socket", return_value=fake_socket), \
                 patch.dict(driver.os.environ, {"OPENAI_API_KEY": "ambient", "SKIP_AUTH": "true", "JWT_SECRET": "ambient"}):
                value = driver.Driver({"run_directory": str(root / "new"), "source_commit": "a", "archive_sha256": "b",
                                       "binary_sha256": "c", "backend": "elephant-db", "expires_at": "2999-01-01T00:00:00Z"}, driver.plan(), {})
                self.assertEqual(value.environment["HOME"], str(root / "new/private"))
                for key in ("OPENAI_API_KEY", "SKIP_AUTH", "JWT_SECRET"):
                    self.assertNotIn(key, value.environment)
                self.assertNotEqual(value.environment["ELEPHANT_DURABILITY_PATH"], str(root / "new/service"))
                with self.assertRaisesRegex(ValueError, "must be new"):
                    driver.Driver(value.manifest, driver.plan(), {})

    def test_private_capture_rejects_overwrite_and_storage_overrun(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "captures").mkdir()
            result = self.stub(root)
            result.save("first", b"original")
            self.assertEqual((root / "captures/first").stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                result.save("first", b"replacement")
            result.capture_bytes = result.cases["limits"]["http_capture_bytes"]
            with self.assertRaisesRegex(ValueError, "storage limit"):
                result.save("second", b"x")
            self.assertFalse((root / "captures/second").exists())

    def test_platform_process_hook_preserves_explicit_arguments_caps_and_owned_cleanup(self):
        value = self.stub(Path("/never-written"))
        owned = Mock()
        owned.wait.return_value = 0
        with patch.object(driver, "OwnedProcess", return_value=owned) as factory:
            self.assertEqual(value.process(["--version"]), "process-001")
            factory.assert_called_once_with(value.binary, ["--version"], value.root / "service", value.environment,
                [value.root / "captures/process-001.stdout", value.root / "captures/process-001.stderr"],
                value.cases["limits"]["process_log_bytes"], initialization_deadline=ANY)
        owned.wait.assert_called_once()
        replacement = Mock()
        replacement.wait.return_value = 0
        class ReviewedPlatform(driver.Driver):
            def create_process(self, arguments, log_paths):
                self.hook_observation = (arguments, log_paths)
                return replacement
        platform = ReviewedPlatform.__new__(ReviewedPlatform)
        platform.__dict__.update(self.stub(Path("/never-written")).__dict__)
        with patch.object(driver, "OwnedProcess", side_effect=AssertionError("wrong platform custody")):
            self.assertEqual(platform.process(["--version"]), "process-001")
        self.assertEqual(platform.hook_observation[0], ["--version"])
        replacement.wait.assert_called_once()

    def test_platform_target_is_source_owned_and_checked_before_candidate_execution(self):
        class ReviewedDarwin(driver.Driver):
            expected_target = "aarch64-apple-darwin"
            observation_format = "recall-installed-darwin-observation/v1"
        for cls, wrong_target in ((driver.Driver, "aarch64-apple-darwin"), (ReviewedDarwin, "x86_64-unknown-linux-gnu")):
            value = cls.__new__(cls)
            value.__dict__.update(self.stub(Path("/never-written")).__dict__)
            value.manifest.update(archive="/never-opened", archive_sha256="a" * 64, expected_target=wrong_target)
            value.process = Mock(side_effect=AssertionError("no candidate before target proof"))
            with patch.object(driver.artifact, "verify", return_value={"target": wrong_target}), \
                 patch.object(driver.capture, "total_deadline", return_value=nullcontext()), \
                 self.assertRaisesRegex(ValueError, "wrong installed target"):
                value.execute()
            value.process.assert_not_called()
        self.assertEqual(driver.Driver.expected_target, "x86_64-unknown-linux-gnu")
        self.assertEqual(driver.Driver.observation_format, "recall-installed-linux-observation/v1")

    def test_log_overflow_kills_only_owned_group_and_keeps_bounded_prefix(self):
        child = driver.OwnedProcess.__new__(driver.OwnedProcess)
        child.process = Mock()
        child.process.poll.return_value = None
        child.pid = 123456
        child.failed = threading.Event()
        child.group_lock = threading.Lock()
        child.reaped = False
        stream, log = io.BytesIO(b"0123456789"), KeptBytes()
        with patch.object(driver.os, "killpg") as kill:
            child.drain(stream, log, 5)
        self.assertEqual(log.saved, b"01234")
        self.assertTrue(child.failed.is_set())
        kill.assert_called_once_with(123456, signal.SIGKILL)

    def test_graceful_timeout_reaps_child_and_cannot_pass(self):
        result = self.stub(Path("/never-written"))
        child = Mock()
        child.reaped = True
        child.poll.return_value = None
        child.wait.side_effect = [subprocess.TimeoutExpired("owned", 1), -9]
        result.child = child
        with self.assertRaisesRegex(ValueError, "did not drain"):
            result.stop()
        self.assertIsNone(result.child)
        self.assertEqual(child.wait.call_count, 2)
        self.assertEqual([call.args[0] for call in child.signal.call_args_list], [signal.SIGTERM, signal.SIGKILL])

    def test_exited_leader_keeps_pid_reserved_for_owned_descendant_cleanup(self):
        child = driver.OwnedProcess.__new__(driver.OwnedProcess)
        child.process = Mock()
        child.process.wait.return_value = 0
        child.process.returncode = 0
        child.pid = 123456
        child.failed = threading.Event()
        child.group_lock = threading.Lock()
        child.reaped = False
        pipe = Mock()
        pipe.is_alive.side_effect = [True, False]
        child.threads = [pipe]
        observed = SimpleNamespace(si_status=0, si_code=driver.os.CLD_EXITED)
        ordering = []
        child.process.wait.side_effect = lambda **_kwargs: ordering.append("reap") or 0
        with patch.object(driver.os, "waitid", return_value=observed) as waitid, \
             patch.object(driver.os, "killpg", side_effect=lambda *_args: ordering.append("kill")) as kill:
            with self.assertRaisesRegex(ValueError, "capture failed"):
                child.wait(timeout=1)
            self.assertEqual(waitid.call_args.args[2] & driver.os.WNOWAIT, driver.os.WNOWAIT)
            self.assertTrue(child.reaped)
            self.assertEqual(ordering, ["kill", "kill", "reap"])
            self.assertEqual(pipe.join.call_count, 2)
            child.signal(signal.SIGKILL)
            self.assertEqual(kill.call_count, 2, "reaped PID must never be signaled again")

    def test_first_or_second_log_thread_start_failure_keeps_original_owned_custody(self):
        for failed_index in (0, 1):
            with self.subTest(failed_index=failed_index), tempfile.TemporaryDirectory() as directory:
                process = Mock(pid=123456, stdout=io.BytesIO(), stderr=io.BytesIO())
                events, threads = [], []
                process.wait.side_effect = lambda **_kwargs: events.append("reap") or -9
                def make_thread(**kwargs):
                    index = len(threads)
                    thread = Mock(ident=None)
                    thread.is_alive.return_value = False
                    def start():
                        if index == failed_index:
                            raise RuntimeError("controlled thread start failure")
                        thread.ident = index + 1
                    thread.start.side_effect = start
                    threads.append((thread, kwargs["args"][1]))
                    return thread
                with patch.object(driver.subprocess, "Popen", return_value=process), \
                     patch.object(driver.threading, "Thread", side_effect=make_thread), \
                     patch.object(driver.os, "killpg", side_effect=lambda *_args: events.append("kill")) as kill:
                    with self.assertRaisesRegex(RuntimeError, "thread start") as caught:
                        driver.OwnedProcess(Path("/never-executed"), [], Path(directory), {},
                            [Path(directory) / "out", Path(directory) / "err"], 1024,
                            initialization_deadline=time.monotonic() + 1)
                    owned = caught.exception.owned_process
                    self.assertTrue(owned.reaped)
                    self.assertEqual(caught.exception.owned_cleanup_errors, ())
                    self.assertTrue(all(log.closed for log in owned.logs))
                    self.assertTrue(process.stdout.closed and process.stderr.closed)
                    self.assertEqual(events, ["kill", "kill", "reap"])
                    self.assertTrue(all(call.args == (123456, signal.SIGKILL) for call in kill.call_args_list))
                    owned.signal(signal.SIGKILL)
                    self.assertEqual(kill.call_count, 2)
                    process.wait.assert_called_once()
                    self.assertGreaterEqual(process.wait.call_args.kwargs["timeout"], 0)
                    self.assertLessEqual(process.wait.call_args.kwargs["timeout"], 1)
                    for thread, _log in threads:
                        self.assertEqual(thread.join.call_count, int(thread.ident is not None))

    def test_partial_log_open_failure_closes_first_log_without_spawning(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "existing").write_bytes(b"preserve")
            opened = []
            fdopen = driver.os.fdopen
            def remember(*args, **kwargs):
                stream = fdopen(*args, **kwargs)
                opened.append(stream)
                return stream
            with patch.object(driver.os, "fdopen", side_effect=remember), \
                 patch.object(driver.subprocess, "Popen") as spawn, self.assertRaises(FileExistsError):
                driver.OwnedProcess(Path("/never-executed"), [], root, {}, [root / "new", root / "existing"], 1024)
            spawn.assert_not_called()
            self.assertEqual(len(opened), 1)
            self.assertTrue(opened[0].closed)
            self.assertEqual((root / "existing").read_bytes(), b"preserve")

    def test_failed_constructor_cleanup_retains_unreaped_handle_and_does_not_reset_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            process = Mock(pid=123456, stdout=io.BytesIO(), stderr=io.BytesIO())
            process.wait.side_effect = subprocess.TimeoutExpired("controlled unreaped leader", 0)
            thread = Mock(ident=None)
            thread.start.side_effect = RuntimeError("controlled startup failure")
            thread.is_alive.return_value = False
            with patch.object(driver.subprocess, "Popen", return_value=process), \
                 patch.object(driver.threading, "Thread", return_value=thread), \
                 patch.object(driver.os, "killpg"), \
                 patch.object(driver.time, "monotonic", side_effect=[10, 12]), \
                 self.assertRaisesRegex(RuntimeError, "startup failure") as caught:
                driver.OwnedProcess(Path("/never-executed"), [], Path(directory), {},
                    [Path(directory) / "out", Path(directory) / "err"], 1024, initialization_deadline=11)
            self.assertFalse(caught.exception.owned_process.reaped)
            self.assertIn("reap-owned-leader:TimeoutExpired", caught.exception.owned_cleanup_errors)
            process.wait.assert_called_once_with(timeout=0)
            self.assertTrue(all(log.closed for log in caught.exception.owned_process.logs))

    def test_failed_final_group_signal_never_reaps_an_already_exited_leader(self):
        with tempfile.TemporaryDirectory() as directory:
            process = Mock(pid=123456, stdout=io.BytesIO(), stderr=io.BytesIO(), returncode=0)
            process.wait.return_value = 0  # Reaping would succeed, but lose the group reservation.
            thread = Mock(ident=None)
            thread.start.side_effect = RuntimeError("controlled startup failure")
            thread.is_alive.return_value = False
            with patch.object(driver.subprocess, "Popen", return_value=process), \
                 patch.object(driver.threading, "Thread", return_value=thread), \
                 patch.object(driver.os, "killpg", side_effect=PermissionError("controlled signal failure")) as kill, \
                 self.assertRaisesRegex(RuntimeError, "startup failure") as caught:
                driver.OwnedProcess(Path("/never-executed"), [], Path(directory), {},
                    [Path(directory) / "out", Path(directory) / "err"], 1024,
                    initialization_deadline=time.monotonic() + 1)
            self.assertFalse(caught.exception.owned_process.reaped)
            self.assertEqual(caught.exception.owned_process.pid, 123456)
            self.assertEqual(caught.exception.owned_cleanup_errors,
                             ("kill-owned-group:PermissionError", "reap-owned-leader:PermissionError"))
            self.assertEqual(kill.call_count, 2)
            process.wait.assert_not_called()

    def test_driver_keeps_constructor_failure_handle_alongside_live_service(self):
        result = self.stub(Path("/never-written"))
        service, command = Mock(reaped=False), Mock(reaped=False)
        result.child = service
        error = RuntimeError("controlled constructor failure")
        error.owned_process = command
        result.create_process = Mock(side_effect=error)
        with self.assertRaisesRegex(RuntimeError, "constructor failure"):
            result.process(["service-token", "issue"])
        self.assertIs(result.child, service)
        self.assertEqual(result.owned_commands, [command])
        service.poll.return_value = command.poll.return_value = None
        service.signal.side_effect = PermissionError("controlled first handle refusal")
        def reap(**_kwargs):
            command.reaped = True
            return 0
        command.wait.side_effect = reap
        with self.assertRaises(PermissionError):
            result.stop()
        self.assertIs(result.child, service)
        service.wait.assert_not_called()
        command.signal.assert_called_once_with(signal.SIGTERM)
        command.wait.assert_called_once()
        self.assertEqual(result.owned_commands, [])

    def test_driver_cleanup_all_handles_share_original_expired_allocation(self):
        result = self.stub(Path("/never-written"))
        result.started = 0
        result.cases["limits"]["total_seconds"] = 1
        handles = [Mock(reaped=False), Mock(reaped=False)]
        result.child, result.owned_commands = handles[0], [handles[1]]
        for handle in handles:
            handle.poll.return_value = None
            handle.wait.side_effect = subprocess.TimeoutExpired("controlled unreaped leader", 0)
        with patch.object(driver.time, "monotonic", return_value=2), self.assertRaises(subprocess.TimeoutExpired):
            result.stop()
        for handle in handles:
            self.assertEqual([call.kwargs["timeout"] for call in handle.wait.call_args_list], [0, 0])
            self.assertEqual([call.args[0] for call in handle.signal.call_args_list], [signal.SIGTERM, signal.SIGKILL])
        self.assertIs(result.child, handles[0])
        self.assertEqual(result.owned_commands, [handles[1]])

    def test_driver_retains_command_when_its_error_cleanup_signal_fails(self):
        result = self.stub(Path("/never-written"))
        command = Mock(reaped=False)
        command.wait.side_effect = subprocess.TimeoutExpired("controlled command", 1)
        command.signal.side_effect = PermissionError("controlled group refusal")
        result.create_process = Mock(return_value=command)
        with self.assertRaises(PermissionError):
            result.process(["--version"])
        self.assertEqual(result.owned_commands, [command])
        self.assertIsNone(result.child)

    def test_process_wait_shares_original_deadline_with_every_log_join_and_reap(self):
        child = driver.OwnedProcess.__new__(driver.OwnedProcess)
        child.pid, child.reaped = 123456, False
        child.failed, child.group_lock = threading.Event(), threading.Lock()
        child.process = Mock(returncode=0)
        child.process.wait.return_value = 0
        clock = [100.0]
        joins = []
        def join(*, timeout):
            joins.append(timeout)
            clock[0] += timeout
        child.threads = [Mock(ident=1), Mock(ident=2)]
        for thread in child.threads:
            thread.join.side_effect = join
            thread.is_alive.return_value = False
        with patch.object(driver.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(driver.os, "waitid", return_value=SimpleNamespace(si_status=0, si_code=driver.os.CLD_EXITED)), \
             patch.object(driver.os, "killpg"):
            self.assertEqual(child.wait(0.25), 0)
        self.assertEqual(joins, [0.25, 0])
        child.process.wait.assert_called_once_with(timeout=0)
        self.assertTrue(child.reaped)

    def test_driver_retains_handle_if_group_cleanup_did_not_reap(self):
        result = self.stub(Path("/never-written"))
        child = Mock()
        child.reaped = False
        child.poll.return_value = 0
        child.wait.side_effect = ValueError("owned capture failure before reap")
        result.child = child
        with self.assertRaises(ValueError):
            result.stop()
        self.assertIs(result.child, child, "final cleanup still needs the original owned group")

    def test_named_configuration_checks_real_readback_and_stable_identity(self):
        result = self.stub(Path("/never-written"))
        space = driver.fixtures.corpus()["spaces"][0]
        body = {"id": "space_config:original", "space_id": space["id"], "updated_at": "before",
                "security": {"policy_id": space["policy_id"], "level": space["level"], "labels": [], "handling": []},
                "embedding_provider": "openai", "embedding_model": f"installed-unapproved-{space['id']}",
                "embedding_dims": space["dimensions"], "purpose": space["extraction_purpose"], "skill": space["extraction_skill"],
                "processing": {"generation": 1, "index_generation": 0, "policy_id": space["policy_id"],
                    "connections": {}, "profiles": {}, "bindings": {}, "active_embedding_profile_id": None, "pending_reindex": None}}
        result.http = Mock(return_value={"status": 200, "body": body})
        baseline = result.configuration(space)
        result.http.assert_called_once_with("GET", f"/v1/spaces/{space['id']}", "service", space["id"])
        changed = copy.deepcopy(body)
        changed["updated_at"] = "source-head-write"
        changed["processing"]["index_generation"] = 4
        result.http.return_value = {"status": 200, "body": changed}
        self.assertEqual(result.configuration(space), baseline)
        mutations = [("space_id", "wrong"), ("embedding_dims", 1536), ("embedding_provider", "fake"),
                     ("embedding_model", "ignored"), ("purpose", "ignored"), ("skill", "ignored"),
                     ("security", dict(body["security"], level=99))]
        for key, value in mutations:
            with self.subTest(key=key):
                result.http.return_value = {"status": 200, "body": dict(body, **{key: value})}
                with self.assertRaises(ValueError):
                    result.configuration(space)
        for key, value in (("generation", 2), ("bindings", {"query_embedding": {}}), ("index_generation", True)):
            with self.subTest(processing=key):
                changed = copy.deepcopy(body)
                changed["processing"][key] = value
                result.http.return_value = {"status": 200, "body": changed}
                with self.assertRaises(ValueError):
                    result.configuration(space)

    def test_denial_must_be_typed_nonretryable_and_payload_free(self):
        body = {"error_code": "forbidden_space", "message": "Forbidden", "retryable": False}
        driver.typed_denial(body, "forbidden_space", ["secret-bearer"])
        for changed in (dict(body, retryable=True), dict(body, error_code="unavailable"),
                        dict(body, data={}), dict(body, message="secret-bearer"),
                        dict(body, message=driver.fixtures.corpus()["sources"][0]["text"])):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                driver.typed_denial(changed, "forbidden_space", ["secret-bearer"])

    def test_cleanup_failure_overrides_success_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.stub(Path(directory))
            result.execute = Mock()
            result.stop = Mock(side_effect=ValueError("owned pipe did not close"))
            self.assertEqual(result.run(), 1)
            receipt = driver.read_json(Path(directory) / "observation.json")
            self.assertEqual(receipt["subset_observation"], "failed")
            self.assertFalse(receipt["product_acceptance"])


if __name__ == "__main__":
    unittest.main()
