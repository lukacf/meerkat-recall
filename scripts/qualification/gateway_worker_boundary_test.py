"""Finite controlled startup contracts, never namespace/provider acceptance."""
from pathlib import Path
import copy
import io
import json
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

import runtime_package_bootstrap as bootstrap
import runtime_package_bootstrap_io as bootstrap_io

ROOT = Path(__file__).resolve().parents[2]
GATEWAY = ROOT / "tests/recall_preview_semantics/gateway"
sys.path.insert(0, str(GATEWAY))
try:
    import bridge
    import launch_context as context
    from test_launch_context import observation
finally:
    sys.path.pop(0)
if Path(bridge.__file__).resolve() != GATEWAY / "bridge.py":
    raise RuntimeError("wrong selected bridge source")

MARKER = "NEVER_RECORD_CREDENTIAL_PAYLOAD_ARGV_OR_LOCAL_938a"


class GatewayWorkerBoundaryTests(unittest.TestCase):
    def setUp(self):
        for name in ("socket.socket", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("no network or processes in pure controls"))
            guard.start()
            self.addCleanup(guard.stop)

    def read_diagnostic(self, path):
        raw = path.read_bytes()
        value = json.loads(raw)
        self.assertLessEqual(len(raw), 16384)
        self.assertNotIn(MARKER.encode(), raw)
        self.assertFalse(value["exception_message_recorded"])
        self.assertFalse(value["locals_or_payloads_recorded"])
        self.assertFalse(value["semantic_acceptance"])
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertLessEqual(len(value["source_frames"]), 16)
        for frame in value["source_frames"]:
            self.assertEqual(set(frame), {"file", "function", "line"})
            self.assertFalse(Path(frame["file"]).is_absolute())
        return raw, value

    def test_bridge_records_exception_and_distinguishes_child_exit_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bridge.failure.json"
            with patch.object(bridge, "FAILURE_PATH", path), \
                    patch.object(bridge, "BRIDGE_STAGE", "runtime_limits"), \
                    patch.object(bridge, "run", side_effect=RuntimeError(MARKER)):
                self.assertEqual(bridge.main(), 78)
            raw, value = self.read_diagnostic(path)
            self.assertEqual(value["stage"], "runtime_limits")
            self.assertEqual(value["exception_type"], "RuntimeError")
            self.assertIsNone(value["child_returncode"])
            self.assertTrue(value["source_frames"])
            with patch.object(bridge, "FAILURE_PATH", path), patch.object(bridge, "run", return_value=9):
                self.assertEqual(bridge.main(), 9)
            self.assertEqual(path.read_bytes(), raw)
            child_path = Path(temporary) / "child.failure.json"
            with patch.object(bridge, "FAILURE_PATH", child_path), patch.object(bridge, "run", return_value=78):
                self.assertEqual(bridge.main(), 78)
            _, child = self.read_diagnostic(child_path)
            self.assertEqual(child["stage"], "worker_child_exit")
            self.assertEqual(child["child_returncode"], 78)
            self.assertIsNone(child["exception_type"])
            self.assertEqual(child["source_frames"], [])

    def test_bootstrap_actual_catch_records_pre_instance_stage_using_shared_writer(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bootstrap.failure.json"
            argv = ["bootstrap", "--execute-authorized", "--phase", "/plan", "/review", "/template"]
            with patch.object(bootstrap, "FAILURE_PATH", path), patch.object(sys, "argv", argv), \
                    patch.object(bootstrap, "static_input", side_effect=ValueError(MARKER)), \
                    patch.object(sys, "stderr", io.StringIO()):
                self.assertEqual(bootstrap.main(), 78)
            _, value = self.read_diagnostic(path)
            self.assertEqual(value["stage"], "phase_inputs")
            self.assertEqual(value["exception_type"], "ValueError")
            self.assertIn("phase_inputs", {row["function"] for row in value["source_frames"]})
            self.assertTrue(all(row["file"].startswith("scripts/qualification/")
                                for row in value["source_frames"]))

    def project(self, work, value, *, changed_digest=False, anchor=None):
        reference = context.write_original(work / "context.json", context.encode(value))
        if changed_digest:
            reference["sha256"] = "0" * 64
        with patch.object(bridge.time, "time", return_value=1002.0), \
                patch.object(bridge.time, "monotonic", return_value=502.0):
            return bridge.write_isolation(context, reference,
                value["anchor"] if anchor is None else anchor, work)

    def test_actual_context_projection_satisfies_existing_consumer_and_is_exclusive(self):
        # The existing pure context fixture is explicitly synthetic. The actual
        # context validator, producer and bootstrap consumer execute unchanged.
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            value = observation()
            reference = self.project(work, value)
            raw = context.read_bounded(Path(reference["path"]), private=True)
            self.assertEqual(context.digest(raw), reference["sha256"])
            proof = context.strict_json(raw)
            self.assertEqual(proof["network_namespace"], value["network"]["namespace"])
            self.assertEqual(proof["parent_network_namespace"], value["network"]["parent_namespace"])
            def proc_bytes(path, *args, **kwargs):
                if Path(path).name == "dev":
                    return b"Inter-| Receive\n lo: 0 0 0 0\n"
                if Path(path).name == "route":
                    return b"Iface Destination Gateway\n"
                raise AssertionError("unexpected proc read")
            with patch.object(bootstrap_io.os, "readlink", return_value="net:[2000]"), \
                    patch.object(bootstrap_io.Path, "resolve", lambda path: path), \
                    patch.object(bootstrap_io, "read_bound", side_effect=proc_bytes):
                self.assertEqual(bootstrap_io.verify_isolation(proof), proof)
                changed = {**proof, "provider_credentials_present": True}
                with self.assertRaises(ValueError):
                    bootstrap_io.verify_isolation(changed)
            reference_context = {"path": str(work / "context.json"),
                                 "sha256": context.digest((work / "context.json").read_bytes())}
            with patch.object(bridge.time, "time", return_value=1002.0), \
                    patch.object(bridge.time, "monotonic", return_value=502.0), \
                    self.assertRaises(FileExistsError):
                bridge.write_isolation(context, reference_context, value["anchor"], work)
            self.assertEqual((work / "isolation.json").read_bytes(), raw)

    def test_changed_context_network_clock_or_anchor_never_writes_isolation(self):
        for case in ("digest", "same_namespace", "foreign_interface", "expired", "anchor"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                value = observation()
                original = copy.deepcopy(value["anchor"])
                if case == "same_namespace":
                    value["network"]["namespace"] = value["network"]["parent_namespace"]
                elif case == "foreign_interface":
                    value["network"]["interfaces"] = ["lo", "eth0"]
                elif case == "expired":
                    value["observed_mono"] = 5901.0
                elif case == "anchor":
                    original["run_nonce"] = "different-original-run-001"
                work = Path(temporary)
                with self.assertRaises((ValueError, RuntimeError)):
                    self.project(work, value, changed_digest=case == "digest", anchor=original)
                self.assertFalse((work / "isolation.json").exists())


if __name__ == "__main__":
    unittest.main()
