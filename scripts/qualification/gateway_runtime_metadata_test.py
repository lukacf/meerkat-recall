"""Finite metadata boundary/diagnostic controls; no runtime or provider calls."""
from pathlib import Path
import json
import sys
import tempfile
import unittest

GATEWAY = Path(__file__).resolve().parents[2] / "tests/recall_preview_semantics/gateway"
sys.path.insert(0, str(GATEWAY))
try:
    import launcher
    import wire
finally:
    sys.path.pop(0)
if Path(launcher.__file__).resolve() != GATEWAY / "launcher.py":
    raise RuntimeError("wrong selected launcher source")


class GatewayRuntimeMetadataTests(unittest.TestCase):
    def test_large_artifact_metadata_keeps_request_limit(self):
        value = {"format": "recall-gateway-runtime-provenance/v1",
                 "files": {"controlled-metadata": "x" * (wire.MAX_BODY_BYTES + 1)}}
        raw = json.dumps(value).encode()
        self.assertEqual(launcher.runtime_package_metadata(raw), value)
        with self.assertRaises(wire.Denied):
            wire.strict_json(raw)
        self.assertEqual(wire.MAX_BODY_BYTES, 131072)

    def test_metadata_strictness_and_explicit_bound(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}',
                    b'{"a":"\\ud800"}', b'[]', b'{} trailing', b'{"a":"\xff"}',
                    b' ' * (launcher.PACKAGE_METADATA_CAP + 1)):
            with self.subTest(prefix=raw[:16]), self.assertRaises((ValueError, wire.Denied)):
                launcher.runtime_package_metadata(raw)
        self.assertEqual(launcher.PACKAGE_METADATA_CAP, 32 * 1024**2)

    def test_failure_diagnostic_excludes_payload_and_preserves_original(self):
        marker = "NEVER_RECORD_CREDENTIAL_OR_PROVIDER_BODY_7ad1"
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary) / "new-run"
            prior_stage = launcher.LAUNCH_STAGE
            launcher.mark_stage("source_control")
            try:
                try:
                    launcher.require(False, marker)
                except wire.Denied as error:
                    reference = launcher.failure_diagnostic(error, run)
                    path = Path(reference["path"])
                    raw = path.read_bytes()
                    value = json.loads(raw)
                    self.assertNotIn(marker.encode(), raw)
                    self.assertFalse(value["exception_message_recorded"])
                    self.assertFalse(value["locals_or_payloads_recorded"])
                    self.assertEqual(value["exception_type"], "Denied")
                    self.assertEqual(value["stage"], "source_control")
                    self.assertTrue(value["source_frames"])
                    self.assertLessEqual(len(raw), 16384)
                    with self.assertRaises(FileExistsError):
                        launcher.failure_diagnostic(error, run)
                    self.assertEqual(path.read_bytes(), raw)
            finally:
                launcher.mark_stage(prior_stage)


if __name__ == "__main__":
    unittest.main()
