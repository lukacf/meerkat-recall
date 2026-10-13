"""Focused failure-evidence controls; no package, tool or runtime execution."""
import json
from pathlib import Path
import tempfile
import unittest

import prepare_gateway_runtime as packager


class PackagingFailureDiagnosticsTests(unittest.TestCase):
    def test_original_failure_stage_without_payload_or_locals(self):
        secret = "synthetic-private-payload-that-must-not-appear"
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary).resolve() / "prospective-runtime"
            packager.diagnostic_stage("debian_package", 17)
            try:
                raise ValueError(secret)
            except ValueError as error:
                reference = packager.failure_diagnostic(error, output)
            raw = Path(reference["path"]).read_bytes()
            record = json.loads(raw)
            self.assertNotIn(secret.encode(), raw)
            self.assertEqual(record["stage"], "debian_package")
            self.assertEqual(record["item_index"], 17)
            self.assertEqual(record["exception_type"], "ValueError")
            self.assertFalse(record["exception_message_recorded"])
            self.assertFalse(record["locals_or_input_payloads_recorded"])
            self.assertTrue(record["source_frames"])
            self.assertEqual(reference["bytes"], len(raw))
            self.assertLessEqual(len(raw), 16384)
            self.assertEqual(Path(reference["path"]).stat().st_mode & 0o777, 0o600)

    def test_existing_failure_evidence_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary).resolve() / "prospective-runtime"
            evidence = output.with_name(output.name + ".failure.json")
            evidence.write_bytes(b"retained original failure\n")
            packager.diagnostic_stage("paired_elf")
            with self.assertRaises(FileExistsError):
                packager.failure_diagnostic(ValueError("private"), output)
            self.assertEqual(evidence.read_bytes(), b"retained original failure\n")


if __name__ == "__main__":
    unittest.main()
