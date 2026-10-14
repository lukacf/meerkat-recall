"""Pure file-carrier controls; synthetic bytes, no gateway/process/provider."""
from pathlib import Path
import tempfile
import unittest

import launch_context as context


class PrimaryInputCapacityTests(unittest.TestCase):
    def test_two_original_backend_capture_sets_stage_byte_exactly(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source, staged = root / "source", root / "staged"
            source.mkdir(mode=0o700)
            for backend in ("surreal-db", "elephant-db"):
                directory = source / backend
                directory.mkdir(mode=0o700)
                for index in range(259):
                    path = directory / (str(index) + ".json")
                    path.write_bytes(context.encode({"synthetic_original": backend, "index": index}))
                    path.chmod(0o600)
            original = context.input_inventory(source)
            self.assertEqual(len(original["files"]), 518)
            expected = context.digest(context.encode(original))
            context.stage_inputs(source, staged, expected)
            self.assertEqual(context.input_inventory(staged), original)
            self.assertEqual(context.input_inventory(source), original)

    def test_1025th_original_file_exceeds_bounded_carrier(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary).resolve()
            for index in range(1025):
                path = source / str(index)
                path.write_bytes(b"synthetic original")
                path.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "launch_inputs_file_type_or_count"):
                context.input_inventory(source)


if __name__ == "__main__":
    unittest.main()
