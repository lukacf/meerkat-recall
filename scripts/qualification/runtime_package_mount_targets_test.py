"""Cross-component primary mount-target contract; no namespace/runtime calls."""
from pathlib import Path
import hashlib
import sys
import unittest
from unittest.mock import patch

import prepare_gateway_runtime as packager

GATEWAY = Path(__file__).resolve().parents[2] / "tests/recall_preview_semantics/gateway"
sys.path.insert(0, str(GATEWAY))
try:
    import launcher
finally:
    sys.path.pop(0)
if Path(launcher.__file__).resolve() != GATEWAY / "launcher.py":
    raise RuntimeError("wrong selected launcher source")


class RecordingMountTargets:
    """Record the real packager API calls without building a runtime tree."""
    def __init__(self):
        self.directories = set()
        self.files = {}

    def directory(self, name):
        self.directories.add(name)

    def file(self, name, stream, size, expected, executable, origin):
        raw = stream.read()
        self.files[name] = (raw, size, expected, executable, origin)


class RuntimeMountTargetTests(unittest.TestCase):
    def test_actual_primary_launcher_bind_destinations_have_packaged_targets(self):
        targets = RecordingMountTargets()
        packager.mount_targets(targets)
        with patch.object(launcher.shutil, "which", return_value="/usr/bin/bwrap"), \
                patch.object(launcher.os, "readlink", return_value="net:[controlled]"):
            argv = launcher.launch_command(
                Path("/controlled/runtime"), Path("/controlled/work"),
                Path("/controlled/gateway.sock"), Path("/controlled/source/bridge.py"),
                ["/usr/bin/python3", "-V"], {}, Path("/controlled/run/anchor.json"),
                Path("/controlled/input-tree"))
        destinations = [argv[index + 2] for index, token in enumerate(argv)
                        if token in ("--ro-bind", "--bind") and argv[index + 2] != "/"]
        self.assertTrue(destinations)
        for destination in destinations:
            with self.subTest(destination=destination):
                name = destination.removeprefix("/")
                self.assertIn(name, targets.files.keys() | targets.directories)
        for name, (raw, size, checksum, executable, origin) in targets.files.items():
            with self.subTest(packaged_file=name):
                self.assertEqual(raw, b"")
                self.assertEqual(size, 0)
                self.assertEqual(checksum, hashlib.sha256(raw).hexdigest())
                self.assertFalse(executable)
                self.assertEqual(origin["generated"], "empty read-only gateway mount target")


if __name__ == "__main__":
    unittest.main()
