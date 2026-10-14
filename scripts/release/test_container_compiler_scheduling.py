"""Check build scheduling boundaries without Docker or subprocess execution."""
from pathlib import Path
import shlex
import unittest


ROOT = Path(__file__).resolve().parents[2]
IDLE_PREFIX = ["chrt", "--idle", "0", "ionice", "-c3"]


class ContainerCompilerSchedulingTests(unittest.TestCase):
    def test_paired_python_build_inherits_idle_scheduling(self):
        source = (ROOT / "packaging/recall-surreal/Dockerfile.build").read_text()
        commands = source.replace("\\\n", " ").split("&&")
        builds = [command for command in commands if ".recall/build.py" in command]
        self.assertEqual(len(builds), 1)
        command = builds[0].split("\n", 1)[0]
        self.assertEqual(
            shlex.split(command),
            IDLE_PREFIX
            + ["python3", ".recall/build.py", "--prepared", "/source", "--bundle", "/paired-bundle"],
        )

    def test_recall_provenance_python_inherits_idle_scheduling(self):
        source = (ROOT / "Dockerfile").read_text()
        lines = [line for line in source.splitlines() if line.startswith("RUN SOURCE_COMMIT=")]
        self.assertEqual(len(lines), 1)
        command = shlex.split(lines[0])[1:]
        expected_environment = [
            "SOURCE_COMMIT=$SOURCE_COMMIT",
            "SOURCE_TREE=$SOURCE_TREE",
            "SOURCE_SHA256=$SOURCE_SHA256",
            "BUILDER_IMAGE=$BUILDER_IMAGE",
            "RUNTIME_IMAGE=$RUNTIME_IMAGE",
        ]
        self.assertEqual(command[:5], expected_environment)
        self.assertEqual(command[5:], IDLE_PREFIX + ["python3", "-", "<<PY"])


if __name__ == "__main__":
    unittest.main()
