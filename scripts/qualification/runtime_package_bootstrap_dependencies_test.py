"""Pure actual producer/loader closure controls; no runtime/provider acceptance."""
from pathlib import Path
import hashlib
import importlib.util
import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import runtime_package_bootstrap_io as startup

ROOT = Path(__file__).resolve().parents[2]
PROFILE = "tests/recall_preview_semantics/workload_profiles.py"
SELF = Path(__file__).resolve().relative_to(ROOT).as_posix()
# Root selects the actual private input producer; source preparation never
# imports it. This variable names code, not a credential or runtime authority.
PRODUCER_PATH = Path(os.environ["RECALL_PRIMARY_PRODUCER_SOURCE"])
PRODUCER_SHA = os.environ["RECALL_PRIMARY_PRODUCER_SHA256"]
raw = PRODUCER_PATH.read_bytes()
if hashlib.sha256(raw).hexdigest() != PRODUCER_SHA:
    raise ValueError("selected primary input producer changed")
spec = importlib.util.spec_from_file_location("controlled_primary_producer", PRODUCER_PATH)
producer = importlib.util.module_from_spec(spec)
exec(compile(raw, str(PRODUCER_PATH), "exec", dont_inherit=True), producer.__dict__)


class StartupDependencyClosureTests(unittest.TestCase):
    def setUp(self):
        for name in ("socket.socket", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("network/process forbidden in pure control"))
            guard.start()
            self.addCleanup(guard.stop)

    def actual_plan(self):
        # __main__ is this controlled test under ROOT, so declare its actual
        # source too. Production's __main__ is the already-declared bootstrap.
        seed_names = (startup.DEPENDENCIES - {PROFILE}) | {SELF}
        return {"dependencies": producer.startup_dependency_hashes(ROOT, seed_names)}

    def test_actual_producer_adds_profile_and_actual_loader_accepts_exact_closure(self):
        plan = self.actual_plan()
        self.assertIn(PROFILE, startup.DEPENDENCIES)
        self.assertEqual(plan["dependencies"][PROFILE],
                         hashlib.sha256((ROOT / PROFILE).read_bytes()).hexdigest())
        wire = startup.load_helpers(plan)
        self.assertEqual(Path(wire.profiles.__file__).resolve(), ROOT / PROFILE)
        self.assertIs(sys.modules["recall_closed_workload_profiles"], wire.profiles)
        self.assertEqual(wire.profiles.SELF, PROFILE)

    def test_missing_or_wrong_profile_digest_refuses_before_wire_import(self):
        for case in ("missing", "wrong_digest"):
            with self.subTest(case=case):
                plan = self.actual_plan()
                if case == "missing":
                    del plan["dependencies"][PROFILE]
                else:
                    plan["dependencies"][PROFILE] = "0" * 64
                with patch.object(startup, "reviewed_module") as imported, self.assertRaises(ValueError):
                    startup.load_helpers(plan)
                imported.assert_not_called()

    def test_registered_unreviewed_module_still_refuses(self):
        plan = self.actual_plan()
        unknown = SimpleNamespace(__file__=str(ROOT / "scripts/qualification/local_fastembed_driver.py"))
        with patch.dict(sys.modules, {"controlled_unreviewed_packaged_module": unknown}), \
                self.assertRaises(ValueError):
            startup.load_helpers(plan)


if __name__ == "__main__":
    unittest.main()
