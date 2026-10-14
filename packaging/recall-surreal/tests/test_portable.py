"""Host launcher tests without Docker, Rust, services or network access."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import portable
import prepare


class PortableTests(unittest.TestCase):
    def test_larger_shared_allowance_preserves_inner_build_ceiling_and_original_ledger(self):
        gib = 1024**3
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source = root / "prepared"
            source.mkdir()
            provenance = {"recall_commit": "a" * 40, "rust_toolchain": "1.94.0"}
            (source / "RECALL_SOURCE_PROVENANCE.json").write_text(json.dumps(provenance))
            inputs = {"builder": {"image": "library/rust@sha256:" + "b" * 64}}
            ledger = root / "original-shared-ledger.json"
            ledger.write_text("original cumulative counters")
            for shared_gib, inner_gib in ((100, 100), (150, 150), (200, 150), (300, 150)):
                with self.subTest(shared_gib=shared_gib):
                    helper = mock.Mock()
                    helper.artifact.source_identity.return_value = {"source_commit": provenance["recall_commit"]}
                    helper.load_inputs.return_value = inputs
                    helper.build_limits.return_value = (8, 250 * gib, shared_gib * gib)
                    with mock.patch.object(portable, "HERE", root / "packaging/recall-surreal"), \
                         mock.patch.object(portable, "release_helpers", return_value=helper), \
                         mock.patch.object(portable.build, "check_prepared", return_value=provenance), \
                         mock.patch.object(portable.build, "portable_inputs", return_value=inputs), \
                         mock.patch.dict(portable.os.environ, {"RECALL_ARTIFACT_BUDGET_FILE": str(ledger)}), \
                         mock.patch.object(portable.subprocess, "check_output", return_value="/owned/docker-root\n") as inspect, \
                         mock.patch.object(portable.subprocess, "Popen", side_effect=AssertionError("real child forbidden")), \
                         mock.patch.object(portable, "run_build", side_effect=RuntimeError("captured build admission")) as launch:
                        with self.assertRaisesRegex(RuntimeError, "captured build admission"):
                            portable.build_portable(root, source, root / ("bundle-" + str(shared_gib)), "test:source")
                    launch.assert_called_once()
                    command, budget = launch.call_args.args
                    self.assertIn("RECALL_PAIRED_MAX_GROWTH_GIB=" + str(inner_gib), command)
                    self.assertIn("CARGO_BUILD_JOBS=8", command)
                    self.assertLessEqual(inner_gib * gib, portable.build.MAX_GROWTH_BYTES)
                    self.assertIs(budget, helper.ResourceBudget.return_value)
                    helper.ResourceBudget.assert_called_once()
                    self.assertEqual(helper.ResourceBudget.call_args.args[0], ledger)
                    self.assertEqual(helper.ResourceBudget.call_args.args[-2:], (250 * gib, shared_gib * gib))
                    self.assertEqual(ledger.read_text(), "original cumulative counters")
                    inspect.assert_called_once_with(["docker", "info", "--format", "{{.DockerRootDir}}"], text=True)

    def test_cache_retries_are_stable_and_other_prepared_contexts_do_not_share(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provenance = {"recall_commit": "a" * 40, "rust_toolchain": "1.94.0"}
            for name in ("first", "second"):
                (root / name).mkdir()
                (root / name / "RECALL_SOURCE_PROVENANCE.json").write_text(json.dumps(provenance))
            first = portable.cache_identity(root / "first", provenance)
            self.assertEqual(first, portable.cache_identity(root / "first", provenance))
            self.assertNotEqual(first, portable.cache_identity(root / "second", provenance))
            provenance["rust_toolchain"] = "changed"
            self.assertNotEqual(first, portable.cache_identity(root / "first", provenance))

    def test_refused_shared_budget_prevents_process_launch(self):
        budget = mock.Mock()
        budget.check.side_effect = RuntimeError("shared allocation refused")
        with mock.patch.object(portable.subprocess, "Popen") as spawn:
            with self.assertRaises(RuntimeError):
                portable.run_build(["never-run"], budget)
            spawn.assert_not_called()

    def test_mid_build_refusal_cancels_only_the_owned_process_group(self):
        budget = mock.Mock()
        budget.check.side_effect = [{}, RuntimeError("shared allocation refused")]
        process = mock.Mock(pid=12345)
        process.poll.return_value = None
        with mock.patch.object(portable.subprocess, "Popen", return_value=process), \
             mock.patch.object(portable.os, "killpg") as kill:
            with self.assertRaises(RuntimeError):
                portable.run_build(["never-run"], budget)
            kill.assert_called_once_with(12345, portable.signal.SIGTERM)
            process.wait.assert_called_once_with(timeout=15)

    def test_bundle_joins_prepared_source_inputs_binary_and_actual_elf(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, bundle = root / "source", root / "bundle"
            source.mkdir(); bundle.mkdir()
            for path in (source, bundle):
                (path / "RECALL_SOURCE_PROVENANCE.json").write_text("reviewed source")
            binary = bundle / "recall-surreal"
            binary.write_bytes(b"test artifact, never executed")
            inputs = {"builder": {"image": "library/rust@sha256:" + "a" * 64}}
            elf = {"binary_sha256": prepare.digest(binary), "baseline": "debian-12-amd64"}
            receipt = {
                "status": "built_unqualified",
                "source_provenance_sha256": prepare.digest(source / "RECALL_SOURCE_PROVENANCE.json"),
                "container_inputs": inputs, "actual_builder_image": inputs["builder"]["image"],
                "binary_sha256": prepare.digest(binary), "elf": elf,
            }
            (bundle / "BUILD_PROVENANCE.json").write_text(json.dumps(receipt))
            artifact = mock.Mock()
            artifact.inspect_elf.return_value = elf
            self.assertEqual(portable.validate_bundle(source, bundle, inputs, artifact), receipt)
            artifact.inspect_elf.return_value = {**elf, "baseline": "different"}
            with self.assertRaises(ValueError):
                portable.validate_bundle(source, bundle, inputs, artifact)
            binary.write_bytes(b"replaced artifact")
            with self.assertRaises(ValueError):
                portable.validate_bundle(source, bundle, inputs, artifact)
