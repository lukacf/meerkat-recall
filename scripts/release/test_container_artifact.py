"""Offline exact-image packaging and ELF baseline refusal contracts."""
import copy
import json
import os
from collections import namedtuple
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import artifact
import container_artifact

Usage = namedtuple("Usage", "total used free")


class ResourceBudgetTests(unittest.TestCase):
    def test_environment_cannot_relax_shared_host_limits(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(container_artifact.build_limits(), (8, 250 * container_artifact.GIB, 150 * container_artifact.GIB))
            for key, value in (("RECALL_BUILD_MIN_FREE_GIB", "249"), ("RECALL_ARTIFACT_MAX_GIB", "801"),
                               ("RECALL_ARTIFACT_MAX_GIB", "0"), ("CARGO_BUILD_JOBS", "17")):
                with self.subTest(key=key, value=value), patch.dict(os.environ, {key: value}), self.assertRaises(ValueError):
                    container_artifact.build_limits()

    def test_explicit_approved_ceiling_does_not_change_default_or_extend_existing_ledger(self):
        gib = container_artifact.GIB
        with patch.dict(os.environ, {"RECALL_ARTIFACT_MAX_GIB": "200"}, clear=True):
            self.assertEqual(container_artifact.build_limits(), (8, 250 * gib, 200 * gib))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            with patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, 100 * gib, 900 * gib)):
                original = container_artifact.ResourceBudget(receipt, (root,)).check()
                self.assertEqual(original["limit_bytes"], 150 * gib)
                unchanged = container_artifact.ResourceBudget(receipt, (root,), allocation=200 * gib).check()
                self.assertEqual(unchanged["limit_bytes"], 150 * gib)
                self.assertEqual(unchanged["consumed_bytes"], original["consumed_bytes"])
            for invalid in (0, 800 * gib + 1):
                with self.assertRaises(ValueError):
                    container_artifact.ResourceBudget(receipt, (root,), allocation=invalid)

    def test_explicit_200_consumer_preserves_cutover_counter_and_refuses_at_new_limit(self):
        gib = container_artifact.GIB
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            budget = container_artifact.ResourceBudget(receipt, (root,), allocation=200 * gib)
            def used(value):
                return patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, value * gib, (1000 - value) * gib))
            # Represents the separately authorized cutover's existing ledger.
            # The consumer must keep its prior consumption, not restart at zero.
            with used(100):
                state = budget.check()
            state.update(consumed_bytes=149 * gib, checks=80)
            receipt.write_text(json.dumps(state))
            with used(150):
                state = budget.check()
            self.assertEqual((state["consumed_bytes"], state["checks"], state["limit_bytes"]),
                             (199 * gib, 81, 200 * gib))
            with used(140):
                self.assertEqual(budget.check()["consumed_bytes"], 199 * gib)
            with used(141), self.assertRaises(RuntimeError):
                budget.check()
            state = json.loads(receipt.read_text())
            self.assertEqual(state["consumed_bytes"], 200 * gib)
            self.assertTrue(state["refused"])
            with used(100), self.assertRaisesRegex(ValueError, "was refused"):
                budget.check()

    def test_explicit_300_requires_cutover_and_preserves_prior_consumption(self):
        gib = container_artifact.GIB
        with patch.dict(os.environ, {"RECALL_ARTIFACT_MAX_GIB": "300"}, clear=True):
            self.assertEqual(container_artifact.build_limits(), (8, 250 * gib, 300 * gib))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            def used(value):
                return patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, value * gib, (1000 - value) * gib))
            with used(100):
                state = container_artifact.ResourceBudget(receipt, (root,), allocation=200 * gib).check()
                state.update(consumed_bytes=199 * gib, checks=80)
                receipt.write_text(json.dumps(state))
                budget = container_artifact.ResourceBudget(receipt, (root,), allocation=300 * gib)
                unchanged = budget.check()
            self.assertEqual(unchanged, {**state, "checks": 81})
            # Only the separately authorized transition may raise this field.
            # This is a private fixture; ordinary consumers cannot perform it.
            receipt.write_text(json.dumps({**unchanged, "limit_bytes": 300 * gib}))
            with used(200):
                current = budget.check()
            self.assertEqual((current["consumed_bytes"], current["checks"], current["limit_bytes"]),
                             (299 * gib, 82, 300 * gib))
            with used(190):
                self.assertEqual(budget.check()["consumed_bytes"], 299 * gib)
            with used(191), self.assertRaises(RuntimeError):
                budget.check()
            state = json.loads(receipt.read_text())
            self.assertEqual(state["consumed_bytes"], 300 * gib)
            self.assertTrue(state["refused"])
            with used(100), self.assertRaisesRegex(ValueError, "was refused"):
                budget.check()

    def test_explicit_400_requires_cutover_and_preserves_prior_consumption(self):
        gib = container_artifact.GIB
        with patch.dict(os.environ, {"RECALL_ARTIFACT_MAX_GIB": "400"}, clear=True):
            self.assertEqual(container_artifact.build_limits(), (8, 250 * gib, 400 * gib))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            def used(value):
                return patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, value * gib, (1000 - value) * gib))
            with used(100):
                state = container_artifact.ResourceBudget(receipt, (root,), allocation=300 * gib).check()
                state.update(consumed_bytes=299 * gib, checks=80)
                receipt.write_text(json.dumps(state))
                budget = container_artifact.ResourceBudget(receipt, (root,), allocation=400 * gib)
                unchanged = budget.check()
            self.assertEqual(unchanged, {**state, "checks": 81})
            # Only the separately authorized transition may raise this field.
            # This is a private fixture; ordinary consumers cannot perform it.
            receipt.write_text(json.dumps({**unchanged, "limit_bytes": 400 * gib}))
            with used(200):
                current = budget.check()
            self.assertEqual((current["consumed_bytes"], current["checks"], current["limit_bytes"]),
                             (399 * gib, 82, 400 * gib))
            with used(190):
                self.assertEqual(budget.check()["consumed_bytes"], 399 * gib)
            with used(191), self.assertRaises(RuntimeError):
                budget.check()
            state = json.loads(receipt.read_text())
            self.assertEqual(state["consumed_bytes"], 400 * gib)
            self.assertTrue(state["refused"])
            with used(100), self.assertRaisesRegex(ValueError, "was refused"):
                budget.check()

    def test_explicit_600_requires_cutover_and_preserves_prior_consumption(self):
        gib = container_artifact.GIB
        with patch.dict(os.environ, {"RECALL_ARTIFACT_MAX_GIB": "600"}, clear=True):
            self.assertEqual(container_artifact.build_limits(), (8, 250 * gib, 600 * gib))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            def used(value):
                return patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, value * gib, (1000 - value) * gib))
            with used(100):
                state = container_artifact.ResourceBudget(receipt, (root,), allocation=400 * gib).check()
                state.update(consumed_bytes=399 * gib, checks=80)
                receipt.write_text(json.dumps(state))
                budget = container_artifact.ResourceBudget(receipt, (root,), allocation=600 * gib)
                unchanged = budget.check()
            self.assertEqual(unchanged, {**state, "checks": 81})
            # A private fixture for the separately authorized cap/floor transition.
            receipt.write_text(json.dumps({**unchanged, "limit_bytes": 600 * gib,
                                           "minimum_free_bytes": 300_000_000_000}))
            with used(300):
                current = budget.check()
            self.assertEqual((current["consumed_bytes"], current["checks"], current["limit_bytes"]),
                             (599 * gib, 82, 600 * gib))
            self.assertEqual(current["minimum_free_bytes"], 300_000_000_000)
            with used(290):
                self.assertEqual(budget.check()["consumed_bytes"], 599 * gib)
            with used(291), self.assertRaises(RuntimeError):
                budget.check()
            state = json.loads(receipt.read_text())
            self.assertEqual(state["consumed_bytes"], 600 * gib)
            self.assertTrue(state["refused"])
            with used(100), self.assertRaisesRegex(ValueError, "was refused"):
                budget.check()

    def test_explicit_800_cannot_clear_a_refusal_or_raise_the_existing_limit(self):
        gib = container_artifact.GIB
        with patch.dict(os.environ, {"RECALL_ARTIFACT_MAX_GIB": "800"}, clear=True):
            self.assertEqual(container_artifact.build_limits(), (8, 250 * gib, 800 * gib))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            with patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(2000 * gib, 100 * gib, 1900 * gib)):
                state = container_artifact.ResourceBudget(receipt, (root,), allocation=600 * gib).check()
                state.update(consumed_bytes=599 * gib, checks=80, minimum_free_bytes=300_000_000_000)
                receipt.write_text(json.dumps(state))
                budget = container_artifact.ResourceBudget(receipt, (root,), allocation=800 * gib)
                unchanged = budget.check()
                self.assertEqual(unchanged, {**state, "checks": 81})
                # Merely requesting a larger cap never authorizes refused-state revival.
                refused = {**unchanged, "consumed_bytes": 600 * gib, "refused": True}
                receipt.write_text(json.dumps(refused))
                original = receipt.read_bytes()
                with self.assertRaisesRegex(ValueError, "was refused"):
                    budget.check()
                self.assertEqual(receipt.read_bytes(), original)
                # Even a cap-only external mutation leaves the refusal sticky.
                refused["limit_bytes"] = 800 * gib
                receipt.write_text(json.dumps(refused))
                original = receipt.read_bytes()
                with self.assertRaisesRegex(ValueError, "was refused"):
                    budget.check()
                self.assertEqual(receipt.read_bytes(), original)

    def test_800_consumer_continues_charged_cutover_sample_and_never_refunds(self):
        gib = container_artifact.GIB
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            budget = container_artifact.ResourceBudget(receipt, (root,), allocation=800 * gib)
            def used(value):
                return patch.object(container_artifact.shutil, "disk_usage",
                                    return_value=Usage(2000 * gib, value * gib, (2000 - value) * gib))
            with used(110):
                state = budget.check()
            # Private fixture of the separately reviewed decision30 transition:
            # it already charged10GiB growth, and retained that checkpoint's sample.
            state.update(consumed_bytes=610 * gib + 17, checks=6077,
                         minimum_free_bytes=300_000_000_000)
            receipt.write_text(json.dumps(state))
            with used(120):
                after = budget.check()
            self.assertEqual((after["consumed_bytes"], after["checks"], after["limit_bytes"]),
                             (620 * gib + 17, 6078, 800 * gib))
            self.assertEqual(after["minimum_free_bytes"], 300_000_000_000)
            with used(110):
                self.assertEqual(budget.check()["consumed_bytes"], 620 * gib + 17)
            with used(290), self.assertRaises(RuntimeError):
                budget.check()
            refused = receipt.read_bytes()
            self.assertEqual(json.loads(refused)["consumed_bytes"], 800 * gib + 17)
            with used(80), self.assertRaisesRegex(ValueError, "was refused"):
                budget.check()
            self.assertEqual(receipt.read_bytes(), refused)

    def test_decimal_growth_floor_is_checked_on_every_sample_without_counter_reset(self):
        gib = container_artifact.GIB
        floor = 300_000_000_000
        self.assertGreater(floor - 1, 250 * gib)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            def free(value):
                return patch.object(container_artifact.shutil, "disk_usage",
                                    return_value=Usage(1000 * gib, 1000 * gib - value, value))
            with free(floor + 1_000_000_000):
                state = container_artifact.ResourceBudget(receipt, (root,), allocation=400 * gib).check()
            state.update(consumed_bytes=319 * gib, checks=80, limit_bytes=600 * gib,
                         minimum_free_bytes=floor)
            receipt.write_text(json.dumps(state))
            # Default constructor floor stays250GiB; the original ledger's stricter
            # decimal policy must survive every new consumer and every check.
            budget = container_artifact.ResourceBudget(receipt, (root,), allocation=600 * gib)
            with free(floor):
                exact = budget.check()
            self.assertEqual(exact["minimum_free_bytes"], floor)
            self.assertEqual(exact["checks"], 81)
            self.assertEqual(exact["consumed_bytes"], 319 * gib + 1_000_000_000)
            self.assertFalse(exact["refused"])
            with free(floor - 1), self.assertRaises(RuntimeError):
                budget.check()
            refused = json.loads(receipt.read_text())
            self.assertTrue(refused["refused"])
            self.assertEqual(refused["consumed_bytes"], exact["consumed_bytes"] + 1)
            self.assertEqual(refused["minimum_free_bytes"], floor)
            with free(floor + 2_000_000_000), self.assertRaisesRegex(ValueError, "was refused"):
                container_artifact.ResourceBudget(receipt, (root,), allocation=600 * gib).check()
            self.assertEqual(json.loads(receipt.read_text()), refused)

    def test_cache_is_stable_for_retry_but_isolates_source_checkout_and_graph(self):
        identity = {"source_commit": "1" * 40, "source_tree": "2" * 40, "cargo_lock_sha256": "3" * 64}
        first = container_artifact.cache_identity(Path("/reviewed/one"), identity)
        self.assertEqual(first, container_artifact.cache_identity(Path("/reviewed/one"), identity))
        self.assertNotEqual(first, container_artifact.cache_identity(Path("/reviewed/two"), identity))
        self.assertNotEqual(first, container_artifact.cache_identity(Path("/reviewed/one"), {**identity, "source_commit": "4" * 40}))
        with patch.object(artifact, "FEATURES", ["rocksdb"]):
            self.assertNotEqual(first, container_artifact.cache_identity(Path("/reviewed/one"), identity))

    def test_serial_builds_share_consumption_and_deletion_does_not_refund_budget(self):
        gib = container_artifact.GIB
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            budget = container_artifact.ResourceBudget(receipt, (root, root))
            def used(value):
                return patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, value * gib, (1000 - value) * gib))
            with used(100):
                self.assertEqual(budget.check()["consumed_bytes"], 0)
            with used(190):
                self.assertEqual(budget.check()["consumed_bytes"], 90 * gib)
            # A new wrapper must continue the same allocation; cache deletion
            # cannot silently authorize another 150 GiB build allocation.
            paired = container_artifact.ResourceBudget(receipt, (root,))
            with used(180):
                self.assertEqual(paired.check()["consumed_bytes"], 90 * gib)
            with used(240), self.assertRaises(RuntimeError):
                paired.check()
            state = json.loads(receipt.read_text())
            self.assertEqual(state["consumed_bytes"], 150 * gib)
            self.assertTrue(state["refused"])
            with used(100), self.assertRaisesRegex(ValueError, "was refused"):
                budget.check()

    def test_hard_floor_and_changed_filesystem_are_refused_without_large_writes(self):
        gib = container_artifact.GIB
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            budget = container_artifact.ResourceBudget(root / "floor.json", (root,))
            with patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, 751 * gib, 249 * gib)), self.assertRaises(RuntimeError):
                budget.check()
            self.assertTrue(json.loads(budget.receipt.read_text())["refused"])
            budget = container_artifact.ResourceBudget(root / "identity.json", (root,))
            with patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, 100 * gib, 900 * gib)):
                budget.check()
                state = json.loads(budget.receipt.read_text())
                state["filesystems"] = {"another-device": next(iter(state["filesystems"].values()))}
                budget.receipt.write_text(json.dumps(state))
                with self.assertRaisesRegex(ValueError, "filesystem set changed"):
                    budget.check()

    def test_caller_cannot_refill_existing_allocation_or_lower_prior_floor(self):
        gib = container_artifact.GIB
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "shared.json"
            with patch.object(container_artifact.shutil, "disk_usage", return_value=Usage(1000 * gib, 100 * gib, 900 * gib)):
                container_artifact.ResourceBudget(receipt, (root,), 300 * gib, 100 * gib).check()
                state = container_artifact.ResourceBudget(receipt, (root,)).check()
            self.assertEqual(state["minimum_free_bytes"], 300 * gib)
            self.assertEqual(state["limit_bytes"], 100 * gib)

HEADER = "  Class:                             ELF64\n  Machine:                           Advanced Micro Devices X86-64\n"
DYNAMIC = " 0x1 (NEEDED) Shared library: [libc.so.6]\n 0x1 (NEEDED) Shared library: [libstdc++.so.6]\n"
VERSIONS = "Version needs section '.gnu.version_r':\n Name: GLIBC_2.34 Flags: none\n Name: GLIBCXX_3.4.30 Flags: none\n Name: CXXABI_1.3.13 Flags: none\n"
LINKS = "libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x0)\nlibstdc++.so.6 => /lib/x86_64-linux-gnu/libstdc++.so.6 (0x0)\n/lib64/ld-linux-x86-64.so.2 (0x0)\n"


class ElfTests(unittest.TestCase):
    def test_actual_import_requirements_accept_baseline_and_reject_newer_or_unknown_abi(self):
        receipt = artifact.elf_dependencies(HEADER, DYNAMIC, VERSIONS)
        self.assertEqual(receipt["needed"], ["libc.so.6", "libstdc++.so.6"])
        for version in ("GLIBC_2.39", "GLIBCXX_3.4.31", "CXXABI_1.3.14", "GLIBC_PRIVATE", "OTHER_1.0"):
            with self.subTest(version=version), self.assertRaises(ValueError):
                artifact.elf_dependencies(HEADER, DYNAMIC, VERSIONS + " Name: " + version)
        for header in (HEADER.replace("ELF64", "ELF32"), HEADER.replace("Advanced Micro Devices X86-64", "AArch64")):
            with self.assertRaises(ValueError):
                artifact.elf_dependencies(header, DYNAMIC, VERSIONS)
        with self.assertRaises(ValueError):
            artifact.elf_dependencies(HEADER, DYNAMIC + " (NEEDED) Shared library: [libonnxruntime.so]\n", VERSIONS)


class ContainerReceiptTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "scripts/release").mkdir(parents=True)
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion="0.1.0"\n')
        source_lock = Path(__file__).with_name("container-inputs.lock.json")
        (self.root / "scripts/release/container-inputs.lock.json").write_bytes(source_lock.read_bytes())
        self.lock = container_artifact.load_inputs(self.root)
        self.binary = self.root / "binary"
        self.binary.write_bytes(b"fixture executable, never executed")
        self.sha = artifact.digest(self.binary)
        self.identity = {"source_commit": "1" * 40, "source_tree": "2" * 40,
                         "source_tar_sha256": "3" * 64, "cargo_lock_sha256": "4" * 64}
        self.image = {"Id": "sha256:" + "5" * 64, "Os": "linux", "Architecture": "amd64",
                      "Config": {"Labels": {"org.opencontainers.image.revision": "1" * 40,
                                             "org.meerkat.recall.source-tree": "2" * 40,
                                             "org.meerkat.recall.source-tar-sha256": "3" * 64}}}
        self.elf = {**artifact.elf_dependencies(HEADER, DYNAMIC, VERSIONS), "binary_sha256": self.sha}
        self.runtime_packages = "libc6:amd64\t2.36-9\nlibssl3:amd64\t3.0.17\nlibstdc++6:amd64\t12.2.0\nlibgomp1:amd64\t12.2.0\nca-certificates\t2025\ncurl\t7.88\n"
        self.provenance = {**self.identity, "format": "meerkat-recall-container/v1", "binary_sha256": self.sha,
                           "version": "0.1.0", "version_output": "meerkat-recall 0.1.0", "rustc": "rustc 1.94.0 (fixture)",
                           "build_command": artifact.BUILD_COMMAND, "build_profile": "deploy",
                           "features": artifact.FEATURES, "default_features": True,
                           "native_reference_capability": {"test": "receipt"},
                           "container_inputs": {"platform": "linux/amd64", "builder_image": self.lock["builder"]["image"],
                                                "runtime_image": self.lock["runtime"]["image"]},
                           "builder_packages": "libc6:amd64\t2.36-9\nlibssl-dev:amd64\t3.0.17\nclang\t14\ncmake\t3.25\npython3\t3.11\nbinutils\t2.40\n",
                           "runtime_dependencies": {**artifact.runtime_dependencies(container_artifact.TARGET, LINKS),
                                                    "binary_sha256": self.sha, "elf": self.elf}}

    def receipt(self):
        with patch.object(artifact, "reference_source_receipt", return_value={"test": "receipt"}), \
                patch.object(artifact, "validate_reference_receipt"), patch.object(artifact, "inspect_elf", return_value=self.elf):
            return container_artifact.native_receipt(self.root, self.identity, self.image, self.provenance,
                                                     self.binary, LINKS, self.runtime_packages)

    def test_receipt_preserves_actual_image_binary_and_installed_packages(self):
        receipt = self.receipt()
        self.assertEqual(receipt["container_image_id"], self.image["Id"])
        self.assertEqual(receipt["binary_sha256"], self.sha)
        self.assertEqual(receipt["target"], container_artifact.TARGET)
        self.assertEqual(receipt["runtime_package_versions"]["libc6:amd64"], "2.36-9")

    def test_mismatched_image_platform_source_lock_binary_features_and_abi_are_refused(self):
        original_image, original_provenance = copy.deepcopy(self.image), copy.deepcopy(self.provenance)
        mutations = [
            lambda: self.image.update(Architecture="arm64"),
            lambda: self.image["Config"]["Labels"].update({"org.opencontainers.image.revision": "9" * 40}),
            lambda: self.provenance.update(cargo_lock_sha256="9" * 64),
            lambda: self.provenance.update(binary_sha256="9" * 64),
            lambda: self.provenance.update(features=["rocksdb"]),
            lambda: self.provenance["container_inputs"].update(builder_image="rust:latest"),
            lambda: self.provenance["runtime_dependencies"]["elf"].update(imported_versions=["GLIBC_2.39"]),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                self.image, self.provenance = copy.deepcopy(original_image), copy.deepcopy(original_provenance)
                mutate()
                with self.assertRaises(ValueError):
                    self.receipt()
        self.image, self.provenance = original_image, original_provenance
        self.runtime_packages = self.runtime_packages.replace("2.36-9", "2.39-0")
        with self.assertRaisesRegex(ValueError, "baseline"):
            self.receipt()


if __name__ == "__main__":
    unittest.main()
