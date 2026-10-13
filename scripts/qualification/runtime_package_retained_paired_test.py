"""Source controls for explicit retained paired provenance; no runtime/model calls."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import prepare_gateway_runtime as package


class RetainedPairedSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.current = "a" * 40
        self.origin = "b" * 40
        self.lock = {"upstream_commit": "c" * 40, "archive_sha256": "d" * 64,
                     "rust_toolchain": "1.94.0", "reference_token_version": 1,
                     "server_package": "surreal", "vendor_paths": {
                         "surrealdb-core": "vendor/core", "surrealdb": "vendor/sdk"},
                     "surrealkv": {"path": "vendor/kv"}}
        lock_path = self.root / "packaging/recall-surreal/source.lock.json"
        lock_path.parent.mkdir(parents=True)
        lock_path.write_text(json.dumps(self.lock))
        self.source = {"recall_commit": self.origin, "source_lock_sha256": package.digest(lock_path),
                       "upstream_commit": self.lock["upstream_commit"],
                       "archive_sha256": self.lock["archive_sha256"], "rust_toolchain": "1.94.0",
                       "capability_version": 1, "server_package": "surreal", "vendor_files": {}}
        for name, path in {**self.lock["vendor_paths"], "surrealkv": "vendor/kv"}.items():
            target = self.root / path / "source.rs"
            target.parent.mkdir(parents=True)
            target.write_bytes((name + " original source").encode())
            self.source["vendor_files"][name] = {"source.rs": package.digest(target)}
        self.inputs = {"_root": self.root, "artifact_source_commit": self.current}
        self.paired = {"source_commit": self.origin}
        self.limits = SimpleNamespace(check=lambda: None)

    def test_explicit_original_with_identical_sources_is_preserved(self):
        original = copy.deepcopy(self.source)
        self.assertEqual(package.retained_paired_source(
            self.inputs, self.paired, self.source, self.limits), self.origin)
        self.assertEqual(self.source, original)

    def test_current_source_keeps_existing_contract(self):
        source = {"recall_commit": self.current}
        self.assertEqual(package.retained_paired_source(self.inputs, {}, source, self.limits), self.current)

    def test_old_origin_without_explicit_binding_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "paired source/build identity"):
            package.retained_paired_source(self.inputs, {}, self.source, self.limits)

    def test_wrong_declared_origin_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "paired source/build identity"):
            package.retained_paired_source(self.inputs, {"source_commit": "e" * 40}, self.source, self.limits)

    def test_changed_added_or_missing_vendor_file_is_rejected(self):
        target = self.root / "vendor/core/source.rs"
        original = target.read_bytes()
        target.write_bytes(b"different implementation")
        with self.assertRaisesRegex(ValueError, "storage bytes differ"):
            package.retained_paired_source(self.inputs, self.paired, self.source, self.limits)
        target.write_bytes(original)
        extra = target.with_name("extra.rs")
        extra.write_bytes(b"unreviewed addition")
        with self.assertRaisesRegex(ValueError, "storage bytes differ"):
            package.retained_paired_source(self.inputs, self.paired, self.source, self.limits)
        extra.unlink()
        target.unlink()
        with self.assertRaisesRegex(ValueError, "storage bytes differ"):
            package.retained_paired_source(self.inputs, self.paired, self.source, self.limits)

    def test_changed_upstream_recipe_is_rejected(self):
        lock_path = self.root / "packaging/recall-surreal/source.lock.json"
        lock_path.write_text(json.dumps({**self.lock, "upstream_commit": "f" * 40}))
        with self.assertRaisesRegex(ValueError, "upstream recipe differs"):
            package.retained_paired_source(self.inputs, self.paired, self.source, self.limits)

    def test_incomplete_recorded_vendor_inventory_is_rejected(self):
        del self.source["vendor_files"]["surrealkv"]
        with self.assertRaisesRegex(ValueError, "storage source inventory differs"):
            package.retained_paired_source(self.inputs, self.paired, self.source, self.limits)

    def test_vendor_symlink_is_rejected(self):
        target = self.root / "vendor/core/source.rs"
        target.rename(target.with_suffix(".original"))
        target.symlink_to(target.with_suffix(".original").name)
        with self.assertRaisesRegex(ValueError, "contains symlink"):
            package.retained_paired_source(self.inputs, self.paired, self.source, self.limits)


if __name__ == "__main__":
    unittest.main()
