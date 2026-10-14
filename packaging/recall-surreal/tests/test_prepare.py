"""Offline source/provenance and response parsing tests; no Rust/server/network."""
import base64
import copy
import hashlib
import hmac
import io
import json
import re
from pathlib import Path
import struct
import sys
import tarfile
import tempfile
import tomllib
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import prepare
import verify
import build


class PreparationTests(unittest.TestCase):
    def test_pinned_server_identity_and_independent_vectors(self):
        lock = prepare.read_lock()
        self.assertEqual(lock["server_package"], "surreal")
        self.assertEqual(lock["upstream_version"], "2.6.5")
        vector = lock["capability"]
        def field(value):
            data = value.encode()
            return struct.pack(">Q", len(data)) + data
        prefix = b"meerkat-recall/reference/v1\0" + field(vector["journal"]) + field(vector["space"]) + field("recall_probe")
        for suffix, expected in [(b"\0" + field("42"), vector["string_token"]), (b"\1" + struct.pack(">q", 42), vector["number_token"])]:
            self.assertEqual(hmac.new(bytes(range(32)), prefix + suffix, hashlib.sha256).hexdigest(), expected)
        self.assertNotEqual(vector["string_token"], vector["number_token"])

    def test_lock_edge_changes_no_other_resolved_package(self):
        original = '''version = 4
[[package]]
name = "hmac"
version = "0.12.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "fixed"
dependencies = ["digest"]
[[package]]
name = "surrealdb-core"
version = "2.6.5"
dependencies = [
 "hex",
 "http",
]
[[package]]
name = "unrelated"
version = "1.2.3"
'''
        result = prepare.add_hmac_lock_edge(original)
        packages = tomllib.loads(result)["package"]
        self.assertEqual(packages[1]["dependencies"], ["hex", "hmac", "http"])
        self.assertEqual(packages[2], {"name": "unrelated", "version": "1.2.3"})
        with self.assertRaises(ValueError):
            prepare.add_hmac_lock_edge(result)
        with self.assertRaises(ValueError):
            prepare.add_hmac_lock_edge(original.replace('version = "0.12.1"', 'version = "0.13.0"'))

    def test_paired_lock_keeps_unrelated_versions_and_registry_identities(self):
        lock = prepare.read_lock()
        spec = lock["surrealkv"]
        original = f'''version = 4
[[package]]
name = "hmac"
version = "0.12.1"
[[package]]
name = "surrealdb-core"
version = "2.6.5"
dependencies = [
 "hex",
 "surrealkv",
 "vart 0.8.1",
]
[[package]]
name = "surrealkv"
version = "0.9.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "{spec['upstream_lock_checksum']}"
dependencies = ["bytes", "vart 0.9.2"]
[[package]]
name = "vart"
version = "0.8.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "unrelated-pinned-identity"
[[package]]
name = "vart"
version = "0.9.2"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "{spec['vart']['upstream_checksum']}"
'''
        packages = tomllib.loads(prepare.paired_lock(original, lock))["package"]
        self.assertEqual(packages[1]["dependencies"], ["hex", "hmac", "surrealkv", "vart 0.8.1"])
        self.assertEqual(packages[2], {"name": "surrealkv", "version": "0.9.3", "dependencies": ["bytes", "vart 0.9.3"]})
        self.assertEqual(packages[3], tomllib.loads(original)["package"][3])
        self.assertEqual(packages[4]["checksum"], spec["vart"]["checksum"])
        with self.assertRaises(ValueError):
            prepare.paired_lock(original.replace(spec['upstream_lock_checksum'], 'unreviewed'), lock)
        with self.assertRaises(ValueError):
            prepare.paired_lock(original + '\n[[package]]\nname = "unreviewed-consumer"\nversion = "1.0.0"\ndependencies = ["vart 0.9.2"]\n', lock)

    def test_kv_overlay_requires_exact_patch_and_original_license(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recall, source = root / "recall", root / "source"
            source.mkdir()
            vendor = recall / "vendor/surrealkv-0.9.3"
            (vendor / "src").mkdir(parents=True)
            (vendor / "Cargo.toml").write_text('[package]\nname = "surrealkv"\nversion = "0.9.3"\n')
            (vendor / "LICENSE").write_text("original upstream license")
            (vendor / "src/snapshot.rs").write_text("original")
            upstream = prepare.file_manifest(vendor)
            (vendor / "src/snapshot.rs").write_text("reviewed repair")
            manifest = vendor.parent / "surrealkv-0.9.3-upstream-sha256.json"
            manifest.write_text(json.dumps(upstream))
            lock = {"surrealkv": {
                "path": "vendor/surrealkv-0.9.3", "version": "0.9.3",
                "provenance_files": {manifest.name: prepare.digest(manifest)},
                "patched_files": {"src/snapshot.rs": prepare.digest(vendor / "src/snapshot.rs")},
            }}
            expected = prepare.file_manifest(vendor)
            self.assertEqual(prepare.overlay_surrealkv(recall, source, lock), expected)
            self.assertEqual(prepare.file_manifest(source / "vendor/surrealkv-0.9.3"), expected)
            (vendor / "LICENSE").write_text("unreviewed replacement")
            with self.assertRaises(ValueError):
                prepare.overlay_surrealkv(recall, root / "refused", lock)
            self.assertFalse((root / "refused").exists())

    def test_archive_paths_and_special_files_cannot_escape_owned_stage(self):
        lock = prepare.read_lock()
        prefix = "surrealdb-" + lock["upstream_commit"]
        for name, kind in [(prefix + "/../outside", tarfile.REGTYPE), ("/outside", tarfile.REGTYPE), (prefix + "/link", tarfile.SYMTYPE)]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                archive = root / "input.tar.gz"
                with tarfile.open(archive, "w:gz") as tar:
                    info = tarfile.TarInfo(name)
                    info.type = kind
                    info.linkname = "../outside"
                    tar.addfile(info, io.BytesIO())
                stage = root / "stage"
                stage.mkdir()
                with self.assertRaises(ValueError):
                    prepare.extract_source(archive, stage, lock)
                self.assertFalse((root / "outside").exists())

    def test_digest_mismatch_refuses_existing_archive_without_network(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "input.tar.gz"
            archive.write_bytes(b"unreviewed source")
            with self.assertRaises(ValueError):
                prepare.acquire(prepare.read_lock(), archive, False)
            self.assertEqual(archive.read_bytes(), b"unreviewed source")

    def test_prepared_build_refuses_modified_source_before_any_process(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root / "Cargo.toml").write_text("reviewed")
            receipt = {"prepared_files": prepare.file_manifest(root)}
            (root / "RECALL_SOURCE_PROVENANCE.json").write_text(json.dumps(receipt))
            self.assertEqual(build.check_prepared(root), receipt)
            (root / "Cargo.toml").write_text("changed")
            with self.assertRaises(ValueError):
                build.check_prepared(root)

    def test_portable_inputs_refuse_tag_digest_platform_or_builder_substitution(self):
        entry = {"image": "library/example@sha256:" + "a" * 64,
                 "manifest_digest": "sha256:" + "a" * 64, "os": "linux", "architecture": "amd64"}
        lock = {"format": "meerkat-recall-container-inputs/v1", "platform": "linux/amd64",
                **{role: copy.deepcopy(entry) for role in ("builder", "runtime", "frontend")}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".recall").mkdir()
            path = root / ".recall/container-inputs.lock.json"
            path.write_text(json.dumps(lock))
            self.assertEqual(build.portable_inputs(root, entry["image"]), lock)
            with self.assertRaises(ValueError):
                build.portable_inputs(root, "library/other@sha256:" + "b" * 64)
            for key, value in [("image", "library/example:latest"), ("manifest_digest", "sha256:" + "b" * 64), ("architecture", "arm64")]:
                wrong = copy.deepcopy(lock)
                wrong["runtime"][key] = value
                path.write_text(json.dumps(wrong))
                with self.assertRaises(ValueError):
                    build.portable_inputs(root, None)

    def test_disk_monitor_enforces_hard_floor_and_assigned_growth(self):
        root = Path("/owned/build")
        gib = 1024**3
        for available, growth, succeeds in [(300, 100, True), (249, 150, False), (300, 50, False)]:
            with self.subTest(available=available, growth=growth):
                with mock.patch.object(build.shutil, "disk_usage", return_value=mock.Mock(free=available*gib)):
                    if succeeds:
                        build.check_disk_budget({root:400*gib}, growth*gib)
                    else:
                        with self.assertRaises(RuntimeError):
                            build.check_disk_budget({root:400*gib}, growth*gib)

    def test_actual_inspector_is_loaded_without_changing_prepared_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".recall").mkdir()
            (root / ".recall/artifact.py").write_text(
                "def inspect_elf(binary):\n    return {'actual_path': str(binary)}\n")
            before = prepare.file_manifest(root)
            binary = root / "server"
            self.assertEqual(build.inspect_binary(root, binary), {"actual_path":str(binary)})
            self.assertEqual(prepare.file_manifest(root), before)

    def test_capability_requires_version_success_and_exact_typed_tokens(self):
        lock = prepare.read_lock()
        tokens = sorted([lock["capability"]["number_token"], lock["capability"]["string_token"]])
        good = [{"status": "OK", "result": 1}, {"status": "OK", "result": tokens}]
        verify.validate_capability(json.dumps(good).encode(), lock)
        for index, replacement in [(0, {"status": "OK", "result": 2}), (1, {"status": "ERR", "result": "unknown function"}), (1, {"status": "OK", "result": [tokens[0], tokens[0]]})]:
            wrong = copy.deepcopy(good)
            wrong[index] = replacement
            with self.assertRaises(ValueError):
                verify.validate_capability(json.dumps(wrong).encode(), lock)
        with self.assertRaises(ValueError):
            verify.validate_capability(b" " * (verify.MAX_RESPONSE_BYTES + 1), lock)
        sql = verify.capability_sql(lock)
        self.assertNotIn("DEFINE", sql)
        self.assertNotIn("CREATE", sql)
        self.assertIn('type::thing("recall_probe", 42)', sql)
        self.assertIn('type::thing("recall_probe", "42")', sql)

    def test_capability_key_uses_supported_native_bytes_decoder(self):
        lock = prepare.read_lock()
        sql = verify.capability_sql(lock)
        match = re.search(r'encoding::base64::decode\("([A-Za-z0-9+/]+)"\)', sql)
        self.assertIsNotNone(match)
        encoded = match.group(1)
        decoded = base64.b64decode(encoded + "=" * (-len(encoded) % 4), validate=True)
        self.assertEqual(decoded, bytes.fromhex(lock["capability"]["key_hex"]))
        core = ROOT.parents[1] / "vendor/surrealdb-core-2.6.5/src"
        self.assertIn('"encoding::base64::decode"', (core / "syn/parser/builtin.rs").read_text())
        decoder = (core / "fnc/encoding.rs").read_text()
        self.assertIn("STANDARD_NO_PAD.decode(arg)", decoder)
        self.assertIn("Value::from(Bytes(", decoder)


if __name__ == "__main__":
    unittest.main()
