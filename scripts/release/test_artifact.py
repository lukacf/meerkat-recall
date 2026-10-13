"""Offline release contracts; no Cargo, network or service process required."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("artifact", Path(__file__).with_name("artifact.py"))
artifact = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifact)


def reference_fixture(vendor):
    for name in artifact.REFERENCE_SOURCE_TREES:
        tree = vendor / name
        tree.mkdir(parents=True, exist_ok=True)
        (tree / "LICENSE").write_text("Business Source License 1.1 fixture")
        (tree / "RECALL_PROVENANCE.json").write_text(json.dumps({
            "package": name.removesuffix("-2.6.5"), "version": "2.6.5",
            "source_revision": artifact.REFERENCE_UPSTREAM_COMMIT,
        }))
    for name in artifact.REFERENCE_SOURCE_RECORDS:
        (vendor / name).write_text("reviewed source record fixture")
    for path in vendor.rglob("*"):
        if path.is_file():
            path.chmod(0o644)
    return artifact.reference_source_receipt(vendor)


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        (self.bundle / "meerkat-recall").write_bytes(b"binary fixture; never executed")
        (self.bundle / "meerkat-recall").chmod(0o755)
        (self.bundle / "elephant").symlink_to("meerkat-recall")
        reference = reference_fixture(self.bundle / "third-party")
        (self.bundle / "licenses").mkdir()
        for name in artifact.REFERENCE_SOURCE_TREES:
            license_path = self.bundle / "licenses" / f"{name}-LICENSE"
            license_path.write_bytes((self.bundle / "third-party" / name / "LICENSE").read_bytes())
            license_path.chmod(0o644)
        self.manifest = {"format": artifact.FORMAT, "target": "aarch64-apple-darwin",
                         "binary_sha256": artifact.digest(self.bundle / "meerkat-recall"),
                         "files": artifact.inventory(self.bundle)}
        self.manifest.update(version="0.1.0", source_commit="1"*40, source_tree="2"*40,
                             source_tar_sha256="3"*64, cargo_lock_sha256="4"*64)
        self.manifest["build"] = {key: self.manifest[key] for key in
                                  ("source_commit", "source_tree", "source_tar_sha256",
                                   "cargo_lock_sha256", "binary_sha256", "target")}
        self.manifest["component_licenses"] = {name: {"license": "BUSL-1.1", "text": f"licenses/{name}-LICENSE"} for name in artifact.REFERENCE_SOURCE_TREES}
        self.manifest["build"].update(
            native_reference_capability=reference,
            build_command=artifact.BUILD_COMMAND, build_profile="deploy",
            features=artifact.FEATURES, default_features=True,
            runtime_dependencies={
                **artifact.runtime_dependencies("aarch64-apple-darwin", "candidate:\n\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0)\n"),
                "binary_sha256": self.manifest["binary_sha256"],
            },
        )
        self.archive = self.root / "candidate.tar.gz"

    def pack(self):
        (self.bundle / "provenance.json").write_text(json.dumps(self.manifest))
        artifact.archive_tree(self.bundle, self.archive, 1700000000)
        return artifact.digest(self.archive)

    def test_install_retains_full_verified_tree_and_alias(self):
        expected = self.pack()
        destination = self.root / "installed"
        self.assertEqual(artifact.verify(self.archive, expected, destination), self.manifest)
        self.assertEqual((destination / "meerkat-recall").read_bytes(), b"binary fixture; never executed")
        self.assertEqual((destination / "elephant").readlink(), Path("meerkat-recall"))
        with self.assertRaisesRegex(ValueError, "already exists"):
            artifact.verify(self.archive, expected, destination)

    def test_wrong_archive_digest_does_not_install(self):
        self.pack()
        destination = self.root / "installed"
        with self.assertRaisesRegex(ValueError, "SHA256"):
            artifact.verify(self.archive, "0" * 64, destination)
        self.assertFalse(destination.exists())

    def test_manifest_detects_tampered_payload_with_new_archive_digest(self):
        (self.bundle / "meerkat-recall").write_bytes(b"changed payload")
        expected = self.pack()
        with self.assertRaisesRegex(ValueError, "contents differ"):
            artifact.verify(self.archive, expected)

    def test_manifest_rejects_extra_files_and_unknown_format(self):
        for field in ("extra", "format"):
            with self.subTest(field=field):
                if field == "extra":
                    (self.bundle / "unexpected").write_text("payload")
                else:
                    (self.bundle / "unexpected").unlink()
                    self.manifest["format"] = "unknown/v99"
                expected = self.pack()
                with self.assertRaises(ValueError):
                    artifact.verify(self.archive, expected)
                self.archive.unlink()

    def test_missing_source_digest_or_mismatched_build_receipt_is_refused(self):
        del self.manifest["source_tar_sha256"]
        expected = self.pack()
        with self.assertRaisesRegex(ValueError, "source_tar_sha256"):
            artifact.verify(self.archive, expected)
        self.archive.unlink()
        self.manifest["source_tar_sha256"] = "5" * 64
        expected = self.pack()
        with self.assertRaisesRegex(ValueError, "disagree"):
            artifact.verify(self.archive, expected)

    def test_same_inputs_produce_same_archive_bytes(self):
        expected = self.pack()
        second = self.root / "second.tar.gz"
        artifact.archive_tree(self.bundle, second, 1700000000)
        self.assertEqual(expected, artifact.digest(second))

    def test_rejects_path_escape_duplicate_and_link_parent_before_extract(self):
        for name, link in [("../escape", None), ("/escape", None), ("escape", "/tmp"), ("elephant/child", None)]:
            with self.subTest(name=name):
                with tarfile.open(self.archive, "w:gz") as tar:
                    if name == "elephant/child":
                        alias = tarfile.TarInfo("elephant")
                        alias.type, alias.linkname = tarfile.SYMTYPE, "meerkat-recall"
                        tar.addfile(alias)
                    member = tarfile.TarInfo(name)
                    if link:
                        member.type, member.linkname = tarfile.SYMTYPE, link
                        tar.addfile(member)
                    else:
                        member.size = 1
                        tar.addfile(member, io.BytesIO(b"x"))
                with self.assertRaises(ValueError):
                    artifact.verify(self.archive, artifact.digest(self.archive))
                self.assertFalse((self.root / "escape").exists())

    def test_actual_packager_preserves_every_patched_source_and_provision_helper(self):
        source = self.root / "source"
        source.mkdir()
        (source / "Cargo.toml").write_text('[workspace.package]\nversion="0.1.0"\n')
        for name in ("LICENSE", "LICENSE-MIT", "LICENSE-APACHE", "THIRD_PARTY_NOTICES.md"):
            (source / name).write_text("license fixture")
        for directory in ("skills", "deploy/native", "scripts/release"):
            (source / directory).mkdir(parents=True)
        for name in ("artifact.py", "provision-fastembed", "THIRD_PARTY_SOURCE_NOTICES.txt", "ARTIFACT_README.md"):
            (source / "scripts/release" / name).write_text("packaged helper fixture")
        for name in artifact.OPERATIONAL_DOCUMENTS:
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("exact operator document: " + name)
        contract = source / artifact.OPERATIONAL_CONTRACT
        contract.mkdir(parents=True)
        for name in ("homecore-operations.md", "examples.json", "operator-template.json"):
            (contract / name).write_text("exact contract fixture: " + name)
        for name in artifact.PATCHED_SOURCE_TREES:
            directory = source / "vendor" / name
            directory.mkdir(parents=True)
            (directory / "LICENSE").write_text("upstream license fixture")
            (directory / "patched.rs").write_text("exact patched source fixture")
        (source / "vendor/provider-http-backport.json").write_text('{"fixture":true}')
        self.manifest["build"]["native_reference_capability"] = reference_fixture(source / "vendor")
        identity = {key: self.manifest[key] for key in
                    ("source_commit", "source_tree", "source_tar_sha256", "cargo_lock_sha256")}
        receipt = self.root / "build.json"
        receipt.write_text(json.dumps(self.manifest["build"]))
        with patch.object(artifact, "source_identity", return_value=identity), patch.object(artifact, "git", return_value="1700000000"):
            archive = artifact.package(source, self.bundle / "meerkat-recall", "aarch64-apple-darwin", self.root / "dist", receipt)
        manifest = artifact.verify(archive, artifact.digest(archive))
        for name in artifact.PATCHED_SOURCE_TREES:
            self.assertIn(f"third-party/{name}/LICENSE", manifest["files"])
            self.assertIn(f"third-party/{name}/patched.rs", manifest["files"])
        for name in artifact.REFERENCE_SOURCE_RECORDS:
            self.assertIn(f"third-party/{name}", manifest["files"])
        for name in artifact.REFERENCE_SOURCE_TREES:
            self.assertIn(f"licenses/{name}-LICENSE", manifest["files"])
        self.assertIn("third-party/provider-http-backport.json", manifest["files"])
        self.assertIn("third-party/SOURCE_NOTICES.txt", manifest["files"])
        self.assertEqual(manifest["files"]["provision-fastembed"]["mode"], 0o755)
        expected_docs = ["README.md", *artifact.OPERATIONAL_DOCUMENTS,
                         *(artifact.OPERATIONAL_CONTRACT + "/" + name for name in
                           ("homecore-operations.md", "examples.json", "operator-template.json"))]
        installed = self.root / "operator-install"
        artifact.verify(archive, artifact.digest(archive), installed)
        for name in expected_docs:
            self.assertIn(name, manifest["files"])
            original = source / ("scripts/release/ARTIFACT_README.md" if name == "README.md" else name)
            self.assertEqual((installed / name).read_bytes(), original.read_bytes())


    def test_native_reference_source_receipt_and_license_are_required(self):
        for field in ("receipt", "license", "source"):
            with self.subTest(field=field):
                original = json.loads(json.dumps(self.manifest))
                if field == "receipt":
                    del self.manifest["build"]["native_reference_capability"]
                elif field == "license":
                    del self.manifest["component_licenses"]
                else:
                    changed = self.bundle / "third-party/surrealdb-core-2.6.5/changed.rs"
                    changed.write_text("changed source")
                    changed.chmod(0o644)
                    self.manifest["files"] = artifact.inventory(self.bundle)
                    self.manifest["files"].pop("provenance.json", None)
                expected = self.pack()
                with self.assertRaisesRegex(ValueError, "reference|license"):
                    artifact.verify(self.archive, expected)
                self.archive.unlink()
                self.manifest = original

    def test_missing_fastembed_or_runtime_binary_mismatch_is_refused(self):
        for field in ("features", "binary"):
            with self.subTest(field=field):
                if field == "features":
                    self.manifest["build"]["features"] = ["rocksdb"]
                else:
                    self.manifest["build"]["features"] = artifact.FEATURES
                    self.manifest["build"]["runtime_dependencies"]["binary_sha256"] = "9" * 64
                expected = self.pack()
                with self.assertRaises(ValueError):
                    artifact.verify(self.archive, expected)
                self.archive.unlink()

    def test_darwin_rejects_homebrew_and_missing_bundled_onnx_libraries(self):
        for library in ("/opt/homebrew/opt/xz/lib/liblzma.5.dylib", "@rpath/libonnxruntime.dylib"):
            with self.subTest(library=library):
                with self.assertRaisesRegex(ValueError, "non-system Darwin"):
                    artifact.runtime_dependencies("aarch64-apple-darwin", f"candidate:\n\t{library} (compatibility version 1.0.0)\n")

    def test_linux_accepts_runtime_image_libraries_and_rejects_unknown_or_missing(self):
        inspection = "linux-vdso.so.1 (0x0)\n libssl.so.3 => /lib/x86_64-linux-gnu/libssl.so.3 (0x0)\n /lib64/ld-linux-x86-64.so.2 (0x0)\n"
        links = artifact.runtime_dependencies("x86_64-unknown-linux-gnu", inspection)
        self.assertEqual(len(links["libraries"]), 2)
        for line in ("libonnxruntime.so => /usr/local/lib/libonnxruntime.so (0x0)", "libssl.so.3 => not found"):
            with self.subTest(line=line):
                with self.assertRaises(ValueError):
                    artifact.runtime_dependencies("x86_64-unknown-linux-gnu", line)

    def test_cache_receipt_binds_assets_and_refuses_external_links(self):
        cache = self.root / "models"
        cache.mkdir()
        (cache / "blob").write_bytes(b"model fixture")
        (cache / "model.onnx").symlink_to("blob")
        before = artifact.model_cache_receipt(cache, "a" * 64)
        (cache / "blob").write_bytes(b"changed model fixture")
        after = artifact.model_cache_receipt(cache, "a" * 64)
        self.assertNotEqual(before["cache_sha256"], after["cache_sha256"])
        (cache / "outside").symlink_to(self.bundle / "meerkat-recall")
        with self.assertRaisesRegex(ValueError, "escapes cache"):
            artifact.model_cache_receipt(cache, "a" * 64)



if __name__ == "__main__":
    unittest.main()
