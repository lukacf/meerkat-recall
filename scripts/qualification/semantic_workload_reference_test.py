"""Offline input-custody controls, without services, providers or qualification.

The graph fixtures deliberately contain unapproved runtime reviews and inert
installed-evidence bytes. They exercise the real input reader and gateway file
inventory; they cannot pass the later independent runtime predicate.
"""
import contextlib
import functools
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


entry = load("semantic_workload_reference_controls", Path(__file__).with_name("semantic_workload.py"))
gateway = load("semantic_workload_original_input_inventory",
               entry.ROOT / "tests/recall_preview_semantics/gateway/launch_context.py")


class ManifestReferenceControls(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        # Only the mount location is redirected. Actual readers, file identity
        # checks, helper hashes and input inventory logic remain in use.
        mounted = patch.object(entry, "INPUT_ROOT", self.root)
        mounted.start()
        self.addCleanup(mounted.stop)
        originals = patch.object(entry, "OriginalFiles",
                                 functools.partial(entry.OriginalFiles, roots=(self.root,)))
        originals.start()
        self.addCleanup(originals.stop)

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        raw = value if isinstance(value, bytes) else entry.encode(value)
        path.write_bytes(raw)
        path.chmod(0o600)
        return {"sha256": entry.digest(raw), "bytes": len(raw)}

    def graph(self, identifier=entry.profiles.LEGACY_ID):
        selected = entry.profiles.profile(identifier)
        directory = self.root / identifier
        manifest_path = directory / "orchestration.json"
        reference_path = directory / "manifest-reference.json"
        command = ["/usr/bin/python3", "-I", "/opt/recall/" + entry.SELF,
                   "--manifest", str(manifest_path), "--manifest-sha256-file", str(reference_path),
                   "--execute-reviewed"]
        nonce = "original_qualification_nonce_0001"
        helpers = {entry.SELF, entry.PHASES, entry.STARTUP, entry.VALIDATOR,
                   "tests/recall_preview_semantics/workload_orchestration.py"}
        if identifier == entry.profiles.SEMANTIC140_ID:
            helpers |= {"tests/recall_preview_semantics/workload_profiles.py",
                        "tests/recall_preview_semantics/semantic140_expectations.py"}
        review = {"format": "recall-dynamic-component-review/v2", "approved": False,
                  "expires_at": "9999-01-01T00:00:00Z",
                  "controller_policy": {"expected_anchor": {
                      "allocation_sha256": selected.hashes["provider_allocation"],
                      "command_sha256": entry.digest(entry.encode(command))}},
                  "helper_sha256": {name: entry.digest((entry.ROOT / name).read_bytes())
                                    for name in helpers},
                  "identities": {backend: {"run_nonce": nonce} for backend in entry.BACKENDS}}
        if identifier == entry.profiles.SEMANTIC140_ID:
            review["profile_id"] = identifier
        review_path = directory / "runtime-review.json"
        review_ref = {**self.write(review_path, review), "kind": "runtime-review"}
        installed_path = directory / "inert-installed-evidence.json"
        installed_ref = {**self.write(installed_path, {"product_acceptance": False}),
                         "kind": "installed-offline-evidence"}
        manifest = {"format": "recall-d28-orchestration/v2", "execution_authorized": True,
                    "provider_dispatch_authorized": True, "backends": list(entry.BACKENDS),
                    "runtime_original_inputs": {},
                    "output_directory": ("/work/semantic140-" if identifier == entry.profiles.SEMANTIC140_ID
                                         else "/work/d28-") + nonce}
        if identifier == entry.profiles.SEMANTIC140_ID:
            manifest["profile_id"] = identifier
        for backend in entry.BACKENDS:
            bundle_path = directory / (backend + ".json")
            bundle = {"format": "recall-runtime-original-inputs/v2", "review": review_ref,
                      "installed_offline": installed_ref,
                      "files": [{"path": str(review_path), "ref": review_ref},
                                {"path": str(installed_path), "ref": installed_ref}]}
            bound = self.write(bundle_path, bundle)
            manifest["runtime_original_inputs"][backend] = {
                "path": str(bundle_path), "sha256": bound["sha256"]}
        bound = self.write(manifest_path, manifest)
        descriptor = {"format": entry.MANIFEST_REFERENCE_FORMAT,
                      "path": str(manifest_path), "sha256": bound["sha256"]}
        self.write(reference_path, descriptor)
        return manifest_path, reference_path, review_path, command, descriptor

    def test_both_profiles_reconstruct_complete_acyclic_input_chain(self):
        for identifier in (entry.profiles.LEGACY_ID, entry.profiles.SEMANTIC140_ID):
            with self.subTest(profile=identifier):
                manifest, reference, review_path, command, descriptor = self.graph(identifier)
                review_before = review_path.read_bytes()
                expected = entry.manifest_digest(manifest, reference)
                loaded, prepared = entry.inputs(manifest, expected)
                self.assertEqual(expected, entry.digest(manifest.read_bytes()))
                self.assertEqual(set(prepared), set(entry.BACKENDS))
                self.assertEqual(json.loads(review_before)["controller_policy"]["expected_anchor"]
                                 ["command_sha256"], entry.digest(entry.encode(command)))
                self.assertNotIn(descriptor["sha256"], command)
                self.assertNotIn(str(reference), review_before.decode())
                self.assertNotIn(entry.digest(reference.read_bytes()), review_before.decode())
                self.assertEqual(review_path.read_bytes(), review_before)
                inventory = gateway.input_inventory(self.root)
                row = next(item for item in inventory["files"]
                           if item["path"] == reference.relative_to(self.root).as_posix())
                self.assertEqual(row["sha256"], entry.digest(reference.read_bytes()))
                self.assertEqual(loaded["backends"], list(entry.BACKENDS))

    def test_original_literal_digest_cli_and_reader_are_preserved(self):
        manifest, _, _, _, descriptor = self.graph()
        args = entry.arguments(["--manifest", str(manifest), "--manifest-sha256",
                                descriptor["sha256"], "--execute-reviewed"])
        self.assertIsNone(args.manifest_sha256_file)
        loaded, _ = entry.inputs(args.manifest, args.manifest_sha256)
        self.assertEqual(loaded["backends"], list(entry.BACKENDS))

    def test_cli_requires_exactly_one_digest_source(self):
        base = ["--manifest", str(self.root / "manifest.json"), "--execute-reviewed"]
        for tail in ([], ["--manifest-sha256", "a" * 64,
                          "--manifest-sha256-file", str(self.root / "reference.json")]):
            with self.subTest(tail=tail), contextlib.redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as error:
                entry.arguments(base + tail)
            self.assertEqual(error.exception.code, 2)

    def test_reference_option_parses_without_loading_or_authorizing(self):
        manifest, reference, _, _, _ = self.graph()
        with patch.object(entry, "read_original") as reader:
            args = entry.arguments(["--manifest", str(manifest), "--manifest-sha256-file",
                                    str(reference), "--execute-reviewed"])
        reader.assert_not_called()
        self.assertEqual(args.manifest_sha256_file, reference)
        self.assertIsNone(args.manifest_sha256)

    def test_changed_manifest_is_rejected_before_run(self):
        manifest, reference, _, _, _ = self.graph()
        value = json.loads(manifest.read_bytes())
        value["output_directory"] += "_substitution"
        self.write(manifest, value)
        argv = ["semantic_workload.py", "--manifest", str(manifest),
                "--manifest-sha256-file", str(reference), "--execute-reviewed"]
        with patch.object(entry.sys, "argv", argv), patch.object(entry, "run") as run, \
                patch.object(entry.asyncio, "run") as loop, \
                self.assertRaisesRegex(ValueError, "d28_original_input_hash"):
            entry.main()
        run.assert_not_called()
        loop.assert_not_called()

    def test_changed_nested_review_still_fails_original_byte_custody(self):
        manifest, reference, review, _, _ = self.graph()
        original = json.loads(review.read_bytes())
        original["approved"] = True
        self.write(review, original)
        with self.assertRaisesRegex(ValueError, "orchestration_original_hash_size"):
            entry.inputs(manifest, entry.manifest_digest(manifest, reference))

    def test_descriptor_target_substitution_is_not_a_new_manifest_authority(self):
        manifest, reference, _, _, descriptor = self.graph()
        descriptor["path"] = str(manifest.with_name("another.json"))
        self.write(reference, descriptor)
        with self.assertRaisesRegex(ValueError, "workload_manifest_reference_fields"):
            entry.manifest_digest(manifest, reference)

    def test_outer_gateway_inventory_detects_descriptor_and_manifest_substitution(self):
        manifest, reference, _, _, descriptor = self.graph()
        before = gateway.digest(gateway.encode(gateway.input_inventory(self.root)))
        value = json.loads(manifest.read_bytes())
        value["output_directory"] += "_different"
        descriptor["sha256"] = self.write(manifest, value)["sha256"]
        self.write(reference, descriptor)
        after = gateway.digest(gateway.encode(gateway.input_inventory(self.root)))
        self.assertNotEqual(before, after)
        with self.assertRaisesRegex(ValueError, "launch_inputs_original_inventory_mismatch"):
            gateway.stage_inputs(self.root, self.root.parent / (self.root.name + "-unused"), before)

    def test_strict_descriptor_fields_format_and_hash(self):
        manifest, reference, _, _, descriptor = self.graph()
        cases = [[], {**descriptor, "format": "unreviewed"}, {**descriptor, "extra": True},
                 {key: value for key, value in descriptor.items() if key != "path"}]
        cases += [{**descriptor, "sha256": value} for value in (None, True, "A" * 64, "a" * 63, "g" * 64)]
        for value in cases:
            with self.subTest(value=value):
                self.write(reference, value)
                with self.assertRaisesRegex(ValueError, "workload_manifest_reference_fields"):
                    entry.manifest_digest(manifest, reference)

    def test_duplicate_members_and_invalid_json_refuse(self):
        manifest, reference, _, _, _ = self.graph()
        for raw in (b'{"sha256":"a","sha256":"b"}', b'{"sha256":NaN}', b'\xff', b'[] trailing'):
            with self.subTest(raw=raw):
                self.write(reference, raw)
                with self.assertRaises(ValueError):
                    entry.manifest_digest(manifest, reference)

    def test_oversized_descriptor_refuses(self):
        manifest, reference, _, _, descriptor = self.graph()
        raw = entry.encode(descriptor)
        self.write(reference, raw + b" " * (entry.MANIFEST_REFERENCE_BYTES + 1 - len(raw)))
        with self.assertRaisesRegex(ValueError, "orchestration_original_file"):
            entry.manifest_digest(manifest, reference)

    def test_relative_foreign_traversal_or_self_reference_refuses(self):
        manifest, reference, _, _, _ = self.graph()
        candidates = [Path("relative.json"), self.root.parent / "foreign.json", manifest,
                      reference.parent / ".." / reference.parent.name / reference.name]
        with patch.object(entry, "read_original") as reader:
            for candidate in candidates:
                with self.subTest(candidate=candidate), self.assertRaisesRegex(ValueError, "reference_path"):
                    entry.manifest_digest(manifest, candidate)
        reader.assert_not_called()
        for target in (Path("relative.json"), self.root.parent / "foreign-manifest.json"):
            with self.subTest(target=target), self.assertRaisesRegex(ValueError, "reference_target"):
                entry.manifest_digest(target, reference)

    def test_symlink_directory_and_fifo_refuse_before_reader(self):
        manifest, reference, _, _, _ = self.graph()
        link, fifo = self.root / "symlink", self.root / "fifo"
        link.symlink_to(reference)
        os.mkfifo(fifo, 0o600)
        with patch.object(entry, "read_original") as reader:
            for path in (link, self.root, fifo):
                with self.subTest(path=path), self.assertRaisesRegex(ValueError, "reference_path"):
                    entry.manifest_digest(manifest, path)
        reader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
