"""Pure adapter controls: in-memory original bytes and an explicit validator spy.

These tests do not qualify Recall or simulate the independent installed oracle.
"""
from __future__ import annotations

import copy
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from primary_provider_driver_contract import encode, sha
import primary_provider_driver_runtime as runtime
import primary_provider_driver_startup as startup


def fixture():
    raw_bootstrap = b'{"qualified":false,"product_acceptance":false}\n'
    raw_context = b'{ "format":"original-context" }\n'
    raw_installed = b'{"format":"original-installed-evidence"}\n'
    raw_review = b'{"format":"original-review"}\n'
    data = {"/work/receipt.json": raw_bootstrap, "/work/context.json": raw_context,
            "/inputs/installed.json": raw_installed, "/inputs/review.json": raw_review}
    kinds = ["runtime-bootstrap", "gateway-context", "installed-offline-evidence", "runtime-review"]
    files = [{"path": path, "ref": {"sha256": sha(raw), "bytes": len(raw), "kind": kind}}
             for (path, raw), kind in zip(data.items(), kinds)]
    manifest = {"format": "recall-primary-provider-run/v2", "source_commit": "a"*40,
        "artifact_sha256": "b"*64, "paired_artifact_sha256": "1"*64,
        "runtime_sha256": "c"*64, "backend": "elephant-db",
        "endpoint": "http://127.0.0.1:43130", "nonce": "originalnonce123456",
        "configuration_sha256": "d"*64,
        "observations": {name: {"path": row["path"], "sha256": row["ref"]["sha256"]}
                         for name, row in zip(("runtime_receipt", "gateway_context"), files)},
        "inputs": {"runtime_review": {"path": "/inputs/review.json", "sha256": sha(raw_review)}}}
    context = {"format": "original-context"}
    observed = {"format": "recall-dynamic-runtime-evidence/v2", "bootstrap": files[0]["ref"],
                "controller": files[1]["ref"], "installed_offline": files[2]["ref"]}
    packet = {"format": runtime.SIDECAR_FORMAT, "observed_runtime": observed,
        "controller_context": {"ref": files[1]["ref"], "observed_wall": 100.0, "observed_monotonic_ns": 123}, "files": files}
    loaded = {"runtime_custody": packet, "gateway_context": context,
              "runtime_review_raw": raw_review, "runtime_review": {"format": "original-review"},
              "runtime_original_inputs": {"format": "recall-runtime-original-inputs/v2",
                  "review": files[3]["ref"], "installed_offline": files[2]["ref"], "files": files[2:]}}
    review = {"dependencies": {runtime.VALIDATOR: "e"*64, runtime.INSTALLED_VALIDATOR: "2"*64}}
    identity = {"source_commit": manifest["source_commit"], "binary_sha256": manifest["artifact_sha256"],
        "artifact_sha256": "f"*64, "paired_artifact_sha256": "1"*64,
        "runtime_sha256": manifest["runtime_sha256"], "backend": manifest["backend"],
        "endpoint": manifest["endpoint"], "run_nonce": manifest["nonce"],
        "outer_configuration_sha256": manifest["configuration_sha256"]}
    component = {"format": "recall-installed-runtime-component/v2", "verdict": "passed",
        "scope": "installed-offline-and-current-runtime", "product_acceptance": False, "identity": identity,
        "remaining_cleanup_obligations": {"recall": "original owned service remains live"},
        "proof_limits": ["not semantic or product acceptance"]}
    return data, manifest, loaded, review, component


class RuntimeAdapterControls(unittest.TestCase):
    def test_fixed_gateway_originals_keep_exact_types_caps_and_lazy_alias_rechecks(self):
        originals = {"/gateway-manifest.json": b'{ "original":"manifest" }\n',
                     "/gateway-launch.json": b'{ "original":"launch" }\n'}
        files = [{"path": path, "ref": {"sha256": sha(raw), "bytes": len(raw),
                                       "kind": runtime.GATEWAY_ORIGINALS[path]}}
                 for path, raw in originals.items()]
        alias = "/work/original-manifest-copy.json"
        originals[alias] = originals[files[0]["path"]]
        files.append({"path": alias, "ref": dict(files[0]["ref"])})
        reads = []
        def read(path, maximum):
            reads.append((path, maximum))
            return originals[path]
        evidence = runtime.original_evidence(files, reader=read)
        self.assertEqual((len(evidence), list(evidence), reads),
                         (2, [row["ref"]["sha256"] for row in files[:2]], []))
        checksum = files[0]["ref"]["sha256"]
        self.assertEqual(evidence[checksum], originals[files[0]["path"]])
        self.assertEqual([path for path, _ in reads], [files[0]["path"], alias])
        originals[files[0]["path"]] += b"changed"
        with self.assertRaisesRegex(ValueError, "bytes or digest"):
            evidence[checksum]
        self.assertEqual(runtime.MAX_RAW_BYTES, 352 * 1024**2)

    def test_fixed_gateway_path_type_size_refusal_precedes_any_read(self):
        ref = {"sha256": "a"*64, "bytes": 10, "kind": "gateway-manifest"}
        cases = [{"path": path, "ref": ref} for path in (
            "/gateway-manifest.json/child", "/gateway-manifest.json/", "/gateway-manifest.json.bak",
            "//gateway-manifest.json", "/gateway-launch.other", "/etc/passwd", "/inputs/../gateway-manifest.json")]
        cases += [{"path": "/gateway-manifest.json", "ref": {**ref, "kind": "runtime-review"}},
                  {"path": "/gateway-launch.json", "ref": ref},
                  {"path": "/gateway-manifest.json", "ref": {**ref, "bytes": 131073}}]
        for row in cases:
            with self.subTest(row=row), self.assertRaises(ValueError):
                runtime.original_evidence([row], reader=lambda *args: self.fail("must refuse before reading"))
        with patch.object(runtime.os, "open", side_effect=AssertionError("must refuse before opening")):
            for maximum in (131073, True, -1):
                with self.assertRaisesRegex(ValueError, "read bound"):
                    runtime.read_original("/gateway-manifest.json", maximum)

    def test_fixed_gateway_actual_reader_requires_private_regular_original_and_no_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            original = Path(temporary) / "original"
            original.write_bytes(b'{ "original":true }\n')
            original.chmod(0o600)
            real_open = os.open
            def opened(path, flags):
                self.assertIn(str(path), runtime.GATEWAY_ORIGINALS)
                self.assertTrue(flags & os.O_NOFOLLOW)
                return real_open(original, flags)
            with patch.object(runtime.os, "open", side_effect=opened):
                self.assertEqual(runtime.read_original("/gateway-manifest.json", 131072), original.read_bytes())
                original.chmod(0o644)
                with self.assertRaisesRegex(ValueError, "not private"):
                    runtime.read_original("/gateway-manifest.json", 131072)
                original.chmod(0o600)
                with patch.object(runtime.os, "getuid", return_value=original.stat().st_uid+1), self.assertRaisesRegex(ValueError, "not private"):
                    runtime.read_original("/gateway-launch.json", 131072)
                original.write_bytes(b"x"*131073)
                with self.assertRaisesRegex(ValueError, "bounded original"):
                    runtime.read_original("/gateway-manifest.json", 131072)
                original.unlink()
                with self.assertRaises(FileNotFoundError):
                    runtime.read_original("/gateway-manifest.json", 131072)
                target = Path(temporary) / "target"
                target.write_bytes(b"untrusted alias")
                original.symlink_to(target)
                with self.assertRaises(OSError):
                    runtime.read_original("/gateway-manifest.json", 131072)

    def test_fixed_gateway_stat_bracket_rejects_same_length_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            original = Path(temporary) / "original"
            original.write_bytes(b"original bytes")
            original.chmod(0o600)
            real_open, real_stat = os.open, os.fstat
            calls = []
            def observed(descriptor):
                info = real_stat(descriptor)
                calls.append(info)
                if len(calls) == 2:
                    os.utime(original, ns=(info.st_atime_ns, info.st_mtime_ns+1000))
                    info = real_stat(descriptor)
                return info
            with patch.object(runtime.os, "open", side_effect=lambda path, flags: real_open(original, flags)), \
                    patch.object(runtime.os, "fstat", side_effect=observed), \
                    self.assertRaisesRegex(ValueError, "changed during"):
                runtime.read_original("/gateway-launch.json", 131072)
            self.assertEqual(len(calls), 2)

    def test_actual_original_bytes_reach_only_reviewed_validator(self):
        data, manifest, loaded, review, component = fixture()
        calls = []
        def validate(original_review, observed, raw, controller):
            calls.append((original_review, observed, raw, controller))
            self.assertIs(original_review["raw_bytes"], loaded["runtime_review_raw"])
            self.assertEqual(controller["raw_bytes"], data["/work/context.json"])
            self.assertNotEqual(controller["raw_bytes"], encode(controller["context"]))
            self.assertEqual(raw[observed["installed_offline"]["sha256"]], data["/inputs/installed.json"])
            return component
        def loader(root, path, expected):
            self.assertEqual(root, Path("/source"))
            if path == runtime.INSTALLED_VALIDATOR:
                self.assertEqual(expected, "2"*64)
                return SimpleNamespace(original_installed_predicate=True)
            self.assertEqual((path, expected), (runtime.VALIDATOR, "e"*64))
            return SimpleNamespace(validate_runtime=validate)
        result = runtime.validate_runtime_custody(Path("/source"),manifest,loaded,review,
            reader=lambda path, maximum:data[path],loader=loader)
        self.assertIs(result,component)
        self.assertEqual(len(calls),1)
        self.assertEqual(result["remaining_cleanup_obligations"],component["remaining_cleanup_obligations"])
        self.assertEqual(set(loaded["runtime_custody"]["controller_context"]), {"ref","observed_wall","observed_monotonic_ns"})

    def test_canonical_import_is_scoped_to_reviewed_helper_on_success_and_failure(self):
        installed = SimpleNamespace(validate_installed=lambda: "original reviewed helper")
        previous = SimpleNamespace(validate_installed=lambda: self.fail("foreign cached helper used"))
        dependencies = {runtime.VALIDATOR: "e"*64, runtime.INSTALLED_VALIDATOR: "2"*64}
        def loader(root, path, expected):
            self.assertEqual(expected, dependencies[path])
            return installed if path == runtime.INSTALLED_VALIDATOR else SimpleNamespace()
        with patch.dict(sys.modules, {"installed_evidence_validator": previous}):
            for refuses in (False, True):
                try:
                    with runtime.reviewed_runtime_validator(Path("/source"), dependencies, loader):
                        from installed_evidence_validator import validate_installed
                        self.assertEqual(validate_installed(), "original reviewed helper")
                        if refuses:
                            raise ValueError("original evidence refused")
                except ValueError as error:
                    self.assertTrue(refuses)
                    self.assertEqual(str(error), "original evidence refused")
                self.assertIs(sys.modules["installed_evidence_validator"], previous)
        with patch.dict(sys.modules):
            sys.modules.pop("installed_evidence_validator", None)
            with runtime.reviewed_runtime_validator(Path("/source"), dependencies, loader):
                self.assertIs(sys.modules["installed_evidence_validator"], installed)
            self.assertNotIn("installed_evidence_validator", sys.modules)
        with self.assertRaisesRegex(ValueError, "dependency absent"):
            with runtime.reviewed_runtime_validator(Path("/source"), {runtime.VALIDATOR: "e"*64},
                    lambda *args: self.fail("missing pin must refuse before loading")):
                self.fail("unreviewed predicate must not run")

    def test_shared_validator_failure_is_not_bootstrap_success(self):
        data, manifest, loaded, review, _ = fixture()
        def refuse(*args):
            raise ValueError("actual installed prerequisite absent")
        with self.assertRaisesRegex(ValueError,"installed prerequisite"):
            runtime.validate_runtime_custody(Path("/source"),manifest,loaded,review,
                reader=lambda path, maximum:data[path],loader=lambda *args:SimpleNamespace(validate_runtime=refuse))
        manifest["format"] = "recall-primary-provider-run/v1"
        with self.assertRaisesRegex(ValueError,"explicit v2"):
            runtime.validate_runtime_custody(Path("/source"),manifest,loaded,review,
                reader=lambda *args:self.fail("legacy run must not read evidence"))

    def test_duplicate_paths_type_substitution_and_bounds_fail_before_read(self):
        data, _, loaded, _, _ = fixture()
        files = loaded["runtime_custody"]["files"]
        cases = [files+[copy.deepcopy(files[0])],
                 [*files,{"path":"/inputs/alias", "ref":{**files[0]["ref"],"kind":"installed-offline"}}],
                 [{"path":"/work/../tokens", "ref":files[0]["ref"]}],
                 [{"path":"/work/file", "ref":{**files[0]["ref"],"bytes":True}}],
                 [{"path":"/etc/shadow", "ref":files[0]["ref"]}],
                 [{"path":f"/work/large-{number}", "ref":{"sha256":f"{number:064x}",
                     "bytes":64*1024**2,"kind":"setup-log"}} for number in range(6)]]
        for changed in cases:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                runtime.original_evidence(changed,reader=lambda *args:self.fail("pre-read refusal required"))

    def test_original_mutation_or_missing_file_is_not_reconstructed(self):
        data, _, loaded, _, _ = fixture()
        files = loaded["runtime_custody"]["files"]
        with self.assertRaisesRegex(ValueError,"bytes or digest"):
            runtime.original_evidence(files, reader=lambda path,maximum:data[path]+b" ")[files[0]["ref"]["sha256"]]
        def absent(*args):
            raise FileNotFoundError("original file is absent")
        with self.assertRaises(FileNotFoundError):
            runtime.original_evidence(files,reader=absent)[files[0]["ref"]["sha256"]]

    def test_lazy_mapping_reads_only_selected_originals_and_rechecks_mutation(self):
        data, _, loaded, _, _ = fixture()
        files = loaded["runtime_custody"]["files"]
        reads = []
        def read(path,maximum):
            reads.append((path,maximum))
            return data[path]
        evidence = runtime.original_evidence(files,reader=read)
        self.assertEqual(reads,[])
        self.assertEqual(len(list(evidence)),4)
        self.assertEqual(reads,[])
        selected = files[2]
        self.assertEqual(evidence[selected["ref"]["sha256"]],data[selected["path"]])
        self.assertEqual(reads,[(selected["path"],selected["ref"]["bytes"])])
        data[selected["path"]] += b"changed"
        with self.assertRaisesRegex(ValueError,"bytes or digest"):
            evidence[selected["ref"]["sha256"]]
        self.assertEqual(len(reads),2)
        self.assertFalse(any(path=="/work/receipt.json" for path,_ in reads))

    def test_same_original_bytes_at_distinct_paths_keep_one_digest(self):
        data, _, loaded, _, _ = fixture()
        files = loaded["runtime_custody"]["files"]
        data["/inputs/same-original"] = data["/work/receipt.json"]
        files.append({"path":"/inputs/same-original","ref":dict(files[0]["ref"])})
        result = runtime.original_evidence(files,reader=lambda path,maximum:data[path])
        self.assertEqual(len(result),4)
        checksum = files[0]["ref"]["sha256"]
        self.assertEqual(result[checksum],data["/work/receipt.json"])
        data["/inputs/same-original"] += b"changed"
        with self.assertRaisesRegex(ValueError,"bytes or digest"):
            result[checksum]

    def test_immutable_installed_bundle_cannot_substitute_current_capture(self):
        data, manifest, loaded, review, _ = fixture()
        loaded["runtime_original_inputs"]["installed_offline"] = loaded["runtime_custody"]["files"][0]["ref"]
        with self.assertRaisesRegex(ValueError,"installed original"):
            runtime.validate_runtime_custody(Path("/source"),manifest,loaded,review,
                reader=lambda path,maximum:data[path],loader=lambda *args:self.fail("must refuse before validator"))

    def test_postvalidation_recheck_keeps_original_deadline(self):
        context = {"anchor":{"clock":{"mono_start":1000.0,"deadline_mono":6400.0}}}
        receipt = {"allocation_started_monotonic_ns":1000*10**9,"started_monotonic_ns":1010*10**9,
                   "observed_monotonic_ns":1020*10**9,"original_deadline_monotonic_ns":3700*10**9,
                   "whole_deadline_monotonic_ns":6400*10**9}
        loaded = {"gateway_context":context,"runtime_receipt":receipt,"original_deadline":3700.0}
        manifest = {"observations":{"gateway_context":{"path":"/work/context","sha256":"a"*64}}}
        review = {"dependencies":{startup.G_HELPER:"b"*64},"expected_launch":{}}
        gateway = SimpleNamespace(read_context=lambda *args,**kwargs:context)
        with patch.object(startup,"load_producer",return_value=gateway), patch.object(startup,"live_ancestry") as ancestry:
            with patch.object(startup.time,"monotonic_ns",return_value=3699*10**9):
                startup.revalidate_current_observations(Path("/source"),manifest,loaded,review)
            with patch.object(startup.time,"monotonic_ns",return_value=3701*10**9), self.assertRaises(ValueError):
                startup.revalidate_current_observations(Path("/source"),manifest,loaded,review)
            self.assertEqual(ancestry.call_count,2)
        self.assertEqual(loaded["original_deadline"],3700.0)

    def test_component_identity_or_scope_cannot_cross_invocations(self):
        data, manifest, loaded, review, component = fixture()
        for changed in ({**component,"scope":"startup-only"},
                        {**component,"product_acceptance":True},
                        {**component,"identity":{**component["identity"],"run_nonce":"replacementnonce"}},
                        {**component,"identity":{**component["identity"],"binary_sha256":"f"*64}},
                        {**component,"identity":{**component["identity"],"paired_artifact_sha256":"0"*64}}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                runtime.validate_runtime_custody(Path("/source"),manifest,loaded,review,
                    reader=lambda path,maximum:data[path],loader=lambda *args:SimpleNamespace(validate_runtime=lambda *args:changed))


if __name__ == "__main__":
    unittest.main()
