"""In-memory carrier controls; all process and network execution forbidden."""
from __future__ import annotations

import asyncio
import copy
from contextlib import ExitStack
from pathlib import Path
import unittest
from unittest import mock

import runtime_package_bootstrap as bootstrap
import runtime_package_bootstrap_contract as contract
import runtime_package_bootstrap_custody as carrier
import runtime_package_bootstrap_test as fixtures


class Controls(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for target in ("subprocess.Popen", "socket.create_connection", "asyncio.open_connection"):
            self.stack.enter_context(mock.patch(target, side_effect=AssertionError("execution forbidden")))
        self.raw, self.writes = {}, {}
        self.receipt = fixtures.receipt(fixtures.plan())
        self.custody = {"files": []}
        for name in carrier.RUNTIME_REFERENCES:
            raw = (b'{ "original controller": true }\n' if name == "controller" else
                   contract.encode(self.receipt) if name == "bootstrap" else contract.encode({"original": name}))
            row = self.row("/work/" + name + ".json", raw, name.replace("_", "-"))
            self.custody["files"].append(row)
            self.custody[name] = row["ref"]
        self.custody["controller_context"] = {"context": {"original controller": True},
            "raw_sha256": self.custody["controller"]["sha256"],
            "raw_bytes": self.raw["/work/controller.json"], "observed_wall": 123.0, "observed_monotonic_ns": 456}
        self.offline = self.row("/inputs/installed.json", b'{ "original installed": true }\n', "installed-offline-evidence")
        identity = {key: self.receipt[key] for key in carrier.IDENTITY_FIELDS - {"binary_sha256"}}
        identity["binary_sha256"] = identity["artifact_sha256"]
        identity["artifact_sha256"] = "e" * 64  # Original archive differs from executable.
        review = {"identities": {self.receipt["backend"]: identity}, "input_refs": {"installed": self.offline["ref"]}}
        self.review = self.row("/inputs/runtime-review.json", b"  " + contract.encode(review), "runtime-review")
        self.bundle = {"format": "recall-runtime-original-inputs/v2", "review": self.review["ref"],
                       "installed_offline": self.offline["ref"], "files": [self.offline, self.review]}
        self.bundle_reference = self.save_bundle()
        self.review_reference = {"path": self.review["path"], "sha256": self.review["ref"]["sha256"]}
        self.reads = []
        def read(path, maximum, **_):
            self.reads.append(str(path))
            raw = self.raw[str(path)]
            if len(raw) > maximum:
                raise ValueError("bounded original read")
            return raw
        self.stack.enter_context(mock.patch.object(carrier, "read_bound", side_effect=read))
        self.stack.enter_context(mock.patch.object(carrier, "private_new", side_effect=lambda path, raw:
                                                    self.writes.__setitem__(str(path), raw)))

    def row(self, path, raw, kind):
        self.raw[path] = raw
        return {"path": path, "ref": {"sha256": contract.sha(raw), "bytes": len(raw), "kind": kind}}

    def save_bundle(self):
        row = self.row("/inputs/runtime-inputs.json", contract.encode(self.bundle), "input-bundle")
        return {"path": row["path"], "sha256": row["ref"]["sha256"]}

    def load(self):
        return carrier.load_original_inputs(self.bundle_reference, self.review_reference)

    def write(self, originals=None):
        return carrier.write_runtime_custody(self.custody, originals or self.load(), self.receipt,
                                            Path("/work/runtime-custody.json"))

    def test_versioned_carrier_retains_original_bytes_and_distinct_archive_identity(self):
        originals = self.load()
        self.assertTrue(originals["review_raw"].startswith(b"  "))
        before = copy.deepcopy(self.custody)
        ref = self.write(originals)
        raw = self.writes[ref["path"]]
        self.assertEqual(contract.sha(raw), ref["sha256"])
        packet = contract.strict_json(raw)
        self.assertEqual(set(packet), {"format", "observed_runtime", "controller_context", "files"})
        self.assertEqual(packet["format"], "recall-runtime-custody/v2")
        self.assertEqual(packet["observed_runtime"]["identity"]["artifact_sha256"], "e" * 64)
        self.assertEqual(packet["observed_runtime"]["identity"]["binary_sha256"], self.receipt["artifact_sha256"])
        self.assertEqual(packet["observed_runtime"]["identity"]["paired_artifact_sha256"],
                         self.receipt["paired_artifact_sha256"])
        self.assertEqual(packet["controller_context"], {"ref": self.custody["controller"],
            "observed_wall": 123.0, "observed_monotonic_ns": 456})
        self.assertEqual(self.custody, before)
        self.assertNotIn("qualified", packet)
        self.assertEqual(packet["observed_runtime"]["outer_launch"], self.custody["outer_launch"])

    def test_only_two_fixed_outer_launch_mounts_have_their_original_kinds_and_bounds(self):
        originals = [self.row(path, b"original " + kind.encode(), kind)
                     for path, kind in carrier.GATEWAY_ORIGINALS.items()]
        self.custody["files"].extend(originals)
        ref = self.write()
        packet = contract.strict_json(self.writes[ref["path"]])
        self.assertTrue(all(row in packet["files"] for row in originals))
        for mutation in ("renamed", "kind", "oversize", "input"):
            rows = copy.deepcopy(originals)
            if mutation == "renamed":
                rows[0]["path"] = "/gateway-user-selected.json"
            elif mutation == "kind":
                rows[0]["ref"]["kind"] = "gateway-launch-spec"
            elif mutation == "oversize":
                rows[0]["ref"]["bytes"] = 131073
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                carrier.inventory(rows, inputs_only=mutation == "input")

    def test_missing_installed_original_refuses_before_original_body_read(self):
        self.bundle["files"] = [self.review]
        self.bundle_reference = self.save_bundle()
        with self.assertRaisesRegex(ValueError, "installed prerequisite absent"):
            self.load()
        self.assertEqual(self.reads, ["/inputs/runtime-inputs.json"])

    def test_duplicate_foreign_and_boolean_sizes_refuse_before_body_reads(self):
        original = copy.deepcopy(self.bundle)
        for change in ("duplicate", "foreign", "size", "kind"):
            self.bundle = copy.deepcopy(original)
            if change == "duplicate":
                self.bundle["files"].append(self.review)
            elif change == "foreign":
                self.bundle["files"][0]["path"] = "/work/installed.json"
            elif change == "size":
                self.bundle["files"][0]["ref"]["bytes"] = True
            else:
                self.bundle["files"].append({"path": "/inputs/review-alias.json",
                    "ref": {**self.review["ref"], "kind": "another-kind"}})
            self.bundle_reference = self.save_bundle()
            self.reads.clear()
            with self.assertRaises(ValueError):
                self.load()
            self.assertEqual(self.reads, ["/inputs/runtime-inputs.json"])

    def test_original_changed_after_load_refuses_without_sidecar(self):
        originals = self.load()
        self.raw[self.offline["path"]] = b"changed"
        with self.assertRaises(ValueError):
            self.write(originals)
        self.assertFalse(self.writes)

    def test_current_capture_change_and_missing_envelope_reference_refuse(self):
        originals = self.load()
        self.raw["/work/bootstrap.json"] = b"changed"
        with self.assertRaises(ValueError):
            self.write(originals)
        self.raw["/work/bootstrap.json"] = contract.encode(self.receipt)
        self.custody["cleanup_obligations"] = self.offline["ref"] | {"sha256": "9" * 64}
        with self.assertRaisesRegex(ValueError, "uncaptured original"):
            self.write(originals)
        self.assertFalse(self.writes)

    def test_different_current_invocation_cannot_borrow_original_review(self):
        originals = self.load()
        self.receipt["run_nonce"] = "another-allocation"
        with self.assertRaisesRegex(ValueError, "another original invocation"):
            self.write(originals)
        self.assertFalse(self.writes)

    def test_different_actual_paired_executable_cannot_borrow_original_review(self):
        originals = self.load()
        self.receipt["paired_artifact_sha256"] = "9" * 64
        with self.assertRaisesRegex(ValueError, "another original invocation"):
            self.write(originals)
        self.assertFalse(self.writes)

    def test_primary_v2_requires_custody_but_preserves_original_permission(self):
        phase = {"plan": fixtures.plan(), "template": {"format": "recall-primary-provider-run/v2",
            "execution_authorized": False, "provider_dispatch_authorized": False, "inputs": {}},
            "review_reference": self.review_reference}
        with self.assertRaisesRegex(ValueError, "v2 requires"):
            bootstrap.primary_manifest(phase, self.receipt, {}, {})
        ref = self.write()
        manifest = bootstrap.primary_manifest(phase, self.receipt, {}, {}, ref)
        self.assertEqual(manifest["observations"]["runtime_custody"], ref)
        self.assertFalse(manifest["execution_authorized"])
        self.assertFalse(manifest["provider_dispatch_authorized"])
        self.assertNotIn("observations", phase["template"])

    def test_historical_v1_refuses_before_context_credentials_or_services(self):
        with mock.patch.object(bootstrap, "actual_context", side_effect=AssertionError("context forbidden")), \
             self.assertRaisesRegex(ValueError, "historical bootstrap-only"):
            asyncio.run(bootstrap.execute_phase({"template": {"format": "recall-primary-provider-run/v1"}}))


if __name__ == "__main__":
    unittest.main()
