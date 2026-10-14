"""Actual pinned input seams only: no runtime, artifacts, processes or network.

Run centrally in the complete source composition, or set
RECALL_FULL_WORKLOAD_SOURCE_ROOT to that original checkout. Module imports here
are stdlib-only; actual reviewed helpers load inside the guarded test body.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch


PACKAGE_SHA256 = "0f84dfc43ced1556d15b18e6b73191b1e002e4694dd6305f5a84c3067355f098"
CANDIDATE_SHA256 = "455d14e8d77ba76c0064e70f403a4b24be2c1ff212dd566a3ea142f76c40f9de"
LOAD_EVIDENCE_SHA256 = "09ba111bcdcc8f9d91c6b2df44c2d17c619f45c76b039e5bc17797f49826d5d4"


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def source_module(name, path, raw):
    """Execute only this already-read original source, never a loader reread."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None:
        raise AssertionError("original module metadata unavailable")
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(path), "exec", dont_inherit=True), module.__dict__)
    return module


class ComposedInputControls(unittest.TestCase):
    def test_actual_full18_package_resolution_and_both_backend_input_binding(self):
        source_root = Path(os.environ.get(
            "RECALL_FULL_WORKLOAD_SOURCE_ROOT", str(Path(__file__).resolve().parents[3]),
        )).resolve(strict=True)
        package_dir = source_root / "tests/recall_preview_semantics/full_workload_plan"
        # The guards cover source loading as well as the exercised functions.
        # No fake process/startup receipt or isolation verdict is supplied.
        with ExitStack() as guards, tempfile.TemporaryDirectory() as temporary:
            for target in (
                "socket.socket", "socket.create_connection", "socket.create_server",
                "subprocess.Popen", "subprocess.run", "os.system",
                "asyncio.open_connection", "asyncio.start_server",
            ):
                guards.enter_context(patch(target, side_effect=AssertionError(
                    "composed input controls forbid runtime or network: " + target,
                )))
            scratch = Path(temporary)
            package_raw = (package_dir / "case-plan.json").read_bytes()
            candidate_raw = (package_dir / "candidate-expectations.json").read_bytes()
            self.assertEqual(digest(package_raw), PACKAGE_SHA256)
            self.assertEqual(digest(candidate_raw), CANDIDATE_SHA256)
            package, candidate = json.loads(package_raw), json.loads(candidate_raw)
            self.assertEqual(set(package["inputs"]), {
                "contract", "corpus", "rendered", "preparation", "graph_manifest",
            })
            self.assertEqual(len(package["graph_recipe_ids"]), 24)
            expected_sources = {
                row["path"]: row["sha256"] for row in package["components"].values()
            }
            self.assertEqual(len(expected_sources), 18)
            for row in package["components"].values():
                raw = (source_root / row["path"]).read_bytes()
                self.assertEqual((digest(raw), len(raw)), (row["sha256"], row["bytes"]))
            review = package["independent_runtime_input_review"]
            review_raw = (source_root / review["path"]).read_bytes()
            self.assertEqual((digest(review_raw), len(review_raw)),
                             (review["sha256"], review["bytes"]))
            reviewed = json.loads(review_raw)
            self.assertEqual(reviewed["source_sha256"][
                "tests/recall_preview_semantics/full_workload_plan/candidate-expectations.json"
            ], CANDIDATE_SHA256)

            assembler_path = package_dir / "assemble.py"
            assembler = source_module("composed_original_assembler", assembler_path,
                                      assembler_path.read_bytes())
            roots = {name: source_root for name in ("qualification", "recipes", "components", "assembly")}
            plan, plan_sha256, provenance = assembler.assemble(
                package, roots, package["candidate_expectations"],
            )
            self.assertEqual(plan_sha256, digest(assembler.encode(plan)))
            self.assertEqual(provenance["expectations_sha256"], CANDIDATE_SHA256)
            self.assertLessEqual(provenance["source_bytes_read"], assembler.PACKAGE_LIMIT)
            self.assertEqual(plan["runtime_principal_templates"], candidate["runtime_principal_templates"])
            self.assertEqual(plan["worker_runtime_expectations"], candidate["worker_runtime_expectations"])
            self.assertEqual(len(plan["cases"]), 14)
            requests = plan["load_plan"]["requests"]
            self.assertEqual(len(requests), 280)
            self.assertEqual(len({row["caller"] for row in requests}), 140)
            requirements = plan["cases"]["FW12"]["query_requirements"]
            self.assertEqual(digest(assembler.encode(requirements)), LOAD_EVIDENCE_SHA256)
            self.assertEqual(sum(bool(row["required_claim_evidence"]) for row in requirements.values()), 48)
            corpus_path = source_root / package["inputs"]["corpus"]["path"]
            corpus = assembler.decode(corpus_path.read_bytes())
            identities = {(row["space"], row["key"]) for row in corpus["sources"]}
            self.assertEqual(len(identities), 50)
            identities.update((row["source_request"]["source"]["space_id"],
                               row["source_request"]["source"]["external_id"])
                              for row in plan["graph_recipes"].values())
            self.assertEqual(len(identities), 56)

            cli_path = source_root / "scripts/qualification/full_workload.py"
            cli_raw = cli_path.read_bytes()
            self.assertEqual(digest(cli_raw), expected_sources["scripts/qualification/full_workload.py"])
            bootstrap_cli = source_module("composed_original_cli_bootstrap", cli_path, cli_raw)
            originals, hashes = bootstrap_cli.snapshot_sources(expected_sources)
            self.assertEqual(hashes, expected_sources)
            with bootstrap_cli.reviewed_code(originals, hashes) as cli:
                self.assertEqual(cli.source_hashes(), expected_sources)
                document, identity = assembler.driver_input_document(plan, plan_sha256, provenance)
                document_raw = assembler.encode(document)
                document_path = scratch / "original-case-plan-input.json"
                document_path.write_bytes(document_raw)
                reader = cli.Inputs()
                resolved = reader.read({"path": str(document_path), "sha256": identity["document_sha256"]})
                self.assertEqual(set(resolved["graph_recipes"]), set(plan["graph_recipes"]))
                # Use the actual CLI original-file reader on every reference
                # emitted by the actual assembler, as its run reader does.
                resolved["graph_recipes"] = {
                    key: reader.read(reference["file"])
                    for key, reference in resolved["graph_recipes"].items()
                }
                self.assertTrue(resolved == plan, "resolved original plan values changed")
                self.assertEqual(digest(cli.encode(resolved)), plan_sha256)
                self.assertLessEqual(reader.bytes, cli.INPUT_CAP)
                contract = assembler.decode((source_root / package["inputs"]["contract"]["path"]).read_bytes())
                cli.validate_inputs(contract, corpus, resolved, plan_sha256)

                for backend in ("elephant-db", "surreal-db"):
                    with self.subTest(backend=backend):
                        # Explicitly synthetic PRE-OUTPUT identities. There is
                        # no archive, executable, service, token or allocation
                        # authorization behind these unit-control values.
                        runtime_inputs = {
                            "backend": backend, "run_nonce": "unit-composed-inputs",
                            "endpoint": "http://127.0.0.1:43130",
                            "spy_endpoint": "http://127.0.0.1:43131/v1",
                            "run_directory": str(scratch / "uncreated-capture" / backend),
                            "archive": str(scratch / "nonexistent-unit-release.tar.gz"),
                            "artifact_sha256": "a" * 64, "binary_sha256": "b" * 64,
                            "source_commit": "c" * 40, "target": "x86_64-unknown-linux-gnu",
                            "paired": None if backend == "elephant-db" else {
                                "binary": str(scratch / "nonexistent-unit-surreal"),
                                "binary_sha256": "d" * 64, "endpoint": "http://127.0.0.1:43132",
                            },
                            "spy_credential": candidate["controlled_spy_credential"]["value"],
                        }
                        runtime, runtime_sha256 = assembler.runtime_plan_document(
                            candidate, runtime_inputs, plan["spy_recipe"],
                        )
                        self.assertEqual(runtime_sha256, digest(cli.encode(runtime)))
                        allocation = {key: runtime_inputs[key] for key in (
                            "backend", "run_nonce", "endpoint", "artifact_sha256", "binary_sha256", "source_commit",
                        )}
                        allocation["execution_authorized"] = False
                        manifest = {key: allocation[key] for key in ("backend", "run_nonce", "endpoint")}
                        manifest.update(spy_port=43131, output=str(scratch / "uncreated-capture"))
                        self.assertIs(cli.bind_runtime_inputs(runtime, plan, manifest, allocation), runtime)
                        for role, field, replacement in (
                            ("manage", "subject", "substituted-unit-manager"),
                            ("manage", "scopes", ["read:source"]),
                            ("revocable", "scopes", ["read:source", "write:source"]),
                        ):
                            changed = copy.deepcopy(runtime)
                            changed["principals"]["qualification-household"][role][field] = replacement
                            with self.subTest(role=role, field=field), self.assertRaisesRegex(
                                sys.modules["full_workload_transport"].Unmet,
                                "original management or revocable principal differs",
                            ):
                                cli.bind_runtime_inputs(changed, plan, manifest, allocation)
                self.assertFalse((scratch / "uncreated-capture").exists())
                print(json.dumps({
                    "control": "actual-full18-original-input-seams", "package_sha256": PACKAGE_SHA256,
                    "candidate_sha256": CANDIDATE_SHA256, "resolved_plan_sha256": plan_sha256,
                    "cli_document_sha256": identity["document_sha256"], "source_components": len(hashes),
                    "source_identities": len(identities), "requests": len(requests),
                    "synthetic_backend_bindings": 2, "runtime_executed": False,
                }, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
