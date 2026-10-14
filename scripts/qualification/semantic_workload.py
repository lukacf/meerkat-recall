#!/usr/bin/env python3
"""Run the original D28 component chain only under its reviewed G allocation.

Source availability is not execution authorization. This entry opens no cohort
until original installed/runtime evidence passes L's independently reviewed
predicate. All service custody and terminal cleanup remain C's original work.
"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
COMPONENTS = ROOT / "tests/recall_preview_semantics"
sys.path.insert(0, str(COMPONENTS))
from workload_orchestration import (
    OriginalFiles, VALIDATOR, STARTUP, current_expiry, current_runtime, digest,
    encode, load_helper, private_new, read_original, require, reviewed_static,
    reviewed_predicates, strict_json, verify_helpers, profiles,
)

SELF = "scripts/qualification/semantic_workload.py"
PHASES = "tests/recall_preview_semantics/workload_phase_capture.py"
BACKENDS = ("elephant-db", "surreal-db")
INPUT_ROOT = Path("/inputs")
MANIFEST_REFERENCE_FORMAT = "recall-workload-manifest-reference/v1"
MANIFEST_REFERENCE_BYTES = 4096


def descriptor(value, maximum):
    require(type(value) is dict and set(value) == {"path", "sha256"}
            and type(value["path"]) is str and Path(value["path"]).is_relative_to(INPUT_ROOT)
            and type(value["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", value["sha256"]),
            "d28_original_input_descriptor")
    raw = read_original(value["path"], maximum)
    require(digest(raw) == value["sha256"], "d28_original_input_hash")
    return strict_json(raw)


def manifest_digest(manifest_path, reference_path):
    """Resolve one original digest under G's frozen, independently reviewed inputs.

    The outer input inventory pins this descriptor. Putting its digest in the
    nested runtime review would recreate the command/manifest/review hash cycle.
    This descriptor grants no execution or provider authority.
    """
    require(manifest_path.is_absolute() and manifest_path.is_relative_to(INPUT_ROOT)
            and manifest_path == manifest_path.resolve() and manifest_path.is_file()
            and not manifest_path.is_symlink(), "workload_manifest_reference_target")
    require(reference_path.is_absolute() and reference_path.is_relative_to(INPUT_ROOT)
            and reference_path == reference_path.resolve() and reference_path != manifest_path
            and reference_path.is_file() and not reference_path.is_symlink(),
            "workload_manifest_reference_path")
    reference = strict_json(read_original(reference_path, MANIFEST_REFERENCE_BYTES))
    require(type(reference) is dict and set(reference) == {"format", "path", "sha256"}
            and reference["format"] == MANIFEST_REFERENCE_FORMAT
            and reference["path"] == str(manifest_path)
            and type(reference["sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", reference["sha256"]),
            "workload_manifest_reference_fields")
    return reference["sha256"]


def inputs(manifest_path, manifest_sha256):
    manifest = descriptor({"path": str(manifest_path), "sha256": manifest_sha256}, 65536)
    require(type(manifest) is dict, "d28_original_run_manifest")
    selected = profiles.profile(manifest.get("profile_id", profiles.LEGACY_ID))
    fields = {"format", "execution_authorized", "provider_dispatch_authorized",
              "backends", "runtime_original_inputs", "output_directory"}
    if selected.id == profiles.SEMANTIC140_ID:
        fields.add("profile_id")
    require(set(manifest) == fields
            and manifest["format"] == "recall-d28-orchestration/v2"
            and manifest["execution_authorized"] is True and manifest["provider_dispatch_authorized"] is True
            and manifest["backends"] == list(BACKENDS)
            and type(manifest["runtime_original_inputs"]) is dict
            and set(manifest["runtime_original_inputs"]) == set(BACKENDS), "d28_original_run_manifest")
    prepared, common_review = {}, None
    for backend in BACKENDS:
        bundle = descriptor(manifest["runtime_original_inputs"][backend], 8 * 1024**2)
        require(type(bundle) is dict and set(bundle) == {"format", "review", "installed_offline", "files"}
                and bundle["format"] == "recall-runtime-original-inputs/v2", "d28_original_runtime_inputs")
        files = OriginalFiles(profile_id=selected.id)
        require(all(type(row) is dict and type(row.get("path")) is str
                    and Path(row["path"]).is_relative_to(INPUT_ROOT) for row in bundle["files"]),
                "d28_static_original_inputs_mount")
        files.import_files(bundle["files"])
        reviewed = reviewed_static(files, bundle["review"])
        review = reviewed["review"]
        require(profiles.profile_for_review(review) is selected, "workload_original_review_profile")
        require(common_review is None or common_review == reviewed["raw_sha256"], "d28_different_original_review")
        common_review = reviewed["raw_sha256"]
        current_expiry(review)
        require({SELF, PHASES} <= set(review["helper_sha256"]), "d28_unreviewed_orchestrator_source")
        verify_helpers(review)
        files.get(bundle["installed_offline"], "installed-offline-evidence")
        prepared[backend] = (files, reviewed, bundle["installed_offline"])
    nonce = prepared[BACKENDS[0]][1]["review"]["identities"][BACKENDS[0]]["run_nonce"]
    require(all(item[1]["review"]["identities"][backend]["run_nonce"] == nonce for backend, item in prepared.items()),
            "d28_original_run_nonce_changed")
    prefix = "/work/semantic140-" if selected.id == profiles.SEMANTIC140_ID else "/work/d28-"
    require(manifest["output_directory"] == prefix + nonce, "d28_original_output_directory")
    return manifest, prepared


async def run(manifest, prepared):
    root = Path(manifest["output_directory"])
    require(root == root.resolve() and not root.exists(), "d28_new_private_output_required")
    root.mkdir(mode=0o700)
    records, error_type = [], None
    try:
        for backend in BACKENDS:
            files, reviewed, installed = prepared[backend]
            # Both contexts reach the same actual G parent and original whole
            # clock. The second is reached only after the first's clean exit.
            verify_helpers(reviewed["review"])
            startup = load_helper("d28_original_startup", STARTUP, reviewed["review"])
            phases = load_helper("d28_actual_phase_capture", PHASES, reviewed["review"])
            record = {"backend": backend, "output_directory": str(root / backend), "status": "started"}
            records.append(record)
            try:
                with reviewed_predicates(reviewed["review"]):
                    validator = load_helper("d28_independent_components", VALIDATOR, reviewed["review"])
                    async with current_runtime(files, reviewed, validator, startup, backend, installed, root / backend) as custody:
                        record["components"] = await phases.prepare_and_measure(custody, manifest)
                record["status"] = "collected-and-cleaned"
            finally:
                record["files"] = [{"path": path, "ref": ref} for path, ref in sorted(files.files.items())]
    except BaseException as error:
        error_type = type(error).__name__
        raise
    finally:
        # Keep original failures and partial records, never a replacement run.
        report = {"format": "recall-d28-orchestration-observation/v2", "backends": records,
                  "run_error_type": error_type, "product_acceptance": False,
                  "provider_request_association": "unmet",
                  "collector_verdict": "passed" if error_type is None and len(records) == 2
                      and all(row["status"] == "collected-and-cleaned" for row in records) else "failed"}
        if manifest.get("profile_id") == profiles.SEMANTIC140_ID:
            report["profile_id"] = profiles.SEMANTIC140_ID
        private_new(root / "observation.json", encode(report))
    return 0


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    expected = parser.add_mutually_exclusive_group(required=True)
    expected.add_argument("--manifest-sha256")
    expected.add_argument("--manifest-sha256-file", type=Path,
                          help="Original bounded manifest reference in the frozen /inputs inventory")
    parser.add_argument("--execute-reviewed", action="store_true", required=True)
    return parser.parse_args(argv)


def main():
    args = arguments()
    require(sys.version_info >= (3, 11), "d28_python_deadline_support")
    expected = (args.manifest_sha256 if args.manifest_sha256_file is None else
                manifest_digest(args.manifest, args.manifest_sha256_file))
    return asyncio.run(run(*inputs(args.manifest, expected)))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(encode({"verdict": "unmet-prerequisite", "error_type": type(error).__name__, "product_acceptance": False}).decode(), file=sys.stderr)
        sys.exit(78)
