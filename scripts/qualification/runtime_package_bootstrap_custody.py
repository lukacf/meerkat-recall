"""Versioned original runtime carrier for the independently reviewed consumer.

This producer verifies locations and original byte identity, never qualification.
The existing v1 startup/context receipts are retained unchanged. Installed proof
must already exist in the immutable reviewed inputs; it cannot be synthesized.
"""
from __future__ import annotations

from pathlib import Path
import re

from runtime_package_bootstrap_contract import encode, fields, require, sha, strict_json
from runtime_package_bootstrap_io import private_new, read_bound

MAX_FILES = 16384
MAX_FILE_BYTES = 64 * 1024**2
MAX_RAW_BYTES = 352 * 1024**2
MAX_METADATA_BYTES = 8 * 1024**2
RUNTIME_REFERENCES = ("bootstrap", "controller", "actual_runtime_configuration", "actual_space_configurations",
                      "current_processes", "capture_inventory", "command_inventory", "cleanup_obligations",
                      "outer_launch")
IDENTITY_FIELDS = {"source_commit", "artifact_sha256", "binary_sha256", "runtime_sha256",
                   "paired_artifact_sha256", "backend", "endpoint", "run_nonce"}
GATEWAY_ORIGINALS = {"/gateway-manifest.json": "gateway-manifest", "/gateway-launch.json": "gateway-launch-spec"}


def inventory(rows, *, inputs_only=False):
    """Check all advertised bounds before the first original body is read."""
    require(type(rows) is list and 0 < len(rows) <= MAX_FILES, "original runtime inventory count differs")
    paths, hashes, total = {}, {}, 0
    for row in rows:
        fields(row, {"path", "ref"})
        value = row["path"]
        require(type(value) is str and 0 < len(value) <= 1024, "original runtime path required")
        path = Path(value)
        require(path.is_absolute() and str(path) == value and ".." not in path.parts
                and (path.is_relative_to("/inputs") if inputs_only else
                     path.is_relative_to("/inputs") or path.is_relative_to("/work")
                     or value == "/launch-anchor.json" or value in GATEWAY_ORIGINALS),
                "original runtime path is outside retained custody")
        require(value not in paths, "duplicate original runtime path")
        ref = row["ref"]
        fields(ref, {"sha256", "bytes", "kind"})
        require(type(ref["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"])
                and type(ref["bytes"]) is int and 0 <= ref["bytes"] <= MAX_FILE_BYTES
                and type(ref["kind"]) is str and 0 < len(ref["kind"]) <= 128,
                "original runtime typed reference invalid")
        if value in GATEWAY_ORIGINALS:
            require(ref["kind"] == GATEWAY_ORIGINALS[value] and ref["bytes"] <= 131072,
                    "fixed original gateway input kind or bound differs")
        if ref["sha256"] in hashes:
            require(hashes[ref["sha256"]] == ref, "original runtime digest has conflicting kind or size")
        else:
            total += ref["bytes"]
            require(total <= MAX_RAW_BYTES, "original runtime aggregate bound exceeded")
            hashes[ref["sha256"]] = dict(ref)
        paths[value] = dict(ref)
    return paths


def original(path, reference):
    raw = read_bound(path, reference["bytes"], private=True)
    require(len(raw) == reference["bytes"] and sha(raw) == reference["sha256"],
            "original runtime bytes changed")
    return raw


def load_original_inputs(bundle_reference, review_reference):
    """Read the immutable location bundle; retain no aggregate body cache."""
    for ref in (bundle_reference, review_reference):
        fields(ref, {"path", "sha256"})
        require(Path(ref["path"]).is_relative_to("/inputs"), "runtime prerequisites must be original inputs")
    raw = read_bound(bundle_reference["path"], MAX_METADATA_BYTES, private=True)
    require(sha(raw) == bundle_reference["sha256"], "original runtime input bundle changed")
    bundle = strict_json(raw)
    fields(bundle, {"format", "review", "installed_offline", "files"})
    require(bundle["format"] == "recall-runtime-original-inputs/v2", "runtime original-input version differs")
    paths = inventory(bundle["files"], inputs_only=True)
    require(paths.get(review_reference["path"]) == bundle["review"]
            and bundle["review"]["sha256"] == review_reference["sha256"]
            and bundle["review"]["kind"] == "runtime-review"
            and bundle["review"]["bytes"] <= MAX_METADATA_BYTES,
            "original independent runtime review differs")
    require(bundle["installed_offline"] in paths.values()
            and bundle["installed_offline"]["kind"] == "installed-offline-evidence",
            "original installed prerequisite absent")
    review_raw = None
    # Stream one bounded file at a time. No provider-ledger channel is read here.
    for path, ref in paths.items():
        value = original(path, ref)
        if path == review_reference["path"]:
            review_raw = value
    require(review_raw is not None, "original runtime review absent")
    return {"bundle": bundle, "review_raw": review_raw, "review": strict_json(review_raw)}


def write_runtime_custody(custody, originals, receipt, destination):
    """Join actual current observations to immutable separately reviewed inputs."""
    bundle = originals["bundle"]
    original_paths = inventory(bundle["files"], inputs_only=True)
    current_paths = inventory(custody["files"])
    combined = dict(original_paths)
    for path, ref in current_paths.items():
        require(path not in combined or combined[path] == ref, "current capture replaced original input")
        combined[path] = ref
    files = [{"path": path, "ref": ref} for path, ref in sorted(combined.items())]
    inventory(files)
    # Rehash original files immediately before publishing their location carrier.
    for path, ref in combined.items():
        original(path, ref)
    review = originals["review"]
    identity = dict(review["identities"][receipt["backend"]])
    fields(identity, IDENTITY_FIELDS)
    for key, legacy in (("source_commit", "source_commit"), ("binary_sha256", "artifact_sha256"),
                        ("paired_artifact_sha256", "paired_artifact_sha256"),
                        ("runtime_sha256", "runtime_sha256"), ("backend", "backend"),
                        ("endpoint", "endpoint"), ("run_nonce", "run_nonce")):
        require(identity[key] == receipt[legacy], "runtime review identifies another original invocation")
    # v2 artifact is the reviewed Recall archive; original v1 artifact means
    # executable. The legacy paired_artifact field means executable in BOTH.
    identity["outer_configuration_sha256"] = receipt["configuration_sha256"]
    observed = {"format": "recall-dynamic-runtime-evidence/v2", "identity": identity,
                "installed_offline": bundle["installed_offline"],
                **{key: custody[key] for key in RUNTIME_REFERENCES}}
    require(all(ref in combined.values() for ref in [bundle["review"], observed["installed_offline"],
                *(observed[key] for key in RUNTIME_REFERENCES)]), "runtime envelope references uncaptured original bytes")
    wrapper = custody["controller_context"]
    require(wrapper["raw_sha256"] == custody["controller"]["sha256"]
            and type(wrapper["raw_bytes"]) is bytes
            and sha(wrapper["raw_bytes"]) == wrapper["raw_sha256"], "original controller bytes missing")
    packet = {"format": "recall-runtime-custody/v2", "observed_runtime": observed,
              "controller_context": {"ref": custody["controller"], "observed_wall": wrapper["observed_wall"],
                                     "observed_monotonic_ns": wrapper["observed_monotonic_ns"]}, "files": files}
    raw = encode(packet)
    require(len(raw) <= MAX_METADATA_BYTES, "runtime custody metadata bound exceeded")
    private_new(Path(destination), raw)
    return {"path": str(destination), "sha256": sha(raw)}
