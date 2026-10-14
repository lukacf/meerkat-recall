"""Bounded original-evidence loading for L's sole dynamic runtime predicate.

This adapter grants no authority, launches no process and collects no replacement
proof. The reviewed C producer owns every original path in the typed sidecar.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import os
from pathlib import Path
import re
import stat
import sys

from primary_provider_driver_contract import require, sha
from primary_provider_driver_startup import load_producer, read_private

VALIDATOR = "tests/recall_preview_semantics/dynamic_component_validator.py"
INSTALLED_VALIDATOR = "tests/recall_preview_semantics/installed_evidence_validator.py"
MAX_RAW_BYTES = 352 * 1024**2
MAX_FILE_BYTES = 64 * 1024**2
MAX_FILES = 16384
SIDECAR_FORMAT = "recall-runtime-custody/v2"
GATEWAY_ORIGINALS = {"/gateway-manifest.json": "gateway-manifest",
                     "/gateway-launch.json": "gateway-launch-spec"}
MAX_GATEWAY_ORIGINAL_BYTES = 131072


def original_path(value):
    require(type(value) is str and 0 < len(value) <= 1024, "runtime original path required")
    path = Path(value)
    require(str(path) == value and path.is_absolute() and ".." not in path.parts
            and (path.is_relative_to("/work") or path.is_relative_to("/inputs")
                 or path == Path("/launch-anchor.json") or value in GATEWAY_ORIGINALS),
            "runtime original path outside retained custody")
    return path


def read_original(path, maximum):
    path = original_path(path)
    if path.is_relative_to("/work"):
        return read_private(path, maximum)
    gateway_original = str(path) in GATEWAY_ORIGINALS
    if gateway_original:
        require(type(maximum) is int and 0 <= maximum <= MAX_GATEWAY_ORIGINAL_BYTES,
                "fixed gateway original read bound differs")
    require(path.parent.resolve() == path.parent, "runtime input parent is not original")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum,
                "runtime input is not a bounded original regular file")
        if gateway_original:
            require(info.st_uid == os.getuid() and info.st_mode & 0o077 == 0,
                    "fixed gateway original is not private to this owner")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(maximum + 1)
        require(len(raw) <= maximum, "runtime original grew beyond its bound")
        if gateway_original:
            after = os.fstat(descriptor)
            identity = lambda value: (value.st_dev, value.st_ino, value.st_size,
                                      value.st_mtime_ns, value.st_ctime_ns)
            require(identity(info) == identity(after) and len(raw) == info.st_size,
                    "fixed gateway original changed during its read")
        return raw
    finally:
        os.close(descriptor)


def original_evidence(files, reader=read_original):
    require(type(files) is list and 0 < len(files) <= MAX_FILES, "runtime original inventory count differs")
    paths, kinds, sizes = set(), {}, {}
    total = 0
    # Bound the complete advertised population before reading the first file.
    for row in files:
        require(type(row) is dict and set(row) == {"path", "ref"}, "runtime original inventory fields differ")
        path = str(original_path(row["path"]))
        require(path not in paths, "runtime original path duplicated")
        paths.add(path)
        ref = row["ref"]
        require(type(ref) is dict and set(ref) == {"sha256", "bytes", "kind"}, "runtime typed reference differs")
        checksum, size, kind = ref["sha256"], ref["bytes"], ref["kind"]
        require(type(checksum) is str and re.fullmatch(r"[a-f0-9]{64}", checksum)
                and type(size) is int and 0 <= size <= MAX_FILE_BYTES
                and type(kind) is str and 0 < len(kind) <= 128, "runtime typed original bounds differ")
        if path in GATEWAY_ORIGINALS:
            require(kind == GATEWAY_ORIGINALS[path] and size <= MAX_GATEWAY_ORIGINAL_BYTES,
                    "fixed gateway original type or size differs")
        if checksum in sizes:
            require(sizes[checksum] == size and kinds[checksum] == kind,
                    "runtime original digest has conflicting type or size")
        else:
            sizes[checksum], kinds[checksum] = size, kind
            total += size
            require(total <= MAX_RAW_BYTES, "runtime original aggregate exceeds bound")
    return OriginalEvidence(files, reader)


class OriginalEvidence(Mapping):
    """An immutable locator index; selected records are verified on every lookup.

    There is no body cache. At most one selected body and one duplicate-path
    comparison body are retained here, each bounded by MAX_FILE_BYTES. The
    validator must consume this Mapping directly rather than materialize it.
    """
    def __init__(self, files, reader):
        self._reader = reader
        self._locations = {}
        for row in files:
            ref = row["ref"]
            self._locations.setdefault(ref["sha256"], []).append((row["path"], ref["bytes"]))
        self._locations = {checksum: tuple(locations) for checksum, locations in self._locations.items()}

    def __len__(self):
        return len(self._locations)

    def __iter__(self):
        return iter(self._locations)

    def __getitem__(self, checksum):
        accepted = None
        for path, size in self._locations[checksum]:
            raw = self._reader(path, size)
            require(type(raw) is bytes and len(raw) == size and sha(raw) == checksum,
                    "runtime original bytes or digest changed")
            if accepted is None:
                accepted = raw
            else:
                require(accepted == raw, "runtime original digest collision")
        return accepted


@contextmanager
def reviewed_runtime_validator(root, dependencies, loader=load_producer):
    """Bind L's canonical pure import to the exact reviewed installed helper.

    The primary executable runs with Python -I. Do not add the validator tree
    or a caller-controlled working directory to import search. Restore any
    previous module entry even when validation refuses the original evidence.
    """
    require(all(type(dependencies.get(path)) is str
                and re.fullmatch(r"[a-f0-9]{64}", dependencies[path])
                for path in (VALIDATOR, INSTALLED_VALIDATOR)),
            "reviewed runtime predicate dependency absent")
    installed = loader(root, INSTALLED_VALIDATOR, dependencies[INSTALLED_VALIDATOR])
    name = "installed_evidence_validator"
    missing = object()
    previous = sys.modules.get(name, missing)
    sys.modules[name] = installed
    try:
        yield loader(root, VALIDATOR, dependencies[VALIDATOR])
    finally:
        if previous is missing:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def validate_runtime_custody(root, manifest, loaded, review, *, reader=read_original, loader=load_producer):
    require(manifest.get("format") == "recall-primary-provider-run/v2", "primary runtime prerequisite requires explicit v2")
    packet = loaded["runtime_custody"]
    require(type(packet) is dict and set(packet) == {"format", "observed_runtime", "controller_context", "files"}
            and packet["format"] == SIDECAR_FORMAT, "primary original runtime sidecar differs")
    raw_evidence = original_evidence(packet["files"], reader)
    packet_files = {row["path"]: row["ref"] for row in packet["files"]}
    observed = packet["observed_runtime"]
    require(type(observed) is dict and observed.get("format") == "recall-dynamic-runtime-evidence/v2",
            "actual runtime evidence envelope absent")
    for name, key in (("runtime_receipt", "bootstrap"), ("gateway_context", "controller")):
        reference = manifest["observations"][name]
        require(observed[key]["sha256"] == reference["sha256"]
                and any(row["path"] == reference["path"] and row["ref"] == observed[key] for row in packet["files"]),
                "runtime custody changed the original startup observation")
    originals = loaded["runtime_original_inputs"]
    require(type(originals) is dict and set(originals) == {"format", "review", "installed_offline", "files"}
            and originals["format"] == "recall-runtime-original-inputs/v2",
            "runtime immutable original-input bundle differs")
    require(type(originals["files"]) is list and 0 < len(originals["files"]) <= MAX_FILES
            and all(type(row) is dict and set(row) == {"path", "ref"}
                    and original_path(row["path"]).is_relative_to("/inputs")
                    and packet_files.get(row["path"]) == row["ref"] for row in originals["files"]),
            "runtime immutable original references changed")
    require(len({row["path"] for row in originals["files"]}) == len(originals["files"])
            and originals["review"]["sha256"] == manifest["inputs"]["runtime_review"]["sha256"]
            and any(row["path"] == manifest["inputs"]["runtime_review"]["path"]
                    and row["ref"] == originals["review"] for row in originals["files"])
            and originals["installed_offline"] == observed["installed_offline"]
            and any(row["ref"] == originals["installed_offline"] for row in originals["files"]),
            "runtime review or installed original was replaced")
    controller = packet["controller_context"]
    require(type(controller) is dict and set(controller) == {"ref", "observed_wall", "observed_monotonic_ns"}
            and controller["ref"] == observed["controller"], "runtime original controller wrapper differs")
    raw_context = raw_evidence[controller["ref"]["sha256"]]
    runtime_review = loaded["runtime_review_raw"]
    require(type(runtime_review) is bytes and len(runtime_review) <= 8 * 1024**2
            and sha(runtime_review) == manifest["inputs"]["runtime_review"]["sha256"],
            "runtime static review original bytes changed")
    with reviewed_runtime_validator(root, review["dependencies"], loader) as validator:
        component = validator.validate_runtime(
            {"review": loaded["runtime_review"], "raw_sha256": sha(runtime_review), "raw_bytes": runtime_review},
            observed, raw_evidence, {"context": loaded["gateway_context"],
                "raw_sha256": controller["ref"]["sha256"], "raw_bytes": raw_context,
                "observed_wall": controller["observed_wall"],
                "observed_monotonic_ns": controller["observed_monotonic_ns"]},
        )
    require(type(component) is dict and component.get("format") == "recall-installed-runtime-component/v2"
            and component.get("verdict") == "passed" and component.get("scope") == "installed-offline-and-current-runtime"
            and component.get("product_acceptance") is False, "runtime prerequisite did not qualify its limited scope")
    identity = component["identity"]
    for field, manifest_field in (("source_commit", "source_commit"), ("binary_sha256", "artifact_sha256"),
                                  ("paired_artifact_sha256", "paired_artifact_sha256"),
                                  ("runtime_sha256", "runtime_sha256"), ("backend", "backend"),
                                  ("endpoint", "endpoint"), ("run_nonce", "nonce"),
                                  ("outer_configuration_sha256", "configuration_sha256")):
        require(identity[field] == manifest[manifest_field], "runtime component belongs to another original invocation")
    # Preserve all original obligations and proof limits for the final join.
    return component
