#!/usr/bin/env python3
"""Finite controlled full-workload capture; execution requires an original allocation.

The verified artifact owner starts and retains the isolated installed service
inside this command's original lifetime, alongside its owned loopback spy. Real
provider/semantic execution and product or consumer acceptance remain disabled.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
COMPONENTS = ROOT / "tests/recall_preview_semantics"
INPUT_CAP = 32 * 1024**2
# Every executed nonstdlib dependency has one original source snapshot. Literal
# module names also bind F's explicit dependency carrier and C's ordinary import.
MODULES = (
    ("fixtures", "tests/recall_preview/fixtures.py"),
    ("matrix", "tests/recall_preview/matrix.py"),
    ("oracle", "tests/recall_preview/oracle.py"),
    ("installed_d_capture", "scripts/qualification/recall_preview.py"),
    ("installed_release_artifact", "scripts/release/artifact.py"),
    ("installed_original_process_capture", "scripts/qualification/installed_process_capture.py"),
    ("full_workload_installed_helpers", "scripts/qualification/installed_linux_offline.py"),
    ("snapshot_inspection_capture", "scripts/qualification/snapshot_inspection_capture.py"),
    ("runtime_package_bootstrap_contract", "scripts/qualification/runtime_package_bootstrap_contract.py"),
    ("full_workload_runtime_inspection", "scripts/qualification/full_workload_runtime_inspection.py"),
    ("full_workload_transport", "tests/recall_preview_semantics/full_workload_transport.py"),
    ("full_workload_spy", "tests/recall_preview_semantics/full_workload_spy.py"),
    ("full_workload_graph", "tests/recall_preview_semantics/full_workload_graph.py"),
    ("full_workload_load", "tests/recall_preview_semantics/full_workload_load.py"),
    ("full_workload_ob3_oracle", "scripts/qualification/ob3_outage_oracle.py"),
    ("full_workload_owned_runtime", "scripts/qualification/full_workload_owned_runtime.py"),
    ("full_workload_driver", "tests/recall_preview_semantics/full_workload_driver.py"),
    ("full_workload_verified_cli", "scripts/qualification/full_workload.py"),
)
INSTALLED_DEPENDENCIES = ("fixtures", "matrix", "oracle", "installed_d_capture",
                        "installed_release_artifact", "installed_original_process_capture")
FILES = [ROOT / MODULES[-1][1], *(ROOT / path for _, path in MODULES[:-1])]
SOURCE_LIMIT = 1024**2
SOURCE_TOTAL = 8 * 1024**2
_SOURCE_HASHES = None
_OB3_ORACLE = None
_RUNTIME_OWNER = _RUNTIME_INSPECTOR = None
_INSTALLED_HELPERS = _PROCESS_CAPTURE = _ARTIFACT_HELPERS = _INSPECTION_HELPERS = None


# Only these stdlib primitives run before every source pin has matched.
# The verified functional module then binds the original transport primitives.
def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate original launch key")
            result[key] = value
        return result
    result = json.loads(raw, object_pairs_hook=unique)
    encode(result)  # Reject non-finite numbers before loading candidate code.
    return result


def snapshot_sources(expected=None):
    """Single-open bounded originals; verify the complete set before execution."""
    originals, total = {}, 0
    for path in FILES:
        require(path.is_absolute() and path == path.resolve(), "canonical original source path required")
        name = str(path.relative_to(ROOT))
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            before = os.fstat(descriptor)
            require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= SOURCE_LIMIT
                    and total + before.st_size <= SOURCE_TOTAL, "original source byte bound exceeded")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read(SOURCE_LIMIT + 1)
            after, visible = os.fstat(descriptor), path.stat(follow_symlinks=False)
            keys = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode")
            require(len(raw) == before.st_size and all(getattr(before, key) == getattr(after, key) == getattr(visible, key) for key in keys),
                    "original code changed while reading")
        finally:
            os.close(descriptor)
        originals[name] = raw
        total += len(raw)
    hashes = {name: sha(raw) for name, raw in originals.items()}
    if expected is not None:
        require(type(expected) is dict and expected == hashes, "original executed source pins differ")
    return originals, hashes


@contextmanager
def reviewed_code(originals, hashes):
    """Use exact verified snapshots, excluding cached modules and bytecode."""
    require(set(originals) == set(hashes) == {str(path.relative_to(ROOT)) for path in FILES}
            and all(type(raw) is bytes and sha(raw) == hashes[name] for name, raw in originals.items()),
            "original code snapshot differs")
    order = MODULES
    missing, modules = object(), {}
    previous = {name: sys.modules.get(name, missing) for name, _ in order}
    previous_path = list(sys.path)
    try:
        for name, path in order:
            spec = importlib.util.spec_from_file_location(name, ROOT / path)
            require(spec is not None, "original module metadata unavailable")
            modules[name] = importlib.util.module_from_spec(spec)
            sys.modules[name] = modules[name]
        for name, path in order:
            if name == "full_workload_installed_helpers":
                modules[name].VERIFIED_DEPENDENCIES = {
                    dependency: {"module": modules[dependency], "path": str(ROOT / relative),
                                 "source_bytes": originals[relative], "sha256": hashes[relative]}
                    for dependency, relative in order if dependency in INSTALLED_DEPENDENCIES
                }
            exec(compile(originals[path], str(ROOT / path), "exec", dont_inherit=True), modules[name].__dict__)
        cli, driver = modules["full_workload_verified_cli"], modules["full_workload_driver"]
        for name in ("CONTRACT_SHA", "CORPUS_SHA", "CASES", "SPACES", "Driver", "NamedClient", "validate_inputs"):
            setattr(cli, name, getattr(driver, name))
        cli.ControlledSpy = modules["full_workload_spy"].ControlledSpy
        for name in ("Capture", "decode", "encode", "endpoint", "require", "sha"):
            setattr(cli, name, getattr(modules["full_workload_transport"], name))
        cli._SOURCE_HASHES = dict(hashes)
        cli._OB3_ORACLE = modules["full_workload_ob3_oracle"].evaluate_receipt_bytes
        require(callable(cli._OB3_ORACLE), "independent original OB3 oracle callable absent")
        cli._RUNTIME_OWNER = modules["full_workload_owned_runtime"].FullWorkloadOwnedRuntime
        cli._RUNTIME_INSPECTOR = modules["full_workload_runtime_inspection"].RuntimeRecipeInspector
        cli._INSTALLED_HELPERS = modules["full_workload_installed_helpers"]
        cli._PROCESS_CAPTURE = modules["installed_original_process_capture"]
        cli._ARTIFACT_HELPERS = modules["installed_release_artifact"]
        cli._INSPECTION_HELPERS = modules["snapshot_inspection_capture"]
        require(cli._INSTALLED_HELPERS.process_capture is cli._PROCESS_CAPTURE
                and cli._INSTALLED_HELPERS.artifact is cli._ARTIFACT_HELPERS,
                "original installed dependency identity differs")
        require(all(cli._INSTALLED_HELPERS.LOADED_SOURCE_HASHES.get(modules[name]) == hashes[path]
                    for name, path in order if name in INSTALLED_DEPENDENCIES),
                "original installed dependency source differs")
        yield cli
    finally:
        sys.path[:] = previous_path
        for name, value in previous.items():
            if value is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value


def source_hashes():
    require(type(_SOURCE_HASHES) is dict, "original code snapshot has not been loaded")
    return dict(_SOURCE_HASHES)


def inventory():
    return {"format": "recall-full-workload-driver-inventory/v1", "source_sha256": source_hashes(),
            "contract_sha256": CONTRACT_SHA, "corpus_sha256": CORPUS_SHA, "cases": list(CASES),
            "execution_authorized": False, "external_provider_dispatch_authorized": False,
            "product_acceptance": False, "semantic_load": "unmet: separate inactive allocation"}


def timestamp(value):
    require(type(value) is str and value.endswith("Z"), "explicit original UTC allocation time required")
    return dt.datetime.fromisoformat(value[:-1] + "+00:00").timestamp()


def timestamp_ns(value):
    require(type(value) is str and value.endswith("Z"), "explicit original UTC allocation time required")
    moment = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    elapsed = moment - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return ((elapsed.days * 86400 + elapsed.seconds) * 10**6 + elapsed.microseconds) * 1000


def same_input_subset(actual, expected):
    """Compare pre-output documents only; no runtime value supplies expectations."""
    if type(expected) is dict:
        return type(actual) is dict and all(key in actual and same_input_subset(actual[key], value)
                                           for key, value in expected.items())
    return encode(actual) == encode(expected)


def bind_runtime_inputs(runtime, plan, manifest, allocation):
    require(type(runtime) is dict and runtime.get("format") == "recall-full-workload-runtime-plan/v1",
            "original pre-start runtime plan required")
    for name in ("backend", "run_nonce", "endpoint"):
        require(runtime.get(name) == manifest[name] == allocation[name], "original runtime allocation differs: " + name)
    for name in ("source_commit", "artifact_sha256", "binary_sha256"):
        require(runtime.get(name) == allocation[name], "original runtime artifact differs: " + name)
    require(runtime.get("spy_endpoint") == f'http://127.0.0.1:{manifest["spy_port"]}/v1'
            and Path(runtime["run_directory"]).parent == Path(manifest["output"]),
            "original runtime endpoint or capture directory differs")
    require(encode(runtime.get("worker_runtime_expectations")) == encode(plan.get("worker_runtime_expectations"))
            and type(plan.get("worker_runtime_expectations")) is dict, "original worker expectation input differs")
    require(type(runtime.get("spaces")) is list and [row.get("name") for row in runtime["spaces"]] == list(SPACES)
            and type(runtime.get("principals")) is dict and set(runtime["principals"]) == set(SPACES),
            "both original runtime spaces and principals required")
    templates = plan.get("runtime_principal_templates")
    require(type(templates) is dict and set(templates) == set(SPACES),
            "both original approved principal template maps required")
    for space, request in zip(SPACES, runtime["spaces"]):
        expected = plan["configurations"][space]
        configuration = {key: value for key, value in expected["configuration"].items()
                         if key not in {"space_id", "subjects", "subjects_state"}}
        require(same_input_subset(request, configuration), "runtime request differs from original expected configuration")
        principals = runtime["principals"][space]
        require(set(principals) == {"data", "manage", "revocable"}
                and principals["data"] == {"subject": expected["subject"], "scopes": expected["grant"]["scopes"]},
                "original data grant principal differs")
        require(type(templates[space]) is dict and set(templates[space]) == {"manage", "revocable"}
                and all(encode(principals[role]) == encode(templates[space][role])
                        for role in ("manage", "revocable")), "original management or revocable principal differs")
        cases = [case for case in plan["cases"]["FW10"]["spaces"] if case["space"] == space]
        require(len(cases) == 1 and cases[0]["role"] == "revocable"
                and principals["revocable"]["subject"] == cases[0]["subject"], "original FW10 principal differs")
    credential = runtime.get("spy_credential")
    rules = plan["spy_recipe"]["rules"]
    if "spy_credentials" in runtime:
        require(_RUNTIME_OWNER is not None, "original reviewed runtime credential validator required")
        authorities = _RUNTIME_OWNER.spy_authorities(runtime)
        require(rules and all(rule.get("space") in authorities and
                rule.get("credential_sha256") == sha(authorities[rule["space"]]["value"].encode()) for rule in rules),
                "per-space controlled spy credentials differ from original rules")
        for request in runtime["spaces"]:
            connections = request["processing"]["connections"]
            require(connections and all(connection["credential_ref"] == authorities[request["name"]]["credential_ref"]
                    and connection["endpoint"] == runtime["spy_endpoint"] for connection in connections.values()),
                    "per-space owned configuration differs from original spy credentials")
    else:
        require(type(credential) is str and credential and rules
                and all(rule.get("credential_sha256") == sha(credential.encode()) for rule in rules),
                "original controlled spy credential differs")
    embedding_keys = [(rule["credential_sha256"], rule["model"], rule["path"])
                      for rule in rules if rule.get("path") == "/v1/embeddings"]
    require(len(embedding_keys) == len(set(embedding_keys)), "ambiguous controlled embedding rule authority")
    authorities = {}
    for rule in rules:
        if rule.get("path") in ("/v1/embeddings", "/v1/responses", "/v1/messages"):
            key = (rule["credential_sha256"], rule["model"], rule["path"])
            authorities.setdefault(key, set()).add(rule["space"])
    require(all(len(spaces) == 1 for spaces in authorities.values()), "ambiguous controlled provider space authority")
    return runtime


def regular_private(path):
    path = Path(path)
    require(path.is_absolute() and path == path.resolve(), "canonical original private input required")
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_mode & 0o077 == 0 and info.st_uid == os.getuid(), "task-owned 0600 credential file required")


class Inputs:
    def __init__(self):
        self.bytes = 0
        self.hashes = {}

    def read(self, descriptor, maximum=8 * 1024**2):
        value, _ = self._read(descriptor, maximum, private=False)
        return value

    def read_with_raw(self, descriptor, maximum=4 * 1024**2):
        """Retain the same original private bytes that were hashed and parsed."""
        return self._read(descriptor, maximum, private=True)

    def _read(self, descriptor, maximum, *, private):
        require(type(descriptor) is dict and set(descriptor) == {"path", "sha256"}, "exact original path/hash descriptor required")
        require(type(maximum) is int and 0 < maximum <= 8 * 1024**2
                and type(descriptor["path"]) is str and type(descriptor["sha256"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", descriptor["sha256"]), "bounded exact original input identity required")
        path = Path(descriptor["path"])
        require(path.is_absolute() and path == path.resolve(), "original canonical input path required")
        descriptor_fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            before = os.fstat(descriptor_fd)
            require(stat.S_ISREG(before.st_mode) and before.st_size <= maximum,
                    "bounded regular input required")
            require(not private or (before.st_mode & 0o077 == 0 and before.st_uid == os.getuid()),
                    "task-owned private original consumer input required")
            require(self.bytes + before.st_size <= INPUT_CAP, "original aggregate source input bound exceeded")
            with os.fdopen(descriptor_fd, "rb", closefd=False) as stream:
                raw = stream.read(maximum + 1)
            after = os.fstat(descriptor_fd)
            visible = path.stat(follow_symlinks=False)
            keys = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode")
            require(all(getattr(before, key) == getattr(after, key) == getattr(visible, key) for key in keys)
                    and len(raw) == before.st_size <= maximum, "original input changed while reading")
        finally:
            os.close(descriptor_fd)
        require(sha(raw) == descriptor["sha256"], "original input hash differs")
        self.bytes += len(raw)
        value = decode(raw)
        require(str(path) not in self.hashes or self.hashes[str(path)] == descriptor["sha256"],
                "original input path was replaced")
        self.hashes[str(path)] = descriptor["sha256"]
        return value, raw


def external_inputs(inputs, descriptors, allocated, backend, nonce):
    """Load independent original receipts and optional pre-output expectations.

    The on-disk descriptor contains only paths and hashes. Neither an expected
    verdict nor an expectation document is derived from a consumer observation.
    Old descriptors remain readable; the independent oracle reports their
    missing expectation prerequisite instead of the loader manufacturing one.
    """
    require(type(descriptors) is dict and type(allocated) is dict, "original external descriptor map required")
    external = {}
    for case, descriptor in descriptors.items():
        require(case in ("FW13", "FW14") and allocated.get(case) == descriptor,
                "unreviewed external consumer receipt")
        require(type(descriptor) is dict and set(descriptor) in ({"owner", "original"}, {"owner", "original", "expectations"})
                and descriptor["owner"] in ("OB3", "HomeCore"), "original external consumer custody descriptor required")
        require("expectations" not in descriptor or descriptor["owner"] == "OB3",
                "independent expectation descriptor is defined only for OB3")
        original, raw = inputs.read_with_raw(descriptor["original"])
        carrier = {"owner": descriptor["owner"], "case": case, "backend": backend,
                   "run_nonce": nonce, "original_sha256": descriptor["original"]["sha256"],
                   "original": original, "original_raw": raw}
        if "expectations" in descriptor:
            _, expected_raw = inputs.read_with_raw(descriptor["expectations"])
            carrier["expectations_raw"] = expected_raw
            carrier["expectations_sha256"] = descriptor["expectations"]["sha256"]
        external[case] = carrier
    return external


def reviewed_inputs(manifest_path, manifest_sha256, original_launch, original_clock):
    inputs = Inputs()
    require(type(original_launch) is tuple and len(original_launch) == 2
            and all(type(raw) is bytes and len(raw) <= 65536 for raw in original_launch),
            "bounded original launch bytes required")
    manifest_raw, allocation_raw = original_launch
    require(sha(manifest_raw) == manifest_sha256, "original launch manifest differs")
    manifest = decode(manifest_raw)
    require(manifest.get("format") == "recall-full-workload-run/v1", "original workload manifest required")
    require(sha(allocation_raw) == manifest["allocation"]["sha256"], "original launch allocation differs")
    allocation = decode(allocation_raw)
    inputs.bytes = len(manifest_raw) + len(allocation_raw)
    inputs.hashes = {str(manifest_path): manifest_sha256, manifest["allocation"]["path"]: manifest["allocation"]["sha256"]}
    require(allocation.get("format") == "recall-full-workload-allocation/v1"
            and allocation.get("execution_authorized") is True
            and allocation.get("scope") == "controlled-owned-spy-only"
            and allocation.get("external_provider_sends") == 0 and allocation.get("paid_model_calls") == 0,
            "original controlled-only execution allocation absent")
    require(allocation.get("driver_source_sha256") == source_hashes()
            and allocation.get("contract_sha256") == CONTRACT_SHA
            and allocation.get("backend") == manifest.get("backend")
            and allocation.get("run_nonce") == manifest.get("run_nonce")
            and allocation.get("endpoint") == manifest.get("endpoint")
            and allocation.get("spy_port") == manifest.get("spy_port"), "original allocation identity differs")
    require(type(allocation.get("source_commit")) is str
            and re.fullmatch(r"[a-f0-9]{40}", allocation["source_commit"]) is not None
            and all(type(allocation.get(key)) is str and re.fullmatch(r"[a-f0-9]{64}", allocation[key]) is not None
                    for key in ("binary_sha256", "artifact_sha256")), "exact original candidate artifact/source hashes required")
    require(allocation.get("case_plan") == manifest["case_plan"]
            and allocation.get("isolation_receipt") == manifest["isolation_receipt"]
            and allocation.get("runtime_plan") == manifest["runtime_plan"]
            and "startup_receipt" not in manifest and "tokens" not in manifest
            and "startup_receipt" not in allocation and "tokens" not in allocation
            and allocation.get("output") == manifest["output"], "original allocation input/output custody differs")
    started_ns, expires_ns = timestamp_ns(allocation["started_at"]), timestamp_ns(allocation["expires_at"])
    whole_start_ns, whole_end_ns = timestamp_ns(allocation["whole_started_at"]), timestamp_ns(allocation["whole_expires_at"])
    require(whole_start_ns <= started_ns < expires_ns <= whole_end_ns
            and 0 < whole_end_ns - whole_start_ns <= 3600 * 10**9, "original two-backend total clock differs")
    require(started_ns <= original_clock["wall_ns"] <= time.time_ns() < expires_ns
            and 0 < expires_ns - started_ns <= 1800 * 10**9, "original backend allocation expired or exceeds 1800 seconds")
    allocation_clock, deadline_ns = _PROCESS_CAPTURE.pin_allocation_clock(
        1800, expires_ns, original_sample=original_clock)
    whole_deadline_ns = original_clock["mono_before_ns"] + min(3600 * 10**9, whole_end_ns - original_clock["wall_ns"])
    deadline = _PROCESS_CAPTURE.deadline_seconds(deadline_ns)
    expires = _PROCESS_CAPTURE.deadline_seconds(expires_ns)
    contract = inputs.read({"path": str(ROOT / "tests/recall_preview/full-workload-contract.json"), "sha256": CONTRACT_SHA})
    corpus = inputs.read({"path": str(ROOT / "tests/recall_preview/workload-corpus.json"), "sha256": CORPUS_SHA})
    require(allocation.get("limits") == contract["controlled_fixture_limits"], "original full-workload caps changed")
    endpoint(manifest["endpoint"])
    require(type(manifest["spy_port"]) is int and 0 < manifest["spy_port"] <= 65535
            and endpoint(manifest["endpoint"])[1] != manifest["spy_port"], "explicit distinct owned spy port required")
    plan = inputs.read(manifest["case_plan"])
    # Large literals may reside in separate independently pinned files. Their
    # canonical resolved plan hash is also approved before any product output.
    for field in ("graph_recipes",):
        plan[field] = {key: inputs.read(value["file"]) if type(value) is dict and set(value) == {"file"} else value
                       for key, value in plan[field].items()}
    for field in ("spy_recipe", "load_plan"):
        if type(plan[field]) is dict and set(plan[field]) == {"file"}:
            plan[field] = inputs.read(plan[field]["file"])
    plan_sha256 = allocation["resolved_plan_sha256"]
    validate_inputs(contract, corpus, plan, plan_sha256)
    runtime_plan = bind_runtime_inputs(inputs.read(manifest["runtime_plan"]), plan, manifest, allocation)
    isolation = inputs.read(manifest["isolation_receipt"])
    require(sys.platform == "linux" and isolation.get("format") == "recall-owned-network-isolation/v1"
            and isolation.get("run_nonce") == manifest["run_nonce"]
            and isolation.get("backend") == manifest["backend"]
            and isolation.get("verdict") == "passed" and isolation.get("outbound_denial_observed") is True
            and isolation.get("external_provider_sends") == 0
            and isolation.get("current_network_namespace") == os.readlink("/proc/self/ns/net")
            and isolation.get("current_network_namespace") != isolation.get("original_host_network_namespace"),
            "actual original owned isolated network namespace proof absent")
    require(type(isolation.get("original_probe_capture_sha256")) is str
            and re.fullmatch(r"[a-f0-9]{64}", isolation["original_probe_capture_sha256"]) is not None,
            "original isolation observation hash absent")
    previous = allocation.get("previous_backend_receipt")
    if manifest["backend"] == "surreal-db":
        require(type(previous) is dict, "original first-backend terminal receipt required before second backend")
        receipt = inputs.read(previous)
        require(receipt.get("format") == "recall-full-workload-cli-receipt/v1"
                and receipt.get("backend") == "elephant-db" and receipt.get("run_nonce") == manifest["run_nonce"]
                and receipt.get("owned_spy_closed") is True and receipt.get("owned_runtime_closed") is True
                and receipt.get("runtime_cleanup") is not None and receipt.get("error_type") is None
                and receipt.get("finished_at", float("inf")) <= started_ns / 10**9,
                "second backend would overlap or replace original first-backend capture")
    else:
        require(manifest["backend"] == "elephant-db" and previous is None, "original sequential backend order differs")
    # The actual owner issues original signed principals after startup. No
    # external token file or future startup receipt can substitute for custody.
    external = external_inputs(inputs, manifest.get("external_consumer_receipts", {}),
                               allocation.get("external_consumer_receipts", {}), manifest["backend"], manifest["run_nonce"])
    require(deadline > time.monotonic(), "original allocation expired during source validation")
    return {"manifest": manifest, "allocation": allocation, "contract": contract, "corpus": corpus,
            "plan": plan, "plan_hash": plan_sha256, "runtime_plan": runtime_plan, "isolation": isolation,
            "external": external, "deadline": deadline, "expires": expires, "deadline_ns": deadline_ns,
            "whole_deadline_ns": whole_deadline_ns, "allocation_clock": allocation_clock, "input_hashes": inputs.hashes}


def retain_cli(capture, name, value):
    """Retain lifecycle failures even when initial inspection prevented Driver."""
    raw = encode(value)
    require(len(raw) <= 8 * 1024**2, "original CLI metadata bound exceeded")
    capture.reserve(len(raw))
    path = capture.directory / name
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    capture.held -= len(raw)
    capture.written += len(raw)
    return {"path": str(path), "sha256": sha(raw), "bytes": len(raw)}


async def execute(loaded):
    manifest, allocation, contract, corpus, plan, plan_hash = (
        loaded[name] for name in ("manifest", "allocation", "contract", "corpus", "plan", "plan_hash"))
    limits = contract["controlled_fixture_limits"]
    capture = Capture(manifest["output"], limits["capture_bytes_per_backend"])
    client = NamedClient(manifest["endpoint"], {}, capture, limits, loaded["deadline"], loaded["expires"])
    spy = ControlledSpy(plan["spy_recipe"], plan["spy_recipe_sha256"], capture, limits, loaded["deadline"])
    observed, failure, owner = None, None, None
    try:
        retain_cli(capture, "cli-allocation.json", {"allocation": allocation, "input_hashes": loaded["input_hashes"],
                   "driver_sources": source_hashes(), "original_clock": loaded["allocation_clock"]})
        owner = _RUNTIME_OWNER(allocation=allocation, runtime_plan=loaded["runtime_plan"], client=client,
            capture=capture, deadline_ns=loaded["deadline_ns"], whole_deadline_ns=loaded["whole_deadline_ns"],
            allocation_clock=loaded["allocation_clock"], isolation=loaded["isolation"],
            installed_helpers=_INSTALLED_HELPERS, process_capture=_PROCESS_CAPTURE,
            artifact_helpers=_ARTIFACT_HELPERS, inspection_helpers=_INSPECTION_HELPERS)
        # B retains 75 cleanup seconds INSIDE the original deadline. Its own
        # __aenter__ bounds startup at the same active end. Keep __aexit__
        # OUTSIDE the active timer so normal cleanup may cross that earlier end.
        require(owner.active_deadline_ns == min(loaded["deadline_ns"], loaded["whole_deadline_ns"]) - 75 * 10**9,
                "original owned cleanup reservation differs")
        async with asyncio.timeout_at(_PROCESS_CAPTURE.deadline_seconds(owner.active_deadline_ns)):
            await spy.start(manifest["spy_port"])
        async with owner:
            async with asyncio.timeout_at(_PROCESS_CAPTURE.deadline_seconds(owner.active_deadline_ns)):
                inspector = _RUNTIME_INSPECTOR(client, {**plan, "backend": manifest["backend"],
                                                       "run_nonce": manifest["run_nonce"]}, owner)
                require(await inspector.capture("initial") is None, "original initial inspection must retain its own captures")
                driver = Driver(client, spy, contract, corpus, plan, plan_hash, backend=manifest["backend"],
                    nonce=manifest["run_nonce"], external_receipts=loaded["external"], ob3_oracle=_OB3_ORACLE,
                    service_control=owner, capacity_capture=owner, runtime_inspection=inspector.capture)
                observed = await driver.run()
                failure = observed.get("error_type")
        require(owner.state == "stopped" and owner.failures == [] and owner.cleanup_receipt is not None,
                "original owned runtime cleanup is incomplete")
    except (Exception, asyncio.CancelledError) as error:
        failure = type(error).__name__
    finally:
        try:
            await spy.close()
        except (Exception, asyncio.CancelledError) as error:
            failure = failure or type(error).__name__
    receipt = {"format": "recall-full-workload-cli-receipt/v1", "backend": manifest["backend"],
               "run_nonce": manifest["run_nonce"], "source_commit": allocation["source_commit"],
               "artifact_sha256": allocation["artifact_sha256"], "binary_sha256": allocation["binary_sha256"],
               "original_allocation_sha256": manifest["allocation"]["sha256"], "case_plan_sha256": plan_hash,
               "observation": observed, "finished_at": time.time(), "error_type": failure,
               "owned_spy_closed": not spy.tasks and (spy.server is None or not spy.server.is_serving()),
               "owned_runtime_closed": owner is not None and owner.state == "stopped" and owner.failures == []
                                       and owner.cleanup_receipt is not None,
               "runtime_startup": owner.startup_receipt if owner is not None else None,
               "runtime_cleanup": owner.cleanup_receipt if owner is not None else None,
               "qualified": False, "product_acceptance": False}
    retain_cli(capture, "cli-terminal-receipt.json", receipt)
    # Missing independent/consumer/runtime evidence always leaves qualification
    # unmet, even when helper protocol captures are internally coherent.
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inventory")
    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--manifest-sha256", required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    args = parser.parse_args()
    if args.command == "inventory":
        originals, hashes = snapshot_sources()
        with reviewed_code(originals, hashes) as cli:
            print(cli.encode(cli.inventory()).decode())
        return 0
    require(sys.version_info >= (3, 11), "Python 3.11+ original absolute async deadlines required")
    original_clock = {"mono_before_ns": time.monotonic_ns(), "wall_ns": time.time_ns(),
                      "mono_after_ns": time.monotonic_ns()}
    # Read and bind both original allocation descriptors before executing any
    # product helper. The verified transport parser receives these SAME bytes.
    bootstrap_inputs = Inputs()
    manifest, manifest_raw = bootstrap_inputs._read({"path": str(args.manifest), "sha256": args.manifest_sha256}, 65536, private=False)
    allocation, allocation_raw = bootstrap_inputs._read(manifest["allocation"], 65536, private=False)
    require(allocation.get("execution_authorized") is True
            and allocation.get("scope") == "controlled-owned-spy-only"
            and allocation.get("external_provider_sends") == 0 and allocation.get("paid_model_calls") == 0,
            "original controlled-only execution allocation absent")
    originals, hashes = snapshot_sources(allocation.get("driver_source_sha256", {}))
    with reviewed_code(originals, hashes) as cli:
        receipt = asyncio.run(cli.execute(cli.reviewed_inputs(
            args.manifest, args.manifest_sha256, (manifest_raw, allocation_raw), original_clock)))
    print(encode({key: receipt[key] for key in ("format", "backend", "error_type", "qualified", "product_acceptance")}).decode())
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(encode({"status": "unmet-prerequisite", "error_type": type(error).__name__, "qualified": False}).decode(), file=sys.stderr)
        sys.exit(78)
