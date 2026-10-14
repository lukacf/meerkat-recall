"""Original live-process and capture custody for the D runtime predicate.

I/O belongs here, in C's reviewed actual G-child producer. The downstream pure
validator receives only original bounded bytes and typed hash references.
"""
from __future__ import annotations

from collections.abc import Mapping
import os
from pathlib import Path
import time

from runtime_package_bootstrap_contract import LIMITS, encode, fields, require, sha, strict_json
from runtime_package_bootstrap_io import file_hash, private_new, read_bound

# Sum of existing independent channels, not a new capture or process allowance.
MAX_BYTES = LIMITS["capture_bytes"] + 2 * LIMITS["processes"] * LIMITS["log_bytes_per_stream"] + 4 * 1024**2
MAX_SNAPSHOTS = 16
OUTER_LAUNCH_FILES = (("manifest", "/gateway-manifest.json", "gateway-manifest", "manifest_sha256"),
                      ("spec", "/gateway-launch.json", "gateway-launch-spec", "configuration_sha256"))


class OriginalEvidence(Mapping):
    """Index original private files; load and verify only the requested digest.

    No body cache survives a lookup. Membership and iteration read metadata only.
    Repeated paths for a digest must still have identical original bytes.
    """
    def __init__(self):
        self._locations = {}

    def add(self, path, reference):
        self._locations.setdefault(reference["sha256"], {})[path] = dict(reference)

    def __len__(self):
        return len(self._locations)

    def __iter__(self):
        return iter(self._locations)

    def __contains__(self, checksum):
        return checksum in self._locations

    def __getitem__(self, checksum):
        locations = self._locations[checksum]
        original = None
        for path, reference in locations.items():
            raw = read_bound(path, reference["bytes"], private=True)
            require(len(raw) == reference["bytes"] and sha(raw) == checksum,
                    "original custody bytes changed after capture")
            require(original is None or original == raw, "original custody digest collision")
            original = raw
        return original


def stat_identity(raw, pid):
    """Interpret identity only; retain the entire original Linux stat separately."""
    text = raw.decode("ascii")
    prefix, separator, tail = text.rpartition(") ")
    require(separator and prefix.split(" ", 1)[0] == str(pid), "original process stat identity differs")
    columns = tail.split()
    require(len(columns) >= 20 and columns[0] not in ("Z", "X", "x"), "original process is not live")
    value = {"pid": pid, "ppid": int(columns[1]), "pgrp": int(columns[2]),
             "session": int(columns[3]), "start_ticks": int(columns[19])}
    require(all(type(number) is int and number > 0 for number in value.values()), "original process identity invalid")
    return value


class Capture:
    def __init__(self, root, sequence):
        require(type(sequence) is int and 1 <= sequence <= MAX_SNAPSHOTS, "live custody snapshot bound exceeded")
        self.root, self.sequence = Path(root), sequence
        self.files, self.raw_evidence, self.total = {}, OriginalEvidence(), 0

    def retain(self, path, raw, kind, expected=None):
        require(type(raw) is bytes and type(kind) is str and kind, "original typed bytes required")
        checksum = sha(raw)
        require(expected is None or checksum == expected, "original custody capture hash differs")
        path = str(path)
        reference = {"sha256": checksum, "bytes": len(raw), "kind": kind}
        if path in self.files:
            require(self.files[path] == reference, "original custody path was reinterpreted")
            return reference
        require(self.total + len(raw) <= MAX_BYTES, "original custody aggregate exceeds existing channel bounds")
        if checksum in self.raw_evidence:
            require(self.raw_evidence[checksum] == raw, "original custody digest collision")
        self.files[path] = reference
        self.raw_evidence.add(path, reference)
        self.total += len(raw)
        return reference

    def original(self, reference, kind, maximum):
        fields(reference, {"path", "sha256"})
        raw = read_bound(reference["path"], maximum, private=True)
        return self.retain(reference["path"], raw, kind, reference["sha256"])

    def write(self, label, raw, kind):
        require(label and all(char.isalnum() or char in "-_" for char in label), "fixed custody label required")
        require(len(raw) <= min(4 * 1024**2, MAX_BYTES - self.total), "bounded custody metadata required")
        path = self.root / f"custody-{self.sequence:04}-{label}.json"
        private_new(path, raw)
        return self.retain(path, raw, kind), {"path": str(path), "sha256": sha(raw)}

    def process(self, role, expected):
        pid = expected["pid"]
        raw = read_bound(Path(f"/proc/{pid}/stat"), 8192)
        identity = stat_identity(raw, pid)
        original = {"pid": pid, "ppid": expected["ppid"],
            "pgrp": expected.get("pgrp", expected.get("process_group")),
            "session": expected["session"], "start_ticks": expected["start_ticks"]}
        require(identity == original, "original process identity changed before capture")
        executable = os.readlink(f"/proc/{pid}/exe")
        executable_hash = file_hash(Path(executable))
        if "executable" in expected:
            require(executable == expected["executable"] and executable_hash == expected["executable_sha256"],
                    "original owned service executable changed")
        after_raw = read_bound(Path(f"/proc/{pid}/stat"), 8192)
        after = stat_identity(after_raw, pid)
        require(after == identity and os.readlink(f"/proc/{pid}/exe") == executable,
                "original process changed across executable observation")
        reference, _ = self.write(role + "-stat", raw, "process-stat")
        after_reference, _ = self.write(role + "-stat-after", after_raw, "process-stat")
        return {"identity": dict(expected), "stat": reference, "stat_after": after_reference, "executable": executable,
                "executable_sha256": executable_hash}


def capture_live_startup(instance, receipt_reference, context_reference, gateway, expected_launch, *, sequence=1):
    """Capture exact L/D carriers without qualification or installed-case claims."""
    instance.allocation.remaining()
    context = gateway.read_context(context_reference["path"], context_reference["sha256"],
                                   expected=expected_launch, immediate_parent=True)
    capture = Capture(instance.root, sequence)
    bootstrap_ref = capture.original(receipt_reference, "runtime-bootstrap", 4 * 1024**2)
    receipt = strict_json(capture.raw_evidence[bootstrap_ref["sha256"]])
    context_ref = capture.original(context_reference, "gateway-context", 131072)
    require(strict_json(capture.raw_evidence[context_ref["sha256"]]) == context, "original G context bytes changed")
    anchor_ref = capture.original({"path": "/launch-anchor.json", "sha256": context["anchor_sha256"]},
                                  "gateway-anchor", 131072)
    require(strict_json(capture.raw_evidence[anchor_ref["sha256"]]) == context["anchor"], "original G anchor changed")
    for name in ("dev", "ipv4", "ipv6"):
        capture.original(context["captures"][name], "gateway-network-" + name, 131072)
    inventory_raw = encode(gateway.input_inventory(Path("/inputs")))
    require(sha(inventory_raw) == context["anchor"]["input_inventory_sha256"], "original read-only input inventory changed")
    inventory_ref, _ = capture.write("input-inventory", inventory_raw, "input-inventory")
    # These are the original G-saved readonly files, not normalized objects
    # rebuilt from the anchor or the downstream preparation caller manifest.
    outer_inputs = {"format": "recall-gateway-launch-inputs/v2", "input_inventory": inventory_ref}
    for name, path, kind, digest_field in OUTER_LAUNCH_FILES:
        outer_inputs[name] = capture.original({"path": path, "sha256": context["anchor"][digest_field]},
                                              kind, 131072)
    outer_raw = encode(outer_inputs)
    require(len(outer_raw) <= 16384, "original outer launch wrapper exceeds bound")
    outer_ref, outer_path = capture.write("outer-launch", outer_raw, "gateway-launch-inputs")
    runtime_ref = capture.original({"path": str(instance.root / "service/.elephant/elephant_config.toml"),
        "sha256": receipt["runtime_config_sha256"]}, "runtime-configuration-toml", 1048576)
    spaces_ref, _ = capture.write("space-configurations", encode(receipt["configurations"]), "space-configurations")
    capture.original({"path": "/work/isolation.json", "sha256": receipt["isolation_sha256"]},
                     "startup-isolation", 131072)
    capture.original({"path": str(instance.root / "plan.json"), "sha256": receipt["plan_sha256"]},
                     "bootstrap-plan", 8 * 1024**2)
    journal_ref = capture.original(receipt["process_commands"], "startup-command-journal", 262144)
    journal = strict_json(capture.raw_evidence[journal_ref["sha256"]])
    require(journal["format"] == "recall-runtime-process-commands/v1"
            and len(journal["processes"]) == len(instance.processes.items), "original command inventory differs")
    states, process_proofs = [], []
    for row, child in zip(journal["processes"], instance.processes.items):
        require(row["pid"] == child.pid, "original command process custody differs")
        logs = []
        if row["exit_code"] is not None:
            require(type(row["exit_code"]) is int and row["exit_code"] == 0 and child.reaped
                    and all(not thread.is_alive() for thread in child.threads), "original setup process not cleanly reaped")
            require(type(row["log_sha256"]) is dict and set(row["log_sha256"]) == set(row["logs"]),
                    "original completed setup log inventory missing")
            for path in row["logs"]:
                reference = capture.original({"path": path, "sha256": row["log_sha256"][path]},
                                             "process-log", LIMITS["log_bytes_per_stream"])
                logs.append({"path": path, "ref": reference})
        else:
            require(not child.reaped and row["log_sha256"] is None, "persistent service logs cannot be terminal")
        states.append({"index": row["index"], "pid": child.pid, "reaped": bool(child.reaped),
                       "capture_drained": all(not thread.is_alive() for thread in child.threads), "logs": logs})
        proof = getattr(child, "capture_proof", None)
        require(proof is not None, "original prelaunch process proof is absent")
        snapshot = proof.snapshot()
        require(snapshot["index"] == row["index"] and snapshot["identity"]["pid"] == child.pid,
                "original process snapshot identifies another command")
        proof_ref, _ = capture.write(f"process-{row['index']:02}-proof", encode(snapshot), "startup-process-proof")
        process_proofs.append(proof_ref)
    require(instance.processes.capture_journal is not None, "original process journal is absent")
    for path, ref in instance.processes.capture_journal.files.items():
        maximum = LIMITS["log_bytes_per_stream"] if ref["kind"] == "process-log" else 2 * 1024**2
        captured = capture.original({"path": path, "sha256": ref["sha256"]}, ref["kind"], maximum)
        require(captured == ref, "original process journal typed bytes changed")
    command_ref, _ = capture.write("command-inventory", encode({
        "format": "recall-dynamic-startup-command-inventory/v2", "commands": journal_ref,
        "process_proofs": process_proofs}), "startup-command-inventory")
    setup_ref, _ = capture.write("setup-states", encode({"format": "recall-runtime-setup-states/v1", "processes": states}),
                                "startup-process-states")
    # Include actual later boundary reads without rewriting the startup receipt.
    original_captures = instance.http.captures
    require(original_captures[:len(receipt["captures"])] == receipt["captures"],
            "original startup capture prefix changed")
    capture_pairs = {}
    for row in original_captures:
        require(row["request_index"] not in capture_pairs, "duplicate original request index")
        pair = {"request_index": row["request_index"]}
        for kind in ("request", "response"):
            pair[kind] = capture.original({"path": row[kind + "_path"], "sha256": row[kind + "_sha256"]},
                                         "startup-" + kind, 2 * LIMITS["response_bytes"] + LIMITS["request_bytes"])
        capture_pairs[row["request_index"]] = pair
    processes = {"controller": capture.process("controller", context["process"]),
                 "bootstrap": capture.process("bootstrap", receipt["bootstrap_process"])}
    require(receipt["bootstrap_process"]["pid"] == os.getpid()
            and receipt["bootstrap_process"]["ppid"] == context["process"]["pid"], "bootstrap is not original current G child")
    for name, identity in receipt["processes"].items():
        require(identity["ppid"] == receipt["bootstrap_process"]["pid"], "service is not original bootstrap child")
        processes[name] = capture.process(name, identity)
    namespace = os.readlink("/proc/self/ns/net")
    boot_id = read_bound(Path("/proc/sys/kernel/random/boot_id"), 128).decode().strip()
    require(namespace == context["network"]["namespace"] and boot_id == context["anchor"]["clock"]["boot_id"],
            "original namespace or boot identity changed")
    observed_wall, observed_ns = time.time(), time.monotonic_ns()
    current = {"format": "recall-dynamic-current-processes/v2", "observed_monotonic_ns": observed_ns,
        "observed_wall": observed_wall, "processes": processes, "current_network_namespace": namespace,
        "boot_id": boot_id, "input_inventory": inventory_ref, "anchor": anchor_ref}
    current_ref, current_path = capture.write("current-processes", encode(current), "current-processes")
    obligations = {"format": "recall-dynamic-cleanup-obligations/v2", "run_nonce": receipt["run_nonce"],
        "backend": receipt["backend"], "original_deadline_monotonic_ns": receipt["original_deadline_monotonic_ns"],
        "processes": receipt["processes"]}
    cleanup_ref, cleanup_path = capture.write("cleanup-obligations", encode(obligations), "cleanup-obligations")
    require(gateway.read_context(context_reference["path"], context_reference["sha256"],
        expected=expected_launch, immediate_parent=True) == context, "original G changed during custody capture")
    # A file changed while processes were observed cannot borrow the earlier
    # anchor agreement. OriginalEvidence rereads each original without a cache.
    for name, _, _, _ in OUTER_LAUNCH_FILES:
        capture.raw_evidence[outer_inputs[name]["sha256"]]
    instance.allocation.remaining()
    controller_wall, controller_ns = time.time(), time.monotonic_ns()
    require(controller_ns >= observed_ns and controller_wall >= observed_wall, "custody observation clock moved backward")
    files = [{"path": path, "ref": reference} for path, reference in sorted(capture.files.items())]
    inventory, inventory_path = capture.write("capture-inventory", encode({
        "format": "recall-dynamic-capture-inventory/v2", "files": files}), "capture-inventory")
    worker_observation = instance.worker_recipe_observation
    provider_observations = instance.embedding_recipe_observations
    require(type(worker_observation) is dict and worker_observation["request_index"] in capture_pairs
            and set(provider_observations) == set(receipt["configurations"])
            and all(row["request_index"] in capture_pairs for row in provider_observations.values()),
            "original current worker or provider recipe captures missing")
    current_custody_ref, current_custody_path = capture.write("current-runtime", encode({
        "format": "recall-dynamic-current-runtime-custody/v2",
        "current_processes": current_ref, "capture_inventory": inventory, "cleanup_obligations": cleanup_ref,
        "outer_launch": outer_ref,
        "worker_recipe_capture": capture_pairs[worker_observation["request_index"]],
        "provider_recipe_captures": {name: capture_pairs[row["request_index"]]
                                     for name, row in provider_observations.items()}}), "runtime-current-custody")
    return {"bootstrap": bootstrap_ref, "controller": context_ref, "actual_runtime_configuration": runtime_ref,
        "actual_space_configurations": spaces_ref, "command_inventory": command_ref, "setup_states": setup_ref,
        "outer_launch": outer_ref,
        "current_custody": current_custody_ref,
        "current_processes": current_ref, "cleanup_obligations": cleanup_ref, "capture_inventory": inventory,
        "controller_context": {"context": context, "raw_sha256": context_ref["sha256"],
            "raw_bytes": capture.raw_evidence[context_ref["sha256"]], "observed_wall": controller_wall, "observed_monotonic_ns": controller_ns},
        "references": {"current_processes": current_path, "cleanup_obligations": cleanup_path,
                       "capture_inventory": inventory_path, "current_custody": current_custody_path,
                       "outer_launch": outer_path},
        "files": [{"path": path, "ref": reference} for path, reference in sorted(capture.files.items())],
        "raw_evidence": capture.raw_evidence}
