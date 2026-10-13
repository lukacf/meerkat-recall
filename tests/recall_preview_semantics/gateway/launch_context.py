"""Original parent launch identity and observed namespace custody, never approval.

Only the reviewed launcher and its readonly bridge produce these files. A child
must verify the original hashes, raw observations and still-live bridge identity.
No provider credentials, JWT secret or private accounting capability is included.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time

ANCHOR_FORMAT = "recall-gateway-launch-anchor/v1"
SEMANTIC140_ANCHOR_FORMAT = "recall-gateway-launch-anchor/v2"
# Independent closed custody expectations; no producer-module import or quota input.
SEMANTIC140_ALLOCATION = "e42df2638349fea37f94eb7f9ea79d2afad170a2d36a655869a40f25f83dcd01"
SEMANTIC140_CORPUS = "e3201eb81bc294391e445a431a847f88603ec0631e23b6068d6f44cf0944ff0f"
CONTEXT_FORMAT = "recall-gateway-launch-context/v1"
MAX_BYTES = 131072
HASH_FIELDS = {"manifest_sha256", "proposal_sha256", "corpus_sha256", "artifact_sha256",
               "paired_artifact_sha256", "runtime_sha256", "configuration_sha256",
               "gateway_sha256", "bindings_sha256", "bridge_sha256", "context_helper_sha256",
               "command_sha256"}
PROCESS_FIELDS = {"pid", "ppid", "pgrp", "session", "start_ticks"}
CLOCK_FIELDS = {"wall_start", "mono_start", "boot_id", "deadline_wall", "deadline_mono", "time_cap_seconds"}
ANCHOR_FIELDS = {"format", "run_nonce", "source_commit", "allocation_sha256", "input_inventory_sha256", "clock", "caps", "parent", *HASH_FIELDS}
CONTEXT_FIELDS = {"format", "anchor", "anchor_sha256", "process", "network", "observed_wall", "observed_mono", "captures"}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def fields(value, names):
    require(type(value) is dict and set(value) == set(names), "launch_context_fields")
    return value


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "launch_context_duplicate_key")
            result[key] = value
        return result
    def finite(value):
        result = float(value)
        require(math.isfinite(result), "launch_context_nonfinite")
        return result
    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("launch_context_nonfinite")))


def read_bounded(path, *, private=False):
    path = Path(path)
    require(path.is_absolute() and path.parent.resolve() == path.parent, "launch_context_noncanonical_path")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_size <= MAX_BYTES, "launch_context_not_bounded_regular")
        if private:
            require(info.st_uid == os.getuid() and not info.st_mode & 0o077, "launch_context_not_private")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(MAX_BYTES + 1)
        require(len(raw) <= MAX_BYTES, "launch_context_byte_limit")
        return raw
    finally:
        os.close(fd)


def write_original(path, raw):
    path = Path(path)
    require(len(raw) <= MAX_BYTES and path.is_absolute() and path.parent.resolve() == path.parent,
            "launch_context_capture_path")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return {"path": str(path), "sha256": digest(raw)}


INPUT_FILE_CAP = 8 * 1024**2
INPUT_TREE_CAP = 32 * 1024**2
INPUT_COUNT_CAP = 1024


def input_bytes(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and not info.st_mode & 0o077 and info.st_size <= INPUT_FILE_CAP,
                "launch_inputs_not_private_bounded_regular")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(INPUT_FILE_CAP + 1)
        after = os.fstat(fd)
        require(len(raw) == info.st_size <= INPUT_FILE_CAP
                and (info.st_size, info.st_mtime_ns, info.st_ino) == (after.st_size, after.st_mtime_ns, after.st_ino),
                "launch_inputs_changed_during_read")
        return raw
    finally:
        os.close(fd)


def input_inventory(root):
    root = Path(root)
    require(root.is_absolute() and root.resolve() == root and not root.is_symlink(), "launch_inputs_noncanonical_root")
    pending, files, directories, entries, total = [root], [], [], 0, 0
    while pending:
        parent = pending.pop()
        info = parent.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077,
                "launch_inputs_not_private_directory")
        with os.scandir(parent) as children:
            for child in children:
                entries += 1
                require(entries <= 2048, "launch_inputs_entry_limit")
                path = Path(child.path)
                relative = path.relative_to(root)
                require(len(relative.parts) <= 16 and len(relative.as_posix()) <= 1024
                        and all(part not in (".", "..") and all(ord(c) >= 32 for c in part) for part in relative.parts),
                        "launch_inputs_relative_path")
                info = child.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    directories.append(relative.as_posix())
                    pending.append(path)
                else:
                    require(stat.S_ISREG(info.st_mode) and len(files) < INPUT_COUNT_CAP,
                            "launch_inputs_file_type_or_count")
                    raw = input_bytes(path)
                    total += len(raw)
                    require(total <= INPUT_TREE_CAP, "launch_inputs_total_limit")
                    files.append({"path": relative.as_posix(), "bytes": len(raw), "sha256": digest(raw)})
    require(files and all(any(item["path"].startswith(directory + "/") for item in files)
                          for directory in directories), "launch_inputs_empty_tree_or_directory")
    return {"format": "recall-gateway-inputs/v1", "files": sorted(files, key=lambda item: item["path"])}


def stage_inputs(source, destination, expected):
    """Copy only the independently pinned originals outside worker-write access."""
    hash_value(expected)
    source, destination = Path(source), Path(destination)
    require(destination.is_absolute() and destination.parent.resolve() == destination.parent,
            "launch_inputs_destination")
    original = input_inventory(source)
    require(digest(encode(original)) == expected, "launch_inputs_original_inventory_mismatch")
    if destination.exists():
        require(digest(encode(input_inventory(destination))) == expected, "launch_inputs_staged_inventory_changed")
        return original
    destination.mkdir(mode=0o700)
    for item in original["files"]:
        raw = input_bytes(source / item["path"])
        require(len(raw) == item["bytes"] and digest(raw) == item["sha256"], "launch_inputs_original_changed")
        target = destination / item["path"]
        parent = destination
        for part in Path(item["path"]).parts[:-1]:
            parent /= part
            parent.mkdir(mode=0o700, exist_ok=True)
        # Static inputs can be larger than a context JSON capture.
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        target.chmod(0o400)
    require(input_inventory(source) == original and input_inventory(destination) == original,
            "launch_inputs_inventory_changed")
    for path in sorted(destination.rglob("*"), reverse=True):
        if path.is_dir():
            path.chmod(0o500)
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    destination.chmod(0o500)
    fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return original


def verify_inputs(anchor):
    if anchor["input_inventory_sha256"] is not None:
        require(digest(encode(input_inventory(Path("/inputs")))) == anchor["input_inventory_sha256"],
                "launch_inputs_mounted_inventory_changed")


def process_identity(pid=None):
    pid = os.getpid() if pid is None else pid
    require(type(pid) is int and pid > 0, "launch_context_process_id")
    raw = read_bounded(Path(f"/proc/{pid}/stat")).decode("ascii")
    prefix, tail = raw.rsplit(")", 1)
    require(int(prefix.split("(", 1)[0].strip()) == pid, "launch_context_process_changed")
    columns = tail.split()
    require(len(columns) >= 20, "launch_context_process_stat")
    return {"pid": pid, "ppid": int(columns[1]), "pgrp": int(columns[2]),
            "session": int(columns[3]), "start_ticks": int(columns[19])}


def validate_process(value):
    fields(value, PROCESS_FIELDS)
    require(all(type(value[key]) is int and value[key] >= (0 if key == "ppid" else 1)
                for key in PROCESS_FIELDS), "launch_context_process_identity")


def hash_value(value, width=64):
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{" + str(width) + r"}", value),
            "launch_context_hash")


def namespace(value):
    require(type(value) is str and re.fullmatch(r"net:\[[0-9]+\]", value), "launch_context_namespace")


def validate_anchor(anchor):
    semantic140 = type(anchor) is dict and anchor.get("format") == SEMANTIC140_ANCHOR_FORMAT
    fields(anchor, ANCHOR_FIELDS | ({"profile_id"} if semantic140 else set()))
    require(anchor["format"] in (ANCHOR_FORMAT, SEMANTIC140_ANCHOR_FORMAT), "launch_context_anchor_format")
    if semantic140:
        require(anchor["profile_id"] == "semantic140-v1"
                and anchor["allocation_sha256"] == anchor["proposal_sha256"] == SEMANTIC140_ALLOCATION
                and anchor["corpus_sha256"] == SEMANTIC140_CORPUS, "launch_context_profile_identity")
    else:
        require(anchor["allocation_sha256"] != SEMANTIC140_ALLOCATION, "launch_context_profile_required")
    for key in HASH_FIELDS:
        hash_value(anchor[key])
    hash_value(anchor["source_commit"], 40)
    for key in ("allocation_sha256", "input_inventory_sha256"):
        if anchor[key] is not None:
            hash_value(anchor[key])
    require(type(anchor["run_nonce"]) is str and 0 < len(anchor["run_nonce"]) <= 128,
            "launch_context_nonce")
    clock = fields(anchor["clock"], CLOCK_FIELDS)
    for key in CLOCK_FIELDS - {"boot_id", "time_cap_seconds"}:
        require(type(clock[key]) in (int, float) and math.isfinite(clock[key]) and clock[key] > 0,
                "launch_context_clock")
    require(type(clock["time_cap_seconds"]) is int and clock["time_cap_seconds"] in (5400, 7800),
            "launch_context_time_cap")
    require(clock["deadline_wall"] == clock["wall_start"] + clock["time_cap_seconds"]
            and clock["deadline_mono"] == clock["mono_start"] + clock["time_cap_seconds"],
            "launch_context_deadline_reset")
    require(type(clock["boot_id"]) is str and re.fullmatch(r"[0-9a-f-]{36}", clock["boot_id"]),
            "launch_context_boot")
    caps = fields(anchor["caps"], {"physical_dispatches", "maximum_nano_usd"})
    require(all(type(value) is int for value in caps.values()), "launch_context_caps")
    expected = ((4260, 5_000_000_000) if semantic140 else
                (2100, 3_000_000_000) if anchor["allocation_sha256"] is not None else (432, 75_000_000_000))
    require((caps["physical_dispatches"], caps["maximum_nano_usd"]) == expected
            and clock["time_cap_seconds"] == (7800 if anchor["allocation_sha256"] is not None else 5400),
            "launch_context_allocation_changed")
    parent = fields(anchor["parent"], {"process", "network_namespace"})
    validate_process(parent["process"])
    namespace(parent["network_namespace"])
    return anchor


def connection_limits(anchor):
    """Closed transport headroom, separately bounded from physical admissions."""
    validate_anchor(anchor)
    if anchor["format"] == SEMANTIC140_ANCHOR_FORMAT:
        return (160, 7800)
    return (64, 7800) if anchor["allocation_sha256"] is not None else (8, 5400)


def parse_network(dev, routes4, routes6):
    interfaces = sorted({line.split(":", 1)[0].strip() for line in dev.decode("ascii").splitlines() if ":" in line})
    require(interfaces == ["lo"], "launch_context_unexpected_interface")
    rows4 = routes4.decode("ascii").splitlines()
    require(len(rows4) == 1 and rows4[0].split()[:2] == ["Iface", "Destination"],
            "launch_context_ipv4_routes")
    rows6 = [row.split() for row in routes6.decode("ascii").splitlines() if row.strip()]
    require(all(len(row) == 10 and row[-1] == "lo" for row in rows6), "launch_context_ipv6_route")
    return {"interfaces": interfaces, "ipv4_routes": 0, "ipv6_loopback_routes": len(rows6)}


def validate_context(context, *, expected, now_wall, now_mono):
    fields(context, CONTEXT_FIELDS)
    require(context["format"] == CONTEXT_FORMAT, "launch_context_format")
    anchor = validate_anchor(context["anchor"])
    require(context["anchor_sha256"] == digest(encode(anchor)), "launch_context_anchor_hash")
    require(type(expected) is dict and expected and set(expected) <= set(anchor), "launch_context_expected_identity")
    for key, value in expected.items():
        require(anchor[key] == value, "launch_context_identity_changed:" + key)
    clock = anchor["clock"]
    for observed, current, start, deadline in ((context["observed_wall"], now_wall, clock["wall_start"], clock["deadline_wall"]),
                                             (context["observed_mono"], now_mono, clock["mono_start"], clock["deadline_mono"])):
        require(type(observed) in (int, float) and type(current) in (int, float)
                and math.isfinite(observed) and math.isfinite(current)
                and start <= observed <= current < deadline, "launch_context_expired_or_clock_rollback")
    validate_process(context["process"])
    network = fields(context["network"], {"namespace", "parent_namespace", "interfaces", "ipv4_routes", "ipv6_loopback_routes"})
    namespace(network["namespace"])
    require(network["parent_namespace"] == anchor["parent"]["network_namespace"]
            and network["namespace"] != network["parent_namespace"]
            and network["interfaces"] == ["lo"] and type(network["ipv4_routes"]) is int
            and network["ipv4_routes"] == 0 and type(network["ipv6_loopback_routes"]) is int
            and 0 <= network["ipv6_loopback_routes"] <= 64, "launch_context_network_identity")
    captures = fields(context["captures"], {"dev", "ipv4", "ipv6"})
    for name, reference in captures.items():
        fields(reference, {"path", "sha256"})
        require(reference["path"] == f'/work/gateway-context-{context["anchor_sha256"]}.{name}.txt',
                "launch_context_capture_identity")
        hash_value(reference["sha256"])
    return context


def observe(anchor_raw, work=Path("/work")):
    anchor = validate_anchor(strict_json(anchor_raw))
    require(anchor_raw == encode(anchor), "launch_context_noncanonical_anchor")
    require(digest(read_bounded(Path("/bridge.py"))) == anchor["bridge_sha256"]
            and digest(read_bounded(Path("/launch_context.py"))) == anchor["context_helper_sha256"],
            "launch_context_producer_source")
    require(read_bounded(Path("/proc/sys/kernel/random/boot_id")).decode().strip() == anchor["clock"]["boot_id"],
            "launch_context_boot_changed")
    verify_inputs(anchor)
    raw = {"dev": read_bounded(Path(f"/proc/{os.getpid()}/net/dev")), "ipv4": read_bounded(Path(f"/proc/{os.getpid()}/net/route")),
           "ipv6": read_bounded(Path(f"/proc/{os.getpid()}/net/ipv6_route"))}
    network = parse_network(raw["dev"], raw["ipv4"], raw["ipv6"])
    network.update(namespace=os.readlink("/proc/self/ns/net"), parent_namespace=anchor["parent"]["network_namespace"])
    checksum = digest(anchor_raw)
    context = {"format": CONTEXT_FORMAT, "anchor": anchor, "anchor_sha256": checksum,
               "process": process_identity(), "network": network,
               "observed_wall": time.time(), "observed_mono": time.monotonic(),
               "captures": {name: {"path": str(work / f"gateway-context-{checksum}.{name}.txt"), "sha256": digest(value)}
                            for name, value in raw.items()}}
    validate_context(context, expected={"manifest_sha256": anchor["manifest_sha256"]},
                     now_wall=time.time(), now_mono=time.monotonic())
    for name, value in raw.items():
        write_original(Path(context["captures"][name]["path"]), value)
    return write_original(work / f"gateway-context-{checksum}.json", encode(context))


def read_context(path, checksum, *, expected, immediate_parent=False):
    """Validate actual bridge evidence before startup/collection, never a boolean."""
    hash_value(checksum)
    raw = read_bounded(Path(path), private=True)
    require(digest(raw) == checksum, "launch_context_observation_changed")
    context = validate_context(strict_json(raw), expected=expected, now_wall=time.time(), now_mono=time.monotonic())
    anchor_raw = read_bounded(Path("/launch-anchor.json"), private=True)
    require(digest(anchor_raw) == context["anchor_sha256"] and strict_json(anchor_raw) == context["anchor"],
            "launch_context_original_mount_changed")
    require(str(path) == f'/work/gateway-context-{context["anchor_sha256"]}.json', "launch_context_observation_path")
    require(process_identity(context["process"]["pid"]) == context["process"], "launch_context_bridge_replaced")
    require(not immediate_parent or os.getppid() == context["process"]["pid"], "launch_context_not_bridge_child")
    require(os.readlink("/proc/self/ns/net") == context["network"]["namespace"], "launch_context_namespace_changed")
    require(read_bounded(Path("/proc/sys/kernel/random/boot_id")).decode().strip() == context["anchor"]["clock"]["boot_id"],
            "launch_context_boot_changed")
    require(digest(read_bounded(Path("/bridge.py"))) == context["anchor"]["bridge_sha256"]
            and digest(read_bounded(Path("/launch_context.py"))) == context["anchor"]["context_helper_sha256"],
            "launch_context_producer_source_changed")
    parse_network(read_bounded(Path(f"/proc/{os.getpid()}/net/dev")),
                  read_bounded(Path(f"/proc/{os.getpid()}/net/route")),
                  read_bounded(Path(f"/proc/{os.getpid()}/net/ipv6_route")))
    captures = {}
    for name, reference in context["captures"].items():
        value = read_bounded(Path(reference["path"]), private=True)
        require(digest(value) == reference["sha256"], "launch_context_raw_capture_changed")
        captures[name] = value
    actual = parse_network(captures["dev"], captures["ipv4"], captures["ipv6"])
    require(all(context["network"][key] == value for key, value in actual.items()), "launch_context_raw_proof_mismatch")
    verify_inputs(context["anchor"])
    validate_context(context, expected=expected, now_wall=time.time(), now_mono=time.monotonic())
    return context
