"""Read-only original accounting channel; no ledger, network or provider access.

The caller owns this bounded Mapping. Pure validators receive bytes by digest
and never open paths. The channel is a separate, explicit launch extension.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat

CHANNEL_FORMAT = "recall-gateway-accounting-channel/v1"
INDEX_FORMAT = "recall-gateway-accounting-index/v1"
BUNDLE_FORMAT = "recall-dynamic-provider-accounting/v2"
MOUNT = "/gateway-accounting"
SIDECAR = "/gateway-accounting-channel.json"
MAX_RECORD = 8 * 1024**2
MAX_TOTAL = 20 * 1024**3
MAX_CACHE = 256 * 1024**2
MAX_FILES = 2100 * 5 + 32
# S validates the same preparation at prepared and premeasurement boundaries.
# Each boundary has one L inventory pass and one F provider-evidence pass.
VERIFICATION_PASSES = 4
# Additional metadata reads outside those passes: original G/S export prelude
# and the L begin/channel checks at both boundaries. These are per export, not
# per physical dispatch. No other body receives a repeat-read allowance.
METADATA_READS = {"provider-accounting": (4, MAX_RECORD),
                  "provider-cohort-begin": (5, 65536),
                  "provider-cohort-close": (3, 65536),
                  "provider-capture-inventory": (2, MAX_RECORD)}


class AccountingUnavailable(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise AccountingUnavailable(message)


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate accounting JSON key")
            result[key] = value
        return result
    try:
        result = json.loads(raw, object_pairs_hook=pairs)
        encode(result)
        return result
    except (ValueError, UnicodeError, RecursionError) as error:
        raise AccountingUnavailable("invalid accounting JSON") from error


def reference(raw, kind):
    return {"sha256": digest(raw), "bytes": len(raw), "kind": kind}


def validate_ref(value):
    require(type(value) is dict and set(value) == {"sha256", "bytes", "kind"}
            and type(value["sha256"]) is str and re.fullmatch(r"[a-f0-9]{64}", value["sha256"])
            and type(value["bytes"]) is int and 0 <= value["bytes"] <= MAX_RECORD
            and type(value["kind"]) is str and 0 < len(value["kind"]) <= 80,
            "invalid bounded accounting reference")


def relative_parts(value):
    require(type(value) is str and 0 < len(value) <= 256, "invalid accounting path")
    path = PurePosixPath(value)
    require(path.parts and not path.is_absolute() and str(path) == value
            and all(part not in (".", "..") for part in path.parts), "unsafe accounting path")
    return path.parts


def directory_identity(path):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and not info.st_mode & 0o077, "unsafe accounting directory")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid,
            "mode": stat.S_IMODE(info.st_mode)}


def read_owned(root, relative, maximum):
    """Resolve every component through owned directory FDs, never a symlink."""
    parts = relative_parts(relative)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
            info = os.fstat(fd)
            require(info.st_uid == os.getuid() and not info.st_mode & 0o077, "unsafe accounting parent")
        source = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
        try:
            before = os.fstat(source)
            require(stat.S_ISREG(before.st_mode) and before.st_uid == os.getuid()
                    and not before.st_mode & 0o077 and before.st_size <= maximum,
                    "unsafe or oversized accounting record")
            with os.fdopen(source, "rb", closefd=False) as stream:
                raw = stream.read(maximum + 1)
            after = os.fstat(source)
            require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    and len(raw) == before.st_size, "accounting record changed during read")
            return raw
        finally:
            os.close(source)
    finally:
        os.close(fd)


def verify_mount(raw, mount=MOUNT):
    """Check the actual per-mount flags; an ordinary filesystem ro claim is insufficient."""
    require(len(raw) <= 1024**2, "accounting mount inventory bound")
    records = []
    for line in raw.decode("utf-8", errors="strict").splitlines():
        fields = line.split()
        require(len(fields) >= 10 and "-" in fields, "invalid mount record")
        if fields[4] == mount:
            records.append(fields)
    require(len(records) == 1 and "ro" in records[0][5].split(",")
            and "rw" not in records[0][5].split(","), "accounting channel is not actually read-only")


class RawEvidence(Mapping):
    """Lazy immutable bytes; bounded cache, total disk reads and access count."""
    def __init__(self, root, files, *, cache_limit=8 * 1024**2):
        require(type(cache_limit) is int and 0 <= cache_limit <= MAX_CACHE, "accounting cache bound")
        require(type(files) is list and 0 < len(files) <= MAX_FILES, "accounting inventory bound")
        self.root = Path(root)
        self.original_directory = directory_identity(self.root)
        self.records, paths, total = {}, set(), 0
        metadata_kinds, metadata_reads = set(), 0
        for entry in files:
            require(type(entry) is dict and set(entry) == {"path", "ref"}, "accounting inventory shape")
            relative_parts(entry["path"])
            validate_ref(entry["ref"])
            require(entry["path"] not in paths, "duplicate accounting path")
            paths.add(entry["path"])
            total += entry["ref"]["bytes"]
            kind = entry["ref"]["kind"]
            if kind in METADATA_READS:
                repeats, maximum = METADATA_READS[kind]
                require(kind not in metadata_kinds and entry["ref"]["bytes"] <= maximum,
                        "duplicate or oversized accounting export metadata")
                metadata_kinds.add(kind)
                metadata_reads += repeats * entry["ref"]["bytes"]
            checksum = entry["ref"]["sha256"]
            if checksum in self.records:
                require(self.records[checksum]["ref"] == entry["ref"], "conflicting accounting digest or kind")
            else:
                self.records[checksum] = entry
        require(total <= MAX_TOTAL, "original accounting capture cap exceeded")
        self.original_bytes = total
        self.read_limit = VERIFICATION_PASSES * total + metadata_reads
        self.cache, self.cache_bytes = OrderedDict(), 0
        self.cache_limit, self.read_bytes, self.accesses = cache_limit, 0, 0

    def __iter__(self):
        return iter(self.records)

    def __len__(self):
        return len(self.records)

    def __contains__(self, checksum):
        # Mapping's default implementation calls __getitem__. Inventory
        # membership must not accidentally read every original response body.
        return checksum in self.records

    def __getitem__(self, checksum):
        entry = self.records[checksum]
        self.accesses += 1
        require(self.accesses <= MAX_FILES * 16, "accounting resolver access bound")
        require(directory_identity(self.root) == self.original_directory, "original accounting directory changed")
        if checksum in self.cache:
            self.cache.move_to_end(checksum)
            return self.cache[checksum]
        size = entry["ref"]["bytes"]
        require(self.read_bytes + size <= self.read_limit, "original accounting read budget exhausted")
        raw = read_owned(self.root, entry["path"], size)
        self.read_bytes += len(raw)
        require(len(raw) == size and digest(raw) == checksum, "original accounting record changed")
        if size <= self.cache_limit:
            while self.cache and self.cache_bytes + size > self.cache_limit:
                _, removed = self.cache.popitem(last=False)
                self.cache_bytes -= len(removed)
            self.cache[checksum] = raw
            self.cache_bytes += size
        return raw


def observe_channel():
    """Bind this actual read-only mount to the unchanged original launch anchor."""
    sidecar_raw = read_owned(Path("/"), SIDECAR[1:], 16384)
    sidecar = decode(sidecar_raw)
    require(type(sidecar) is dict and set(sidecar) == {
        "format", "anchor_sha256", "path", "directory", "helper_sha256", "read_only"}
        and sidecar["format"] == CHANNEL_FORMAT and sidecar["path"] == MOUNT
        and sidecar["read_only"] is True, "accounting channel sidecar differs")
    anchor = read_owned(Path("/"), "launch-anchor.json", MAX_RECORD)
    require(digest(anchor) == sidecar["anchor_sha256"], "accounting original launch anchor differs")
    observed_directory = directory_identity(Path(MOUNT))
    require(observed_directory == sidecar["directory"], "accounting original directory differs")
    directory_raw = encode({"format": "recall-provider-accounting-directory/v2",
                            "path": MOUNT, **observed_directory})
    require(len(directory_raw) <= 16384, "accounting directory observation bound")
    with Path(__file__).open("rb") as source:
        helper = source.read(MAX_RECORD + 1)
    require(len(helper) <= MAX_RECORD and digest(helper) == sidecar["helper_sha256"], "accounting reader source differs")
    with Path("/proc/self/mountinfo").open("rb") as source:
        mountinfo = source.read(1024**2 + 1)
    verify_mount(mountinfo, MOUNT)
    return sidecar_raw, mountinfo, directory_raw


def open_export(manifest, begin, close, exported):
    """Read the original small index, then supply a lazy bounded byte Mapping."""
    sidecar_raw, mountinfo, directory_raw = observe_channel()
    require(type(exported) is dict and set(exported) == {"path", "ref"}, "closed accounting index absent")
    validate_ref(exported["ref"])
    require(exported["ref"]["kind"] == "provider-accounting-index", "accounting index kind differs")
    index_raw = read_owned(Path(MOUNT), exported["path"], exported["ref"]["bytes"])
    require(reference(index_raw, "provider-accounting-index") == exported["ref"], "accounting original index differs")
    index = decode(index_raw)
    require(type(index) is dict and set(index) == {
        "format", "cohort_id", "backend", "phase", "caller_sha256", "begin_sha256", "close_sha256", "reference", "files"}
        and index["format"] == INDEX_FORMAT
        and index["cohort_id"] == begin.get("cohort_id") == close.get("cohort_id")
        and type(index["cohort_id"]) is int
        and index["backend"] == manifest.get("backend") == begin.get("backend")
        and index["phase"] == manifest.get("provider_phase", "measurement") == begin.get("phase")
        and index["caller_sha256"] == digest(encode(manifest))
        and index["begin_sha256"] == digest(encode(begin))
        and index["close_sha256"] == digest(encode(close)), "accounting index substituted original cohort")
    require(exported["path"] == f'cohort-{index["cohort_id"]:02d}/index.json', "accounting original index path differs")
    validate_ref(index["reference"])
    require(index["reference"]["kind"] == "provider-accounting", "accounting bundle kind differs")
    raw = RawEvidence(Path(MOUNT), index["files"])
    observed_directory = decode(directory_raw)
    require(raw.original_directory == {key: observed_directory[key]
            for key in ("device", "inode", "uid", "mode")}, "original observed accounting directory changed")
    bundle = raw[index["reference"]["sha256"]]
    require(len(bundle) == index["reference"]["bytes"], "accounting bundle length differs")
    # These are original raw channel observations for the caller's typed carrier;
    # they are not merged into or substituted for a gateway v1 record.
    return {"reference": index["reference"], "raw_evidence": raw, "files": index["files"],
            "channel": {"sidecar": sidecar_raw, "mountinfo": mountinfo,
                        "index": index_raw, "directory": directory_raw}}
