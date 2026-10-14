"""Bounded, data-only inputs for the isolated qualification runtime.

No package scripts, candidate executable, interpreter, service or network client
are executed here. git, dpkg-deb and readelf are trusted HOST inspection tools.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import stat
import subprocess
import tarfile
import threading
import time

GIB = 1024**3
MAX_TREE = 2 * GIB
MAX_POOL = 4 * GIB
MAX_ENTRIES = 99_999
SHA256 = re.compile(r"[a-f0-9]{64}")
HOST_ENV = {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode()


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def object_fields(value, required, optional=()):
    require(type(value) is dict and set(required) <= set(value)
            and set(value) <= set(required) | set(optional), "input object fields differ from contract")
    return value


def relative(value):
    require(type(value) is str and value and len(value) <= 4096 and "\0" not in value,
            "invalid package path")
    name = PurePosixPath(value)
    require(not name.is_absolute() and ".." not in name.parts
            and name.as_posix() == value and value != ".", "noncanonical package path")
    return value


def virtual_target(parent, target):
    require(type(target) is str and target and "\0" not in target, "invalid package link")
    result = [] if target.startswith("/") else list(PurePosixPath(parent).parts)
    for part in PurePosixPath(target).parts:
        if part in ("/", ".", ""):
            continue
        if part == "..":
            require(result, "package link escapes supplied filesystem")
            result.pop()
        else:
            result.append(part)
    require(result, "package link resolves to filesystem root")
    return "/".join(result)


class Limits:
    def __init__(self, seconds, budget):
        require(type(seconds) is int and 1 <= seconds <= 600, "deadline must be 1..600 seconds")
        self.deadline = time.monotonic() + seconds
        self.budget = budget
        self.pool_bytes = self.output_bytes = self.entries = 0
        self.next_disk_check = 0.0
        self.check(force=True)

    def check(self, *, pool=0, output=0, entries=0, force=False):
        self.pool_bytes += pool
        self.output_bytes += output
        self.entries += entries
        require(self.pool_bytes <= MAX_POOL and self.output_bytes <= MAX_TREE
                and self.entries <= MAX_ENTRIES, "runtime packaging size/count limit")
        now = time.monotonic()
        require(now < self.deadline, "runtime packaging deadline")
        if force or now >= self.next_disk_check:
            self.budget.check()
            self.next_disk_check = now + 1

    def remaining(self):
        self.check()
        return max(0.001, self.deadline - time.monotonic())


def checked_file(spec, maximum=MAX_TREE):
    object_fields(spec, ("path", "sha256", "bytes"))
    require(type(spec["path"]) is str and SHA256.fullmatch(spec["sha256"] or "") is not None
            and type(spec["bytes"]) is int and 0 <= spec["bytes"] <= maximum,
            "invalid input file identity")
    path = Path(spec["path"])
    require(path.is_absolute() and path.resolve() == path, "input path must be absolute and canonical")
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and not info.st_mode & 0o7022,
            "input must be a regular non-writable-by-others non-set-id file")
    require(info.st_size == spec["bytes"] and digest(path) == spec["sha256"],
            "input file differs from reviewed identity")
    return path


def copy_stream(stream, destination, size, expected, limits, *, pool=False, executable=False):
    require(type(size) is int and 0 <= size <= MAX_TREE, "file size limit")
    destination.parent.mkdir(parents=True, exist_ok=True)
    checksum = hashlib.sha256()
    remaining = size
    with destination.open("xb") as output:
        while remaining:
            block = stream.read(min(1024 * 1024, remaining))
            require(block, "truncated input file")
            limits.check(**({"pool": len(block)} if pool else {"output": len(block)}))
            output.write(block)
            checksum.update(block)
            remaining -= len(block)
        output.flush()
        os.fsync(output.fileno())
    value = checksum.hexdigest()
    require(expected is None or value == expected, "copied bytes differ from reviewed identity")
    os.chmod(destination, 0o555 if executable else 0o444)
    return value


class InspectedProcess:
    """Retain the leader until timeout signaling and group cleanup have ended."""
    def __init__(self, process):
        self.process = process
        self.stdout = process.stdout
        self.lock = threading.Lock()
        self.reaped = False

    def kill(self):
        with self.lock:
            if not self.reaped:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def wait_unreaped(self, timeout):
        deadline = time.monotonic() + timeout
        while True:
            with self.lock:
                require(not self.reaped, "inspection leader was already reaped")
                event = os.waitid(os.P_PID, self.process.pid,
                                  os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if event is not None:
                return event.si_status if event.si_code == os.CLD_EXITED else -event.si_status
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired("owned inspection child", timeout)
            time.sleep(min(0.02, remaining))

    def reap(self):
        # The caller cancels AND joins the timer first. The lock also protects
        # against any late signal caller: no killpg can run after PID reuse.
        with self.lock:
            if not self.reaped:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=5)
                self.reaped = True


@contextmanager
def inspected_process(command, limits):
    require(command[0] in ("/usr/bin/dpkg-deb", "/usr/bin/readelf", "/usr/bin/git"),
            "only fixed host inspection tools are supported")
    timeout = limits.remaining()
    raw = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, env=HOST_ENV, close_fds=True,
                           start_new_session=True)
    process = InspectedProcess(raw)
    timer = threading.Timer(timeout, process.kill)
    timer.daemon = True
    try:
        timer.start()
        yield process
        require(process.wait_unreaped(limits.remaining()) == 0, "host inspection tool failed")
        limits.check()
    finally:
        timer.cancel()
        if timer.ident is not None:
            timer.join()
        try:
            process.reap()
        finally:
            if process.stdout:
                process.stdout.close()


def tool_text(command, limits, maximum=1024 * 1024):
    with inspected_process(command, limits) as process:
        raw = process.stdout.read(maximum + 1)
        require(len(raw) <= maximum, "host inspection output limit")
        return raw.decode("utf-8", "strict")


class PackagePool:
    """Virtual package namespace: links never touch the host filesystem."""
    def __init__(self, root, limits):
        self.root, self.limits = root, limits
        root.mkdir(mode=0o700)
        self.entries = {}
        self.children = {}
        self.serial = 0

    def add(self, name, entry):
        relative(name)
        previous = self.entries.get(name)
        if previous:
            require(previous["kind"] == entry["kind"], "package entry type collision")
            if entry["kind"] == "file":
                require((previous["sha256"], previous["size"], previous["executable"])
                        == (entry["sha256"], entry["size"], entry["executable"]),
                        "package file collision")
            elif entry["kind"] == "link":
                require(previous["target"] == entry["target"], "package link collision")
            return
        require(len(self.entries) < MAX_ENTRIES, "package entry limit")
        self.entries[name] = entry
        parts = PurePosixPath(name).parts
        for index in range(len(parts)):
            parent = "/".join(parts[:index])
            child = parts[index]
            self.children.setdefault(parent, set()).add(child)

    def read_tar(self, stream, package):
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            for member in archive:
                self.limits.check()
                name = member.name
                while name.startswith("./"):
                    name = name[2:]
                name = name.rstrip("/")
                if not name or name == ".":
                    require(member.isdir(), "invalid archive root")
                    continue
                relative(name)
                if member.isdir():
                    self.add(name, {"kind": "directory", "package": package})
                elif member.issym() or member.islnk():
                    target = virtual_target("" if member.islnk() else str(PurePosixPath(name).parent),
                                            member.linkname)
                    self.add(name, {"kind": "link", "target": target, "package": package})
                elif member.isfile():
                    require(not member.mode & 0o7000, "package contains set-id or special permissions")
                    self.serial += 1
                    payload = self.root / str(self.serial)
                    with archive.extractfile(member) as data:
                        checksum = copy_stream(data, payload, member.size, None, self.limits, pool=True)
                    self.add(name, {"kind": "file", "payload": payload, "size": member.size,
                                    "sha256": checksum, "executable": bool(member.mode & 0o111),
                                    "package": package})
                else:
                    raise ValueError("package contains a device, socket or unsupported archive entry")

    def resolve(self, name, chain=()):
        require(len(chain) < 64 and name not in chain, "cyclic or excessive package link chain")
        parts = PurePosixPath(name).parts
        for index in range(1, len(parts) + 1):
            prefix = "/".join(parts[:index])
            entry = self.entries.get(prefix)
            if entry and entry["kind"] == "link":
                target = "/".join((entry["target"], *parts[index:]))
                return self.resolve(target, (*chain, name))
        entry = self.entries.get(name)
        if entry is None and name in self.children:
            entry = {"kind": "directory"}
        require(entry is not None, "package link or selected path lacks a supplied target")
        return name, entry

    def materialize(self, source, destination, output, ancestors=()):
        resolved, entry = self.resolve(relative(source))
        if entry["kind"] == "directory":
            require(resolved not in ancestors and len(ancestors) < 64, "cyclic directory alias")
            output.directory(destination)
            for child in sorted(self.children.get(resolved, ())):
                self.materialize(resolved + "/" + child, destination + "/" + child,
                                 output, (*ancestors, resolved))
        else:
            with entry["payload"].open("rb") as stream:
                output.file(destination, stream, entry["size"], entry["sha256"],
                            entry["executable"], {"package": entry["package"],
                                                  "package_path": resolved})


class RuntimeTree:
    def __init__(self, root, limits):
        self.root, self.limits = root, limits
        root.mkdir(mode=0o700)
        self.inventory = {}

    def directory(self, name):
        name = relative(name)
        path = self.root
        for component in PurePosixPath(name).parts:
            path = path / component
            require(not path.exists() or path.is_dir(), "runtime destination collision")
            if not path.exists():
                self.limits.check(entries=1)
                path.mkdir(mode=0o700)

    def file(self, name, stream, size, checksum, executable, origin):
        name = relative(name)
        require(name not in self.inventory, "duplicate runtime destination")
        parent = PurePosixPath(name).parent.as_posix()
        if parent != ".":
            self.directory(parent)
        self.limits.check(entries=1)
        actual = copy_stream(stream, self.root / name, size, checksum, self.limits,
                             executable=executable)
        self.inventory[name] = {"sha256": actual, "bytes": size,
                                "mode": 0o555 if executable else 0o444, "origin": origin}

    def seal(self):
        for path in sorted(self.root.rglob("*"), reverse=True):
            if path.is_dir():
                os.chmod(path, 0o555)
        os.chmod(self.root, 0o555)
        self.limits.check(force=True)


def package_control(text):
    values = {}
    field = None
    for line in text.splitlines():
        if line.startswith((" ", "\t")):
            require(field is not None, "invalid package control continuation")
            continue
        if not line:
            break
        name, colon, value = line.partition(":")
        require(colon and name not in values, "invalid package control fields")
        values[name], field = value.strip(), name
    return values


def add_deb(spec, pool, private_inputs, limits):
    object_fields(spec, ("file", "package", "version", "architecture"))
    require(re.fullmatch(r"[a-z0-9][a-z0-9+.-]*", spec["package"] or "") is not None
            and type(spec["version"]) is str and spec["version"]
            and spec["architecture"] in ("amd64", "all"), "invalid package identity")
    original = checked_file(spec["file"], maximum=512 * 1024**2)
    snapshot = private_inputs / (spec["file"]["sha256"] + ".deb")
    with original.open("rb") as stream:
        copy_stream(stream, snapshot, spec["file"]["bytes"], spec["file"]["sha256"], limits, pool=True)
    fields = package_control(tool_text(["/usr/bin/dpkg-deb", "--field", str(snapshot)], limits, 65536))
    require(tuple(fields.get(key) for key in ("Package", "Version", "Architecture"))
            == (spec["package"], spec["version"], spec["architecture"]),
            "Debian package metadata differs from reviewed identity")
    identity = spec["package"] + "=" + spec["version"]
    with inspected_process(["/usr/bin/dpkg-deb", "--fsys-tarfile", str(snapshot)], limits) as process:
        pool.read_tar(process.stdout, identity)
        trailing = 0
        for block in iter(lambda: process.stdout.read(16384), b""):
            limits.check()
            trailing += len(block)
            require(trailing <= 1024 * 1024 and not any(block), "unexpected package archive trailer")
    return {"package": spec["package"], "version": spec["version"],
            "architecture": spec["architecture"], "sha256": spec["file"]["sha256"]}
