#!/usr/bin/env python3
"""Bounded, separately allocated offline qualification of supplied local images.

Importing this module and rendering a plan never contacts Docker or a service.
A report records observations; independent qualification owns the final verdict.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import http.client
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[2]
CASES_PATH = ROOT / "tests/recall_preview_semantics/container_offline_cases.json"
IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
LABEL = "org.meerkat.recall.qualification"
MAX_SECONDS = 1500
MAX_COPY_BYTES = 512 * 1024 * 1024
DEPENDENCIES = ("scripts/qualification/installed_linux_offline.py",
                "scripts/qualification/recall_preview.py", "scripts/release/artifact.py",
                "tests/recall_preview/fixtures.py", "tests/recall_preview/matrix.py",
                "tests/recall_preview/oracle.py")
MAX_COMMANDS = 400
MAX_COMMAND_BYTES = 1024 * 1024
MAX_HTTP_BYTES = 262144
MAX_HTTP_CALLS = 640
MIN_FREE_BYTES = 250 * 1024 ** 3
RECALL_ROOT = "/var/lib/meerkat-recall"
AUTH_ROOT = "/run/recall-auth"


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def encoded(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise ValueError("nonfinite JSON number")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_private_json(path):
    path = Path(path)
    require(path.is_absolute() and not path.is_symlink(), "absolute regular custody file required")
    require(path.resolve() == path, "custody file must have canonical parents")
    require(path.is_file() and path.stat().st_size <= MAX_COMMAND_BYTES, "custody file exceeds bound")
    return strict_json(path.read_bytes())


def utc(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None and parsed.utcoffset() == dt.timedelta(0), "UTC timestamp required")
    return parsed


def private_new(path, data):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def plan():
    cases = strict_json(CASES_PATH.read_bytes())
    return {"format": "recall-container-plan/v1", "expectations": cases,
            "expectations_sha256": digest(CASES_PATH.read_bytes()),
            "driver_sha256": digest(Path(__file__).read_bytes()),
            "product_acceptance": False}


def validate_allocation(path, now=None):
    allocation = read_private_json(path)
    require(allocation.get("format") == "recall-container-run/v1", "unsupported allocation format")
    require(allocation.get("execution_authorized") is True, "run has not been allocated for execution")
    require(allocation.get("backend") in ("elephant-db", "surreal-db"), "explicit supported backend required")
    run_id = allocation.get("run_id", "")
    require(str(uuid.UUID(run_id)) == run_id and uuid.UUID(run_id).version == 4, "fresh canonical UUIDv4 run ID required")
    require(SHA.fullmatch(allocation.get("source_commit", "")), "exact source commit required")
    require(SHA.fullmatch(allocation.get("source_tree", "")), "exact source tree required")
    require(HEX64.fullmatch(allocation.get("source_tar_sha256", "")), "exact source archive digest required")
    for field in ("recall_image_id", "paired_image_id"):
        require(IMAGE.fullmatch(allocation.get(field, "")), "both exact supplied local image IDs required")
    for field in ("recall_binary_sha256", "paired_binary_sha256", "recall_provenance_sha256",
                  "paired_source_provenance_sha256", "paired_build_provenance_sha256", "review_sha256"):
        require(HEX64.fullmatch(allocation.get(field, "")), f"trusted {field} required")
    now = now or dt.datetime.now(dt.timezone.utc)
    require(now < utc(allocation["expires_at"]) <= now + dt.timedelta(hours=2), "allocation expired or unbounded")
    directory = Path(allocation["run_directory"])
    require(directory.is_absolute() and directory.resolve() == directory, "canonical absolute run directory required")
    require(not directory.exists() and not directory.is_symlink(), "run directory must be new")
    for parent in directory.parents:
        require(not (parent / ".elephant").exists() and not (parent / "Cargo.toml").exists(),
                "run directory must be outside existing runtimes and projects")
    docker_socket = Path(allocation["docker_socket"])
    require(docker_socket.is_absolute() and docker_socket.resolve() == docker_socket,
            "explicit canonical local Docker socket required")
    inventory = plan()
    review_path = Path(allocation["review"])
    review = read_private_json(review_path)
    require(digest(review_path.read_bytes()) == allocation["review_sha256"], "independent review digest changed")
    require(review.get("format") == "recall-container-review/v1" and review.get("reviewer") == "qualification"
            and review.get("approved") is True, "independent reviewed case inventory required")
    for field in ("driver_sha256", "expectations_sha256"):
        require(review.get(field) == inventory[field], f"independent review has different {field}")
    hashes = allocation.get("utility_sha256", {})
    require(set(hashes) == set(DEPENDENCIES), "all frozen utility dependencies required")
    require(review.get("utility_sha256") == hashes, "utility review differs")
    for relative, expected in hashes.items():
        require(HEX64.fullmatch(expected) and digest((ROOT / relative).read_bytes()) == expected,
                "reviewed utility source changed")
    return allocation, inventory


class CommandFailure(RuntimeError):
    """A bounded command failed; raw output remains private and is never in this error."""


class WorkCancellation:
    """Record signals without asynchronous exceptions inside owned cleanup."""
    def __init__(self):
        self.requested = False
        self.cleaning = False

    def request(self, _signal=None, _frame=None):
        self.requested = True

    def check(self, *, cleanup=False):
        if self.requested and not (cleanup or self.cleaning):
            raise InterruptedError("allocated run interrupted")

    def begin_cleanup(self):
        self.cleaning = True


class DockerCommands:
    """No shell, ambient credentials, remote context, build, pull or broad cleanup."""
    def __init__(self, allocation, directory, monotonic=time.monotonic, *, cancellation=None):
        self.allocation = allocation
        self.directory = Path(directory)
        self.clock = monotonic
        self.started = monotonic()
        self.commands = 0
        self.records = []
        self.secrets = []
        self.capture_bytes = 0
        self.cleanup_deadline = None
        self.cancellation = cancellation or WorkCancellation()
        config = self.directory / "docker-config"
        config.mkdir(mode=0o700)
        self.environment = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                            "DOCKER_CONFIG": str(config), "HOME": str(self.directory)}
        self.prefix = ["/usr/bin/docker", "--host", "unix://" + allocation.get("docker_socket", "/var/run/docker.sock")]

    def remaining(self, cleanup=False):
        self.cancellation.check(cleanup=cleanup)
        if cleanup:
            if self.cleanup_deadline is None:
                self.cleanup_deadline = self.clock() + 90
            remaining = self.cleanup_deadline - self.clock()
        else:
            require(dt.datetime.now(dt.timezone.utc) < utc(self.allocation["expires_at"]), "allocation expired")
            remaining = MAX_SECONDS - (self.clock() - self.started)
        require(remaining > 0, "owned run deadline exceeded")
        return remaining

    def redact(self, raw):
        for secret in self.secrets:
            if secret:
                raw = raw.replace(secret.encode(), b"[REDACTED]")
        return raw

    def run(self, arguments, *, timeout=30, cleanup=False, secret=False, check=True, input_bytes=None, output_limit=MAX_COMMAND_BYTES):
        require(isinstance(arguments, list) and arguments and all(isinstance(x, str) for x in arguments),
                "explicit command argument vector required")
        require(arguments[0] in {"image", "container", "network", "volume", "create", "start", "stop",
                                    "kill", "exec", "inspect", "logs", "wait", "info"}, "unapproved Docker operation")
        require(not any(x in {"prune", "build", "pull", "push", "system"} for x in arguments),
                "unowned Docker operation refused")
        self.commands += 1
        require(self.commands <= MAX_COMMANDS + (128 if cleanup else 0), "command count exceeded")
        budget = min(timeout, self.remaining(cleanup))
        start = self.clock()
        command = self.prefix + arguments
        require(0 < output_limit <= MAX_COPY_BYTES, "command output allocation exceeds hard bound")
        input_file = None
        if input_bytes is not None:
            require(len(input_bytes) <= 65536, "command input exceeds bound")
            input_file = tempfile.TemporaryFile(dir=self.directory)
            input_file.write(input_bytes)
            input_file.seek(0)
        child = subprocess.Popen(command, env=self.environment, stdin=input_file or subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, start_new_session=True)
        stdout, stderr = bytearray(), bytearray()
        failure = None
        selector = selectors.DefaultSelector()
        try:
            for stream, target in ((child.stdout, stdout), (child.stderr, stderr)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, target)
            while selector.get_map():
                self.cancellation.check(cleanup=cleanup)
                if self.clock() - start >= budget:
                    raise TimeoutError("owned Docker command deadline")
                for key, _ in selector.select(min(0.1, budget - (self.clock() - start))):
                    chunk = os.read(key.fileobj.fileno(), 16384)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    key.data.extend(chunk)
                    if len(stdout) + len(stderr) > output_limit:
                        raise ValueError("owned Docker command output bound")
            code = child.wait(timeout=max(0.01, budget - (self.clock() - start)))
        except BaseException as error:
            failure = type(error).__name__
            # This process is still our unreaped child, so its process-group ID
            # cannot have been reused. Never signal a PID obtained from a file.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=5)
            raise
        finally:
            selector.close()
            if input_file is not None:
                input_file.close()
            child.stdout.close()
            child.stderr.close()
            record = {"arguments": [self.redact(x.encode()).decode() for x in arguments],
                      "started_monotonic": start, "ended_monotonic": self.clock(),
                      "deadline_monotonic": start + budget,
                      "duration_ms": (self.clock() - start) * 1000, "failure": failure,
                      "stdout_sha256": digest(stdout), "stderr_sha256": digest(stderr)}
            if not secret:
                record.update(stdout=self.redact(bytes(stdout[:MAX_COMMAND_BYTES])).decode(errors="replace"),
                              stderr=self.redact(bytes(stderr[:MAX_COMMAND_BYTES])).decode(errors="replace"))
            raw = encoded(record)
            self.capture_bytes += len(raw)
            require(self.capture_bytes <= 32 * MAX_COMMAND_BYTES, "command capture budget exceeded")
            private_new(self.directory / f"command-{self.commands:03d}.json", raw)
            self.records.append(record)
        if check and code != 0:
            raise CommandFailure(f"owned Docker {arguments[0]} exited {code}")
        return code, bytes(stdout), bytes(stderr)

    def json(self, arguments, **kwargs):
        code, output, _ = self.run(arguments, **kwargs)
        require(code == 0, "Docker identity command failed")
        return strict_json(output)


class OwnedResources:
    """Remember created identities and recheck exact private labels before removal."""
    def __init__(self, commands, run_id):
        self.commands = commands
        self.run_id = run_id
        self.prefix = "recall-q-" + run_id.replace("-", "")
        self.nonce = uuid.uuid4().hex
        self.resources = []

    def labels(self, purpose):
        return {LABEL: self.run_id, LABEL + ".nonce": self.nonce, LABEL + ".purpose": purpose}

    def inspect(self, kind, identity, cleanup=False):
        rows = self.commands.json([kind, "inspect", identity], cleanup=cleanup, secret=True)
        require(isinstance(rows, list) and len(rows) == 1, "ambiguous resource identity")
        return rows[0]

    def verify(self, kind, identity, purpose, cleanup=False):
        row = self.inspect(kind, identity, cleanup)
        actual = row.get("Config", {}).get("Labels", {}) if kind == "container" else row.get("Labels", {})
        require(all(actual.get(key) == value for key, value in self.labels(purpose).items()),
                "resource is not owned by this exact run")
        require(row.get("Name") == identity if kind == "volume" else row.get("Id") == identity,
                "resource identity changed")
        return row

    def create(self, kind, purpose, extra=None):
        require(kind in {"volume", "network", "container"}, "unsupported owned resource")
        require(re.fullmatch(r"[a-z][a-z0-9-]{0,40}", purpose), "bounded resource purpose required")
        require(len(self.resources) < 64, "owned resource count exceeded")
        name = self.prefix + "-" + purpose
        code, _, _ = self.commands.run([kind, "inspect", name], check=False, secret=True)
        require(code != 0, "refusing an existing resource name")
        arguments = [kind, "create"] if kind != "container" else ["create"]
        for key, value in self.labels(purpose).items():
            arguments += ["--label", key + "=" + value]
        if kind == "volume":
            arguments += [name]
        elif kind == "network":
            arguments += ["--internal", "--driver", "bridge", name]
        else:
            arguments += ["--name", name, *(extra or [])]
        _, output, _ = self.commands.run(arguments)
        identity = output.decode().strip()
        require(identity == name if kind == "volume" else re.fullmatch(r"[a-f0-9]{64}", identity),
                "created resource has invalid identity")
        # Retain created identity even if verification fails. Cleanup always
        # rechecks both labels and identity, and reports any unprovable resource.
        self.resources.append((kind, identity, purpose))
        self.verify(kind, identity, purpose)
        if kind == "network":
            require(self.inspect(kind, identity).get("Internal") is True, "network permits external routing")
        return identity

    def cleanup(self):
        errors = []
        for kind, identity, purpose in reversed(self.resources):
            try:
                self.verify(kind, identity, purpose, cleanup=True)
                arguments = [kind, "rm"] + (["--force"] if kind == "container" else []) + [identity]
                self.commands.run(arguments, cleanup=True, timeout=20)
            except Exception as error:
                errors.append({"kind": kind, "identity": identity, "error_type": type(error).__name__})
        return errors


def frozen_utilities():
    """Only the agreed pure parsers/deadline; never instantiate S's driver."""
    spec = importlib.util.spec_from_file_location("container_installed_utilities", ROOT / DEPENDENCIES[0])
    require(spec is not None and spec.loader is not None, "frozen utility unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.mcp_value, module.typed_denial, module.capture.total_deadline


def one_tar_file(raw, maximum):
    """Inspect one Docker copy stream without extracting paths onto the host."""
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        entries = archive.getmembers()
        require(len(entries) == 1 and entries[0].isfile() and 0 <= entries[0].size <= maximum,
                "copy must contain exactly one bounded regular file")
        stream = archive.extractfile(entries[0])
        require(stream is not None, "copy has no regular content")
        data = stream.read(maximum + 1)
        require(len(data) == entries[0].size, "copy size differs")
        return data


def operation_matches(accepted, observed, *, complete=False):
    original = accepted["operation"]
    for wire, receipt in (("operation_id", "operation_id"), ("internal_source_id", "source_id"),
                          ("generation", "source_generation"), ("desired_revision", "desired_revision")):
        require(observed.get(wire) == original[receipt], "original acknowledged operation identity changed")
    require(observed.get("lifecycle") == "active" and observed.get("cleanup_state") == "not_required",
            "metadata operation changed lifecycle")
    if complete:
        require(observed.get("state") == "complete" and observed.get("error_code") is None
                and observed.get("published_revision") == original["desired_revision"],
                "required Metadata publication did not complete")
    else:
        require(observed.get("state") in ("pending", "running") and observed.get("published_revision") is None,
                "disabled workers unexpectedly completed accepted Metadata")


def stable_space_configuration(body, expected):
    """Compare all persisted fields, omitting only two documented storage clocks."""
    require(isinstance(body, dict) and body.get("space_id") == expected["name"]
            and isinstance(body.get("id"), str) and body["id"], "stored named-space identity differs")
    for field in ("security", "embedding_provider", "embedding_model", "embedding_dims", "purpose", "skill"):
        require(body.get(field) == expected[field], f"stored named-space {field} differs")
    actual = strict_json(encoded(body))
    require(isinstance(actual.pop("updated_at", None), str), "stored configuration update clock missing")
    processing = actual.get("processing")
    require(isinstance(processing, dict), "stored processing authority missing")
    index_generation = processing.pop("index_generation", None)
    require(type(index_generation) is int and index_generation >= 0, "stored content epoch invalid")
    expected_processing = {key: value for key, value in expected["processing"].items() if key != "index_generation"}
    require(processing == expected_processing, "stored processing policy/profile/binding authority differs")
    return actual


def verify_backup_payload(manifest, payload, backend, journal_id):
    require(manifest.get("backend") == backend and manifest.get("journal_id") == journal_id,
            "backup backend or original journal differs")
    require(type(manifest.get("bytes")) is int and manifest["bytes"] == len(payload) > 0
            and manifest.get("sha256") == digest(payload), "copied backup payload differs from registered receipt")


def require_missing_journal_reason(backend, output):
    reasons = {
        "elephant-db": b"live recovery identity requires its original erasure journal",
        "surreal-db": b"store requires its original independently retained erasure journal",
    }
    require(reasons[backend] in output, "negative failed without the original-journal diagnostic")


class ContainerDriver:
    """Actual public lifecycle flow, bounded to this allocation's owned resources."""
    def __init__(self, allocation, inventory, commands=None, utilities=None):
        self.allocation = allocation
        self.root = Path(allocation["run_directory"])
        self.commands = commands or DockerCommands(allocation, self.root)
        self.owned = OwnedResources(self.commands, allocation["run_id"])
        self.mcp_value, self.typed_denial, self.deadline = utilities or frozen_utilities()
        self.cases = inventory["expectations"]
        self.report = {"format": "recall-container-observations/v1", "backend": allocation["backend"],
            "run_id": allocation["run_id"], "source_commit": allocation["source_commit"],
            "source_tree": allocation["source_tree"], "source_tar_sha256": allocation["source_tar_sha256"],
            "image_ids": {key: allocation[key] for key in ("recall_image_id", "paired_image_id")},
            "driver_sha256": inventory["driver_sha256"], "expectations_sha256": inventory["expectations_sha256"],
            "utility_sha256": allocation["utility_sha256"], "review_sha256": allocation["review_sha256"],
            "cases": {case: {"verdict": "not_executed"} for case in self.cases["required_cases"]},
            "product_acceptance": False, "schema": {"observed_stored_hashes": None,
                "limitation": "Readiness verifies compiled schema; stored checksums are not exposed by HTTP."},
            "limits": self.cases["limits"], "claims": self.cases["claims"]}
        self.tokens = {}
        self.http_calls = 0
        self.serial = 0
        self.port = None
        self.server = None
        self.database = None
        self.volumes = {}
        self.network = None
        self.operator_scopes = ["manage:grant", "manage:provider", "read:space_config", "write:space_config",
                                "read:status", "read:system", "write:system"]
        self.service_scopes = ["read:status", "read:space_config", "read:source", "write:source"]
        self.environment = {"ELEPHANT_HOST": "0.0.0.0", "ELEPHANT_PORT": "3000",
            "ELEPHANT_DURABILITY_PATH": RECALL_ROOT + "/recovery", "ELEPHANT_SNAPSHOT_DIR": RECALL_ROOT + "/backups",
            "JWT_SECRET_FILE": AUTH_ROOT + "/signing.key", "JWT_ALGORITHM": "HS256",
            "JWT_ISSUER": "container-offline", "JWT_AUDIENCE": "recall-container", "RUST_LOG": "warn"}

    def save(self, name, value):
        private_new(self.root / name, encoded(value))

    def case(self, name, action):
        require(name in self.report["cases"] and self.report["cases"][name]["verdict"] == "not_executed",
                "case must execute once")
        start = time.monotonic()
        row = self.report["cases"][name]
        row.update(started_monotonic=start, deadline_monotonic=self.commands.started + MAX_SECONDS)
        try:
            observations = action()
            row.update(verdict="pass", observations=observations)
        except BaseException as error:
            row.update(verdict="fail", error_type=type(error).__name__)
            raise
        finally:
            row["ended_monotonic"] = time.monotonic()
            self.save(f"case-{name}.json", row)

    def disk_check(self):
        for path in (self.root, Path(self.report["docker_root"])):
            info = os.statvfs(path)
            require(info.f_bavail * info.f_frsize >= MIN_FREE_BYTES, "free space below shared 250 GiB floor")

    def container(self, purpose, image, arguments, *, network=None, mounts=(), environment=None,
                  user=None, entrypoint=None, publish=False):
        extra = ["--pull=never", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
                 "--pids-limit=256", "--memory=4g", "--memory-swap=4g", "--cpus=2",
                 "--ulimit=core=0", "--log-driver=json-file", "--log-opt=max-size=1m", "--log-opt=max-file=2",
                 "--no-healthcheck", "--tmpfs=/tmp:rw,noexec,nosuid,size=64m", "--network", network or "none"]
        if user:
            extra += ["--user", user]
        if user == "0:0":
            # Initialization/cold-copy helpers see only owned volumes, no network.
            require(network is None, "root helper must be offline")
            extra += ["--cap-add=CHOWN", "--cap-add=DAC_OVERRIDE", "--cap-add=FOWNER"]
        if entrypoint:
            extra += ["--entrypoint", entrypoint]
        if image == self.allocation["recall_image_id"]:
            extra += ["--workdir", RECALL_ROOT + "/data"]
        for volume, target, readonly in mounts:
            require(any(kind == "volume" and identity == volume for kind, identity, _ in self.owned.resources),
                    "mount is not an owned volume")
            extra += ["--mount", f"type=volume,source={volume},target={target}" + (",readonly" if readonly else "")]
        for key, value in (environment or {}).items():
            require(re.fullmatch(r"[A-Z][A-Z0-9_]*", key), "invalid environment key")
            extra += ["--env", key + "=" + value]
        if publish:
            extra += ["--publish", "127.0.0.1::3000"]
        if purpose.startswith("database"):
            extra += ["--network-alias", "paired-store"]
        return self.owned.create("container", purpose, [*extra, image, *arguments])

    def recall_mounts(self, volumes=None, journal=None):
        volumes = volumes or self.volumes
        return [(volumes["data"], RECALL_ROOT + "/data", False),
                (journal or volumes["recovery"], RECALL_ROOT + "/recovery", False),
                (volumes["backups"], RECALL_ROOT + "/backups", False),
                (self.volumes["auth"], AUTH_ROOT, True)]

    def copy(self, container, path, maximum=MAX_COMMAND_BYTES):
        require(path.startswith("/") and ".." not in Path(path).parts, "absolute bounded copy path required")
        _, output, _ = self.commands.run(["container", "cp", container + ":" + path, "-"], secret=True,
                                        output_limit=min(MAX_COPY_BYTES, maximum + 1024 * 1024), timeout=60)
        return one_tar_file(output, maximum)

    def once(self, purpose, arguments, *, mounts=(), environment=None, network=None, user="10001:10001",
             entrypoint=None):
        self.serial += 1
        container = self.container(f"{purpose}-{self.serial}", self.allocation["recall_image_id"], arguments,
                                   mounts=mounts, environment=environment, network=network, user=user, entrypoint=entrypoint)
        self.commands.run(["start", container])
        _, output, _ = self.commands.run(["wait", container], timeout=90, secret=True)
        require(output.strip() == b"0", "owned one-shot container failed")
        return container

    def image_identities(self):
        socket_path = Path(self.allocation["docker_socket"])
        require(stat.S_ISSOCK(socket_path.stat().st_mode), "allocated Docker endpoint is not a local socket")
        info = self.commands.json(["info", "--format", "{{json .}}"], secret=True)
        require(info.get("OSType") == "linux", "Linux local Docker daemon required")
        root = Path(info["DockerRootDir"])
        require(root.is_absolute() and root.is_dir(), "local Docker root must be inspectable")
        self.report["docker_root"] = str(root)
        self.disk_check()
        result = {}
        for role in ("recall", "paired"):
            identity = self.allocation[role + "_image_id"]
            rows = self.commands.json(["image", "inspect", identity], secret=True)
            require(len(rows) == 1 and rows[0]["Id"] == identity, "supplied local image identity differs")
            row = rows[0]
            require(row["Os"] == "linux" and row["Architecture"] == "amd64", "wrong image target")
            config = row["Config"]
            require(not config.get("OnBuild") and not config.get("Volumes"), "image declares implicit actions or volumes")
            allowed_environment = {"PATH", "LANG", "LC_ALL", "DEBIAN_FRONTEND", "ELEPHANT_HOST", "ELEPHANT_PORT",
                                   "ELEPHANT_DURABILITY_PATH", "ELEPHANT_SNAPSHOT_DIR", "RUST_LOG",
                                   "PIPELINE_SKILLS_DIR", "FASTEMBED_CACHE_DIR", "SSL_CERT_FILE", "SSL_CERT_DIR"}
            require(all(entry.split("=", 1)[0] in allowed_environment for entry in config.get("Env", [])),
                    "image embeds an unreviewed environment value")
            expected_user = "10001:10001" if role == "recall" else "65532:65532"
            require(config.get("User") in (expected_user, expected_user.split(":")[0]), "wrong image runtime UID")
            require(not config.get("Entrypoint") if role == "recall"
                    else config.get("Entrypoint") == ["/usr/local/bin/recall-surreal"], "unexpected image entrypoint")
            container = self.container("inspect-" + role, identity, [], user=expected_user)
            binary_path = "/usr/local/bin/" + ("meerkat-recall" if role == "recall" else "recall-surreal")
            binary_hash = digest(self.copy(container, binary_path, MAX_COPY_BYTES - MAX_COMMAND_BYTES))
            require(binary_hash == self.allocation[role + "_binary_sha256"], "copied binary differs from trusted receipt")
            paths = {"recall_provenance": "/usr/share/meerkat-recall/provenance.json"} if role == "recall" else {
                "paired_source_provenance": "/usr/share/recall-surreal/RECALL_SOURCE_PROVENANCE.json",
                "paired_build_provenance": "/usr/share/recall-surreal/BUILD_PROVENANCE.json"}
            documents = {}
            for field, path in paths.items():
                raw = self.copy(container, path)
                require(digest(raw) == self.allocation[field + "_sha256"], "copied provenance differs from trusted receipt")
                documents[field] = strict_json(raw)
            if role == "recall":
                provenance = documents["recall_provenance"]
                for field in ("source_commit", "source_tree", "source_tar_sha256"):
                    require(provenance.get(field) == self.allocation[field], "Recall source provenance differs")
                labels = config.get("Labels", {})
                for label, field in (("org.opencontainers.image.revision", "source_commit"),
                                     ("org.meerkat.recall.source-tree", "source_tree"),
                                     ("org.meerkat.recall.source-tar-sha256", "source_tar_sha256")):
                    require(labels.get(label) == self.allocation[field], "Recall image label differs")
                require(provenance.get("binary_sha256") == binary_hash, "Recall binary/provenance mismatch")
            else:
                require(documents["paired_source_provenance"].get("recall_commit") == self.allocation["source_commit"],
                        "paired source belongs to another Recall revision")
                require(documents["paired_build_provenance"].get("binary_sha256") == binary_hash,
                        "paired binary/provenance mismatch")
            result[role] = {"image_id": identity, "repo_digests": row.get("RepoDigests", []),
                            "binary_sha256": binary_hash, "documents": documents}
        self.report["images"] = result
        return result

    def volumes_and_auth(self):
        self.network = self.owned.create("network", "primary-network")
        for purpose in ("data", "recovery", "backups", "auth", "database"):
            self.volumes[purpose] = self.owned.create("volume", purpose)
        # The image supplies owned Recall mountpoints; arbitrary auth/database
        # mountpoints need explicit root initialization of these exact NEW volumes.
        mounts = [(self.volumes[key], "/owned/" + key, False) for key in self.volumes]
        self.once("initialize-volumes", ["-c", "chown 10001:10001 /owned/data /owned/recovery /owned/backups /owned/auth && chmod 700 /owned/data /owned/recovery /owned/backups /owned/auth && chown 65532:65532 /owned/database && chmod 700 /owned/database"],
                  mounts=mounts, user="0:0", entrypoint="/bin/sh")
        auth_mounts = [(self.volumes["auth"], AUTH_ROOT, False)]
        self.once("auth-bootstrap", ["meerkat-recall", "service-token", "bootstrap", "--secret-file", AUTH_ROOT + "/signing.key"], mounts=auth_mounts)
        clearances = [{"policy_id": "default", "max_level": 0, "labels": [], "handling_allow": []}]
        # Constant script plus base64 of synthetic JSON; neither user input nor a
        # credential is interpolated as executable shell syntax.
        payload = base64.b64encode(encoded(clearances)).decode()
        self.once("auth-clearances", ["-c", "printf '%s' \"$1\" | base64 -d > " + AUTH_ROOT + "/clearances.json && chmod 600 " + AUTH_ROOT + "/clearances.json", "write-clearances", payload],
                  mounts=auth_mounts, entrypoint="/bin/sh")
        for role in ("operator", "service", "one-space"):
            arguments = ["meerkat-recall", "service-token", "issue", "--secret-file", AUTH_ROOT + "/signing.key",
                         "--subject", "container-" + role, "--expires-in", "1800", "--issuer", "container-offline",
                         "--audience", "recall-container",
                         "--output", AUTH_ROOT + "/" + role + ".token"]
            if role == "operator":
                arguments += ["--global-admin"]
            else:
                arguments += ["--clearances-file", AUTH_ROOT + "/clearances.json"]
                for space in self.cases["spaces"][:1 if role == "one-space" else 2]:
                    arguments += ["--space", space]
            for scope in self.operator_scopes if role == "operator" else self.service_scopes:
                arguments += ["--scope", scope]
            container = self.once("auth-issue", arguments, mounts=auth_mounts)
            token = self.copy(container, AUTH_ROOT + "/" + role + ".token", 16384).decode().strip()
            require(token.count(".") == 2 and len(token) < 16384, "CLI did not issue a bounded JWT")
            self.tokens[role] = token
            self.commands.secrets.append(token)
        return {"network": self.network, "volumes": self.volumes, "credential_roles": sorted(self.tokens),
                "ambient_credentials": False, "owned_nonce": self.owned.nonce}

    def launch_database(self, network, volume, purpose):
        password = "synthetic-" + self.owned.nonce
        self.commands.secrets.append(password)
        container = self.container(purpose, self.allocation["paired_image_id"],
            ["start", "--bind", "0.0.0.0:8000", "file:/data/database.db"], network=network,
            mounts=[(volume, "/data", False)], environment={"SURREAL_USER": "qualification", "SURREAL_PASS": password})
        self.commands.run(["start", container])
        self.environment.update(ELEPHANT_DB="surreal-db", SURREALDB_URL="ws://paired-store:8000",
                                SURREALDB_NAMESPACE="offline", SURREALDB_DATABASE="fixture",
                                SURREALDB_USER="qualification", SURREALDB_PASS=password)
        self.wait_database(network)
        return container

    def wait_database(self, network=None):
        self.once("database-health", ["curl", "--fail", "--silent", "--retry", "20", "--retry-connrefused",
            "--retry-delay", "1", "--max-time", "2", "http://paired-store:8000/health"], network=network or self.network)

    def launch(self, *, server_only=True, volumes=None, journal=None, network=None):
        self.disk_check()
        self.serial += 1
        arguments = ["meerkat-recall", "serve", "--shutdown-timeout-seconds", "60"]
        if server_only:
            arguments.append("--server-only")
        self.server = self.container(f"recall-{self.serial}", self.allocation["recall_image_id"], arguments,
            network=network or self.network, mounts=self.recall_mounts(volumes, journal),
            environment=self.environment, publish=True)
        self.commands.run(["start", self.server])
        row = self.owned.inspect("container", self.server)
        ports = row.get("NetworkSettings", {}).get("Ports", {}).get("3000/tcp", [])
        require(len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1", "service listener is not unique loopback")
        self.port = int(ports[0]["HostPort"])
        require(1024 <= self.port <= 65535, "invalid assigned service port")
        return self.server

    def stop(self, container=None, *, kill=False):
        identity = container or self.server
        require(any(kind == "container" and item == identity for kind, item, _ in self.owned.resources),
                "cannot stop an unowned container")
        if kill:
            self.commands.run(["kill", "--signal=KILL", identity])
        else:
            self.commands.run(["stop", "--time=65", identity], timeout=70)
        row = self.owned.inspect("container", identity)
        require(row["State"]["Running"] is False, "owned process is still running")
        if not kill:
            require(row["State"]["ExitCode"] == 0, "owned service failed graceful shutdown")
        return row["State"]

    def http(self, method, path, role=None, space=None, body=None, *, mcp=False):
        if role == "operator" and space is not None:
            require(path == "/v1/spaces" or re.fullmatch(r"/v1/spaces/[^/]+(?:/grants/[^/]+)?", path),
                    "global operator cannot select an ordinary data space")
        self.http_calls += 1
        require(self.http_calls <= MAX_HTTP_CALLS, "HTTP call bound exceeded")
        timeout = min(90 if path.startswith("/v1/system/snapshot/") else 10, self.commands.remaining())
        headers = {"Accept": "application/json, text/event-stream" if mcp else "application/json"}
        if role:
            headers["Authorization"] = "Bearer " + self.tokens[role]
        if space:
            headers["X-Space-Id"] = space
        if body is not None:
            headers["Content-Type"] = "application/json"
        started = time.monotonic()
        raw = b""
        status = None
        complete = False
        error_type = None
        framing = {}
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            with self.deadline(timeout):
                connection.request(method, path, None if body is None else encoded(body), headers)
                response = connection.getresponse()
                status = response.status
                lengths = response.headers.get_all("Content-Length", [])
                transfers = response.headers.get_all("Transfer-Encoding", [])
                framing = {"content_length": lengths, "transfer_encoding": transfers}
                while len(raw) <= MAX_HTTP_BYTES:
                    chunk = response.read1(min(8192, MAX_HTTP_BYTES + 1 - len(raw)))
                    if not chunk:
                        break
                    raw += chunk
                require(len(raw) <= MAX_HTTP_BYTES, "HTTP body exceeds bound")
                require(not lengths or (len(lengths) == 1 and lengths[0].isdigit() and int(lengths[0]) == len(raw)),
                        "HTTP length is incomplete or ambiguous")
                require(not transfers or (not lengths and transfers == ["chunked"]), "HTTP framing is ambiguous")
                complete = True
                value = self.mcp_value(raw, body["id"]) if mcp and status == 200 else strict_json(raw)
                return {"status": status, "body": value}
        except BaseException as error:
            error_type = type(error).__name__
            raise
        finally:
            connection.close()
            self.save(f"http-{self.http_calls:03d}.json", {"method": method, "path": path, "space": space,
                "credential_role": role, "request": body, "status": status,
                "raw_body_base64": base64.b64encode(raw[:MAX_HTTP_BYTES]).decode(), "body_sha256": digest(raw[:MAX_HTTP_BYTES]),
                "raw_body_complete": complete, "transport_error_type": error_type, "framing": framing,
                "started_monotonic": started, "ended_monotonic": time.monotonic(), "deadline_monotonic": started + timeout})

    def initialize_mcp(self):
        request_id = self.http_calls + 1
        result = self.http("POST", "/mcp/full", "operator", body={"jsonrpc": "2.0", "id": request_id,
            "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "container-offline", "version": "1"}}}, mcp=True)
        require(result["status"] == 200 and "result" in result["body"] and "error" not in result["body"],
                "MCP initialization failed")

    def tool(self, name, arguments):
        request_id = self.http_calls + 1
        result = self.http("POST", "/mcp/full", "operator", body={"jsonrpc": "2.0", "id": request_id,
            "method": "tools/call", "params": {"name": name, "arguments": arguments}}, mcp=True)
        require(result["status"] == 200 and "error" not in result["body"], "MCP tool transport failed")
        content = result["body"]["result"]
        require(content.get("isError") is not True, "MCP tool returned a failure")
        texts = [block["text"] for block in content.get("content", []) if block.get("type") == "text"]
        require(len(texts) == 1, "single typed MCP tool result required")
        value = strict_json(texts[0])
        return value["data"] if isinstance(value, dict) and value.get("ok") is True else value

    def wait_ready(self, *, configured=True, seconds=60):
        end = time.monotonic() + min(seconds, self.commands.remaining())
        samples = []
        while time.monotonic() < end:
            try:
                result = self.http("GET", "/v1/readiness", "operator")
                samples.append({"capture": self.http_calls, "status": result["status"], "body": result["body"]})
                body = result["body"]
                if not configured and body.get("configured") is False and body.get("ready") is False:
                    return samples
                if result["status"] == 200 and body.get("ready") is True and body.get("storage_ready") is True:
                    require(body.get("recovery_ready") is True and body.get("draining") is False,
                            "readiness lacks completed recovery barrier")
                    return samples
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(min(0.25, self.commands.remaining()))
        raise ValueError("runtime did not reach expected readiness before bound")

    def intended_space(self, space):
        ordinal = self.cases["spaces"].index(space)
        # The actual global bootstrap CLI has no data clearances. Existing
        # create/grant routes still enforce record ABAC, so this fixture uses
        # their supported default level-zero administration envelope.
        return {"name": space, "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                "embedding_provider": "openai", "embedding_model": "offline-unapproved-" + space,
                "embedding_dims": 8 + 4 * ordinal, "purpose": "synthetic metadata " + space,
                "skill": "offline-" + space, "processing": {"generation": 1, "index_generation": 0,
                    "policy_id": space, "connections": {}, "profiles": {}, "bindings": {},
                    "active_embedding_profile_id": None, "pending_reindex": None}}

    def configuration(self, space):
        # The retained route is /v1/spaces/{space}; there is no /config suffix.
        result = self.http("GET", f"/v1/spaces/{space}", "service", space)
        require(result["status"] == 200, "persisted named-space configuration unavailable")
        return stable_space_configuration(result["body"], self.intended_space(space))

    def verify_configurations(self):
        observed = {space: self.configuration(space) for space in self.cases["spaces"]}
        require(observed == self.configurations, "persisted named-space baseline changed across lifecycle")
        return observed

    def bootstrap_and_spaces(self):
        if self.allocation["backend"] == "surreal-db":
            self.database = self.launch_database(self.network, self.volumes["database"], "database-primary")
            # A TCP connection from the Recall CLI has bounded retries in storage.
            # Migration is the one documented explicit fresh-schema operation.
            self.once("fresh-migrate", ["meerkat-recall", "migrate"], mounts=self.recall_mounts(),
                      environment=self.environment, network=self.network)
        self.launch()
        if self.allocation["backend"] == "elephant-db":
            self.wait_ready(configured=False)
            self.initialize_mcp()
            value = self.tool("initialize_runtime", {"backend": "elephant-db", "ingest_enabled": False,
                "extractor_required": False, "snapshot_path": RECALL_ROOT + "/data/state.snapshot"})
        else:
            self.wait_ready()
            self.initialize_mcp()
            value = self.tool("configure_runtime", {"ingest_enabled": False, "extractor_required": False})
        require(value.get("configured") is True and value.get("storage_ready") is True,
                "explicit runtime initialization failed")
        require(value.get("backend") == self.allocation["backend"], "initialized backend differs")
        self.report["runtime_initialized"] = value
        samples = self.wait_ready()
        self.report["schema"]["verification_readiness_capture"] = samples[-1]["capture"]
        for space in self.cases["spaces"]:
            result = self.http("POST", "/v1/spaces", "operator", space, self.intended_space(space))
            require(result["status"] == 201, "actual named-space creation failed")
            grant = self.http("PUT", f"/v1/spaces/{space}/grants/container-service", "operator", space,
                              {"active": True, "scopes": self.service_scopes, "expected_generation": None})
            require(grant["status"] == 200, "actual current grant creation failed")
        self.configurations = {space: self.configuration(space) for space in self.cases["spaces"]}
        require(len({row["id"] for row in self.configurations.values()}) == 2, "named configs share a physical identity")
        first, second = self.cases["spaces"]
        grant = self.http("PUT", f"/v1/spaces/{first}/grants/container-one-space", "operator", first,
                          {"active": True, "scopes": self.service_scopes, "expected_generation": None})
        require(grant["status"] == 200, "single-space positive grant missing")
        require(self.http("GET", f"/v1/spaces/{first}/status", "one-space", first)["status"] == 200,
                "same single-space credential failed own-space positive control")
        denials = [(self.http("GET", f"/v1/spaces/{first}/status"), 401, "unauthorized"),
                   (self.http("GET", f"/v1/spaces/{second}/status", "one-space", second), 403, "forbidden_space"),
                   (self.http("GET", f"/v1/spaces/{second}/status", "service", first), 403, "forbidden_space")]
        for result, status, code in denials:
            require(result["status"] == status, "space/auth negative was admitted")
            self.typed_denial(result["body"], code, self.tokens.values())
        return {"readiness_samples": samples, "spaces": self.cases["spaces"], "negative_statuses": [x[1] for x in denials],
                "persisted_configurations": self.configurations}

    @staticmethod
    def source_path(space, key):
        return f"/v1/spaces/{urllib.parse.quote(space, safe='')}/sources/{urllib.parse.quote(key, safe='')}"

    def accept(self, space, key, revision, generation=None):
        intake = {"revision_id": revision, "idempotency_key": revision + "-" + key}
        if generation is not None:
            intake["expected_generation"] = generation
        result = self.http("POST", "/v1/sources/ingest", "service", space,
            {"name": key, "source_type": "manual", "ingest_mode": "metadata", "intake": intake})
        require(result["status"] == 202, "actual Metadata acceptance failed")
        body = result["body"]
        operation = body["operation"]
        require(operation.get("space_id") == space and operation.get("desired_revision") == revision,
                "acceptance belongs to another space/revision")
        require(operation.get("state") in ("accepted", "processing") and operation.get("completed_at") is None,
                "acceptance substituted a completed receipt")
        require(operation.get("source_id") == body.get("source_id") and body.get("doc_id") is None,
                "metadata-only source identity differs or acquired a document")
        for field in ("operation_id", "source_generation", "source_id"):
            require(isinstance(operation.get(field), str) and operation[field], "acceptance lacks durable identity")
        return body

    def head(self, space, key):
        result = self.http("GET", self.source_path(space, key) + "/head", "service", space)
        require(result["status"] == 200, "source head unavailable")
        return result["body"]

    def operation(self, space, operation_id):
        result = self.http("GET", "/v1/operations/" + urllib.parse.quote(operation_id, safe=""), "service", space)
        require(result["status"] == 200, "original operation unavailable")
        return result["body"]

    def published(self, space, key, accepted):
        end = time.monotonic() + min(120, self.commands.remaining())
        while time.monotonic() < end:
            operation = self.operation(space, accepted["operation"]["operation_id"])
            require(operation.get("state") not in ("failed_final", "failed_retryable"), "Metadata operation failed")
            if operation.get("state") == "complete":
                operation_matches(accepted, operation, complete=True)
                head = self.head(space, key)
                require(all(head.get(field) == operation.get(field) for field in
                    ("operation_id", "internal_source_id", "generation", "desired_revision", "published_revision", "state", "lifecycle", "cleanup_state")),
                    "completed operation and current published head disagree")
                source = self.http("GET", "/v1/sources/" + urllib.parse.quote(accepted["source_id"], safe=""), "service", space)
                require(source["status"] == 200 and source["body"].get("current_ingest_mode") == "metadata"
                        and source["body"].get("lifecycle_status") == "metadata_ready",
                        "completed receipt lacks required actual Metadata effect")
                return {"head": head, "operation": operation, "source": source["body"]}
            time.sleep(min(0.5, self.commands.remaining()))
        raise ValueError("Metadata did not publish within bounded worker deadline")

    def journal_identity(self, container=None):
        container = container or self.server
        result = {}
        for name in ("journal-id", "reference-key-fingerprint"):
            raw = self.copy(container, RECALL_ROOT + "/recovery/" + name, 512)
            value = raw.decode().strip()
            require(bool(value) and len(value) <= 256, "original journal identity missing")
            result[name] = value
        require(HEX64.fullmatch(result["reference-key-fingerprint"]), "journal fingerprint malformed")
        uuid.UUID(result["journal-id"])
        return result

    def pending_restart(self):
        self.accepted = {space: self.accept(space, self.cases["source_key"], "metadata-v1") for space in self.cases["spaces"]}
        require(len({entry["source_id"] for entry in self.accepted.values()}) == 2, "same external key merged named spaces")
        before = {}
        for space, accepted in self.accepted.items():
            operation = self.operation(space, accepted["operation"]["operation_id"])
            operation_matches(accepted, operation)
            before[space] = {"head": self.head(space, self.cases["source_key"]), "operation": operation}
        self.original_journal = self.journal_identity()
        killed = self.stop(kill=True)
        if self.database:
            self.stop(self.database)
            self.commands.run(["start", self.database])
            self.wait_database()
        self.launch()
        readiness = self.wait_ready()
        for space, accepted in self.accepted.items():
            operation = self.operation(space, accepted["operation"]["operation_id"])
            operation_matches(accepted, operation)
            require(operation == before[space]["operation"] and self.head(space, self.cases["source_key"]) == before[space]["head"],
                    "pending acknowledged state changed across restart")
        require(self.journal_identity() == self.original_journal, "original journal changed across reopen")
        self.verify_configurations()
        worker = self.tool("configure_runtime", {"ingest_enabled": True})
        require(worker.get("worker_ready") is True, "actual worker did not start")
        self.baseline = {space: self.published(space, self.cases["source_key"], accepted) for space, accepted in self.accepted.items()}
        return {"accepted": self.accepted, "before_restart": before, "completed": self.baseline,
                "kill_state": killed, "original_journal": self.original_journal, "first_reopen_readiness": readiness}

    def cold_inventory(self, volumes):
        mounts = [(volume, "/inventory/" + key, True) for key, volume in volumes.items()]
        container = self.once("cold-inventory", ["-c", "find /inventory -type f -exec sha256sum {} + | LC_ALL=C sort"],
                              mounts=mounts, user="0:0", entrypoint="/bin/sh")
        _, raw, _ = self.commands.run(["logs", container])
        return {"sha256": digest(raw), "files": raw.decode()}

    def clone_volume(self, key):
        clone = self.owned.create("volume", "clone-" + key)
        self.once("cold-copy", ["-c", "cp -a /original/. /clone/"],
                  mounts=[(self.volumes[key], "/original", True), (clone, "/clone", False)],
                  user="0:0", entrypoint="/bin/sh")
        return clone

    def missing_journal_clone(self):
        self.stop()
        if self.database:
            self.stop(self.database)
        original_container = self.server
        original_paths = {key: self.volumes[key] for key in ("data", "database")}
        before = self.cold_inventory(original_paths)
        clones = {"data": self.clone_volume("data"), "backups": self.owned.create("volume", "clone-backups"),
                  "recovery": self.owned.create("volume", "clone-missing-journal")}
        cloned_database = self.clone_volume("database") if self.database else None
        network = self.owned.create("network", "clone-network")
        clone_db = self.launch_database(network, cloned_database, "database-clone") if cloned_database else None
        self.launch(volumes=clones, network=network)
        samples = []
        end = time.monotonic() + min(15, self.commands.remaining())
        while time.monotonic() < end:
            state = self.owned.inspect("container", self.server)["State"]
            if state["Running"] is False:
                require(state["ExitCode"] != 0, "missing-journal clone exited successfully")
                samples.append({"state": state})
                break
            try:
                result = self.http("GET", "/v1/readiness", "operator")
                require(result["status"] != 200 and result["body"].get("ready") is False,
                        "established cloned store became Ready without its original journal")
                samples.append({"capture": self.http_calls, "readiness": result})
            except (OSError, http.client.HTTPException):
                samples.append({"capture": self.http_calls, "connection_unavailable": True})
            time.sleep(min(0.5, self.commands.remaining()))
        require(any("state" in sample or "readiness" in sample for sample in samples),
                "connection failures alone cannot prove missing-journal fail-closed")
        state = self.owned.inspect("container", self.server)["State"]
        if state["Running"]:
            self.stop(kill=True)
        # Logs are bounded by the container log quota and remain private.
        _, stdout, stderr = self.commands.run(["logs", "--tail", "200", self.server], check=False)
        require_missing_journal_reason(self.allocation["backend"], stdout + stderr)
        if clone_db:
            self.stop(clone_db)
        require(self.cold_inventory(original_paths) == before, "negative candidate altered the original stopped data")
        require(self.journal_identity(original_container) == self.original_journal, "negative candidate altered original journal identity")
        if self.database:
            self.commands.run(["start", self.database])
            self.wait_database()
        self.launch(server_only=False)
        readiness = self.wait_ready()
        for space, accepted in self.accepted.items():
            require(self.published(space, self.cases["source_key"], accepted) == self.baseline[space],
                    "original positive recovery changed completed Metadata state")
        configurations = self.verify_configurations()
        return {"persisted_configurations": configurations, "original_volumes": original_paths, "clone_volumes": clones, "clone_database": cloned_database,
                "original_cold_inventory": before, "negative_samples": samples, "positive_readiness_samples": readiness,
                "original_journal": self.original_journal, "observation_window_seconds": 15}

    def backup(self, name, seconds):
        path = RECALL_ROOT + "/backups/" + name
        result = self.http("POST", "/v1/system/snapshot/export", "operator", body={"path": path,
            "name": name, "retention_seconds": seconds, "quiesce_timeout_seconds": 30})
        require(result["status"] == 200 and result["body"].get("success") is True, "actual registered snapshot export failed")
        manifest = result["body"]["manifest"]
        require(manifest["journal_id"] == self.original_journal["journal-id"], "backup names another original journal")
        require(0 < (utc(manifest["expires_at"]) - utc(manifest["created_at"])).total_seconds() <= seconds,
                "backup retention differs from requested bound")
        require(manifest["bytes"] > 0 and HEX64.fullmatch(manifest["sha256"]), "backup lacks actual payload identity")
        copied = strict_json(self.copy(self.server, path + "/manifest.json"))
        require(copied == manifest, "actual registered manifest bytes differ from HTTP receipt")
        require(type(manifest["bytes"]) is int and 0 < manifest["bytes"] <= MAX_COPY_BYTES - MAX_COMMAND_BYTES,
                "backup payload exceeds offline copy allocation")
        payload = self.copy(self.server, path + "/data.snapshot", manifest["bytes"])
        verify_backup_payload(manifest, payload, self.allocation["backend"], self.original_journal["journal-id"])
        return {"path": path, "manifest": manifest}

    def restore(self, backup):
        result = self.http("POST", "/v1/system/snapshot/import", "operator", body={"path": backup["path"]})
        require(result["status"] == 200 and result["body"].get("success") is True, "actual snapshot restore failed")
        return self.wait_ready()

    def clean_restore(self):
        self.original_backup = self.backup("baseline", 1200)
        replacements = {}
        for space in self.cases["spaces"]:
            accepted = self.accept(space, self.cases["source_key"], "metadata-v2", self.baseline[space]["head"]["generation"])
            replacements[space] = self.published(space, self.cases["source_key"], accepted)
        canary_key = "synthetic/post-backup-only"
        canary_space = self.cases["spaces"][0]
        canary = self.accept(canary_space, canary_key, "canary-v1")
        self.published(canary_space, canary_key, canary)
        readiness = self.restore(self.original_backup)
        for space, accepted in self.accepted.items():
            require(self.published(space, self.cases["source_key"], accepted) == self.baseline[space],
                    "restore merged state or changed original baseline")
        missing = self.http("GET", self.source_path(canary_space, canary_key) + "/head", "service", canary_space)
        require(missing["status"] == 404, "clean restore retained post-backup-only source")
        require(self.journal_identity() == self.original_journal, "restore replaced original journal")
        configurations = self.verify_configurations()
        return {"persisted_configurations": configurations, "backup": self.original_backup, "replacement": replacements, "canary_acceptance": canary,
                "first_observed_readiness": readiness, "canary_absent_capture": self.http_calls}

    def erase_restore_replay(self):
        first, second = self.cases["spaces"]
        result = self.http("POST", self.source_path(first, self.cases["source_key"]) + ":erase", "service", first,
                           {"expected_generation": self.baseline[first]["head"]["generation"]})
        require(result["status"] == 202, "actual public source erasure was not accepted")
        receipt = result["body"]
        end = time.monotonic() + min(90, self.commands.remaining())
        operation = None
        while time.monotonic() < end:
            operation = self.operation(first, receipt["operation_id"])
            if operation.get("state") == "complete":
                break
            require(operation.get("state") in ("pending", "running"), "erasure operation failed")
            time.sleep(min(0.5, self.commands.remaining()))
        require(operation is not None and operation.get("state") == "complete" and operation.get("lifecycle") == "erased"
                and operation.get("cleanup_state") == "complete", "erasure cleanup never completed")
        before = self.http("GET", self.source_path(first, self.cases["source_key"]) + "/head", "service", first)
        require(before["status"] == 404, "erased source remains publicly available")
        readiness = self.restore(self.original_backup)
        # This is the first observed Ready after synchronous restore. These
        # finite samples do not establish a continuous blackout interval.
        after = self.http("GET", self.source_path(first, self.cases["source_key"]) + "/head", "service", first)
        require(after["status"] == 404, "old backup resurrected erased source after observed Ready")
        unaffected = self.published(second, self.cases["source_key"], self.accepted[second])
        require(unaffected == self.baseline[second], "replay altered independent named-space state")
        require(self.journal_identity() == self.original_journal, "replay lost original journal")
        configurations = self.verify_configurations()
        return {"persisted_configurations": configurations, "accepted_erasure": receipt, "completed_erasure": operation, "first_observed_readiness": readiness,
                "other_space_before": self.baseline[second], "other_space_after": unaffected,
                "scope": "public anti-resurrection replay; not an independent physical source inventory or OB3 verdict"}

    def retention(self):
        backup = self.backup("short-retention", 5)
        # Actual wall time and actual manifest deadline, never mutate a clock or receipt.
        expiry = utc(backup["manifest"]["expires_at"])
        while dt.datetime.now(dt.timezone.utc) <= expiry:
            time.sleep(min(0.25, self.commands.remaining()))
        self.stop()
        if self.database:
            self.stop(self.database)
            self.commands.run(["start", self.database])
            self.wait_database()
        self.launch(server_only=False)
        readiness = self.wait_ready()
        path = backup["path"]
        # Test the two exact registered payload files, not queue state or a 404.
        require(dt.datetime.now(dt.timezone.utc) > expiry, "retention deadline not actually reached")
        self.commands.run(["exec", self.server, "/bin/sh", "-c", "test ! -e \"$1/manifest.json\" && test ! -e \"$1/data.snapshot\"", "expired-backup", path])
        refused = self.http("POST", "/v1/system/snapshot/import", "operator", body={"path": path})
        require(400 <= refused["status"] < 600 and refused["body"].get("success") is not True,
                "expired/deleted backup restore was admitted")
        configurations = self.verify_configurations()
        return {"persisted_configurations": configurations, "backup": backup, "observed_expired_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "physical_files_absent": [path + "/manifest.json", path + "/data.snapshot"],
                "readiness_samples": readiness, "restore_refusal": refused}

    def execute(self):
        flow = [("immutable_images", self.image_identities), ("owned_topology_auth", self.volumes_and_auth),
                ("two_spaces_auth", self.bootstrap_and_spaces), ("pending_metadata_restart", self.pending_restart),
                ("cold_clone_missing_journal", self.missing_journal_clone), ("clean_registered_restore", self.clean_restore),
                ("original_journal_replay", self.erase_restore_replay), ("bounded_backup_retention", self.retention)]
        require([name for name, _ in flow] == self.cases["required_cases"], "driver/case inventory differs")
        for name, action in flow:
            self.case(name, action)
        require(self.journal_identity() == self.original_journal, "final original journal identity changed")

    def finish(self, failure=None):
        cleanup_errors = self.owned.cleanup()
        self.report.update(failure_type=failure, cleanup_errors=cleanup_errors,
            owned_resources=[{"kind": kind, "identity": identity, "purpose": purpose}
                             for kind, identity, purpose in self.owned.resources],
            duration_seconds=time.monotonic() - self.commands.started, commands=self.commands.commands, http_calls=self.http_calls)
        self.report["product_acceptance"] = (failure is None and not cleanup_errors
            and all(row["verdict"] == "pass" for row in self.report["cases"].values()))
        self.report["acceptance_scope"] = "this backend and reviewed eight cases only; independent D review remains authoritative"
        self.save("observations.json", self.report)
        return self.report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("plan", help="render inert case inventory; never access Docker")
    run = commands.add_parser("run", help="execute a centrally allocated, independently reviewed local-image run")
    run.add_argument("--allocation", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.action == "plan":
        print(encoded(plan()).decode(), end="")
        return 0
    allocation, inventory = validate_allocation(args.allocation)
    directory = Path(allocation["run_directory"])
    directory.mkdir(mode=0o700, parents=False)
    driver = None
    failure = None
    cancellation = WorkCancellation()
    prior_term = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, cancellation.request)
    try:
        commands = DockerCommands(allocation, directory, cancellation=cancellation)
        driver = ContainerDriver(allocation, inventory, commands=commands)
        driver.execute()
        cancellation.check()
    except BaseException as error:
        failure = type(error).__name__
        if driver is None:
            private_new(directory / "startup-failure.json", encoded({"failure_type": failure, "product_acceptance": False}))
    finally:
        # The handler only records a flag, including during this transition.
        # Repeated signals cannot unwind exact-label cleanup or its own timer.
        cancellation.begin_cleanup()
        try:
            if driver is not None:
                report = driver.finish(failure)
            else:
                report = {"product_acceptance": False}
        finally:
            signal.signal(signal.SIGTERM, prior_term)
    print(encoded({"report_directory": str(directory), "product_acceptance": report["product_acceptance"]}).decode(), end="")
    return 0 if report["product_acceptance"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
