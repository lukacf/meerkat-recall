#!/usr/bin/env python3
"""Fail-closed qualification launcher; never a public product service.

Requires a separately reviewed run manifest, current spending approval, packaged
runtime, and explicit credential FD. It never resolves ambient provider secrets.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import stat
import subprocess
import sys
import threading

from capture import Capture, write_new
from cohort_profile import SEMANTIC140
from ledger import Ledger, TIME_CAP, COUNT_CAP, MONEY_CAP, sync_directory
import launch_context
from policy import Policy, private_read
from server import GatewayServer
from transport import Forwarder, PinnedTLS
from wire import Denied, digest, encode, fields, require, strict_json, text


PACKAGE_METADATA_CAP = 32 * 1024**2
LAUNCH_STAGE = "arguments"


def mark_stage(name: str) -> None:
    global LAUNCH_STAGE
    LAUNCH_STAGE = name


def runtime_package_metadata(raw: bytes) -> dict:
    """Parse bounded artifact metadata without widening provider request limits."""
    require(type(raw) is bytes and len(raw) <= PACKAGE_METADATA_CAP,
            "runtime_package_provenance_limit")
    package = launch_context.strict_json(raw)
    require(type(package) is dict, "runtime_package_object_required")
    launch_context.encode(package)  # Reject unpaired surrogates/nonfinite values.
    return package


def failure_diagnostic(error: Exception, run_directory: Path) -> dict:
    """Record only source stage, exception class and bounded source locations."""
    require(run_directory.is_absolute()
            and run_directory.parent.resolve() == run_directory.parent,
            "diagnostic_destination_not_canonical")
    source = Path(__file__).resolve().parent
    frames = []
    traceback = error.__traceback__
    while traceback is not None:
        code = traceback.tb_frame.f_code
        try:
            relative = Path(code.co_filename).resolve().relative_to(source)
        except ValueError:
            pass
        else:
            frames.append({"file": relative.as_posix(), "function": code.co_name,
                           "line": traceback.tb_lineno})
            frames = frames[-16:]
        traceback = traceback.tb_next
    value = {"format": "recall-gateway-launch-failure/v1", "state": "failed",
             "stage": LAUNCH_STAGE, "exception_type": type(error).__name__,
             "source_frames": frames, "exception_message_recorded": False,
             "locals_or_payloads_recorded": False, "semantic_acceptance": False}
    raw = encode(value) + b"\n"
    require(len(raw) <= 16384, "diagnostic_byte_limit")
    path = run_directory.with_name(run_directory.name + ".failure.json")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(path.parent)
    return {"path": str(path), "sha256": digest(raw), "bytes": len(raw)}


def file_hash(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def tree_hash(root: Path) -> str:
    """Packaging must dereference symlinks; no host links/sockets/device mounts."""
    require(root.is_dir() and not root.is_symlink(), "unsafe_runtime_root")
    entries = []
    total = 0
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode), "unsupported_runtime_entry")
        relative = path.relative_to(root).as_posix()
        require(not info.st_mode & (stat.S_ISUID | stat.S_ISGID | 0o022), "unsafe_runtime_mode")
        total += info.st_size if path.is_file() else 0
        require(total <= 2 * 1024**3 and len(entries) < 100000, "runtime_size_limit")
        entries.append([relative, stat.S_IMODE(info.st_mode), file_hash(path) if path.is_file() else None])
    return digest(encode(entries))


def launch_command(runtime: Path, work: Path, gateway_socket: Path, bridge: Path,
                   command: list[str], environment: dict[str, str], anchor: Path,
                   inputs: Path | None = None, accounting: Path | None = None,
                   accounting_sidecar: Path | None = None) -> list[str]:
    bwrap = shutil.which("bwrap")
    require(bwrap is not None, "bubblewrap_required")
    args = [bwrap, "--unshare-all", "--new-session", "--die-with-parent", "--clearenv",
            "--ro-bind", str(runtime), "/", "--proc", "/proc", "--dev", "/dev",
            "--tmpfs", "/tmp", "--tmpfs", "/home", "--dir", "/home/worker",
            "--bind", str(work), "/work", "--ro-bind", str(gateway_socket), "/gateway.sock",
            "--ro-bind", str(bridge), "/bridge.py",
            "--ro-bind", str(bridge.parent / "launch_context.py"), "/launch_context.py",
            "--ro-bind", str(anchor), "/launch-anchor.json",
            "--ro-bind", str(anchor.parent / "manifest.json"), "/gateway-manifest.json",
            "--ro-bind", str(anchor.parent / "launch.json"), "/gateway-launch.json",
            "--chdir", "/work",
            "--setenv", "HOME", "/home/worker", "--setenv", "PATH", "/usr/bin:/bin",
            "--setenv", "PYTHONDONTWRITEBYTECODE", "1"]
    if inputs is not None:
        args += ["--ro-bind", str(inputs), "/inputs"]
    if accounting is not None:
        require(accounting_sidecar is not None, "accounting_launch_sidecar_required")
        args += ["--ro-bind", str(accounting), "/gateway-accounting",
                 "--ro-bind", str(accounting_sidecar), "/gateway-accounting-channel.json",
                 "--ro-bind", str(bridge.parent / "accounting_channel.py"), "/accounting_channel.py"]
    else:
        require(accounting_sidecar is None, "accounting_launch_directory_required")
    for key, value in sorted(environment.items()):
        args += ["--setenv", key, value]
    args += ["--", "/usr/bin/python3", "-I", "/bridge.py", os.readlink("/proc/self/ns/net"), *command]
    return args


def verify_original_launch_inputs(directory: Path, manifest_hash: str, spec_raw: bytes) -> None:
    """Recheck the exact saved originals before credential access on create/resume."""
    require(digest(private_read(directory / "manifest.json")) == manifest_hash, "run_anchor_mismatch")
    require(private_read(directory / "launch.json") == spec_raw, "run_launch_mismatch")


def reviewed_runtime_directory(runtime: Path, value: str) -> None:
    """A declared asset directory must be inside the immutable runtime mount."""
    path = PurePosixPath(value)
    require(path.is_absolute() and str(path) == value and ".." not in path.parts
            and value.startswith("/opt/recall/"), "unsafe_runtime_asset_path")
    target = runtime.joinpath(*path.parts[1:])
    require(target.is_dir() and not target.is_symlink() and target.resolve() == target, "missing_runtime_asset_directory")


def original_launch_anchor(policy, ledger, runtime, spec, source, *, cohort):
    mark_stage("anchor_metadata")
    package_path = runtime / "opt/recall/share/runtime-package.json"
    with package_path.open("rb") as stream:
        raw = stream.read(PACKAGE_METADATA_CAP + 1)
    package = runtime_package_metadata(raw)
    require(package.get("format") == "recall-gateway-runtime-provenance/v1"
            and package.get("status") == "packaged_unqualified", "runtime_package_identity")
    require(package["binaries"]["recall"] == policy.manifest["artifact_sha256"], "runtime_package_artifact_identity")
    paired = file_hash(runtime / "opt/recall/bin/recall-surreal")
    require(paired == package["binaries"]["paired"], "runtime_package_paired_identity")
    mark_stage("anchor_clock")
    with ledger.transaction():
        ledger.check_time()
        row = ledger.db.execute("SELECT wall_start,mono_start,boot FROM run").fetchone()
    require(row is not None, "missing_original_run_clock")
    if cohort:
        require(policy.profile == ledger.profile, "cohort_launch_profile_mismatch")
        count_cap, money_cap, time_cap = (policy.profile.count_cap,
                                        policy.profile.money_cap, policy.profile.time_cap)
    else:
        count_cap, money_cap, time_cap = COUNT_CAP, MONEY_CAP, TIME_CAP
    manifest = policy.manifest
    anchor = {
        "format": launch_context.ANCHOR_FORMAT, "run_nonce": manifest["run_id"],
        "manifest_sha256": policy.manifest_hash, "proposal_sha256": manifest.get("proposal_sha256", manifest.get("allocation_sha256")),
        "corpus_sha256": manifest["corpus_sha256"], "allocation_sha256": manifest.get("allocation_sha256"),
        "input_inventory_sha256": spec.get("inputs", {}).get("inventory_sha256"),
        "source_commit": package["artifact_source_commit"], "artifact_sha256": manifest["artifact_sha256"],
        "paired_artifact_sha256": paired, "runtime_sha256": manifest["runtime_sha256"],
        "configuration_sha256": manifest["configuration_sha256"], "gateway_sha256": manifest["gateway_sha256"],
        "bindings_sha256": digest(encode(manifest["bindings"])),
        "bridge_sha256": file_hash(source / "bridge.py"), "context_helper_sha256": file_hash(source / "launch_context.py"),
        "command_sha256": digest(encode(spec["command"])),
        "clock": {"wall_start": row[0], "mono_start": row[1], "boot_id": row[2],
                  "deadline_wall": row[0] + time_cap, "deadline_mono": row[1] + time_cap, "time_cap_seconds": time_cap},
        "caps": {"physical_dispatches": count_cap, "maximum_nano_usd": money_cap},
        "parent": {"process": launch_context.process_identity(), "network_namespace": os.readlink("/proc/self/ns/net")},
    }
    if cohort and policy.profile == SEMANTIC140:
        anchor["format"] = launch_context.SEMANTIC140_ANCHOR_FORMAT
        anchor["profile_id"] = SEMANTIC140.profile_id
    mark_stage("anchor_validation")
    launch_context.validate_anchor(anchor)
    return launch_context.encode(anchor)


def run(args) -> int:
    # All authorization/artifact checks precede reading even the explicitly
    # supplied secret descriptor. This code has not been live exercised.
    mark_stage("manifest_policy")
    manifest_format = strict_json(private_read(args.manifest)).get("format")
    if manifest_format == SEMANTIC140.manifest_format:
        from cohort_policy import Semantic140Policy
        policy = Semantic140Policy(args.manifest, args.approval)
    elif manifest_format == "recall-exclusive-embedding-manifest/v1":
        from cohort_policy import CohortPolicy
        policy = CohortPolicy(args.manifest, args.approval)
    else:
        policy = Policy(args.manifest, args.approval)
    cohort = getattr(policy, "cohort", False)
    require(args.run_directory.is_absolute() and str(args.run_directory) == policy.manifest["run_directory"]
            and args.run_directory.parent.resolve() == args.run_directory.parent, "run_directory_mismatch")
    mark_stage("source_identity")
    source = Path(__file__).resolve().parent
    source_files = sorted(source.glob("*.py"))
    require(digest(encode([[p.name, file_hash(p)] for p in source_files])) == policy.manifest["gateway_sha256"], "gateway_source_mismatch")
    mark_stage("runtime_identity")
    require(not args.runtime.is_symlink(), "unsafe_runtime_root")
    runtime = args.runtime.resolve()
    require(tree_hash(runtime) == policy.manifest["runtime_sha256"], "runtime_digest_mismatch")
    require(file_hash(runtime / "opt/recall/bin/elephant") == policy.manifest["artifact_sha256"], "installed_artifact_mismatch")
    mark_stage("launch_specification")
    spec_raw = private_read(args.launch_spec)
    require(digest(spec_raw) == policy.manifest["configuration_sha256"], "configuration_digest_mismatch")
    spec = strict_json(spec_raw)
    fields(spec, {"format", "command", "environment", "bindings", "inputs"}, {"format", "command", "environment", "bindings"})
    require(spec["format"] == "recall-gateway-launch/v1", "unsupported_launch_spec")
    require(spec["bindings"] == policy.manifest["bindings"], "launch_binding_mismatch")
    command = spec["command"]
    require(type(command) is list and 1 <= len(command) <= 64, "invalid_worker_command")
    for arg in command:
        require(len(text(arg)) <= 4096 and "\x00" not in arg, "invalid_worker_argument")
    require(command[0].startswith("/"), "absolute_worker_command_required")
    allowed_environment = {"RUST_LOG", "SURREALDB_URL", "SURREALDB_NAMESPACE", "SURREALDB_DATABASE",
                                      "ELEPHANT_HOST", "ELEPHANT_PORT", "RECALL_BACKEND", "JWT_ISSUER", "JWT_AUDIENCE", "PIPELINE_SKILLS_DIR"}
    if cohort:
        allowed_environment.add("PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS")
    env = fields(spec["environment"], allowed_environment)
    if cohort:
        require(env.get("PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS") == "3", "cohort_queue_retry_config")
    for value in env.values():
        require(len(text(value)) <= 1024 and "\x00" not in value, "invalid_worker_environment")
    if "PIPELINE_SKILLS_DIR" in env:
        reviewed_runtime_directory(runtime, env["PIPELINE_SKILLS_DIR"])
    env.update(policy.worker_environment())
    env["RECALL_GATEWAY_ENDPOINT"] = policy.manifest["gateway_endpoint"]
    env["RECALL_GATEWAY_BINDINGS"] = encode(spec["bindings"]).decode()
    env["RECALL_RUN_ARTIFACT_DIR"] = "/work"
    # Reject startup with unsupported isolation before reading a credential.
    mark_stage("isolation_and_credential_descriptor")
    require(shutil.which("bwrap") is not None and Path("/proc/self/ns/net").exists(), "isolation_unavailable")
    require(args.credential_fd > 2, "dedicated_credential_fd_required")
    mark_stage("ledger")
    if cohort:
        from cohort import CohortLedger
        require(shutil.disk_usage(args.run_directory.parent).free >= policy.profile.capture_cap + 1024**3,
                "cohort_capture_capacity_unavailable")
        ledger = CohortLedger(args.run_directory, policy.manifest_hash, create=args.create,
                              identity=policy.identity, scopes=policy.scopes, profile_id=policy.profile.profile_id)
    elif policy.settlement:
        from settlement import SettlementLedger
        context_identity = {"account_id": policy.manifest["account_id"],
                            "credential_generation": policy.manifest["credential_generation"],
                            "configuration_sha256": policy.manifest["configuration_sha256"],
                            "bindings_sha256": digest(encode(policy.manifest["bindings"])),
                            "reasoning_sdk_sha256": policy.manifest["reasoning_sdk_sha256"],
                            "reasoning_projection_version": policy.manifest["reasoning_projection_version"]}
        ledger = SettlementLedger(args.run_directory, policy.manifest_hash, create=args.create,
                                  context_identity=context_identity)
    else:
        ledger = Ledger(args.run_directory, policy.manifest_hash, create=args.create)
    mark_stage("saved_original_inputs")
    work = args.run_directory / "worker"
    if args.create:
        work.mkdir(mode=0o700)
        write_new(args.run_directory / "manifest.json", private_read(args.manifest))
        write_new(args.run_directory / "launch.json", spec_raw)
        sync_directory(args.run_directory)
    # Resuming never replaces stored evidence/configuration. Creation also
    # rechecks the saved bytes against the originally validated manifest/spec.
    verify_original_launch_inputs(args.run_directory, policy.manifest_hash, spec_raw)
    # Resume cannot expose a worker-created host socket/device to the next child.
    for path in work.rglob("*"):
        require(stat.S_ISREG(path.lstat().st_mode) or stat.S_ISDIR(path.lstat().st_mode), "unsafe_worker_artifact")
    # Immutable original allocation/clock, sealed outside the child's writable
    # mount before any explicit provider credential is read.
    mark_stage("input_staging")
    input_mount = None
    if "inputs" in spec:
        fields(spec["inputs"], {"directory", "inventory_sha256"}, {"directory", "inventory_sha256"})
        require(type(spec["inputs"]["directory"]) is str, "invalid_immutable_input_directory")
        input_mount = args.run_directory / "read-only-inputs"
        inventory = launch_context.stage_inputs(Path(spec["inputs"]["directory"]), input_mount,
                                                spec["inputs"]["inventory_sha256"])
        inventory_path = args.run_directory / "inputs-inventory.json"
        inventory_raw = launch_context.encode(inventory)
        if inventory_path.exists():
            require(private_read(inventory_path) == inventory_raw, "original_inputs_inventory_changed")
        else:
            launch_context.write_original(inventory_path, inventory_raw)
    mark_stage("launch_anchor")
    anchor_raw = original_launch_anchor(policy, ledger, runtime, spec, source, cohort=cohort)
    mark_stage("anchor_persistence")
    anchor_path = args.run_directory / ("launch-anchor-" + digest(anchor_raw) + ".json")
    if anchor_path.exists():
        require(private_read(anchor_path) == anchor_raw, "launch_anchor_changed")
    else:
        launch_context.write_original(anchor_path, anchor_raw)
    env["RECALL_GATEWAY_ANCHOR_SHA256"] = digest(anchor_raw)
    mark_stage("accounting_channel")
    accounting_directory, accounting_sidecar = None, None
    if cohort:
        from accounting_export import export_root
        from accounting_channel import CHANNEL_FORMAT, directory_identity
        accounting_directory = export_root(ledger)
        channel = {"format": CHANNEL_FORMAT, "anchor_sha256": digest(anchor_raw),
                   "path": "/gateway-accounting", "directory": directory_identity(accounting_directory),
                   "helper_sha256": file_hash(source / "accounting_channel.py"), "read_only": True}
        channel_raw = encode(channel)
        accounting_sidecar = args.run_directory / ("accounting-channel-" + digest(channel_raw) + ".json")
        if accounting_sidecar.exists():
            require(private_read(accounting_sidecar) == channel_raw, "original_accounting_channel_changed")
        else:
            write_new(accounting_sidecar, channel_raw)
            sync_directory(args.run_directory)
    mark_stage("credential_read")
    raw_secret = os.read(args.credential_fd, 4097)
    os.close(args.credential_fd)
    require(0 < len(raw_secret) <= 4096, "invalid_credential_fd")
    try:
        credential = raw_secret.decode("ascii").strip()
    except UnicodeError as error:
        raise Denied("invalid_credential_fd") from error
    require(credential and all(32 < ord(c) < 127 for c in credential), "invalid_credential_fd")
    del raw_secret
    mark_stage("gateway_setup")
    address = policy.manifest["upstream_ips"][0]  # no implicit failover/retry
    forwarder = Forwarder(ledger, policy.check, lambda timeout: PinnedTLS(address, timeout), credential,
                          lambda identifier, request, original: Capture(args.run_directory, identifier, request, original,
                              binding=ledger.binding(request.scope) if policy.settlement or cohort else None))
    socket_path = args.run_directory / "gateway.sock"
    if socket_path.exists():
        require(stat.S_ISSOCK(socket_path.lstat().st_mode), "unsafe_gateway_socket")
        socket_path.unlink()  # exclusive ledger lock prevents concurrent owner
    gateway = GatewayServer(socket_path, policy, forwarder, diagnostic_directory=args.run_directory)
    os.chmod(socket_path, 0o600)
    mark_stage("child_command")
    child_args = launch_command(runtime, work, socket_path, source / "bridge.py", command, env, anchor_path,
                                input_mount, accounting_directory, accounting_sidecar)
    thread = threading.Thread(target=gateway.serve_forever, daemon=True)
    thread.start()
    child = None
    try:
        mark_stage("child_start")
        policy.check()
        # Neither actual credential nor its descriptor crosses this boundary.
        child = subprocess.Popen(child_args, env={"PATH": "/usr/bin:/bin"}, close_fds=True,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, start_new_session=True)
        with ledger.transaction():
            remaining = ledger.check_time()
        mark_stage("child_wait")
        return child.wait(timeout=remaining)
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
        gateway.shutdown()
        gateway.server_close()
        try:
            gateway.close_denial_capture()
        except Exception:
            # Keep ordinary ledger closure even when diagnostic custody fails.
            sys.stderr.write(encode({"gateway_denial_capture_complete": False,
                                     "code": "denial_capture_closure_failed"}).decode() + "\n")
        # Receipts alone never establish source/search/model quality acceptance.
        mark_stage("terminal_receipt")
        receipt = encode(ledger.receipt())
        write_new(args.run_directory / ("receipt-" + digest(receipt) + ".json"), receipt)
        ledger.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument("--launch-spec", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--run-directory", type=Path, required=True)
    parser.add_argument("--credential-fd", type=int, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--create", action="store_true")
    mode.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    try:
        return run(args)
    except Exception as error:
        # Never log exception messages, source-line text, frame locals or payloads.
        try:
            diagnostic = failure_diagnostic(error, args.run_directory)
            sys.stderr.write(encode({"gateway_failed": True, "diagnostic": diagnostic}).decode() + "\n")
        except Exception:
            sys.stderr.write(encode({"gateway_failed": True, "stage": LAUNCH_STAGE,
                                     "diagnostic_not_written": True}).decode() + "\n")
        return 78


if __name__ == "__main__":
    sys.exit(main())
