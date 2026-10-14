"""Sandbox-only loopback bridge. No provider credentials or upstream network."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import selectors
import secrets
import re
import socket
import socketserver
import subprocess
import sys
import threading
import time


BRIDGE_STAGE = "entry"
FAILURE_PATH = Path("/work/bridge.failure.json")


def mark_stage(stage):
    global BRIDGE_STAGE
    BRIDGE_STAGE = stage


def failure_diagnostic(error, path, *, stage, source_root=None, child_returncode=None):
    """Bounded source locations only: never messages, locals, argv or child IO."""
    if not re.fullmatch(r"[a-z_]{1,64}", stage):
        raise ValueError("invalid diagnostic stage")
    path = Path(path)
    if not path.is_absolute() or path.parent.resolve() != path.parent:
        raise ValueError("noncanonical diagnostic destination")
    source = Path(__file__).resolve()
    frames = []
    traceback = error.__traceback__ if error is not None else None
    while traceback is not None:
        code = traceback.tb_frame.f_code
        location = Path(code.co_filename).resolve()
        relative = None
        if source_root is not None:
            try:
                relative = location.relative_to(source_root).as_posix()
            except ValueError:
                pass
        elif location in (source, source.with_name("launch_context.py")):
            relative = location.name
        if relative is not None:
            frames.append({"file": relative, "function": code.co_name,
                           "line": traceback.tb_lineno})
            frames = frames[-16:]
        traceback = traceback.tb_next
    value = {"format": "recall-gateway-worker-failure/v1", "state": "failed",
             "stage": stage, "exception_type": type(error).__name__ if error is not None else None,
             "source_frames": frames, "child_returncode": child_returncode,
             "exception_message_recorded": False, "locals_or_payloads_recorded": False,
             "semantic_acceptance": False}
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    if len(raw) > 16384:
        raise ValueError("diagnostic byte limit")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Bridge(socketserver.ThreadingTCPServer):
    allow_reuse_address = False
    daemon_threads = True
    request_queue_size = 4
    slots = threading.BoundedSemaphore(8)

    def process_request(self, request, address):
        if not self.slots.acquire(False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()

    def handle_error(self, request, address):
        pass


class Relay(socketserver.BaseRequestHandler):
    def handle(self):
        upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        upstream.settimeout(125)
        self.request.settimeout(125)
        try:
            upstream.connect("/gateway.sock")
            with selectors.DefaultSelector() as selector:
                selector.register(self.request, selectors.EVENT_READ, upstream)
                selector.register(upstream, selectors.EVENT_READ, self.request)
                while True:
                    ready = selector.select(125)
                    if not ready:
                        return
                    for key, _ in ready:
                        block = key.fileobj.recv(8192)
                        if not block:
                            return
                        key.data.sendall(block)
        finally:
            upstream.close()


def write_isolation(context_module, context_reference, anchor, work=Path("/work")):
    """Project the actual bridge observation for existing bootstrap consumers."""
    raw = context_module.read_bounded(Path(context_reference["path"]), private=True)
    if context_module.digest(raw) != context_reference["sha256"]:
        raise RuntimeError("original_bridge_context_changed")
    context = context_module.validate_context(context_module.strict_json(raw),
        expected={"manifest_sha256": anchor["manifest_sha256"]},
        now_wall=time.time(), now_mono=time.monotonic())
    if context["anchor"] != anchor:
        raise RuntimeError("original_bridge_anchor_changed")
    network = context["network"]
    proof = {"format": "recall-gateway-isolation/v1",
             "network_namespace": network["namespace"],
             "parent_network_namespace": network["parent_namespace"],
             "interfaces": network["interfaces"], "ipv4_routes": network["ipv4_routes"],
             # launcher.launch_command clears the child environment and closes
             # descriptors; only synthetic gateway capabilities cross it.
             "provider_credentials_present": False}
    return context_module.write_original(work / "isolation.json", context_module.encode(proof))


def worker_environment(environment: dict[str, str]) -> dict[str, str]:
    """New synthetic API auth only; never consume a host/provider signing key."""
    if "SKIP_AUTH" in environment or "JWT_SECRET" in environment:
        raise RuntimeError("ambient_or_bypassed_auth_forbidden")
    result = dict(environment)
    result["JWT_SECRET"] = secrets.token_urlsafe(48)
    result["JWT_ALGORITHM"] = "HS256"
    return result


def runtime_limits(context_module, anchor, connections, duration):
    """Environment cannot increase the capacity of the original launch anchor."""
    expected_connections, expected_duration = context_module.connection_limits(anchor)
    if ((connections, duration) != (str(expected_connections), str(expected_duration))
            or int(duration) != anchor["clock"]["time_cap_seconds"]):
        raise RuntimeError("unapproved_gateway_runtime_limits")
    return expected_connections, expected_connections if expected_connections in (64, 160) else 4


def run():
    mark_stage("namespace_validation")
    parent_net = sys.argv[1]
    # Called only INSIDE bwrap; these reads do not inspect host namespaces.
    net = os.readlink("/proc/self/ns/net")
    if net == parent_net:
        raise RuntimeError("network_namespace_not_isolated")
    interfaces = {line.split(":", 1)[0].strip() for line in Path("/proc/net/dev").read_text().splitlines() if ":" in line}
    if interfaces != {"lo"}:
        raise RuntimeError("unexpected_network_interface")
    routes = Path("/proc/net/route").read_text().splitlines()
    if len(routes) > 1:
        raise RuntimeError("unexpected_network_route")
    # The bridge imports only its reviewed readonly absolute helper under -I.
    mark_stage("context_module")
    spec = importlib.util.spec_from_file_location("_recall_launch_context", "/launch_context.py")
    context_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(context_module)
    mark_stage("anchor_validation")
    anchor_raw = context_module.read_bounded(Path("/launch-anchor.json"), private=True)
    if context_module.digest(anchor_raw) != os.environ["RECALL_GATEWAY_ANCHOR_SHA256"]:
        raise RuntimeError("original_launch_anchor_changed")
    anchor = context_module.validate_anchor(context_module.strict_json(anchor_raw))
    if (anchor["parent"]["network_namespace"] != parent_net
            or context_module.digest(context_module.encode(sys.argv[2:])) != anchor["command_sha256"]):
        raise RuntimeError("original_launch_command_changed")
    mark_stage("context_observation")
    context_reference = context_module.observe(anchor_raw)
    mark_stage("isolation_projection")
    write_isolation(context_module, context_reference, anchor)
    mark_stage("runtime_limits")
    connections = os.environ.get("RECALL_GATEWAY_MAX_CONNECTIONS", "8")
    duration = os.environ.get("RECALL_GATEWAY_TIME_CAP", "5400")
    slots, backlog = runtime_limits(context_module, anchor, connections, duration)
    Bridge.slots = threading.BoundedSemaphore(slots)
    Bridge.request_queue_size = backlog
    mark_stage("relay_bind")
    with Bridge(("127.0.0.1", 43129), Relay) as server:
        mark_stage("relay_thread")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            mark_stage("worker_environment")
            environment = worker_environment(dict(os.environ))
            environment["RECALL_GATEWAY_CONTEXT_PATH"] = context_reference["path"]
            environment["RECALL_GATEWAY_CONTEXT_SHA256"] = context_reference["sha256"]
            mark_stage("worker_deadline")
            remaining = min(anchor["clock"]["deadline_wall"] - time.time(),
                            anchor["clock"]["deadline_mono"] - time.monotonic())
            if remaining <= 0:
                raise RuntimeError("original_launch_deadline_expired")
            mark_stage("worker_subprocess")
            return subprocess.run(sys.argv[2:], env=environment, close_fds=True, timeout=remaining,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
        finally:
            server.shutdown()


def main():
    try:
        returncode = run()
    except Exception as error:
        try:
            failure_diagnostic(error, FAILURE_PATH, stage=BRIDGE_STAGE)
        except Exception:
            pass  # Diagnostic failure must preserve the original failure exit.
        return 78
    if returncode != 0:
        try:
            failure_diagnostic(None, FAILURE_PATH, stage="worker_child_exit",
                               child_returncode=returncode)
        except Exception:
            pass
    return returncode


if __name__ == "__main__":
    sys.exit(main())
