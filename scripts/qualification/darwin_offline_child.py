#!/usr/bin/env python3
"""Trusted launch handshake and synthetic Seatbelt controls, never an installer."""
from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import resource
import socket
import sys


def denied(action):
    try:
        action()
    except OSError as error:
        if error.errno in (errno.EACCES, errno.EPERM):
            return error.errno
        raise
    raise ValueError("sandbox admitted an operation that must be denied")


def probe(arguments):
    allowed, forbidden, listener_port, candidate_port = arguments
    allowed, forbidden = Path(allowed), Path(forbidden)
    (allowed / "write-positive").write_bytes(b"synthetic writable control")
    assert (allowed / "write-positive").read_bytes() == b"synthetic writable control"
    results = {"outside_read_errno": denied(lambda: (forbidden / "read-control").read_bytes()),
               "outside_write_errno": denied(lambda: (forbidden / "write-control").write_bytes(b"denied"))}
    with socket.socket() as connection:
        connection.settimeout(2)
        results["outbound_errno"] = denied(lambda: connection.connect(("127.0.0.1", int(listener_port))))
    # A successful bind/listen proves the profile can support the exact endpoint
    # used by the installed case suite. No request goes to a foreign process.
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", int(candidate_port)))
        endpoint.listen(1)
        results["owned_loopback_bind"] = True
    results["owned_write"] = True
    print(json.dumps(results, sort_keys=True))


def main():
    if sys.argv[1] == "--probe":
        probe(sys.argv[2:])
        return
    if sys.argv[1] != "--await-parent":
        raise ValueError("unknown trusted child mode")
    descriptor = int(sys.argv[2])
    try:
        if os.read(descriptor, 1) != b"G":
            raise ValueError("parent did not release the registered child")
    finally:
        os.close(descriptor)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_CPU, (180, 180))
    resource.setrlimit(resource.RLIMIT_NOFILE, (1024, 1024))
    os.nice(10)
    # The same session leader becomes sandbox-exec and then the exact command.
    # No shell, subprocess tree, environment loading or parent-selected PID.
    os.execve(sys.argv[3], sys.argv[3:], dict(os.environ))


if __name__ == "__main__":
    main()
