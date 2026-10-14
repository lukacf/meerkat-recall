#!/usr/bin/env python3
"""Wait for the Mac supervisor to register the original group leader, then exec."""
import os
import resource
import sys


def main():
    if len(sys.argv) < 4 or sys.argv[1] != "--await-parent":
        raise ValueError("supervisor handshake required")
    descriptor = int(sys.argv[2])
    try:
        if os.read(descriptor, 1) != b"G":
            raise ValueError("supervisor did not release this child")
    finally:
        os.close(descriptor)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_NOFILE, (8192, 8192))
    os.nice(20)
    os.execve(sys.argv[3], sys.argv[3:], dict(os.environ))


if __name__ == "__main__":
    main()
