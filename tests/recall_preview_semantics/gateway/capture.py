"""Private, bounded synthetic qualification bodies; never credential headers."""
from __future__ import annotations

import os
from pathlib import Path

from ledger import RESPONSE_CAP, sync_directory
from wire import Request, digest, encode, require


def write_new(path: Path, body: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(body)
        output.flush()
        os.fsync(output.fileno())


class Capture:
    def __init__(self, directory: Path, identifier: int, request: Request, original: bytes, *, binding=None):
        self.directory = directory / f"dispatch-{identifier:06d}"
        self.directory.mkdir(mode=0o700)
        sync_directory(directory)
        require(digest(original) == request.original_hash, "original_capture_mismatch")
        write_new(self.directory / "request.original.json", original)
        write_new(self.directory / "request.forwarded.json", request.body)
        intent = {
            "format": "recall-private-synthetic-capture/v1", "dispatch_id": identifier,
            "original_sha256": request.original_hash, "forwarded_sha256": request.forwarded_hash,
            "transforms": request.transforms, "transport_response_byte_cap": RESPONSE_CAP,
            "credential_headers_retained": False,
        }
        if binding is not None:
            intent["binding"] = binding
        write_new(self.directory / "intent.json", encode(intent))
        fd = os.open(self.directory / "response.body", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.output = os.fdopen(fd, "wb")
        self.count = 0
        sync_directory(self.directory)

    def write(self, block: bytes) -> None:
        require(self.count + len(block) <= RESPONSE_CAP, "capture_byte_limit")
        self.output.write(block)
        self.count += len(block)

    def finish(self, *, complete: bool, status: int | None, response_hash: str, content_type: str | None = None, transport: dict | None = None) -> None:
        self.output.flush()
        os.fsync(self.output.fileno())
        receipt = {
            "complete": complete, "status": status, "response_sha256": response_hash,
            "response_bytes": self.count, "provider_quality_acceptance": False,
        }
        if content_type is not None:
            receipt["content_type"] = content_type
        if transport is not None:
            receipt["transport"] = transport
        write_new(self.directory / "capture.json", encode(receipt))
        sync_directory(self.directory)

    def close(self) -> None:
        self.output.close()
