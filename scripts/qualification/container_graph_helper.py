"""Owned fixture sidecar: G's controlled spy inside Recall's exact namespace.

Only its loopback provider listener and private Unix control socket are used.
No upstream client, product storage, ambient credential, or shell is exposed.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import socket
import sys
import time

from container_graph_budget import BudgetLedger, require
from full_workload_transport import Capture, decode, encode, private_new, sha
from full_workload_spy import ControlledSpy

SOURCE_FILES = ("container_graph_helper.py", "container_graph_budget.py", "full_workload_transport.py",
                "full_workload_spy.py")
CONTROL_CAP = 1024 * 1024


class SharedCapture(Capture):
    def __init__(self, directory, ledger):
        self.ledger, self._held, self._written = ledger, 0, 0
        super().__init__(directory, ledger.change()["maximum_bytes"])

    @property
    def held(self):
        return self._held

    @held.setter
    def held(self, value):
        self.ledger.change(held=(self._held, value), allow_expired=value <= self._held)
        self._held = value

    @property
    def written(self):
        return self._written

    @written.setter
    def written(self, value):
        require(value >= self._written, "provider capture cannot discard written observations")
        self.ledger.change(written=value-self._written, allow_expired=True)
        self._written = value

    def save(self, identifier, request, observed, reservation):
        # Convert the original reservation to written bytes in ONE locked
        # operation: another process must not spend the intermediate release.
        raw = observed.pop("raw_body", b"")
        observed["response_body_sha256"] = sha(raw)
        observed["response_body_bytes"] = len(raw)
        metadata = encode(observed)
        size = len(request) + len(raw) + len(metadata)
        require(0 <= size <= reservation <= self._held, "reserved capture capacity exceeded")
        self.ledger.change(held=(self._held, self._held - reservation), written=size, allow_expired=True)
        self._held -= reservation
        self._written += size
        private_new(self.directory / f"{identifier:06}.request.json", request)
        private_new(self.directory / f"{identifier:06}.response.bin", raw)
        private_new(self.directory / f"{identifier:06}.exchange.json", metadata)


async def serve(directory):
    directory = Path(directory)
    require(directory.is_absolute() and directory.resolve() == directory, "owned canonical fixture directory required")
    raw = (directory / "settings.json").read_bytes()
    require(len(raw) <= 8 * 1024**2, "bounded original helper settings required")
    settings = decode(raw)
    require(set(settings) == {"format", "nonce", "recipe", "recipe_sha256", "deadline", "limits"}
            and settings["format"] == "recall-container-graph-helper/v1", "helper settings shape differs")
    ledger = BudgetLedger(directory / "budget.json", settings["nonce"])
    require(abs(ledger.change()["deadline_ns"] / 1e9 - settings["deadline"]) < 0.001,
            "helper cannot replace the original deadline")
    capture = SharedCapture(directory / "provider-captures", ledger)
    spy = ControlledSpy(settings["recipe"], settings["recipe_sha256"], capture, settings["limits"], settings["deadline"])
    stop = asyncio.Event()
    control_tasks = set()
    async def provider(reader, writer):
        try:
            ledger.change(calls=1)
        except Exception as error:
            spy.failure = type(error).__name__
            writer.close()
            async with asyncio.timeout(0.25):
                await writer.wait_closed()
            return
        await spy.handle(reader, writer)
    spy.server = await asyncio.start_server(provider, "127.0.0.1", 0, limit=65536)
    sockets = spy.server.sockets
    require(len(sockets) == 1 and sockets[0].getsockname()[0] == "127.0.0.1", "helper listener escaped owned loopback")
    address = sockets[0].getsockname()
    identity = {"format": "recall-container-graph-listener/v1", "nonce": settings["nonce"],
                "host": address[0], "port": address[1], "endpoint": f"http://127.0.0.1:{address[1]}/v1"}
    async def control(reader, writer):
        task = asyncio.current_task()
        control_tasks.add(task)
        try:
            async with asyncio.timeout(2):
                line = await reader.readline()
                require(line.endswith(b"\n") and len(line) <= 4096, "bounded complete control required")
                command = decode(line)
                require(type(command) is dict and command.get("nonce") == settings["nonce"], "original control nonce changed")
                action = command.get("action")
                require(set(command) == ({"action", "nonce", "physical_call_id"} if action == "release" else {"action", "nonce"}), "unknown control fields")
                if action == "release":
                    spy.release(command["physical_call_id"])
                elif action == "close":
                    stop.set()
                else:
                    require(action == "snapshot", "unsupported helper control action")
                value = {"listener": identity, "provider": spy.snapshot_calls(),
                         "rule_counts": spy.rule_counts, "held_rules": spy.held_rules,
                         "budget": ledger.change(allow_expired=True)}
                response = encode(value)
                require(len(response) <= CONTROL_CAP, "helper control output exceeded original command cap")
                writer.write(response + b"\n")
                await writer.drain()
        except (Exception, asyncio.CancelledError) as error:
            spy.failure = type(error).__name__
            stop.set()
        finally:
            writer.close()
            try:
                async with asyncio.timeout(0.1):
                    await writer.wait_closed()
            except (Exception, asyncio.CancelledError):
                pass
            control_tasks.discard(task)
    path = directory / "control.sock"
    require(not path.exists(), "original helper control socket already exists")
    server = None
    try:
        server = await asyncio.start_unix_server(control, path=str(path), limit=4096)
        async with asyncio.timeout(max(0.001, settings["deadline"] - time.monotonic())):
            await stop.wait()
    finally:
        if server is not None:
            server.close()
            await server.wait_closed()
        await spy.close()
        pending = control_tasks - {asyncio.current_task()}
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        final = encode({"listener": identity, "provider": spy.snapshot_calls(), "rule_counts": spy.rule_counts})
        ledger.change(written=len(final), allow_expired=True)
        private_new(directory / "final.json", final)
        path.unlink(missing_ok=True)


def control(directory, nonce, action, physical_call_id):
    command = {"action": action, "nonce": nonce}
    if action == "release":
        require(physical_call_id is not None and physical_call_id > 100000, "original held call identity required")
        command["physical_call_id"] = physical_call_id
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as channel:
        channel.settimeout(3)
        deadline = time.monotonic() + 3
        while True:
            try:
                channel.connect(str(Path(directory) / "control.sock"))
                break
            except FileNotFoundError:
                require(time.monotonic() < deadline, "owned helper did not become ready")
                time.sleep(0.025)
        channel.sendall(encode(command) + b"\n")
        result = bytearray()
        while not result.endswith(b"\n"):
            part = channel.recv(min(65536, CONTROL_CAP + 1 - len(result)))
            require(part and len(result) + len(part) <= CONTROL_CAP, "incomplete or oversized helper control response")
            result.extend(part)
        decode(bytes(result))
        sys.stdout.buffer.write(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("inventory")
    run = sub.add_parser("serve")
    run.add_argument("--directory", required=True)
    call = sub.add_parser("control")
    call.add_argument("--directory", required=True)
    call.add_argument("--nonce", required=True)
    call.add_argument("--command", choices=("snapshot", "release", "close"), required=True)
    call.add_argument("--physical-call-id", type=int)
    args = parser.parse_args()
    if args.action == "inventory":
        root = Path(__file__).resolve().parent
        print(json.dumps({"python_version": list(sys.version_info[:2]), "source_sha256": {
            name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in SOURCE_FILES}}))
    elif args.action == "serve":
        asyncio.run(serve(args.directory))
    else:
        control(args.directory, args.nonce, args.command, args.physical_call_id)


if __name__ == "__main__":
    main()
