"""Installed embedding-negative specialization of the original governed spy."""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
from pathlib import Path
import sys
import threading
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "tests/recall_preview_semantics"))
import installed_fake_vectors_contract as contract
from full_workload_spy import ControlledSpy
from full_workload_transport import Capture, decode


class SeededSpy(ControlledSpy):
    @staticmethod
    def validate_recipe(recipe, expected_sha256):
        corpus = decode((ROOT / "tests/recall_preview_semantics/corpus.json").read_bytes())
        return contract.validate_recipe(recipe, expected_sha256, corpus)

    provider_response = staticmethod(contract.response)

    def select(self, observed, body):
        # The actual governed embedding wire has no X-Space-Id. Bind scope to
        # the separately configured synthetic credential, never text or timing.
        matches = [rule for rule in self.recipe["rules"] if all(rule[key] == observed[key]
                   for key in ("credential_sha256", "model", "path"))]
        contract.require(len(matches) == 1, "physical embedding credential/model lacks unique declared space")
        wire_space = observed.get("space")
        contract.require(wire_space in (None, matches[0]["space"]), "physical space header conflicts with credential authority")
        observed["wire_space"] = wire_space
        observed["space"] = matches[0]["space"]
        observed["space_authority"] = "declared unique synthetic credential hash and exact model"
        return super().select(observed, body)


class OwnedSpy:
    """One listener/thread; shutdown consumes the caller's original clock."""
    def __init__(self, recipe, directory, port, work_deadline, whole_deadline):
        self.recipe, self.directory, self.port = recipe, Path(directory), port
        self.work_deadline, self.whole_deadline = work_deadline, whole_deadline
        self.ready = concurrent.futures.Future()
        self.loop = None
        self.spy = None
        self.thread = threading.Thread(target=self._run, name="installed-seeded-spy", daemon=True)
        self.close_requested = threading.Event()
        self.thread_error = None
        self.closed = False

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            if self.close_requested.is_set():
                return
            capture = Capture(self.directory, 64 * 1024**2)
            self.spy = SeededSpy(self.recipe, contract.digest(self.recipe), capture,
                                 contract.SPY_LIMITS, self.work_deadline)
            if self.close_requested.is_set():
                return
            self.loop.run_until_complete(self.spy.start(self.port))
            if not self.close_requested.is_set():
                self.ready.set_result(True)
                # A racing close also schedules loop.stop. Checking the latch
                # here prevents a late run_forever after a startup timeout.
                if not self.close_requested.is_set():
                    self.loop.run_forever()
        except BaseException as error:
            self.thread_error = error
            if not self.ready.done():
                self.ready.set_exception(error)
        finally:
            try:
                if self.spy is not None:
                    self.loop.run_until_complete(self.spy.close())
                tasks = asyncio.all_tasks(self.loop)
                for task in tasks:
                    task.cancel()
                if tasks:
                    self.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            except BaseException as error:
                self.thread_error = self.thread_error or error
            finally:
                try:
                    self.loop.close()
                except BaseException as error:
                    self.thread_error = self.thread_error or error
                if not self.ready.done():
                    self.ready.set_exception(ValueError("owned spy closed before readiness"))

    def start(self):
        contract.require(time.monotonic() < self.work_deadline, "original spy work allocation exhausted")
        self.thread.start()
        self.ready.result(timeout=max(0, min(5, self.work_deadline - time.monotonic())))

    def snapshot(self):
        contract.require(self.spy is not None and self.loop is not None and self.thread.is_alive(), "original spy unavailable")
        async def take():
            # The API can receive a response just before the server records its
            # final close. Drain that same physical call; never resubmit it.
            async with asyncio.timeout(max(0, min(4, self.work_deadline - time.monotonic()))):
                async with self.spy.changed:
                    while self.spy.active:
                        await self.spy.changed.wait()
            return self.spy.snapshot_calls()
        return asyncio.run_coroutine_threadsafe(take(), self.loop).result(
            timeout=max(0, min(5, self.whole_deadline - time.monotonic())))

    def close(self):
        if self.closed:
            return
        self.close_requested.set()
        errors = []
        future = None
        try:
            if self.loop is not None and self.thread.is_alive() and self.spy is not None and self.loop.is_running():
                closing = self.spy.close()
                try:
                    future = asyncio.run_coroutine_threadsafe(closing, self.loop)
                except BaseException:
                    closing.close()
                    raise
                future.result(timeout=max(0, min(6, self.whole_deadline - time.monotonic())))
        except BaseException as error:
            errors.append(error)
            if future is not None:
                future.cancel()
        finally:
            # A failed close future must not skip stop/join; this also covers
            # a live startup thread that has not yet constructed its spy.
            try:
                if self.loop is not None and self.thread.is_alive() and not self.loop.is_closed():
                    self.loop.call_soon_threadsafe(self.loop.stop)
            except BaseException as error:
                errors.append(error)
            finally:
                try:
                    if self.thread.ident is not None:
                        self.thread.join(timeout=max(0, min(6, self.whole_deadline - time.monotonic())))
                except BaseException as error:
                    errors.append(error)
        self.closed = not self.thread.is_alive() and (self.spy is None or not self.spy.tasks)
        if not self.closed:
            errors.append(ValueError("owned seeded listener/thread cleanup incomplete"))
        if self.thread_error is not None:
            errors.append(self.thread_error)
        if errors:
            raise errors[0]

    def input_counts(self):
        """Count actual captured texts, never intended calls or guessed timing."""
        snapshot = self.snapshot()
        contract.require(snapshot["error_type"] is None and snapshot["active"] == 0
                         and not snapshot["held_ids"], "original provider calls incomplete")
        counts = {}
        for call in snapshot["complete_calls"]:
            contract.require(call.get("request_complete") is True and call.get("response_complete") is True
                             and call.get("status") == 200 and not call.get("error_type")
                             and not call.get("connection_cleanup_failed"), "actual seeded provider call failed")
            raw = (self.directory / f'{call["id"]:06}.request.json').read_bytes()
            contract.require(hashlib.sha256(raw).hexdigest() == call["request_sha256"],
                             "original captured embedding request changed")
            request = decode(raw)
            rule = next(rule for rule in self.recipe["rules"] if rule["id"] == call["rule_id"])
            content_type, expected_response = contract.response(rule, request)
            response = (self.directory / f'{call["id"]:06}.response.bin').read_bytes()
            contract.require(response == expected_response and call["content_type"] == content_type
                             and hashlib.sha256(response).hexdigest() == call["response_body_sha256"]
                             and len(response) == call["response_body_bytes"],
                             "physical response differs from original declared seeded vectors")
            for text in request["input"]:
                key = (call["space"], text)
                counts[key] = counts.get(key, 0) + 1
        return snapshot, counts
