"""Owned D28 startup boundary for the reviewed direct G-child orchestrator.

This module is a library, not an execution entry point or qualification oracle.
S must validate the original independent static review before calling it and the
complete D runtime predicate before opening any provider cohort. It preserves C's
original startup receipt and hands original evidence bytes to that one validator.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import datetime as dt
from pathlib import Path
import time

from runtime_package_bootstrap import Bootstrap, actual_context, static_input, BINARY, PAIRED
from runtime_package_bootstrap_evidence import capture_live_startup
from runtime_package_bootstrap_contract import (
    D28_ALLOCATION_SHA256, D28_WHOLE_SECONDS, LIMITS, encode, fields, hex_identity,
    SEMANTIC140_ALLOCATION_SHA256, SEMANTIC140_PROFILE_ID, SEMANTIC140_ANCHOR_FORMAT,
    require, sha, strict_json, utc, validate_plan,
)
from runtime_package_bootstrap_io import (
    Allocation, ensure_ports_absent, load_helpers, private_new, read_bound, verify_isolation,
)

SELF = "scripts/qualification/d28_runtime_bootstrap.py"
EVIDENCE_SELF = "scripts/qualification/runtime_package_bootstrap_evidence.py"
MAX_ORIGINAL_BYTES = LIMITS["capture_bytes"] + 4 * 1024**2


def phase_from_original(plan_reference, expected_launch):
    """Read the original reviewed plan; no future receipt or approval is minted."""
    fields(plan_reference, {"path", "sha256"})
    hex_identity(plan_reference["sha256"])
    plan_raw = static_input(plan_reference["path"])
    require(sha(plan_raw) == plan_reference["sha256"], "original D28 startup plan changed")
    plan = validate_plan(strict_json(plan_raw))
    require({SELF, EVIDENCE_SELF} <= set(plan["dependencies"]),
            "D28 startup adapter or custody producer is not in the reviewed helper inventory")
    require(type(expected_launch) is dict and expected_launch.get("allocation_sha256")
            in (D28_ALLOCATION_SHA256, SEMANTIC140_ALLOCATION_SHA256), "closed workload allocation is required")
    if expected_launch["allocation_sha256"] == SEMANTIC140_ALLOCATION_SHA256:
        require(expected_launch.get("profile_id") == SEMANTIC140_PROFILE_ID
                and expected_launch.get("format") == SEMANTIC140_ANCHOR_FORMAT,
                "semantic140 startup profile differs")
    else:
        require("profile_id" not in expected_launch, "successor profile under original D28 startup")
    # Snapshot caller inputs before awaits. The reviewed G helper checks every
    # exact expectation and immutable binary/helper identity against actual G.
    return {"plan": plan, "plan_raw": plan_raw,
            "review": {"expected_launch": strict_json(encode(expected_launch))}}


class LiveStartup:
    """Live custody and original bytes, with no installed/prepared verdict."""

    def __init__(self, instance, phase, gateway, context_reference, receipt, receipt_reference,
                 isolation_raw):
        self.instance, self.phase, self.gateway = instance, phase, gateway
        self.receipt_raw = encode(receipt)
        self.receipt_reference = dict(receipt_reference)
        self.context_reference = dict(context_reference)
        self.runtime_configuration_reference = {
            "path": str(instance.root / "service/.elephant/elephant_config.toml"),
            "sha256": receipt["runtime_config_sha256"],
        }
        self.isolation_reference = {"path": "/work/isolation.json", "sha256": sha(isolation_raw)}
        # Paths are ordinary scoped token files, never token bytes or signing material.
        self.token_files = dict(receipt["token_files"])
        self.original_references = [self.receipt_reference, self.context_reference,
            self.runtime_configuration_reference, self.isolation_reference,
            {"path": str(instance.root / "plan.json"), "sha256": receipt["plan_sha256"]},
            dict(receipt["process_commands"])]
        for capture in receipt["captures"]:
            for kind in ("request", "response"):
                self.original_references.append({"path": capture[kind + "_path"],
                                                 "sha256": capture[kind + "_sha256"]})
        require(len({row["path"] for row in self.original_references}) == len(self.original_references),
                "duplicate original startup evidence path")
        self.closed = False
        self.cleanup_reference = None
        self.custody = None
        self.custody_sequence = 0

    @property
    def receipt(self):
        # Return a fresh parsed copy; consumers cannot change our original fact.
        return strict_json(self.receipt_raw)

    def original_bytes(self):
        """Return original path->bytes after bounded, hash-checked rereads.

        The D/S validator owns its {sha256,bytes,kind} wrappers. This function
        does not reconstruct requests, invent kind labels, or read token files.
        """
        require(not self.closed, "D28 live startup custody has ended")
        result, total = {}, 0
        for reference in self.original_references:
            raw = read_bound(reference["path"], min(8 * 1024**2, MAX_ORIGINAL_BYTES - total), private=True)
            total += len(raw)
            require(total <= MAX_ORIGINAL_BYTES and sha(raw) == reference["sha256"],
                    "original startup evidence changed or exceeded its bound")
            result[reference["path"]] = raw
        require(result[self.receipt_reference["path"]] == self.receipt_raw,
                "saved original startup receipt differs")
        return result

    async def revalidate_worker_recipe(self):
        """Capture an authorized actual read of the same worker construction."""
        self.revalidate_custody()
        worker = await self.instance.observe_worker_recipe()
        bindings = await self.instance.observe_embedding_recipes()
        self.revalidate_custody()
        return {"worker": worker, "bindings": bindings}

    def capture_original_custody(self):
        """Capture new actual process observations under the same original clock."""
        self.revalidate_custody()
        self.custody_sequence += 1
        self.custody = capture_live_startup(self.instance, self.receipt_reference,
            self.context_reference, self.gateway, self.phase["review"]["expected_launch"],
            sequence=self.custody_sequence)
        self.revalidate_custody()
        return self.custody

    def revalidate_custody(self):
        """Recheck actual original G and owned live processes, without a new clock."""
        require(not self.closed, "D28 live startup custody has ended")
        self.instance.allocation.remaining()
        context = self.gateway.read_context(self.context_reference["path"], self.context_reference["sha256"],
            expected=self.phase["review"]["expected_launch"], immediate_parent=True)
        require(sha(encode(context)) == sha(encode(self.instance.context)), "original G context changed")
        original = self.receipt["processes"]
        actual = {"recall": self.instance.processes.identity(self.instance.recall, str(BINARY),
                                                           self.phase["plan"]["artifact_sha256"])}
        if self.instance.paired is not None:
            actual["paired"] = self.instance.processes.identity(self.instance.paired, str(PAIRED),
                                                               self.phase["plan"]["paired_artifact_sha256"])
        require(actual == original, "original owned service identity changed")
        return actual


@asynccontextmanager
async def reviewed_d28_startup(plan_reference, expected_launch):
    """Keep one backend live for S's runtime predicate/preparation/measurement.

    The caller is G's actual immediate child. It supplies references already
    bound by D's independent review, and must use the same current G authority
    for its original preparation and measurement phases. No provider cohort is
    opened here. Exiting always cleans C's original owned processes; failure
    propagates, so callers must not begin a second backend after a failed exit.
    """
    phase = phase_from_original(plan_reference, expected_launch)
    gateway, context, context_raw, context_reference = actual_context(
        phase, allocation_sha256=phase["review"]["expected_launch"]["allocation_sha256"])
    plan = phase["plan"]
    wire = load_helpers(plan)
    clock = context["anchor"]["clock"]
    require(clock["time_cap_seconds"] == D28_WHOLE_SECONDS,
            "D28 must retain the original whole allocation")
    # Both backend startups consume the SAME original G clock. This is not a
    # fresh 7800-second allowance for each context entry or a preparation clock.
    started = int(clock["mono_start"] * 10**9)
    deadline = min(int(clock["deadline_mono"] * 10**9), started + D28_WHOLE_SECONDS * 10**9)
    expiry = min(utc(plan["expires_at"]), dt.datetime.fromtimestamp(clock["deadline_wall"], dt.timezone.utc))
    allocation = Allocation(started, deadline, deadline, expiry.isoformat(), maximum_seconds=D28_WHOLE_SECONDS)
    allocation.remaining()
    ensure_ports_absent()
    isolation_raw = read_bound(Path("/work/isolation.json"), 131072, private=True)
    verify_isolation(strict_json(isolation_raw))
    instance, live, error_type = None, None, None
    try:
        instance = Bootstrap(plan, phase["plan_raw"], context, context_raw, allocation, wire, gateway)
        await instance.credentials()
        await instance.services()
        await instance.spaces()
        gateway.read_context(context_reference["path"], context_reference["sha256"],
                             expected=phase["review"]["expected_launch"], immediate_parent=True)
        receipt = instance.receipt(isolation_raw)
        raw = encode(receipt)
        receipt_path = instance.root / "runtime-bootstrap.json"
        private_new(receipt_path, raw)
        live = LiveStartup(instance, phase, gateway, context_reference, receipt,
                           {"path": str(receipt_path), "sha256": sha(raw)}, isolation_raw)
        # Lift only the startup subphase ceiling. The original whole deadline,
        # wall expiry and cleanup reserve remain unchanged throughout S's work.
        allocation.bootstrap = False
        live.capture_original_custody()
        yield live
        live.revalidate_custody()
    except BaseException as error:
        error_type = type(error).__name__
        raise
    finally:
        if live is not None:
            live.closed = True
        if instance is not None:
            failures = await instance.processes.cleanup()
            terminal = {"format": "recall-runtime-bootstrap-cleanup/v1", "qualified": False,
                "product_acceptance": False, "backend": plan["backend"], "run_nonce": plan["run_nonce"],
                "status": "complete" if not failures and error_type is None else "incomplete",
                "cleanup_complete": not failures, "error_type": error_type,
                "collector_process_index": None, "cleanup_failures": failures,
                "processes": instance.processes.records, "finished_monotonic_ns": time.monotonic_ns()}
            raw = encode(terminal)
            path = instance.root / "cleanup.json"
            private_new(path, raw)
            if live is not None:
                live.cleanup_reference = {"path": str(path), "sha256": sha(raw)}
            require(not failures, "owned D28 service cleanup was incomplete")
