"""Reuse the selected actual-capacity observer without changing historical quotas.

This is a packaging adapter, not a new allocation or a background monitor. The
unchanged, hash-pinned observer owns its ordinary check loop and retains a copy
of the original historical accounting. Static launch validation reads that same
lineage and current filesystem samples without changing its counters.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil

from runtime_package_io import GIB, checked_file, object_fields, require

ADAPTER_SHA256 = "95133d39da906f720a6c3b1390b44e3a9566c210517cb3e0db60a3c368a9dacb"
ADAPTER_BYTES = 12212
CAPACITY_SHA256 = "1726b7872f9a2565c639d0e9252dc303ea4c397b62e5b8302305c2034fcd8a30"
CAPACITY_BYTES = 898
CARRY_SHA256 = "ab6591431e2fb4c457f1975623b665dd64ec649a4fbfd81f0608c124b67fb86f"
CARRY_BYTES = 4755
HISTORY = Path("/home/luka/recall-orchestration/artifacts/shared-allocation.json")
FLOOR = 120 * GIB


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate capacity binding field")
        result[key] = value
    return result


def read_json(path):
    require(path.is_absolute() and path.resolve() == path and not path.is_symlink()
            and path.is_file() and path.stat().st_size <= 65536,
            "capacity record path or byte bound differs")
    return json.loads(path.read_bytes(), object_pairs_hook=unique)


def binding(reference, historical_path):
    path = checked_file(reference, maximum=65536)
    value = read_json(path)
    object_fields(value, ("format", "adapter", "capacity_receipt", "historical_ledger", "history_carry",
                          "observation", "root_minimum_free_bytes", "target_minimum_free_bytes"))
    require(value["format"] == "recall-runtime-package-current-capacity/v1"
            and value["historical_ledger"] == str(historical_path) == str(HISTORY),
            "selected historical accounting identity differs")
    for name, sha256, size in (("adapter", ADAPTER_SHA256, ADAPTER_BYTES),
                               ("capacity_receipt", CAPACITY_SHA256, CAPACITY_BYTES)):
        require(value[name]["sha256"] == sha256 and value[name]["bytes"] == size,
                "selected actual capacity source or receipt differs")
        checked_file(value[name], maximum=65536)
    require(type(value["root_minimum_free_bytes"]) is int
            and type(value["target_minimum_free_bytes"]) is int
            and value["root_minimum_free_bytes"] == value["target_minimum_free_bytes"] == FLOOR,
            "selected actual free-space floors differ")
    require(value["history_carry"]["sha256"] == CARRY_SHA256
            and value["history_carry"]["bytes"] == CARRY_BYTES,
            "selected latest cumulative accounting carry differs")
    carry = read_json(checked_file(value["history_carry"], maximum=65536))
    require(carry["state"] == "IMMUTABLE_METADATA_CARRY_FROM_ROOT_CLOSED_ORIGINAL"
            and carry["original_shared_ledger"]["path"] == str(HISTORY)
            and carry["original_observation"]["sha256"] == carry["sealed_exact_copy"]["sha256"]
            and carry["original_observation"]["bytes"] == carry["sealed_exact_copy"]["bytes"],
            "original cumulative accounting provenance differs")
    for name in ("original_observation", "sealed_exact_copy", "original_shared_ledger",
                 "original_build_closed"):
        checked_file(carry[name], maximum=65536)
    history = read_json(Path(carry["sealed_exact_copy"]["path"]))
    require(history["checks"] == carry["inherited_counters"]["checks"]
            and history["consumed_bytes"] == carry["inherited_counters"]["consumed_bytes"]
            and history["filesystems"] == carry["filesystems"],
            "latest cumulative counters or filesystem observations changed")
    value["_carry"] = carry
    return value


class PackagingBudget:
    """The packager stops on a low-floor checkpoint; it has no pausable worker."""

    def __init__(self, observer):
        self.observer = observer
        self.receipt = observer.receipt

    def check(self):
        state = self.observer.check()
        require(state.get("format") == "recall-actual-capacity-observation/v1"
                and state.get("nominal_cumulative_limit_enforced") is False
                and state.get("capacity_ok") is True,
                "actual capacity below selected floor; preserve original observation")
        return state


def current_budget(reference, destination, paths, historical_path):
    value = binding(reference, historical_path)
    observation = destination.with_name(destination.name + ".capacity.json")
    require(value["observation"] == str(observation), "packaging observation path differs")
    # Load exactly the already-selected observer. Never substitute its resource
    # transition, reset its historical counters, or patch release build limits.
    spec = importlib.util.spec_from_file_location("_runtime_selected_actual_capacity", value["adapter"]["path"])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    observer = module.ActualCapacityBudget(
        observation, capacity_receipt=value["capacity_receipt"],
        historical_ledger=Path(value["_carry"]["sealed_exact_copy"]["path"]),
        root_minimum_free_bytes=value["root_minimum_free_bytes"],
        target_minimum_free_bytes=value["target_minimum_free_bytes"])
    for path in paths:
        require(path.is_absolute() and path.resolve() == path and path.exists()
                and str(path.stat().st_dev) in observer.devices,
                "packaging path is outside the selected root and target filesystems")
    return PackagingBudget(observer)


def validate_samples(state, samples):
    """Compare current samples with the original two-device observation, read-only."""
    require(state.get("format") == "recall-actual-capacity-observation/v1"
            and state.get("nominal_cumulative_limit_enforced") is False
            and state.get("limit_bytes") is None
            and state.get("capacity_ok") is True,
            "original packaging capacity checkpoint differs")
    previous = state.get("filesystems")
    require(type(previous) is dict and len(previous) == 2
            and set(samples) == set(previous)
            and {item["path"] for item in previous.values()} == {"/", "/mnt/recall-release"},
            "selected root/target device set differs")
    for device, actual in samples.items():
        prior = previous[device]
        require(prior["minimum_free_bytes"] == FLOOR and actual["path"] == prior["path"]
                and type(actual["total"]) is int and actual["total"] == prior["total"]
                and type(actual["free"]) is int and type(actual["used"]) is int
                and actual["used"] >= 0 and actual["free"] >= FLOOR
                and actual["used"] + actual["free"] <= actual["total"],
                "actual filesystem identity, capacity or retained safety floor differs")
    return {"format": "recall-runtime-package-capacity-read/v1", "capacity_ok": True,
            "historical_snapshot": state["historical_snapshot"],
            "capacity_receipt": state["capacity_receipt"], "filesystems": samples,
            "original_checks": state["checks"], "original_consumed_bytes": state["consumed_bytes"],
            "counters_modified": False, "nominal_cumulative_limit_enforced": False}


def validate_lineage(state, original, carry, capacity_receipt):
    selected = carry["sealed_exact_copy"]
    snapshot = state["historical_snapshot"]
    require(state["historical_snapshot"] == original["historical_snapshot"]
            and snapshot["sha256"] == selected["sha256"]
            and snapshot["bytes"] == selected["bytes"]
            and state["historical_checks"] == carry["inherited_counters"]["checks"]
            and state["historical_consumed_bytes"] == carry["inherited_counters"]["consumed_bytes"]
            and state["capacity_receipt"] == original["capacity_receipt"] == capacity_receipt
            and state["checks"] >= original["checks"]
            and state["consumed_bytes"] >= original["consumed_bytes"],
            "original capacity accounting lineage changed")


def validate_current_capacity(reference, receipt, paths, historical_path):
    value = binding(reference, historical_path)
    require(receipt.get("resource_budget_binding") == reference
            and receipt.get("resource_budget_file") == value["observation"],
            "original packaged capacity binding differs")
    state = read_json(Path(value["observation"]))
    validate_lineage(state, receipt["resource_budget"], value["_carry"], value["capacity_receipt"])
    checked_file(state["historical_snapshot"], maximum=65536)
    samples = {}
    for device, prior in state["filesystems"].items():
        path = Path(prior["path"])
        require(path.is_absolute() and path.resolve() == path and str(path.stat().st_dev) == device,
                "selected capacity filesystem changed or unmounted")
        usage = shutil.disk_usage(path)
        require(str(path.stat().st_dev) == device, "filesystem changed during capacity sampling")
        samples[device] = {"path": str(path), "total": usage.total,
                           "used": usage.used, "free": usage.free}
    result = validate_samples(state, samples)
    for path in paths:
        require(path.is_absolute() and path.resolve() == path and str(path.stat().st_dev) in samples,
                "launch input/output outside selected filesystems")
    return result
