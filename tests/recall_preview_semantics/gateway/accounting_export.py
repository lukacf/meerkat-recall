"""Parent-owned export of one already closed original cohort.

Only reviewed final synthetic bodies are hardlinked. The private ledger,
credentials, unrelated headers and live response files are never exported.
"""
from __future__ import annotations

import os
import stat

from accounting_channel import BUNDLE_FORMAT, INDEX_FORMAT, MAX_RECORD, MAX_TOTAL, directory_identity, reference
from capture import write_new
from cohort import read_capture
from ledger import sync_directory
from wire import digest, encode, require, strict_json


def export_root(ledger):
    root = ledger.directory / "accounting-export"
    root.mkdir(mode=0o700, exist_ok=True)
    directory_identity(root)
    sync_directory(ledger.directory)
    return root


def closed_export(ledger, backend, phase, caller, close):
    """Snapshot original retained facts, never a current receipt or new clock."""
    with ledger.mutex:
        row = ledger.db.execute(
            "SELECT id,state,started_wall,started_mono,deadline,baseline,baseline_hash,receipt,observation "
            "FROM cohort WHERE backend=? AND phase=? AND caller=?", (backend, phase, caller)
        ).fetchone()
        require(row is not None and row[1] == "closed", "accounting_original_cohort_not_closed")
        cohort, _, wall, mono, deadline, baseline, before_hash, receipt, observation = row
        close_raw = receipt.encode()
        require(strict_json(close_raw) == close and close.get("verdict") == "passed"
                and close.get("admission_closed") is True, "accounting_original_close_differs")
        baseline_raw = baseline.encode()
        require(digest(baseline_raw) == before_hash == close["before_sha256"], "accounting_original_baseline_differs")
        begin = {"format": "recall-exclusive-cohort-begin/v1", "cohort_id": cohort,
                 "backend": backend, "phase": phase, "baseline_sha256": before_hash,
                 "allocation_sha256": ledger.identity["allocation_sha256"],
                 "authority": "exclusive_gateway_cohort"}
        sealed_path = ledger.directory / f"cohort-{cohort:02d}.json"
        sealed_raw = read_capture(sealed_path, MAX_RECORD)
        require(digest(sealed_raw) == close["sealed_receipt_sha256"], "accounting_original_seal_differs")
        sealed = strict_json(sealed_raw)
        require(set(sealed) == {"before_sha256", "after", "capture_inventory", "observation_sha256", "identity"}
                and sealed["before_sha256"] == before_hash and sealed["identity"] == ledger.identity
                and sealed["observation_sha256"] == observation
                and digest(encode(sealed["after"])) == close["after_sha256"]
                and digest(encode(sealed["capture_inventory"])) == close["capture_inventory_sha256"],
                "accounting_sealed_original_chain_differs")
        original_clock = ledger.db.execute("SELECT manifest,wall_start,mono_start,boot FROM run").fetchone()
        require(original_clock is not None and original_clock[0] == sealed["after"]["manifest_sha256"],
                "accounting_original_run_differs")
        clock = {"format": "recall-original-cohort-clock/v1", "cohort_id": cohort,
                 "backend": backend, "phase": phase, "state": "closed",
                 "run": {"wall_start": original_clock[1], "mono_start": original_clock[2], "boot_id": original_clock[3]},
                 "phase_start": {"wall": wall, "mono": mono}, "deadline_mono": deadline,
                 "time_cap_seconds": ledger.profile.phase_limits(phase)[3],
                 "closure_timestamp": None}
        associations = ledger.db.execute(
            "SELECT dispatch_id,binding,send_intent FROM cohort_dispatch WHERE cohort_id=? ORDER BY dispatch_id",
            (cohort,)).fetchall()
        require(0 < len(associations) <= ledger.profile.count_cap, "accounting_dispatch_inventory_bound")
        originals = {item["id"]: item for item in sealed["after"]["dispatches"]}
        dispatches = []
        for identifier, binding, sent in associations:
            require(identifier in originals and sent == 1 and originals[identifier]["finished"] is not None
                    and originals[identifier]["outcome"] in ("complete", "upstream_error"),
                    "accounting_original_dispatch_incomplete")
            dispatches.append({"ledger": originals[identifier], "binding": strict_json(binding.encode()), "send_intent": sent})
        require([item["ledger"]["id"] for item in dispatches]
                == [item["dispatch_id"] for item in sealed["capture_inventory"]],
                "accounting_original_dispatch_membership_differs")
        root = export_root(ledger)
        folder = root / f"cohort-{cohort:02d}"
        folder.mkdir(mode=0o700, exist_ok=True)
        directory_identity(folder)
        files = []

        def retain(name, raw, kind):
            require(len(raw) <= MAX_RECORD, "accounting_record_bound")
            path = folder / name
            if path.exists():
                require(read_capture(path, MAX_RECORD) == raw, "accounting_export_changed")
            else:
                write_new(path, raw)
            ref = reference(raw, kind)
            files.append({"path": path.relative_to(root).as_posix(), "ref": ref})
            return ref

        bundle = {"format": BUNDLE_FORMAT,
                  "begin": retain("begin.json", encode(begin), "provider-cohort-begin"),
                  "close": retain("close.json", close_raw, "provider-cohort-close"),
                  "baseline": retain("baseline.json", baseline_raw, "provider-baseline"),
                  "phase_clock": retain("phase-clock.json", encode(clock), "provider-phase-clock"),
                  "sealed": retain("sealed.json", sealed_raw, "provider-sealed"),
                  "capture_inventory": retain("capture-inventory.json", encode(sealed["capture_inventory"]), "provider-capture-inventory"),
                  "dispatch_records": retain("dispatch-records.json", encode(dispatches), "provider-dispatch-records")}
        allowed = {"request.original.json": "provider-request", "request.forwarded.json": "provider-request",
                   "intent.json": "provider-dispatch-intent", "response.body": "provider-response", "capture.json": "provider-capture"}
        for item in sealed["capture_inventory"]:
            require(set(item) == {"dispatch_id", "files"} and set(item["files"]) == set(allowed),
                    "accounting_final_capture_shape")
            original = ledger.directory / f'dispatch-{item["dispatch_id"]:06d}'
            directory_identity(original)
            target = folder / original.name
            target.mkdir(mode=0o700, exist_ok=True)
            directory_identity(target)
            for name, kind in allowed.items():
                source = original / name
                raw = read_capture(source, MAX_RECORD)
                require(digest(raw) == item["files"][name], "accounting_final_capture_changed")
                before = source.lstat()
                require(stat.S_ISREG(before.st_mode) and before.st_nlink in (1, 2), "accounting_unreviewed_file_alias")
                destination = target / name
                if not destination.exists():
                    require(before.st_nlink == 1, "accounting_existing_foreign_alias")
                    os.link(source, destination, follow_symlinks=False)
                copied = destination.lstat()
                require((copied.st_dev, copied.st_ino) == (before.st_dev, before.st_ino)
                        and copied.st_nlink == 2 and stat.S_ISREG(copied.st_mode), "accounting_alias_escape")
                require(read_capture(destination, MAX_RECORD) == raw, "accounting_alias_changed")
                files.append({"path": destination.relative_to(root).as_posix(), "ref": reference(raw, kind)})
            sync_directory(target)
        bundle_ref = retain("bundle.json", encode(bundle), "provider-accounting")
        require(sum(item["ref"]["bytes"] for item in files) <= min(MAX_TOTAL, ledger.profile.capture_cap), "accounting_original_capture_bound")
        index = {"format": INDEX_FORMAT, "cohort_id": cohort, "backend": backend, "phase": phase,
                 "caller_sha256": caller, "begin_sha256": digest(encode(begin)), "close_sha256": digest(close_raw),
                 "reference": bundle_ref, "files": files}
        index_raw = encode(index)
        require(len(index_raw) <= MAX_RECORD, "accounting_index_bound")
        index_path = folder / "index.json"
        if index_path.exists():
            require(read_capture(index_path, MAX_RECORD) == index_raw, "accounting_index_changed")
        else:
            write_new(index_path, index_raw)
        sync_directory(folder)
        sync_directory(root)
        return {"path": index_path.relative_to(root).as_posix(), "ref": reference(index_raw, "provider-accounting-index")}
