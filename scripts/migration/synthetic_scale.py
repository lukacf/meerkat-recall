#!/usr/bin/env python3
"""Source-only qualification driver. Execution belongs to the existing sole lane.

Every invocation creates fresh synthetic input inside its measured clock. This
measures planner/initial-copy/adoption/reconciliation, not live export, network
transfer, provider embedding, certification, or the full household acceptance.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
import synthetic_household as household
import synthetic_process as supervision

STAMP = "2026-10-10T00:00:00Z"
OPERATIONAL_DISPOSITIONS = ("native", "managed_archive_v1")
OPERATIONAL_TABLES = frozenset(("audit_log", "cost_ledger_daily", "work_item", "work_run",
    "work_item_archive", "work_run_archive", "work_artifact", "work_edge", "work_deadletter", "system_event"))
PROFILES = {
    "tiny": {"doc": 8, "doc_node": 32, "chunk_embedding": 8, "entity": 4,
             "work_item": 8, "work_item_archive": 2, "work_run": 8,
             "work_run_archive": 2, "work_artifact": 8, "audit_log": 8,
             "cost_ledger_daily": 2, "system_event": 4},
    "1k": {"doc": 1000, "doc_node": 4000, "chunk_embedding": 128, "entity": 64,
           "work_item": 1000, "work_item_archive": 100, "work_run": 1000,
           "work_run_archive": 100, "work_artifact": 1000, "audit_log": 1000,
           "cost_ledger_daily": 100, "system_event": 32},
    "10k": {"doc": 10000, "doc_node": 40000, "chunk_embedding": 1280, "entity": 640,
            "work_item": 10000, "work_item_archive": 1000, "work_run": 10000,
            "work_run_archive": 1000, "work_artifact": 10000, "audit_log": 10000,
            "cost_ledger_daily": 1000, "system_event": 320},
    "household-core-shape": {"doc": 7518, "doc_node": 187614, "chunk_embedding": 179684,
            "entity": 1489, "work_item": 25193, "work_item_archive": 54656,
            "work_run": 27659, "work_run_archive": 55784, "work_artifact": 65671,
            "audit_log": 440914, "cost_ledger_daily": 91259, "system_event": 596},
}
PROFILES[household.PROFILE] = household.COUNTS
# Not a claim to recreate every household table or document lifecycle distribution.
OMITTED_HOUSEHOLD_SHAPE = ["99 deleted docs", "301 additional revisions beyond one per stored Doc", "assertion", "attribute",
    "conflict_group", "event", "freshness", "identity_candidate", "ingest_decision", "media_asset",
    "predicate_alias", "predicate_registry", "rel", "source", "truth_slot", "migration_history"]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    with Path(path).open("xb") as f:
        f.write(canonical(value) + b"\n")
        f.flush()
        os.fsync(f.fileno())


def rid(table, i):
    return table + ":" + str(uuid.uuid5(uuid.NAMESPACE_URL, f"recall-synthetic-scale-v1:{table}:{i}"))


def base(table, i):
    return {"id": rid(table, i), "space_id": "default", "created_at": STAMP,
            "updated_at": STAMP, "created_by": "synthetic-scale", "provenance": {},
            "security": {"policy_id": "fixture-policy", "level": 0}}


def text(i):
    # Exact UTF-8, one bounded long line, and deterministic immutable span offsets.
    return f"Synthetic document {i}: αβ 🐘\n" + "bounded-original-text " * 16


def work(i, archived=False):
    row = base("work_item", i)
    row.update(kind="extract_doc", payload={"space_id": "default", "ordinal": i},
               status="done" if archived else "runnable", priority=i % 5,
               idempotency_key=f"synthetic:{i}", attempts=2)
    return row


def run(i, counts, archived=False):
    row = base("work_run", i)
    work_number = (counts["work_item"] + i % counts["work_item_archive"] if archived
                   else i % counts["work_item"])
    row.update(work_item=rid("work_item", work_number), attempt=1, started_at=STAMP,
               completed_at=STAMP, success=True, worker_id="synthetic-original",
               worker_version="fixture-v1", metrics={"tokens_used": 0, "steps_taken": 1,
               "wall_time_ms": 1, "estimated_cost_euros": 0.0, "custom": {}})
    return row


def row(table, i, counts):
    n = counts["doc"]
    result = base(table, i)
    if table == "doc":
        result.update(title=f"Synthetic {i}", text=text(i), rendition_key=rid(table, i), metadata={})
    elif table == "doc_revision":
        body = text(i)
        result.update(doc=rid("doc", i), text=body, text_hash=hashlib.sha256(body.encode()).hexdigest(),
                      renderer={"name": "synthetic-original", "version": "v1", "config_hash": None},
                      status="active", rendered_at=STAMP)
    elif table == "doc_node":
        doc = i % n
        result.update(revision=rid("doc_revision", doc), role="chunk", span={"start": 0, "end": len(text(doc).encode())},
                      node_key=f"original:{i}", position=i // n, depth=0)
    elif table == "chunk_embedding":
        # Exact f32-representable values; dimensions do not imply profile compatibility.
        result.update(node=rid("doc_node", i), model=f"legacy-synthetic-{i % 4}", dims=3072,
                      vector=[((j + i) % 17 - 8) / 8 for j in range(3072)])
    elif table == "entity":
        # The preservation loader rejects omitted serializer-present fields;
        # this is a complete synthetic original, not a request DTO.
        result.update(type="person", display_name=f"Synthetic person {i}", aliases=[],
                      summary=None, handles=[], identity_keys=[], risk_level=None,
                      trust_level=None, known_safe=False, first_seen=None, last_seen=None,
                      status="active", redirect_to=None, metadata={})
    elif table == "work_item":
        return work(i)
    elif table == "work_item_archive":
        original = work(counts["work_item"] + i, archived=True)
        result = dict(original, id=original["id"].replace("work_item:", "work_item_archive:"))
        result.update(original_id=original["id"], archived_at=STAMP, record=original)
        for key in list(result):
            if key not in base(table, i) and key not in {"original_id", "archived_at", "record"}:
                del result[key]
    elif table == "work_run":
        return run(i, counts)
    elif table == "work_run_archive":
        original = run(counts["work_run"] + i, counts, archived=True)
        result = {k: v for k, v in original.items() if k in base(table, i)}
        result.update(id=original["id"].replace("work_run:", "work_run_archive:"),
                      original_id=original["id"], archived_at=STAMP, record=original)
    elif table == "work_artifact":
        result.update(work_run=rid("work_run", i % counts["work_run"]), artifact_type="synthetic_original",
                      data={"original_ordinal": i}, status="draft")
    elif table == "audit_log":
        result.update(action="update", actor="synthetic-original", record=rid("doc", i % n),
                      before=None, after=None,
                      timestamp=STAMP, reason="original synthetic history")
    elif table == "cost_ledger_daily":
        result.update(date="2026-10-10", mode="graph", worker_kind=f"synthetic-{i}", total_euros=0.0,
                      total_items=1, total_tokens=0, total_runs=1, total_failures=0,
                      reserved_euros=0.0, reconciled_euros=0.0)
    elif table == "system_event":
        result.update(seq=i + 1, event_type="records.changed", payload={"original": i})
    else:
        raise ValueError(table)
    return result


def generate(root, profile, max_snapshot_bytes, operational_disposition="native"):
    require(operational_disposition in OPERATIONAL_DISPOSITIONS, "unsupported operational disposition")
    counts = dict(PROFILES[profile]); counts.setdefault("doc_revision", counts["doc"])
    full_shape = profile == household.PROFILE
    snapshot = root / "snapshot.json"
    h = hashlib.sha256(); total = 0
    with snapshot.open("xb") as f:
        def emit(raw):
            nonlocal total
            total += len(raw)
            if total > max_snapshot_bytes:
                raise ValueError("generated snapshot exceeds explicit byte bound")
            f.write(raw); h.update(raw)
        emit(b'{"backend":"elephant-db","version":1,"exported_at":' + canonical(STAMP) + b',"tables":{')
        for ti, table in enumerate(sorted(counts)):
            emit((b"," if ti else b"") + canonical(table) + b":[")
            for i in range(counts[table]):
                encoded = canonical(household.row(table, i, counts, base, rid,
                    lambda j: household.document_text(j, text), STAMP, row)
                    if full_shape else row(table, i, counts))
                require(len(encoded) <= 64 * 1024 * 1024, "synthetic row exceeds parser byte cap")
                emit((b"," if i else b"") + encoded)
            emit(b"]")
        emit(b"}}")
        f.flush(); os.fsync(f.fileno())
    # Fresh offline synthetic ledger backup follows the completed synthetic cut.
    ledger = sqlite3.connect(root / "ledger.sqlite")
    ledger.execute("CREATE TABLE decisions(doc_key TEXT PRIMARY KEY, keep INTEGER NOT NULL)")
    ledger.commit(); backup = sqlite3.connect(root / "ledger-backup.sqlite")
    ledger.backup(backup); backup.close(); ledger.close()
    connection = {"id":"fixture","provider":"openai","account_id":"unused",
                  "endpoint":"https://example.invalid/v1","credential_ref":"UNUSED",
                  "credential_generation":1,"revoked":False}
    config = {"method":"POST","path":"/v1/spaces","headers":{"X-Space-Id":"household"},
              "body":{"name":"household","security":{"policy_id":"fixture-policy","level":0},
              "embedding_provider":"openai","embedding_model":"synthetic","embedding_dims":3072,
              "processing":{"generation":1,"index_generation":0,"policy_id":"fixture-policy",
              "connections":{"fixture":connection},"profiles":{"embedding":{"id":"embedding",
              "connection_id":"fixture","model":"synthetic","model_version":"v1","dimensions":3072,
              "preprocessing":"text-full-v2"}},"bindings":{op:{"id":op,"profile_id":"embedding","revoked":False}
              for op in ["document_embedding","query_embedding"]},"active_embedding_profile_id":"embedding",
              "pending_reindex":None}}}
    save(root / "space-request.json", config)
    manifest = {"format":"recall-synthetic-scale-input-v1","profile":profile,"synthetic":True,
                "operational_disposition":operational_disposition,
                "tables":counts,"rows":sum(counts.values()),"snapshot_bytes":total,
                "snapshot_sha256":h.hexdigest(),"ledger_sha256":sha(root / "ledger-backup.sqlite"),
                "dimensions":3072,"vector_partitions":4,
                "live_documents":household.LIVE_DOCS if full_shape else counts["doc"],
                "deleted_documents":99 if full_shape else 0,
                "decoded_media_bytes":household.MEDIA_BYTES if full_shape else 0,
                "live_text_bytes":household.LIVE_TEXT_BYTES if full_shape else None,
                "omitted_household_tables":[] if full_shape else OMITTED_HOUSEHOLD_SHAPE,
                "unmatched_household_semantics":household.UNMATCHED if full_shape else OMITTED_HOUSEHOLD_SHAPE,
                "generation_maxrss_kib":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
    save(root / "input.json", manifest)


def disk_bytes(root):
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file() and not p.is_symlink())


def phase(name, command, root, env, deadline, disk_limit, reserve):
    started=time.monotonic(); usage=root / f"{name}.usage.json"
    wrapped=["/usr/bin/time","-f",'{"elapsed_seconds":%e,"maxrss_kib":%M,"exit_code":%x}',"-o",str(usage),*map(str,command)]
    failure=None; interrupted=None
    with (root / f"{name}.log").open("xb") as log:
        process=None
        try:
            with supervision.defer_launch_signals():
                process=supervision.Phase(wrapped,root,name,env,log)
            while not process.finished():
                if time.monotonic() >= deadline:
                    failure="overall admission deadline";break
                if disk_bytes(root) >= disk_limit - reserve:
                    failure="sampled disk high-water bound";break
                time.sleep(1)
        except BaseException as error:
            failure="controller interrupted"
            interrupted=error
        finally:
            if process is not None:
                try:
                    closure=process.close(terminate=failure is not None)
                except BaseException as cleanup_error:
                    if interrupted is not None:
                        interrupted.add_note(f'owned phase cleanup also failed: {cleanup_error!r}')
                        raise interrupted
                    raise
        if process is None:
            raise interrupted
    code=closure["exit_code"]
    if closure["unexpected_live_descendants_at_exit"]:
        failure="phase leader exited with live owned descendants"
    if closure.get("receipt_error"):
        failure="phase closure evidence could not be persisted"
    result={"phase":name,"seconds":time.monotonic()-started,"exit_code":code,
            "resource_limit_failure":failure,"command":list(map(str,command)),"disk_bytes":disk_bytes(root),
            "process_closure":closure,"log_sha256":sha(root / f"{name}.log")}
    if usage.exists():
        try: result["usage"]=json.loads(usage.read_text().splitlines()[-1])
        except (ValueError,IndexError): result["usage_parse_failed"]=True
    try:
        save(root / f"{name}.result.json",result)
    except BaseException as evidence_error:
        if interrupted is not None:
            interrupted.add_note(f'phase result evidence also failed: {evidence_error!r}')
            raise interrupted
        raise
    if interrupted is not None:
        raise interrupted
    if failure or code: raise RuntimeError(f"{name} failed; original log retained")
    return result


def verify_disposition(mode, fixture, plan, report):
    """Verify source coverage and storage location without calling archive rows native writes."""
    require(mode in OPERATIONAL_DISPOSITIONS, "unknown mode")
    require(fixture["operational_disposition"] == plan["operational_disposition"] == report["operational_disposition"] == mode,
            "input/plan/report disposition differs")
    source = fixture["tables"]
    archived = {t:n for t,n in source.items() if t in OPERATIONAL_TABLES} if mode == "managed_archive_v1" else {}
    copied = report["copied_tables"]
    require(report["archived_tables"] == archived, "archive declarations differ")
    require(report["source_tables"] == source, "source census differs")
    expected_accounting = {}
    for table, count in source.items():
        storage = ("recognized_global_retained_control" if table == "migration_history" else
                   "managed_archive_exact_lexical" if table in archived else "native_exact_projection")
        expected_accounting[table] = {"rows":count, "declared_storage":storage,
            "disposition":"declared_empty" if count == 0 else storage}
        expected_native = 0 if table in archived or table == "migration_history" else count
        require(copied.get(table, 0) == expected_native, "native copy count differs: "+table)
    require(not any((t not in source and n != 0) or (t in source and n != (0 if t in archived or t == "migration_history" else source[t]))
                    for t,n in copied.items()), "extra or double-counted native source population")
    require(report["source_accounting"] == {"operational_disposition":mode,"rows":sum(source.values()),"tables":expected_accounting},
            "complete native/archive/global/empty accounting differs")
    baseline = report["catchup_baseline"]
    require(baseline["operational_disposition"] == mode, "baseline disposition differs")
    holds = report["historical_work_holds"]
    require(holds["verified"] is True, "historical inertness unverified")
    for table in ("work_item", "work_item_archive"):
        require(holds["rows"].get(table,0) == source.get(table,0), "historical work census differs")
    result = {"mode":mode,"source_rows":sum(source.values()),"archived_rows":sum(archived.values()),
        "native_source_rows":sum(n for t,n in source.items() if t not in archived and t != "migration_history"),
        "retained_global_rows":source.get("migration_history",0),"archived_tables":archived}
    if mode == "native":
        require("operational_archive" not in report and "operational_archive" not in baseline, "native receipt must be absent")
        require(holds["mode"] == "native_historical_holds", "native hold mode differs")
    else:
        receipt = report["operational_archive"]
        require(baseline["operational_archive"] == receipt, "baseline archive receipt differs")
        binding, seal = receipt["binding"], receipt["seal"]
        require(binding["snapshot_sha256"] == fixture["snapshot_sha256"]
            and binding["spool_sha256"] == plan["inventory_sha256"] and binding["plan_id"] == plan["plan_id"]
            and binding["original_table_declarations"] == source and binding["parent_generation_sha256"] is None
            and binding["source_space_id"] == "default" and binding["target_space_id"] == "household"
            and binding["journal_identity"] == baseline["journal_identity"], "original archive source binding differs")
        inventory = plan["operational_archive_inventory"]
        require(seal["table_counts"] == inventory["tables"] == archived and not seal["redacted"], "archive census differs")
        require(seal["ordered_inventory_sha256"] == inventory["ordered_inventory_sha256"] == report["archive_ordered_inventory_sha256"], "exact archive inventory differs")
        require(report["archive_lexical_bytes"] == inventory["lexical_bytes"], "archive source byte census differs")
        require(holds["mode"] == "managed_archive_inert" and holds["native_holds"] == 0, "archive history was made runnable")
        require(receipt["registration"]["binding_sha256"] == seal["binding_sha256"]
            and receipt["registration"]["snapshot_sha256"] == fixture["snapshot_sha256"], "archive registration differs")
        result.update(archive_file_bytes=seal["bytes"],archive_file_sha256=seal["file_sha256"],
            archive_lexical_bytes=inventory["lexical_bytes"],archive_ordered_inventory_sha256=seal["ordered_inventory_sha256"])
    require(result["source_rows"] == result["native_source_rows"]+result["archived_rows"]+result["retained_global_rows"], "source rows counted twice")
    return result


def execute(args):
    require(args.max_seconds > 0 and args.max_snapshot_bytes > 0 and args.disk_reserve_bytes > 0, "positive execution bounds required")
    root=Path(args.output).resolve(); root.mkdir(mode=0o700,parents=False,exist_ok=False)
    started=time.monotonic(); deadline=started+args.max_seconds
    binary=Path(args.binary).resolve(); workspace=Path(args.workspace).resolve()
    if sha(binary) != args.binary_sha256: raise ValueError("binary digest differs")
    source_manifest=Path(args.source_manifest).resolve()
    require(sha(source_manifest)==args.source_manifest_sha256, "qualified source manifest differs")
    planner=workspace / "scripts/migration/preservation_plan.py"
    if sha(planner) != args.planner_sha256: raise ValueError("planner digest differs")
    if args.max_run_bytes <= args.disk_reserve_bytes or args.max_snapshot_bytes >= args.max_run_bytes:
        raise ValueError("invalid explicit disk bounds")
    if shutil.disk_usage(root).free < args.max_run_bytes: raise ValueError("insufficient admitted free disk")
    source={"binary_sha256":args.binary_sha256,"planner_sha256":args.planner_sha256,
            "driver_sha256":sha(__file__),"process_helper_sha256":sha(supervision.__file__),"household_helper_sha256":sha(household.__file__),"source_manifest_sha256":args.source_manifest_sha256,"profile":args.profile,"operational_disposition":args.operational_disposition,"clock":"starts before fresh synthetic generation",
            "limits":vars(args),"disk_enforcement":"1s sampled high-water with explicit reserve; no filesystem quota/hard RSS guarantee"}
    save(root / "binding.json",source)
    env={k:v for k,v in os.environ.items() if not any(token in k for token in ["API_KEY","TOKEN","PASSWORD","SECRET"])}
    env.update(ELEPHANT_DURABILITY_PATH=str(root / "journal"),PYTHONDONTWRITEBYTECODE="1",
               SURREALDB_USER="root",SURREALDB_PASS="root")
    phases=[]
    try:
        phases.append(phase("01-generate",[sys.executable,__file__,"_generate","--output",root,
            "--profile",args.profile,"--max-snapshot-bytes",args.max_snapshot_bytes,
            "--operational-disposition",args.operational_disposition],root,env,deadline,args.max_run_bytes,args.disk_reserve_bytes))
        fixture=json.loads((root / "input.json").read_text())
        require(fixture["operational_disposition"] == args.operational_disposition, "fresh fixture disposition differs")
        phases.append(phase("02-plan",[sys.executable,planner,"inventory","--snapshot",root/"snapshot.json",
            "--snapshot-sha256",fixture["snapshot_sha256"],"--ledger-backup",root/"ledger-backup.sqlite",
            "--ledger-sha256",fixture["ledger_sha256"],"--ledger-cut-order","store_cut_then_ledger_backup",
            "--space","default","--target-space","household","--operational-disposition",args.operational_disposition,"--output",root/"plan"],root,env,deadline,args.max_run_bytes,args.disk_reserve_bytes))
        plan=json.loads((root/"plan/plan.json").read_text())
        require(plan["tables"] == fixture["tables"] and plan["documents"] == fixture["live_documents"], "planned counts differ")
        require(not plan["runtime_blockers"], "planner runtime blockers present")
        require(plan["operational_disposition"] == args.operational_disposition, "planner disposition differs")
        phases.append(phase("03-transfer",[binary,"data-migrate","--preservation-plan",root/"plan/plan.json",
            "--historical-config",root/"space-request.json","--url",f"rocksdb://{root}/target",
            "--namespace","synthetic","--database","scale","--report",root/"transfer.json"],root,env,deadline,args.max_run_bytes,args.disk_reserve_bytes))
        report=json.loads((root/"transfer.json").read_text())
        require(report["format"]=="recall-legacy-transfer-v2", "wrong transfer format")
        require(report["state"]=="initial_history_reconciled_vector_coverage_pending", "wrong transfer terminal state")
        require(report["source_tables"]==fixture["tables"], "transferred source counts differ")
        # Retained global controls and declared empty legacy tables are not
        # copied tenant rows. Check their explicit disposition, never count them
        # as writes or silently drop a nonempty unsupported table.
        retained = report["retained_source_controls"]["migration_history"]
        require(len(retained) == fixture["tables"].get("migration_history", 0), "retained control count differs")
        if retained:
            expected_control = household.row("migration_history", 0, fixture["tables"], base, rid, text, STAMP, row)
            require(retained == [expected_control], "retained control payload differs")
        disposition = verify_disposition(args.operational_disposition, fixture, plan, report)
        if fixture["profile"] == household.PROFILE:
            media = report["media_preservation"]
            require(media == plan["media_preservation"], "media proof differs from exact planner proof")
            require(media["rows"] == 86 and media["inline_rows"] == 86
                    and media["decoded_inline_bytes"] == fixture["decoded_media_bytes"]
                    and media["external_rows"] == 0 and media["self_contained"]
                    and media["inline_size_sha256_verified"], "full-shape media custody differs")
        require(report["source_sha256"]==fixture["snapshot_sha256"], "transferred snapshot differs")
        baseline=report["catchup_baseline"]
        require(baseline["expected_ledger_sha256"]==sha(root/"transfer.transfer.sqlite"), "closed ledger differs")
        require(baseline["bindings_sha256"]==sha(root/"transfer.catchup-bindings.jsonl"), "closed bindings differ")
        with sqlite3.connect((root/"transfer.transfer.sqlite").as_uri()+"?mode=ro&immutable=1", uri=True) as db:
            native_expected = dict(db.execute("SELECT t,count(*) FROM expected GROUP BY t"))
            if args.operational_disposition == "managed_archive_v1":
                require(not (set(native_expected) & OPERATIONAL_TABLES), "archived row entered the closed native inventory")
        disposition["closed_native_inventory_rows"] = sum(native_expected.values())
        disposition["closed_native_inventory_tables"] = native_expected
        require(not (root/"journal/transfer-pending.json").exists(), "transfer remains pending")
        with sqlite3.connect((root/"plan/inventory.sqlite").as_uri()+"?mode=ro",uri=True) as db:
            actual=dict(db.execute("SELECT t,count(*) FROM rows GROUP BY t"))
            require(actual=={t:n for t,n in fixture["tables"].items() if n}, "actual SQLite rows differ")
        supervision.finish_result(root,{"state":"SYNTHETIC_INITIAL_PATH_PASS","phases":phases,
            "seconds_including_generation":time.monotonic()-started,"phase_seconds":report["phase_seconds"],
            "table_phases":report["table_phases"],"input":fixture,"disposition":disposition,
            "operational_disposition":args.operational_disposition,
            "archive_table_phases":report.get("archive_table_phases"),"archive_seal_seconds":report.get("archive_seal_seconds"),"report_sha256":sha(root/"transfer.json"),
            "whole_household_requirement_qualified":False,"provider_calls":False,
            "remaining_clock_phases":["live original-store export","transport","final cut/delta","embedding rebuild","activation/certification","consumer switch"]},started,deadline,save)
    except BaseException as error:
        try:
            save(root/"failure.json",{"state":"FAILED_OR_INTERRUPTED","error":str(error),"phases":phases,
                "seconds_including_generation":time.monotonic()-started,"no_retry_or_cleanup":True})
        except BaseException as evidence_error:
            error.add_note(f'failure evidence also failed: {evidence_error!r}')
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    subs=parser.add_subparsers(dest="command",required=True)
    gen=subs.add_parser("_generate",help=argparse.SUPPRESS)
    for p in [gen,subs.add_parser("run")]:
        p.add_argument("--output",required=True);p.add_argument("--profile",choices=PROFILES,required=True)
        p.add_argument("--max-snapshot-bytes",type=int,required=True)
        p.add_argument("--operational-disposition",choices=OPERATIONAL_DISPOSITIONS,default="native")
    run_parser=subs.choices["run"]
    for name in ["binary","binary-sha256","workspace","planner-sha256","source-manifest","source-manifest-sha256"]:run_parser.add_argument("--"+name,required=True)
    for name in ["max-run-bytes","disk-reserve-bytes","max-seconds"]:run_parser.add_argument("--"+name,type=int,required=True)
    args=parser.parse_args()
    if args.command=="_generate":generate(Path(args.output),args.profile,args.max_snapshot_bytes,args.operational_disposition)
    else:
        with supervision.interruptible():
            execute(args)

if __name__=="__main__":main()
