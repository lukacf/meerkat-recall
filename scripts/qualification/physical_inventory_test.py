"""Pure counterexamples for independent physical evidence; no installed verdict."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import sys
import tarfile
import unittest


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


P = load("physical_inventory", "physical_inventory.py")
F = load("physical_original_fixture", "snapshot_report_oracle_test.py")
O = F.ORACLE
L = load("native_private_membership", "native_private_membership.py")
LF = load("physical_private_fixture", "native_private_membership_test.py")


def raw(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def tar(root, files=None, extra=()):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        directories = {root}
        for name in files or {}:
            directories.update(str(parent) for parent in P.PurePosixPath(root, name).parents if str(parent) != ".")
        for name in sorted(directories, key=lambda x: (x.count("/"), x)):
            member = tarfile.TarInfo(name)
            member.type = tarfile.DIRTYPE
            archive.addfile(member)
        for name, data in (files or {}).items():
            member = tarfile.TarInfo(root + "/" + name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
        for member in extra:
            archive.addfile(member)
    return output.getvalue()


def checkpoint(tables, journal, *, changes=()):
    store = "00000000-0000-4000-8000-000000000099"
    payload = {"backend": "elephant-db", "version": 1, "exported_at": F.NOW, "tables": tables}
    body = raw(payload)
    identity = {"version": 1, "store_id": store, "journal_id": journal, "initialized": True}
    preimage = b"recall-embedded-checkpoint-v1\0" + store.encode() + b"\0" + journal.encode() + b"\0" + struct.pack(">Q", 0) + body
    envelope = {"format": "recall-embedded-checkpoint-v1", "store_id": store, "journal_id": journal,
                "sequence": 0, "sha256": P.sha(preimage), "data": body.decode()}
    header = raw({"version": 1, "store_id": store, "journal_id": journal, "base_sequence": 0})
    wal = b"RCLOG001" + struct.pack(">I", len(header)) + header + hashlib.sha256(header).digest()
    for index, value in enumerate(changes, 1):
        payload = raw(value)
        numbers = struct.pack(">QQ", index, len(payload))
        prefix = b"RCDEL001" + numbers + hashlib.sha256(numbers + payload).digest()
        wal += prefix + hashlib.sha256(prefix).digest() + payload + b"COMMIT01"
    return {"state.snapshot": raw(envelope), "state.snapshot.durable/identity.json": raw(identity),
            "state.snapshot.durable/commits.wal": wal}


def checked(payload, domain=b"meerkat-recall/reference-journal/v1\0"):
    return raw({"payload": payload, "checksum": P.sha(domain + raw(payload))})


def fixture(native=False):
    reports, evidence = F.fixture(native=native)
    journal = evidence["journal"]["journal_id"]
    fingerprint = evidence["journal"]["reference_key_fingerprint"]
    intent = evidence["journal"]["original_erasure_intent"]
    if native:
        for report in reports[1:]:
            report["rows"].append(F.row("source_erasure_guard", "original-guard", {
                "space_id": evidence["A"]["space_id"], "journal_id": journal, "key_fingerprint": fingerprint,
                "version": 1, "membership_count": 0, "revision": 1}, True))
            F.refresh(report)
    snapshots, registries, tables = {}, {}, {}
    for phase, report in zip(("pre", "post", "replay"), reports):
        table = {}
        for row in report["rows"]:
            _, logical = O.TaggedDecoder().value(row["record"])
            if native:
                # Native inspection represents its physical ID as a Thing;
                # the synthetic backup bytes below retain a JSON spelling.
                logical["id"] = logical["id"].table + ":" + logical["id"].identifier
            table.setdefault(row["identity"]["table"], []).append(logical)
        tables[phase] = table
        payload = raw({"backend": evidence["backend"], "version": 1, "exported_at": F.NOW, "tables": table})
        manifest = report["manifest"]
        manifest.update(bytes=len(payload), sha256=P.sha(payload), expires_at="2026-10-07T02:20:00Z")
        report["manifest"] = manifest
        evidence["snapshots"][phase] = {"capture": phase + ".json", "sha256": P.sha(raw(report)),
                                        "bytes": len(raw(report)), "manifest": manifest}
        snapshots[phase + "/manifest.json"] = raw(manifest)
        snapshots[phase + "/data.snapshot"] = payload
        if native:
            snapshots[phase + "/history-origin.json"] = backup_origin(evidence, manifest)
        registries[manifest["snapshot_id"] + ".json"] = raw({"path": P.PREFIX + "backups/" + phase,
            "snapshot_id": manifest["snapshot_id"], "expires_at": manifest["expires_at"],
            "raw_sha256": None, "deleting": False, "temporary": False})
    captures = {}
    for phase, minute in (("pre", 0), ("post", 2), ("replay", 3), ("expired", 21)):
        active = ["pre"] if phase == "pre" else ["pre", "post"] if phase == "post" else ["pre", "post", "replay"] if phase == "replay" else []
        table = copy.deepcopy(tables["pre" if phase == "pre" else "post"])
        if phase != "pre":
            table["source_erasure_guard"] = [{"id": {"tb": "source_erasure_guard", "id": evidence["A"]["space_id"]},
                "space_id": evidence["A"]["space_id"], "journal_id": journal, "key_fingerprint": fingerprint,
                "version": 1, "membership_count": 0, "revision": 1}]
        data = checkpoint(table, journal)
        memberships = {} if phase == "pre" else {intent["intent_id"] + ".json": checked({
            "version": 1, "journal_identity": journal, "key_fingerprint": fingerprint,
            "intent_id": intent["intent_id"], "intent_digest": P.sha(b"meerkat-recall/reference-journal/v1\0" + raw(intent)), "tokens": []})}
        index = checked({"version": 1, "journal_identity": journal, "key_fingerprint": fingerprint,
                         "intents": [] if phase == "pre" else [intent["intent_id"]]})
        captures[phase] = {"stopped": True, "journal_id": journal, "observed_at": f"2026-10-07T02:{minute:02}:00Z",
            "archives": {"data": tar("data", data), "database": tar("database"),
                         "backups": tar("backups", {name: value for name, value in snapshots.items() if name.split('/')[0] in active}),
                         "registry": tar("backups", {name: value for name, value in registries.items()
                             if json.loads(value)["path"].split('/')[-1] in active})},
            "membership": {"index": index, "fingerprint": fingerprint.encode(), "files": tar("reference-memberships", memberships)}}
    return captures, evidence, reports


def verdict(captures, evidence, reports, export=None):
    return P.evaluate(captures, evidence, export or {"verdict": "pass", "scope": "exported_inventory", "product_acceptance": False},
                      oracle=O, original_report=raw(reports[0]))


def original_origin():
    return {"content": "a" * 64, "origin_mode": "versionless_current", "original_version": 0,
            "original_timestamp": 0, "causal_sources": []}


def backup_origin(evidence, manifest):
    original = {key: manifest[key] for key in P.MANIFEST_FIELDS}
    payload = {"kind": "meerkat-recall-backup-history-origin/v1", "journal_identity": evidence["journal"]["journal_id"],
        "key_fingerprint": evidence["journal"]["reference_key_fingerprint"], "namespace": "offline", "database": "fixture",
        "snapshot_id": manifest["snapshot_id"], "snapshot_sha256": manifest["sha256"],
        "manifest_sha256": P.sha(raw(original)), "rows": [original_origin()]}
    return raw({"payload": payload, "authentication": "c" * 64})


def imported_origin(evidence):
    manifest = evidence["snapshots"]["pre"]["manifest"]
    payload = {"kind": "meerkat-recall-imported-history-origin/v1", "journal_identity": evidence["journal"]["journal_id"],
        "key_fingerprint": evidence["journal"]["reference_key_fingerprint"], "scopes": [{"namespace": "offline", "database": "fixture", "pending_import": None,
            "rows": [{"origin": original_origin(), "imported_version": 100, "imported_timestamp": 200,
                      "snapshot_id": manifest["snapshot_id"], "snapshot_sha256": manifest["sha256"]}]}]}
    return raw({"payload": payload, "authentication": "d" * 64})


def origin_capture(evidence, capture):
    capture["membership"]["origins"] = imported_origin(evidence)
    index = json.loads(capture["membership"]["index"])["payload"]
    index["restore_origins_required"] = True
    capture["membership"]["index"] = checked(index)


def native_capture():
    """Source-only complete fresh-generation evidence, not engine execution."""
    _, evidence, _ = fixture(native=True)
    start = int(P.timestamp(evidence["journal"]["erase_started_at"]).timestamp() * 1000)
    old_directory, current_directory = {"device": 2, "inode": 10}, {"device": 2, "inode": 20}
    def generations(identity_inode, sst_inode):
        return [{"name": "IDENTITY", "bytes": 11, "device": 2, "inode": identity_inode},
                {"name": "000001.sst", "bytes": 10, "device": 2, "inode": sst_inode}]
    checkpoint = dict(zip(P.NATIVE_CHECKPOINT_FIELDS, (1, old_directory, current_directory,
        "old-engine", "new-engine", 20, "a" * 64, start, start + 1000, None,
        generations(11, 12), generations(21, 22), 0, 0, True, False)))
    receipt = {"format": "recall-rocksdb-physical-checkpoint/v1", "engine": "rocksdb",
               "wrapper_version": "0.24.0-surreal.5", "current": checkpoint,
               "current_sha256": P.sha(raw(checkpoint)), "previous": None}
    control_hash = P.sha(b"/data/database.db")
    database = {"database.db/IDENTITY": b"new-engine\n", "database.db/000001.sst": b"compressed-survivors",
        "database.db/LOG": b"fresh shutdown log", ".database.db.recall-physical-checkpoint.json": raw(receipt),
        ".database.db.recall-native-owner.lock": b"",
        ".recall-physical-" + control_hash + "/owner": b"recall-native-generation-v1\n" + control_hash.encode()}
    captures = {root: tar("backups" if root == "registry" else root, database if root == "database" else {}) for root in P.ROOTS}
    capture = {"archives": captures, "observed_at": "2026-10-07T02:02:00Z", "stopped": True,
               "maintenance_started_at": evidence["journal"]["erase_started_at"]}
    files, directories = {}, set()
    for root, archive in captures.items():
        files.update(P.archive(archive, root, directory_inventory=directories))
    metadata = {}
    for index, name in enumerate(sorted((set(files) | directories) - {"registry"})):
        if name.startswith("registry/"):
            continue
        kind = "f" if name in files else "d"
        metadata[name] = {"kind": kind, "device": 2, "inode": 100 + index,
                          "bytes": len(files[name]) if kind == "f" else 4096, "links": 1 if kind == "f" else 2}
    metadata[P.NATIVE_ROOT].update(current_directory)
    # A retired inode may be reused by a later flush in the proven fresh root.
    metadata[P.NATIVE_ROOT + "/LOG"]["inode"] = 12
    capture["metadata"] = metadata_bytes(metadata)
    return capture, files, evidence, receipt, metadata


def metadata_bytes(values):
    return b"".join(("\0".join(str(row[key]) for key in ("kind", "device", "inode", "bytes", "links"))
                     + "\0/physical/" + name + "\0").encode() for name, row in values.items())


def history_fixture():
    captures, evidence, reports = fixture()
    intent = evidence["journal"]["original_erasure_intent"]
    scope = {"history_fingerprints": ["a" * 64], "erased_versions": ["b" * 64]}
    proof = {"version": 1, "journal_identity": evidence["journal"]["journal_id"],
        "key_fingerprint": evidence["journal"]["reference_key_fingerprint"],
        "intent_id": intent["intent_id"],
        "intent_digest": P.sha(b"meerkat-recall/history-journal/v1\0" + raw(intent)), "scopes": {"1" * 64: scope}}
    for phase in ("post", "replay", "expired"):
        member = captures[phase]["membership"]
        index = json.loads(member["index"])["payload"]
        index["history_intents"] = [intent["intent_id"]]
        member["index"] = checked(index)
        member["history"] = tar("history-commitments", {
            intent["intent_id"] + ".json": checked(proof, b"meerkat-recall/history-journal/v1\0")})
    return captures, evidence, reports, proof


def complete_native_fixture():
    """Composed synthetic captures; never an assertion about actual native SQL."""
    captures, evidence, reports = fixture(native=True)
    _, post_files, _, post_receipt, _ = native_capture()
    replay = copy.deepcopy(post_receipt)
    replay["previous"] = copy.deepcopy(post_receipt["current"])
    replay["current"].update(sequence=2, old_directory={"device": 2, "inode": 20},
        current_directory={"device": 2, "inode": 30}, old_engine_identity="new-engine", engine_identity="replayed-engine",
        previous_sha256=post_receipt["current_sha256"], started_unix_ms=post_receipt["current"]["started_unix_ms"] + 150000,
        completed_unix_ms=post_receipt["current"]["started_unix_ms"] + 151000)
    replay["current_sha256"] = P.sha(raw(replay["current"]))
    for phase, capture in captures.items():
        database = {name.split("/", 1)[1]: value for name, value in post_files.items() if name.startswith("database/")}
        directory_inode = 20
        if phase == "pre":
            database = {"database.db/IDENTITY": b"old-engine\n", "database.db/000001.sst": b"compressed-original"}
            directory_inode = 10
        elif phase in ("replay", "expired"):
            database["database.db/IDENTITY"] = b"replayed-engine\n"
            database[".database.db.recall-physical-checkpoint.json"] = raw(replay)
            directory_inode = 30
        capture["archives"]["database"] = tar("database", database)
        capture["archives"]["data"] = tar("data")
        files, directories = {}, set()
        for root, archive in capture["archives"].items():
            files.update(P.archive(archive, root, directory_inventory=directories))
        metadata = {name: {"kind": "f" if name in files else "d", "device": 2, "inode": 1000 + index,
            "bytes": len(files[name]) if name in files else 4096, "links": 1 if name in files else 2}
            for index, name in enumerate(sorted(set(files) | directories)) if not name.startswith("registry")}
        metadata[P.NATIVE_ROOT]["inode"] = directory_inode
        capture["metadata"] = metadata_bytes(metadata)
        capture["maintenance_started_at"] = evidence["journal"]["erase_started_at"] if phase == "post" else "2026-10-07T02:02:30Z"
        capture["private_membership"] = raw(LF.wire([]))
    return captures, evidence, reports


class PhysicalInventory(unittest.TestCase):
    def test_origin_control_accounting_never_claims_secret_mac_verification(self):
        captures, evidence, _ = fixture(native=True)
        manifest = evidence["snapshots"]["pre"]["manifest"]
        decoded = P.origin_envelope(backup_origin(evidence, manifest), evidence, manifest=manifest)
        self.assertFalse(decoded["mac_verified"])
        self.assertFalse(decoded["product_acceptance"])
        origin_capture(evidence, captures["replay"])
        P.journal_memberships(captures["replay"], evidence)
        restored = P.restored_origins(captures["replay"], evidence)
        self.assertTrue(restored["required"])
        self.assertEqual(len(restored["identities"]), 1)
        self.assertFalse(restored["mac_verified"])

    def test_origin_sidecar_full_manifest_and_original_scope_cannot_be_substituted(self):
        _, evidence, _ = fixture(native=True)
        manifest = evidence["snapshots"]["pre"]["manifest"]
        original = json.loads(backup_origin(evidence, manifest))
        for mutation in (lambda p: p.update(namespace="foreign"), lambda p: p.update(snapshot_sha256="e" * 64),
                         lambda p: p.update(journal_identity=F.IDS[9]), lambda p: p.update(key_fingerprint="e" * 64),
                         lambda p: p.update(snapshot_id=F.IDS[9]), lambda p: p.update(manifest_sha256="f" * 64)):
            value = copy.deepcopy(original)
            mutation(value["payload"])
            with self.assertRaises(P.Incomplete):
                P.origin_envelope(raw(value), evidence, manifest=manifest)
        changed = dict(manifest, expires_at="2026-10-08T02:20:00Z")
        with self.assertRaisesRegex(P.Incomplete, "exact_original_manifest_binding"):
            P.origin_envelope(raw(original), evidence, manifest=changed)

    def test_origin_unknown_payload_duplicate_tokens_and_alternate_canonical_bytes_refuse(self):
        _, evidence, _ = fixture(native=True)
        manifest = evidence["snapshots"]["pre"]["manifest"]
        original = json.loads(backup_origin(evidence, manifest))
        for mutation in (lambda p: p["rows"][0].update(raw_key="retained-source"),
                         lambda p: p["rows"][0].update(causal_sources=["e" * 64, "e" * 64]),
                         lambda p: p["rows"][0].update(original_version=True),
                         lambda p: p["rows"][0].update(origin_mode="native_versions"),
                         lambda p: p["rows"].append(copy.deepcopy(p["rows"][0]))):
            value = copy.deepcopy(original)
            mutation(value["payload"])
            with self.assertRaises(P.Incomplete):
                P.origin_envelope(raw(value), evidence, manifest=manifest)
        for invalid in (raw(original) + b"\n", json.dumps(original, indent=2).encode(),
                        raw(original).replace(b'"original_version":0', b'"original_version":0,"original_version":0')):
            with self.assertRaises(P.Incomplete):
                P.origin_envelope(invalid, evidence, manifest=manifest)

    def test_origin_acknowledged_file_or_entire_anchor_loss_refuses(self):
        captures, evidence, _ = fixture(native=True)
        capture = captures["post"]
        origin_capture(evidence, capture)
        initial = P.restored_origins(capture, evidence)
        capture["membership"]["origins"] = None
        with self.assertRaisesRegex(P.Incomplete, "acknowledged_file_inventory"):
            P.restored_origins(capture, evidence)
        index = json.loads(capture["membership"]["index"])["payload"]
        del index["restore_origins_required"]
        capture["membership"]["index"] = checked(index)
        with self.assertRaisesRegex(P.Violation, "anchor_lost"):
            P.origins_monotonic(initial, P.restored_origins(capture, evidence))
        capture["membership"]["origins"] = imported_origin(evidence)
        with self.assertRaisesRegex(P.Incomplete, "acknowledged_file_inventory"):
            P.restored_origins(capture, evidence)

    def test_origin_exact_imported_version_causality_cannot_shrink_or_change(self):
        captures, evidence, _ = fixture(native=True)
        capture = captures["post"]
        origin_capture(evidence, capture)
        initial = P.restored_origins(capture, evidence)
        for change in (lambda row: row.update(imported_version=101),
                       lambda row: row["origin"].update(causal_sources=["e" * 64]),
                       lambda row: row.update(snapshot_sha256="f" * 64)):
            payload = json.loads(imported_origin(evidence))
            change(payload["payload"]["scopes"][0]["rows"][0])
            capture["membership"]["origins"] = raw(payload)
            with self.assertRaisesRegex(P.Violation, "binding_changed_or_lost"):
                P.origins_monotonic(initial, P.restored_origins(capture, evidence))

    def test_origin_zero_version_import_binding_and_unknown_scope_refuse(self):
        _, evidence, _ = fixture(native=True)
        for change in (lambda scope: scope["rows"][0].update(imported_version=0, imported_timestamp=0),
                       lambda scope: scope.update(database="other")):
            payload = json.loads(imported_origin(evidence))
            change(payload["payload"]["scopes"][0])
            with self.assertRaises(P.Incomplete):
                P.origin_envelope(raw(payload), evidence)
        captures, evidence, _ = fixture(native=True)
        capture = captures["replay"]
        origin_capture(evidence, capture)
        payload = json.loads(capture["membership"]["origins"])
        manifest = evidence["snapshots"]["pre"]["manifest"]
        payload["payload"]["scopes"][0]["pending_import"] = {"snapshot_id": manifest["snapshot_id"],
            "snapshot_sha256": manifest["sha256"], "origin_sha256": "e" * 64, "max_preimport_version": 99}
        capture["membership"]["origins"] = raw(payload)
        self.assertFalse(P.origin_envelope(capture["membership"]["origins"], evidence)["mac_verified"])
        with self.assertRaisesRegex(P.Incomplete, "pending_import_not_completed"):
            P.restored_origins(capture, evidence)

    def test_restore_origin_escaped_payload_failure_survives_damaged_index(self):
        captures, evidence, reports = fixture()
        captures["post"]["membership"]["origins"] = raw({"hidden": evidence["A"]["canary"]}).replace(b"urn", b"\\u0075rn")
        captures["post"]["membership"]["index"] = b"incomplete"
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "fail", result)
        self.assertIn("post_positive_original_canary_in_restore_origins", result["failures"])

    def test_native_registered_origin_sidecar_is_required_and_cannot_outlive_original_retention(self):
        captures, evidence, reports = complete_native_fixture()
        post = P.archive(captures["post"]["archives"]["backups"], "backups")
        del post["backups/post/history-origin.json"]
        captures["post"]["archives"]["backups"] = tar("backups", {name.split("/", 1)[1]: value for name, value in post.items()})
        result = verdict(captures, evidence, reports)
        self.assertIn("native_registered_backup_origin_missing", result["unmet"])
        captures, evidence, reports = complete_native_fixture()
        pre = P.archive(captures["pre"]["archives"]["backups"], "backups")
        captures["expired"]["archives"]["backups"] = tar("backups", {"pre/history-origin.json": pre["backups/pre/history-origin.json"]})
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "fail", result)
        self.assertIn("expired_expired_original_generation_retained", result["failures"])

    def test_composed_native_generation_private_export_and_retention_evidence(self):
        captures, evidence, reports = complete_native_fixture()
        result = P.evaluate(captures, evidence, {"verdict": "pass", "scope": "exported_inventory", "product_acceptance": False},
            oracle=O, original_report=raw(reports[0]), private_oracle=L,
            phase_reports={phase: raw(report) for phase, report in zip(("post", "replay"), reports[1:])})
        self.assertEqual(result["verdict"], "pass", result)
        self.assertFalse(result["product_acceptance"])
        self.assertEqual(len([check for check in result["checks"] if "complete_private_memberships" in check]), 3)

    def test_private_escaped_payload_failure_survives_missing_generation_receipt(self):
        captures, evidence, reports = complete_native_fixture()
        private = LF.wire([])
        private[3]["result"] = [{"hidden": evidence["A"]["canary"]}]
        captures["post"]["private_membership"] = raw(private).replace(b"urn", b"\\u0075rn")
        captures["post"]["archives"]["database"] = tar("database", {"database.db/IDENTITY": b"new-engine\n"})
        result = P.evaluate(captures, evidence, {"verdict": "pass", "scope": "exported_inventory", "product_acceptance": False},
            oracle=O, original_report=raw(reports[0]), private_oracle=L, phase_reports={})
        self.assertEqual(result["verdict"], "fail", result)
        self.assertIn("erased_canary_in_private_response", result["failures"])
        self.assertTrue(result["unmet"])

    def test_private_escaped_payload_failure_survives_malformed_original_journal(self):
        captures, evidence, reports = complete_native_fixture()
        private = LF.wire([])
        private[3]["result"] = [{"hidden": evidence["A"]["canary"]}]
        captures["post"]["private_membership"] = raw(private).replace(b"urn", b"\\u0075rn")
        captures["post"]["membership"]["index"] = b"incomplete"
        result = P.evaluate(captures, evidence, {"verdict": "pass", "scope": "exported_inventory", "product_acceptance": False},
            oracle=O, original_report=raw(reports[0]), private_oracle=L, phase_reports={})
        self.assertEqual(result["verdict"], "fail", result)
        self.assertIn("post_positive_original_canary_in_private_native_rows", result["failures"])
        self.assertTrue(result["unmet"])

    def test_history_proof_binds_original_intent_with_payload_free_commitments(self):
        captures, evidence, reports, proof = history_fixture()
        tokens, histories = P.journal_memberships(captures["post"], evidence, with_history=True)
        self.assertEqual(tokens, {evidence["A"]["space_id"]: set()})
        self.assertEqual(histories, {proof["intent_id"]: proof["scopes"]})
        self.assertEqual(verdict(captures, evidence, reports)["verdict"], "pass")
        # JSON object order does not replace the producer's defined struct order.
        envelope = json.loads(checked(proof, b"meerkat-recall/history-journal/v1\0"))
        envelope["payload"]["scopes"]["1" * 64] = dict(reversed(list(proof["scopes"]["1" * 64].items())))
        captures["post"]["membership"]["history"] = tar("history-commitments", {proof["intent_id"] + ".json": raw(envelope)})
        self.assertEqual(P.journal_memberships(captures["post"], evidence, with_history=True)[1], histories)

    def test_history_acknowledged_missing_or_unindexed_files_never_qualify(self):
        captures, evidence, _, proof = history_fixture()
        capture = captures["post"]
        for history in (None, tar("history-commitments"), tar("history-commitments", {".recall-write-incomplete": b"pending"})):
            invalid = copy.deepcopy(capture)
            invalid["membership"]["history"] = history
            with self.assertRaisesRegex(P.Incomplete, "complete_acknowledged_file_inventory"):
                P.journal_memberships(invalid, evidence)
        index = json.loads(capture["membership"]["index"])["payload"]
        del index["history_intents"]
        capture["membership"]["index"] = checked(index)
        with self.assertRaisesRegex(P.Incomplete, "complete_acknowledged_file_inventory"):
            P.journal_memberships(capture, evidence)

    def test_history_unknown_payload_wrong_identity_digest_and_duplicates_refuse(self):
        captures, evidence, _, original = history_fixture()
        for change in (lambda p: p.update(payload="source-body"), lambda p: p.update(journal_identity=F.IDS[9]),
                       lambda p: p.update(version=True), lambda p: p.update(intent_digest="f" * 64),
                       lambda p: p["scopes"]["1" * 64].update(erased_versions=["b" * 64] * 2),
                       lambda p: p["scopes"]["1" * 64].update(history_fingerprints=[]),
                       lambda p: p["scopes"]["1" * 64].update(erased_versions=["raw-record-key"])):
            proof = copy.deepcopy(original)
            change(proof)
            captures["post"]["membership"]["history"] = tar("history-commitments", {
                original["intent_id"] + ".json": checked(proof, b"meerkat-recall/history-journal/v1\0")})
            with self.assertRaises(P.Incomplete):
                P.journal_memberships(captures["post"], evidence)

    def test_history_checksum_domains_are_not_interchangeable(self):
        captures, evidence, _, proof = history_fixture()
        captures["post"]["membership"]["history"] = tar("history-commitments", {proof["intent_id"] + ".json": checked(proof)})
        with self.assertRaisesRegex(P.Incomplete, "membership_checksum"):
            P.journal_memberships(captures["post"], evidence)

    def test_acknowledged_history_cannot_disappear_across_original_replay(self):
        captures, evidence, reports, proof = history_fixture()
        changed = copy.deepcopy(proof)
        changed["scopes"]["1" * 64]["erased_versions"] = []
        captures["replay"]["membership"]["history"] = tar("history-commitments", {
            proof["intent_id"] + ".json": checked(changed, b"meerkat-recall/history-journal/v1\0")})
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "fail", result)
        self.assertIn("acknowledged_history_commitments_lost", result["failures"])

    def test_surrealkv_receipt_uses_own_version_clock_checksum_and_inventory(self):
        _, _, _, receipt, _ = native_capture()
        old = receipt["current"]
        fields = (*P.NATIVE_CHECKPOINT_FIELDS[:6], "high_water", *P.NATIVE_CHECKPOINT_FIELDS[6:])
        old.update(old_engine_identity=F.IDS[8], engine_identity=F.IDS[9], high_water=150)
        for key in ("original_payload_generations", "current_payload_generations"):
            old[key][0]["name"] = "recall-engine.identity"
            old[key][0]["bytes"] = 36
            old[key][1]["name"] = "clog/00000000000000000001.clog"
            old[key] += [{"name": "manifest/00000000000000000001.manifest", "bytes": 50, "device": 2, "inode": 90},
                         {"name": "recall-checkpoint.clock", "bytes": 20, "device": 2, "inode": 91}]
            old[key].sort(key=lambda value: value["name"])
        receipt.update(format="recall-surrealkv-physical-checkpoint/v1", engine="surrealkv", wrapper_version="0.9.3",
                       current={key: old[key] for key in fields})
        receipt["current_sha256"] = P.sha(raw(receipt["current"]))
        self.assertEqual(P.native_receipt(raw(receipt))["current"]["high_water"], 150)
        receipt["current"]["high_water"] = 151
        with self.assertRaisesRegex(P.Incomplete, "current_checksum"):
            P.native_receipt(raw(receipt))
        receipt["current_sha256"] = P.sha(raw(receipt["current"]))
        for name in ("manifest/00000000000000000001.manifest", "recall-checkpoint.clock"):
            invalid = copy.deepcopy(receipt)
            invalid["current"]["current_payload_generations"] = [item for item in invalid["current"]["current_payload_generations"]
                                                                 if item["name"] != name]
            invalid["current_sha256"] = P.sha(raw(invalid["current"]))
            with self.assertRaises(P.Incomplete):
                P.native_receipt(raw(invalid))

    def test_native_export_guard_counts_bind_actual_private_journal_memberships(self):
        reports, evidence = F.fixture(native=True)
        report = reports[1]
        guard = {"space_id": evidence["A"]["space_id"], "journal_id": evidence["journal"]["journal_id"],
                 "key_fingerprint": evidence["journal"]["reference_key_fingerprint"],
                 "version": 1, "membership_count": 1, "revision": 1}
        report["rows"].append(F.row("source_erasure_guard", "original-guard", guard, True))
        F.refresh(report)
        tokens = {evidence["A"]["space_id"]: {"a" * 64}}
        P.native_export_guards(raw(report), O, evidence, "post", tokens)
        with self.assertRaisesRegex(P.Violation, "original_membership_counts"):
            P.native_export_guards(raw(report), O, evidence, "post", {evidence["A"]["space_id"]: set()})

    def test_native_generation_origin_allows_later_flush_and_reused_inode(self):
        capture, files, evidence, receipt, _ = native_capture()
        known = P.native_generation(capture, files, evidence, "post")
        self.assertIn(P.NATIVE_ROOT + "/LOG", known)
        self.assertNotEqual(len(files[P.NATIVE_ROOT + "/000001.sst"]),
                            receipt["current"]["current_payload_generations"][1]["bytes"])

    def test_native_receipt_is_not_its_own_directory_or_engine_observation(self):
        capture, files, evidence, _, metadata = native_capture()
        metadata[P.NATIVE_ROOT]["inode"] += 1
        capture["metadata"] = metadata_bytes(metadata)
        with self.assertRaisesRegex(P.Incomplete, "directory_receipt_mismatch"):
            P.native_generation(capture, files, evidence, "post")
        capture, files, evidence, _, _ = native_capture()
        files[P.NATIVE_ROOT + "/IDENTITY"] = b"old-engine\n"
        with self.assertRaisesRegex(P.Incomplete, "engine_receipt_mismatch"):
            P.native_generation(capture, files, evidence, "post")

    def test_native_old_receipt_cannot_qualify_original_restore(self):
        capture, files, evidence, _, _ = native_capture()
        with self.assertRaisesRegex(P.Incomplete, "restore_requires_new_generation"):
            P.native_generation(capture, files, evidence, "replay", (capture, files))

    def test_native_unknown_sibling_and_hardlink_refuse(self):
        capture, files, evidence, _, metadata = native_capture()
        metadata[P.NATIVE_ROOT + "/000001.sst"]["links"] = 2
        capture["metadata"] = metadata_bytes(metadata)
        with self.assertRaisesRegex(P.Incomplete, "external_hardlink"):
            P.native_generation(capture, files, evidence, "post")
        capture, files, evidence, _, metadata = native_capture()
        name = "database/retained-old-engine"
        files[name] = b"compressed old payload"
        metadata[name] = {"kind": "f", "device": 2, "inode": 900, "bytes": len(files[name]), "links": 1}
        capture["metadata"] = metadata_bytes(metadata)
        capture["archives"]["database"] = tar("database", {name.split("/", 1)[1]: value
            for name, value in files.items() if name.startswith("database/")})
        with self.assertRaisesRegex(P.Incomplete, "unaccounted_sibling_generation"):
            P.native_generation(capture, files, evidence, "post")

    def test_native_unexpected_opaque_file_inside_fresh_root_is_not_exempt(self):
        capture, files, evidence, _, metadata = native_capture()
        name = P.NATIVE_ROOT + "/opaque-copy.bin"
        files[name] = b"compressed old payload without a literal canary"
        metadata[name] = {"kind": "f", "device": 2, "inode": 901, "bytes": len(files[name]), "links": 1}
        capture["metadata"] = metadata_bytes(metadata)
        capture["archives"]["database"] = tar("database", {name.split("/", 1)[1]: value
            for name, value in files.items() if name.startswith("database/")})
        with self.assertRaisesRegex(P.Incomplete, "unknown_current_generation_file"):
            P.native_generation(capture, files, evidence, "post")

    def test_native_receipt_unknown_fields_checksum_and_uncompleted_refuse(self):
        _, _, _, original, _ = native_capture()
        for mutate in (lambda value: value.update(hidden_payload="source-body"),
                       lambda value: value["current"].update(completed=False),
                       lambda value: value["current"].update(rows=21),
                       lambda value: value["current"]["current_directory"].update(payload="hidden")):
            receipt = copy.deepcopy(original)
            mutate(receipt)
            with self.assertRaises(P.Incomplete):
                P.native_receipt(raw(receipt))

    def test_stopped_metadata_binds_every_actual_archived_file(self):
        files = {"data/plain": b"abc", "database/store/LOG": b"trace", "backups/a\nname": b""}
        directories = {"data", "database", "database/store", "backups"}
        rows = [("f", 1, 40, 3, 1, "data/plain"), ("f", 2, 41, 5, 1, "database/store/LOG"),
                ("f", 3, 42, 0, 1, "backups/a\nname")]
        rows += [("d", 1, 50 + index, 4096, 2, name) for index, name in enumerate(sorted(directories))]
        metadata = b"".join(("\0".join(map(str, (*row[:5], "/physical/" + row[5]))) + "\0").encode() for row in rows)
        actual = P.file_metadata(metadata, files, directories)
        self.assertEqual(set(actual), set(files) | directories)
        self.assertEqual(actual["database/store/LOG"], {"kind": "f", "device": 2, "inode": 41, "bytes": 5, "links": 1})

    def test_metadata_missing_alias_duplicate_or_wrong_size_never_proves_retirement(self):
        files = {"database/IDENTITY": b"abc"}
        valid = b"f\0" + b"1\0" + b"40\0" + b"3\0" + b"1\0/physical/database/IDENTITY\0"
        for value in (b"", valid[:-1], valid + valid, valid.replace(b"40\0", b"-1\0"),
                      valid.replace(b"3\0", b"2\0"), valid.replace(b"database/", b"database/../"),
                      valid.replace(b"/physical/database/", b"/physical/journal/")):
            with self.assertRaises(P.Incomplete):
                P.file_metadata(value, files, set())

    def test_complete_embedded_metadata_fixture_has_no_invented_media_requirement(self):
        captures, evidence, reports = fixture()
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "pass", result)
        self.assertFalse(result["product_acceptance"])
        self.assertIn("unexercised", result["coverage"]["media_erasure_behavior"])

    def test_unparsed_second_archive_and_nonzero_suffix_refuse(self):
        good = tar("data", {"file": b"visible"})
        self.assertEqual(P.archive(good, "data"), {"data/file": b"visible"})
        for raw in (good + tar("data", {"hidden": b"secret"}), good + b"hidden"):
            with self.assertRaises(P.Incomplete):
                P.archive(raw, "data")

    def test_links_special_alias_duplicate_and_truncated_archives_refuse(self):
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE):
            entry = tarfile.TarInfo("data/link")
            entry.type, entry.linkname = kind, "outside"
            with self.assertRaises(P.Incomplete):
                P.archive(tar("data", extra=[entry]), "data")
        for name in ("data/../escape", "data//alias", "/data/escape", "data"):
            entry = tarfile.TarInfo(name)
            with self.assertRaises(P.Incomplete):
                P.archive(tar("data", extra=[entry]), "data")
        with self.assertRaises((P.Incomplete, tarfile.TarError)):
            P.archive(tar("data", {"x": b"secret"})[:1026], "data")

    def test_duplicate_and_overflow_nested_json_cannot_prove_absence(self):
        for raw_value in (b'{"a":1,"a":2}', b'{"a":1e999}', b'{"a":NaN}'):
            with self.assertRaises(P.Incomplete):
                P.decode(raw_value)
            with self.assertRaises(P.Incomplete):
                P.contains(raw_value.decode(), "absent")
        self.assertTrue(P.contains('{"x":"\\u0061bc"}', "abc"))

    def test_every_historical_wal_frame_is_checked_even_when_later_deleted(self):
        captures, evidence, reports = fixture()
        files = P.archive(captures["post"]["archives"]["data"], "data")
        tables = json.loads(json.loads(files["data/state.snapshot"])["data"])["tables"]
        row = {"id": evidence["A"]["source_id"], "space_id": evidence["A"]["space_id"], "payload_without_original_canary": True}
        changes = [[{"kind": "typed", "table": "source", "id": row["id"], "value": row}],
                   [{"kind": "typed", "table": "source", "id": row["id"], "value": None}]]
        captures["post"]["archives"]["data"] = tar("data", checkpoint(tables, evidence["journal"]["journal_id"], changes=changes))
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "fail", result)
        self.assertIn("erased_source_exclusive_decoded_physical_row", result["failures"])

    def test_corrupt_or_torn_wal_is_never_empty_evidence(self):
        files = checkpoint({}, "journal")
        for suffix in (b"RCDEL", b"garbage"):
            value = {"data/" + key: raw for key, raw in files.items()}
            value["data/state.snapshot.durable/commits.wal"] += suffix
            with self.assertRaises(P.Incomplete):
                P.embedded(value, "journal")
        value = {"data/" + key: raw for key, raw in files.items()}
        value["data/state.snapshot.durable/commits.wal"] = value["data/state.snapshot.durable/commits.wal"][:-1] + b"X"
        with self.assertRaises(P.Incomplete):
            P.embedded(value, "journal")

    def test_old_baseline_survives_short_backup_expiry_is_failure(self):
        captures, evidence, reports = fixture()
        captures["expired"]["archives"]["backups"] = captures["post"]["archives"]["backups"]
        self.assertEqual(verdict(captures, evidence, reports)["verdict"], "fail")

    def test_new_registration_cannot_renew_old_source_payload(self):
        captures, evidence, reports = fixture()
        old = P.archive(captures["post"]["archives"]["registry"], "registry")
        first = next(iter(old))
        changed = json.loads(old.pop(first))
        changed["expires_at"] = "2026-10-08T02:00:00Z"
        old[first] = raw(changed)
        captures["post"]["archives"]["registry"] = tar("backups", {key.split('/', 1)[1]: value for key, value in old.items()})
        self.assertNotEqual(verdict(captures, evidence, reports)["verdict"], "pass")

    def test_unknown_native_compressed_files_remain_unmet(self):
        captures, evidence, reports = fixture(native=True)
        captures["pre"]["archives"]["database"] = tar("database", {
            "database.db/IDENTITY": b"original-native-engine\n", "database.db/000001.sst": b"compressed-original"})
        files, directories = {}, set()
        for root, archive in captures["pre"]["archives"].items():
            files.update(P.archive(archive, root, directory_inventory=directories))
        metadata = {name: {"kind": "f" if name in files else "d", "device": 2, "inode": 100 + index,
            "bytes": len(files[name]) if name in files else 4096, "links": 1 if name in files else 2}
            for index, name in enumerate(sorted(set(files) | directories))
            if name.split("/", 1)[0] != "registry"}
        captures["pre"]["metadata"] = metadata_bytes(metadata)
        for phase in ("post", "replay", "expired"):
            captures[phase]["archives"]["database"] = tar("database", {"database.db/000002.sst": b"compressed"})
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "unmet", result)
        self.assertNotIn("report_manifest_identity", result["unmet"])
        for phase in ("post", "replay", "expired"):
            self.assertIn(phase + "_native_storage_requires_complete_offline_decoder_or_verified_reclamation", result["unmet"])

    def test_export_failure_survives_missing_physical_capture(self):
        result = P.evaluate({}, {}, {"verdict": "fail", "scope": "exported_inventory", "product_acceptance": False})
        self.assertEqual(result["verdict"], "fail")

    def test_live_positive_failure_survives_unrelated_unmet(self):
        captures, evidence, reports = fixture()
        captures["post"]["archives"]["database"] = tar("database", {"leftover": evidence["A"]["canary"].encode()})
        captures["replay"]["archives"]["data"] = b"invalid"
        result = verdict(captures, evidence, reports, {"verdict": "unmet", "scope": "exported_inventory", "product_acceptance": False})
        self.assertEqual(result["verdict"], "fail", result)

    def test_private_native_payload_failure_survives_incomplete_generation_evidence(self):
        captures, evidence, reports = fixture(native=True)
        captures["post"]["private_membership"] = raw({"unexpected": evidence["A"]["canary"]})
        captures["post"]["archives"]["database"] = b"invalid"
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "fail", result)
        self.assertIn("post_positive_original_canary_in_private_native_rows", result["failures"])

    def test_independent_B_physical_row_change_is_failure(self):
        captures, evidence, reports = fixture()
        files = P.archive(captures["post"]["archives"]["data"], "data")
        tables = json.loads(json.loads(files["data/state.snapshot"])["data"])["tables"]
        tables["source"][0]["changed_independent"] = True
        captures["post"]["archives"]["data"] = tar("data", checkpoint(tables, evidence["journal"]["journal_id"]))
        self.assertEqual(verdict(captures, evidence, reports)["verdict"], "fail")

    def test_membership_index_missing_actual_file_is_incomplete(self):
        captures, evidence, reports = fixture()
        captures["post"]["membership"]["files"] = tar("reference-memberships")
        self.assertEqual(verdict(captures, evidence, reports)["verdict"], "unmet")

    def test_wal_target_identity_cannot_differ_from_full_record(self):
        changes = [{"kind": "extension", "table": "artifact", "key": "artifact:first",
                    "value": {"id": "artifact:second", "space_id": "a"}}]
        with self.assertRaisesRegex(P.Incomplete, "target_row"):
            list(P.typed_rows(changes, True))
        changes[0]["value"] = None
        changes[0]["key"] = "source:wrong-table"
        with self.assertRaisesRegex(P.Incomplete, "target_identity"):
            list(P.typed_rows(changes, True))
        for identity, key in ((123, "artifact:123"), ("123", "artifact:`123`"),
                              ({"tb": "artifact", "id": "123"}, "artifact:`123`")):
            changes[0].update(key=key, value={"id": identity, "space_id": "a"})
            self.assertEqual(list(P.typed_rows(changes, True)), [("artifact", changes[0]["value"])])
            changes[0]["key"] = "artifact:123" if key.endswith("`") else "artifact:`123`"
            with self.assertRaisesRegex(P.Incomplete, "target_row"):
                list(P.typed_rows(changes, True))

    def test_typed_alias_deletion_preserves_other_physical_table(self):
        captures, evidence, reports = fixture()
        original, _ = O.report_inventory(reports[0], reports[0]["manifest"], evidence["backend"], evidence)
        files = P.archive(captures["post"]["archives"]["data"], "data")
        tables = json.loads(json.loads(files["data/state.snapshot"])["data"])["tables"]
        row = {"id": "doc_node:" + F.IDS[9], "space_id": evidence["B"]["space_id"]}
        ref = O.reference(row["id"])
        original[b"independent-node"] = {"table": "doc_node", "reference": ref,
            "space": row["space_id"], "canaries": {"A": False}, "references": set(),
            "sha256": O.digest(O.encoded(F.tagged(row)))}
        tables["doc_node"], tables["chunk_embedding"] = [row], [copy.deepcopy(row)]
        change = {"kind": "typed", "table": "chunk_embedding", "id": row["id"], "value": row}
        self.assertEqual(list(P.typed_rows([change], True)), [("chunk_embedding", row)])
        change["value"] = None
        values = [{"tables": tables}, {"mutations": [change], "apply": True}]
        P.decoded_policy(values, O, original, evidence, {evidence["A"]["space_id"]: set()})
        change["table"] = "doc_node"
        with self.assertRaisesRegex(P.Violation, "independent_B_complete_live_rows_changed"):
            P.decoded_policy(values, O, original, evidence, {evidence["A"]["space_id"]: set()})

    def test_duplicate_checkpoint_record_cannot_hide_behind_identity_map(self):
        captures, evidence, reports = fixture()
        files = P.archive(captures["post"]["archives"]["data"], "data")
        tables = json.loads(json.loads(files["data/state.snapshot"])["data"])["tables"]
        tables["source"].append(copy.deepcopy(tables["source"][0]))
        captures["post"]["archives"]["data"] = tar("data", checkpoint(tables, evidence["journal"]["journal_id"]))
        result = verdict(captures, evidence, reports)
        self.assertEqual(result["verdict"], "unmet", result)
        self.assertIn("duplicate_checkpoint_physical_identity", result["unmet"])

    def test_raw_registration_cannot_exempt_arbitrary_media(self):
        files = {"registry/raw.json": raw({"path": P.PREFIX + "data/media", "snapshot_id": "raw",
                 "expires_at": "2026-10-07T02:20:00Z", "raw_sha256": P.sha(b"payload")}), "data/media": b"payload"}
        with self.assertRaises(ValueError):
            P.registry(files, "journal")


if __name__ == "__main__":
    unittest.main()
