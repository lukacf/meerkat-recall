"""Independent, bounded stopped-volume evidence for the original physical gate.

No process, database, network, filesystem traversal or journal-key access occurs
here. The existing container owner supplies complete raw Docker archives. This
component never substitutes a literal-byte scan for decoding opaque storage.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import math
from pathlib import PurePosixPath
import re
import struct
import tarfile
import uuid

CAPTURE_CAP = 96 * 1024 * 1024  # Existing boundary evidence allocation, not extra.
ENTRY_CAP = 100_000
HEX = re.compile(r"[a-f0-9]{64}\Z")
ROOTS = ("data", "database", "backups", "registry")
PREFIX = "/var/lib/meerkat-recall/"


class Incomplete(ValueError):
    pass


class Violation(ValueError):
    pass


def need(value, reason):
    if not value:
        raise Incomplete(reason)


def invariant(value, reason):
    if not value:
        raise Violation(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    def finite(value):
        result = float(value)
        need(math.isfinite(result), "nonfinite_json")
        return result
    return json.loads(raw, object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: need(False, "nonfinite_json"))


def timestamp(value):
    need(type(value) is str and len(value) <= 64, "timestamp_shape")
    value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    need(value.tzinfo is not None, "timestamp_timezone")
    return value


def archive(raw, root, *, directory_inventory=None):
    """Decode every member; never extract or follow archive paths/links."""
    need(root in (*ROOTS, "reference-memberships", "history-commitments") and type(raw) is bytes and 0 < len(raw) <= CAPTURE_CAP,
         "archive_bound_or_root")
    files, directories, seen, total = {}, set(), set(), 0
    archive_root = "backups" if root == "registry" else root
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as stream:
        for member in stream:
            name = member.name.rstrip("/")
            path = PurePosixPath(name)
            need(name == str(path) and not path.is_absolute() and path.parts
                 and path.parts[0] == archive_root and ".." not in path.parts,
                 "archive_path_escape_or_noncanonical")
            name = str(PurePosixPath(root, *path.parts[1:]))
            need(name not in seen and len(seen) < ENTRY_CAP, "archive_duplicate_or_entry_bound")
            seen.add(name)
            need(not member.issparse() and (member.isdir() or member.isfile()),
                 "archive_link_special_or_sparse")
            if member.isdir():
                directories.add(name)
                continue
            total += member.size
            need(0 <= member.size <= CAPTURE_CAP and total <= CAPTURE_CAP,
                 "archive_payload_bound")
            value = stream.extractfile(member)
            need(value is not None, "archive_unreadable_member")
            payload = value.read(member.size + 1)
            need(len(payload) == member.size, "archive_truncated_member")
            files[name] = payload
        need(len(raw) % 512 == 0 and len(raw) - stream.offset >= 1024
             and not any(raw[stream.offset:]), "archive_unparsed_trailing_bytes")
    need(root in directories, "archive_root_directory_missing")
    for name in seen:
        for parent in PurePosixPath(name).parents:
            if str(parent) != ".":
                need(str(parent) in directories, "archive_parent_missing_or_file")
    if directory_inventory is not None:
        directory_inventory.update(directories)
    return files


def inventories(capture):
    need(set(capture["archives"]) == set(ROOTS) and capture["stopped"] is True,
         "complete_stopped_original_roots_required")
    files, total = {}, 0
    for root in ROOTS:
        raw = capture["archives"][root]
        total += len(raw)
        need(total <= CAPTURE_CAP, "combined_archive_bound")
        files.update(archive(raw, root))
    return files


def file_metadata(raw, files, directories):
    """Bind stopped-volume inode observations to every archived product file."""
    need(type(raw) is bytes and len(raw) <= CAPTURE_CAP, "physical_metadata_bound")
    parts = raw.split(b"\0")
    need(parts[-1] == b"" and (len(parts) - 1) % 6 == 0, "physical_metadata_framing")
    result = {}
    for offset in range(0, len(parts) - 1, 6):
        kind = parts[offset]
        need(kind in (b"d", b"f"), "physical_metadata_nonregular_entry")
        numbers = parts[offset + 1:offset + 5]
        need(all(re.fullmatch(rb"[0-9]{1,20}", value) and int(value) < 2**64 for value in numbers),
             "physical_metadata_unsigned_identity")
        path = parts[offset + 5].decode("utf-8")
        need(path.startswith("/physical/"), "physical_metadata_owned_path")
        name = path[len("/physical/"):]
        parsed = PurePosixPath(name)
        need(name == str(parsed) and parsed.parts and parsed.parts[0] in ("data", "database", "backups")
             and not parsed.is_absolute() and ".." not in parsed.parts and name not in result
             and len(result) < ENTRY_CAP, "physical_metadata_path_or_duplicate")
        device, inode, size, links = map(int, numbers)
        need(inode > 0 and links > 0 and (kind == b"f" and name in files and len(files[name]) == size
                           or kind == b"d" and name in directories and name not in files),
             "physical_metadata_archive_mismatch")
        result[name] = {"kind": kind.decode(), "device": device, "inode": inode, "bytes": size, "links": links}
    expected = {name for name in set(files) | directories
                if name.split("/", 1)[0] in ("data", "database", "backups")}
    need(set(result) == expected, "physical_metadata_complete_file_inventory")
    return result


NATIVE_ROOT = "database/database.db"
NATIVE_RECEIPT = "database/.database.db.recall-physical-checkpoint.json"
NATIVE_CHECKPOINT_FIELDS = ("sequence", "old_directory", "current_directory", "old_engine_identity",
    "engine_identity", "rows", "stream_sha256", "started_unix_ms", "completed_unix_ms", "previous_sha256",
    "original_payload_generations", "current_payload_generations", "native_snapshots", "background_errors",
    "completed", "hard_cancellation_supported")
ROCKS_FILE = re.compile(r"(?:(?:archive/)?[0-9]+\.(?:sst|ldb|blob|log|dbtmp)|CURRENT|LOCK|IDENTITY|LOG|LOG\.old\.[0-9]+|(?:MANIFEST|OPTIONS)-[0-9]+)\Z")
SKV_FILE = re.compile(r"(?:recall-engine\.identity|recall-checkpoint\.clock|clog/[0-9]{20}\.clog|manifest/[0-9]{20}\.manifest)\Z")


def unsigned(value):
    return type(value) is int and 0 <= value < 2**64


ORIGIN_CAP = 64 * 1024 * 1024
ORIGIN_FIELDS = ("content", "origin_mode", "original_version", "original_timestamp", "causal_sources")
ORIGIN_BACKUP_FIELDS = ("kind", "journal_identity", "key_fingerprint", "namespace", "database",
                       "snapshot_id", "snapshot_sha256", "manifest_sha256", "rows")
ORIGIN_IMPORTED_FIELDS = ("kind", "journal_identity", "key_fingerprint", "scopes")
ORIGIN_IMPORT_ROW_FIELDS = ("origin", "imported_version", "imported_timestamp", "snapshot_id", "snapshot_sha256")
MANIFEST_FIELDS = ("version", "format", "snapshot_id", "journal_id", "backend", "name",
                   "created_at", "expires_at", "bytes", "sha256")


def ordered_fields(value, fields, reason):
    need(type(value) is dict and set(value) == set(fields), reason)
    return {key: value[key] for key in fields}


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def canonical_uuid(value):
    return type(value) is str and str(uuid.UUID(value)) == value


def origin_envelope(raw, fixture, *, manifest=None):
    """Opaque physical accounting only: original product verifies the secret MAC."""
    need(type(raw) is bytes and 0 < len(raw) <= ORIGIN_CAP, "origin_capture_bound")
    envelope = ordered_fields(decode(raw), ("payload", "authentication"), "origin_signed_envelope")
    need(type(envelope["authentication"]) is str and HEX.fullmatch(envelope["authentication"]),
         "origin_authentication_shape_only")
    fields = ORIGIN_BACKUP_FIELDS if manifest is not None else ORIGIN_IMPORTED_FIELDS
    proof = ordered_fields(envelope["payload"], fields, "origin_payload_shape")
    kind = "backup" if manifest is not None else "imported"
    need(proof["kind"] == "meerkat-recall-" + kind + "-history-origin/v1"
         and canonical_uuid(proof["journal_identity"])
         and proof["journal_identity"] == fixture["journal"]["journal_id"]
         and proof["key_fingerprint"] == fixture["journal"]["reference_key_fingerprint"],
         "origin_original_journal_identity")
    total_rows, total_tokens = 0, 0
    def origin(value):
        nonlocal total_rows, total_tokens
        row = ordered_fields(value, ORIGIN_FIELDS, "origin_record_shape")
        total_rows += 1
        need(type(row["content"]) is str and HEX.fullmatch(row["content"])
             and row["origin_mode"] in ("native_versions", "versionless_current")
             and unsigned(row["original_version"]) and unsigned(row["original_timestamp"])
             and (row["origin_mode"] == "versionless_current")
                 == (row["original_version"] == row["original_timestamp"] == 0), "origin_record_identity")
        tokens = row["causal_sources"]
        need(type(tokens) is list and all(type(token) is str and HEX.fullmatch(token) for token in tokens)
             and tokens == sorted(set(tokens)), "origin_opaque_causal_tokens")
        total_tokens += len(tokens)
        need(total_rows <= 1_000_000 and total_tokens <= 1_000_000, "origin_aggregate_bounds")
        return row
    def scope(value):
        # The independently specified original paired container recipe owns this scope.
        need(value["namespace"] == "offline" and value["database"] == "fixture", "origin_original_paired_scope")
    identities = {}
    if manifest is not None:
        scope(proof)
        original = ordered_fields(manifest, MANIFEST_FIELDS, "origin_original_manifest_shape")
        need(original["backend"] == "surreal-db" and canonical_uuid(proof["snapshot_id"])
             and proof["snapshot_id"] == original["snapshot_id"]
             and proof["snapshot_sha256"] == original["sha256"]
             and type(proof["snapshot_sha256"]) is str and HEX.fullmatch(proof["snapshot_sha256"])
             and proof["manifest_sha256"] == sha(compact(original)), "origin_exact_original_manifest_binding")
        need(type(proof["rows"]) is list, "origin_complete_record_array")
        proof["rows"] = [origin(row) for row in proof["rows"]]
        contents = [row["content"] for row in proof["rows"]]
        need(contents == sorted(set(contents)), "origin_sorted_unique_record_contents")
    else:
        need(type(proof["scopes"]) is list and 0 < len(proof["scopes"]) <= 1024, "origin_complete_scopes")
        scopes = []
        keys = []
        for value in proof["scopes"]:
            value = ordered_fields(value, ("namespace", "database", "pending_import", "rows"), "origin_import_scope_shape")
            scope(value)
            pending = value["pending_import"]
            if pending is not None:
                pending = ordered_fields(pending, ("snapshot_id", "snapshot_sha256", "origin_sha256", "max_preimport_version"),
                                         "origin_pending_import_shape")
                need(canonical_uuid(pending["snapshot_id"])
                     and all(type(pending[field]) is str and HEX.fullmatch(pending[field])
                             for field in ("snapshot_sha256", "origin_sha256"))
                     and unsigned(pending["max_preimport_version"]), "origin_pending_import_binding")
                value["pending_import"] = pending
            key = value["namespace"], value["database"]
            keys.append(key)
            need(type(value["rows"]) is list, "origin_complete_imported_rows")
            rows, row_keys = [], []
            for item in value["rows"]:
                row = ordered_fields(item, ORIGIN_IMPORT_ROW_FIELDS, "origin_imported_record_shape")
                row["origin"] = origin(row["origin"])
                need(unsigned(row["imported_version"]) and unsigned(row["imported_timestamp"])
                     and not (row["imported_version"] == row["imported_timestamp"] == 0)
                     and canonical_uuid(row["snapshot_id"])
                     and type(row["snapshot_sha256"]) is str and HEX.fullmatch(row["snapshot_sha256"]),
                     "origin_imported_native_version_or_snapshot")
                row_key = row["origin"]["content"], row["imported_version"], row["imported_timestamp"]
                row_keys.append(row_key)
                identities[(*key, *row_key)] = row
                rows.append(row)
            need(row_keys == sorted(set(row_keys)), "origin_sorted_unique_imported_records")
            value["rows"] = rows
            scopes.append(value)
        need(keys == sorted(set(keys)), "origin_sorted_unique_scopes")
        proof["scopes"] = scopes
    envelope["payload"] = proof
    need(compact(envelope) == raw, "origin_noncanonical_or_lossy_bytes")
    return {"payload": proof, "identities": identities, "mac_verified": False,
            "scope": "opaque_physical_inventory_only", "product_acceptance": False}


def restored_origins(capture, fixture):
    index = decode(capture["membership"]["index"])["payload"]
    required = index.get("restore_origins_required", False)
    need(type(required) is bool and ("restore_origins_required" not in index or required),
         "origin_required_index_shape")
    raw = capture["membership"].get("origins")
    need(required == (raw is not None), "origin_acknowledged_file_inventory")
    if raw is None:
        return {"required": False, "identities": {}, "scopes": set(), "mac_verified": False}
    result = origin_envelope(raw, fixture)
    need(all(scope["pending_import"] is None for scope in result["payload"]["scopes"]),
         "origin_pending_import_not_completed")
    return {"required": required, "identities": result["identities"], "mac_verified": False,
            "scopes": {(scope["namespace"], scope["database"]) for scope in result["payload"]["scopes"]}}


def origins_monotonic(previous, current):
    invariant(not previous["required"] or current["required"], "acknowledged_restore_origin_anchor_lost")
    invariant(previous["scopes"] <= current["scopes"], "acknowledged_restore_origin_scope_lost")
    invariant(all(key in current["identities"] and current["identities"][key] == row
                  for key, row in previous["identities"].items()), "acknowledged_restore_origin_binding_changed_or_lost")


def native_receipt(raw):
    """Decode the exact bounded producer receipt, never treat it as evidence alone."""
    need(type(raw) is bytes and 0 < len(raw) <= 16 * 1024 * 1024, "native_receipt_bound")
    value = decode(raw)
    need(type(value) is dict and set(value) == {"format", "engine", "wrapper_version", "current",
         "current_sha256", "previous"}, "native_receipt_shape")
    engine = value["engine"]
    need(engine in ("rocksdb", "surrealkv") and value["format"] == "recall-" + engine + "-physical-checkpoint/v1"
         and value["wrapper_version"] == {"rocksdb": "0.24.0-surreal.5", "surrealkv": "0.9.3"}[engine],
         "native_receipt_reviewed_engine")
    fields = NATIVE_CHECKPOINT_FIELDS
    if engine == "surrealkv":
        fields = (*fields[:6], "high_water", *fields[6:])
    grammar = ROCKS_FILE if engine == "rocksdb" else SKV_FILE
    identity_file = "IDENTITY" if engine == "rocksdb" else "recall-engine.identity"
    def checkpoint(row):
        need(type(row) is dict and set(row) == set(fields), "native_checkpoint_shape")
        for key in ("sequence", "rows", "started_unix_ms", "completed_unix_ms", "native_snapshots", "background_errors"):
            need(unsigned(row[key]), "native_checkpoint_unsigned_fields")
        if engine == "surrealkv":
            need(unsigned(row["high_water"]) and row["high_water"] < 2**64 - 1, "native_version_high_water")
        need(row["sequence"] > 0 and (engine == "surrealkv" or row["rows"] > 0)
             and row["started_unix_ms"] <= row["completed_unix_ms"]
             and row["completed"] is True and row["hard_cancellation_supported"] is False
             and row["native_snapshots"] == row["background_errors"] == 0, "native_checkpoint_complete")
        for key in ("stream_sha256", "previous_sha256"):
            need(key == "previous_sha256" and row[key] is None
                 or type(row[key]) is str and HEX.fullmatch(row[key]), "native_checkpoint_hash")
        for key in ("old_engine_identity", "engine_identity"):
            need(type(row[key]) is str and re.fullmatch(r"[A-Za-z0-9-]{1,128}", row[key]), "native_engine_identity")
            if engine == "surrealkv":
                need(str(uuid.UUID(row[key])) == row[key] and uuid.UUID(row[key]).int != 0, "native_version_engine_uuid")
        for key in ("old_directory", "current_directory"):
            item = row[key]
            need(type(item) is dict and set(item) == {"device", "inode"}
                 and all(unsigned(item[field]) for field in item) and item["inode"] > 0,
                 "native_directory_identity")
        need(row["old_engine_identity"] != row["engine_identity"]
             and row["old_directory"] != row["current_directory"]
             and row["old_directory"]["device"] == row["current_directory"]["device"], "native_fresh_generation")
        for field in ("original_payload_generations", "current_payload_generations"):
            records, seen = row[field], set()
            need(type(records) is list and 0 < len(records) <= ENTRY_CAP, "native_generation_array")
            for item in records:
                need(type(item) is dict and set(item) == {"name", "bytes", "device", "inode"}, "native_generation_shape")
                name = item["name"]
                need(type(name) is str and name not in seen and grammar.fullmatch(name),
                     "native_generation_name")
                seen.add(name)
                need(all(unsigned(item[key]) for key in ("bytes", "device", "inode")) and item["inode"] > 0,
                     "native_generation_unsigned_fields")
            need(identity_file in seen and any(name.endswith((".sst", ".ldb", ".log", ".blob", ".clog")) for name in seen),
                 "native_generation_populated")
            if engine == "surrealkv":
                directory = row["old_directory" if field == "original_payload_generations" else "current_directory"]
                need(directory["device"] > 0 and [item["name"] for item in records] == sorted(seen)
                     and len({item["inode"] for item in records}) == len(records)
                     and all(item["device"] == directory["device"] for item in records)
                     and all("/" not in name or int(name.split("/")[1].split(".")[0]) < 2**64 for name in seen)
                     and any(item["name"] == identity_file and item["bytes"] == 36 for item in records)
                     and any(name.startswith("manifest/") for name in seen), "native_version_file_inventory")
                if field == "current_payload_generations":
                    need(any(item["name"] == "recall-checkpoint.clock" and item["bytes"] == 20 for item in records),
                         "native_version_durable_clock")
        # Match the producer's defined serde struct field order, independent of
        # input JSON object ordering. No unrecognized field can be discarded.
        ordered = {key: row[key] for key in fields}
        for key in ("old_directory", "current_directory"):
            ordered[key] = {field: row[key][field] for field in ("device", "inode")}
        for key in ("original_payload_generations", "current_payload_generations"):
            ordered[key] = [{field: item[field] for field in ("name", "bytes", "device", "inode")} for item in row[key]]
        return sha(json.dumps(ordered, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
    current, previous = value["current"], value["previous"]
    need(checkpoint(current) == value["current_sha256"], "native_receipt_current_checksum")
    if previous is None:
        need(current["sequence"] == 1 and current["previous_sha256"] is None, "native_receipt_missing_adjacency")
    else:
        need(checkpoint(previous) == current["previous_sha256"] and previous["sequence"] + 1 == current["sequence"]
             and previous["engine_identity"] == current["old_engine_identity"]
             and previous["current_directory"] == current["old_directory"]
             and previous["completed_unix_ms"] <= current["started_unix_ms"], "native_receipt_adjacency")
    return value


def native_generation(capture, files, fixture, phase, prior=None):
    """Bind source-reviewed generation replacement to complete independent bytes."""
    directories = set()
    for root in ROOTS:
        archive(capture["archives"][root], root, directory_inventory=directories)
    metadata = file_metadata(capture["metadata"], files, directories)
    receipt = native_receipt(files[NATIVE_RECEIPT])
    current = receipt["current"]
    engine = receipt["engine"]
    # The original paired recipe explicitly launches file:/data/database.db.
    # SurrealKV version-history authority has separate product qualification;
    # a versionless export cannot establish its historical payload policy.
    need(engine == "rocksdb", "native_receipt_engine_differs_from_original_paired_file_recipe")
    identity_file = "IDENTITY" if engine == "rocksdb" else "recall-engine.identity"
    grammar = ROCKS_FILE if engine == "rocksdb" else SKV_FILE
    identity = files[NATIVE_ROOT + "/" + identity_file].decode().strip()
    need(identity == current["engine_identity"], "native_actual_engine_receipt_mismatch")
    actual = metadata.get(NATIVE_ROOT, {})
    need(actual.get("kind") == "d" and {key: actual.get(key) for key in ("device", "inode")} == current["current_directory"],
         "native_actual_directory_receipt_mismatch")
    observed_ms = int(timestamp(capture["observed_at"]).timestamp() * 1000)
    need(current["completed_unix_ms"] <= observed_ms, "native_receipt_after_capture")
    if phase in ("post", "replay"):
        start = capture["maintenance_started_at"]
        need(type(start) is str and int(timestamp(start).timestamp() * 1000) <= current["started_unix_ms"],
             "native_generation_predates_original_maintenance")
        if phase == "post":
            need(start == fixture["journal"]["erase_started_at"], "native_original_erasure_interval")
    if prior is not None:
        old_identity = prior[1][NATIVE_ROOT + "/" + identity_file].decode().strip()
        if phase in ("post", "replay"):
            need(identity != old_identity,
                 "native_original_restore_requires_new_generation")
        if NATIVE_RECEIPT in prior[1]:
            old_receipt = native_receipt(prior[1][NATIVE_RECEIPT])
            need(old_receipt["engine"] == engine, "native_original_engine_changed")
            old = old_receipt["current"]
            need(current["sequence"] >= old["sequence"], "native_generation_sequence_regressed")
            if phase in ("post", "replay"):
                need(current["sequence"] > old["sequence"], "native_original_restore_requires_new_generation")
            if current["sequence"] == old["sequence"]:
                need(receipt == old_receipt, "native_original_generation_receipt_changed")
    # The fixed owned engine path is part of the original paired-container recipe.
    # The filesystem promotion helper may leave only its exact payload-free owner.
    control_hash = sha(b"/data/database.db")
    control = "database/.recall-physical-" + control_hash
    owner = control + "/owner"
    lock = "database/.database.db.recall-native-owner.lock"
    need(files.get(owner) == b"recall-native-generation-v1\n" + control_hash.encode()
         and files.get(lock) == b"", "native_stable_owner_or_generation_custody")
    known = {NATIVE_RECEIPT, owner, lock}
    for name, item in metadata.items():
        if name == NATIVE_ROOT or name.startswith(NATIVE_ROOT + "/"):
            need(item["device"] == current["current_directory"]["device"], "native_generation_device_boundary")
            need(item["kind"] == "d" or item["links"] == 1, "native_generation_external_hardlink")
            if item["kind"] == "f":
                need(grammar.fullmatch(name[len(NATIVE_ROOT) + 1:]), "native_unknown_current_generation_file")
                known.add(name)
            else:
                allowed = ("archive",) if engine == "rocksdb" else ("clog", "manifest")
                need(name in {NATIVE_ROOT, *(NATIVE_ROOT + "/" + part for part in allowed)},
                     "native_unknown_current_generation_directory")
        elif name.startswith("database/"):
            need(name in {control, *known}, "native_unaccounted_sibling_generation")
            need(item["kind"] == "d" and name == control or item["kind"] == "f" and item["links"] == 1,
                 "native_control_kind_or_hardlink")
    # No exact file-size/inode freeze: legitimate later flush/close changes files
    # in the proven fresh generation, and deleted inode integers may be reused.
    return known


def contains(value, canary):
    # Includes nested encoded JSON and object keys, with bounded expansion.
    stack, nodes, expanded = [(value, 0)], 0, 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        need(nodes <= 1_000_000 and depth <= 256, "decoded_json_complexity_bound")
        if type(item) is str:
            if canary in item:
                return True
            if item[:1] in ("{", "[", '"'):
                expanded += len(item.encode())
                need(expanded <= CAPTURE_CAP, "encoded_json_expansion_bound")
                try:
                    nested = decode(item)
                except json.JSONDecodeError:
                    continue
                if nested != item:
                    stack.append((nested, depth + 1))
        elif type(item) is dict:
            stack.extend((part, depth + 1) for pair in item.items() for part in pair)
        elif type(item) is list:
            stack.extend((part, depth + 1) for part in item)
    return False


def embedded(files, journal_id):
    """Validate the complete checkpoint and every committed WAL frame."""
    required = {"data/state.snapshot", "data/state.snapshot.durable/identity.json",
                "data/state.snapshot.durable/commits.wal"}
    need(required <= files.keys(), "embedded_checkpoint_identity_or_wal_missing")
    identity = decode(files["data/state.snapshot.durable/identity.json"])
    need(set(identity) == {"version", "store_id", "journal_id", "initialized"}
         and identity["version"] == 1 and identity["initialized"] is True
         and identity["journal_id"] == journal_id, "embedded_store_identity")
    checkpoint = decode(files["data/state.snapshot"])
    need(set(checkpoint) == {"format", "store_id", "journal_id", "sequence", "sha256", "data"}
         and checkpoint["format"] == "recall-embedded-checkpoint-v1"
         and checkpoint["store_id"] == identity["store_id"]
         and checkpoint["journal_id"] == journal_id
         and type(checkpoint["sequence"]) is int and 0 <= checkpoint["sequence"] < 2**64
         and type(checkpoint["data"]) is str, "embedded_checkpoint_shape")
    body = checkpoint["data"].encode()
    checksum = (b"recall-embedded-checkpoint-v1\0" + identity["store_id"].encode() + b"\0"
                + journal_id.encode() + b"\0" + struct.pack(">Q", checkpoint["sequence"]) + body)
    need(sha(checksum) == checkpoint["sha256"], "embedded_checkpoint_checksum")
    payload = decode(body)
    need(set(payload) == {"backend", "version", "exported_at", "tables"}
         and payload["backend"] == "elephant-db" and payload["version"] == 1
         and type(payload["tables"]) is dict
         and all(type(rows) is list for rows in payload["tables"].values()),
         "embedded_checkpoint_payload")
    wal = files["data/state.snapshot.durable/commits.wal"]
    need(len(wal) >= 44 and wal[:8] == b"RCLOG001", "wal_header_magic")
    length = int.from_bytes(wal[8:12], "big")
    need(0 < length <= 4096 and 44 + length <= len(wal), "wal_header_length")
    raw = wal[12:12 + length]
    need(hashlib.sha256(raw).digest() == wal[12 + length:44 + length], "wal_header_checksum")
    header = decode(raw)
    need(set(header) == {"version", "store_id", "journal_id", "base_sequence"}
         and header["version"] == 1 and header["store_id"] == identity["store_id"]
         and header["journal_id"] == journal_id and type(header["base_sequence"]) is int
         and 0 <= header["base_sequence"] <= checkpoint["sequence"], "wal_header_identity")
    offset, sequence, values = 44 + length, header["base_sequence"], [payload]
    while offset < len(wal):
        need(len(wal) - offset >= 96, "wal_torn_frame")
        frame = wal[offset:offset + 88]
        need(frame[:8] == b"RCDEL001", "wal_frame_magic")
        number, size = struct.unpack(">QQ", frame[8:24])
        need(number == sequence + 1 and size <= CAPTURE_CAP
             and offset + 96 + size <= len(wal), "wal_frame_sequence_or_length")
        need(hashlib.sha256(frame[:56]).digest() == frame[56:88], "wal_frame_header_checksum")
        raw = wal[offset + 88:offset + 88 + size]
        need(hashlib.sha256(frame[8:24] + raw).digest() == frame[24:56]
             and wal[offset + 88 + size:offset + 96 + size] == b"COMMIT01", "wal_frame_checksum_or_footer")
        changes = decode(raw)
        need(type(changes) is list and changes, "wal_empty_or_invalid_mutations")
        for change in changes:
            need(type(change) is dict and change.get("kind") in ("typed", "extension"), "wal_mutation_kind")
            key = "id" if change["kind"] == "typed" else "key"
            need(set(change) == {"kind", "table", key, "value"}, "wal_mutation_shape")
        values.append({"mutations": changes, "apply": number > checkpoint["sequence"]})
        # Every physical historical frame is checked; obsolete sequence frames
        # must not overwrite the newer checkpoint while deriving effective state.
        offset += 96 + size
        sequence = number
    return values, required


def registry(files, journal_id):
    result = {}
    for name, raw in files.items():
        if not name.startswith("registry/"):
            continue
        value = decode(raw)
        need(type(value) is dict and {"path", "snapshot_id", "expires_at"} <= value.keys()
             and set(value) <= {"path", "snapshot_id", "expires_at", "raw_sha256", "deleting", "temporary"},
             "registered_generation_shape")
        need(name == "registry/" + value["snapshot_id"] + ".json"
             and value["snapshot_id"] not in result, "registered_generation_identity")
        path = value["path"]
        need(type(path) is str and path.startswith(PREFIX)
             and path == str(PurePosixPath(path)) and ".." not in PurePosixPath(path).parts,
             "registered_path_outside_owned_roots")
        relative = path[len(PREFIX):]
        need(relative.split("/", 1)[0] in ("data", "backups"), "registered_path_outside_owned_roots")
        value = dict(value, relative=relative)
        timestamp(value["expires_at"])
        if value.get("raw_sha256") is not None:
            need(type(value["raw_sha256"]) is str and HEX.fullmatch(value["raw_sha256"]), "registered_raw_hash")
            value["members"] = {}
            if relative in files and sha(files[relative]) == value["raw_sha256"]:
                envelope = decode(files[relative])
                need(set(envelope) == {"format", "sha256", "data", "journal_id"}
                     and envelope["format"] == "recall-live-v1" and envelope["journal_id"] == journal_id
                     and type(envelope["data"]) is str and sha(envelope["data"].encode()) == envelope["sha256"],
                     "registered_raw_snapshot_format")
                payload = decode(envelope["data"])
                need(payload.get("backend") == "elephant-db" and type(payload.get("tables")) is dict,
                     "registered_raw_snapshot_payload")
                value["members"] = {relative: value["raw_sha256"]}
            # A mismatched path may be a legitimate newer generation. It is not
            # exempted by this obsolete registry record. Another actual record
            # must establish that generation or it remains an unknown file.
        else:
            names = {relative + "/manifest.json", relative + "/data.snapshot"}
            if names <= files.keys():
                manifest = decode(files[relative + "/manifest.json"])
                need(manifest.get("snapshot_id") == value["snapshot_id"]
                     and manifest.get("journal_id") == journal_id
                     and manifest.get("expires_at") == value["expires_at"]
                     and manifest.get("bytes") == len(files[relative + "/data.snapshot"])
                     and manifest.get("sha256") == sha(files[relative + "/data.snapshot"]),
                     "registered_original_manifest_payload")
                value["members"] = {name: sha(files[name]) for name in names}
            else:
                need(not any(name.startswith(relative + "/") for name in files),
                     "registered_orphan_or_partial_payload")
                value["members"] = {}
        result[value["snapshot_id"]] = value
    return result


def retention_deadline(capture, journal_id):
    """Original maximum observed deadline only; never invent or renew a clock."""
    values = registry(inventories(capture), journal_id)
    return max((timestamp(row["expires_at"]) for row in values.values()), default=None)


def extension_key(value, table):
    def string_key(identifier):
        numeric = re.fullmatch(r"[+-]?[0-9]+", identifier)
        signed = numeric is not None and -(2**63) <= int(identifier) < 2**63
        return "`" + identifier + "`" if not identifier or signed or re.fullmatch(r"[0-9_]+", identifier) else identifier
    if type(value) is str:
        return value if ":" in value else table + ":" + string_key(value)
    if type(value) is dict:
        declared = value.get("tb", value.get("table", table))
        need(type(declared) is str, "wal_extension_identity_table")
        identifier = value.get("id")
        if type(identifier) is str:
            return declared + ":" + string_key(identifier)
        value, table = identifier, declared
    need(type(value) is int and -(2**63) <= value < 2**63, "wal_extension_identity_shape")
    return table + ":" + str(value)


def typed_rows(payload, mutations=False):
    if mutations:
        for item in payload:
            need(type(item["table"]) is str, "wal_table_shape")
            target = item["id"] if item["kind"] == "typed" else item["key"]
            need(type(target) is str and ":" in target, "wal_target_identity")
            if item["kind"] == "typed":
                namespace, identifier = target.split(":", 1)
                need(re.fullmatch(r"[a-z][a-z0-9_]*", namespace) is not None
                     and str(uuid.UUID(identifier)) == identifier, "wal_target_identity")
            else:
                need(target.startswith(item["table"] + ":"), "wal_target_identity")
            if item["value"] is not None:
                need(type(item["value"]) is dict, "wal_row_not_object")
                actual = item["value"].get("id")
                if item["kind"] == "extension":
                    actual = extension_key(actual, item["table"])
                elif type(actual) is dict and type(actual.get("id")) is str:
                    declared = actual.get("tb", actual.get("table"))
                    need(type(declared) is str, "wal_target_row_identity_mismatch")
                    actual = declared + ":" + actual["id"]
                need(actual == target, "wal_target_row_identity_mismatch")
                yield item["table"], item["value"]
    else:
        for table, rows in payload["tables"].items():
            need(type(table) is str and type(rows) is list, "checkpoint_tables_shape")
            for row in rows:
                need(type(row) is dict, "checkpoint_row_not_object")
                yield table, row


def physical_reference(value, table, oracle):
    # Kernel Typed rows retain their exact RecordId even when a separate
    # physical table stores them (for example doc_node in chunk_embedding).
    # The pair (physical table, reference) remains the inventory identity.
    domain = oracle.reference(value)
    if domain is not None:
        return domain
    return oracle.physical_record_identity(value, table)


def decoded_policy(values, oracle, original, fixture, tokens, *, history=True):
    """Apply the existing independent row policy to each physical old/new row."""
    a_ref = oracle.source_reference(fixture["A"]["source_id"])
    intent = oracle.fixture_contract(fixture)
    original_refs = {row["reference"] for row in original.values()
                     if isinstance(row["reference"], oracle.Reference)}
    original_by_ref = {(row["table"], row["reference"]): row for row in original.values()
                       if isinstance(row["reference"], oracle.Reference)}
    closure, exclusive = {a_ref}, set()
    while True:
        added = {(row["table"], row["reference"]) for row in original.values()
                 if isinstance(row["reference"], oracle.Reference)
                 and (row["table"], row["reference"]) not in exclusive and row["table"] not in oracle.CONTROL_TABLES
                 and (row["reference"] in closure or row["canaries"]["A"] or row["references"] & closure)}
        if not added:
            break
        exclusive.update(added)
        closure.update(ref for _, ref in added)
    membership_rows, guards, effective = {}, {}, {}
    checkpoint_identities = set()
    for index, physical in enumerate(values):
        payload = physical["mutations"] if index > 0 else physical
        apply = index == 0 or physical["apply"]
        if index > 0:
            # Validate all target identities before applying even deletion-only frames.
            list(typed_rows(payload, True))
            targets = set()
            for change in payload:
                field = "id" if change["kind"] == "typed" else "key"
                target = (change["table"], change[field])
                need(target not in targets, "duplicate_wal_physical_target")
                targets.add(target)
                if apply and change["value"] is None:
                    field = "id" if change["kind"] == "typed" else "key"
                    ref = physical_reference(change[field], change["table"], oracle)
                    effective.pop((change["table"], ref), None)
        for table, logical in typed_rows(payload, index > 0):
            ref = physical_reference(logical.get("id"), table, oracle)
            if index == 0:
                need((table, ref) not in checkpoint_identities, "duplicate_checkpoint_physical_identity")
                checkpoint_identities.add((table, ref))
            if apply:
                effective[(table, ref)] = logical
            row = {"reference": ref, "table": table, "record": logical, "space": logical.get("space_id")}
            refs = {found for value in oracle.walk(logical) if (found := oracle.reference(value)) is not None}
            invariant(not contains(logical, fixture["A"]["canary"]), "erased_canary_in_decoded_physical_row")
            invariant((table, ref) not in exclusive, "erased_source_exclusive_decoded_physical_row")
            if table == "source_erasure_membership":
                # Private live checkpoints legitimately retain payload-free
                # tokens. This explicit shape is independently decoded, never
                # copied from the product's erasure selection algorithm.
                need(set(logical) == {"id", "space_id", "token"}
                     and type(logical["space_id"]) is str and type(logical["token"]) is str
                     and HEX.fullmatch(logical["token"]), "private_live_membership_shape")
                need(logical["token"] in tokens.get(logical["space_id"], set()), "private_live_membership_not_in_original_journal")
                if index == 0:
                    members = membership_rows.setdefault(logical["space_id"], set())
                    need(logical["token"] not in members, "duplicate_private_membership_token")
                    members.add(logical["token"])
            elif table in ("native_read_fence", "canonical_epoch", "source_erasure_guard"):
                oracle.payload_free_fence(row, fixture)
                if index == 0 and table == "source_erasure_guard":
                    need(logical["space_id"] not in guards, "duplicate_private_guard")
                    guards[logical["space_id"]] = logical["membership_count"]
            elif a_ref in refs or refs & closure or (row["space"] == fixture["A"]["space_id"]
                                                     and table in oracle.CONTROL_TABLES):
                oracle.retained_control(row, fixture, intent, original_by_ref.get((table, ref)), original_refs)
    need(all(membership_rows.get(space, set()) == expected and guards.get(space) == len(expected)
             for space, expected in tokens.items()), "private_live_membership_inventory_or_guard_count")
    if history:
        final = {"tables": {}}
        for (table, _), row in effective.items():
            final["tables"].setdefault(table, []).append(row)
        decoded_policy([final], oracle, original, fixture, tokens, history=False)
    else:
        def tagged(value):
            if value is None: return {"kind": "null"}
            if type(value) is bool: return {"kind": "bool", "value": value}
            if type(value) is int:
                return {"kind": "int", "value": value} if -(2**63) <= value < 2**63 else {"kind": "uint", "value": str(value)}
            if type(value) is float: return {"kind": "float_bits", "value": struct.pack(">d", value).hex()}
            if type(value) is str: return {"kind": "string", "value": value}
            if type(value) is list: return {"kind": "array", "value": [tagged(x) for x in value]}
            need(type(value) is dict, "unsupported_embedded_value")
            return {"kind": "map", "value": {key: tagged(value[key]) for key in sorted(value)}}
        expected = {(row["table"], row["reference"]): row["sha256"] for row in original.values()
                    if row["space"] == fixture["B"]["space_id"]}
        actual = {key: oracle.digest(oracle.encoded(tagged(row))) for key, row in effective.items()
                  if row.get("space_id") == fixture["B"]["space_id"]}
        invariant(actual == expected and bool(expected), "independent_B_complete_live_rows_changed")


def journal_memberships(capture, fixture, *, with_history=False):
    identity, fingerprint = fixture["journal"]["journal_id"], fixture["journal"]["reference_key_fingerprint"]
    evidence = capture["membership"]
    need(evidence["fingerprint"].decode().strip() == fingerprint, "original_reference_fingerprint")
    def checked(raw, fields, domain=b"meerkat-recall/reference-journal/v1\0"):
        envelope = decode(raw)
        need(set(envelope) == {"payload", "checksum"}, "membership_checked_envelope")
        payload = envelope["payload"]
        need(type(payload) is dict and set(payload) == set(fields), "membership_payload_shape")
        original_order = {key: payload[key] for key in fields}
        if "scopes" in fields:
            need(type(payload["scopes"]) is dict and all(type(item) is dict
                 and set(item) == {"history_fingerprints", "erased_versions"}
                 for item in payload["scopes"].values()), "history_payload_free_scope_shape")
            original_order["scopes"] = {scope: {field: payload["scopes"][scope][field]
                for field in ("history_fingerprints", "erased_versions")} for scope in sorted(payload["scopes"])}
        encoded = json.dumps(original_order, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        need(sha(domain + encoded) == envelope["checksum"], "membership_checksum")
        need(type(payload["version"]) is int and payload["version"] == 1 and payload["journal_identity"] == identity
             and payload["key_fingerprint"] == fingerprint, "membership_original_identity")
        return payload
    index_fields = ("version", "journal_identity", "key_fingerprint", "intents")
    for optional in ("history_intents", "restore_origins_required"):
        if optional in decode(evidence["index"]).get("payload", {}):
            index_fields += (optional,)
    index = checked(evidence["index"], index_fields)
    need(type(index["intents"]) is list and index["intents"] == sorted(set(index["intents"]))
         and all(type(x) is str and str(uuid.UUID(x)) == x for x in index["intents"]), "membership_intent_index")
    need("restore_origins_required" not in index or index["restore_origins_required"] is True,
         "origin_required_index_shape")
    files = archive(evidence["files"], "reference-memberships")
    need(set(files) == {"reference-memberships/" + identifier + ".json" for identifier in index["intents"]},
         "membership_complete_file_inventory")
    intents = {entry["intent"]["intent_id"]: entry["intent"] for entry in fixture["journal"]["after_intents"]}
    need(set(index["intents"]) == set(intents), "completed_original_intents_have_membership")
    tokens = {}
    for identifier in index["intents"]:
        value = checked(files["reference-memberships/" + identifier + ".json"],
            ("version", "journal_identity", "key_fingerprint", "intent_id", "intent_digest", "tokens"))
        need(identifier == value["intent_id"] and identifier in intents, "membership_original_intent")
        intent = intents[identifier]
        raw = json.dumps({key: intent[key] for key in ("intent_id", "space_id", "source_id", "recorded_at")},
                         ensure_ascii=False, separators=(",", ":")).encode()
        need(value["intent_digest"] == sha(b"meerkat-recall/reference-journal/v1\0" + raw)
             and type(value["tokens"]) is list and value["tokens"] == sorted(set(value["tokens"]))
             and all(type(token) is str and HEX.fullmatch(token) for token in value["tokens"]),
             "membership_tokens_or_intent_digest")
        tokens.setdefault(intent["space_id"], set()).update(value["tokens"])
    history_ids = index.get("history_intents", [])
    need(type(history_ids) is list and history_ids == sorted(set(history_ids))
         and all(type(identifier) is str and identifier in intents for identifier in history_ids)
         and ("history_intents" not in index or bool(history_ids)), "history_original_intent_index")
    history_raw = evidence.get("history")
    history_files = {} if history_raw is None else archive(history_raw, "history-commitments")
    need(set(history_files) == {"history-commitments/" + identifier + ".json" for identifier in history_ids},
         "history_complete_acknowledged_file_inventory")
    proofs = {}
    for identifier in history_ids:
        proof = checked(history_files["history-commitments/" + identifier + ".json"],
            ("version", "journal_identity", "key_fingerprint", "intent_id", "intent_digest", "scopes"),
            b"meerkat-recall/history-journal/v1\0")
        intent = intents[identifier]
        raw = json.dumps({key: intent[key] for key in ("intent_id", "space_id", "source_id", "recorded_at")},
                         ensure_ascii=False, separators=(",", ":")).encode()
        need(proof["intent_id"] == identifier
             and proof["intent_digest"] == sha(b"meerkat-recall/history-journal/v1\0" + raw)
             and type(proof["scopes"]) is dict and 0 < len(proof["scopes"]) <= 1024,
             "history_original_intent_or_scope")
        count = 0
        for scope, commitment in proof["scopes"].items():
            need(type(scope) is str and HEX.fullmatch(scope) and type(commitment) is dict
                 and set(commitment) == {"history_fingerprints", "erased_versions"}, "history_payload_free_scope_shape")
            for field in ("history_fingerprints", "erased_versions"):
                values = commitment[field]
                need(type(values) is list and values == sorted(set(values))
                     and all(type(value) is str and HEX.fullmatch(value) for value in values),
                     "history_payload_free_commitments")
                count += len(values)
            need(bool(commitment["history_fingerprints"]) and count <= 1_000_000, "history_commitment_bound")
        proofs[identifier] = proof["scopes"]
    return (tokens, proofs) if with_history else tokens


def history_monotonic(previous, current):
    for intent, scopes in previous.items():
        invariant(intent in current, "acknowledged_history_intent_lost")
        for scope, commitments in scopes.items():
            invariant(scope in current[intent], "acknowledged_history_scope_lost")
            for field, values in commitments.items():
                invariant(set(values) <= set(current[intent][scope][field]), "acknowledged_history_commitments_lost")


def native_export_guards(raw, oracle, fixture, phase, tokens):
    report = oracle.strict_json(raw)
    oracle.report_inventory(report, fixture["snapshots"][phase]["manifest"], fixture["backend"], fixture)
    decoder = oracle.TaggedDecoder(report.get("blobs"))
    guards = {}
    for row in report["rows"]:
        if row["identity"]["table"] != "source_erasure_guard":
            continue
        _, logical = decoder.value(row["record"], refs=report["format"] == "recall-snapshot-inspection-v2")
        _, ref = decoder.identity(row["identity"])
        oracle.payload_free_fence({"table": "source_erasure_guard", "reference": ref,
                                  "record": logical}, fixture)
        space = logical["space_id"]
        need(type(space) is str and space not in guards, "native_export_duplicate_guard")
        guards[space] = logical["membership_count"]
    invariant(all(guards.get(space) == len(expected) for space, expected in tokens.items())
              and all(count == len(tokens.get(space, set())) for space, count in guards.items()),
              "native_export_guard_original_membership_counts")


def evaluate(captures, fixture, export_result, *, oracle=None, original_report=None, private_oracle=None,
             phase_reports=None):
    result = {"format": "recall-physical-inventory/v1", "scope": "owned_stopped_product_volumes",
              "product_acceptance": False, "verdict": "unmet", "checks": [], "unmet": [], "failures": [],
              "coverage": {"media_erasure_behavior": "unexercised by original Metadata fixture"}}
    def incomplete(error):
        result["unmet"].append(str(error) if isinstance(error, Incomplete) else "incomplete_" + type(error).__name__)
    if export_result.get("verdict") == "fail":
        result["failures"].append("independent_export_component_failed")
    elif export_result.get("verdict") != "pass":
        result["unmet"].append("independent_export_component_unmet")
    # Positive evidence is useful even when an unrelated phase is malformed.
    # Validate each independent root before recording its definite byte hit.
    # These two live roots never borrow a registered-backup retention exemption.
    try:
        marker = fixture["A"]["canary"].encode()
        for phase in ("post", "replay", "expired"):
            capture = captures.get(phase, {})
            private = capture.get("private_membership")
            if type(private) is bytes:
                positive = marker in private
                if not positive and len(private) <= CAPTURE_CAP:
                    try:
                        positive = contains(decode(private), fixture["A"]["canary"])
                    except (ValueError, TypeError, RecursionError):
                        pass  # A malformed capture is reported by its full validator.
                if positive:
                    result["failures"].append(phase + "_positive_original_canary_in_private_native_rows")
            origins = capture.get("membership", {}).get("origins")
            if type(origins) is bytes:
                positive = marker in origins
                if not positive and len(origins) <= ORIGIN_CAP:
                    try:
                        positive = contains(decode(origins), fixture["A"]["canary"])
                    except (ValueError, TypeError, RecursionError):
                        pass
                if positive:
                    result["failures"].append(phase + "_positive_original_canary_in_restore_origins")
            if capture.get("stopped") is not True:
                continue
            for root in ("data", "database"):
                try:
                    for name, value in archive(capture["archives"][root], root).items():
                        if marker in value and (root == "database" or name in {"data/state.snapshot", "data/state.snapshot.durable/commits.wal"}):
                            result["failures"].append(phase + "_positive_original_canary:" + name)
                except (ValueError, KeyError, TypeError, tarfile.TarError):
                    pass  # The complete pass below reports this missing evidence.
    except (KeyError, TypeError, AttributeError):
        pass
    try:
        need(export_result.get("scope") == "exported_inventory" and export_result.get("product_acceptance") is False,
             "independent_export_component_required")
        need(set(captures) == {"pre", "post", "replay", "expired"}, "four_original_captures_required")
        canary, journal_id = fixture["A"]["canary"], fixture["journal"]["journal_id"]
        original = None
        if oracle is not None and original_report is not None:
            original, _ = oracle.report_inventory(oracle.strict_json(original_report),
                fixture["snapshots"]["pre"]["manifest"], fixture["backend"], fixture)
        else:
            result["unmet"].append("independent_original_typed_inventory_missing")
        files, registrations = {}, {}
        for phase, capture in captures.items():
            try:
                need(capture["journal_id"] == journal_id, "original_journal_custody")
                timestamp(capture["observed_at"])
                value = inventories(capture)
                files[phase] = value
                registrations[phase] = registry(value, journal_id)
            except (ValueError, KeyError, TypeError, UnicodeError, RecursionError, tarfile.TarError) as error:
                incomplete(error)
        need("pre" in files and "pre" in registrations, "original_pre_inventory_incomplete")
        if fixture["backend"] == "elephant-db":
            need(any(canary.encode() in raw or (name.endswith(".snapshot") and contains(decode(raw), canary))
                     for name, raw in files["pre"].items() if name.startswith(("data/", "database/"))),
                 "original_live_canary_positive_missing")
        else:
            # Native compression need not preserve a literal canary. Its actual
            # populated physical capture is tied to the independent complete
            # typed source and accepted-input positives in the original export.
            need(original is not None and all(any(row["table"] == table and row["positives"].get(side)
                     for row in original.values()) for side in ("A", "B") for table in ("source", "source_revision")),
                 "original_native_complete_typed_positives_missing")
            need(any(NATIVE_ROOT + "/" + name in files["pre"] for name in ("IDENTITY", "recall-engine.identity"))
                 and any(raw for name, raw in files["pre"].items()
                 if name.startswith(NATIVE_ROOT + "/") and name.endswith((".sst", ".ldb", ".log", ".blob", ".clog"))),
                 "original_native_populated_volume_missing")
            directories = set()
            for root in ROOTS:
                archive(captures["pre"]["archives"][root], root, directory_inventory=directories)
            original_metadata = file_metadata(captures["pre"]["metadata"], files["pre"], directories)
            need(original_metadata.get(NATIVE_ROOT, {}).get("kind") == "d", "original_native_actual_directory_missing")
        baseline = fixture["snapshots"]["pre"]["manifest"]
        need(baseline["snapshot_id"] in registrations["pre"], "original_baseline_backup_registry_missing")
        baseline_row = registrations["pre"][baseline["snapshot_id"]]
        need(baseline_row["members"] and decode(files["pre"][baseline_row["relative"] + "/manifest.json"]) == baseline,
             "original_baseline_backup_actual_generation")
        result["checks"].append("original_populated_product_files_and_journal")
        times = [timestamp(captures[phase]["observed_at"]) for phase in ("pre", "post", "replay", "expired")]
        need(times == sorted(times), "capture_order")
        previous_history = {}
        previous_origins = {"required": False, "identities": {}, "scopes": set()}
        try:
            pre_fixture = dict(fixture, journal=dict(fixture["journal"], after_intents=fixture["journal"]["before_intents"]))
            _, previous_history = journal_memberships(captures["pre"], pre_fixture, with_history=True)
            previous_origins = restored_origins(captures["pre"], fixture)
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, tarfile.TarError) as error:
            incomplete(error)
        origin_members = {}
        for captured_phase, registered in registrations.items():
            origin_members[captured_phase] = set()
            for generation in registered.values():
                if generation.get("raw_sha256") is not None or not generation["members"]:
                    continue
                sidecar = generation["relative"] + "/history-origin.json"
                manifest = decode(files[captured_phase][generation["relative"] + "/manifest.json"])
                if manifest.get("backend") != "surreal-db":
                    continue
                try:
                    need(sidecar in files[captured_phase], "native_registered_backup_origin_missing")
                    origin_envelope(files[captured_phase][sidecar], fixture, manifest=manifest)
                    generation["members"][sidecar] = sha(files[captured_phase][sidecar])
                    origin_members[captured_phase].add(sidecar)
                except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
                    incomplete(error)
        for phase in ("post", "replay", "expired"):
            if phase not in files:
                continue
            value = files[phase]
            observed = timestamp(captures[phase]["observed_at"])
            current = registrations.get(phase, {})
            try:
                _, history = journal_memberships(captures[phase], fixture, with_history=True)
                history_monotonic(previous_history, history)
                previous_history = history
                origins = restored_origins(captures[phase], fixture)
                origins_monotonic(previous_origins, origins)
                previous_origins = origins
            except Exception as error:
                if isinstance(error, Violation):
                    result["failures"].append(str(error))
                else:
                    incomplete(error)
            # Only a byte-exact, originally registered generation may retain
            # original source data before its original immutable deadline.
            exempt = set()
            for identifier, original_generation in registrations["pre"].items():
                active = current.get(identifier)
                if active and any(active.get(key) != original_generation.get(key)
                                  for key in ("path", "snapshot_id", "expires_at", "raw_sha256")):
                    result["failures"].append("original_registered_generation_changed")
                expired = timestamp(original_generation["expires_at"]) <= observed
                for name, digest in original_generation["members"].items():
                    if name in value and sha(value[name]) == digest:
                        if expired:
                            result["failures"].append(phase + "_expired_original_generation_retained")
                        elif active:
                            exempt.add(name)
                if phase == "expired" and not expired:
                    result["unmet"].append("original_retention_deadline_not_observed:" + identifier)
            known = set()
            if fixture["backend"] == "elephant-db":
                try:
                    values, known = embedded(value, journal_id)
                    for decoded in values:
                        invariant(not contains(decoded, canary), phase + "_erased_payload_in_checkpoint_or_wal")
                    if original is not None:
                        decoded_policy(values, oracle, original, fixture, journal_memberships(captures[phase], fixture))
                    result["checks"].append(phase + "_complete_embedded_checkpoint_and_historical_wal_policy")
                except Exception as error:
                    if isinstance(error, Violation) or (oracle is not None and isinstance(error, oracle.Violation)):
                        result["failures"].append(str(error))
                    else:
                        incomplete(error)
            else:
                try:
                    need(NATIVE_RECEIPT in value,
                         phase + "_native_storage_requires_complete_offline_decoder_or_verified_reclamation")
                    previous_phase = {"post": "pre", "replay": "post", "expired": "replay"}[phase]
                    need(previous_phase in files, "native_prior_original_capture_missing")
                    prior = (captures[previous_phase], files[previous_phase])
                    known = native_generation(captures[phase], value, fixture, phase, prior)
                    result["checks"].append(phase + "_actual_native_fresh_generation_and_complete_volume_custody")
                except Exception as error:
                    if isinstance(error, Violation):
                        result["failures"].append(str(error))
                    else:
                        incomplete(error)
                try:
                    need(private_oracle is not None and "private_membership" in captures[phase],
                         phase + "_native_actual_private_membership_inspection_missing")
                    tokens = journal_memberships(captures[phase], fixture)
                    private_oracle.validate(captures[phase]["private_membership"], tokens, canary)
                    report_phase = "replay" if phase == "expired" else phase
                    need(oracle is not None and phase_reports is not None and report_phase in phase_reports,
                         "native_actual_guard_export_missing")
                    native_export_guards(phase_reports[report_phase], oracle, fixture, report_phase, tokens)
                    result["checks"].append(phase + "_actual_native_complete_private_memberships_and_schema")
                except Exception as error:
                    if isinstance(error, Violation) or (private_oracle is not None and isinstance(error, private_oracle.Violation)):
                        result["failures"].append(str(error))
                    else:
                        incomplete(error)
            for name, raw in value.items():
                if name.startswith("registry/") or name in exempt:
                    continue
                if canary.encode() in raw:
                    result["failures"].append(phase + "_erased_canary_in_product_file:" + name)
                if name in known or name in origin_members.get(phase, set()) or not raw:
                    continue
                # Complete post-erasure registered exports use the independent
                # export oracle; every other nonempty file must be interpreted.
                registered = next((row for row in current.values() if name in row["members"]), None)
                if registered is not None and registered.get("raw_sha256") is None:
                    manifest = decode(value[registered["relative"] + "/manifest.json"])
                    permitted = [fixture["snapshots"][name]["manifest"] for name in ("post", "replay")]
                    if manifest in permitted:
                        continue
                if name == "data/.elephant/elephant_config.toml":
                    # Configuration is outside source storage but still inspect
                    # its actual decoded strings, not a filename blanket skip.
                    import tomllib
                    try:
                        invariant(not contains(tomllib.loads(raw.decode()), canary), "erased_payload_in_runtime_configuration")
                        continue
                    except Violation as error:
                        result["failures"].append(str(error))
                    except (ValueError, UnicodeError) as error:
                        incomplete(error)
                result["unmet"].append(phase + "_unclassified_file:" + name)
            # Every observed registered generation keeps its original deadline;
            # compare the first actual observation even if created after erase.
            for earlier in ("pre", "post", "replay"):
                if earlier == phase:
                    break
                for identifier, old in registrations.get(earlier, {}).items():
                    active = current.get(identifier)
                    if active and any(active.get(key) != old.get(key)
                                      for key in ("path", "snapshot_id", "expires_at", "raw_sha256")):
                        result["failures"].append("observed_registered_generation_changed")
                    if phase == "expired":
                        if timestamp(old["expires_at"]) > observed:
                            result["unmet"].append("observed_retention_deadline_not_reached:" + identifier)
                        elif any(name in value and sha(value[name]) == digest for name, digest in old["members"].items()):
                            result["failures"].append("expired_observed_generation_retained")
            for identifier, row in current.items():
                if timestamp(row["expires_at"]) <= observed and any(name in value for name in row["members"]):
                    result["failures"].append(phase + "_expired_registered_files_retained")
        result["checks"].append("all_original_registered_generation_deadlines_and_physical_bytes")
        result["coverage"]["restore_origin_authentication"] = "not verified by collector; opaque inventory only; original product owns HMAC verification"
    except Exception as error:
        if isinstance(error, Violation) or (oracle is not None and isinstance(error, oracle.Violation)):
            result["failures"].append(str(error))
        else:
            incomplete(error)
    result["verdict"] = "fail" if result["failures"] else "unmet" if result["unmet"] else "pass"
    return result
