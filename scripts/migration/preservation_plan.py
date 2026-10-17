#!/usr/bin/env python3
"""Offline preservation inventory and delta validation; never a database writer."""
from __future__ import annotations

import argparse
import base64
import binascii
from contextlib import closing
import hashlib
import json
import math
import re
import sqlite3
from pathlib import Path

HEX = re.compile(r"[0-9a-f]{64}\Z")
TABLE = re.compile(r"[a-z][a-z0-9_]*\Z")
SAFE_KEY = re.compile(r"[A-Za-z0-9._-]{1,120}\Z")
FORMAT = "recall-preservation-plan-v1"
HASH_ALGORITHM = "homecore-python-json-v1"
HASH_CONTRACT = {
    "id": HASH_ALGORITHM,
    "encoding": "UTF-8; json.dumps(sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)",
    "numbers": "Python arbitrary precision integers and finite IEEE754 binary64 floats with Python JSON rendering; distinguish 1, 1.0 and negative zero",
    "runtime": "Offline evidence hashes only. Do not compare against Rust canonical hashes without an explicit compatible adapter; input file hashes bind original lexical bytes.",
}
# Inventory retains every table. These authorities require journal-aware restore,
# never a migration-generated source head or an inferred erasure waiver.
MODERN_AUTHORITY = frozenset("""
source_head source_identity source_revision source_operation source_authorization
source_effect_support source_commit_receipt source_decision_receipt source_decision_sequence
source_publication_manifest source_publication_manifest_history source_publication_completion
source_document_mapping source_revision_mapping source_bundle_registration
source_tombstone_ref source_erasure_files source_erasure_guard source_erasure_membership
recall_durability publication_schema source_contact_intent work_attempt work_producer_origin
canonical_epoch native_read_fence provider_call_receipt
""".split())
LEGACY_TABLES = frozenset("""
space_config entity source cost_ledger_daily ingest_decision doc doc_revision doc_node
media_asset chunk_embedding entity_identity_key identity_candidate entity_merge
predicate_registry predicate_alias assertion attribute rel conflict_group truth_slot event
freshness reprocess_job work_item work_edge work_run work_item_archive work_run_archive
work_artifact work_deadletter system_event audit_log id_map doc_chunk rel_evidence
knowledge_view knowledge_view_revision knowledge_view_definition knowledge_view_dependency
source_watch source_watch_check fact migration_history
""".split())


OPERATIONAL_TABLES = ("audit_log", "cost_ledger_daily", "work_item", "work_run",
    "work_item_archive", "work_run_archive", "work_artifact", "work_edge",
    "work_deadletter", "system_event")
OPERATIONAL_DISPOSITIONS = ("native", "managed_archive_v1")


def operational_mode(value):
    require(value in OPERATIONAL_DISPOSITIONS, "unsupported_operational_disposition")
    return value


def baseline_operational_mode(db):
    saved = db.execute("SELECT v FROM metadata WHERE k='operational_disposition'").fetchone()
    return operational_mode("native" if saved is None else strict_load(saved[0]))


def archive_inventory(db, counts):
    """Exact Rust ArchiveInventoryDigest framing over independently consumed bytes."""
    selected = {t:n for t,n in sorted(counts.items()) if t in OPERATIONAL_TABLES}
    digest = hashlib.sha256(b"recall-operational-archive-rows-v1\0")
    rows = total_bytes = 0
    for table, count in selected.items():
        digest.update(canonical([table, count]))
        actual = 0
        for key, raw, lexical_hash in db.execute("SELECT k,lexical,lexical_sha256 FROM raw_rows WHERE t=? ORDER BY k", (table,)):
            require(sha(raw) == lexical_hash, "archive_original_hash_mismatch")
            digest.update(canonical([table, key, lexical_hash, len(raw)]))
            actual += 1
            total_bytes += len(raw)
        require(actual == count, "archive_source_count_mismatch")
        rows += actual
    mode_hash = hashlib.sha256(b"recall-managed-operational-archive-v1\0")
    for table in OPERATIONAL_TABLES:
        mode_hash.update(table.encode() + b"\0")
    return {"format":"recall-operational-archive-source-inventory-v1",
            "disposition_sha256":mode_hash.hexdigest(), "tables":selected,
            "rows":rows, "lexical_bytes":total_bytes,
            "ordered_inventory_sha256":digest.hexdigest(), "runtime_verified":False}


class Refused(ValueError):
    """Inputs do not prove the declared offline plan."""


def require(ok, code):
    if not ok:
        raise Refused(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def value_hash(value):
    return sha(canonical(value))


def check_sha(value):
    require(type(value) is str and HEX.fullmatch(value), "invalid_sha256")
    return value


def strict_pairs(items):
    out = {}
    for key, value in items:
        require(key not in out, "duplicate_json_key")
        out[key] = value
    return out


def finite(value):
    result = float(value)
    require(math.isfinite(result), "nonfinite_json")
    return result


def nonfinite(_):
    raise Refused("nonfinite_json")


DECODER = json.JSONDecoder(object_pairs_hook=strict_pairs, parse_float=finite,
                           parse_constant=nonfinite)


def strict_load(raw):
    return json.loads(raw, object_pairs_hook=strict_pairs, parse_float=finite,
                      parse_constant=nonfinite)


class JsonStream:
    """Incremental UTF-8 JSON reader; bounds a single value, not the whole file."""

    def __init__(self, path, max_value_bytes=64 * 1024 * 1024, chunk_bytes=1024 * 1024):
        import codecs
        require(type(max_value_bytes) is int and max_value_bytes > 0 and chunk_bytes > 0,
                "invalid_value_limit")
        self.file = Path(path).open("rb")
        self.utf8 = codecs.getincrementaldecoder("utf-8")("strict")
        self.buffer = ""
        self.pos = 0
        self.eof = False
        self.digest = hashlib.sha256()
        self.bytes = 0
        self.limit = max_value_bytes
        self.chunk = min(chunk_bytes, max_value_bytes)
        self.tables = []
        self.last_value_raw = b""
        self.export_notes = None
        self.export_notes_sink = None
        self.row_counts = {}

    def close(self):
        self.file.close()

    def fill(self):
        self.buffer = self.buffer[self.pos:]
        self.pos = 0
        raw = self.file.read(self.chunk)
        self.digest.update(raw)
        self.bytes += len(raw)
        self.eof = not raw
        self.buffer += self.utf8.decode(raw, final=self.eof)

    def peek(self):
        while True:
            while self.pos < len(self.buffer) and self.buffer[self.pos] in " \t\r\n":
                self.pos += 1
            if self.pos < len(self.buffer):
                return self.buffer[self.pos]
            if self.eof:
                return ""
            self.fill()

    def take(self, expected):
        require(self.peek() == expected, "invalid_json_structure")
        self.pos += 1

    def value(self):
        require(bool(self.peek()), "truncated_json")
        while True:
            try:
                value, end = DECODER.raw_decode(self.buffer, self.pos)
            except json.JSONDecodeError:
                require(not self.eof, "invalid_json_value")
                require(len(self.buffer[self.pos:].encode("utf-8")) <= self.limit,
                        "json_value_limit")
                self.fill()
                continue
            # A numeric prefix at a read boundary is not yet a complete value.
            if end == len(self.buffer) and not self.eof:
                require(len(self.buffer[self.pos:end].encode("utf-8")) <= self.limit,
                        "json_value_limit")
                self.fill()
                continue
            # raw_decode can accept the integer prefix of a partial exponent or
            # fraction. Wait for a delimiter before rejecting that token.
            if end < len(self.buffer) and self.buffer[end] not in " \t\r\n,]}:":
                suffix = self.buffer[self.pos:]
                if type(value) in (int, float) and not self.eof and not any(c in " \t\r\n,]}:" for c in suffix):
                    require(len(suffix.encode("utf-8")) <= self.limit, "json_value_limit")
                    self.fill()
                    continue
            require(len(self.buffer[self.pos:end].encode("utf-8")) <= self.limit,
                    "json_value_limit")
            require(end == len(self.buffer) or self.buffer[end] in " \t\r\n,]}:",
                    "invalid_json_delimiter")
            self.last_value_raw = self.buffer[self.pos:end].encode("utf-8")
            self.pos = end
            return value

    def members(self):
        self.take("{")
        seen = set()
        if self.peek() == "}":
            self.take("}")
            return
        while True:
            key = self.value()
            require(type(key) is str and key not in seen, "duplicate_or_invalid_member")
            seen.add(key)
            self.take(":")
            yield key
            if self.peek() == "}":
                self.take("}")
                return
            self.take(",")

    def array(self):
        self.take("[")
        if self.peek() == "]":
            self.take("]")
            return
        while True:
            yield self.value()
            if self.peek() == "]":
                self.take("]")
                return
            self.take(",")

    def snapshot(self):
        headers = {}
        for key in self.members():
            require(key in {"backend", "version", "exported_at", "tables", "export_notes"},
                    "unsupported_snapshot_envelope")
            if key == "export_notes":
                from export_notes import read_notes
                self.export_notes = read_notes(self, require, self.export_notes_sink)
                continue
            if key != "tables":
                headers[key] = self.value()
                continue
            headers[key] = True
            for table in self.members():
                require(TABLE.fullmatch(table), "invalid_table")
                self.tables.append(table)
                self.row_counts[table] = 0
                for row in self.array():
                    require(type(row) is dict, "row_not_object")
                    self.row_counts[table] += 1
                    yield table, row
        if self.export_notes is not None:
            require(self.export_notes["output_counts"] == self.row_counts, "export_notes_row_count_mismatch")
        require(set(headers) == {"backend", "version", "exported_at", "tables"}
                and headers["backend"] == "elephant-db"
                and type(headers["version"]) is int and headers["version"] == 1,
                "unsupported_snapshot")
        from datetime import datetime
        try:
            stamp = datetime.fromisoformat(headers["exported_at"].replace("Z", "+00:00"))
            require(stamp.tzinfo is not None, "snapshot_timestamp_timezone")
        except (ValueError, AttributeError) as error:
            raise Refused("invalid_snapshot_timestamp") from error
        require(self.peek() == "", "trailing_json")
        self.snapshot_header = {key: headers[key] for key in ("backend", "version", "exported_at")}


def source_id(prefix, key):
    require(type(key) is str and bool(key), "missing_source_key")
    return prefix + (key if SAFE_KEY.fullmatch(key) else "sha256-" + sha(key.encode())[:40])


def row_id(table, row):
    key = row.get("id")
    require(type(key) is str and key.startswith(table + ":") and len(key) > len(table) + 1,
            "invalid_row_identity")
    return key


def jsonl(path, max_line_bytes=64 * 1024 * 1024):
    with Path(path).open("rb") as handle:
        while raw := handle.readline(max_line_bytes + 1):
            require(len(raw) <= max_line_bytes, "jsonl_line_limit")
            require(raw.strip(), "blank_jsonl_line")
            row = strict_load(raw)
            require(type(row) is dict, "jsonl_row_not_object")
            yield raw, row


def reference_candidates(row):
    """Lexical reverse index only; typed source closure must validate each edge.

    Paths distinguish payload text/metadata from typed link fields. Dictionary
    keys are indexed too, with a trailing /@key. Root row identity is excluded.
    No ownership or transitive graph membership is inferred here.
    """
    stack = [("", row)]
    while stack:
        path, value = stack.pop()
        if isinstance(value, dict):
            for key, child in value.items():
                escaped = key.replace("~", "~0").replace("/", "~1")
                child_path = path + "/" + escaped
                if isinstance(child, (str, dict, list)):
                    stack.append((child_path, child))
                stack.append((child_path + "/@key", key))
        elif isinstance(value, list):
            stack.extend((path + "/" + str(i), child) for i, child in enumerate(value)
                         if isinstance(child, (str, dict, list)))
        elif isinstance(value, str) and path != "/id" and ":" in value:
            table, suffix = value.split(":", 1)
            if TABLE.fullmatch(table) and suffix and not any(c.isspace() for c in value):
                yield path, table, value


def create_index(path):
    # Create-only prevents accidental replacement of a prior plan or baseline.
    with Path(path).open("xb"):
        pass
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE rows(t TEXT, k TEXT, h TEXT, space TEXT, deleted INTEGER, PRIMARY KEY(t,k))")
    db.execute("CREATE TABLE raw_rows(t TEXT,k TEXT,lexical BLOB,lexical_sha256 TEXT,PRIMARY KEY(t,k))")
    db.execute("CREATE TABLE mappings(doc TEXT PRIMARY KEY, external TEXT, class TEXT, content TEXT, payload TEXT)")
    db.execute("CREATE TABLE documents(doc TEXT PRIMARY KEY, external TEXT, h TEXT, content TEXT, text_hash TEXT, payload TEXT)")
    db.execute("CREATE INDEX mappings_external ON mappings(external)")
    db.execute("CREATE INDEX documents_external ON documents(external,doc)")
    db.execute("CREATE TABLE reference_candidates(row_table TEXT,row_id TEXT,path TEXT,ref_table TEXT,ref_id TEXT,PRIMARY KEY(row_table,row_id,path,ref_table,ref_id))")
    db.execute("CREATE INDEX reverse_references ON reference_candidates(ref_table,ref_id,row_table,row_id)")
    db.execute("CREATE TABLE export_note_parts(field TEXT,part TEXT,lexical BLOB,lexical_sha256 TEXT,PRIMARY KEY(field,part))")
    db.execute("CREATE TABLE export_shadows(row_index INTEGER PRIMARY KEY,entity_id TEXT UNIQUE)")
    db.execute("CREATE TABLE source_row_ordinals(t TEXT,n INTEGER,k TEXT,PRIMARY KEY(t,n))")
    db.execute("CREATE TABLE metadata(k TEXT PRIMARY KEY, v TEXT)")
    return db


def metadata(db, key):
    row = db.execute("SELECT v FROM metadata WHERE k=?", (key,)).fetchone()
    require(row is not None, "baseline_metadata_missing")
    return json.loads(row[0])


def put_meta(db, key, value):
    db.execute("INSERT INTO metadata VALUES (?,?)", (key, canonical(value).decode()))



def add_mapping(db, item):
    doc = item.get("old_doc_id")
    require(type(doc) is str and doc.startswith("doc:"), "idmap_doc_identity")
    kind = item.get("class")
    require(kind in {"derived", "source_less"}, "idmap_class")
    key = item.get("source_key") if kind == "derived" else doc
    external = source_id("hc-src-" if kind == "derived" else "hc-legacy-", key)
    check_sha(item.get("content_sha256"))
    existing = db.execute("SELECT payload FROM mappings WHERE external=? LIMIT 1", (external,)).fetchone()
    if existing and kind == "derived":
        require(json.loads(existing[0]).get("source_key") == key, "external_identity_collision")
    db.execute("INSERT INTO mappings VALUES (?,?,?,?,?)",
               (doc, external, kind, item["content_sha256"], canonical(item).decode()))
    return external


def require_closed_sqlite(path):
    path = Path(path)
    require(path.is_file(), "ledger_backup_missing")
    with path.open("rb") as handle:
        header = handle.read(100)
    require(len(header) == 100 and header[:16] == b"SQLite format 3\0", "sqlite_header_invalid")
    require(header[18:20] == b"\x01\x01", "sqlite_wal_header_unbound")
    require(not any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")),
            "sqlite_sidecar_unbound")


def load_keep_backup(db, path, expected_sha256, cut_order, max_value_bytes):
    """Read only the explicit offline backup, never the live conveyor database."""
    check_sha(expected_sha256)
    require(cut_order == "store_cut_then_ledger_backup", "ledger_cut_order_required")
    require_closed_sqlite(path)
    require(file_sha(path) == expected_sha256, "ledger_backup_sha256_mismatch")
    db.execute("CREATE TABLE ledger_keep(doc_key TEXT PRIMARY KEY)")
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro&immutable=1", uri=True)) as ledger:
        require(ledger.execute("SELECT type FROM sqlite_schema WHERE name='decisions'").fetchone() == ("table",),
                "ledger_decisions_table_required")
        # Inspect byte length before fetching arbitrary values into Python.
        require(ledger.execute("SELECT count(*) FROM decisions WHERE keep=1 AND (typeof(doc_key)!='text' OR length(CAST(doc_key AS BLOB))=0 OR length(CAST(doc_key AS BLOB))>?)", (max_value_bytes,)).fetchone()[0] == 0,
                "ledger_keep_key_shape_or_bound")
        for (key,) in ledger.execute("SELECT doc_key FROM decisions WHERE keep=1"):
            db.execute("INSERT INTO ledger_keep VALUES (?)", (key,))
    require_closed_sqlite(path)
    require(file_sha(path) == expected_sha256, "ledger_backup_changed")
    return {"format":"homecore-conveyor-keep-v1", "sha256":expected_sha256,
            "cut_order":cut_order, "keys":db.execute("SELECT count(*) FROM ledger_keep").fetchone()[0],
            "classification_source":"homecore:0bfba844203c71d8d5b07594179404b2f74f04bc",
            "cut_order_verified_by_planner":False}


def fresh_mapping(db, doc):
    metadata = doc.get("metadata")
    key = None
    kind = "source_less"
    if not isinstance(metadata, dict):
        reason = "no_metadata"
    elif metadata.get("source_doc_keys"):
        reason = "multi_source_packet"
    else:
        keys = {metadata[name] for name in ("source_doc_key", "source_index_doc_key")
                if isinstance(metadata.get(name), str) and metadata[name]}
        if not keys:
            reason = "no_source_key"
        elif len(keys) > 1:
            reason = "conflicting_source_keys"
        else:
            key = next(iter(keys))
            if db.execute("SELECT 1 FROM ledger_keep WHERE doc_key=?", (key,)).fetchone():
                kind, reason = "derived", "conveyor_ledger_keep"
            else:
                reason = "source_key_not_kept_in_ledger"
    require(type(doc.get("title")) is str and type(doc.get("text")) is str, "document_text_shape")
    return {"old_doc_id":doc["id"], "class":kind, "reason":reason, "source_key":key,
            "content_sha256":value_hash({"title":doc["title"],"text":doc["text"]})}

def validate_media(row, *, max_decoded_bytes=64 * 1024 * 1024):
    """Validate inline bytes in bounded chunks; never open/resolve storage_path."""
    size, digest = row.get("size_bytes"), row.get("sha256")
    require(type(size) is int and 0 <= size <= max_decoded_bytes, "media_size_bound")
    check_sha(digest)
    require(type(row.get("storage_path")) is str, "media_storage_path_shape")
    payload = row.get("payload_base64")
    if payload is None:
        return {"inline": False, "decoded_bytes": 0, "external_bytes": size}
    require(type(payload) is str and len(payload) % 4 == 0, "media_base64_shape")
    require(len(payload) == 4 * ((size + 2) // 3), "media_encoded_length_mismatch")
    hasher = hashlib.sha256()
    decoded = 0
    for start in range(0, len(payload), 64 * 1024):
        chunk = payload[start:start + 64 * 1024]
        require(start + len(chunk) == len(payload) or "=" not in chunk, "media_nonfinal_padding")
        try:
            raw = base64.b64decode(chunk, validate=True)
        except (binascii.Error, ValueError) as error:
            raise Refused("media_invalid_base64") from error
        # Reject nonzero pad bits and alternate encodings of identical bytes.
        require(base64.b64encode(raw).decode("ascii") == chunk, "media_noncanonical_base64")
        decoded += len(raw)
        require(decoded <= size and decoded <= max_decoded_bytes, "media_decoded_length_mismatch")
        hasher.update(raw)
    require(decoded == size, "media_decoded_length_mismatch")
    require(hasher.hexdigest() == digest, "media_digest_mismatch")
    return {"inline": True, "decoded_bytes": decoded, "external_bytes": 0}


def media_summary():
    return {"rows": 0, "inline_rows": 0, "external_rows": 0, "decoded_inline_bytes": 0,
            "declared_external_bytes": 0, "inline_size_sha256_verified": True,
            "self_contained": True, "external_availability_verified": False}


def record_media(summary, row, max_bytes):
    proof = validate_media(row, max_decoded_bytes=max_bytes)
    summary["rows"] += 1
    summary["inline_rows" if proof["inline"] else "external_rows"] += 1
    summary["decoded_inline_bytes"] += proof["decoded_bytes"]
    summary["declared_external_bytes"] += proof["external_bytes"]
    summary["self_contained"] = summary["external_rows"] == 0


def inventory(snapshot, idmap, output, *, space, snapshot_sha256, idmap_sha256=None,
              ledger_backup=None, ledger_sha256=None, ledger_cut_order=None, target_space=None,
              max_value_bytes=64 * 1024 * 1024, operational_disposition="native"):
    operational_mode(operational_disposition)
    require(type(space) is str and space.strip() == space and space, "invalid_space")
    check_sha(snapshot_sha256)
    target_space = space if target_space is None else target_space
    require(type(target_space) is str and target_space.strip() == target_space and target_space,
            "invalid_target_space")
    fresh = ledger_backup is not None
    require((idmap is None) == fresh, "choose_idmap_or_ledger_backup")
    if fresh:
        require(idmap_sha256 is None, "fresh_idmap_hash_is_derived")
    else:
        check_sha(idmap_sha256)
        require(ledger_sha256 is None and ledger_cut_order is None, "unexpected_ledger_binding")
    output = Path(output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    db = create_index(output / "inventory.sqlite")
    stream = None
    try:
        ledger_binding = None
        if fresh:
            ledger_binding = load_keep_backup(db, ledger_backup, ledger_sha256, ledger_cut_order, max_value_bytes)
        else:
            digest = hashlib.sha256()
            for raw, item in jsonl(idmap, max_value_bytes):
                digest.update(raw)
                add_mapping(db, item)
            require(digest.hexdigest() == idmap_sha256, "idmap_sha256_mismatch")
        stream = JsonStream(snapshot, max_value_bytes)
        def retain_notes(field, part, value, raw, digest):
            db.execute("INSERT INTO export_note_parts VALUES (?,?,?,?)", (field,part,raw,digest))
            if field == "name_embedding_metadata_shadows":
                db.execute("INSERT INTO export_shadows VALUES (?,?)", (value["output_row_index"],value["id"]))
        stream.export_notes_sink = retain_notes
        counts = {}
        blockers = set()
        media = media_summary()
        held_work = {"work_item":0, "work_item_archive":0, "running":0}
        for table, row in stream.snapshot():
            key = row_id(table, row)
            row_space = row.get("space_id")
            if table != "migration_history":
                require(type(row_space) is str and bool(row_space), "row_space_missing")
            if table == "media_asset":
                record_media(media, row, max_value_bytes)
                if not media["self_contained"]:
                    blockers.add("external_media_availability_required")
            digest_row = value_hash(row)
            db.execute("INSERT INTO rows VALUES (?,?,?,?,?)",
                       (table, key, digest_row, row_space, int(row.get("deleted_at") is not None)))
            db.execute("INSERT INTO raw_rows VALUES (?,?,?,?)",
                       (table, key, stream.last_value_raw, sha(stream.last_value_raw)))
            db.executemany("INSERT INTO reference_candidates VALUES (?,?,?,?,?)",
                           ((table, key, path, target_table, target_id)
                            for path, target_table, target_id in reference_candidates(row)))
            db.execute("INSERT INTO source_row_ordinals VALUES (?,?,?)", (table,counts.get(table,0),key))
            counts[table] = counts.get(table, 0) + 1
            if table in MODERN_AUTHORITY:
                blockers.add("journal_aware_authority:" + table)
            elif table not in LEGACY_TABLES:
                blockers.add("unsupported_table:" + table)
            if table in {"work_item", "work_item_archive"}:
                held_work[table] += 1
                work = row if table == "work_item" else row.get("record", {})
                if work.get("status") == "running":
                    held_work["running"] += 1
            if table != "doc" or row_space != space or row.get("deleted_at") is not None:
                continue
            if fresh:
                add_mapping(db, fresh_mapping(db, row))
            match = db.execute("SELECT external,content FROM mappings WHERE doc=?", (key,)).fetchone()
            require(match is not None, "live_document_missing_idmap")
            title, text = row.get("title"), row.get("text")
            require(type(title) is str and type(text) is str, "document_text_shape")
            content = value_hash({"title": title, "text": text})
            require(content == match[1], "idmap_document_content_mismatch")
            payload = {"legacy_doc_id": key, "external_source_id": match[0],
                       "original_row_sha256": digest_row, "doc_content_sha256": content,
                       "original_text_sha256": sha(text.encode()),
                       "original_text_utf8_bytes": len(text.encode()),
                       "title_sha256": sha(title.encode()),
                       "security_sha256": value_hash(row.get("security")),
                       "provenance_sha256": value_hash(row.get("provenance")),
                       "accepted_request_fingerprint": None,
                       "accepted_revision": None}
            db.execute("INSERT INTO documents VALUES (?,?,?,?,?,?)",
                       (key, match[0], digest_row, content, payload["original_text_sha256"], canonical(payload).decode()))
        require(stream.digest.hexdigest() == snapshot_sha256, "snapshot_sha256_mismatch")
        if stream.export_notes is not None:
            for index, identity in db.execute("SELECT row_index,entity_id FROM export_shadows"):
                require(db.execute("SELECT k FROM source_row_ordinals WHERE t='entity' AND n=?", (index,)).fetchone() == (identity,), "export_shadow_row_binding")
            if stream.export_notes["inexact_decimal_values"]:
                blockers.add("native_export_inexact_decimal_review")
            if stream.export_notes["converted_geometry_values"]:
                blockers.add("native_export_geometry_review")
        for table in stream.tables:
            counts.setdefault(table, 0)
            if table in MODERN_AUTHORITY:
                blockers.add("journal_aware_authority:" + table)
            elif table not in LEGACY_TABLES:
                blockers.add("unsupported_table:" + table)
        if target_space != space:
            require(db.execute("SELECT count(*) FROM rows WHERE space=?", (target_space,)).fetchone()[0] == 0,
                    "target_space_collision")
        missing = db.execute("SELECT count(*) FROM mappings m LEFT JOIN documents d ON d.doc=m.doc WHERE d.doc IS NULL").fetchone()[0]
        require(missing == 0, "idmap_document_missing_deleted_or_foreign")
        if fresh:
            require_closed_sqlite(ledger_backup)
            require(file_sha(ledger_backup) == ledger_sha256, "ledger_backup_changed")
            with (output / "inventory.idmap.jsonl").open("xb") as handle:
                for (payload,) in db.execute("SELECT payload FROM mappings ORDER BY doc"):
                    handle.write(payload.encode() + b"\n")
            idmap_sha256 = file_sha(output / "inventory.idmap.jsonl")
        with (output / "documents.jsonl").open("xb") as handle:
            for (payload,) in db.execute("SELECT payload FROM documents ORDER BY external,doc"):
                handle.write(payload.encode() + b"\n")
        groups = 0
        with (output / "groups.jsonl").open("xb") as handle:
            for external, count in db.execute("SELECT external,count(*) FROM documents GROUP BY external ORDER BY external"):
                group_hash = hashlib.sha256()
                for doc, digest_row in db.execute("SELECT doc,h FROM documents WHERE external=? ORDER BY doc", (external,)):
                    group_hash.update(canonical([doc, digest_row]) + b"\n")
                handle.write(canonical({"external_source_id": external, "documents": count,
                             "document_mapping_sha256": group_hash.hexdigest()}) + b"\n")
                groups += 1
        runtime_requirements = []
        archive = archive_inventory(db, counts) if operational_disposition == "managed_archive_v1" else None
        if archive is not None:
            runtime_requirements.append({"kind":"managed_operational_archive_v1",
                "snapshot_sha256":snapshot_sha256,"source_inventory":archive,
                "historical_work":held_work,"disposition":"archive_inert","verified":False,
                "must_complete":"journal-owned exact original archive, native event floor, complete source accounting and recovery receipt before readiness"})
        elif held_work["work_item"] or held_work["work_item_archive"]:
            runtime_requirements.append({"kind":"historical_work_hold_v1", "snapshot_sha256":snapshot_sha256,
                "rows":held_work, "original_payload_unchanged":True, "verified":False,
                "must_complete":"register exact Unknown historical holds in original transfer custody after payload reconciliation, verify every marker before readiness"})
        result = {"format": FORMAT, "state": "offline_inventory_only", "space_id": space,
                  "source_space_id":space, "target_space_id":target_space,
                  "operational_disposition":operational_disposition,
                  "operational_archive_inventory":archive,
                  "runtime_requirements":runtime_requirements, "media_preservation":media,
                  "canonical_hash_contract": HASH_CONTRACT,
                  "spool_schema": "recall-original-lexical-row-spool-v1",
                  "snapshot_header": stream.snapshot_header,
                  "export_notes": stream.export_notes,
                  "reference_index": {"format": "recall-lexical-reference-candidates-v1",
                                      "rows": db.execute("SELECT count(*) FROM reference_candidates").fetchone()[0],
                                      "typed_membership_verified": False},
                  "snapshot_sha256": snapshot_sha256, "snapshot_bytes": stream.bytes,
                  "idmap_sha256": idmap_sha256,
                  "identity_input": {"mode":"fresh_bound_ledger" if fresh else "provided_idmap",
                                     "ledger":ledger_binding},
                  "tables": dict(sorted(counts.items())),
                  "documents": db.execute("SELECT count(*) FROM documents").fetchone()[0],
                  "groups": groups, "runtime_blockers": sorted(blockers),
                  "publication_certified": False, "vectors_adopted": False,
                  "graph_closure_certified": False,
                  "groups_sha256": file_sha(output / "groups.jsonl"),
                  "documents_sha256": file_sha(output / "documents.jsonl")}
        result["plan_id"] = value_hash(result)
        for key in ["format", "space_id", "snapshot_sha256", "idmap_sha256", "plan_id", "tables", "runtime_blockers", "canonical_hash_contract", "spool_schema", "snapshot_header", "reference_index", "identity_input", "export_notes", "source_space_id", "target_space_id", "runtime_requirements", "media_preservation", "operational_disposition", "operational_archive_inventory"]:
            put_meta(db, key, result[key])
        db.commit()
        db.close()
        result["inventory_sha256"] = file_sha(output / "inventory.sqlite")
        write_new(output / "plan.json", result)
        return result
    except (sqlite3.IntegrityError, UnicodeError) as error:
        raise Refused("duplicate_identity_or_invalid_utf8") from error
    finally:
        if stream:
            stream.close()
        db.close()


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while raw := handle.read(1024 * 1024):
            digest.update(raw)
    return digest.hexdigest()


def write_new(path, value):
    with Path(path).open("xb") as handle:
        handle.write(canonical(value) + b"\n")


def change_kind(before, after):
    require(before is not None or after is not None, "empty_delta_change")
    if before is None:
        return "created"
    if after is None:
        return "removed"
    old, new = bool(before.get("deleted_at")), bool(after.get("deleted_at"))
    if not old and new:
        return "deleted"
    if old and not new:
        return "restored"
    require(canonical(before) != canonical(after), "unchanged_delta_row")
    return "updated"


def validate_retained_control(row):
    """Known source-global evidence; never a target migration receipt."""
    import datetime
    require(type(row) is dict and set(row) == {"id", "name", "checksum", "applied_at"}, "retained_control_shape")
    require(row["id"] == "migration_history:fake_embedding_purge_v1" and
            row["name"] == "fake_embedding_purge_v1" and row["checksum"] == "fake-embedding-ramp-v1",
            "retained_control_identity")
    require(type(row["applied_at"]) is str and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})", row["applied_at"]), "retained_control_time")
    try:
        datetime.datetime.fromisoformat(row["applied_at"].replace("Z", "+00:00"))
    except ValueError as error:
        raise Refused("retained_control_time") from error


def validate_change(change, db, space):
    mode = baseline_operational_mode(db)
    require(set(change) == {"table", "class", "key", "kind", "flags", "changed_fields",
                           "updated_at", "deleted_at", "row_sha256", "dependency_rank", "before", "after"},
            "delta_shape")
    table, key = change["table"], change["key"]
    require(type(table) is str and TABLE.fullmatch(table), "delta_table")
    require(table in LEGACY_TABLES, "delta_unsupported_authority")
    require(type(change["class"]) is str and change["class"] in
            {"policy_metadata", "document", "operational", "graph", "unclassified"}, "delta_class")
    require(type(change["flags"]) is list and all(type(flag) is str for flag in change["flags"])
            and len(set(change["flags"])) == len(change["flags"])
            and set(change["flags"]) <= {"policy_metadata", "security", "metadata", "lifecycle"}, "delta_flags")
    require(type(change["dependency_rank"]) is int and change["dependency_rank"] >= 0, "delta_rank")
    before, after = change["before"], change["after"]
    hashes = {}
    for label, row in [("s0", before), ("s1", after)]:
        require(row is None or type(row) is dict, "delta_row_shape")
        if row is not None:
            if table == "migration_history":
                validate_retained_control(row)
            require(row_id(table, row) == key and (table == "migration_history" or row.get("space_id") == space), "delta_identity_or_space")
        hashes[label] = None if row is None else value_hash(row)
    require(hashes == change["row_sha256"], "delta_row_hash_mismatch")
    require(change_kind(before, after) == change["kind"], "delta_kind_mismatch")
    original = db.execute("SELECT h,space FROM rows WHERE t=? AND k=?", (table, key)).fetchone()
    require((original is None and before is None) or
            (original is not None and original == (hashes["s0"], None if table == "migration_history" else space)), "delta_baseline_mismatch")
    fields = sorted(k for k in set(before or {}) | set(after or {})
                    if (k in (before or {})) != (k in (after or {}))
                    or canonical((before or {}).get(k)) != canonical((after or {}).get(k)))
    require(fields == change["changed_fields"], "delta_changed_fields_mismatch")
    for field in ("updated_at", "deleted_at"):
        require(change[field] == {"s0": (before or {}).get(field), "s1": (after or {}).get(field)},
                "delta_timestamp_mismatch")
    return {"table": table, "key": key, "kind": change["kind"],
            "before_sha256": hashes["s0"], "after_sha256": hashes["s1"],
            "destructive_intent_required": change["kind"] in {"removed", "deleted"},
            "physical_erase_authorized": False,
            "disposition": ("retain_source_control_only" if table == "migration_history" else
                            "guarded_archive_row_change" if mode == "managed_archive_v1" and table in OPERATIONAL_TABLES else
                            "guarded_target_row_change"),
            "advisory_metadata": {"class": change["class"], "flags": change["flags"],
                                  "dependency_rank": change["dependency_rank"]},
            "target_guard": {"kind": "source_provenance_only", "target_schema_write": False} if table == "migration_history" else
                            ({"kind":"exact_archive_before_image", "sha256":hashes["s0"], "target_native_write":False}
                             if mode == "managed_archive_v1" and table in OPERATIONAL_TABLES else
                            ({"kind": "absent"} if before is None else
                            {"kind": "exact_before_image", "sha256": hashes["s0"]})),
            "before": before, "after": after}


def validate_target_guard(change, current_row):
    """Pure guard predicate for runtime integration, never a target read/write."""
    actual = None if current_row is None else value_hash(current_row)
    require(actual == change["before_sha256"], "target_before_image_conflict")


def delta(baseline, changes, after_snapshot, output, *, before_snapshot_sha256, after_snapshot_sha256,
          changes_sha256, baseline_sha256, max_line_bytes=64 * 1024 * 1024):
    for digest in [before_snapshot_sha256, after_snapshot_sha256, changes_sha256, baseline_sha256]:
        check_sha(digest)
    output = Path(output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    db = sqlite3.connect(Path(baseline).resolve().as_uri() + "?mode=ro", uri=True)
    seen = sqlite3.connect(output / "validation.sqlite")
    seen.execute("CREATE TABLE changes(t TEXT,k TEXT,after_h TEXT,PRIMARY KEY(t,k))")
    seen.execute("CREATE TABLE after_rows(t TEXT,k TEXT,h TEXT,PRIMARY KEY(t,k))")
    try:
        require(file_sha(baseline) == baseline_sha256, "baseline_index_hash_mismatch")
        require(metadata(db, "format") == FORMAT and metadata(db, "snapshot_sha256") == before_snapshot_sha256,
                "delta_baseline_binding")
        require(metadata(db, "canonical_hash_contract") == HASH_CONTRACT, "baseline_hash_contract")
        require(not metadata(db, "runtime_blockers"), "baseline_has_runtime_blockers")
        digest = hashlib.sha256()
        count = 0
        with (output / "changes.jsonl").open("xb") as handle:
            for raw, change in jsonl(changes, max_line_bytes):
                digest.update(raw)
                plan = validate_change(change, db, metadata(db, "space_id"))
                seen.execute("INSERT INTO changes VALUES (?,?,?)", (plan["table"], plan["key"], plan["after_sha256"]))
                plan["operation_id"] = value_hash({"format": "recall-preservation-delta-v1",
                    "before_snapshot_sha256": before_snapshot_sha256,
                    "after_snapshot_sha256": after_snapshot_sha256, "change": plan})
                handle.write(canonical(plan) + b"\n")
                count += 1
        require(digest.hexdigest() == changes_sha256, "delta_file_hash_mismatch")
        # A caller-supplied diff is not complete merely because its rows validate.
        # Compare every S1 row and every baseline deletion against the change set.
        stream = JsonStream(after_snapshot, max_line_bytes)
        after_media = media_summary()
        after_blockers = set()
        try:
            for table, row in stream.snapshot():
                if table == "migration_history":
                    validate_retained_control(row)
                if table == "media_asset":
                    record_media(after_media, row, max_line_bytes)
                    if not after_media["self_contained"]:
                        after_blockers.add("external_media_availability_required")
                key = row_id(table, row)
                row_hash = value_hash(row)
                seen.execute("INSERT INTO after_rows VALUES (?,?,?)", (table, key, row_hash))
                old = db.execute("SELECT h FROM rows WHERE t=? AND k=?", (table, key)).fetchone()
                change = seen.execute("SELECT after_h FROM changes WHERE t=? AND k=?", (table, key)).fetchone()
                if old is None or old[0] != row_hash:
                    require(change == (row_hash,), "incomplete_or_wrong_delta")
                else:
                    require(change is None, "spurious_delta_change")
            require(stream.digest.hexdigest() == after_snapshot_sha256, "after_snapshot_hash_mismatch")
            require(all(t in LEGACY_TABLES for t in stream.tables), "delta_unsupported_table")
            table_declarations = {"added": sorted(set(stream.tables) - set(metadata(db, "tables"))),
                                  "removed": sorted(set(metadata(db, "tables")) - set(stream.tables))}
        finally:
            stream.close()
        for table, key in db.execute("SELECT t,k FROM rows"):
            exists = seen.execute("SELECT 1 FROM after_rows WHERE t=? AND k=?", (table, key)).fetchone()
            if exists is None:
                change = seen.execute("SELECT after_h FROM changes WHERE t=? AND k=?", (table, key)).fetchone()
                require(change == (None,), "missing_removed_delta")
        for table, key, after_hash in seen.execute("SELECT t,k,after_h FROM changes"):
            actual = seen.execute("SELECT h FROM after_rows WHERE t=? AND k=?", (table, key)).fetchone()
            require(actual == (after_hash,) if after_hash is not None else actual is None,
                    "delta_after_image_absent")
        require(file_sha(baseline) == baseline_sha256, "baseline_index_changed")
        result = {"format": "recall-preservation-delta-v1", "state": "offline_delta_validation_only",
                  "canonical_hash_contract": HASH_CONTRACT,
                  "operational_disposition":baseline_operational_mode(db),
                  "table_declaration_changes": table_declarations,
                  "media_preservation":after_media, "runtime_blockers":sorted(after_blockers),
                  "baseline_plan_id": metadata(db, "plan_id"), "space_id": metadata(db, "space_id"),
                  "before_snapshot_sha256": before_snapshot_sha256,
                  "after_snapshot_sha256": after_snapshot_sha256,
                  "input_changes_sha256": changes_sha256, "changes": count,
                  "output_changes_sha256": file_sha(output / "changes.jsonl"),
                  "target_cas_verified": False, "target_source_generation": None,
                  "after_snapshot_completeness_verified": True,
                  "baseline_index_sha256": baseline_sha256,
                  "runtime_requirement": "Verify complete snapshot diff, current source/head/epoch and row CAS atomically; preserve ownership and explicit tombstone intent. No extraction."}
        seen.commit()
        result["plan_id"] = value_hash(result)
        write_new(output / "delta.json", result)
        return result
    except sqlite3.IntegrityError as error:
        raise Refused("duplicate_delta_identity") from error
    finally:
        db.close()
        seen.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    inv = sub.add_parser("inventory")
    for name in ["snapshot", "output", "space", "snapshot-sha256"]:
        inv.add_argument("--" + name, required=True)
    identity = inv.add_mutually_exclusive_group(required=True)
    identity.add_argument("--idmap")
    identity.add_argument("--ledger-backup")
    for name in ["idmap-sha256", "ledger-sha256", "ledger-cut-order"]:
        inv.add_argument("--" + name)
    inv.add_argument("--target-space")
    inv.add_argument("--operational-disposition", choices=OPERATIONAL_DISPOSITIONS, default="native")
    inv.add_argument("--max-value-bytes", type=int, default=64 * 1024 * 1024)
    diff = sub.add_parser("delta")
    for name in ["baseline", "changes", "after-snapshot", "output", "before-snapshot-sha256", "after-snapshot-sha256", "changes-sha256", "baseline-sha256"]:
        diff.add_argument("--" + name, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    try:
        require(args.get("max_value_bytes", 1) > 0, "invalid_value_limit")
        result = inventory(**args) if command == "inventory" else delta(**args)
    except (Refused, OSError, ValueError, sqlite3.Error, RecursionError) as error:
        # Errors use stable codes, never print row content or a provider token.
        parser.exit(1, f"preservation plan refused: {str(error) if isinstance(error, Refused) else type(error).__name__}\n")
    print(json.dumps({"state": result["state"], "plan_id": result["plan_id"]}))


if __name__ == "__main__":
    main()
