"""Deterministic household-count fixture; no original household content.

Only called by the measured scale driver. This module does not generate input
at import time. Exact counts come from retained metadata, while text, graph
connectivity, tenant allocation, payloads and partition distribution are synthetic.
"""
import base64
import hashlib
import json

COUNTS = {
    "assertion": 11666, "attribute": 6877, "conflict_group": 626,
    "doc": 7617, "doc_node": 187614, "doc_revision": 7918,
    "entity": 1489, "event": 11617, "freshness": 576,
    "identity_candidate": 5040, "ingest_decision": 7652,
    "media_asset": 86, "predicate_alias": 27, "predicate_registry": 452,
    "rel": 2475, "source": 7626, "truth_slot": 6226,
    "chunk_embedding": 179684, "audit_log": 440914,
    "cost_ledger_daily": 91259, "system_event": 596,
    "work_artifact": 65671, "work_item": 25193, "work_item_archive": 54656,
    "work_run": 27659, "work_run_archive": 55784, "migration_history": 1,
    "entity_identity_key": 0, "entity_merge": 0, "id_map": 0,
    "knowledge_view": 0, "knowledge_view_definition": 0,
    "knowledge_view_dependency": 0, "knowledge_view_revision": 0,
    "rel_evidence": 0, "reprocess_job": 0, "source_watch": 0,
    "source_watch_check": 0, "space_config": 0, "work_edge": 0,
}
LIVE_DOCS = 7518
LIVE_TEXT_BYTES = 40 * 1024 * 1024
MEDIA_BYTES = 145302409
# Same total decoded bytes; synthetic size distribution, including one large row.
FIRST_MEDIA_BYTES = 20000000
PROFILE = "household-full-shape"
UNMATCHED = [
    "original text and original graph degree/distribution",
    "original per-table tenant and security distributions",
    "original vector values, model identity and partition distribution",
    "original document revision/deletion relationships and timestamps",
    "original largest-row identity/bytes and media size distribution",
    "original hot work status distribution and derived KEEP/source-key mix",
]


def media_size(i):
    if i == 0:
        return FIRST_MEDIA_BYTES
    quotient, remainder = divmod(MEDIA_BYTES - FIRST_MEDIA_BYTES, COUNTS["media_asset"] - 1)
    return quotient + (i <= remainder)


def document_text(i, ordinary_text):
    """Approximately reported 40 MiB, with exact synthetic live-byte accounting."""
    quotient, remainder = divmod(LIVE_TEXT_BYTES, LIVE_DOCS)
    size = quotient + (i < remainder)
    prefix = ordinary_text(i)
    return prefix + "x" * (size - len(prefix.encode()))


def node_span(i, text):
    doc = i % COUNTS["doc"]
    position = i // COUNTS["doc"]
    nodes = (COUNTS["doc_node"] - 1 - doc) // COUNTS["doc"] + 1
    length = len(text(doc).encode())
    # Boundaries after the first page are in the ASCII padding, beyond the
    # Unicode title. Every byte belongs to exactly one node, with no split UTF-8.
    return {"start": length * position // nodes, "end": length * (position + 1) // nodes}


def media_fields(i, size):
    """Shared byte-preservation constructor, parameterizable for bounded controls."""
    if not 0 <= size <= FIRST_MEDIA_BYTES:
        raise ValueError("synthetic media byte bound")
    pattern = hashlib.sha256(f"synthetic-media:{i}".encode()).digest()
    payload = (pattern * ((size + len(pattern) - 1) // len(pattern)))[:size]
    return {"kind": "image", "media_type": "application/octet-stream",
            "storage_path": f"media_asset/synthetic-{i}.bin", "size_bytes": size,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "payload_base64": base64.b64encode(payload).decode("ascii")}


def evidence(i, rid, text):
    doc = i % LIVE_DOCS
    body = text(doc).encode()
    span = node_span(doc, text)
    return {"revision": rid("doc_revision", doc), "node": rid("doc_node", doc),
            **span, "quote_hash": hashlib.sha256(body[span["start"]:span["end"]]).hexdigest()}


def evidence_ref(i, rid, text, stamp):
    assertion = i % COUNTS["assertion"]
    return {"assertion": rid("assertion", assertion), "span": evidence(assertion, rid, text),
            "time_basis": "observed_at", "time_value": stamp}


def record_partition(base):
    """Product truth-maintenance partition over the exact typed envelope."""
    security = base["security"]
    digest = hashlib.sha256(security["policy_id"].encode())
    digest.update(security["level"].to_bytes(4, "big", signed=True))
    for field in ("labels", "handling"):
        for item in sorted(security.get(field, [])):
            digest.update(item.encode())
    envelope = {"security": digest.hexdigest(), "subjects": sorted(set(base.get("subjects", []))),
                "subjects_state": base.get("subjects_state", "unknown")}
    return hashlib.sha256(json.dumps(envelope, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False).encode()).hexdigest()


def slot_key(subject, predicate, base):
    return "::".join((subject, predicate, record_partition(base)))


def attribute_slot(i, counts):
    """Two retained claims compete only in the first 626 synthetic slots."""
    return i - counts["conflict_group"] if counts["conflict_group"] <= i < 2 * counts["conflict_group"] else i


def row(table, i, counts, base, rid, text, stamp, ordinary):
    """Return one bounded row, never a complete table or vector population."""
    value = base(table, i)
    predicate = f"synthetic_predicate_{i % counts['predicate_registry']}"
    subject = rid("entity", i % counts["entity"])
    if table == "migration_history":
        return {"id": "migration_history:fake_embedding_purge_v1", "name": "fake_embedding_purge_v1",
                "checksum": "fake-embedding-ramp-v1", "applied_at": stamp}
    if table == "doc":
        value = ordinary(table, i, counts)
        value["text"] = text(i)
        if i < counts["media_asset"]:
            value["media_attachments"] = [{"attachment_id": f"synthetic:{i}", "kind": "image",
                "media_type": "application/octet-stream", "location": {
                    "transport": "managed_asset", "asset_id": rid("media_asset", i)},
                "size_bytes": media_size(i)}]
        if i >= LIVE_DOCS:
            value["deleted_at"] = stamp
    elif table == "doc_revision":
        # One current revision per Doc and 301 extra, superseded revisions.
        doc = i if i < counts["doc"] else i - counts["doc"]
        body = text(doc)
        value.update(doc=rid("doc", doc), text=body,
                     text_hash=hashlib.sha256(body.encode()).hexdigest(),
                     renderer={"name": "synthetic-original", "version": "v1", "config_hash": None},
                     status="active" if i < counts["doc"] else "superseded", rendered_at=stamp)
    elif table == "doc_node":
        # Partition each immutable text into valid byte spans, including nodes
        # retained under deleted Docs. Distribution is explicitly synthetic.
        value = ordinary(table, i, counts)
        value["span"] = node_span(i, text)
    elif table == "assertion":
        relation = counts["attribute"] <= i < counts["attribute"] + counts["rel"]
        claim = i - counts["attribute"] if relation else i
        slot = claim if relation else attribute_slot(claim, counts)
        predicate = f"synthetic_predicate_{slot % counts['predicate_registry']}"
        value.update(subject_text=f"Synthetic person {slot % counts['entity']}",
                     predicate_raw=predicate, predicate_id=predicate,
                     evidence=evidence(i, rid, text), observed_at=stamp, asserted_at=stamp,
                     assertion_key=f"synthetic-assertion:{i}", resolution_status="resolved",
                     retry_state={"resolution_attempts": 0},
                     subject_entity=rid("entity", slot % counts["entity"]), confidence=0.75,
                     lifecycle_tier="active")
        if relation:
            value.update(object_text=f"Synthetic person {(claim + 1) % counts['entity']}",
                         object_entity=rid("entity", (claim + 1) % counts["entity"]))
        else:
            value["object_literal"] = f"value-{i}"
    elif table == "attribute":
        # First 626 slots have a second, genuinely competing claim with the
        # same subject/predicate and a distinct value and retained evidence.
        slot = attribute_slot(i, counts)
        value.update(subject=rid("entity", slot % counts["entity"]),
                     attr_type=f"synthetic_predicate_{slot % counts['predicate_registry']}", value={"type": "string", "value": f"value-{i}"},
                     status="approved", confidence=0.75, evidence=[evidence_ref(i, rid, text, stamp)])
    elif table == "rel":
        value.update({"in": subject, "out": rid("entity", (i + 1) % counts["entity"])})
        value.update(rel_type=predicate, status="approved", confidence=0.75,
                     evidence=[evidence_ref(counts["attribute"] + i, rid, text, stamp)])
    elif table == "conflict_group":
        value.update(slot_key=slot_key(subject, predicate, value), slot_type="attribute",
                     claim_ids=[rid("attribute", i), rid("attribute", i + counts["conflict_group"])],
                     resolved=False, detected_at=stamp)
    elif table == "truth_slot":
        claim = i if i < counts["conflict_group"] else i + counts["conflict_group"]
        slot = attribute_slot(claim, counts)
        value.update(slot_key=slot_key(rid("entity", slot % counts["entity"]),
                     f"synthetic_predicate_{slot % counts['predicate_registry']}", value), slot_type="attribute",
                     subject_entity=rid("entity", slot % counts["entity"]),
                     predicate_id=f"synthetic_predicate_{slot % counts['predicate_registry']}",
                     current_values=[{"claim_id": rid("attribute", claim), "confidence": 0.75,
                                      "status": "approved", "evidence": [evidence_ref(claim, rid, text, stamp)]}],
                     confidence=0.75, semantics_version=0, alias_version=0, stale=False,
                     computed_at=stamp, security_hash=record_partition(value))
        if i < counts["conflict_group"]:
            value["conflict_group"] = rid("conflict_group", i)
    elif table == "event":
        value.update(title=f"Synthetic event {i}", status="approved", time_precision="unknown",
                     confidence=0.75, time_confidence=0.75,
                     assertion_refs=[rid("assertion", i % counts["assertion"])], source_refs=[],
                     metadata={"original_values": [{"original_ordinal": i}, None]} if i % 3 == 0 else {})
    elif table == "freshness":
        value.update(target=rid("attribute", i % counts["attribute"]), target_type="attribute",
                     decay_model="linear", score=0.75, confidence=0.75,
                     factors={"evidence_age_days": 0.0, "conflict_severity": 0.0,
                              "evidence_strength": 1.0, "source_reliability": 1.0,
                              "corroboration_count": 1, "custom": {}}, computed_at=stamp, demote=False)
    elif table == "identity_candidate":
        value.update(entity_a=subject, entity_b=rid("entity", (i + 1) % counts["entity"]),
                     signals={"synthetic": True}, confidence=0.75, status="pending",
                     proposed_by="synthetic-original", decided_by=None, decided_at=None)
    elif table == "ingest_decision":
        value.update(source=rid("source", i % counts["source"]), decision="graph",
                     reason_codes=["synthetic-original"], confidence=0.75, estimated_cost=0.0)
    elif table == "source":
        value.update(type="manual", lifecycle_status="graph_ready", current_ingest_mode=None,
                     latest_ingest_decision=None, external_id=None, uri=None, path=None, parent=None,
                     range=None, timestamp=None, author=None, channel=None,
                     hash=hashlib.sha256(f"synthetic-original-source:{i}".encode()).hexdigest(), metadata={})
    elif table == "predicate_registry":
        value.update(predicate_id=predicate, status="active", semantics_status="default", usage_count=0)
    elif table == "predicate_alias":
        value.update(alias=f"synthetic-alias:{i}", alias_type="raw_string", confidence=0.75,
                     status="active", scope="global", predicate_id=predicate, confidence_version=0, usage_count=0)
    elif table == "media_asset":
        size = media_size(i)
        # Deterministic opaque bytes exercise exact base64/size/hash custody;
        # they are not a claim of valid decoded image content or image processing.
        value.update(media_fields(i, size))
    else:
        value = ordinary(table, i, counts)
    return value
