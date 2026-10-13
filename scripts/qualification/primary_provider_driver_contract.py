"""Pure primary-run contracts. No network, process launch, or credential access."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import uuid

BACKENDS = ("elephant-db", "surreal-db")
MODES = ("semantic", "hybrid")
RENDERED_MAP_SHA256 = "aa3579d3252fa6cbe396a1003d84124b1c9541857fa78176eec57317045fb7d1"
CORPUS_DIGEST = "fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b"
LIMITS = {"source_revisions": 28, "queries": 24, "http_requests": 8192,
          "backend_seconds": 2700, "publication_seconds": 900, "request_seconds": 15,
          "conditional_seconds": 720, "response_bytes": 262144,
          "request_bytes": 131072, "capture_bytes": 1073741824, "inventory_pages": 16}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha(encode(value))


def utc(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "explicit timezone required")
    return parsed


def corpus_contract(corpus, expectations, proposal):
    require(digest(corpus) == CORPUS_DIGEST == expectations["corpus_canonical_sha256"]
            == proposal["corpus_canonical_sha256"], "original frozen primary corpus differs")
    require(expectations["status"] == "expectations-approved-before-execution"
            and expectations["reviewer"] == "qualification (independent D lane)", "independent expectation seal absent")
    sources, queries = corpus["sources"], corpus["queries"]
    require(len(sources) == 28 and len(queries) == 24, "original case count differs")
    require(len({(r["space"], r["id"], r["revision"]) for r in sources}) == 28
            and len({q["id"] for q in queries}) == 24, "duplicate original cases")
    require(proposal["source_revisions_per_backend"] == 28 and proposal["original_queries_per_backend"] == 24
            and set(proposal["backends"]) == set(BACKENDS) and set(proposal["ranking_modes"]) == set(MODES),
            "primary proposal schedule differs")
    # A proposal remains planning-only. Separate run approval and physical gateway
    # enforcement are mandatory; this driver never changes the proposal's status.
    require(proposal["budgets"]["dispatches_both_backends"] == 432
            and proposal["budgets"]["usd_stop_cap"] == "75.00", "unreviewed primary allocation")


def current_inventory(corpus, space):
    return {row["id"]: row["revision"] for row in corpus["sources"] if row["space"] == space}


def search_request(corpus, query, mode):
    require(mode in MODES, "unsupported ranking mode")
    return {"query": query["query"], "limit": 3, "ranking_mode": mode,
            "source_revision_constraints": current_inventory(corpus, query["space"])}


def source_request(source, backend, nonce, timestamp, security, generation):
    require(backend in BACKENDS, "persistent backend required")
    identity = [backend, nonce, source["space"], source["id"]]
    def base(table, revision):
        record = uuid.uuid5(uuid.NAMESPACE_URL, "primary/" + digest([*identity, table, revision]))
        return {"id": f"{table}:{record}", "space_id": source["space"],
                "created_at": timestamp, "updated_at": timestamp, "created_by": "primary-qualification",
                "deleted_at": None, "security": json.loads(encode(security)),
                "subjects": [], "subjects_state": "unknown", "provenance": {"sources": [], "chain": []}}
    request_key = "primary-" + digest([*identity, source["revision"]])
    return {"source": {**base("source", None), "type": "manual", "hash": sha(source["text"].encode()),
                       "external_id": source["id"], "metadata": {}},
            "doc": {**base("doc", source["revision"]), "title": source["id"], "text": source["text"],
                    "source": None, "sidecar_text": None, "media_attachments": [],
                    "rendition_key": request_key, "metadata": {}},
            "ingest_mode": "graph", "sync": False,
            "intake": {"revision_id": source["revision"], "idempotency_key": request_key,
                       "expected_generation": generation}}


def acceptance(source, value):
    operation = value["operation"]
    require(operation["space_id"] == source["space"] and operation["desired_revision"] == source["revision"],
            "acceptance is for another original revision")
    for key in ("operation_id", "source_generation", "source_id"):
        require(isinstance(operation.get(key), str) and operation[key], "original acceptance identity absent")
    require(value["source_id"] == operation["source_id"]
            and isinstance(value.get("doc_id"), str) and value["doc_id"].startswith("doc:")
            and isinstance(value.get("work_item_id"), str) and value["work_item_id"].startswith("work_item:"),
            "original document/work receipt absent")
    return {"operation_id": operation["operation_id"], "generation": operation["source_generation"],
            "internal_source_id": value["source_id"], "desired_revision": source["revision"],
            "doc_id": value["doc_id"], "work_item_id": value["work_item_id"]}


def original_operation(original, observed, complete=False):
    for key in ("operation_id", "generation", "internal_source_id", "desired_revision"):
        require(observed.get(key) == original[key], "operation/head substituted original identity: " + key)
    require(observed.get("lifecycle") == "active", "original source is no longer active")
    require(observed.get("state") in ("pending", "running", "failed_retryable", "failed_final", "complete"),
            "unknown original operation state")
    if complete:
        require(observed["state"] == "complete" and observed["published_revision"] == original["desired_revision"]
                and observed.get("error_code") is None, "original publication has not completed")


def stable_configuration(value):
    value = json.loads(encode(value))
    value.pop("updated_at", None)
    processing = value["processing"]
    generation = processing.pop("index_generation")
    require(type(generation) is int and generation >= 0, "invalid observed index generation")
    return value


def rendered_contract(value, corpus, corpus_sha256):
    require(value.get("format") == "recall-independent-rendered-map/v1"
            and value.get("reviewer") == "qualification (independent D lane)"
            and value.get("execution_authorized") is False and value.get("product_acceptance") is False
            and value["corpus_sha256"] == corpus_sha256
            and value["corpus_path"] == "tests/recall_preview_semantics/corpus.json",
            "missing or foreign independently reviewed rendered map")
    rows = value["sources"]
    require(value["source_count"] == len(rows) == len(corpus["sources"]) == 28,
            "rendered map changed original case count")
    observed = {(row["space"], row["source_key"], row["revision"]): row for row in rows}
    require(len(observed) == len(rows), "duplicate rendered input identity")
    for source in corpus["sources"]:
        row = observed.get((source["space"], source["id"], source["revision"]))
        require(row is not None, "original source missing from rendered map")
        original, rendered = source["text"].encode(), row["rendered_text"].encode()
        require(row["input_sha256"] == sha(original) and row["input_utf8_bytes"] == len(original)
                and row["rendered_sha256"] == sha(rendered) and row["rendered_utf8_bytes"] == len(rendered)
                and row["root_span"] == {"start": 0, "end": len(rendered)}, "rendered map byte identity differs")
        for kind in ("paragraph_spans", "chunk_spans"):
            require(isinstance(row[kind], list) and row[kind], "original expected spans missing")
            for span in row[kind]:
                require(set(span) == {"start", "end"} and type(span["start"]) is int
                        and type(span["end"]) is int and 0 <= span["start"] < span["end"] <= len(rendered),
                        "rendered map span outside original UTF-8 bytes")
                rendered[span["start"]:span["end"]].decode("utf-8")
    return observed


def publication_evidence(source, accepted, doc, record, pipeline, nodes, expected, rendered):
    require((rendered["space"], rendered["source_key"], rendered["revision"]) ==
            (source["space"], source["id"], source["revision"]), "rendered evidence belongs to another original input")
    require(doc["id"] == accepted["doc_id"] and doc["text"] == rendered["rendered_text"]
            and sha(doc["text"].encode()) == rendered["rendered_sha256"]
            and doc["source"] == accepted["internal_source_id"], "original immutable document differs")
    require(record["id"] == accepted["internal_source_id"] and record["external_id"] == source["id"]
            and record["hash"] == sha(source["text"].encode())
            and record["current_ingest_mode"] == "graph" and record["lifecycle_status"] == "graph_ready",
            "actual original Graph source publication absent")
    for row in (doc, record):
        require(row["space_id"] == source["space"] and row.get("deleted_at") is None
                and row["security"] == expected["source_security"], "published authority differs")
    revision = pipeline.get("revision")
    require(pipeline["doc_id"] == accepted["doc_id"] and isinstance(revision, str)
            and revision.startswith("doc_revision:"), "original pipeline revision absent")
    require(isinstance(nodes, list) and 0 < len(nodes) <= 1024
            and len({row["id"] for row in nodes}) == len(nodes), "node inventory absent, duplicate, or unbounded")
    embedded = []
    raw = rendered["rendered_text"].encode()
    # NodeRole::Document serializes as "document". The independent map's
    # root_span is a coordinate key, not a public node-role spelling.
    spans = {role: [] for role in ("document", "paragraph", "chunk")}
    for node in nodes:
        require(node["space_id"] == source["space"] and node["revision"] == revision
                and node["security"] == expected["source_security"] and node.get("deleted_at") is None,
                "node authority or original immutable revision differs")
        require(node["role"] in spans, "unreviewed rendered node role")
        spans[node["role"]].append(node["span"])
        start, end = node["span"]["start"], node["span"]["end"]
        require(type(start) is int and type(end) is int and 0 <= start < end <= len(raw), "invalid immutable UTF-8 span")
        require(node.get("text") in (None, raw[start:end].decode("utf-8")), "indexed text differs from original evidence")
        if node["role"] in ("paragraph", "chunk"):
            require(node.get("embedding_model") == expected["embedding_partition_model"], "actual embedding profile marker differs")
            embedded.append(node)
    order = lambda values: sorted(values, key=lambda span: (span["start"], span["end"]))
    require(spans["document"] == [rendered["root_span"]]
            and order(spans["paragraph"]) == order(rendered["paragraph_spans"])
            and order(spans["chunk"]) == order(rendered["chunk_spans"]),
            "actual rendered coverage differs from pre-output span map")
    require(bool(embedded) and pipeline["chunks"] == {"total": len(embedded), "embedded": len(embedded)}
            and pipeline["nodes_count"] == len(nodes), "public embedding coverage incomplete")


def revision_evidence(source, accepted, revision_id, revision, rendered, renderer, expected):
    require(revision["id"] == revision_id and revision["doc"] == accepted["doc_id"]
            and revision["space_id"] == source["space"] and revision.get("deleted_at") is None
            and revision["security"] == expected["source_security"] and revision["status"] == "active",
            "immutable revision identity or authority differs")
    require(revision["text"] == rendered["rendered_text"]
            and revision["text_hash"] == rendered["rendered_sha256"] == sha(revision["text"].encode())
            and revision["renderer"]["name"] == renderer["name"]
            and revision["renderer"]["version"] == renderer["version"],
            "actual immutable revision differs from pre-output rendered expectation")


def conditional_contract(value, spaces):
    require(value.get("format") == "recall-primary-conditional-cases/v1"
            and value.get("reviewer") == "qualification (independent D lane)"
            and value.get("status") == "approved-before-execution", "conditional cases are not frozen")
    cases = value["cases"]
    require(isinstance(cases, list) and len(cases) <= 4 and len({c["id"] for c in cases}) == len(cases),
            "conditional case budget differs")
    require(set(value["disposition"]) == {"truth_maintenance", "wiki"}, "explicit conditional disposition required")
    for category, decision in value["disposition"].items():
        require(decision["status"] in ("required", "not-applicable") and bool(decision["reason"]),
                "conditional absence needs a pre-output reason")
        selected = [c for c in cases if c["category"] == category]
        require(bool(selected) == (decision["status"] == "required"), "conditional cases contradict disposition")
    for case in cases:
        require(case["space"] in spaces and re.fullmatch(r"[a-z0-9-]{1,80}", case["id"]), "invalid conditional identity")
        args = case["arguments"]
        require(isinstance(args, dict) and len(encode(args)) <= 32768, "conditional arguments exceed bound")
        if case["category"] == "truth_maintenance":
            require(case["tool"] == "create_reprocess_job" and args["target_stages"] == ["re-maintain"]
                    and args.get("budget_limit_euros") is None, "only explicit governed maintenance stage is allocated")
            require(args.get("max_sources_per_day") == 28, "bounded original corpus maintenance required")
        elif case["category"] == "wiki" and case["tool"] == "compile_wiki_article":
            require(set(case) == {"id", "category", "space", "tool", "arguments", "definition"}
                    and args == {"publish": True} and type(args["publish"]) is bool,
                    "scoped wiki cannot supply an ID, override, or extra command")
            require(encode(case["definition"]) == encode(scoped_wiki_definition(case["space"])),
                    "scoped wiki definition differs from the frozen primary selector")
        else:
            require(case["category"] == "wiki" and case["tool"] == "create_wiki_from_graph"
                    and args.get("manage") is False and args.get("publish") is True
                    and type(args.get("max_articles")) is int and 1 <= args["max_articles"] <= 2
                    and args.get("article_max_hops") in (1, 2)
                    and type(args.get("article_max_members")) is int and 1 <= args["article_max_members"] <= 64,
                    "conditional wiki must be bounded and nonrecurring")
            require("writer_model" not in args and "verifier_model" not in args, "provider overrides forbidden")
    return cases


def scoped_wiki_definition(space):
    recipes = {"semantics-household": ("Dishwasher repair", "dishwasher", "synthetic-household", 3),
               "semantics-business": ("Project Lantern", "Lantern", "synthetic-business", 7)}
    require(space in recipes, "scoped wiki requires an original primary space")
    title, name, policy, level = recipes[space]
    return {"title": title, "publication_status": "published", "related_articles": [],
            "selector": {"type": "query", "entity_types": [], "name_contains": name,
                         "attributes": [], "scan_limit": 256, "max_members": 64},
            "security": {"policy_id": policy, "level": level, "labels": [], "handling": []}}


def scoped_wiki_plan_binding(cases, plan):
    scoped = any(case["tool"] == "compile_wiki_article" for case in cases)
    require((plan.get("scoped_wiki") is True) == scoped
            and ("scoped_wiki" not in plan or plan["scoped_wiki"] is True),
            "scoped wiki cases and original bootstrap grant marker differ")


def wiki_record_id(value, table):
    require(isinstance(value, str) and value.startswith(table + ":"), "wiki record identity differs")
    suffix = value[len(table) + 1:]
    require(str(uuid.UUID(suffix)) == suffix, "wiki record ID is not canonical")
    return value


def wiki_definition_evidence(case, row):
    definition_id = wiki_record_id(row["id"], "knowledge_view_definition")
    require(row["space_id"] == case["space"] and row.get("deleted_at") is None
            and all(row.get(key) == expected for key, expected in case["definition"].items())
            and row.get("subjects_state") == "complete" and row.get("subjects") == []
            and row.get("navigation") is None, "actual scoped wiki definition differs")
    require(isinstance(row.get("updated_at"), str), "original definition version absent")
    utc(row["updated_at"])
    return definition_id


def wiki_publication_inputs(space, publications, current, rendered_map):
    """Only completed original publications still current in this named space."""
    inputs = {}
    for publication in publications:
        owner, source_key, revision_key = publication["case"]
        original = publication["original"]
        if owner != space or current.get((owner, source_key)) != original:
            continue
        rendered = next(row for row in rendered_map["sources"]
                        if (row["space"], row["source_key"], row["revision"]) ==
                           (owner, source_key, revision_key))
        revision = publication["pipeline"]["revision"]
        require(revision not in inputs, "duplicate original wiki evidence revision")
        inputs[revision] = {"source_id": original["internal_source_id"],
                            "text": rendered["rendered_text"].encode(),
                            "nodes": {node["id"]: node["span"] for node in publication["nodes"]}}
    require(bool(inputs), "scoped wiki has no completed original publications")
    return inputs


def wiki_projection_evidence(case, definition, response, originals, *, context=None):
    """Check public receipts against original input bytes, without grading prose."""
    article = context is not None
    view, revision = response["view"], response["revision"]
    definition_id = wiki_definition_evidence(case, definition)
    view_id = wiki_record_id(view["id"], "knowledge_view")
    revision_id = wiki_record_id(revision["id"], "knowledge_view_revision")
    require(type(response["changed"]) is bool and (not article or response["changed"] is True),
            "scoped wiki requires a newly compiled article")
    require(view["current_revision"] == revision_id and revision["view_id"] == view_id
            and view["root_record"] == revision["root_record"] == definition_id
            and view["kind"] == revision["kind"] == ("article" if article else "query")
            and view["surface"] == ("wiki" if article else "inspector")
            and view["publication_status"] == ("published" if article else "draft")
            and view["state"] == "current",
            "wiki current pointer, root, or publication differs")
    require(view["subjects"] == revision["subjects"] and bool(view["subjects"])
            and len(set(view["subjects"])) == len(view["subjects"]), "wiki subjects are empty or inconsistent")
    for row in (view, revision):
        require(row["space_id"] == case["space"] and row["security"] == definition["security"]
                and row.get("deleted_at") is None and row["subjects_state"] == "complete",
                "wiki publication authority differs")
    if not article:
        require(response["stale_fallback"] is False and response.get("warning") is None,
                "scoped wiki cannot use a stale context")
    else:
        require(view["subjects"] == context["view"]["subjects"], "article changed original context subjects")
    freshness, validation = revision["freshness"], revision["validation"]
    require(freshness["state"] == "current" and freshness["selection_truncated"] is False
            and all(type(freshness[key]) is int and freshness[key] == 0 for key in
                    ("stale_truth_slots", "unresolved_conflicts", "unsupported_claims", "expired_claims")),
            "scoped wiki context is degraded or truncated")
    require(all(validation[key] is True for key in
                ("citations_resolve", "factual_blocks_cited", "statements_supported", "prose_is_structured"))
            and validation["warnings"] == [], "wiki validation receipt is not clean")
    dependencies = revision["dependencies"]
    by_id = {row["record_id"]: row for row in dependencies}
    require(bool(dependencies) and len(by_id) == len(dependencies), "wiki dependencies absent or duplicated")
    require(by_id.get(definition_id, {}).get("observed_updated_at") == definition["updated_at"],
            "wiki definition dependency version differs")
    members = {row["record_id"] for row in dependencies if row["record_type"] == "entity"
               and "selected_member" in row["role"].split("+")}
    require(bool(members) and members.issubset(set(view["subjects"])), "scoped wiki selected no governed members")
    sources = {row["record_id"] for row in dependencies if row["record_type"] == "source"}
    require(bool(sources) and sources.issubset({row["source_id"] for row in originals.values()}),
            "wiki source dependency is outside current original publications")
    if article:
        for original in context["revision"]["dependencies"]:
            actual = by_id.get(original["record_id"])
            require(actual is not None and actual["record_type"] == original["record_type"]
                    and (original.get("observed_updated_at") is None or
                         actual.get("observed_updated_at") == original["observed_updated_at"])
                    and set(original["role"].split("+")).issubset(actual["role"].split("+")),
                    "article changed an original context dependency")
    citations = {row["id"]: row for row in revision["citations"]}
    require(bool(citations) and len(citations) == len(revision["citations"]), "wiki citations absent or duplicated")
    grounded = set()
    for identifier, citation in citations.items():
        span = citation.get("evidence_span")
        require(citation["record_id"] in by_id or
                (article and span is not None and span["revision"] in by_id),
                "wiki citation has no dependency")
        if span is None:
            continue
        require(citation["kind"] == "evidence_span" and span["revision"] in originals
                and span["revision"] in by_id, "wiki span is not an original immutable revision")
        original = originals[span["revision"]]
        require(citation.get("source_id") == original["source_id"] and original["source_id"] in sources,
                "wiki span source differs from original publication")
        start, end = span["start"], span["end"]
        require(type(start) is int and type(end) is int and 0 <= start < end <= len(original["text"]),
                "wiki evidence span is outside original UTF-8 bytes")
        original["text"][start:end].decode("utf-8")
        quote_hash = sha(original["text"][start:end])
        require(span["quote_hash"] in (quote_hash, "sha256:" + quote_hash), "wiki evidence quote hash differs")
        if span.get("node") is not None:
            node = original["nodes"].get(span["node"])
            require(node is not None and node["start"] <= start < end <= node["end"],
                    "wiki span node differs from original publication")
        grounded.add(identifier)
    blocks = revision["blocks"]
    require(bool(blocks) and len({row["id"] for row in blocks}) == len(blocks), "wiki blocks absent or duplicated")
    statement_count = 0
    for block in blocks:
        statements = block["statements"]
        require(bool(statements) and len({row["id"] for row in statements}) == len(statements),
                "wiki statements absent or duplicated")
        statement_count += len(statements)
        for statement in statements:
            require(isinstance(statement["text"], str) and bool(statement["text"].strip())
                    and set(statement["citations"]).issubset(citations), "wiki statement support differs")
            if article:
                require(statement["derivation"] == "synthesized"
                        and statement["verification"] == "evidence_entailment"
                        and bool(grounded.intersection(statement["citations"])),
                        "wiki statement lacks verified original source evidence")
        if article:
            rendered = "\n\n".join(statement["text"] + "".join(" [^" + citation + "]" for citation in statement["citations"])
                                   for statement in statements)
            require(block["markdown"] == rendered
                    and set(block["citations"]) == {citation for statement in statements for citation in statement["citations"]},
                    "wiki prose differs from the original statement ledger")
    if article:
        title = view.get("title")
        require(type(title) is str and all(type(block.get("heading")) is str for block in blocks),
                "wiki article title or heading is absent")
        # Match Rust str::trim's Unicode White_Space set and the original
        # wiki_editorial::render_article_markdown, including every separator.
        title = title.strip("\t\n\v\f\r \u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005"
                            "\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000")
        rendered = "# " + title + "\n" + "".join(
            "\n## " + block["heading"] + "\n\n" + block["markdown"] + "\n" for block in blocks)
        require(revision.get("rendered_markdown") == rendered,
                "wiki rendered article differs from the original statement blocks")
    expected_counts = {"citation_count": len(citations), "statement_count": statement_count,
                       "factual_block_count": sum(article or row["kind"] != "evidence_status" for row in blocks)}
    require(all(type(validation[key]) is int and validation[key] == count and count > 0
                for key, count in expected_counts.items()), "wiki validation counts differ from actual content")
    return {"definition_id": definition_id, "view_id": view_id, "revision_id": revision_id,
            "original_source_ids": sorted(sources), "grounded_citation_ids": sorted(grounded),
            "semantic_verdict": "unmet: independent review of original prose and evidence required",
            "full_graph_planner": "unexercised", "full_graph_organizer": "unexercised"}
