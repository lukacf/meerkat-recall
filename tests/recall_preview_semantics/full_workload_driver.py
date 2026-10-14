"""Fourteen-case public workload orchestration; independent D owns expectations.

This module never starts a product process, edits storage, calls a real provider,
creates expected evidence from product output, or certifies consumer completion.
"""
from __future__ import annotations

import asyncio
import math
import re
import time
from urllib.parse import quote
import uuid

from full_workload_graph import GraphFixture, validate_recipe as validate_graph
from full_workload_load import run_load, validate_plan as validate_load, validate_requirements
from full_workload_spy import validate_recipe as validate_spy
from full_workload_transport import Client, Unmet, decode, encode, private_new, require, sha

CONTRACT_SHA = "44a91b9f0ae4c2ae32d2f8aec9eb783a48ccca63eeee010ff2427ef1ec4a0c0b"
CORPUS_SHA = "2192437954109fc17ebbea5b3960eb6b398a2ee91a725ce70666b119e86179dc"
CASES = tuple(f"FW{number:02}" for number in range(1, 15))
SPACES = ("qualification-household", "qualification-business")
TIMESTAMP = "2026-10-07T00:00:00Z"
STATES = {"pending", "running", "complete", "failed_retryable", "failed_final"}
PROOF_LIMITS = ["controlled providers establish protocol and ordering, not model quality",
                "public markers do not prove physical vectors or private publication receipts",
                "public invisibility does not prove physical erasure, WAL, media or backup closure",
                "client intervals do not prove server execution overlap",
                "external consumers own size fallback and outage completion",
                "independent D review of original captures is required"]


def clone(value):
    return decode(encode(value))


def exact_type_equal(left, right):
    return type(left) is type(right) and encode(left) == encode(right)


def subset(actual, expected):
    """Literal, independently reviewed fields only; booleans never equal integers."""
    if type(expected) is dict:
        return type(actual) is dict and all(key in actual and subset(actual[key], value) for key, value in expected.items())
    return exact_type_equal(actual, expected)


def original_identity(accepted, operation, revision, *, active=True):
    original = accepted["operation"]
    require(type(operation) is dict and operation.get("state") in STATES, "unknown actual operation state")
    for public, retained in (("operation_id", "operation_id"), ("internal_source_id", "source_id"),
                             ("generation", "source_generation")):
        require(operation.get(public) == original.get(retained), "original operation/source/generation substituted")
    require(operation.get("desired_revision") == revision, "original accepted revision substituted")
    if active:
        require(operation.get("lifecycle") == "active", "original source is no longer active")


def accepted_identity(request, body):
    require(type(body) is dict and type(body.get("operation")) is dict, "actual typed acceptance absent")
    operation = body["operation"]
    require(operation.get("space_id") == request["source"]["space_id"]
            and operation.get("desired_revision") == request["intake"]["revision_id"], "actual acceptance identity differs")
    for key in ("source_id", "source_generation", "operation_id"):
        require(type(operation.get(key)) is str and operation[key], "actual acceptance identity missing:" + key)
    require(body.get("source_id") == operation["source_id"] and type(body.get("doc_id")) is str
            and body["doc_id"].startswith("doc:"), "actual accepted source/document mapping absent")
    return clone(body)


def head_path(space, key):
    return f"/v1/spaces/{quote(space, safe='')}/sources/{quote(key, safe='')}/head"


def source_request(source, backend, nonce, security):
    """Retain one deterministic input intent; returned identity is always observed."""
    identity = sha(encode([backend, nonce, source["space"], source["key"], source["revision"]]))
    def base(table):
        return {"id": table + ":" + str(uuid.uuid5(uuid.NAMESPACE_URL, "full-workload/" + identity + "/" + table)),
                "space_id": source["space"], "created_at": TIMESTAMP, "updated_at": TIMESTAMP,
                "created_by": "d28-qualification", "deleted_at": None, "security": clone(security),
                "subjects": [], "subjects_state": "unknown", "provenance": {"sources": [], "chain": []}}
    return {"source": {**base("source"), "type": "manual", "hash": source["sha256"],
                       "external_id": source["key"], "uri": source.get("source_url"), "metadata": {}},
            "doc": {**base("doc"), "title": source["key"], "text": source["text"], "source": None,
                    "sidecar_text": None, "media_attachments": [], "metadata": {},
                    "rendition_key": f"d28/text/{source['key']}/{source['revision']}"},
            "ingest_mode": "retrieval", "intake": {"revision_id": source["revision"], "idempotency_key": "fw-" + identity}}


def large_text(contract):
    recipe = contract["full_fixture"]["large_recipe"]
    prefix, filler, suffix = (recipe[key].encode("ascii") for key in ("prefix", "filler", "suffix"))
    count = recipe["target_bytes"] - len(prefix) - len(suffix)
    require(recipe["target_bytes"] == 2_000_000 and 0 < count and filler, "original large input dimensions differ")
    raw = prefix + (filler * ((count + len(filler) - 1) // len(filler)))[:count] + suffix
    require(sha(raw) == recipe["sha256"] and sha(raw + b"X") == recipe["over_limit_sha256"], "independent large input hash differs")
    return raw.decode("ascii")



# Unicode White_Space used by the versioned text renderer; Python str.rstrip()
# additionally removes U+001C..U+001F, so its implicit character set is unsuitable.
RENDER_WHITESPACE = "\u0009\u000a\u000b\u000c\u000d\u0020\u0085\u00a0\u1680" + "".join(chr(c) for c in range(0x2000, 0x200b)) + "\u2028\u2029\u202f\u205f\u3000"


def rendered_origins(original, renderer):
    """Independent versioned byte-origin check; never searches for a quotation."""
    require(type(original) is str and len(original.encode()) <= 2_000_000,
            "bounded original source text required")
    require(type(renderer) is dict and set(renderer) == {"name", "version", "config_hash"}
            and renderer["version"] == "1.0.0" and renderer["config_hash"] is None,
            "unrecognized original immutable renderer")
    raw = original.encode()
    if renderer["name"] == "elephant.renderer.passthrough":
        return raw, [(0, len(raw), 0, 1)]
    require(renderer["name"] == "elephant.renderer.text", "unrecognized text transform")
    chunks, runs, offset, raw_offset, char_offset = [], [], 0, 0, 0
    for match in re.finditer(r"\r\n|\r|\n", original):
        content = original[char_offset:match.start()]
        trimmed = content.rstrip(RENDER_WHITESPACE).encode()
        if trimmed:
            chunks.append(trimmed)
            runs.append((offset, offset + len(trimmed), raw_offset, 1))
            offset += len(trimmed)
        raw_offset += len(content.encode())
        ending = len(match.group().encode())
        chunks.append(b"\n")
        runs.append((offset, offset + 1, raw_offset, ending))
        offset += 1
        raw_offset += ending
        char_offset = match.end()
    if char_offset < len(original) or not original:
        trimmed = original[char_offset:].rstrip(RENDER_WHITESPACE).encode()
        if trimmed:
            chunks.append(trimmed)
            runs.append((offset, offset + len(trimmed), raw_offset, 1))
        chunks.append(b"\n")  # Generated terminal LF has no original origin.
    return b"".join(chunks), runs


def utf8_slice(raw, span):
    require(type(span) is dict and set(span) == {"start", "end"}, "exact byte interval required")
    start, end = span["start"], span["end"]
    require(type(start) is int and type(end) is int and 0 <= start < end <= len(raw),
            "nonempty original byte interval required")
    try:
        raw[start:end].decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise Unmet("interval splits original UTF-8 scalar") from error
    return raw[start:end]


def validate_inputs(contract, corpus, plan, plan_sha256):
    require(contract.get("format") == "recall-full-workload-source-contract/v1"
            and [row["id"] for row in contract["phase_inventory"]] == list(CASES), "original fourteen-case contract differs")
    require(sha(encode(plan)) == plan_sha256 and plan.get("format") == "recall-full-workload-case-plan/v1"
            and plan.get("contract_sha256") == CONTRACT_SHA and set(plan.get("cases", {})) == set(CASES), "independent full case plan absent or changed")
    require(corpus["spaces"] == contract["spaces"] and len(corpus["sources"]) == 50, "original corpus distribution differs")
    keys = set()
    for row in corpus["sources"]:
        identity = (row["space"], row["key"])
        require(row["space"] in SPACES and identity not in keys and sha(row["text"].encode()) == row["sha256"], "original corpus input changed")
        keys.add(identity)
    require(all(sum(row["space"] == space for row in corpus["sources"]) == 25 for space in SPACES), "original 25+25 source counts differ")
    require(set(plan["configurations"]) == set(SPACES), "both independently reviewed configurations required")
    require(set(plan["graph_recipes"]) == set(plan["graph_recipe_sha256"]), "independent Graph recipe/hash inventory differs")
    for key, recipe in plan["graph_recipes"].items():
        validate_graph(recipe, plan["graph_recipe_sha256"][key])
        space = recipe["source_request"]["source"]["space_id"]
        require(space in SPACES and recipe["source_request"]["source"]["security"]
                == plan["configurations"][space]["configuration"]["security"], "Graph fixture changed the named policy envelope")
    validate_spy(plan["spy_recipe"], plan["spy_recipe_sha256"])
    validate_load(plan["load_plan"], plan["load_plan_sha256"])
    validate_case_inputs(plan)
    requirements = plan["cases"]["FW12"]["query_requirements"]
    validate_requirements(plan["load_plan"], requirements, plan["cases"]["FW12"]["query_requirements_sha256"])
    large_text(contract)
    return clone(plan)


def validate_case_inputs(plan):
    """Reject incomplete independent recipes before the first source mutation."""
    for field in ("configurations", "baseline_rendered", "large_rendered"):
        require(field in plan, "missing independent input:" + field)
    require(type(plan["baseline_rendered"]) is list and len(plan["baseline_rendered"]) == 50
            and set(plan["large_rendered"]) == set(SPACES), "complete independent rendered input inventory required")
    for space in SPACES:
        expected = plan["configurations"][space]
        require(type(expected) is dict and {"configuration", "subject", "grant"} <= set(expected), "original named configuration/grant expectation missing")
        for row in (plan["large_rendered"][space],):
            require(type(row.get("rendered_text")) is str and sha(row["rendered_text"].encode()) == row.get("rendered_sha256"),
                    "independent large rendered text hash differs")
    required = {"FW03": {"revision"}, "FW04": {"shared_recipes", "actions"},
                "FW05": {"source_labels"}, "FW06": {"recipe", "pending_query", "completed_query"},
                "FW07": {"recipe", "idempotency_key", "hidden_query", "original_terminal"},
                "FW08": {"recipes"}, "FW09": {"running_recipe", "pending_recipe", "failed_recipe", "intake_recipe", "observe_seconds", "queries", "failed_cleanup", "required_observation_order"},
                "FW10": {"space", "source_label", "subject", "role"},
                "FW11": {"recipe", "revoke_kind", "revoke_id", "observe_seconds", "allowed_original_failure_states", "residual_cleanup"},
                "FW12": {"large_graph_recipes", "query_requirements", "query_requirements_sha256"}}
    references = []
    for identifier, keys in required.items():
        case = plan["cases"][identifier]
        if "FW06" <= identifier <= "FW11":
            require(type(case.get("spaces")) is list and len(case["spaces"]) == 2
                    and {row.get("space") for row in case["spaces"]} == set(SPACES), "complete two-space case inventory required:" + identifier)
            rows = case["spaces"]
        else:
            rows = [case]
        for row in rows:
            require(type(row) is dict and keys <= set(row), "incomplete independent case recipe:" + identifier)
            if "observe_seconds" in row:
                require(type(row["observe_seconds"]) is int and 0 < row["observe_seconds"] <= 120, "original finite lifecycle observation cap required")
            if identifier == "FW09":
                require(row["required_observation_order"] == ["failed_retryable", "running", "pending"],
                        "failed source must be observed before the original held running source")
                validate_cleanup(row["failed_cleanup"], "before_release_of_running")
            if identifier == "FW11":
                states = row["allowed_original_failure_states"]
                require(type(states) is list and len(states) == len(set(states)) and states
                        and set(states) <= {"failed_retryable", "failed_final"}, "explicit public failure states required")
                validate_cleanup(row["residual_cleanup"], "before_binding_restore")
            for key in ("recipe", "running_recipe", "pending_recipe", "failed_recipe", "intake_recipe"):
                if key in row:
                    references.append(row[key])
            for key in ("recipes", "shared_recipes", "large_graph_recipes"):
                if key in row:
                    require(type(row[key]) is list, "explicit recipe list required")
                    references.extend(row[key])
    require(all(type(key) is str and key in plan["graph_recipes"] for key in references), "required independently reviewed Graph recipe absent")
    require(len(plan["cases"]["FW04"]["shared_recipes"]) == 4
            and len(plan["cases"]["FW12"]["large_graph_recipes"]) == 2
            and len(plan["cases"]["FW05"]["source_labels"]) >= 2, "original fixture/no-op counts differ")




def evaluate_ob3_originals(oracle, raw_receipt, raw_expectations):
    """Use the bootstrap's verified-byte callable without reopening code files."""
    require(callable(oracle), "original verified OB3 oracle callable is unavailable")
    return oracle(raw_receipt, raw_expectations)


def validate_cleanup(cleanup, ordering):
    require(type(cleanup) is dict and cleanup.get("action") == "retract"
            and cleanup.get(ordering) is True and cleanup.get("no_later_original_physical_sends") is True
            and type(cleanup.get("idempotency_key")) is str and cleanup["idempotency_key"]
            and subset(cleanup.get("original_terminal"), {"state": "failed_final", "lifecycle": "active"})
            and subset(cleanup.get("current_head"), {"lifecycle": "retracted", "state": "complete", "published_revision": None}),
            "reviewed original public retraction cleanup required")


class NamedClient(Client):
    """One physical budget and clock; each named space selects its own JWT."""
    async def request(self, method, path, space, body=None, *, role, **options):
        selected = f"{space}:{role}"
        require(selected in self.tokens, "original named-space principal missing")
        return await super().request(method, path, space, body, role=selected, **options)


class Driver:
    def __init__(self, client, spy, contract, corpus, plan, plan_sha256, *, backend, nonce,
                 external_receipts=None, service_control=None, capacity_capture=None, ob3_oracle=None,
                 runtime_inspection=None):
        self.client, self.spy, self.contract, self.corpus = client, spy, contract, corpus
        self.plan = validate_inputs(contract, corpus, plan, plan_sha256)
        self.plan_sha256, self.backend, self.nonce = plan_sha256, backend, nonce
        require(backend in contract["controlled_fixture_limits"]["backends"], "unsupported original backend")
        require(type(nonce) is str and re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", nonce), "original run nonce missing")
        self.external_receipts, self.service_control = external_receipts or {}, service_control
        require(ob3_oracle is None or callable(ob3_oracle), "invalid original OB3 oracle callable")
        self.ob3_oracle = ob3_oracle
        require(runtime_inspection is None or callable(runtime_inspection), "invalid original runtime inspection callable")
        self.runtime_inspection = runtime_inspection
        self.capacity_capture = capacity_capture
        self.graph_publications = {}
        self.coordinate_cache, self.fenced_rules = {}, {}
        self.inputs, self.accepted, self.published, self.graphs, self.held = {}, {}, {}, {}, {}
        self.results, self.events, self.event_count = [], [], 0
        self.current_case, self.stopped = None, False

    def check(self):
        require(self.client.clock() < self.client.deadline and time.time() < self.client.expires, "original full workload deadline expired")
        require(self.spy.failure is None, "original controlled provider failed")
        for rule, original_count in self.fenced_rules.items():
            require(self.spy.rule_counts.get(rule, 0) == original_count,
                    "retracted original source dispatched another physical request")

    def event(self, kind, value):
        self.event_count += 1
        raw = encode({"format": "recall-full-workload-event/v1", "case": self.current_case,
                      "kind": kind, "monotonic_ns": time.monotonic_ns(), "value": value})
        self.client.capture.reserve(len(raw))
        path = self.client.capture.directory / f"event-{self.event_count:06}.json"
        private_new(path, raw)
        self.client.capture.held -= len(raw)
        self.client.capture.written += len(raw)
        row = {"file": path.name, "sha256": sha(raw), "bytes": len(raw), "kind": kind}
        self.events.append(row)
        return row

    async def exchange(self, method, path, space, body=None, *, role="data", statuses=(200,), **options):
        self.check()
        if method != "GET":
            self.event("original-public-request-intent", {"method": method, "path": path,
                "space": space, "role": role, "body": body, "idempotency_key": options.get("idempotency")})
        row = await self.client.request(method, path, space, body, role=role, **options)
        require(row.get("framing_complete") is True and row.get("error_type") is None
                and row.get("status") in statuses, "actual public status/framing unavailable")
        return row

    async def get(self, path, space, *, role="data"):
        return (await self.exchange("GET", path, space, role=role))["body"]

    async def head(self, space, key):
        return await self.get(head_path(space, key), space)

    async def pause(self, maximum=0.1):
        self.check()
        await asyncio.sleep(max(0.0, min(maximum, self.client.deadline - self.client.clock(), self.client.expires - time.time())))

    async def operation(self, request, accepted, *, active=True):
        value = await self.get("/v1/operations/" + quote(accepted["operation"]["operation_id"], safe=""), request["source"]["space_id"])
        original_identity(accepted, value, request["intake"]["revision_id"], active=active)
        return value

    async def wait_state(self, request, accepted, wanted, *, active=True):
        while True:
            value = await self.operation(request, accepted, active=active)
            if value["state"] in wanted:
                return value
            require(value["state"] != "failed_final", "original operation failed before required state")
            await self.pause()

    async def accept(self, label, request):
        require(label not in self.inputs, "original submission cannot be automatically retried")
        self.inputs[label] = clone(request)  # Retain before the first physical send, including uncertain failure.
        self.event("original-source-intent", {"label": label, "request": request})
        row = await self.exchange("POST", "/v1/sources/ingest", request["source"]["space_id"], request, statuses=(202,))
        body = accepted_identity(request, row["body"])
        require(type(body.get("work_item_id")) is str and body["work_item_id"], "original source acceptance did not enqueue work")
        self.accepted[label] = body
        return body

    async def observe_retrieval(self, label, rendered):
        request, accepted = self.inputs[label], self.accepted[label]
        space, revision = request["source"]["space_id"], request["intake"]["revision_id"]
        operation = await self.wait_state(request, accepted, {"complete"})
        require(operation.get("published_revision") == revision and operation.get("error_code") is None, "completed original revision differs")
        head = await self.head(space, request["source"]["external_id"])
        original_identity(accepted, head, revision)
        require(head.get("published_revision") == revision and head.get("state") == "complete", "original current publication incomplete")
        doc_path = "/v1/docs/" + quote(accepted["doc_id"], safe="")
        doc, source, pipeline = await self.get(doc_path, space), await self.get("/v1/sources/" + quote(accepted["source_id"], safe=""), space), await self.get(doc_path + "/pipeline-status", space)
        require(doc.get("id") == accepted["doc_id"] and doc.get("source") == accepted["source_id"]
                and doc.get("text") == rendered["rendered_text"] and sha(doc["text"].encode()) == rendered["rendered_sha256"], "actual independently expected rendered bytes differ")
        require(source.get("id") == accepted["source_id"] and source.get("external_id") == request["source"]["external_id"]
                and source.get("hash") == request["source"]["hash"] and source.get("current_ingest_mode") == "retrieval"
                and source.get("lifecycle_status") == "retrieval_ready", "original source has not reached RetrievalReady")
        for value in (doc, source):
            require(value.get("space_id") == space and value.get("deleted_at") is None, "published record envelope differs")
            for key in ("security", "subjects", "subjects_state"):
                require(value.get(key) == request["source"][key], "original source/document policy changed")
        require(pipeline.get("doc_id") == accepted["doc_id"] and type(pipeline.get("revision")) is str
                and pipeline["revision"].startswith("doc_revision:"), "actual immutable revision mapping absent")
        immutable = await self.get(doc_path + "/revisions/" + quote(pipeline["revision"], safe=""), space)
        require(immutable.get("id") == pipeline["revision"] and immutable.get("doc") == accepted["doc_id"]
                and immutable.get("space_id") == space and immutable.get("status") == "active"
                and immutable.get("deleted_at") is None and immutable.get("text") == rendered["rendered_text"]
                and immutable.get("text_hash") == rendered["rendered_sha256"], "actual immutable Retrieval revision differs")
        for key in ("security", "subjects", "subjects_state"):
            require(immutable.get(key) == request["source"][key], "immutable Retrieval revision policy differs")
        provenance = immutable.get("provenance", {})
        require(provenance.get("sources") == [accepted["source_id"]]
                and any(step.get("step") == "source_content" and step.get("input") == accepted["source_id"]
                        and step.get("output") == request["source"]["hash"] for step in provenance.get("chain", [])),
                "immutable Retrieval revision lacks original raw-source binding")
        observed = {"accepted": accepted, "operation": operation, "head": head, "source": source,
                    "doc": doc, "pipeline": pipeline, "immutable_revision": immutable}
        self.published[label] = observed
        self.event("completed-retrieval-publication", {"label": label, **observed})
        return observed

    async def query(self, recipe):
        space, body = recipe["space"], recipe["body"]
        require(space in SPACES and body.get("ranking_mode") == "lexical", "controlled phase requires reviewed lexical query")
        row = await self.exchange("POST", f"/v1/spaces/{quote(space, safe='')}/search", space, body,
                                  statuses=tuple(recipe.get("statuses", [200])), response_limit=262144)
        require(subset(row["body"], recipe["expected"]), "actual query differs from independent literal expectation")
        if row["status"] == 200:
            self.query_evidence(space, body, row["body"], recipe.get("required_evidence", []),
                                recipe.get("required_claim_evidence", []))
        self.event("independent-query-observation", {"recipe": recipe, "exchange_id": row["id"], "body": row["body"]})
        return row["body"]

    def query_evidence(self, space, request, response, required, required_claim=()):
        """Verify complete actual evidence against original inputs and observed IDs."""
        require(type(response) is dict and type(response.get("results")) is list
                and type(response.get("coverage")) is dict and type(response.get("partial")) is bool,
                "actual complete query response shape absent")
        constraints = request.get("source_revision_constraints")
        require(type(constraints) is dict and constraints, "original query revision scope absent")
        require(type(required) is list and len(required) <= 100
                and type(required_claim) in (list, tuple) and len(required_claim) <= 100,
                "bounded literal evidence expectations required")
        fields = {"source_key", "revision", "quote", "quote_hash"}
        for expected in [*required, *required_claim]:
            require(type(expected) is dict and set(expected) == fields
                    and all(type(expected[key]) is str and expected[key] for key in fields)
                    and sha(expected["quote"].encode()) == expected["quote_hash"]
                    and constraints.get(expected["source_key"]) == expected["revision"],
                    "independent literal evidence differs from original query")
        originals = {}
        for label, observation in self.published.items():
            original = self.inputs[label]
            if original["source"]["space_id"] == space:
                originals[(original["source"]["external_id"], original["intake"]["revision_id"])] = (original, observation)
        for key, observation in self.graph_publications.items():
            fixture = self.graphs[key]
            if fixture.space == space:
                originals[(fixture.request["source"]["external_id"], fixture.request["intake"]["revision_id"])] = (fixture.request, observation)
        found, claims = set(), set()
        for result in response["results"]:
            require(type(result) is dict and set(result) == {"kind", "text", "evidence", "score"}
                    and result["kind"] in ("claim", "entity", "chunk") and type(result["text"]) is str
                    and type(result["score"]) in (int, float) and math.isfinite(result["score"]) and type(result["evidence"]) is list
                    and result["evidence"], "actual result lacks complete source-backed evidence")
            for evidence in result["evidence"]:
                require(type(evidence) is dict and set(evidence) == {"source_id", "revision", "span", "quote", "internal_doc_revision_id", "doc_id",
                            "rendered_span", "rendered_quote_sha256", "rendered_text_sha256"}
                        and type(evidence["source_id"]) is str and type(evidence["revision"]) is str
                        and type(evidence["quote"]) is str and type(evidence["span"]) is dict
                        and set(evidence["span"]) == {"start", "end"}, "actual evidence fields differ")
                identity = (evidence["source_id"], evidence["revision"])
                require(identity in originals and constraints.get(identity[0]) == identity[1]
                        and type(response["coverage"].get(identity[0])) is dict
                        and response["coverage"][identity[0]].get("published_revision") == identity[1]
                        and response["coverage"][identity[0]].get("state") == "complete",
                        "query evidence is foreign, unknown or not the requested published revision")
                original, observation = originals[identity]
                self.evidence_coordinates(space, original, observation, evidence)
                literal = encode({"source_key": identity[0], "revision": identity[1], "quote": evidence["quote"], "quote_hash": sha(evidence["quote"].encode())})
                found.add(literal)
                if result["kind"] == "claim":
                    claims.add(literal)
        require(all(encode(expected) in found for expected in required), "independent required evidence was not actually returned")
        require(all(encode(expected) in claims for expected in required_claim),
                "independent required canonical claim evidence was not actually returned")

    def evidence_coordinates(self, space, original, observation, evidence):
        """Bind both intervals to the full, actually observed completed revision."""
        source, accepted, immutable = original["source"], observation["accepted"], observation["immutable_revision"]
        observed_source = observation["source"]
        require(observed_source.get("id") == accepted["source_id"] and observed_source.get("space_id") == space
                and observed_source.get("external_id") == source["external_id"]
                and observed_source.get("hash") == source["hash"] and observed_source.get("deleted_at") is None,
                "original accepted source differs from actual completed publication")
        revision = original["intake"]["revision_id"]
        for value in (observation["operation"], observation["head"]):
            original_identity(accepted, value, revision)
            require(value.get("state") == "complete" and value.get("published_revision") == revision
                    and value.get("error_code") is None, "original completed publication proof absent")
        require(source["space_id"] == space and immutable.get("space_id") == space
                and immutable.get("id") == evidence["internal_doc_revision_id"]
                and type(evidence["doc_id"]) is str
                and re.fullmatch(r"doc:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", evidence["doc_id"])
                and evidence["doc_id"] == immutable.get("doc") == accepted["doc_id"]
                and immutable.get("status") == "active"
                and immutable.get("deleted_at") is None and type(immutable.get("text")) is str,
                "query evidence substituted the original full immutable revision")
        for field in ("security", "subjects", "subjects_state"):
            require(immutable.get(field) == source[field], "immutable evidence policy differs from original input")
        raw, rendered = original["doc"]["text"].encode(), immutable["text"].encode()
        require(sha(raw) == source["hash"], "original accepted raw input hash differs")
        provenance = immutable.get("provenance", {})
        require(provenance.get("sources") == [accepted["source_id"]]
                and any(step.get("step") == "source_content" and step.get("input") == accepted["source_id"]
                        and step.get("output") == source["hash"] for step in provenance.get("chain", [])),
                "full immutable revision lacks original raw-source binding")
        for key in ("rendered_quote_sha256", "rendered_text_sha256"):
            require(type(evidence[key]) is str and re.fullmatch(r"[0-9a-f]{64}", evidence[key]),
                    "exact lowercase rendered evidence digest required")
        require(sha(rendered) == immutable.get("text_hash") == evidence["rendered_text_sha256"],
                "whole immutable rendered evidence hash differs")
        require(utf8_slice(raw, evidence["span"]) == evidence["quote"].encode(),
                "query evidence quote differs from original UTF-8 source span")
        require(sha(utf8_slice(rendered, evidence["rendered_span"])) == evidence["rendered_quote_sha256"],
                "rendered support digest differs from exact immutable interval")
        renderer = immutable.get("renderer")
        key = (source["hash"], immutable["text_hash"], encode(renderer))
        if key not in self.coordinate_cache:
            actual_rendered, runs = rendered_origins(original["doc"]["text"], renderer)
            require(actual_rendered == rendered, "full immutable text differs from original versioned transform")
            self.coordinate_cache[key] = runs
        start, end = evidence["rendered_span"]["start"], evidence["rendered_span"]["end"]
        cover = None
        for left, right, original_start, stride in self.coordinate_cache[key]:
            low, high = max(left, start), min(right, end)
            if low < high:
                interval = (original_start + (low - left) * stride, original_start + (high - left) * stride)
                cover = (cover[0], interval[1]) if cover is not None else interval
        require(cover is not None and evidence["span"] == {"start": cover[0], "end": cover[1]},
                "raw evidence is not the minimal original cover of rendered support")

    def retain_graph_publication(self, recipe_id, observation):
        require(recipe_id in self.graphs and recipe_id not in self.graph_publications,
                "original Graph publication observation duplicated or unknown")
        self.graph_publications[recipe_id] = clone(observation)
        return observation

    async def complete_graph(self, recipe_id):
        fixture = self.graphs[recipe_id]
        return self.retain_graph_publication(recipe_id, await fixture.observe_completed(await fixture.wait_complete()))

    async def fixture(self, recipe_id, *, replace=False, generation=None):
        require(recipe_id not in self.graphs, "original Graph recipe cannot be resubmitted")
        original = self.plan["graph_recipes"][recipe_id]
        recipe = clone(original)
        request = recipe["source_request"]
        if replace:
            require("expected_generation" not in request["intake"], "template must not invent a future generation")
            head = await self.head(request["source"]["space_id"], request["source"]["external_id"])
            require(head.get("lifecycle") == "active" and type(head.get("generation")) is str, "actual original active head unavailable")
            require(generation is None or head["generation"] == generation, "original competing CAS head changed before dispatch")
            request["intake"]["expected_generation"] = generation or head["generation"]
            self.event("original-generation-binding", {"recipe": recipe_id, "reviewed_template_sha256": self.plan["graph_recipe_sha256"][recipe_id], "head": head})
        actual_hash = sha(encode(recipe))
        self.event("resolved-graph-intent", {"recipe": recipe_id, "reviewed_template_sha256": self.plan["graph_recipe_sha256"][recipe_id], "wire_recipe_sha256": actual_hash, "source_request": request})
        fixture = GraphFixture(self.client, self.spy, recipe, actual_hash)
        self.graphs[recipe_id] = fixture
        return fixture

    async def held_fixture(self, recipe_id):
        fixture = await self.fixture(recipe_id, replace=True)
        held = await fixture.hold_next_exact_source()
        self.held[recipe_id] = held
        self.event("original-extraction-held", {"recipe": recipe_id, **held})
        return fixture

    def release(self, recipe_id):
        require(recipe_id in self.held, "no original held extraction for this recipe")
        self.graphs[recipe_id].release_original(self.held.pop(recipe_id))

    async def lifecycle(self, space, key, action, generation, intent):
        require(action in ("retract", "erase"), "unsupported public lifecycle action")
        row = await self.exchange("POST", f"/v1/spaces/{quote(space, safe='')}/sources/{quote(key, safe='')}:{action}",
            space, {"expected_generation": generation}, statuses=(202,), idempotency=intent)
        value = row["body"]
        require(type(value) is dict and type(value.get("operation_id")) is str
                and type(value.get("generation")) is str and value.get("lifecycle") == ("retracted" if action == "retract" else "erased"), "actual lifecycle acceptance unavailable")
        self.event("original-lifecycle-acceptance", {"action": action, "space": space, "key": key, "body": value})
        return value

    async def fw01(self):
        for space in SPACES:
            expected = self.plan["configurations"][space]
            actual = await self.get("/v1/spaces/" + quote(space, safe=""), space, role="manage")
            # Operational timestamps are retained in capture but not independent configuration authority.
            stable = clone(actual)
            stable.pop("updated_at", None)
            require(subset(stable, expected["configuration"]), "actual classified named configuration differs")
            security = expected["configuration"]["security"]
            require(security["level"] == (3 if space == SPACES[0] else 7)
                    and security["policy_id"] == ("household-policy" if space == SPACES[0] else "business-policy"), "independent policy3/7 expectation differs")
            grant = await self.get(f"/v1/spaces/{quote(space, safe='')}/grants/{quote(expected['subject'], safe='')}", space, role="manage")
            require(subset(grant, expected["grant"]), "actual current named-space grant differs")
            other = SPACES[1] if space == SPACES[0] else SPACES[0]
            denied = await self.exchange("GET", "/v1/spaces/" + quote(other, safe=""), space, statuses=(403,))
            require(type(denied["body"]) is dict and denied["body"].get("error_code") == "forbidden_space", "wrong-space call lacked typed denial")
            self.event("classified-configuration", {"space": space, "configuration": actual, "grant": grant, "wrong_space": denied})
        # Refuse a non-fresh fixture before any source mutation, including extra keys.
        for space in SPACES:
            keys = [row["key"] for row in self.corpus["sources"] if row["space"] == space] + ["full/large", "full/shared/a", "full/shared/b", "full/large-over-limit"]
            for key in keys:
                absent = await self.exchange("GET", head_path(space, key), space, statuses=(404,))
                require(absent["body"].get("error_code") == "not_found", "fresh original source absence unproved")

    async def fw02(self):
        rendered = {(row["space"], row["source_key"], row["revision"]): row for row in self.plan["baseline_rendered"]}
        require(len(rendered) == 50, "independent complete fifty-source rendered map required")
        for source in self.corpus["sources"]:
            label = source["space"] + "/" + source["key"]
            security = self.plan["configurations"][source["space"]]["configuration"]["security"]
            request = source_request(source, self.backend, self.nonce, security)
            await self.accept(label, request)
            await self.observe_retrieval(label, rendered[(source["space"], source["key"], source["revision"])])
        require(len(self.accepted) == len(self.published) == 50, "actual fifty-source publication incomplete")
        # Known foreign IDs are tested after real IDs exist, never input UUID placeholders.
        for space in SPACES:
            other = SPACES[1] if space == SPACES[0] else SPACES[0]
            foreign = self.published[other + "/workload/atlas-00"]["accepted"]
            for kind, identifier in (("sources", foreign["source_id"]), ("docs", foreign["doc_id"])):
                denied = await self.exchange("GET", f"/v1/{kind}/{quote(identifier, safe='')}", space, statuses=(403, 404))
                require(denied["body"].get("error_code") in ("forbidden_space", "not_found"), "known foreign ID lacked typed denial")

    async def fw03(self):
        text = large_text(self.contract)
        for space in SPACES:
            source = {"space": space, "key": "full/large", "revision": self.plan["cases"]["FW03"]["revision"], "text": text, "sha256": sha(text.encode()), "source_url": None}
            security = self.plan["configurations"][space]["configuration"]["security"]
            label = space + "/full/large"
            await self.accept(label, source_request(source, self.backend, self.nonce, security))
            await self.observe_retrieval(label, self.plan["large_rendered"][space])
            await self.quiescent()
            before = self.spy.snapshot_calls()["physical_connections"]
            bad = {**source, "key": "full/large-over-limit", "text": text + "X", "sha256": sha((text + "X").encode())}
            request = source_request(bad, self.backend, self.nonce, security)
            denial = await self.exchange("POST", "/v1/sources/ingest", space, request, statuses=(400, 413))
            require(denial["body"].get("error_code") == "payload_too_large", "limit+1 lacked typed size refusal")
            require(not any(key in denial["body"] for key in ("operation", "operation_id", "work_item_id", "source_id")), "refused size request minted public acceptance")
            absent = await self.exchange("GET", head_path(space, bad["key"]), space, statuses=(404,))
            require(absent["body"].get("error_code") == "not_found" and self.spy.snapshot_calls()["physical_connections"] == before,
                    "refused size request changed visible head or physical provider inventory")
            self.event("large-limit-refusal", {"space": space, "input_bytes": 2_000_001, "input_sha256": bad["sha256"], "response": denial, "head": absent, "provider_before": before, "provider_after": self.spy.snapshot_calls()["physical_connections"], "private_work_inventory": "unmet: not exposed by these public reads"})

    async def large_graph_capacity(self):
        recipes = self.plan["cases"]["FW12"].get("large_graph_recipes", [])
        require(len(recipes) == 2, "independently reviewed two-space full 2 MB Graph recipes required")
        spaces = set()
        for key in recipes:
            fixture = await self.fixture(key, replace=True)
            require(fixture.space in SPACES and fixture.space not in spaces
                    and fixture.request["source"]["external_id"] == "full/large"
                    and fixture.request["doc"]["text"] == large_text(self.contract),
                    "large Graph must reuse the original two exact 2 MB source identities")
            spaces.add(fixture.space)
            observed = self.retain_graph_publication(key, await fixture.accept_and_wait())
            self.event("actual-full-2MB-Graph-publication", {"recipe": key, "observation": observed})
            if self.capacity_capture is None:
                self.event("large-Graph-export-capacity", {"status": "unmet", "reason": "original artifact-owner export/compact report byte capture required"})
            else:
                receipt = await self.capacity_capture.capture_original_export(self.backend, self.nonce, observed)
                self.event("large-Graph-export-capacity", receipt)

    async def quiescent(self):
        while True:
            self.check()
            calls = self.spy.snapshot_calls()
            if calls["active"] == 0 and not calls["held_ids"]:
                return calls
            await self.pause()

    async def fw04(self):
        case = self.plan["cases"]["FW04"]
        require(len(case["shared_recipes"]) == 4, "exactly four independent shared-support inputs required")
        seen, shared = set(), {}
        for key in case["shared_recipes"]:
            fixture = await self.fixture(key)
            request = fixture.request
            identity = (fixture.space, request["source"]["external_id"])
            require(identity not in seen and identity[0] in SPACES and identity[1] in ("full/shared/a", "full/shared/b")
                    and request["doc"]["text"] == self.contract["full_fixture"]["shared_text"], "independent shared inputs differ")
            seen.add(identity)
            observed = self.retain_graph_publication(key, await fixture.accept_and_wait())
            shared[identity] = observed
            self.event("completed-shared-graph", {"recipe": key, "observation": observed})
        require(len(seen) == 4 and len(self.accepted) + len(seen) == 56, "original fifty-six source fixture incomplete")
        for action in case["actions"]:
            fixture = self.graphs[action["recipe"]]
            other_space = SPACES[1] if fixture.space == SPACES[0] else SPACES[0]
            counterpart = shared[(other_space, fixture.request["source"]["external_id"])]
            async def full_other_payload():
                original = counterpart["accepted"]
                doc_path = "/v1/docs/" + quote(original["doc_id"], safe="")
                return {"head": await self.head(other_space, fixture.request["source"]["external_id"]),
                        "source": await self.get("/v1/sources/" + quote(original["source_id"], safe=""), other_space),
                        "doc": await self.get(doc_path, other_space),
                        "immutable_revision": await self.get(doc_path + "/revisions/" + quote(counterpart["pipeline"]["revision"], safe=""), other_space)}
            other_before = await full_other_payload()
            before = await self.head(fixture.space, fixture.request["source"]["external_id"])
            await self.query(action["before_query"])
            await self.lifecycle(fixture.space, fixture.request["source"]["external_id"], action["action"], before["generation"], action["idempotency_key"])
            await self.query(action["survivor_query"])
            for query in action["other_space_queries"]:
                await self.query(query)
            other_after = await full_other_payload()
            require(other_after == other_before, "shared support removal changed the independent other-space full payload")
            self.event("independent-other-space-full-payload", {"before": other_before, "after": other_after})
            self.event("shared-proof-limit", {"private_support_receipt_chain": "unmet: public facts do not expose complete private support receipts", "physical_erasure": "unmet: requires independent export/media evidence"})

    async def fw05(self):
        await self.quiescent()
        for label in self.plan["cases"]["FW05"]["source_labels"]:
            require(label in self.published, "original no-op publication unavailable")
            request, original = self.inputs[label], self.published[label]
            space, key = request["source"]["space_id"], request["source"]["external_id"]
            head_before = await self.head(space, key)
            operation_before = await self.operation(request, self.accepted[label])
            calls = self.spy.snapshot_calls()["physical_connections"]
            retry = await self.exchange("POST", "/v1/sources/ingest", space, request, statuses=(202,))
            accepted_identity(request, retry["body"])
            receipt = retry["body"]["operation"]
            original_receipt = self.accepted[label]["operation"]
            require(all(receipt.get(key) == original_receipt.get(key) for key in
                        ("space_id", "operation_id", "source_id", "source_generation", "desired_revision", "accepted_at"))
                    and receipt.get("state") == "completed"
                    and receipt.get("published_revision") == operation_before["published_revision"]
                    and receipt.get("error_code") == operation_before.get("error_code")
                    and retry["body"]["source_id"] == self.accepted[label]["source_id"]
                    and retry["body"]["doc_id"] == self.accepted[label]["doc_id"] and retry["body"].get("work_item_id") is None,
                    "exact original retry minted a different receipt or new work")
            require(await self.head(space, key) == head_before and await self.operation(request, self.accepted[label]) == operation_before
                    and self.spy.snapshot_calls()["physical_connections"] == calls, "original no-op changed publication or provider inventory")
            self.event("original-intent-no-op", {"label": label, "original": self.accepted[label], "retry": retry, "head": head_before, "operation": operation_before, "provider_count": calls})

    def space_cases(self, identifier):
        cases = self.plan["cases"][identifier]["spaces"]
        require(type(cases) is list and len(cases) == 2 and {row["space"] for row in cases} == set(SPACES),
                "both original named-space lifecycle cases required")
        for row in cases:
            if "observe_seconds" in row:
                require(type(row["observe_seconds"]) is int and 0 < row["observe_seconds"] <= 120,
                        "bounded original lifecycle observation window required")
            for key in ("recipe", "running_recipe", "pending_recipe", "failed_recipe", "intake_recipe"):
                if key in row:
                    require(self.plan["graph_recipes"][row[key]]["source_request"]["source"]["space_id"] == row["space"],
                            "reviewed lifecycle recipe belongs to another named space")
        return cases

    async def fw06(self):
        for case in self.space_cases("FW06"):
            await self._fw06(case)

    async def _fw06(self, case):
        fixture = await self.held_fixture(case["recipe"])
        try:
            await self.query(case["pending_query"])
            original = await self.operation(fixture.request, fixture.accepted)
            require(original["state"] == "running", "held original operation was not publicly Running")
        finally:
            self.release(case["recipe"])
        self.event("completed-replacement-graph", await self.complete_graph(case["recipe"]))
        await self.query(case["completed_query"])

    async def fw07(self):
        for case in self.space_cases("FW07"):
            await self._fw07(case)

    async def _fw07(self, case):
        fixture = await self.held_fixture(case["recipe"])
        try:
            head = await self.head(fixture.space, fixture.request["source"]["external_id"])
            await self.lifecycle(fixture.space, fixture.request["source"]["external_id"], "retract", head["generation"], case["idempotency_key"])
            await self.query(case["hidden_query"])
        finally:
            self.release(case["recipe"])
        original = await self.wait_state(fixture.request, fixture.accepted, {"failed_final"}, active=False)
        require(subset(original, case["original_terminal"]), "stale original attempt outcome differs")
        head = await self.head(fixture.space, fixture.request["source"]["external_id"])
        require(head.get("lifecycle") == "retracted", "released stale extraction republished retracted source")
        await self.query(case["hidden_query"])
        self.event("stale-original-retraction", {"original_operation": original, "current_head": head})

    async def fw08(self):
        for case in self.space_cases("FW08"):
            await self._fw08(case)

    async def _fw08(self, case):
        require(len(case["recipes"]) == 2, "exactly two independent competing revisions required")
        recipes = [self.plan["graph_recipes"][key] for key in case["recipes"]]
        source = recipes[0]["source_request"]["source"]
        require(all(row["source_request"]["source"]["space_id"] == source["space_id"] and row["source_request"]["source"]["external_id"] == source["external_id"] for row in recipes)
                and recipes[0]["source_request"]["intake"]["revision_id"] != recipes[1]["source_request"]["intake"]["revision_id"], "original CAS intents are not distinct revisions of one source")
        head = await self.head(source["space_id"], source["external_id"])
        first = await self.fixture(case["recipes"][0], replace=True, generation=head["generation"])
        await first.accept()
        second = clone(recipes[1]["source_request"])
        require("expected_generation" not in second["intake"], "competing template invented current authority")
        second["intake"]["expected_generation"] = head["generation"]
        self.event("original-competing-CAS-intent", {"original_head": head, "reviewed_template_sha256": self.plan["graph_recipe_sha256"][case["recipes"][1]], "request": second})
        loser = await self.exchange("POST", "/v1/sources/ingest", source["space_id"], second, statuses=(409,))
        require(loser["body"].get("error_code") == "conflict_generation", "competing original CAS lacked typed conflict")
        self.event("competing-original-CAS", {"original_head": head, "winner": first.accepted, "loser_request": second, "loser": loser})
        self.event("completed-CAS-winner", await self.complete_graph(case["recipes"][0]))

    async def fw09(self):
        boundaries = ("before_fw09_first_space", "before_fw09_second_space")
        for boundary, case in zip(boundaries, self.space_cases("FW09"), strict=True):
            await self.inspect_runtime(boundary)
            await self._fw09(case)
        await self.inspect_runtime("after_fw09_cleanup")

    async def inspect_runtime(self, boundary):
        self.check()
        require(callable(self.runtime_inspection), "original effective worker runtime inspection is unavailable")
        # The bound inspector retains its original initial process/recipe and
        # captures actual responses using this same transport and allocation.
        # A startup boolean cannot stand in for that independent validation.
        result = await self.runtime_inspection(boundary)
        require(result is None, "runtime inspector must retain original observations without returning a verdict")
        self.check()
        self.event("original-runtime-revalidated", {"boundary": boundary})

    async def _fw09(self, case):
        require(case["required_observation_order"] == ["failed_retryable", "running", "pending"],
                "independent failed-first backlog ordering differs")
        validate_cleanup(case["failed_cleanup"], "before_release_of_running")
        failed = await self.fixture(case["failed_recipe"], replace=True)
        await failed.accept()
        first_deadline = min(self.client.deadline, self.client.clock() + case["observe_seconds"])
        first_failure = None
        while self.client.clock() < first_deadline:
            value = await self.operation(failed.request, failed.accepted)
            self.event("original-backlog-failure-sample", value)
            if value["state"] == "failed_retryable":
                first_failure = value
                break
            require(value["state"] not in {"complete", "failed_final"}, "original failed source missed retryable observation")
            await self.pause()
        require(first_failure is not None, "original retryable source was not observed before holding the worker")
        failed_rule, failed_count = self.original_rule_count(case["failed_recipe"])
        require(failed_count > 0, "retryable source has no actual original controlled dispatch")
        running = await self.held_fixture(case["running_recipe"])
        try:
            pending = await self.fixture(case["pending_recipe"], replace=True)
            await pending.accept()
            # The original worker profile must expose all three current states;
            # neither a hold nor the earlier failed sample can stand in for them.
            deadline = min(self.client.deadline, self.client.clock() + case["observe_seconds"])
            observed = None
            while self.client.clock() < deadline:
                rows = [await self.operation(item.request, item.accepted) for item in (running, pending, failed)]
                self.event("actual-backlog-sample", rows)
                require(self.spy.rule_counts.get(failed_rule, 0) == failed_count,
                        "failed-first source retried before the original backlog was fenced")
                if [row["state"] for row in rows] == ["running", "pending", "failed_retryable"]:
                    observed = rows
                    break
                await self.pause()
            require(observed is not None, "distinct Running/Pending/FailedRetryable states not publicly observed")
            for query in case["queries"]:
                await self.query(query)
            intake = await self.fixture(case["intake_recipe"], replace=True)
            await intake.accept()
            self.event("bounded-backlog-intake", {"accepted": intake.accepted, "states": observed})
            await self.retract_original(case["failed_recipe"], case["failed_cleanup"], failed_count)
        finally:
            self.release(case["running_recipe"])
        # Only the three surviving active intents are expected to complete.
        self.event("post-backlog-original-states", [await self.operation(item.request, item.accepted, active=False)
                    for item in (running, pending, failed)])
        for recipe_id in (case["running_recipe"], case["pending_recipe"], case["intake_recipe"]):
            self.event("completed-original-backlog-graph", await self.complete_graph(recipe_id))
        await self.quiescent()
        self.check()

    def original_rule_count(self, recipe_id):
        rule_id = self.plan["graph_recipes"][recipe_id]["extract_rule_id"]
        rules = [row for row in self.spy.recipe["rules"] if row["id"] == rule_id]
        require(len(rules) == 1 and rules[0]["space"] == self.graphs[recipe_id].space,
                "exact original source dispatch rule missing")
        count = self.spy.rule_counts.get(rule_id, 0)
        require(type(count) is int and count >= 0, "actual source dispatch count malformed")
        return rule_id, count

    async def retract_original(self, recipe_id, cleanup, original_count):
        fixture = self.graphs[recipe_id]
        rule_id, current_count = self.original_rule_count(recipe_id)
        require(current_count == original_count and rule_id not in self.fenced_rules,
                "original source dispatched before cleanup or cleanup was repeated")
        key = fixture.request["source"]["external_id"]
        before = await self.head(fixture.space, key)
        original_identity(fixture.accepted, before, fixture.request["intake"]["revision_id"])
        self.event("original-failed-source-cleanup-intent", {"recipe": recipe_id, "head": before,
                    "cleanup": cleanup, "original_rule": rule_id, "original_rule_count": original_count})
        receipt = await self.lifecycle(fixture.space, key, "retract", before["generation"], cleanup["idempotency_key"])
        require(receipt.get("internal_source_id") == fixture.accepted["source_id"]
                and receipt["operation_id"] != fixture.accepted["operation"]["operation_id"]
                and receipt["generation"] != before["generation"], "cleanup substituted original source or reused original operation")
        original = await self.wait_state(fixture.request, fixture.accepted, {"failed_final"}, active=False)
        head = await self.head(fixture.space, key)
        require(subset(original, cleanup["original_terminal"]) and subset(head, cleanup["current_head"])
                and head.get("operation_id") == receipt["operation_id"] and head.get("generation") == receipt["generation"]
                and head.get("internal_source_id") == fixture.accepted["source_id"],
                "original failed receipt and new retracted head were not separately observed")
        require(self.spy.rule_counts.get(rule_id, 0) == original_count,
                "original source dispatched during cleanup")
        self.fenced_rules[rule_id] = original_count
        result = {"original_operation": original, "cleanup_receipt": receipt, "current_head": head,
                  "original_rule": rule_id, "original_rule_count": original_count}
        self.event("original-failed-source-retracted", result)
        return result

    async def fw10(self):
        for case in self.space_cases("FW10"):
            await self._fw10(case)

    async def mcp(self, space, role, method, params):
        identifier = self.client.count + 1
        row = await self.exchange("POST", "/mcp/full", space,
            {"jsonrpc": "2.0", "id": identifier, "method": method, "params": params},
            role=role, response_limit=262144)
        envelope = row["body"]
        require(type(envelope) is dict and envelope.get("jsonrpc") == "2.0"
                and type(envelope.get("id")) is int and envelope["id"] == identifier
                and set(envelope) == {"jsonrpc", "id", "result"}, "actual original MCP response unavailable")
        return envelope["result"]

    async def mcp_head(self, space, role, external_key, *, denied=False):
        result = await self.mcp(space, role, "tools/call", {"name": "get_source_head", "arguments": {"external_source_id": external_key}})
        require(type(result) is dict and type(result.get("content")) is list
                and len(result["content"]) == 1 and type(result["content"][0]) is dict
                and result["content"][0].get("type") == "text"
                and type(result["content"][0].get("text")) is str, "actual single typed MCP tool content absent")
        value = decode(result["content"][0]["text"].encode())
        if denied:
            require(result.get("isError") is True and type(value) is dict
                    and set(value) == {"error_code", "message", "retryable"}
                    and value["error_code"] == "forbidden_space" and value["retryable"] is False
                    and type(value["message"]) is str and len(value["message"].encode()) <= 512,
                    "revoked original principal MCP call lacks exact typed denial")
        else:
            require(result.get("isError") is not True and type(value) is dict and value.get("ok") is True
                    and type(value.get("data")) is dict, "authorized original MCP head unavailable")
            value = value["data"]
        return value

    async def _fw10(self, case):
        space, label = case["space"], case["source_label"]
        source = self.published[label]
        path = f"/v1/spaces/{quote(space, safe='')}/grants/{quote(case['subject'], safe='')}"
        grant = await self.get(path, space, role="manage")
        require(grant.get("active") is True and type(grant.get("generation")) is str, "original current data grant unavailable")
        original = {"head": await self.head(space, self.inputs[label]["source"]["external_id"]),
                    "source": await self.get("/v1/sources/" + quote(source["accepted"]["source_id"], safe=""), space),
                    "doc": await self.get("/v1/docs/" + quote(source["accepted"]["doc_id"], safe=""), space),
                    "operation": await self.operation(self.inputs[label], self.accepted[label])}
        external_key = self.inputs[label]["source"]["external_id"]
        initialized = await self.mcp(space, case["role"], "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "recall-full-workload", "version": "1"}})
        require(type(initialized) is dict and initialized.get("protocolVersion") == "2025-03-26", "original MCP initialization unavailable")
        require(await self.mcp_head(space, case["role"], external_key) == original["head"], "original HTTP/MCP head differs before revocation")
        revoked = (await self.exchange("PUT", path, space, {"active": False, "scopes": grant["scopes"], "expected_generation": grant["generation"]}, role="manage"))["body"]
        require(revoked.get("active") is False, "actual grant revocation unobserved")
        mcp = await self.mcp_head(space, case["role"], external_key, denied=True)
        self.event("actual-revoked-principal-MCP", mcp)
        for kind, identifier in (("sources", source["accepted"]["source_id"]), ("docs", source["accepted"]["doc_id"])):
            deny = await self.exchange("GET", f"/v1/{kind}/{quote(identifier, safe='')}", space, role=case["role"], statuses=(403,))
            require(deny["body"].get("error_code") == "forbidden_space", "revoked principal read lacked typed denial")
        after = {"head": await self.head(space, self.inputs[label]["source"]["external_id"]),
                 "source": await self.get("/v1/sources/" + quote(source["accepted"]["source_id"], safe=""), space),
                 "doc": await self.get("/v1/docs/" + quote(source["accepted"]["doc_id"], safe=""), space),
                 "operation": await self.operation(self.inputs[label], self.accepted[label])}
        require(after == original and original["head"]["lifecycle"] == "active", "permission change altered source lifecycle/content")
        restored = (await self.exchange("PUT", path, space, {"active": True, "scopes": grant["scopes"], "expected_generation": revoked["generation"]}, role="manage"))["body"]
        restored_doc = await self.get("/v1/docs/" + quote(source["accepted"]["doc_id"], safe=""), space, role=case["role"])
        require(restored.get("active") is True and restored_doc == original["doc"], "explicit grant restoration changed original content")
        require(await self.mcp_head(space, case["role"], external_key) == original["head"], "explicit restoration changed original MCP head")
        self.event("permission-is-not-retraction", {"original": original, "revoked_grant": revoked, "restored_grant": restored, "restored_doc": restored_doc,
                    "mcp_revoked_principal": mcp})

    async def fw11(self):
        for case in self.space_cases("FW11"):
            await self._fw11(case)

    async def _fw11(self, case):
        validate_cleanup(case["residual_cleanup"], "before_binding_restore")
        allowed = case["allowed_original_failure_states"]
        require(type(allowed) is list and allowed and len(allowed) == len(set(allowed))
                and set(allowed) <= {"failed_retryable", "failed_final"}, "original failure state set differs")
        await self.quiescent()
        original_recipe = self.plan["graph_recipes"][case["recipe"]]
        rule = next(row for row in self.spy.recipe["rules"] if row["id"] == original_recipe["extract_rule_id"])
        ordinal = self.spy.rule_counts.get(rule["id"], 0) + 1
        require(ordinal in rule["hold_calls"] and rule["statuses"][ordinal - 1] == 503,
                "original retry case requires independently scheduled held 503 attempt")
        fixture = await self.held_fixture(case["recipe"])
        config_path = "/v1/spaces/" + quote(fixture.space, safe="")
        before = await self.get(config_path, fixture.space, role="manage")
        processing = clone(before["processing"])
        group = case["revoke_kind"]
        require(group in ("bindings", "connections") and processing[group][case["revoke_id"]]["revoked"] is False, "original provider permission missing")
        generation = processing.get("generation")
        require(type(generation) is int and 0 < generation < 2**64 - 1, "original processing generation unavailable")
        processing["generation"] = generation + 1
        processing[group][case["revoke_id"]]["revoked"] = True
        try:
            await self.exchange("PUT", config_path, fixture.space, {"processing": processing}, role="manage")
            current = await self.get(config_path, fixture.space, role="manage")
            require(current["processing"] == processing, "actual current provider revocation differs from original intent")
            original_count = self.spy.snapshot_calls()["physical_connections"]
        finally:
            self.release(case["recipe"])
        deadline = min(self.client.deadline, self.client.clock() + case["observe_seconds"])
        samples = []
        rule_id, original_rule_count = self.original_rule_count(case["recipe"])
        while self.client.clock() < deadline:
            samples.append(await self.operation(fixture.request, fixture.accepted, active=False))
            require(self.spy.snapshot_calls()["physical_connections"] == original_count, "revoked original retry dispatched a new physical provider request")
            await self.pause()
        require(any(row["state"] in allowed and type(row.get("error_code")) is str and row["error_code"] for row in samples),
                "actual original public revocation failure state/category unavailable")
        self.event("revoked-original-provider-attempt", {"before_configuration": before, "revoked_configuration": current,
                    "original_count": original_count, "after_count": self.spy.snapshot_calls()["physical_connections"], "original_operation_samples": samples})
        # Fence the original failed work through the public source lifecycle
        # before restoring permission. An idle spy alone does not prove draining.
        await self.retract_original(case["recipe"], case["residual_cleanup"], original_rule_count)
        # The lifecycle may advance the storage-owned content index epoch.
        # Read its actual value without adopting any changed processing authority.
        restore_basis = await self.get(config_path, fixture.space, role="manage")
        restore_processing = clone(restore_basis["processing"])
        identity = lambda value: {key: item for key, item in value.items() if key != "index_generation"}
        require(identity(restore_processing) == identity(current["processing"]),
                "processing authority changed before original permission restoration")
        current_generation = restore_processing.get("generation")
        require(type(current_generation) is int and 0 < current_generation < 2**64 - 1,
                "original restoration generation unavailable")
        restore_processing["generation"] = current_generation + 1
        restore_processing[group][case["revoke_id"]]["revoked"] = False
        # Permission restoration is a separate captured intent, never an implicit retry.
        await self.exchange("PUT", config_path, fixture.space, {"processing": restore_processing}, role="manage")
        restored = await self.get(config_path, fixture.space, role="manage")
        require(restored["processing"] == restore_processing,
                "explicit provider permission restoration differs from original intent")

        self.check()
        after = await self.operation(fixture.request, fixture.accepted, active=False)
        require(subset(after, case["residual_cleanup"]["original_terminal"])
                and self.spy.rule_counts.get(rule_id, 0) == original_rule_count,
                "restoration revived the original failed source execution")
        self.event("restored-binding-original-source-remains-terminal", {"configuration": restored,
                    "original_operation": after, "original_rule": rule_id, "original_rule_count": original_rule_count})

    async def fw12(self):
        await self.large_graph_capacity()
        await self.quiescent()
        requirements = self.plan["cases"]["FW12"]["query_requirements"]
        requirement_sha = self.plan["cases"]["FW12"]["query_requirements_sha256"]
        self.event("original-load-evidence-requirements", {"case_plan_sha256": self.plan_sha256,
                    "requirements_sha256": requirement_sha, "requirements": requirements})
        observed = await run_load(self.client, self.spy, self.plan["load_plan"], self.plan["load_plan_sha256"],
                                  evidence_requirements=requirements, evidence_sha256=requirement_sha,
                                  validate_evidence=self.query_evidence)
        self.event("actual-140-caller-load", observed)
        require(observed["protocol_verdict"] == "passed", "actual lexical load protocol incomplete")

    def capture_original_bytes(self, kind, raw, expected_sha):
        require(type(raw) is bytes and 0 < len(raw) <= 4 * 1024**2 and sha(raw) == expected_sha,
                "bounded original external bytes differ from pinned hash")
        self.event_count += 1
        self.client.capture.reserve(len(raw))
        path = self.client.capture.directory / f"external-{self.event_count:06}-{kind}.json"
        private_new(path, raw)
        self.client.capture.held -= len(raw)
        self.client.capture.written += len(raw)
        return {"file": path.name, "sha256": sha(raw), "bytes": len(raw)}

    def external(self, case):
        receipt = self.external_receipts.get(case)
        if receipt is None:
            return {"status": "unmet", "reason": "original externally owned consumer receipt absent", "receipt_imported": False}
        custody = {"owner", "case", "backend", "run_nonce", "original_sha256", "original"}
        raw_fields = {"original_raw", "expectations_raw", "expectations_sha256"}
        require(type(receipt) is dict and custody <= set(receipt) <= custody | raw_fields
                and receipt["case"] == case and receipt["backend"] == self.backend
                and receipt["run_nonce"] == self.nonce and receipt["owner"] in ("OB3", "HomeCore")
                and type(receipt["original_sha256"]) is str
                and re.fullmatch(r"[a-f0-9]{64}", receipt["original_sha256"]) is not None
                and type(receipt["original"]) is dict, "original externally owned consumer receipt absent")
        if "original_raw" not in receipt:
            return {"status": "unmet", "reason": "original externally owned receipt bytes unavailable",
                    "receipt_imported": False}
        retained = {key: clone(receipt[key]) for key in custody}
        if "original_raw" in receipt:
            retained["original_capture"] = self.capture_original_bytes("receipt", receipt["original_raw"], receipt["original_sha256"])
            require(exact_type_equal(decode(receipt["original_raw"]), receipt["original"]),
                    "parsed external receipt differs from original captured bytes")
        if "expectations_raw" in receipt:
            require("expectations_sha256" in receipt, "original independent expectation hash missing")
            retained["expectations_capture"] = self.capture_original_bytes("expectations", receipt["expectations_raw"], receipt["expectations_sha256"])
        self.event("external-consumer-receipt", retained)
        accounting = "unmet: original raw receipt and independent expectation bytes are required"
        if receipt["owner"] == "OB3":
            original = receipt["original"]
            require(original.get("receipt_version") in ("ob3-recall-outage-1", "ob3-recall-outage-2")
                    and type(original.get("run_id")) is str and original["run_id"]
                    and type(original.get("ob3_git_sha")) is str and original["ob3_git_sha"]
                    and type(original.get("recall_contract_version")) is str and original["recall_contract_version"],
                    "original frozen OB3 receipt/source/run/contract identity absent")
            require({"batch", "expected_participating_workers", "participating_workers",
                     "read_total", "workers", "por_push", "verdict"} <= set(original)
                    and type(original["read_total"]) is dict
                    and {"reads", "evidence", "fallbacks", "size_retries"} <= set(original["read_total"]),
                    "original OB3 receipt groups and short read counters absent")
            if case == "FW14":
                require(original.get("recall_endpoint_state") in ("down", "refused", "timeout", "unavailable"),
                        "original OB3 outage state absent")
            if raw_fields <= set(receipt):
                accounting = evaluate_ob3_originals(self.ob3_oracle, receipt["original_raw"], receipt["expectations_raw"])
                require(type(accounting) is dict and accounting.get("scope") == "external_receipt_consistency"
                        and accounting.get("product_acceptance") is False
                        and accounting.get("receipt_sha256") == receipt["original_sha256"]
                        and accounting.get("expectations_sha256") == receipt["expectations_sha256"]
                        and accounting.get("verdict") in ("pass", "fail", "unmet"),
                        "independent external oracle returned an unsupported scope or byte identity")
                self.event("independent-external-receipt-consistency", accounting)
        # Even a consistent v2 receipt does not prove physical retry history,
        # installed artifact custody, or the whole external consumer acceptance.
        return {"status": "unmet", "reason": "external physical traces and independent owner acceptance remain required",
                "receipt_imported": True, "consumer_accounting": accounting,
                "product_acceptance": False}

    async def fw13(self):
        case = self.plan["cases"]["FW13"]
        if case.get("queries"):
            require(len(case["queries"]) == 2 and [row["body"]["limit"] for row in case["queries"]] == [20, 10], "exact one 20-to-10 size retry required")
            first = await self.query(case["queries"][0])
            require(first.get("error_code") == "payload_too_large", "consumer retry requires actual typed size refusal")
            await self.query(case["queries"][1])
        return self.external("FW13")

    async def fw14(self):
        if self.service_control is not None:
            # The enclosing installed/container owner controls its original service.
            # No PID, shell, process launcher or stop endpoint is accepted here.
            receipt = await self.service_control.stop_original(self.backend, self.nonce)
            self.event("original-service-stop", receipt)
            row = await self.client.request("GET", "/v1/spaces/" + SPACES[0], SPACES[0], role="data")
            require(row.get("error_type") is not None or row.get("status") in (502, 503, 504), "owned Recall outage was not actually observed")
            self.event("actual-owned-service-outage", row)
        return self.external("FW14")

    async def run(self):
        failure = None
        try:
            for identifier in CASES:
                self.current_case = identifier
                if self.stopped:
                    self.results.append({"id": identifier, "status": "unexecuted", "reason": "earlier original case prerequisite or execution failed"})
                    continue
                begin = len(self.events)
                try:
                    self.check()
                    result = await getattr(self, identifier.lower())()
                    self.check()
                    row = {"id": identifier, "status": "captured", "qualified": False, **(result or {})}
                except (Exception, asyncio.CancelledError) as error:
                    row = {"id": identifier, "status": "unmet", "qualified": False, "error_type": type(error).__name__}
                    self.stopped = True
                    failure = type(error).__name__
                row["events"] = self.events[begin:]
                self.results.append(row)
                # Consumer receipts cannot authorize or stand in for dependent work.
                # FW14 is an independent final owner receipt, so still import it if
                # FW13's only unmet result is a captured external-verdict boundary.
        finally:
            try:
                await self.spy.close()
            except (Exception, asyncio.CancelledError) as error:
                failure = failure or type(error).__name__
        observation = {"format": "recall-full-workload-observation/v1", "backend": self.backend, "run_nonce": self.nonce,
                       "case_plan_sha256": self.plan_sha256, "contract_sha256": CONTRACT_SHA,
                       "cases": self.results, "public_http_calls": self.client.count,
                       "controlled_provider": self.spy.snapshot_calls(), "capture_bytes": self.client.capture.written,
                       "retained_retrieval_submission_attempts": len(self.inputs),
                       "retained_graph_submission_attempts": sum(fixture.accepted is not None for fixture in self.graphs.values()),
                       "qualified": False, "product_acceptance": False,
                       "proof_limits": PROOF_LIMITS, "error_type": failure}
        self.event("terminal-observation", observation)
        return observation
