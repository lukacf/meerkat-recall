"""Actual public Graph preparation shared by workload and installed consumers.

No direct storage writes, fabricated receipts, provider forwarding or processes.
An independently pinned recipe and a live caller-owned service are prerequisites.
"""
from __future__ import annotations

import asyncio
from urllib.parse import quote

from full_workload_transport import decode, encode, require, sha


ASSERTION_FIELDS = {"subject_text", "predicate_raw", "object_text", "object_literal",
                    "subject_entity_type", "object_entity_type", "confidence", "significance", "source_event_time"}
EXPECTED_FIELDS = ASSERTION_FIELDS | {"start", "end", "quote", "quote_hash"}
RECIPE_FIELDS = {"format", "source_request", "rendered_text", "rendered_sha256",
                 "expected_assertions", "extract_rule_id"}


def validate_recipe(recipe, expected_sha256):
    require(sha(encode(recipe)) == expected_sha256, "original independent Graph recipe hash differs")
    require(type(recipe) is dict and set(recipe) == RECIPE_FIELDS
            and recipe["format"] == "recall-public-graph-fixture/v1", "Graph recipe fields differ")
    request = recipe["source_request"]
    require(type(request) is dict and set(request) == {"source", "doc", "ingest_mode", "intake"}
            and request["ingest_mode"] == "graph", "actual typed Graph source request required")
    source, doc, intake = request["source"], request["doc"], request["intake"]
    require(all(type(row) is dict for row in (source, doc, intake)), "typed original inputs required")
    require(source.get("type") == "manual" and type(source.get("external_id")) is str and source["external_id"]
            and type(doc.get("text")) is str and doc["text"] and len(doc["text"].encode()) <= 2_000_000
            and sha(doc["text"].encode()) == source.get("hash"), "exact bounded original source bytes required")
    require(type(source.get("space_id")) is str and source["space_id"]
            and doc.get("space_id") == source["space_id"], "original named space differs")
    for key in ("security", "subjects", "subjects_state"):
        require(key in source and doc.get(key) == source[key], "explicit original envelope differs:" + key)
    require(source.get("deleted_at") is None and doc.get("deleted_at") is None
            and doc.get("source") is None, "original live manual document required")
    require(type(intake.get("revision_id")) is str and intake["revision_id"]
            and type(intake.get("idempotency_key")) is str and intake["idempotency_key"], "original intake identity missing")
    rendered = recipe["rendered_text"]
    require(type(rendered) is str and 0 < len(rendered.encode()) <= 2_000_001
            and sha(rendered.encode()) == recipe["rendered_sha256"], "independent rendered bytes differ")
    expected = recipe["expected_assertions"]
    require(type(expected) is list and 1 <= len(expected) <= 128, "nonempty bounded independent Graph expectations required")
    identities = set()
    for row in expected:
        require(type(row) is dict and set(row) == EXPECTED_FIELDS, "full assertion expectation required")
        start, end = row["start"], row["end"]
        require(type(start) is int and type(end) is int and 0 <= start < end <= len(rendered.encode()), "expected original span invalid")
        raw = rendered.encode()[start:end]
        require(raw.decode("utf-8") == row["quote"] and sha(raw) == row["quote_hash"], "independent quote/span/hash differs")
        require(encode(row) not in identities, "duplicate original assertion expectation")
        identities.add(encode(row))
    require(type(recipe["extract_rule_id"]) is str and recipe["extract_rule_id"], "original controlled extraction rule required")
    return decode(encode(recipe))


class GraphFixture:
    def __init__(self, client, spy, reviewed_recipe, recipe_sha256):
        self.client, self.spy = client, spy
        self.recipe = validate_recipe(reviewed_recipe, recipe_sha256)
        self.request = self.recipe["source_request"]
        self.space = self.request["source"]["space_id"]
        self.accepted, self.held_id = None, None
        self.original_spy_count = spy.snapshot_calls()["physical_connections"]
        rules = [rule for rule in spy.recipe["rules"]
                 if rule["id"] == self.recipe["extract_rule_id"] and rule["space"] == self.space]
        require(len(rules) == 1 and rules[0]["mode"] == "structured" and rules[0]["required_text"]
                and all(value in self.recipe["rendered_text"] for value in rules[0]["required_text"]),
                "Graph recipe lacks an exact source-selecting extraction rule")
        self.original_rule_count = spy.rule_counts.get(self.recipe["extract_rule_id"], 0)

    async def request_json(self, method, path, body=None):
        result = await self.client.request(method, path, self.space, body, role="data")
        require(result.get("framing_complete") is True and result.get("error_type") is None
                and result.get("status") in (200, 201, 202), "actual complete public Graph response unavailable")
        require(result.get("body") is not None, "actual public Graph response body absent")
        return result["body"]

    async def accept(self):
        require(self.accepted is None, "original Graph request cannot be silently resubmitted")
        # Mark original intent before the physical HTTP call, including an
        # ambiguous failure. A failed call cannot gain a new allowance here.
        self.accepted = {"unresolved_original_intent": True}
        accepted = await self.request_json("POST", "/v1/sources/ingest", self.request)
        operation = accepted.get("operation", {})
        require(operation.get("space_id") == self.space
                and operation.get("desired_revision") == self.request["intake"]["revision_id"], "accepted Graph input identity changed")
        for key in ("operation_id", "source_id", "source_generation"):
            require(type(operation.get(key)) is str and operation[key], "actual original acceptance identity missing")
        require(accepted.get("source_id") == operation["source_id"]
                and type(accepted.get("doc_id")) is str and accepted["doc_id"].startswith("doc:")
                and type(accepted.get("work_item_id")) is str and accepted["work_item_id"], "actual enqueued document/source absent")
        self.accepted = decode(encode(accepted))
        return decode(encode(accepted))

    def operation_identity(self, value):
        original = self.accepted["operation"]
        for observed, accepted in (("operation_id", "operation_id"), ("internal_source_id", "source_id"),
                                    ("generation", "source_generation")):
            require(value.get(observed) == original[accepted], "original operation/source/generation changed")
        require(value.get("desired_revision") == self.request["intake"]["revision_id"]
                and value.get("lifecycle") == "active", "original source revision is no longer active")
        require(value.get("state") in ("pending", "running", "complete", "failed_retryable", "failed_final"), "unknown original operation state")

    async def wait_complete(self):
        require(self.accepted is not None and "operation" in self.accepted, "original acceptance unresolved")
        identifier = quote(self.accepted["operation"]["operation_id"], safe="")
        while True:
            require(self.spy.failure is None, "controlled source fixture failed")
            operation = await self.request_json("GET", "/v1/operations/" + identifier)
            self.operation_identity(operation)
            require(operation["state"] != "failed_final", "original Graph publication failed terminally")
            if operation["state"] == "complete":
                require(operation.get("published_revision") == self.request["intake"]["revision_id"]
                        and operation.get("error_code") is None, "completed operation differs from original revision")
                return operation
            remaining = self.client.deadline - self.client.clock()
            require(remaining > 0, "original Graph preparation deadline expired")
            await asyncio.sleep(min(0.05, remaining))

    async def accept_and_wait(self):
        await self.accept()
        return await self.observe_completed(await self.wait_complete())

    async def hold_next_exact_source(self):
        rule_id = self.recipe["extract_rule_id"]
        rule = next(rule for rule in self.spy.recipe["rules"] if rule["id"] == rule_id)
        next_ordinal = self.spy.rule_counts.get(rule_id, 0) + 1
        require(next_ordinal in rule["hold_calls"], "next original extraction is not independently scheduled to hold")
        accepted = await self.accept()
        self.held_id = await self.spy.wait_held(rule_id)
        return {"accepted": accepted, "physical_call_id": self.held_id, "rule_id": rule_id}

    def release_original(self, held):
        require(self.held_id is not None and held.get("physical_call_id") == self.held_id
                and held.get("accepted") == self.accepted and held.get("rule_id") == self.recipe["extract_rule_id"],
                "held request is not this original accepted Graph invocation")
        self.spy.release(self.held_id)
        self.held_id = None

    def snapshot_calls(self):
        return self.spy.snapshot_calls()

    async def observe_completed(self, operation):
        self.operation_identity(operation)
        require(operation.get("state") == "complete", "Graph preparation has not actually completed")
        source_id, doc_id = self.accepted["source_id"], self.accepted["doc_id"]
        source_input = self.request["source"]
        head = await self.request_json("GET", f"/v1/spaces/{quote(self.space, safe='')}/sources/{quote(source_input['external_id'], safe='')}/head")
        self.operation_identity(head)
        require(head.get("state") == "complete" and head.get("published_revision") == self.request["intake"]["revision_id"], "original Graph head is not published")
        doc = await self.request_json("GET", "/v1/docs/" + quote(doc_id, safe=""))
        source = await self.request_json("GET", "/v1/sources/" + quote(source_id, safe=""))
        pipeline = await self.request_json("GET", "/v1/docs/" + quote(doc_id, safe="") + "/pipeline-status")
        require(doc.get("id") == doc_id and doc.get("source") == source_id
                and doc.get("text") == self.recipe["rendered_text"]
                and sha(doc["text"].encode()) == self.recipe["rendered_sha256"], "actual normalized document differs")
        require(source.get("id") == source_id and source.get("external_id") == source_input["external_id"]
                and source.get("hash") == source_input["hash"]
                and source.get("current_ingest_mode") == "graph" and source.get("lifecycle_status") == "graph_ready", "actual source has not reached GraphReady")
        for row in (doc, source):
            require(row.get("space_id") == self.space and row.get("deleted_at") is None, "actual source/document envelope invalid")
            for key in ("security", "subjects", "subjects_state"):
                require(row.get(key) == source_input[key], "actual original source policy changed:" + key)
        revision = pipeline.get("revision")
        require(pipeline.get("doc_id") == doc_id and type(revision) is str and revision.startswith("doc_revision:"), "actual immutable revision mapping absent")
        immutable_revision = await self.request_json("GET", "/v1/docs/" + quote(doc_id, safe="")
                                                    + "/revisions/" + quote(revision, safe=""))
        require(immutable_revision.get("id") == revision and immutable_revision.get("doc") == doc_id
                and immutable_revision.get("space_id") == self.space and immutable_revision.get("deleted_at") is None
                and immutable_revision.get("status") == "active"
                and immutable_revision.get("text") == self.recipe["rendered_text"]
                and immutable_revision.get("text_hash") == self.recipe["rendered_sha256"],
                "actual full immutable rendered revision differs from original expectation")
        for key in ("security", "subjects", "subjects_state"):
            require(immutable_revision.get(key) == source_input[key], "immutable revision policy differs:" + key)
        provenance = immutable_revision.get("provenance", {})
        require(provenance.get("sources") == [source_id]
                and any(step.get("step") == "source_content" and step.get("input") == source_id
                        and step.get("output") == source_input["hash"] for step in provenance.get("chain", [])),
                "immutable rendered revision lacks exact original raw-source binding")
        page = await self.request_json("POST", "/v1/assertions/search", {"source_id": source_id, "limit": 128})
        assertions = page.get("results")
        expected = self.recipe["expected_assertions"]
        require(type(assertions) is list and len(assertions) == len(expected) and page.get("has_more") is False
                and page.get("cursor") is None and page.get("total") == len(expected), "exact complete source assertion set unavailable")
        actual, entity_expectations = [], {}
        for assertion in assertions:
            require(assertion.get("space_id") == self.space and assertion.get("security") == source_input["security"]
                    and assertion.get("deleted_at") is None and assertion.get("resolution_status") == "resolved"
                    and assertion.get("provenance", {}).get("sources") == [source_id], "assertion support/security/resolution differs")
            evidence = assertion.get("evidence", {})
            start, end = evidence.get("start"), evidence.get("end")
            require(evidence.get("revision") == revision and type(start) is int and type(end) is int
                    and 0 <= start < end <= len(self.recipe["rendered_text"].encode()), "assertion cites a different immutable revision")
            raw = self.recipe["rendered_text"].encode()[start:end]
            require(evidence.get("quote_hash") == sha(raw), "actual evidence digest differs")
            actual.append({**{key: assertion.get(key) for key in ASSERTION_FIELDS}, "start": start, "end": end,
                           "quote": raw.decode("utf-8"), "quote_hash": evidence["quote_hash"]})
            require(type(assertion.get("subject_entity")) is str and assertion["subject_entity"].startswith("entity:"), "actual promoted subject entity missing")
            for role in ("subject", "object"):
                identifier = assertion.get(role + "_entity")
                if identifier is not None:
                    expectation = (assertion.get(role + "_text"), assertion.get(role + "_entity_type"))
                    require(identifier not in entity_expectations or entity_expectations[identifier] == expectation,
                            "distinct original mentions collapsed into one incompatible entity")
                    entity_expectations[identifier] = expectation
            if assertion.get("object_text") is not None:
                require(type(assertion.get("object_entity")) is str and assertion["object_entity"].startswith("entity:"), "actual promoted object entity missing")
        require(sorted(map(encode, actual)) == sorted(map(encode, expected)), "actual Graph semantics differ from independent expectations")
        entities = []
        for identifier, (name, entity_type) in sorted(entity_expectations.items()):
            entity = await self.request_json("GET", "/v1/entities/" + quote(identifier, safe=""))
            require(entity.get("id") == identifier and entity.get("space_id") == self.space
                    and entity.get("display_name") == name and entity.get("entity_type") == entity_type
                    and entity.get("security") == source_input["security"] and entity.get("deleted_at") is None
                    and source_id in entity.get("provenance", {}).get("sources", []),
                    "actual promoted entity lacks the original content/security/source contribution")
            entities.append(entity)
        calls = self.spy.snapshot_calls()
        require(calls["error_type"] is None and any(row.get("rule_id") == self.recipe["extract_rule_id"]
                    and row.get("rule_ordinal", 0) > self.original_rule_count
                    and row.get("response_complete") is True and row.get("status") == 200
                    for row in calls["complete_calls"]),
                "Graph completion did not observe this original extraction rule complete")
        return {"accepted": decode(encode(self.accepted)), "operation": operation, "head": head,
                "doc": doc, "source": source, "pipeline": pipeline, "assertions": assertions,
                "immutable_revision": immutable_revision,
                "entities": entities,
                "provider_observation": self.snapshot_calls(), "model_quality_qualified": False,
                "private_manifest": "unmet: not exposed by these public reads",
                "physical_vector_inventory": "unmet: not exposed by these public reads"}
