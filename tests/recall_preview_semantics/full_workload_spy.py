"""Bounded controlled-provider fixture; no upstream client or provider forwarding.

Independent recipes supply responses and semantic support expectations. This
qualifies protocol/ordering only, never model quality or provider billing.
"""
from __future__ import annotations

import asyncio
import base64
import re
import time

from full_workload_transport import Unmet, decode, encode, private_new, require, sha

RULE_FIELDS = {"id", "space", "model", "path", "credential_sha256", "required_text", "forbidden_text",
               "maximum_calls", "hold_calls", "statuses", "mode", "output"}
SEMANTIC_FIELDS = {"subject", "predicate", "object", "literal", "subject_type", "object_type",
                   "subject_entity", "object_entity", "event_time", "event_precision", "event_title",
                   "event_lineage", "confidence", "significance"}
COORDINATE_FIELDS = {"revision", "node", "quote_hash", "start", "end"}


def user_texts(body, path):
    """Only actual user content can select a source/support fixture response."""
    values = body.get("messages") if path == "/v1/messages" else body.get("input")
    if isinstance(values, str):
        return [values]
    require(type(values) is list and len(values) <= 1024, "bounded provider message list required")
    result = []
    for message in values:
        if type(message) is not dict or message.get("role") != "user":
            continue
        content = message.get("content")
        if isinstance(content, str):
            result.append(content)
        elif type(content) is list:
            require(len(content) <= 1024, "bounded content block list required")
            for block in content:
                if type(block) is dict and block.get("type") in ("text", "input_text"):
                    require(type(block.get("text")) is str, "user text block must contain text")
                    result.append(block["text"])
    return result


def support_response(texts, expectations):
    """Match the complete independently frozen semantic tuple and exact quote."""
    batches = []
    for text in texts:
        try:
            value = decode(text.encode())
        except Unmet:
            continue
        if type(value) is list and value and all(type(row) is dict and set(row) == {"index", "claim", "quote"} for row in value):
            batches.append(value)
    require(len(batches) == 1 and len(batches[0]) <= 32, "one bounded actual support candidate batch required")
    seen, decisions = set(), []
    for row in batches[0]:
        index, claim, quote = row["index"], row["claim"], row["quote"]
        require(type(index) is int and 0 <= index <= 100000 and index not in seen,
                "original unique support index required")
        seen.add(index)
        supported = False
        if type(claim) is dict and set(claim) == SEMANTIC_FIELDS | COORDINATE_FIELDS and type(quote) is str:
            start, end = claim["start"], claim["end"]
            coordinates = (type(start) is int and type(end) is int and 0 <= start < end <= 2**32 - 1
                           and end - start == len(quote.encode()) and claim["quote_hash"] == sha(quote.encode())
                           and type(claim["revision"]) is str and re.fullmatch(r"doc_revision:[0-9a-f-]{36}", claim["revision"])
                           and (claim["node"] is None or type(claim["node"]) is str
                                and re.fullmatch(r"doc_node:[0-9a-f-]{36}", claim["node"])))
            semantic = {key: claim[key] for key in SEMANTIC_FIELDS}
            supported = bool(coordinates and any(quote == expected["quote"] and encode(semantic) == encode(expected["semantic"])
                                                for expected in expectations))
        decisions.append({"index": index, "supported": supported})
    return {"decisions": decisions}


def provider_response(rule, body):
    mode, model = rule["mode"], body["model"]
    if mode == "embedding":
        inputs = body.get("input")
        require(type(inputs) is list and 1 <= len(inputs) <= 128 and all(type(value) is str and value for value in inputs),
                "bounded explicit embedding text batch required")
        dimensions = rule["output"]["dimensions"]
        require(body.get("dimensions", dimensions) == dimensions and body.get("encoding_format", "float") == "float",
                "reviewed embedding dimensions/format changed")
        vector = [1.0] + [0.0] * (dimensions - 1)
        return "application/json", encode({"object": "list", "model": model,
            "data": [{"index": index, "object": "embedding", "embedding": vector} for index in range(len(inputs))],
            "usage": {"prompt_tokens": 1, "total_tokens": 1}})
    texts = user_texts(body, rule["path"])
    output = support_response(texts, rule["output"]) if mode == "support" else rule["output"]
    text = encode(output).decode()
    require(body.get("stream") is True, "controlled generative fixture requires the actual streaming protocol")
    if rule["path"] == "/v1/messages":
        events = [
            {"type": "message_start", "message": {"id": "controlled-message", "role": "assistant", "model": model,
                "content": [], "usage": {"input_tokens": 1, "output_tokens": 0}}},
            {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
            {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text}},
            {"type": "content_block_stop", "index": 0},
            {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 1}},
            {"type": "message_stop"}]
    else:
        events = [{"type": "response.output_text.delta", "delta": text},
            {"type": "response.completed", "response": {"id": "controlled-response", "status": "completed",
                "output": [{"id": "controlled-message", "type": "message", "role": "assistant", "status": "completed",
                    "content": [{"type": "output_text", "text": text, "annotations": []}]}],
                "usage": {"input_tokens": 1, "output_tokens": 1}}}]
    return "text/event-stream", b"".join(b"data: " + encode(event) + b"\n\n" for event in events)


def validate_recipe(recipe, expected_sha256):
    require(sha(encode(recipe)) == expected_sha256, "independent original spy recipe hash differs")
    require(type(recipe) is dict and set(recipe) == {"format", "rules"}
            and recipe["format"] == "recall-controlled-graph-spy/v1", "unsupported controlled spy recipe")
    rules = recipe["rules"]
    require(type(rules) is list and 1 <= len(rules) <= 128, "bounded explicit spy rules required")
    ids = set()
    for rule in rules:
        require(type(rule) is dict and set(rule) == RULE_FIELDS, "controlled rule fields differ")
        for key in ("id", "space", "model"):
            require(type(rule[key]) is str and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", rule[key]), "explicit controlled rule identity required")
        require(rule["id"] not in ids, "duplicate controlled rule identity")
        ids.add(rule["id"])
        require(type(rule["credential_sha256"]) is str and re.fullmatch(r"[a-f0-9]{64}", rule["credential_sha256"]), "explicit synthetic credential identity required")
        require(type(rule["maximum_calls"]) is int and 1 <= rule["maximum_calls"] <= 4096, "rule call cap differs")
        require(type(rule["hold_calls"]) is list and len(rule["hold_calls"]) == len(set(rule["hold_calls"]))
                and all(type(value) is int and 1 <= value <= rule["maximum_calls"] for value in rule["hold_calls"]), "invalid original held call selection")
        require(type(rule["statuses"]) is list and len(rule["statuses"]) == rule["maximum_calls"]
                and all(type(value) is int and value in (200, 400, 503) for value in rule["statuses"]), "original response schedule differs")
        for key in ("required_text", "forbidden_text"):
            require(type(rule[key]) is list and len(rule[key]) <= 32
                    and all(type(value) is str and 0 < len(value.encode()) <= 2_000_000 for value in rule[key]), "bounded source selectors required")
        if rule["mode"] == "embedding":
            require(rule["path"] == "/v1/embeddings" and type(rule["output"]) is dict
                    and set(rule["output"]) == {"dimensions"} and type(rule["output"]["dimensions"]) is int
                    and rule["output"]["dimensions"] in (1536, 3072)
                    and rule["required_text"] == rule["forbidden_text"] == [], "embedding fixture contract differs")
        else:
            require(rule["path"] in ("/v1/responses", "/v1/messages") and rule["mode"] in ("structured", "support"), "unexpected controlled provider protocol")
            if rule["mode"] == "support":
                require(type(rule["output"]) is list and 1 <= len(rule["output"]) <= 128, "independent support expectations required")
                for row in rule["output"]:
                    require(type(row) is dict and set(row) == {"quote", "semantic"}
                            and type(row["quote"]) is str and 0 < len(row["quote"].encode()) <= 2_000_000
                            and type(row["semantic"]) is dict and set(row["semantic"]) == SEMANTIC_FIELDS,
                            "full independently authored support tuple required")
            else:
                require(type(rule["output"]) is dict and rule["required_text"], "structured output needs a pinned source selector")
    embedding_keys = [(rule["credential_sha256"], rule["model"], rule["path"])
                      for rule in rules if rule["mode"] == "embedding"]
    require(len(embedding_keys) == len(set(embedding_keys)), "ambiguous controlled embedding rule authority")
    authorities = {}
    for rule in rules:
        key = (rule["credential_sha256"], rule["model"], rule["path"])
        authorities.setdefault(key, set()).add(rule["space"])
    require(all(len(spaces) == 1 for spaces in authorities.values()), "ambiguous controlled provider space authority")
    require(len(encode(recipe)) <= 8 * 1024**2, "spy recipe exceeds original input bound")
    return decode(encode(recipe))


async def request_frame(reader, observed, reserve, limit):
    header_bytes = 0
    async def line():
        nonlocal header_bytes
        value = await reader.readline()
        header_bytes += len(value)
        require(value.endswith(b"\r\n") and len(value) <= 8192 and header_bytes <= 65536, "bounded complete request line required")
        return value
    first = await line()
    require(re.fullmatch(rb"POST /v1/(embeddings|responses|messages) HTTP/1\.1\r\n", first), "unexpected controlled request target")
    observed["path"] = first.split(b" ")[1].decode()
    headers = {}
    for _ in range(128):
        value = await line()
        if value == b"\r\n":
            break
        require(b":" in value and not value.startswith((b" ", b"\t")), "invalid request field")
        key, value = value[:-2].split(b":", 1)
        require(re.fullmatch(rb"[A-Za-z0-9-]+", key), "invalid request field name")
        key, value = key.decode().lower(), value.strip(b" \t").decode("ascii")
        require(key not in headers and all(32 <= ord(char) < 127 for char in value), "duplicate or invalid request field")
        headers[key] = value
    else:
        raise Unmet("request header count exceeded")
    require("transfer-encoding" not in headers and headers.get("content-type", "").split(";", 1)[0].lower() == "application/json",
            "controlled request requires unambiguous JSON framing")
    length = headers.get("content-length", "")
    require(length.isascii() and length.isdecimal() and len(length) <= 8 and 0 < int(length) <= limit, "bounded explicit request length required")
    reserve(int(length))
    raw = bytearray()
    while len(raw) < int(length):
        block = await reader.read(min(65536, int(length) - len(raw)))
        require(bool(block), "truncated controlled request body")
        raw.extend(block)
        observed["raw_request"] = bytes(raw)
    observed["space"] = headers.get("x-space-id")
    credential = headers.get("x-api-key") if observed["path"] == "/v1/messages" else headers.get("authorization", "").removeprefix("Bearer ")
    require(type(credential) is str and credential and (observed["path"] == "/v1/messages" or headers.get("authorization", "").startswith("Bearer ")),
            "explicit synthetic provider credential required")
    observed["credential_sha256"] = sha(credential.encode())
    observed["request_sha256"] = sha(raw)
    body = decode(bytes(raw))
    require(type(body) is dict and type(body.get("model")) is str, "actual provider model required")
    observed["model"] = body["model"]
    observed["request_complete"] = True
    return body


class ControlledSpy:
    # Specializations retain the original parser, durable intent, caps and
    # cleanup; both defaults preserve the full-workload fixture contract.
    validate_recipe = staticmethod(validate_recipe)
    provider_response = staticmethod(provider_response)

    def __init__(self, recipe, recipe_sha256, capture, limits, deadline, clock=time.monotonic):
        self.recipe = self.validate_recipe(recipe, recipe_sha256)
        self.capture, self.limits, self.deadline, self.clock = capture, limits, deadline, clock
        for key, maximum in (("owned_spy_dispatches_per_backend", 4096), ("owned_spy_request_bytes", 8 * 1024**2),
                             ("owned_spy_response_bytes", 8 * 1024**2), ("owned_spy_concurrency", 160), ("held_response_seconds", 120)):
            require(type(limits.get(key)) is int and 0 < limits[key] <= maximum, "controlled spy original cap differs:" + key)
        require(deadline > clock(), "original spy deadline expired")
        self.count, self.active = 0, 0
        self.calls, self.rule_counts, self.held = [], {}, {}
        self.held_rules, self.tasks = {}, set()
        self.changed = asyncio.Condition()
        self.server = None
        self.failure = None

    async def start(self, port):
        require(self.server is None and type(port) is int and 0 < port <= 65535, "one explicit owned loopback listener required")
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", port, limit=65536)
        return self

    def select(self, observed, body):
        # Both governed generation and embeddings omit the space header.
        # Bind space before source-text selection; text cannot resolve scope.
        candidates = [rule for rule in self.recipe["rules"] if all(rule[key] == observed[key]
                      for key in ("credential_sha256", "model", "path"))]
        spaces = {rule["space"] for rule in candidates}
        require(len(spaces) == 1, "provider credential/model/path lacks one declared space")
        if observed["path"] == "/v1/embeddings":
            require(len(candidates) == 1, "embedding credential/model/path lacks one declared rule")
        space = next(iter(spaces))
        wire_space = observed.get("wire_space", observed.get("space"))
        require(wire_space in (None, space) and observed.get("space") in (None, space),
                "provider space header conflicts with credential authority")
        # A specialized spy may already have retained the original absent
        # header before calling this method; never relabel it as wire data.
        observed.setdefault("wire_space", wire_space)
        observed.setdefault("space_authority", "declared unique synthetic credential hash, model and path")
        observed["space"] = space
        text = "\n".join(user_texts(body, observed["path"])) if observed["path"] != "/v1/embeddings" else ""
        rules = [rule for rule in self.recipe["rules"]
                 if all(rule[key] == observed[key] for key in ("space", "model", "path", "credential_sha256"))
                 and all(value in text for value in rule["required_text"])
                 and all(value not in text for value in rule["forbidden_text"])]
        require(len(rules) == 1, "request does not match exactly one independently frozen rule")
        rule = rules[0]
        count = self.rule_counts.get(rule["id"], 0) + 1
        self.rule_counts[rule["id"]] = count
        require(count <= rule["maximum_calls"], "original per-rule physical call cap exhausted")
        return rule, count

    def original_intent(self, identifier, request, observed):
        # Intent and complete original body are durable before a held response.
        raw = encode({"format": "recall-controlled-spy-intent/v1", **observed,
                      "request_base64": base64.b64encode(request).decode()})
        self.capture.reserve(len(raw))
        private_new(self.capture.directory / f"{identifier:06}.intent.json", raw)
        self.capture.held -= len(raw)
        self.capture.written += len(raw)

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        self.count += 1
        identifier = 100000 + self.count
        observed = {"id": identifier, "started_ns": time.monotonic_ns(), "request_complete": False,
                    "response_complete": False, "status": None, "raw_body": b"", "raw_request": b""}
        reservation, admitted = 0, False
        def reserve(count):
            nonlocal reservation
            self.capture.reserve(count)
            reservation += count
        try:
            reserve(65536)
            require(self.failure is None, "controlled spy already failed; original run cannot resume")
            require(self.count <= self.limits["owned_spy_dispatches_per_backend"] and self.active < self.limits["owned_spy_concurrency"],
                    "original physical spy count or concurrency exhausted")
            self.active += 1
            admitted = True
            async with asyncio.timeout(min(self.limits["held_response_seconds"], self.deadline - self.clock())):
                require(self.clock() < self.deadline, "original controlled spy deadline expired")
                body = await request_frame(reader, observed, reserve, self.limits["owned_spy_request_bytes"])
                rule, ordinal = self.select(observed, body)
                status = rule["statuses"][ordinal - 1]
                if status == 200:
                    content_type, response = self.provider_response(rule, body)
                else:
                    content_type, response = "application/json", encode({"error": {"type": "controlled_fixture_failure", "message": "Independently scheduled fixture refusal"}})
                require(len(response) <= self.limits["owned_spy_response_bytes"], "controlled response exceeds original bound")
                reserve(len(response))
                observed.update(rule_id=rule["id"], rule_ordinal=ordinal, status=status, content_type=content_type)
                request = observed["raw_request"]
                intent = {key: value for key, value in observed.items() if key not in ("raw_request", "raw_body")}
                self.original_intent(identifier, request, intent)
                if ordinal in rule["hold_calls"]:
                    event = self.held[identifier] = asyncio.Event()
                    self.held_rules[identifier] = rule["id"]
                    observed["held_ns"] = time.monotonic_ns()
                    async with self.changed:
                        self.changed.notify_all()
                    await event.wait()
                    observed["released_ns"] = time.monotonic_ns()
                require(self.clock() < self.deadline, "original deadline expired before controlled response")
                reason = {200: "OK", 400: "Bad Request", 503: "Service Unavailable"}[status]
                wire = (f"HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {len(response)}\r\nConnection: close\r\n\r\n").encode() + response
                writer.write(wire)
                observed["raw_body"] = response
                await writer.drain()
                observed["response_complete"] = True
        except (Exception, asyncio.CancelledError) as error:
            self.failure = type(error).__name__
            observed["error_type"] = self.failure
        finally:
            self.held.pop(identifier, None)
            self.held_rules.pop(identifier, None)
            if admitted:
                self.active -= 1
            writer.close()
            try:
                async with asyncio.timeout(0.1):
                    await writer.wait_closed()
            except (Exception, asyncio.CancelledError):
                observed["connection_cleanup_failed"] = True
            observed["ended_ns"] = time.monotonic_ns()
            request = observed.pop("raw_request")
            if reservation:
                try:
                    self.capture.save(identifier, request, observed, reservation)
                except Exception as error:
                    self.failure = type(error).__name__
                    observed["capture_error_type"] = self.failure
            # Keep the terminal in-memory verdict serializable even when the
            # first capture reservation itself was refused.
            uncaptured = observed.pop("raw_body", b"")
            if "response_body_sha256" not in observed:
                observed["response_body_sha256"] = sha(uncaptured)
                observed["response_body_bytes"] = len(uncaptured)
            self.calls.append(observed)
            if self.failure and self.server is not None:
                self.server.close()
            async with self.changed:
                self.changed.notify_all()
            self.tasks.discard(task)

    async def wait_held(self, rule_id):
        require(any(rule["id"] == rule_id and rule["hold_calls"] for rule in self.recipe["rules"]), "rule has no original held call")
        async with asyncio.timeout(min(self.limits["held_response_seconds"], self.deadline - self.clock())):
            async with self.changed:
                while True:
                    require(self.failure is None, "controlled spy failed before expected held call")
                    # The intent is durable; active held IDs identify physical
                    # calls only. Their association to a source comes from the
                    # independently frozen unique rule, never a guessed timing.
                    for identifier, original_rule in self.held_rules.items():
                        if original_rule == rule_id:
                            return identifier
                    await self.changed.wait()

    def release(self, identifier):
        require(identifier in self.held and not self.held[identifier].is_set(), "original held physical call is not pending")
        self.held[identifier].set()

    def snapshot_calls(self):
        return {"physical_connections": self.count, "active": self.active, "held_ids": sorted(self.held),
                "complete_calls": decode(encode(self.calls)), "error_type": self.failure,
                "model_quality_qualified": False}

    async def close(self):
        # Release only fixture-owned waits. This does not mark any product work
        # complete and is not a retry or permission restoration.
        for event in self.held.values():
            event.set()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        pending = self.tasks - {asyncio.current_task()}
        if pending:
            _, unfinished = await asyncio.wait(pending, timeout=max(0.0, min(5.0, self.deadline - self.clock())))
            for task in unfinished:
                task.cancel()
            if unfinished:
                await asyncio.gather(*unfinished, return_exceptions=True)
