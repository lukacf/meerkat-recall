"""Independent static review of the exact Runtime16 pristine schema proposal.

Reads pinned source and original JSON only. Never imports a product, attribution
helper, database driver or oracle, and never installs an allowed profile.
"""
from __future__ import annotations

import ast
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import subprocess

SOURCE = "a7e46c17fad0be72b2d483afafe8d1675356d65e"
PROFILE_SHA = "fb59d6f6182f87617f883839de4cbc6c522893230206be00ab3cde995469172b"
MAP_SHA = "35cc5b1468fef4146bbd863fdab2e1473f7d71203cc281138a22c185cb5ea500"
STAGES = {"fresh-migrations-001-through-043": 1186, "standard-restore-refreshed": 1188}
CLAUSES = {"TYPE", "FLEXIBLE", "DEFAULT", "VALUE", "ASSERT", "PERMISSIONS",
           "READONLY", "REFERENCE", "COMMENT"}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def check(condition, message):
    if not condition:
        raise ValueError(message)


def token_values(text):
    """Scan this pinned SQL subset without folding any identifier's case."""
    values, pos = [], 0
    while pos < len(text):
        char = text[pos]
        if char.isspace():
            pos += 1
            continue
        if text.startswith(("--", "//"), pos):
            end = text.find("\n", pos)
            pos = len(text) if end == -1 else end + 1
            continue
        if text.startswith("/*", pos):
            end = text.find("*/", pos + 2)
            check(end != -1, "unclosed comment")
            pos = end + 2
            continue
        if char in "'\"`/":
            end = pos + 1
            while end < len(text):
                if text[end] == "\\":
                    end += 2
                elif text[end] == char:
                    break
                else:
                    end += 1
            check(end < len(text), "unclosed quoted token")
            spelling = text[pos:end + 1]
            if char in "'\"":
                value = json.dumps(ast.literal_eval(spelling), ensure_ascii=False)
            elif char == "`":
                check(re.fullmatch(r"`[A-Za-z_][A-Za-z_0-9]*`", spelling), "unsupported quoted identifier")
                value = spelling[1:-1]
            else:
                value = spelling
            values.append(value)
            pos = end + 1
            continue
        match = re.match(r"\$?[A-Za-z_][A-Za-z_0-9]*|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?[fd]?|::|!=|>=|<=|==|&&|\|\|", text[pos:])
        value = match.group() if match else char
        pos += len(value)
        if re.fullmatch(r"\d+\.\d+(?:[eE][+-]?\d+)?f?|\d+f", value):
            value = str(Decimal(value.removesuffix("f")).normalize()) + "f"
        values.append(value)
    return values


def definition_tokens(text):
    values = token_values(text)
    check(values[:1] == ["DEFINE"], "not a source definition")
    if values[-1:] == [";"]:
        values.pop()
    if values[2:5] == ["IF", "NOT", "EXISTS"]:
        del values[2:5]
    elif values[2:3] == ["OVERWRITE"]:
        del values[2]
    if values[1] in ("FIELD", "EVENT", "INDEX"):
        on = values.index("ON")
        if values[on + 1] == "TABLE":
            del values[on + 1]
    result, pos = [], 0
    while pos < len(values):
        if values[pos:pos + 3] == ["IS", "NOT", "NONE"]:
            result.extend(["!=", "NONE"])
            pos += 3
        elif values[pos:pos + 2] == ["IS", "NONE"]:
            result.extend(["=", "NONE"])
            pos += 2
        else:
            result.append(values[pos])
            pos += 1
    return result


def field_clauses(text):
    tokens = definition_tokens(text)
    result, current, stack = {}, None, []
    for value in tokens[tokens.index("ON") + 2:]:
        if not stack and value in CLAUSES:
            check(value not in result, "duplicate field clause")
            current = value
            result[current] = []
        else:
            check(current is not None, "unknown field clause")
            result[current].append(value)
            if value in ("(", "[", "{"):
                stack.append(value)
            elif value in (")", "]", "}"):
                check(stack and stack.pop() == {")": "(", "]": "[", "}": "{"}[value], "unbalanced clause")
    check(not stack, "unclosed clause")
    result.setdefault("PERMISSIONS", ["FULL"])
    return result


def block_tokens(text):
    tokens = definition_tokens(text)
    pos = 0
    while pos < len(tokens):
        if tokens[pos:pos + 2] == ["NOT", "IN"]:
            tokens[pos:pos + 2] = ["NOTINSIDE"]
        elif tokens[pos] == "IN":
            tokens[pos] = "INSIDE"
        pos += 1
    # Only the concrete pinned single untyped closure parameter gets `any`.
    for pos in reversed(range(len(tokens) - 2)):
        if tokens[pos] == "|" and re.fullmatch(r"\$\w+", tokens[pos + 1]) and tokens[pos + 2] == "|":
            tokens[pos + 2:pos + 2] = [":", "any"]
    # The engine's single-value function-call block omits its one terminator.
    for pos in reversed(range(len(tokens) - 1)):
        if tokens[pos:pos + 2] != ["{", "fn"]:
            continue
        depth, semis, end = 1, [], pos + 1
        while depth and end < len(tokens):
            if tokens[end] == "{":
                depth += 1
            elif tokens[end] == "}":
                depth -= 1
            elif tokens[end] == ";" and depth == 1:
                semis.append(end)
            end += 1
        if semis == [end - 2] and tokens[end - 3] == ")":
            del tokens[end - 2]
    # Only parentheses on a complete LET SELECT RHS are formatting here.
    for pos in reversed(range(2, len(tokens) - 1)):
        if tokens[pos - 1:pos + 2] != ["=", "(", "SELECT"]:
            continue
        depth, end = 1, pos + 1
        while depth and end < len(tokens):
            depth += (tokens[end] == "(") - (tokens[end] == ")")
            end += 1
        check(depth == 0, "unclosed SELECT")
        if tokens[end:end + 1] == [";"]:
            del tokens[end - 1]
            del tokens[pos]
    return tokens


def expected_field(record, defines):
    source = field_clauses(defines[-1]["statement"])
    if record["origin_category"] != "engine-array-subfield":
        return source
    kind = source["TYPE"]
    if kind[:2] == ["option", "<"]:
        kind = kind[2:-1]
    check(kind[:2] in (["array", "<"], ["set", "<"]), "unknown array origin")
    expected = {"TYPE": kind[2:-1], "PERMISSIONS": ["FULL"]}
    for original in defines:
        clauses = field_clauses(original["statement"])
        kind = clauses["TYPE"]
        if any(kind[i:i + 2] in (["array", "<"], ["set", "<"]) for i in range(len(kind) - 1)):
            if "FLEXIBLE" in clauses:
                expected["FLEXIBLE"] = []
            break
    return expected


def compare_body(record):
    defines = [x for x in record["source_history"] if x["operation"] != "REMOVE"]
    check(defines, "missing source history")
    original, actual = defines[-1]["statement"], record["statement"]
    category, origin = record["category"], record["origin_category"]
    if category == "fields":
        if origin == "explicit-migration-history":
            source_header, actual_header = definition_tokens(original), definition_tokens(actual)
            check(source_header[:source_header.index("ON") + 2]
                  == actual_header[:actual_header.index("ON") + 2], "field identity differs")
        if origin == "engine-relation-endpoint":
            parent = definition_tokens(original)
            endpoint = "IN" if record["name"] == "in" else "OUT"
            expected = {"TYPE": ["record", "<", parent[parent.index(endpoint) + 1], ">"], "PERMISSIONS": ["FULL"]}
        else:
            expected = expected_field(record, defines)
        check(expected == field_clauses(actual), "source field body differs")
    elif category == "tables":
        expected = (["DEFINE", "TABLE", record["name"]]
                    if origin in ("engine-implicit-table", "engine-recreated-table")
                    else definition_tokens(original))
        if "TYPE" not in expected:
            expected[3:3] = ["TYPE", "NORMAL" if "SCHEMAFULL" in expected else "ANY"]
        if not {"SCHEMAFULL", "SCHEMALESS"}.intersection(expected):
            expected.append("SCHEMALESS")
        if "PERMISSIONS" not in expected:
            expected.extend(["PERMISSIONS", "NONE"])
        check(expected == definition_tokens(actual), "source table body differs")
    elif category == "indexes":
        expected = ["FIELDS" if x == "COLUMNS" else x for x in definition_tokens(original)]
        if expected[-1:] == ["BM25"]:
            expected.extend(["(", "1.2f", ",", "0.75f", ")"])
            for key in ("DOC_IDS_ORDER", "DOC_LENGTHS_ORDER", "POSTINGS_ORDER", "TERMS_ORDER",
                        "DOC_IDS_CACHE", "DOC_LENGTHS_CACHE", "POSTINGS_CACHE", "TERMS_CACHE"):
                expected.extend([key, "100"])
        check(expected == definition_tokens(actual), "source index body differs")
    elif category in ("events", "functions"):
        if origin == "standard-refresh-template":
            original = original[original.index("DEFINE"):]
        expected = block_tokens(original)
        if category == "functions" and expected[-2:] != ["PERMISSIONS", "FULL"]:
            expected.extend(["PERMISSIONS", "FULL"])
        check(expected == block_tokens(actual), "source event/function body differs")
    elif category == "analyzers":
        expected = definition_tokens(original)
        check(expected[:3] == ["DEFINE", "ANALYZER", "simple"], "unexpected analyzer")
        check(expected[:3] + [x.upper() for x in expected[3:]] == definition_tokens(actual), "source analyzer body differs")
    else:
        raise ValueError("unknown schema category")


def review(profile_path, map_path, repository):
    profile_raw, map_raw = Path(profile_path).read_bytes(), Path(map_path).read_bytes()
    check(digest(profile_raw) == PROFILE_SHA and digest(map_raw) == MAP_SHA, "original proposal bytes differ")
    profile, attribution = json.loads(profile_raw), json.loads(map_raw)
    check(profile["source_commit"] == attribution["source_commit"] == SOURCE, "original source differs")
    check({x["stage"] for x in profile["stages"]} == set(STAGES), "stage inventory differs")
    source = {}
    for path, expected in attribution["source_files"].items():
        raw = subprocess.check_output(["git", "show", SOURCE + ":" + path], cwd=repository)
        check(digest(raw) == expected["sha256"] and len(raw) == expected["bytes"], "original source file differs")
        source[path] = raw.decode()
    check(len(source) == 73, "source inventory differs")
    reviewed = []
    for stage in attribution["stages"]:
        captured = next(x for x in profile["stages"] if x["stage"] == stage["stage"])
        exports = {x["ordinal"]: x for x in captured["export_schema_statements"]}
        originals = {(x["table"], x["category"], x["name"]): x for x in captured["definitions"]}
        seen = set()
        check(len(originals) == len(exports) == STAGES[stage["stage"]], "complete original inventory differs")
        for record in stage["definitions"]:
            key = record["table"], record["category"], record["name"]
            check(key not in seen and key in originals, "duplicate/substituted identity")
            seen.add(key)
            check(record["statement"] == originals[key]["statement"] and digest(record["statement"].encode()) == record["sha256"], "original definition changed")
            exported = exports[record["export_ordinal"]]
            check(digest(exported["statement"].encode()) == exported["sha256"] == record["export_sha256"], "original export changed")
            body = re.sub(r"\A(?:\s|--[^\n]*(?:\n|$))*", "", exported["statement"]).rstrip().removesuffix(";")
            check(body == record["statement"], "INFO/export exact body mismatch")
            for origin in record["source_history"]:
                check(digest(origin["statement"].encode()) == origin["sha256"], "source fragment digest differs")
                if origin["operation"] != "REFRESH_TEMPLATE":
                    lines = source[origin["path"]].splitlines(keepends=True)
                    fragment = "".join(lines[origin["line"] - 1:origin["end_line"]])
                    check(origin["statement"] in fragment, "source fragment absent from original lines")
                else:
                    check(origin["path"] == "crates/storage/src/reference_fence.rs", "unknown refresh origin")
            try:
                compare_body(record)
            except ValueError as error:
                raise ValueError(f"{stage['stage']} {key}: {error}") from error
        check(seen == set(originals), "incomplete body review")
        reviewed.append({"stage": stage["stage"], "definitions": len(seen),
                         "categories": dict(Counter(x["category"] for x in stage["definitions"]))})
    return {"format": "recall-independent-pristine-source-review/v1", "source_commit": SOURCE,
            "original_profile_sha256": PROFILE_SHA, "source_attribution_sha256": MAP_SHA,
            "source_files_verified": len(source), "stages": reviewed,
            "product_acceptance": False, "allowed_profile_installed": False,
            "limits": ["static pinned-source/body comparison only", "no database or product execution",
                       "standard-refresh literal origins require separate E original-template receipt",
                       "runtime header grammar and exact complete profiles require a separate frozen oracle"]}
