#!/usr/bin/env python3
"""Read-only source attribution for an UNREVIEWED native schema capture.

This is a lexical review aid, not a SQL interpreter or an allowed-schema oracle.
Original definition bytes, source spans and unmatched bodies remain in the report.
It never reads a database, imports a candidate evaluator, or installs a baseline.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import subprocess
from decimal import Decimal


KINDS = {"TABLE": "tables", "FIELD": "fields", "INDEX": "indexes",
         "EVENT": "events", "FUNCTION": "functions", "ANALYZER": "analyzers"}
TOKEN = re.compile(
    r"\s+|--[^\n]*|//[^\n]*|/\*[\s\S]*?\*/|"
    r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|`(?:\\.|[^`\\])*`|"
    r"/(?:\\.|[^/\n])+/|\$?[A-Za-z_][A-Za-z_0-9]*|"
    r"\d+(?:\.\d+)?(?:[eE][+-]?\d+)?[fd]?|::|!=|>=|<=|==|&&|\|\||.",
    re.DOTALL,
)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def tokens(sql: str):
    for match in TOKEN.finditer(sql):
        value = match.group()
        if value.isspace() or value.startswith(("--", "//", "/*")):
            continue
        yield value, match.start(), match.end()


def statements(sql: str):
    """Keep quoted literals and nested block semicolons intact."""
    depth = 0
    start = None
    for value, begin, end in tokens(sql):
        if start is None:
            start = begin
        if value in ("{", "[", "("):
            depth += 1
        elif value in ("}", "]", ")"):
            depth -= 1
            if depth < 0:
                raise ValueError("unbalanced source delimiters")
        if value == ";" and depth == 0:
            yield start, end, sql[start:end]
            start = None
    if depth:
        raise ValueError("unclosed source delimiters")
    if start is not None:
        yield start, len(sql), sql[start:]


def identity(sql: str):
    values = [v for v, _, _ in tokens(sql)]
    upper = [v.upper() for v in values]
    if len(values) < 3 or upper[0] not in ("DEFINE", "REMOVE") or upper[1] not in KINDS:
        return None
    offset = 2
    if upper[offset:offset + 3] == ["IF", "NOT", "EXISTS"]:
        offset += 3
    elif upper[offset:offset + 2] == ["IF", "EXISTS"]:
        offset += 2
    elif upper[offset] == "OVERWRITE":
        offset += 1
    kind = upper[1]
    if kind in ("FIELD", "INDEX", "EVENT"):
        on = upper.index("ON", offset)
        name = "".join(values[offset:on]).replace("`", "")
        table_index = on + 2 if upper[on + 1] == "TABLE" else on + 1
        table = values[table_index].strip("`")
    elif kind == "FUNCTION":
        name = "".join(values[offset:values.index("(", offset)])
        name = name.removeprefix("fn::")
        table = None
    else:
        name, table = values[offset].strip("`"), None
    return table, KINDS[kind], name


def lexical(sql: str):
    """Small, disclosed formatter conveniences; NOT semantic equivalence."""
    values = [v for v, _, _ in tokens(sql)]
    # Identifier identity is checked separately, but case folding here is only
    # a review hint. Quoted content stays byte-for-byte case sensitive.
    values = [v if v.startswith(("'", '"', "`", "/")) else v.upper() for v in values]
    text = " ".join(values).rstrip(" ;")
    text = re.sub(r"^(DEFINE \w+) (IF NOT EXISTS |OVERWRITE )", r"\1 ", text)
    text = text.replace(" ON TABLE ", " ON ")
    return text


def review_tokens(sql: str):
    """Review token values only; identifiers retain case and strings retain value."""
    values = []
    for value, _, _ in tokens(sql):
        if value.startswith(("'", '"')):
            value = json.dumps(ast.literal_eval(value), ensure_ascii=False)
        elif re.fullmatch(r"`[A-Za-z_][A-Za-z_0-9]*`", value):
            value = value[1:-1]
        elif re.fullmatch(r"\d+\.\d+(?:[eE][+-]?\d+)?[f]?|\d+f", value):
            value = str(Decimal(value.removesuffix("f")).normalize()) + "f"
        values.append(value)
    if values[-1:] == [";"]:
        values.pop()
    if values[2:5] == ["IF", "NOT", "EXISTS"]:
        del values[2:5]
    elif values[2:3] == ["OVERWRITE"]:
        del values[2:3]
    for index in range(len(values) - 1, 0, -1):
        if values[index - 1:index + 1] == ["ON", "TABLE"]:
            del values[index]
    # These are the parser's exact operator aliases, outside quoted strings.
    index = 0
    while index < len(values):
        if values[index:index + 3] == ["IS", "NOT", "NONE"]:
            values[index:index + 3] = ["!=", "NONE"]
        elif values[index:index + 2] == ["IS", "NONE"]:
            values[index:index + 2] = ["=", "NONE"]
        index += 1
    return values


def field_properties(sql: str):
    values = review_tokens(sql)
    on = values.index("ON")
    values = values[on + 2:]
    names = {"TYPE", "FLEXIBLE", "DEFAULT", "VALUE", "ASSERT", "PERMISSIONS", "READONLY", "REFERENCE", "COMMENT"}
    result = {}
    current = None
    depth = 0
    for value in values:
        if not depth and value in names:
            if value in result:
                raise ValueError("duplicate source field clause")
            current = value
            result[current] = []
            continue
        if current is None:
            raise ValueError("unknown source field clause")
        result[current].append(value)
        if value in ("(", "[", "{"):
            depth += 1
        elif value in (")", "]", "}"):
            depth -= 1
    result.setdefault("PERMISSIONS", ["FULL"])
    return result


def block_review_tokens(sql: str):
    values = review_tokens(sql)
    index = 0
    while index < len(values):
        if values[index:index + 2] == ["NOT", "IN"]:
            values[index:index + 2] = ["NOTINSIDE"]
        elif values[index] == "IN":
            values[index] = "INSIDE"
        index += 1
    # Pinned parse_closure defaults an omitted single argument type to any;
    # Closure::fmt always writes that type. Do not erase explicit other types.
    for index in reversed(range(len(values) - 2)):
        if (values[index] == "|" and re.fullmatch(r"\$[A-Za-z_][A-Za-z_0-9]*", values[index + 1])
                and values[index + 2] == "|"):
            values[index + 2:index + 2] = [":", "any"]
    # Display for a one-value Block prints { value } rather than { value; }.
    # Limit this aid to the concrete fn::call form; no general statement or
    # semicolon removal, and no removal of an additional/changed expression.
    for start in reversed(range(len(values))):
        if values[start:start + 2] != ["{", "fn"]:
            continue
        depth = 1
        semicolons = []
        end = start + 1
        while end < len(values) and depth:
            token = values[end]
            if token == "{":
                depth += 1
            elif token == "}":
                depth -= 1
            elif token == ";" and depth == 1:
                semicolons.append(end)
            end += 1
        if semicolons == [end - 2] and values[end - 3] == ")":
            del values[end - 2]
    # Parentheses are emitted by Subquery::Select around an unparenthesized
    # SELECT on a LET RHS. Preserve every interior token and all other parens.
    for start in reversed(range(len(values))):
        if values[start:start + 2] != ["(", "SELECT"] or values[start - 1:start] != ["="]:
            continue
        depth = 1
        end = start + 1
        while end < len(values) and depth:
            if values[end] == "(":
                depth += 1
            elif values[end] == ")":
                depth -= 1
            end += 1
        if depth == 0 and values[end:end + 1] == [";"]:
            del values[end - 1]
            del values[start]
    return values


def body_review_hint(definition, origins):
    """Conservative property comparisons, still subject to independent D review."""
    category = definition["category"]
    actual = definition["statement"]
    defines = [s for s in origins if s["operation"] != "REMOVE"]
    if not defines:
        return {"matched": False, "rule": "missing-origin"}
    expected = defines[-1]["statement"]
    origin_category = definition["origin_category"]
    if category == "fields":
        wanted = field_properties(expected) if origin_category != "engine-relation-endpoint" else {}
        if origin_category == "engine-array-subfield":
            kind = wanted["TYPE"]
            if kind[:2] == ["option", "<"]:
                kind = kind[2:-1]
            if kind[:2] not in (["array", "<"], ["set", "<"]):
                return {"matched": False, "rule": "array-parent-not-typed"}
            wanted = {"TYPE": kind[2:-1], "PERMISSIONS": ["FULL"]}
            # The pinned field implementation preserves an existing subtype's
            # flex bit when later parent definitions change. Parent REMOVE does
            # not remove the separately persisted subtype definition.
            typed_defines = [entry for entry in defines if any(
                field_properties(entry["statement"])["TYPE"][i:i + 2] in (["array", "<"], ["set", "<"])
                for i in range(len(field_properties(entry["statement"])["TYPE"]) - 1))]
            if typed_defines and "FLEXIBLE" in field_properties(typed_defines[0]["statement"]):
                wanted["FLEXIBLE"] = []
        elif origin_category == "engine-relation-endpoint":
            parent = review_tokens(expected)
            endpoint = "IN" if definition["name"] == "in" else "OUT"
            if endpoint not in parent:
                return {"matched": False, "rule": "relation-parent-missing-endpoint"}
            wanted = {"TYPE": ["record", "<", parent[parent.index(endpoint) + 1], ">"], "PERMISSIONS": ["FULL"]}
        actual_properties = field_properties(actual)
        return {"matched": wanted == actual_properties, "rule": "field-clauses-and-pinned-defaults",
                "expected_properties": wanted, "observed_properties": actual_properties}
    if category == "tables":
        values = review_tokens(expected) if origin_category not in ("engine-implicit-table", "engine-recreated-table") else ["DEFINE", "TABLE", definition["name"]]
        if "TYPE" not in values:
            values[3:3] = ["TYPE", "NORMAL" if "SCHEMAFULL" in values else "ANY"]
        if "SCHEMALESS" not in values and "SCHEMAFULL" not in values:
            values.append("SCHEMALESS")
        if "PERMISSIONS" not in values:
            values.extend(["PERMISSIONS", "NONE"])
        return {"matched": values == review_tokens(actual), "rule": "table-parser-defaults-or-implicit-table"}
    if category == "indexes":
        values = review_tokens(expected)
        values = ["FIELDS" if v == "COLUMNS" else v for v in values]
        if values[-1:] == ["BM25"]:
            values.extend(["(", "1.2f", ",", "0.75f", ")"])
            for name in ("DOC_IDS_ORDER", "DOC_LENGTHS_ORDER", "POSTINGS_ORDER", "TERMS_ORDER", "DOC_IDS_CACHE", "DOC_LENGTHS_CACHE", "POSTINGS_CACHE", "TERMS_CACHE"):
                values.extend([name, "100"])
        return {"matched": values == review_tokens(actual), "rule": "index-columns-alias-and-fulltext-defaults"}
    if category in ("events", "functions"):
        if origin_category == "standard-refresh-template":
            expected = next(s for _, _, s in statements(expected) if s.startswith("DEFINE"))
        wanted = block_review_tokens(expected)
        if category == "functions" and wanted[-2:] != ["PERMISSIONS", "FULL"]:
            wanted.extend(["PERMISSIONS", "FULL"])
        observed = block_review_tokens(actual)
        return {"matched": wanted == observed, "rule": "block-operator-quote-single-value-subquery-formatting",
                **({} if wanted == observed else {"expected_tokens": wanted, "observed_tokens": observed})}
    if category == "analyzers":
        wanted = review_tokens(expected)
        wanted[3:] = [v.upper() for v in wanted[3:]]
        return {"matched": wanted == review_tokens(actual), "rule": "analyzer-tokenizer-filter-enum-formatting"}
    return {"matched": False, "rule": "body-requires-separate-review"}


def git(repo: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(repo), *args])


def build(profile_path: Path, repo: Path):
    raw = profile_path.read_bytes()
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("profile exceeds original bound")
    profile = json.loads(raw)
    if (profile["format"] != "recall-pristine-native-schema-profile/v1"
            or profile["review_state"] != "unreviewed-proposal"
            or profile["product_acceptance"] is not False):
        raise ValueError("not the original unreviewed profile")
    commit = profile["source_commit"]
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("invalid original source commit")
    if git(repo, "rev-parse", commit + "^{tree}").decode().strip() != profile["source_tree"]:
        raise ValueError("source tree mismatch")
    engine = "vendor/surrealdb-core-2.6.5"
    if git(repo, "rev-parse", commit + ":" + engine).decode().strip() != profile["engine_source_tree"]:
        raise ValueError("engine source tree mismatch")
    source_inventory = {}

    def source(path, expected=None):
        original = git(repo, "show", commit + ":" + path)
        digest = sha(original)
        if expected is not None and digest != expected:
            raise ValueError("pinned source hash mismatch: " + path)
        source_inventory[path] = {"sha256": digest, "bytes": len(original)}
        return original.decode()

    for path, digest in profile["compiled_source_sha256"].items():
        source(path, digest)
    histories = defaultdict(list)
    for ordinal, migration in enumerate(profile["ordered_migrations"], 1):
        if migration["ordinal"] != ordinal:
            raise ValueError("migration order mismatch")
        sql = source(migration["path"], migration["sha256"])
        for start, end, statement in statements(sql):
            key = identity(statement)
            if key is None:
                continue
            histories[key].append({"path": migration["path"], "line": sql.count("\n", 0, start) + 1,
                                   "end_line": sql.count("\n", 0, end) + 1,
                                   "migration_ordinal": ordinal, "statement": statement,
                                   "sha256": sha(statement.encode()), "operation": statement.split()[0].upper()})
    engine_paths = ["sql/statements/define/field.rs", "sql/statements/define/table.rs",
                    "sql/statements/define/index.rs", "sql/statements/define/event.rs",
                    "sql/statements/define/function.rs", "sql/statements/define/analyzer.rs",
                    "kvs/tx.rs", "syn/parser/stmt/define.rs", "sql/index.rs", "sql/kind.rs",
                    "sql/scoring.rs", "sql/number.rs", "sql/operator.rs", "sql/permission.rs",
                    "sql/block.rs", "sql/subquery.rs", "sql/closure.rs", "syn/parser/prime.rs"]
    for path in engine_paths:
        source(engine + "/src/" + path)
    refresh_path = "crates/storage/src/reference_fence.rs"
    refresh = source(refresh_path)
    # Decode only the two literal strings in the pinned, explicitly named helper.
    body = refresh.split("pub(crate) fn native_reference_event(table: &str) -> String {", 1)[1]
    body = body.split("/// Sealed inventory observation", 1)[0]
    literals = re.findall(r'"(?:\\.|[^"\\])*"', body)
    template = next(json.loads(s) for s in literals if "REMOVE EVENT" in s and "{table}" in s)
    head_template = next(json.loads(s) for s in literals if "REMOVE EVENT" in s and "{table}" not in s)
    exempt_body = refresh.split("fn reference_guard_exempt(table: &str) -> bool {", 1)[1].split("pub(crate) fn native_reference_event", 1)[0]
    exempt = set(re.findall(r'"([a-z_]+)"', exempt_body))
    stages = []
    for stage in profile["stages"]:
        is_refresh = stage["stage"] == "standard-restore-refreshed"
        export_by_identity = {}
        for ordinal, item in enumerate(stage["export_schema_statements"]):
            if ordinal != item["ordinal"] or sha(item["statement"].encode()) != item["sha256"]:
                raise ValueError("original export hash/ordinal mismatch")
            key = identity(item["statement"])
            if key in export_by_identity or key is None:
                raise ValueError("duplicate/unrecognized export identity")
            export_by_identity[key] = item
        records = []
        seen = set()
        for definition in stage["definitions"]:
            # INFO field-map keys use formatter-quoted reserved identifiers.
            # Preserve those original bytes in the record, compare their simple
            # identifier identity to the parsed DEFINE/source spelling.
            name = re.sub(r"`([A-Za-z_][A-Za-z_0-9]*)`", r"\1", definition["name"])
            key = definition["table"], definition["category"], name
            statement = definition["statement"]
            if key in seen or identity(statement) != key or sha(statement.encode()) != definition["sha256"]:
                raise ValueError("original definition identity/hash mismatch")
            seen.add(key)
            exported = export_by_identity.pop(key)
            if lexical(exported["statement"]) != lexical(statement):
                raise ValueError("original INFO/export body differs")
            origins = list(histories.get(key, []))
            category = "explicit-migration-history" if origins else "UNRECOGNIZED"
            if key[1] == "tables" and origins and origins[-1]["operation"] == "REMOVE":
                removal = origins[-1]
                later = [entry for other, entries in histories.items() if other[0] == key[2]
                         for entry in entries if entry["operation"] == "DEFINE"
                         and (entry["migration_ordinal"], entry["line"]) > (removal["migration_ordinal"], removal["line"])]
                if later:
                    origins.extend(sorted(later, key=lambda entry: (entry["migration_ordinal"], entry["line"])))
                    category = "engine-recreated-table"
            if is_refresh and key[1:] == ("events", "erased_reference_fence") and key[0] not in exempt:
                generated = head_template if key[0] == "source_head" else template.replace("{table}", key[0]).replace("{{", "{").replace("}}", "}")
                origins.append({"path": refresh_path, "line": 375, "statement": generated,
                                "sha256": sha(generated.encode()), "operation": "REFRESH_TEMPLATE"})
                category = "standard-refresh-template"
            elif not origins and key[1] == "fields" and key[2].endswith("[*]"):
                parent = key[2].removesuffix("[*]")
                parents = histories.get((key[0], "fields", parent), [])
                if parents:
                    origins = list(parents)
                    category = "engine-array-subfield"
            elif not origins and key[1] == "fields" and key[2] in ("in", "out"):
                origins = list(histories.get((None, "tables", key[0]), []))
                if origins:
                    category = "engine-relation-endpoint"
            elif not origins and key[1] == "tables":
                origins = [entry for other, entries in histories.items() if other[0] == key[2] for entry in entries]
                if origins:
                    category = "engine-implicit-table"
            last_define = next((s for s in reversed(origins) if s["operation"] != "REMOVE"), None)
            content_match = last_define is not None and lexical(last_define["statement"]) == lexical(statement)
            record = {**definition, "export_ordinal": exported["ordinal"],
                            "export_sha256": exported["sha256"], "origin_category": category,
                            "source_history": origins, "latest_origin_lexically_matches": content_match,
                            "content_verdict": "LEXICAL_REVIEW_HINT_ONLY" if content_match else "NEEDS_FORMATTER_OR_TRANSFORMATION_REVIEW"}
            record["body_review_hint"] = body_review_hint(record, origins)
            records.append(record)
        if export_by_identity:
            raise ValueError("unmatched original exported definitions")
        stages.append({"stage": stage["stage"], "definitions": records,
                       "origin_counts": dict(Counter(r["origin_category"] for r in records)),
                       "lexical_match_count": sum(r["latest_origin_lexically_matches"] for r in records),
                       "body_review_hint_matches": sum(r["body_review_hint"]["matched"] for r in records)})
    return {"format": "recall-pristine-schema-source-map/v1", "review_state": "source-attribution-not-approval",
            "product_acceptance": False, "allowed_profile": False,
            "mapping_tool": {"path": "scripts/qualification/semantic_schema_map.py",
                             "sha256": sha(Path(__file__).read_bytes())},
            "original_profile": {"path": str(profile_path), "sha256": sha(raw), "bytes": len(raw)},
            "source_commit": commit, "source_tree": profile["source_tree"],
            "engine_source_tree": profile["engine_source_tree"], "source_files": source_inventory,
            "transformation_sources": {
                "field-clauses-and-pinned-defaults": [
                    [engine + "/src/sql/statements/define/field.rs", 97],
                    [engine + "/src/sql/statements/define/field.rs", 125],
                    [engine + "/src/sql/statements/define/field.rs", 397],
                    [engine + "/src/sql/statements/define/table.rs", 203],
                    [engine + "/src/sql/permission.rs", 17],
                    [engine + "/src/sql/number.rs", 207]],
                "table-parser-defaults-or-implicit-table": [
                    [engine + "/src/syn/parser/stmt/define.rs", 707],
                    [engine + "/src/kvs/tx.rs", 1889]],
                "index-columns-alias-and-fulltext-defaults": [
                    [engine + "/src/syn/parser/stmt/define.rs", 1063],
                    [engine + "/src/sql/scoring.rs", 53],
                    [engine + "/src/sql/index.rs", 249]],
                "block-operator-quote-single-value-subquery-formatting": [
                    [engine + "/src/sql/operator.rs", 125],
                    [engine + "/src/sql/block.rs", 141],
                    [engine + "/src/sql/subquery.rs", 123],
                    [engine + "/src/syn/parser/prime.rs", 414],
                    [engine + "/src/sql/closure.rs", 66]],
                "analyzer-tokenizer-filter-enum-formatting": [
                    [engine + "/src/sql/statements/define/analyzer.rs", 72]],
                "standard-refresh-template": [[refresh_path, 311], [refresh_path, 375]],
            },
            "stages": stages, "limits": ["source identity attribution is not body equivalence",
                "lexical hints preserve quoted literals but case fold other tokens",
                "body review preserves identifier case; enumerated transformations remain independently reviewable",
                "D must independently review formatter transformations and freeze allowed profiles",
                "no database, runtime, erasure, restore, or product acceptance"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = build(args.profile, args.repo)
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    for stage in report["stages"]:
        print(stage["stage"], stage["origin_counts"], "lexical hints", stage["lexical_match_count"])


if __name__ == "__main__":
    main()
