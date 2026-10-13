"""Pure custody checks for original bounded offline snapshot inspection bytes.

No runtime imports, file reads, processes or physical-erasure verdicts. The
caller supplies the actual original report and registered export manifest.
Independent qualification owns compact decoding and physical acceptance.
"""
from __future__ import annotations

import json
import re

INSPECTION_CAP = 32 * 1024 * 1024 + 1
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def encoded(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise ValueError("nonfinite JSON number")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def checked_inspection(raw, original_manifest, *, expected_format="recall-snapshot-inspection-v1"):
    """Check inventory custody without expanding the original typed report.

    This validates custody and completeness only. It does not turn a decoder's
    hashes or a public404 into a source-erasure verdict.
    """
    require(len(raw) <= INSPECTION_CAP, "offline inspection capture exceeds core report bound")
    report = strict_json(raw)
    require(expected_format in ("recall-snapshot-inspection-v1", "recall-snapshot-inspection-v2")
            and isinstance(report, dict) and report.get("format") == expected_format,
            "unsupported offline inspection report")
    if expected_format == "recall-snapshot-inspection-v2":
        blobs = report.get("blobs")
        require(isinstance(blobs, dict), "compact report omitted complete blob dictionary")
        declared_bytes = 0
        for key, blob in blobs.items():
            require(isinstance(key, str) and HEX64.fullmatch(key)
                    and isinstance(blob, dict) and set(blob) == {"bytes", "base64"}
                    and type(blob["bytes"]) is int and 0 <= blob["bytes"] <= 14065536
                    and isinstance(blob["base64"], str), "compact dictionary custody shape invalid")
            declared_bytes += blob["bytes"]
            require(declared_bytes <= 64 * 1024 * 1024, "compact dictionary exceeds original decoded byte bound")
        # D's sole decoder verifies canonical base64, lengths/hashes/UTF8,
        # reference closure and ALL-occurrence expansion limits, then hashes
        # one original row at a time. Custody must not expand a whole inventory
        # or substitute a second physical predicate implementation.
    else:
        require("blobs" not in report, "v1 report unexpectedly contains compact dictionary")
    require(report.get("manifest") == original_manifest, "inspector read a different registered backup")
    rows = report.get("rows")
    counts = report.get("table_counts")
    require(isinstance(rows, list) and isinstance(counts, dict) and len(counts) <= 256,
            "complete bounded typed rows and table inventory required")
    require(type(report.get("row_count")) is int and len(rows) == report["row_count"] <= 200000,
            "inspector omitted physical rows")
    require(all(isinstance(table, str) and type(count) is int and count >= 0 for table, count in counts.items()),
            "invalid physical table counts")
    require(sum(counts.values()) == len(rows), "physical row count differs from complete table inventory")
    identities = set()
    observed = {}
    for row in rows:
        require(isinstance(row, dict) and isinstance(row.get("identity"), dict), "physical identity missing")
        identity = row["identity"]
        require(set(identity) == {"table", "id"} and identity["table"] in counts,
                "physical identity outside declared inventory")
        key = encoded(identity)
        require(key not in identities, "duplicate physical identity in inspection")
        identities.add(key)
        observed[identity["table"]] = observed.get(identity["table"], 0) + 1
        require(HEX64.fullmatch(row.get("sha256", "")) and isinstance(row.get("references"), list)
                and row.get("record") is not None, "explicit include-records report omitted payload or references")
    require(all(observed.get(table, 0) == count for table, count in counts.items()), "typed table counts differ")
    schema = report.get("schema_statements")
    require(isinstance(schema, list) and type(report.get("statement_count")) is int
            and len(schema) <= report["statement_count"] <= 100000, "schema inventory incomplete or unbounded")
    require(all(isinstance(row, dict) and HEX64.fullmatch(row.get("sha256", ""))
                and isinstance(row.get("statement"), str) for row in schema),
            "explicit schema bodies required for later independent physical checks")
    return report
