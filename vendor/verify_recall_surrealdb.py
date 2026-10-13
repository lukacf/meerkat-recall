#!/usr/bin/env python3
"""Audit the bounded vendored delta without compiling or executing Rust."""
import hashlib
import hmac
import json
from pathlib import Path
import struct
import tomllib

ROOT = Path(__file__).resolve().parent
PATCHED = {
    'surrealdb-core-2.6.5': {
        'Cargo.toml',
        'src/ctx/context.rs',
        'src/dbs/iterator.rs',
        'src/dbs/options.rs',
        'src/doc/document.rs',
        'src/doc/field.rs',
        'src/doc/process.rs',
        'src/fnc/array.rs',
        'src/fnc/mod.rs',
        'src/fnc/object.rs',
        'src/idx/index.rs',
        'src/idx/trees/store/hnsw.rs',
        'src/idx/trees/store/mod.rs',
        'src/key/debug.rs',
        'src/kvs/ds.rs',
        'src/kvs/index.rs',
        'src/kvs/mod.rs',
        'src/kvs/rocksdb/mod.rs',
        'src/kvs/surrealkv/mod.rs',
        'src/kvs/tr.rs',
        'src/kvs/tx.rs',
        'src/lib.rs',
        'src/rpc/method.rs',
        'src/rpc/mod.rs',
        'src/rpc/protocol/v1.rs',
        'src/rpc/protocol/v2.rs',
        'src/rpc/request.rs',
        'src/rpc/response.rs',
        'src/sql/array.rs',
        'src/sql/closure.rs',
        'src/sql/idiom.rs',
        'src/sql/object.rs',
        'src/sql/statements/foreach.rs',
        'src/sql/statements/select.rs',
        'src/sql/statements/update.rs',
        'src/sql/subquery.rs',
        'src/sql/value/every.rs',
        'src/sql/value/get.rs',
        'src/sql/value/value.rs',
        'src/syn/parser/builtin.rs',
    },
    'surrealdb-2.6.5': {
        'Cargo.toml',
        'src/api/conn/cmd.rs',
        'src/api/engine/local/mod.rs',
        'src/api/engine/remote/mod.rs',
        'src/api/method/mod.rs',
        'src/api/method/query.rs',
        'src/api/method/tests/server.rs',
        'src/lib.rs',
    },
}
ADDED = {
    'surrealdb-core-2.6.5': {
        'RECALL_PROVENANCE.json',
        'src/fnc/recall.rs',
        'src/fnc/recall_hmac_tests.rs',
        'src/kvs/physical_generation.rs',
        'src/kvs/recall_history.rs',
        'src/kvs/recall_history_inventory.rs',
        'src/kvs/recall_history_selection.rs',
        'src/kvs/recall_history_tests.rs',
        'src/kvs/recall_index_erasure.rs',
        'src/kvs/recall_index_erasure_tests.rs',
        'src/kvs/recall_memory.rs',
        'src/kvs/recall_physical_tests.rs',
        'src/kvs/recall_scalar_projection.rs',
        'src/kvs/recall_scalar_projection_tests.rs',
        'src/kvs/rocksdb/physical_checkpoint.rs',
        'src/kvs/rocksdb/physical_checkpoint_tests.rs',
        'src/kvs/rocksdb/source_erasure.rs',
        'src/kvs/surrealkv/physical_checkpoint.rs',
        'src/kvs/surrealkv/physical_checkpoint_tests.rs',
        'src/rpc/recall_history_inventory.rs',
        'src/rpc/recall_physical_checkpoint.rs',
        'src/rpc/recall_physical_history_checkpoint.rs',
        'src/sql/statements/foreach_ownership_tests.rs',
        'src/sql/value/dispatch_tests.rs',
        'src/sql/value/missing_field_tests.rs',
    },
    'surrealdb-2.6.5': {
        'RECALL_PROVENANCE.json',
        'src/api/method/recall_history_inventory.rs',
        'src/api/method/recall_physical_checkpoint.rs',
        'src/api/method/recall_physical_history_checkpoint.rs',
    },
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def field(text):
    value = text.encode()
    return struct.pack(">Q", len(value)) + value


def main():
    checked = 0
    for name, allowed in PATCHED.items():
        base = ROOT / name
        manifest = json.loads((ROOT / f"{name}-upstream-sha256.json").read_text())
        provenance = json.loads((base / "RECALL_PROVENANCE.json").read_text())
        assert provenance["version"] == "2.6.5"
        changes = set()
        for relative, expected in manifest.items():
            path = base / relative
            assert path.is_file(), f"missing upstream file: {name}/{relative}"
            if digest(path) != expected:
                changes.add(relative)
            checked += 1
        assert changes == allowed, f"unreviewed/absent upstream delta {name}: {changes ^ allowed}"
        actual = {str(path.relative_to(base)) for path in base.rglob("*") if path.is_file()}
        assert actual - set(manifest) == ADDED[name], f"unexpected new files in {name}"
        tomllib.loads((base / "Cargo.toml").read_text())
    context = (ROOT / "surrealdb-core-2.6.5/src/ctx/context.rs").read_text()
    protected = (ROOT / "surrealdb-core-2.6.5/src/cnf/mod.rs").read_text()
    for name in ("__recall_reference_key", "__recall_journal_identity", "__recall_erasure_reservation"):
        assert name in context
        assert name not in protected, "SDK binding must remain allowed"
    tomllib.loads((ROOT.parent / "Cargo.toml").read_text())
    prefix = b"meerkat-recall/reference/v1\0" + field("00000000-0000-0000-0000-000000000001") + field("__recall_capability__") + field("recall_probe")
    expected = (
        (b"\0" + field("42"), "40e9322203b5e4d741f086599f851ac1b7c4e8a2e0bad1398da4ffde5911874c"),
        (b"\1" + struct.pack(">q", 42), "03d2d2962853eaa5410909793d65f296968ae5e6024ab522121d575cf1108454"),
    )
    test = (ROOT.parent / "crates/storage/tests/native_reference_tokens.rs").read_text()
    for message, token in expected:
        assert hmac.new(bytes(range(32)), prefix + message, hashlib.sha256).hexdigest() == token
        assert token in test
    print(f"PASS: {checked} original files audited; bounded patch allowlist and two independent HMAC vectors verified")


if __name__ == "__main__":
    main()
