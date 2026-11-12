# Rusqlite 0.37 with patched bundled SQLite

Recall and Meerkat 0.9.10 share the `rusqlite` 0.37 API. Their native `sqlite3`
link must resolve to one library. This copy retains the wrapper's Rust source
and updates its `libsqlite3-sys` requirement from 0.35.0 to 0.39.1. The root
lockfile selects 0.39.1, bundling SQLite 3.54.1 with the WAL page-size repair in
[RUSTSEC-2026-0459](https://rustsec.org/advisories/RUSTSEC-2026-0459).

## Provenance

- Registry archive: `rusqlite` 0.37.0.
- Archive SHA-256: `165ca6e57b20e1351573e3729b958bc62f0e48025386970b6e4d29e7a7e71f3f`.
- All 71 original archive files and their licenses are retained. The original
  revision is recorded in [the packaged VCS metadata](rusqlite-0.37.0/.cargo_vcs_info.json).
- [Original checksums](rusqlite-0.37.0-upstream-sha256.json) and the
  [complete delta](rusqlite-0.37.0-backport.patch) permit exact reconstruction.
- Changes are limited to both dependency manifests, the standalone test
  lockfile, and one bundled-version regression test. Wrapper implementation
  source is unchanged. The native library and bindings come from the published
  `libsqlite3-sys` package selected in the root lockfile.

## Scope and validation

The bundled-library test checks the linked runtime version against the first
patched SQLite release, 3.54.0. The wrapper's existing library tests run against
that library; Recall's workspace suite separately checks consumer behavior.

```sh
./scripts/repo-cargo test --manifest-path vendor/rusqlite-0.37.0/Cargo.toml \
  --locked --features bundled --lib --test bundled_sqlite_patch
```

This check is for bundled SQLite. Linking a system SQLite uses that system's
version; SQLCipher is a separate bundled source with its own version and
advisory exposure. Neither is declared repaired by this test. The upstream
vulnerability concerns opening a database alongside a crafted WAL file; this
dependency update does not establish the cause of a production allocator abort.
