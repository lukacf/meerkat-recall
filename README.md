# Meerkat Recall

Meerkat Recall is a Rust knowledge service with named spaces, source-backed
evidence, entity and relationship management, semantic search, wiki workflows,
truth maintenance and policy enforcement. It can run independently with embedded
durable storage or a compatible patched SurrealDB backend.

This repository contains a selected source snapshot and distribution materials.
It has no inherited development history. Production Rust sources, lockfile and
patched dependencies retain their build-source identity; documentation and
distribution helpers have an explicit mapping in `PUBLIC_SOURCE.json`.
Native provenance records the original build commit separately from this public
snapshot. Public source identity is not a claim that the binary was rebuilt here.

- [Releases](https://github.com/lukacf/meerkat-recall-releases/releases)
- [Native installation](docs/installation.md)
- [Service operations](docs/operations.md)
- [Build and provenance](scripts/release/README.md)
- [Project license](LICENSE) and [third-party notices](THIRD_PARTY_NOTICES.md)

The source declares version 0.1.0. Use each release's actual qualification record
for supported target/runtime scope. Source and packaging metadata alone do not
establish provider, persistence or integration acceptance.
