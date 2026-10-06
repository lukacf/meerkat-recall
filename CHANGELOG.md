# Changelog

All notable changes to Elephant will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

Changes prepared for the first release. A workspace version or candidate build
does not by itself establish a published release.

### Runtime and licensing
- Governed Living Wiki generation, revision visibility, and durable cancellation.
- Indexed queue operations, bounded archival of terminal work history, and
  database execution deadlines to address growing-queue saturation.
- Direct budget-record reads and indexed daily ledger aggregation prevent
  budget checks from scanning the complete accounting history.
- Retention and retry transactions preserve acknowledged updates across the
  active/archive boundary. A pinned SurrealKV snapshot-publication backport
  fixes the corresponding in-process engine race; remote database servers
  require their own engine correction.
- Meerkat 0.8.51, current MCP/PDF dependencies, and stricter API authorization
  and ingestion error handling. This candidate keeps the six direct Meerkat
  family dependencies at exactly 0.8.51; it does not upgrade to a 0.9 platform
  or establish compatibility with a separately installed MobKit release.
- Dual licensing under MIT OR Apache-2.0.

### Security
- An explicitly empty `subject_allowlist` remains a restriction. Ordinary
  entity updates and batch upserts cannot change existing `Security`, `subjects`
  or `subjects_state` fields, and compare the complete authorized snapshot in
  the write to reject stale replacements.
- HTTP and MCP authentication share the token verifier and retain the verified
  legacy caller identity through dispatch. Operation-bound exchange verification
  remains separate groundwork; resource routes continue refusing exchange tokens.
- Multiword document and assertion text queries keep every term inside the
  selected space and soft-deletion filters, including their count queries.
- Assertion delete and restore routes check record write authorization.
  Restore also requires record read authorization before mutation because it
  returns the restored assertion; write-only callers cannot use it to read
  either active or deleted assertions.
- REST restore routes for documents, entities, relationships, sources, and
  timeline events, plus assertion resolution updates, now require record read
  authorization before returning existing content, in addition to write
  authorization.
- REST restores, MCP entity restore, and assertion-delete mutations compare
  the stored authorization fields and lifecycle against the authorized
  snapshot in the same write.
  Stale requests return a conflict without changing or returning the newer row.
- Assertion resolution writes only the resolution fields under the same guard,
  preserving a concurrent classification or subject restriction.
- Ordinary timeline updates cannot change `Security` fields, even when the
  caller can access both classifications. A concurrent security change causes
  a conflict instead of being overwritten by the update.
- Assertion search counts only authorized matching rows, including queries
  filtered by an entity link and requests continued with a cursor.
- Independent V1 restriction and protected dependency contracts preserve exact
  ceilings, unresolved contributors and source-local policy observations through
  ordinary derivation. Shared normalized vectors exercise this pure algebra;
  no active source, tool, provider or recipient boundary consumes it yet.
- Independent security contract compatibility negotiation refuses unknown
  versions, missing duties and profile downgrades against the shared Meerkat
  vectors. This prerequisite does not enable governed HTTP/MCP dispatch.
- Incoming MCP requests use only the caller's Authorization header; internal
  worker credentials no longer replace missing or restricted caller credentials.
- Authenticated server startup rejects missing or short signing secrets and
  unsupported JWT algorithms before starting workers or opening a listener.
- Authentication-free demos bind only to loopback.
- Secret scanning enables upstream detection rules, includes merge history, and
  limits fixture exceptions to their exact synthetic values and source paths.

### Release tooling
- Release candidates are built explicitly for Linux x86-64 and macOS ARM64
  with RocksDB and the default Meerkat extraction support.
- Candidate archives include license texts, source/build provenance, and flat
  checksum files; tags no longer trigger automatic binary publication.
- Releases are promoted from verified artifacts after source and checksum
  checks, preserving the exact executable selected for deployment.

### Added
- Managed runtime status and bootstrap surfaces on the default `/mcp` endpoint.
- Source-first ingestion with staged artifact validation/commit and downstream
  truth maintenance.
- MCP source-ingest tools: `ingest_source`, `ingest_source_simple`, and
  `batch_ingest_sources`.
- Embedded `elephant-db` snapshot import/export operations for local deployments.

### Changed
- Source-first ingest is the primary ontology flow for staged document/source
  ingestion.
- The active ingest/extraction path is
  `ingest source -> triage_source -> ingest_metadata -> validate_artifacts -> commit_artifacts -> upgrade_ingest_mode -> render_doc -> build_doc_tree -> plan_extraction -> extract_assertions -> validate_artifacts -> commit_artifacts -> outbox -> truth_maintenance`.
- `POST /v1/docs/ingest` now returns `source_id`, `lifecycle_status`,
  `current_ingest_mode`, and `work_item_id`.
- MCP `ingest_doc*` responses now include the persisted source link on returned
  docs.
- MCP document search filters no longer accept `content_type`; callers should
  use query/collection metadata instead.
- The legacy assertion-resolution worker path is removed from the active
  runtime and documented only as legacy/non-active behavior.

### Fixed
- Document paragraph and section spans preserve original byte offsets across
  LF and CRLF line endings, preventing misplaced evidence and UTF-8 slicing
  panics during document-tree construction.
- Fixed-size chunks and sentence-free fallback chunks keep UTF-8 characters
  intact at chunk and overlap boundaries while preserving complete coverage.
- Document semantic search uses partitioned vector candidates and blends
  lexical and semantic ranks. Assertion text search uses indexed candidates.
- Document semantic search loads vector-hit nodes, revisions, and documents
  by record ID instead of scanning their tables while assembling results.
- OpenAI embedding requests cap inputs at 24,000 characters, shorten inputs
  rejected for exceeding the model limit, and split batches rejected for their
  aggregate token count. Oversized inputs embed a prefix, not the entire text;
  stored source evidence remains intact.
- MCP entity search reuses vector snapshots per space and dimension for up to
  60 seconds and loads matching entity records in batches. Ranking can lag
  vector writes until refresh; current records still pass request authorization.
- Space ID mismatch in create handlers: body `space_id` is now overridden with
  the principal's space.

### Documentation
- Top-level docs now point readers to `docs/active-architecture.md` for active
  runtime behavior and `docs/non-active-catalog.md` for removed/planned surfaces.
- Public release docs clarify setup, contribution, security, and release
  validation expectations.

### Core capabilities
- Initial workspace structure
- Core crates: types, storage, policy, identity, pipeline, embeddings, mcp
- Unified `elephant` CLI and managed API/pipeline runtime
- Entity management and CRUD operations
- Relationship traversal
- Identity resolution and merge strategies
- Assertion storage with provenance tracking
- Document embedding pipeline
- ABAC-based policy enforcement
- MCP server integration
- Scenario-based testing framework
- SurrealDB integration with connection pooling

### Security
- JWT authentication
- Space-based tenant isolation
- Field-level redaction policies

[Unreleased]: https://github.com/lukacf/elephant/commits/main
