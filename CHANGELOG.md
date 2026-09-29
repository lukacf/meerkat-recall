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
- Meerkat 0.8.47, current MCP/PDF dependencies, and stricter API authorization
  and ingestion error handling.
- Dual licensing under MIT OR Apache-2.0.

### Security
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
