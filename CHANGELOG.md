# Changelog

All notable changes to Meerkat Recall will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Timeline
- Page `list_upcoming_events` past its 50-event limit: a truncated response returns
  `next_cursor`, and passing it back as `cursor` continues the same window after the
  last returned `(time_start, id)`. A page cut by the scan ceiling resumes after the
  last scanned row even when filtering left none of it visible.

## [0.9.1] - 2026-10-15

### Deployment and diagnostics
- Select runtime TOML explicitly with `serve --config`, `migrate --config` or
  `ELEPHANT_CONFIG`; a missing or malformed selected file fails without fallback.
- Diagnose worker MCP discovery and current named-space authority separately
  from process liveness, using existing token and grant checks.
- Prepare authenticated, digest-pinned Compose installations with persistent
  state and a paired SurrealDB server; promote qualified container archives to
  the canonical registry with explicit source and anonymous-pull checks.

### Bulk ingestion
- Inspect effective daily graph/retrieval limits, usage and reservation-aware
  headroom through an authorized, selected-space `GET /api/v1/budget`.
- Document complete-input `text-full-v2` profile selection, staged reindex
  activation and fresh revision/generation retries for terminal failed sources.
- Return typed oversized-input guidance without truncating or relabeling `text-v1`;
  retain complete-input segmentation and pooling in explicitly selected `text-full-v2`.
- Preserve safe typed extraction causes and actions in worker diagnostics and
  failed-run summaries without copying provider output into those diagnostics.
- Preserve typed per-source budget and missing-publication wait reasons, and
  increase the bounded worker transient retry default to 10 attempts.
- Persist duplicate-vector bitmap insertions and removals, and fix filtered HNSW
  candidate selection so foreign or deleted vectors do not consume eligible
  result slots. The paired server carries the same engine corrections.
- Document recovery from existing missing vector membership through original
  document intake with fresh revision/key and current-generation guards; an
  upgrade or redacted source GET response alone cannot restore missing members.

### Security
- Upgrade Rustls from 0.23.45 to 0.23.46 for RUSTSEC-2026-0346,
  RUSTSEC-2026-0347, RUSTSEC-2026-0348 and RUSTSEC-2026-0349. Keep
  dependency auditing enabled and retain the exact Meerkat 0.9.0 SDK pins.

### CI reliability
- Use a conservative reader-prose path while preserving required check names,
  secret scanning, dependency auditing and reader-documentation validation.
- Cache the Cargo registry and build directories actually used by the repository
  wrapper, separated by feature/profile lane.
- Provision a checksum-pinned local model for the existing constructor-reuse
  regression instead of selecting it without its required fixture.

## [0.9.0] - 2026-10-14

### Meerkat 0.9 integration
- Exact-pin the published Meerkat 0.9.0 family and align Recall workspace and
  helper package versions to 0.9.0.
- Retire the three active provider HTTP backports in favor of upstream native
  redirect refusal and fail-closed client construction; preserve their historical
  source evidence and record the replacement registry checksums.

### Security and correctness
- Upgrade the Ammonia HTML sanitizer to 4.1.7. Retain the documented,
  advisory-specific `unsafe-libyaml` maintenance exception because the published
  Meerkat 0.9 SDK still depends on that unmaintained parser.
- Identity merge audit records retain the authorization restrictions of both
  the original and replacement identities, including subject references removed
  from the live row but preserved in the audit's before image.
- Scope secret-scanner fixture exceptions to both the reviewed exact value and
  its file path; different values in those paths remain subject to detection.

- Preserve original approved identity decisions across successive entity merges,
  including inherited restrictions in a single atomic merge plan.
- Pause native background writers during snapshot inventory and export so the
  captured records and exported history describe the same snapshot.

### Release tooling
- Include all six active patched dependency source trees in native and container
  distributions, including the Tokenizers and rav1e maintenance patches.
- Check helper manifest and lockfile version parity alongside Rust workspace
  packages, and require a single exact version across direct Meerkat SDK pins.

## [0.1.0] - 2026-10-13

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
- The product and native command are Meerkat Recall and `meerkat-recall`.
  Archives retain an `elephant` executable alias for the 0.1 line; configuration,
  data and HTTP/MCP wire identifiers keep their existing spelling.
- `--version` identifies the package version. `status --require-ready` fails
  unless health and semantic ingestion are ready, with bounded HTTP requests
  and diagnostic JSON preserved on stdout.
- Snapshot migration preflight rejects any occupied target table, unverifiable
  metadata and unsupported source extensions before schema/data writes. Reports
  never overwrite existing files, and partial-copy failures remain explicit.
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

[Unreleased]: https://github.com/lukacf/meerkat-recall/commits/main
