# Meerkat Recall

Meerkat Recall is an open-source knowledge system for applications and AI
agents, written in Rust. It keeps source documents, extracted claims, entities
and relationships together, so an answer can carry the evidence behind it.
Document, vector and graph retrieval work over the same knowledge, with space
isolation, access control and background maintenance as sources change.

Use it as a standalone service through HTTP or MCP, from any agent stack.
Within Meerkat, Core runs agents and Meta manages their application and memory
lifecycle; Recall adds shared, evidence-backed knowledge. It is an optional,
independently released component. See the
[Meerkat introduction](https://docs.rkat.ai/introduction) to choose your starting point.

Recall 0.9.1 is available with the exact-pinned Meerkat 0.9.0 SDK.
Download the [v0.9.1 release](https://github.com/lukacf/meerkat-recall/releases/tag/v0.9.1).
Find source, native downloads, supported platforms and known limits in the
[release notes](https://github.com/lukacf/meerkat-recall/releases).

Read the [Recall documentation](https://docs.rkat.ai/recall/introduction) for
setup, HTTP and MCP integration, deployment, and the knowledge model.

## Installation paths

Native releases target Linux x86-64 and macOS ARM64.
Recall 0.9.1 passed archive verification, isolated installation and a local-model
inference case on macOS 26.5.2 (Apple Silicon): one document, two semantic queries
and stored 384-dimensional vectors. Older macOS versions were not exercised. See the
release qualification record for the model, source mapping and test scope.
[Binary installation](docs/installation.mdx) covers the archive layout,
checksums, and model-provider prerequisites. Use each archive with its matching
release record. The source demo below is a separate build without model extraction.

The command is `meerkat-recall`; native archives retain an `elephant` alias
for existing deployments. Existing `.elephant` configuration, `ELEPHANT_*` variables,
database names and HTTP/MCP wire identifiers remain unchanged. Recall 0.9.1
provides source and native archives plus Linux container images. A published
Rust library is not part of this release.

For a persistent native service, follow [service setup](docs/operations/service-setup.mdx).
Select its TOML explicitly with `serve --config /absolute/runtime.toml` and use
the same file for `migrate`; `ELEPHANT_CONFIG` provides the service environment
equivalent. Missing or malformed selected files fail without fallback.
Recall 0.9.1 exact-pins the published Meerkat 0.9.0 family. The SDK now includes
the native provider transport protections previously carried as Recall patches.
The v0.1.0 artifacts continue using their original Meerkat 0.8.51 dependencies.
Consult the release notes for tested behavior and remaining limits.

## Try the Living Wiki demo

From a checkout, with Rust 1.94 or newer installed:

```bash
git clone https://github.com/lukacf/meerkat-recall.git
cd meerkat-recall
./scripts/repo-cargo run -p meerkat-recall-bin --no-default-features -- demo-wiki
```

Open the [context inspector](http://localhost:3000/inspect/knowledge-views) and
[evidence browser](http://localhost:3000/evidence). This demo builds cited context
projections from the project's architecture document in an isolated in-memory
store. It needs no database server or model credentials and binds to loopback.
The article wiki at `/wiki` stays empty in this build. To generate articles with
a configured model, see the [full demo](docs/architecture/living-wiki.mdx#runnable-projection-demo).
The first source build can take several minutes. Press Ctrl-C to stop it.

## Deploy with release containers

For Linux AMD64, use the [authenticated Compose installation](deploy/compose/README.md)
with both immutable image digests from the selected release record. The Recall
image runs HTTP/MCP and managed workers; the paired database image supplies the
required reference-fence capability. The recipe creates private configuration,
retains persistent state and runs migrations before serving. Complete worker
credentials, space grants and provider approvals before requiring semantic readiness.

The [deployment guide](docs/operations/deployment.mdx) covers readiness, storage,
service roles and Kubernetes. Before bulk imports, check the authorized
[daily budget view](docs/operations/service-setup.mdx#inspect-budget-before-an-import).
For long documents, configure an approved
[complete-input embedding profile](docs/operations/service-setup.mdx#long-documents-and-embedding-profiles)
and follow the guarded reindex and failed-source retry guidance.

The development Compose example below retains
its local-build workflow and authentication bypass.

## Run a persistent local server

Docker Compose runs Meerkat Recall and its paired remote SurrealDB server.
First build and load the reviewed image using the
[paired server recipe](packaging/recall-surreal/README.md); set
`RECALL_SURREAL_IMAGE` to its actual local tag or digest. Vanilla SurrealDB
cannot satisfy the required reference-fence capability, and the Compose
example does not imply an available qualified image.

For semantic ingestion, copy [.env.example](.env.example) to `.env` and configure
credentials plus explicit approved per-space processing bindings. The retained
environment defaults select OpenAI `gpt-6.1-sol` for extraction, multimodal
rendering and wiki writing, with `claude-sonnet-5-5` for the auxiliary Anthropic
roles (summaries, conflict adjudication and promotion disambiguation). Pipeline
embeddings remain `text-embedding-3-large` at 3072 dimensions. Explicit model
settings and approved per-space profiles remain authoritative; defaults do not
approve a provider or migrate existing profiles. Model calls can incur provider
charges.

```bash
cp .env.example .env
# Edit .env with your local configuration; use your actual paired image below.
export RECALL_SURREAL_IMAGE=recall-surreal:REVIEWED_SOURCE
docker compose up -d --wait surrealdb
docker compose run --build --rm elephant meerkat-recall migrate
docker compose up -d elephant
curl -fsS http://localhost:3000/v1/runtime/status
```

The `surrealdb_data` volume holds the database and `recall_state` retains the
original recovery journal and managed backups. Migration and serving use the
same journal; normal Compose shutdown preserves both volumes. See [DEV.md](DEV.md)
for the local-process variant and recovery constraints.

Configure the selected space's processing approvals and inspect its status before
expecting semantic completion. Durable source acceptance can remain available
without workers; acceptance alone does not establish searchable publication.
A successful liveness request alone does not establish extraction readiness. The local
Compose configuration uses development authentication bypass; see
[deployment and authentication](docs/operations/deployment.mdx) before
exposing an instance to other users.

Meerkat Recall's public command is `meerkat-recall` ([CLI reference](docs/cli/overview.mdx)):

- `serve` runs HTTP, MCP, and managed workers together.
- `serve --server-only` runs HTTP and MCP without workers.
- `worker` runs separate workers against a shared SurrealDB server.
- `migrate` applies the SurrealDB schema.
- `status` checks a running server.
- `snapshot export/restore` (with `import` as an alias) coordinates a managed snapshot directory through the running HTTP service for either retained backend.

## Connect an MCP client

Use Streamable HTTP at `http://localhost:3000/mcp`. `meerkat-recall serve` is an HTTP
server, not a stdio MCP process. Configure the client with this URL and an
`Authorization: Bearer <token>` header when authentication is enabled.

Call `get_runtime_status` first. In an unconfigured runtime, loopback clients
can use `initialize_runtime`. Ingest tools become available only when storage,
workers, and a real semantic extractor are ready. Self-referential extraction
through `/mcp/agent` is retried after the listener binds.
Configure the worker's [MCP endpoint, bearer and per-space read grants](docs/operations/service-setup.mdx#configure-extractor-mcp-access), including predicate reads.

The default profile supports common ingest, search, navigation, and inspection
operations. `/mcp/agent` is read-only, `/mcp/maintenance` adds maintenance
operations, and `/mcp/full` exposes the full registered surface subject to
authorization. Fetch `tools/list` for the current schema and tool availability.
See the [MCP guide](docs/mcp/overview.mdx).

## Read the documentation

Start with the [knowledge model](docs/concepts/overview.mdx) for the
relationship between evidence, claims, the graph, and derived views. Follow the
[worked example](docs/guides/putting-it-together.mdx) to see those concepts in a
complete task, or use the [HTTP API](docs/api/overview.mdx) and
[MCP workflows](docs/mcp/workflows.mdx) when building an integration.

## Storage and data model

| Backend | Selection | Persistence |
| --- | --- | --- |
| SurrealDB | `ELEPHANT_DB=surreal-db` (default) | Durable server or embedded database; requires schema migrations |
| ElephantDB | `ELEPHANT_DB=elephant-db` | Graph/vector kernel with durable managed commits when a persistent destination is configured; explicit backup/restore |

Spaces isolate tenants. Sources and immutable document revisions retain the
evidence; extraction stages artifacts before committing assertions,
relationships, attributes, and events. Truth maintenance records agreement and
conflict over time. Living Wiki pages are derived views with citations,
authorization, and freshness receipts.

Managed ElephantDB with a persistent destination synchronizes each acknowledged
mutation to its write-ahead log and replays it on startup. Snapshot export is a
backup operation, not the ordinary commit durability boundary. Explicitly
ephemeral library/test stores remain volatile. See
[managed embedded commit durability](docs/embedded-durable-commits.md) for
configuration, recovery and erasure behavior.

Embedded SurrealDB has finite engine query deadlines, defaulting to 30 seconds.
Remote SurrealDB requires its own server-side query deadline. See
[capacity and retention](docs/work-queue-capacity.md) for timeout semantics,
queue indexes, archival, and operational acceptance checks.

## Use the Rust library

The workspace `elephant` crate provides an embedded facade:

```rust
use elephant::{Elephant, IngestTextRequest};

let elephant = Elephant::embedded().open().await?;
let receipt = elephant.ingest_text(IngestTextRequest {
    space_id: "local".into(),
    actor: "app".into(),
    title: "Note".into(),
    text: "Meerkat Recall can run as a library.".into(),
    ..Default::default()
}).await?;
```

This stages source-backed work. Processing it requires an appropriately
configured worker and extractor. Library integrations are responsible for
establishing their own caller authorization before using repositories directly.

## Documentation and development

- [Quickstart](docs/quickstart.mdx) - configure, ingest, and inspect evidence.
- [Active architecture](docs/active-architecture.md) - current runtime contract.
- [Non-active catalog](docs/non-active-catalog.md) - removed and planned surfaces.
- [HTTP API](docs/api/overview.mdx) and [MCP tools](docs/mcp/tools.mdx).
- [Development](docs/operations/development.mdx) and [contributing](CONTRIBUTING.md).
- [Release process](RELEASE.md), [changelog](CHANGELOG.md), and [security](SECURITY.md).

Use `./scripts/repo-cargo` for Rust commands so separate checkouts have isolated
build state. `make ci-smoke` runs the local smoke validation lane; `make ci`
runs the broader checks.

## License

Licensed under either of the following, at your option:

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

Third-party components retain their own licenses. In particular, the patched
SurrealDB components use BUSL-1.1; the project license does not replace those
terms. Native archives include component licenses and patched sources. See
[third-party notices](THIRD_PARTY_NOTICES.md) and
[the component source notices](scripts/release/THIRD_PARTY_SOURCE_NOTICES.txt).
