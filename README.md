# Elephant

Elephant is a Rust knowledge system for applications and AI agents. It combines
entity graphs, document and vector search, evidence-backed extraction, and
truth maintenance behind HTTP and MCP interfaces with space isolation and
attribute-based access control.

This is pre-1.0 software. The workspace version is a development version until
verified artifacts and release notes appear on the
[releases page](https://github.com/lukacf/elephant/releases).

## Installation paths

The candidate binary distribution targets Linux x86-64 and macOS ARM64.
[Binary installation](docs/installation.mdx) covers the archive layout,
checksums, and model-provider prerequisites. Candidate availability is not a
published release; use only artifacts supplied with their matching release
record. The source demo below is a separate build without model extraction.

## Try the Living Wiki demo

From a checkout, with Rust 1.94 or newer installed:

```bash
git clone https://github.com/lukacf/elephant.git
cd elephant
./scripts/repo-cargo run -p elephant-bin --no-default-features -- demo-wiki
```

Open the [context inspector](http://localhost:3000/inspect/knowledge-views) and
[evidence browser](http://localhost:3000/evidence). This demo builds cited context
projections from the project's architecture document in an isolated in-memory
store. It needs no database server or model credentials and binds to loopback.
The article wiki at `/wiki` stays empty in this build. To generate articles with
a configured model, see the [full demo](docs/architecture/living-wiki.mdx#runnable-projection-demo).
The first source build can take several minutes. Press Ctrl-C to stop it.

## Run a persistent local server

Docker Compose runs Elephant and SurrealDB. For semantic ingestion, copy
[.env.example](.env.example) to `.env` and configure `OPENAI_API_KEY` for the
Compose embedding provider and `ANTHROPIC_API_KEY` for its extraction model.
These calls can incur provider charges.

```bash
cp .env.example .env
# Edit .env with your local provider configuration before continuing.
docker compose up -d surrealdb
docker compose run --rm elephant elephant migrate
docker compose up -d elephant
curl -fsS http://localhost:3000/v1/runtime/status
```

Wait for `semantic_ingest_ready: true` before ingesting evidence. A successful
health request alone does not establish extraction readiness. The local
Compose configuration uses development authentication bypass; see
[deployment and authentication](docs/operations/deployment.mdx) before
exposing an instance to other users.

Elephant's public command is `elephant` ([CLI reference](docs/cli/overview.mdx)):

- `serve` runs HTTP, MCP, and managed workers together.
- `serve --server-only` runs HTTP and MCP without workers.
- `worker` runs separate workers against a shared SurrealDB server.
- `migrate` applies the SurrealDB schema.
- `status` checks a running server.
- `snapshot export/import` transfers a live embedded ElephantDB snapshot.

## Connect an MCP client

Use Streamable HTTP at `http://localhost:3000/mcp`. `elephant serve` is an HTTP
server, not a stdio MCP process. Configure the client with this URL and an
`Authorization: Bearer <token>` header when authentication is enabled.

Call `get_runtime_status` first. In an unconfigured runtime, loopback clients
can use `initialize_runtime`. Ingest tools become available only when storage,
workers, and a real semantic extractor are ready. Self-referential extraction
through `/mcp/agent` is retried after the listener binds.

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
| ElephantDB | `ELEPHANT_DB=elephant-db` | In-memory graph/vector kernel with explicit snapshot import/export |

Spaces isolate tenants. Sources and immutable document revisions retain the
evidence; extraction stages artifacts before committing assertions,
relationships, attributes, and events. Truth maintenance records agreement and
conflict over time. Living Wiki pages are derived views with citations,
authorization, and freshness receipts.

Embedded ElephantDB is volatile between snapshots. Load a previous snapshot
with `ELEPHANT_DB_SNAPSHOT_LOAD` and export before shutdown when persistence is
needed. SurrealDB is the normal durable deployment path.

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
    text: "Elephant can run as a library.".into(),
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
