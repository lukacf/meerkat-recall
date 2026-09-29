# elephant-local-helper

A local MCP stdio server that reads UTF-8 files and sends their contents to an
Elephant HTTP server at `/v1/docs/ingest`. It lets a client ingest files that are
not mounted into the Elephant server's container or host.

## Run from source

Use a checkout of this repository and a Node.js runtime satisfying
`package.json` (Node 18 or later). Install the helper's dependencies once:

```bash
cd elephant-local-helper
npm ci
node index.js --url http://localhost:3000 --space default
```

The process speaks MCP over standard input/output. Configure your MCP client to
launch it; it is not an interactive shell. These instructions use the checked-in
source and do not assume an npm publication exists.

A client configuration supporting `mcpServers` can use:

```json
{
  "mcpServers": {
    "elephant-local": {
      "command": "node",
      "args": [
        "/absolute/path/to/elephant/elephant-local-helper/index.js",
        "--url", "http://localhost:3000",
        "--space", "default"
      ]
    }
  }
}
```

Supply `ELEPHANT_API_TOKEN` through your client's environment or secret settings
when the server requires authentication. The token must authorize the target
space; the helper's space option does not override JWT authority. Connect the
main Elephant MCP server separately at `/mcp` for retrieval tools.

## Tools

`ingest_file` reads one file. Optional fields are `collection`, `source_type`, and
`space_id`.

```json
{"path":"/path/to/reports/summary.md","collection":"reports"}
```

`ingest_directory` walks a directory and ingests matching files. Its optional
fields are `pattern` (default `**/*`), `collection`, and `space_id`.

```json
{"path":"/path/to/reports","pattern":"*.md","collection":"reports"}
```

Pattern matching supports `*`, `**`, and `?`. Use `*.md` for files immediately in
the directory and `**/*.md` for Markdown files below subdirectories. Hidden
directories and `node_modules` are skipped; hidden files are not excluded
universally. Use a dedicated input directory containing only intended files.

## Data handling and limits

Files are decoded as UTF-8 text. The helper does not parse PDFs, Office documents,
images, or other binary formats. Convert them to text before using it. The
`source_type` value is a metadata hint, not a binary parser selector.

The helper sends file contents, the absolute source path, inferred content type,
and size to the configured server. Successful responses return document IDs and
enqueue status, not proof that extraction completed. File text is sent directly
to Elephant rather than included in the tool's success response; later server
processing can send it to the configured model provider. Review both the source
files and the destination before ingestion.

Directory ingestion is sequential, can partially succeed, and reports per-file
errors. Repeated calls create new document IDs; do not assume reruns are
idempotent.

## Options

| Flag | Environment variable | Default |
|---|---|---|
| `--url` | `ELEPHANT_URL` | `http://localhost:3000` |
| `--space` | `ELEPHANT_SPACE_ID` | `default` |
| `--token` | `ELEPHANT_API_TOKEN` | Unset |

Prefer environment-based token configuration to passing credentials in process
arguments. Use HTTPS for a remote server.
