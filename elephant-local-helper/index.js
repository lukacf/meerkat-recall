#!/usr/bin/env node

/**
 * Elephant Local Helper
 *
 * A lightweight MCP server (stdio transport) that bridges the local filesystem
 * with a remote Elephant server. It exposes file and directory ingestion tools
 * that read files locally and forward their content to the Elephant API.
 *
 * Usage:
 *   npx elephant-local-helper --url https://elephant.example.com
 *
 * Claude Desktop / Claude Code config:
 *   {
 *     "mcpServers": {
 *       "elephant-local": {
 *         "command": "npx",
 *         "args": ["elephant-local-helper", "--url", "https://elephant.example.com"]
 *       }
 *     }
 *   }
 *
 * Environment variables:
 *   ELEPHANT_URL       - Elephant API base URL (default: http://localhost:3000)
 *   ELEPHANT_SPACE_ID  - Space to ingest into (default: "default")
 *   ELEPHANT_API_TOKEN - Bearer token for authentication (optional)
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { readFile, readdir, stat } from "node:fs/promises";
import { resolve, basename, extname, relative } from "node:path";

// Parse CLI args
const args = process.argv.slice(2);
let elephantUrl = process.env.ELEPHANT_URL || "http://localhost:3000";
let spaceId = process.env.ELEPHANT_SPACE_ID || "default";
let apiToken = process.env.ELEPHANT_API_TOKEN || "";

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--url" && args[i + 1]) elephantUrl = args[++i];
  else if (args[i] === "--space" && args[i + 1]) spaceId = args[++i];
  else if (args[i] === "--token" && args[i + 1]) apiToken = args[++i];
}

// Trim trailing slash
elephantUrl = elephantUrl.replace(/\/+$/, "");

/**
 * POST to the Elephant ingest endpoint.
 */
async function ingestDoc(title, text, collection, metadata = {}, targetSpace = null) {
  const effectiveSpace = targetSpace || spaceId;
  const headers = {
    "Content-Type": "application/json",
    "X-Space-Id": effectiveSpace,
  };
  if (apiToken) headers["Authorization"] = `Bearer ${apiToken}`;

  const body = {
    doc: {
      id: `doc:${crypto.randomUUID()}`,
      space_id: effectiveSpace,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      created_by: "elephant-local-helper",
      security: { policy_id: "default", level: 0, labels: [], handling: [] },
      provenance: { sources: [], chain: [] },
      subjects: [],
      subjects_state: "unknown",
      title,
      text,
      rendition_key: `local-${crypto.randomUUID()}`,
      metadata: { ...metadata, ...(collection ? { collection } : {}) },
    },
    collection: collection || undefined,
  };

  const resp = await fetch(`${elephantUrl}/v1/docs/ingest`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`Elephant API error ${resp.status}: ${errText}`);
  }

  return await resp.json();
}

/**
 * Infer source type from file extension.
 */
function inferSourceType(filePath) {
  const ext = extname(filePath).toLowerCase();
  const map = {
    ".md": "text/markdown",
    ".txt": "text/plain",
    ".json": "application/json",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
    ".html": "text/html",
    ".htm": "text/html",
    ".xml": "text/xml",
    ".csv": "text/csv",
    ".rs": "text/x-rust",
    ".py": "text/x-python",
    ".js": "text/javascript",
    ".ts": "text/typescript",
    ".toml": "text/toml",
    ".surql": "text/x-surql",
  };
  return map[ext] || "text/plain";
}

/**
 * Simple glob matching: supports *, **, and ? patterns.
 */
function globMatch(filepath, pattern) {
  if (pattern === "**/*") return true;
  const regexStr = pattern
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*\*/g, "\0")
    .replace(/\*/g, "[^/]*")
    .replace(/\0/g, ".*")
    .replace(/\?/g, "[^/]");
  return new RegExp(`^${regexStr}$`).test(filepath);
}

// Create MCP server
const server = new McpServer({
  name: "elephant-local-helper",
  version: "0.9.0",
});

// Tool: ingest_file
server.tool(
  "ingest_file",
  "Read a local file and ingest it into Elephant. The file is read from the local filesystem and sent to the remote Elephant server.",
  {
    path: z.string().describe("Absolute path to the file to ingest"),
    collection: z
      .string()
      .optional()
      .describe("Optional collection name for organizing documents"),
    source_type: z
      .string()
      .optional()
      .describe("Content type hint (e.g. text/markdown). Auto-detected from extension if omitted."),
    space_id: z
      .string()
      .optional()
      .describe("Target space ID to ingest into (defaults to configured space)"),
  },
  async ({ path: filePath, collection, source_type, space_id: targetSpace }) => {
    try {
      const absPath = resolve(filePath);
      const fileStat = await stat(absPath);

      if (!fileStat.isFile()) {
        return { content: [{ type: "text", text: `Error: ${absPath} is not a file` }], isError: true };
      }

      const content = await readFile(absPath, "utf-8");
      const title = basename(absPath);
      const sourceType = source_type || inferSourceType(absPath);

      const result = await ingestDoc(title, content, collection, {
        source_file: absPath,
        source_type: sourceType,
        size_bytes: fileStat.size,
      }, targetSpace);

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({
              ok: true,
              doc_id: result.doc_id,
              enqueued: result.enqueued,
              title,
              size_bytes: fileStat.size,
              space_id: targetSpace || spaceId,
            }),
          },
        ],
      };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
    }
  }
);

// Tool: ingest_directory
server.tool(
  "ingest_directory",
  "Ingest all matching files from a local directory into Elephant. Reads files from the local filesystem and sends them to the remote Elephant server.",
  {
    path: z.string().describe("Absolute path to the directory"),
    pattern: z
      .string()
      .default("**/*")
      .describe("Glob pattern to match files (default: **/*). Example: '*.md' or '**/*.txt'"),
    collection: z
      .string()
      .optional()
      .describe("Optional collection name for all ingested documents"),
    space_id: z
      .string()
      .optional()
      .describe("Target space ID to ingest into (defaults to configured space)"),
  },
  async ({ path: dirPath, pattern, collection, space_id: targetSpace }) => {
    try {
      const absDir = resolve(dirPath);
      const dirStat = await stat(absDir);

      if (!dirStat.isDirectory()) {
        return { content: [{ type: "text", text: `Error: ${absDir} is not a directory` }], isError: true };
      }

      // Walk directory recursively
      const files = [];
      async function walk(dir) {
        const entries = await readdir(dir, { withFileTypes: true });
        for (const entry of entries) {
          const fullPath = resolve(dir, entry.name);
          if (entry.isDirectory()) {
            // Skip hidden directories and node_modules
            if (!entry.name.startsWith(".") && entry.name !== "node_modules") {
              await walk(fullPath);
            }
          } else if (entry.isFile()) {
            const rel = relative(absDir, fullPath);
            if (globMatch(rel, pattern)) {
              files.push(fullPath);
            }
          }
        }
      }

      await walk(absDir);

      if (files.length === 0) {
        return {
          content: [{ type: "text", text: `No files matched pattern '${pattern}' in ${absDir}` }],
        };
      }

      const results = [];
      const errors = [];

      for (const file of files) {
        try {
          const content = await readFile(file, "utf-8");
          const title = basename(file);
          const sourceType = inferSourceType(file);
          const fileStat = await stat(file);

          const result = await ingestDoc(title, content, collection, {
            source_file: file,
            source_type: sourceType,
            size_bytes: fileStat.size,
          }, targetSpace);

          results.push({ file: relative(absDir, file), doc_id: result.doc_id });
        } catch (err) {
          errors.push({ file: relative(absDir, file), error: err.message });
        }
      }

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({
              ok: true,
              ingested: results.length,
              failed: errors.length,
              results,
              errors: errors.length > 0 ? errors : undefined,
            }),
          },
        ],
      };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
    }
  }
);

// Start server
const transport = new StdioServerTransport();
await server.connect(transport);
