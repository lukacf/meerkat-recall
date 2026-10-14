#!/bin/sh
# Fixed, optional secret-file adapters. Never print a secret or enable tracing.
set -eu
read_secret() {
    test -f "$1" && test ! -L "$1" || { echo 'secret must be a regular file' >&2; exit 1; }
    test "$(stat -c %a "$1")" = 600 || { echo 'secret must have mode 0600' >&2; exit 1; }
    test "$(wc -c < "$1")" -le 65536 || { echo 'secret exceeds 64 KiB' >&2; exit 1; }
    secret=$(cat "$1")
    test -n "$secret" || { echo 'empty secret' >&2; exit 1; }
}
if [ -n "${SURREALDB_PASS_FILE:-}" ]; then
    test -z "${SURREALDB_PASS:-}" || { echo 'choose database password or password file' >&2; exit 1; }
    read_secret "$SURREALDB_PASS_FILE"
    SURREALDB_PASS=$secret; export SURREALDB_PASS
fi
if [ -n "${MCP_BEARER_TOKEN_FILE:-}" ]; then
    test -z "${MCP_BEARER_TOKEN:-}" || { echo 'choose MCP bearer or bearer file' >&2; exit 1; }
    read_secret "$MCP_BEARER_TOKEN_FILE"
    MCP_BEARER_TOKEN=$secret; export MCP_BEARER_TOKEN
fi
if [ -n "${OPENAI_API_KEY_FILE:-}" ]; then
    test -z "${OPENAI_API_KEY:-}" || { echo 'choose OpenAI key or key file' >&2; exit 1; }
    read_secret "$OPENAI_API_KEY_FILE"
    OPENAI_API_KEY=$secret; export OPENAI_API_KEY
fi
if [ -n "${ANTHROPIC_API_KEY_FILE:-}" ]; then
    test -z "${ANTHROPIC_API_KEY:-}" || { echo 'choose Anthropic key or key file' >&2; exit 1; }
    read_secret "$ANTHROPIC_API_KEY_FILE"
    ANTHROPIC_API_KEY=$secret; export ANTHROPIC_API_KEY
fi
unset secret
exec "$@"
