#!/bin/sh
# A scoped token and selected space are deployment inputs, never printed.
set -eu
: "${RECALL_READINESS_TOKEN_FILE:?Set a current private read:status token file}"
: "${RECALL_READINESS_SPACE:?Select the status token's authorized space}"
exec meerkat-recall --api-url http://127.0.0.1:3000 \
    --token-file "$RECALL_READINESS_TOKEN_FILE" \
    status --require-ready --space "$RECALL_READINESS_SPACE" --json
