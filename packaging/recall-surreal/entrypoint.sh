#!/bin/sh
# Upstream CLI accepts SURREAL_USER/SURREAL_PASS; keep passwords out of argv.
set -eu
if [ -n "${SURREAL_PASS_FILE:-}" ]; then
    test -z "${SURREAL_PASS:-}" || { echo 'choose database password or password file' >&2; exit 1; }
    test -f "$SURREAL_PASS_FILE" && test ! -L "$SURREAL_PASS_FILE" || exit 1
    test "$(stat -c %a "$SURREAL_PASS_FILE")" = 600 || exit 1
    test "$(wc -c < "$SURREAL_PASS_FILE")" -le 65536 || exit 1
    SURREAL_PASS=$(cat "$SURREAL_PASS_FILE")
    test -n "$SURREAL_PASS" || exit 1
    export SURREAL_PASS
fi
exec /usr/local/bin/recall-surreal "$@"
