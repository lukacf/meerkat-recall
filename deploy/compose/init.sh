#!/bin/sh
# Run once before either service. Named secret volumes keep host bind UID
# differences out of the unprivileged service processes; never overwrite keys.
set -eu
umask 077
copy_secret() {
    source=$1 target=$2 owner=$3
    test -f "$source" && test ! -L "$source" || exit 1
    test "$(wc -c < "$source")" -ge 32 && test "$(wc -c < "$source")" -le 65536 || exit 1
    if [ -e "$target" ]; then
        test -f "$target" && test ! -L "$target" && cmp -s "$source" "$target" || {
            echo 'existing deployment secret differs; use the documented rotation procedure' >&2; exit 1;
        }
    else
        cp "$source" "$target"
    fi
    chmod 600 "$target"
    chown "$owner" "$target"
}
mkdir -p /recall-auth /database-auth
copy_secret /input/signing-key /recall-auth/signing-key 10001:10001
copy_secret /input/database-password /recall-auth/database-password 10001:10001
copy_secret /input/database-password /database-auth/database-password 65532:65532
chmod 700 /recall-auth /database-auth
chown 10001:10001 /recall-auth
chown 65532:65532 /database-auth
for path in data recovery backups model-cache model-assets config tmp logs; do
    mkdir -p "/state/$path"
    chown 10001:10001 "/state/$path"
done
chown 10001:10001 /state
if [ ! -e /state/config/runtime.toml ]; then
    cp /input/runtime.toml /state/config/runtime.toml
    chmod 600 /state/config/runtime.toml
    chown 10001:10001 /state/config/runtime.toml
fi
test -f /state/config/runtime.toml && test ! -L /state/config/runtime.toml || exit 1
mkdir -p /database-data
chown 65532:65532 /database-data
