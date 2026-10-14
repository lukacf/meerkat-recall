# Independent configuration publication review

D reviewed L05af7b6c58fbca6507aaedf94cf55220cd4ef172 and
Cbdfbf0ba5e9cbdc62e3ce368a6fdd490148f5eee as one source group.
These changes repair the active runtime configuration writer and its actual
initialize/configure callers. D did not execute their Rust tests.

The writer uses the shared storage atomic-write core. A private0600 exclusively
created temporary file is written, flushed and synced before replacement, then
the parent directory is synced. Pre-replacement failure preserves prior bytes;
post-replacement directory-sync failure is explicitly `CommitUncertain`. Cleanup
removes only the temporary file this call created. Existing cross-directory
checkpoint temporary-path support remains covered. Configuration parent traversal,
symlink components and nonfile destinations are refused, and existing parent
permissions are unchanged. The documented precondition is a caller-controlled
directory and serialized writers; path checks do not exclude a concurrent actor
with independent directory mutation authority.

Actual runtime callers prepare without starting workers, then save before
activation. An uncertain replacement makes runtime readiness false, drains the
owned worker and requires restart. The helper operates under the existing
reconfigure lock and does not recursively call shutdown. A saved configuration
followed by prior-worker stop failure leaves `workers_drained=false`, refuses
snapshot publication and does not activate the prepared replacement.

The11 L and4 C test definitions exercise partial temporary ENOSPC, failure after
file sync/before rename, post-rename directory-sync uncertainty, first save,
parseable old/new configuration, exact permissions, path refusal, exclusive
ownership, one-shot fault scope, active-store identity and actual worker join/
shutdown failure. These are deterministic source fault injections, not an
installed binary on an OS-full filesystem. D22 still requires the bounded
installed fault evidence; D23 canonical migration rollback remains separate
from optional extensions and installed initialization error observations.

One retained-platform fixture defect was reported to L/C: conventional Darwin
`/var` and `/tmp` aliases can make temporary test roots themselves symlinked.
The owned existing test root must be canonicalized before exercising this writer,
while explicit symlink-negative children must remain symlinked. Production path
checks should not be relaxed to repair a fixture. Darwin installed acceptance
is still unexecuted.
