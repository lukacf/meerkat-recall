# Native generation recovery

Native engine erasure rebuilds the required retained key/value state into a fresh
generation. Logical deletion or compaction alone cannot establish that source
bytes have left engine logs, manifests or historical data files. The adapter
owns the definition of retained state, including unrelated version history when
the backend exposes it. The shared filesystem helper owns promotion and physical
retirement of the complete original directory.

The adapter acquires a stable OS owner lock before recovery or engine open and
retains it for the full datastore lifetime, including background and blocking
work. It stops transaction admission and drains all readers, writers and engine
handles before promotion. The helper does not acquire another lock or infer that
an engine has closed. The parent namespace must remain in the owner's custody;
path checks do not claim protection against a hostile concurrent replacement.

`Generation::prepare(live)` returns a private, empty staged directory through
`staged_path()`. The adapter rebuilds and verifies its exact retained native
state, flushes and closes the stage, and records its durable pending verification
proof outside the engine directory. It closes the original engine before calling
`promote(original_deadline, validate)`. The helper synchronizes all staged files and directories,
then records a payload-free promotion decision containing the original device
and both directory inode identities. The decision is written and synchronized
in a bounded temporary file, then atomically renamed and its parent synchronized.
A partial temporary is never interpreted as a promotion decision.
An uncommitted staged rebuild may be
abandoned; a durable promotion decision always rolls forward.

The original monotonic deadline is checked while walking and synchronizing the
stage and immediately before recording the irreversible decision. Once that
decision is durable, forward recovery and retirement drain under the persistent
owner even if the original deadline expires. A completed drain after expiry
returns a timeout; it does not renew the budget or authorize readiness.

Promotion renames the original directory to the owned retired location and the
stage to the original live path, synchronizing both parent directories. It calls
the adapter's validator with that final live path before deleting any retired
file. The validator must reopen using the actual engine options, independently
compare native state and identity against the durable pending proof, and close
the validation handle. Failure preserves the decision and remaining retired
files, withholds readiness and prevents erasure completion. A successful
validator does not alone complete erasure: every retired file must be removed,
the retired directory must be absent, and directory updates must be synchronized.

The private sibling control directory has a deterministic hash-derived name. It
contains a permanent ownership marker, an optional stage, an optional retired
generation and an optional promotion decision or decision temporary. Its marker contains no source
keys or values. Unknown entries, wrong identities, symlinks, hard links, device
crossings, special files and parent traversal are refused. Retirement first
validates the complete directory tree and removes only the captured files and
directories; it never follows links or recursively deletes an arbitrary path.
The engine-specific checkpoint certificate and stable OS lock are separate
adjacent files and remain under the adapter's authority.

When a native core datastore opens the unpublished stage, its fixed sibling
`.staged.recall-native-owner.lock` may also appear in the private control
directory. Recovery and promotion accept only that exact name with an empty,
single-link regular file on the original device. They must acquire its OS lock
nonblockingly before removing it and synchronizing the control directory. A held
lock, payload bytes or an alias blocks the transition. The original live owner
lock is never removed. The caller must still drain and close every staged engine
handle; acquiring this temporary sibling lock does not replace that obligation.

Startup calls `recover(live, validate)` under the same stable owner lock before
opening an engine or declaring readiness. Recovery without a promotion decision
abandons an incomplete stage and preserves the original engine for the original
erasure journal's retry. Recovery with a decision completes the forward renames,
revalidates the final path and finishes any partial retirement. It never restores
the erased generation over a committed replacement. A missing target, malformed
or incomplete marker, foreign control directory or impossible path combination
blocks startup; recovery does not invent successful completion or delete an
unproven directory. Before a first stage exists, initial ownership is installed by
the same synchronized-temporary and atomic-rename sequence. Under the caller's
stable OS lock, an interrupted initialization may resume only in a private,
same-device control directory containing no entries or only an exact bounded
prefix of its deterministic ownership temporary. Every foreign entry or mismatched
byte is retained and refused. An interrupted decision temporary similarly must
match the exact decision prefix for the observed original and staged directory
identities before it may be discarded along with the undecided stage.

The helper's tests define failures after target synchronization, durable decision,
each rename and parent synchronization, final validation, retirement, decision
removal and completion, including partial marker writes and the gap between a
marker rename and its parent synchronization. They also cover repeated recovery, failed final-path
verification, partial retirement, identity replacement, links, foreign entries
and subsequent generations. These filesystem controls do not replace engine
tests proving retained native state, transaction drainage or physical erasure.
