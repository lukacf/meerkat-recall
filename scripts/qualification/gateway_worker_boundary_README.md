# Gateway worker startup boundary repair

The closed 7147 provider2 original produced its actual bridge context but no
per-backend startup tree, then exited 78 with zero dispatches. Existing bridge
and bootstrap catches discarded exception class and frames. The saved result
does not identify its exact exception. Independently, source proves bootstrap
requires `/work/isolation.json`, while the selected bridge had no producer.

The bridge now validates and hash-checks its actual persisted context and
projects its observed network fields into the existing isolation format before
starting the worker. Exclusive private creation preserves the original proof.
The credential-absence field describes the retained launch boundary: cleared
child environment, closed descriptors and synthetic gateway capabilities only.
It is not a new scan or proof that arbitrary provider secrets can be discovered.
All original namespace, current-network, context, anchor and deadline checks
remain in force. No request, timeout, provider or acceptance threshold changes.

Bridge failures write `/work/bridge.failure.json`; bootstrap failures write
`/work/bootstrap.failure.json`. Files use exclusive no-follow 0600 creation,
file and parent fsync, a 16 KiB cap, and at most 16 relative source frames.
Only a fixed stage, exception class, selected source file/function/line and
actual child return code are recorded. No exception messages, source lines,
frame locals, arguments, child output, credentials or provider bodies are
recorded. A child nonzero exit is identified as a child exit, not mislabelled
as a bridge exception. Reporting failure preserves the original exit status.

The four named pure controls cover both actual exception handlers, the shared
writer, existing bootstrap isolation consumer, original-file preservation,
private mode and changed context/network/clock/anchor refusal. Network and
process construction are forbidden by test guards. Context fixtures are
synthetic and explicitly provide no provider or installed semantic acceptance.
Root alone selects and executes the controls after G owns the source checkpoint.
S authored source only; no AST/import/test/runtime/provider execution occurred.

A bootstrap import-time error before its main function remains distinguishable
only as a bridge child return; this patch does not capture Python stderr. The
packaged dependency set and normal source review remain the import boundary.
