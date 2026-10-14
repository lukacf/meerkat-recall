# Provider dispatch epoch conflicts

The closed 0441 full-local original completed sixteen publications, then failed
the budget-r1 embedding work. Its saved worker log reports the final physical
dispatch guard's exact "Provider dispatch inputs changed" refusal, carried
through the SDK callback as `recall_dispatch_terminal`. It did not reach the
publication or operation polling limits. The immutable failure and cleanup
receipt remain part of qualification history.

The guard captures the space's canonical epoch before reading original producer
ancestry, standing source authority and selected inputs. It refuses dispatch
when the final epoch differs. This predicate is deliberately coarse: unrelated
canonical writes can invalidate the read window. The failure log identifies
that comparison but does not identify the concurrent writer.

The comparison previously returned generic `PipelineError::InvalidState`, which
the existing typed failure classifier treats as terminal. It now returns a
typed storage conflict. The unchanged callback adapter preserves the transient
category through SDK string wrapping; the unchanged finite retry policy delays
another attempt and remains terminal on exhaustion. No error-text allowlist or
general invalid-state reclassification is introduced.

The failing dispatch still stops before the usage receipt or provider call.
No new epoch is adopted within that dispatch. A retry repeats the complete
original-authority and input capture, so changed work, revocation, source
classification, deleted content and private producer restrictions still refuse.
The read-only final epoch verifier is shared by the actual guard and deterministic
tests; no timing-based race or production test hook is needed.

New controls on both retained backends perform a real unrelated entity write
between epoch capture and final verification. They require refusal, bounded
retry/exhaustion, an unchanged original attempt, successful fresh validation of
unchanged authority, and refusal after the original source exceeds its ceiling.
A callback control checks embedding and SDK-wrapped extraction errors and keeps
the same wording in an invalid-state error terminal. Existing standing authority
and revocation controls remain in the focused selection.

This narrow repair is combined with the separately motivated completion wakeup
candidate. Neither changes source publication gates, provider bindings,
workload size, qualification limits or the frozen 0441 artifact. Tests are
authored; Root owns execution and acceptance of the coherent source batch.
