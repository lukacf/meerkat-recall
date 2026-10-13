# Work queue completion wakeup

The worker previously waited a fresh configured polling interval after every
partial claim batch. A short task that durably enqueued its next causal stage
could finish immediately, but the next stage still waited for another poll.
The saved 0441 local records show about 29 seconds before rendering, consistent
with six five-second scheduling intervals. This is a source-supported timing
explanation, not a measured trace of individual stages or the cause of the
later budget-r1 terminal failure.

The active dispatcher now also waits for an owned JoinSet task to finish. A
completion wakes the existing batch claim loop after the task returns and
releases its permits. Completed tasks are coalesced before the claim pass. The
same queue selector, run_after eligibility, original attempt fencing, semaphore
limits, configuration blockage and retry transitions remain authoritative.
Completion is only a scheduling hint; it grants no publication authority.

The fallback timer persists across completion wakes, so a stream of short tasks
cannot postpone recovery sweeps, delayed retries or work from other producers.
When both events are ready the timer takes precedence. Only a timer wake runs
the source recovery sweep and resets its deadline. An empty JoinSet disables the
completion branch, avoiding an immediately-ready None loop. The existing outer
shutdown select cancels the pending wait and drains the same owned tasks.

Six focused controls cover completion before a distant fallback, an empty queue,
a long-running task, timer precedence and deadline preservation, cancellation
without losing owned work, and actual due/future-due queue claims with permit
release on both retained storage backends. The controls use manually polled
futures and explicit timer readiness rather than elapsed-time thresholds.
No model, provider, native qualification workload or polling cap is changed.

This is a separate prospective source candidate. The frozen 0441 native source,
its original qualification evidence and all 600/1200/1800 limits are retained.
Tests are authored; Root selects and executes validation before integration.
