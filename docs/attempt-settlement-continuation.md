# Original attempt settlement continuation

The worker records an execution outcome before acknowledging queue completion.
A transaction conflict during that bookkeeping can leave a successful run
receipt beside a Running work item. Accounting recovery settles the receipt;
expired-lease recovery may still execute that work again. Producers therefore
remain responsible for idempotent effects and candidate lineage across attempts.

`attempt_settlement` keeps the same captured result alive for a bounded retry of
outcome persistence and usage settlement. All phases share at most 15 seconds,
capped by the remaining original lease. Each phase permits at most eight tries,
and each try rereads and compares the original claim before persisting. This is
in addition to the storage client's existing four atomic transaction attempts.
It does not rerun work, call a provider, create a new reservation, renew a lease,
or adopt a successor's claim. Canonical source and queue mutations retain their
existing transaction fences.

The read check does not add an atomic fence to historical `WorkRun` updates.
Such receipt writes retain their existing storage semantics; authority for
queue transitions and canonical publication still comes from their own fenced
transactions. No claim is made that bookkeeping and publication are one atomic
transaction.

Only the storage adapter's transaction-conflict diagnostic is retryable here.
Erasure, stale ownership, invalid input and other failures stop continuation.
Timeouts also stop: a cancelled persistence future may already have committed.
The original receipt and lease remain available to existing durable recovery.
Unknown provider usage retains its reservation under the unchanged accounting
rules; estimates never become measured consumption.

The focused source tests inject persistence conflicts, bound exhaustion, replace
the original queue claim, and retain a successful run through idempotent ledger
settlement and queue completion. No provider is required. This repair reduces
avoidable lease-length delays; it does not promise exactly-once execution or
replace producer replay checks after a process interruption.
