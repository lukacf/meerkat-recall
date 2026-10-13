# Canonical job execution ancestry

A newly created ReprocessJob in a canonical commit inherits the exact original
queue attempt when the commit has a publication or maintenance attempt. After
read-only receipt replay, storage captures recovery's private
`PreparedReprocessParent` from that original Work, WorkRun and producer origin.
The same canonical transaction verifies that authority and inserts the job's
private `Inherited` execution origin with the job, effects, outbox and receipt.
Unknown/missing original queue history refuses the write. Archival and later
execution use recovery's original-history resolver; they do not adopt a new run.

The existing explicitly trusted unqueued Rust canonical path creates an
`Independent` intent through recovery's root constructor. Selecting an older
Watch-produced source generation does not inherit its service grant: content
selection and execution ancestry are separate. Signed/public entry points must
supply their actual admitted context; an inherited queued job cannot become
independent by editing public job fields, actor strings or current source heads.

The maintenance lane's actual both-backend budget oracle (checkpoint `3564007`)
creates a canonical child job under a real claimed parent and persisted WorkRun,
then checks inherited sticky/current restrictions, independent-root positive,
ended original parent history and substituted-run refusal. These hooks are a
source dependency of that oracle. No tests or Cargo ran in the integrity lane.
Standing-grant and record ABAC checks at all descendant/provider writes remain
separate required consumers of recovery's same bounded ancestry resolver.

Exact Source head/revision/operation payload lookups use recovery's bounded
read-only private-control accessor on the embedded backend. They no longer enter
`EmbeddedStore::mutate` during authority inspection. Actual transaction guards
still use the mutation's original snapshot. This change covers point lookups;
existing full control inventories remain separate scans and are not claimed to
be free of managed checkpoint activity. Read-only WAL/checkpoint behavior must
still be exercised in the central managed-storage gate.

Canonical queued maintenance now captures authority from the exact Work identified
by its original attempt. It uses the same recovery-owned ancestry resolver as
source finalization. Every stored Source, Doc, DocRevision, DocNode and
WorkArtifact dependency and effect-before row is authorized under **all**
applicable original standing principals and retained for same-transaction checks.
Proposed effect-after rows pass the intake owner's same pure policy mapper, but
are never represented as already persisted input rows.

The final prepared effect set is checked after retirement and continuation
expansion. Current original source inputs come from maintenance's shared
`PreparedStandingDispatchInputs`, which reuses intake's desired-document selector
and finalization's closed-manifest enumeration. Its head, present/absent document,
and manifest observations join the canonical transaction with distinct binding
namespaces per source; recovery retains its exact returned domain records. The
canonical transaction still compares and actually changes the original planning
epoch, fencing input membership. A proven Direct or Independent execution keeps
its original private ancestry guards without acquiring permissions or vetoes from
an unrelated historical content producer.

Eight authored both-backend regressions use a real queued Watch claim and durable
local delegation. Each performs an allowed canonical Doc update first, then
checks denied proposed Doc policy, denied original Doc policy, denied current
Source policy outside explicit plan dependencies, or paused original Watch.
Negative plans capture a fresh epoch after policy mutation and require unchanged
Doc plus no receipt. No provider call, Cargo command or Rust test execution was
performed by this implementation lane. This source depends on the coherent
recovery/intake/maintenance/finalization standing-authority family.

Read-only dispatch capture and verification use
`CanonicalRepository::current_epoch`, which requires an already existing epoch
and performs only a bounded private read. The older planning `epoch` API can
initialize missing state and enters an embedded mutation closure; it must not be
used to promise checkpoint-free observation. The new API does not grant space
permission or refresh an old captured epoch. Two authored backend tests check
real ordinary writer advancement, stable repeated observations, and refusal
without creating an epoch for a missing scope. Managed WAL behavior remains a
central durability validation obligation, not a local execution claim.

The outer execution observer can use `observe_epoch`, an exact global point read
returning `Option<String>`. It retains `None` as an original observation and must
compare the same absence after all authority/accounting reads. A new epoch is a
change, not an excuse to refresh the captured token. Existing foreign or malformed
physical rows refuse rather than masquerading as absence. `current_epoch` remains
the strict present-only wrapper for actual source inputs. Modern ordinary
SpaceConfig and ReprocessJob creation advance the epoch on both backends; epoch
absence still carries no conclusion about a source-less execution's authority.
Two additional backend test sources cover stable original absence, actual named
SpaceConfig creation invalidating that absence, and rejection of a foreign row
at the exact physical key. The execution resolver remains the sole authority.
