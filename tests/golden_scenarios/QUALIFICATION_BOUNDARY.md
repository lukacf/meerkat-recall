# Original-query evidence boundary

The legacy live runner keeps its original scenario thresholds. Its acceptance
path now uses the original query verbatim and supplies only the corresponding
retrieved documents/events to the judge. It no longer substitutes normalized or
scenario-anchored queries, fetches known ingested document IDs for judge context,
or supplies unfiltered assertions, truth slots or timeline rows as retrieval
rescue. Those graph diagnostics remain recorded separately.

A below-threshold judge score, a fail/unavailable verdict, disabled judge,
extraction/embedding timeout or failed work item is a failure. High graph counts
cannot make these conditions advisory. Judge score fields must be numeric within
[0,1]. Acceptance mode is the default and exits nonzero on a failed scenario.
`--exploratory` is explicitly diagnostic and never emits semantic acceptance.
Missing live credentials or required judge/embedding configuration produces an
`unmet_prerequisite` receipt and exit 2 before service/model execution. The future
live run must explicitly select `--allow-paid-providers` under its own authorization.

Compose now names the retained `elephant` service, and migration uses
`meerkat-recall-bin` through the worktree Cargo wrapper with idle scheduling.
These source fixes are not an executed container/migration receipt. Named-space
approved provider configuration must be provisioned through the current owner
contract before running; conventional environment keys alone confer no per-space
processing authority. The runner's existing reset/start workflow remains a local
isolated-fixture workflow, never a production procedure. No script in this repair
was run against Recall, a database, external datasets or a model provider.

This older corpus mixes mutable external feeds with synthetic meeting scaffolding,
and its legacy routes report aggregate diagnostics. It is useful for its declared
scenario checks; it is not the independently frozen preview known-answer corpus,
provider relevance benchmark, exact artifact acceptance, source-head/erasure
qualification or consumer adoption. The independent qualification fixtures and
receipts for those gates are outside this selected public source distribution.
Use the actual release qualification record for executed evidence. No repaired
shell exit status changes that evidence scope.

The private offline regression harness sources the actual shell functions with
model/network/service spies, checking that sparse results, below-threshold
judgment, unknown judgment and original query identity cannot turn into
acceptance. Those are evaluator self-tests, never product test results. That
harness is not included here. Merely sourcing the script no longer executes `main`.
