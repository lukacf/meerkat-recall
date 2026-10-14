# Truth-maintenance decision fingerprints

Claim IDs identify membership; they do not identify the inputs to a semantic
decision. Recall separately hashes selected claim values, confidence, evidence
references and assertion contents, predicate semantics, and the adjudicator
model and prompt version. The hash is stable under record ordering and does
not include unrelated corpus records. Literal array order remains meaningful.

The resolver checks this fingerprint before reusing a committed decision.
Detection may schedule a check for the same IDs because it does not own the
current adjudicator configuration. The commit path rejects stale semantic
inputs before applying claim effects. Verdict generation alone does not install
a completion memo. The transaction owner records the memo with the successful
canonical outcome.

A fingerprint is not evidence of support. Missing evidence has an explicit
representation in the fingerprint; source-backed approval still requires the
publication and maintenance grounding checks. Likewise, a source-less
operator assertion is distinct from a model interpretation derived from a
source. Their support authorities must not be interchanged.

Security policy, classification, handling restrictions, subject scope and source
support partitions isolate conflict groups. Contradictions across those
partitions are deliberately not adjudicated together. No generic conjunction
between different policy identifiers is inferred.

Deterministic source tests exercise same-ID semantic changes, ordering, and
unrelated-corpus isolation. They are separate from live adjudicator accuracy
and atomic failure-boundary qualification.
