# Source input and derived support

A Source row is the mutable projection of accepted input. Its desired generation,
immutable revision, authorization snapshot and original attempt govern mutation.
It is not a derived contribution supported by itself.

Previously, publication wrote a `source_effect_support` row for that same Source
ID. Because intake had already created the Source, this row was marked independent.
A subsequent accepted revision correctly changed the Source input and reset its
processing state, but source retirement compared the new input to the prior
publication's fingerprint and refused with `source_support_fingerprint_changed`.
The actual A -> pending B -> newly accepted A publication test exposed this path.

The canonical writer now excludes only a Source supporting its own physical ID.
Derived graph/document support and cross-source provenance keep their ordinary
fingerprint checks. Head CAS, immutable payload checks, authorization and attempt
fencing are unchanged. The source-input projection is still a canonical effect
inside publication; omitting its self-support row does not omit that effect.

The regression source includes an actual publication/replacement oracle owned by
finalization and a focused input-versus-derived contribution assertion. Runtime
validation is centralized; no Rust test was run in the integrity worktree. Old
unqualified candidate self-support rows are not silently rewritten or migrated.
