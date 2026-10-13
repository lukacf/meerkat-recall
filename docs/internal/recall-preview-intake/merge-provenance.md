# Identity merge and physical evidence

An explicit identity merge joins the winner's security restrictions, subjects and contributing sources. New merge decision records carry that combined lineage. Existing factual records whose identity references change retain their original complete provenance and evidence span. Repointing a Source B assertion from Alice B to Alice A does not make Source A evidence for that assertion. Source-less manual facts remain source-less.

Rewritten records still inherit the survivor's security level, labels, handling and subjects, including conservative subject completeness. Preparation exposes both original and proposed authorization envelopes and retains the original canonical epoch and atomic commit path. This change does not relax physical evidence validation or source erasure proofs.

The two backend regression tests `embedded_merge_preserves_physical_assertion_provenance` and `surreal_merge_preserves_physical_assertion_provenance` prepare a real merge over two independently accepted immutable Sources. A typed B-only evidence fixture proves that identity rewriting preserves its full provenance and span, propagates restrictions, and keeps an independent manual relationship source-less. They inspect both authorization envelopes and verify preparation changes neither the stored assertion/Doc nor pending source receipts. These are preparation oracles; they do not fabricate publication or claim finalization. The existing contradictory-events end-to-end scenario exercises the committed merge and downstream exact evidence checks.

Validation at this checkpoint: direct rustfmt and `git diff --check` only. Cargo and test execution are delegated to the parent integration lane; the new tests are unexecuted.
