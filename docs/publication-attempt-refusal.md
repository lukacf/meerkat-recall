# Publication attempt refusal

The shared native attempt fence throws the exact `stale_attempt` sentinel when
its original claim is cancelled, expired or superseded. Embedded transactions
already report a storage conflict. Native checked queries preserve the pinned
driver's `An error occurred: stale_attempt` text, which previously remained a
generic query error in source publication and document staging.

The private publication mapper now classifies that exact checked-query refusal
and its original structured native/remote driver equivalents as
`Conflict("stale_attempt")`. Other variants, incidental mentions, suffixes and
whitespace remain unchanged. Existing publication-prefixed errors and embedded
conflicts retain their behavior. The shared SQL fence, lease/generation checks,
transaction rollback and wire error authority are unchanged.

Two source controls cover exact native/remote/checked-query forms and negative
near matches. The existing embedded/native cancelled Watch producer tests remain
the actual staging/check and zero-effect regression oracles. Only formatting and
static checks were performed locally; central integration owns execution.
