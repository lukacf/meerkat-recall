# Source view citation observations

Runtime21 reached both actual source publications and their authorized entity
merge in the original erasure projection scenario. Its first source view rebuild
then refused invalid or withdrawn evidence, before any erasure or history checks.
The previous stack overflow was no longer the observed failure.

The compiler records the immutable published Source timestamp as an explicit
dependency. The shared reader also checks every record-only citation. That
additional Source citation previously lost the captured timestamp and therefore
required the live Source to match the accepted snapshot semantically. Canonical
metadata completion adds normalized metadata and a title to the live row, so
even the original completed publication can legitimately differ from acceptance.
A pending replacement can differ further.

Source citations now reuse an unambiguous timestamp already recorded for the
exact Source ID and type in the revision's original dependency list. All original
dependencies and all citations remain checked. Missing or conflicting captured
observations retain the existing unbound comparison and fail closed when live
content differs. Other record kinds keep their original citation behavior.
No Source authority, content projection, security envelope, publication fence,
evidence span or timestamp comparison is weakened. Current desired input cannot
replace the originally cited publication.

Two pure controls cover canonical metadata differences, a pending successor, a
different completed publication, missing observations, conflicting observations
in both orders, foreign identities, wrong types and non-Source citations. These
do not create or certify any publication. The original populated erasure scenario
and all of its assertions remain unchanged and must still pass centrally on both
backends, through actual view publication, erasure and independent history checks.
This source change has only been formatted and reviewed locally; Runtime21's
failure remains the latest observed result until the next coherent runtime gate.
