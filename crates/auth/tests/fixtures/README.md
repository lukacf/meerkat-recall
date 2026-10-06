# Canonical principal wire fixture

Extracted without semantic edits from Meerkat's generated
`artifacts/schemas/wire-types.json`, PrincipalRef root, on 2026-10-01.
Source bundle SHA-256: `f8671675e08e6d6dd0899955089067a4b905ae0d7423c10614e4a319029b398f`.
Source commit: `e4ada6d943e0b12dcfa2fb309c1f72967bc44428`.
Canonical owner: `meerkat-core::auth::PrincipalRef`.

This package accepts only the closed qualified subset at an authenticated
exchange boundary. Unqualified legacy principals and unknown extra fields
accepted by the broader canonical compatibility schema are deliberately refused.
The fixture is wire conformance evidence, not identity or grant authority.
Re-extract it when the versioned protocol changes; do not independently edit
its rules to make an Elephant test pass.
