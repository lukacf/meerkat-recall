# Recall dependency maintenance patch

This is the complete crates.io `rav1e 0.8.1` archive. Its original license,
Rust sources, tests, and feature declarations are retained unchanged.
`RECALL_PROVENANCE.json` records the publisher archive checksum and source revision.

`Cargo.toml` resolves the existing `paste` import to `pastey =0.2.3`, removing
the abandoned `paste` package identified by RUSTSEC-2024-0436. The maintained
replacement exports both `paste!` and `item!`, including the entry points used
by this crate. The original `Cargo.toml.orig` remains an unmodified historical
manifest; Cargo uses the patched normalized `Cargo.toml`.

No advisory exemption, license metadata override, feature removal, or source
algorithm change accompanies this patch. Remove the local patch when a
compatible upstream release contains the maintained dependency change.
