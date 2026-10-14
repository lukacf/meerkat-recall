# Recall native provider client patch

Published package: `meerkat-gemini-0.8.51`, MIT OR Apache-2.0. Registry archive SHA-256:
`528fae31a25fda3af5d9c4a01b65a6c439718068b4a380b34184437c1bb9cd89` (checked against Cargo.lock and the local registry archive).
Registry source commit: `1d5d844c8a75a70cbe90e62673dca5cd3621744e`, path `crates/meerkat-gemini`.
Source, normalized/original manifests and package tests copied from that released
archive; license texts copied from its exact source commit.

Narrow backport from upstream `a34b9fc7bbadd95e9284bcf1182d6d10bcc072be`: preserve shared HTTP client build
errors in the client and fail requests before any send rather than substitute
a default client. Base-URL changes preserve a new build failure too. Native
redirect handling remains in the separately pinned meerkat-llm-core helper.
No unrelated unreleased client implementation was copied. Public constructor
signatures and package versions remain unchanged. Browser redirect behavior
remains governed by the browser.

Regression source is included. No Cargo/tests or provider probes were run.
Retain this provenance and both licenses in source/artifact notices; remove
these pins after an equivalent upstream release is independently qualified.
