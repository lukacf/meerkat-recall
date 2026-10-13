# Primary bootstrap transitive profile dependency

The original 0441 provider launch exited 78 before dispatch. Its bounded saved
bootstrap diagnostic identifies `load_helpers`' loaded-module guard. Source
tracing shows `recall_workload.py` unconditionally loads and registers the
hash-pinned `workload_profiles.py` through the installed helper loader, but
neither the seed primary plan nor bootstrap's required dependency set names it.
The file was already present in the selected 77-file packaged runtime.

Declare that one dependency in bootstrap's required set and in the active
primary input producer. The producer hashes the selected file into each plan,
which is then bound into the existing review/input inventory. Every existing
loaded-module, hash, canonical-path and source guard remains unchanged. No
blanket dependency allowlist, ignored module, outcome threshold, provider
budget, corpus or native binary changes. Prior failed originals are preserved.

The three Root-only pure controls compose the actual private producer with the
actual bootstrap loader, prove the registered profile is accepted at its exact
source hash, and refuse missing/wrong profile hashes and an unreviewed loaded
module. Network and process creation are forbidden. The test's own __main__
source is declared solely because the control runs under the repository root;
production already declares its bootstrap main. Root supplies exact producer
path/hash in the two RECALL_PRIMARY_PRODUCER_* variables. No provider credential
is involved. Authored tests and source validity are not semantic acceptance.
