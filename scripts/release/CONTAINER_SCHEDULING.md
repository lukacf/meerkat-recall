Container compiler scheduling
============================

The paired SurrealDB builder starts `.recall/build.py` through
`chrt --idle 0 ionice -c3`. This applies the required scheduling policy before
the Python process runs its Rust compiler identity probe, and descendants
inherit that policy. The existing separately prefixed Cargo build and its
resource limits remain in effect.

The Recall container's final provenance Python process uses the same prefix.
Its compiler version probe therefore inherits the build scheduling policy too.
The resulting provenance fields and compiler command arguments are unchanged.

Run the offline recipe regression checks with:

```sh
python3 -I -B scripts/release/test_container_compiler_scheduling.py
```

These checks inspect the actual shell command boundaries and do not start
Docker, the compiler, or subprocesses. An actual build still needs to establish
that its owned compiler processes satisfy the scheduling policy at runtime.
