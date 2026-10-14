# Native object field navigation

The native ancestry qualification still uses its original 240-second deadline,
256-work boundary, 257-source boundary, and per-invocation reference fences. The
R29 run did not meet that deadline. A bounded diagnostic of its exact compiled
binary repeatedly sampled allocation, copying, and destruction of `Value::get`
futures during ordinary field access. These samples motivate this change; they
do not establish exclusive CPU attribution or a successful performance result.

`Value::get` now keeps ordinary `Object` plus `Field` navigation in a small
wrapper. It first applies the original full path-length check, then borrows each
existing child without evaluating it. An empty remaining path clones the value,
as the original evaluator did. The caller still computes that result with its
original context and options. No computed value or database read is cached.

The wrapper stops before a missing field, a complex record ID, any other value
type, or any other path part. It passes the same borrowed value, remaining path,
context, options, and cursor to the original evaluator body through the native
stack. Futures, dynamic indices, arrays, remote records, methods, graph paths,
references, and recursion retain their original evaluation. The borrowed local
prefix performs no effects and consumes no `Options` computation depth; the
original recursive field branches likewise consumed none. Their repeated
path-length checks only examined shorter suffixes of the already checked path.

Keeping the full evaluator future outside the wrapper avoids embedding its
large state in every ordinary caller and scheduling a new task for each local
field. The original slow match body remains byte-for-byte unchanged. The change
does not alter reference guards, transaction writes, events, query permissions,
normalization, or the original qualification fixture.

`crates/storage/tests/native_value_navigation.rs` exercises the public native
SDK: exact typed values against dynamic-index paths, complex object/array record
IDs, array and method fallback, fresh remote reads, stored-future effects and
rollback, dynamic-path effect order and first error, and the original path
limit before an absent field. These are ordinary workspace integration tests
under the `native_value_navigation` target. The source checkpoint adds five
controls; execution and the original ancestry gate remain central validation
work. No local Cargo, compiler, or test execution was used to author this batch.
