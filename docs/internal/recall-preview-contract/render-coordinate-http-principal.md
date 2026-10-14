# Mounted coordinate fixture authorization

Runtime17's original `actual_search_preserves_literal_raw_and_rendered_coordinates`
failed at the first mounted HTTP search with `403 forbidden_space`. The HTTP
fixture's explicit reader clearances covered the classified source/document
policies but omitted the configuration policy. The shared provider fixture
creates its original `SpaceConfig` with `BaseFields::new`, whose security envelope
is `default` at level zero. The mounted route authorizes this selected
configuration before searching evidence. Explicit clearances do not inherit a
legacy default-policy clearance.

The HTTP fixture now declares the same fixed `default`/zero clearance already
present in the MCP fixture, alongside its original classified document
clearances. It does so before reading any stored or returned record. A mounted
negative removes only that configuration clearance and requires the original
typed `403` response before the positive request. This denial occurs before any
semantic provider request, preserving the original dispatch count assertions.

The product route and policy engine are unchanged. The seven independent literal
fixtures, exact source/revision/document mapping, all raw/rendered byte and hash
assertions, full immutable GET, raw-scope and foreign-space refusals, and original
tamper checks remain. This is a fixture source repair following the sealed
failure; it does not claim a successful execution of the repaired Rust test.
