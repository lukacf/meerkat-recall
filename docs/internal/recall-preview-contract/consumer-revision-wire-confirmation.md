# Consumer confirmation: immutable revision wire and source spans

This is a source trace at next-product commit
`326f7d8595452b6e470a17252befb239d20dfba5`. It supplements the unchanged final
citation packet. It is not an observed response or qualified-build result.

The mounted `GET /v1/docs/{id}/revisions/{revision_id}` returns the full
`DocRevision` JSON. `docs.rs:2526` mounts the actual handler;
`doc_revisions.rs:80` encodes the authorized revision and `:123` calls
`serde_json::to_writer` directly on that DTO. No projection removes fields.
`crates/types/src/records/doc_revision.rs:157` derives `Serialize`; its
`status` at `:184` and `rendered_at` at `:187` have no serialization omission
attribute. Thus both are present. `status` is the snake-case enum string
(`active`, `superseded`, `failed`); `rendered_at` is the serialized UTC date-time
string. The `serde(default)` on status affects deserialization, not whether the
writer includes it. The v3 example deliberately contains an expected-field
projection, not an exhaustive response.

The top-level `space_id` is the revision's flattened `BaseFields.space_id`
(`doc_revision.rs:159`; `records/base.rs:178`). In the authenticated mounted
route it equals the selected, authorized `X-Space-Id` exactly. Header parsing
validates the original UTF-8 value without trimming or substitution
(`bin/elephant-api/src/auth.rs:277`); selection and signed grant binding occur
at `:325` and `:388`, with exact grant equality at `:129`. The handler reads both
records in that selected space (`doc_revisions.rs:30`, `:47`, `:52`), checks the
revision's parent and equal document/revision spaces (`:55`), and authorizes
both records for `read:doc` and `read:doc:raw` (`:58`). The shared policy engine
rejects any principal/record space mismatch (`crates/policy/src/engine.rs:117`).
The full original records and publication proof are rechecked before return
(`doc_revisions.rs:83`). A multispaced token requires explicit selection on this
route; the existing explicitly signed legacy single-space fallback is unchanged.

For every citation produced by scoped HTTP/MCP search, the raw source span is
nonempty and uses UTF-8 byte offsets. Its `end - start` equals the UTF-8 byte
length of `quote`. The complete current rendered evidence is first validated
(`crates/storage/src/read_eligibility.rs:425`). All claim, entity and chunk
results use the same production evidence assembler
(`crates/mcp/src/scoped_search.rs:522`), which validates the original accepted
source hashes and full rendered hash (`:591`), calls the exact renderer mapper
(`:605`), and obtains the returned quote directly from the original source
string slice (`:617`). It performs checked offset conversions and retains
current original observations before constructing the citation.

The two supported mappings share that guarantee:

- Default `elephant.renderer.text` version `1.0.0` requires an exact full rendered transform;
  `raw_cover` rejects empty or invalid rendered intervals and intervals supported
  only by generated text (`crates/types/src/rendered_text.rs:65`, `:164`). Every
  contributing original run has positive extent. CRLF and removed whitespace
  can make the raw cover longer than the rendered interval.
- `elephant.renderer.passthrough` version `1.0.0` requires original and rendered
  text equality, a nonempty interval and valid UTF-8 slice boundaries
  (`rendered_text.rs:181`). Unknown renderers/configurations refuse mapping.

Consumers must compare quote bytes to the **raw** span length, not the rendered
span length. A raw byte-length check alone does not prove that a repeated quote
came from the correct occurrence; the original binding, current server proof
and renderer mapping remain required. The separate passthrough literal fixture
is owned by grounding. Qualified-build and actual consumer/runtime evidence
remain outstanding; these conclusions do not replace those gates.
