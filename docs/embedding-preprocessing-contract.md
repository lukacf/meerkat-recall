# Explicit embedding preprocessing profiles

`ModelProfile.preprocessing` selects a versioned vector representation. Existing
profiles and historical receipts are immutable; a new recipe needs a new profile
ID. Adding that profile does not activate it. Populated spaces use the existing
checked reindex and activation transition. The vector partition includes space,
provider, account, endpoint, profile, model version, dimensions and preprocessing.

| Recipe | Providers | Query input | Document and reindex input |
| --- | --- | --- | --- |
| `text-v1` | Existing supported embedding providers | Complete original text | Complete original text |
| `text-full-v2` | OpenAI, FastEmbed | Complete text through bounded segmentation and pooling | Complete text through bounded segmentation and pooling |
| `bge-en-retrieval-v1` | FastEmbed English BGE v1.5 models | Fixed retrieval instruction followed by complete original text | Complete original text |

`text-v1` remains the direct OpenAI configuration default. It sends complete input
or reports oversize; it never silently keeps a prefix. `text-full-v2` preserves
all original UTF-8 bytes through bounded segmentation, aggregates complete
successful segment vectors using byte-weighted f64 means and L2 normalization,
and retains current authority and accounting for every physical call. See the
[full-input execution contract](embedding-full-input-runtime.md).

## English BGE retrieval

`bge-en-retrieval-v1` is an explicit opt-in for `bge-small-en-v1.5` (384 dimensions),
`bge-base-en-v1.5` (768), and `bge-large-en-v1.5` (1024), including each `-q` variant.
Other providers, models, dimension combinations and non-vector profiles reject
this recipe. OpenAI's direct provider also rejects it before sending a request.

Every `query_embedding` input receives this exact instruction, including one
trailing ASCII space, followed by every original query byte:

```json
"Represent this sentence for searching relevant passages: "
```

The approved operation determines the transform. `document_embedding` and
`reindex_embedding` inputs retain their original bytes. There is no branching on
query content, no generated rewrite and no special treatment for a query that
already contains the instruction. Each invocation applies the transform once.
The public search request and returned immutable source evidence remain unchanged.

The [pinned BAAI model card](https://huggingface.co/BAAI/bge-small-en-v1.5/blob/5c38ec7c405ec4b44b94cc5a9bb96e735b38267a/README.md)
recommends an instruction for short-query retrieval and also supports unprefixed
v1.5 queries. This recipe adopts that recommendation as a separate representation;
it does not change `text-v1` or imply that an existing unprefixed profile is invalid.
The existing model-specific CLS pooling, quantization choice and L2 normalization
remain unchanged.

The actual tokenizer counts the transformed query, including instruction and
special tokens, before inference. Oversize inputs fail; this recipe does not
silently truncate or segment them. Document token counts exclude the instruction.
Measured usage includes every token actually presented before batch padding.
Model resources contain no operation, query text or reusable authority. Each
request's pinned dispatch supplies its operation, and current operation/profile
approval is rechecked after model-lock and blocking-pool waits, before inference.
A missing operation or changed approval cannot dispatch a prepared query.

## Provision and activate

Provision already selected model files before an offline service run. For example,
using an existing BGE-small bundle as the source of unchanged model/tokenizer bytes:

```sh
scripts/release/provision-fastembed \
  --root /srv/recall/model-assets \
  --model bge-small-en-v1.5 \
  --preprocessing bge-en-retrieval-v1 \
  --onnx /srv/approved-bge/onnx/model.onnx \
  --tokenizer-dir /srv/approved-bge
```

This command performs no download, inference or provider approval. It packages
copies into a fresh content-addressed directory, records each copied file's size
and SHA256, and emits the endpoint and new `model_version`. Its manifest records
the new recipe and its own asset layout; it must never overwrite or relabel the
old manifest. Review all five asset digests against the selected input bundle.
The emitted `model_version` hashes the manifest bytes, not the ONNX file alone.
Mount the provisioned asset root read-only during the service run.

Create a new immutable profile with the emitted `model_version`,
`preprocessing: "bge-en-retrieval-v1"`, matching model/dimensions and the approved
local connection. Fresh spaces bind both query and document operations to this
same profile. For populated spaces, add the profile, approve the existing reindex
operation and complete the repository's checked reindex/activation flow. Do not
rename existing stored vectors, mutate a profile in place, or bind queries to a
different active partition from documents. Numerically unchanged document vectors
still belong to the new profile and partition.

Inspect the selected space's status. Both embedding operations must be approved
and available, and their `embedding_execution_recipes` must show the expected
partition and operation. The existing `partition.preprocessing` plus
`provenance.operation` identifies the complete deterministic recipe. Overall Graph
`semantic_ingest_ready` additionally requires its generative operations; embedding
readiness alone does not establish it.

Token, manifest, dispatch and partition controls establish implementation
invariants. They do not establish retrieval quality. A changed recipe still needs
qualification against the complete unchanged query/corpus set, evidence and
security requirements, and each supported public transport. Record the original
query and the explicit transformed-input identity when collecting inference
receipts. Preserve earlier failures and all fixed relevance thresholds.
