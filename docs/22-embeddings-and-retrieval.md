# Embeddings and retrieval policy

MongrelDB treats embedding **generation** as an optional pluggable layer. Core
storage and ANN indexes never hard-code an external embedding vendor.

## Sources

```rust
pub enum EmbeddingSource {
    SuppliedByApplication,
    LocalModel {
        provider_id: String,
        model_id: String,
        model_version: String,
    },
    GeneratedColumn {
        spec: GeneratedEmbeddingSpec,
    },
}
```

| Source | Who produces vectors | Needs registry provider? |
|--------|----------------------|---------------------------|
| `SuppliedByApplication` (default) | Client / application | No |
| `LocalModel` | Kit or operator-loaded local model | Yes (`provider_id`) |
| `GeneratedColumn` | Server-registered synchronous provider | Yes (`spec.provider_id`) |

Column catalog metadata may record an optional `embedding_source` on
`ColumnDef`. `GeneratedColumn` stores provider/model identity, model version,
source columns, input template, dimension, normalization, and failure policy.
It never stores a node-local model path.

During insert or update, the commit path expands triggers and constraint
actions, applies write permissions and row-level security, builds canonical
provider input from the final source cells, reserves AI memory, validates the
provider result, and stages the vector before any WAL append. Provider failure,
cancellation, timeout, count mismatch, dimension mismatch, non-finite output,
or normalization mismatch aborts the complete source write. Replication carries
the materialized vector, so followers do not invoke the provider.

Only synchronous `AbortWrite` generation is currently exposed. No pending or
background-generation state is implied.

## Recommended behavior

1. **Users may supply vectors directly.** This is the default path for dense
   ANN. Dimension and finiteness checks still apply at the write edge.
2. **MongrelDB-Kit may offer bundled local models.** Register them under a
   stable provider ID via `EmbeddingProviderRegistry::register_new`.
3. **The server may register local or remote providers.** Process-local
   registry only; nothing is implied about a specific cloud vendor. Async
   runtime callers use `embed_async_controlled`; providers marked `Blocking`
   run on a bounded Tokio blocking pool, while `Remote` providers implement
   the async transport hook. Both paths honor execution cancellation and
   deadlines.
4. **Core storage remains independent of any embedding vendor.** There is no
   built-in OpenAI/Anthropic/etc. client in `mongreldb-core`.
5. **ANN indexes operate only on committed vectors.** Provider/model metadata
   is available through `EmbeddingModelMeta`; the graph itself is built from
   stored `Value::Embedding` cells.
6. **Sparse retrieval remains available with no embedding model.** The sparse
   inverted index is model-agnostic (SPLADE-style weights or any tokenizer).

## Do not invent dense vectors

**Do not invent arbitrary dense vectors merely to claim Dense ANN is being
used.** A weak hashed or random pseudo-embedding can perform worse than
MongrelDB’s native Sparse index while consuming more storage and creating
misleading “semantic search” expectations.

If no real model is available:

- Prefer **sparse** (or hybrid sparse + lexical) retrieval.
- Or require the application to supply real embeddings.

## APIs

- `mongreldb_core::EmbeddingSource`
- `mongreldb_core::GeneratedEmbeddingSpec`
- `mongreldb_core::EmbeddingProvider` / `EmbeddingProviderRegistry`
- `Database::embedding_providers()`
- Server: `SHOW RESOURCE GROUPS` includes registered `embedding_providers`
