# Embeddings and retrieval policy

MongrelDB treats embedding **generation** as an optional pluggable layer. Core
storage and ANN indexes never hard-code an external embedding vendor.

## Sources

```rust
pub enum EmbeddingSource {
    SuppliedByApplication,
    LocalModel {
        model_path: PathBuf,
        model_id: String,
    },
    GeneratedColumn {
        provider: String,
    },
}
```

| Source | Who produces vectors | Needs registry provider? |
|--------|----------------------|---------------------------|
| `SuppliedByApplication` (default) | Client / application | No |
| `LocalModel` | Kit or operator-loaded local model | Yes (`model_id`) |
| `GeneratedColumn` | Server-registered provider (local or remote) | Yes (`provider`) |

Column catalog metadata may record an optional `embedding_source` on
`ColumnDef`. Storage still accepts plain `Value::Embedding` arrays on write.

## Recommended behavior

1. **Users may supply vectors directly.** This is the default path for dense
   ANN. Dimension and finiteness checks still apply at the write edge.
2. **MongrelDB-Kit may offer bundled local models.** Register them under a
   stable `model_id` via `EmbeddingProviderRegistry::register`.
3. **The server may register local or remote providers.** Process-local
   registry only; nothing is implied about a specific cloud vendor.
4. **Core storage remains independent of any embedding vendor.** There is no
   built-in OpenAI/Anthropic/etc. client in `mongreldb-core`.
5. **ANN indexes operate only on vectors and model metadata.** Generations
   may record `model_id` / source kind via `EmbeddingModelMeta`; the graph
   itself is built from stored `Value::Embedding` cells.
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
- `mongreldb_core::EmbeddingProvider` / `EmbeddingProviderRegistry`
- `Database::embedding_providers()`
- Server: `SHOW RESOURCE GROUPS` includes registered `embedding_providers`
