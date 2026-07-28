# Production recipe: single-node HNSW Dense + auth

This is the **recommended first production profile** for MongrelDB as an
AI-native store: one daemon process, catalog authentication, and HNSW
nearest-neighbor over **real dense embeddings**.

It matches the product’s design defaults for algorithm and graph parameters,
overrides the engine’s memory-oriented default quantization (**BinarySign**)
with **Dense** for semantic quality, and assumes operators pin a release and
run their own soak.

Residual-closure calendar history and exact-SHA final-closure evidence are
**separate** from this recipe. This document does not claim residual-closed
status.

## When to use this recipe

Use it when:

- You run **one node** (daemon or embedded `Database`), not a sharded cluster.
- Workloads are **semantic / RAG-style** top-k over app- or provider-supplied
  vectors.
- Multiple identities must not see each other’s rows without grants (and
  optionally RLS).

Do **not** use this recipe as a promise of exact top-k under arbitrary churn.
HNSW is approximate; documented oracle floors for HNSW Dense are about
**0.90** recall@k on the deterministic corpus — validate on **your** model.

## Pin a version

Ship a **released tag** (for example `v0.64.13`) or a SHA you have soaked.
Do not treat an arbitrary mid-development `master` tip as the production
artifact.

## Index: HNSW + Dense

Engine defaults for the graph are already production-shaped:

| Parameter | Value | Notes |
|-----------|------:|-------|
| `algorithm` | `hnsw` | Default ANN algorithm |
| `m` | `16` | Default graph degree |
| `ef_construction` | `64` | Default build search list |
| `ef_search` | `64` | Default query search list |
| `quantization` | **`dense`** | **Set explicitly** (engine default is `binary_sign`) |

Typical query size for this product shape: **`k = 10`**.

Canonical column shape in embeddings docs: **`embedding(384)`**. Use the
dimension your model actually emits; keep one fixed dim per column.

### SQL

```sql
CREATE TABLE documents (
  id BIGINT PRIMARY KEY,
  embedding embedding(384) NOT NULL
  -- optional: title, owner, body, …
);

CREATE INDEX documents_embedding_ann
ON documents USING ann (embedding)
WITH (
  algorithm = 'hnsw',
  quantization = 'dense',
  m = 16,
  ef_construction = 64,
  ef_search = 64
);
```

### Rust schema sketch

```rust
use mongreldb_core::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef,
    IndexKind, IndexOptions, Schema, TypeId,
};

fn documents_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "embedding".into(),
                ty: TypeId::Embedding { dim: 384 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None, // or ConfiguredModel / GeneratedColumnSpec
            },
        ],
        indexes: vec![IndexDef {
            name: "documents_embedding_ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(AnnOptions {
                    m: 16,
                    ef_construction: 64,
                    ef_search: 64,
                    quantization: AnnQuantization::Dense,
                    algorithm: AnnAlgorithm::Hnsw,
                    ..AnnOptions::default()
                }),
                ..IndexOptions::default()
            },
        }],
        ..Schema::default()
    }
}
```

### When BinarySign is better

Keep `quantization = 'binary_sign'` (engine default) only when RAM or speed
dominates and you have measured Hamming-space quality for your vectors. It is
not the preferred quality path for general text embeddings.

`algorithm = 'hnsw'` + **product** quantization is a **flat PQ** path with
`hnsw` as a compatibility selector — not a full HNSW graph. Prefer Dense or
BinarySign HNSW for this recipe.

## Embeddings

- Prefer **real** model vectors (app-supplied or registered providers). See
  [Embeddings and retrieval](22-embeddings-and-retrieval.md).
- Do not invent pseudo-embeddings “to use Dense ANN.”
- ANN indexes only see **committed** vectors.

## Auth

1. Bootstrap with credentials so `require_auth` is enabled
   (`Database::create_with_credentials` or equivalent Kit/server path).
2. Create the first **admin** before exposing the process.
3. Run the daemon with user auth (and TLS if not localhost-only):

```bash
mongreldb-server ./data --port 8453 --auth-users
# optional shared token in addition to users:
# mongreldb-server ./data --port 8453 --auth-token "$TOKEN" --auth-users
```

4. Grant least privilege to ANN readers, for example `SELECT` on `documents`
   (and column grants if you use column-level permissions).
5. For multi-tenant rows, add RLS / policies as in
   [Users, roles & permissions](14-auth.md) and
   [Credential enforcement](15-credential-enforcement.md).

Unauthorized principals must not receive other tenants’ ANN hits. Residual
eligibility tests cover ranked ANN + authorization; still verify with your
roles before cutover.

## Process and data plane

- Run under **systemd** / supervisor with restart policy
  ([Daemon mode](08-daemon.md)).
- Listen on loopback or private network; terminate TLS at reverse proxy or
  daemon as appropriate.
- **Backup** the data directory; run one **restore** and re-check authorized
  ANN queries ([PITR](17-point-in-time-recovery.md) when you use archives).
- Size RAM for Dense HNSW (graph + full vectors) plus peak `ef_search` work.

## Semantics to design around

| Topic | Behavior |
|-------|----------|
| Approximate search | Expect ~0.90-class recall for HNSW Dense on healthy corpora; not exact |
| Snapshots across reopen | **Contract B:** in-memory snapshot handles do **not** survive reopen. After restart, query the **current** snapshot (or durable PITR), not a stashed `Snapshot` token |
| Tuning | If recall is soft on your model, raise **`ef_search`** first (e.g. 96–128) before raising `m` |
| Version | Pin release; plan upgrades deliberately (0.x) |

## Soak checklist (minimum)

1. Load real embeddings at your production dim under auth.
2. Authorized principal: HNSW **k=10**; sample recall vs brute force on a holdout.
3. Unauthorized principal: deny / empty — **no** foreign rows.
4. Update/delete ~10–20% of rows; re-query; quality still acceptable.
5. Restart the daemon; re-auth; re-query on the current snapshot.
6. Restore from backup once; repeat steps 2–3.

## CI lock

The integration test
`production_profile_hnsw_dense_auth` in
`crates/mongreldb-core/tests/production_profile_hnsw_dense_auth.rs`
locks the structural recipe: `require_auth`, HNSW+Dense with m/ef 16/64,
authorized ANN hits, unauthorized denial, and reopen under credentials.

It uses a small dim for speed; production should still use your model dim
(e.g. 384).

## Related docs

- [Indexes](06-indexes.md) — ANN matrix and recall floors  
- [Daemon mode](08-daemon.md) — process ops  
- [Users, roles & permissions](14-auth.md)  
- [Credential enforcement](15-credential-enforcement.md)  
- [Embeddings and retrieval](22-embeddings-and-retrieval.md)  
