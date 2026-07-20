# SQL scored-search decision

MongrelDB uses table functions that return requested document columns directly.

This avoids exposing hidden `_row_id`, `_epoch`, or `_deleted` columns and does
not create a privileged row-ID join path. A caller supplies table, indexed
column, query, `k`, and a comma-separated projection. The function returns the
projected columns plus typed score columns.

Examples:

```sql
SELECT id, body, ann_distance
FROM ann_search_scored('docs', 'embedding', '[1,-1,1,-1]', 20, 'id,body')
ORDER BY ann_distance, id;
```

BinarySign ANN indexes emit non-null `ann_distance` (`UInt32` Hamming). Dense
ANN indexes emit non-null `ann_cosine_distance` (`Float32`, `1 - cosine_similarity`)
instead — selected from the authoritative index schema. Dense scores are never
cast into the Hamming column.

Exact float-vector reranking is available when candidate recall needs float
refinement:

```sql
SELECT id, hamming_distance, exact_score
FROM ann_search_exact(
  'docs', 'embedding', '[1,-1,1,-1]', 100, 10, 'cosine', 'id'
)
ORDER BY search_rank;
```

For Dense indexes, `ann_search_exact` returns `ann_cosine_distance` in place of
`hamming_distance` for the ANN candidate field; `exact_score` keeps its
metric-dependent meaning.

```sql
SELECT id, sparse_score
FROM sparse_search_scored('docs', 'sparse', '[[1,2.0],[2,1.0]]', 20, 'id');
```

```sql
SELECT id, estimated_jaccard
FROM minhash_search_scored('docs', 'members', '["a","b"]', 20, 'id');
```

```sql
SELECT id, estimated_jaccard, exact_jaccard
FROM set_similarity_scored(
  'docs', 'members', '["a","b"]', 100, 0.8, 20, 'id'
);
```

`hybrid_search_scored(table, request_json, projection)` wraps the core
`SearchRequest`. Named `ann`, `sparse`, and `minhash` retrievers are unioned
and fused with RRF. The result adds `search_rank`, `fused_score`, and a JSON
`components` column containing each retriever's rank, raw score, and RRF
contribution.

Table functions obey normal table and column authorization. System columns stay
hidden. Retrieval runs when the physical scan executes, so cached logical plans,
prepared statements, and views see current rows, indexes, roles, policies, and
masks. Boolean ANN and Sparse predicates remain available only to trusted
embedded SQL. Remote `/sql` and prepared SQL reject them; use the scored table
functions so timeout, work, and concurrency limits always apply.
