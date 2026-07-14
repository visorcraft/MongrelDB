ROWS = [
    [1, 1, 2, "published", 3, [[1, 2.0], [2, 1.0]], 4, [1, -1, 1, -1, 1, -1, 1, -1], 5, ["a", "b", "c", "d"]],
    [1, 2, 2, "draft", 3, [[1, 1.0], [3, 3.0]], 4, [-1, 1, -1, 1, -1, 1, -1, 1], 5, ["a", "b", "c", "x"]],
    [1, 3, 2, "published", 3, [[2, 5.0]], 4, [1, -1, 1, -1, 1, -1, -1, -1], 5, ["p", "q", "r", "s"]],
]
ROWS.extend(
    [
        1,
        row_id,
        2,
        "published" if row_id % 2 else "draft",
        3,
        [[100 + row_id, 1.0]],
        4,
        [1 if (row_id + bit) % 3 else -1 for bit in range(8)],
        5,
        [f"row-{row_id}-{member}" for member in range(4)],
    ]
    for row_id in range(4, 11)
)

CREATE = {
    "columns": [
        {"id": 1, "name": "id", "ty": "int64", "primary_key": True},
        {"id": 2, "name": "status", "ty": "bytes"},
        {"id": 3, "name": "sparse", "ty": "bytes"},
        {"id": 4, "name": "embedding", "ty": "embedding(8)"},
        {"id": 5, "name": "members", "ty": "bytes"},
    ],
    "indexes": [
        {"name": "status_bm", "column_id": 2, "kind": "bitmap"},
        {"name": "sparse_idx", "column_id": 3, "kind": "sparse"},
        {"name": "embedding_ann", "column_id": 4, "kind": "ann"},
        {"name": "members_minhash", "column_id": 5, "kind": "minhash"},
    ],
}

RETRIEVERS = {
    "ann": {"ann": {"column_id": 4, "query": [1, -1, 1, -1, 1, -1, 1, -1], "k": 1}},
    "sparse": {"sparse": {"column_id": 3, "query": [[1, 2.0]], "k": 1}},
    "minhash": {"min_hash": {"column_id": 5, "members": ["a", "b", "c", "d"], "k": 1}},
}
