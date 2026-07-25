use mongreldb_core::query::Retriever;
use mongreldb_core::schema::{
    ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId,
};
use mongreldb_core::{Snapshot, Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
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
                ty: TypeId::Embedding { dim: 8 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// Regression probe for the fixed-size `AnnIndex::search_filtered` over-fetch
/// window. Deleted HNSW nodes remain in the graph by design. More stale nearest
/// neighbors than the over-fetch window must not hide the farther live row.
#[test]
fn stale_nearest_neighbors_do_not_exhaust_ann_overfetch() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    let query = vec![1.0_f32; 8];

    // k=1 currently fetches max(4*k, k+16) == 17 candidates per layer.
    // Create far more exact stale neighbors so the first window can contain no
    // visible row at all.
    let mut stale = Vec::new();
    for id in 0..64_i64 {
        stale.push(
            table
                .put(vec![
                    (1, Value::Int64(id)),
                    (2, Value::Embedding(query.clone())),
                ])
                .unwrap(),
        );
    }

    // The only row left live is deliberately farther from the query.
    let survivor = table
        .put(vec![
            (1, Value::Int64(10_000)),
            (2, Value::Embedding(vec![-1.0_f32; 8])),
        ])
        .unwrap();
    table.commit().unwrap();
    table.flush().unwrap();

    for row_id in stale {
        table.delete(row_id).unwrap();
    }
    table.commit().unwrap();

    assert!(
        table.get(survivor, Snapshot::unbounded()).is_some(),
        "the farther survivor must be visible before ANN retrieval"
    );

    let hits = table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query,
            k: 1,
        })
        .unwrap();

    assert_eq!(
        hits.len(),
        1,
        "stale nearest graph nodes exhausted ANN's over-fetch window"
    );
    assert_eq!(hits[0].row_id, survivor);
}
