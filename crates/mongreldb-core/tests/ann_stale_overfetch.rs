use mongreldb_core::query::Retriever;
use mongreldb_core::schema::{
    AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind, IndexOptions, Schema,
    TypeId,
};
use mongreldb_core::{RowId, Snapshot, Table, Value};
use tempfile::{tempdir, TempDir};

fn schema(quantization: AnnQuantization) -> Schema {
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
            options: IndexOptions {
                ann: Some(AnnOptions {
                    quantization,
                    ..AnnOptions::default()
                }),
                ..IndexOptions::default()
            },
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn saturated_cluster(quantization: AnnQuantization) -> (TempDir, Table, Vec<RowId>, RowId) {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(quantization), 1).unwrap();
    let duplicate = vec![1.0_f32; 8];
    let mut cluster = Vec::new();
    for id in 0..64_i64 {
        cluster.push(
            table
                .put(vec![
                    (1, Value::Int64(id)),
                    (2, Value::Embedding(duplicate.clone())),
                ])
                .unwrap(),
        );
    }
    let outlier = table
        .put(vec![
            (1, Value::Int64(10_000)),
            (2, Value::Embedding(vec![-1.0_f32; 8])),
        ])
        .unwrap();
    table.commit().unwrap();
    table.flush().unwrap();
    (directory, table, cluster, outlier)
}

/// Saturating every old node's neighbor list with identical vectors must not
/// make a later, distant node unreachable from the graph entry point.
#[test]
fn late_outlier_remains_reachable_before_any_delete() {
    for quantization in [AnnQuantization::BinarySign, AnnQuantization::Dense] {
        let (_directory, mut table, _cluster, outlier) = saturated_cluster(quantization);
        let hits = table
            .retrieve(&Retriever::Ann {
                column_id: 2,
                query: vec![-1.0_f32; 8],
                k: 1,
            })
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "late {quantization:?} HNSW outlier became unreachable"
        );
        assert_eq!(hits[0].row_id, outlier);
    }
}

/// Deleted HNSW nodes remain in the graph by design. More stale nearest
/// neighbors than the candidate window must not hide the farther live row.
#[test]
fn stale_nearest_neighbors_do_not_exhaust_ann_overfetch() {
    for quantization in [AnnQuantization::BinarySign, AnnQuantization::Dense] {
        let (_directory, mut table, stale, survivor) = saturated_cluster(quantization);
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
                query: vec![1.0_f32; 8],
                k: 1,
            })
            .unwrap();

        assert_eq!(
            hits.len(),
            1,
            "stale nearest {quantization:?} graph nodes exhausted ANN candidate discovery"
        );
        assert_eq!(hits[0].row_id, survivor);
    }
}
