use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;

fn insert(db: &Database, id: i64) {
    db.transaction(|transaction| {
        transaction.put(
            "docs",
            vec![
                (1, Value::Int64(id)),
                (
                    2,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(1, 1.0)])?),
                ),
            ],
        )?;
        Ok(())
    })
    .unwrap();
}

fn row_count(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::test]
async fn scored_udtf_is_live_through_plan_prepare_and_view_caches() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
        "docs",
        Schema {
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
                    name: "sparse".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "sparse".into(),
                column_id: 2,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            }],
            ..Schema::default()
        },
    )
    .unwrap();
    insert(&db, 1);
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    let sql = "SELECT * FROM sparse_search_scored('docs','sparse','[[1,1.0]]',10,'id')";
    assert_eq!(row_count(&session.run(sql).await.unwrap()), 1);
    insert(&db, 2);
    assert_eq!(row_count(&session.run(sql).await.unwrap()), 2);

    session
        .run(&format!("PREPARE live_scored AS {sql}"))
        .await
        .unwrap();
    assert_eq!(
        row_count(&session.run("EXECUTE live_scored").await.unwrap()),
        2
    );
    insert(&db, 3);
    assert_eq!(
        row_count(&session.run("EXECUTE live_scored").await.unwrap()),
        3
    );

    session
        .run(&format!("CREATE VIEW live_scored_view AS {sql}"))
        .await
        .unwrap();
    assert_eq!(
        row_count(&session.run("SELECT * FROM live_scored_view").await.unwrap()),
        3
    );
    insert(&db, 4);
    assert_eq!(
        row_count(&session.run("SELECT * FROM live_scored_view").await.unwrap()),
        4
    );
}
