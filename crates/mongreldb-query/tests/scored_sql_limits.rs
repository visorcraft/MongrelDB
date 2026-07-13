use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;

async fn session() -> (tempfile::TempDir, MongrelSession) {
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
                },
                ColumnDef {
                    id: 2,
                    name: "embedding".into(),
                    ty: TypeId::Embedding { dim: 8 },
                    flags: ColumnFlags::empty(),
                    default_value: None,
                },
                ColumnDef {
                    id: 3,
                    name: "sparse".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                },
            ],
            indexes: vec![
                IndexDef {
                    name: "docs_ann".into(),
                    column_id: 2,
                    kind: IndexKind::Ann,
                    predicate: None,
                    options: Default::default(),
                },
                IndexDef {
                    name: "docs_sparse".into(),
                    column_id: 3,
                    kind: IndexKind::Sparse,
                    predicate: None,
                    options: Default::default(),
                },
            ],
            ..Schema::default()
        },
    )
    .unwrap();
    db.transaction(|transaction| {
        transaction.put(
            "docs",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Embedding(vec![1.0; 8])),
                (
                    3,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(1, 1.0)])?),
                ),
            ],
        )?;
        Ok(())
    })
    .unwrap();
    let session = MongrelSession::open(db).unwrap();
    (dir, session)
}

#[tokio::test]
async fn scored_sql_limits_fail_with_errors() {
    let (_dir, session) = session().await;
    assert!(session
        .run("SELECT * FROM sparse_search_scored('docs','sparse','[[1,1.0]]',100001,'id')")
        .await
        .is_err());
    assert!(session
        .run("SELECT * FROM ann_search_exact('docs','embedding','[1,1,1,1,1,1,1,1]',100001,1,'cosine','id')")
        .await
        .is_err());

    let projection = std::iter::repeat("id")
        .take(mongreldb_core::query::MAX_PROJECTION_COLUMNS + 1)
        .collect::<Vec<_>>()
        .join(",");
    assert!(session
        .run(&format!(
            "SELECT * FROM ann_search_scored('docs','embedding','[1,1,1,1,1,1,1,1]',1,'{projection}')"
        ))
        .await
        .is_err());

    let retrievers = (0..=mongreldb_core::query::MAX_RETRIEVERS)
        .map(|index| {
            serde_json::json!({
                "name":format!("dense{index}"),
                "weight":1.0,
                "ann":{"column":"embedding","query":[1,1,1,1,1,1,1,1],"k":1}
            })
        })
        .collect::<Vec<_>>();
    let request = serde_json::json!({"retrievers":retrievers,"limit":1});
    assert!(session
        .run(&format!(
            "SELECT * FROM hybrid_search_scored('docs','{}','id')",
            request.to_string().replace('\'', "''")
        ))
        .await
        .is_err());

    let request = serde_json::json!({
        "retrievers":[{
            "name":"dense","weight":f64::MAX,
            "ann":{"column":"embedding","query":[1,1,1,1,1,1,1,1],"k":1}
        }],
        "limit":1
    });
    assert!(session
        .run(&format!(
            "SELECT * FROM hybrid_search_scored('docs','{}','id')",
            request.to_string().replace('\'', "''")
        ))
        .await
        .is_err());
}

#[tokio::test]
async fn boolean_ai_udfs_fail_closed_without_pushdown() {
    let (_dir, session) = session().await;
    let error = session
        .run("SELECT ann_search(1,'[1,1,1,1,1,1,1,1]',1)")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("ann_search requires MongrelDB index pushdown"));
}

#[tokio::test]
async fn exact_ann_rerank_is_available_in_scored_sql() {
    let (_dir, session) = session().await;
    let batches = session
        .run("SELECT * FROM ann_search_exact('docs','embedding','[1,1,1,1,1,1,1,1]',10,1,'cosine','id')")
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let score = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float32Array>()
        .unwrap()
        .value(0);
    assert!(score.is_finite());
}
