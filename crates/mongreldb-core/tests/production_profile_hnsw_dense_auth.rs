//! Production recipe lock: single-node HNSW Dense + catalog auth.
//!
//! Mirrors `docs/24-production-single-node-hnsw-auth.md`:
//! - `require_auth` via `create_with_credentials`
//! - ANN index: HNSW + Dense, m=16, ef_construction=64, ef_search=64
//! - authorized principal receives ANN hits
//! - principal without SELECT is denied
//! - reopen under credentials still serves ANN on the current snapshot
//!
//! Dim is small for CI speed; production should use the model dim (e.g. 384).

use mongreldb_core::auth::Permission;
use mongreldb_core::query::{Condition, Query, Retriever, RetrieverScore};
use mongreldb_core::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind,
    IndexOptions, Schema, TypeId,
};
use mongreldb_core::{Database, MongrelError, Value};
use tempfile::tempdir;

const DIM: usize = 8;
const K: usize = 10;

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
                ty: TypeId::Embedding { dim: DIM as u32 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
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

fn unit_embedding(axis: usize) -> Value {
    let mut v = vec![0.0f32; DIM];
    v[axis % DIM] = 1.0;
    Value::Embedding(v)
}

fn query_as(
    db: &Database,
    principal: &mongreldb_core::Principal,
    table_name: &str,
    query: &Query,
) -> mongreldb_core::Result<Vec<mongreldb_core::Row>> {
    let condition_columns = mongreldb_core::query::condition_columns(&query.conditions);
    db.with_authorized_read(
        table_name,
        Some(principal),
        true,
        |table, snapshot, allowed, effective_principal| {
            db.require_columns_for(
                table_name,
                mongreldb_core::ColumnOperation::Select,
                &condition_columns,
                effective_principal,
            )?;
            let rows = table.query_at_with_allowed(query, snapshot, allowed)?;
            db.secure_rows_for(table_name, rows, effective_principal)
        },
    )
}

#[test]
fn production_profile_hnsw_dense_auth() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let admin = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        assert!(
            admin.require_auth_enabled(),
            "recipe requires require_auth bootstrap"
        );

        admin.create_table("documents", documents_schema()).unwrap();
        {
            let handle = admin.table("documents").unwrap();
            let table = handle.lock();
            let ann = table
                .schema()
                .indexes
                .iter()
                .find(|idx| idx.name == "documents_embedding_ann")
                .and_then(|idx| idx.options.ann.as_ref())
                .expect("ANN index options");
            assert_eq!(ann.algorithm, AnnAlgorithm::Hnsw);
            assert_eq!(ann.quantization, AnnQuantization::Dense);
            assert_eq!(ann.m, 16);
            assert_eq!(ann.ef_construction, 64);
            assert_eq!(ann.ef_search, 64);
        }

        // Seed a few axis-aligned vectors so top-1 for e0 is row 0.
        let mut tx = admin.begin();
        for id in 0..5i64 {
            tx.put(
                "documents",
                vec![
                    (1, Value::Int64(id)),
                    (2, unit_embedding(id as usize)),
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();

        admin.create_user("reader", "reader-pw").unwrap();
        admin.create_user("nobody", "nobody-pw").unwrap();
        admin.create_role("ann_reader").unwrap();
        admin
            .grant_permission(
                "ann_reader",
                Permission::Select {
                    table: "documents".into(),
                },
            )
            .unwrap();
        admin.grant_role("reader", "ann_reader").unwrap();
        // "nobody" has no grants.

        let reader = admin.resolve_principal("reader").unwrap();
        let query = Query::new().and(Condition::Ann {
            column_id: 2,
            query: {
                let mut q = vec![0.0f32; DIM];
                q[0] = 1.0;
                q
            },
            k: K,
        });
        let rows = query_as(&admin, &reader, "documents", &query).unwrap();
        assert!(
            !rows.is_empty(),
            "authorized reader must receive ANN hits; got 0"
        );
        assert_eq!(
            rows[0].columns.get(&1),
            Some(&Value::Int64(0)),
            "nearest to e0 should be row 0"
        );

        // Scored retrieve under the same auth surface (Dense cosine).
        let retriever = Retriever::Ann {
            column_id: 2,
            query: {
                let mut q = vec![0.0f32; DIM];
                q[0] = 1.0;
                q
            },
            k: K,
        };
        let hits = admin
            .with_authorized_read(
                "documents",
                Some(&reader),
                true,
                |table, snapshot, allowed, effective_principal| {
                    db_require_embedding_select(&admin, "documents", effective_principal)?;
                    table.retrieve_at_with_allowed_and_context(
                        &retriever,
                        snapshot,
                        allowed,
                        None,
                    )
                },
            )
            .unwrap();
        assert!(!hits.is_empty());
        assert!(
            matches!(hits[0].score, RetrieverScore::AnnCosineDistance(_)),
            "Dense HNSW must report cosine distance scores, got {:?}",
            hits[0].score
        );

        let nobody = admin.resolve_principal("nobody").unwrap();
        match query_as(&admin, &nobody, "documents", &query) {
            Err(MongrelError::PermissionDenied { .. }) => {}
            other => panic!("expected PermissionDenied for unprivileged user, got {other:?}"),
        }
    }

    // Plain open must fail (require_auth).
    match Database::open(&path) {
        Err(MongrelError::AuthRequired) => {}
        other => panic!("expected AuthRequired on plain open, got {other:?}"),
    }

    // Reopen with credentials; current-snapshot ANN still works (Contract B:
    // do not reuse stashed Snapshot handles across reopen).
    let admin = Database::open_with_credentials(&path, "admin", "admin-pw").unwrap();
    assert!(admin.require_auth_enabled());
    let reader = admin.resolve_principal("reader").unwrap();
    let query = Query::new().and(Condition::Ann {
        column_id: 2,
        query: {
            let mut q = vec![0.0f32; DIM];
            q[0] = 1.0;
            q
        },
        k: K,
    });
    let rows = query_as(&admin, &reader, "documents", &query).unwrap();
    assert!(
        !rows.is_empty(),
        "post-reopen authorized ANN must return hits"
    );
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(0)));
}

/// Avoid duplicating column-id plumbing in the test body.
fn db_require_embedding_select(
    db: &Database,
    table: &str,
    principal: Option<&mongreldb_core::Principal>,
) -> mongreldb_core::Result<()> {
    db.require_columns_for(table, mongreldb_core::ColumnOperation::Select, &[2], principal)
}
