use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn column(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
        default_value: None,
        embedding_source: None,
    }
}

fn index(name: &str, column_id: u16, kind: IndexKind, predicate: Option<&str>) -> IndexDef {
    IndexDef {
        name: name.into(),
        column_id,
        kind,
        predicate: predicate.map(str::to_owned),
        options: Default::default(),
    }
}

#[test]
fn partial_bitmap_cannot_filter_an_unqualified_query() {
    let dir = tempdir().unwrap();
    let schema = Schema {
        schema_id: 1,
        columns: vec![
            column(
                1,
                "id",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            column(2, "category", TypeId::Bytes, ColumnFlags::empty()),
            column(
                3,
                "active",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![index(
            "category_active",
            2,
            IndexKind::Bitmap,
            Some("active IS NOT NULL"),
        )],
        ..Schema::default()
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"same".to_vec())),
            (3, Value::Null),
        ])
        .unwrap();
    table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"same".to_vec())),
            (3, Value::Int64(1)),
        ])
        .unwrap();
    table.commit().unwrap();

    // Indexes are semantically transparent. A query that does not imply the
    // partial predicate must still see the row outside the predicate.
    let rows = table
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"same".to_vec(),
        }))
        .unwrap();
    assert_eq!(rows.len(), 2, "partial index caused a false negative");
}

#[test]
fn deleted_fm_candidate_must_not_consume_query_limit() {
    let dir = tempdir().unwrap();
    let schema = Schema {
        schema_id: 2,
        columns: vec![
            column(
                1,
                "id",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            column(2, "body", TypeId::Bytes, ColumnFlags::empty()),
        ],
        indexes: vec![index("body_fm", 2, IndexKind::FmIndex, None)],
        ..Schema::default()
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    let deleted = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"common first".to_vec())),
        ])
        .unwrap();
    table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"common second".to_vec())),
        ])
        .unwrap();
    table.commit().unwrap();
    table.delete(deleted).unwrap();
    table.commit().unwrap();

    let rows = table
        .query(
            &Query::new()
                .and(Condition::FmContains {
                    column_id: 2,
                    pattern: b"common".to_vec(),
                })
                .with_limit(1),
        )
        .unwrap();
    assert_eq!(rows.len(), 1, "stale FM candidate consumed LIMIT 1");
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(2)));
}

#[test]
fn encrypted_bitmap_replace_must_remove_tokenized_old_membership() {
    let dir = tempdir().unwrap();
    let schema = Schema {
        schema_id: 3,
        columns: vec![
            column(
                1,
                "id",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            column(
                2,
                "category",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::ENCRYPTED_INDEXABLE),
            ),
        ],
        indexes: vec![index("category_bitmap", 2, IndexKind::Bitmap, None)],
        ..Schema::default()
    };
    let mut table = Table::create_encrypted(dir.path(), schema, 1, "audit-pass").unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"alpha".to_vec())),
        ])
        .unwrap();
    table.commit().unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"alpha".to_vec())),
        ])
        .unwrap();
    table.commit().unwrap();

    let rows = table
        .query(
            &Query::new()
                .and(Condition::BitmapEq {
                    column_id: 2,
                    value: b"alpha".to_vec(),
                })
                .with_limit(1),
        )
        .unwrap();
    assert_eq!(rows.len(), 1, "stale encrypted bitmap id consumed LIMIT 1");
}

#[test]
fn rebuild_indexes_must_not_count_rows_tombstoned_in_a_newer_run() {
    let dir = tempdir().unwrap();
    let schema = Schema {
        schema_id: 4,
        columns: vec![
            column(
                1,
                "id",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            column(2, "group_id", TypeId::Int64, ColumnFlags::empty()),
        ],
        indexes: vec![index("group_bitmap", 2, IndexKind::Bitmap, None)],
        ..Schema::default()
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let row_id = table
        .put(vec![(1, Value::Int64(1)), (2, Value::Int64(7))])
        .unwrap();
    table.flush().unwrap();
    table.delete(row_id).unwrap();
    table.flush().unwrap();
    table.rebuild_indexes().unwrap();

    let count = table
        .fk_join_count(
            2,
            &[Value::Int64(7).encode_key()],
            &[],
            table.snapshot(),
        )
        .unwrap();
    assert_eq!(count, 0, "rebuild indexed a row hidden by a newer-run tombstone");
}
