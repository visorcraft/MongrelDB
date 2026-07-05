//! Phase 8.2: reservoir-sample approximate aggregate correctness.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{ApproxAgg, Condition, Table, Value};
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
            },
            ColumnDef {
                id: 2,
                name: "category".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "cat_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

fn fill(dir: &std::path::Path, n: i64) -> Table {
    let mut db = Table::create(dir, schema(), 1).unwrap();
    for i in 0..n {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Int64(i % 10)),
            (3, Value::Int64(i * 2 + 1)),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    db
}

#[test]
fn approx_is_exact_when_sample_covers_table() {
    // 1000 rows < reservoir capacity (8192) ⇒ the sample is the whole table.
    let dir = tempdir().unwrap();
    let mut db = fill(dir.path(), 1000);

    let conds = [Condition::BitmapEq {
        column_id: 2,
        value: Value::Int64(0).encode_key(),
    }];

    // Exact filtered count = 100 rows (category 0: i = 0,10,…,990).
    let r = db
        .approx_aggregate(&conds, None, ApproxAgg::Count, 1.96)
        .unwrap()
        .unwrap();
    assert_eq!(r.point, 100.0);
    assert_eq!(r.ci_low, 100.0, "census ⇒ zero-width interval");
    assert_eq!(r.ci_high, 100.0);
    assert_eq!(r.n_passing, 100);
    assert_eq!(r.n_population, 1000);

    // Exact filtered sum of value (i*2+1 for i in {0,10,…,990}).
    let exact_sum: i64 = (0..1000).step_by(10).map(|i| i * 2 + 1).sum();
    let r = db
        .approx_aggregate(&conds, Some(3), ApproxAgg::Sum, 1.96)
        .unwrap()
        .unwrap();
    assert_eq!(r.point, exact_sum as f64);
    assert_eq!(r.ci_low, exact_sum as f64);

    // Exact filtered avg of value.
    let exact_avg = exact_sum as f64 / 100.0;
    let r = db
        .approx_aggregate(&conds, Some(3), ApproxAgg::Avg, 1.96)
        .unwrap()
        .unwrap();
    assert!((r.point - exact_avg).abs() < 1e-9);
    assert!((r.ci_low - exact_avg).abs() < 1e-9);
}

#[test]
fn approx_count_unfiltered_is_population() {
    let dir = tempdir().unwrap();
    let mut db = fill(dir.path(), 500);
    // COUNT(*) with no filter ⇒ exact population, regardless of sample.
    let r = db
        .approx_aggregate(&[], None, ApproxAgg::Count, 1.96)
        .unwrap()
        .unwrap();
    assert_eq!(r.point, 500.0);
    assert_eq!(r.ci_high, 500.0);
}

#[test]
fn approx_sampling_brackets_truth() {
    // 20_000 rows > reservoir capacity (8192, ~41 %) ⇒ genuine sampling with a
    // tight-enough CI to reliably bracket the truth. The point estimate must be
    // within a few %.
    let dir = tempdir().unwrap();
    let n = 20_000i64;
    let mut db = fill(dir.path(), n);

    let conds = [Condition::BitmapEq {
        column_id: 2,
        value: Value::Int64(3).encode_key(),
    }];

    // Exact values for category == 3 (i = 3,13,…): 2_000 rows.
    let exact_count = 2_000i64;
    let exact_sum: i64 = (3..n).step_by(10).map(|i| i * 2 + 1).sum();
    let exact_avg = exact_sum as f64 / exact_count as f64;

    let z = 1.96;
    let rc = db
        .approx_aggregate(&conds, None, ApproxAgg::Count, z)
        .unwrap()
        .unwrap();
    assert!(
        rc.ci_low <= exact_count as f64 && rc.ci_high >= exact_count as f64,
        "count CI {:?} must bracket {exact_count}",
        (rc.ci_low, rc.ci_high)
    );
    assert!(
        (rc.point - exact_count as f64).abs() / (exact_count as f64) < 0.05,
        "count point {rc:?} within 5%"
    );

    let rs = db
        .approx_aggregate(&conds, Some(3), ApproxAgg::Sum, z)
        .unwrap()
        .unwrap();
    assert!(
        rs.ci_low <= exact_sum as f64 && rs.ci_high >= exact_sum as f64,
        "sum CI {:?} must bracket {exact_sum}",
        (rs.ci_low, rs.ci_high)
    );

    let ra = db
        .approx_aggregate(&conds, Some(3), ApproxAgg::Avg, z)
        .unwrap()
        .unwrap();
    assert!(
        ra.ci_low <= exact_avg && ra.ci_high >= exact_avg,
        "avg CI {:?} must bracket {exact_avg}",
        (ra.ci_low, ra.ci_high)
    );
}

#[test]
fn approx_rebuilt_on_reopen() {
    // After reopen, the reservoir is repopulated from visible rows.
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let _ = fill(&path, 300);
    }
    let mut db = Table::open(&path).unwrap();
    let r = db
        .approx_aggregate(&[], None, ApproxAgg::Count, 1.96)
        .unwrap()
        .unwrap();
    assert_eq!(r.point, 300.0, "sample repopulated on open");
}
