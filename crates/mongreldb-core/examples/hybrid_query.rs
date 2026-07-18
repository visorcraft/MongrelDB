//! Hybrid-query demo — the single differentiating query no SQL FTS pipeline or
//! HTTP vector DB can serve in one shot:
//!
//!     ann_search(vec) ∩ fm_contains(text) ∩ bitmap_eq(category)
//!
//! Loads a small trip dataset, then issues ONE `Table::query` that intersects
//! HNSW semantic search, FM-index substring, and a roaring bitmap — all over the
//! shared row-id space, in-process, with no network hop.
//!
//! Run: `cargo run -p mongreldb-core --example hybrid_query --release`

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

const DIM: usize = 16;

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
                name: "city".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "blurb".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 4,
                name: "category".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 5,
                name: "vec".into(),
                ty: TypeId::Embedding { dim: DIM as u32 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "blurb_fm".into(),
                column_id: 3,
                predicate: None,
                kind: IndexKind::FmIndex,
                options: Default::default(),
            },
            IndexDef {
                name: "category_bm".into(),
                column_id: 4,
                predicate: None,
                kind: IndexKind::Bitmap,
                options: Default::default(),
            },
            IndexDef {
                name: "vec_ann".into(),
                column_id: 5,
                predicate: None,
                kind: IndexKind::Ann,
                options: Default::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// Deterministic pseudo-embedding for a label so "similar" labels land nearby.
fn emb(label: &str) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    for (i, b) in label.bytes().cycle().take(DIM).enumerate() {
        v[i] = if (b + i as u8).is_multiple_of(3) {
            1.0
        } else {
            -1.0
        };
    }
    v
}

fn main() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(dir.path(), schema(), 1)?;

    let cities = [
        ("Rome", "city", "ancient ruins and pasta in rome"),
        (
            "Paris",
            "city",
            "cafe culture and the eiffel tower in paris",
        ),
        ("Tokyo", "city", "neon nights and sushi in tokyo"),
        ("Malibu", "beach", "surf and sun on the malibu coast"),
        ("Aspen", "mountain", "powder skiing in aspen"),
        ("Amalfi", "beach", "cliffside lemon groves above amalfi"),
    ];

    // 6 000 rows: 1 000 per city, each blurb tagged with the city + an index.
    let mut rows = Vec::with_capacity(6_000);
    for i in 0..6_000i64 {
        let (city, cat, base) = &cities[(i % 6) as usize];
        rows.push(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(city.as_bytes().to_vec())),
            (3, Value::Bytes(format!("{base} #{i}").into_bytes())),
            (4, Value::Bytes(cat.as_bytes().to_vec())),
            (5, Value::Embedding(emb(city))),
        ]);
    }
    db.bulk_load(rows)?;
    println!("loaded {} rows across {} cities", db.count(), cities.len());

    // The hybrid query: semantic nearest to "Rome" embeddings ∩ blurb mentions
    // "rome" ∩ category == "city". One call, three index families intersected.
    let q = Query::new()
        .and(Condition::Ann {
            column_id: 5,
            query: emb("Rome"),
            k: 64,
        })
        .and(Condition::FmContains {
            column_id: 3,
            pattern: b"rome".to_vec(),
        })
        .and(Condition::BitmapEq {
            column_id: 4,
            value: b"city".to_vec(),
        });

    let rows = db.query(&q)?;
    println!(
        "\nhybrid query → {} surviving rows (ann ∩ fm ∩ bitmap)",
        rows.len()
    );
    for r in rows.iter().take(8) {
        let city = match r.columns.get(&2) {
            Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
            _ => "?".to_string(),
        };
        let blurb = match r.columns.get(&3) {
            Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
            _ => "?".to_string(),
        };
        println!(
            "  row_id={:>4}  city={:<7}  blurb=\"{}\"",
            r.row_id.0, city, blurb
        );
    }
    if rows.len() > 8 {
        println!("  … ({} more)", rows.len() - 8);
    }

    // Correctness check: every survivor must satisfy all three predicates.
    for r in &rows {
        let cat = match r.columns.get(&4) {
            Some(Value::Bytes(b)) => b.as_slice(),
            _ => &[],
        };
        assert_eq!(cat, b"city", "bitmap predicate violated");
        let blurb = match r.columns.get(&3) {
            Some(Value::Bytes(b)) => std::str::from_utf8(b).unwrap_or(""),
            _ => "",
        };
        assert!(
            blurb.to_ascii_lowercase().contains("rome"),
            "fm predicate violated: {blurb}"
        );
    }
    println!("\n✓ all survivors satisfy ann ∧ fm_contains(\"rome\") ∧ category=city");

    Ok(())
}
