//! Authenticated 10k-row commit characterization.

use mongreldb_core::{ColumnDef, ColumnFlags, Database, Permission, Schema, TypeId, Value};
use std::time::Instant;

fn main() {
    let rows = std::env::var("MONGRELDB_AUTH_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10_000);
    let dir = tempfile::tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin
        .create_table(
            "docs",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let writer = Database::open_with_credentials(dir.path(), "writer", "writer-pw").unwrap();
    let reads_before = writer.security_catalog_disk_read_count();
    let started = Instant::now();
    writer
        .transaction(|transaction| {
            transaction.put_batch(
                "docs",
                (0..rows)
                    .map(|id| vec![(1, Value::Int64(id as i64))])
                    .collect(),
            )?;
            Ok(())
        })
        .unwrap();
    println!(
        "{}",
        serde_json::json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": rows,
            "elapsed_ms": started.elapsed().as_millis(),
            "catalog_disk_reads": writer
                .security_catalog_disk_read_count()
                .saturating_sub(reads_before),
            "rows_committed": writer.table("docs").unwrap().lock().count(),
        })
    );
}
