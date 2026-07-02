//! §5.10: Standalone `import-table` utility.
//!
//! Imports an old single-table MongrelDB directory into a table in a new
//! multi-table `Database`. Previously only proven as a manual pattern in a
//! test (`check_doctor.rs::import_single_table_into_database`); this wraps it
//! as a runnable utility.
//!
//! Usage:
//!   cargo run --example import_table -- <old-single-table-dir> <new-database-dir> <table-name>
//!
//! Reads the schema from the old table's manifest, creates `<table-name>` in
//! the database (or a fresh one), and bulk-inserts every visible row.

use std::env;
use std::process::ExitCode;

use mongreldb_core::{Database, Table};

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: import_table <old-single-table-dir> <new-database-dir> <table-name>");
        return ExitCode::from(2);
    }
    let old_dir = &args[1];
    let new_dir = &args[2];
    let table_name = &args[3];

    if let Err(e) = run(old_dir, new_dir, table_name) {
        eprintln!("import failed: {e}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn run(old_dir: &str, new_dir: &str, table_name: &str) -> mongreldb_core::Result<()> {
    let old = Table::open(old_dir)?;
    let schema = old.schema().clone();
    let snap = old.snapshot();
    let rows = old.visible_rows(snap)?;
    drop(old);

    let n = rows.len();
    let db = Database::create(new_dir)?;
    db.create_table(table_name, schema)?;

    for row in rows {
        let cells: Vec<(u16, mongreldb_core::Value)> =
            row.columns.iter().map(|(&cid, v)| (cid, v.clone())).collect();
        db.transaction(|t| {
            t.put(table_name, cells)?;
            Ok(())
        })?;
    }

    eprintln!("imported {n} rows into '{table_name}' at {new_dir}");
    Ok(())
}
