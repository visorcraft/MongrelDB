use mongreldb_core::{
    database::VTAB_DIR, ColumnDef, ColumnFlags, Database, ExternalTableDefinition,
    ExternalTableEntry, ModuleArg, ModuleCapabilities, Schema, StoredTrigger, TriggerCell,
    TriggerCondition, TriggerDefinition, TriggerEvent, TriggerProgram, TriggerStep, TriggerTarget,
    TriggerTiming, TriggerValue, TypeId, Value,
};
use tempfile::tempdir;

fn base_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn external_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![ColumnDef {
            id: 1,
            name: "value".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty(),
            default_value: None,
            embedding_source: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn external_entry(name: &str, trigger_safe: bool) -> ExternalTableEntry {
    ExternalTableEntry::new(
        name,
        ExternalTableDefinition {
            module: "series".into(),
            args: vec![ModuleArg::Number("3".into())],
            declared_schema: external_schema(),
            hidden_columns: Vec::new(),
            options: Default::default(),
            capabilities: ModuleCapabilities {
                read_only: true,
                deterministic: true,
                trigger_safe,
                ..ModuleCapabilities::default()
            },
        },
        0,
    )
    .unwrap()
}

fn writable_external_entry(name: &str) -> ExternalTableEntry {
    ExternalTableEntry::new(
        name,
        ExternalTableDefinition {
            module: "writable_ext".into(),
            args: Vec::new(),
            declared_schema: external_schema(),
            hidden_columns: Vec::new(),
            options: Default::default(),
            capabilities: ModuleCapabilities {
                writable: true,
                trigger_safe: true,
                transaction_safe: true,
                ..ModuleCapabilities::default()
            },
        },
        0,
    )
    .unwrap()
}

fn select_external_trigger(name: &str, source: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table("base".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::Select {
                    id: "probe".into(),
                    table: source.into(),
                    conditions: vec![TriggerCondition::IsNotNull { column_id: 1 }],
                }],
            },
        },
        0,
    )
    .unwrap()
}

fn insert_external_trigger(name: &str, target: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table("base".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::Insert {
                    table: target.into(),
                    cells: vec![TriggerCell {
                        column_id: 1,
                        value: TriggerValue::NewColumn(1),
                    }],
                }],
            },
        },
        0,
    )
    .unwrap()
}

#[test]
fn trigger_safe_external_tables_are_valid_trigger_sources() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("base", base_schema()).unwrap();
    db.create_external_table(external_entry("ext", true))
        .unwrap();

    let trigger = db
        .create_trigger(select_external_trigger("base_ai", "ext"))
        .unwrap();
    assert_eq!(trigger.name, "base_ai");
}

#[test]
fn non_trigger_safe_external_tables_are_rejected_as_trigger_sources() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("base", base_schema()).unwrap();
    db.create_external_table(external_entry("ext", false))
        .unwrap();

    let err = db
        .create_trigger(select_external_trigger("base_ai", "ext"))
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("external table"), "{msg}");
    assert!(msg.contains("not trigger-safe"), "{msg}");
}

#[test]
fn plain_core_transactions_require_external_trigger_bridge_for_external_trigger_targets() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("base", base_schema()).unwrap();
    db.create_external_table(writable_external_entry("ext"))
        .unwrap();
    db.create_trigger(insert_external_trigger("base_ai", "ext"))
        .unwrap();

    let err = db
        .transaction(|tx| tx.put("base", vec![(1, Value::Int64(7))]).map(|_| ()))
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("external table"), "{msg}");
    assert!(msg.contains("external trigger bridge"), "{msg}");
}

#[test]
fn orphan_external_table_state_is_reported_and_reclaimed_by_gc() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_external_table(external_entry("ext", true))
        .unwrap();

    let vtab_dir = db.root().join(VTAB_DIR);
    std::fs::create_dir_all(vtab_dir.join("ext")).unwrap();
    std::fs::create_dir_all(vtab_dir.join("orphan")).unwrap();

    let issues = db.check();
    assert!(
        issues.iter().any(|issue| issue.severity == "warning"
            && issue.table_name == "orphan"
            && issue.description.contains("orphan external table state")),
        "{issues:?}"
    );

    let reclaimed = db.gc().unwrap();
    assert!(reclaimed >= 1);
    assert!(vtab_dir.join("ext").exists());
    assert!(!vtab_dir.join("orphan").exists());
}
