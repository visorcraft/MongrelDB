use mongreldb_core::{
    database::VTAB_DIR, ColumnDef, ColumnFlags, Database, ExternalTableDefinition,
    ExternalTableEntry, ModuleCapabilities, Schema, StoredTrigger, TriggerCell, TriggerDefinition,
    TriggerEvent, TriggerProgram, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue, TypeId,
    Value,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn insert_trigger(name: &str, with_audit_write: bool) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table("items".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: if with_audit_write {
                    vec![TriggerStep::Insert {
                        table: "audit".into(),
                        cells: vec![TriggerCell {
                            column_id: 1,
                            value: TriggerValue::NewColumn(1),
                        }],
                    }]
                } else {
                    Vec::new()
                },
            },
        },
        0,
    )
    .unwrap()
}

fn external(name: &str) -> ExternalTableEntry {
    ExternalTableEntry::new(
        name,
        ExternalTableDefinition {
            module: "test".into(),
            args: vec![],
            declared_schema: schema(),
            hidden_columns: vec![],
            options: BTreeMap::new(),
            capabilities: ModuleCapabilities {
                writable: true,
                transaction_safe: true,
                ..ModuleCapabilities::default()
            },
        },
        0,
    )
    .unwrap()
}

#[test]
fn prepared_transaction_rejects_dropped_and_recreated_table_generation() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let old_id = db.create_table("items", schema()).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let hook_entered = entered.clone();
    let hook_resume = resume.clone();
    db.__set_catalog_commit_hook(move || {
        hook_entered.wait();
        hook_resume.wait();
    });
    let mut transaction = db.begin();
    transaction
        .put("items", vec![(1, Value::Int64(1))])
        .unwrap();

    std::thread::scope(|scope| {
        let commit = scope.spawn(move || transaction.commit());
        entered.wait();
        db.drop_table("items").unwrap();
        let new_id = db.create_table("items", schema()).unwrap();
        assert_ne!(new_id, old_id);
        resume.wait();
        assert!(commit.join().unwrap().is_err());
    });

    assert_eq!(db.table("items").unwrap().lock().count(), 0);
}

#[test]
fn prepared_transaction_rejects_recreated_external_generation() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_external_table(external("ext")).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let hook_entered = entered.clone();
    let hook_resume = resume.clone();
    db.__set_catalog_commit_hook(move || {
        hook_entered.wait();
        hook_resume.wait();
    });
    let mut transaction = db.begin();
    transaction
        .put_external_state("ext", b"old generation".to_vec())
        .unwrap();

    std::thread::scope(|scope| {
        let commit = scope.spawn(move || transaction.commit());
        entered.wait();
        db.drop_external_table("ext").unwrap();
        db.create_external_table(external("ext")).unwrap();
        resume.wait();
        assert!(commit.join().unwrap().is_err());
    });

    assert!(!dir
        .path()
        .join(VTAB_DIR)
        .join("ext")
        .join("state.json")
        .exists());
}

#[test]
fn prepared_transaction_rejects_replaced_trigger_revision() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_table("audit", schema()).unwrap();
    db.create_trigger(insert_trigger("items_ai", true)).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    db.__set_catalog_commit_hook({
        let entered = Arc::clone(&entered);
        let resume = Arc::clone(&resume);
        move || {
            entered.wait();
            resume.wait();
        }
    });
    let mut transaction = db.begin();
    transaction
        .put("items", vec![(1, Value::Int64(1))])
        .unwrap();

    std::thread::scope(|scope| {
        let commit = scope.spawn(move || transaction.commit());
        entered.wait();
        db.create_or_replace_trigger(insert_trigger("items_ai", false))
            .unwrap();
        resume.wait();
        assert!(commit.join().unwrap().is_err());
    });

    assert_eq!(db.table("items").unwrap().lock().count(), 0);
    assert_eq!(db.table("audit").unwrap().lock().count(), 0);
}

#[test]
fn recovery_does_not_resurrect_state_from_an_old_external_generation() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_external_table(external("ext")).unwrap();
        db.commit_external_table_state("ext", b"old generation")
            .unwrap();
        db.drop_external_table("ext").unwrap();
        db.create_external_table(external("ext")).unwrap();
    }

    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.external_table("ext").is_some());
    assert!(!dir
        .path()
        .join(VTAB_DIR)
        .join("ext")
        .join("state.json")
        .exists());
}
