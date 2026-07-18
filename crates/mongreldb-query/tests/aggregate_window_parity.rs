use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Table, Value};
use mongreldb_query::MongrelSession;
use rusqlite::{types::ValueRef, Connection};
use tempfile::tempdir;

#[derive(Clone)]
struct ParityRow {
    id: i64,
    grp: i64,
    val: Option<i64>,
    score: Option<f64>,
    label: Option<&'static str>,
}

fn rows() -> Vec<ParityRow> {
    vec![
        ParityRow {
            id: 1,
            grp: 1,
            val: Some(10),
            score: Some(1.5),
            label: Some("a"),
        },
        ParityRow {
            id: 2,
            grp: 1,
            val: Some(20),
            score: None,
            label: Some("b"),
        },
        ParityRow {
            id: 3,
            grp: 1,
            val: None,
            score: Some(3.5),
            label: Some("c"),
        },
        ParityRow {
            id: 4,
            grp: 2,
            val: Some(5),
            score: Some(4.0),
            label: Some("d"),
        },
        ParityRow {
            id: 5,
            grp: 2,
            val: Some(5),
            score: None,
            label: None,
        },
        ParityRow {
            id: 6,
            grp: 2,
            val: Some(30),
            score: Some(6.5),
            label: Some("e"),
        },
    ]
}

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
                name: "grp".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "val".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 4,
                name: "score".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 5,
                name: "label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

async fn setup_mongrel() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    for row in rows() {
        table
            .put(vec![
                (1, Value::Int64(row.id)),
                (2, Value::Int64(row.grp)),
                (3, row.val.map(Value::Int64).unwrap_or(Value::Null)),
                (4, row.score.map(Value::Float64).unwrap_or(Value::Null)),
                (
                    5,
                    row.label
                        .map(|value| Value::Bytes(value.as_bytes().to_vec()))
                        .unwrap_or(Value::Null),
                ),
            ])
            .unwrap();
    }
    table.flush().unwrap();
    let session = MongrelSession::new(table);
    session.register("parity").await.unwrap();
    (dir, session)
}

fn setup_sqlite() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "create table parity(
            id integer primary key,
            grp integer not null,
            val integer,
            score real,
            label text
        );",
    )
    .unwrap();
    for row in rows() {
        conn.execute(
            "insert into parity(id, grp, val, score, label) values (?1, ?2, ?3, ?4, ?5)",
            (row.id, row.grp, row.val, row.score, row.label),
        )
        .unwrap();
    }
    conn
}

async fn mongrel_rows(session: &MongrelSession, sql: &str) -> Vec<Vec<String>> {
    session
        .run(sql)
        .await
        .unwrap()
        .iter()
        .flat_map(batch_rows)
        .collect()
}

fn batch_rows(batch: &RecordBatch) -> Vec<Vec<String>> {
    (0..batch.num_rows())
        .map(|row| {
            (0..batch.num_columns())
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        return "NULL".to_string();
                    }
                    let scalar = ScalarValue::try_from_array(array, row).unwrap();
                    canonical_arrow_scalar(&scalar)
                })
                .collect()
        })
        .collect()
}

fn canonical_arrow_scalar(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Int64(Some(value)) => value.to_string(),
        ScalarValue::Float64(Some(value)) => canonical_float(*value),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => value.clone(),
        other => other.to_string(),
    }
}

fn sqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let columns = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            (0..columns)
                .map(|idx| match row.get_ref(idx).unwrap() {
                    ValueRef::Null => "NULL".to_string(),
                    ValueRef::Integer(value) => value.to_string(),
                    ValueRef::Real(value) => canonical_float(value),
                    ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned(),
                    ValueRef::Blob(value) => value
                        .iter()
                        .map(|byte| format!("{byte:02X}"))
                        .collect::<String>(),
                })
                .collect::<Vec<_>>()
                .pipe(Ok)
        })
        .unwrap();
    rows.map(Result::unwrap).collect()
}

fn canonical_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.6}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

async fn assert_sqlite_parity(sql: &str) {
    let (_dir, session) = setup_mongrel().await;
    let conn = setup_sqlite();
    assert_eq!(
        mongrel_rows(&session, sql).await,
        sqlite_rows(&conn, sql),
        "{sql}"
    );
}

#[tokio::test]
async fn grouped_aggregate_results_match_sqlite() {
    assert_sqlite_parity(
        "select grp,
                count(*) as count_all,
                count(val) as count_val,
                sum(val) as sum_val,
                avg(val) as avg_val,
                min(val) as min_val,
                max(val) as max_val
         from parity
         group by grp
         order by grp",
    )
    .await;
}

#[tokio::test]
async fn sqlite_compat_aggregate_aliases_match_sqlite() {
    assert_sqlite_parity(
        "select grp,
                group_concat(label) as labels,
                group_concat(label, '|') as labels_piped,
                total(val) as total_val
         from parity
         group by grp
         order by grp",
    )
    .await;

    assert_sqlite_parity(
        "select total(val),
                total(val) filter (where grp = 2),
                total(val) filter (where grp = 99)
         from parity
         where grp = 99",
    )
    .await;
}

#[tokio::test]
async fn percentile_family_follows_sqlite_extension_rules() {
    let (_dir, session) = setup_mongrel().await;

    let rows = mongrel_rows(
        &session,
        "select grp,
                median(val),
                percentile(val, 50),
                percentile_cont(val, 0.5),
                percentile_disc(val, 0.5)
         from parity
         group by grp
         order by grp",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![
                "1".to_string(),
                "15.0".to_string(),
                "15.0".to_string(),
                "15.0".to_string(),
                "10.0".to_string()
            ],
            vec![
                "2".to_string(),
                "5.0".to_string(),
                "5.0".to_string(),
                "5.0".to_string(),
                "5.0".to_string()
            ]
        ]
    );

    let rows = mongrel_rows(
        &session,
        "select percentile(val, 75),
                percentile_cont(val, 0.75),
                percentile_disc(val, 0.75)
         from parity",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            "20.0".to_string(),
            "20.0".to_string(),
            "20.0".to_string()
        ]]
    );

    let rows = mongrel_rows(
        &session,
        "select median(val), percentile(val, 50) from parity where grp = 99",
    )
    .await;
    assert_eq!(rows, vec![vec!["NULL".to_string(), "NULL".to_string()]]);
}

#[tokio::test]
async fn percentile_family_rejects_invalid_percentile_arguments() {
    let (_dir, session) = setup_mongrel().await;

    let err = session
        .run("select percentile(val, 101) from parity")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("between 0"));

    let err = session
        .run("select percentile(val, id) from parity")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("same for every row"));
}

#[tokio::test]
async fn aggregate_null_distinct_and_filter_results_match_sqlite() {
    assert_sqlite_parity(
        "select count(distinct val),
                sum(distinct val),
                avg(distinct val),
                count(*) filter (where score is null),
                sum(val) filter (where grp = 2)
         from parity",
    )
    .await;
}

#[tokio::test]
async fn ranking_window_results_match_sqlite() {
    assert_sqlite_parity(
        "select id,
                grp,
                row_number() over (partition by grp order by val nulls last, id) as rn,
                rank() over (partition by grp order by val nulls last) as rnk,
                dense_rank() over (partition by grp order by val nulls last) as dense
         from parity
         order by grp, rn",
    )
    .await;
}

#[tokio::test]
async fn offset_and_running_window_results_match_sqlite() {
    assert_sqlite_parity(
        "select id,
                grp,
                lag(val, 1, -1) over (partition by grp order by id) as prev_val,
                lead(val, 1, -1) over (partition by grp order by id) as next_val,
                sum(coalesce(val, 0)) over (
                    partition by grp
                    order by id
                    rows between unbounded preceding and current row
                ) as running_sum
         from parity
         order by grp, id",
    )
    .await;
}

#[tokio::test]
async fn percentile_window_results_follow_sqlite_extension_rules() {
    let (_dir, session) = setup_mongrel().await;
    let rows = mongrel_rows(
        &session,
        "select id,
                percentile_cont(val, 0.5) over (
                    partition by grp
                    order by id
                    rows between unbounded preceding and current row
                ) as running_p50
         from parity
         order by id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "10.0".to_string()],
            vec!["2".to_string(), "15.0".to_string()],
            vec!["3".to_string(), "15.0".to_string()],
            vec!["4".to_string(), "5.0".to_string()],
            vec!["5".to_string(), "5.0".to_string()],
            vec!["6".to_string(), "5.0".to_string()],
        ]
    );
}
